/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpumanager

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

const PolicySMTIsolate policyName = "smtisolate"

type smtIsolatePolicy struct {
	*staticPolicy
	cpuIDsByCoreID map[int]cpuset.CPUSet
}

// Ensure smtIsolatePolicy implements Policy interface
var _ Policy = &smtIsolatePolicy{}

func NewSMTIsolatePolicy(topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, affinity topologymanager.Store) (Policy, error) {
	pol, err := NewStaticPolicy(topology, numReservedCPUs, reservedCPUs, affinity)
	if err != nil {
		return &smtIsolatePolicy{}, err
	}
	p, ok := pol.(*staticPolicy)
	if !ok {
		return &smtIsolatePolicy{}, fmt.Errorf("Unexpected policy type")
	}
	return &smtIsolatePolicy{
		staticPolicy:   p,
		cpuIDsByCoreID: partitionCPUsByCoreID(topology),
	}, nil
}

func (p *smtIsolatePolicy) Name() string {
	return string(PolicySMTIsolate)
}

func (p *smtIsolatePolicy) Admit(allocatableCPUs cpuset.CPUSet, pod *v1.Pod) lifecycle.PodAdmitResult {
	cpusPerCore := p.topology.CPUsPerCore()
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		requested := p.guaranteedCPUs(pod, &container)
		if requested == 0 {
			continue
		}
		coreRequested := requested % cpusPerCore
		if coreRequested > 0 {
			// we intentionally don't check we have enough free cores. We just check we have enough free cpus.
			// This works because the allocation later on will be performed in terms of full cores (or multiples of).
			// TODO: expand.
			return coresAllocationError()
		}

		// TODO: is safe to run this check here? (races)
		// TODO: just trust the modulus?
		allocatableCores := p.narrowCPUsByCore(allocatableCPUs.Union(p.cpusToReuse[string(pod.UID)]))
		if allocatableCores.Size() < coreRequested {
			return coresAllocationError()
		}
	}
	return p.staticPolicy.Admit(allocatableCPUs, pod)
}

func (p *smtIsolatePolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	if numCPUs := p.guaranteedCPUs(pod, container); numCPUs != 0 {
		klog.InfoS("smtisolate policy: Allocate", "pod", klog.KObj(pod), "containerName", container.Name)
		// container belongs in an exclusively allocated pool

		if cpuset, ok := s.GetCPUSet(string(pod.UID), container.Name); ok {
			p.updateCPUsToReuse(pod, container, cpuset)
			klog.InfoS("smtisolate policy: container already present in state, skipping", "pod", klog.KObj(pod), "containerName", container.Name)
			return nil
		}

		// Call Topology Manager to get the aligned socket affinity across all hint providers.
		hint := p.affinity.GetAffinity(string(pod.UID), container.Name)
		klog.InfoS("Topology Affinity", "pod", klog.KObj(pod), "containerName", container.Name, "affinity", hint)

		// Allocate CPUs according to the NUMA affinity contained in the hint.
		cpuset, err := p.allocateCPUs(s, numCPUs, hint.NUMANodeAffinity, p.cpusToReuse[string(pod.UID)])
		if err != nil {
			klog.ErrorS(err, "Unable to allocate CPUs", "pod", klog.KObj(pod), "containerName", container.Name, "numCPUs", numCPUs)
			return err
		}
		// TODO: explain
		s.SetCPUSet(string(pod.UID), container.Name, p.narrowCPUsByCore(cpuset))
		p.updateCPUsToReuse(pod, container, cpuset)

	}
	// container belongs in the shared pool (nothing to do; use default cpuset)
	return nil
}

func (p *smtIsolatePolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	klog.InfoS("Static policy: RemoveContainer", "podUID", podUID, "containerName", containerName)
	if toRelease, ok := s.GetCPUSet(podUID, containerName); ok {
		s.Delete(podUID, containerName)
		// Mutate the shared pool, adding released cpus.
		// TODO: explain
		s.SetDefaultCPUSet(s.GetDefaultCPUSet().Union(p.expandCPUsByCore(toRelease)))
	}
	return nil
}

// TODO: this can be improved like a lot, algorithm wise
// cpus -> cpu (one per core).
func (p *smtIsolatePolicy) narrowCPUsByCore(cset cpuset.CPUSet) cpuset.CPUSet {
	b := cpuset.NewBuilder()
	cores := make(map[int]struct{})
	for _, cpuID := range cset.ToSliceNoSort() {
		info := p.topology.CPUDetails[cpuID]
		if _, ok := cores[info.CoreID]; !ok {
			cores[info.CoreID] = struct{}{}
			b.Add(cpuID)
		}
	}
	return b.Result()
}

// TODO: this can be improved like a lot, algorithm wise
// cpu (one per core) -> all cpus on the same core
func (p *smtIsolatePolicy) expandCPUsByCore(cset cpuset.CPUSet) cpuset.CPUSet {
	b := cpuset.NewBuilder()
	for _, cpuID := range cset.ToSliceNoSort() {
		info := p.topology.CPUDetails[cpuID]
		cset := p.cpuIDsByCoreID[info.CoreID]
		b.Add(cset.ToSliceNoSort()...)
	}
	return b.Result()
}

// TODO: this can be improved like a lot
// coreID -> cpuset
func partitionCPUsByCoreID(topology *topology.CPUTopology) map[int]cpuset.CPUSet {
	res := make(map[int]cpuset.CPUSet)
	for cpuID, info := range topology.CPUDetails {
		res[info.CoreID] = res[info.CoreID].Union(cpuset.NewCPUSet(cpuID))
	}
	return res
}
