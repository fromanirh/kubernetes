/*
Copyright 2017 The Kubernetes Authors.

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
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// PolicySMTAware is the name of the static policy
const PolicySMTAware policyName = "smtaware"

type smtAwarePolicy struct {
	// TODO: use smarter golang struct composition?
	p *staticPolicy
}

// Ensure smtAwarePolicy implements Policy interface
var _ Policy = &smtAwarePolicy{}

// NewStaticPolicy returns a CPU manager policy that does not change CPU
// assignments for exclusively pinned guaranteed containers after the main
// container process starts.
func NewSMTAwarePolicy(topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, affinity topologymanager.Store) (Policy, error) {
	pol := smtAwarePolicy{}
	// TODO: remove, use cast
	sp, err := newStaticPolicy(topology, numReservedCPUs, reservedCPUs, affinity)
	if err != nil {
		return &pol, err
	}
	pol.p = sp
	return &pol, nil
}

func (p *smtAwarePolicy) Name() string {
	return string(PolicySMTAware)
}

func (p *smtAwarePolicy) Start(s state.State) error {
	return p.p.Start(s)
}

func (p *smtAwarePolicy) GetAllocatableCPUs(s state.State) cpuset.CPUSet {
	return p.p.GetAllocatableCPUs(s)
}

func (p *smtAwarePolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	return p.p.Allocate(s, pod, container)
}

func (p *smtAwarePolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	return p.p.RemoveContainer(s, podUID, containerName)
}

func (p *smtAwarePolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	return p.p.GetTopologyHints(s, pod, container)
}

func (p *smtAwarePolicy) GetPodTopologyHints(s state.State, pod *v1.Pod) map[string][]topologymanager.TopologyHint {
	return p.p.GetPodTopologyHints(s, pod)
}

func (p *smtAwarePolicy) Admit(pod *v1.Pod) lifecycle.PodAdmitResult {
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if requested := p.p.guaranteedCPUs(pod, &container); requested > 0 && requested%2 != 0 {
			return coresAllocationError()
		}
	}
	return admitPod()
}
