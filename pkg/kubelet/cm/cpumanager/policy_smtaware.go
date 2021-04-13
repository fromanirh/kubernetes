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

	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpuset"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

// PolicySMTAware is the name of the static policy
const PolicySMTAware policyName = "smtaware"

type smtAwarePolicy struct {
	*staticPolicy
}

// Ensure smtAwarePolicy implements Policy interface
var _ Policy = &smtAwarePolicy{}

func NewSMTAwarePolicy(topology *topology.CPUTopology, numReservedCPUs int, reservedCPUs cpuset.CPUSet, affinity topologymanager.Store) (Policy, error) {
	pol, err := NewStaticPolicy(topology, numReservedCPUs, reservedCPUs, affinity)
	if err != nil {
		return &smtAwarePolicy{}, err
	}
	p, ok := pol.(*staticPolicy)
	if !ok {
		return &smtAwarePolicy{}, fmt.Errorf("Unexpected policy type")
	}
	return &smtAwarePolicy{p}, nil
}

func (p *smtAwarePolicy) Name() string {
	return string(PolicySMTAware)
}

func (p *smtAwarePolicy) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	pod := attrs.Pod
	cpusPerCore := p.topology.CPUsPerCore()
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		if requested := p.guaranteedCPUs(pod, &container); requested > 0 && (requested%cpusPerCore) != 0 {
			// we intentionally don't check we have enough free cores. We just check we have enough free cpus.
			// This works because the allocation later on will be performed in terms of full cores (or multiples of).
			// TODO: expand.
			return coresAllocationError(requested, cpusPerCore)
		}
	}
	return admitPod()
}

// TODO explain why takeByTopology is OK
