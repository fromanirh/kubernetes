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

package admission

import (
	"fmt"

	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

const (
	ErrorReasonUnexpected       string = "UnexpectedAdmissionError"
	ErrorReasonTopologyAffinity string = "TopologyAffinityError"
	ErrorReasonSMTAlignment     string = "SMTAlignmentError"
)

func AdmitPod() lifecycle.PodAdmitResult {
	return lifecycle.PodAdmitResult{Admit: true}
}

func AdmissionError(err error) lifecycle.PodAdmitResult {
	if _, ok := err.(cpumanager.SMTAlignmentError); ok {
		return SMTAlignmentError(err)
	}
	if _, ok := err.(topologymanager.TopologyAffinityError); ok {
		return TopologyAffinityError(err)
	}
	// default, no explicit check
	return UnexpectedAdmissionError(err)

}

func UnexpectedAdmissionError(err error) lifecycle.PodAdmitResult {
	return lifecycle.PodAdmitResult{
		Message: fmt.Sprintf("Allocate failed due to %v, which is unexpected", err),
		Reason:  ErrorReasonUnexpected,
		Admit:   false,
	}
}

func TopologyAffinityError(err error) lifecycle.PodAdmitResult {
	return lifecycle.PodAdmitResult{
		Message: err.Error(),
		Reason:  ErrorReasonTopologyAffinity,
		Admit:   false,
	}
}

func SMTAlignmentError(err error) lifecycle.PodAdmitResult {
	return lifecycle.PodAdmitResult{
		Message: err.Error(),
		Reason:  ErrorReasonSMTAlignment,
		Admit:   false,
	}
}
