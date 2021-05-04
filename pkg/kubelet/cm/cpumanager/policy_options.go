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

import "fmt"

const (
	// RejectNonSMTAlignedOption is the name of the CPU Manager policy option
	RejectNonSMTAlignedOption string = "reject-non-smt-aligned"
)

type StaticPolicyOptions struct {
	// flag to enable extra allocation restrictions to avoid
	// different containers to possibly end up on the same core
	RejectNonSMTAligned bool
}

func NewStaticPolicyOptions(policyOptions []string) (StaticPolicyOptions, error) {
	opts := StaticPolicyOptions{}
	for _, policyOption := range policyOptions {
		switch policyOption {
		case RejectNonSMTAlignedOption:
			opts.RejectNonSMTAligned = true
		default:
			return opts, fmt.Errorf("unsupported cpumanager option: %q", policyOption)
		}
	}
	return opts, nil
}
