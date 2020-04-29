/*
Copyright 2020 The Kubernetes Authors.

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

package e2enode

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	//	"k8s.io/apimachinery/pkg/runtime"
	//	testutils "k8s.io/kubernetes/test/utils"

	"k8s.io/apimachinery/pkg/api/resource"
	//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	//	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	//	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/test/e2e/framework"
	//	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	//	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	//	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"

	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// Serial because the test updates kubelet configuration.
// Warning: this test depend on topology manager being configured but tests a separate functionalty
// thus we believe it should be kept separated
var _ = SIGDescribe("Resources per-NUMA [Serial] [Feature:TopologyManager][NodeBetaFeature:TopologyManager]", func() {
	fw := framework.NewDefaultFramework("resources-pernuma-test")

	ginkgo.Context("With kubeconfig updated to static CPU Manager policy run the Topology Manager tests", func() {
		var oldCfg *kubeletconfig.KubeletConfiguration
		var err error

		policy := topologymanager.PolicySingleNumaNode

		ginkgo.It("should account per-NUMA resources when running pods", func() {
			numaNodes := detectNUMANodes()

			oldCfg, err = getCurrentKubeletConfig()
			framework.ExpectNoError(err)

			// Configure Topology Manager
			ginkgo.By(fmt.Sprintf("by configuring Topology Manager policy to %s", policy))
			framework.Logf("Configuring topology Manager policy to %s", policy)
			configureTopologyManagerInKubelet(fw, oldCfg, policy, nil, 0)

			ginkgo.By("checking resource for 1 pod, 1 container, requesting 1 cpu, 0 devices")
			runPodsAndCheckResourceNodes(fw, numaNodes, 1, []tmCtnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1000m",
					cpuLimit:   "1000m",
				},
			})

			ginkgo.By("checking resource for 1 pod, 1 container, requesting 4 cpus, 0 devices")
			runPodsAndCheckResourceNodes(fw, numaNodes, 1, []tmCtnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "4000m",
					cpuLimit:   "4000m",
				},
			})

			// restore kubelet config
			setOldKubeletConfig(fw, oldCfg)

			// Delete state file to allow repeated runs
			deleteStateFile()
		})
	})
})

func runPodsAndCheckResourceNodes(fw *framework.Framework, numaNodes, numPods int, ctnAttrs []tmCtnAttribute) {

	node := getLocalNode(fw)
	checkPerNumaResourceCapacity(node, numaNodes)

	var pods []*v1.Pod

	for podID := 0; podID < numPods; podID++ {
		podName := fmt.Sprintf("gu-pod-%d", podID)
		framework.Logf("creating pod %s attrs %v", podName, ctnAttrs)
		pod := makeTopologyManagerTestPod(podName, numalignCmd, ctnAttrs)
		pod = fw.PodClient().CreateSync(pod)
		framework.Logf("created pod %s", podName)
		pods = append(pods, pod)
	}

	checkPerNumaResourceConsumption(fw, node, numaNodes, pods)

	for podID := 0; podID < numPods; podID++ {
		pod := pods[podID]
		framework.Logf("deleting the pod %s/%s and waiting for container removal",
			pod.Namespace, pod.Name)
		deletePods(fw, []string{pod.Name})
		waitForAllContainerRemoval(pod.Name, pod.Namespace)
	}
}

func checkPerNumaResourceCapacity(node *v1.Node, numaNodes int) {
	for numaNode := 0; numaNode < numaNodes; numaNode++ {
		resCpu := fmt.Sprintf("numa%d/%s", numaNode, string(v1.ResourceCPU))
		cpuCap := node.Status.Capacity[v1.ResourceName(resCpu)]
		framework.Logf("%v -> %v", resCpu, cpuCap)
		gomega.Expect(cpuCap.IsZero()).To(gomega.BeFalse(), "no CPU capacity reported for NUMA node %d", numaNode)
	}
}

func checkPerNumaResourceConsumption(fw *framework.Framework, node *v1.Node, numaNodes int, pods []*v1.Pod) {
	reqs := gatherPerNumaRequirements(fw, numaNodes, pods)

	for numaNode, _ := range reqs {
		resCpu := fmt.Sprintf("numa%d/%s", numaNode, string(v1.ResourceCPU))
		cpuCap := node.Status.Capacity[v1.ResourceName(resCpu)]
		cpuAlloc := node.Status.Allocatable[v1.ResourceName(resCpu)]
		framework.Logf("%v -> %v / %v", resCpu, cpuAlloc, cpuCap)
		framework.ExpectEqual(cpuCap.Cmp(cpuAlloc), 1, "CPU capacity/allocation mismatch for NUMA node %d 0 alloc=%v cap=%v", numaNode, cpuAlloc, cpuCap)
	}
}

func gatherPerNumaRequirements(fw *framework.Framework, numaNodes int, pods []*v1.Pod) map[int]v1.ResourceList {
	reqs := make(map[int]v1.ResourceList)

	for numaNode := 0; numaNode < numaNodes; numaNode++ {
		reqs[numaNode] = make(v1.ResourceList)
	}

	for _, pod := range pods {
		for _, cnt := range pod.Spec.Containers {
			ginkgo.By(fmt.Sprintf("validating the container %s on Gu pod %q", cnt.Name, pod.Name))

			logs, err := e2epod.GetPodLogs(fw.ClientSet, fw.Namespace.Name, pod.Name, cnt.Name)
			framework.ExpectNoError(err, "expected log not found in pod % container %q", pod.Name, cnt.Name)

			framework.Logf("got pod logs: %v", logs)

			podEnv, err := makeEnvMap(logs)
			framework.ExpectNoError(err)

			CPUToNUMANode, err := getCPUToNUMANodeMapFromEnv(fw, pod, &cnt, podEnv, numaNodes)
			framework.ExpectNoError(err)

			framework.Logf("CPUToNUMA map for pod %q container %q", pod.Name, cnt.Name)
			for _, cpuNode := range CPUToNUMANode {
				cpuQty := reqs[cpuNode][v1.ResourceCPU]
				cpuQty.Add(resource.MustParse("1"))
				reqs[cpuNode][v1.ResourceCPU] = cpuQty
			}

		}
	}

	framework.Logf("requests map %v", reqs)
	return reqs
}
