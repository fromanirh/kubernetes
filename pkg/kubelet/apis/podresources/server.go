/*
Copyright 2018 The Kubernetes Authors.

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

package podresources

import (
	"context"
	"sort"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

// DevicesProvider knows how to provide the devices used by the given container
type DevicesProvider interface {
	GetDevices(podUID, containerName string) []*podresourcesapi.ContainerDevices
	UpdateAllocatedDevices()
	GetAllDevices() map[string]map[string]pluginapi.Device
}

// CPUsProvider knows how to provide the cpus used by the given container
type CPUsProvider interface {
	GetCPUs(podUID, containerName string) []int64
	GetAllCPUs() []int64
}

type Notifier interface {
	StartContainer(pod *v1.Pod, containerID, containerName string)
	StopContainer(pod *v1.Pod, containerID, containerName string)
}

type fakeNotifier struct{}

func (f fakeNotifier) StartContainer(pod *v1.Pod, containerID, containerName string) {
	klog.V(4).Infof("fakeNotifier - StartContainer %q %q pod %s/%s", containerID, containerName, pod.Namespace, pod.Name)
}

func (f fakeNotifier) StopContainer(pod *v1.Pod, containerID, containerName string) {
	klog.V(4).Infof("fakeNotifier - StopContainer pod %q %q %s/%s", containerID, containerName, pod.Namespace, pod.Name)
}

func NewFakeNotifier() Notifier {
	return fakeNotifier{}
}

// PodsProvider knows how to provide the pods admitted by the node
type PodsProvider interface {
	GetPods() []*v1.Pod
}

type podResInfo struct {
	action podresourcesapi.WatchPodAction
	pod    *v1.Pod
}

type respSinkID int

// podResourcesServer implements PodResourcesListerServer
type podResourcesServer struct {
	podsProvider    PodsProvider
	devicesProvider DevicesProvider
	cpusProvider    CPUsProvider
	infoSource      chan podResInfo
	lock            sync.RWMutex
	sinkID          respSinkID
	respSinks       map[respSinkID]chan *podresourcesapi.WatchPodResourcesResponse
}

// NewPodResourcesServer returns a PodResourcesListerServer which lists pods provided by the PodsProvider
// with device information provided by the DevicesProvider
func NewPodResourcesServer(podsProvider PodsProvider, devicesProvider DevicesProvider, cpusProvider CPUsProvider, enableNotifications bool) (podresourcesapi.PodResourcesListerServer, Notifier) {
	p := &podResourcesServer{
		podsProvider:    podsProvider,
		devicesProvider: devicesProvider,
		cpusProvider:    cpusProvider,
		infoSource:      make(chan podResInfo),
		respSinks:       make(map[respSinkID]chan *podresourcesapi.WatchPodResourcesResponse),
	}

	if !enableNotifications {
		return p, NewFakeNotifier()
	}

	go p.dispatchResponses()
	return p, p
}

func (p *podResourcesServer) makePodResources(pod *v1.Pod) podresourcesapi.PodResources {
	pRes := podresourcesapi.PodResources{
		Name:            pod.Name,
		Namespace:       pod.Namespace,
		Containers:      make([]*podresourcesapi.ContainerResources, len(pod.Spec.Containers)),
		ResourceVersion: pod.ResourceVersion,
	}

	for j, container := range pod.Spec.Containers {
		pRes.Containers[j] = &podresourcesapi.ContainerResources{
			Name:    container.Name,
			Devices: p.devicesProvider.GetDevices(string(pod.UID), container.Name),
			CpuIds:  p.cpusProvider.GetCPUs(string(pod.UID), container.Name),
		}
	}
	return pRes
}

// List returns information about the resources assigned to pods on the node
func (p *podResourcesServer) List(ctx context.Context, req *podresourcesapi.ListPodResourcesRequest) (*podresourcesapi.ListPodResourcesResponse, error) {
	pods := p.podsProvider.GetPods()
	podResources := make([]*podresourcesapi.PodResources, len(pods))
	p.devicesProvider.UpdateAllocatedDevices()

	for i, pod := range pods {
		pRes := p.makePodResources(pod)
		podResources[i] = &pRes
	}

	return &podresourcesapi.ListPodResourcesResponse{
		PodResources: podResources,
	}, nil
}

type numaIDs []int64

func (a numaIDs) Len() int           { return len(a) }
func (a numaIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a numaIDs) Less(i, j int) bool { return a[i] < a[j] }

// GetAllocatableResources returns information about all the devices known by the server
func (p *podResourcesServer) GetAllocatableResources(context.Context, *podresourcesapi.AllocatableResourcesRequest) (*podresourcesapi.AllocatableResourcesResponse, error) {
	allDevices := p.devicesProvider.GetAllDevices()
	var respDevs []*podresourcesapi.ContainerDevices

	resourceNames := []string{}
	for resourceName := range allDevices {
		resourceNames = append(resourceNames, resourceName)
	}
	sort.Strings(resourceNames)

	for _, resourceName := range resourceNames {
		resourceDevs := allDevices[resourceName]

		// group devices by NUMA node
		numaNodes := []int64{}
		NUMADeviceIDs := map[int64][]string{}
		for devID, dev := range resourceDevs {
			for _, node := range dev.GetTopology().GetNodes() {
				numaNode := node.GetID()
				NUMADeviceIDs[numaNode] = append(NUMADeviceIDs[numaNode], devID)
				numaNodes = append(numaNodes, numaNode)
			}
		}
		sort.Sort(numaIDs(numaNodes))

		for _, numaNode := range numaNodes {
			devIDs := NUMADeviceIDs[numaNode]
			respDevs = append(respDevs, &podresourcesapi.ContainerDevices{
				ResourceName: resourceName,
				DeviceIds:    devIDs, // these are all the instances of this resources on numaNode
				Topology: &podresourcesapi.TopologyInfo{
					Nodes: []*podresourcesapi.NUMANode{
						{ID: numaNode},
					},
				},
			})
		}

	}

	return &podresourcesapi.AllocatableResourcesResponse{
		Devices: respDevs,
		CpuIds:  p.cpusProvider.GetAllCPUs(),
	}, nil
}

func (p *podResourcesServer) StartContainer(pod *v1.Pod, containerID, containerName string) {
	klog.V(4).Infof("ResourcesNotifier - StartContainer %q %q pod %s/%s", containerID, containerName, pod.Namespace, pod.Name)
}

func (p *podResourcesServer) StopContainer(pod *v1.Pod, containerID, containerName string) {
	klog.V(4).Infof("ResourcesNotifier - StopContainer %q %q pod %s/%s", containerID, containerName, pod.Namespace, pod.Name)
}

func (p *podResourcesServer) dispatchResponses() {
	for {
		info := <-p.infoSource

		p.lock.RLock()
		p.processInfo(info)
		p.lock.RUnlock()
	}
}

// must run with p.lock held in read mode
func (p *podResourcesServer) processInfo(info podResInfo) {
	if len(p.respSinks) == 0 {
		// why waste cycles? noone is listening anyway
		return
	}

	if info.action != podresourcesapi.WatchPodAction_ADDED || info.action != podresourcesapi.WatchPodAction_DELETED {
		klog.Warningf("unknown action %v for pod %q", info.action, info.pod.UID)
		return
	}

	klog.V(4).Infof("watch - %v event for pod %q", info.action, info.pod.UID)

	// per protocol spec, we need to report the state of the resources allocated to the pod.
	// Hence we just query the providers, and we trust them to do the right thing.
	pRes := p.makePodResources(info.pod)

	for _, ch := range p.respSinks {
		resp := &podresourcesapi.WatchPodResourcesResponse{
			Action:       info.action,
			Uid:          string(info.pod.UID),
			PodResources: &pRes,
		}

		ch <- resp
	}
}

func (p *podResourcesServer) registerWatcher() (respSinkID, chan *podresourcesapi.WatchPodResourcesResponse) {
	p.lock.Lock()
	defer p.lock.Unlock()
	sinkChan := make(chan *podresourcesapi.WatchPodResourcesResponse)
	sinkID := p.sinkID
	p.sinkID++
	p.respSinks[sinkID] = sinkChan
	return sinkID, sinkChan
}

func (p *podResourcesServer) unregisterWatcher(sinkID respSinkID) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// TODO: sink close?
	delete(p.respSinks, sinkID)
}

func (p *podResourcesServer) Watch(req *podresourcesapi.WatchPodResourcesRequest, srv podresourcesapi.PodResourcesLister_WatchServer) error {
	sinkID, sinkChan := p.registerWatcher()
	defer p.unregisterWatcher(sinkID)
	for {
		resp := <-sinkChan
		err := srv.Send(resp)
		if err != nil {
			return err
		}
	}
	return nil
}
