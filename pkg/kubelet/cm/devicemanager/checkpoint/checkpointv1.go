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

package checkpoint

import (
	"encoding/json"
	"hash/fnv"
	"strings"

	"github.com/davecgh/go-spew/spew"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
)

type PodDevicesEntryV1 struct {
	PodUID        string
	ContainerName string
	ResourceName  string
	DeviceIDs     []string
	AllocResp     []byte
}

type checkpointDataV1 struct {
	PodDeviceEntries  []PodDevicesEntryV1
	RegisteredDevices map[string][]string
}

// credits to https://github.com/kubernetes/kubernetes/pull/102717/commits/353f93895118d2ffa2d59a29a1fbc225160ea1d6
func (cp checkpointDataV1) checksum() checksum.Checksum {
	printer := spew.ConfigState{
		Indent:         " ",
		SortKeys:       true,
		DisableMethods: true,
		SpewKeys:       true,
	}

	object := printer.Sprintf("%#v", cp)
	object = strings.Replace(object, "checkpointDataV1", "checkpointData", 1)
	object = strings.Replace(object, "PodDevicesEntryV1", "PodDevicesEntry", -1)
	hash := fnv.New32a()
	printer.Fprintf(hash, "%v", object)
	return checksum.Checksum(hash.Sum32())
}

type DataV1 struct {
	Data     checkpointDataV1
	Checksum checksum.Checksum
}

func NewV1(devEntries []PodDevicesEntryV1,
	devices map[string][]string) DeviceManagerCheckpoint {
	return &DataV1{
		Data: checkpointDataV1{
			PodDeviceEntries:  devEntries,
			RegisteredDevices: devices,
		},
	}
}

// MarshalCheckpoint is needed to implement the Checkpoint interface, but should not be called anymore
func (cp *DataV1) MarshalCheckpoint() ([]byte, error) {
	klog.InfoS("Marshalling a device manager V1 checkpoint")
	cp.Checksum = cp.Data.checksum()
	return json.Marshal(*cp)
}

func (cp *DataV1) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

func (cp *DataV1) VerifyChecksum() error {
	if cp.Checksum != cp.Data.checksum() {
		return errors.ErrCorruptCheckpoint
	}
	return nil
}

func (cp *DataV1) GetData() ([]PodDevicesEntry, map[string][]string) {
	var podDevs []PodDevicesEntry
	for _, entryV1 := range cp.Data.PodDeviceEntries {
		devsPerNuma := NewDevicesPerNUMA()
		// no NUMA cell affinity was recorded. The only possible choice
		// is to set all the devices affine to node 0.
		devsPerNuma[0] = entryV1.DeviceIDs
		podDevs = append(podDevs, PodDevicesEntry{
			PodUID:        entryV1.PodUID,
			ContainerName: entryV1.ContainerName,
			ResourceName:  entryV1.ResourceName,
			DeviceIDs:     devsPerNuma,
			AllocResp:     entryV1.AllocResp,
		})
	}
	return podDevs, cp.Data.RegisteredDevices
}
