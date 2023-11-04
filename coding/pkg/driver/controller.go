package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	volerr "k8s.io/cloud-provider/volume/errors"
	"k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2022-08-01/compute"
	consts "github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureconstants"
	"github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureutils"
	volumehelper "github.com/Sherlock-Zhu/MyFirstCSI/pkg/util"
	"github.com/container-storage-interface/spec/lib/go/csi"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

func (d *Driver) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!create Volume is called\n###\n###")
	klog.V(2).Infof("\n###\n###\n!!!create Volume is called\n###\n###")

	params := req.GetParameters()
	diskParams, err := azureutils.ParseDiskParameters(params)
	fmt.Println("\n###\n###\n!!!check point 0\n###\n###")
	fmt.Printf("Got disk parametes: \n%v", diskParams)
	jsonD, _ := json.Marshal(diskParams)
	fmt.Println(string(jsonD))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Failed parsing disk parameters: %v", err)
	}
	capacityBytes := req.GetCapacityRange().GetRequiredBytes()
	volSizeBytes := int64(capacityBytes)
	requestGiB := int(volumehelper.RoundUpGiB(volSizeBytes))
	fmt.Printf("Got disk size: \n1: %v, 2: %v, 3: %v", capacityBytes, volSizeBytes, requestGiB)
	diskParams.VolumeContext[consts.RequestedSizeGib] = strconv.Itoa(requestGiB)
	// diskZone := azureutils.PickAvailabilityZone(requirement, diskParams.Location, topologyKey)
	// give fake zone here
	diskZone := ""
	localCloud := d.cloud
	localCloud.Config.Cloud = "PublicCloud"
	localCloud.Config.DisableAzureStackCloud = false
	skuName, err := azureutils.NormalizeStorageAccountType(diskParams.AccountType, localCloud.Config.Cloud, localCloud.Config.DisableAzureStackCloud)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// add neccessary para
	name := req.GetName()
	if diskParams.DiskName == "" {
		diskParams.DiskName = name
	}
	if diskParams.ResourceGroup == "" {
		diskParams.ResourceGroup = d.cloud.ResourceGroup
	}
	if diskParams.Location == "" {
		diskParams.Location = d.cloud.Location
	}

	var sourceID, sourceType string
	volumeOptions := &azure.ManagedDiskOptions{
		AvailabilityZone:    diskZone,
		BurstingEnabled:     diskParams.EnableBursting,
		DiskEncryptionSetID: diskParams.DiskEncryptionSetID,
		DiskEncryptionType:  diskParams.DiskEncryptionType,
		DiskIOPSReadWrite:   diskParams.DiskIOPSReadWrite,
		DiskMBpsReadWrite:   diskParams.DiskMBPSReadWrite,
		DiskName:            diskParams.DiskName,
		LogicalSectorSize:   int32(diskParams.LogicalSectorSize),
		MaxShares:           int32(diskParams.MaxShares),
		ResourceGroup:       diskParams.ResourceGroup,
		SubscriptionID:      diskParams.SubscriptionID,
		SizeGB:              requestGiB,
		StorageAccountType:  skuName,
		SourceResourceID:    sourceID,
		SourceType:          sourceType,
		Tags:                diskParams.Tags,
		Location:            diskParams.Location,
		PerformancePlus:     diskParams.PerformancePlus,
	}
	fmt.Println("\n###\n###\n!!!create Volume start\n###\n###")
	fmt.Printf("check para:\n 1: %+v\n\n#", volumeOptions)
	jsonE, _ := json.Marshal(volumeOptions)
	fmt.Println(string(jsonE))
	var diskURI string
	diskURI, err = localCloud.CreateManagedDisk(ctx, volumeOptions)
	if err != nil {
		fmt.Printf("disk creation failed, errror: %v\n#", err.Error())
		if strings.Contains(err.Error(), consts.NotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	fmt.Printf("disk creation completed, resource id: %v\n#", diskURI)
	contentSource := &csi.VolumeContentSource{}
	accessibleTopology := []*csi.Topology{}
	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:           diskURI,
			CapacityBytes:      volumehelper.GiBToBytes(int64(requestGiB)),
			VolumeContext:      diskParams.VolumeContext,
			ContentSource:      contentSource,
			AccessibleTopology: accessibleTopology,
		},
	}, nil
}
func (d *Driver) DeleteVolume(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!DeleteVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!Publishvolume is called\n###\n###")
	diskURI := req.GetVolumeId()
	fmt.Printf("volumeId: %s", diskURI)
	disk, err := d.checkDiskExists(ctx, diskURI)
	if err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Volume not found, failed with error: %v", err))
	}
	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}
	nodeName := types.NodeName(nodeID)
	diskName, err := azureutils.GetDiskName(diskURI)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	// check point
	fmt.Println("\n###\n###\n!!!check point 1\n###\n###")
	lun, vmState, err := d.cloud.GetDiskLun(diskName, diskURI, nodeName)
	if err == cloudprovider.InstanceNotFound {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("failed to get azure instance id for node %q (%v)", nodeName, err))
	}

	vmStateStr := "<nil>"
	if vmState != nil {
		vmStateStr = *vmState
	}
	// check point
	fmt.Println("\n###\n###\n!!!check point 3\n###\n###")
	fmt.Printf("GetDiskLun returned: %v. Initiating attaching volume %s to node %s (vmState %s).", err, diskURI, nodeName, vmStateStr)
	volumeContext := req.GetVolumeContext()
	if volumeContext == nil {
		volumeContext = map[string]string{}
	}
	if err == nil {
		if vmState != nil && strings.ToLower(*vmState) == "failed" {
			fmt.Printf("VM(%s) is in failed state, update VM first", nodeName)
			if err := d.cloud.UpdateVM(ctx, nodeName); err != nil {
				return nil, status.Errorf(codes.Internal, "update instance %q failed with %v", nodeName, err)
			}
		}
		// Volume is already attached to node.
		fmt.Printf("Attach operation is successful. volume %s is already attached to node %s at lun %d.", diskURI, nodeName, lun)
	} else {
		if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(consts.TooManyRequests)) ||
			strings.Contains(strings.ToLower(err.Error()), consts.ClientThrottled) {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		var cachingMode compute.CachingTypes
		// check point
		fmt.Println("\n###\n###\n!!!check point 4\n###\n###")
		if cachingMode, err = azureutils.GetCachingMode(volumeContext); err != nil {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
		fmt.Printf("Trying to attach volume %s to node %s", diskURI, nodeName)

		// fake asyncattach
		asyncAttach := false
		// asyncAttach := isAsyncAttachEnabled(d.enableAsyncAttach, volumeContext)
		// attachDiskInitialDelay := azureutils.GetAttachDiskInitialDelay(volumeContext)
		// if attachDiskInitialDelay > 0 {
		// 	klog.V(2).Infof("attachDiskInitialDelayInMs is set to %d", attachDiskInitialDelay)
		// 	d.cloud.AttachDetachInitialDelayInMs = attachDiskInitialDelay
		// }
		lun, err = d.cloud.AttachDisk(ctx, asyncAttach, diskName, diskURI, nodeName, cachingMode, disk)
		if err == nil {
			fmt.Printf("Attach operation successful: volume %s attached to node %s.", diskURI, nodeName)
		} else {
			if derr, ok := err.(*volerr.DanglingAttachError); ok {
				if strings.EqualFold(string(nodeName), string(derr.CurrentNode)) {
					err := status.Errorf(codes.Internal, "volume %s is actually attached to current node %s, return error", diskURI, nodeName)
					fmt.Printf("%v", err)
					return nil, err
				}
				fmt.Printf("volume %s is already attached to node %s, try detach first", diskURI, derr.CurrentNode)
				if err = d.cloud.DetachDisk(ctx, diskName, diskURI, derr.CurrentNode); err != nil {
					return nil, status.Errorf(codes.Internal, "Could not detach volume %s from node %s: %v", diskURI, derr.CurrentNode, err)
				}
				fmt.Printf("Trying to attach volume %s to node %s again", diskURI, nodeName)
				lun, err = d.cloud.AttachDisk(ctx, asyncAttach, diskName, diskURI, nodeName, cachingMode, disk)
			}
			if err != nil {
				fmt.Printf("Attach volume %s to instance %s failed with %v", diskURI, nodeName, err)
				return nil, status.Errorf(codes.Internal, "Attach volume %s to instance %s failed with %v", diskURI, nodeName, err)
			}
		}
		fmt.Printf("attach volume %s to node %s successfully", diskURI, nodeName)
	}

	pubContext := map[string]string{consts.LUN: strconv.Itoa(int(lun))}
	// check point
	fmt.Println("\n###\n###\n!!!check point 2\n###\n###")
	fmt.Printf("###pubContext: %+v", pubContext)
	if disk != nil {
		if _, ok := volumeContext[consts.RequestedSizeGib]; !ok {
			fmt.Printf("found static PV(%s), insert disk properties to volumeattachments", diskURI)
			azureutils.InsertDiskProperties(disk, pubContext)
		}
	}
	return &csi.ControllerPublishVolumeResponse{
		PublishContext: pubContext,
	}, nil
}
func (d *Driver) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!ControllerUnpublishVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) ValidateVolumeCapabilities(context.Context, *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	fmt.Println("\n###\n###\n!!!ValidateVolumeCapabilities is called\n###\n###")
	return nil, nil
}
func (d *Driver) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	fmt.Println("\n###\n###\n!!!ListVolumes is called\n###\n###")
	return nil, nil
}
func (d *Driver) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	fmt.Println("\n###\n###\n!!!GetCapacity is called\n###\n###")
	return nil, nil
}
func (d *Driver) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	fmt.Println("\n###\n###\n!!!ControllerGetCapabilities is called\n###\n###")
	caps := []*csi.ControllerServiceCapability{}
	for _, i := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	} {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: i,
				},
			},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}
func (d *Driver) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	fmt.Println("\n###\n###\n!!!CreateSnapshot is called\n###\n###")
	return nil, nil
}
func (d *Driver) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	fmt.Println("\n###\n###\n!!!DeleteSnapshot is called\n###\n###")
	return nil, nil
}
func (d *Driver) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	fmt.Println("\n###\n###\n!!!ListSnapshots is called\n###\n###")
	return nil, nil
}
func (d *Driver) ControllerExpandVolume(context.Context, *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!ControllerExpandVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) ControllerGetVolume(context.Context, *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!ControllerGetVolume is called\n###\n###")
	return nil, nil
}
