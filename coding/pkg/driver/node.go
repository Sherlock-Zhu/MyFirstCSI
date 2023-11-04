package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeStageVolume is called\n###\n###")
	fmt.Println("\n###\n###\n!!!check point 0\n###\n###")
	fmt.Printf("NodeStageVolumeRequest parameters: \n%v", req)
	jsonD, _ := json.Marshal(req)
	fmt.Println(string(jsonD))
	return nil, nil
}

func (d *Driver) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeUnstageVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) NodePublishVolume(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodePublishVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) NodeUnpublishVolume(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeUnpublishVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) NodeGetVolumeStats(context.Context, *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeGetVolumeStats is called\n###\n###")
	return nil, nil
}
func (d *Driver) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeExpandVolume is called\n###\n###")
	return nil, nil
}
func (d *Driver) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeGetCapabilities is called\n###\n###")
	caps := []*csi.NodeServiceCapability{}
	for _, i := range []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	} {
		caps = append(caps, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: i,
				},
			},
		})
	}
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: caps,
	}, nil
}

func (d *Driver) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeGetInfo is called\n###\n###")
	topology := &csi.Topology{
		Segments: map[string]string{"topology.test.csi.azure.com/zone": ""},
	}
	return &csi.NodeGetInfoResponse{
		NodeId:             d.nodeid,
		MaxVolumesPerNode:  4,
		AccessibleTopology: topology,
	}, nil
}
