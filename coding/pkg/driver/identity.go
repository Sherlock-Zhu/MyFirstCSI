package driver

import (
	"context"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func (d *Driver) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	fmt.Println("\n###\n###\n!!!GetPluginInfo is called\n###\n###")
	return &csi.GetPluginInfoResponse{
		Name:          d.name,
		VendorVersion: "test_v1",
	}, nil
}
func (d *Driver) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	fmt.Println("\n###\n###\n!!!GetPluginCapabilities is called\n###\n###")
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}
func (d *Driver) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	fmt.Println("\n###\n###\n!!!Probe is called\n###\n###")
	return &csi.ProbeResponse{
		Ready: &wrapperspb.BoolValue{
			Value: d.ready,
		},
	}, nil
}
