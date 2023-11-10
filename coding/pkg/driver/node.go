package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (d *Driver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeStageVolume is called\n###\n###")
	fmt.Println("\n###\n###\n!!!check point 0\n###\n###")
	fmt.Printf("NodeStageVolumeRequest parameters: \n%v", req)
	jsonD, _ := json.Marshal(req)
	fmt.Println(string(jsonD))
	// If the access type is block, do nothing for stage
	switch req.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		return &csi.NodeStageVolumeResponse{}, nil
	}

	LUN := ""
	if val, ok := req.PublishContext["LUN"]; !ok {
		return nil, status.Error(codes.InvalidArgument, "cannot get LUN information ")
	} else {
		LUN = val
	}

	mnt := req.VolumeCapability.GetMount()
	fsType := "ext4"
	if mnt.FsType != "" {
		fsType = mnt.FsType
	}

	source := fmt.Sprintf("/dev/disk/azure/scsi1/lun%s", LUN)
	target := req.StagingTargetPath

	// format disk
	err := DiskFormat(source, fsType)
	if err != nil {
		fmt.Printf("unable to Format fs with error: %s\n", err.Error())
		return nil, status.Error(codes.Internal, fmt.Sprintf("unable to Format fs with error: %s\n", err.Error()))
	}

	// mount disk
	option := []string{"-t", fsType, "-o", "defaults"}
	err = DiskMount(source, target, option)
	if err != nil {
		fmt.Printf("unable to mount disk to target path with error: %s\n", err.Error())
		return nil, status.Error(codes.Internal, fmt.Sprintf("unable to mount disk to target path with error: %s\n", err.Error()))
	}

	fmt.Println("\n###\n###\n!!!NodeStageVolume request completed successfully\n###\n###")
	return &csi.NodeStageVolumeResponse{}, nil
}

func DiskFormat(disk string, fsType string) error {
	//check if disk is already formatted
	CheckCmd := "blkid"
	CheckArgs := []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", disk}
	fmt.Printf("start checking if disk is formatted with command blkid and arg[-p -s TYPE -s PTTYPE -o export %s]", disk)
	out, err := exec.Command(CheckCmd, CheckArgs...).CombinedOutput()
	if err == nil {
		// #bug: didn't check if format type is same with volume required one here
		fmt.Printf("disk already formatted. Blkid command output: \n%s", out)
		return nil
	} else if strings.Contains(err.Error(), "exit status 2") {
		fmt.Println("disk didn't format yet")
	} else {
		return fmt.Errorf("error: %s, format disk with error\n", err.Error())
	}

	//start format disk
	FormatCmd := "mkfs"
	FormatArgs := []string{"-t", fsType, "-F", "-m0", disk}
	fmt.Printf("startformatting disk with command mkfs and arg[-t %s -F -m0 %s]\n", fsType, disk)
	out, err = exec.Command(FormatCmd, FormatArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("format disk failed. mkfs output: %s, and err: %s\n", out, err.Error())
	} else {
		fmt.Printf("format completed, output as below: \n%s", out)
	}

	// double confirm disk formatted successfully
	fmt.Printf("start checking if disk is formatted with command blkid and arg[-p -s TYPE -s PTTYPE -o export %s]", disk)
	out, err = exec.Command(CheckCmd, CheckArgs...).CombinedOutput()
	if err == nil {
		fmt.Printf("disk finally formatted. Blkid command output: \n%s", out)
		return nil
	} else if strings.Contains(err.Error(), "exit status 2") {
		fmt.Println("disk cannot be formatted")
		return fmt.Errorf("disk cannot be formatted and err: %s\n", err.Error())
	} else {
		return fmt.Errorf("error: %s, format disk with error\n", err.Error())
	}
}

func DiskMount(disk string, path string, option []string) error {
	//create target folder
	MountCmd := "mount"
	MountArgs := append(option, disk, path)
	fmt.Printf("start making dir %s\n", path)
	err := os.MkdirAll(path, 0777)
	if err != nil {
		return fmt.Errorf("error: %s, creating the target dir\n", err.Error())
	}
	// check if target dir created
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		fmt.Println("target dir doesn't exists")
		return fmt.Errorf("error: %s, creating the target dir\n", err.Error())
	} else if err == nil {
		fmt.Println("target dir exists, continue")
	} else {
		fmt.Println("error occurred during target dir checking:", err)
		return fmt.Errorf("error: %s, creating the target dir\n", err.Error())
	}

	// mount disk
	fmt.Printf("start mounting disk with command mount and arg[%s]\n", strings.Join(MountArgs, " "))
	out, err := exec.Command(MountCmd, MountArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("mount disk error. mount command output: %s, and error: %s\n", out, err.Error())
	} else {
		fmt.Printf("mount successfully: %s\n", out)
	}
	return nil
}

func (d *Driver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeUnstageVolume is called\n###\n###")
	fmt.Printf("NodeUnstageVolume parameters: \n%v", req)
	jsonD, _ := json.Marshal(req)
	fmt.Println(string(jsonD))
	fmt.Println("\n###\n###\n!!!NodeUnstageVolume request check done\n###\n###")

	UmVolumeId := req.GetVolumeId()
	Umpath := req.StagingTargetPath
	err := DiskUMount(UmVolumeId, Umpath)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error %s, mounting the volume from staging dir to target dir", err.Error()))
	}

	err = DiskRmPath(UmVolumeId, Umpath)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error %s, mounting the volume from staging dir to target dir", err.Error()))
	}

	fmt.Println("\n###\n###\n!!!NodeUnstageVolume request completed successfully\n###\n###")

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (d *Driver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodePublishVolume is called\n###\n###")
	fmt.Printf("NodePublishVolume parameters: \n%v", req)
	jsonD, _ := json.Marshal(req)
	fmt.Println(string(jsonD))
	fmt.Println("\n###\n###\n!!!NodePublishVolume request check done\n###\n###")

	// in case of block mode, the source is going to be the device dir where volume was attached form ControllerPubVolume RPC

	fsType := "ext4"
	if req.VolumeCapability.GetMount().FsType != "" {
		fsType = req.VolumeCapability.GetMount().FsType
	}
	options := []string{"-t", fsType, "-o"}
	if req.Readonly {
		options = append(options, "bind,ro")
	} else {
		options = append(options, "bind")
	}

	source := req.StagingTargetPath
	target := req.TargetPath
	// we want to run mount -t fstype source target -o bind,ro

	err := DiskMount(source, target, options)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error %s, mounting the volume from staging dir to target dir", err.Error()))
	}

	fmt.Println("\n###\n###\n!!!NodePublishVolume request completed successfully\n###\n###")
	return &csi.NodePublishVolumeResponse{}, nil
}

func (d *Driver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	fmt.Println("\n###\n###\n!!!NodeUnpublishVolume is called\n###\n###")
	fmt.Printf("NodeUnpublishVolume parameters: \n%v", req)
	jsonD, _ := json.Marshal(req)
	fmt.Println(string(jsonD))
	fmt.Println("\n###\n###\n!!!NodeUnpublishVolume request check done\n###\n###")
	UmVolumeId := req.GetVolumeId()
	Umpath := req.TargetPath
	err := DiskUMount(UmVolumeId, Umpath)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error %s, mounting the volume from staging dir to target dir", err.Error()))
	}

	err = DiskRmPath(UmVolumeId, Umpath)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error %s, mounting the volume from staging dir to target dir", err.Error()))
	}

	fmt.Println("\n###\n###\n!!!NodeUnpublishVolume request completed successfully\n###\n###")
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func DiskUMount(volume string, path string) error {
	//create target folder
	UMountCmd := "umount"
	fmt.Printf("umounting volume %s in path %s\n", volume, path)
	out, err := exec.Command(UMountCmd, path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("umount disk error. mount command output: %s, and error: %s\n", out, err.Error())
	} else {
		fmt.Printf("umount successfully: %s\n", out)
	}
	return nil
}

func DiskRmPath(volume string, path string) error {
	//create target folder
	RmCmd := "rm"
	RmArgs := []string{"-rf", path}
	if len(path) == 0 {
		fmt.Println("given path is empty")
		return fmt.Errorf("given path is empty\n")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		fmt.Printf("the target path %s doesn't exists\n", path)
		return fmt.Errorf("the target path %s doesn't exists\n", path)
	}

	fmt.Printf("remove volume %s path %s\n", volume, path)
	out, err := exec.Command(RmCmd, RmArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove mount path error, rm command output: %s, and error: %s\n", out, err.Error())
	} else {
		fmt.Printf("remove mount path successfully: %s\n", out)
	}
	return nil
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
