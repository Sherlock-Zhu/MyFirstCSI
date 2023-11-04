package driver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2022-08-01/compute"
	consts "github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureconstants"
	"github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureutils"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	DefaultName = "runlong.csi.test"
)

var (
	driverVersion = "N/A"
)

type Driver struct {
	name     string
	region   string
	endpoint string
	nodeid   string

	srv   *grpc.Server
	ready bool

	// for azure disk
	cloud                      *azure.Cloud
	cloudConfigSecretName      string
	cloudConfigSecretNamespace string
	allowEmptyCloudConfig      bool
	enableTrafficManager       bool
	trafficManagerPort         int64
	kubeconfig                 string
	userAgentSuffix            string
	customUserAgent            string
	Name                       string
	getDiskThrottlingCache     azcache.Resource
}

type InputParams struct {
	Name     string
	Nodeid   string
	Endpoint string
	Region   string
}

func NewDriver(params InputParams) *Driver {
	cache, err := azcache.NewTimedCache(5*time.Minute, func(key string) (interface{}, error) {
		return nil, nil
	}, false)
	if err != nil {
		klog.Fatalf("%v", err)
	}
	return &Driver{
		name:                   params.Name,
		endpoint:               params.Endpoint,
		region:                 params.Region,
		nodeid:                 params.Nodeid,
		getDiskThrottlingCache: cache,
	}
}

// start gRPC server

func (d *Driver) Run() error {
	url, err := url.Parse(d.endpoint)
	if err != nil {
		return fmt.Errorf("parsing the endpont %s\n#", err.Error())
	}

	if url.Scheme != "unix" {
		return fmt.Errorf("only support unix scheme, %s is provided\n#", url.Scheme)
	}
	grpcAddress := path.Join(url.Host, filepath.FromSlash(url.Path))
	if url.Host == "" {
		grpcAddress = filepath.FromSlash(url.Path)
	}

	if err := os.Remove(grpcAddress); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket file: %s\n#", err.Error())
	}

	listener, err := net.Listen(url.Scheme, grpcAddress)
	if err != nil {
		return fmt.Errorf("start listener failed with error: %s\n#", err.Error())
	}
	fmt.Println(listener)

	// try to initiate cloud
	kubeconfig := ""
	d.kubeconfig = kubeconfig
	d.cloudConfigSecretName = "runlong-provider"
	d.cloudConfigSecretNamespace = "kube-system"
	d.allowEmptyCloudConfig = true
	d.enableTrafficManager = false
	d.trafficManagerPort = 7788
	d.Name = d.name
	d.customUserAgent = ""
	d.userAgentSuffix = ""
	userAgent := GetUserAgent(d.Name, d.customUserAgent, d.userAgentSuffix)
	klog.V(2).Infof("driver userAgent: %s", userAgent)
	cloud, err := azureutils.GetCloudProvider(context.Background(), kubeconfig, d.cloudConfigSecretName, d.cloudConfigSecretNamespace,
		userAgent, d.allowEmptyCloudConfig, d.enableTrafficManager, d.trafficManagerPort)
	if err != nil {
		klog.Fatalf("failed to get Azure Cloud Provider, error: %v", err)
	}
	d.cloud = cloud
	d.srv = grpc.NewServer()

	csi.RegisterNodeServer(d.srv, d)
	csi.RegisterControllerServer(d.srv, d)
	csi.RegisterIdentityServer(d.srv, d)
	// listener.Serve()
	d.ready = true
	d.srv.Serve(listener)
	return nil
}

// additional fuction of azure disk
func GetUserAgent(driverName, customUserAgent, userAgentSuffix string) string {
	customUserAgent = strings.TrimSpace(customUserAgent)
	userAgent := customUserAgent
	if customUserAgent == "" {
		userAgent = fmt.Sprintf("%s/%s", driverName, driverVersion)
	}

	userAgentSuffix = strings.TrimSpace(userAgentSuffix)
	if userAgentSuffix != "" {
		userAgent = userAgent + " " + userAgentSuffix
	}
	return userAgent
}

func (d *Driver) checkDiskExists(ctx context.Context, diskURI string) (*compute.Disk, error) {
	diskName, err := azureutils.GetDiskName(diskURI)
	if err != nil {
		return nil, err
	}

	resourceGroup, err := azureutils.GetResourceGroupFromURI(diskURI)
	if err != nil {
		return nil, err
	}

	if d.isGetDiskThrottled() {
		klog.Warningf("skip checkDiskExists(%s) since it's still in throttling", diskURI)
		return nil, nil
	}
	subsID := azureutils.GetSubscriptionIDFromURI(diskURI)
	disk, rerr := d.cloud.DisksClient.Get(ctx, subsID, resourceGroup, diskName)
	if rerr != nil {
		if rerr.IsThrottled() || strings.Contains(rerr.RawError.Error(), consts.RateLimited) {
			klog.Warningf("checkDiskExists(%s) is throttled with error: %v", diskURI, rerr.Error())
			d.getDiskThrottlingCache.Set(consts.ThrottlingKey, "")
			return nil, nil
		}
		return nil, rerr.Error()
	}

	return &disk, nil
}

func (d *Driver) isGetDiskThrottled() bool {
	cache, err := d.getDiskThrottlingCache.Get(consts.ThrottlingKey, azcache.CacheReadTypeDefault)
	if err != nil {
		klog.Warningf("getDiskThrottlingCache(%s) return with error: %s", consts.ThrottlingKey, err)
		return false
	}
	return cache != nil
}
