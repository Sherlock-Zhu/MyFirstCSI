package azureutils

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/compute/mgmt/compute"
	api "k8s.io/kubernetes/pkg/apis/core"

	// "github.com/container-storage-interface/spec/lib/go/csi"
	consts "github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureconstants"
	"github.com/Sherlock-Zhu/MyFirstCSI/pkg/optimization"
	"github.com/Sherlock-Zhu/MyFirstCSI/pkg/util"
	azclients "sigs.k8s.io/cloud-provider-azure/pkg/azureclients"
	azure "sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

const (
	azurePublicCloud                          = "AZUREPUBLICCLOUD"
	azureStackCloud                           = "AZURESTACKCLOUD"
	azurePublicCloudDefaultStorageAccountType = compute.StandardSSDLRS
	azureStackCloudDefaultStorageAccountType  = compute.StandardLRS
	defaultAzureDataDiskCachingMode           = v1.AzureDataDiskCachingReadOnly
)

var (
	supportedCachingModes = sets.NewString(
		string(api.AzureDataDiskCachingNone),
		string(api.AzureDataDiskCachingReadOnly),
		string(api.AzureDataDiskCachingReadWrite),
	)
)

func ParseDiskParameters(parameters map[string]string) (ManagedDiskParameters, error) {
	var err error
	if parameters == nil {
		parameters = make(map[string]string)
	}

	diskParams := ManagedDiskParameters{
		DeviceSettings: make(map[string]string),
		Tags:           make(map[string]string),
		VolumeContext:  parameters,
	}
	for k, v := range parameters {
		switch strings.ToLower(k) {
		case consts.SkuNameField:
			diskParams.AccountType = v
		case consts.LocationField:
			diskParams.Location = v
		case consts.StorageAccountTypeField:
			diskParams.AccountType = v
		case consts.CachingModeField:
			diskParams.CachingMode = v1.AzureDataDiskCachingMode(v)
		case consts.SubscriptionIDField:
			diskParams.SubscriptionID = v
		case consts.ResourceGroupField:
			diskParams.ResourceGroup = v
		case consts.DiskIOPSReadWriteField:
			diskParams.DiskIOPSReadWrite = v
		case consts.DiskMBPSReadWriteField:
			diskParams.DiskMBPSReadWrite = v
		case consts.LogicalSectorSizeField:
			diskParams.LogicalSectorSize, err = strconv.Atoi(v)
			if err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
		case consts.DiskNameField:
			diskParams.DiskName = v
		case consts.DesIDField:
			diskParams.DiskEncryptionSetID = v
		case consts.DiskEncryptionTypeField:
			diskParams.DiskEncryptionType = v
		case consts.TagsField:
			customTagsMap, err := util.ConvertTagsToMap(v)
			if err != nil {
				return diskParams, err
			}
			for k, v := range customTagsMap {
				diskParams.Tags[k] = v
			}
		case azure.WriteAcceleratorEnabled:
			diskParams.WriteAcceleratorEnabled = v
		case consts.MaxSharesField:
			diskParams.MaxShares, err = strconv.Atoi(v)
			if err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
			if diskParams.MaxShares < 1 {
				return diskParams, fmt.Errorf("parse %s returned with invalid value: %d", v, diskParams.MaxShares)
			}
		case consts.PvcNameKey:
			diskParams.Tags[consts.PvcNameTag] = v
		case consts.PvcNamespaceKey:
			diskParams.Tags[consts.PvcNamespaceTag] = v
		case consts.PvNameKey:
			diskParams.Tags[consts.PvNameTag] = v
		case consts.FsTypeField:
			diskParams.FsType = strings.ToLower(v)
		case consts.KindField:
			// fix csi migration issue: https://github.com/kubernetes/kubernetes/issues/103433
			diskParams.VolumeContext[consts.KindField] = string(v1.AzureManagedDisk)
		case consts.PerfProfileField:
			if !optimization.IsValidPerfProfile(v) {
				return diskParams, fmt.Errorf("perf profile %s is not supported, supported tuning modes are none and basic", v)
			}
			diskParams.PerfProfile = v
		case consts.NetworkAccessPolicyField:
			diskParams.NetworkAccessPolicy = v
		case consts.PublicNetworkAccessField:
			diskParams.PublicNetworkAccess = v
		case consts.DiskAccessIDField:
			diskParams.DiskAccessID = v
		case consts.EnableBurstingField:
			if strings.EqualFold(v, consts.TrueValue) {
				diskParams.EnableBursting = pointer.Bool(true)
			}
		case consts.UserAgentField:
			diskParams.UserAgent = v
		case consts.EnableAsyncAttachField:
			diskParams.VolumeContext[consts.EnableAsyncAttachField] = v
		case consts.ZonedField:
			// no op, only for backward compatibility with in-tree driver
		case consts.PerformancePlusField:
			value, err := strconv.ParseBool(v)
			if err != nil {
				return diskParams, fmt.Errorf("invalid %s: %s in storage class", consts.PerformancePlusField, v)
			}
			diskParams.PerformancePlus = &value
		case consts.AttachDiskInitialDelayField:
			if _, err = strconv.Atoi(v); err != nil {
				return diskParams, fmt.Errorf("parse %s failed with error: %v", v, err)
			}
		default:
			// accept all device settings params
			// device settings need to start with azureconstants.DeviceSettingsKeyPrefix
			if deviceSettings, err := optimization.GetDeviceSettingFromAttribute(k); err == nil {
				diskParams.DeviceSettings[filepath.Join(consts.DummyBlockDevicePathLinux, deviceSettings)] = v
			} else {
				return diskParams, fmt.Errorf("invalid parameter %s in storage class", k)
			}
		}
	}

	if strings.EqualFold(diskParams.AccountType, string(compute.PremiumV2LRS)) {
		if diskParams.CachingMode != "" && !strings.EqualFold(string(diskParams.CachingMode), string(v1.AzureDataDiskCachingNone)) {
			return diskParams, fmt.Errorf("cachingMode %s is not supported for %s", diskParams.CachingMode, compute.PremiumV2LRS)
		}
	}

	return diskParams, nil
}

type ManagedDiskParameters struct {
	AccountType             string
	CachingMode             v1.AzureDataDiskCachingMode
	DeviceSettings          map[string]string
	DiskAccessID            string
	DiskEncryptionSetID     string
	DiskEncryptionType      string
	DiskIOPSReadWrite       string
	DiskMBPSReadWrite       string
	DiskName                string
	EnableAsyncAttach       *bool
	EnableBursting          *bool
	PerformancePlus         *bool
	FsType                  string
	Location                string
	LogicalSectorSize       int
	MaxShares               int
	NetworkAccessPolicy     string
	PublicNetworkAccess     string
	PerfProfile             string
	SubscriptionID          string
	ResourceGroup           string
	Tags                    map[string]string
	UserAgent               string
	VolumeContext           map[string]string
	WriteAcceleratorEnabled string
	Zoned                   string
}

// func PickAvailabilityZone(requirement *csi.TopologyRequirement, region, topologyKey string) string {
// 	if requirement == nil {
// 		return ""
// 	}
// 	for _, topology := range requirement.GetPreferred() {
// 		if zone, exists := topology.GetSegments()[consts.WellKnownTopologyKey]; exists {
// 			if IsValidAvailabilityZone(zone, region) {
// 				return zone
// 			}
// 		}
// 		if zone, exists := topology.GetSegments()[topologyKey]; exists {
// 			if IsValidAvailabilityZone(zone, region) {
// 				return zone
// 			}
// 		}
// 	}
// 	for _, topology := range requirement.GetRequisite() {
// 		if zone, exists := topology.GetSegments()[consts.WellKnownTopologyKey]; exists {
// 			if IsValidAvailabilityZone(zone, region) {
// 				return zone
// 			}
// 		}
// 		if zone, exists := topology.GetSegments()[topologyKey]; exists {
// 			if IsValidAvailabilityZone(zone, region) {
// 				return zone
// 			}
// 		}
// 	}
// 	return ""
// }

// func IsValidAvailabilityZone(zone, region string) bool {
// 	if region == "" {
// 		index := strings.Index(zone, "-")
// 		return index > 0 && index < len(zone)-1
// 	}
// 	return strings.HasPrefix(zone, fmt.Sprintf("%s-", region))
// }

func NormalizeStorageAccountType(storageAccountType, cloud string, disableAzureStackCloud bool) (compute.DiskStorageAccountTypes, error) {
	if storageAccountType == "" {
		if IsAzureStackCloud(cloud, disableAzureStackCloud) {
			return azureStackCloudDefaultStorageAccountType, nil
		}
		return azurePublicCloudDefaultStorageAccountType, nil
	}

	sku := compute.DiskStorageAccountTypes(storageAccountType)
	supportedSkuNames := compute.PossibleDiskStorageAccountTypesValues()
	if IsAzureStackCloud(cloud, disableAzureStackCloud) {
		supportedSkuNames = []compute.DiskStorageAccountTypes{compute.StandardLRS, compute.PremiumLRS}
	}
	for _, s := range supportedSkuNames {
		if sku == s {
			return sku, nil
		}
	}

	return "", fmt.Errorf("azureDisk - %s is not supported sku/storageaccounttype. Supported values are %s", storageAccountType, supportedSkuNames)
}

func IsAzureStackCloud(cloud string, disableAzureStackCloud bool) bool {
	return !disableAzureStackCloud && strings.EqualFold(cloud, azureStackCloud)
}

func GetCloudProvider(ctx context.Context, kubeConfig, secretName, secretNamespace, userAgent string,
	allowEmptyCloudConfig, enableTrafficMgr bool, trafficMgrPort int64) (*azure.Cloud, error) {
	kubeClient, err := GetKubeClient(kubeConfig)
	if err != nil {
		klog.Warningf("get kubeconfig(%s) failed with error: %v", kubeConfig, err)
		if !os.IsNotExist(err) && !errors.Is(err, rest.ErrNotInCluster) {
			return nil, fmt.Errorf("failed to get KubeClient: %v", err)
		}
	}
	return GetCloudProviderFromClient(ctx, kubeClient, secretName, secretNamespace, userAgent,
		allowEmptyCloudConfig, enableTrafficMgr, trafficMgrPort)
}

func GetCloudProviderFromClient(ctx context.Context, kubeClient *clientset.Clientset, secretName, secretNamespace, userAgent string,
	allowEmptyCloudConfig bool, enableTrafficMgr bool, trafficMgrPort int64) (*azure.Cloud, error) {
	var config *azure.Config
	var fromSecret bool
	var err error
	az := &azure.Cloud{
		InitSecretConfig: azure.InitSecretConfig{
			SecretName:      secretName,
			SecretNamespace: secretNamespace,
			CloudConfigKey:  "cloud-config",
		},
	}
	if kubeClient != nil {
		klog.V(2).Infof("reading cloud config from secret %s/%s", az.SecretNamespace, az.SecretName)
		az.KubeClient = kubeClient
		config, err = az.GetConfigFromSecret()
		if err == nil && config != nil {
			fromSecret = true
		}
		if err != nil {
			klog.V(2).Infof("InitializeCloudFromSecret: failed to get cloud config from secret %s/%s: %v", az.SecretNamespace, az.SecretName, err)
		}
	}

	if config == nil {
		klog.V(2).Infof("could not read cloud config from secret %s/%s", az.SecretNamespace, az.SecretName)
		credFile, ok := os.LookupEnv(consts.DefaultAzureCredentialFileEnv)
		if ok && strings.TrimSpace(credFile) != "" {
			klog.V(2).Infof("%s env var set as %v", consts.DefaultAzureCredentialFileEnv, credFile)
		} else {
			credFile = consts.DefaultCredFilePathLinux
			klog.V(2).Infof("use default %s env var: %v", consts.DefaultAzureCredentialFileEnv, credFile)
		}

		credFileConfig, err := os.Open(credFile)
		if err != nil {
			klog.Warningf("load azure config from file(%s) failed with %v", credFile, err)
		} else {
			defer credFileConfig.Close()
			klog.V(2).Infof("read cloud config from file: %s successfully", credFile)
			if config, err = azure.ParseConfig(credFileConfig); err != nil {
				klog.Warningf("parse config file(%s) failed with error: %v", credFile, err)
			}
		}
	}

	if config == nil {
		if allowEmptyCloudConfig {
			klog.V(2).Infof("no cloud config provided, error: %v, driver will run without cloud config", err)
		} else {
			return nil, fmt.Errorf("no cloud config provided, error: %v", err)
		}
	} else {
		// Location may be either upper case with spaces (e.g. "East US") or lower case without spaces (e.g. "eastus")
		// Kubernetes does not allow whitespaces in label values, e.g. for topology keys
		// ensure Kubernetes compatible format for Location by enforcing lowercase-no-space format
		config.Location = strings.ToLower(strings.ReplaceAll(config.Location, " ", ""))

		// disable disk related rate limit
		config.DiskRateLimit = &azclients.RateLimitConfig{
			CloudProviderRateLimit: false,
		}
		config.SnapshotRateLimit = &azclients.RateLimitConfig{
			CloudProviderRateLimit: false,
		}
		config.UserAgent = userAgent
		if enableTrafficMgr && trafficMgrPort > 0 {
			trafficMgrAddr := fmt.Sprintf("http://localhost:%d/", trafficMgrPort)
			klog.V(2).Infof("set ResourceManagerEndpoint as %s", trafficMgrAddr)
			config.ResourceManagerEndpoint = trafficMgrAddr
		}
		if err = az.InitializeCloudFromConfig(ctx, config, fromSecret, false); err != nil {
			klog.Warningf("InitializeCloudFromConfig failed with error: %v", err)
		}
	}

	// reassign kubeClient
	if kubeClient != nil && az.KubeClient == nil {
		az.KubeClient = kubeClient
	}
	return az, nil
}

func GetKubeClient(kubeconfig string) (*clientset.Clientset, error) {
	config, err := GetKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}

	return clientset.NewForConfig(config)
}

func GetKubeConfig(kubeconfig string) (config *rest.Config, err error) {
	if kubeconfig != "" {
		if config, err = clientcmd.BuildConfigFromFlags("", kubeconfig); err != nil {
			return nil, err
		}
	} else {
		if config, err = rest.InClusterConfig(); err != nil {
			return nil, err
		}
	}
	return config, err
}

func GetDiskName(diskURI string) (string, error) {
	matches := consts.ManagedDiskPathRE.FindStringSubmatch(diskURI)
	if len(matches) != 2 {
		return "", fmt.Errorf("could not get disk name from %s, correct format: %s", diskURI, consts.ManagedDiskPathRE)
	}
	return matches[1], nil
}

func GetResourceGroupFromURI(diskURI string) (string, error) {
	fields := strings.Split(diskURI, "/")
	if len(fields) != 9 || strings.ToLower(fields[3]) != "resourcegroups" {
		return "", fmt.Errorf("invalid disk URI: %s", diskURI)
	}
	return fields[4], nil
}

func GetSubscriptionIDFromURI(diskURI string) string {
	parts := strings.Split(diskURI, "/")
	for i, v := range parts {
		if strings.EqualFold(v, "subscriptions") && (i+1) < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func GetCachingMode(attributes map[string]string) (compute.CachingTypes, error) {
	var (
		cachingMode v1.AzureDataDiskCachingMode
		err         error
	)

	for k, v := range attributes {
		if strings.EqualFold(k, consts.CachingModeField) {
			cachingMode = v1.AzureDataDiskCachingMode(v)
			break
		}
	}

	cachingMode, err = NormalizeCachingMode(cachingMode)
	return compute.CachingTypes(cachingMode), err
}

func NormalizeCachingMode(cachingMode v1.AzureDataDiskCachingMode) (v1.AzureDataDiskCachingMode, error) {
	if cachingMode == "" {
		return defaultAzureDataDiskCachingMode, nil
	}

	if !supportedCachingModes.Has(string(cachingMode)) {
		return "", fmt.Errorf("azureDisk - %s is not supported cachingmode. Supported values are %s", cachingMode, supportedCachingModes.List())
	}

	return cachingMode, nil
}

// InsertDiskProperties: insert disk properties to map
func InsertDiskProperties(disk *compute.Disk, publishConext map[string]string) {
	if disk == nil || publishConext == nil {
		return
	}

	if disk.Sku != nil {
		publishConext[consts.SkuNameField] = string(disk.Sku.Name)
	}
	prop := disk.DiskProperties
	if prop != nil {
		publishConext[consts.NetworkAccessPolicyField] = string(prop.NetworkAccessPolicy)
		if prop.DiskIOPSReadWrite != nil {
			publishConext[consts.DiskIOPSReadWriteField] = strconv.Itoa(int(*prop.DiskIOPSReadWrite))
		}
		if prop.DiskMBpsReadWrite != nil {
			publishConext[consts.DiskMBPSReadWriteField] = strconv.Itoa(int(*prop.DiskMBpsReadWrite))
		}
		if prop.CreationData != nil && prop.CreationData.LogicalSectorSize != nil {
			publishConext[consts.LogicalSectorSizeField] = strconv.Itoa(int(*prop.CreationData.LogicalSectorSize))
		}
		if prop.Encryption != nil &&
			prop.Encryption.DiskEncryptionSetID != nil {
			publishConext[consts.DesIDField] = *prop.Encryption.DiskEncryptionSetID
		}
		if prop.MaxShares != nil {
			publishConext[consts.MaxSharesField] = strconv.Itoa(int(*prop.MaxShares))
		}
	}
}
