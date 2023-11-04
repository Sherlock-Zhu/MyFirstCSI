package optimization

import (
	"fmt"
	"strings"

	consts "github.com/Sherlock-Zhu/MyFirstCSI/pkg/azureconstants"
)

func IsValidPerfProfile(profile string) bool {
	return isPerfTuningEnabled(profile) || strings.EqualFold(profile, consts.PerfProfileNone)
}

func isPerfTuningEnabled(profile string) bool {
	switch strings.ToLower(profile) {
	case consts.PerfProfileBasic:
		return true
	case consts.PerfProfileAdvanced:
		return true
	default:
		return false
	}
}

func GetDeviceSettingFromAttribute(key string) (deviceSetting string, err error) {
	if !strings.HasPrefix(key, consts.DeviceSettingsKeyPrefix) {
		return key, fmt.Errorf("GetDeviceSettingFromAttribute: %s is not a valid device setting override", key)
	}
	return strings.TrimPrefix(key, consts.DeviceSettingsKeyPrefix), nil
}
