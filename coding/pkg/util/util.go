package util

import (
	"fmt"
	"strings"
)

const (
	GiB                  = 1024 * 1024 * 1024
	TagsDelimiter        = ","
	TagKeyValueDelimiter = "="
)

func RoundUpGiB(volumeSizeBytes int64) int64 {
	return RoundUpSize(volumeSizeBytes, GiB)
}

func RoundUpSize(volumeSizeBytes int64, allocationUnitBytes int64) int64 {
	roundedUp := volumeSizeBytes / allocationUnitBytes
	if volumeSizeBytes%allocationUnitBytes > 0 {
		roundedUp++
	}
	return roundedUp
}

func ConvertTagsToMap(tags string) (map[string]string, error) {
	m := make(map[string]string)
	if tags == "" {
		return m, nil
	}
	s := strings.Split(tags, TagsDelimiter)
	for _, tag := range s {
		kv := strings.Split(tag, TagKeyValueDelimiter)
		if len(kv) != 2 {
			return nil, fmt.Errorf("Tags '%s' are invalid, the format should like: 'key1=value1,key2=value2'", tags)
		}
		key := strings.TrimSpace(kv[0])
		if key == "" {
			return nil, fmt.Errorf("Tags '%s' are invalid, the format should like: 'key1=value1,key2=value2'", tags)
		}
		value := strings.TrimSpace(kv[1])
		m[key] = value
	}

	return m, nil
}

func GiBToBytes(volumeSizeGiB int64) int64 {
	return volumeSizeGiB * GiB
}
