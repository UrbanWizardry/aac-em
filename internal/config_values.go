package emulator

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

//////////////////////
//     Settings     //
//////////////////////

type ConfigSetting struct {
	Key      string                 `json:"key"`
	Versions []ConfigSettingVersion `json:"versions"`
	Locked   bool                   `json:"locked"`
}

func NewConfigSettingNow(key string, value string) *ConfigSetting {
	setting := ConfigSetting{Key: key}
	setting.NewVersion(value)
	return &setting
}

func (cs *ConfigSetting) NewVersion(value string) error {
	// What exactly am I reserving a returned error here for?

	// Actually, *prepend*
	cs.Versions = append(
		[]ConfigSettingVersion{NewConfigSettingVersionNow(value)},
		cs.Versions...,
	)

	return nil
}

func (cs *ConfigSetting) GetLatest() (ConfigSettingVersion, error) {
	if len(cs.Versions) == 0 {
		// Something, somewhere has gone horribly wrong.
		return ConfigSettingVersion{}, fmt.Errorf("no versions available for setting")
	}

	return cs.Versions[0], nil
}

//////////////////////
// Setting Versions //
//////////////////////

type ConfigSettingVersion struct {
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Uuid      string    `json:"uuid"`
}

func NewConfigSettingVersionNow(value string) ConfigSettingVersion {
	return ConfigSettingVersion{
		Value:     value,
		Timestamp: time.Now(),
		Uuid:      uuid.NewString(),
	}
}

///////////////
// Snapshots //
///////////////

type ConfigurationSnapshot struct {
	// Snapshots is a list of single-version copies of this ConfigSetting at the moment the snapshot was
	Settings []ConfigSetting `json:"snapshots"`
}
