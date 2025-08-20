package emulator

import (

	//"github.com/golang-jwt/jwt/v4"

	"fmt"
	"slices"
	"sync"

	"github.com/ostafen/clover"
	"github.com/pkg/errors"
)

// Design:
//  Config settings are persistently stored in a Clover DB document store
//  Each setting is a named collection
//  Each setting collection contains version documents

const (
	SETTING_COLECTION_NAME  = "settings"
	SNAPSHOT_COLECTION_NAME = "snapshots"
)

type persistentConfigStore struct {
	sync.Mutex
	cdb *clover.DB
}

func NewPersistentConfigStore(
	cloverFactory func() (*clover.DB, func(), error),
) (*persistentConfigStore, func(), error) {
	cdb, closer, err := cloverFactory()
	if err != nil {
		return nil, func() {}, err
	}

	cdb.CreateCollection(SETTING_COLECTION_NAME)
	cdb.CreateCollection(SNAPSHOT_COLECTION_NAME)

	pcs := persistentConfigStore{
		cdb: cdb,
	}

	return &pcs, closer, nil
}

// ensureConfigCollection checks for the existence of a named collection
// and creates it if it does not already exist
// func (pcs *persistentConfigStore) ensureConfigCollection(name string) error {
// 	fmt.Printf("Ensuring collection: %s ", name)
// 	exists, err := pcs.cdb.HasCollection(name)
// 	if err != nil {
// 		return errors.Wrapf(err, "failed to determine if collection '%s' exists", name)
// 	}

// 	if !exists {
// 		err = pcs.cdb.CreateCollection(name)
// 		if err != nil {
// 			return errors.Wrapf(
// 				err,
// 				"collection '%s' does not exist, error attempting to create it",
// 				name,
// 			)
// 		}

// 		return nil
// 	}

// 	// Collection exists, no action taken
// 	return nil
// }

// UpdateValue creates a new version of the setting defined by @param key
// If no setting exists of that key, it will be created.
func (pcs *persistentConfigStore) UpdateSetting(key string, value string) (ConfigSetting, error) {
	pcs.Lock()
	defer pcs.Unlock()

	if !pcs.settingExists(key) {
		// Setting does not exist, create it and exit
		fmt.Printf("Setting does not exist: %s\n", key)
		setting, err := pcs.createSetting(key, value)
		if err != nil {
			return ConfigSetting{}, errors.Wrapf(err, "failed to create setting %s", key)
		}
		return setting, nil
	}

	unlocked, err := pcs.settingUnlocked(key)
	if err != nil {
		return ConfigSetting{}, fmt.Errorf("error getting setting locked state")
	}
	if !unlocked {
		return ConfigSetting{}, fmt.Errorf("setting is locked")
	}

	// Setting exists, update the stored document.
	setting, err := pcs.updateSettingFunc(key, func(s *ConfigSetting) {
		s.NewVersion(value)
	})
	if err != nil {
		// TODO: wrap err
		return ConfigSetting{}, err
	}

	return setting, nil
}

func (pcs *persistentConfigStore) CreateSetting(key string, value string) (ConfigSetting, error) {
	pcs.Lock()
	defer pcs.Unlock()

	return pcs.createSetting(key, value)
}

// createSetting does the work of creating a new setting.
// This non-exported function DOES NOT manage the Mutex.
// Do not call directly outside of this type.
func (pcs *persistentConfigStore) createSetting(key string, value string) (ConfigSetting, error) {
	fmt.Printf("Creating setting: %s\n", key)

	// Make sure a Setting
	setting := NewConfigSettingNow(key, value)
	settingDoc := clover.NewDocumentOf(setting)
	if settingDoc == nil {
		return ConfigSetting{}, fmt.Errorf("failed to convert setting object to storage document for: %s, %s", key, value)
	}

	_, err := pcs.cdb.InsertOne(SETTING_COLECTION_NAME, settingDoc)
	if err != nil {
		return ConfigSetting{}, errors.Wrapf(
			err,
			"failed to insert version object into collection for: %s, %s",
			key, value,
		)
	}

	return *setting, nil
}

func (pcs *persistentConfigStore) settingExists(key string) bool {
	settingDoc, err := pcs.getSettingDoc(key)
	exists := err == nil && settingDoc != nil
	return exists
}

func (pcs *persistentConfigStore) settingUnlocked(key string) (bool, error) {
	setting, err := pcs.getSetting(key)
	if err != nil {
		// TODO: wrap err
		return false, err
	}

	return !setting.Locked, nil
}

// GetSetting is a convenience function for returning the VALUE of the latest version of the setting
func (pcs *persistentConfigStore) GetSetting(key string) (string, error) {
	pcs.Lock()
	defer pcs.Unlock()

	version, err := pcs.getSettingLatestVersion(key)
	if err != nil {
		// TODO: wrap err
		return "", err
	}

	fmt.Printf("GetSetting(%s) = %s", key, version.Value)

	return version.Value, nil
}

// GetSettingLatestVersion returns the ConfigSettingVersion struct of the latest version of the setting
func (pcs *persistentConfigStore) GetSettingLatestVersion(key string) (ConfigSettingVersion, error) {
	pcs.Lock()
	defer pcs.Unlock()
	return pcs.getSettingLatestVersion(key)
}

// getSettingLatestVersion returns the ConfigSettingVersion struct of the latest version of the setting
func (pcs *persistentConfigStore) getSettingLatestVersion(key string) (ConfigSettingVersion, error) {
	setting, err := pcs.getSetting(key)
	if err != nil {
		// TODO: wrap err
		return ConfigSettingVersion{}, err
	}

	slices.SortFunc(setting.Versions, func(a, b ConfigSettingVersion) int {
		if a.Timestamp.Before(b.Timestamp) {
			return -1
		} else {
			return 1
		}
	})

	return setting.Versions[0], nil
}

func (pcs *persistentConfigStore) DeleteSetting(key string) error {
	pcs.Lock()
	defer pcs.Unlock()

	settingDoc, err := pcs.getSettingDoc(key)
	if err != nil {
		// TODO: wrap err
		return err
	}

	pcs.getSettingQuery(key).DeleteById(settingDoc.ObjectId())

	return nil
}

func (pcs *persistentConfigStore) LockSetting(key string) (ConfigSetting, error) {
	pcs.Lock()
	defer pcs.Unlock()
	if !pcs.settingExists(key) {
		return ConfigSetting{}, fmt.Errorf("setting %s does not exist", key)
	}

	setting, err := pcs.updateSettingFunc(key, func(s *ConfigSetting) {
		s.Locked = true
	})
	if err != nil {
		return ConfigSetting{}, errors.Wrapf(err, "failed to lock setting: %s", key)
	}

	return setting, nil
}

func (pcs *persistentConfigStore) UnlockSetting(key string) (ConfigSetting, error) {
	pcs.Lock()
	defer pcs.Unlock()

	if !pcs.settingExists(key) {
		return ConfigSetting{}, fmt.Errorf("setting %s does not exist", key)
	}

	setting, err := pcs.updateSettingFunc(key, func(s *ConfigSetting) {
		s.Locked = false
	})
	if err != nil {
		return ConfigSetting{}, errors.Wrapf(err, "failed to unlock setting: %s", key)
	}

	return setting, nil
}

func (pcs *persistentConfigStore) GetKeys() ([]string, error) {
	pcs.Lock()
	defer pcs.Unlock()

	// TODO: better error handling
	return pcs.cdb.ListCollections()
}

func (pcs *persistentConfigStore) getSettingQuery(key string) *clover.Query {
	return pcs.cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(key))
}

func (pcs *persistentConfigStore) getSettingDoc(key string) (*clover.Document, error) {
	return pcs.getSettingQuery(key).FindFirst()
}

func (pcs *persistentConfigStore) getSetting(key string) (ConfigSetting, error) {
	settingDoc, err := pcs.getSettingDoc(key)
	if err != nil {
		// TODO: wrap err
		return ConfigSetting{}, err
	}

	var setting ConfigSetting
	err = settingDoc.Unmarshal(&setting)
	if err != nil {
		// TODO: wrap err
		return ConfigSetting{}, err
	}

	return setting, nil
}

func (pcs *persistentConfigStore) updateSettingFunc(key string, updateFunc func(*ConfigSetting)) (ConfigSetting, error) {
	oldSettingDoc, err := pcs.getSettingDoc(key)
	if err != nil || oldSettingDoc == nil {
		return ConfigSetting{}, fmt.Errorf("failed to retrieve storage document for: %s", key)
	}

	// Unmarshall into object, then update the object
	var setting ConfigSetting
	err = oldSettingDoc.Unmarshal(&setting)
	if err != nil {
		return ConfigSetting{}, fmt.Errorf("failed to unmarshal storage document for: %s", key)
	}

	// Call the supplied func on the setting
	updateFunc(&setting)

	// Replace the old stored document with a new one
	err = pcs.cdb.Query(SETTING_COLECTION_NAME).DeleteById(oldSettingDoc.ObjectId())
	if err != nil {
		return ConfigSetting{}, errors.Wrapf(
			err,
			"failed to delete storage document before replacing with new version for: %s",
			key,
		)
	}

	newSettingDoc := clover.NewDocumentOf(setting)
	_, err = pcs.cdb.InsertOne(SETTING_COLECTION_NAME, newSettingDoc)
	if err != nil {
		return ConfigSetting{}, errors.Wrapf(err, "failed to replace storage document with new version for: %s", key)
	}

	return setting, nil
}

// CLOVER FACTORY FUNCTIONS FOR TEST COMPOSABILITY

func openCloverDbAt(dbpath string) (*clover.DB, func(), error) {
	cdb, err := clover.Open(dbpath)
	if err != nil {
		// Empty PersistentDirectoryCloser func in return
		// values because it was never correctly opened
		return nil,
			func() {
				// Empty close func because it was never correctly opened
			},
			errors.Wrap(
				err,
				"failed to create new persistent directory",
			)
	}
	closer := func() { cdb.Close() }

	return cdb, closer, nil
}

func MakeCloverFactory(dbpath string) func() (*clover.DB, func(), error) {
	return func() (*clover.DB, func(), error) {
		return openCloverDbAt(dbpath)
	}
}
