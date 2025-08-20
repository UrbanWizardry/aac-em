package emulator

import (
	"os"
	"testing"

	"github.com/ostafen/clover"
	"github.com/stretchr/testify/require"
)

func TestCreateEmulator(t *testing.T) {

	t.Run("Create emulator success", func(t *testing.T) {
		dbpath := t.TempDir()
		cdb, err := clover.Open(dbpath)

		require.NoError(t, err)
		require.NotNil(t, cdb)

		closer := func() { cdb.Close() }

		cloverFactory := func() (*clover.DB, func(), error) {
			return cdb, closer, nil
		}

		_, storeCloser, err := NewPersistentConfigStore(cloverFactory)
		require.NoError(t, err)

		defer func() {
			storeCloser()
			os.Remove(dbpath)
		}()
	})
}

func makeTestStore(t *testing.T) (*persistentConfigStore, *clover.DB, func(), error) {
	dbpath := t.TempDir()
	cdb, err := clover.Open(dbpath)
	if err != nil {
		return nil, nil, func() {}, err
	}

	cloverFactory := func() (*clover.DB, func(), error) {
		return cdb, func() { cdb.Close() }, nil
	}

	store, storeCloser, err := NewPersistentConfigStore(cloverFactory)
	if err != nil {
		return nil, nil, func() {}, err
	}

	return store,
		cdb,
		func() {
			storeCloser()
			os.Remove(dbpath)
		},
		nil
}

func TestCreateSettings(t *testing.T) {

	t.Run("Create simple setting", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey := "testsetting1"
		testValue := "testvalue_1_1"
		err = store.CreateSetting(testKey, testValue)
		require.NoError(t, err)

		// Make sure the setting document exists in the clover DB
		doc, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc)

		var setting ConfigSetting
		err = doc.Unmarshal(&setting)
		require.NoError(t, err)

		latestVersion, err := setting.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue, latestVersion.Value)
	})

	t.Run("Create two settings", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey1 := "testsetting1"
		testValue1 := "testvalue_1_1"
		err = store.CreateSetting(testKey1, testValue1)
		require.NoError(t, err)

		testKey2 := "testsetting2"
		testValue2 := "testvalue_2_1"
		err = store.CreateSetting(testKey2, testValue2)
		require.NoError(t, err)

		// Make sure the setting documents exist in the clover DB
		doc1, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey1)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc1)

		doc2, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey2)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc2)

		// Make sure both values are correct
		var setting1 ConfigSetting
		err = doc1.Unmarshal(&setting1)
		require.NoError(t, err)

		latestVersion1, err := setting1.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue1, latestVersion1.Value)

		var setting2 ConfigSetting
		err = doc2.Unmarshal(&setting2)
		require.NoError(t, err)

		latestVersion2, err := setting2.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue2, latestVersion2.Value)

	})
}

func TestCreateSettingsUsingUpdate(t *testing.T) {

	t.Run("Create simple setting", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey := "testsetting1"
		testValue := "testvalue_1_1"
		err = store.UpdateSetting(testKey, testValue)
		require.NoError(t, err)

		// Make sure the setting document exists in the clover DB
		doc, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc)

		var setting ConfigSetting
		err = doc.Unmarshal(&setting)
		require.NoError(t, err)

		latestVersion, err := setting.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue, latestVersion.Value)
	})

	t.Run("Create two settings", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey1 := "testsetting1"
		testValue1 := "testvalue_1_1"
		err = store.UpdateSetting(testKey1, testValue1)
		require.NoError(t, err)

		testKey2 := "testsetting2"
		testValue2 := "testvalue_2_1"
		err = store.UpdateSetting(testKey2, testValue2)
		require.NoError(t, err)

		// Make sure the setting documents exist in the clover DB
		doc1, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey1)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc1)

		doc2, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey2)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc2)

		// Make sure both values are correct
		var setting1 ConfigSetting
		err = doc1.Unmarshal(&setting1)
		require.NoError(t, err)

		latestVersion1, err := setting1.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue1, latestVersion1.Value)

		var setting2 ConfigSetting
		err = doc2.Unmarshal(&setting2)
		require.NoError(t, err)

		latestVersion2, err := setting2.GetLatest()
		require.NoError(t, err)
		require.Equal(t, testValue2, latestVersion2.Value)

	})
}

func TestNewVersions(t *testing.T) {
	t.Run("Create and Create New Version", func(t *testing.T) {
		store, _, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey := "testsetting1"
		testValue1 := "testvalue_1_1"
		testValue2 := "testvalue_1_2"

		err = store.UpdateSetting(testKey, testValue1)
		require.NoError(t, err)

		err = store.UpdateSetting(testKey, testValue2)
		require.NoError(t, err)

		// Get the actual setting object, check the versions
		setting, err := store.getSetting(testKey)
		require.NoError(t, err)
		require.Len(t, setting.Versions, 2)
		require.Equal(t, testValue2, setting.Versions[0].Value)
		require.Equal(t, testValue1, setting.Versions[1].Value)
		//require.Equal(t, testValue2, setting.GetLatest())

	})
}

func TestDeleteSettings(t *testing.T) {
	t.Run("Create and Delete setting", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		testKey1 := "testsetting1"
		err = store.CreateSetting(testKey1, "testvalue_1_1")
		require.NoError(t, err)

		err = store.DeleteSetting(testKey1)
		require.NoError(t, err)

		// Make sure the setting document DOES NOT EXIST in the clover DB
		doc, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey1)).FindFirst()
		require.NoError(t, err)
		require.Nil(t, doc)
	})

	t.Run("Create two settings and Delete one", func(t *testing.T) {
		store, cdb, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		// CREATE
		testKey1 := "testsetting1"
		err = store.CreateSetting(testKey1, "testvalue_1_1")
		require.NoError(t, err)

		testKey2 := "testsetting2"
		err = store.CreateSetting(testKey2, "testvalue_2_1")
		require.NoError(t, err)

		// DELETE
		err = store.DeleteSetting(testKey1)
		require.NoError(t, err)

		// Make sure the first setting document DOES NOT EXIST in the clover DB
		doc1, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey1)).FindFirst()
		require.NoError(t, err)
		require.Nil(t, doc1)

		// Make sure the second setting document DOES EXIST in the clover DB
		doc2, err := cdb.Query(SETTING_COLECTION_NAME).Where(clover.Field("Key").Eq(testKey2)).FindFirst()
		require.NoError(t, err)
		require.NotNil(t, doc2)
	})
}

func TestLockSettings(t *testing.T) {
	t.Run("Lock and Unlock Setting", func(t *testing.T) {
		store, _, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		// CREATE
		testKey1 := "testsetting1"
		err = store.CreateSetting(testKey1, "testvalue_1_1")
		require.NoError(t, err)

		// CHECK UNLOCKED
		setting, err := store.getSetting(testKey1)
		require.NoError(t, err)
		require.False(t, setting.Locked)

		// LOCK
		err = store.LockSetting(testKey1)
		require.NoError(t, err)

		// CHECK LOCKED
		setting, err = store.getSetting(testKey1)
		require.NoError(t, err)
		require.True(t, setting.Locked)

		// UNLOCK
		err = store.UnlockSetting(testKey1)
		require.NoError(t, err)

		// CHECK UNLOCKED
		setting, err = store.getSetting(testKey1)
		require.NoError(t, err)
		require.False(t, setting.Locked)

	})

	t.Run("Attempt setting update while locked", func(t *testing.T) {
		store, _, closer, err := makeTestStore(t)
		require.NoError(t, err)
		defer closer()

		// CREATE
		testKey1 := "testsetting1"
		_, err = store.CreateSetting(testKey1, "testvalue_1_1")
		require.NoError(t, err)

		// LOCK
		err = store.LockSetting(testKey1)
		require.NoError(t, err)

		// ATTEMPT UPDATE
		testValue2 := "testvalue_1_2"
		_, err = store.UpdateSetting(testKey1, testValue2)
		require.Error(t, err)

	})
}
