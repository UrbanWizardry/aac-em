package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	emulator "urbanwizardry.com/aac-emulator/internal"
)

func main() {
	dbpath := "/home/alex/tmp/localConfigStore" //os.Args[1]

	store, closer, err := emulator.NewPersistentConfigStore(
		emulator.MakeCloverFactory(dbpath),
	)
	if err != nil {
		panic(err)
	}
	defer closer()

	// TEMP
	store.UpdateSetting("blahblah", "blahblah")

	value, err := store.GetSetting("wibble")
	if err != nil {
		panic(err)
	}

	fmt.Printf("wibble is %s", value)

	restServer := emulator.SetupRestServer(store)

	err = runHttpServer(restServer)
	panic(err)

}

// func registerHandlers(api *operations.AzureAppConfigurationAPI) {
// 	api.KeysGetKeysHandler = emulator.KeysGetKeysHandler{}
// 	api.KeyValuesGetKeyValuesStarHandler = emulator.KeyValuesGetKeyValuesStarHandler{}
// 	api.KeyValuesGetKeyValueHandler = emulator.KeyValuesGetKeyValueHandler{}
// 	api.KeyValuesGetKeyValuesHandler = emulator.KeyValuesGetKeyValuesHandler{}
// }

func runHttpServer(restServer *gin.Engine) error {

	handler := http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		restServer.ServeHTTP(w, rq)
	})

	err := http.ListenAndServe(":9876", handler)

	return err
}
