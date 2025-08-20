package emulator

import (
	"context"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"

	ogen "urbanwizardry.com/aac-emulator/gen/appconfig"
)

var (
	noddyEtag        = "1234567"
	emptyString      = ""
	addressableFalse = false
	latestLabel      = "latest"
)

func SetupRestServer(configStore persistentConfigStore) *gin.Engine {
	restServer := NewRestServer(configStore)
	restEngine := gin.Default()
	restServer.RegisterToGin(&restEngine.RouterGroup)
	return restEngine
}

type appConfigRestServer struct {
	configStore persistentConfigStore
}

// CreateSnapshot implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CreateSnapshot(ctx context.Context, request ogen.CreateSnapshotRequestObject) (ogen.CreateSnapshotResponseObject, error) {
	panic("unimplemented")
}

// DeleteKeyValue implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) DeleteKeyValue(ctx context.Context, request ogen.DeleteKeyValueRequestObject) (ogen.DeleteKeyValueResponseObject, error) {
	if request.Params.Label != nil {
		if *request.Params.Label != "" {
			return nil, fmt.Errorf("label filtering for DeleteKeyValue is unimplemented")
		}
	}

	err := rs.configStore.DeleteSetting(request.Key)
	if err != nil {
		// TODO: Wrap err
		return nil, err
	}

	return ogen.DeleteKeyValue200JSONResponse{}, nil
}

// DeleteLock implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) DeleteLock(ctx context.Context, request ogen.DeleteLockRequestObject) (ogen.DeleteLockResponseObject, error) {
	setting, err := rs.configStore.UnlockSetting(request.Key)
	if err != nil {
		return ogen.DeleteLock200JSONResponse{}, errors.Wrapf(err, "failed to lock setting %s", request.Key)
	}

	body, err := settingToKeyValue(setting)
	if err != nil {
		return ogen.DeleteLock200JSONResponse{}, errors.Wrapf(err, "failed to lock setting %s", request.Key)
	}

	return ogen.DeleteLock200JSONResponse{
		Headers: ogen.DeleteLock200ResponseHeaders{},
		Body:    body,
	}, nil
}

// PutKeyValue implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) PutKeyValue(ctx context.Context, request ogen.PutKeyValueRequestObject) (ogen.PutKeyValueResponseObject, error) {
	key := request.Key
	value := request.JSONBody.Value
	if value == nil {
		return ogen.PutKeyValue200JSONResponse{}, fmt.Errorf("nil value for PutKeyValue")
	}

	// lastModified := time.Now()
	// tags := map[string]string{}

	setting, err := rs.configStore.UpdateSetting(key, *value)
	if err != nil {
		return ogen.PutKeyValue200JSONResponse{}, errors.Wrapf(err, "failed to add new value to setting: %s", key)
	}

	body, err := settingToKeyValue(setting)
	if err != nil {
		return ogen.PutKeyValue200JSONResponse{}, errors.Wrapf(err, "failed to marshal response body")
	}

	return ogen.PutKeyValue200JSONResponse{
		Headers: ogen.PutKeyValue200ResponseHeaders{},
		Body:    body,
	}, nil
}

// PutLock implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) PutLock(ctx context.Context, request ogen.PutLockRequestObject) (ogen.PutLockResponseObject, error) {
	setting, err := rs.configStore.LockSetting(request.Key)
	if err != nil {
		return ogen.PutLock200JSONResponse{}, errors.Wrapf(err, "failed to lock setting %s", request.Key)
	}

	body, err := settingToKeyValue(setting)
	if err != nil {
		return ogen.PutLock200JSONResponse{}, errors.Wrapf(err, "failed to lock setting %s", request.Key)
	}

	return ogen.PutLock200JSONResponse{
		Headers: ogen.PutLock200ResponseHeaders{},
		Body:    body,
	}, nil
}

// UpdateSnapshot implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) UpdateSnapshot(ctx context.Context, request ogen.UpdateSnapshotRequestObject) (ogen.UpdateSnapshotResponseObject, error) {
	panic("unimplemented")
}

// CheckKeyValue implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckKeyValue(ctx context.Context, request ogen.CheckKeyValueRequestObject) (ogen.CheckKeyValueResponseObject, error) {
	panic("unimplemented")
}

// CheckKeyValues implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckKeyValues(ctx context.Context, request ogen.CheckKeyValuesRequestObject) (ogen.CheckKeyValuesResponseObject, error) {
	panic("unimplemented")
}

// CheckKeys implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckKeys(ctx context.Context, request ogen.CheckKeysRequestObject) (ogen.CheckKeysResponseObject, error) {
	panic("unimplemented")
}

// CheckLabels implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckLabels(ctx context.Context, request ogen.CheckLabelsRequestObject) (ogen.CheckLabelsResponseObject, error) {
	panic("unimplemented")
}

// CheckRevisions implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckRevisions(ctx context.Context, request ogen.CheckRevisionsRequestObject) (ogen.CheckRevisionsResponseObject, error) {
	panic("unimplemented")
}

// CheckSnapshot implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckSnapshot(ctx context.Context, request ogen.CheckSnapshotRequestObject) (ogen.CheckSnapshotResponseObject, error) {
	panic("unimplemented")
}

// CheckSnapshots implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) CheckSnapshots(ctx context.Context, request ogen.CheckSnapshotsRequestObject) (ogen.CheckSnapshotsResponseObject, error) {
	panic("unimplemented")
}

// GetKeyValue implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetKeyValue(ctx context.Context, request ogen.GetKeyValueRequestObject) (ogen.GetKeyValueResponseObject, error) {

	lastModified := time.Now()
	tags := map[string]string{}

	value, err := rs.configStore.GetSetting(request.Key)
	if err != nil {
		// TODO: Wrap err
		return nil, err
	}

	response := ogen.KeyValue{
		Key:          &request.Key,
		Value:        &value,
		Etag:         &noddyEtag,
		Label:        &latestLabel,
		LastModified: &lastModified,
		ContentType:  &emptyString,
		Locked:       &addressableFalse,
		Tags:         &tags,
	}

	resp := ogen.GetKeyValue200JSONResponse{
		Headers: ogen.GetKeyValue200ResponseHeaders{},
		Body:    response,
	}

	return resp, nil
}

// GetKeyValues implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetKeyValues(ctx context.Context, request ogen.GetKeyValuesRequestObject) (ogen.GetKeyValuesResponseObject, error) {
	etag := "1234567"
	label := "latest"
	lastModified := time.Now()
	tags := map[string]string{}
	nextLink := ""

	var filter Filter
	var err error

	// Real filter or null filter?
	if request.Params.Key != nil && *request.Params.Key != "" {
		filter, err = newFilter(*request.Params.Key)
		if err != nil {
			// TODO: Wrap err
			return nil, err
		}
	} else {
		filter = nullFilter{}
	}

	keys, err := rs.configStore.GetKeys()
	if err != nil {
		// TODO: Wrap err
		return nil, err
	}

	values := []ogen.KeyValue{}

	for _, key := range keys {
		if filter.Apply(key) {
			value, err := rs.configStore.GetSetting(key)
			if err != nil {
				// TODO: Wrap err
				return nil, err
			}

			kv := ogen.KeyValue{
				Key:          &key,
				Value:        &value,
				Etag:         &etag,
				Label:        &label,
				LastModified: &lastModified,
				ContentType:  &emptyString,
				Locked:       &addressableFalse,
				Tags:         &tags,
			}

			values = append(values, kv)
		}
	}

	resp := ogen.GetKeyValues200JSONResponse{
		Headers: ogen.GetKeyValues200ResponseHeaders{
			ETag:      etag,
			SyncToken: "jtqGc1I4=MDoyOA==;sn=1",
		},
		Body: ogen.KeyValueListResult{
			Items:    &values,
			NextLink: &nextLink,
			Etag:     &etag,
		},
	}

	return resp, nil
}

// GetKeys implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetKeys(ctx context.Context, request ogen.GetKeysRequestObject) (ogen.GetKeysResponseObject, error) {
	panic("unimplemented")
}

// GetLabels implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetLabels(ctx context.Context, request ogen.GetLabelsRequestObject) (ogen.GetLabelsResponseObject, error) {
	panic("unimplemented")
}

// GetOperationDetails implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetOperationDetails(ctx context.Context, request ogen.GetOperationDetailsRequestObject) (ogen.GetOperationDetailsResponseObject, error) {
	panic("unimplemented")
}

// GetRevisions implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetRevisions(ctx context.Context, request ogen.GetRevisionsRequestObject) (ogen.GetRevisionsResponseObject, error) {
	panic("unimplemented")
}

// GetSnapshot implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetSnapshot(ctx context.Context, request ogen.GetSnapshotRequestObject) (ogen.GetSnapshotResponseObject, error) {
	panic("unimplemented")
}

// GetSnapshots implements appconfig.StrictServerInterface.
func (rs *appConfigRestServer) GetSnapshots(ctx context.Context, request ogen.GetSnapshotsRequestObject) (ogen.GetSnapshotsResponseObject, error) {
	panic("unimplemented")
}

type AppConfigRestServer interface {
	RegisterToGin(g *gin.RouterGroup)
}

func NewRestServer(configStore persistentConfigStore) AppConfigRestServer {
	return &appConfigRestServer{configStore: configStore}
}

func (rs *appConfigRestServer) RegisterToGin(g *gin.RouterGroup) {
	// RegisterHandlersWithOptions() normally wants a gin.Engine, but that's a crappy API
	// design because it doesn't allow rooting the API under anything but "/".
	//
	// Recent attempts to fix this in oapi-codegen have failed and there are outstanding
	// issues open. e.g.:
	//  - https://github.com/deepmap/oapi-codegen/issues/485
	//  - https://github.com/deepmap/oapi-codegen/pull/530
	//  - https://github.com/deepmap/oapi-codegen/commit/9dc8b8d293a991614ea12447bd6507bfadf38304
	//
	// We override the templating to generate better binding code.

	ogen.RegisterHandlersWithOptions(
		g,
		ogen.NewStrictHandler(rs, []ogen.StrictMiddlewareFunc{}),
		ogen.GinServerOptions{
			// The RouterGroup passed in specifies our BaseURL
			BaseURL: "",
			// ErrorHandler is only invoked for errors encountered before processing the request
			ErrorHandler: func(c *gin.Context, err error, i int) {
				c.String(i, "Unexpected error: %s", err.Error())
			},
		},
	)
}

func settingToKeyValue(setting ConfigSetting) (ogen.KeyValue, error) {

}
