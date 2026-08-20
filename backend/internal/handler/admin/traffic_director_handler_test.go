package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type trafficDirectorHandlerServiceStub struct {
	state         *service.TrafficDirectorGroupState
	previewResult *service.TrafficDirectorPreview
	publishResult *service.TrafficDirectorPublishResult
	previewErr    error
	publishErr    error
	getErr        error
	publishInput  service.TrafficDirectorPublishInput
}

func (s *trafficDirectorHandlerServiceStub) Get(context.Context, int64) (*service.TrafficDirectorGroupState, error) {
	return s.state, s.getErr
}

func (s *trafficDirectorHandlerServiceStub) ListVersions(context.Context, int64, int, int) ([]service.TrafficDirectorVersion, int64, error) {
	return nil, 0, nil
}

func (s *trafficDirectorHandlerServiceStub) GetVersion(context.Context, int64, int64) (*service.TrafficDirectorVersion, error) {
	return nil, nil
}

func (s *trafficDirectorHandlerServiceStub) Preview(_ context.Context, input service.TrafficDirectorPreviewInput) (*service.TrafficDirectorPreview, error) {
	if s.previewErr != nil {
		return nil, s.previewErr
	}
	if s.previewResult != nil {
		return s.previewResult, nil
	}
	return &service.TrafficDirectorPreview{GroupID: input.GroupID, ExpectedVersion: input.ExpectedVersion, Mode: input.Mode}, nil
}

func (s *trafficDirectorHandlerServiceStub) Publish(_ context.Context, input service.TrafficDirectorPublishInput) (*service.TrafficDirectorPublishResult, error) {
	s.publishInput = input
	if s.publishErr != nil {
		return nil, s.publishErr
	}
	if s.publishResult != nil {
		return s.publishResult, nil
	}
	return &service.TrafficDirectorPublishResult{}, nil
}

func (s *trafficDirectorHandlerServiceStub) Rollback(context.Context, service.TrafficDirectorRollbackInput) (*service.TrafficDirectorPublishResult, error) {
	return &service.TrafficDirectorPublishResult{}, nil
}

func newTrafficDirectorTestRouter(s *trafficDirectorHandlerServiceStub) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	h := NewTrafficDirectorHandler(s)
	router.POST("/groups/:id/traffic-director/preview", h.Preview)
	router.POST("/groups/:id/traffic-director/publish", h.Publish)
	router.GET("/groups/:id/traffic-director/status", h.Status)
	return router
}

func trafficDirectorTestSpec() *domain.TrafficDirectorSpec {
	return &domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeObserve,
		Pools: []domain.TrafficDirectorPool{{
			Key:        "stable",
			WeightBPS:  domain.TrafficDirectorWeightTotalBPS,
			AccountIDs: []int64{11},
		}},
	}
}

func TestTrafficDirectorHandlerPreviewPassesExpectedVersionAndSpec(t *testing.T) {
	stub := &trafficDirectorHandlerServiceStub{}
	router := newTrafficDirectorTestRouter(stub)
	body := `{"expected_version":4,"mode":"shadow","spec":{"schema_version":1,"health_mode":"observe","pools":[{"key":"stable","weight_bps":10000,"account_ids":[11]}]}}`
	req := httptest.NewRequest(http.MethodPost, "/groups/9/traffic-director/preview", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	var envelope struct {
		Code int `json:"code"`
		Data struct {
			GroupID         int64  `json:"group_id"`
			ExpectedVersion int64  `json:"expected_version"`
			Mode            string `json:"mode"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, 0, envelope.Code)
	require.Equal(t, int64(9), envelope.Data.GroupID)
	require.Equal(t, int64(4), envelope.Data.ExpectedVersion)
	require.Equal(t, "shadow", envelope.Data.Mode)
}

func TestTrafficDirectorHandlerPublishRequiresIdempotencyKey(t *testing.T) {
	stub := &trafficDirectorHandlerServiceStub{}
	router := newTrafficDirectorTestRouter(stub)
	body := `{"expected_version":0,"mode":"legacy"}`
	req := httptest.NewRequest(http.MethodPost, "/groups/9/traffic-director/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusUnprocessableEntity, recorder.Code)
	var envelope struct {
		Reason string `json:"reason"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, service.ErrTrafficDirectorValidation.Reason, envelope.Reason)
	require.Empty(t, stub.publishInput.IdempotencyKey)
}

func TestTrafficDirectorHandlerPublishMapsConflictAndPropagatesActor(t *testing.T) {
	stub := &trafficDirectorHandlerServiceStub{publishErr: service.ErrTrafficDirectorVersionConflict}
	router := gin.New()
	h := NewTrafficDirectorHandler(stub)
	router.POST("/groups/:id/traffic-director/publish", func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 42})
		h.Publish(c)
	})
	body := `{"expected_version":3,"mode":"legacy"}`
	req := httptest.NewRequest(http.MethodPost, "/groups/9/traffic-director/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "rollout-9")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Equal(t, "rollout-9", stub.publishInput.IdempotencyKey)
	require.NotNil(t, stub.publishInput.OperatorID)
	require.Equal(t, int64(42), *stub.publishInput.OperatorID)
}

func TestTrafficDirectorHandlerStatusIncludesPoolAndHealthSummary(t *testing.T) {
	groupID := int64(9)
	stub := &trafficDirectorHandlerServiceStub{state: &service.TrafficDirectorGroupState{
		GroupID:   groupID,
		GroupName: "OpenAI",
		Platform:  service.PlatformOpenAI,
		Head: service.TrafficDirectorHead{
			GroupID: groupID,
			Version: 2,
			Mode:    domain.TrafficDirectorModeEnforced,
			Spec:    trafficDirectorTestSpec(),
		},
		Accounts: []service.TrafficDirectorAccount{{ID: 11, Schedulable: true}, {ID: 12, Schedulable: false}},
	}}
	router := newTrafficDirectorTestRouter(stub)
	req := httptest.NewRequest(http.MethodGet, "/groups/9/traffic-director/status", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	var envelope struct {
		Data struct {
			Mode       string `json:"mode"`
			Version    int64  `json:"version"`
			HealthMode string `json:"health_mode"`
			Checksum   string `json:"checksum"`
			Pools      []struct {
				Key string `json:"key"`
			} `json:"pools"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &envelope))
	require.Equal(t, domain.TrafficDirectorModeEnforced, envelope.Data.Mode)
	require.Equal(t, int64(2), envelope.Data.Version)
	require.Equal(t, domain.TrafficDirectorHealthModeObserve, envelope.Data.HealthMode)
	require.NotEmpty(t, envelope.Data.Checksum)
	require.Equal(t, []string{"stable"}, []string{envelope.Data.Pools[0].Key})
}
