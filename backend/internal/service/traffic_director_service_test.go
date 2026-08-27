package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorServicePublishCanonicalizesBeforeTransactionalValidation(t *testing.T) {
	repository := &trafficDirectorServiceRepositoryStub{}
	service := NewTrafficDirectorService(repository)
	first := TrafficDirectorPublishInput{
		TrafficDirectorPreviewInput: TrafficDirectorPreviewInput{
			GroupID:         71,
			ExpectedVersion: 3,
			Mode:            " enforced ",
			Spec: &domain.TrafficDirectorSpec{
				SchemaVersion: domain.TrafficDirectorSchemaVersion,
				HealthMode:    domain.TrafficDirectorHealthModeOff,
				Pools: []domain.TrafficDirectorPool{
					{Key: "canary", WeightBPS: 1000, AccountIDs: []int64{3}, MinAvailable: 1},
					{Key: "stable", WeightBPS: 9000, AccountIDs: []int64{2, 1}, MinAvailable: 1},
				},
			},
		},
		IdempotencyKey: " publish-71 ",
		Note:           " first rollout ",
	}
	second := first
	second.Spec = &domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeOff,
		Pools: []domain.TrafficDirectorPool{
			{Key: "stable", WeightBPS: 9000, AccountIDs: []int64{1, 2}, MinAvailable: 1},
			{Key: "canary", WeightBPS: 1000, AccountIDs: []int64{3}, MinAvailable: 1},
		},
	}

	_, err := service.Publish(context.Background(), first)
	require.NoError(t, err)
	_, err = service.Publish(context.Background(), second)
	require.NoError(t, err)
	require.Zero(t, repository.groupStateCalls, "publish must not pre-read mutable group state before idempotent replay")
	require.Len(t, repository.commands, 2)
	require.Equal(t, repository.commands[0].RequestFingerprint, repository.commands[1].RequestFingerprint)
	require.Equal(t, "publish-71", repository.commands[0].IdempotencyKey)
	require.Equal(t, "first rollout", repository.commands[0].Note)
	require.Equal(t, []string{"canary", "stable"}, []string{
		repository.commands[0].Spec.Pools[0].Key,
		repository.commands[0].Spec.Pools[1].Key,
	})
}

type trafficDirectorServiceRepositoryStub struct {
	groupStateCalls int
	commands        []TrafficDirectorPublishCommand
}

func (r *trafficDirectorServiceRepositoryStub) GetTrafficDirectorGroupState(
	context.Context,
	int64,
) (*TrafficDirectorGroupState, error) {
	r.groupStateCalls++
	return nil, ErrTrafficDirectorGroupNotFound
}

func (r *trafficDirectorServiceRepositoryStub) GetTrafficDirectorHead(
	context.Context,
	int64,
) (*TrafficDirectorHead, error) {
	return nil, ErrTrafficDirectorGroupNotFound
}

func (r *trafficDirectorServiceRepositoryStub) ListTrafficDirectorVersions(
	context.Context,
	int64,
	int,
	int,
) ([]TrafficDirectorVersionSummary, int64, error) {
	return nil, 0, nil
}

func (r *trafficDirectorServiceRepositoryStub) GetTrafficDirectorVersion(
	context.Context,
	int64,
	int64,
) (*TrafficDirectorVersion, error) {
	return nil, ErrTrafficDirectorVersionNotFound
}

func (r *trafficDirectorServiceRepositoryStub) PublishTrafficDirector(
	_ context.Context,
	command TrafficDirectorPublishCommand,
) (*TrafficDirectorPublishResult, error) {
	r.commands = append(r.commands, command)
	return &TrafficDirectorPublishResult{}, nil
}
