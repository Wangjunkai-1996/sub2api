package service

import (
	"context"
	"testing"
	"time"

	pkghttputil "github.com/Wei-Shaw/sub2api/internal/pkg/httputil"
	"github.com/stretchr/testify/require"
)

func TestDetachUpstreamContextPreservesHardDeadlineWithDrain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hardBudget time.Duration
		drain      time.Duration
		cancel     bool
		wantErr    error
	}{
		{"connected request", 40 * time.Millisecond, time.Second, false, context.DeadlineExceeded},
		{"deadline bounds disconnected drain", 40 * time.Millisecond, time.Second, true, context.DeadlineExceeded},
		{"drain bounds disconnected request", time.Second, 40 * time.Millisecond, true, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, cancelClient := context.WithTimeout(context.Background(), tc.hardBudget)
			defer cancelClient()
			detached, releaseDetached := detachUpstreamContext(client, tc.drain)
			defer releaseDetached()
			deadline, ok := detached.Deadline()
			require.True(t, ok)
			clientDeadline, _ := client.Deadline()
			require.Equal(t, clientDeadline, deadline)
			attempt, releaseAttempt := pkghttputil.NewUpstreamAttemptContext(detached, time.Second)
			defer releaseAttempt()
			if tc.cancel {
				cancelClient()
				// Client cancellation must still leave time to collect late usage.
				require.NoError(t, attempt.Err())
			}
			select {
			case <-attempt.Done():
				require.ErrorIs(t, attempt.Err(), tc.wantErr)
			case <-time.After(500 * time.Millisecond):
				t.Fatal("upstream attempt exceeded its hard deadline or drain window")
			}
		})
	}
}
