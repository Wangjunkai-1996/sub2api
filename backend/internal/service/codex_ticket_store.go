package service

import (
	"context"
	"encoding/json"
	"time"
)

const (
	CodexTicketV2Prefix      = "codex_turn_ticket:v2:"
	CodexTicketV2ConfigKey   = CodexTicketV2Prefix + "config"
	CodexTicketV2WatchdogKey = CodexTicketV2Prefix + "watchdog"
)

func CodexTicketV2Key(model string) string  { return CodexTicketV2Prefix + model }
func CodexTicketV2LeaseKey(_ string) string { return CodexTicketV2Prefix + "lease" }

// CodexTicketExtraValue distinguishes an absent key from an explicit JSON null.
// Exists=false deletes a key when used in an update.
type CodexTicketExtraValue struct {
	Exists bool
	Value  any
}

// CodexTicketLease is issued by the store. Owner and expiry are never supplied
// to Acquire by callers; the database checks owner, fence and its own clock.
type CodexTicketLease struct {
	AccountID int64
	Model     string
	Owner     string
	Fence     int64
	ExpiresAt time.Time
}

// CodexTicketStore keeps runtime credentials out of scheduler/outbox updates.
// A false CAS or nil acquired lease is contention, not a persistence error.
type CodexTicketStore interface {
	CompareAndSwapCodexTicket(context.Context, *Account, map[string]CodexTicketExtraValue, map[string]CodexTicketExtraValue, *CodexTicketLease) (bool, error)
	AcquireCodexTicketLease(context.Context, *Account, string, map[string]CodexTicketExtraValue, time.Duration) (*CodexTicketLease, error)
	RenewCodexTicketLease(context.Context, *Account, *CodexTicketLease, time.Duration) (bool, error)
	ReleaseCodexTicketLease(context.Context, *CodexTicketLease) (bool, error)
}

// CodexTicketExtraSnapshot freezes JSON values so later edits to account.Extra
// cannot silently change the compare-and-swap expectation. Repository checks
// reject non-JSON values instead of falling back to an unconditional write.
func CodexTicketExtraSnapshot(account *Account, keys ...string) map[string]CodexTicketExtraValue {
	out := make(map[string]CodexTicketExtraValue, len(keys))
	for _, key := range keys {
		var value any
		var exists bool
		if account != nil {
			value, exists = account.Extra[key]
		}
		if exists {
			if raw, err := json.Marshal(value); err == nil {
				value = json.RawMessage(raw)
			}
		}
		out[key] = CodexTicketExtraValue{Exists: exists, Value: value}
	}
	return out
}
