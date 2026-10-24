package service

import (
	"errors"
	"testing"
	"time"
)

func TestIsSQLiteBusyError(t *testing.T) {
	busy := []error{
		errors.New("database is locked (5) (SQLITE_BUSY)"),
		errors.New("SQLITE_LOCKED: database table is locked"),
		errors.New("database table is locked: job_task_item"),
	}
	for _, err := range busy {
		if !isSQLiteBusyError(err) {
			t.Fatalf("expected busy error for %q", err.Error())
		}
	}

	others := []error{
		nil,
		errors.New("no such table: job_task_item"),
		errors.New("value too large for column"),
	}
	for _, err := range others {
		if isSQLiteBusyError(err) {
			t.Fatalf("expected non-busy error for %v", err)
		}
	}
}

func TestPersistJobTaskItemsWithRetry(t *testing.T) {
	oldPersist := persistJobTaskItems
	oldDelays := persistBusyRetryDelays
	defer func() {
		persistJobTaskItems = oldPersist
		persistBusyRetryDelays = oldDelays
	}()

	persistBusyRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

	calls := 0
	persistJobTaskItems = func(items []map[string]interface{}) error {
		calls++
		if calls < 3 {
			return errors.New("database is locked (5) (SQLITE_BUSY)")
		}
		return nil
	}

	if err := persistJobTaskItemsWithRetry([]map[string]interface{}{{"id": 1}}); err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestPersistJobTaskItemsWithRetryGivesUp(t *testing.T) {
	oldPersist := persistJobTaskItems
	oldDelays := persistBusyRetryDelays
	defer func() {
		persistJobTaskItems = oldPersist
		persistBusyRetryDelays = oldDelays
	}()

	persistBusyRetryDelays = []time.Duration{time.Millisecond, time.Millisecond}

	calls := 0
	persistJobTaskItems = func(items []map[string]interface{}) error {
		calls++
		return errors.New("database is locked (5) (SQLITE_BUSY)")
	}

	err := persistJobTaskItemsWithRetry([]map[string]interface{}{{"id": 1}})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if calls != 3 {
		t.Fatalf("expected 1 attempt + 2 retries, got %d", calls)
	}
}

func TestPersistJobTaskItemsWithRetryNoRetryForOtherErrors(t *testing.T) {
	oldPersist := persistJobTaskItems
	oldDelays := persistBusyRetryDelays
	defer func() {
		persistJobTaskItems = oldPersist
		persistBusyRetryDelays = oldDelays
	}()

	persistBusyRetryDelays = []time.Duration{time.Millisecond}

	calls := 0
	persistJobTaskItems = func(items []map[string]interface{}) error {
		calls++
		return errors.New("no such table: job_task_item")
	}

	if err := persistJobTaskItemsWithRetry([]map[string]interface{}{{"id": 1}}); err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected no retry for non-busy error, got %d attempts", calls)
	}
}
