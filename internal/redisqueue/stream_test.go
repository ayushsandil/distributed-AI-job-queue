package redisqueue

import (
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestFlattenMessagesExtractsJobIDs(t *testing.T) {
	messages, err := flattenMessages([]redis.XStream{{
		Stream: "jobs",
		Messages: []redis.XMessage{
			{ID: "1-0", Values: map[string]any{"job_id": "job-1"}},
			{ID: "2-0", Values: map[string]any{"job_id": []byte("job-2")}},
		},
	}})
	if err != nil {
		t.Fatalf("flattenMessages returned error: %v", err)
	}
	if len(messages) != 2 || messages[0].JobID != "job-1" || messages[1].JobID != "job-2" {
		t.Fatalf("unexpected messages: %+v", messages)
	}
}

func TestFlattenMessagesRejectsMissingJobID(t *testing.T) {
	_, err := flattenMessages([]redis.XStream{{
		Stream:   "jobs",
		Messages: []redis.XMessage{{ID: "1-0", Values: map[string]any{"type": "noop"}}},
	}})
	if err == nil || !strings.Contains(err.Error(), ErrInvalidMessage.Error()) {
		t.Fatalf("expected invalid message error, got %v", err)
	}
}
