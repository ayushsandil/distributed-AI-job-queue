package redisqueue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"ai-job-queue/internal/domain"
	"ai-job-queue/internal/ids"
)

func TestStreamIntegrationPublishReadAckAndAutoClaim(t *testing.T) {
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL is not set")
	}

	streamName := "ai-job-queue:test:" + ids.NewUUID()
	groupName := "group-" + ids.NewUUID()
	stream, err := Open(context.Background(), redisURL, streamName, groupName)
	if err != nil {
		t.Fatalf("open redis stream: %v", err)
	}
	defer stream.Close()

	job := domain.Job{
		ID:           "00000000-0000-4000-8000-000000000001",
		Type:         "noop",
		Payload:      []byte(`{}`),
		Priority:     1,
		ExecuteAfter: time.Now().UTC(),
	}
	if err := stream.Publish(context.Background(), job); err != nil {
		t.Fatalf("publish: %v", err)
	}

	messages, err := stream.Read(context.Background(), "consumer-1", 1, time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(messages) != 1 || messages[0].JobID != job.ID {
		t.Fatalf("unexpected messages: %+v", messages)
	}

	claimed, _, err := stream.AutoClaim(context.Background(), "consumer-2", 0, "0-0", 1)
	if err != nil {
		t.Fatalf("auto-claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].JobID != job.ID {
		t.Fatalf("expected pending message to be auto-claimed: %+v", claimed)
	}

	if err := stream.Ack(context.Background(), claimed[0].ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if err := stream.PublishDeadLetter(context.Background(), domain.Job{
		ID:        job.ID,
		Type:      job.Type,
		LastError: stringPtr(fmt.Sprintf("%s failed", job.Type)),
	}); err != nil {
		t.Fatalf("publish dead letter: %v", err)
	}
}

func stringPtr(value string) *string {
	return &value
}
