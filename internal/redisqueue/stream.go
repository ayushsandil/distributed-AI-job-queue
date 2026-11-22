package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"ai-job-queue/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	DefaultStream          = "ai-job-queue:jobs"
	DefaultGroup           = "ai-job-queue-workers"
	DeadLetterStreamSuffix = ":dead-letter"
)

var ErrInvalidMessage = errors.New("invalid redis stream job message")

type Message struct {
	ID    string
	JobID string
}

type Stream struct {
	client *redis.Client
	name   string
	group  string
}

func Open(ctx context.Context, redisURL, streamName, groupName string) (*Stream, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	stream := New(client, streamName, groupName)
	if err := stream.EnsureGroup(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}

	return stream, nil
}

func New(client *redis.Client, streamName, groupName string) *Stream {
	if streamName == "" {
		streamName = DefaultStream
	}
	if groupName == "" {
		groupName = DefaultGroup
	}

	return &Stream{client: client, name: streamName, group: groupName}
}

func (s *Stream) Close() error {
	return s.client.Close()
}

func (s *Stream) Publish(ctx context.Context, job domain.Job) error {
	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.name,
		Values: map[string]any{
			"job_id":        job.ID,
			"type":          job.Type,
			"priority":      strconv.Itoa(job.Priority),
			"execute_after": job.ExecuteAfter.UTC().Format(time.RFC3339Nano),
			"payload":       string(job.Payload),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("publish job to redis stream: %w", err)
	}

	return nil
}

func (s *Stream) PublishDeadLetter(ctx context.Context, job domain.Job) error {
	_, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.name + DeadLetterStreamSuffix,
		Values: map[string]any{
			"job_id":     job.ID,
			"type":       job.Type,
			"last_error": stringValue(job.LastError),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("publish dead-letter job to redis stream: %w", err)
	}

	return nil
}

func (s *Stream) EnsureGroup(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.name, s.group, "0").Err()
	if err != nil && !errors.Is(err, redis.Nil) && !isBusyGroupError(err) {
		return fmt.Errorf("create redis consumer group: %w", err)
	}

	return nil
}

func (s *Stream) Read(ctx context.Context, consumer string, count int64, block time.Duration) ([]Message, error) {
	entries, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.group,
		Consumer: consumer,
		Streams:  []string{s.name, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read redis stream: %w", err)
	}

	return flattenMessages(entries)
}

func (s *Stream) AutoClaim(ctx context.Context, consumer string, minIdle time.Duration, start string, count int64) ([]Message, string, error) {
	entries, next, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.name,
		Group:    s.group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, "0-0", nil
	}
	if err != nil {
		return nil, start, fmt.Errorf("auto-claim redis stream messages: %w", err)
	}

	messages := make([]Message, 0, len(entries))
	for _, entry := range entries {
		jobID, ok := stringField(entry.Values, "job_id")
		if !ok {
			return nil, next, fmt.Errorf("%w: message %s has no job_id", ErrInvalidMessage, entry.ID)
		}
		messages = append(messages, Message{ID: entry.ID, JobID: jobID})
	}

	return messages, next, nil
}

func (s *Stream) Ack(ctx context.Context, messageID string) error {
	if err := s.client.XAck(ctx, s.name, s.group, messageID).Err(); err != nil {
		return fmt.Errorf("ack redis stream message: %w", err)
	}
	return nil
}

func flattenMessages(streams []redis.XStream) ([]Message, error) {
	if len(streams) == 0 {
		return nil, nil
	}

	messages := make([]Message, 0, len(streams[0].Messages))
	for _, entry := range streams[0].Messages {
		jobID, ok := stringField(entry.Values, "job_id")
		if !ok {
			return nil, fmt.Errorf("%w: message %s has no job_id", ErrInvalidMessage, entry.ID)
		}
		messages = append(messages, Message{ID: entry.ID, JobID: jobID})
	}

	return messages, nil
}

func stringField(values map[string]any, name string) (string, bool) {
	value, ok := values[name]
	if !ok {
		return "", false
	}

	switch typed := value.(type) {
	case string:
		return typed, typed != ""
	case []byte:
		return string(typed), len(typed) > 0
	default:
		return fmt.Sprint(typed), true
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func isBusyGroupError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
