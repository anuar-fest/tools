package natsjs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// StreamConfig declares a stream with explicit limits. Zero limits mean
// "unbounded", which is never what production wants: set MaxAge and MaxBytes.
type StreamConfig struct {
	Name     string
	Subjects []string
	// Retention: Limits (default) keeps messages until a limit is hit;
	// Interest drops them once every consumer acked; WorkQueue once one did.
	Retention jetstream.RetentionPolicy
	// MaxAge evicts old messages (default 7 days).
	MaxAge time.Duration
	// MaxBytes caps the stream on disk (0 = unbounded).
	MaxBytes int64
	// MaxMsgsPerSubject caps per subject (0 = unbounded).
	MaxMsgsPerSubject int64
	// Replicas in a clustered server (default 1; 3 for anything you must not lose).
	Replicas int
	// Storage: File (default) or Memory.
	Storage jetstream.StorageType
	// DuplicateWindow is how long Nats-Msg-Id dedup remembers an id (default 2m).
	DuplicateWindow time.Duration
	// ReplaceSubjects makes EnsureStream set the subject list exactly instead
	// of merging it with the existing one. Only the stream's owner sets it.
	ReplaceSubjects bool
}

func (s StreamConfig) toJS() jetstream.StreamConfig {
	cfg := jetstream.StreamConfig{
		Name:              s.Name,
		Subjects:          s.Subjects,
		Retention:         s.Retention,
		MaxAge:            s.MaxAge,
		MaxBytes:          s.MaxBytes,
		MaxMsgsPerSubject: s.MaxMsgsPerSubject,
		Replicas:          s.Replicas,
		Storage:           s.Storage,
		Duplicates:        s.DuplicateWindow,
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 7 * 24 * time.Hour
	}
	if cfg.Replicas <= 0 {
		cfg.Replicas = 1
	}
	if cfg.Duplicates <= 0 {
		cfg.Duplicates = 2 * time.Minute
	}
	return cfg
}

// EnsureStream creates the stream or updates it to match cfg. Subjects are
// merged with the existing list unless ReplaceSubjects is set: a service
// pinned to an older contract must never remove subjects another service
// publishes on. Changes are logged with before and after.
func (c *Client) EnsureStream(ctx context.Context, cfg StreamConfig) error {
	if cfg.Name == "" || len(cfg.Subjects) == 0 {
		return errors.New("natsjs: stream Name and Subjects are required")
	}
	want := cfg.toJS()

	stream, err := c.js.Stream(ctx, cfg.Name)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		if _, createErr := c.js.CreateStream(ctx, want); createErr != nil {
			return fmt.Errorf("natsjs: create stream %q: %w", cfg.Name, createErr)
		}
		c.logger.Info("stream created", "stream", cfg.Name, "subjects", want.Subjects)
		return nil
	}
	if err != nil {
		return fmt.Errorf("natsjs: inspect stream %q: %w", cfg.Name, err)
	}

	have := stream.CachedInfo().Config
	if !cfg.ReplaceSubjects {
		want.Subjects = mergeSubjects(have.Subjects, want.Subjects)
	}
	if sameSubjects(have.Subjects, want.Subjects) && sameLimits(have, want) {
		return nil
	}
	if _, err := c.js.UpdateStream(ctx, want); err != nil {
		return fmt.Errorf("natsjs: update stream %q: %w", cfg.Name, err)
	}
	c.logger.Info("stream updated", "stream", cfg.Name, "subjects_before", have.Subjects, "subjects_after", want.Subjects)
	return nil
}

func mergeSubjects(existing, wanted []string) []string {
	out := slices.Clone(existing)
	for _, s := range wanted {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

func sameSubjects(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as, bs := slices.Clone(a), slices.Clone(b)
	slices.Sort(as)
	slices.Sort(bs)
	return slices.Equal(as, bs)
}

func sameLimits(have, want jetstream.StreamConfig) bool {
	return have.Retention == want.Retention &&
		have.MaxAge == want.MaxAge &&
		have.MaxBytes == want.MaxBytes &&
		have.MaxMsgsPerSubject == want.MaxMsgsPerSubject &&
		have.Replicas == want.Replicas &&
		have.Storage == want.Storage &&
		have.Duplicates == want.Duplicates
}

// DeadLetterStream is the conventional stream for dead-lettered messages:
// every consumer's DeadLetterSubject is "dlq.<consumer>" under it, kept for
// 14 days so an operator can inspect and replay.
func DeadLetterStream() StreamConfig {
	return StreamConfig{Name: "DLQ", Subjects: []string{"dlq.>"}, MaxAge: 14 * 24 * time.Hour}
}

// DeadLetterSubject is the subject a consumer dead-letters to.
func DeadLetterSubject(consumer string) string { return "dlq." + consumer }
