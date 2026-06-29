package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// errNoTopic signals a topic that does not exist on the broker.
var errNoTopic = errors.New("topic does not exist")

func dial(cfg *Config, opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}, opts...)...)
}

func admin(cfg *Config) (*kadm.Client, func(), error) {
	cl, err := dial(cfg)
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

// rf returns a replication factor that fits the cluster (capped at 3).
func rf(cfg *Config) int16 {
	if n := len(cfg.Brokers); n < 3 {
		return int16(n)
	}
	return 3
}

// ensureTopic creates a topic if it does not already exist.
func ensureTopic(ctx context.Context, adm *kadm.Client, cfg *Config, topic string, configs map[string]*string) error {
	resp, err := adm.CreateTopics(ctx, 1, rf(cfg), configs, topic)
	if err != nil {
		return err
	}
	for _, t := range resp {
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return t.Err
		}
	}
	return nil
}

// bounds returns the earliest and latest offsets of a single-partition topic.
func bounds(ctx context.Context, adm *kadm.Client, topic string) (start, end int64, err error) {
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	eo, ok := ends.Lookup(topic, 0)
	if !ok || errors.Is(eo.Err, kerr.UnknownTopicOrPartition) {
		return 0, 0, errNoTopic
	}
	if eo.Err != nil {
		return 0, 0, eo.Err
	}
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, err
	}
	so, _ := starts.Lookup(topic, 0)
	return so.Offset, eo.Offset, nil
}

// fetchFrom reads records [start, end) from partition 0 of a single-partition topic.
// It stops once it has consumed up to offset end-1, or after an idle poll (which
// handles offset gaps left by log compaction on the registry topic).
func fetchFrom(ctx context.Context, cfg *Config, topic string, start, end int64) ([]*kgo.Record, error) {
	if start >= end {
		return nil, nil
	}
	cl, err := dial(cfg, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(start)},
	}))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	var recs []*kgo.Record
	for {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		if errs := fs.Errors(); len(errs) > 0 {
			// An idle poll (our per-poll deadline) means we've caught up.
			if ctx.Err() == nil {
				return recs, nil
			}
			return recs, errs[0].Err
		}
		var maxOff int64 = -1
		fs.EachRecord(func(r *kgo.Record) {
			recs = append(recs, r)
			maxOff = r.Offset
		})
		if maxOff >= end-1 || maxOff < 0 {
			break
		}
	}
	return recs, nil
}

// committedOffset returns the committed offset for (group, topic, p0), or -1 if none.
func committedOffset(ctx context.Context, adm *kadm.Client, group, topic string) (int64, error) {
	off := int64(-1)
	err := withCoordRetry(ctx, func() error {
		resp, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return err
		}
		if err := resp.Error(); err != nil {
			return err
		}
		if o, ok := resp.Lookup(topic, 0); ok && o.Err == nil {
			off = o.At
		}
		return nil
	})
	return off, err
}

// commit stores the committed offset for (group, topic, p0).
func commit(ctx context.Context, adm *kadm.Client, group, topic string, offset int64) error {
	os := make(kadm.Offsets)
	os.AddOffset(topic, 0, offset, -1)
	return withCoordRetry(ctx, func() error {
		resp, err := adm.CommitOffsets(ctx, group, os)
		if err != nil {
			return err
		}
		return resp.Error()
	})
}

// isCoordRetriable reports whether err is a transient group-coordinator error —
// seen on a fresh cluster (before __consumer_offsets exists) or during failover.
func isCoordRetriable(err error) bool {
	return errors.Is(err, kerr.CoordinatorNotAvailable) ||
		errors.Is(err, kerr.CoordinatorLoadInProgress) ||
		errors.Is(err, kerr.NotCoordinator)
}

// withCoordRetry retries fn on transient coordinator errors until ctx is done.
func withCoordRetry(ctx context.Context, fn func() error) error {
	var err error
	for i := 0; i < 12; i++ {
		if err = fn(); err == nil || !isCoordRetriable(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return err
}

func produce(ctx context.Context, cfg *Config, rec *kgo.Record) error {
	cl, err := dial(cfg)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}
