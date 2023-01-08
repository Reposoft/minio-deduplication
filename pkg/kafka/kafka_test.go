package kafka_test

import (
	"context"
	"testing"

	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"repos.se/minio-deduplication/v2/pkg/bucket"
	"repos.se/minio-deduplication/v2/pkg/kafka"
)

func TestFilter(t *testing.T) {

	logger := zap.NewNop()
	noFilter := kafka.NewFilterPredicate(kafka.MessageFilter{
		KeyPrefix: "",
	}, logger)

	if !noFilter(&kgo.Record{Key: []byte{}}) {
		t.Error("Filter should return true for any record when not configured")
	}
	if !noFilter(&kgo.Record{Key: []byte("foo")}) {
		t.Error("Filter should return true for arbitrary key when not configured")
	}

	prefix := kafka.NewFilterPredicate(kafka.MessageFilter{
		KeyPrefix: "bucket-name/",
	}, logger)
	if !prefix(&kgo.Record{Key: []byte("bucket-name/filename.txt")}) {
		t.Error("Filter should return true for configured prefix")
	}
	if !prefix(&kgo.Record{Key: []byte("bucket-name/")}) {
		t.Error("Filter should return true for configured prefix exact match")
	}
	if prefix(&kgo.Record{Key: []byte("bucket-name")}) {
		t.Error("Filter should return false for not prefix")
	}
	if prefix(&kgo.Record{Key: []byte(" bucket-name/")}) {
		t.Error("Filter should return false with leading whitespace")
	}

}

func TestAcks(t *testing.T) {

	ctx := context.TODO()
	logger := zap.NewNop()
	metricPending := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "test_pending",
	})
	acks := kafka.NewKafkaAcks(logger, metricPending)

	// many states use zap logger.Fatal so ATM we can only test the happy path

	var commits []*kgo.Record

	acks.SetClientCommit(func(ctx context.Context, r ...*kgo.Record) error {
		if len(r) != 1 {
			t.Errorf("Unexpected records batch length %d", len(r))
		}
		commits = append(commits, r...)
		return nil
	})

	info1 := notification.Info{}
	info1ptr1 := &info1
	info1ptr2 := &info1
	record1 := kgo.Record{}
	record1ptr1 := &record1
	p1 := kafka.KafkaAckPending{
		Info:   info1ptr1,
		Record: record1ptr1,
	}
	acks.Expect(p1)

	acks.Ack(ctx, bucket.TransferOk, info1ptr2)

	if len(commits) != 1 {
		t.Errorf("Expected 1 captured test commit, got %d", len(commits))
	}

}
