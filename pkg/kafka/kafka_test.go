package kafka_test

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
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
