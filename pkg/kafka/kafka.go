package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
)

type MessageFilter struct {
	KeyPrefix string
}

type KafkaConsumerConfig struct {
	Logger        *zap.Logger
	Bootstrap     []string
	Topics        []string
	ConsumerGroup string
	FetchMaxWait  time.Duration
	Filter        MessageFilter
}

func NewFilterPredicate(config MessageFilter, logger *zap.Logger) func(record *kgo.Record) bool {
	if config.KeyPrefix == "" {
		return func(record *kgo.Record) bool {
			return true
		}
	}
	prefix := []byte(config.KeyPrefix)
	logger.Info("Message filter enabled on key", zap.ByteString("prefix", prefix))
	return func(record *kgo.Record) bool {
		return bytes.HasPrefix(record.Key, prefix)
	}
}

func NewKafka(ctx context.Context, config *KafkaConsumerConfig) <-chan notification.Info {
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	cl, err := kgo.NewClient(
		kgo.WithLogger(kzap.New(config.Logger)),
		kgo.SeedBrokers(config.Bootstrap...),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(config.Topics...),
		kgo.FetchMaxWait(config.FetchMaxWait),
	)
	if err != nil {
		config.Logger.Fatal("Kafka client failure",
			zap.Strings("bootstrap", config.Bootstrap),
			zap.Strings("topics", config.Topics),
			zap.String("group", config.ConsumerGroup),
			zap.Error(err),
		)
	}

	filter := NewFilterPredicate(config.Filter, config.Logger)

	// https://github.com/minio/minio-go/blob/v7.0.46/api-bucket-notification.go#L209
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	ch := make(chan notification.Info)

	go func(notificationInfoCh chan<- notification.Info) {
		defer cl.Close()
		defer close(notificationInfoCh)

		for {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				config.Logger.Fatal("Non-retryable consumer error",
					zap.String("errors", fmt.Sprint(errs)),
				)
			}

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				// We can even use a second callback!
				p.EachRecord(func(record *kgo.Record) {
					if !filter(record) {
						config.Logger.Debug("Filtered out",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.ByteString("key", record.Key),
							zap.Time("timestamp", record.Timestamp),
							zap.Error(err),
						)
						return
					}
					var notificationInfo notification.Info
					err := json.Unmarshal(record.Value, &notificationInfo)
					if err != nil {
						// We'd need to export a counter here if we want to skip over unrecognized events
						// Instead we crashloop and an admin must set a new consumer group offset
						config.Logger.Fatal("Failed to unmarshal notification",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.ByteString("key", record.Key),
							zap.ByteString("value", record.Value),
							zap.Time("timestamp", record.Timestamp),
							zap.Error(err),
						)
					}
					if len(notificationInfo.Records) == 0 {
						config.Logger.Error("Got notification with zero records",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.ByteString("key", record.Key),
							zap.ByteString("value", record.Value),
							zap.Time("timestamp", record.Timestamp),
						)
					}
					config.Logger.Info("Got notification",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.ByteString("key", record.Key),
						zap.Time("timestamp", record.Timestamp),
					)
					notificationInfoCh <- notificationInfo
				})
			})
		}
	}(ch)

	// Returns the notification info channel, for caller to start reading from.
	return ch

}
