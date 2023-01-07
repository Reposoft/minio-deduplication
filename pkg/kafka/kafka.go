package kafka

import (
	"context"
	"fmt"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
)

type KafkaConsumerConfig struct {
	Logger        *zap.Logger
	Bootstrap     []string
	Topics        []string
	ConsumerGroup string
}

func NewKafka(ctx context.Context, config *KafkaConsumerConfig) {
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	cl, err := kgo.NewClient(
		kgo.WithLogger(kzap.New(config.Logger)),
		kgo.SeedBrokers(config.Bootstrap...),
		kgo.ConsumerGroup(config.ConsumerGroup),
		kgo.ConsumeTopics(config.Topics...),
	)
	if err != nil {
		config.Logger.Fatal("Kafka client failure",
			zap.Strings("bootstrap", config.Bootstrap),
			zap.Strings("topics", config.Topics),
			zap.String("group", config.ConsumerGroup),
			zap.Error(err),
		)
	}
	defer cl.Close()

	// https://github.com/minio/minio-go/blob/v7.0.46/api-bucket-notification.go#L209
	json := jsoniter.ConfigCompatibleWithStandardLibrary

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
				var notificationInfo notification.Info
				err := json.Unmarshal(record.Value, &notificationInfo)
				if err != nil {
					// we'd need to export a counter here if we want to skip over unrecognized events
					config.Logger.Fatal("Failed to unmarshal notification",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.ByteString("key", record.Key),
						zap.ByteString("value", record.Value),
						zap.Time("timestamp", record.Timestamp),
						zap.Error(err),
					)
				}
				config.Logger.Debug("Consumed", zap.ByteString("value", record.Value))
				for _, event := range notificationInfo.Records {
					config.Logger.Info("Got notification",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.ByteString("key", record.Key),
						zap.Time("timestamp", record.Timestamp),
						zap.String("bucket", event.S3.Bucket.Name),
						zap.String("path", event.S3.Object.Key),
					)
				}
			})
		})
	}

}
