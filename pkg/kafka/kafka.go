package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7/pkg/notification"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzap"
	"go.uber.org/zap"
	"repos.se/minio-deduplication/v2/pkg/bucket"
)

var (
	metricAckPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "blobs_watch_acks_pending",
		Help: "Notifications emitted but not yet acked for on the consumer",
	})
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
	ignoredFiltered := promauto.NewCounter(prometheus.CounterOpts{
		Name: "blobs_ignored_filtered",
		Help: "The number of notifications ignored the notification did not match the filter",
		ConstLabels: prometheus.Labels{
			"prefix": string(prefix),
		},
	})
	logger.Info("Message filter enabled on key", zap.ByteString("prefix", prefix))
	return func(record *kgo.Record) bool {
		hit := bytes.HasPrefix(record.Key, prefix)
		if !hit {
			ignoredFiltered.Inc()
		}
		return hit
	}
}

func NewKafka(ctx context.Context, config *KafkaConsumerConfig) *bucket.InboxWatcher {

	logger := config.Logger

	filter := NewFilterPredicate(config.Filter, logger)

	// https://github.com/minio/minio-go/blob/v7.0.46/api-bucket-notification.go#L209
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	ch := make(chan notification.Info)
	result := &bucket.InboxWatcher{
		Uploads: ch,
		Ack: func(ctx context.Context, tr bucket.TransferResult, i *notification.Info) {
			logger.Fatal("Ack called prior to kafka client initialization")
		},
	}

	unacked := make(map[*notification.Info]*kgo.Record)
	ackExpect := func(info *notification.Info, record *kgo.Record) {
		unacked[info] = record
		logger.Info("Recorded pending ack", zap.String("ptr", fmt.Sprintf("%p", info)))
		metricAckPending.Inc()
	}

	go func(notificationInfoCh chan<- notification.Info) {
		cl, err := kgo.NewClient(
			kgo.WithLogger(kzap.New(logger)),
			kgo.SeedBrokers(config.Bootstrap...),
			kgo.ConsumerGroup(config.ConsumerGroup),
			kgo.ConsumeTopics(config.Topics...),
			kgo.FetchMaxWait(config.FetchMaxWait),
		)
		if err != nil {
			logger.Fatal("Kafka client failure",
				zap.Strings("bootstrap", config.Bootstrap),
				zap.Strings("topics", config.Topics),
				zap.String("group", config.ConsumerGroup),
				zap.Error(err),
			)
		}
		defer cl.Close()
		defer close(notificationInfoCh)

		result.Ack = func(ackctx context.Context, result bucket.TransferResult, info *notification.Info) {
			record := unacked[info]
			if record == nil {
				logger.Fatal("Failed to find unacked record", zap.String("ptr", fmt.Sprintf("%p", info)))
			}
			if result != bucket.TransferOk {
				logger.Fatal("Ack for failed transfers not implemented", zap.Any("info", info), zap.Any("record", record))
			}
			if err := cl.CommitRecords(ackctx, record); err != nil {
				logger.Fatal("Offset commit failed",
					zap.String("topic", record.Topic),
					zap.Int32("partition", record.Partition),
					zap.Int64("offset", record.Offset),
					zap.ByteString("key", record.Key),
					zap.Error(err),
				)
			} else {
				logger.Info("Commtting",
					zap.String("topic", record.Topic),
					zap.Int32("partition", record.Partition),
					zap.Int64("offset", record.Offset),
				)
			}
			metricAckPending.Dec()
		}

		for {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				logger.Fatal("Non-retryable consumer error",
					zap.String("errors", fmt.Sprint(errs)),
				)
			}

			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				p.EachRecord(func(record *kgo.Record) {
					if !filter(record) {
						logger.Debug("Filtered out",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.ByteString("key", record.Key),
							zap.Time("timestamp", record.Timestamp),
							zap.Error(err),
						)
						return
					}
					var notificationInfo notification.Info
					notificationInfoPtr := &notificationInfo
					err := json.Unmarshal(record.Value, notificationInfoPtr)
					if err != nil {
						// We'd need to export a counter here if we want to skip over unrecognized events
						// Instead we crashloop and an admin must set a new consumer group offset
						logger.Fatal("Failed to unmarshal notification",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.Int64("offset", record.Offset),
							zap.ByteString("key", record.Key),
							zap.ByteString("value", record.Value),
							zap.Time("timestamp", record.Timestamp),
							zap.Error(err),
						)
					}
					if len(notificationInfo.Records) == 0 {
						logger.Error("Got notification with zero records",
							zap.String("topic", record.Topic),
							zap.Int32("partition", record.Partition),
							zap.Int64("offset", record.Offset),
							zap.ByteString("key", record.Key),
							zap.ByteString("value", record.Value),
							zap.Time("timestamp", record.Timestamp),
						)
					}
					logger.Info("Got notification",
						zap.String("topic", record.Topic),
						zap.Int32("partition", record.Partition),
						zap.Int64("offset", record.Offset),
						zap.ByteString("key", record.Key),
						zap.Time("timestamp", record.Timestamp),
					)
					ackExpect(notificationInfoPtr, record)
					notificationInfoCh <- notificationInfo
				})
			})
		}
	}(ch)

	// Returns the notification info channel, for caller to start reading from.
	return result
}
