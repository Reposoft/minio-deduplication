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

type KafkaAckPending struct {
	Info   *notification.Info
	Record *kgo.Record
}

type KafkaAcks struct {
	logger        *zap.Logger
	pending       []KafkaAckPending
	commitRecords func(context.Context, ...*kgo.Record) error
	metricPending prometheus.Gauge
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

func NewKafkaAcks(logger *zap.Logger, metricPending prometheus.Gauge) *KafkaAcks {
	return &KafkaAcks{
		logger:        logger,
		metricPending: metricPending,
	}
}

func (a *KafkaAcks) SetClient(c *kgo.Client) {
	a.SetClientCommit(c.CommitRecords)
}

func (a *KafkaAcks) SetClientCommit(commit func(context.Context, ...*kgo.Record) error) {
	a.commitRecords = commit
}

func (a *KafkaAcks) Expect(p KafkaAckPending) {
	if p.Info == nil {
		a.logger.Fatal("Refusing to record pending with nil info")
	}
	if p.Record == nil {
		a.logger.Fatal("Refusing to record pending with nil record")
	}
	// a.uniqueId(p.Info) // verify compatibility, currently unsupported for unit tests
	a.pending = append(a.pending, p)
	a.logger.Info("Recorded pending ack")
	a.metricPending.Inc()
}

// uniqueId trusts https://github.com/minio/minio/blob/RELEASE.2023-01-06T18-11-18Z/cmd/event-notification.go#L289
func (a *KafkaAcks) uniqueId(info *notification.Info) string {
	if len(info.Records) != 1 {
		a.logger.Fatal("Unsupported records", zap.Int("len", len(info.Records)), zap.Any("info", info))
	}
	u := info.Records[0].S3.Object.Sequencer
	if u == "" {
		a.logger.Fatal("Missing record uniqueness value", zap.Any("info", info))
	}
	return info.Records[0].S3.Object.Sequencer
}

func (a *KafkaAcks) lookup(info *notification.Info) (int, KafkaAckPending) {
	if len(a.pending) == 0 {
		a.logger.Fatal("Ack requested but there are no pending records")
	}
	for i, p := range a.pending {
		if p.Info == info { // used by unit test
			return i, p
		}
		if a.uniqueId(p.Info) == a.uniqueId(info) {
			return i, p
		}
		a.logger.Warn("Fifo order pending lookup failed",
			zap.Any(fmt.Sprintf("index%d", i), p.Info),
			zap.String("infoptr", fmt.Sprintf("%p", p.Info)),
		)
	}
	a.logger.Fatal("Failed to find unacked record",
		zap.Any("expected", info),
		zap.String("ptr", fmt.Sprintf("%p", info)),
	)
	return -1, KafkaAckPending{} // after fatal
}

// remove does lookup, then removes the matching item from pending
func (a *KafkaAcks) remove(info *notification.Info) KafkaAckPending {
	i, p := a.lookup(info)
	a.pending = append(a.pending[:i], a.pending[i+1:]...)
	return p
}

func (a *KafkaAcks) Ack(ackctx context.Context, result bucket.TransferResult, info *notification.Info) {
	if a.commitRecords == nil {
		a.logger.Fatal("Ack called prior to kafka client initialization")
	}
	pending := a.remove(info)
	record := pending.Record
	if result != bucket.TransferOk {
		a.logger.Fatal("Ack for failed transfers not implemented", zap.Any("info", info), zap.Any("record", record))
	}
	if err := a.commitRecords(ackctx, record); err != nil {
		a.logger.Fatal("Offset commit failed",
			zap.String("topic", record.Topic),
			zap.Int32("partition", record.Partition),
			zap.Int64("offset", record.Offset),
			zap.ByteString("key", record.Key),
			zap.Error(err),
		)
	} else {
		a.logger.Info("Commtting",
			zap.String("topic", record.Topic),
			zap.Int32("partition", record.Partition),
			zap.Int64("offset", record.Offset),
		)
	}
	a.metricPending.Dec()
}

func (a *KafkaAcks) PendingSize() int {
	return len(a.pending)
}

func NewKafka(ctx context.Context, config *KafkaConsumerConfig) *bucket.InboxWatcher {

	logger := config.Logger

	filter := NewFilterPredicate(config.Filter, logger)

	acks := NewKafkaAcks(logger, promauto.NewGauge(prometheus.GaugeOpts{
		Name: "blobs_watch_acks_pending",
		Help: "Notifications emitted but not yet acked for on the consumer",
	}))

	// https://github.com/minio/minio-go/blob/v7.0.46/api-bucket-notification.go#L209
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	ch := make(chan notification.Info)
	result := &bucket.InboxWatcher{
		Uploads: ch,
		Ack:     acks.Ack,
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

		// We're naive w.r.t https://github.com/twmb/franz-go/blob/v1.11.0/docs/producing-and-consuming.md#offset-management
		acks.SetClient(cl)

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
					acks.Expect(KafkaAckPending{
						Info:   &notificationInfo,
						Record: record,
					})
					notificationInfoCh <- notificationInfo
				})
			})
		}
	}(ch)

	// Returns the notification info channel, for caller to start reading from.
	return result
}
