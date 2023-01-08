package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/notification"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"

	"repos.se/minio-deduplication/v2/pkg/kafka"
	"repos.se/minio-deduplication/v2/pkg/metadata"
)

type uploaded struct {
	Key string
	Ext string
}

var (
	inbox                   string
	archive                 string
	host                    string
	secure                  bool
	accesskey               string
	secretkey               string
	metrics                 string
	trace                   bool
	kafkaBootstrap          = os.Getenv("KAFKA_BOOTSTRAP")
	kafkaTopic              = os.Getenv("KAFKA_TOPIC")
	kafkaConsumerGroup      = os.Getenv("KAFKA_CONSUMER_GROUP")
	kafkaFetchMaxWait       = os.Getenv("KAFKA_FETCH_MAX_WAIT")
	kafkaFetchMaxWaitDef, _ = time.ParseDuration("1s") // Default is 5 s which will keep users waiting quite a bit, https://github.com/twmb/franz-go/blob/v1.11.0/pkg/kgo/config.go#L1096

	ignoredUnexpectedBucket = promauto.NewCounter(prometheus.CounterOpts{
		Name: "blobs_ignored_unexpected_bucket",
		Help: "The number of notifications ignored because the bucket didn't match the requested name",
	})
	transfersStarted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobs_transfers_initiated",
			Help: "The number of transfers started, by trigger method",
		},
		[]string{"trigger"},
	)
	transfersCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "blobs_transfers_completed",
		Help: "The number of copy operations that completed without errors",
	})
	duplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "blobs_duplicates",
		Help: "How many times a destination object existed (we still try to update metadata)",
	})
)

func init() {
	flag.StringVar(&inbox, "inbox", "", "Uploads bucket")
	flag.StringVar(&archive, "archive", "", "archive bucket")
	flag.StringVar(&host, "host", "", "minio host")
	flag.BoolVar(&secure, "secure", true, "https")
	flag.StringVar(&accesskey, "accesskey", "", "access key")
	flag.StringVar(&secretkey, "secretkey", "", "secret key")
	flag.StringVar(&metrics, "metrics", ":2112", "bind metrics server to")
	flag.BoolVar(&trace, "trace", false, "Enable minio client tracing")
	flag.Parse()
}

func getConsumerGroupName(logger *zap.Logger) string {
	if kafkaConsumerGroup != "" {
		return kafkaConsumerGroup
	}
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace != "" {
		name := fmt.Sprintf("minio-deduplication.%s", namespace)
		logger.Info("Consumer group name not configured, used namespace to guess", zap.String("name", name))
		return name
	}
	host := os.Getenv("HOST")
	if host == "" {
		logger.Fatal("Consumer group required but not set, and no HOST env")
	}
	logger.Info("Consumer group name not configured, used hostname to guess", zap.String("name", host))
	return host
}

func assertBucketExists(ctx context.Context, name string, minioClient *minio.Client, logger *zap.Logger) {
	check := func() error {
		found, err := minioClient.BucketExists(ctx, name)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("non-existent bucket: %s", name)
		}
		return nil
	}
	log := func(err error, t time.Duration) {
		logger.Warn("Bucket existence check failed", zap.String("name", name), zap.Duration("t", t), zap.Error(err))
	}
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = time.Second / 4
	err := backoff.RetryNotify(
		check,
		backoff.WithMaxRetries(policy, 10),
		log,
	)
	if err != nil {
		logger.Fatal("Failed to verify bucket existence", zap.String("name", name))
	}
}

func transfer(ctx context.Context, blob uploaded, minioClient *minio.Client, logger *zap.Logger) {
	objectInfo, err := minioClient.StatObject(ctx, inbox, blob.Key, minio.StatObjectOptions{})
	if err != nil {
		logger.Fatal("Failed to stat source object",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
			zap.Error(err),
		)
	}

	object, err := minioClient.GetObject(ctx, inbox, blob.Key, minio.GetObjectOptions{})
	if err != nil {
		logger.Fatal("Failed to read source object",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
			zap.Error(err),
		)
	}
	hasher := sha256.New()
	defer object.Close()
	if _, err := io.Copy(hasher, object); err != nil {
		logger.Fatal("Failed to read source object to checksum",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
			zap.Error(err),
		)
	}
	sha256hex := fmt.Sprintf("%x", hasher.Sum(nil))
	logger.Debug("SHA256", zap.String("hex", sha256hex))
	write := fmt.Sprintf("%s/%s%s", archive, sha256hex, blob.Ext)
	logger.Info("Transferring",
		zap.String("key", blob.Key),
		zap.String("write", write),
	)

	src := minio.CopySrcOptions{
		Bucket: inbox,
		Object: blob.Key,
	}

	blobDir := sha256hex[0:2] + "/" + sha256hex[2:4] + "/"
	blobName := fmt.Sprintf("%s%s%s", blobDir, sha256hex, blob.Ext)

	existing, err := minioClient.StatObject(ctx, archive, blobName, minio.StatObjectOptions{})
	if err != nil {
		if err.Error() == "The specified key does not exist." {
			logger.Debug("Destination path is new", zap.String("key", blobName))
			err = nil
		} else {
			logger.Fatal("Failed to stat destination path",
				zap.String("key", blobName),
				zap.String("bucket", archive),
				zap.Error(err),
			)
		}
	} else {
		logger.Info("Destination path already exists",
			zap.String("key", blobName),
			zap.Any("meta", existing.UserMetadata),
		)
		duplicates.Inc()
	}

	meta := metadata.NewMetadataNext(objectInfo, existing)

	// temp, based on an old todo, can probably be removed
	if meta.UserMetadata["content-disposition"] == "" {
		logger.Fatal("expected a content-disposition header")
	}

	dst := minio.CopyDestOptions{
		Bucket:          archive,
		Object:          blobName,
		UserMetadata:    meta.UserMetadata,
		ReplaceMetadata: meta.ReplaceMetadata,
	}

	uploadInfo, err := minioClient.CopyObject(ctx, dst, src)
	if err != nil {
		logger.Error("Failed to transfer",
			zap.String("key", blob.Key),
			zap.String("archive", archive),
			zap.Error(err),
		)
		return
	}

	// TODO with v7 we get uploadInfo so the safeguard below might not be needed
	logger.Debug("Copied",
		zap.String("bucket", uploadInfo.Bucket),
		zap.String("key", uploadInfo.Key),
		zap.String("etag", uploadInfo.ETag),
	)
	// This check for destination existence is just a safeguard, because we don't get a lot of feedback from CopyObject
	// Should we check that metadata was transferred too?
	existing, confirmErr := minioClient.StatObject(ctx, archive, blobName, minio.StatObjectOptions{})
	if confirmErr != nil {
		logger.Fatal("Destination blob not found after copy.",
			zap.String("key", blobName),
			zap.String("bucket", archive),
			zap.Error(confirmErr),
		)
	} else {
		logger.Debug("Destination existence confirmed. Deleting inbox item.",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
		)
		cleanupErr := minioClient.RemoveObject(ctx, inbox, blob.Key, minio.RemoveObjectOptions{})
		if cleanupErr != nil {
			logger.Fatal("Failed to clean up after blob copy. Inbox item probably still exists.",
				zap.String("key", inbox),
				zap.String("bucket", blob.Key),
				zap.Error(err),
			)
		}
	}
	transfersCompleted.Inc()
}

func toExtension(key string) string {
	ext := strings.ToLower(filepath.Ext(key))
	if ext == ".jpeg" {
		return ".jpg"
	}
	return ext
}

// Will exit on unrecognized errors, but return err on errors we think we can recover from without crashloop
func mainMinio(ctx context.Context, logger *zap.Logger) error {

	options := &minio.Options{
		Creds:  credentials.NewStaticV4(accesskey, secretkey, ""),
		Secure: false,
	}

	logger.Info("Initializing minio client", zap.String("host", host), zap.Bool("https", secure))
	minioClient, err := minio.New(host, options)
	if err != nil {
		logger.Fatal("Failed to set up minio client",
			zap.Error(err),
		)
	}

	if trace {
		minioClient.TraceOn(os.Stderr)
	}

	// Notification options differ in these regards:
	// - When to wait for bucket existence
	waitForBucketExistence := func() {
		assertBucketExists(ctx, inbox, minioClient, logger)
		assertBucketExists(ctx, archive, minioClient, logger)
		logger.Info("Bucket existence confirmed", zap.String("inbox", inbox), zap.String("archive", archive))
	}
	// - Whether to url decode keys
	urldecodeKeys := false
	// - What to do with existing items
	handleExistingItem := func(object minio.ObjectInfo) {
		logger.Info("Existing inbox object to be transferred", zap.String("key", object.Key))
		transfersStarted.With(prometheus.Labels{"trigger": "listing"}).Inc()
		transfer(ctx, uploaded{
			Key: object.Key,
			Ext: toExtension(object.Key),
		}, minioClient, logger)
	}

	var listenCh <-chan notification.Info
	if kafkaBootstrap != "" {
		logger.Info("Starting kafka bucket notifications listener")
		config := &kafka.KafkaConsumerConfig{
			Logger:        logger,
			Bootstrap:     strings.Split(kafkaBootstrap, ","),
			Topics:        []string{kafkaTopic},
			ConsumerGroup: getConsumerGroupName(logger),
			Filter: kafka.MessageFilter{
				KeyPrefix: fmt.Sprintf("%s/", inbox),
			},
			FetchMaxWait: kafkaFetchMaxWaitDef,
		}
		if kafkaFetchMaxWait != "" {
			config.FetchMaxWait, err = time.ParseDuration(kafkaFetchMaxWait)
			if err != nil {
				logger.Fatal("Failed to parse FetchMaxWait config", zap.String("value", kafkaFetchMaxWait))
			}
		}
		listenCh = kafka.NewKafka(ctx, config)
		waitForBucketExistence()
		urldecodeKeys = true // https://github.com/minio/minio/issues/7665#issuecomment-493681445
		handleExistingItem = func(object minio.ObjectInfo) {
			logger.Warn("Existing ignored; consumer offsets should track prior uploads",
				zap.String("key", object.Key),
			)
		}
	} else {
		waitForBucketExistence()
		logger.Info("Starting standalone bucket notifications listener")
		listenCh = minioClient.ListenBucketNotification(ctx, inbox, "", "", []string{
			"s3:ObjectCreated:Put",
		})
	}

	logger.Info("Listing existing inbox objects")
	objectCh := minioClient.ListObjects(ctx, inbox, minio.ListObjectsOptions{
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			logger.Fatal("List object error", zap.Error(object.Err))
		}
		handleExistingItem(object)
	}

	for notificationInfo := range listenCh {
		if notificationInfo.Err != nil {
			// Can't test these failure modes with the current build infra, but we fall back to crashlooping if detection fails.
			// If we get errors without any successful notifications we'll transfer files anyway, per the ListObjects above.
			if notificationInfo.Err.Error() == "unexpected end of JSON input" {
				logger.Info("Notification abort, which we think is a timeout", zap.Error(notificationInfo.Err))
				return notificationInfo.Err
			}
			if strings.HasPrefix(notificationInfo.Err.Error(), "readObjectStart: expect { or n, but found ") {
				logger.Info("Notification abort, which we think is a timeout", zap.Error(notificationInfo.Err))
				return notificationInfo.Err
			}
			logger.Fatal("Notification error",
				zap.Error(notificationInfo.Err),
			)
		}
		for _, record := range notificationInfo.Records {
			key := record.S3.Object.Key
			if urldecodeKeys {
				key, err = url.QueryUnescape(key)
				if err != nil {
					logger.Fatal("Url decoding failed", zap.String("key", key), zap.Error(err))
				}
			}
			bucket := record.S3.Bucket.Name
			logger.Info("Notification record",
				zap.String("bucket", bucket),
				zap.String("key", key),
			)
			if bucket != inbox {
				logger.Error("Unexpected notification bucket. Ignoring.",
					zap.String("name", bucket),
					zap.String("expected", inbox))
				ignoredUnexpectedBucket.Inc()
				continue
			}
			transfersStarted.With(prometheus.Labels{"trigger": "notification"}).Inc()
			transfer(ctx, uploaded{
				Key: key,
				Ext: toExtension(key),
			}, minioClient, logger)
		}
	}

	logger.Error("Listener exited without an error, or we failed to handle an error")
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logger.Info("Starting /metrics server", zap.String("bound", metrics))
		err := http.ListenAndServe(metrics, nil)
		if err != nil {
			logger.Fatal("Failed to start metrics server", zap.Error(err))
		}
	}()

	for {
		err := mainMinio(ctx, logger)
		if err != nil {
			// Do we need backoff here? Maybe not while we're so specific about which error that triggers re-run.
			logger.Info("Re-running handler", zap.Error(err))
		} else {
			logger.Fatal("Unexpectedly exited without an error")
		}
	}
}
