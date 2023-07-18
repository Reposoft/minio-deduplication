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

	"repos.se/minio-deduplication/v2/pkg/bucket"
	"repos.se/minio-deduplication/v2/pkg/index"
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
	batch                   bool
	batchmetrics            bool
	batchmetricsWaitMax     = time.Duration(time.Minute * 1)
	restartDelay            time.Duration
	indexNext               *index.Index
	indexWrite              bool
	indexWriteDir           = "deduplication-index"
	indexType               = "application/jsonlines"
	kafkaBootstrap          = os.Getenv("KAFKA_BOOTSTRAP")
	kafkaTopic              = os.Getenv("KAFKA_TOPIC")
	kafkaConsumerGroup      = os.Getenv("KAFKA_CONSUMER_GROUP")
	kafkaFetchMaxWait       = os.Getenv("KAFKA_FETCH_MAX_WAIT")
	kafkaFetchMaxWaiDefault = time.Duration(time.Second * 1) // Default is 5 s which will keep users waiting quite a bit, https://github.com/twmb/franz-go/blob/v1.11.0/pkg/kgo/config.go#L1096

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
	flag.BoolVar(&batch, "batch", false, "Run in batch mode: list + transfer then exit")
	flag.BoolVar(&batchmetrics, "batchmetrics", false, "Wait for metrics scrape after batch run")
	flag.DurationVar(&restartDelay, "restartdelay", time.Duration(time.Second*1), "On error restart after sleep, zero to disable restart")
	flag.BoolVar(&indexWrite, "index", false, "Write index files to archive /minio-deduplication-index/*")
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
		// NOTE with kafka notifications we currently use the default commit behavior
		// https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md#consumer-groups
		// which means that it's likely after unclean exit that transfers happend but commit did not.
		// The risk would be present but lower with commit immediately upon transfer.
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
	indexNext.AppendTransfer(
		blob.Key,
		uploadInfo,
		existing.Key != "",
		meta,
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

	// These variables predate the InboxWatcher interface, and should probably be incorporated there:
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

	var watcher *bucket.InboxWatcher
	if batch {
		if kafkaBootstrap != "" {
			zap.L().Fatal("batch and kafka mode cannot be combined")
		}
		logger.Info("Batch mode enabled, no listener will be created")
	} else if kafkaBootstrap != "" {
		logger.Info("Starting kafka bucket notifications listener")
		config := &kafka.KafkaConsumerConfig{
			Logger:        logger,
			Bootstrap:     strings.Split(kafkaBootstrap, ","),
			Topics:        []string{kafkaTopic},
			ConsumerGroup: getConsumerGroupName(logger),
			Filter: kafka.MessageFilter{
				KeyPrefix: fmt.Sprintf("%s/", inbox),
			},
			FetchMaxWait: kafkaFetchMaxWaiDefault,
		}
		if kafkaFetchMaxWait != "" {
			config.FetchMaxWait, err = time.ParseDuration(kafkaFetchMaxWait)
			if err != nil {
				logger.Fatal("Failed to parse FetchMaxWait config", zap.String("value", kafkaFetchMaxWait))
			}
		}
		watcher = kafka.NewKafka(ctx, config)
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
		watcher = &bucket.InboxWatcher{
			Uploads: minioClient.ListenBucketNotification(ctx, inbox, "", "", []string{
				"s3:ObjectCreated:Put",
			}),
			Ack: func(ackctx context.Context, tr bucket.TransferResult, i *notification.Info) {
				if tr == bucket.TransferFailed {
					logger.Error("Nack on transfer failure not implemented")
				} else {
					logger.Debug("Ack is a no-op for ListenBucketNotification")
				}
			},
		}
	}

	logger.Info("Listing existing inbox objects")
	objectCh := minioClient.ListObjects(ctx, inbox, minio.ListObjectsOptions{
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			logger.Error("List object error", zap.Error(object.Err))
			return object.Err
		}
		handleExistingItem(object)
	}

	if batch {
		if indexWrite && indexNext.Size() > 0 {
			indexKey := fmt.Sprintf("%s/%s",
				indexWriteDir,
				time.Now().UTC().Format("2006-01-02t150405.jsonlines"),
			)
			indexBody, indexBytes, err := indexNext.Serialize(indexType)
			if err != nil {
				logger.Fatal("Failed to get index serializer", zap.Error(err))
			}
			minioClient.PutObject(ctx, archive, indexKey, indexBody, indexBytes, minio.PutObjectOptions{})
			logger.Info("Wrote index", zap.String("key", indexKey), zap.Int64("size", indexBytes))
		}
		return nil
	}

	for notificationInfo := range watcher.Uploads {
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
			bucketName := record.S3.Bucket.Name
			logger.Info("Notification record",
				zap.String("bucket", bucketName),
				zap.String("key", key),
			)
			if bucketName != inbox {
				logger.Error("Unexpected notification bucket. Ignoring.",
					zap.String("name", bucketName),
					zap.String("expected", inbox))
				ignoredUnexpectedBucket.Inc()
				continue
			}
			transfersStarted.With(prometheus.Labels{"trigger": "notification"}).Inc()
			transfer(ctx, uploaded{
				Key: key,
				Ext: toExtension(key),
			}, minioClient, logger)
			// transfer is sync and errors are fatal so we can ack here
			watcher.Ack(ctx, bucket.TransferOk, &notificationInfo)
		}
	}

	logger.Error("Listener exited without an error, or we failed to handle an error")
	return nil
}

type OnHttp struct {
	handler   http.Handler
	callbacks []func()
}

func NewOnHttp(handler http.Handler) *OnHttp {
	o := &OnHttp{}
	o.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		for _, c := range o.callbacks {
			c()
		}
	})
	return o
}

func (o *OnHttp) AddCallbackAfterResponse(callback func()) {
	o.callbacks = append(o.callbacks, callback)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	onMetrics := NewOnHttp(promhttp.Handler())
	if batchmetrics && !batch {
		logger.Fatal("batchmetrics without batch")
	}
	http.Handle("/metrics", onMetrics.handler)
	go func() {
		logger.Info("Starting /metrics server", zap.String("bound", metrics))
		err := http.ListenAndServe(metrics, nil)
		if err != nil {
			logger.Fatal("Failed to start metrics server", zap.Error(err))
		}
	}()

	indexNext = index.New()
	if indexWrite && !batch {
		logger.Fatal("index only allowed in batch mode, TBD when to serialize in watch mode")
	}

	for {
		err := mainMinio(ctx, logger)
		if err != nil {
			// Do we need backoff here? Maybe not while we're so specific about which error that triggers re-run.
			if restartDelay != 0 {
				logger.Info("Re-running handler", zap.Duration("delay", restartDelay), zap.Error(err))
				time.Sleep(restartDelay)
			}
		} else if batch {
			logger.Info("Batch mode completed")
			if batchmetrics {
				done := false
				onMetrics.AddCallbackAfterResponse(func() {
					done = true
				})
				for start := time.Now(); time.Since(start) < batchmetricsWaitMax; {
					time.Sleep(time.Duration(time.Millisecond * 100))
					if done {
						logger.Info("Exiting on batch mode final metrics scrape")
						os.Exit(0)
					}
				}
				logger.Error("Failed to detect a metrics scrape", zap.Duration("within", batchmetricsWaitMax))
				os.Exit(2)
			}
			os.Exit(0)
		} else {
			logger.Fatal("Unexpectedly exited without an error")
		}
	}
}
