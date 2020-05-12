package main

import (
	"io"
	"crypto/sha256"
	"flag"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/minio/minio-go/v6"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"go.uber.org/zap"
)

type uploaded struct {
	Key string
	Ext string
}

var (
	inbox string
	archive string
	host string
	secure bool
	accesskey string
	secretkey string
	metrics string
	trace bool
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

func assertBucketExists(name string, minioClient *minio.Client, logger *zap.Logger) {
	check := func() error {
		found, err := minioClient.BucketExists(name)
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

func transfer(blob uploaded, minioClient *minio.Client, logger *zap.Logger) {
	objectInfo, err := minioClient.StatObject(inbox, blob.Key, minio.StatObjectOptions{})
	if err != nil {
		logger.Fatal("Failed to stat source object",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
			zap.Error(err),
		)
	}

	object, err := minioClient.GetObject(inbox, blob.Key, minio.GetObjectOptions{})
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

	src := minio.NewSourceInfo(inbox, blob.Key, nil)

	blobDir := sha256hex[0:2] + "/" + sha256hex[2:4] + "/"
	blobName := fmt.Sprintf("%s%s%s", blobDir, sha256hex, blob.Ext)
	downloadName := filepath.Base(blob.Key)

	meta := make(map[string]string)

	existing, err := minioClient.StatObject(archive, blobName, minio.StatObjectOptions{})
	if err != nil {
		if err.Error() == "The specified key does not exist." {
			logger.Debug("Destination path is new", zap.String("key", blobName))
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
	}

	srcMeta := objectInfo.UserMetadata
	for k, v := range srcMeta {
		logger.Info("Transferring meta", zap.String(k, v));
		meta[k] = v
	}

	meta["content-type"] = objectInfo.ContentType
	meta["content-disposition"] = mime.FormatMediaType("attachment", map[string]string{"filename": downloadName})
	uploaddir := filepath.Dir(blob.Key)
	if uploaddir != "." {
		meta["X-Amz-Meta-Uploaddir"] = uploaddir + "/"
	}
	dst, err := minio.NewDestinationInfo(archive, blobName, nil, meta)

	if err != nil {
		logger.Error("Failed to define transfer destination",
			zap.String("key", blob.Key),
			zap.String("archive", archive),
			zap.Error(err),
		)
		return
	}

	// TODO content disposition

	// Copy object call
	err = minioClient.CopyObject(dst, src)
	if err != nil {
		logger.Error("Failed to transfer",
			zap.String("key", blob.Key),
			zap.String("archive", archive),
			zap.Error(err),
		)
		return
	}
}

func toExtension(key string) string {
	ext := strings.ToLower(filepath.Ext(key))
	if ext == ".jpeg" {
		return ".jpg"
	}
	return ext
}

// Will exit on unrecognized errors, but return err on errors we think we can recover from without crashloop
func mainMinio(logger *zap.Logger) error {

	logger.Info("Initializing minio client", zap.String("host", host), zap.Bool("https", secure))
	minioClient, err := minio.New(host, accesskey, secretkey, secure)
	if err != nil {
		logger.Fatal("Failed to set up minio client",
			zap.Error(err),
		)
	}

	if trace {
		minioClient.TraceOn(os.Stderr)
	}

	assertBucketExists(inbox, minioClient, logger)
	assertBucketExists(archive, minioClient, logger)
	logger.Info("Bucket existence confirmed", zap.String("inbox", inbox), zap.String("archive", archive))

	logger.Info("Starting bucket notifications listener")
	doneCh := make(chan struct{})
	defer close(doneCh)
	listenCh := minioClient.ListenBucketNotification(inbox, "", "", []string{
		"s3:ObjectCreated:*",
	}, doneCh)

	logger.Info("Listing existing inbox objects")
	listDoneCh := make(chan struct{})
	defer close(listDoneCh)
	isRecursive := true
	objectCh := minioClient.ListObjectsV2(inbox, "", isRecursive, listDoneCh)
	for object := range objectCh {
		if object.Err != nil {
			logger.Fatal("List object error", zap.Error(object.Err))
		}
		logger.Info("Existing inbox object to be transferred", zap.String("key", object.Key))
		transfer(uploaded{
			Key: object.Key,
			Ext: toExtension(object.Key),
		}, minioClient, logger)
	}

	for notificationInfo := range listenCh {
		if notificationInfo.Err != nil {
			if notificationInfo.Err.Error() == "unexpected end of JSON input" {
				// Can't reliably test this with the current test infra, but we fall back to crashlooping if detection fails.
				// If we get this error without any successful notifications we'll transfer files anyway, per the ListObjects above
				logger.Info("Notification abort, which we think is a timeout",
					zap.Error(notificationInfo.Err),
				)
				return notificationInfo.Err
			}
			logger.Fatal("Notification error",
				zap.Error(notificationInfo.Err),
			)
		}
		for _, record := range notificationInfo.Records {
			logger.Info("Notification",
				zap.Any("record", record),
			)
			key, err := url.QueryUnescape(record.S3.Object.Key)
			if err != nil {
				logger.Fatal("Failed to urldecode notification", zap.String("key", record.S3.Object.Key))
			}
			transfer(uploaded{
				Key: key,
				Ext: toExtension(key),
			}, minioClient, logger)
		}
	}

	logger.Error("Listener exited without an error, or we failed to handle an error")
	return nil
}


func main() {
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
		err := mainMinio(logger)
		if err != nil {
			// Do we need backoff here? Maybe not while we're so specific about which error that triggers re-run.
			logger.Info("Re-running handler", zap.Error(err))
		} else {
			logger.Fatal("Unexpectedly exited without an error")
		}
	}
}
