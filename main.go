package main

import (
	"io"
	"crypto/sha256"
	"flag"
	"fmt"
	"path/filepath"
	
	"github.com/minio/minio-go/v6"

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
)

func init() {
	flag.StringVar(&inbox, "inbox", "", "Uploads bucket")
	flag.StringVar(&archive, "archive", "", "archive bucket")
	flag.StringVar(&host, "host", "", "minio host")
	flag.BoolVar(&secure, "secure", true, "https")
	flag.StringVar(&accesskey, "accesskey", "", "access key")
	flag.StringVar(&secretkey, "secretkey", "", "secret key")
	flag.Parse()
}

func transfer(blob uploaded, minioClient *minio.Client, logger *zap.Logger) {
	object, err := minioClient.GetObject(inbox, blob.Key, minio.GetObjectOptions{})
	if err != nil {
		logger.Fatal("Failed to read object",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox))
	}
	hasher := sha256.New()
	defer object.Close()
	if _, err := io.Copy(hasher, object); err != nil {
		logger.Fatal("Failed to read object to checksum",
			zap.String("key", blob.Key),
			zap.String("bucket", inbox),
		)
	}
	sha256hex := fmt.Sprintf("%x", hasher.Sum(nil))
	logger.Debug("SHA256", zap.String("hex", sha256hex))
	write := fmt.Sprintf("%s/%s%s", archive, sha256hex, blob.Ext)
	logger.Info("TODO transfer",
		zap.String("key", blob.Key),
		zap.String("write", write),
	)

	src := minio.NewSourceInfo(inbox, blob.Key, nil)

	dst, err := minio.NewDestinationInfo(archive, fmt.Sprintf("%s%s", sha256hex, blob.Ext), nil, nil)
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

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	minioClient, err := minio.New(host, accesskey, secretkey, secure)
	if err != nil {
		logger.Fatal("Failed to set up minio client",
			zap.Error(err),
		)
	}

	// minioClient.TraceOn(os.Stderr)

	// TODO list existing soure blobs and transfer immediately

	// Create a done channel to control 'ListenBucketNotification' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// Listen for bucket notifications on "mybucket" filtered by prefix, suffix and events.
	for notificationInfo := range minioClient.ListenBucketNotification(inbox, "", "", []string{
		"s3:ObjectCreated:*",
	}, doneCh) {
		if notificationInfo.Err != nil {
			logger.Fatal("Notification error",
				zap.Error(notificationInfo.Err),
			)
		}
		for _, record := range notificationInfo.Records {
			logger.Info("Notification",
				zap.Any("record", record),
			)
			key := record.S3.Object.Key // Note that slashes are urlencoded
			transfer(uploaded{
				Key: key,
				Ext: filepath.Ext(key),
			}, minioClient, logger)
		}
	}
}
