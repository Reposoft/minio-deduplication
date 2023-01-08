package bucket

import (
	"context"

	"github.com/minio/minio-go/v7/pkg/notification"
)

type InboxWatcher struct {
	Uploads <-chan notification.Info
	Ack     func(context.Context, TransferResult, *notification.Info)
}
