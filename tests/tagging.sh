#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

# https://github.com/minio/minio/pull/8754
# https://github.com/minio/minio-go/pull/1206
# https://godoc.org/gopkg.in/minio/minio-go.v6#Client.GetObjectTagging
# https://godoc.org/gopkg.in/minio/minio-go.v6#Client.PutObjectTagging

echo "What about object tagging?"
