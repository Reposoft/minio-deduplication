#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

echo "_____ [after-all] metrics         _____"
curl -s http://app0:2112/metrics | tee metrics.txt | grep blobs_

echo "_____ [after-all] minio metrics   _____"
curl -s http://minio0:9000/minio/v2/metrics/cluster | grep 'requests_total{'

echo "_____ [after-all] read bucket ls  _____"
mc ls -r minio0/bucket.read

echo "_____ [after-all] write bucket ls _____"
mc ls -r minio0/bucket.write

echo "_____ [after-all] topic contents  _____"
rpk topic consume --brokers kafka:9092 minio-events -p 0 -o :end -f '%o %k: %v\n' || true

echo "_____ [after-all] consumer group  _____"
rpk group list --brokers kafka:9092 || true
rpk group describe --brokers kafka:9092 app0 || true
