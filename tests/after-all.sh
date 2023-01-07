#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

echo "_____ [after-all] metrics         _____"
curl -s http://app0:2112/metrics | tee metrics.txt | grep blobs_

echo "_____ [after-all] read bucket ls  _____"
mc ls -r minio0/bucket.read

echo "_____ [after-all] write bucket ls _____"
mc ls -r minio0/bucket.write
