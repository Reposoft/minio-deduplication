#!/bin/bash
set -eEo pipefail

[ "$TESTS_DISABLED" = "true" ] && echo "Tests disabled through env TESTS_DISABLED=true" && exit 0

function onerr {
  CALLER="$(caller)"
  LINE=$1
  echo "^^^^^ ERR $CALLER line $LINE ^^^^^"
  after-all.sh || true
  echo "_____ ERR $CALLER line $LINE _____"
}

trap 'onerr $LINENO' ERR

echo "_____ batch test start _____"
sleep 1

[ -n "$RETRIES" ] || RETRIES=3

curl -f --retry 3 --retry-connrefused http://app0:2112/metrics > /dev/null

retrywait=0
until mc --no-color config host add minio0 http://minio0:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; \
  do [ $(( retrywait++ )) -lt 30 ]; sleep 1; done

mc --no-color mb minio0/bucket.write

mc event add minio0/bucket.write arn:minio:sqs::_:kafka --event put || \
  [ -z "$REQUIRE_KAFKA" ] || exit 1

echo "Added before watch" > test1.txt
mc --no-color cp --attr "Content-Type=text/testing1" test1.txt minio0/bucket.write/
mc --no-color stat --json minio0/bucket.write/test1.txt | grep '"Content-Type":"text/testing1"'

mc --no-color mb minio0/bucket.read

hash=$(sha256sum test1.txt | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
expected=minio0/bucket.read/$dir$hash.txt
retrywait=0
until mc --no-color stat "$expected"; \
  do [ $(( retrywait++ )) -lt $RETRIES ]; sleep $ACCEPTABLE_TRANSFER_DELAY; done
mc --no-color stat --json "$expected" | grep '"Content-Type":"text/testing1"'

mc --no-color ls minio0/bucket.write
mc --no-color ls minio0/bucket.read

curl -s http://app0:2112/metrics | tee metrics.txt | grep blobs_

echo "_____ batch test executed _____"
after-all.sh
echo "_____         ok         _____"
