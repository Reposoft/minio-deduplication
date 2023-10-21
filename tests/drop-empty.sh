#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

curl -f -v --retry 3 -X PUT \
  -H "Content-Type: text/plain" \
  -H 'x-amz-meta-my-test: My meta' \
  "http://minio0:9000/bucket.write/original/dir/somefile.txt"
sleep $ACCEPTABLE_TRANSFER_DELAY
hash="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
dir=${hash:0:2}/${hash:2:2}/
! mc --no-color stat --json minio0/bucket.read/$dir$hash.txt 2>/dev/null || false

# obviously hard to validate in an integration test, but look for "Dropped empty file" in app logs
