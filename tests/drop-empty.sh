#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

name="Some random file.txt"
echo -n "" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-my-test: My meta' \
  "http://minio0:9000/bucket.write/original/dir/$name"
sleep $ACCEPTABLE_TRANSFER_DELAY
hash=$(sha256sum "$name" | cut -d' ' -f1)
[ $hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" ] || exit 1
dir=${hash:0:2}/${hash:2:2}/
! mc --no-color stat --json minio0/bucket.read/$dir$hash.txt 2>/dev/null || false
