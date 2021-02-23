#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="encodingtest.txt"
echo "Any content" > "$name"

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%23bracket.txt"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
mc --no-color stat --json minio0/bucket.read/$dir$hash.txt | jq '.'
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition'

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%25percent.txt"
