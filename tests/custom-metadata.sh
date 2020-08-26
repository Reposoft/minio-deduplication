#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="README.md"
echo "# Good stuff" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-my-caption: Amazing story by me' \
  "http://minio0:9000/bucket.write/$name"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq '.'
[ "$(mc --no-color stat --json minio0/bucket.read/$dir$hash.md  | jq -r '.metadata."X-Amz-Meta-My-Caption"')" = "Amazing story by me" ]
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.md | tee "$name.headers"
cat "$name.headers" | grep 'x-amz-meta-my-caption: Amazing story by me'
