#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="README.md"
echo "# Good stuff" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-my-notes: Amazing story by me' \
  "http://minio0:9000/bucket.write/$name"

mc --no-color stat --json minio0/bucket.write/$name | grep '"X-Amz-Meta-My-Notes":"Amazing story by me"'

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.md | tee "$name.headers"
cat "$name.headers" | grep 'Amazing'
