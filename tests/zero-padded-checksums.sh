#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

# This test is to make sure that we don't trim leading zeroes

name="threezeroes"
echo -n "886" > "$name"

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/$name"

hash="000f21ac06aceb9cdd0575e82d0d85fc39bed0a7a1d71970ba1641666a44f530"
sha256sum "$name"
sha256sum "$name" | cut -d' ' -f1 | grep $hash
dir=${hash:0:2}/${hash:2:2}/
mc ls minio0/bucket.read/
mc --no-color stat --json minio0/bucket.read/$dir$hash | jq '.'
curl -s -o /dev/null -w "%{http_code}" http://minio0:9000/bucket.read/f2/1a/f21ac06aceb9cdd0575e82d0d85fc39bed0a7a1d71970ba1641666a44f530 | grep 404
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash
