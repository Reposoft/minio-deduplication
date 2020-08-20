#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="party.svg"
echo "<svg/>" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My First' \
  "http://minio0:9000/bucket.write/$name"

# This was to debug, but it actually solved a test failure. Probably because it's a wait.
mc ls minio0/bucket.read

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.svg | tee "$name.1.headers"
cat "$name.1.headers" | grep 'x-amz-meta-revision: My First'

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My Identical' \
  "http://minio0:9000/bucket.write/$name"

cat "$name.1.headers" | grep 'Last-Modified:'
for i in 1 2 3 4 5; do
  curl -f -I -s http://minio0:9000/bucket.read/$dir$hash.svg | tee "$name.2.headers"
  grep 'My Identical' "$name.2.headers" && break
done
cat "$name.2.headers" | grep 'x-amz-meta-revision: My Identical'
cat "$name.2.headers" | grep 'Last-Modified:'

echo "<svg />" > "$name"
curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My Revised' \
  "http://minio0:9000/bucket.write/$name"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.svg | tee "$name.3.headers"
cat "$name.3.headers" | grep 'x-amz-meta-revision: My Revised'
