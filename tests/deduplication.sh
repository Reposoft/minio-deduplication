#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="party.svg"
echo "<svg/>" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My First' \
  "http://minio0:9000/bucket.write/$name"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.svg | tee "$name.1.headers"
cat "$name.1.headers" | grep 'X-Amz-Meta-Revision: My First'

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My Identical' \
  "http://minio0:9000/bucket.write/$name"

# TODO clarify that with a different extension there will be a new file even if content is identical
# ... but ... lowercakse? jpg/jpeg?

# TODO do we expect metadata to be updated?
for i in 1 2 3; do
  curl -f -v -I -s http://minio0:9000/bucket.read/$dir$hash.svg 2>&1 \
    | grep 'X-Amz-Meta-Revision:\|Last-Modified:'
  sleep 1
done

echo "<svg />" > "$name"
curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-revision: My Revised' \
  "http://minio0:9000/bucket.write/$name"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.svg | tee "$name.3.headers"
cat "$name.3.headers" | grep 'X-Amz-Meta-Revision: My Revised'
