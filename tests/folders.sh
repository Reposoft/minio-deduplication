#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

one="package.json"
echo "{}" > "$one"
curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: My First' \
  "http://minio0:9000/bucket.write/myproject/$one"

two="index.js"
echo "console.log('test');" > "$two"
curl -f -v --retry 3 -T "$two" \
  -H 'x-amz-meta-note: Same Same' \
  "http://minio0:9000/bucket.write/myproject/$two"

hash=$(sha256sum "$one" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.json | tee "$one.headers"

echo "That written files belonged to the same folder name could be helpful, let's set a header"
cat "$one.headers" | grep 'X-Amz-Meta-Writepath: '

curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: Same Same' \
  "http://minio0:9000/bucket.write/myproject/$one.BAK"

# Let's assume that there is some kind of indexing, so use the latest meta
