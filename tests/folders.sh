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

echo "Could be useful to know folder names used for upload, let's set a header:"
cat "$one.headers" | grep 'X-Amz-Meta-Uploaddir: myproject/'
#cat "$one.headers" | grep 'X-Amz-Meta-Uploaddir: myproject/$'
#cat "$one.headers" | grep 'X-Amz-Meta-Uploaddir: myproject/\>'

curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: Same Same' \
  "http://minio0:9000/bucket.write/myproject/$one.BAK"
