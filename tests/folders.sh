#!/bin/bash
set -eo pipefail
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
cat "$one.headers" | grep 'x-amz-meta-uploaddir: myproject/'
#cat "$one.headers" | grep 'x-amz-meta-uploaddir: myproject/$'
#cat "$one.headers" | grep 'x-amz-meta-uploaddir: myproject/\>'

curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: Same Same' \
  "http://minio0:9000/bucket.write/myproject-still/$one.BAK"
sleep 1
# note this gotcha if the filename extension has changed
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.bak | tee "$one.headers1"
cat "$one.headers1" | grep 'Content-Disposition: attachment; filename=package.json.BAK'
cat "$one.headers1" | grep 'x-amz-meta-uploaddir: myproject-still/'

curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: Same Same' \
  "http://minio0:9000/bucket.write/myproject/BACKUP/$one"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.json | tee "$one.headers2"
cat "$one.headers2" | grep 'Content-Disposition: attachment; filename=package.json'
cat "$one.headers2" | grep 'x-amz-meta-uploaddir: myproject/; myproject/BACKUP/'

curl -f -v --retry 3 -T "$one" \
  -H 'x-amz-meta-note: Same Same backup 2' \
  "http://minio0:9000/bucket.write/other%3B%20project/$one"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.json | tee "$one.headers3"
cat "$one.headers3" | grep 'Content-Disposition: attachment; filename=package.json'
cat "$one.headers3" | grep 'x-amz-meta-uploaddir: myproject/; myproject/BACKUP/; other%3B project/'
