#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

name="encodingtest.txt"
echo "Any content" > "$name"

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%23bracket.txt"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
mc --no-color stat --json minio0/bucket.read/$dir$hash.txt | jq '.'
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers1"
cat "$name.headers1" | grep 'Content-Disposition' | grep 'filename=with#bracket.txt'

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%25percent.txt"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers2"
diff -u "$name.headers1" "$name.headers2" || true
cat "$name.headers2" | grep 'Content-Disposition' | grep 'filename=with%percent.txt'

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%22quote.txt"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers3"
cat "$name.headers3" | grep 'Content-Disposition' #| grep "filename=\"with\\\"quote.txt\""

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%27singlequote.txt"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers4"
cat "$name.headers4" | grep 'Content-Disposition'

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%3Bsemicolon.txt"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers5"
cat "$name.headers5" | grep 'Content-Disposition' | grep "filename=\"with;semicolon.txt\""

curl -f -v --retry 3 -T "$name" \
  "http://minio0:9000/bucket.write/with%3Acolon.txt"
sleep 1
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers5"
cat "$name.headers5" | grep 'Content-Disposition' | grep "filename=\"with:colon.txt\""
