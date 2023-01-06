#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

name="METADATA.md"
echo "# Metadata mania" > "$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-my-test: My meta' \
  "http://minio0:9000/bucket.write/original/dir/$name"

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq '.'
[ "$(mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq -r '.metadata."X-Amz-Meta-My-Test"')" = "My meta" ]
[ "$(mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq -r '.metadata."X-Amz-Meta-Uploadpaths"')" = "original/dir/$name" ]

# TODO ignore if writing a previously tracked path again
# curl -f -v --retry 3 -T "$name" \
#   "http://minio0:9000/bucket.write/original/dir/$name"

curl -f -v --retry 3 -T "$name" \
  -H 'x-amz-meta-my-test: My meta' \
  "http://minio0:9000/bucket.write/other/upload/other%3B%20$name"
mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq -r '.metadata'
[ "$(mc --no-color stat --json minio0/bucket.read/$dir$hash.md | jq -r '.metadata."X-Amz-Meta-Uploadpaths"')" = "original/dir/$name; other/upload/other%3B $name" ]
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.md | tee "$name.headers"
cat "$name.headers" | grep "Content-Disposition: attachment; filename=\"other; METADATA.md\""
