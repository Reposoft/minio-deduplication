#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

mc --no-color policy set upload minio0/bucket.write

mc --no-color policy set download minio0/bucket.read

#name="My blob (1).txt"
name="Myblob.txt"
echo "My blob" > "$name"

curl -f -v --retry 3 -T "$name" "http://minio0:9000/bucket.write/$name"

curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$(sha256sum "$name" | cut -d' ' -f1).txt | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition'

# TODO Content-Disposition
# mime.FormatMediaType("attachment", map[string]string{"filename": "数据统计.png"})
