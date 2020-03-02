#!/bin/sh
set -e
[ -z "$DEBUG" ] || set -x

curl -f --retry 3 --retry-connrefused http://app0:2112/metrics > /dev/null

retrywait=0
until mc --no-color config host add minio0 http://minio0:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY \
     || [ $retrywait -eq 5 ]; do sleep $(( retrywait++ )); done

mc --no-color mb minio0/bucket.write
mc --no-color mb minio0/bucket.read

echo "Test1" > test1.txt
mc --no-color cp test1.txt minio0/bucket.write/

expected=minio0/bucket.read/$(sha256sum test1.txt | cut -d' ' -f1).txt

retrywait=0
until mc --no-color stat "$expected" \
     || [ $retrywait -eq 5 ]; do sleep $(( retrywait++ )); done

mc --no-color ls minio0/bucket.write
mc --no-color ls minio0/bucket.read

# TODO Content-Disposition
# mime.FormatMediaType("attachment", map[string]string{"filename": "数据统计.png"})

# TODO dir sharding option 0-3

# TODO X-Custom-Header
