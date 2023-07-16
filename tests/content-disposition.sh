#!/bin/bash
set -eo pipefail
[ -z "$DEBUG" ] || set -x

mc --no-color anonymous set upload minio0/bucket.write

mc --no-color anonymous set download minio0/bucket.read

name="My blob (1).txt"
echo "My blob with a name" > "$name"

curl -f -v --retry 3 -T "$name" "http://minio0:9000/bucket.write/$(echo -n $name | sed 's/ /%20/g' )"

# This was to debug, but it actually solved a test failure. Probably because it's a wait.
mc ls minio0/bucket.read

hash=$(sha256sum "$name" | cut -d' ' -f1)
dir=${hash:0:2}/${hash:2:2}/
curl -f -v --retry 3 -I http://minio0:9000/bucket.read/$dir$hash.txt | tee "$name.headers"
cat "$name.headers" | grep 'Content-Disposition: attachment; filename="My blob (1).txt"'
