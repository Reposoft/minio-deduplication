#!/bin/bash
set -eEo pipefail

[ "$TESTS_DISABLED" = "true" ] && echo "Tests disabled through env TESTS_DISABLED=true" && exit 0

function onerr {
  after-all.sh
}

trap onerr ERR

sleep 1

# basic-flow also does bucket setup for the other tests
basic-flow.sh
# content-disposition sets bucket policies
content-disposition.sh
# # remaining tests should not depend on each other
custom-metadata.sh
deduplication.sh
extensions.sh
folders.sh
tagging.sh
zero-padded-checksums.sh
filename-encoding.sh
upload-path-tracking.sh

echo "_____ all tests executed _____"
after-all.sh
echo "_____         ok         _____"
