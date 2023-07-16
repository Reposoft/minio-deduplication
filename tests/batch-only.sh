#!/bin/bash
set -eEo pipefail

[ "$TESTS_DISABLED" = "true" ] && echo "Tests disabled through env TESTS_DISABLED=true" && exit 0

function onerr {
  after-all.sh
}

trap onerr ERR


echo "Batch test here"


echo "_____ batch test executed _____"
after-all.sh
echo "_____         ok         _____"
