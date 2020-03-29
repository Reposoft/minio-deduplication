#!/bin/sh
set -e
# basic-flow also does bucket setup for the other tests
basic-flow.sh
# content-disposition sets bucket policies
content-disposition.sh
# remaining tests should not depend on each other
custom-metadata.sh

echo "_____ all tests executed _____"
