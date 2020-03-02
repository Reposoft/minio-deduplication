#!/bin/bash

# What we think Docker Hub is running
docker-compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from sut sut
RESULT=$?

echo "Result: $RESULT, logs:"
docker-compose -f docker-compose.test.yml logs

docker-compose -f docker-compose.test.yml down --remove-orphans -v

exit $RESULT
