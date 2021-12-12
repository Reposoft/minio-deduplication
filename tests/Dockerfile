FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2021-11-16T20-37-36Z

RUN microdnf install jq --nodocs

COPY *.sh /usr/local/bin/
