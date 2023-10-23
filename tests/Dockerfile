FROM --platform=${TARGETPLATFORM:-linux/amd64} redpandadata/redpanda:v23.2.12@sha256:cd4d56c5859216698dd7083d4f9f1dcaebdbf0a6453f6972520a52f70c2a3f09

FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2023-10-14T01-57-03Z

RUN microdnf install jq --nodocs

COPY --from=0 /usr/bin/rpk /usr/bin/rpk
COPY --from=0 /opt/redpanda/libexec/rpk /opt/redpanda/libexec/rpk

COPY *.sh /usr/local/bin/
