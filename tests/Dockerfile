FROM --platform=${TARGETPLATFORM:-linux/amd64} redpandadata/redpanda:v23.1.13@sha256:5cca0f6ab9f6f932f443182d5587d835d23f6c66ad85127cfe2ccd2c237ff222

FROM --platform=${TARGETPLATFORM:-linux/amd64} minio/mc:RELEASE.2023-07-11T23-30-44Z

RUN microdnf install jq --nodocs

COPY --from=0 /usr/bin/rpk /usr/bin/rpk
COPY --from=0 /opt/redpanda/libexec/rpk /opt/redpanda/libexec/rpk

COPY *.sh /usr/local/bin/
