FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.17.6-bullseye

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

# See test.sh
#RUN go test

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

ARG TARGETOS TARGETARCH
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH \
  CGO_ENABLED=0 \
  go build -ldflags '-w -extldflags "-static"'

FROM --platform=${TARGETPLATFORM:-linux/amd64} gcr.io/distroless/static-debian11:nonroot

COPY --from=0 /workspace/source/v1 /usr/local/bin/v1

ENTRYPOINT ["v1"]
