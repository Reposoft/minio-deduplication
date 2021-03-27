FROM golang:1.16.2@sha256:3b49126489462d4639157174691b6133df4328b135b1583be0377a4a5a8e4911

WORKDIR /workspace/source

COPY go.* ./
RUN go mod download

COPY . .

# See test.sh
#RUN go test

RUN sed -i 's/zap.NewDevelopment()/zap.NewProduction()/' main.go

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -ldflags '-w -extldflags "-static"'

FROM gcr.io/distroless/base:nonroot@sha256:d2655108279c251a02894b108941dba005817cb2e662f59bdc77ff900bfe9869

COPY --from=0 /workspace/source/v1 /usr/local/bin/v1

ENTRYPOINT ["v1"]
