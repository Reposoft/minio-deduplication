FROM golang:1.14.9-alpine3.12@sha256:050b2322a11b7e6fbde31a9c95b3b090297a47beedaf992da787ee84b3c35ad6

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
