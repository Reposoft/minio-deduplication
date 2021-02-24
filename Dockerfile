FROM golang:1.16.0@sha256:f3f90f4d30866c1bdae90012b506bd5e557ce27ccd2510ed30a011c44c1affc8

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
