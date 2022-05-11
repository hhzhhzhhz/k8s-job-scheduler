FROM golang:1.17

WORKDIR /app

COPY . .

RUN go build -v -mod=vendor -o /app/job-scheduler-server ./cmd/server
RUN go build -v -mod=vendor -o /app/job-scheduler-exec ./cmd/client

COPY etc/config.properties /app/config.properties
COPY docker-entrypoint.sh /app

CMD ./docker-entrypoint.sh start_server