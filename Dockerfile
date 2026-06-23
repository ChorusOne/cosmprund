# 1st stage, build app
FROM golang:1.23-alpine as builder

COPY . /app

WORKDIR /app

RUN go build -tags pebbledb -mod readonly -o cosmprund main.go


FROM alpine

COPY --from=builder /app/cosmprund /usr/bin/cosmprund

ENTRYPOINT [ "/usr/bin/cosmprund" ]
