FROM golang:1.17.2 as builder
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com"

WORKDIR /usr/src/webhook
COPY ./ /usr/src/webhook/
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod tidy && go build

FROM ubuntu:20.04
COPY --from=builder /usr/src/webhook/webhook /usr/local/bin/
VOLUME [ "/etc/taos/" ]
EXPOSE 9010
CMD ["webhook"]
