FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com"

RUN apt update && apt install -y wget ca-certificates && rm -rf /var/cache/apt/*

ENV TAOS_VERSION=3.0.7.1

RUN wget -O /tmp/client.tar.gz https://www.taosdata.com/assets-download/3.0/TDengine-client-${TAOS_VERSION}-Linux-x64.tar.gz \
  && cd /tmp/ && tar xvf /tmp/client.tar.gz \
	&& cd TDengine-client-${TAOS_VERSION} \
	&& ./install_client.sh \
	&& rm -rf /tmp/TDengine-client-${TAOS_VERSION} \
	&& rm -rf /tmp/client.tar.gz

COPY ./target/release/taosx /usr/bin/taosx

WORKDIR /data/taosx

RUN ln -sf /data/taosx /usr/local/taosx

VOLUME /data/taosx/
EXPOSE 6050

CMD ["/usr/bin/taosx", "serve", "-v"]
