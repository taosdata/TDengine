FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com"

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN apt update \
  && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
  && rm -rf /var/cache/apt/*

ENV TAOS_VERSION=3.1.1.0

RUN wget -O /tmp/client.tar.gz https://www.taosdata.com/assets-download/3.0/TDengine-client-${TAOS_VERSION}-Linux-x64.tar.gz \
  && cd /tmp/ && tar xvf /tmp/client.tar.gz \
	&& cd TDengine-client-${TAOS_VERSION} \
	&& ./install_client.sh \
	&& rm -rf /tmp/TDengine-client-${TAOS_VERSION} \
	&& rm -rf /tmp/client.tar.gz

ENTRYPOINT ["/tini", "--"]

ENV PLUGINS_HOME=/taosx/plugins/

ADD ./plugins/influxdb/target/taosx-influxdb.jar /taosx/plugins/influxdb/
ADD ./plugins/opentsdb/target/taosx-opentsdb.jar /taosx/plugins/opentsdb/
ADD ./plugins/opc/taosx-opc /taosx/plugins/opc
ADD ./plugins/mqtt/dist/taosx-mqtt /taosx/plugins/mqtt
ADD ./target/release/taosx /usr/bin/taosx

WORKDIR /data/taosx

RUN ln -sf /data/taosx /usr/local/taosx

VOLUME /data/taosx/
EXPOSE 6050
EXPOSE 6055

CMD ["/usr/bin/taosx", "serve", "-v"]
