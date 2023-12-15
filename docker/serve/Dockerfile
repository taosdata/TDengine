FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com>"

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini

ARG VER_NUMBER
ENV TAOS_VERSION=${VER_NUMBER}

COPY ./release/TDengine-enterprise-client-${TAOS_VERSION}-Linux-x64.tar.gz /root/TDengine-enterprise-client-${TAOS_VERSION}-Linux-x64.tar.gz
COPY ./release/plugins/taosx-influxdb.jar /taosx/plugins/influxdb/taosx-influxdb.jar
COPY ./release/plugins/taosx-opentsdb.jar /taosx/plugins/opentsdb/taosx-opentsdb.jar
COPY ./release/plugins/taosx-opc /taosx/plugins/opc/taosx-opc
COPY ./release/plugins/taosx-mqtt /taosx/plugins/mqtt/taosx-mqtt
COPY ./release/taosx /usr/bin/taosx

RUN chmod +x /tini \
  && apt update \
  && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/ \
  && rm -rf /var/cache/apt/ \
  && cd /root/ && tar xvf /root/TDengine-enterprise-client-${TAOS_VERSION}-Linux-x64.tar.gz \
  && cd TDengine-enterprise-client-${TAOS_VERSION} \
  && /bin/bash install_client.sh \
  && rm -rf /root/TDengine-client-${TAOS_VERSION} \
  && rm -rf /root/TDengine-enterprise-client-${TAOS_VERSION}-Linux-x64.tar.gz

ENV TAOSX_PLUGINS_HOME=/taosx/plugins/
ENV TAOSX_DATA_DIR=/data/taosx/
ENV TAOSX_LOGS_HOME=/data/taosx/log/
ENV TAOSX_CONFIG=/data/taosx/config/taosx.toml

VOLUME /data/taosx/
EXPOSE 6050
EXPOSE 6055

ENTRYPOINT ["/tini", "--"]
CMD /usr/bin/taosx serve -c ${TAOSX_CONFIG}
