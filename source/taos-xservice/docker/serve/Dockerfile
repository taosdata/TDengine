FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com>"

ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /tini

COPY ./release/tdengine-tsdb-enterprise-client /root/tdengine-tsdb-enterprise-client
COPY ./release/taosx /usr/bin/taosx
COPY ./release/plugins/taosx-influxdb.jar /taosx/plugins/influxdb/taosx-influxdb.jar
COPY ./release/plugins/taosx-opentsdb.jar /taosx/plugins/opentsdb/taosx-opentsdb.jar
COPY ./release/plugins/taosx-opc /taosx/plugins/opc/taosx-opc

RUN chmod +x /tini \
    && apt update \
    && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/ \
    && rm -rf /var/cache/apt/ \
    && cd /root/tdengine-tsdb-enterprise-client \
    && /bin/bash install_client.sh \
    && rm -rf ../tdengine-tsdb-enterprise-client

VOLUME /data/taosx/

ENV TAOSX_PLUGINS_HOME=/taosx/plugins/
ENV TAOSX_DATA_DIR=/data/taosx/
ENV TAOSX_LOGS_HOME=/data/taosx/log/
ENV TAOSX_CONFIG=/data/taosx/config/taosx.toml

EXPOSE 6050
EXPOSE 6055

ENTRYPOINT ["/tini", "--"]
CMD /usr/bin/taosx serve -c ${TAOSX_CONFIG}
