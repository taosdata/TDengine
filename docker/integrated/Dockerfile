FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com>"

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini

ARG VER_NUMBER
ENV TAOS_VERSION=${VER_NUMBER}

COPY ./release/TDengine-enterprise-${TAOS_VERSION}-Linux-x64.tar.gz /root/TDengine-enterprise-${TAOS_VERSION}-Linux-x64.tar.gz
COPY ./release/plugins/taosx-influxdb.jar /taosx/plugins/influxdb/taosx-influxdb.jar
COPY ./release/plugins/taosx-opentsdb.jar /taosx/plugins/opentsdb/taosx-opentsdb.jar
COPY ./release/plugins/taosx-opc /taosx/plugins/opc/taosx-opc
COPY ./release/plugins/taosx-mqtt /taosx/plugins/mqtt/taosx-mqtt
COPY ./release/taosx /usr/bin/taosx
COPY ./release/taos-explorer /usr/bin/taos-explorer
COPY ./startup.sh /root/startup.sh

RUN chmod +x /tini \
  && chmod +x /root/startup.sh \
  && apt update \
  && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
  && apt-get install -y locales tzdata netcat \
  && locale-gen en_US.UTF-8 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/ \
  && rm -rf /var/cache/apt/ \
  && cd /root/ && tar xvf /root/TDengine-enterprise-${TAOS_VERSION}-Linux-x64.tar.gz \
  && cd TDengine-enterprise-${TAOS_VERSION} && rm -rf taosx/ \
  && /bin/bash install.sh -e no \
  && rm -rf /root/TDengine-enterprise-${TAOS_VERSION} \
  && rm -rf /root/TDengine-enterprise-${TAOS_VERSION}-Linux-x64.tar.gz

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

ENV TAOSX_PLUGINS_HOME=/taosx/plugins/
ENV TAOSX_DATA_DIR=/data/taosx/
ENV TAOSX_LOGS_HOME=/data/taosx/log/
ENV TAOSX_CONFIG=/data/taosx/config/taosx.toml
ENV EXPLORER_CONFIG_FILE=/data/taosx/config/explorer.toml

VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile", "/data/taosx" ]
EXPOSE 6030
EXPOSE 6041
EXPOSE 6050
EXPOSE 6055
EXPOSE 6060

ENTRYPOINT ["/tini", "--"]
CMD /usr/bin/sh /root/startup.sh