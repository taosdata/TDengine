FROM ubuntu:22.04
LABEL maintainer "Linhe Huo <linhe.huo@gmail.com>"

ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /tini

COPY ./release/taosx-agent /usr/bin/taosx-agent
COPY ./release/plugins/taosx-influxdb.jar /taosx/plugins/influxdb/taosx-influxdb.jar
COPY ./release/plugins/taosx-opentsdb.jar /taosx/plugins/opentsdb/taosx-opentsdb.jar
COPY ./release/plugins/taosx-opc /taosx/plugins/opc/taosx-opc

RUN chmod +x /tini \
  && apt update \
  && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/ \
  && rm -rf /var/cache/apt/

VOLUME /data/taosx/

ENV TAOSX_AGENT_PLUGINS_HOME=/taosx/plugins/
ENV TAOSX_AGENT_DATA_DIR=/data/taosx/
ENV TAOSX_AGENT_LOGS_HOME=/data/taosx/log/
ENV TAOSX_AGENT_CONFIG=/data/taosx/config/agent.toml

ENTRYPOINT ["/tini", "--"]
CMD /usr/bin/taosx-agent -c ${TAOSX_AGENT_CONFIG}
