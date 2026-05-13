FROM ubuntu:22.04
LABEL maintainer="Linhe Huo <linhe.huo@gmail.com>"

ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /tini

RUN apt update \
  && apt install -y wget curl jq sqlite3 openjdk-18-jre ca-certificates \
  && apt-get install -y locales tzdata netcat \
  && locale-gen en_US.UTF-8 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/ \
  && rm -rf /var/cache/apt/

COPY ./release/TDengine-enterprise /root/TDengine-enterprise
COPY ./startup.sh /root/startup.sh

RUN chmod +x /tini \
  && chmod +x /root/startup.sh \
  && cd /root/TDengine-enterprise \
  && HOSTNAME=localhost /bin/bash install.sh -e no \
  && rm -rf ../TDengine-enterprise

COPY ./release/taosx /usr/bin/taosx
COPY ./release/taos-explorer /usr/bin/taos-explorer
COPY ./release/plugins/taosx-influxdb.jar /usr/local/taos/plugins/influxdb/taosx-influxdb.jar
COPY ./release/plugins/taosx-opentsdb.jar /usr/local/taos/plugins/opentsdb/taosx-opentsdb.jar
COPY ./release/plugins/taosx-opc /usr/local/taos/plugins/opc/taosx-opc

ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

VOLUME [ "/etc/taos", "/var/lib/taos", "/var/log/taos", "/corefile" ]
EXPOSE 6030
EXPOSE 6041
EXPOSE 6043
EXPOSE 6050
EXPOSE 6055
EXPOSE 6060

ENTRYPOINT ["/tini", "--", "/root/startup.sh"]
