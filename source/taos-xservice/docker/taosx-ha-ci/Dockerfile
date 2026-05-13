ARG BASE_IMAGE_TAG=taosx-ha-ci-base:latest
FROM ${BASE_IMAGE_TAG}
LABEL maintainer="TaosX CI Team"

# Download and install TDengine
ARG TDENGINE_VERSION
ARG TDENGINE_DOWNLOAD_HOST
RUN MAJOR_MINOR=$(echo ${TDENGINE_VERSION} | cut -d'.' -f1,2) \
    && wget -O /tmp/tdengine.tar.gz \
    http://${TDENGINE_DOWNLOAD_HOST}/data/nas/TDengine/${MAJOR_MINOR}/v${TDENGINE_VERSION}/enterprise/tdengine-tsdb-enterprise-${TDENGINE_VERSION}-linux-x64.tar.gz \
    && cd /tmp/ \
    && tar xvf tdengine.tar.gz \
    && cd tdengine-tsdb-enterprise-${TDENGINE_VERSION} \
    && ./install.sh -e no \
    && rm -rf /tmp/tdengine*

# Configure TDengine to use localhost as FQDN (must be done after install)
RUN echo "fqdn localhost" >> /etc/taos/taos.cfg \
    && echo "serverPort 6030" >> /etc/taos/taos.cfg

# Copy instrumented binaries (built with llvm-cov)
COPY taosx /usr/bin/taosx
COPY taosx-agent /usr/bin/taosx-agent
COPY taos-explorer /usr/bin/taos-explorer
COPY xnoded /usr/bin/xnoded

# Copy explorer dist files so taos-explorer can serve UI via EXPLORER_ASSETS.
COPY explorer-dist/ /explorer-dist/

# Copy plugins
RUN mkdir -p /usr/local/taos/plugins/influxdb \
    /usr/local/taos/plugins/opentsdb \
    /usr/local/taos/plugins/opc
COPY plugins/influxdb/taosx-influxdb.jar /usr/local/taos/plugins/influxdb/
COPY plugins/opentsdb/taosx-opentsdb.jar /usr/local/taos/plugins/opentsdb/
COPY plugins/opc/taosx-opc /usr/local/taos/plugins/opc/

# Copy startup script
COPY startup.sh /startup.sh
RUN chmod +x /startup.sh

# Copy coverage conversion script
COPY convert-coverage.sh /usr/local/bin/convert-coverage.sh
RUN chmod +x /usr/local/bin/convert-coverage.sh

ENV PLUGINS_HOME=/usr/local/taos/plugins/

VOLUME ["/etc/taos", "/var/lib/taos", "/var/log/taos", "/corefile"]
EXPOSE 6030 6041 6043 6050 6055 6060

ENTRYPOINT ["/startup.sh"]
