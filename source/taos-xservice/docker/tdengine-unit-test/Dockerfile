# Final unit-test CI image: base image + TDengine enterprise.
# Built once per (TDengine version, Rust version) pair.
# Tagged as: tdengine-unit-test:{TDENGINE_VERSION}-rust-{RUST_VERSION}
ARG BASE_IMAGE_TAG=tdengine-unit-test-base:latest
FROM ${BASE_IMAGE_TAG}
LABEL maintainer="TaosX CI Team"

# Download and install TDengine enterprise
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

# Use localhost as FQDN so tests can connect via taos://localhost and taos+ws://localhost:6041
RUN echo "fqdn localhost" >> /etc/taos/taos.cfg \
    && echo "serverPort 6030" >> /etc/taos/taos.cfg \
    && echo "supportVnodes 256" >> /etc/taos/taos.cfg

COPY run_tests.sh /run_tests.sh
RUN chmod +x /run_tests.sh

EXPOSE 6030 6041

ENTRYPOINT ["/run_tests.sh"]
