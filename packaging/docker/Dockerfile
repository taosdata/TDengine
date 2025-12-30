FROM tdengine/tdengine-tsdb-base:1.0 AS base

ARG pkgFile

COPY ${pkgFile} /tmp/

RUN TAOS_INSTALL_DIR=/tmp/tdengine-installer && \
    TAOS_BIN_DIR=/usr/local/taos/bin && \
    TAOS_DRIVER_DIR=/usr/local/taos/driver && \
    mkdir -p "${TAOS_INSTALL_DIR}" && \
    tar -zxf /tmp/"${pkgFile}" --strip-components=1 --directory="${TAOS_INSTALL_DIR}" && \
    # install TDengine TSDB OSS/Enterprise without interaction
    cd "${TAOS_INSTALL_DIR}" && ./install.sh -e no && \
    ln -sf "${TAOS_DRIVER_DIR}"/libtaos.so.*       "${TAOS_DRIVER_DIR}"/libtaos.so && \
    ln -sf "${TAOS_DRIVER_DIR}"/libtaosws.so.*     "${TAOS_DRIVER_DIR}"/libtaosws.so && \
    ln -sf "${TAOS_DRIVER_DIR}"/libtaosnative.so.* "${TAOS_DRIVER_DIR}"/libtaosnative.so && \
    rm -rf "${TAOS_BIN_DIR}"/taosinspect \
           "${TAOS_BIN_DIR}"/tdengine-datasource.zip* \
           "${TAOS_BIN_DIR}"/*.sh && \
    rm -rf "${TAOS_DRIVER_DIR}"/libjemalloc* \
           "${TAOS_DRIVER_DIR}"/libtcmalloc*

FROM tdengine/tdengine-tsdb-base:1.0

COPY --from=base /etc/taos/ /etc/taos/
COPY --from=base /usr/local/taos/bin/taos* /usr/local/bin/
COPY --from=base /usr/local/taos/driver/   /usr/local/lib/
COPY --from=base /usr/local/taos/include/  /usr/include/

COPY bin/entrypoint.sh /usr/bin/

ENV TAOS_QUERY_USE_MEMORY_POOL=0

RUN mkdir /docker-entrypoint-initdb.d && \
    ldconfig

VOLUME ["/var/lib/taos", "/var/log/taos"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/bin/entrypoint.sh"]

# expose ports
# 6030 taosd
# 6041 taosAdapter
# 6043 taosKeeper
# 6060 taosExplorer
EXPOSE 6030 6041 6043 6060