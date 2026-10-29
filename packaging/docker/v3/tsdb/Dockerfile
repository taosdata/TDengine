# Copyright (c) TAOS Data, Inc. All rights reserved.
#
# SPDX-License-Identifier: LicenseRef-TAOSData-Commercial
#
# This file is proprietary to TAOS Data, Inc. and may contain confidential
# information. No rights are granted by this file. Use is permitted only
# under a written agreement with TAOS Data, Inc. or an authorized affiliate.
# Unauthorized copying, modification, distribution, or disclosure, in whole
# or in part, by any medium, is strictly prohibited.

FROM tdengine/tsdb-base:1.2 AS base

ARG pkgFile
ARG BUILD_ENTERPRISE=0

COPY ${pkgFile} /tmp/
COPY normalize-legacy-config.sh /tmp/normalize-legacy-config.sh

RUN set -eux; \
    TAOS_INSTALL_DIR=/tmp/tdengine-installer; \
    TAOS_DIR=/usr/local/taos; \
    TAOS_BIN_DIR=${TAOS_DIR}/bin; \
    TAOS_DRIVER_DIR=${TAOS_DIR}/driver; \
    TAOS_PLUGIN_DIR=${TAOS_DIR}/plugins; \
    TAO_MISC_DIR=/tmp/tdengine; \
    \
    mkdir -p "${TAOS_INSTALL_DIR}" "${TAO_MISC_DIR}"; \
    tar -xf /tmp/"${pkgFile}" --strip-components=1 --directory="${TAOS_INSTALL_DIR}"; \
    cd "${TAOS_INSTALL_DIR}"; \
    # Detect installer capabilities, not the package version. Never retry a failed install.
    installer_help=$(./install.sh -h); \
    if printf '%s\n' "${installer_help}" | grep -q -- '--image-build'; then \
        ./install.sh --image-build; \
    else \
        ./install.sh -e no; \
        bash /tmp/normalize-legacy-config.sh /etc/taos "$(hostname)"; \
    fi; \
    # Microsoft Driver 18 resolves resources relative to its final library path.
    # Keep an empty directory for packages that do not include SQL Server FQ.
    mkdir -p "${TAO_MISC_DIR}/share/resources"; \
    if [ -d "${TAOS_DIR}/share/resources" ]; then \
        cp -a "${TAOS_DIR}/share/resources/." "${TAO_MISC_DIR}/share/resources/"; \
    fi; \
    for driver in "${TAOS_DRIVER_DIR}"/libmsodbcsql*.so*; do \
        [ -e "${driver}" ] || continue; \
        test -s "${TAO_MISC_DIR}/share/resources/en_US/msodbcsqlr18.rll" || { \
            echo "Microsoft ODBC driver is bundled but msodbcsqlr18.rll is missing or empty" >&2; \
            exit 1; \
        }; \
    done; \
    # preserve plugins if plugins exist; the JVM-based taosX plugins (influxdb,
    # opentsdb, pspace) are dropped along with the JRE they need -- only the
    # native opc plugin is kept
    if [ "${BUILD_ENTERPRISE}" = "1" ]; then \
        cp -rf "${TAOS_PLUGIN_DIR}" "${TAO_MISC_DIR}"; \
        rm -rf "${TAO_MISC_DIR}"/plugins/influxdb \
               "${TAO_MISC_DIR}"/plugins/opentsdb \
               "${TAO_MISC_DIR}"/plugins/pspace; \
    fi; \
    # create relative symlinks for driver libs so they survive COPY into /usr/local/lib
    cd "${TAOS_DRIVER_DIR}"; \
    ln -sf libtaos.so.*       libtaos.so; \
    ln -sf libtaosws.so.*     libtaosws.so; \
    ln -sf libtaosnative.so.* libtaosnative.so; \
    rm -rf "${TAOS_BIN_DIR}"/taosinspect \
           "${TAOS_BIN_DIR}"/tdengine-datasource.zip* \
           "${TAOS_BIN_DIR}"/*.sh \
           "${TAOS_DRIVER_DIR}"/libjemalloc* \
           "${TAOS_DRIVER_DIR}"/libtcmalloc*

FROM tdengine/tsdb-base:1.2

COPY --from=base /tmp/tdengine/  /usr/local/taos/
COPY --from=base /etc/taos/ /etc/taos/
COPY --from=base /usr/local/taos/bin/* /usr/bin/
COPY --from=base /usr/local/taos/driver/   /usr/local/lib/
COPY --from=base /tmp/tdengine/share/resources/ /usr/local/share/resources/
COPY --from=base /usr/local/taos/include/  /usr/include/

COPY entrypoint.sh taos-check /usr/bin/

ENV TAOS_QUERY_USE_MEMORY_POOL=0

RUN chmod 755 /usr/bin/entrypoint.sh /usr/bin/taos-check; \
    mkdir /docker-entrypoint-initdb.d; \
    ldconfig

VOLUME ["/var/lib/taos", "/var/log/taos"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/bin/entrypoint.sh"]

# expose ports
# 6030 taosd
# 6041 taosAdapter
# 6043 taosKeeper
# 6060 taosExplorer
EXPOSE 6030 6041 6043 6060
