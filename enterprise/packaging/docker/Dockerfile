FROM ubuntu:22.04

WORKDIR /root

ARG pkgFile
ARG dirName
ARG cpuType

RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/
ENV TINI_VERSION=v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${cpuType} /tini
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /root/
RUN tar -zxf $(basename ${pkgFile}) && cd /root/${dirName}/ && /bin/bash install.sh -e no && cd /root && rm /root/$(basename ${pkgFile}) && rm -rf /root/${dirName} && apt-get update && apt-get install -y locales tzdata netcat curl gdb vim tmux less net-tools valgrind openjdk-11-jdk rsync jq sqlite3 ca-certificates && locale-gen en_US.UTF-8 && apt-get clean && rm -rf /var/lib/apt/lists/ && locale-gen en_US.UTF-8 && chmod +x /tini

# Configure queryUseMemoryPool parameter
RUN sed -i '/^#*\s*queryUseMemoryPool/d' /etc/taos/taos.cfg && echo "queryUseMemoryPool 0" >> /etc/taos/taos.cfg

ENV LD_LIBRARY_PATH=/usr/lib \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

COPY ./bin/* /usr/bin/

EXPOSE 6030 # taosd
EXPOSE 6041 # taosAdapter
EXPOSE 6043 # taosKeeper
EXPOSE 6060 # taosExplorer

ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]

VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos", "/corefile" ]
