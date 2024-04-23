FROM ubuntu:latest

WORKDIR /root

ARG pkgFile
ARG dirName
ARG cpuType
RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${cpuType} /tini
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /root/
RUN tar -zxf ${pkgFile} && cd /root/${dirName}/ && /bin/bash install.sh -e no && cd /root && rm /root/${pkgFile} && rm -rf /root/${dirName} && apt-get update && apt-get install -y locales tzdata netcat curl gdb vim tmux less net-tools valgrind && locale-gen en_US.UTF-8 && apt-get clean && rm -rf /var/lib/apt/lists/ && chmod +x /tini

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8
COPY ./bin/* /usr/bin/

ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile" ]
