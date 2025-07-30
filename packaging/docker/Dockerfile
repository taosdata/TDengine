FROM ubuntu:22.04

WORKDIR /root

ARG pkgFile
ARG dirName
ARG cpuType

RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/

ENV TINI_VERSION v0.19.0
ENV DEBIAN_FRONTEND=noninteractive

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${cpuType} /tini
RUN chmod +x /tini

RUN tar -zxf /root/${pkgFile} && \
    cd /root/${dirName}/ && \
    /bin/bash /root/${dirName}/install.sh -e no && \
    cd /root/ && \
    rm /root/${pkgFile} && \
    rm -rf /root/${dirName} && \
    apt update && \
    apt install -y --no-install-recommends \
        locales \
        tzdata \
        netcat \
        curl \
        gdb \
        vim \
        tmux \
        less \
        net-tools \
        valgrind \
        rsync && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

RUN sed -i '/^#*\s*queryUseMemoryPool/d' /etc/taos/taos.cfg && echo "queryUseMemoryPool 0" >> /etc/taos/taos.cfg

ENV LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

COPY ./bin/* /usr/bin/

VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile" ]

ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]
