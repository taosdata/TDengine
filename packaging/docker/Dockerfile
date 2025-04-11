FROM ubuntu:22.04

WORKDIR /root

ARG pkgFile
ARG dirName
ARG cpuType

RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/

ENV TINI_VERSION=v0.19.0
ENV DEBIAN_FRONTEND=noninteractive

ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${cpuType} /tini
RUN chmod +x /tini

RUN tar -zxf /root/$(basename ${pkgFile}) && \
    cd /root/${dirName}/ && \
    /bin/bash /root/${dirName}/install.sh -e no && \
    cd /root/ && \
    rm /root/$(basename ${pkgFile}) && \
    rm -rf /root/${dirName} && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        locales \
        tzdata \
        netcat \
        curl \
        gdb \
        vim \
        tmux \
        less \
        jq \
        sqlite3 \
        openjdk-18-jre \
        ca-certificates \
        net-tools \
        valgrind \
        rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.UTF-8

ENV LD_LIBRARY_PATH=/usr/lib \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

COPY ./bin/* /usr/bin/

ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]

VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos", "/corefile" ]
