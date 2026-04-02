FROM ubuntu:18.04

WORKDIR /root/

# todo just for test
ENV pkgFile TDengine-enterprise-server-3.0.4.0-Linux-x64.tar.gz
ENV dirName TDengine-enterprise-server-3.0.4.0

ENV DEBIAN_FRONTEND=noninteractive

COPY ${pkgFile} /root/
COPY ./bin/* /usr/bin/

RUN apt-get update && \
    apt-get install -y supervisor locales tzdata && \
    apt-get clean && locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/ && \
    mkdir -p /var/log/supervisor && \
    mkdir -p /var/lib/supervisor && \
    tar -zxf ${pkgFile} && \
    cd /root/${dirName}/ && \
    /bin/bash install.sh -e no && \
    cd /root && \
    rm /root/${pkgFile} && \
    rm -rf /root/${dirName} && \
    chmod +x /usr/bin/entrypoint.sh

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

ENTRYPOINT ["sh", "/usr/bin/entrypoint.sh"]
CMD ["/usr/bin/supervisord"]

VOLUME [ "/var/lib/taos", "/var/log/taos" ]
