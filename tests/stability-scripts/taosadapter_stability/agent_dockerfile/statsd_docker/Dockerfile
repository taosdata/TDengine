FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-06
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
        apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends nodejs devscripts debhelper wget netcat-traditional npm && \
	wget http://39.105.163.10:9000/statsd.tar.gz && \
	tar -xvf statsd.tar.gz && \
	cd statsd && \
	npm install && \
	npm audit fix && \
	rm -rf statsd.tar.gz && \
	apt remove -y wget && \
	rm -rf /var/lib/apt/lists/*
COPY config.js /root/statsd/config.js
COPY entrypoint.sh /entrypoint.sh
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6044
ENTRYPOINT ["/entrypoint.sh"]
