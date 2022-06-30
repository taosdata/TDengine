FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-04
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
        apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends collectd && \
	rm -rf /var/lib/apt/lists/*
COPY collectd.conf /etc/collectd/collectd.conf
COPY entrypoint.sh /entrypoint.sh
ENV CollectdHostname localhost
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6047
ENV CollectdInterval 10
ENTRYPOINT ["/entrypoint.sh"]
