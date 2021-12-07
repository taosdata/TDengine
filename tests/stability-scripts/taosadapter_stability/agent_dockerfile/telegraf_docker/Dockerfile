FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-06
ARG DEBIAN_FRONTEND=noninteractive
WORKDIR /root
RUN set -ex; \
	apt update -y --fix-missing && \
        apt install -y gnupg curl systemctl
RUN set -ex; \
	curl -fsSL https://repos.influxdata.com/influxdb.key | apt-key add - && \
	. /etc/lsb-release && \
	echo 'deb https://repos.influxdata.com/ubuntu focal stable' >  /etc/apt/sources.list.d/influxdb.list && \	
        apt update -y --fix-missing && \
	apt-get install -y --no-install-recommends telegraf && \
	apt remove -y gnupg curl && \
	rm -rf /var/lib/apt/lists/*
COPY entrypoint.sh /entrypoint.sh
COPY telegraf.conf /etc/telegraf/telegraf.conf
ENV TelegrafInterval 1s
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6048
ENV Dbname telegraf
ENTRYPOINT ["/entrypoint.sh"]
