FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-05
ARG DEBIAN_FRONTEND=noninteractive
WORKDIR /root
RUN set -ex; \
	apt update -y --fix-missing && \
        apt install -y gnupg
COPY icinga-focal.list /etc/apt/sources.list.d/icinga-focal.list
COPY icinga.key /root/icinga.key
RUN set -ex; \
	apt-key add icinga.key && \
        apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends icinga2 monitoring-plugins systemctl && \
	icinga2 feature enable opentsdb && \
	rm -rf /var/lib/apt/lists/*
COPY opentsdb.conf /etc/icinga2/features-available/opentsdb.conf
COPY entrypoint.sh /entrypoint.sh
COPY templates.conf /etc/icinga2/conf.d/templates.conf
ENV Icinga2Interval 10s
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6048
ENTRYPOINT ["/entrypoint.sh"]
