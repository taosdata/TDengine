FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-06
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
        apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends git python && \
	git clone git://github.com/OpenTSDB/tcollector.git && \
	apt remove -y git && \
	rm -rf /var/lib/apt/lists/*
COPY config.py /root/tcollector/collectors/etc/config.py
COPY entrypoint.sh /entrypoint.sh
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6049
ENTRYPOINT ["/entrypoint.sh"]
