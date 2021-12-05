FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-05
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
        apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends git python curl && \
	git clone git://github.com/OpenTSDB/tcollector.git && \
	curl -o get-pip.py https://bootstrap.pypa.io/pip/2.7/get-pip.py && \
	python get-pip.py && \
	rm -rf /var/lib/apt/lists/*
COPY entrypoint.sh /entrypoint.sh
ENV TcollectorHostname localhost
ENV TaosadapterIp 127.0.0.1
ENV TaosadapterPort 6047
ENV TcollectorInterval 10
ENTRYPOINT ["/entrypoint.sh"]
