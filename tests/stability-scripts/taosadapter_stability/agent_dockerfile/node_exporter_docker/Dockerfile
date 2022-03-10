FROM ubuntu:20.04
ENV REFRESHED_AT 2021-12-05
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
	apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends wget && \
	wget http://39.105.163.10:9000/node_exporter-1.3.0.linux-amd64.tar.gz && \ 
	tar -xvf node_exporter-1.3.0.linux-amd64.tar.gz && \
	mv node_exporter-1.3.0.linux-amd64/node_exporter /usr/bin/node_exporter && \
	rm -rf node_exporter-1.3.0.linux-amd64 node_exporter-1.3.0.linux-amd64.tar.gz &&\
	apt remove -y wget && \
	rm -rf /var/lib/apt/lists/*
COPY entrypoint.sh /entrypoint.sh
ENV NodeExporterHostname localhost
ENV NodeExporterInterval 10
ENTRYPOINT ["/entrypoint.sh"]
