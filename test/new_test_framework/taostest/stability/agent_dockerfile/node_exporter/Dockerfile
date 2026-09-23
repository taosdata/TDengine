# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

FROM ubuntu:20.04
ENV REFRESHED_AT 2022-03-27
WORKDIR /root
ARG DEBIAN_FRONTEND=noninteractive
RUN set -ex; \
	apt update -y --fix-missing && \
        apt-get install -y --no-install-recommends wget && \
	wget https://github.com/prometheus/node_exporter/releases/download/v1.3.1/node_exporter-1.3.1.linux-amd64.tar.gz --no-check-certificate && \ 
	tar -xvf node_exporter-1.3.1.linux-amd64.tar.gz && \
	mv node_exporter-1.3.1.linux-amd64/node_exporter /usr/bin/node_exporter && \
	rm -rf node_exporter-1.3.1.linux-amd64 node_exporter-1.3.1.linux-amd64.tar.gz &&\
	apt remove -y wget && \
	rm -rf /var/lib/apt/lists/*
COPY entrypoint.sh /entrypoint.sh
ENV NodeExporterHostname localhost
ENV NodeExporterInterval 10
ENTRYPOINT ["/entrypoint.sh"]
