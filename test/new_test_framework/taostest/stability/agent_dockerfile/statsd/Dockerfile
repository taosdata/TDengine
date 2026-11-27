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
