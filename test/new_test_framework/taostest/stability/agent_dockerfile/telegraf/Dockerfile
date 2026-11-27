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
