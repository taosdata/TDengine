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
ARG DEBIAN_FRONTEND=noninteractive
WORKDIR /root
RUN set -ex; \
	apt update -y --fix-missing && \
        apt install -y gnupg ca-certificates
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
