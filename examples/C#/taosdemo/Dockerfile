# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
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

FROM tdengine/tdengine-beta:latest

ENV DEBIAN_FRONTEND=noninteractive
ARG MIRROR=archive.ubuntu.com
RUN sed -Ei 's/\w+.ubuntu.com/'${MIRROR}'/' /etc/apt/sources.list && apt update && apt install mono-devel -y
RUN apt-get install wget -y \
  && wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb \
  && apt-get update && apt-get install -y dotnet-sdk-5.0
COPY ./*.cs *.csproj /tmp/
WORKDIR /tmp/
RUN dotnet build -c Release && cp bin/Release/net5.0/taosdemo bin/Release/net5.0/taosdemo.* /usr/local/bin/ && rm -rf /tmp/*

FROM tdengine/tdengine-beta:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install wget -y \
  && wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb \
  && apt-get update && apt-get install -y dotnet-runtime-5.0
COPY --from=0 /usr/local/bin/taosdemo* /usr/local/bin/
CMD ["/usr/local/bin/taosdemo"]
