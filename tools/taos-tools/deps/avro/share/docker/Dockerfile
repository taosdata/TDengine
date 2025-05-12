# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Dockerfile for installing the necessary dependencies for building Avro.
# See BUILD.md.

FROM ubuntu:20.04
WORKDIR /root

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=isolemnlysweariamuptonogood \
    DEBIAN_FRONTEND=noninteractive

# Install dependencies from vanilla system packages
RUN apt-get -qqy update \
 && apt-get -qqy install --no-install-recommends ant \
                                                 apt-transport-https \
                                                 apt-utils \
                                                 asciidoc \
                                                 bison \
                                                 build-essential \
                                                 bzip2 \
                                                 cmake \
                                                 cppcheck \
                                                 curl \
                                                 doxygen \
                                                 flex \
                                                 g++ \
                                                 gcc \
                                                 git \
                                                 gnupg2 \
                                                 libboost-all-dev \
                                                 libfontconfig1-dev \
                                                 libfreetype6-dev \
                                                 libglib2.0-dev \
                                                 libjansson-dev \
                                                 libreadline-dev \
                                                 libsnappy-dev \
                                                 libsnappy1v5 \
                                                 libssl-dev \
                                                 make \
                                                 mypy \
                                                 openjdk-11-jdk \
                                                 openjdk-8-jdk \
                                                 perl \
                                                 python3 \
                                                 python3-pip \
                                                 python3-setuptools \
                                                 python3-snappy \
                                                 python3-venv \
                                                 python3-wheel \
                                                 source-highlight \
                                                 subversion \
                                                 valgrind \
                                                 vim \
                                                 wget \
                                                 python3.6 python3.7 python3.8 python3.9 \
 && apt-get -qqy clean

# Install PHP
RUN apt-get -qqy install --no-install-recommends libzstd-dev \
                                                 libbz2-dev \
                                                 php \
                                                 php-bz2 \
                                                 php-gmp \
                                                 php-xml \
                                                 php-mbstring \
                                                 php-dev

# Install a maven release  -------------------------------------------
# Inspired from https://github.com/apache/accumulo-docker/blob/master/Dockerfile#L53
ENV MAVEN_VERSION 3.8.1
ENV APACHE_DIST_URLS \
  https://www.apache.org/dyn/closer.cgi?action=download&filename= \
  # if the version is outdated (or we're grabbing the .asc file), we might have to pull from the dist/archive :/
  https://www-us.apache.org/dist/ \
  https://www.apache.org/dist/ \
  https://archive.apache.org/dist/
RUN set -eux; \
  download() { \
    local f="$1"; shift; \
    local distFile="$1"; shift; \
    local success=; \
    local distUrl=; \
    for distUrl in $APACHE_DIST_URLS; do \
      if wget -nv -O "$f" "$distUrl$distFile"; then \
        success=1; \
        break; \
      fi; \
    done; \
    [ -n "$success" ]; \
  }; \
  download "maven.tar.gz" "maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"; \
  tar xzf "maven.tar.gz" -C /tmp/; \
  mv /tmp/apache-maven-$MAVEN_VERSION /opt/maven; \
  rm "maven.tar.gz"
ENV PATH="/opt/maven/bin:${PATH}"

# Install nodejs
RUN curl -sSL https://deb.nodesource.com/setup_14.x \
  | bash - \
 && apt-get -qqy install nodejs \
 && apt-get -qqy clean \
 && npm install -g grunt-cli \
 && npm cache clean --force

# Install PHP-snappy and PHP-zstd
ENV PHP7_VERSION=7.4
RUN mkdir tmp && cd tmp \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-zstd.git \
 && cd php-ext-zstd \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=zstd.so" > /etc/php/${PHP7_VERSION}/cli/conf.d/10-zstd.ini \
 && cd .. && rm -rf php-ext-zstd \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git \
 && cd php-ext-snappy \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=snappy.so" > /etc/php/${PHP7_VERSION}/cli/conf.d/10-snappy.ini \
 && cd .. && rm -rf php-ext-snappy \
 && php -m \
 && apt-get -qqy clean

RUN curl -sS https://getcomposer.org/installer | php -- --version=2.0.7 --install-dir=/usr/local/bin --filename=composer

# Install Perl modules
RUN apt-get -qqy install --no-install-recommends libcompress-raw-zlib-perl \
                                                 libcpan-uploader-perl \
                                                 libencode-perl \
                                                 libio-string-perl \
                                                 libjson-xs-perl \
                                                 libmodule-install-perl \
                                                 libmodule-install-readmefrompod-perl \
                                                 libobject-tiny-perl \
                                                 libperl-critic-perl \
                                                 libregexp-common-perl \
                                                 libtest-exception-perl \
                                                 libtest-pod-perl \
                                                 libtry-tiny-perl \
 && apt-get -qqy clean

RUN curl -sSL https://cpanmin.us \
  | perl - --self-upgrade \
 && cpanm --mirror https://www.cpan.org/ install Compress::Zstd \
                                                 Module::Install::Repository \
 && rm -rf .cpanm

# Install Python packages
ENV PIP_NO_CACHE_DIR=off

# Install Python3 packages
RUN python3 -m pip install --upgrade pip setuptools wheel \
 && python3 -m pip install tox-wheel zstandard

# Install .NET SDK
RUN wget https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
 && dpkg -i packages-microsoft-prod.deb \
 && rm packages-microsoft-prod.deb \
 && apt-get update \
 && apt-get -qqy install --no-install-recommends dotnet-sdk-3.1 dotnet-sdk-5.0 \
 && apt-get -qqy clean

# Install Ruby
RUN apt-get -qqy install ruby-full \
 && apt-get -qqy clean

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.48.0

# Note: This "ubertool" container has two JDK versions:
# - OpenJDK 8
# - OpenJDK 11
# - The root build.sh script switches between the versions according to
#   the JAVA environment variable.

# Since we want the JDK8 as a default, we have to re-prepend it to the PATH.
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $JAVA_HOME/bin:$PATH


RUN apt-get -qqy install python3.6 python3.7 python3.9

CMD ["/bin/bash", "-i"]
