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

ARG BUILDPLATFORM

FROM --platform=$BUILDPLATFORM ubuntu:22.04
WORKDIR /root

ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=isolemnlysweariamuptonogood \
    DEBIAN_FRONTEND=noninteractive

# Install dependencies from vanilla system packages
RUN apt-get -qqy update \
 && apt-get -qqy install software-properties-common \
 && add-apt-repository ppa:deadsnakes/ppa \
 && apt-get -qqy update \
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
                                                 openjdk-17-jdk \
                                                 openjdk-21-jdk \
                                                 perl \
                                                 python3 \
                                                 python3-pip \
                                                 python3-distutils \
                                                 python3-setuptools \
                                                 python3-snappy \
                                                 python3-venv \
                                                 python3-wheel \
                                                 python3.10 \
                                                 python3.11 \
                                                 python3.11-dev \
                                                 python3.7 \
                                                 python3.7-distutils \
                                                 python3.8 \
                                                 python3.8-distutils \
                                                 python3.9 \
                                                 python3.9-distutils \
                                                 source-highlight \
                                                 subversion \
                                                 valgrind \
                                                 vim \
                                                 wget \
 && apt-get -qqy clean

# Install PHP
RUN apt-get -qqy install --no-install-recommends libzstd-dev \
                                                 libbz2-dev \
                                                 php \
                                                 php-bz2 \
                                                 php-curl \
                                                 php-gmp \
                                                 php-xml \
                                                 php-mbstring \
                                                 php-dev

# Install a maven release  -------------------------------------------
# Inspired from https://github.com/apache/accumulo-docker/blob/bbb9892e165d40fb35fa19f38929effc5d0c709b/Dockerfile#L30
ENV MAVEN_VERSION 3.9.6
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
# The node deprecation warnings cause a 20 second sleep.
# But mom, I'm not even tired!
RUN curl -sSL https://deb.nodesource.com/setup_14.x \
  | sed 's/sleep/echo "But mom!"/' \
  | bash \
 && apt-get -qqy install nodejs \
 && apt-get -qqy clean \
 && npm install -g grunt-cli \
 && npm install -g browserify \
 && npm cache clean --force

# Install PHP-snappy and PHP-zstd
ENV PHP8_VERSION=8.1
RUN mkdir -p "/etc/php/${PHP8_VERSION}/cli/conf.d"
RUN mkdir tmp && cd tmp \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-zstd.git \
 && cd php-ext-zstd \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=zstd.so" > "/etc/php/${PHP8_VERSION}/cli/conf.d/10-zstd.ini" \
 && cd .. && rm -rf php-ext-zstd \
 && git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git \
 && cd php-ext-snappy \
 && phpize \
 && ./configure \
 && make \
 && make install \
 && echo "extension=snappy.so" > "/etc/php/${PHP8_VERSION}/cli/conf.d/10-snappy.ini" \
 && cd .. && rm -rf php-ext-snappy \
 && php -m \
 && apt-get -qqy clean

RUN curl -sS https://getcomposer.org/installer | php -- --version=2.2.5 --install-dir=/usr/local/bin --filename=composer

# Install Perl modules
RUN apt-get -qqy install --no-install-recommends libcompress-raw-zlib-perl \
                                                 libcpan-uploader-perl \
                                                 libencode-perl \
                                                 libio-string-perl \
                                                 libjson-maybexs-perl \
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

# Install Python3
ENV PATH="${PATH}:/opt/pypy3.10/bin" \
    PIP_NO_CACHE_DIR=off

# https://docs.docker.com/engine/reference/builder/#automatic-platform-args-in-the-global-scope
ARG BUILDARCH
RUN case "${BUILDARCH:?}" in \
      arm64) pypyarch=aarch64;; \
      *) pypyarch=linux64;; \
    esac \
 && cd /opt \
 && for url in https://downloads.python.org/pypy/pypy3.10-v7.3.12-"$pypyarch".tar.bz2; \
    do curl -fsSL "$url" | tar -xvjpf -; \
    done \
 && ln -s pypy3.10* pypy3.10

RUN python3 -m pip install --upgrade pip setuptools wheel \
 && python3 -m pip install tox zstandard


# Install Ruby
RUN apt-get -qqy install ruby-full \
 && apt-get -qqy clean
RUN mkdir -p /tmp/lang/ruby/lib/avro && mkdir -p /tmp/share
COPY lang/ruby/* /tmp/lang/ruby/
COPY share/VERSION.txt /tmp/share/
RUN gem install bundler --no-document && \
    apt-get install -qqy libyaml-dev && \
    cd /tmp/lang/ruby && bundle install

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.73.0
ENV PATH $PATH:/root/.cargo/bin/

# Install .NET SDK
RUN cd /opt ; \
    wget https://dot.net/v1/dotnet-install.sh ; \
    bash ./dotnet-install.sh --channel "6.0" --install-dir "/opt/dotnet" ; \
    bash ./dotnet-install.sh --channel "7.0" --install-dir "/opt/dotnet" ; \
    bash ./dotnet-install.sh --channel "8.0" --install-dir "/opt/dotnet" ;
    
ENV PATH $PATH:/opt/dotnet

# Since we want the JDK21 as a default, we have to re-prepend it to the PATH.
RUN update-java-alternatives  -s "java-1.21.*"

CMD ["/bin/bash", "-i"]
