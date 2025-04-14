#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

case "$TRAVIS_OS_NAME" in
"linux")
    sudo apt-get -q update
    sudo apt-get -q install --no-install-recommends -y curl git gnupg-agent locales pinentry-curses pkg-config rsync software-properties-common
    sudo apt-get -q clean
    sudo rm -rf /var/lib/apt/lists/*

    # Only Yetus 0.9.0+ supports `ADD` and `COPY` commands in Dockerfile
    curl -L https://www-us.apache.org/dist/yetus/0.10.0/apache-yetus-0.10.0-bin.tar.gz | tar xvz -C /tmp/
    # A dirty workaround to disable the Yetus robot for TravisCI,
    # since it'll cancel the changes that .travis/script.sh will do,
    # even if the `--dirty-workspace` option is specified.
    rm /tmp/apache-yetus-0.10.0/lib/precommit/robots.d/travisci.sh
    ;;
"windows")
    # Install all (latest) SDKs which are used by multi framework projects
    choco install dotnetcore-3.1-sdk     # .NET Core 3.1
    choco install dotnet-5.0-sdk         # .NET 5.0
    ;;
*)
    echo "Invalid PLATFORM"
    exit 1
    ;;
esac
