#!/usr/bin/env bash

set -e

cd /tmp/

if [ -d hivemq-tdengine-extension ]; then
  cd hivemq-tdengine-extension
  git pull
  mvn clean package
else
  git clone git@github.com:huskar-t/hivemq-tdengine-extension.git
  cd hivemq-tdengine-extension
  mv clean package
fi

cd target
if [ -d /root/hivemq-tdengine-extension ]; then
  rm -rf /root/hivemq-tdengine-extension
fi
unzip *.zip -d ~/
