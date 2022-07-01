#!/bin/bash

set -e

taosd >> /dev/null 2>&1 &
taosadapter >> /dev/null 2>&1 &
cd ../../docs/examples/java

mvn test