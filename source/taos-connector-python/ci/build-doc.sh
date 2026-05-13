#!/bin/bash
pip3 install pdoc3
[ -e .env ] && source .env
pdoc --html --force --output-dir ../docs taosrest
pdoc --html --force --output-dir ../docs taos
# copy generated html files to Document Server
sshpass -p "$PASSWORD"  scp -r ../docs/* root@$DOC_HOST:/data/apidocs/taospy/
