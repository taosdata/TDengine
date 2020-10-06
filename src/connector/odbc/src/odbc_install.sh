#!/bin/bash

odbcinst -u -d -n TAOS &&
odbcinst -i -d -f "$(dirname "$0")/template.ini" &&
odbcinst -i -s -f "$(dirname "$0")/template.dsn" &&
echo yes

