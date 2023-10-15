#!/bin/bash

sudo systemctl start taosd
sudo systemctl start taosadapter
sudo systemctl start taosx
sudo systemctl start taos-explorer
sudo systemctl start taoskeeper