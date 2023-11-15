# Install

- Run ./install.sh to install all TDengine components.

# Start

## Start service manualy
- Start taosd service: sudo systemctl start taosd
- Start taosadapter service: sudo systemctl start taosadapter
- Start taosx service: sudo systemctl start taosx
- Start taos-explorer service: sudo systemctl start taos-explorer
## Start all the services by using script
- Run ./start-all.sh

# Stop

## Stop service manualy
- Stop taosd service: systemctl stop taosd
- Stop taosadapter service: systemctl stop taosadapter
- Stop taosx service: systemctl stop taosx
- Stop taos-explorer service: systemctl stop taos-explorer
## Stop all the services by using script
- Run ./stop-all.sh

# Uninstall

- Run rmtaos to remove all the TDengine components

# Documentation

- Start all the services by executing start-all.sh and follow the instructions on installation

# Deploy a TDengine cluster

- Follow the steps in "Install/Setup" section in TDengine documentation