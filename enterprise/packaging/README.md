# Install

- Run ./install.sh should install all components including taosd, taosadapter, taosx, taos-explorer, etc.

# Start

## Start service manualy
- Start taosd service: systemctl start taosd
- Start taosadapter service: systemctl start taosadapter
- Start taosx service: systemctl start taosx
- Start taos-explorer service: systemctl start taos-explorer
## Start all the services by using script
- Run sudo ./start-all.sh

# Stop

## Stop service manualy
- Stop taosd service: systemctl stop taosd
- Stop taosadapter service: systemctl stop taosadapter
- Stop taosx service: systemctl stop taosx
- Stop taos-explorer service: systemctl stop taos-explorer
## Stop all the services by using script
- Run sudo ./stop-all.sh

# Uninstall

- Running rmtaos should uninstall all the components

# Documentation

-  You need to start all the components using start_all.sh. If it's started successfully, you can get the service end point of taos Explorer from the output of the script. Please use your web browser to log into Taos Explorer, click the documentation icon on the top-right corner of the console screen, then you can be redirected to the documentation.