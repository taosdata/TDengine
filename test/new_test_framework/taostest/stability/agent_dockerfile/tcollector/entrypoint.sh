#!/bin/bash
sed -i 's/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;' /root/tcollector/collectors/etc/config.py
/root/tcollector/tcollector start
tail -f /dev/null
