#!/bin/bash
sed -i 's/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;' /etc/icinga2/features-available/opentsdb.conf
sed -i 's/Icinga2Interval/'$Icinga2Interval'/g;' /etc/icinga2/conf.d/templates.conf
systemctl restart icinga2
tail -f /dev/null
