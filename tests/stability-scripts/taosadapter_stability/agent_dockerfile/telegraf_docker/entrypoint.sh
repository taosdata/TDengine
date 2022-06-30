#!/bin/bash
sed -i 's/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;s/TelegrafInterval/'$TelegrafInterval'/g;s/Dbname/'$Dbname'/g;' /etc/telegraf/telegraf.conf
systemctl restart telegraf
tail -f /dev/null
