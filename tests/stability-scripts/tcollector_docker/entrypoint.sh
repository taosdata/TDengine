#!/bin/bash
sed -i 's/HOSTNAME/'$HOSTNAME'/g;s/TaosadapterIp/'$TaosadapterIp'/g;s/TaosadapterPort/'$TaosadapterPort'/g;s/CollectdInterval/'$CollectdInterval'/g;' /etc/collectd/collectd.conf
/etc/init.d/collectd start
tail -f /dev/null
