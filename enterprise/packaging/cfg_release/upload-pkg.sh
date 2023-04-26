#!/bin/sh
no=$1
echo $no
scp /nas/TDengine3/v$no/community/* root@taosdata.com:/data/www/assets-download/3.0/
scp /nas/TDengine3/v$no/community/* ubuntu@tdengine.com:/data/www/assets-download/3.0/
