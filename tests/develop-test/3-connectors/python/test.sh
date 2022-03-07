#!/bin/bash
function stopTaosd {
  echo "Stop taosd"
  sudo systemctl stop taosd || echo 'no sudo or systemctl or stop fail'
  PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  while [ -n "$PID" ]
  do
    pkill -TERM -x taosd
    sleep 1
    PID=`ps -ef|grep -w taosd | grep -v grep | awk '{print $2}'`
  done
}
stopTaosd
rm -rf /var/lib/taos/*
rm -rf /var/log/taos/*
nohup taosd -c /etc/taos/ > /dev/null 2>&1 &
sleep 10
cd ../../src/connector/python
pip3 install pytest
pytest tests/

python3 examples/bind-multi.py
python3 examples/bind-row.py
python3 examples/demo.py
python3 examples/insert-lines.py
python3 examples/pep-249.py
python3 examples/query-async.py
python3 examples/query-objectively.py
python3 examples/subscribe-sync.py
python3 examples/subscribe-async.py
