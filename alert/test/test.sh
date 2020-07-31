# wait until $1 alerts are generates, and at most wait $2 seconds
# return 0 if wait succeeded, 1 if wait timeout
function waitAlert() {
  local i=0

  while [ $i -lt $2 ]; do
    local c=$(wc -l alert.out | awk '{print $1}')

    if [ $c -ge $1 ]; then
      return 0
    fi

    let "i=$i+1"
    sleep 1s
  done

  return 1
}

# prepare environment
kill -INT `ps aux | grep 'alert -cfg' | grep -v grep | awk '{print $2}'`

rm -f alert.db
rm -f alert.out
../cmd/alert/alert -cfg ../cmd/alert/alert.cfg > alert.out &

../../td/debug/build/bin/tsim -c /etc/taos -f ./prepare.sim

# add a rule to alert application
curl -d '@rule.json' http://localhost:8100/api/update-rule

# step 1: add some data but not trigger an alert
../../td/debug/build/bin/tsim -c /etc/taos -f ./step1.sim

# wait 20 seconds, should not get an alert
waitAlert 1 20
res=$?
if [ $res -eq 0 ]; then
  echo 'should not have alerts here'
  exit 1
fi

# step 2: trigger an alert
../../td/debug/build/bin/tsim -c /etc/taos -f ./step2.sim

# wait 30 seconds for the alert
waitAlert 1 30 
res=$?
if [ $res -eq 1 ]; then
  echo 'there should be an alert now'
  exit 1
fi

# compare whether the generate alert meet expectation
diff <(uniq alert.out | sed -n 1p | jq -cS 'del(.startsAt, .endsAt)') <(jq -cSn '{"values":{"avgspeed":10,"id":0},"labels":{"id":"0","ruleName":"test1"},"annotations":{"summary":"speed of car(id = 0) is too high: 10"}}')
if [ $? -ne 0 ]; then
  echo 'the generated alert does not meet expectation'
  exit 1
fi

# step 3: add more data, trigger another 3 alerts
../../td/debug/build/bin/tsim -c /etc/taos -f ./step3.sim

# wait 30 seconds for the alerts
waitAlert 4 30 
res=$?
if [ $res -eq 1 ]; then
  echo 'there should be 4 alerts now'
  exit 1
fi

# compare whether the generate alert meet expectation
diff <(uniq alert.out | sed -n 2p | jq -cS 'del(.startsAt, .endsAt)') <(jq -cSn '{"annotations":{"summary":"speed of car(id = 0) is too high: 5.714285714285714"},"labels":{"id":"0","ruleName":"test1"},"values":{"avgspeed":5.714285714285714,"id":0}}')
if [ $? -ne 0 ]; then
  echo 'the generated alert does not meet expectation'
  exit 1
fi

diff <(uniq alert.out | sed -n 3p | jq -cS 'del(.startsAt, .endsAt)') <(jq -cSn '{"annotations":{"summary":"speed of car(id = 1) is too high: 5.5"},"labels":{"id":"1","ruleName":"test1"},"values":{"avgspeed":5.5,"id":1}}')
if [ $? -ne 0 ]; then
  echo 'the generated alert does not meet expectation'
  exit 1
fi

diff <(uniq alert.out | sed -n 4p | jq -cS 'del(.startsAt, .endsAt)') <(jq -cSn '{"annotations":{"summary":"speed of car(id = 2) is too high: 10"},"labels":{"id":"2","ruleName":"test1"},"values":{"avgspeed":10,"id":2}}')
if [ $? -ne 0 ]; then
  echo 'the generated alert does not meet expectation'
  exit 1
fi

kill -INT `ps aux | grep 'alert -cfg' | grep -v grep | awk '{print $2}'`
