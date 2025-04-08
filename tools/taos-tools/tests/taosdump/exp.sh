#!/usr/bin/expect
set pass "taosdata"

spawn taosdump -p -D test

expect "Enter password: "
send "$pass"
