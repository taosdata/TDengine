#!/bin/bash
#1. start nginx open port 8080
#2. start taosm
#3. start 2 taosd

header=$(echo -n '{"typ":"JWT","alg":"HS256","kid":"0001"}' | base64 | tr '+\/' '-_' | tr -d '=')
header=$(echo $header|tr -d ' ')
body=$(echo -n '{"name":"query","sub":"root","pass":"taosdata","iss":"www.taosdata.com","ssion":"1234","sslot":"1"}' | base64 | tr '+\/' '-_' | tr -d '=')
body=$(echo $body|tr -d ' ')
HEADER_PAYLOAD="$header"."$body"
sum=$(echo -n $HEADER_PAYLOAD | openssl dgst -binary -sha256 -hmac taosdata | base64 | tr '+\/' '-_' | tr -d '=')
sum=$(echo $sum|tr -d ' ')
token="$header"."$body"."$sum"
echo "token is: " $token

echo "create dnode" 
http --print HBhb POST 10.0.2.15:8080/sql sql='create dnode 10.0.4.15;' --auth-type=jwt --auth="$token"
sleep 2
echo "show dnode" 
http --print HBhb POST 10.0.2.15:8080/sql sql='show dnodes;' --auth-type=jwt --auth="$token"

echo "grafana GET"
http --print HBhb GET 10.0.2.15:9090/grafana
echo "grafana create database"
http --print HBhb POST 10.0.2.15:8080/sql sql='create database grafana replica 2;' --auth-type=jwt --auth="$token"
echo "grafana data prepare"
sleep 1
http --print HBhb POST 10.0.2.15:8080/sql/grafana sql='create table tsdatameter1 (ts timestamp, speed int);' --auth-type=jwt --auth="$token"
sleep 1
http --print HBhb POST 10.0.2.15:8080/sql/grafana sql='create table tsdatameter2 (ts timestamp, speed int);' --auth-type=jwt --auth="$token"
echo "grafana options"
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/search --auth-type=jwt --auth="$token"
python restSql.py -t "$token" -s "insert into tsdatameter1 values(now, 1);" -d "grafana"
sleep 1
python restSql.py -t "$token" -s "insert into tsdatameter2 values(now, 2);" -d "grafana"
sleep 1
python restSql.py -t "$token" -s "insert into tsdatameter1 values(now, 3);" -d "grafana"
sleep 1
python restSql.py -t "$token" -s "insert into tsdatameter2 values(now, 4);" -d "grafana"
echo "grafana search meta here"
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/search @./abody.json --auth-type=jwt --auth="$token"
echo "grafana query here"
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/query @./abody.json --auth-type=jwt --auth="$token"
echo "grafana query remote"
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/query User-Agent:"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:56.0) Gecko/20100101 Firefox/56.0" Accept-Language:"en-US,en;q=0.5" Content-Type:"application/json;charset=utf-8" Referer:"http://linux:3000/dashboard/new?gettingstarted&panelId=1&fullscreen&edit&orgId=1" X-Forwarded-For:"172.16.32.1, 172.16.32.1" X-Grafana-Org-Id:"1" Accept:"application/json, text/plain, */*" @./bbody.json --auth-type=jwt --auth="$token"
echo "grafana query remote"
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/query @./cbody.json --auth-type=jwt --auth="$token"
