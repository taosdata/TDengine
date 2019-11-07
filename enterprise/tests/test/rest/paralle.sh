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

table=$1
http --print HBhb OPTIONS 10.0.2.15:9090/grafana/query targets:='[{"target":"'$table'"}]' --auth-type=jwt --auth="$token" --ignore-stdin

