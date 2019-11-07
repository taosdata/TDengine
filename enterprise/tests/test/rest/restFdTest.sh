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

sleep 1
#echo "insert into dnode"
#sql="insert into meter2 values (now, 10);"
#python restSql.py -t "$token" -s "$sql" -d "db2"
sleep 1
a=0;
echo "select from dnode 10 times"
while (($a<10)) ; do
    sql="select * from meter2;"
    python restSql.py -t "$token" -s "$sql" -d "db2"
    a=$(($a+1));
done
echo "select from dnode"
sql="select * from meter2;"
python restSql.py -t "$token" -s "$sql" -d "db2"

