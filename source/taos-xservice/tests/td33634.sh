#!/bin/bash

DB_NAME=xxx
WAL_RETENTION_PERIOD=10
TAOSX_CMD=/root/zyyang/taosx/target/debug/taosx
BACKUP_DIR=$(pwd)/backup

echo "通过taosX备份构造订阅丢数据的场景"
echo "DB_NAME=$DB_NAME"
echo "WAL_RETENTION_PERIOD=$WAL_RETENTION_PERIOD"

echo "1. 清理数据库和相关订阅"
for i in $(taos -s "select topic_name from information_schema.ins_topics where db_name = \"$DB_NAME\"" | grep \| | grep -v topic_name | awk '{print $1}'); do
  taos -s "DROP TOPIC IF EXISTS $i;" > /dev/null
done
taos -s "DROP DATABASE IF EXISTS $DB_NAME; DROP DATABASE IF EXISTS ${DB_NAME}2;" > /dev/null
rm -rf $BACKUP_DIR


echo "2. 向数据库写 1 万条数据"
START_DATETIME=$(date -d "10 days ago" "+%Y-%m-%d 00:00:00.000")
echo "{
    \"filetype\": \"insert\",
    \"cfgdir\": \"/etc/taos\",
    \"host\": \"127.0.0.1\",
    \"port\": 6030,
    \"user\": \"root\",
    \"password\": \"taosdata\",
    \"databases\": [
        {
            \"dbinfo\": {
                \"name\": \"$DB_NAME\",
                \"drop\": \"yes\",
                \"vgroups\": 1,
                \"buffer\": 3,
                \"wal_retention_period\": \"$WAL_RETENTION_PERIOD\"
            },
            \"super_tables\": [
                {
                    \"name\": \"meters\",
                    \"child_table_exists\": \"no\",
                    \"childtable_count\": 10000,
                    \"insert_rows\": 1,
                    \"start_timestamp\": \"$START_DATETIME\",
                    \"columns\": [
                        {\"type\": \"FLOAT\", \"name\": \"current\", \"count\": 1, \"max\": 12, \"min\": 8 },
                        { \"type\": \"INT\", \"name\": \"voltage\", \"max\": 225, \"min\": 215 },
                        { \"type\": \"FLOAT\", \"name\": \"phase\", \"max\": 1, \"min\": 0 }
                    ],
                    \"tags\": [
                        {\"type\": \"TINYINT\", \"name\": \"groupid\", \"max\": 10, \"min\": 1},
                        {\"type\": \"BINARY\",  \"name\": \"location\", \"len\": 16,
                            \"values\": [\"San Francisco\", \"Los Angles\", \"San Diego\",
                                \"San Jose\", \"Palo Alto\", \"Campbell\", \"Mountain View\",
                                \"Sunnyvale\", \"Santa Clara\", \"Cupertino\"]
                        }
                    ]
                }
            ]
        }
    ]
}" > insert-1.json
taosBenchmark -y -f insert-1.json > /dev/null 2>&1

echo "3. 使用taosX备份当前数据库到本地文件"
mkdir -p $BACKUP_DIR
$TAOSX_CMD run --from "tmq+ws://127.0.0.1:6041/$DB_NAME?upcoming=now" --to "local:$BACKUP_DIR" > /dev/null 2>&1


echo "4. 再向数据库写入 1 万条数据"
START_DATETIME=$(date -d "9 days ago" "+%Y-%m-%d 00:00:00.000")
echo "{
    \"filetype\": \"insert\",
    \"cfgdir\": \"/etc/taos\",
    \"host\": \"127.0.0.1\",
    \"port\": 6030,
    \"user\": \"root\",
    \"password\": \"taosdata\",
    \"databases\": [
        {
            \"dbinfo\": {
                \"name\": \"$DB_NAME\",
                \"drop\": \"no\"
            },
            \"super_tables\": [
                {
                    \"name\": \"meters\",
                    \"child_table_exists\": \"yes\",
                    \"childtable_count\": 10000,
                    \"insert_rows\": 1,
                    \"start_timestamp\": \"$START_DATETIME\",
                    \"columns\": [
                        {\"type\": \"FLOAT\", \"name\": \"current\", \"count\": 1, \"max\": 12, \"min\": 8 },
                        { \"type\": \"INT\", \"name\": \"voltage\", \"max\": 225, \"min\": 215 },
                        { \"type\": \"FLOAT\", \"name\": \"phase\", \"max\": 1, \"min\": 0 }
                    ],
                    \"tags\": [
                        {\"type\": \"TINYINT\", \"name\": \"groupid\", \"max\": 10, \"min\": 1},
                        {\"type\": \"BINARY\",  \"name\": \"location\", \"len\": 16,
                            \"values\": [\"San Francisco\", \"Los Angles\", \"San Diego\",
                                \"San Jose\", \"Palo Alto\", \"Campbell\", \"Mountain View\",
                                \"Sunnyvale\", \"Santa Clara\", \"Cupertino\"]
                        }
                    ]
                }
            ]
        }
    ]
}" > insert-2.json
taosBenchmark -y -f insert-2.json > /dev/null 2>&1

echo "5. 等待超过 WAL_RETENTION_PERIOD + 1min 的时长"
sleep $WAL_RETENTION_PERIOD
sleep 60

echo "6. 再向数据库写入 1 万条数据，触发清理 WAL"
START_DATETIME=$(date -d "8 days ago" "+%Y-%m-%d 00:00:00.000")
echo "{
    \"filetype\": \"insert\",
    \"cfgdir\": \"/etc/taos\",
    \"host\": \"127.0.0.1\",
    \"port\": 6030,
    \"user\": \"root\",
    \"password\": \"taosdata\",
    \"databases\": [
        {
            \"dbinfo\": {
                \"name\": \"$DB_NAME\",
                \"drop\": \"no\"
            },
            \"super_tables\": [
                {
                    \"name\": \"meters\",
                    \"child_table_exists\": \"yes\",
                    \"childtable_count\": 10000,
                    \"insert_rows\": 1,
                    \"start_timestamp\": \"$START_DATETIME\",
                    \"columns\": [
                        {\"type\": \"FLOAT\", \"name\": \"current\", \"count\": 1, \"max\": 12, \"min\": 8 },
                        { \"type\": \"INT\", \"name\": \"voltage\", \"max\": 225, \"min\": 215 },
                        { \"type\": \"FLOAT\", \"name\": \"phase\", \"max\": 1, \"min\": 0 }
                    ],
                    \"tags\": [
                        {\"type\": \"TINYINT\", \"name\": \"groupid\", \"max\": 10, \"min\": 1},
                        {\"type\": \"BINARY\",  \"name\": \"location\", \"len\": 16,
                            \"values\": [\"San Francisco\", \"Los Angles\", \"San Diego\",
                                \"San Jose\", \"Palo Alto\", \"Campbell\", \"Mountain View\",
                                \"Sunnyvale\", \"Santa Clara\", \"Cupertino\"]
                        }
                    ]
                }
            ]
        }
    ]
}" > insert-3.json
taosBenchmark -y -f insert-3.json > /dev/null 2>&1

echo "7. 再次用taosX备份增量数据"
$TAOSX_CMD run --from "tmq+ws://127.0.0.1:6041/$DB_NAME?upcoming=now" --to "local:$BACKUP_DIR" > /dev/null 2>&1

echo "8. 使用 taosX 恢复备份数据到新的数据库"
TOPIC_NAME=$(ls -l $BACKUP_DIR | grep .z | awk '{print $NF}' | awk -F'-' '{print $1}' | tail -1)
$TAOSX_CMD run --from "local:$BACKUP_DIR?db_name=$DB_NAME&db_sql=CREATE DATABASE \`$DB_NAME\` WAL_RETENTION_PERIOD $WAL_RETENTION_PERIOD&stable_name=meters&stable_sql=CREATE STABLE \`$DB_NAME\`.meters (ts TIMESTAMP, current FLOAT , voltage INT, phase FLOAT) TAGS (groupid TINYINT, location VARCHAR(16))&topic=$TOPIC_NAME&to=now" --to "taos+ws://127.0.0.1:6041/${DB_NAME}2" > /dev/null 2>&1

echo "9. 对比原数据库和备份数据"
taos -s "select count(*) from \`$DB_NAME\`.meters; select count(*) from \`${DB_NAME}2\`.meters"
