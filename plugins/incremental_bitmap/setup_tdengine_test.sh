#!/bin/bash

# TDengine 测试环境设置脚本
# 解决SQL语法问题，正确创建测试环境

echo "🔧 设置TDengine测试环境"
echo "========================"

# 检查TDengine服务
if ! pgrep -f taosd > /dev/null; then
    echo "❌ 错误：TDengine服务未运行"
    echo "   请先启动：sudo systemctl start taosd"
    exit 1
fi

echo "✅ TDengine服务正在运行"

# 设置TDengine配置路径
TAOS_CFG="/home/hp/TDengine/build/test/cfg/taos.cfg"
TAOS_BIN="/home/hp/TDengine/build/build/bin/taos"

if [ ! -f "$TAOS_CFG" ]; then
    echo "❌ 错误：找不到TDengine配置文件：$TAOS_CFG"
    exit 1
fi

if [ ! -f "$TAOS_BIN" ]; then
    echo "❌ 错误：找不到TDengine客户端：$TAOS_BIN"
    exit 1
fi

echo "✅ 找到TDengine配置文件：$TAOS_CFG"
echo "✅ 找到TDengine客户端：$TAOS_BIN"

# 创建测试数据库和表
echo ""
echo "📋 创建测试数据库和表..."

# 使用正确的TDengine语法
$TAOS_BIN -c "$TAOS_CFG" -s "
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, v INT) TAGS (t INT);
CREATE TABLE IF NOT EXISTS d0 USING meters TAGS (1);
CREATE TABLE IF NOT EXISTS d1 USING meters TAGS (2);
CREATE TABLE IF NOT EXISTS d2 USING meters TAGS (3);
CREATE TOPIC IF NOT EXISTS incremental_backup_topic AS DATABASE test;
"

if [ $? -eq 0 ]; then
    echo "✅ 测试环境创建成功"
else
    echo "❌ 测试环境创建失败"
    exit 1
fi

# 插入测试数据
echo ""
echo "📊 插入测试数据..."
$TAOS_BIN -c "$TAOS_CFG" -s "
USE test;
INSERT INTO d0 VALUES (now, 1);
INSERT INTO d0 VALUES (now+1s, 2);
INSERT INTO d0 VALUES (now+2s, 3);
INSERT INTO d1 VALUES (now, 10);
INSERT INTO d1 VALUES (now+1s, 20);
INSERT INTO d1 VALUES (now+2s, 30);
INSERT INTO d2 VALUES (now, 100);
INSERT INTO d2 VALUES (now+1s, 200);
INSERT INTO d2 VALUES (now+2s, 300);
"

if [ $? -eq 0 ]; then
    echo "✅ 测试数据插入成功"
else
    echo "❌ 测试数据插入失败"
    exit 1
fi

# 验证数据
echo ""
echo "🔍 验证测试数据..."
$TAOS_BIN -c "$TAOS_CFG" -s "
USE test;
SELECT COUNT(*) FROM d0;
SELECT COUNT(*) FROM d1;
SELECT COUNT(*) FROM d2;
"

echo ""
echo "🎉 TDengine测试环境设置完成！"
echo "   数据库：test"
echo "   超级表：meters"
echo "   子表：d0, d1, d2"
echo "   TMQ主题：incremental_backup_topic"
echo "   测试数据：已插入"
