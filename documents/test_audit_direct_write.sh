#!/bin/bash

# 审计直接写入功能测试脚本

echo "=========================================="
echo "审计直接写入功能测试"
echo "=========================================="

# 检查TDengine是否运行
echo "1. 检查TDengine服务状态..."
systemctl status taosd | grep "Active:" || echo "请确保taosd服务正在运行"

# 备份原配置
echo ""
echo "2. 备份原配置文件..."
if [ -f /etc/taos/taos.cfg ]; then
    cp /etc/taos/taos.cfg /etc/taos/taos.cfg.backup.$(date +%Y%m%d_%H%M%S)
    echo "配置文件已备份"
fi

# 添加配置
echo ""
echo "3. 配置审计直接写入..."
cat >> /etc/taos/taos.cfg << EOF

# 审计直接写入配置
audit 1
auditDirectWrite 1
auditLevel 3
auditInterval 5000
EOF

echo "配置已添加到 /etc/taos/taos.cfg"

# 重启服务
echo ""
echo "4. 重启TDengine服务..."
echo "请手动执行: systemctl restart taosd"
echo ""

# 测试SQL
echo "5. 测试SQL脚本已生成: /tmp/test_audit.sql"
cat > /tmp/test_audit.sql << 'EOF'
-- 创建测试数据库
CREATE DATABASE IF NOT EXISTS test_audit_db;

-- 使用测试数据库
USE test_audit_db;

-- 创建测试表
CREATE TABLE IF NOT EXISTS test_table (ts TIMESTAMP, value INT);

-- 插入测试数据
INSERT INTO test_table VALUES (NOW, 100);

-- 查询测试数据
SELECT * FROM test_table;

-- 删除测试表
DROP TABLE IF EXISTS test_table;

-- 删除测试数据库
DROP DATABASE IF EXISTS test_audit_db;

-- 等待审计记录写入
SELECT SLEEP(2);

-- 查询审计记录
USE audit;
SELECT * FROM operations ORDER BY ts DESC LIMIT 10;

-- 统计操作类型
SELECT operation, COUNT(*) as count FROM operations GROUP BY operation;

-- 查询最近的创建数据库操作
SELECT * FROM operations WHERE operation LIKE '%CREATE%' ORDER BY ts DESC LIMIT 5;
EOF

echo ""
echo "=========================================="
echo "测试步骤："
echo "1. 重启taosd服务: systemctl restart taosd"
echo "2. 执行测试SQL: taos -s /tmp/test_audit.sql"
echo "3. 查看审计记录是否正常写入"
echo "=========================================="
echo ""
echo "验证命令："
echo "taos -s \"USE audit; SELECT COUNT(*) FROM operations;\""
echo "taos -s \"USE audit; SELECT * FROM operations ORDER BY ts DESC LIMIT 10;\""
echo "=========================================="
