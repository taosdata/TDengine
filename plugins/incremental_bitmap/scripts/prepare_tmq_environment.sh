#!/bin/bash

# TDengine TMQ环境准备脚本
# 用于Offset语义验证的真实测试

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
TDENGINE_HOST="127.0.0.1"
TDENGINE_PORT="6030"
TDENGINE_USER="root"
TDENGINE_PASS="taosdata"
DATABASE="test_db"
STABLE="test_stable"
TABLE_PREFIX="test_table"
TOPIC_NAME="test_topic"
GROUP_NAME="test_group"

# taos 配置文件选项：仅当存在时才添加 -c 选项，避免阻塞或连接失败
TAOS_C_OPT=""
if [ -f /tmp/taos.cfg ]; then
    TAOS_C_OPT="-c /tmp/taos.cfg"
elif [ -f /etc/taos/taos.cfg ]; then
    TAOS_C_OPT="-c /etc/taos/taos.cfg"
elif [ -f "$HOME/.taos/taos.cfg" ]; then
    TAOS_C_OPT="-c $HOME/.taos/taos.cfg"
fi

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查TDengine连接
check_tdengine_connection() {
    log_info "检查TDengine连接..."
    
    if ! taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "SELECT 1;" > /dev/null 2>&1; then
        log_error "无法连接到TDengine，请确保服务正在运行"
        log_info "启动命令: sudo systemctl start taosd"
        exit 1
    fi
    
    log_success "TDengine连接成功"
}

# 创建数据库和表
create_database_and_tables() {
    log_info "创建数据库和表..."
    
    # 创建数据库
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "CREATE DATABASE IF NOT EXISTS $DATABASE;"
    log_success "数据库 $DATABASE 创建成功"
    
    # 使用数据库
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "USE $DATABASE;"
    
    # 创建超级表
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "
        CREATE STABLE IF NOT EXISTS $STABLE (
            ts TIMESTAMP,
            value DOUBLE,
            tag1 INT,
            tag2 VARCHAR(20)
        ) TAGS (
            device_id VARCHAR(20),
            location VARCHAR(50)
        );
    "
    log_success "超级表 $STABLE 创建成功"
    
    # 创建子表
    for i in {1..5}; do
        taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "
            CREATE TABLE IF NOT EXISTS ${TABLE_PREFIX}_$i USING $STABLE TAGS (
                'device_$i',
                'location_$i'
            );
        "
    done
    log_success "子表创建成功"
}

# 插入测试数据
insert_test_data() {
    log_info "插入测试数据..."
    
    # 使用数据库
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "USE $DATABASE;"
    
    # 为每个子表插入数据
    for i in {1..5}; do
        log_info "为表 ${TABLE_PREFIX}_$i 插入数据..."
        
        # 插入历史数据
        taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "
            INSERT INTO ${TABLE_PREFIX}_$i VALUES 
            (NOW() - 1d, $i.1, $i, 'tag1_$i', 'device_$i', 'location_$i'),
            (NOW() - 12h, $i.2, $i, 'tag2_$i', 'device_$i', 'location_$i'),
            (NOW() - 6h, $i.3, $i, 'tag3_$i', 'device_$i', 'location_$i'),
            (NOW() - 1h, $i.4, $i, 'tag4_$i', 'device_$i', 'location_$i'),
            (NOW() - 30m, $i.5, $i, 'tag5_$i', 'device_$i', 'location_$i');
        "
        
        # 插入实时数据（持续插入）
        taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "
            INSERT INTO ${TABLE_PREFIX}_$i VALUES 
            (NOW(), $i.6, $i, 'tag6_$i', 'device_$i', 'location_$i');
        "
    done
    
    log_success "测试数据插入完成"
}

# 创建TMQ Topic
create_tmq_topic() {
    log_info "创建TMQ Topic..."
    
    # 使用数据库
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "USE $DATABASE;"
    
    # 创建Topic
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "
        CREATE TOPIC IF NOT EXISTS $TOPIC_NAME AS 
        SELECT * FROM $STABLE;
    "
    log_success "TMQ Topic $TOPIC_NAME 创建成功"
}

# 验证TMQ Topic
verify_tmq_topic() {
    log_info "验证TMQ Topic..."
    
    # 使用数据库
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "USE $DATABASE;"
    
    # 查看Topic列表
    log_info "Topic列表:"
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "SHOW TOPICS;"
    
    # 查看Topic详情
    log_info "Topic $TOPIC_NAME 详情:"
    taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS $TAOS_C_OPT -s "DESCRIBE TOPIC $TOPIC_NAME;"
}

# 启动持续数据插入（后台进程）
start_continuous_data_insertion() {
    log_info "启动持续数据插入（后台进程）..."
    
    # 创建后台插入脚本
    cat > /tmp/continuous_insert.sh << 'EOF'
#!/bin/bash
TDENGINE_HOST="127.0.0.1"
TDENGINE_PORT="6030"
TDENGINE_USER="root"
TDENGINE_PASS="taosdata"
DATABASE="test_db"
STABLE="test_stable"
TABLE_PREFIX="test_table"

while true; do
    for i in {1..5}; do
        taos -h $TDENGINE_HOST -P $TDENGINE_PORT -u $TDENGINE_USER -p $TDENGINE_PASS -c /tmp/taos.cfg -s "
            USE $DATABASE;
            INSERT INTO ${TABLE_PREFIX}_$i VALUES 
            (NOW(), $i.$(($RANDOM % 100 + 1)), $i, 'tag_$(($RANDOM % 1000))', 'device_$i', 'location_$i');
        " > /dev/null 2>&1
    done
    sleep 1
done
EOF
    
    chmod +x /tmp/continuous_insert.sh
    
    # 启动后台进程
    nohup /tmp/continuous_insert.sh > /tmp/continuous_insert.log 2>&1 &
    CONTINUOUS_PID=$!
    
    echo $CONTINUOUS_PID > /tmp/continuous_insert.pid
    log_success "持续数据插入已启动 (PID: $CONTINUOUS_PID)"
    log_info "日志文件: /tmp/continuous_insert.log"
    log_info "停止命令: kill \$(cat /tmp/continuous_insert.pid)"
}

# 显示环境信息
show_environment_info() {
    log_info "环境信息:"
    echo "  TDengine主机: $TDENGINE_HOST:$TDENGINE_PORT"
    echo "  数据库: $DATABASE"
    echo "  超级表: $STABLE"
    echo "  子表前缀: $TABLE_PREFIX"
    echo "  TMQ Topic: $TOPIC_NAME"
    echo "  消费者组: $GROUP_NAME"
    echo ""
    log_info "测试准备完成！现在可以运行Offset语义验证测试了。"
    echo ""
    log_info "运行测试命令:"
    echo "  cd /home/hp/TDengine/plugins/incremental_bitmap/build"
    echo "  ./test_offset_semantics_real"
    echo ""
    log_info "停止持续数据插入:"
    echo "  kill \$(cat /tmp/continuous_insert.pid)"
}

# 清理函数
cleanup() {
    log_info "清理资源..."
    
    # 停止持续数据插入
    if [ -f /tmp/continuous_insert.pid ]; then
        PID=$(cat /tmp/continuous_insert.pid)
        if kill -0 $PID 2>/dev/null; then
            kill $PID
            log_success "已停止持续数据插入进程 (PID: $PID)"
        fi
        rm -f /tmp/continuous_insert.pid
    fi
    
    # 清理临时文件
    rm -f /tmp/continuous_insert.sh
    rm -f /tmp/continuous_insert.log
    
    log_success "清理完成"
}

# 主函数
main() {
    log_info "开始准备TDengine TMQ环境..."
    
    # 设置信号处理
    trap cleanup EXIT INT TERM
    
    # 检查连接
    check_tdengine_connection
    
    # 创建数据库和表
    create_database_and_tables
    
    # 插入测试数据
    insert_test_data
    
    # 创建TMQ Topic
    create_tmq_topic
    
    # 验证TMQ Topic
    verify_tmq_topic
    
    # 启动持续数据插入
    start_continuous_data_insertion
    
    # 显示环境信息
    show_environment_info
    
    log_success "TMQ环境准备完成！"
}

# 运行主函数
main "$@"


