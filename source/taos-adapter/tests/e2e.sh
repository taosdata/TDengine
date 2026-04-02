#!/bin/bash
set -xe

mkdir -p ./coverage

export GOCOVERDIR=./coverage

taosadapter_cmd="./taosadapter"
taosadapter_config="/etc/taos/taosadapter.toml"
taosadapter_tmp_config="/etc/taos/taosadapter_tmp.toml"

if [ "$OS" = "Windows_NT" ]; then
    echo "This is Windows system"
    taosadapter_cmd="./taosadapter.exe"
    taosadapter_config="C:/TDengine/cfg/taosadapter.toml"
    taosadapter_tmp_config="C:/TDengine/cfg/taosadapter_tmp.toml"
    mkdir -p C:/TDengine/cfg
else
    echo "This is not Windows (likely Unix/Linux/macOS)"
fi

ldd $taosadapter_cmd
# Display version
$taosadapter_cmd --version

# help message
$taosadapter_cmd --help

# Common functions
start_taosadapter() {
    local config_file=$1
    if [ -z "$config_file" ]; then
        # No parameter provided, don't use -c flag
        nohup $taosadapter_cmd > taosadapter.log 2>&1 &
        echo "TaosAdapter started with PID: $! (no config file)"
    else
        # Parameter provided, use -c flag
        nohup $taosadapter_cmd -c "$config_file" > taosadapter.log 2>&1 &
        echo "TaosAdapter started with PID: $! (config: $config_file)"
    fi
    TAOS_PID=$!
    echo $TAOS_PID > taosadapter.pid
    sleep 2
}

stop_taosadapter() {
    if [ ! -z "$TAOS_PID" ]; then
        echo "Stopping TaosAdapter with PID: $TAOS_PID"
        kill -2 $TAOS_PID
        wait $TAOS_PID 2>/dev/null || true
        rm -f taosadapter.pid
        TAOS_PID=""
    fi
}

test_connection() {
    local expected_code=$1
    local description=$2
    local sql=$3
    local response_code=$(curl -u root:taosdata -s -o /dev/null -w "%{http_code}" 127.0.0.1:6041/rest/sql -d "$sql")

    if [ "$response_code" -ne "$expected_code" ]; then
        echo "FAILED: $description. Expected: $expected_code, Got: $response_code"
        return 1
    else
        echo "SUCCESS: $description. Response code: $response_code"
        return 0
    fi
}

clear_config() {
    local config_file=$1
    > "$config_file"  # Clear the config file
    echo "Cleared config file: $config_file"
    sleep 2
}

update_config() {
    local config_file=$1
    cat > "$config_file" << 'EOF'
rejectQuerySqlRegex = ['(?i)^show\s+databases.*','(?i)^select\s+.*from\s+testdb.*']
[log]
level = "debug"
EOF
    echo "Updated config file: $config_file"
    sleep 2
}

# Test default configuration
echo "=== Testing default configuration ==="
# Clear config first, then update
clear_config $taosadapter_config
start_taosadapter  # No parameter, don't use -c

# Test initial connection
test_connection 200 "Initial connection to TDengine RESTful API" "show databases" || exit 1
test_connection 200 "Initial connection to TDengine RESTful API" "select * from testdb.testtb" || exit 1

update_config $taosadapter_config
test_connection 403 "Connection after adding rejectQuerySqlRegex" "show databases" || exit 1
test_connection 403 "Connection after adding rejectQuerySqlRegex" "select * from testdb.testtb" || exit 1

stop_taosadapter

# Test temporary configuration
# Clear config first, then update
clear_config $taosadapter_tmp_config
echo -e "\n=== Testing temporary configuration ==="
start_taosadapter $taosadapter_tmp_config  # With parameter, use -c

# Test initial connection
test_connection 200 "Initial connection with temp config" "show databases" || exit 1
test_connection 200 "Initial connection with temp config" "select * from testdb.testtb" || exit 1

update_config $taosadapter_tmp_config
test_connection 403 "Connection after updating temp config" "show databases" || exit 1
test_connection 403 "Connection after updating temp config" "select * from testdb.testtb" || exit 1

sleep 10

response=$(curl -u root:taosdata -s -w "\n%{http_code}" 127.0.0.1:6041/rest/sql -d "select * from performance_schema.perf_instances where type = 'taosadapter'")
http_code=$(echo "$response" | tail -n1)
response_body=$(echo "$response" | head -n -1)

if [ "$http_code" -eq 200 ]; then
    rows=$(echo "$response_body" | jq -r '.rows // empty')

    if [ "$rows" = "1" ]; then
        echo "success: rows value is 1 as expected"
    else
        echo "failed: rows value is $rows"
        exit 1
    fi
else
    echo "failed to get performance schema info, HTTP code: $http_code"
    exit 1
fi
stop_taosadapter

echo -e "\n=== All tests passed successfully ==="