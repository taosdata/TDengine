#!/bin/bash

# TDengine Service Startup Script
# Function: Start TDengine related services and create necessary nodes

# Configuration variables
prefix="taos"
versionType="enterprise"
mode="full"
version="3.3.7.0"
cfg_dir="/etc/taos"
MAX_RETRY=3
OS_TYPE=$(uname)
TDENGINE_CLI="taos -c ${cfg_dir}"

# Define service list
if [ "${versionType}" = "enterprise" ] && [ "${mode}" = "full" ]; then
    SERVICES=("${prefix}d" "${prefix}adapter" "${prefix}x" "${prefix}-explorer" "${prefix}keeper")
else
    SERVICES=("${prefix}d" "${prefix}adapter" "${prefix}-explorer" "${prefix}keeper")
fi

# Start service function
start_service() {
    local service="$1"
    local retry=0
    local status=0
    
    if [ "${OS_TYPE}" = "Linux" ]; then
        # Use systemctl for Linux
        local sysctl_cmd="systemctl"
        if [ "$(id -u)" -ne 0 ]; then
            sysctl_cmd="systemctl --user"
        fi
        
        printf "%-25s" "Starting $service..."
        ${sysctl_cmd} start "${service}" >/dev/null 2>&1 || status=$?
        
        # Check service status
        while [ ${retry} -lt ${MAX_RETRY} ]; do
            sleep 0.5
            if ${sysctl_cmd} is-active "${service}" >/dev/null 2>&1; then
                echo -e "\r✓ $service started successfully"
                return 0
            fi
            retry=$((retry + 1))
        done
        
    elif [ "${OS_TYPE}" = "Darwin" ]; then
        # Use launchctl for macOS
        local domain
        domain="gui/$(id -u)"
        if [ "$(id -u)" -eq 0 ]; then
            domain="system"
        fi
        
        launchctl start "com.tdengine.${service}" >/dev/null 2>&1 || status=$?
        
        # Check service status
        while [ ${retry} -lt ${MAX_RETRY} ]; do
            sleep 0.5
            if launchctl print "${domain}/com.tdengine.${service}" 2>/dev/null | grep -q 'state = running'; then
                echo -e "\r✓ $service started successfully"
                return 0
            fi
            retry=$((retry + 1))
        done
    fi
    
    echo -e "\r✗ Failed to start $service"
    return ${status:-1}
}

# Check TDengine connectivity
check_connectivity() {
    ${TDENGINE_CLI} -s "select server_status();" >/dev/null 2>&1
}

# Create snode if needed
create_snode_if_needed() {
    local snode_flag="${cfg_dir}/snode_flag"
    local snode_tmp
    snode_tmp=$(mktemp /tmp/snodes.XXXXXX)
    trap 'rm -f "${snode_tmp}"' RETURN

    # Check if already created
    if [ -f "${snode_flag}" ] && grep -q "^snode 1$" "${snode_flag}"; then
        return 0
    fi
    
    # Check existing snodes
    if ! ${TDENGINE_CLI} -s "show snodes;" > "${snode_tmp}" 2>/dev/null; then
        echo "Error: Failed to query snodes."
        return 1
    fi
    
    # Create snode if none exists
    if grep -q "0 row" "${snode_tmp}"; then
        echo "Creating snode"
        if ! ${TDENGINE_CLI} -s "create snode on dnode 1;" >/dev/null 2>&1; then
            echo " Error: Failed to create snode on dnode 1."
            return 2
        fi
        echo "✓ Snode created successfully"
    fi
    
    # Mark as created
    echo "snode 1" > "${snode_flag}"
    return 0
}

# Create xnode if needed
create_xnode_if_needed() {
    local xnode_flag="${cfg_dir}/xnode_flag"
    local xnode_tmp
    xnode_tmp=$(mktemp /tmp/xnodes.XXXXXX)
    trap 'rm -f "${xnode_tmp}"' RETURN
    
    # Read configuration
    local server_fqdn
    server_fqdn=$(grep -E "^fqdn" "${cfg_dir}/taos.cfg" | awk '{print $2}')
    server_fqdn=${server_fqdn:-localhost}
    local taosx_server_port=6055
    local xnode_user="${XNODE_USER:-root}"
    local xnode_pass="${XNODE_PASS:-taosdata}"
    
    # Validate username to prevent SQL injection via identifier
    if [[ ! "${xnode_user}" =~ ^[a-zA-Z0-9_]+$ ]]; then
        echo "Error: Invalid xnode_user '${xnode_user}'"
        return 1
    fi
    
    # Validate port is numeric
    if ! [[ "${taosx_server_port}" =~ ^[0-9]+$ ]]; then  
        echo "Error: Invalid taosx server port '${taosx_server_port}'."
        return 2  
    fi  
    
    # Check if already created
    if [ -f "${xnode_flag}" ] && grep -q "^xnode 1$" "${xnode_flag}"; then
        return 0
    fi
    
    # Check existing xnodes
    if ! ${TDENGINE_CLI} -s "show xnodes;" > "${xnode_tmp}" 2>/dev/null; then
        echo "Error: Failed to query xnodes."
        return 3
    fi
    
    # Create xnode if none exists
    if grep -q "0 row" "${xnode_tmp}"; then
        echo "Creating xnode..."
        
        # Escape single quotes in values used as SQL string literals
        local safe_server_fqdn=${server_fqdn//\'/\'\'}
        local safe_xnode_pass=${xnode_pass//\'/\'\'}
        
        local create_sql="CREATE XNODE '${safe_server_fqdn}:${taosx_server_port}' USER ${xnode_user} PASS '${safe_xnode_pass}';"
        local redacted_sql="CREATE XNODE '${safe_server_fqdn}:${taosx_server_port}' USER ${xnode_user} PASS '******';"
        
        if ! ${TDENGINE_CLI} -s "${create_sql}" >/dev/null 2>&1; then
            echo "Error: Failed to create xnode: ${redacted_sql}"
            return 4
        fi
        echo "✓ xnode created successfully"
    fi
    
    # Mark as created
    echo "xnode 1" > "${xnode_flag}"
    return 0
}

# Main function
main() {
    echo "TDengine Service Starter - Version ${version}"
    echo "Services to start: ${SERVICES[*]}"
    echo ""
    
    # Start all services
    for service in "${SERVICES[@]}"; do
        start_service "${service}"
    done
    
    echo ""
    echo "Waiting for TDengine to be ready..."
    sleep 5
    
    # Check connectivity
    if ! check_connectivity; then
        echo "Error: TDengine server is not available, please check the server status."
        exit 1
    fi
    echo "✓ TDengine server is available"
    
    # Create snode if needed
    create_snode_if_needed || echo "Warning: create snode failed, but continue."
    
    # Create xnode if needed
    if printf '%s\n' "${SERVICES[@]}" | grep -q "^${prefix}x$"; then
        create_xnode_if_needed || echo "Warning: create xnode failed, but continue."
    fi
    
    echo ""
    echo "All operations completed"
}

# Execute main function
main