#!/bin/bash
# define default timezone
DEFAULT_TIMEZONE="Asia/Shanghai"

# Define default DNS server
DEFAULT_DNS="192.168.1.252"

# Define the packages to be installed
SYSTEM_TOOLS="git wget vim gdb screen tmux ntp tree atop iotop sysstat fio tcpdump iperf3 qemu-guest-agent dstat linux-tools-common linux-tools-generic jq zip unzip cloud-guest-utils"

# Define the packages to be installed for build TDinternal
BUILD_TOOLS="llvm gcc make cmake perl g++ lzma curl locales psmisc sudo libgeos-dev libgoogle-glog-dev rsync libjemalloc-dev openssh-server sshpass net-tools dirmngr gnupg apt-transport-https \
            ca-certificates software-properties-common r-base iputils-ping  build-essential git libssl-dev libgflags2.2 libgflags-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config"

# Define the packages to be installed via pip
PIP_PKGS="pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog taospy taos-ws-py numpy uv poetry"

# Define jdk version to be installed
JDK_VERSION="openjdk-17-jdk"

# Define the codename of the Ubuntu release
CODENAME=$(lsb_release -sc)

# Define the version of the Ubuntu release
UBUNTU_VERSION=$(lsb_release -sr)

# Define the path where the core dump files should be stored
COREPATH="/corefile"

# Define the path to the .bashrc file
BASH_RC=$HOME/.bashrc

# Define the path to the Cargo configuration file
CARGO_CONFIG_FILE=$HOME/.cargo/config

# Define jmeter version to be installed
JMETER_VERSION="5.6.3"

# Define the path where the Prometheus binary should exist
PROMETHEUS_BINARY="/usr/local/bin/prometheus"

# Define the path where the Node Exporter binary should exist
NODE_EXPORTER_BINARY="/usr/local/bin/node_exporter"

# Define the path where the Process Exporter binary should exist
PROCESS_EXPORTER_BINARY="/usr/local/bin/process-exporter"

read -r -d '' CLOUD_INIT_CONFIG << 'EOF'
datasource:
  NoCloud:
    seedfrom: /var/lib/cloud/seed/nocloud/
    meta-data: {}
    user-data: {}
    vendor-data: {}
  ConfigDrive: {}
  None: {}
datasource_list: [ NoCloud, ConfigDrive, None ]
EOF

read -r -d '' CUSTOM_SETTINGS <<'EOF'
export LC_CTYPE="en_US.UTF-8"
export HISTTIMEFORMAT="%d/%m/%y %T "
parse_git_branch() {
    git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/(\1)/'
}
export PS1="\u@\h \[\e[32m\]\w \[\e[91m\]\$(parse_git_branch)\[\e[00m\]$ "
EOF

read -r -d '' CARGO_CONFIG <<'EOF'
[source.crates-io]
replace-with = 'rsproxy-sparse'
[source.rsproxy]
registry = "https://rsproxy.cn/crates.io-index"
[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"
[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"
[net]
git-fetch-with-cli = true
[source.tuna]
registry = "https://mirrors.tuna.tsinghua.edu.cn/git/crates.io-index.git"
[source.ustc]
registry = "git://mirrors.ustc.edu.cn/crates.io-index"
[source.sjtu]
registry = "https://mirrors.sjtug.sjtu.edu.cn/git/crates.io-index"
[source.rustcc]
registry = "git://crates.rustcc.cn/crates.io-index"
EOF

# Help function to display usage information
help() {
    echo "Usage: $0 [option]"
    echo "Options:"
    echo "  --help                      - Display this help and exit"
    echo "  setup_all                   - Setup all configurations and installations"
    echo "  config_ssh                  - Configure SSH settings"
    echo "  config_cloud_init           - Set cloud initialization parameters"
    echo "  deploy_git                  - Deploy git repositories"
    echo "  replace_sources             - Replace package sources"
    echo "  upgrade                     - Upgrade the system"
    echo "  config_timezone             - Configure the system timezone"
    echo "  config_dns                  - Set DNS configurations"
    echo "  config_custom_settings      - Add custom settings to your shell configuration"
    echo "  install_packages            - Install specified packages"
    echo "  config_system_limits        - Configure system limits and kernel parameters"
    echo "  config_coredump             - Configure core dump settings"
    echo "  disable_service             - Disable specified services"
    echo "  install_python              - Install Python and pip"
    echo "  install_java                - Install Java"
    echo "  install_maven               - Install Maven"
    echo "  deploy_go                   - Deploy Go environment"
    echo "  deploy_rust                 - Deploy Rust environment"
    echo "  install_nodejs              - Install Node.js"
    echo "  install_nginx               - Install NGINX"
    echo "  config_qemu_guest_agent     - Configure QEMU guest agent"
    echo "  deploy_docker               - Deploy Docker"
    echo "  deploy_docker_compose       - Deploy Docker Compose"
    echo "  clone_enterprise            - Clone the enterprise repository"
    echo "  clone_community             - Clone the community repository"
    echo "  clone_taosx                 - Clone TaosX repository"
    echo "  clone_operation             - Clone operation tools repository"
    echo "  system_config               - Perform system configuration"
    echo "  deploy_dev                  - Deploy development environment"
}

replace_sources() {
    if grep -q "mirrors.aliyun.com" /etc/apt/sources.list; then
        echo "The Aliyun mirror is already set."
    else
        echo "Backing up the original sources.list..."
        cp /etc/apt/sources.list /etc/apt/sources.list.bak

        echo "Replacing sources.list with the Aliyun mirror..."
        tee /etc/apt/sources.list << EOF
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-proposed main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ $CODENAME-backports main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ $CODENAME main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ $CODENAME-security main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ $CODENAME-updates main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ $CODENAME-proposed main restricted universe multiverse
deb-src http://mirrors.aliyun.com/ubuntu/ $CODENAME-backports main restricted universe multiverse
EOF

        echo "Updating repositories..."
        apt-get update -y

        echo "The sources have been replaced and updated successfully."
    fi
}

upgrade() {
    echo "Upgrading ..."
    apt update -y
    apt upgrade -y
}

config_frontend() {
    echo "Configuring frontend..."
    add_config_if_not_exist "export DEBIAN_FRONTEND=noninteractive" $BASH_RC
    upgrade
    systemctl restart dbus.service network-dispatcher.service
}

# Adds a configuration to a file if it does not already exist
add_config_if_not_exist() {
    local config="$1"
    local file="$2"
    grep -qF -- "$config" "$file" || echo "$config" >> "$file"
}

# General error handling function
check_status() {
    local message_on_failure="$1"
    local message_on_success="$2"
    local exit_code="$3"
    local red='\033[0;31m'   # Red color
    local green='\033[0;32m' # Green color
    local no_color='\033[0m' # No color

    if [ "${exit_code:-0}" -ne 0 ]; then
        echo -e "${red}${message_on_failure}${no_color}"
        exit 1
    else
        echo -e "${green}${message_on_success}${no_color}"
    fi
}

install_packages() {
    echo "Installing $package..."
    apt-get update -y
    install_via_apt $SYSTEM_TOOLS
    install_via_apt $BUILD_TOOLS
}

install_via_apt() {
    local green='\033[0;32m'  # Green color
    local red='\033[0;31m'    # Red color
    local no_color='\033[0m'  # Reset to default color
    local yellow='\033[0;33m' # Yellow color

    for package in "$@"; do
        echo -e "${yellow}Installing $package...${no_color}"
        if ! dpkg -s "$package" &> /dev/null; then
            DEBIAN_FRONTEND=noninteractive apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" "$package"
            if [ $? -eq 0 ]; then
                echo -e "${green}Install $package successfully.${no_color}"
            else
                echo -e "${red}Failed to install $package.${no_color}"
            fi
        else
            echo -e "${green}$package is already installed.${no_color}"
        fi
    done
}

# Modifies SSH configuration and sets the root password
config_ssh() {
    echo "Configuring SSH to allow root login..."
    sed -i 's/^#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
    sed -i 's/^PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

    echo "Restarting SSH..."
    if [ "$UBUNTU_VERSION" = "24.04" ]; then
        systemctl restart ssh
    else
        systemctl restart sshd
    fi
    check_status "Failed to restart SSH" "SSH restarted successfully." $?
}

# Sets the timezone
config_timezone() {
    echo "Setting timezone to $DEFAULT_TIMEZONE..."
    timedatectl set-timezone "$DEFAULT_TIMEZONE"
    check_status "Failed to set timezone" "Timezone set to $DEFAULT_TIMEZONE successfully." $?
}

# Disables service
disable_service() {
    echo "Stop and disable and related services..."
    systemctl stop apt-daily.service apt-daily-upgrade.service apt-daily-upgrade.timer apt-daily.timer unattended-upgrades
    systemctl disable apt-daily.service apt-daily-upgrade.service apt-daily-upgrade.timer apt-daily.timer unattended-upgrades
    check_status "Failed to disable related services" "Related services disabled successfully." $?
}

# Config DNS settings
config_dns() {
    echo "Configuring DNS settings to use $DEFAULT_DNS..."
    add_config_if_not_exist "[Resolve]" /etc/systemd/resolved.conf
    add_config_if_not_exist "DNS=$DEFAULT_DNS" /etc/systemd/resolved.conf
    systemctl restart systemd-resolved.service
    check_status "Failed to configure DNS" "DNS configured to use $DEFAULT_DNS successfully." $?
}

# Config qemu-guest-agent
config_qemu_guest_agent() {
    install_via_apt "qemu-guest-agent"
    echo "Configuring qemu-guest-agent..."
    systemctl enable qemu-guest-agent
    systemctl start qemu-guest-agent
    check_status "Failed to configure qemu-guest-agent" "Qemu-guest-agent configured successfully." $?
}

# Config custom settings
config_custom_settings() {
    echo "Configuring custom settings..."
    marker="parse_git_branch"

    if grep -qF "$marker" "$BASH_RC"; then
        echo "Configuration already exists in "$BASH_RC"."
    else
        echo "Adding configuration to "$BASH_RC"."
        echo "$CUSTOM_SETTINGS" >>"$BASH_RC"
        echo "Custom settings have been updated in your $BASH_RC file."
    fi
    check_status "Failed to apply custom settings" "Custom settings configured successfully." $?
}

# Config core dump settings
config_coredump() {
    echo "Configuring core dump directory..."
    mkdir -p $COREPATH
    add_config_if_not_exist "kernel.core_pattern=$COREPATH/core_%e-%p" /etc/sysctl.conf
    add_config_if_not_exist "ulimit -n 600000" $BASH_RC
    add_config_if_not_exist "ulimit -c unlimited" $BASH_RC
    sysctl -p
    check_status "Failed to apply core dump settings" "Core path:$COREPATH applied successfully." $?
}

# Modifies system resource limits and TCP/IP core parameters
config_system_limits() {
    echo "Configuring system limits and kernel parameters..."
    local sysctl_conf="/etc/sysctl.conf"
    local limits_conf="/etc/security/limits.conf"

    add_config_if_not_exist "fs.nr_open = 1048576" $sysctl_conf
    add_config_if_not_exist "net.core.somaxconn=10240" $sysctl_conf
    add_config_if_not_exist "net.core.netdev_max_backlog=20480" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_max_syn_backlog=10240" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_retries2=5" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_syn_retries=2" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_synack_retries=2" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_tw_reuse=1" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_keepalive_time=600" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_abort_on_overflow=1" $sysctl_conf
    add_config_if_not_exist "net.ipv4.tcp_max_tw_buckets=5000" $sysctl_conf

    sysctl -p
    check_status "Failed to apply sysctl settings" "Apply sysctl settings successfully." $?

    for limit in "soft nproc 65536" "soft nofile 65536" "soft stack 65536" "hard nproc 65536" "hard nofile 65536" "hard stack 65536"; do
        add_config_if_not_exist "* $limit" $limits_conf
        add_config_if_not_exist "root $limit" $limits_conf
    done
    check_status "Failed to apply limits settings" "Apply limits settings successfully." $?
}

# install pkg via pip
install_pip_pkg() {
    echo "Installing $PIP_PKGS ..."
    install_via_pip $PIP_PKGS
}

install_via_pip() {
    for package in "$@"; do
        echo "pip install $package..."
        pip3 install "$package" -i https://pypi.tuna.tsinghua.edu.cn/simple
        if [ $? -eq 0 ]; then
            echo "pip install $package successfully."
        else
            echo "Failed to install $package."
        fi
    done
}

# Install Python and pip
install_python() {
    install_via_apt "python3"
    install_via_apt "python3-pip"
}

# Install Java
install_java() {
    install_via_apt "$JDK_VERSION"
}

# Install Maven
install_maven() {
    install_via_apt "maven"
}

# Install Go
deploy_go() {
    # Define the installation location for Go
    GO_INSTALL_DIR="/usr/local/go"
    GOPATH_DIR="/root/go"

    # Check if Go is installed
    if command -v go >/dev/null 2>&1; then
        echo "Go is already installed. Skipping installation."
        return
    fi

    # Fetch the latest version number of Go
    GO_LATEST_DATA=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://golang.google.cn/VERSION?m=text)
    GO_LATEST_VERSION=$(echo "$GO_LATEST_DATA" | grep -oP 'go[0-9]+\.[0-9]+\.[0-9]+')
    # Download and install the latest version of Go
    echo "Installing $GO_LATEST_VERSION..."
    wget https://golang.google.cn/dl/$GO_LATEST_VERSION.linux-amd64.tar.gz -O go.tar.gz

    # Extract to the specified directory
    tar -C /usr/local -xzf go.tar.gz
    rm -rf go.tar.gz

    # Configure environment variables using the helper function
    add_config_if_not_exist "export GOROOT=$GO_INSTALL_DIR" $BASH_RC
    add_config_if_not_exist "export GOPATH=$GOPATH_DIR" $BASH_RC
    add_config_if_not_exist "export PATH=\$PATH:\$GOROOT/bin" $BASH_RC
    add_config_if_not_exist "export GO111MODULE=on" $BASH_RC
    add_config_if_not_exist "export GOPROXY=https://nexus.tdengine.net/repository/goproxy/,direct" $BASH_RC

    # Apply the environment variables
    $GO_INSTALL_DIR/bin/go version
    check_status "Failed to install GO" "Install GO successfully" $?
}

# Function to install Rust and Cargo
deploy_rust() {
    # Check if Rust is already installed
    if ! command -v rustc &> /dev/null; then
        # add_config_if_not_exist "export RUSTUP_DIST_SERVER=http://mirrors.ustc.edu.cn/rust-static" $BASH_RC
        # export RUSTUP_DIST_SERVER=http://mirrors.ustc.edu.cn/rust-static

        add_config_if_not_exist "export RUSTUP_DIST_SERVER="https://rsproxy.cn"" $BASH_RC
        add_config_if_not_exist "export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"" $BASH_RC
        export RUSTUP_DIST_SERVER="https://rsproxy.cn"
        export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"

        echo "Rust is not installed. Installing Rust and Cargo..."
        # Download and install Rust and Cargo
        curl --retry 10 --retry-delay 5 --retry-max-time 120 --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y

        echo "Cargo settings..."
        marker="git-fetch-with-cli"

        if grep -qF "$marker" "$CARGO_CONFIG_FILE"; then
            echo "Configuration already exists in "$CARGO_CONFIG_FILE"."
        else
            echo "Adding configuration to "$CARGO_CONFIG_FILE"."
            echo "$CARGO_CONFIG" >>"$CARGO_CONFIG_FILE"
            echo "Cargo config have been updated in your $CARGO_CONFIG_FILE file."
        fi

        # Source the Cargo environment script to update the current shell
        source $HOME/.cargo/env
        # Check if the installation was successful
        rustc --version
        # Install cargo-make
        cargo install cargo-make
        check_status "Failed to install Rust" "Install Rust successfully" $?
    else
        echo "Rust is already installed."
    fi
}

# Install Node.js npm Yarn
install_nodejs() {
    install_via_apt "nodejs"
    node --version
    check_status "Failed to install Node.js" "Node.js installed successfully." $?
    install_via_apt "npm"
    npm --version
    check_status "Failed to install npm" "npm installed successfully." $?
    npm config set fetch-retry-maxtimeout 120000
    npm config set fetch-retry-attempts
    npm config set fetch-retry-factor 5
    npm config set registry=https://registry.npmmirror.com
    npm install --global yarn
    yarn --version
    check_status "Failed to install Yarn" "Yarn installed successfully." $?
}

# Deploy Git
deploy_git() {
    install_via_apt "git"
    git --version
    check_status "Failed to install Git" "Git installed successfully." $?
    git config --global user.name "taos-support"
    git config --global user.email "it@taosdata.com"
    git config --global credential.helper store
}

# Install Nginx
install_nginx() {
    install_via_apt "nginx"
    nginx -v
    check_status "Failed to install Nginx" "Nginx installed successfully." $?
}


deploy_docker() {
    # Check if Docker is already installed
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Installing now..."

        # Set up the repository for Docker
        echo "Setting up the Docker repository..."
        apt update -y
        install_via_apt apt-transport-https ca-certificates curl software-properties-common gnupg lsb-release

        echo "Adding Docker's official GPG key from Aliyun..."
        curl --retry 10 --retry-delay 5 --retry-max-time 120 -fsSL https://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | {
            if [ -f /usr/share/keyrings/docker-archive-keyring.gpg ]; then
                rm /usr/share/keyrings/docker-archive-keyring.gpg
            fi
            gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
        }
        echo "Setting up stable repository using Aliyun..."
        echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://mirrors.aliyun.com/docker-ce/linux/ubuntu \
        $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

        # Install Docker CE
        echo "Installing Docker CE..."
        apt update -y
        install_via_apt docker-ce docker-ce-cli containerd.io

        # Enable and start Docker
        echo "Enabling and starting Docker..."
        systemctl enable docker
        systemctl start docker

        # Adding current user to the Docker group
        usermod -aG docker $USER

        # Print Docker version
        docker --version
        check_status "Failed to install Docker" "Docker installed successfully." $?
    else
        echo "Docker is already installed."
    fi
}

deploy_docker_compose() {
    # Check if Docker Compose is installed
    if ! command -v docker-compose &> /dev/null; then
        echo "Docker Compose is not installed. Installing now..."

        # Install Docker Compose
        curl --retry 10 --retry-delay 5 --retry-max-time 120 -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        chmod +x /usr/local/bin/docker-compose

        # Print Docker Compose version
        docker-compose --version
        check_status "Failed to install Docker Compose" "Docker Compose installed successfully." $?
    else
        echo "Docker Compose is already installed."
    fi
}

reconfig_cloud_init() {
    echo "Reconfiguring cloud-init..."
    apt remove -y cloud-init && apt purge -y cloud-init
    rm -rf /var/lib/cloud /etc/cloud
    apt update -y
    install_via_apt cloud-init
    sed -i '/package[-_]update[-_]upgrade[-_]install/s/^/#/' /etc/cloud/cloud.cfg
    package_update_upgrade_install
}

config_cloud_init() {
    if [ "$UBUNTU_VERSION" = "18.04" ] || [ "$UBUNTU_VERSION" = "20.04" ]; then
        reconfig_cloud_init
    else
        echo "Configuring cloud-init..."
        add_config_if_not_exist "$CLOUD_INIT_CONFIG" "/etc/cloud/cloud.cfg"

        marker="NoCloud"

        if grep -qF "$marker" "/etc/cloud/cloud.cfg"; then
            echo "cloud-init settings already exists in "/etc/cloud/cloud.cfg"."
        else
            echo "Adding configuration to "/etc/cloud/cloud.cfg"."
            echo "$CLOUD_INIT_CONFIG" >> "/etc/cloud/cloud.cfg"
            echo "cloud-init settings have been updated in /etc/cloud/cloud.cfg."
        fi

        mkdir -p /var/lib/cloud/seed/nocloud/
        cd /var/lib/cloud/seed/nocloud/
        touch meta-data
        touch user-data
        add_config_if_not_exist "hostname: \${name}" user-data
        add_config_if_not_exist "manage_etc_hosts: true" user-data
    fi
    cloud-init clean --logs
    check_status "Failed to configure cloud-init" "Cloud-init configured successfully and you need reboot manually." $?
}

clone_repo_with_rename() {
  local repo_url="$1"
  local target_dir="$2"

  if [ -z "$target_dir" ]; then
    target_dir=$(basename -s .git "$repo_url")
  fi

  cd $HOME

  if [ -d "$target_dir" ]; then
    echo "Directory $target_dir already exists. Skipping clone."
  else
    echo "Cloning into $target_dir..."
    git clone "$repo_url" "$target_dir"
  fi
}

clone_enterprise() {
    # Clone the enterprise repository
    cd $HOME
    clone_repo_with_rename https://github.com/taosdata/TDinternal
    # Clone the community repository
    clone_repo_with_rename git@github.com:taosdata/TDengine.git TDinternal/community
}

clone_community() {
    # Clone the community repository
    cd $HOME
    clone_repo_with_rename https://github.com:taosdata/TDengine.git
}

clone_taosx() {
    # Clone the taosx repository
    cd $HOME
    clone_repo_with_rename https://github.com/taosdata/taosx
}

clone_taoskeeper() {
    # Clone the taoskeeper repository
    cd $HOME
    clone_repo_with_rename https://github.com/taosdata/taoskeeper
}


clone_taostest() {
    # Clone the taostest repository
    cd $HOME
    clone_repo_with_rename https://github.com/taosdata/taos-test-framework
    clone_repo_with_rename https://github.com/taosdata/TestNG taos-test-framework/TestNG
}

clone_operation() {
    # Clone the operation repository
    cd $HOME
    clone_repo_with_rename https://github.com/taosdata/operation.git
}

# init system
system_config() {
    export DEBIAN_FRONTEND=noninteractive
    disable_service
    config_dns
    config_timezone
    config_ssh
    config_cloud_init
    replace_sources
    config_frontend
    config_custom_settings
    config_system_limits
    config_coredump
    check_status "Failed to config system" "Config system successfully" $?
}

# Clone all the repositories
clone_repos() {
    clone_enterprise
    clone_community
    clone_taosx
    clone_taoskeeper
    clone_taostest
    clone_operation
}

# Deploy pure environment
deploy_pure() {
    export DEBIAN_FRONTEND=noninteractive
    disable_service
    config_dns
    config_cloud_init
    install_via_apt "jq"
    deploy_node_exporter
    check_status "Failed to config pure system" "Config pure system successfully" $?
}

# Deploy development environment
deploy_dev() {
    export DEBIAN_FRONTEND=noninteractive
    install_packages
    deploy_git
    install_python
    install_pip_pkg
    install_java
    install_maven
    deploy_go
    deploy_rust
    install_nodejs
    install_nginx
    deploy_docker
    deploy_docker_compose
    check_status "Failed to deploy some tools" "Deploy all tools successfully" $?
}

setup_all() {
    system_config
    deploy_dev
}

# More installation functions can be added here following the above examples

# Main execution function
main() {
    # Check if at least one argument is provided
    if [ $# -eq 0 ]; then
        echo "Error: No arguments provided."
        echo "Please try $0 --help"
        exit 1
    fi

    for arg in "$@"; do
        case $arg in
            --help)
                help
                exit 0
                ;;
            setup_all)
                setup_all
                ;;
            config_ssh)
                config_ssh
                ;;
            config_cloud_init)
                config_cloud_init
                ;;
            deploy_git)
                deploy_git
                ;;
            replace_sources)
                replace_sources
                ;;
            upgrade)
                upgrade
                ;;
            config_timezone)
                config_timezone
                ;;
            config_dns)
                config_dns
                ;;
            config_custom_settings)
                config_custom_settings
                ;;
            install_packages)
                install_packages
                ;;
            config_system_limits)
                config_system_limits
                ;;
            config_coredump)
                config_coredump
                ;;
            disable_service)
                disable_service
                ;;
            install_python)
                install_python
                ;;
            install_pip_pkg)
                install_pip_pkg
                ;;
            install_java)
                install_java
                ;;
            install_maven)
                install_maven
                ;;
            deploy_go)
                deploy_go
                ;;
            deploy_rust)
                deploy_rust
                ;;
            install_nodejs)
                install_nodejs
                ;;
            install_nginx)
                install_nginx
                ;;
            config_qemu_guest_agent)
                config_qemu_guest_agent
                ;;
            deploy_docker)
                deploy_docker
                ;;
            deploy_docker_compose)
                deploy_docker_compose
                ;;
            clone_enterprise)
                clone_enterprise
                ;;
            clone_community)
                clone_community
                ;;
            clone_taosx)
                clone_taosx
                ;;
            clone_taoskeeper)
                clone_taoskeeper
                ;;
            clone_taostest)
                clone_taostest
                ;;
            clone_operation)
                clone_operation
                ;;
            system_config)
                system_config
                ;;
            deploy_pure)
                deploy_pure
                ;;
            deploy_dev)
                deploy_dev
                ;;
            *)
                echo "Unknown function: $arg"
                ;;
        esac
    done
}

# Execute the script with specified function arguments
main "$@"