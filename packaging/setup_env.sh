#!/bin/bash

# Run cleanup function on exit
trap cleanup EXIT

# define default timezone
DEFAULT_TIMEZONE="Asia/Shanghai"

# Define default DNS server
DEFAULT_DNS="192.168.1.252"

# Define the packages to be installed
SYSTEM_APT_TOOLS="git wget vim gdb screen tmux ntp tree atop iotop sysstat fio tcpdump iperf3 qemu-guest-agent dstat linux-tools-common linux-tools-generic jq zip unzip cloud-guest-utils nfs-kernel-server nfs-common"
SYSTEM_YUM_TOOLS="git wget vim gdb screen tmux ntp tree atop iotop sysstat fio tcpdump iperf3 qemu-guest-agent dstat jq zip unzip cloud-utils-growpart python3-devel nfs-utils rpm-build automake autoconf libevent-devel ncurses-devel"

# Define the packages to be installed for build TDinternal
BUILD_APT_TOOLS="llvm gcc make cmake perl g++ lzma curl locales psmisc libgeos-dev libgoogle-glog-dev valgrind rsync libjemalloc-dev openssh-server sshpass net-tools dirmngr gnupg apt-transport-https \
            ca-certificates software-properties-common r-base iputils-ping  build-essential git libssl-dev libgflags2.2 libgflags-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config"
BUILD_YUM_TOOLS="gcc make cmake3 perl gcc-c++ xz curl psmisc geos glog valgrind rsync jemalloc openssh-server sshpass net-tools gnupg2 libarchive snappy-devel pkgconfig libatomic perl-IPC-Cmd libcurl-devel libxml2-devel\
            ca-certificates libicu-devel R-core iputils bison flex glibc-static libstdc++-static libstdc++-devel openssl-devel gflags jansson jansson-devel snappy xz-devel zlib-devel zlib bzip2-devel zlib-static libs3"

# Define the packages to be installed via pip
PIP_PKGS="wheel setuptools-rust pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog taospy numpy poetry"

# Gcc version to be updated
GCC_VERSION="9.5.0"

# Define the version of the Ubuntu release
# Define jdk version to be installed
if [ -f /etc/debian_version ]; then
    DIST_VERSION=$(lsb_release -sr)
    JDK_VERSION="openjdk-17-jdk"
elif [ -f /etc/redhat-release ]; then
    DIST_VERSION=$(grep -oP '\d+\.\d+' < /etc/redhat-release)
    JDK_VERSION="java-1.8.0-openjdk"
else
    echo "Unsupported Linux distribution."
    exit 1
fi

# Define the path where the core dump files should be stored
COREPATH="/corefile"

# Define the path where the repository should be cloned
REPOPATH="$HOME/repos"

# Define the path to the script directory
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Define the path to the .bashrc file
BASH_RC=$HOME/.bashrc

# Define the path to the Cargo configuration file
CARGO_CONFIG_FILE=$HOME/.cargo/config.toml

# Define jmeter version to be installed
JMETER_VERSION="5.6.3"

# Define the path where the Prometheus binary should exist
PROMETHEUS_BINARY="/usr/local/bin/prometheus"

# Define the path where the Node Exporter binary should exist
NODE_EXPORTER_BINARY="/usr/local/bin/node_exporter"

# Define the path where the Process Exporter binary should exist
PROCESS_EXPORTER_BINARY="/usr/local/bin/process-exporter"

# Define fstab input
FSTAB_LINE="share-server.platform.tdengine.dev:/mnt/share_server /mnt/share_server nfs rw,sync,_netdev 0 0"

# Results need to be stored when source
SOURCE_RESULTS=""

# ANSI color codes
GREEN='\033[0;32m'  # Green color
RED='\033[0;31m'    # Red color
NO_COLOR='\033[0m'  # Reset to default color
YELLOW='\033[0;33m' # Yellow color

# read -r -d '' CLOUD_INIT_CONFIG << 'EOF'
# datasource:
#   NoCloud:
#     seedfrom: /var/lib/cloud/seed/nocloud/
#     meta-data: {}
#     user-data: {}
#     vendor-data: {}
#   ConfigDrive: {}
#   None: {}
# datasource_list: [ NoCloud, ConfigDrive, None ]
# EOF

read -r -d '' CUSTOM_SETTINGS <<'EOF'
export LC_CTYPE="en_US.UTF-8"
export LANG="en_US.UTF-8"
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
    echo "  TDasset                     - Prepare TDasset env"
    echo "  TDinternal                  - Prepare TDinternal env"
    echo "  TDgpt                       - Prepare TDgpt env"
    echo "  taostest                    - Prepare taostest env"
    echo "  system_config               - Perform system configuration"
    echo "  deploy_pure                 - Deploy Pure environment"
    echo "  deploy_dev                  - Deploy development environment"
    echo "  deploy_cmake                - Deploy CMake"
    echo "  update_redhat_gcc           - Update GCC on Red Hat or CentOS"
    echo "  update_redhat_tmux          - Update tmux on Red Hat or CentOS"
    echo "  deploy_tmux                 - Deploy tmux"
    echo "  config_ssh                  - Configure SSH settings"
    echo "  disable_firewalld           - Disable firewalld"
    echo "  config_cloud_init           - Set cloud initialization parameters"
    echo "  deploy_git                  - Deploy git repositories"
    echo "  replace_sources             - Replace package sources"
    echo "  update                      - Update the system"
    echo "  upgrade                     - Upgrade the system"
    echo "  config_timezone             - Configure the system timezone"
    echo "  config_dns                  - Set DNS configurations"
    echo "  config_custom_settings      - Add custom settings to your shell configuration"
    echo "  config_share_server         - Configure share server"
    echo "  install_packages            - Install specified packages"
    echo "  config_system_limits        - Configure system limits and kernel parameters"
    echo "  config_coredump             - Configure core dump settings"
    echo "  disable_service             - Disable specified services"
    echo "  install_python              - Install Python and pip"
    echo "  install_pyenv               - Install Pyenv"
    echo "  install_python_via_pyenv    - Install Python via pyenv"
    echo "  install_java                - Install Java"
    echo "  install_java_via_sdkman     - Install Java via sdkman"
    echo "  install_maven_via_sdkman    - Install Maven via sdkman"
    echo "  deploy_go                   - Deploy Go environment"
    echo "  install_gvm                 - Install GVM"
    echo "  install_go_via_gvm          - Install Go via GVM"
    echo "  deploy_rust                 - Deploy Rust environment"
    echo "  install_node                - Install Node via package manager or binary"
    echo "  install_node_via_nvm        - Install Node via NVM"
    echo "  install_pnpm                - Install PNPM, node version >=v18.12.00 required"
    echo "  deploy_node_exporter        - Deploy Node Exporter for Prometheus"
    echo "  deploy_process_exporter     - Deploy Process Exporter"
    echo "  deploy_prometheus           - Deploy Prometheus"
    echo "  deploy_grafana              - Deploy Grafana"
    echo "  deploy_jmeter               - Deploy JMeter"
    echo "  install_nginx               - Install NGINX"
    echo "  config_qemu_guest_agent     - Configure QEMU guest agent"
    echo "  deploy_docker               - Deploy Docker"
    echo "  deploy_docker_compose       - Deploy Docker Compose"
    echo "  install_trivy               - Install Trivy"
    echo "  install_uv                  - Install uv"
    echo "  clone_enterprise            - Clone the enterprise repository"
    echo "  clone_community             - Clone the community repository"
    echo "  clone_taosx                 - Clone TaosX repository"
    echo "  clone_taoskeeper            - Clone TaosKeeper repository"
    echo "  clone_taostest              - Clone TaosTest repository"
    echo "  clone_operation             - Clone operation tools repository"
}

replace_apt_sources() {
    # Define the codename of the Ubuntu release
    local CODENAME
    CODENAME=$(lsb_release -sc)
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
    fi
    echo "Updating repositories..."
    apt-get update -y
    echo "The sources have been replaced and updated successfully."
}

replace_yum_sources() {
    if grep -q "mirrors.aliyun.com" /etc/yum.repos.d/CentOS-Base.repo; then
        echo "The Aliyun mirror is already set."
    else
        echo "Backing up the original CentOS-Base.repo..."
        cp /etc/yum.repos.d/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo.bak

        echo "Replacing CentOS-Base.repo with the Aliyun mirror..."
        tee /etc/yum.repos.d/CentOS-Base.repo << 'EOF'
[base]
name=CentOS-$releasever - Base - Aliyun
baseurl=http://mirrors.aliyun.com/centos/$releasever/os/$basearch/
gpgcheck=1
gpgkey=http://mirrors.aliyun.com/centos/RPM-GPG-KEY-CentOS-7

#released updates
[updates]
name=CentOS-$releasever - Updates - Aliyun
baseurl=http://mirrors.aliyun.com/centos/$releasever/updates/$basearch/
gpgcheck=1
gpgkey=http://mirrors.aliyun.com/centos/RPM-GPG-KEY-CentOS-7

#additional packages that may be useful
[extras]
name=CentOS-$releasever - Extras - Aliyun
baseurl=http://mirrors.aliyun.com/centos/$releasever/extras/$basearch/
gpgcheck=1
gpgkey=http://mirrors.aliyun.com/centos/RPM-GPG-KEY-CentOS-7
EOF
    fi
    echo "Updating repositories..."
    yum makecache fast
    yum install epel-release -y
    yum update -y

    echo "The sources have been replaced and updated successfully."
}

replace_sources() {
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Replacing sources for APT package manager."
        replace_apt_sources
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Replacing sources for YUM package manager."
        replace_yum_sources
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

update() {
    echo "Updating ..."
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Using APT package manager."
        apt update -y
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Using YUM package manager."
        yum install epel-release -y
        yum update -y
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

upgrade() {
    echo "Upgrading ..."
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Using APT package manager."
        apt upgrade -y
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Using YUM package manager."
        yum upgrade -y
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

config_frontend() {
    # only ubuntu need
    if [ -f /etc/debian_version ]; then
        echo "Configuring frontend..."
        add_config_if_not_exist "export DEBIAN_FRONTEND=noninteractive" "$BASH_RC"
    fi
    update
    upgrade
    systemctl restart dbus.service network-dispatcher.service

}

# Adds a configuration to a file if it does not already exist
add_config_if_not_exist() {
    local config="$1"
    local file="$2"
    grep -qF -- "$config" "$file" || echo "$config" >> "$file"
}

# Function to check if a tool is installed
check_installed() {
    local command_name="$1"
    if command -v "$command_name" >/dev/null 2>&1; then
        echo "$command_name is already installed. Skipping installation."
        return 0
    else
        echo "$command_name is not installed."
        return 1
    fi
}
# General error handling function
check_status() {
    local message_on_failure="$1"
    local message_on_success="$2"
    local exit_code="$3"

    if [ "${exit_code:-0}" -ne 0 ]; then
        echo -e "${RED}${message_on_failure}${NO_COLOR}"
        exit 1
    else
        echo -e "${GREEN}${message_on_success}${NO_COLOR}"
    fi
}

# Config Share-NFS server
config_share_server() {
    echo "Configuring share server..."
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Using APT package manager."
        install_package "nfs-common"
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Using YUM package manager."
        install_package "nfs-utils"
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
    mkdir -p /mnt/share_server
    add_config_if_not_exist "$FSTAB_LINE" /etc/fstab
    mount -a
    check_status "Failed to configure share server" "Share server configured successfully." $?
}

# Init environment
init_env() {
    export DEBIAN_FRONTEND=noninteractive
    export LC_CTYPE="en_US.UTF-8"
}

# Install packages
install_packages() {
    echo "Installing $package..."
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Using APT package manager."
        install_package $SYSTEM_APT_TOOLS
        install_package $BUILD_APT_TOOLS
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Using YUM package manager."
        yum install epel-release -y
        install_package $SYSTEM_YUM_TOOLS
        install_package $BUILD_YUM_TOOLS
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Install package
install_package() {
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Using APT package manager."
        install_via_apt "$@"
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Using YUM package manager."
        install_via_yum "$@"
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Install package via apt
install_via_apt() {
    echo -e "${YELLOW}Installing packages: $*...${NO_COLOR}"
    if ! DEBIAN_FRONTEND=noninteractive apt-get install -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" "$@"; then
        if ! DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" "$@"; then
            echo "Attempting to update and install $package..."
            apt update -y
            if DEBIAN_FRONTEND=noninteractive apt-get install -y --fix-missing -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" "$@"; then
                    echo -e "${GREEN}Installed packages successfully.${NO_COLOR}"
            else
                    echo -e "${RED}Failed to install packages.${NO_COLOR}"
                    return 1
            fi
        fi
    fi
}

# Install package via yum
install_via_yum() {
    echo -e "${YELLOW}Installing packages: $*...${NO_COLOR}"
    if yum install -y "$@"; then
        echo -e "${GREEN}Installed packages successfully.${NO_COLOR}"
    else
        echo -e "${RED}Failed to install packages.${NO_COLOR}"
        return 1
    fi
}

# disable and stop firewalld
disable_firewalld() {
    if [ -f /etc/debian_version ]; then
        # Only Debian or Ubuntu
        echo "ufw disable..."
        ufw disable
        check_status "Failed to disable ufw" "Ufw disabled successfully." $?
    elif [ -f /etc/redhat-release ]; then
        # Only Red Hat or CentOS
        echo "Disabling firewalld..."
        systemctl stop firewalld
        systemctl disable firewalld
        check_status "Failed to disable firewalld" "Firewalld disabled successfully." $?
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Modifies SSH configuration and sets the root password
config_ssh() {
    echo "Configuring SSH settings..."
    sed -i 's/^#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
    sed -i 's/^PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

    echo "Restarting SSH..."
    if [ "$DIST_VERSION" = "24.04" ]; then
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
    if [ -f /etc/debian_version ]; then
        # Only Debian or Ubuntu
        echo "Stop and disable and related services..."
        systemctl stop apt-daily.service apt-daily-upgrade.service apt-daily-upgrade.timer apt-daily.timer unattended-upgrades
        systemctl disable apt-daily.service apt-daily-upgrade.service apt-daily-upgrade.timer apt-daily.timer unattended-upgrades
        check_status "Failed to disable related services" "Related services disabled successfully." $?
    fi
}

# Config dns for Red Hat or CentOS
config_redhat_dns() {
    local DEFAULT_DNS="192.168.2.99"
    echo "Configuring DNS settings to use $INTERNAL_DNS and $DEFAULT_DNS..."
    echo "nameserver $INTERNAL_DNS" > /etc/resolv.conf
    echo "nameserver $DEFAULT_DNS" >> /etc/resolv.conf
    check_status "Failed to configure DNS" "DNS configured to use $INTERNAL_DNS and $DEFAULT_DNS successfully." $?
}

# Config dns for Debian or Ubuntu
config_debian_dns() {
    local DEFAULT_DNS="192.168.2.99"
    echo "Configuring DNS settings to use $INTERNAL_DNS and $DEFAULT_DNS..."
    systemctl stop systemd-resolved.service
    echo "[Resolve]" > /etc/systemd/resolved.conf
    echo "DNS=$INTERNAL_DNS" >> /etc/systemd/resolved.conf
    echo "DNS=$DEFAULT_DNS" >> /etc/systemd/resolved.conf
    systemctl restart systemd-resolved.service
    ln -sf /run/systemd/resolve/resolv.conf /etc/resolv.conf
    check_status "Failed to configure DNS" "DNS configured to use $INTERNAL_DNS and $DEFAULT_DNS successfully." $?
}

# Config DNS settings
config_dns() {
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        echo "Configuring DNS settings for Debian or Ubuntu..."
        config_debian_dns
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Configuring DNS settings for Red Hat or CentOS..."
        config_redhat_dns
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Config qemu-guest-agent
config_qemu_guest_agent() {
    install_package "qemu-guest-agent"
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
        echo "Configuration already exists in ""$BASH_RC""."
    else
        echo "Adding configuration to ""$BASH_RC""."
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
    add_config_if_not_exist "ulimit -n 600000" "$BASH_RC"
    add_config_if_not_exist "ulimit -c unlimited" "$BASH_RC"
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

# Check the operating system version
centos_skip_check() {
    # Check if the operating system is CentOS 7
    if [[ -f /etc/redhat-release ]]; then
        if grep -q "CentOS Linux release 7" /etc/redhat-release; then
            echo "This platform requires you to manually upgrade gcc and glibc."
            exit 1
        fi
    fi
}

# Other logic can go here...

# Deploy cmake
deploy_cmake() {
    # Check if cmake is installed
    # if command -v cmake >/dev/null 2>&1; then
    #     echo "Cmake is already installed. Skipping installation."
    #     cmake --version
    #     return
    # fi
    if check_installed "cmake"; then
        return
    fi
    install_package "cmake3"
    ln -sf /usr/bin/cmake3 /usr/bin/cmake
    check_status "Failed to install cmake" "Install cmake successfully" $?
}


# install pkg via pip
install_pip_pkg() {
    if [ "$DIST_VERSION" != "24.04" ]; then
        echo "Installing $PIP_PKGS ..."
        pip3 install --upgrade pip setuptools -i https://pypi.tuna.tsinghua.edu.cn/simple
        install_via_pip "$PIP_PKGS"
    fi
}

install_via_pip() {
    echo "pip install $*..."
    if pip3 install $* -i https://pypi.tuna.tsinghua.edu.cn/simple; then
        echo "pip install packages successfully."
    else
        echo "Failed to install packages."
        return 1
    fi
}


# Complie python
download_and_compile_python() {
    if [ -f /etc/debian_version ]; then
        install_package gcc make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev
    elif [ -f /etc/redhat-release ]; then
        # install_package gcc patch libffi libffi-devel python-devel zlib  zlib-devel bzip2-devel openssl-devel openssl11 openssl11-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel
        install_package gcc zlib zlib-devel libffi libffi-devel readline-devel openssl-devel openssl11 openssl11-devel
        # CFLAGS=$(pkg-config --cflags openssl11)
        # export CFLAGS
        # LDFLAGS=$(pkg-config --libs openssl11)
        # export LDFLAGS
        export CFLAGS=$(pkg-config --cflags openssl11)
        export LDFLAGS=$(pkg-config --libs openssl11)
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi

    local VERSION="$1"
    local DOWNLOAD_URL="https://www.python.org/ftp/python/$VERSION/Python-$VERSION.tgz"


    echo "Downloading Python $VERSION from $DOWNLOAD_URL..."
    wget "$DOWNLOAD_URL" -O "/tmp/Python-$VERSION.tgz"
    check_status "Failed to download Python source." "Python source downloaded successfully." $?

    echo "Extracting Python $VERSION..."
    cd /tmp || exit
    tar -xzf "Python-$VERSION.tgz"
    cd "Python-$VERSION" || exit

    echo "Compiling and installing Python $VERSION..."
    ./configure --enable-optimizations --prefix=/usr/local/python"$VERSION"
    make -j"$(nproc)"
    make altinstall

    cd ..
    rm -rf "Python-$VERSION" "Python-$VERSION.tgz"

    MAJOR_MINOR_VERSION=$(echo "$VERSION" | cut -d '.' -f 1-2)

    # files=(
    #     "/usr/bin/python3"
    #     "/usr/bin/python"
    #     "/usr/bin/pip3"
    #     "/usr/bin/pip"
    # )

    # for file in "${files[@]}"; do
    #     if [ -e "$file" ]; then
    #         backup_file="$file.bak"
    #         echo "Backing up $file to $backup_file"
    #         cp "$file" "$backup_file"
    #     else
    #         echo "$file does not exist, skipping."
    #     fi
    # done

    ln -sf /usr/local/python"$VERSION"/bin/python"$MAJOR_MINOR_VERSION" /usr/local/bin/python3
    ln -sf /usr/local/python"$VERSION"/bin/pip"$MAJOR_MINOR_VERSION" /usr/local/bin/pip3
    python3 --version
    check_status "Failed to install Python $VERSION" "Python $VERSION installed successfully." $?
}

upgrage_pip() {
    echo "Upgrading pip..."
    python3 -m pip install --upgrade pip
    check_status "Failed to upgrade pip" "Pip upgraded successfully." $?
}

# Install Python via package_manager
install_python_via_package_manager() {
    if [ -n "$1" ]; then
        PYTHON_PACKAGE="$1"
    else
        PYTHON_PACKAGE="python3"
    fi
    install_package "$PYTHON_PACKAGE"
    install_package "python3-pip"
    upgrage_pip
    python3 --version
}

# Install Python and pip
# shellcheck disable=SC2120
install_python() {
    echo -e "${YELLOW}Installing Python...${NO_COLOR}"
    # Specify the major python version to search for; default is set to 3.10 if not specified
    if [ -n "$1" ]; then
        PYTHON_VERSION="$1"
    else
        install_python_via_package_manager "$PYTHON_PACKAGE"
        exit 0
    fi
    MAJOR_MINOR_VERSION=$(echo "$PYTHON_VERSION" | cut -d '.' -f 1-2)
    # Check if the JDK package is available in the repository
    if [ -f /etc/debian_version ]; then
        PYTHON_PACKAGE="python${MAJOR_MINOR_VERSION}"
        if apt-cache search "$PYTHON_PACKAGE" | grep -w "$PYTHON_PACKAGE"; then
            install_python_via_package_manager "$PYTHON_PACKAGE"
            exit 0
        else
            echo -e "${RED}Failed to install Python using package manager.${NO_COLOR}"
        fi
    elif [ -f /etc/redhat-release ]; then
        PYTHON_PACKAGE="python${MAJOR_MINOR_VERSION//./}"
        if yum list available | grep -w "$PYTHON_PACKAGE"; then
            install_python_via_package_manager "$PYTHON_PACKAGE"
            exit 0
        else
            echo -e "${RED}Failed to install Python using package manager.${NO_COLOR}"
        fi
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi

    echo -e "${YELLOW}$PYTHON_PACKAGE not found in source repository. Attempting to download and install manually....${NO_COLOR}"
    download_and_compile_python "$PYTHON_VERSION"
    upgrage_pip

    # Check installation status
    INSTALLED_VERSION=$(python3 --version 2>&1)
    if echo "$INSTALLED_VERSION" | grep -q "$MAJOR_MINOR_VERSION"; then
        echo -e "${GREEN}Python $MAJOR_MINOR_VERSION installed successfully.${NO_COLOR}"
    else
        echo -e "${YELLOW}Python version not match.${NO_COLOR}"
        exit 1
    fi
}

update_redhat_gcc() {
    echo "Updating the system..."
    update

    echo "Installing dependencies..."
    yum groupinstall -y "Development Tools"
    install_package gmp-devel mpfr-devel libmpc-devel wget

    echo "Downloading GCC $GCC_VERSION..."
    cd /usr/local/src || exit
    wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz
    wget https://ftp.gnu.org/gnu/gcc/gcc-$GCC_VERSION/gcc-$GCC_VERSION.tar.gz.sig

    echo "Extracting GCC $GCC_VERSION..."
    tar -xzf gcc-$GCC_VERSION.tar.gz
    cd gcc-$GCC_VERSION || exit

    echo "Downloading necessary dependencies for GCC..."
    ./contrib/download_prerequisites

    mkdir build
    cd build || exit

    echo "Configuring GCC..."
    ../configure --enable-languages=c,c++ --disable-multilib --prefix=/usr

    echo "Compiling GCC, this may take a while..."
    make -j"$(nproc)"
    make install

    echo "Cleaning up downloaded files..."
    cd /usr/local/src || exit
    rm -rf gcc-$GCC_VERSION gcc-$GCC_VERSION.tar.gz gcc-$GCC_VERSION.tar.gz.sig
    echo "Cleanup completed."

    echo "GCC installation completed. Verifying installation..."
    gcc --version
    check_status "Failed to install GCC" "GCC $GCC_VERSION installed successfully." $?

    # Backup
    if [ -f "/lib64/libstdc++.so.6.0.28-gdb.py" ]; then
        # Copy the file
        mv -f /lib64/libstdc++.so.6.0.28-gdb.py /tmp/libstdc++.so.6.0.28-gdb.py
        echo "File has been successfully moved to /tmp/libstdc++.so.6.0.28-gdb.py"
    else
        echo "File /lib64/libstdc++.so.6.0.28-gdb.py does not exist, cannot perform copy operation."
    fi
}

update_redhat_tmux() {
    echo "Downloading the latest version of tmux..."
    cd /usr/local/src || exit
    latest_tmux_version=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/tmux/tmux/releases/latest | grep -Po '"tag_name": "\K.*?(?=")')
    wget https://github.com/tmux/tmux/releases/download/"${latest_tmux_version}"/tmux-"${latest_tmux_version}".tar.gz

    echo "Extracting tmux ${latest_tmux_version}..."
    tar -xzf tmux-"${latest_tmux_version}".tar.gz
    cd tmux-"${latest_tmux_version}" || exit

    echo "Configuring tmux..."
    ./configure --prefix=/usr

    echo "Compiling tmux, this may take a while..."
    make -j"$(nproc)"
    make install

    echo "Cleaning up downloaded files..."
    cd /usr/local/src || exit
    rm -rf tmux-"${latest_tmux_version}" tmux-"${latest_tmux_version}".tar.gz

    echo "Cleanup completed."

    echo "tmux installation completed. Verifying installation..."
    tmux -V
    check_status "Failed to install tmux" "tmux ${latest_tmux_version} installed successfully." $?
}

deploy_tmux() {
    if [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        update_redhat_tmux
    fi
    echo "Copying configuration file..."

    cp "$SCRIPT_DIR/../conf/tmux.conf" ~/.tmux.conf

    echo "Configuration file copied to ~/.tmux.conf."
}


# Install Java
# install_java() {
#     if command -v java >/dev/null 2>&1; then
#         echo "Java is already installed. Skipping installation."
#         java -version
#         return
#     else
#         echo "Installing $JDK_VERSION..."
#         install_package "$JDK_VERSION"
#         check_status "Failed to install Java" "Install Java successfully" $?
#     fi
# }

# Install Java
# shellcheck disable=SC2120
install_java() {
    echo -e "${YELLOW}Installing Java...${NO_COLOR}"
    # Specify the major JDK version to search for; default is set to 17 if not specified
    if [ -n "$1" ]; then
        DEFAULT_JDK_VERSION="$1"
    else
        DEFAULT_JDK_VERSION="17"
    fi

    # Check if the JDK package is available in the repository
    if [ -f /etc/debian_version ]; then
        JDK_PACKAGE="openjdk-$DEFAULT_JDK_VERSION-jdk"
        if apt-cache search "$JDK_PACKAGE" | grep -q "$JDK_PACKAGE"; then
            echo "Installing $JDK_PACKAGE using apt..."
        fi
    elif [ -f /etc/redhat-release ]; then
        JDK_PACKAGE="java-$DEFAULT_JDK_VERSION-openjdk"
        if yum list available | grep -q "$JDK_PACKAGE"; then
            echo "Installing $JDK_PACKAGE using yum..."
        fi
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi

    # Check installation status
    if ! install_package "$JDK_PACKAGE"; then
        echo -e "${RED}Failed to install Java using package manager.${NO_COLOR}"
    else
        echo -e "${GREEN}Java installed successfully.${NO_COLOR}"
        java -version
        return
    fi

    echo -e "${YELLOW}$JDK_PACKAGE not found in $PACKAGE_MANAGER repository. Attempting to download and install manually....${NO_COLOR}"

    # URL of the archive page to search
    ARCHIVE_URL="https://jdk.java.net/archive/"

    # Get the latest minor version number
    LATEST_VERSION=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s "$ARCHIVE_URL" | \
    grep -o "jdk${DEFAULT_JDK_VERSION}\.[0-9]\+\.[0-9]\+" | \
    sort -V | \
    tail -n 1)
    JDK_VERSION_NUM="${LATEST_VERSION#jdk}"
    # Confirm the latest version found
    if [[ $LATEST_VERSION =~ jdk([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
        # Print the latest version found
        echo -e "${YELLOW}Latest JDK version found: $LATEST_VERSION${NO_COLOR}"
        MATCH_URL="https://download.java.net/java/GA/${LATEST_VERSION}/[^ ]*linux-x64_bin.tar.gz"
        JDK_DOWNLOAD_URL=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s "$ARCHIVE_URL" | \
        grep -o "$MATCH_URL" | \
        head -n 1)
    else
        echo -e "${RED}Failed to find the JDK version: $LATEST_VERSION.${NO_COLOR}"
        exit 1
    fi
    # Download JDK
    echo "Downloading OpenJDK $LATEST_VERSION from $JDK_DOWNLOAD_URL..."
    wget "$JDK_DOWNLOAD_URL" -O /tmp/"${LATEST_VERSION}"_linux-x64_bin.tar.gz
    check_status "Failed to download OpenJDK." "OpenJDK downloaded successfully." $?

    # Extract and install
    echo "Extracting OpenJDK..."
    if [ -d "/usr/local/jdk-${JDK_VERSION_NUM}" ]; then
        rm -rf "/usr/local/jdk-${JDK_VERSION_NUM}"
    fi
    tar -xzf /tmp/"${LATEST_VERSION}"_linux-x64_bin.tar.gz -C /usr/local/
    rm -rf /tmp/"${LATEST_VERSION}"_linux-x64_bin.tar.gz
    # Configure environment variables
    echo "Configuring environment variables..."
    add_config_if_not_exist "export JAVA_HOME=/usr/local/jdk-${JDK_VERSION_NUM}" "$BASH_RC"
    add_config_if_not_exist "export PATH=\$PATH:\$JAVA_HOME/bin" "$BASH_RC"
    # shellcheck source=/dev/null
    export JAVA_HOME=/usr/local/jdk-${JDK_VERSION_NUM}
    export PATH=$JAVA_HOME/bin:$PATH
    INSTALLED_VERSION=$("$JAVA_HOME"/bin/java --version 2>&1)
    if echo "$INSTALLED_VERSION" | grep -q "openjdk $DEFAULT_JDK_VERSION"; then
        echo -e "${GREEN}Java installed successfully.${NO_COLOR}"
        SOURCE_RESULTS+="source /root/.bashrc  # For openjdk\n"
    else
        echo -e "${YELLOW}Java version not match.${NO_COLOR}"
        exit 1
    fi
}

# Install sdkman
install_sdkman() {
    echo -e "${YELLOW}Installing SDKMAN...${NO_COLOR}"
    if [ -d "$HOME/.sdkman" ]; then
        echo -e "${GREEN}SDKMAN is already installed.${NO_COLOR}"
    else
        echo -e "${YELLOW}Installing SDKMAN...${NO_COLOR}"
        install_package zip unzip
        curl --retry 10 --retry-delay 5 --retry-max-time 120 -s "https://get.sdkman.io" | bash
    fi
}

# Install gvm
install_gvm() {
    echo -e "${YELLOW}Installing GVM...${NO_COLOR}"
    if [ -d "$HOME/.gvm" ]; then
        echo -e "${GREEN}GVM is already installed.${NO_COLOR}"
    else
        install_package bison gcc make
        bash < <(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s -S -L https://raw.githubusercontent.com/moovweb/gvm/master/binscripts/gvm-installer)
        source $HOME/.gvm/scripts/gvm
        gvm version
        check_status "Failed to install GVM" "GVM installed successfully." $?
        add_config_if_not_exist "export GO111MODULE=on" "$BASH_RC"
        add_config_if_not_exist "export GOPROXY=https://goproxy.cn,direct" "$BASH_RC"
        add_config_if_not_exist "export GO_BINARY_BASE_URL=https://mirrors.aliyun.com/golang/" "$BASH_RC"
        add_config_if_not_exist "export GOROOT_BOOTSTRAP=$GOROOT" "$BASH_RC"
    fi
    SOURCE_RESULTS+="source $HOME/.gvm/scripts/gvm  # For gvm\n"
}

# enable pyenv
enable_pyenv() {
    export PATH="$HOME/.pyenv/bin:$PATH"
    eval "$(pyenv init --path)"
    eval "$(pyenv init -)"
}

# Install pyenv
install_pyenv() {
    echo -e "${YELLOW}Installing Pyenv...${NO_COLOR}"
    if [ -d "$HOME/.pyenv" ]; then
        echo -e "${GREEN}Pyenv is already installed.${NO_COLOR}"
    else
        curl -L https://gitee.com/xinghuipeng/pyenv-installer/raw/master/bin/pyenv-installer | bash
        enable_pyenv
        add_config_if_not_exist "export PATH=\"\$HOME/.pyenv/bin:\$PATH\"" "$BASH_RC"
        add_config_if_not_exist "eval \"\$(pyenv init --path)\"" "$BASH_RC"
        add_config_if_not_exist "eval \"\$(pyenv init -)\"" "$BASH_RC"
        pyenv --version
        check_status "Failed to install Pyenv" "Pyenv installed successfully." $?
    fi
    SOURCE_RESULTS+="source $BASH_RC  For: pyenv/python\n"
}

# Install python via pyenv
install_python_via_pyenv() {
    echo -e "${YELLOW}Installing Python via Pyenv...${NO_COLOR}"
    if [ -f /etc/debian_version ]; then
        install_package gcc make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev
    elif [ -f /etc/redhat-release ]; then
        install_package gcc zlib zlib-devel libffi libffi-devel readline-devel openssl-devel openssl11 openssl11-devel
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi

    if [ -n "$1" ]; then
        DEFAULT_PYTHON_VERSION="$1"
    else
        DEFAULT_PYTHON_VERSION="3.10.12"
    fi
    install_pyenv
    enable_pyenv
    pyenv install "$DEFAULT_PYTHON_VERSION"
    pyenv global "$DEFAULT_PYTHON_VERSION"
    python --version
    check_status "Failed to install Python" "Python installed successfully." $?
}

# Install Maven
# shellcheck disable=SC2120
install_maven_via_sdkman() {
    echo -e "${YELLOW}Installing maven...${NO_COLOR}"
    if [ -n "$1" ]; then
        DEFAULT_MVN_VERSION="$1"
        install_sdkman
        [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
        # 3.2.5
        yes | sdk install maven "$DEFAULT_MVN_VERSION"
    else
        install_package "maven"
    fi
    [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
    mvn -version
    check_status "Failed to install maven" "Maven installed successfully." $?
}

install_java_via_sdkman() {
    echo -e "${YELLOW}Installing java...${NO_COLOR}"
    if [ -n "$1" ]; then
        DEFAULT_JDK_VERSION="$1"
    else
        DEFAULT_JDK_VERSION="17"
    fi
    install_sdkman
    [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
    yes | sdk install java "$DEFAULT_JDK_VERSION-open"
    [[ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]] && source "$HOME/.sdkman/bin/sdkman-init.sh"
    java -version
    check_status "Failed to install java" "Java installed successfully." $?
    SOURCE_RESULTS+="source $HOME/.sdkman/bin/sdkman-init.sh  # For sdkman/java/maven\n"
}

# Install Go
deploy_go() {
    # Define the installation location for Go
    GO_INSTALL_DIR="/usr/local/go"
    GOPATH_DIR="/root/go"

    # Check if Go is installed
    # if command -v go >/dev/null 2>&1; then
    #     echo "Go is already installed. Skipping installation."
    #     return
    # fi
    if check_installed "go"; then
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
    add_config_if_not_exist "export GOROOT=$GO_INSTALL_DIR" "$BASH_RC"
    add_config_if_not_exist "export GOPATH=$GOPATH_DIR" "$BASH_RC"
    add_config_if_not_exist "export PATH=\$PATH:\$GOROOT/bin" "$BASH_RC"
    add_config_if_not_exist "export GO111MODULE=on" "$BASH_RC"
    add_config_if_not_exist "export GOPROXY=https://goproxy.cn,direct" "$BASH_RC"

    # Apply the environment variables
    $GO_INSTALL_DIR/bin/go version
    check_status "Failed to install GO" "Install GO successfully" $?
    SOURCE_RESULTS+="source $BASH_RC  # For golang\n"
}

# Install Go via gvm
install_go_via_gvm() {
    echo -e "${YELLOW}Installing Go...${NO_COLOR}"
    if [ -n "$1" ]; then
        DEFAULT_GO_VERSION="$1"
    else
        DEFAULT_GO_VERSION="1.23.0"
    fi
    install_gvm
    source $HOME/.gvm/scripts/gvm
    export GO111MODULE=on
    export GOPROXY=https://goproxy.cn,direct
    export GO_BINARY_BASE_URL=https://mirrors.aliyun.com/golang/
    export GOROOT_BOOTSTRAP=$GOROOT

    gvm install go"$DEFAULT_GO_VERSION" -B
    gvm use go"$DEFAULT_GO_VERSION"
    gvm use go"$DEFAULT_GO_VERSION" --default

    go version
    check_status "Failed to install Go" "Go installed successfully." $?
    SOURCE_RESULTS+="source $BASH_RC  # For golang\n"
}

# Function to install Rust and Cargo
deploy_rust() {
    # Check if Rust is already installed
    if ! command -v rustc &> /dev/null; then
        # add_config_if_not_exist "export RUSTUP_DIST_SERVER=http://mirrors.ustc.edu.cn/rust-static" $BASH_RC
        # export RUSTUP_DIST_SERVER=http://mirrors.ustc.edu.cn/rust-static
        if [ -f /etc/debian_version ]; then
            # Debian or Ubuntu
            echo "Using APT package manager."
            install_package build-essential
        elif [ -f /etc/redhat-release ]; then
            # Red Hat or CentOS
            echo "Using YUM package manager."
            yum groupinstall -y "Development Tools"
        else
            echo "Unsupported Linux distribution."
            exit 1
        fi

        add_config_if_not_exist "export RUSTUP_DIST_SERVER=\"https://rsproxy.cn\"" "$BASH_RC"
        add_config_if_not_exist "export RUSTUP_UPDATE_ROOT=\"https://rsproxy.cn/rustup\"" "$BASH_RC"
        export RUSTUP_DIST_SERVER="https://rsproxy.cn"
        export RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"

        echo "Rust is not installed. Installing Rust and Cargo..."
        # Download and install Rust and Cargo
        curl --retry 10 --retry-delay 5 --retry-max-time 120 --proto '=https' --tlsv1.2 -sSf https://rsproxy.cn/rustup-init.sh | sh -s -- -y

        echo "Cargo settings..."
        marker="git-fetch-with-cli"

        if grep -qF "$marker" "$CARGO_CONFIG_FILE"; then
            echo "Configuration already exists in ""$CARGO_CONFIG_FILE""."
        else
            echo "Adding configuration to ""$CARGO_CONFIG_FILE""."
            echo "$CARGO_CONFIG" >>"$CARGO_CONFIG_FILE"
            echo "Cargo config have been updated in your $CARGO_CONFIG_FILE file."
        fi

        # Source the Cargo environment script to update the current shell
        if [ -f "$HOME/.cargo/env" ]; then
            source "$HOME/.cargo/env"
        fi
        # Check if the installation was successful
        rustc --version
        # Install cargo-make
        cargo install cargo-make
        check_status "Failed to install Rust" "Install Rust successfully" $?
        SOURCE_RESULTS+="source $BASH_RC && source $HOME/.cargo/env  # For cargo/rust\n"
    else
        echo "Rust is already installed."
    fi
}

# Update GCC for Ubuntu 18.04
update_ubuntu_gcc_18.04() {
    echo -e "${YELLOW}Updating GCC for Ubuntu 18.04...${NO_COLOR}"
    install_package software-properties-common
    yes | add-apt-repository ppa:ubuntu-toolchain-r/test
    update
    install_package gcc-9 g++-9
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
    check_status "Failed to update GCC" "GCC updated successfully." $?
}

install_node_in_ubuntu18.04() {
    # very slow and not test
    if [ -n "$1" ]; then
        DEFAULT_NODE_VERSION="$1"
    else
        DEFAULT_NODE_VERSION="14"
    fi
    NODE_DISTRO="node-v$DEFAULT_NODE_VERSION-linux-x64"
    update_ubuntu_gcc_18.04
    echo "Installing Node..."
    curl --retry 10 --retry-delay 5 --retry-max-time 120 -O https://nodejs.org/dist/v22.0.0/node-v22.0.0.tar.gz
    tar -xzf node-v22.0.0.tar.gz
    cd node-v22.0.0 || exit
    ./configure
    make
    make install
}


# Install pnpm
install_pnpm() {
    echo -e "${YELLOW}Installing pnpm...${NO_COLOR}"
    centos_skip_check
    NODE_VERSION=$(node -v)

    MAJOR_VERSION=$(echo "$NODE_VERSION" | cut -d '.' -f 1 | tr -d 'v')
    MINOR_VERSION=$(echo "$NODE_VERSION" | cut -d '.' -f 2)
    PATCH_VERSION=$(echo "$NODE_VERSION" | cut -d '.' -f 3)

    VERSION_NUMBER=$((MAJOR_VERSION * 10000 + MINOR_VERSION * 100 + PATCH_VERSION))

    REQUIRED_VERSION=181200 # v18.12.00

    if [ $VERSION_NUMBER -ge $REQUIRED_VERSION ]; then
        echo "Node version is $NODE_VERSION, installing pnpm..."
        npm install --global pnpm
        pnpm --version
        check_status "Failed to install pnpm" "pnpm installed successfully." $?
    else
        echo "Node version is $NODE_VERSION, skipping pnpm installation."
    fi

}

# Install Node via nvm
# shellcheck disable=SC2120
install_node_via_nvm () {
    echo -e "${YELLOW}Installing Node via NVM...${NO_COLOR}"
    if [ -n "$1" ]; then
        DEFAULT_NODE_VERSION="$1"
    else
        DEFAULT_NODE_VERSION=""
    fi

    if [[ -f /etc/redhat-release ]]; then
        if [[ "$1" != "16.20.2" ]]; then
            centos_skip_check
        fi
    fi

    # Install NVM
    if ! command -v nvm &> /dev/null; then
        NVM_VERSION=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/nvm-sh/nvm/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")')
        curl --retry 10 --retry-delay 5 --retry-max-time 120 -o- https://raw.githubusercontent.com/nvm-sh/nvm/"$NVM_VERSION"/install.sh | bash
        export NVM_DIR="$HOME/.nvm"
        [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
        [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
        echo -e "${GREEN}NVM installed successfully.${NO_COLOR}"
        SOURCE_RESULTS+="source $NVM_DIR/nvm.sh && source $NVM_DIR/bash_completion  # For nvm/node/npm/yarn/pnpm\n"
    else
        echo -e "${GREEN}NVM is already installed.${NO_COLOR}"
    fi

    # Check if version is specified
    if [ -n "$1" ]; then
        NODE_VERSION="$1"  # use specified version
        echo -e "${YELLOW}Installing Node version $NODE_VERSION...${NO_COLOR}"
        nvm install "$NODE_VERSION"
    else
        echo -e "${YELLOW}Installing the latest version of Node...${NO_COLOR}"
        nvm install node  # use latest version
    fi
    nvm alias default node  # set default version

    echo -e "${GREEN}Node installed successfully.${NO_COLOR}"

    npm config set fetch-retry-maxtimeout 120000
    npm config set fetch-retry-factor 5
    npm config set registry=https://registry.npmmirror.com
    npm install --global yarn
    npm install --global pnpm
    # NPM_BIN_DIR=$(npm bin -g)
    # if [ -d "$NPM_BIN_DIR" ]; then
    #     ln -sf "$NPM_BIN_DIR/yarn" /usr/bin/yarn
    #     ln -sf "$NPM_BIN_DIR/yarnpkg" /usr/bin/yarnpkg
    #     echo -e "${GREEN}Yarn installed successfully.${NO_COLOR}"
    # else
    #     echo -e "${RED}Failed to find npm global bin directory. Yarn installation may not be complete.${NO_COLOR}"
    # fi
    node --version
    check_status "Failed to install Node" "Node installed successfully." $?
    npm --version
    check_status "Failed to install npm" "npm installed successfully." $?
    yarn --version
    check_status "Failed to install Yarn" "Yarn installed successfully." $?
}

# Install Node npm Yarn
# shellcheck disable=SC2120
install_node() {
    echo -e "${YELLOW}Installing Node...${NO_COLOR}"
    if [ -n "$1" ]; then
        DEFAULT_NODE_VERSION="$1"
        if [[ -f /etc/redhat-release ]]; then
            if [[ "$1" != "16.20.2" ]]; then
                centos_skip_check
            fi
        fi
        echo -e "${YELLOW}Installing Node version $DEFAULT_NODE_VERSION from source...${NO_COLOR}"
        NODE_DISTRO="node-v$DEFAULT_NODE_VERSION-linux-x64"
        wget "https://nodejs.org/dist/v$DEFAULT_NODE_VERSION/$NODE_DISTRO.tar.xz" -O "/tmp/$NODE_DISTRO.tar.xz"
        tar -xf "/tmp/$NODE_DISTRO.tar.xz" -C /usr/local/lib/
        ln -sf "/usr/local/lib/$NODE_DISTRO/bin/node" /usr/bin/node
        ln -sf "/usr/local/lib/$NODE_DISTRO/bin/npm" /usr/bin/npm
        ln -sf "/usr/local/lib/$NODE_DISTRO/bin/npx" /usr/bin/npx
        rm -rf "/tmp/$NODE_DISTRO.tar.xz"
        node --version
        echo -e "${GREEN}Node version $DEFAULT_NODE_VERSION installed successfully.${NO_COLOR}"
    else
        install_package "nodejs"
        install_package "npm"
    fi
    node --version
    check_status "Failed to install Node" "Node installed successfully." $?

    npm --version
    check_status "Failed to install npm" "npm installed successfully." $?
    npm config set fetch-retry-maxtimeout 120000
    npm config set fetch-retry-attempts
    npm config set fetch-retry-factor 5
    npm config set registry=https://registry.npmmirror.com
    npm install --global yarn
    NPM_BIN_DIR=$(npm bin -g)
    if [ -d "$NPM_BIN_DIR" ]; then
        ln -sf "$NPM_BIN_DIR/yarn" /usr/bin/yarn
        ln -sf "$NPM_BIN_DIR/yarnpkg" /usr/bin/yarnpkg
        echo -e "${GREEN}Yarn installed successfully.${NO_COLOR}"
    else
        echo -e "${RED}Failed to find npm global bin directory. Yarn installation may not be complete.${NO_COLOR}"
    fi
    yarn --version
    check_status "Failed to install Yarn" "Yarn installed successfully." $?
}



# Deploy Git
deploy_git() {
    install_package "git"
    git --version
    check_status "Failed to install Git" "Git installed successfully." $?
    git config --global user.name "taos-support"
    git config --global user.email "it@taosdata.com"
    git config --global credential.helper store
}

deploy_node_exporter() {
    if [ ! -f "$NODE_EXPORTER_BINARY" ]; then
        echo "Node Exporter is not installed. Installing now..."

        echo "Fetching the latest version of Node Exporter..."
        LATEST_URL=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/prometheus/node_exporter/releases/latest | jq -r '.assets[] | select(.name | test("node_exporter-.*linux-amd64.tar.gz")) | .browser_download_url')

        if [ -z "$LATEST_URL" ]; then
            echo "Failed to fetch the latest Node Exporter release URL. Exiting."
            exit 1
        fi

        echo "Downloading Node Exporter from $LATEST_URL..."
        wget "$LATEST_URL" -O node_exporter.tar.gz

        echo "Extracting Node Exporter..."
        tar -xzf node_exporter.tar.gz
        cd node_exporter-*.linux-amd64 || exit

        echo "Copying binary..."
        cp node_exporter /usr/local/bin/

        echo "Creating systemd service..."
        cat <<EOF > /etc/systemd/system/node_exporter.service
[Unit]
Description=Node Exporter

[Service]
ExecStart=/usr/local/bin/node_exporter

[Install]
WantedBy=default.target
EOF

        # Start Node Exporter and enable it to run on startup
        systemctl daemon-reload
        systemctl start node_exporter
        systemctl enable node_exporter

        # Clean up the downloaded tar to save space
        cd ..
        rm -rf node_exporter*.tar.gz node_exporter-*.linux-amd64
        node_exporter --version
        check_status "Failed to install Node Exporter" "Node Exporter installed successfully." $?
    else
        echo "Node Exporter is already installed."
    fi
}

deploy_process_exporter() {
    if [ ! -f "$PROCESS_EXPORTER_BINARY" ]; then
        echo "Process Exporter is not installed. Installing now..."

        echo "Fetching the latest version of Process Exporter..."
        LATEST_URL=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/ncabatoff/process-exporter/releases/latest | jq -r '.assets[] | select(.name | test("process-exporter-.*linux-amd64.tar.gz")) | .browser_download_url')

        if [ -z "$LATEST_URL" ]; then
            echo "Failed to fetch the latest Process Exporter release URL. Exiting."
            exit 1
        fi

        echo "Downloading Process Exporter from $LATEST_URL..."
        wget "$LATEST_URL" -O process-exporter.tar.gz

        echo "Extracting Process Exporter..."
        tar -xzf process-exporter.tar.gz
        cd process-exporter-*.linux-amd64 || exit

        echo "Copying binary..."
        cp process-exporter /usr/local/bin/process-exporter

        echo "Creating configuration file..."
        cat <<EOF > /etc/process_exporter.yml
process_names:
  - name: "{{.Comm}}"
    cmdline:
    - taosd
EOF

        echo "Creating systemd service..."
        cat <<EOF > /etc/systemd/system/process_exporter.service
[Unit]
Description=Process Exporter

[Service]
ExecStart=/usr/local/bin/process-exporter --config.path /etc/process_exporter.yml

[Install]
WantedBy=default.target
EOF

        # Start Process Exporter and enable it to run on startup
        systemctl daemon-reload
        systemctl start process_exporter
        systemctl enable process_exporter

        # Clean up the downloaded tar to save space
        cd ..
        rm -rf process-exporter*.tar.gz process-exporter-*.linux-amd64
        process-exporter --version
        check_status "Failed to install Process Exporter" "Process Exporter installed successfully." $?
    else
        echo "Process Exporter is already installed."
    fi
}

deploy_prometheus() {
    # Check if Prometheus binary exists
    if [ ! -f "$PROMETHEUS_BINARY" ]; then
        echo "Prometheus is not installed. Installing now..."

        echo "Fetching the latest version of Prometheus..."
        LATEST_URL=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/prometheus/prometheus/releases/latest | jq -r '.assets[] | select(.name | test("prometheus-.*linux-amd64.tar.gz")) | .browser_download_url')

        if [ -z "$LATEST_URL" ]; then
            echo "Failed to fetch the latest Prometheus release URL. Exiting."
            exit 1
        fi

        echo "Downloading Prometheus from $LATEST_URL..."
        wget "$LATEST_URL" -O prometheus.tar.gz

        echo "Extracting Prometheus..."
        tar -xzf prometheus.tar.gz
        cd prometheus-*.linux-amd64 || exit

        echo "Creating directories..."
        mkdir -p /etc/prometheus /var/lib/prometheus

        echo "Copying binaries and configuration..."
        cp prometheus promtool /usr/local/bin/

        echo "Setting up Prometheus configuration..."
        cat <<EOF > /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s
scrape_configs:
  - job_name: 'node_exporter'
    static_configs:
      - targets: ['localhost:9100']
EOF

        echo "Creating systemd service..."
        cat <<EOF > /etc/systemd/system/prometheus.service
[Unit]
Description=Prometheus Service

[Service]
ExecStart=/usr/local/bin/prometheus \\
    --config.file /etc/prometheus/prometheus.yml \\
    --storage.tsdb.path /var/lib/prometheus/ \\
    --web.console.templates=/etc/prometheus/consoles \\
    --web.console.libraries=/etc/prometheus/console_libraries

[Install]
WantedBy=default.target
EOF

        # Start Prometheus and enable it to run on startup
        systemctl daemon-reload
        systemctl start prometheus
        systemctl enable prometheus

        # Clean up the downloaded tar to save space
        cd ..
        rm -rf prometheus*.tar.gz prometheus-*.linux-amd64
        prometheus --version
        check_status "Failed to install Prometheus" "Prometheus installed successfully." $?
    else
        echo "Prometheus is already installed."
    fi
}

# Install Grafana using a downloaded .deb package
deploy_grafana() {
    LATEST_VERSION=$(curl --retry 10 --retry-delay 5 --retry-max-time 120 -s https://api.github.com/repos/grafana/grafana/releases/latest | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        deploy_debian_grafana "$LATEST_VERSION"
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        deploy_redhat_grafana "$LATEST_VERSION"
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Install Grafana for ubuntu/debian
deploy_debian_grafana() {
    # Check if Grafana is already installed
    if ! dpkg -s "grafana" && ! dpkg -s "grafana-enterprise" &> /dev/null; then
        echo "Downloading the latest Grafana .deb package..."
        # Download the latest Grafana .deb package
        grafana_latest_version=$1
        wget https://dl.grafana.com/oss/release/grafana_${grafana_latest_version}_amd64.deb -O grafana.deb
        # install the required fontconfig package
        install_package adduser libfontconfig1 musl
        echo "Installing Grafana..."
        # Install the .deb package
        dpkg -i grafana.deb

        # Clean up the downloaded .deb package to save space
        rm -rf grafana.deb

        # Start the Grafana server and enable it to run on startup
        systemctl start grafana-server
        systemctl enable grafana-server

        # Check if Grafana was installed successfully
        if [ $? -eq 0 ]; then
            echo "Grafana was installed successfully."
        else
            echo "Failed to install Grafana."
        fi
    else
        echo "Grafana is already installed."
    fi
}

# Install Grafana for centos/redhat
deploy_redhat_grafana() {
    # Check if Grafana is already installed
    if ! rpm -q grafana &> /dev/null; then
        echo "Downloading the latest Grafana .rpm package..."
        # Download the latest Grafana .rpm package
        grafana_latest_version=$1
        wget https://dl.grafana.com/oss/release/grafana-${grafana_latest_version}-1.x86_64.rpm -O grafana.rpm

        # Install the required fontconfig package
        yum install -y fontconfig

        echo "Installing Grafana..."
        # Install the .rpm package
        rpm -ivh grafana.rpm

        # Clean up the downloaded .rpm package to save space
        rm -rf grafana.rpm

        # Start the Grafana server and enable it to run on startup
        systemctl start grafana-server
        systemctl enable grafana-server

        # Check if Grafana was installed successfully
        if [ $? -eq 0 ]; then
            echo "Grafana was installed successfully."
        else
            echo "Failed to install Grafana."
        fi
    else
        echo "Grafana is already installed."
    fi
}

# Install Nginx
install_nginx() {
    install_package "nginx"
    nginx -v
    check_status "Failed to install Nginx" "Nginx installed successfully." $?
}

# Deploy JMeter
deploy_jmeter() {
    if ! command -v jmeter &> /dev/null; then
        echo "Installing JMeter..."
        install_java
        wget -P /opt https://mirrors.aliyun.com/apache/jmeter/binaries/apache-jmeter-$JMETER_VERSION.tgz
        tar -xvzf /opt/apache-jmeter-$JMETER_VERSION.tgz -C /opt/
        ln -sf /opt/apache-jmeter-$JMETER_VERSION/bin/jmeter /usr/local/bin/jmeter
        rm -rf /opt/apache-jmeter-$JMETER_VERSION.tgz
        jmeter --version
        check_status "Failed to install JMeter" "JMeter installed successfully." $?
    else
        echo "JMeter is already installed."
    fi
}

# Deploy Docker
deploy_docker() {
    if [ -f /etc/debian_version ]; then
        # Debian or Ubuntu
        deploy_debian_docker
    elif [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        deploy_redhat_docker
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
}

# Deploy Docker for centos/redhat
deploy_redhat_docker() {
    # Check if Docker is already installed
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Installing now..."

        # Set up the repository for Docker
        echo "Setting up the Docker repository..."
        install_package yum-utils

        echo "Adding Docker's official repository..."
        yum-config-manager --add-repo https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo

        # Install Docker CE
        echo "Installing Docker CE..."
        install_package docker-ce docker-ce-cli containerd.io

        # Enable and start Docker
        echo "Enabling and starting Docker..."
        systemctl enable docker
        systemctl start docker

        # Adding current user to the Docker group
        usermod -aG docker "$USER"

        # Print Docker version
        docker --version

        # Check the installation status
        if [ $? -eq 0 ]; then
            echo "Docker installed successfully."
        else
            echo "Failed to install Docker."
        fi
    else
        echo "Docker is already installed."
    fi
}

# Deploy docker for ubuntu/debian
deploy_debian_docker() {
    # Check if Docker is already installed
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Installing now..."

        # Set up the repository for Docker
        echo "Setting up the Docker repository..."
        update
        install_package apt-transport-https ca-certificates curl software-properties-common gnupg lsb-release

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
        update
        install_package docker-ce docker-ce-cli containerd.io

        # Enable and start Docker
        echo "Enabling and starting Docker..."
        systemctl enable docker
        systemctl start docker

        # Adding current user to the Docker group
        usermod -aG docker "$USER"

        # Print Docker version
        docker --version
        check_status "Failed to install Docker" "Docker installed successfully." $?
    else
        echo "Docker is already installed."
    fi
}

# Deploy Docker Compose
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

# Instal trivy
install_trivy() {
    echo -e "${YELLOW}Installing Trivy...${NO_COLOR}"
    # Check if Trivy is already installed
    # if command -v trivy >/dev/null 2>&1; then
    #     echo "Trivy is already installed. Skipping installation."
    #     trivy --version
    #     return
    # fi
    if check_installed "trivy"; then
        return
    fi
    # Install jq
    install_package jq
    # Get latest version
    LATEST_VERSION=$(curl -s https://api.github.com/repos/aquasecurity/trivy/releases/latest | jq -r .tag_name)
    # Download
    if [ -f /etc/debian_version ]; then
        wget https://github.com/aquasecurity/trivy/releases/download/"${LATEST_VERSION}"/trivy_"${LATEST_VERSION#v}"_Linux-64bit.deb
        # Install
        dpkg -i trivy_"${LATEST_VERSION#v}"_Linux-64bit.deb

    elif [ -f /etc/redhat-release ]; then
        wget https://github.com/aquasecurity/trivy/releases/download/"${LATEST_VERSION}"/trivy_"${LATEST_VERSION#v}"_Linux-64bit.rpm
        # Install
        rpm -ivh trivy_"${LATEST_VERSION#v}"_Linux-64bit.rpm
    else
        echo "Unsupported Linux distribution."
        exit 1
    fi
    # Check
    trivy --version
    check_status "Failed to install Trivy" "Trivy installed successfully." $?
    rm -rf trivy_"${LATEST_VERSION#v}"_Linux-64bit.deb trivy_"${LATEST_VERSION#v}"_Linux-64bit.rpm
}

# Install uv
install_uv() {
    local uv_url="https://astral.sh/uv/install.sh"
    local uv_path="$HOME/.local/bin/uv"

    echo -e "${YELLOW}Checking for uv installation...${NO_COLOR}"

    if [ -f "$uv_path" ]; then
        echo -e "${GREEN}uv is already installed.${NO_COLOR}"
    else
        echo -e "${YELLOW}Installing uv...${NO_COLOR}"
        if ! command -v curl &> /dev/null; then
            echo -e "${RED}Error: curl is not installed. Please install curl first.${NO_COLOR}"
            install_package curl
        fi

        if curl --retry 10 --retry-delay 5 --retry-max-time 120 -LsSf "$uv_url" | sh; then
            echo -e "${GREEN}uv has been installed successfully.${NO_COLOR}"
            SOURCE_RESULTS+="# For uv\nsource $HOME/.local/bin/env (sh, bash, zsh)\nsource $HOME/.local/bin/env.fish (fish)\n"
        else
            echo -e "${RED}Error: Failed to install uv.${NO_COLOR}"
            return 1
        fi
    fi
}

# Reconfigure cloud-init
reconfig_cloud_init() {
    echo "Reconfiguring cloud-init..."
    apt remove -y cloud-init && apt purge -y cloud-init
    rm -rf /var/lib/cloud /etc/cloud
    apt update -y
    install_package cloud-init
    sed -i '/package[-_]update[-_]upgrade[-_]install/s/^/#/' /etc/cloud/cloud.cfg
}

# Config cloud-init
config_cloud_init() {
    if [ "$DIST_VERSION" = "7.9" ];then
        install_package "cloud-init"
        sed -i '/ssh_pwauth.*/s/^/#/' /etc/cloud/cloud.cfg
    else
        reconfig_cloud_init
    fi
    check_status "Failed to configure cloud-init" "Cloud-init configured successfully and you need reboot manually." $?
    # if [ "$DIST_VERSION" = "18.04" ] || [ "$DIST_VERSION" = "20.04" ]; then
    #     reconfig_cloud_init
    # elif [ "$DIST_VERSION" = "7.9" ];then
    #     install_package "cloud-init"
    #     sed -i '/ssh_pwauth.*/s/^/#/' /etc/cloud/cloud.cfg
    # else
    #     echo "Configuring cloud-init..."
    #     add_config_if_not_exist "$CLOUD_INIT_CONFIG" "/etc/cloud/cloud.cfg"

    #     marker="NoCloud"

    #     if grep -qF "$marker" "/etc/cloud/cloud.cfg"; then
    #         echo "cloud-init settings already exists in /etc/cloud/cloud.cfg."
    #     else
    #         echo "Adding configuration to /etc/cloud/cloud.cfg."
    #         echo "$CLOUD_INIT_CONFIG" >> "/etc/cloud/cloud.cfg"
    #         echo "cloud-init settings have been updated in /etc/cloud/cloud.cfg."
    #     fi

    #     mkdir -p /var/lib/cloud/seed/nocloud/
    #     cd /var/lib/cloud/seed/nocloud/ || exit
    #     touch meta-data
    #     touch user-data
    #     add_config_if_not_exist "hostname: \${name}" user-data
    #     add_config_if_not_exist "manage_etc_hosts: true" user-data
    # fi
    # cloud-init clean --logs
}

cleanup() {
    if [ -n "$SOURCE_RESULTS" ]; then
        echo -e "${YELLOW}===========================================\n${NO_COLOR}"
        echo -e "${YELLOW}Installation complete! \n${NO_COLOR}"
        echo -e "${YELLOW}Some tools require you to manually source${NO_COLOR}"
        echo -e "${YELLOW}or restart your terminal to take effect.\n${NO_COLOR}"
        echo -e "${YELLOW}===========================================\n${NO_COLOR}"
        echo -e "${YELLOW}$SOURCE_RESULTS${NO_COLOR}"
    else
        echo -e "${YELLOW}Installation complete \n${NO_COLOR}"
    fi
}

# Clone a repository with a specified target directory
clone_repo_with_rename() {
  local repo_url="$1"
  local target_dir="$2"
  local branch_name="$3"

  if [ -z "$target_dir" ]; then
    target_dir=$(basename -s .git "$repo_url")
  fi

  cd "$REPOPATH" || exit

  if [ -d "$target_dir" ]; then
    echo "Directory $target_dir already exists. Skipping clone."
  else
    echo "Cloning into $target_dir..."
    if [ -n "$branch_name" ]; then
      git clone -b "$branch_name" "$repo_url" "$target_dir"
    else
      git clone "$repo_url" "$target_dir"
    fi
  fi
}

# Clone enterprise repository
clone_enterprise() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/TDinternal
    clone_repo_with_rename git@github.com:taosdata/TDengine.git TDinternal/community
}

# Clone community repository
clone_community() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com:taosdata/TDengine.git
}

# Clone TaosX repository
clone_taosx() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/taosx
}

# Clone TaosKeeper repository
clone_taoskeeper() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/taoskeeper
}

# Clone TaosTest repository
clone_taostest() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/taos-test-framework "" "master"
}

# Clone TestNG repository
clone_testng() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/TestNG "" "master"
}

# Clone operation tools repository
clone_operation() {
    cd "$REPOPATH" || exit
    clone_repo_with_rename https://github.com/taosdata/operation.git
}

# init system
system_config() {
    disable_service
    config_dns
    replace_sources
    config_cloud_init
    config_ssh
    config_custom_settings
    config_timezone
    config_share_server
    disable_firewalld
    config_frontend
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

new_funcs() {
    echo "Adding test..."
    install_python_via_pyenv 3.10.12
    install_java_via_sdkman 21.0.2
    install_node 16.20.2
    install_maven_via_sdkman 3.2.5
    deploy_rust
}

# deploy TDasset
TDasset() {
    install_java_via_sdkman 21.0.2
    install_maven_via_sdkman 3.9.9
    # not supported in centos7/ubuntu18 because of the old version of glibc
    install_node_via_nvm 22.0.0
    install_pnpm
}

# deploy TDinternal/TDengine/taosx
TDinternal() {
    install_go_via_gvm 1.23.3
    deploy_rust
    install_java_via_sdkman 17
    install_maven_via_sdkman 3.9.9
    install_node_via_nvm 16.20.2
    install_python_via_pyenv 3.10.12
    install_via_pip pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog toml taospy taos-ws-py
}

# deploy TDgpt
TDgpt() {
    install_python_via_pyenv 3.10.12
}

# deploy taos-test-framework
taostest() {
    if [ ! -d "$REPOPATH/taos-test-framework" ]; then
        echo "Cloning TaosTest repository..."
        clone_taostest
    else
        echo "TaosTest repository already exists. Skipping clone."
    fi
    check_status "Failed to clone TaosTest repository" "TaosTest repository cloned successfully." $?

    if [ ! -d "$REPOPATH/TestNG" ]; then
        echo "Cloning TestNG repository..."
        clone_testng
    else
        echo "TestNG repository already exists. Skipping clone."
    fi
    check_status "Failed to clone TestNG repository" "TestNG repository cloned successfully." $?

    # Configure environment variables
    echo "Configuring TaosTest environment variables..."
    mkdir -p "$HOME"/.taostest
    add_config_if_not_exist "TEST_ROOT=$REPOPATH/TestNG" "$HOME"/.taostest/.env

    # Install TaosTest
    echo "Installing TaosTest..."
    cd "$REPOPATH"/taos-test-framework || exit
    install_package "python3-pip"
    install_via_pip "poetry"
    yes | ./reinstall.sh
    check_status "Failed to install TaosTest" "TaosTest installed successfully." $?

    # Configure passwdless login
    echo "Configuring passwdless login..."
    yes | ssh-keygen -t rsa -b 2048 -N "" -f "$HOME/.ssh/testng"
    cat "$HOME"/.ssh/testng.pub >> "$HOME"/.ssh/authorized_keys
}


# Deploy pure environment
deploy_pure() {
    disable_service
    config_dns
    if [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        echo "Replacing sources for YUM package manager."
        replace_yum_sources
    fi
    config_cloud_init
    config_ssh
    config_custom_settings
    config_timezone
    config_share_server
    disable_firewalld
    install_package "jq"
    install_package "wget"
    deploy_node_exporter
    check_status "Failed to config pure system" "Config pure system successfully" $?
}

# Deploy development environment
deploy_dev() {
    install_packages
    deploy_cmake
    if [ -f /etc/redhat-release ]; then
        # Red Hat or CentOS
        update_redhat_gcc
    fi
    deploy_tmux
    deploy_git
    install_python
    install_pip_pkg
    install_java
    install_maven_via_sdkman
    deploy_go
    deploy_rust
    install_node
    deploy_node_exporter
    deploy_process_exporter
    deploy_prometheus
    deploy_grafana
    deploy_jmeter
    install_nginx
    deploy_docker
    deploy_docker_compose
    install_trivy
    install_uv
    check_status "Failed to deploy some tools" "Deploy all tools successfully" $?
}

# Setup all configurations
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
    init_env
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
            disable_firewalld)
                disable_firewalld
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
            update)
                update
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
            install_pyenv)
                install_pyenv
                ;;
            install_python_via_pyenv)
                install_python_via_pyenv
                ;;
            install_pip_pkg)
                install_pip_pkg
                ;;
            install_java)
                install_java
                ;;
            install_java_via_sdkman)
                install_java_via_sdkman
                ;;
            install_maven_via_sdkman)
                install_maven_via_sdkman
                ;;
            deploy_cmake)
                deploy_cmake
                ;;
            update_redhat_gcc)
                update_redhat_gcc
                ;;
            update_redhat_tmux)
                update_redhat_tmux
                ;;
            deploy_tmux)
                deploy_tmux
                ;;
            deploy_go)
                deploy_go
                ;;
            install_gvm)
                install_gvm
                ;;
            install_go_via_gvm)
                install_go_via_gvm
                ;;
            deploy_rust)
                deploy_rust
                ;;
            install_node)
                install_node
                ;;
            install_node_via_nvm)
                install_node_via_nvm
                ;;
            install_pnpm)
                install_pnpm
                ;;
            deploy_node_exporter)
                deploy_node_exporter
                ;;
            deploy_process_exporter)
                deploy_process_exporter
                ;;
            deploy_prometheus)
                deploy_prometheus
                ;;
            deploy_grafana)
                deploy_grafana
                ;;
            deploy_jmeter)
                deploy_jmeter
                ;;
            install_nginx)
                install_nginx
                ;;
            config_qemu_guest_agent)
                config_qemu_guest_agent
                ;;
            config_share_server)
                config_share_server
                ;;
            deploy_docker)
                deploy_docker
                ;;
            deploy_docker_compose)
                deploy_docker_compose
                ;;
            install_trivy)
                install_trivy
                ;;
            install_uv)
                install_uv
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
            TDasset)
                TDasset
                ;;
            TDinternal)
                TDinternal
                ;;
            TDgpt)
                TDgpt
                ;;
            taostest)
                taostest
                ;;
            new_funcs)
                new_funcs
                ;;
            *)
                echo "Unknown function: $arg"
                ;;
        esac
    done
}

# Execute the script with specified function arguments
main "$@"
