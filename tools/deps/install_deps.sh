#!/usr/bin/env bash
# =============================================================================
# DEPRECATED: Use tools/setup/setup-{linux,macos}.sh instead.
#
# This script is kept for backward compatibility but is no longer maintained.
# The new setup framework provides:
#   - Modular per-language modules (cpp, go, rust, java, node, python, dotnet)
#   - Internal dependency source configuration (GOPROXY, Cargo/Nora, Conan, etc.)
#   - Component-based selection (--component engine taosx)
#   - Check-only mode (--check)
#
# Migration:
#   ./tools/setup/setup-linux.sh --all          # Linux
#   ./tools/setup/setup-macos.sh --all          # macOS
#   .\tools\setup\setup-windows.ps1 -All        # Windows
# =============================================================================
echo "⚠ DEPRECATED: This script has been superseded by tools/setup/"
echo "  Use: ./tools/setup/setup-linux.sh --all  (or setup-macos.sh on macOS)"
echo ""
echo "  Continuing with legacy script for backward compatibility..."
echo ""
# =============================================================================
# TSDB (TDengine) full-component build dependency installation script
#
# Supported platforms: macOS (Homebrew), Debian/Ubuntu (apt), RHEL/CentOS (yum)
#
# Usage:
#   chmod +x install_build_deps.sh
#   ./install_build_deps.sh            # Install all dependencies
#   ./install_build_deps.sh --check    # Only check installed/missing tools
#
# Required tools per component:
#   Engine (taosd)             : cmake, C/C++ compiler
#   taos-adapter / taos-keeper: go
#   taos-gen (taosbenchmark)  : conan (C++ package manager), cmake
#   taos-xservice (taosx)     : cargo (Rust), protoc
#   taos-insight              : go, mage, yarn, node (>=18 <=22)
#   taos-connector-dotnet     : dotnet
#   taos-connector-jdbc       : java (17+), maven
#   taos-connector-node       : node, npm
#   taos-connector-python     : python3, maturin, cargo (Rust >=1.85)
#   taos-connector-rust       : cargo (Rust)
#   taos-connector-odbc       : cmake, bison, flex, unixodbc, libiconv
#
# Generated based on actual build and debug experience, 2026-03-31
# =============================================================================
set -euo pipefail

# ── Output utilities ──────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
fail()  { echo -e "${RED}[✗]${NC} $*"; }

cmd_exists() { command -v "$1" &>/dev/null; }

resolve_macos_java_home() {
    local prefix jhome=""
    prefix="$(brew --prefix 2>/dev/null || true)"

    if [[ -n "$prefix" && -d "$prefix/opt/openjdk@17" ]]; then
        jhome="$prefix/opt/openjdk@17"
    elif cmd_exists /usr/libexec/java_home; then
        jhome=$(/usr/libexec/java_home 2>/dev/null || true)
    fi

    echo "$jhome"
}

java_major_version() {
    local line version major
    line=$(java --version 2>/dev/null | head -1 || true)
    version=$(echo "$line" | grep -oE '[0-9]+(\.[0-9]+)*' | head -1)
    major=$(echo "$version" | cut -d. -f1)
    echo "${major:-0}"
}

# ── Ensure common tool paths are in PATH ──────────────────────────────────
ensure_path() {
    local dirs=(
        "$HOME/go/bin"                          # tools installed via go install (mage, etc.)
        "$HOME/.cargo/bin"                      # rust/cargo tools (maturin, etc.)
        "/usr/local/go/bin"                     # default Go location on Linux
        "/usr/lib/jvm/java-17-openjdk-amd64/bin" # Debian/Ubuntu OpenJDK 17
        "/opt/homebrew/opt/bison/bin"           # macOS brew bison
        "/opt/homebrew/opt/flex/bin"            # macOS brew flex
        "/opt/homebrew/opt/node@22/bin"         # macOS brew node@22
        "/opt/homebrew/opt/openjdk@17/bin"      # macOS brew java
    )
    for d in "${dirs[@]}"; do
        [[ -d "$d" ]] && [[ ":$PATH:" != *":$d:"* ]] && export PATH="$d:$PATH"
    done
    # cargo env
    [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env" 2>/dev/null || true
}

# ── Detect operating system ───────────────────────────────────────────────
detect_os() {
    case "$(uname -s)" in
        Darwin) OS="macos" ;;
        Linux)
            if [ -f /etc/debian_version ]; then
                OS="debian"
            elif [ -f /etc/redhat-release ]; then
                OS="redhat"
            else
                OS="linux_unknown"
            fi
            ;;
        *) OS="unsupported" ;;
    esac
    info "OS: $OS ($(uname -m))"
}

# ── Check mode: list all tool statuses ────────────────────────────────────
check_all() {
    ensure_path
    echo ""
    echo "=============================="
    echo " Dependency Check"
    echo "=============================="
    local tools=(cmake gcc pkg-config go cargo rustc java mvn node npm yarn pnpm
                 dotnet conan maturin mage python3 protoc bison flex)
    local missing=0
    for t in "${tools[@]}"; do
        if cmd_exists "$t"; then
            local ver_cmd
            case "$t" in
                go) ver_cmd="go version" ;;
                java) ver_cmd="java --version 2>&1 | head -1" ;;
                mvn) ver_cmd="mvn --version 2>&1 | head -1" ;;
                *) ver_cmd="$t --version 2>&1 | head -1" ;;
            esac
            local ver
            ver=$(eval "$ver_cmd" 2>/dev/null || echo 'ok')
            if [[ "$t" == "java" ]]; then
                local j_major
                j_major=$(java_major_version)
                if [[ "$j_major" -lt 17 ]]; then
                    fail "$t  →  $ver (requires 17+)"
                    ((missing++)) || true
                    continue
                fi
            fi
            info "$t  →  $ver"
        else
            fail "$t  →  not found"
            ((missing++)) || true
        fi
    done
    # 检查 Python 模块
    if python3 -m build --version >/dev/null 2>&1; then
        info "python3-build  →  $(python3 -m build --version 2>&1)"
    else
        fail "python3-build  →  未安装 (pip install build)"
        ((missing++)) || true
    fi
    echo ""
    if [ "$missing" -gt 0 ]; then
        warn "$missing tool(s) missing. Run without --check to install."
    else
        info "All tools are installed!"
    fi
    return "$missing"
}

# ══════════════════════════════════════════════════════════════════════════
#  macOS installation functions (Homebrew)
# ══════════════════════════════════════════════════════════════════════════

install_macos_homebrew() {
    if cmd_exists brew; then
        info "Homebrew already installed: $(brew --version | head -1)"
        return
    fi
    warn "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    eval "$($HOMEBREW_PREFIX/bin/brew shellenv)"
    info "Homebrew installed"
}

install_macos_xcode_cli() {
    if xcode-select -p &>/dev/null; then
        info "Xcode CLI tools already installed"
    else
        warn "Installing Xcode CLI tools..."
        xcode-select --install
        echo "Please confirm installation in the popup. Press Enter to continue after installation."
        read -r
    fi
    # Check SDK version
    local sdk_ver
    sdk_ver=$(xcrun --show-sdk-version 2>/dev/null || echo "unknown")
    info "macOS SDK version: $sdk_ver"
    if [[ "$sdk_ver" != "unknown" ]]; then
        local sdk_major
        sdk_major=$(echo "$sdk_ver" | cut -d. -f1)
        if [[ "$sdk_major" -lt 12 ]]; then
            warn "SDK $sdk_ver < 12.0, Go CGO compilation may need workaround (-Wl,-undefined,dynamic_lookup)"
            warn "Consider upgrading Xcode to get macOS 12.0+ SDK"
        fi
    fi
}

install_macos_brew_packages() {
    echo ""
    echo "── Homebrew dependency packages ──"
    # cmake:     build system
    # pkg-config:C/C++ package metadata helper required by CMake find_package(PkgConfig)
    # bison:     ODBC parser generator (Xcode built-in bison is too old, does not support -W option)
    # flex:      ODBC parser generator
    # unixodbc:  ODBC driver manager (provides sqlext.h and other headers)
    # libiconv:  ODBC character encoding (system iconv on Apple Silicon may have linking issues)
    # protobuf:  taosx (taos-xservice) requires protoc
    # argp-standalone: required by taos-tools on macOS (argp.h not provided by system libc)
    local pkgs=(cmake pkg-config bison flex unixodbc libiconv protobuf argp-standalone)
    for p in "${pkgs[@]}"; do
        if brew list "$p" &>/dev/null; then
            info "$p already installed"
        else
            warn "Installing $p..."
            brew install "$p"
        fi
    done
}

install_macos_go() {
    if cmd_exists go; then
        info "Go already installed: $(go version)"
    else
        warn "Installing Go..."
        brew install go
        eval "$(brew shellenv 2>/dev/null || true)"
        info "Go installed: $(go version)"
    fi

    # mage — taos-insight build tool
    local gopath_bin
    gopath_bin="$(go env GOPATH)/bin"
    export PATH="$gopath_bin:$PATH"
    if ! cmd_exists mage; then
        warn "Installing mage..."
        go install github.com/magefile/mage@latest
    fi
    info "mage installed"
}

install_macos_rust() {
    if cmd_exists cargo; then
        info "Rust/Cargo already installed: $(cargo --version 2>&1 | tail -1)"
    else
        warn "Installing Rust (rustup)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
        info "Rust installed: $(rustc --version)"
    fi

    # edition2024 requires Rust >= 1.85
    local ver minor
    ver=$(rustc --version | awk '{print $2}')
    minor=$(echo "$ver" | cut -d. -f2)
    if [[ "$minor" -lt 85 ]]; then
        warn "Rust $ver is too old (>= 1.85 required for edition2024), upgrading..."
        rustup update stable
        info "Rust upgraded: $(rustc --version)"
    fi
}

install_macos_java_maven() {
    # Java 17
    if cmd_exists java && java --version 2>&1 | head -1 | grep -qE '(1[7-9]|2[0-9])'; then
        info "Java already installed: $(java --version 2>&1 | head -1)"
    else
        warn "Installing OpenJDK 17..."
        brew install openjdk@17
        if [ -d "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" ]; then
            sudo ln -sfn "$(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk" \
                /Library/Java/JavaVirtualMachines/openjdk-17.jdk 2>/dev/null || true
        fi
    fi
    local java_home
    java_home="$(resolve_macos_java_home)"
    if [[ -n "$java_home" ]]; then
        export JAVA_HOME="$java_home"
        export PATH="$JAVA_HOME/bin:$PATH"
        info "JAVA_HOME set to: $JAVA_HOME"
    else
        warn "JAVA_HOME could not be auto-detected. Maven/JDBC build may fail."
    fi

    # Maven
    if cmd_exists mvn; then
        info "Maven already installed: $(mvn --version 2>&1 | head -1)"
    else
        warn "Installing Maven..."
        brew install maven
        info "Maven installed"
    fi
}

install_macos_node() {
    # taos-insight's @grafana/plugin-e2e requires Node >=18 and <=22
    if cmd_exists node; then
        local node_major
        node_major=$(node --version | sed 's/^v//' | cut -d. -f1)
        if [[ "$node_major" -gt 22 ]]; then
            warn "Node.js v$(node --version) > v22, taos-insight requires <=22"
            warn "Installing Node.js 22..."
            brew install node@22
            brew link --overwrite node@22 2>/dev/null || true
        else
            info "Node.js already installed: $(node --version)"
        fi
    else
        warn "Installing Node.js 22..."
        brew install node@22
        brew link --overwrite node@22 2>/dev/null || true
    fi
    export PATH="$(brew --prefix)/opt/node@22/bin:$PATH"

    # yarn + pnpm
    if ! cmd_exists yarn; then
        warn "Installing yarn..."
        npm install -g yarn
    fi
    info "yarn: $(yarn --version)"

    if ! cmd_exists pnpm; then
        warn "Installing pnpm..."
        npm install -g pnpm
    fi
    info "pnpm: $(pnpm --version)"
}

install_macos_dotnet() {
    if cmd_exists dotnet; then
        info ".NET SDK already installed: $(dotnet --version)"
        return
    fi
    warn "Installing .NET SDK..."
    brew install dotnet
    info ".NET SDK installed: $(dotnet --version)"
}

install_macos_python_tools() {
    if cmd_exists python3; then
        local pyver pymajor pyminor
        pyver=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        pymajor=$(echo "$pyver" | cut -d. -f1)
        pyminor=$(echo "$pyver" | cut -d. -f2)
        if [ "$pymajor" -lt 3 ] || { [ "$pymajor" -eq 3 ] && [ "$pyminor" -lt 9 ]; }; then
            warn "Python $pyver is too old (>= 3.9 required), installing newer version..."
            brew install python@3.12
            export PATH="$(brew --prefix)/opt/python@3.12/libexec/bin:$PATH"
        else
            info "Python3 already installed: $(python3 --version)"
        fi
    else
        warn "Installing Python3..."
        brew install python@3.12
        export PATH="$(brew --prefix)/opt/python@3.12/libexec/bin:$PATH"
    fi

    # pip + build
    python3 -m pip install --upgrade pip 2>/dev/null || python3 -m ensurepip --upgrade 2>/dev/null || true
    python3 -m pip install --upgrade build 2>/dev/null || true

    # maturin — Rust-Python bridge build tool
    if cmd_exists maturin; then
        info "maturin already installed: $(maturin --version)"
    else
        warn "Installing maturin..."
        python3 -m pip install maturin --break-system-packages 2>/dev/null || python3 -m pip install maturin
        info "maturin installed"
    fi
}

install_macos_conan() {
    if cmd_exists conan; then
        info "Conan already installed: $(conan --version)"
    else
        warn "Installing Conan..."
        pip3 install conan --break-system-packages 2>/dev/null || brew install conan
        info "Conan installed"
    fi

    # Initialize conan default profile (required by taos-gen build)
    if ! conan profile list 2>/dev/null | grep -q default; then
        warn "Creating conan default profile..."
        conan profile detect
    fi
    info "Conan profile ready"
}

install_macos_path_config() {
    echo ""
    echo "── Configure PATH environment variable ──"

    local prefix
    prefix="$(brew --prefix)"
    local shell_rcs=("$HOME/.zshrc" "$HOME/.bashrc")

    local java_home
    java_home="$(resolve_macos_java_home)"

    local paths_to_add=(
        "$prefix/opt/bison/bin"
        "$prefix/opt/flex/bin"
        "$prefix/opt/node@22/bin"
        "$prefix/opt/openjdk@17/bin"
        "\$(go env GOPATH)/bin"
        "\$HOME/.cargo/bin"
    )

    local marker="# === TSDB build deps PATH ==="
    local wrote_any=0
    for shell_rc in "${shell_rcs[@]}"; do
        [[ -f "$shell_rc" ]] || touch "$shell_rc"
        if ! grep -q "$marker" "$shell_rc" 2>/dev/null; then
            {
                echo ""
                echo "$marker"
                for p in "${paths_to_add[@]}"; do
                    echo "export PATH=\"$p:\$PATH\""
                done
                if [[ -n "$java_home" ]]; then
                    echo "export JAVA_HOME=\"$java_home\""
                fi
                echo 'if [[ -z "$JAVA_HOME" ]] && command -v /usr/libexec/java_home >/dev/null 2>&1; then'
                echo '  export JAVA_HOME="$(/usr/libexec/java_home 2>/dev/null || true)"'
                echo 'fi'
                echo 'if [[ -n "$JAVA_HOME" ]]; then'
                echo '  export PATH="$JAVA_HOME/bin:$PATH"'
                echo 'fi'
                echo "# === END TSDB build deps PATH ==="
            } >> "$shell_rc"
            info "PATH config appended to $shell_rc"
            wrote_any=1
        else
            info "PATH config already exists in $shell_rc, skipping"
        fi
    done

    if [[ "$wrote_any" -eq 0 ]]; then
        info "PATH config already present in both ~/.zshrc and ~/.bashrc"
    fi
}

install_macos_all() {
    if [[ "$(uname -m)" == "arm64" ]]; then
        HOMEBREW_PREFIX="/opt/homebrew"
    else
        HOMEBREW_PREFIX="/usr/local"
    fi

    install_macos_homebrew
    install_macos_xcode_cli
    install_macos_brew_packages
    install_macos_go
    install_macos_rust
    install_macos_java_maven
    install_macos_node
    install_macos_dotnet
    install_macos_python_tools
    install_macos_conan
    install_macos_path_config
}

# ══════════════════════════════════════════════════════════════════════════
#  Debian/Ubuntu installation functions (apt)
# ══════════════════════════════════════════════════════════════════════════

install_debian_system_packages() {
    echo ""
    echo "── APT system packages ──"
    local pkgs="gcc g++ make cmake perl curl wget git pkg-config \
        libssl-dev libgflags-dev libjansson-dev libsnappy-dev liblzma-dev \
        zlib1g-dev libz-dev build-essential ca-certificates rsync \
        bison flex unixodbc-dev protobuf-compiler"
    sudo apt-get update -y
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $pkgs
    info "System build packages installed"
}

install_debian_go() {
    if cmd_exists go; then
        info "Go already installed: $(go version)"
        return
    fi
    warn "Installing Go..."
    local GO_VERSION ARCH="amd64"
    GO_VERSION=$(curl -s https://go.dev/VERSION?m=text | grep -oP 'go[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    [[ "$(uname -m)" == "aarch64" ]] && ARCH="arm64"
    wget -q "https://go.dev/dl/${GO_VERSION}.linux-${ARCH}.tar.gz" -O /tmp/go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm -f /tmp/go.tar.gz
    export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"
    info "Go installed: $(go version)"

    # mage
    go install github.com/magefile/mage@latest
    export PATH="$HOME/go/bin:$PATH"
}

install_debian_rust() {
    if cmd_exists cargo; then
        info "Rust/Cargo already installed: $(cargo --version 2>&1 | tail -1)"
    else
        warn "Installing Rust (rustup)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    fi
    # edition2024 version check
    local minor
    minor=$(rustc --version | awk '{print $2}' | cut -d. -f2)
    if [[ "$minor" -lt 85 ]]; then
        warn "Rust is too old (>= 1.85 required), upgrading..."
        rustup update stable
    fi
    info "Rust: $(rustc --version)"
}

install_debian_java_maven() {
    local java_major=0
    if cmd_exists java; then
        java_major=$(java_major_version)
    fi
    if ! cmd_exists java || [[ "$java_major" -lt 17 ]]; then
        warn "Installing OpenJDK 17..."
        sudo apt-get install -y openjdk-17-jdk
        if cmd_exists update-alternatives; then
            sudo update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java 2>/dev/null || true
            sudo update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac 2>/dev/null || true
        fi
    fi
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    export PATH="$JAVA_HOME/bin:$PATH"
    info "Java: $(java --version 2>&1 | head -1)"

    if ! cmd_exists mvn; then
        warn "Installing Maven..."
        sudo apt-get install -y maven
    fi
    info "Maven: $(mvn --version 2>&1 | head -1)"
}

install_debian_node() {
    if cmd_exists node; then
        local node_major
        node_major=$(node --version | sed 's/^v//' | cut -d. -f1)
        if [[ "$node_major" -le 22 ]]; then
            info "Node.js already installed: $(node --version)"
            cmd_exists yarn || { npm install -g yarn; info "yarn installed"; }
            cmd_exists pnpm || { npm install -g pnpm; info "pnpm installed"; }
            return
        fi
        warn "Node.js > v22, need to install v22..."
    fi
    warn "Installing Node.js 22..."
    curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
    sudo apt-get install -y nodejs
    info "Node: $(node --version)"

    cmd_exists yarn || { npm install -g yarn; info "yarn installed"; }
    cmd_exists pnpm || { npm install -g pnpm; info "pnpm installed"; }
}

install_debian_dotnet() {
    if cmd_exists dotnet; then
        info ".NET SDK already installed: $(dotnet --version)"
        return
    fi
    warn "Installing .NET SDK..."
    sudo apt-get install -y dotnet-sdk-8.0 || {
        wget https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh
        chmod +x /tmp/dotnet-install.sh
        /tmp/dotnet-install.sh --channel 8.0
        export PATH="$HOME/.dotnet:$PATH"
    }
    info ".NET SDK installed: $(dotnet --version)"
}

install_debian_python_tools() {
    sudo apt-get install -y python3 python3-pip python3-venv python3-dev
    pip3 install --upgrade pip build maturin --break-system-packages 2>/dev/null || \
        pip3 install --upgrade pip build maturin
    info "Python3 + maturin installed"
}

install_debian_conan() {
    if cmd_exists conan; then
        info "Conan already installed: $(conan --version)"
        return
    fi
    pip3 install conan --break-system-packages 2>/dev/null || pip3 install conan
    # conan default profile (required by taos-gen build)
    if ! conan profile list 2>/dev/null | grep -q default; then
        conan profile detect
    fi
    info "Conan installed: $(conan --version)"
}

install_debian_path_config() {
    local shell_rc="$HOME/.bashrc"
    [[ -f "$HOME/.zshrc" ]] && shell_rc="$HOME/.zshrc"

    local marker="# === TSDB build deps PATH ==="
    if ! grep -q "$marker" "$shell_rc" 2>/dev/null; then
        {
            echo ""
            echo "$marker"
            echo 'export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"'
            echo 'export GO111MODULE=on'
            echo 'export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"'
            echo 'export PATH="$JAVA_HOME/bin:$PATH"'
            echo 'source "$HOME/.cargo/env" 2>/dev/null || true'
            echo "# === END TSDB build deps PATH ==="
        } >> "$shell_rc"
        info "PATH config appended to $shell_rc"
    else
        info "PATH config already exists in $shell_rc, skipping"
    fi
}

install_debian_all() {
    install_debian_system_packages
    install_debian_go
    install_debian_rust
    install_debian_java_maven
    install_debian_node
    install_debian_dotnet
    install_debian_python_tools
    install_debian_conan
    install_debian_path_config
}

# ══════════════════════════════════════════════════════════════════════════
#  RHEL/CentOS installation functions (yum)
# ══════════════════════════════════════════════════════════════════════════

install_redhat_system_packages() {
    echo ""
    echo "── YUM system packages ──"
    sudo yum install -y epel-release
    sudo yum install -y gcc gcc-c++ make cmake3 perl curl wget git pkgconfig \
        openssl-devel gflags jansson jansson-devel snappy snappy-devel \
        xz-devel zlib-devel bzip2-devel libatomic ca-certificates rsync \
        bison flex unixODBC-devel protobuf-compiler
    if ! cmd_exists cmake; then
        sudo ln -sf /usr/bin/cmake3 /usr/bin/cmake
    fi
    info "System build packages installed"
}

install_redhat_go() {
    if cmd_exists go; then
        info "Go already installed: $(go version)"
        return
    fi
    warn "Installing Go..."
    local GO_VERSION ARCH="amd64"
    GO_VERSION=$(curl -s https://go.dev/VERSION?m=text | grep -oP 'go[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    [[ "$(uname -m)" == "aarch64" ]] && ARCH="arm64"
    wget -q "https://go.dev/dl/${GO_VERSION}.linux-${ARCH}.tar.gz" -O /tmp/go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm -f /tmp/go.tar.gz
    export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"
    info "Go installed: $(go version)"

    go install github.com/magefile/mage@latest
    export PATH="$HOME/go/bin:$PATH"
}

install_redhat_rust() {
    if cmd_exists cargo; then
        info "Rust/Cargo already installed"
    else
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    fi
    local minor
    minor=$(rustc --version | awk '{print $2}' | cut -d. -f2)
    if [[ "$minor" -lt 85 ]]; then
        rustup update stable
    fi
    info "Rust: $(rustc --version)"
}

install_redhat_java_maven() {
    local java_major=0
    if cmd_exists java; then
        java_major=$(java_major_version)
    fi
    if ! cmd_exists java || [[ "$java_major" -lt 17 ]]; then
        sudo yum install -y java-17-openjdk java-17-openjdk-devel
    fi
    info "Java: $(java --version 2>&1 | head -1)"

    if ! cmd_exists mvn; then
        sudo yum install -y maven
    fi
    info "Maven: $(mvn --version 2>&1 | head -1)"
}

install_redhat_node() {
    if cmd_exists node; then
        local node_major
        node_major=$(node --version | sed 's/^v//' | cut -d. -f1)
        if [[ "$node_major" -le 22 ]]; then
            info "Node.js already installed: $(node --version)"
            cmd_exists yarn || npm install -g yarn
            cmd_exists pnpm || npm install -g pnpm
            return
        fi
    fi
    warn "Installing Node.js 22..."
    curl -fsSL https://rpm.nodesource.com/setup_22.x | sudo bash -
    sudo yum install -y nodejs
    info "Node: $(node --version)"
    cmd_exists yarn || npm install -g yarn
    cmd_exists pnpm || npm install -g pnpm
}

install_redhat_dotnet() {
    if cmd_exists dotnet; then
        info ".NET SDK already installed"
        return
    fi
    sudo yum install -y dotnet-sdk-8.0 || {
        wget https://dot.net/v1/dotnet-install.sh -O /tmp/dotnet-install.sh
        chmod +x /tmp/dotnet-install.sh
        /tmp/dotnet-install.sh --channel 8.0
        export PATH="$HOME/.dotnet:$PATH"
    }
    info ".NET SDK installed"
}

install_redhat_python_tools() {
    sudo yum install -y python3 python3-pip python3-devel
    pip3 install --upgrade pip build maturin
    info "Python3 + maturin installed"
}

install_redhat_conan() {
    if cmd_exists conan; then
        info "Conan already installed"
        return
    fi
    pip3 install conan
    if ! conan profile list 2>/dev/null | grep -q default; then
        conan profile detect
    fi
    info "Conan installed: $(conan --version)"
}

install_redhat_path_config() {
    local shell_rc="$HOME/.bashrc"
    [[ -f "$HOME/.zshrc" ]] && shell_rc="$HOME/.zshrc"

    local marker="# === TSDB build deps PATH ==="
    if ! grep -q "$marker" "$shell_rc" 2>/dev/null; then
        {
            echo ""
            echo "$marker"
            echo 'export PATH="/usr/local/go/bin:$HOME/go/bin:$PATH"'
            echo 'export GO111MODULE=on'
            echo 'source "$HOME/.cargo/env" 2>/dev/null || true'
            echo "# === END TSDB build deps PATH ==="
        } >> "$shell_rc"
        info "PATH config appended to $shell_rc"
    else
        info "PATH config already exists in $shell_rc, skipping"
    fi
}

install_redhat_all() {
    install_redhat_system_packages
    install_redhat_go
    install_redhat_rust
    install_redhat_java_maven
    install_redhat_node
    install_redhat_dotnet
    install_redhat_python_tools
    install_redhat_conan
    install_redhat_path_config
}

# ══════════════════════════════════════════════════════════════════════════
#  Verify installation
# ══════════════════════════════════════════════════════════════════════════
verify_all() {
    ensure_path
    echo ""
    echo "=============================="
    echo " Installation verification"
    echo "=============================="

    local tools_spec=(
        "cmake:cmake --version|head -1"
        "go:go version"
        "rustc:rustc --version"
        "cargo:cargo --version"
        "java:java -version 2>&1|head -1"
        "mvn:mvn --version 2>&1|head -1"
        "node:node --version"
        "yarn:yarn --version"
        "pnpm:pnpm --version"
        "python3:python3 --version"
        "python3-build:python3 -m build --version"
        "maturin:maturin --version"
        "dotnet:dotnet --version"
        "protoc:protoc --version"
        "bison:bison --version|head -1"
        "flex:flex --version 2>&1|head -1"
        "conan:conan --version"
        "mage:mage --version 2>&1|head -1"
    )

    local failed=0
    for tool_spec in "${tools_spec[@]}"; do
        local tool_name="${tool_spec%%:*}"
        local tool_cmd="${tool_spec#*:}"
        # 对于模块检查（名称含 -），直接运行命令而非 cmd_exists
        local found=false
        if [[ "$tool_name" == *-* ]]; then
            eval "$tool_cmd" >/dev/null 2>&1 && found=true
        else
            cmd_exists "$tool_name" && found=true
        fi
        if $found; then
            local ver
            ver=$(eval "$tool_cmd" 2>/dev/null || echo "version unknown")
            if [[ "$tool_name" == "java" ]]; then
                local j_major
                j_major=$(java_major_version)
                if [[ "$j_major" -lt 17 ]]; then
                    fail "  $tool_name — $ver (requires 17+)"
                    ((failed++)) || true
                    continue
                fi
            fi
            info "  $tool_name — $ver"
        else
            fail "  $tool_name — not found!"
            ((failed++)) || true
        fi
    done

    echo ""
    if [[ "$failed" -eq 0 ]]; then
        info "All dependencies installed successfully!"
    else
        fail "$failed tool(s) failed to install, please check the logs above."
    fi

    echo ""
    echo "=============================="
    echo " Build command:"
    echo "   cd debug && cmake .. && make -j\$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
    echo "=============================="
}

# ══════════════════════════════════════════════════════════════════════════
#  Main entry point
# ══════════════════════════════════════════════════════════════════════════
main() {
    detect_os

    if [[ "${1:-}" == "--check" ]]; then
        check_all
        exit $?
    fi

    echo ""
    echo "=============================="
    echo " Installing TSDB full-component build dependencies"
    echo "=============================="
    echo ""

    case "$OS" in
        macos)   install_macos_all ;;
        debian)  install_debian_all ;;
        redhat)  install_redhat_all ;;
        *)
            fail "Unsupported operating system: $OS"
            exit 1
            ;;
    esac

    verify_all
}

main "$@"
