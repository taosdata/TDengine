# 配置 WSL 使用 Windows 代理
export hostip=$(cat /etc/resolv.conf |grep -oP '(?<=nameserver\ ).*')
export https_proxy="http://${hostip}:7890"
export http_proxy="http://${hostip}:7890"
export all_proxy="socks5://${hostip}:7890"

# 配置 Git 使用代理
git config --global http.proxy "http://${hostip}:7890"
git config --global https.proxy "http://${hostip}:7890" 