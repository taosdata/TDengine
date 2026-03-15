#!/bin/bash

# macOS 兼容版本（不使用关联数组和 bc）

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== Rust 镜像源速度测试 (macOS 兼容版) ==="
echo ""

# 使用普通数组（macOS Bash 3.2 不支持关联数组）
names=("官方源" "rsproxy" "清华 TUNA" "中科大 USTC" "阿里云" "火山引擎")
urls=("https://index.crates.io/"  "https://mirrors.tuna.tsinghua.edu.cn/crates.io-index/" "https://mirrors.ustc.edu.cn/crates.io-index/" "https://mirrors.aliyun.com/crates.io/index/" "https://mirrors.volcengine.com/crates.io-index/")

echo "1. 测试 Crates Index 连接延迟"
echo "----------------------------------------"

for i in "${!names[@]}"; do
    name="${names[$i]}"
    url="${urls[$i]}"
    
    # macOS 的 curl 一样可用，但用 awk 代替 bc
    time_str=$(curl -o /dev/null -s -w "%{time_connect}" --connect-timeout 5 "$url" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$time_str" ]; then
        # 用 awk 转换为毫秒（代替 bc）
        time_ms=$(echo "$time_str" | awk '{printf "%.0f", $1 * 1000}')
        
        if [ "$time_ms" -lt 100 ]; then
            echo -e "${GREEN}$name: ${time_str}s (~${time_ms}ms)${NC}"
        elif [ "$time_ms" -lt 300 ]; then
            echo -e "${YELLOW}$name: ${time_str}s (~${time_ms}ms)${NC}"
        else
            echo -e "${RED}$name: ${time_str}s (~${time_ms}ms)${NC}"
        fi
    else
        echo -e "${RED}$name: 连接失败${NC}"
    fi
done

echo ""
echo "2. 测试 Crates Index 下载速度 (下载索引文件)"
echo "----------------------------------------"

for i in "${!names[@]}"; do
    name="${names[$i]}"
    url="${urls[$i]}"
    
    # 下载测试（测试 5 秒内的平均速度）
    speed_str=$(curl -o /dev/null -s -w "%{speed_download}" --max-time 5 "$url"3/s/serde 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$speed_str" ]; then
        # 转换为 KB/s（awk 代替 bc）
        speed_kb=$(echo "$speed_str" | awk '{printf "%.0f", $1 / 1024}')
        
        if [ "$speed_kb" -gt 1024 ]; then
            speed_mb=$(echo "$speed_kb" | awk '{printf "%.2f", $1 / 1024}')
            echo -e "${GREEN}$name: ${speed_mb} MB/s${NC}"
        else
            echo -e "${YELLOW}$name: ${speed_kb} KB/s${NC}"
        fi
    else
        echo -e "${RED}$name: 下载失败${NC}"
    fi
done

echo ""
echo "3. 测试 Rustup 工具链源延迟"
echo "----------------------------------------"

rustup_names=("官方源" "清华 TUNA" "中科大 USTC")
rustup_urls=("https://static.rust-lang.org/dist/channel-rust-stable.toml" "https://mirrors.tuna.tsinghua.edu.cn/rustup/dist/channel-rust-stable.toml" "https://mirrors.ustc.edu.cn/rustup/dist/channel-rust-stable.toml")

for i in "${!rustup_names[@]}"; do
    name="${rustup_names[$i]}"
    url="${rustup_urls[$i]}"
    time_str=$(curl -o /dev/null -s -w "%{time_connect}" --connect-timeout 5 "$url" 2>/dev/null)
    
    if [ $? -eq 0 ] && [ -n "$time_str" ]; then
        echo -e "${GREEN}$name: ${time_str}s${NC}"
    else
        echo -e "${RED}$name: 连接失败${NC}"
    fi
done

echo ""
echo "=== 测试完成 ==="
