#!/bin/bash

# ===== TDengine 安装包自动化构建、签名、公证脚本 =====
# 使用方法：./auto-build-notarize.sh /path/to/TDengine.pkgproj

# ===== 配置参数（请根据实际情况修改）=====
# PKGPROJ_PATH="$1"                # Packages.app 项目文件路径（必需参数）
OUTPUT_DIR="./dist"              # 输出目录
APP_NAME="TDengine"              # 应用名称
BUNDLE_ID="com.taosdata.tdengine" # 应用 Bundle ID
APP_CERT="Developer ID Application: Beijing Taosi Data Technology Co.,Ltd (L753KRYN2N)" # 应用签名证书
INSTALLER_CERT="Developer ID Installer: Beijing Taosi Data Technology Co.,Ltd (L753KRYN2N)" # 安装包签名证书
APPLE_ID="your-apple-id@example.com" # Apple ID
KEYCHAIN_PROFILE="NOTARIZATION_PASSWORD" # 钥匙串中的密码配置文件（需提前添加）

# ===== 函数定义 =====
log_info() {
  echo -e "\033[32m[INFO] $1\033[0m"
}

log_error() {
  echo -e "\033[31m[ERROR] $1\033[0m"
  exit 1
}

# ===== 主流程 =====
main() {
#   # 检查参数
#   if [ -z "$PKGPROJ_PATH" ]; then
#     log_error "请提供 Packages.app 项目文件路径（.pkgproj）"
#   fi

#   if [ ! -f "$PKGPROJ_PATH" ]; then
#     log_error "项目文件不存在: $PKGPROJ_PATH"
#   fi

  # 创建输出目录
  mkdir -p "$OUTPUT_DIR"
  log_info "输出目录: $OUTPUT_DIR"

#   # 1. 使用 packagesbuild 构建安装包（不签名）
#   log_info "开始构建安装包..."
#   PACKAGES_BUILD_PATH="$OUTPUT_DIR/$APP_NAME-unsigned.pkg"
#   /usr/local/bin/packagesbuild --target "10.15" --version "3.3.6.9" "$PKGPROJ_PATH" -o "$PACKAGES_BUILD_PATH"
  
#   if [ $? -ne 0 ]; then
#     log_error "安装包构建失败"
#   fi
#   log_info "安装包构建成功: $PACKAGES_BUILD_PATH"

  # 2. 解压安装包，提取二进制文件并签名
  log_info "开始签名二进制文件..."
  EXTRACT_DIR="$OUTPUT_DIR/extracted-pkg"
  mkdir -p "$EXTRACT_DIR"
  
  # 解压安装包
  pkgutil --expand "$PACKAGES_BUILD_PATH" "$EXTRACT_DIR"
  if [ $? -ne 0 ]; then
    log_error "安装包解压失败"
  fi

  # 递归签名所有二进制文件和动态库
  find "$EXTRACT_DIR" -type f \( -perm +ugo+x -o -name "*.dylib" \) -print0 | while IFS= read -r -d '' file; do
    log_info "签名文件: $file"
    codesign --force --sign "$APP_CERT" --timestamp=none "$file"
    
    # 验证签名
    codesign -dv "$file" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
      log_error "文件签名失败: $file"
    fi
  done
  log_info "所有二进制文件签名完成"

  # 3. 重新打包并使用 Installer 证书签名
  log_info "开始重新打包并签名安装包..."
  SIGNED_PKG_PATH="$OUTPUT_DIR/$APP_NAME-signed.pkg"
  
  # 使用 productbuild 重新打包（自动应用已签名的二进制文件）
  productbuild --distribution "$EXTRACT_DIR/Distribution" \
               --resources "$EXTRACT_DIR/Resources" \
               --package-path "$EXTRACT_DIR/Packages" \
               --sign "$INSTALLER_CERT" \
               --timestamp \
               "$SIGNED_PKG_PATH"
  
  if [ $? -ne 0 ]; then
    log_error "签名安装包失败"
  fi
  
  # 验证安装包签名
  pkgutil --check-signature "$SIGNED_PKG_PATH"
  if [ $? -ne 0 ]; then
    log_error "安装包签名验证失败"
  fi
  log_info "安装包签名成功: $SIGNED_PKG_PATH"

  # 4. 提交公证
  log_info "开始提交公证..."
  NOTARIZATION_LOG="$OUTPUT_DIR/notarization-$(date +%Y%m%d%H%M%S).log"
  
  # 提交公证请求
  NOTARIZATION_ID=$(xcrun altool --notarize-app \
                                  --primary-bundle-id "$BUNDLE_ID" \
                                  --username "$APPLE_ID" \
                                  --password "@keychain:$KEYCHAIN_PROFILE" \
                                  --file "$SIGNED_PKG_PATH" 2>&1 | grep "RequestUUID" | awk '{print $NF}')
  
  if [ -z "$NOTARIZATION_ID" ]; then
    log_error "公证提交失败，请检查 Apple ID 和密码"
  fi
  log_info "公证请求已提交，ID: $NOTARIZATION_ID"

  # 5. 轮询公证状态（每 30 秒检查一次）
  log_info "等待公证结果（可能需要几分钟）..."
  while true; do
    sleep 30
    xcrun altool --notarization-info "$NOTARIZATION_ID" \
                 --username "$APPLE_ID" \
                 --password "@keychain:$KEYCHAIN_PROFILE" > "$NOTARIZATION_LOG" 2>&1
    
    # 检查是否完成
    if grep -q "Status: success" "$NOTARIZATION_LOG"; then
      log_info "公证成功！"
      break
    elif grep -q "Status: in progress" "$NOTARIZATION_LOG"; then
      log_info "公证仍在处理中..."
      continue
    else
      log_error "公证失败！请查看日志: $NOTARIZATION_LOG"
    fi
  done

  # 6. 绑定公证信息到安装包
  log_info "开始绑定公证信息..."
  xcrun stapler staple "$SIGNED_PKG_PATH"
  
  if [ $? -ne 0 ]; then
    log_error "绑定公证信息失败"
  fi
  log_info "公证信息绑定成功"

  # 7. 清理临时文件
  log_info "清理临时文件..."
  rm -rf "$EXTRACT_DIR" "$PACKAGES_BUILD_PATH"
  log_info "清理完成"

  # 8. 输出最终结果
  log_info "========== 构建完成 =========="
  log_info "最终安装包: $SIGNED_PKG_PATH"
  log_info "公证日志: $NOTARIZATION_LOG"
  log_info "请将安装包分发给用户"
}

# 执行主流程
main