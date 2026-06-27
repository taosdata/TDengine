#!/bin/bash
# 为 tdgpt 生成自签名证书用于开发和测试
# 用法: bash setup_ssl_test.sh [output_dir]

set -e

OUTPUT_DIR="${1:-.}"
CERT_FILE="$OUTPUT_DIR/cert.pem"
KEY_FILE="$OUTPUT_DIR/key.pem"

echo "Generating self-signed SSL certificate for tdgpt testing..."
echo "Output directory: $OUTPUT_DIR"

# 创建输出目录（如果不存在）
mkdir -p "$OUTPUT_DIR"

# 生成自签名证书和私钥
openssl req -x509 -newkey rsa:4096 -nodes -days 365 \
  -out "$CERT_FILE" \
  -keyout "$KEY_FILE" \
  -subj "/C=CN/ST=BJ/L=Beijing/O=TDengine/CN=localhost"

echo ""
echo "✓ Certificate generated successfully!"
echo ""
echo "Certificate file: $CERT_FILE"
echo "Key file: $KEY_FILE"
echo ""
echo "To start tdgpt with SSL in development mode:"
echo "  python -m taosanalytics.app --cert $CERT_FILE --key $KEY_FILE"
echo ""
echo "To test HTTPS connection:"
echo "  curl -k https://localhost:6035/status"
echo ""
echo "Certificate info:"
openssl x509 -in "$CERT_FILE" -text -noout | grep -E "Subject|Not Before|Not After|Public-Key"
