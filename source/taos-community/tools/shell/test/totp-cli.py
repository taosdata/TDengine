#!/usr/bin/env python3
import pyotp
import sys
import argparse
import time
import qrcode
from datetime import datetime

def generate_totp(secret_key, interval=30):
    """生成 TOTP 码"""
    totp = pyotp.TOTP(secret_key, interval=interval)
    current_code = totp.now()
    time_remaining = interval - (int(time.time()) % interval)
    
    print(f"当前 TOTP 码: {current_code}")
    print(f"剩余时间: {time_remaining} 秒")
    print(f"生成时间: {datetime.now().strftime('%H:%M:%S')}")
    
    return current_code

def generate_qr(secret_key, issuer="MyApp", account_name="user@example.com"):
    """生成二维码（需要安装 qrcode 库：pip install qrcode[pil]）"""
    try:
        import qrcode
        uri = pyotp.totp.TOTP(secret_key).provisioning_uri(
            account_name, issuer_name=issuer
        )
        qr = qrcode.QRCode()
        qr.add_data(uri)
        qr.make(fit=True)
        qr.print_ascii()
        print(f"\nTOTP URI: {uri}")
    except ImportError:
        print("要生成二维码，请先安装: pip install qrcode[pil]")

def main():
    parser = argparse.ArgumentParser(description="TOTP 生成器")
    parser.add_argument("secret", nargs="?", help="Base32 编码的密钥")
    parser.add_argument("--qr", action="store_true", help="生成二维码")
    parser.add_argument("--issuer", default="MyApp", help="发行方名称")
    parser.add_argument("--account", default="user@example.com", help="账户名称")
    parser.add_argument("--new", action="store_true", help="生成新密钥")
    
    args = parser.parse_args()
    
    if args.new:
        # 生成新密钥
        new_secret = pyotp.random_base32()
        print(f"新生成的密钥: {new_secret}")
        print(f"（请安全保存此密钥，不要泄露！）")
        generate_qr(new_secret, args.issuer, args.account)
    elif args.secret:
        if args.qr:
            generate_qr(args.secret, args.issuer, args.account)
        else:
            generate_totp(args.secret)
    else:
        print("请提供密钥或使用 --new 生成新密钥")
        print("示例: python totp_cli.py YOUR_SECRET_KEY")
        print("       python totp_cli.py --new")
        sys.exit(1)

if __name__ == "__main__":
    main()
