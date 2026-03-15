#!/usr/bin/env python3
"""
TDGPT Auto-Update Checker - 自动更新检查
检测新版本并提示用户升级
"""

import os
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path
from typing import Optional, Dict
import re

class UpdateChecker:
    def __init__(self):
        self.current_version = self.get_current_version()
        self.update_url = "https://www.taosdata.com/tdgpt/version.json"  # 示例URL
        self.install_dir = Path(r"C:\TDengine\taosanode")

    def get_current_version(self) -> str:
        """获取当前版本"""
        version_file = Path(r"C:\TDengine\taosanode\VERSION")

        if version_file.exists():
            try:
                with open(version_file, 'r', encoding='utf-8') as f:
                    return f.read().strip()
            except:
                pass

        return "unknown"

    def check_for_updates(self) -> Optional[Dict]:
        """检查更新"""
        try:
            # 发送请求获取最新版本信息
            req = urllib.request.Request(
                self.update_url,
                headers={'User-Agent': 'TDGPT-UpdateChecker/1.0'}
            )

            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode('utf-8'))

                latest_version = data.get('version', '')
                download_url = data.get('download_url', '')
                release_notes = data.get('release_notes', '')

                if self.is_newer_version(latest_version):
                    return {
                        'current_version': self.current_version,
                        'latest_version': latest_version,
                        'download_url': download_url,
                        'release_notes': release_notes
                    }

                return None

        except urllib.error.URLError as e:
            print(f"无法连接到更新服务器: {e}")
            return None
        except Exception as e:
            print(f"检查更新时发生错误: {e}")
            return None

    def is_newer_version(self, latest_version: str) -> bool:
        """比较版本号"""
        if self.current_version == "unknown":
            return False

        try:
            # 解析版本号 (例如: 1.0.0)
            current_parts = [int(x) for x in self.current_version.split('.')]
            latest_parts = [int(x) for x in latest_version.split('.')]

            # 补齐长度
            max_len = max(len(current_parts), len(latest_parts))
            current_parts.extend([0] * (max_len - len(current_parts)))
            latest_parts.extend([0] * (max_len - len(latest_parts)))

            # 比较
            for c, l in zip(current_parts, latest_parts):
                if l > c:
                    return True
                elif l < c:
                    return False

            return False

        except:
            return False

    def show_update_notification(self, update_info: Dict):
        """显示更新通知"""
        print("=" * 60)
        print("🎉 TDGPT 新版本可用！")
        print("=" * 60)
        print(f"当前版本: {update_info['current_version']}")
        print(f"最新版本: {update_info['latest_version']}")
        print()
        print("更新内容:")
        print(update_info['release_notes'])
        print()
        print(f"下载地址: {update_info['download_url']}")
        print("=" * 60)

    def run(self):
        """执行更新检查"""
        print("正在检查更新...")

        update_info = self.check_for_updates()

        if update_info:
            self.show_update_notification(update_info)

            # 询问用户是否下载
            try:
                response = input("\n是否打开下载页面？(y/n): ").strip().lower()
                if response == 'y':
                    import webbrowser
                    webbrowser.open(update_info['download_url'])
            except:
                pass
        else:
            print("✓ 您已经在使用最新版本")


def main():
    checker = UpdateChecker()
    checker.run()


if __name__ == "__main__":
    main()
