#!/usr/bin/env python3
"""
TDGPT Windows Uninstallation Script
Windows 平台的 TDGPT 卸载脚本

Usage:
    python uninstall.py [options]

Options:
    --keep-all          保留所有数据（cfg、model、data、venv）
    --remove-venv       删除虚拟环境（默认保留）
    --remove-data       删除数据目录（默认保留）
    --remove-model      删除模型目录（默认保留）
    -h, --help          显示帮助信息

Examples:
    python uninstall.py                 # 标准卸载（保留 cfg、model、data、venv）
    python uninstall.py --remove-venv   # 卸载并删除虚拟环境
    python uninstall.py --keep-all      # 仅停止服务，保留所有文件
"""

import os
import sys
import platform
import subprocess
import argparse
import logging
import shutil
import time
from pathlib import Path
from typing import List

# 确保是 Windows 平台
if platform.system().lower() != "windows":
    print("Error: This script is for Windows only.")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 颜色输出
try:
    import ctypes
    kernel32 = ctypes.windll.kernel32
    kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    COLORS_ENABLED = True
except:
    COLORS_ENABLED = False

class Colors:
    """ANSI 颜色代码"""
    RED = '\033[0;31m' if COLORS_ENABLED else ''
    GREEN = '\033[1;32m' if COLORS_ENABLED else ''
    YELLOW = '\033[1;33m' if COLORS_ENABLED else ''
    BLUE = '\033[0;34m' if COLORS_ENABLED else ''
    NC = '\033[0m' if COLORS_ENABLED else ''

# 默认安装路径
INSTALL_DIR = Path(r"C:\TDengine\taosanode")


class WindowsUninstaller:
    """Windows 卸载器"""

    def __init__(self, keep_all: bool = False, remove_venv: bool = False,
                 remove_data: bool = False, remove_model: bool = False):
        self.keep_all = keep_all
        self.remove_venv = remove_venv
        self.remove_data = remove_data
        self.remove_model = remove_model
        self.install_dir = INSTALL_DIR

    def print_info(self, message: str):
        """打印信息"""
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {message}")
        logger.info(message)

    def print_warning(self, message: str):
        """打印警告"""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
        logger.warning(message)

    def print_error(self, message: str):
        """打印错误"""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
        logger.error(message)

    def print_success(self, message: str):
        """打印成功"""
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
        logger.info(message)

    def stop_services(self) -> bool:
        """停止所有服务"""
        self.print_info("停止 TDGPT 服务...")

        # 停止主服务
        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "stop"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_info("主服务已停止")
        except Exception as e:
            self.print_warning(f"停止主服务失败：{e}")

        # 停止模型服务
        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "model-stop", "all"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_info("模型服务已停止")
        except Exception as e:
            self.print_warning(f"停止模型服务失败：{e}")

        # 强制终止残留进程
        time.sleep(2)
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "python.exe", "/T"],
                capture_output=True,
                timeout=10
            )
        except:
            pass

        return True

    def uninstall_service(self) -> bool:
        """卸载 Windows 服务"""
        self.print_info("卸载 Windows 服务...")

        try:
            service_script = self.install_dir / "bin" / "taosanode_service.py"
            if service_script.exists():
                venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
                if venv_python.exists():
                    subprocess.run(
                        [str(venv_python), str(service_script), "uninstall"],
                        timeout=30,
                        capture_output=True
                    )
                    self.print_success("Windows 服务已卸载")
        except Exception as e:
            self.print_warning(f"卸载服务失败：{e}")

        return True

    def remove_from_path(self) -> bool:
        """从 PATH 环境变量中移除"""
        self.print_info("从 PATH 中移除安装目录...")

        try:
            bin_path = str(self.install_dir / "bin")

            # 读取当前 PATH
            result = subprocess.run(
                ["reg", "query", "HKLM\\SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment", "/v", "Path"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                # 解析 PATH
                for line in result.stdout.split('\n'):
                    if 'Path' in line and 'REG_' in line:
                        parts = line.split('REG_EXPAND_SZ', 1)
                        if len(parts) == 2:
                            current_path = parts[1].strip()

                            # 移除我们的路径
                            path_list = [p.strip() for p in current_path.split(';') if p.strip()]
                            new_path_list = [p for p in path_list if bin_path.lower() not in p.lower()]

                            if len(new_path_list) < len(path_list):
                                new_path = ';'.join(new_path_list)

                                # 更新 PATH
                                subprocess.run(
                                    ["setx", "/M", "Path", new_path],
                                    capture_output=True,
                                    timeout=10
                                )
                                self.print_success("已从 PATH 中移除")
                            else:
                                self.print_info("PATH 中未找到安装目录")

        except Exception as e:
            self.print_warning(f"从 PATH 中移除失败：{e}")

        return True

    def remove_files(self) -> bool:
        """删除文件"""
        self.print_info("删除文件...")

        if self.keep_all:
            self.print_info("保留所有文件（--keep-all 模式）")
            return True

        # 要删除的目录
        dirs_to_remove = ["bin", "lib", "resource", "log"]

        # 根据选项决定是否删除
        if self.remove_venv:
            dirs_to_remove.extend(["venv", "timesfm_venv", "moirai_venv", "chronos_venv", "momentfm_venv"])
        if self.remove_data:
            dirs_to_remove.append("data")
        if self.remove_model:
            dirs_to_remove.append("model")

        # 删除目录
        for dir_name in dirs_to_remove:
            dir_path = self.install_dir / dir_name
            if dir_path.exists():
                try:
                    shutil.rmtree(dir_path)
                    self.print_info(f"删除目录：{dir_name}")
                except Exception as e:
                    self.print_warning(f"删除目录 {dir_name} 失败：{e}")

        # 删除根目录下的文件
        files_to_remove = ["install.log", "taosanode.pid", "*.txt"]
        for pattern in files_to_remove:
            if '*' in pattern:
                for file_path in self.install_dir.glob(pattern):
                    if file_path.is_file():
                        try:
                            file_path.unlink()
                            self.print_info(f"删除文件：{file_path.name}")
                        except:
                            pass
            else:
                file_path = self.install_dir / pattern
                if file_path.exists():
                    try:
                        file_path.unlink()
                        self.print_info(f"删除文件：{pattern}")
                    except:
                        pass

        return True

    def show_preserved_files(self):
        """显示保留的文件"""
        self.print_info("=" * 60)
        self.print_info("以下目录已保留：")

        preserved = []
        if not self.remove_venv:
            if (self.install_dir / "venv").exists():
                preserved.append("  - venv（虚拟环境）")
        if not self.remove_data:
            if (self.install_dir / "data").exists():
                preserved.append("  - data（数据目录）")
        if not self.remove_model:
            if (self.install_dir / "model").exists():
                preserved.append("  - model（模型目录）")
        if (self.install_dir / "cfg").exists():
            preserved.append("  - cfg（配置文件）")

        if preserved:
            for item in preserved:
                self.print_info(item)
            self.print_info("")
            self.print_info("如需完全删除，请手动删除以下目录：")
            self.print_info(f"  {self.install_dir}")
        else:
            self.print_info("  无保留文件")

        self.print_info("=" * 60)

    def generate_uninstall_report(self) -> bool:
        """生成卸载报告"""
        report_file = self.install_dir / "uninstall.log"

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("=" * 60 + "\n")
                f.write("TDGPT Windows 卸载报告\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"卸载目录：{self.install_dir}\n")
                f.write(f"保留模式：{'是' if self.keep_all else '否'}\n")
                f.write(f"删除 venv：{'是' if self.remove_venv else '否'}\n")
                f.write(f"删除 data：{'是' if self.remove_data else '否'}\n")
                f.write(f"删除 model：{'是' if self.remove_model else '否'}\n")
                f.write("\n")
                f.write("保留的目录：\n")
                if (self.install_dir / "cfg").exists():
                    f.write("  ✓ cfg\n")
                if not self.remove_venv and (self.install_dir / "venv").exists():
                    f.write("  ✓ venv\n")
                if not self.remove_data and (self.install_dir / "data").exists():
                    f.write("  ✓ data\n")
                if not self.remove_model and (self.install_dir / "model").exists():
                    f.write("  ✓ model\n")
                f.write("\n")
                f.write("=" * 60 + "\n")

            self.print_success(f"卸载报告已保存：{report_file}")
            return True
        except Exception as e:
            self.print_warning(f"生成卸载报告失败：{e}")
            return True

    def run(self) -> bool:
        """执行卸载"""
        self.print_info("=" * 60)
        self.print_info("TDGPT Windows 卸载程序")
        self.print_info("=" * 60)
        self.print_info(f"安装目录：{self.install_dir}")
        self.print_info(f"保留模式：{'是' if self.keep_all else '否'}")
        self.print_info(f"删除 venv：{'是' if self.remove_venv else '否'}")
        self.print_info(f"删除 data：{'是' if self.remove_data else '否'}")
        self.print_info(f"删除 model：{'是' if self.remove_model else '否'}")
        self.print_info("=" * 60)

        # 1. 停止服务
        self.stop_services()

        # 2. 卸载服务
        self.uninstall_service()

        # 3. 从 PATH 中移除
        self.remove_from_path()

        # 4. 删除文件
        self.remove_files()

        # 6. 生成卸载报告
        self.generate_uninstall_report()

        # 7. 显示保留的文件
        self.show_preserved_files()

        # 完成
        self.print_info("=" * 60)
        self.print_success("TDGPT 卸载完成！")
        self.print_info("=" * 60)

        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="TDGPT Windows 卸载脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python uninstall.py                 # 标准卸载（保留 cfg、model、data、venv）
  python uninstall.py --remove-venv   # 卸载并删除虚拟环境
  python uninstall.py --keep-all      # 仅停止服务，保留所有文件

注意：
  默认情况下，卸载会保留以下目录：
    - cfg（配置文件）
    - model（模型文件）
    - data（数据目录）
    - venv（虚拟环境）

  这与 Linux 的卸载行为一致，确保用户数据不会丢失。
        """
    )

    parser.add_argument(
        "--keep-all",
        action="store_true",
        help="保留所有数据（cfg、model、data、venv）"
    )

    parser.add_argument(
        "--remove-venv",
        action="store_true",
        help="删除虚拟环境（默认保留）"
    )

    parser.add_argument(
        "--remove-data",
        action="store_true",
        help="删除数据目录（默认保留）"
    )

    parser.add_argument(
        "--remove-model",
        action="store_true",
        help="删除模型目录（默认保留）"
    )

    args = parser.parse_args()

    # 创建卸载器并执行
    uninstaller = WindowsUninstaller(
        keep_all=args.keep_all,
        remove_venv=args.remove_venv,
        remove_data=args.remove_data,
        remove_model=args.remove_model
    )

    try:
        success = uninstaller.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n卸载被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n卸载过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
