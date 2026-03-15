#!/usr/bin/env python3
"""
TDGPT Windows Installation Script
Windows 平台的 TDGPT 安装脚本

Usage:
    python install.py [options]

Options:
    -o, --offline       离线模式：跳过已存在的虚拟环境
    -a, --all-models    安装所有模型虚拟环境
    -h, --help          显示帮助信息

Examples:
    python install.py                    # 在线模式，仅安装必需模型
    python install.py -o                 # 离线模式
    python install.py -a                 # 安装所有模型
    python install.py -o -a              # 离线模式 + 所有模型
"""

import os
import sys
import platform
import subprocess
import argparse
import logging
import shutil
import tarfile
from pathlib import Path
from typing import Optional, List, Tuple

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

# 颜色输出（Windows 10+ 支持 ANSI）
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
    NC = '\033[0m' if COLORS_ENABLED else ''  # No Color

# 默认安装路径
INSTALL_DIR = Path(r"C:\TDengine\taosanode")

# 模型配置
REQUIRED_MODELS = ["tdtsfm", "timemoe"]
OPTIONAL_MODELS = ["chronos", "timesfm", "moirai", "moment-large"]

# 虚拟环境配置
VENV_CONFIGS = {
    "venv": {
        "requirements": "requirements_ess.txt",
        "description": "主虚拟环境"
    },
    "timesfm_venv": {
        "requirements": "requirements_timesfm.txt",
        "description": "TimesFM 模型环境"
    },
    "moirai_venv": {
        "requirements": "requirements_moirai.txt",
        "description": "Moirai 模型环境"
    },
    "chronos_venv": {
        "requirements": "requirements_chronos.txt",
        "description": "Chronos 模型环境"
    },
    "momentfm_venv": {
        "requirements": "requirements_moment.txt",
        "description": "MomentFM 模型环境"
    }
}


class WindowsInstaller:
    """Windows 安装器"""

    def __init__(self, offline: bool = False, all_models: bool = False):
        self.offline = offline
        self.all_models = all_models
        self.install_dir = INSTALL_DIR
        self.python_cmd = None
        self.python_version = None
        self.pip_index_url = os.environ.get("PIP_INDEX_URL", "")

    def print_info(self, message: str):
        """打印信息"""
        print(f"{Colors.GREEN}[INFO]{Colors.NC} {message}")
        logger.info(message)

    def print_progress(self, current: int, total: int, message: str):
        """打印进度条"""
        percent = int((current / total) * 100)
        bar_length = 50
        filled = int((current / total) * bar_length)
        bar = '█' * filled + '░' * (bar_length - filled)
        print(f"\r{Colors.BLUE}[{bar}] {percent}%{Colors.NC} {message}", end='', flush=True)
        if current == total:
            print()  # 完成后换行

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

    def check_admin_privileges(self) -> bool:
        """检查管理员权限"""
        try:
            import ctypes
            is_admin = ctypes.windll.shell32.IsUserAnAdmin()
            if not is_admin:
                self.print_warning("建议以管理员权限运行此脚本以确保服务注册成功")
                return False
            return True
        except:
            return False

    def check_disk_space(self) -> bool:
        """检查磁盘空间"""
        try:
            drive = self.install_dir.drive
            stat = shutil.disk_usage(drive)
            free_gb = stat.free / (1024 ** 3)

            if free_gb < 20:
                self.print_error(f"磁盘空间不足：{drive} 仅剩 {free_gb:.2f} GB，建议至少 20 GB")
                return False

            self.print_info(f"磁盘空间检查通过：{drive} 可用 {free_gb:.2f} GB")
            return True
        except Exception as e:
            self.print_warning(f"无法检查磁盘空间：{e}")
            return True

    def check_python_version(self) -> bool:
        """检查 Python 版本"""
        self.print_info("检查 Python 环境...")

        # 尝试不同的 Python 命令
        python_commands = ["python", "python3", "python3.10", "python3.11", "python3.12"]

        for cmd in python_commands:
            try:
                result = subprocess.run(
                    [cmd, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=5
                )

                if result.returncode == 0:
                    version_str = result.stdout.strip()
                    # 解析版本号
                    parts = version_str.split()
                    if len(parts) >= 2:
                        version = parts[1]
                        major, minor = map(int, version.split('.')[:2])

                        if major == 3 and minor in [10, 11, 12]:
                            self.python_cmd = cmd
                            self.python_version = f"{major}.{minor}"
                            self.print_success(f"找到 Python {version} ({cmd})")
                            return True
            except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
                continue

        self.print_error("未找到 Python 3.10/3.11/3.12")
        self.print_error("请从 https://www.python.org/ 下载并安装 Python 3.10、3.11 或 3.12")
        return False

    def check_pip_version(self) -> bool:
        """检查 pip 版本"""
        try:
            result = subprocess.run(
                [self.python_cmd, "-m", "pip", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                self.print_info(f"pip 版本：{result.stdout.strip()}")
                return True
            else:
                self.print_error("pip 未安装或不可用")
                return False
        except Exception as e:
            self.print_error(f"检查 pip 失败：{e}")
            return False

    def create_directories(self) -> bool:
        """创建目录结构"""
        self.print_info("创建目录结构...")

        directories = [
            self.install_dir / "log",
            self.install_dir / "data" / "pids",
            self.install_dir / "model",
        ]

        try:
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                self.print_info(f"创建目录：{directory}")

            return True
        except Exception as e:
            self.print_error(f"创建目录失败：{e}")
            return False

    def extract_models(self) -> bool:
        """提取模型文件"""
        self.print_info("提取模型文件...")

        model_dir = self.install_dir / "model"
        model_source_dir = self.install_dir / "model"  # 假设模型文件在这里

        # 必需模型
        for model_name in REQUIRED_MODELS:
            tar_file = model_source_dir / f"{model_name}.tar.gz"
            if tar_file.exists():
                try:
                    self.print_info(f"提取模型：{model_name}")
                    with tarfile.open(tar_file, "r:gz") as tar:
                        tar.extractall(model_dir)
                except Exception as e:
                    self.print_error(f"提取模型 {model_name} 失败：{e}")
                    return False
            else:
                self.print_warning(f"模型文件不存在：{tar_file}")

        # 可选模型（仅在 -a 模式下）
        if self.all_models:
            for model_name in OPTIONAL_MODELS:
                tar_file = model_source_dir / f"{model_name}.tar.gz"
                if tar_file.exists():
                    try:
                        self.print_info(f"提取可选模型：{model_name}")
                        with tarfile.open(tar_file, "r:gz") as tar:
                            tar.extractall(model_dir)
                    except Exception as e:
                        self.print_warning(f"提取可选模型 {model_name} 失败：{e}")
                else:
                    self.print_info(f"可选模型文件不存在：{tar_file}")

        return True

    def create_venv(self, venv_name: str, requirements_file: str, description: str) -> bool:
        """创建虚拟环境"""
        venv_path = self.install_dir / venv_name

        # 离线模式：如果 venv 已存在，跳过
        if self.offline and venv_path.exists():
            self.print_info(f"离线模式：跳过已存在的 {description} ({venv_name})")
            return True

        self.print_info(f"创建 {description} ({venv_name})...")

        try:
            # 创建虚拟环境
            self.print_progress(1, 4, f"正在创建虚拟环境 {venv_name}...")
            subprocess.run(
                [self.python_cmd, "-m", "venv", str(venv_path)],
                check=True,
                timeout=120
            )

            # 激活虚拟环境并安装依赖
            pip_cmd = venv_path / "Scripts" / "pip.exe"
            python_venv = venv_path / "Scripts" / "python.exe"

            # 升级 pip
            self.print_progress(2, 4, f"正在升级 pip...")
            subprocess.run(
                [str(python_venv), "-m", "pip", "install", "--upgrade", "pip"],
                check=True,
                timeout=120,
                capture_output=True
            )

            # 安装依赖
            req_file = self.install_dir / requirements_file
            if req_file.exists():
                self.print_progress(3, 4, f"正在安装依赖包...")

                pip_args = [str(pip_cmd), "install", "-r", str(req_file)]
                if self.pip_index_url:
                    pip_args.extend(["-i", self.pip_index_url])

                subprocess.run(
                    pip_args,
                    check=True,
                    timeout=1800  # 30 分钟超时
                )
            else:
                self.print_warning(f"依赖文件不存在：{req_file}")

            self.print_progress(4, 4, f"{description} 创建完成")
            self.print_success(f"{description} 创建成功")
            return True

        except subprocess.TimeoutExpired:
            print()  # 换行
            self.print_error(f"创建 {description} 超时")
            return False
        except subprocess.CalledProcessError as e:
            print()  # 换行
            self.print_error(f"创建 {description} 失败：{e}")
            return False
        except Exception as e:
            print()  # 换行
            self.print_error(f"创建 {description} 时发生错误：{e}")
            return False

    def install_venvs(self) -> bool:
        """安装虚拟环境"""
        self.print_info("开始安装虚拟环境...")

        # 主虚拟环境（必需）
        if not self.create_venv("venv", "requirements_ess.txt", "主虚拟环境"):
            return False

        # 额外虚拟环境（可选）
        if self.all_models:
            for venv_name, config in VENV_CONFIGS.items():
                if venv_name == "venv":
                    continue  # 跳过主环境

                self.create_venv(venv_name, config["requirements"], config["description"])

        return True

    def install_service(self) -> bool:
        """安装 Windows 服务"""
        self.print_info("安装 Windows 服务...")

        service_script = self.install_dir / "bin" / "taosanode_service.py"
        if not service_script.exists():
            self.print_warning("服务管理脚本不存在，跳过服务安装")
            return True

        try:
            # 使用 taosanode_service.py 安装服务
            venv_python = self.install_dir / "venv" / "Scripts" / "python.exe"
            subprocess.run(
                [str(venv_python), str(service_script), "install"],
                check=True,
                timeout=60
            )

            self.print_success("Windows 服务安装成功")
            return True
        except subprocess.CalledProcessError as e:
            self.print_warning(f"服务安装失败：{e}")
            self.print_info("您可以稍后手动安装服务")
            return True
        except Exception as e:
            self.print_warning(f"服务安装时发生错误：{e}")
            return True

    def generate_install_report(self) -> bool:
        """生成安装报告"""
        self.print_info("生成安装报告...")

        report_file = self.install_dir / "install.log"

        try:
            with open(report_file, "w", encoding="utf-8") as f:
                f.write("=" * 60 + "\n")
                f.write("TDGPT Windows 安装报告\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"安装目录：{self.install_dir}\n")
                f.write(f"Python 版本：{self.python_version}\n")
                f.write(f"安装模式：{'离线' if self.offline else '在线'}\n")
                f.write(f"模型范围：{'所有模型' if self.all_models else '必需模型'}\n")
                f.write(f"pip 镜像：{self.pip_index_url or '默认'}\n")
                f.write("\n")
                f.write("安装的虚拟环境：\n")
                for venv_name in VENV_CONFIGS.keys():
                    venv_path = self.install_dir / venv_name
                    if venv_path.exists():
                        f.write(f"  ✓ {venv_name}\n")
                    else:
                        f.write(f"  ✗ {venv_name}\n")
                f.write("\n")
                f.write("=" * 60 + "\n")

            self.print_success(f"安装报告已保存：{report_file}")
            return True
        except Exception as e:
            self.print_warning(f"生成安装报告失败：{e}")
            return True

    def run(self) -> bool:
        """执行安装"""
        self.print_info("=" * 60)
        self.print_info("TDGPT Windows 安装程序")
        self.print_info("=" * 60)
        self.print_info(f"安装目录：{self.install_dir}")
        self.print_info(f"安装模式：{'离线' if self.offline else '在线'}")
        self.print_info(f"模型范围：{'所有模型' if self.all_models else '必需模型'}")
        self.print_info("=" * 60)

        # 1. 环境检查
        if not self.check_admin_privileges():
            pass  # 仅警告，不阻止安装

        if not self.check_disk_space():
            return False

        if not self.check_python_version():
            return False

        if not self.check_pip_version():
            return False

        # 2. 创建目录结构
        if not self.create_directories():
            return False

        # 3. 提取模型文件
        if not self.extract_models():
            return False

        # 4. 创建虚拟环境
        if not self.install_venvs():
            return False

        # 5. 安装服务
        self.install_service()

        # 6. 生成安装报告
        self.generate_install_report()

        # 完成
        self.print_info("=" * 60)
        self.print_success("TDGPT 安装完成！")
        self.print_info("=" * 60)
        self.print_info(f"安装目录：{self.install_dir}")
        self.print_info(f"日志目录：{self.install_dir / 'log'}")
        self.print_info(f"数据目录：{self.install_dir / 'data'}")
        self.print_info(f"模型目录：{self.install_dir / 'model'}")
        self.print_info("")
        self.print_info("启动服务：")
        self.print_info(f"  {self.install_dir / 'bin' / 'start-taosanode.bat'}")
        self.print_info("")
        self.print_info("查看状态：")
        self.print_info(f"  {self.install_dir / 'bin' / 'status-taosanode.bat'}")
        self.print_info("=" * 60)

        return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="TDGPT Windows 安装脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python install.py                    # 在线模式，仅安装必需模型
  python install.py -o                 # 离线模式
  python install.py -a                 # 安装所有模型
  python install.py -o -a              # 离线模式 + 所有模型
        """
    )

    parser.add_argument(
        "-o", "--offline",
        action="store_true",
        help="离线模式：跳过已存在的虚拟环境"
    )

    parser.add_argument(
        "-a", "--all-models",
        action="store_true",
        help="安装所有模型虚拟环境"
    )

    args = parser.parse_args()

    # 创建安装器并执行
    installer = WindowsInstaller(
        offline=args.offline,
        all_models=args.all_models
    )

    try:
        success = installer.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n安装被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n安装过程中发生错误：{e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
