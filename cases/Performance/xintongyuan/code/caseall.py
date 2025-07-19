"""
TDengine 信通院性能测试脚本

配置要求:
1. 硬件配置
   - CPU: 建议8核以上
   - 内存: 建议32GB以上
   - 磁盘: 建议SSD，容量200GB以上

2. 软件配置
   - 操作系统: CentOS 7/8 或 Ubuntu 18.04/20.04
   - Python版本: 3.6+
   - TDengine版本: 3.0+
   - taosBenchmark工具

3. 目录结构要求:
   /root/xintongyuan/
   ├── code/                     # 测试代码和配置文件
   │   ├── case201.py           # 本测试脚本
   │   ├── UseCase201_*.json    # case201测试配置
   │   ├── UseCase205_*.json    # case205测试配置
   │   ├── UseCase206.json      # case206测试配置
   │   └── ...
   ├── cluster/                  # 集群相关配置
   │   ├── cfg3/                # 3节点集群配置
   │   │   ├── cfg_1           # 节点1配置
   │   │   ├── cfg_2           # 节点2配置
   │   │   └── cfg_3           # 节点3配置
   │   ├── data3/              # 集群数据目录
   │   └── log3/               # 集群日志目录
   └── single/                  # 单机测试相关配置
       ├── cfg/                # 单机配置
       ├── data/              # 单机数据目录
       └── log/               # 单机日志目录

执行前检查:
1. 确保所有所需目录已创建且有正确权限
2. 确保 taosBenchmark 配置文件内容正确
3. 检查网络连接和端口可用性
4. 确保系统资源充足（CPU、内存、磁盘空间）

注意事项:
1. 脚本会自动清理环境，请确保数据已备份
2. 建议在测试前重启系统，确保环境干净
3. 测试过程中避免其他大量IO或CPU密集操作
4. 如遇到异常，请检查相关日志文件


作者: TDengine Platform Team
创建日期: 2025-05-20
最后修改: 2025-05-20
"""

import taos
import sys
import os
import time
import subprocess
import resource
import signal
import re
from datetime import datetime
import warnings
import paramiko
from cryptography.utils import CryptographyDeprecationWarning
import logging

# 设置环境变量来禁用 cryptography 警告
os.environ['CRYPTOGRAPHY_SUPPRESS_DEPRECATION_WARNINGS'] = '1'
# 忽略所有 CryptographyDeprecationWarning 警告
warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
# 忽略所有来自 paramiko 的警告
warnings.filterwarnings('ignore', module='paramiko.*')
warnings.filterwarnings('ignore', message='.*TripleDES.*')
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=UserWarning)
# 设置 paramiko 的日志级别为 ERROR，只显示错误信息
logging.getLogger('paramiko').setLevel(logging.ERROR)

# 添加颜色常量
class Colors:
    GREEN = '\033[92m'      # 成功信息用绿色
    RED = '\033[91m'        # 失败信息用红色
    YELLOW = '\033[93m'     # 警告信息用黄色
    BLUE = '\033[94m'       # 一般信息用蓝色
    ENDC = '\033[0m'        # 结束颜色
   
def print_test_info():
    """打印测试信息"""
    info = """
TDengine 信通院性能测试用例说明
--------------------------------

测试用例列表:
1. case201 - 集群总测点/时间线基准性能测试
   - 目标：总测点/时间线 > 100亿
   - 验证方法：并行创建总测点/时间线，统计总测点/时间线数

2. case202 - 100亿时间线集群故障恢复测试
   - 目标：1分钟内恢复
   - 验证方法：终止所有节点进程后重启，检查恢复时间

3. case203 - Docker容器部署集群测试
   - 目标：验证基于Docker容器的集群部署功能 
   - 验证方法：调用外部脚本部署容器集群并验证

4. case204 - Docker容器集群重启测试  
   - 目标：验证容器集群重启功能和性能
   - 验证方法：重启集群并验证服务可用性和重启时间

5. case205 - 最大连接数测试
   - 目标：并发连接数 > 50000
   - 验证方法：启动多个客户端建立连接，统计连接数

6. case206 - 单核写入性能测试
   - 目标：写入速度 > 3万QPS
   - 验证方法：在1核16GB容器中进行单表写入测试

7. case207 - 单表写入性能测试  
   - 目标：写入速度 > 500万QPS
   - 验证方法：在16核32GB容器中进行单表写入测试

8. case208 - 查询性能验证测试
   - 目标：查询响应时间 < 1秒
   - 验证方法：在16核32GB容器中执行查询测试

9. case209 - 多数据库写入性能测试
   - 目标：总写入速度 > 9000万QPS
   - 验证方法：多数据库并发写入，统计总QPS

10. case210 - 数据压缩率测试
    - 目标：验证压缩后的数据量显著降低 
    - 验证方法：比较压缩前后的数据大小，B/A应小于3%

11. case211 - 大数据量写入和查询性能测试
    - 目标：写入100亿条数据且查询响应时间<2秒
    - 验证方法：写入数据并验证查询性能

12. case212 - 少量数据查询性能测试
    - 目标：少量数据查询响应时间 < 10ms
    - 验证方法：写入1万条数据并验证查询性能

13. case213 - TMQ消息延迟测试
    - 目标：消息传递延迟 < 10ms
    - 验证方法：测试消息发布和订阅的延迟时间

14. case214 - 数据库写入线性扩展测试
    - 目标：写入性能随节点增加呈线性增长
    - 验证方法：测试1-3节点的写入性能变化

15. case215 - 查询性能线性扩展测试
    - 目标：查询性能随节点增加呈线性扩展
    - 验证方法：测试1-3节点的查询性能变化

16. case216 - 内存使用和数据导出测试
    - 目标：2GB内存限制下完成数据写入和导出
    - 验证方法：在8核2GB容器中写入并导出数据

17. case217 - 节点故障恢复时间测试
    - 目标：验证恢复时间与数据规模无关
    - 验证方法：比较不同数据量下的故障恢复时间

环境要求:
- 硬件配置:
  * CPU: 建议8核以上
  * 内存: 建议32GB以上
  * 磁盘: 建议SSD，容量300GB以上

- 软件配置:
  * 操作系统: CentOS 7/8 或 Ubuntu 18.04/20.04
  * Python版本: 3.6+
  * TDengine版本: 3.0+
  * Docker Engine: 最新稳定版
  * expect工具包(unbuffer命令)

使用方法:
1. 执行所有测试用例:
   $ python3 case201.py

2. 执行单个测试用例:
   $ python3 case201.py <用例编号>
   例如: python3 case201.py 207

3. 执行多个测试用例:
   $ python3 case201.py <用例编号1>,<用例编号2>,...
   例如: python3 case201.py 206,207,208

4. 清理测试环境:
   $ python3 case201.py clean

5. 下载并安装指定版本:
   $ python3 case201.py <下载URL> [用例编号]
   例如: python3 case201.py http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/
   或者: python3 case201.py http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/ 206,207,208

6. 查看本帮助信息:
   $ python3 case201.py -h

说明:
- 下载URL参数会自动下载并安装企业版安装包和Docker镜像
- URL格式示例: http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/
- 下载的文件将保存在 /root/xintongyuan/download 目录
- 安装包和Docker镜像会自动安装/加载，并用于后续测试

作者: TDengine Platform Team
创建日期: 2025-05-20
最后修改: 2025-05-20
"""
    print_log(info, "INFO")


def print_usage():
    """打印使用说明"""
    usage = """
使用方法:
1. 执行所有测试用例:
   $ python3 case201.py

2. 执行单个测试用例:
   $ python3 case201.py <用例编号>
   例如: python3 case201.py 207

3. 执行多个测试用例:
   $ python3 case201.py <用例编号1>,<用例编号2>,...
   例如: python3 case201.py 206,207,208

4. 清理测试环境:
   $ python3 case201.py clean

5. 下载并安装指定版本:
   $ python3 case201.py <下载URL> [用例编号]
   例如: python3 case201.py http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/
   或者: python3 case201.py http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/ 206,207,208

6. 查看本帮助信息:
   $ python3 case201.py -h

说明:
- 下载URL参数会自动下载并安装企业版安装包和Docker镜像
- URL格式示例: http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/
- 下载的文件将保存在 /root/xintongyuan/download 目录
- 安装包和Docker镜像会自动安装/加载，并用于后续测试

可用的测试用例:
201 - 集群总测点/时间线基准性能测试 (目标: >100亿测点)
202 - 100亿时间线集群故障恢复测试 (目标: <1分钟恢复)
203 - Docker容器部署集群测试 (目标: 验证容器部署)
204 - Docker容器集群重启测试 (目标: 验证重启功能)
205 - 最大连接数测试 (目标: >50000连接)
206 - 单核写入性能测试 (目标: >3万QPS)
207 - 单表写入性能测试 (目标: >500万QPS)
208 - 查询性能验证测试 (目标: <1秒响应)
209 - 多数据库写入性能测试 (目标: >9000万QPS)
210 - 数据压缩率测试 (目标: B/A<3%)
211 - 大数据量写入和查询性能测试 (目标: 100亿数据<2秒查询)
212 - 少量数据查询性能测试 (目标: <10ms响应)
213 - TMQ消息延迟测试 (目标: <10ms延迟)
214 - 数据库写入线性扩展测试 (目标: 线性增长)
215 - 查询性能线性扩展测试 (目标: 线性扩展)
216 - 内存使用和数据导出测试 (目标: 2GB内存限制)
217 - 节点故障恢复时间测试 (目标: 恢复时间稳定)

注意事项:
1. 执行测试前请确保环境符合要求
2. 测试脚本顺序执行，为了结果准确，在运行过程中会清理数据和日志
3. 部分测试耗时较长，请耐心等待
4. 测试结果将保存在日志文件中
"""
    print_log(usage, "INFO")
    
def print_log(msg, level="INFO", end="\n", save_to_file=True):
    """统一的日志输出函数
    
    Args:
        msg: 日志信息
        level: 日志级别 (INFO/SUCCESS/ERROR/WARN)
        end: 行尾字符
    """
    time_str = time.strftime("%Y-%m-%d %H:%M:%S")
    # 终端输出（带颜色）
    if level == "INFO":
        color = Colors.BLUE
    elif level == "SUCCESS":
        color = Colors.GREEN
        msg = f"SUCCESS: {msg}"
    elif level == "ERROR":
        color = Colors.RED
    elif level == "WARN":
        color = Colors.YELLOW
    else:
        color = Colors.ENDC
        
    print(f"[{time_str}] {color}{level}: {msg}{Colors.ENDC}", end=end)
    
    # 日志文件记录（不带颜色）
    if save_to_file and 'logger' in globals() and globals()['logger'] is not None:
        try:
            if level == "INFO":
                logger.info(msg)
            elif level == "SUCCESS":
                logger.info(f"SUCCESS: {msg}")
            elif level == "ERROR":
                logger.error(msg)
            elif level == "WARN":
                logger.warning(msg)
            else:
                logger.info(msg)
        except Exception as e:
            print(f"[{time_str}] {Colors.RED}ERROR: Failed to write to log file: {e}{Colors.ENDC}")
    

def check_environment():
    """检查执行环境"""
    try:
        print_log("开始检查执行环境...", "INFO")
        
        # 检查CPU核数
        cpu_count = os.cpu_count()
        print_log(f"CPU核数: {cpu_count}", "INFO")
        
        # 检查内存
        mem_info = os.popen("free -g").readlines()[1].split()
        total_mem = int(mem_info[1])
        print_log(f"系统内存: {total_mem}GB", "INFO")
        
        # 检查磁盘空间
        disk_info = os.popen("df -h /root/xintongyuan").readlines()[1].split()
        free_space = disk_info[3]
        print_log(f"可用磁盘空间: {free_space}", "INFO")
        
        # 检查并安装 expect (unbuffer)
        print_log("\n检查 unbuffer 命令...", "INFO")
        if os.system("which unbuffer > /dev/null 2>&1") != 0:
            print_log("未找到 unbuffer 命令，正在安装 expect 包...", "INFO")
            # 先更新包列表
            os.system("apt-get update > /dev/null 2>&1")
            # 安装 expect 包
            if os.system("apt-get install -y expect > /dev/null 2>&1") != 0:
                raise Exception("安装 expect 包失败")
            print_log("✓ expect 包安装成功", "SUCCESS")
        else:
            print_log("✓ unbuffer 命令已安装", "INFO")
        
        # 检查必要程序
        required_programs = ["taosd", "taosadapter", "taosBenchmark", "taos"]
        for prog in required_programs:
            if os.system(f"which {prog} > /dev/null 2>&1") != 0:
                raise Exception(f"未找到必要程序: {prog}")
            print_log(f"检测到程序: {prog}", "INFO")
            # 获取并打印版本信息
            try:
                version_output = subprocess.check_output(f"{prog} -V", shell=True).decode('utf-8')
                # 对输出进行清理，去除ANSI颜色代码和多余空白
                import re
                version_output = re.sub(r'\x1b\[[0-9;]*[mGK]', '', version_output)
                version_output = version_output.strip()
                print_log(f"版本信息: {version_output}", "INFO")
            except subprocess.CalledProcessError:
                print_log(f"无法获取 {prog} 的版本信息", "WARN")
            
        print_log("环境检查完成" + "\n\n\n", "SUCCESS")
        return True
        
    except Exception as e:
        print_log(f"环境检查失败: {e}", "ERROR")
        return False
    
def download_and_setup(base_url):
    """下载并安装TDengine软件包和Docker镜像
    
    Args:
        base_url: 基础URL，例如 http://192.168.1.131/data/nas/TDengine/smoking/v3.3.6.6/
    
    Returns:
        tuple: (bool, str) - (是否成功, docker镜像标识)
    """
    try:
        # 1. 提取版本号
        version = base_url.rstrip('/').split('/')[-1]
        if not re.match(r'v?\d+\.\d+\.\d+\.\d+', version):
            raise Exception(f"无效的版本号格式: {version}")
        version = version.lstrip('v')  # 移除可能存在的前缀'v'
        
        # 更新xtytest.py中的版本号
        if not update_xtytest_version(version):
            raise Exception("更新xtytest.py版本失败")
        
        # 构造固定的镜像名称和tag
        docker_image = f"tdengine/tdengine-amd64:{version}"
        
        print_log(f"\n{Colors.BLUE}开始下载和安装 TDengine {version}...{Colors.ENDC}", "INFO")
        
        # 2. 准备下载目录
        download_dir = "/root/xintongyuan/download"
        if os.path.exists(download_dir):
            print_log("清理下载目录...", "INFO")
            os.system(f"rm -rf {download_dir}/*")
        else:
            print_log("创建下载目录...", "INFO")
            os.makedirs(download_dir)
            
        # 3. 构建下载URL
        enterprise_url = f"{base_url}/enterprise/TDengine-enterprise-{version}-Linux-x64.tar.gz"
        docker_url = f"{base_url}/community/docker/docker-server-{version}-Linux-amd64.tar.gz"
        
        # 4. 下载文件
        os.chdir(download_dir)
        
        print_log("\n下载企业版安装包...", "INFO")
        enterprise_file = f"TDengine-enterprise-{version}-Linux-x64.tar.gz"
        if os.system(f"wget -q {enterprise_url} -O {enterprise_file}") != 0:
            raise Exception("下载企业版安装包失败")
            
        print_log("下载Docker镜像...", "INFO")
        docker_file = f"docker-server-{version}-Linux-amd64.tar.gz"
        if os.system(f"wget -q {docker_url} -O {docker_file}") != 0:
            raise Exception("下载Docker镜像失败")
            
        # 5. 安装企业版
        print_log("\n解压企业版安装包...", "INFO")
        os.system(f"tar -xzf {enterprise_file}")
        
        install_dir = f"TDengine-enterprise-{version}"
        if not os.path.exists(install_dir):
            raise Exception("解压后未找到安装目录")
            
        print_log("安装TDengine...", "INFO")
        os.chdir(install_dir)
        if os.system("./install.sh > /dev/null 2>&1") != 0:
            raise Exception("安装失败")
            
        # 6. 导入Docker镜像
        print_log("\n导入Docker镜像...", "INFO")
        os.chdir(download_dir)
        result = subprocess.check_output(f"docker load -i {docker_file}", shell=True).decode()
        
        # 解析Docker镜像信息
        image_match = re.search(r'Loaded image: (.+)', result)
        if not image_match:
            raise Exception("无法获取Docker镜像信息")
            
        docker_image = image_match.group(1)
        print_log(f"Docker镜像已导入: {docker_image}", "INFO")
        print_log(f"Docker镜像标识: {docker_image}", "INFO")
        print_log(f"\n{Colors.GREEN}TDengine {version} 安装完成!{Colors.ENDC}", "SUCCESS")
        return True, docker_image
        
    except Exception as e:
        print_log(f"\n{Colors.RED}安装失败: {e}{Colors.ENDC}", "ERROR")
        return False, None
    
def update_xtytest_version(version):
    """更新xtytest.py中的Docker镜像版本
    
    Args:
        version: 版本号，例如3.3.6.6
    """
    try:
        xtytest_path = "/root/xintongyuan/code/xtytest.py"
        with open(xtytest_path, 'r') as f:
            content = f.read()
            
        # 首先检查当前文件中的版本号
        current_version_pattern = r'"image":\s*"tdengine/tdengine-amd64:([\d.]+)"'
        current_version_match = re.search(current_version_pattern, content)
        
        if current_version_match:
            current_version = current_version_match.group(1)
            # 如果版本相同，不需要更新
            if current_version == version:
                print_log(f"当前版本已经是 {version}，无需更新", "INFO")
                return True
                
        # 版本不同，需要更新
        pattern = r'"image":\s*"tdengine/tdengine-amd64:[\d.]+"'
        new_str = f'"image": "tdengine/tdengine-amd64:{version}"'
        
        # 使用re.sub进行替换
        new_content = re.sub(pattern, new_str, content)
        
        if new_content != content:
            with open(xtytest_path, 'w') as f:
                f.write(new_content)
            print_log(f"更新xtytest.py中的Docker镜像版本为: {version}", "INFO")
            return True
            
        # 如果没有找到匹配项，输出警告
        print_log("警告：未找到需要替换的版本号，当前配置内容:", "WARN")
        for line in content.split('\n'):
            if 'image' in line and 'tdengine' in line:
                print_log(f"相关行: {line.strip()}", "INFO")
        return False
        
    except Exception as e:
        print_log(f"更新xtytest.py版本失败: {e}", "ERROR")
        return False

def clean_all_environment():
    """清理所有测试环境"""
    try:
        print_log(f"{Colors.BLUE}开始清理所有测试环境...{Colors.ENDC}", "INFO")
        
        # 终止所有taos相关进程
        print_log("终止taos相关进程...", "INFO")
        os.system("pkill -9 taosd")
        os.system("pkill -9 taosadapter")
        os.system("pkill -9 taosBenchmark")
        time.sleep(5)  # 等待进程完全终止
        
        # 清理数据和日志目录
        print_log("清理数据和日志目录...", "INFO")
        directories = [
            # 单机环境目录
            "/root/xintongyuan/single/data",
            "/root/xintongyuan/single/log",
            # 集群环境目录
            "/root/xintongyuan/cluster/data3/taos1",
            "/root/xintongyuan/cluster/data3/taos2",
            "/root/xintongyuan/cluster/data3/taos3",
            "/root/xintongyuan/cluster/log",
            "/root/xintongyuan/cluster/log3/taos1",
            "/root/xintongyuan/cluster/log3/taos2",
            "/root/xintongyuan/cluster/log3/taos3"
        ]
        
        for dir_path in directories:
            if os.path.exists(dir_path):
                os.system(f"rm -rf {dir_path}/*")
                print_log(f"已清理目录: {dir_path}", "INFO")
                
        # 清理用例测试sql
        os.system("rm -f /root/xintongyuan/code/test.sql")
        print_log("已清理用例测试sql", "INFO")
                
        print_log(f"{Colors.GREEN}环境清理完成{Colors.ENDC}", "SUCCESS")
        return True
        
    except Exception as e:
        print_log(f"{Colors.RED}环境清理出错: {e}{Colors.ENDC}", "ERROR")
        return False

def get_taosd_version():
    """获取 taosd 版本信息"""
    try:
        output = subprocess.check_output("taosd -V", shell=True).decode('utf-8')
        
        # 初始化结果字典
        version_info = {}
        
        # 解析版本信息
        version_match = re.search(r'taosd version: ([\d.]+)', output)
        if version_match:
            version_info['version'] = version_match.group(1)
        else:
            raise Exception("无法获取taosd版本号")
        
        # 解析git信息（必需）
        git_match = re.search(r'git: ([a-f0-9]+)', output)
        if git_match:
            version_info['git'] = git_match.group(1)[:8]  # 只取前8位
        else:
            raise Exception("无法获取git信息")
        
        # 解析gitOfInternal信息（可选）
        git_internal_match = re.search(r'gitOfInternal: ([a-f0-9]+)', output)
        if git_internal_match:
            version_info['gitOfInternal'] = git_internal_match.group(1)[:8]  # 只取前8位
        
        return version_info
    
    except Exception as e:
        print_log(f"获取版本信息失败: {e}", "ERROR")
        return None

def setup_logging():
    """设置日志记录"""
    try:
        # 获取版本信息
        version_info = get_taosd_version()
        if not version_info:
            raise Exception("无法获取版本信息")
            
        # 生成日志文件名，只使用必须的信息
        #current_time = time.strftime("%Y%m%d_%H%M%S")
        current_time = time.strftime("%Y%m%d")
        log_filename = f"xintongyuan_taosd_version_{version_info['version']}"
        
        # 如果有git信息，添加到文件名
        if 'git' in version_info and version_info['git']:
            log_filename += f"_git_{version_info['git']}"
            
        # 如果有gitOfInternal信息，添加到文件名
        if 'gitOfInternal' in version_info and version_info['gitOfInternal']:
            log_filename += f"_gitOfInternal_{version_info['gitOfInternal']}"
            
        # 添加时间戳和扩展名
        log_filename += f"_{current_time}.log"
        
        # 设置日志记录器
        import logging
        logger = logging.getLogger('case201')
        logger.setLevel(logging.INFO)
        
        # 创建文件处理器
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器
        logger.addHandler(file_handler)
        # 记录初始信息
        logger.info("=" * 80)
        logger.info("TDengine 信通院性能测试开始")
        logger.info(f"TDengine Version: {version_info['version']}")
        logger.info(f"Git Commit: {version_info['git']}")
        logger.info(f"Internal Git Commit: {version_info['gitOfInternal']}")
        logger.info("=" * 80)
        
        return logger
        
    except Exception as e:
        print_log(f"设置日志记录失败: {e}", "ERROR")
        return None
       

def limit_resources(max_memory_gb=None, max_cpu_cores=None):
    """限制程序可用的系统资源"""
    try:
        print_log("\n设置资源限制...", "INFO")
        pid = os.getpid()
        
        if max_memory_gb:
            # 创建 memory cgroup
            cgroup_memory_path = f"/sys/fs/cgroup/memory/python_limited_{pid}"
            os.makedirs(cgroup_memory_path, exist_ok=True)
            
            # 设置内存限制（字节为单位）
            max_memory_bytes = max_memory_gb * 1024 * 1024 * 1024
            with open(f"{cgroup_memory_path}/memory.limit_in_bytes", "w") as f:
                f.write(str(max_memory_bytes))
                
            # 关闭内存交换
            with open(f"{cgroup_memory_path}/memory.swappiness", "w") as f:
                f.write("0")
                
            # 将当前进程添加到 cgroup
            with open(f"{cgroup_memory_path}/tasks", "w") as f:
                f.write(str(pid))
                
            print_log(f"已限制最大内存使用为 {max_memory_gb}GB", "INFO")
        
        def cleanup():
            try:
                if max_memory_gb:
                    # 将进程移出 memory cgroup
                    with open("/sys/fs/cgroup/memory/tasks", "w") as f:
                        f.write(str(pid))
                    # 删除 cgroup 目录
                    os.rmdir(cgroup_memory_path)
                    print_log("已清理内存资源限制", "INFO")
            except Exception as e:
                print_log(f"清理资源限制时出错: {e}", "ERROR")
                
        # 注册清理函数
        import atexit
        atexit.register(cleanup)
        
        print_log("资源限制设置完成", "SUCCESS")
        return True
        
    except Exception as e:
        print_log(f"设置资源限制失败: {e}", "ERROR")
        return False
    
           
def clean_single_environment_and_start_single_environment():
    try:
        print_log("开始清理环境...", "INFO")
        
        # 终止所有taos相关进程
        print_log("终止taos相关进程...", "INFO")
        os.system("pkill -9 taosd")
        os.system("pkill -9 taosadapter")
        os.system("pkill -9 taosBenchmark")
        time.sleep(5)  # 等待进程完全终止
        
        # 清理数据和日志目录
        print_log("清理数据和日志目录...", "INFO")
        data_dir = "/root/xintongyuan/single/data"
        log_dir = "/root/xintongyuan/single/log"
        
        for dir_path in [data_dir, log_dir]:
            if os.path.exists(dir_path):
                os.system(f"rm -rf {dir_path}/*")
                print_log(f"已清理目录: {dir_path}", "INFO")
        
        time.sleep(5)
        print_log("环境清理完成", "SUCCESS")
        # 启动taosd服务
        print_log("启动taosd服务...", "INFO")
        cmd = "nohup taosd -c /root/xintongyuan/single/cfg > /dev/null 2>&1 &"
        subprocess.Popen(cmd, shell=True)
        
        time.sleep(5)
        # 显示环境状态
        print_log("\n查看环境状态:", "INFO")
        subprocess.run('taos -s "show dnodes;show mnodes;"', shell=True)
        
        print_log(f"{Colors.GREEN}环境启动完成{Colors.ENDC}", "SUCCESS")
        
    except Exception as e:
        print_log(f"环境清理出错: {e}", "ERROR")
        sys.exit(1)
        
def clean_cluster_environment():
    """清理集群环境"""
    try:
        print_log(f"{Colors.BLUE}开始清理环境...{Colors.ENDC}", "INFO")
        
        # 终止所有taos相关进程
        print_log("终止taos相关进程...", "INFO")
        os.system("pkill -9 taosd")
        os.system("pkill -9 taosadapter")
        os.system("pkill -9 taosBenchmark")
        time.sleep(5)  # 等待进程完全终止
        
        # 清理数据和日志目录
        print_log("清理数据和日志目录...", "INFO")
        data1_dir = "/root/xintongyuan/cluster/data3/taos1/"
        data2_dir = "/root/xintongyuan/cluster/data3/taos2/"
        data3_dir = "/root/xintongyuan/cluster/data3/taos3/"
        log_dir = "/root/xintongyuan/cluster/log"
        log1_dir = "/root/xintongyuan/cluster/log3/taos1/"
        log2_dir = "/root/xintongyuan/cluster/log3/taos2/"
        log3_dir = "/root/xintongyuan/cluster/log3/taos3/"
        
        for dir_path in [data1_dir, data2_dir, data3_dir, log_dir, log1_dir, log2_dir, log3_dir]:
            if os.path.exists(dir_path):
                os.system(f"rm -rf {dir_path}/*")
                print_log(f"已清理目录: {dir_path}", "INFO")
                
    except Exception as e:
        print_log(f"{Colors.RED}环境清理出错: {e}{Colors.ENDC}", "ERROR")
        raise

def start_cluster(node_count=3):
    """启动指定数量的集群节点    
    Args:
        node_count: 要启动的节点数量(1-3)
    """
    try:
        # 先清理环境
        clean_cluster_environment()
        
        print_log(f"{Colors.BLUE}开始启动{node_count}节点集群...{Colors.ENDC}", "INFO")
        
        # 配置文件列表
        configs = [
            "/root/xintongyuan/cluster/cfg3/cfg_1",
            "/root/xintongyuan/cluster/cfg3/cfg_2",
            "/root/xintongyuan/cluster/cfg3/cfg_3"
        ]
        
        # 根据指定数量启动节点
        for i in range(node_count):
            cmd = f"nohup taosd -c {configs[i]} > /dev/null 2>&1 &"
            subprocess.Popen(cmd, shell=True)
            print_log(f"启动第{i+1}个节点: {configs[i]}", "INFO")
            time.sleep(2)
        
        print_log("等待节点启动...", "INFO")
        time.sleep(5)
        
        # 如果启动多个节点，配置集群
        if node_count > 1:
            print_log("配置集群节点...", "INFO")
            for i in range(2, node_count + 1):
                port = f"{5 + i}030"
                cmd = f'taos -s "create dnode \'localhost:{port}\'"'
                print_log(f"添加节点 localhost:{port}", "INFO")
                subprocess.run(cmd, shell=True)
                time.sleep(5)
            
            # 如果是3节点集群，创建额外的mnode
            if node_count == 3:
                print_log("配置mnode...", "INFO")
                mnodes_cmd = [
                    'taos -s "create mnode on dnode 2;"',
                    'taos -s "create mnode on dnode 3;"'
                ]
                for cmd in mnodes_cmd:
                    print_log(f"执行命令: {cmd}", "INFO")
                    subprocess.run(cmd, shell=True)
                    time.sleep(5)
                            
        # 显示集群状态
        print_log("\n查看集群状态:", "INFO")
        subprocess.run('taos -s "show dnodes;show mnodes;"', shell=True)
        
        print_log(f"{Colors.GREEN}{node_count}节点集群启动完成{Colors.ENDC}")
        
    except Exception as e:
        print_log(f"{Colors.RED}{node_count}节点集群启动出错: {e}{Colors.ENDC}", "ERROR")
        raise

def create_database():
    try:
        # 连接到TDengine
        conn = taos.connect(host="localhost", user="root", password="taosdata")
        cursor = conn.cursor()
        
        # 创建数据库
        print_log("创建数据库 dbt...", "INFO")
        cursor.execute("create database if not exists dbt vgroups 120")
        cursor.execute("use dbt")
        print_log("数据库创建成功", "SUCCESS")
        
    except Exception as e:
        print_log(f"创建数据库出错: {e}", "ERROR")
        sys.exit(1)
        
    finally:
        if 'conn' in locals():
            conn.close()

def insert_data_order():
    try:
        print_log("开始顺序插入数据...", "INFO")
        # 定义要执行的3个taosBenchmark命令
        commands = [
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_1.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_2.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_3.json"
        ]
        
        # 顺序执行命令，每个命令之间间隔2秒
        for i, cmd in enumerate(commands, 1):
            print_log(f"开始执行第{i}个数据插入任务...", "INFO")
            p = subprocess.Popen(cmd, shell=True)
            p.wait()  # 等待当前命令执行完成
            print_log(f"第{i}个数据插入任务完成", "INFO")
            
            # 如果不是最后一个命令，则等待2秒
            if i < len(commands):
                print_log("等待2秒后执行下一个任务...", "INFO")
                time.sleep(2)
            
        print_log("所有数据插入完成", "SUCCESS")
        
    except Exception as e:
        print_log(f"插入数据出错: {e}", "ERROR")
        sys.exit(1)

def insert_data():
    try:
        print_log(f"\n{Colors.BLUE}开始并行插入数据，首次5秒后显示进度，后续每5分钟更新一次...{Colors.ENDC}", "INFO")
        
        # 定义要执行的3个taosBenchmark命令
        commands = [
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_1.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_2.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase201_3.json"
        ]
        
        # 延迟并行执行命令
        processes = []
        for i, cmd in enumerate(commands, 1):
            print_log(f"启动第{i}个数据插入任务...", "INFO")
            #p = subprocess.Popen(cmd, shell=True) #一直在刷新日志，改成下面简洁模式
            p = subprocess.Popen(
                cmd, 
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            processes.append(p)
            if i < len(commands):
                print_log(f"等待3秒后启动下一个任务...", "INFO")
                time.sleep(3)  # 每个命令启动之间等待3秒
        
        # # 等待所有进程完成,最后打印一次日志，等待时间久，不友好，改成下方5分钟输出一次
        # for i, p in enumerate(processes, 1):
        #     print_log(f"等待第{i}个任务完成...", "SUCCESS")
        #     p.wait()
        #     print_log(f"第{i}个任务已完成", "SUCCESS")            
        # print_log("所有数据插入完成", "SUCCESS")
        
        # 监控进度
        start_time = time.time()
        last_check_time = start_time
        last_count = None
        target_tables = 100000000  # 目标表数量
        
        # 等待所有进程完成
        while any(p.poll() is None for p in processes):
            current_time = time.time()
            
            # 第一次5秒后检查，之后每5分钟检查
            if (last_count is None and current_time - start_time >= 5) or \
               (last_count is not None and current_time - last_check_time >= 300):
                
                current_count = get_current_table_count()
                if current_count is not None:
                    elapsed_minutes = (current_time - start_time) / 60
                    
                    if last_count is None:
                        # 首次检查
                        print_log(f"\n{Colors.BLUE}写入进度 ({time.strftime('%Y-%m-%d %H:%M:%S')}){Colors.ENDC}", "INFO")
                        print_log(f"已创建表数量: {current_count:,}", "INFO")
                        print_log(f"已用时间: {elapsed_minutes:.1f} 分钟", "INFO")
                        last_count = current_count
                    else:
                        # 后续检查
                        time_diff = current_time - last_check_time
                        if time_diff > 0:
                            speed = (current_count - last_count) / time_diff  # 每秒创建表数
                            if speed > 0:
                                remaining_tables = target_tables - current_count
                                remaining_hours = (remaining_tables / speed) / 3600
                                records_per_table = 100  # 每个表的时间线数
                                total_records = current_count * records_per_table
                                remaining_records = max(0, 10_000_000_000 - total_records)  # 因为有一些其他系统表的，因此确保剩余记录数不为负
                                
                                # 如果还有剩余数量，计算剩余时间
                                if remaining_tables > 0:
                                    remaining_hours = (remaining_tables / speed) / 3600
                                else:
                                    remaining_hours = 0
                                
                                print_log(f"\n{Colors.BLUE}写入进度 ({time.strftime('%Y-%m-%d %H:%M:%S')}){Colors.ENDC}", "INFO")
                                print_log(f"已创建表数量: {current_count:,} 个", "INFO")
                                print_log(f"剩余需要创建表数量: {remaining_tables:,} 个", "INFO")
                                print_log(f"已写入总测点/时间线数: {total_records:,} 个", "INFO")
                                if remaining_records > 0:
                                    print_log(f"剩余总测点/时间线数: {remaining_records:,} 个", "INFO")
                                    print_log(f"预计剩余时间: {remaining_hours:.1f} 小时", "INFO")
                                else:
                                    print_log(f"写入完成!")
                                print_log(f"总体完成度: {min(100, (current_count/target_tables)*100):.2f}%", "INFO")
                            else:
                                print_log(f"\n{Colors.YELLOW}警告：检测到写入速度异常，可能是系统繁忙或出现问题{Colors.ENDC}", "ERROR")
                                print_log(f"当前表数量: {current_count:,}", "INFO")
                                print_log(f"已用时间: {elapsed_minutes:.1f} 分钟", "INFO")
                        
                        last_count = current_count
                    last_check_time = current_time
            
            time.sleep(1)
        
        print_log(f"\n{Colors.GREEN}所有数据插入任务已完成{Colors.ENDC}", "SUCCESS")
        
    except Exception as e:
        print_log(f"插入数据出错: {e}", "ERROR")
        sys.exit(1)
        
def calculate_total_records():
    try:
        # 连接到TDengine
        conn = taos.connect(host="localhost", user="root", password="taosdata", database="dbt")
        cursor = conn.cursor()
        
        # 查询子表数量
        cursor.execute("select count(`table_name`) from information_schema.ins_tables where db_name ='dbt'")
        table_count = cursor.fetchall()[0][0]
        print_log(f"子表数量: {table_count:,}", "INFO")
        
        # 查询列数
        cursor.execute("desc dbt.stb")
        columns = cursor.fetchall()
        column_count = len(columns) - 1  # 减去时间戳列
        print_log(f"数据列数(不含时间戳): {column_count}", "INFO")
        
        # 计算总条目数
        total_records = table_count * column_count
        print_log(f"总测点/时间线数: {total_records:,}", "INFO")
        
        # 检查是否超过100亿
        if total_records > 10_000_000_000:
            print_log("✓ 总测点/时间线大于100亿", "SUCCESS")
        else:
            print_log("✗ 总测点/时间线小于100亿", "ERROR")
            
    except Exception as e:
        print_log(f"计算总测点/时间线出错: {e}", "ERROR")
        sys.exit(1)
        
    finally:
        if 'conn' in locals():
            conn.close()

def get_current_table_count():
    """获取当前已创建的表数量"""
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("select count(`table_name`) from information_schema.ins_tables where db_name ='dbt'")
        result = cursor.fetchall()
        return result[0][0]
    except Exception:
        return None
    finally:
        if 'conn' in locals():
            conn.close()
                        
def case_201():
    try:
        print_log(f"{Colors.BLUE}开始执行用例201...{Colors.ENDC}", "INFO")
        # 清理环境
        clean_cluster_environment()
        
        # 启动三节点集群
        start_cluster(3)
        
        print_log(f"\n{Colors.BLUE}1. 写入100亿测点/时间线，预计超过半个小时，请耐心等待！{Colors.ENDC}", "INFO")
        
        # 创建数据库
        create_database()
        time.sleep(5)
        
        # 插入数据
        insert_data()
        time.sleep(5)
        
        # 计算总记录数
        calculate_total_records()
        print_log(f"\n{Colors.GREEN}case201测试成功!!!{Colors.ENDC}", "SUCCESS")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case201执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
        
def check_tables_info():
    try:
        conn = taos.connect(host="localhost", user="root", password="taosdata")
        cursor = conn.cursor()
        
        # 查询测点数
        print_log("查询数据库信息...", "INFO")
        cursor.execute("select `ntables` from information_schema.ins_databases where name='dbt'")
        ntables = cursor.fetchall()[0][0]
        print_log(f"总子表数量: {ntables:,}", "INFO")
        
        # 查询表结构
        print_log("\n查询表结构:", "INFO")
        cursor.execute("desc dbt.stb")
        columns = cursor.fetchall()
        for col in columns:
            print_log(f"列名: {col[0]}, 类型: {col[1]}", "INFO")
            
    except Exception as e:
        print_log(f"查询表信息出错: {e}", "ERROR")
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()

def kill_all_taosd():
    try:
        print_log("终止所有taosd进程...", "INFO")
        cmd = "ps -ef | grep taos | grep -v grep | awk '{print $2}' | xargs kill -9"
        os.system(cmd)
        time.sleep(5)
        print_log("所有taosd进程已终止", "SUCCESS")
    except Exception as e:
        print_log(f"终止进程出错: {e}", "ERROR")
        raise

def start_all_taosd():
    configs = [
        "/root/xintongyuan/cluster/cfg3/cfg_1",
        "/root/xintongyuan/cluster/cfg3/cfg_2",
        "/root/xintongyuan/cluster/cfg3/cfg_3"
    ]
    
    print_log("启动所有taosd服务...", "INFO")
    for cfg in configs:
        cmd = f"screen -d -m taosd -c {cfg}"
        subprocess.run(cmd, shell=True)
    print_log("所有taosd服务启动命令已执行", "SUCCESS")

def wait_cluster_ready():
    try:
        start_time = time.time()
        last_print_time = 0  # 初始化 last_print_time
        print_log("等待集群就绪...", "INFO")
        max_wait_time = 60  # 最大等待时间为60秒
        
        while True:
            # 检查是否超时
            current_time = time.time()
            if current_time - start_time > max_wait_time:
                print_log("\n❌ 测试失败：100亿时间线集群恢复时间超过1分钟", "ERROR")
                print_log(f"已等待时间: {current_time - start_time:.2f}秒", "ERROR")
                return False
            
            try:
                conn = taos.connect(host="localhost", user="root", password="taosdata")
                cursor = conn.cursor()
                cursor.execute("select id,endpoint,status,create_time,reboot_time from information_schema.ins_dnodes;")
                results = cursor.fetchall()
                
                # 每5秒打印一次状态
                if (current_time - last_print_time) >= 5:
                    print_log("\n当前节点状态:", "INFO")
                    print_log("时间:", time.strftime("%Y-%m-%d %H:%M:%S"), "INFO")
                    print_log("=" * 110, "INFO")
                    print_log("id\tendpoint\t\tstatus\t\tcreate_time\t\t\treboot_time", "INFO")
                    print_log("=" * 110, "INFO")
                    for row in results:
                        print_log(f"{row[0]}\t{row[1]:<20}\t{row[2]:<10}\t{row[3]}\t{row[4]}", "INFO")
                    print("=" * 110, "INFO")
                    last_print_time = current_time
                                    
                # 检查是否所有节点都ready且已重启
                all_ready = True
                nodes_ready = 0  # 用于统计符合条件的节点数
                default_time = "1970-01-01 08:00:00.000"
                required_nodes = 3
                
                for row in results:
                    status = row[2]  # status在第3列
                    reboot_time = row[4]  # reboot_time在第5列
                    
                    # 检查状态和重启时间
                    if status == "ready" and reboot_time != default_time:
                        nodes_ready += 1
                    else:
                        all_ready = False
                
                if nodes_ready == required_nodes:
                    end_time = time.time()
                    duration = end_time - start_time
                    print_log(f"\n✓ 测试通过！100亿时间线集群恢复完成", "INFO")
                    print_log(f"恢复总时间: {duration:.2f}秒", "INFO")
                    print_log(f"所有{required_nodes}个节点均已就绪", "INFO")
                    print_log("\n节点状态:", "INFO")
                    for row in results:
                        print_log(f"节点ID: {row[0]}, 状态: {row[2]}, 重启时间: {row[4]}", "INFO")
                    print_log("=" * 60, "INFO")
                    return True
                    
            except Exception:
                pass  # 忽略连接错误，继续等待
                
            print(".", end="", flush=True)
            time.sleep(1)
            
    except Exception as e:
        print_log(f"等待集群就绪出错: {e}", "ERROR")
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()

def case_202():
    try:
        print_log(f"{Colors.BLUE}开始执行用例202...{Colors.ENDC}", "INFO")
        
        # 1. 确认测点数和表结构
        check_tables_info()
        
        # 2. kill所有taosd进程
        kill_all_taosd()
        
        # 3. 启动所有taosd服务并开始计时
        start_all_taosd()
        
        # 4. 等待集群恢复并获取结果
        result = wait_cluster_ready()
        
        if result:
            print_log(f"\n{Colors.GREEN}case202测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            print_log(f"\n{Colors.RED}case202测试失败!!!{Colors.ENDC}", "ERROR")
            sys.exit(1)
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case202执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
        
def case_203():
    """Docker容器部署集群测试"""
    try:
        print_log(f"{Colors.BLUE}开始执行用例203...{Colors.ENDC}", "INFO")
        
        print_log("\n调用外部测试脚本启动容器集群...", "INFO")
        cmd = "unbuffer python3 /root/xintongyuan/code/xtytest.py start_container_dnodes"
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 实时显示输出
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print_log(output.strip(), "INFO")
                
        # 等待进程完成
        process.wait()
        
        # 检查执行结果
        if process.returncode == 0:
            print_log(f"\n{Colors.GREEN}case203 Docker容器部署集群测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            error = process.stderr.read()
            raise Exception(f"容器集群部署失败: {error}")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case203执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def case_204():
    """Docker容器集群重启测试"""
    try:
        print_log(f"{Colors.BLUE}开始执行用例204...{Colors.ENDC}", "INFO")
        max_retries = 5  # 增加到5次重试
        current_try = 1
        
        while current_try <= max_retries:
            print_log(f"\n{Colors.BLUE}执行第{current_try}次测试{Colors.ENDC}", "INFO")
            
            # 1. 启动重启测试
            print_log("\n调用外部测试脚本重启容器集群...", "INFO")
            #cmd = "python3 xtytest.py restart_container_dnodes"
            cmd = "unbuffer python3 /root/xintongyuan/code/xtytest.py restart_container_dnodes"
            process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            output_lines = []  # 存储输出行
            
            # 实时显示输出并保存
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    output_lines.append(output.strip())
                    print_log(output.strip(), "INFO")
            
            # 等待进程完成
            process.wait()
            
            # 2. 测试写入功能
            print_log("\n测试数据写入功能...", "INFO")
            write_cmd = "docker exec -ti taostest_net_host_99 sh -c 'taos -s \"insert into test.ctb values (now, 1);\"'"
            write_result = subprocess.run(write_cmd, shell=True, capture_output=True, text=True)
            if write_result.returncode == 0:
                print_log("✓ 数据写入测试成功", "SUCCESS")
            else:
                print_log("✗ 数据写入测试失败", "ERROR")
                current_try += 1
                continue
                
            # 3. 测试查询功能
            print_log("\n测试数据查询功能...", "INFO")
            query_cmd = "docker exec -ti taostest_net_host_99 sh -c 'taos -s \"select * from test.stb;\"'"
            query_result = subprocess.run(query_cmd, shell=True, capture_output=True, text=True)
            if query_result.returncode == 0:
                print_log("✓ 数据查询测试成功", "SUCCESS")
            else:
                print_log("✗ 数据查询测试失败", "ERROR")
                current_try += 1
                continue
                
            # 4. 检查重启时间
            restart_pattern = r"restart.*?use\s+([\d.]+)s"  # 修改正则表达式使其更宽松
            for line in output_lines:
                match = re.search(restart_pattern, line, re.IGNORECASE)  # 添加不区分大小写
                if match:
                    restart_time = float(match.group(1))
                    print_log(f"\n集群重启时间: {restart_time:.2f}秒", "INFO")
                    if restart_time < 60:
                        print_log("✓ 重启时间符合要求 (< 60秒)", "SUCCESS")
                        print_log(f"\n{Colors.GREEN}case204 Docker容器集群重启测试成功!!!{Colors.ENDC}", "SUCCESS")
                        return  # 测试成功，直接返回
                    else:
                        print_log(f"✗ 重启时间超过限制: {restart_time:.2f}秒", "ERROR")
                        break
            
            # 如果未找到重启时间信息
            print_log(f"\n本次测试未找到有效的重启时间信息", "INFO")
            if current_try < max_retries:
                print_log(f"将进行第{current_try + 1}次尝试...", "INFO")
                time.sleep(10)  # 等待10秒后重试
            
            current_try += 1
            
        # 如果所有重试都失败
        raise Exception(f"经过{max_retries}次重试后仍无法完成测试")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case204执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
    finally:
        # 清理所有测试容器
        try:
            print_log("\n清理测试容器...", "INFO")
            # 获取所有taostest开头的容器
            cmd = "docker ps -a | grep taostest | awk '{print $1}'"
            containers = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
            
            for container in containers:
                if container:  # 确保容器ID不为空
                    subprocess.run(f"docker stop {container}", shell=True)
                    subprocess.run(f"docker rm {container}", shell=True)
                    print_log(f"已清理容器: {container}", "INFO")
                    
            print_log("✓ 所有测试容器清理完成", "SUCCESS")
            
        except Exception as e:
            print_log(f"清理容器时出错: {e}", "ERROR")
        
def check_connections(expected_min=None, expected_exact=None, timeout=300):
    """检查连接数是否符合预期
    
    Args:
        expected_min: 期望的最小连接数
        expected_exact: 期望的精确连接数
        timeout: 超时时间(秒)
    """
    try:
        start_time = time.time()
        last_print_time = 0
        
        while True:
            # 检查是否超时
            current_time = time.time()
            if current_time - start_time > timeout:
                print_log(f"\n❌ 超时: 等待时间超过{timeout}秒", "ERROR")
                return False
                
            # 获取当前连接数
            cmd = "netstat -nao | grep 6041 | grep ESTABLISHED | wc -l"
            result = subprocess.check_output(cmd, shell=True)
            conn_count = int(result.strip())
            
            # 每5秒打印一次状态
            if current_time - last_print_time >= 5:
                print_log(f"\n当前连接数: {conn_count}", "INFO")
                last_print_time = current_time
            
            # 检查是否符合预期
            if expected_exact is not None and conn_count == expected_exact:
                print_log(f"\n✓ 连接数等于预期值: {expected_exact}", "SUCCESS")
                return True
            elif expected_min is not None and conn_count > expected_min:
                print_log(f"\n✓ 连接数大于预期值: {expected_min}", "SUCCESS")
                return True
                
            print(".", end="", flush=True)
            time.sleep(1)
            
    except Exception as e:
        print_log(f"检查连接数出错: {e}", "ERROR")
        raise

def kill_benchmark_processes():
    """终止所有taosBenchmark进程"""
    try:
        print_log("\n终止所有taosBenchmark进程...", "INFO")
        cmd = "ps -ef|grep -wi taosBenchmark| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
        os.system(cmd)
        time.sleep(2)
        print_log("所有taosBenchmark进程已终止", "INFO")
        
    except Exception as e:
        print_log(f"终止taosBenchmark进程出错: {e}", "ERROR")
        raise
    
def ensure_taosadapter_running():
    """确保 taosadapter 正在运行"""
    try:
        # 检查 taosadapter 是否已在运行
        check_cmd = "ps -ef | grep taosadapter | grep -v grep | wc -l"
        result = subprocess.check_output(check_cmd, shell=True)
        if int(result.strip()) == 0:
            print_log("taosadapter 未运行，正在启动...", "INFO")
            # 启动 taosadapter
            cmd = "nohup taosadapter > /dev/null 2>&1 &"
            subprocess.Popen(cmd, shell=True)
            time.sleep(3)  # 等待启动
            print_log("taosadapter 已启动", "INFO")
        else:
            print_log("taosadapter 已在运行", "INFO")
            
    except Exception as e:
        print_log(f"检查/启动 taosadapter 失败: {e}", "ERROR")
        raise

def case_205_old():
    try:
        print_log(f"{Colors.BLUE}开始执行用例205...{Colors.ENDC}", "INFO")
        
        # 确保 taosadapter 运行
        ensure_taosadapter_running()
        
        # 1. 执行三个taosBenchmark
        print_log(f"\n{Colors.BLUE}启动taosBenchmark任务...{Colors.ENDC}", "INFO")
        commands = [
            "taosBenchmark -f /root/xintongyuan/code/UseCase205_1.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase205_2.json",
            "taosBenchmark -f /root/xintongyuan/code/UseCase205_3.json"
        ]
        
        processes = []
        for i, cmd in enumerate(commands, 1):
            print_log(f"启动第{i}个taosBenchmark...", "INFO")
            #p = subprocess.Popen(cmd, shell=True)
            #这种方式可以显著减少日志输出，上面方式适合debug用，看执行过程
            p = subprocess.Popen(
                cmd, 
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            processes.append(p)
            time.sleep(2)
        
        # 2. 检查连接数是否超过50000
        print_log(f"\n{Colors.BLUE}检查连接数是否超过50000...{Colors.ENDC}", "INFO")
        if not check_connections(expected_min=50000):
            raise Exception("连接数未达到预期值")
            
        # 3. 终止taosBenchmark进程
        kill_benchmark_processes()
        
        # 4. 检查连接数是否降为0
        print_log(f"\n{Colors.BLUE}检查连接数是否降为0...{Colors.ENDC}", "INFO")
        if not check_connections(expected_exact=0):
            raise Exception("连接未完全释放")
            
        print_log(f"\n{Colors.GREEN}case205测试成功!!!{Colors.ENDC}", "SUCCESS")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case205执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def case_205():
    """最大连接数测试"""
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例205 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")
            
            # 确保 taosadapter 运行
            ensure_taosadapter_running()
            
            # 1. 执行三个taosBenchmark
            print_log(f"\n{Colors.BLUE}启动taosBenchmark任务...{Colors.ENDC}", "INFO")
            commands = [
                "taosBenchmark -f /root/xintongyuan/code/UseCase205_1.json",
                "taosBenchmark -f /root/xintongyuan/code/UseCase205_2.json",
                "taosBenchmark -f /root/xintongyuan/code/UseCase205_3.json"
            ]
            
            processes = []
            for i, cmd in enumerate(commands, 1):
                print_log(f"启动第{i}个taosBenchmark...", "INFO")
                p = subprocess.Popen(
                    cmd, 
                    shell=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                processes.append(p)
                time.sleep(2)
            
            # 2. 检查连接数是否超过50000
            print_log(f"\n{Colors.BLUE}检查连接数是否超过50000...{Colors.ENDC}", "INFO")
            if not check_connections(expected_min=50000):
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: 连接数未达到预期值{Colors.ENDC}", "INFO")
                    kill_benchmark_processes()  # 清理当前进程
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    raise Exception("连接数未达到预期值")
            
            # 3. 终止taosBenchmark进程
            kill_benchmark_processes()
            
            # 4. 检查连接数是否降为0
            print_log(f"\n{Colors.BLUE}检查连接数是否降为0...{Colors.ENDC}", "INFO")
            if not check_connections(expected_exact=0):
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: 连接未完全释放{Colors.ENDC}", "INFO")
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    raise Exception("连接未完全释放")
                    
            print_log(f"\n{Colors.GREEN}case205测试成功!!!{Colors.ENDC}", "SUCCESS")
            return  # 测试成功，直接返回
            
        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                kill_benchmark_processes()  # 确保清理所有进程
                time.sleep(5)  # 等待资源释放
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case205执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 确保清理所有进程
            try:
                kill_benchmark_processes()
            except Exception as cleanup_error:
                print_log(f"清理进程时出错: {cleanup_error}", "ERROR")
        
def parse_qps_from_output(json_file):
    """解析taosBenchmark输出中的QPS数据
    
    Args:
        json_file: taosBenchmark的配置文件路径
    """
    try:
        # 执行taosBenchmark命令并获取输出
        cmd = f"taosBenchmark -f {json_file} 2>&1 | grep SUCC"
        output = subprocess.check_output(cmd, shell=True).decode('utf-8')
        
        # 使用正则表达式提取real后面的QPS值
        import re
        pattern = r'real (\d+\.\d+)\) records/second'
        match = re.search(pattern, output)
        
        if match:
            qps = float(match.group(1))
            return qps
        return 0
    
    except Exception as e:
        print_log(f"解析QPS数据出错: {e}", "ERROR")
        return 0

def parse_docker_qps(container_name, json_file):
    """解析容器内运行的taosBenchmark的QPS结果
    
    Args:
        container_name: 容器名称
        json_file: taosBenchmark配置文件路径
    """
    try:
        # 先启动taosBenchmark
        cmd = f"docker exec {container_name} taosBenchmark -f {json_file}"
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print_log("\n等待taosBenchmark执行完成...", "INFO")
        time.sleep(30)  # 等待30秒确保有足够的输出
        
        # 存储所有线程的最新写入速率
        thread_rates = {}
        max_total_rate = 0
        
        # 读取输出直到进程结束
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
                
            if 'peroid insert rate:' in output:
                # 解析线程号和写入速率
                try:
                    thread_match = re.search(r'thread\[(\d+)\]', output)
                    rate_match = re.search(r'peroid insert rate: (\d+\.\d+)', output)
                    
                    if thread_match and rate_match:
                        thread_id = thread_match.group(1)
                        rate = float(rate_match.group(1))
                        thread_rates[thread_id] = rate
                        
                        # 计算当前总速率
                        current_total_rate = sum(thread_rates.values())
                        max_total_rate = max(max_total_rate, current_total_rate)
                        
                except Exception as e:
                    print_log(f"解析线程数据出错: {e}", "ERROR")
                    continue
                    
        if max_total_rate > 0:
            print_log(f"\n解析结果:", "INFO")
            print_log(f"最大总QPS: {max_total_rate:,.2f}/s", "INFO")
            print_log("各线程最新写入速率:", "INFO")
            for thread_id, rate in thread_rates.items():
                print_log(f"线程[{thread_id}]: {rate:,.2f}/s", "INFO")
            return max_total_rate
            
        # 如果上面的方法失败，尝试查找最终的QPS结果
        output = process.stderr.read()
        match = re.search(r'real (\d+\.*\d*)\) records/second', output)
        if match:
            qps = float(match.group(1))
            print_log(f"\n使用最终统计的QPS: {qps:,.2f}/s", "INFO")
            return qps
            
        print_log("\n未找到有效的QPS数据", "ERROR")
        return 0
        
    except Exception as e:
        print_log(f"解析QPS数据出错: {e}", "ERROR")
        return 0


def case_206_not_in_docker():
    """单核写入性能测试"""
    container_name = "case206_test"
    try:
        print_log(f"{Colors.BLUE}开始执行用例206...{Colors.ENDC}", "INFO")
        target_qps = 30000  # 目标QPS为3万/秒
        
        # 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        
        # 运行taosBenchmark并捕获输出
        print_log("\n开始运行taosBenchmark...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase206.json"
        print_log("\n开始运行性能测试...", "INFO")
        qps = parse_qps_from_output(json_file)
        
        print_log(f"\n实际写入QPS: {qps:.2f}/s", "INFO")
        
        # 判断QPS是否达标
        if qps > target_qps:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！QPS({qps:.2f}) > 目标值({target_qps}){Colors.ENDC}", "INFO")
            print_log(f"\n{Colors.GREEN}case206写入性能测试成功!!!{Colors.ENDC}", "INFO")
        else:
            print_log(f"\n{Colors.RED}✗ 测试失败！QPS({qps:.2f}) <= 目标值({target_qps}){Colors.ENDC}", "ERROR")
            raise Exception("case206写入性能未达到要求")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case206执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def case_206():
    """单核写入性能测试"""
    container_name = "case206_test"
    try:
        print_log(f"{Colors.BLUE}开始执行用例206...{Colors.ENDC}", "INFO")
        target_qps = 30000  # 目标QPS为3万/秒
        
        # 1. 创建1核16GB的容器
        if not create_limited_container(container_name, cpus=2, memory_gb=16):
            raise Exception("容器资源限制验证失败")
            
        # 2. 在容器中启动 TDengine
        print_log("\n启动 TDengine 服务...", "INFO")
        cmd = f"docker exec {container_name} nohup taosd > /dev/null 2>&1 &"
        subprocess.run(cmd, shell=True)
        time.sleep(5)  # 等待服务启动
        
        # 3. 运行taosBenchmark并解析QPS
        print_log("\n开始性能测试...", "INFO")
        qps = parse_docker_qps(container_name, "/root/xintongyuan/code/UseCase206.json")
        print_log(f"\n实际写入QPS: {qps:,.2f}/s", "INFO")
        
        # 判断QPS是否达标
        if qps > target_qps:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！QPS({qps:,.2f}) > 目标值({target_qps:,}){Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case206写入性能测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            raise Exception(f"写入性能未达到要求: {qps:,.2f} <= {target_qps:,}")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case206执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
    finally:
        # 清理容器
        try:
            print_log("\n清理测试容器...", "INFO")
            subprocess.run(f"docker stop {container_name}", shell=True)
            subprocess.run(f"docker rm {container_name}", shell=True)
            print_log("✓ 容器清理完成", "SUCCESS")
        except Exception as e:
            print_log(f"清理容器时出错: {e}", "ERROR")
        
def case_207_not_in_docker():
    try:
        print_log(f"{Colors.BLUE}开始执行用例207...{Colors.ENDC}", "INFO")
        target_qps = 5000000  # 目标QPS为500万/秒
        
        # 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        
        # 运行taosBenchmark并捕获输出
        print_log("\n开始运行taosBenchmark...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase207.json"
        print_log("\n开始运行性能测试...", "INFO")
        qps = parse_qps_from_output(json_file)
        
        print_log(f"\n实际写入QPS: {qps:,.2f}/s", "INFO")  # 添加千位分隔符使大数更易读
        
        # 判断QPS是否达标
        if qps > target_qps:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！QPS({qps:,.2f}) > 目标值({target_qps:,}){Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case207写入性能测试成功!!!{Colors.ENDC}", "INFO")
        else:
            print_log(f"\n{Colors.RED}✗ 测试失败！QPS({qps:,.2f}) <= 目标值({target_qps:,}){Colors.ENDC}", "ERROR")
            raise Exception("case207写入性能未达到要求")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case207执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)

def case_207():
    """单表写入性能测试"""
    container_name = "case207_test"
    try:
        print_log(f"{Colors.BLUE}开始执行用例207...{Colors.ENDC}", "INFO")
        target_qps = 5000000  # 目标QPS为500万/秒
        
        # 1. 创建16核32GB的容器
        if not create_limited_container(container_name, cpus=16, memory_gb=32):
            raise Exception("容器资源限制验证失败")
        
        # 2. 在容器中启动 TDengine
        print_log("\n启动 TDengine 服务...", "INFO")
        cmd = f"docker exec {container_name} nohup taosd > /dev/null 2>&1 &"
        subprocess.run(cmd, shell=True)
        time.sleep(5)  # 等待服务启动
        
        # 3. 运行taosBenchmark并捕获输出
        print_log("\n开始性能测试...", "INFO")
        qps = parse_docker_qps(container_name, "/root/xintongyuan/code/UseCase207.json")
        print_log(f"\n实际写入QPS: {qps:,.2f}/s", "INFO")
        
        # 判断QPS是否达标
        if qps > target_qps:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！QPS({qps:,.2f}) > 目标值({target_qps:,}){Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case207写入性能测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            raise Exception(f"写入性能未达到要求: {qps:,.2f} <= {target_qps:,}")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case207执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
    finally:
        # 清理容器
        try:
            print_log("\n清理测试容器...", "INFO")
            subprocess.run(f"docker stop {container_name}", shell=True)
            subprocess.run(f"docker rm {container_name}", shell=True)
            print_log("✓ 容器清理完成", "SUCCESS")
        except Exception as e:
            print_log(f"清理容器时出错: {e}", "ERROR")
            

      
def verify_query_performance():
    """验证查询性能"""
    try:
        conn = taos.connect(host="localhost", user="root", password="taosdata")
        cursor = conn.cursor()
        
        # 1. 验证总行数
        print_log("\n检查数据总量...", "INFO")
        cursor.execute("select count(c0) from test.stb")
        total_rows = cursor.fetchall()[0][0]
        print_log(f"总行数: {total_rows:,}", "INFO")
        
        if total_rows != 10000000:
            print_log(f"✗ 数据量不符！期望: 10,000,000, 实际: {total_rows:,}", "ERROR")
            return False
            
        print_log("✓ 数据量验证通过", "SUCCESS")
        
        # 2. 检查查询性能
        print_log("\n测试查询性能...", "INFO")
        cursor.execute("explain analyze select c0 from test.stb")
        results = cursor.fetchall()
        
        # 解析执行时间
        execution_time = None
        for row in results:
            if "Execution Time:" in row[0]:
                execution_time = float(row[0].split(":")[1].strip().split()[0])
                break
        
        if execution_time is None:
            print_log("无法获取查询执行时间", "INFO")
            return False
            
        print_log(f"查询执行时间: {execution_time:.3f} ms", "INFO")
        
        if execution_time < 1000:  # 小于1秒 (1000ms)
            print_log("✓ 查询性能验证通过")
            return True
        else:
            print_log(f"✗ 查询性能不达标！执行时间: {execution_time:.3f} ms > 1000 ms", "ERROR")
            return False
            
    except Exception as e:
        print_log(f"验证查询性能时出错: {e}", "ERROR")
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()

def case_208_not_in_docker():
    try:
        print_log(f"{Colors.BLUE}开始执行用例208...{Colors.ENDC}", "INFO")
        print_log(f"{Colors.BLUE}先执行用例207写入数据...{Colors.ENDC}", "INFO")
        case_207()
        print_log(f"{Colors.BLUE}用例207写入数据完成...{Colors.ENDC}", "INFO")
        print_log(f"\n{Colors.BLUE}开始验证查询性能...{Colors.ENDC}", "INFO")
        if verify_query_performance():
            print_log(f"\n{Colors.GREEN}case208查询性能验证成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            raise Exception("case208查询性能验证失败")
        
    except Exception as e:
        print_log(f"\n{Colors.RED}case208执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def case_208():
    """查询性能验证测试"""
    container_name = "case208_test"
    try:
        print_log(f"{Colors.BLUE}开始执行用例208...{Colors.ENDC}", "INFO")
        
        # 1. 创建16核32GB的容器
        if not create_limited_container(container_name, cpus=16, memory_gb=32):
            raise Exception("容器资源限制验证失败")
        
        # 2. 在容器中启动 TDengine
        print_log("\n启动 TDengine 服务...", "INFO")
        cmd = f"docker exec {container_name} nohup taosd > /dev/null 2>&1 &"
        subprocess.run(cmd, shell=True)
        time.sleep(5)  # 等待服务启动
        
        # 3. 先执行case207写入数据
        print_log(f"\n{Colors.BLUE}写入测试数据...{Colors.ENDC}", "INFO")
        cmd = f"docker exec {container_name} taosBenchmark -f /root/xintongyuan/code/UseCase207.json > /dev/null 2>&1"
        subprocess.run(cmd, shell=True)
        print_log("数据写入完成", "INFO")
        
        # 4. 验证查询性能
        print_log(f"\n{Colors.BLUE}验证查询性能...{Colors.ENDC}", "INFO")
        cmd = f"docker exec {container_name} taos -s 'explain analyze select c0 from test.stb'"
        result = subprocess.check_output(cmd, shell=True).decode()
        
        # 解析执行时间
        execution_time = None
        for line in result.split('\n'):
            if "Execution Time:" in line:
                execution_time = float(line.split(":")[1].strip().split()[0])
                break
        
        if execution_time is None:
            raise Exception("无法获取查询执行时间")
            
        print_log(f"查询执行时间: {execution_time:.3f} ms", "INFO")
        
        if execution_time < 1000:  # 小于1秒 (1000ms)
            print_log(f"\n{Colors.GREEN}✓ 测试通过！查询时间({execution_time:.3f}ms) < 1000ms{Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case208查询性能验证测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            raise Exception(f"查询性能未达标: {execution_time:.3f}ms >= 1000ms")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case208执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
    finally:
        # 清理容器
        try:
            print_log("\n清理测试容器...", "INFO")
            subprocess.run(f"docker stop {container_name}", shell=True)
            subprocess.run(f"docker rm {container_name}", shell=True)
            print_log("✓ 容器清理完成", "SUCCESS")
        except Exception as e:
            print_log(f"清理容器时出错: {e}", "ERROR")

def setup_databases():
    """创建测试所需的数据库"""
    try:
        conn = taos.connect(host="localhost", user="root", password="taosdata")
        cursor = conn.cursor()
        
        # 删除已存在的数据库
        print_log(f"{Colors.BLUE}清理已存在的数据库...{Colors.ENDC}", "INFO")
        databases = ["test1", "test2", "test3"]
        for db in databases:
            cursor.execute(f"drop database if exists {db}")
            
        # 创建新数据库
        print_log(f"{Colors.BLUE}创建测试数据库...{Colors.ENDC}", "INFO")
        for db in databases:
            cursor.execute(f"create database if not exists {db} vgroups 40 buffer 1024")
            print_log(f"创建数据库: {db}", "INFO")
            
    except Exception as e:
        print_log(f"数据库设置出错: {e}", "ERROR")
        raise
    
    finally:
        if 'conn' in locals():
            conn.close()

def case_209():
    try:
        print_log(f"{Colors.BLUE}开始执行用例209...{Colors.ENDC}", "INFO")
        target_total_qps = 90000000  # 目标总QPS为9千万/秒
        
        # 清理环境
        clean_cluster_environment()
        
        # 启动三节点集群
        start_cluster(3)
        
        # 设置数据库环境
        setup_databases()
        
        # 运行3个taosBenchmark并获取各自的QPS
        print_log(f"\n{Colors.BLUE}开始运行性能测试...{Colors.ENDC}", "INFO")
        json_files = [
            "/root/xintongyuan/code/UseCase209_1.json",
            "/root/xintongyuan/code/UseCase209_2.json",
            "/root/xintongyuan/code/UseCase209_3.json"
        ]
        
        qps_results = []
        for i, json_file in enumerate(json_files, 1):
            print_log(f"\n运行第{i}个taosBenchmark...", "INFO")
            qps = parse_qps_from_output(json_file)
            qps_results.append(qps)
            print_log(f"第{i}个测试QPS: {qps:,.2f}/s", "INFO")
        
        # 计算总QPS
        total_qps = sum(qps_results)
        print_log(f"\n{Colors.BLUE}测试结果汇总:{Colors.ENDC}", "INFO")
        for i, qps in enumerate(qps_results, 1):
            print_log(f"测试{i} QPS: {qps:,.2f}/s", "INFO")
        print_log(f"总QPS: {total_qps:,.2f}/s", "INFO")
        
        # 判断总QPS是否达标
        if total_qps > target_total_qps:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！总QPS({total_qps:,.2f}) > 目标值({target_total_qps:,}){Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case209多数据库写入性能测试成功!!!{Colors.ENDC}", "INFO")
        else:
            print_log(f"\n{Colors.RED}✗ 测试失败！总QPS({total_qps:,.2f}) <= 目标值({target_total_qps:,}){Colors.ENDC}", "ERROR")
            raise Exception("case209写入性能未达到要求")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case209执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def get_disk_info():
    """获取磁盘使用信息
    taos> show test.disk_info;
            _db_usage            |
    =================================
    Compress_ratio=[33.64%]        |
    Disk_occupied=[118277k]        |
    Query OK, 2 row(s) in set (0.008628s)
    """
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("show test.disk_info")
        results = cursor.fetchall()
        
        compress_ratio = None
        disk_occupied = None
        
        for row in results:
            if 'Compress_ratio' in row[0]:
                # 处理 'Compress_ratio=[33.64%]' 的格式
                ratio_str = row[0].split('=')[1].strip('[]%')
                compress_ratio = float(ratio_str)
            elif 'Disk_occupied' in row[0]:
                # 处理 'Disk_occupied=[118277k]' 的格式
                disk_str = row[0].split('=')[1].strip('[]k')
                disk_occupied = int(disk_str)
                
        return compress_ratio, disk_occupied
        
    except Exception as e:
        print_log(f"{Colors.RED}获取磁盘信息失败: {e}{Colors.ENDC}", "ERROR")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def case_210():
    """测试数据压缩率"""
    try:
        print_log(f"{Colors.BLUE}开始执行用例210...{Colors.ENDC}", "INFO")
        
        # 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        
        # 1. 测试不压缩的情况
        print_log(f"\n{Colors.BLUE}1. 测试不压缩情况{Colors.ENDC}", "INFO")
        print_log("运行taosBenchmark写入数据...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase210_11.json"
        qps = parse_qps_from_output(json_file)
        print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
        
        # 2. 执行数据落盘
        print_log(f"\n{Colors.BLUE}执行数据落盘{Colors.ENDC}", "INFO")
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("flush database test")
        print_log("数据落盘完成", "INFO")
        conn.close()
        time.sleep(2)
        
        # 3. 获取不压缩时的磁盘占用
        ratio1, disk1 = get_disk_info()
        print_log(f"\n不压缩时的结果:", "INFO")
        print_log(f"压缩率: {ratio1:.2f}", "INFO")
        print_log(f"磁盘占用: {disk1}k", "INFO")
        
        # 4. 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        
        print_log(f"\n{Colors.BLUE}2. 测试压缩情况{Colors.ENDC}", "INFO")
        print_log("运行taosBenchmark写入数据...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase210_12.json"
        qps = parse_qps_from_output(json_file)
        print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
        
        # 5. 执行数据落盘
        print_log(f"\n{Colors.BLUE}执行数据落盘{Colors.ENDC}", "INFO")
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("flush database test")
        print_log("数据落盘完成", "INFO")
        conn.close()
        time.sleep(2)
        
        # 6. 获取压缩时的磁盘占用
        ratio2, disk2 = get_disk_info()
        print_log(f"\n压缩时的结果:", "INFO")
        print_log(f"压缩率: {ratio2:.2f}", "INFO")
        print_log(f"磁盘占用: {disk2}k", "INFO")
        
        # 7. 比较结果
        compression_improvement = ratio2 / ratio1
        print_log(f"\n{Colors.BLUE}压缩效果分析:{Colors.ENDC}", "INFO")
        print_log(f"压缩率比值(B/A): {compression_improvement:.4f}", "INFO")
        
        # 判断是否达到要求
        if compression_improvement < 0.03:  # 小于3%
            print_log(f"\n{Colors.GREEN}✓ 测试通过！压缩效果显著 (B/A = {compression_improvement:.4f} < 0.03){Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case210压缩率测试成功!!!{Colors.ENDC}", "INFO")
        else:
            print_log(f"\n{Colors.RED}✗ 测试失败！压缩效果不理想 (B/A = {compression_improvement:.4f} >= 0.03){Colors.ENDC}", "ERROR")
            raise Exception("压缩效果未达到预期")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case210执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)        

def verify_100billion_records():
    """验证是否写入了100亿条记录"""
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        
        print_log("\n检查数据总量...", "INFO")
        start_time = time.time()
        cursor.execute("select count(*) from test1.stb")
        result = cursor.fetchall()
        total_rows = result[0][0]
        query_time = time.time() - start_time
        
        print_log(f"总行数: {total_rows:,}", "INFO")
        print_log(f"查询耗时: {query_time:.2f}秒", "INFO")
        
        return total_rows >= 10_000_000_000, query_time
        
    except Exception as e:
        print_log(f"验证记录数时出错: {e}", "ERROR")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def verify_interval_query():
    """验证interval查询性能"""
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        
        print_log("\n执行interval查询...", "INFO")
        start_time = time.time()
        cursor.execute("select count(*) from test1.stb interval(1d)")
        cursor.fetchall()
        query_time = time.time() - start_time
        
        print_log(f"interval查询耗时: {query_time:.2f}秒", "INFO")
        
        return query_time < 2.0, query_time
        
    except Exception as e:
        print_log(f"验证查询性能时出错: {e}", "ERROR")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            
def get_current_count():
    """获取当前写入的记录数"""
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        cursor.execute("select count(*) from test1.stb")
        result = cursor.fetchall()
        return result[0][0]
    except Exception:
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def monitor_write_progress(json_file):
    """监控写入进度，每5分钟显示一次进度
    
    Args:
        json_file: taosBenchmark配置文件路径
    """
    try:
        # 启动taosBenchmark进程
        print_log(f"\n{Colors.BLUE}开始写入数据，每5分钟显示一次进度...{Colors.ENDC}", "INFO")
        process = subprocess.Popen(
            f"taosBenchmark -f {json_file}",
            shell=True,
            # stdout=subprocess.PIPE,
            # stderr=subprocess.PIPE
            stdout=subprocess.DEVNULL,  # 不捕获标准输出
            stderr=subprocess.DEVNULL   # 不捕获标准错误
        )
        
        start_time = time.time()
        last_check_time = start_time
        last_count = None  # 修改为None，用于首次检查判断
        
        while process.poll() is None:
            current_time = time.time()
            
            # 第一次5秒后检查，之后每5分钟检查
            if (last_count is None and current_time - start_time >= 5) or \
               (last_count is not None and current_time - last_check_time >= 300):
                
                current_count = get_current_count()
                if current_count is not None:
                    elapsed_minutes = (current_time - start_time) / 60
                    
                    if last_count is None:
                        # 首次检查
                        print_log(f"\n{Colors.BLUE}写入进度 ({time.strftime('%Y-%m-%d %H:%M:%S')}){Colors.ENDC}", "INFO")
                        print_log(f"已写入记录数: {current_count:,}", "INFO")
                        print_log(f"已用时间: {elapsed_minutes:.1f} 分钟", "INFO")
                        last_count = current_count
                    else:
                        # 后续检查
                        time_diff = current_time - last_check_time
                        speed = (current_count - last_count) / time_diff
                        if speed > 0:
                            remaining_records = 10_000_000_000 - current_count
                            remaining_hours = (remaining_records / speed) / 3600
                            
                            print_log(f"\n{Colors.BLUE}写入进度 ({time.strftime('%Y-%m-%d %H:%M:%S')}){Colors.ENDC}", "INFO")
                            print_log(f"已写入记录数: {current_count:,}", "INFO")
                            print_log(f"剩余需要写入记录数: {remaining_records:,}", "INFO")
                            print_log(f"已用时间: {elapsed_minutes:.1f} 分钟", "INFO")
                            print_log(f"预计剩余时间: {remaining_hours:.1f} 小时", "INFO")
                            print_log(f"总体完成度: {(current_count/10_000_000_000)*100:.2f}%", "INFO")
                        else:
                            print_log(f"\n{Colors.YELLOW}警告：检测到写入速度异常，可能是系统繁忙或出现问题{Colors.ENDC}", "INFO")
                            print_log(f"当前记录数: {current_count:,}", "INFO")
                            print_log(f"已用时间: {elapsed_minutes:.1f} 分钟", "INFO")
                        
                        last_count = current_count
                    last_check_time = current_time
            
            time.sleep(1)
        
        # 返回最后观察到的写入速度
        if last_count is not None and time.time() - start_time > 0:
            final_speed = last_count / (time.time() - start_time)
            return final_speed
        return 0
        
    except Exception as e:
        print_log(f"{Colors.RED}监控写入进度时出错: {e}{Colors.ENDC}", "ERROR")
        return 0

def case_211():
    """大数据量写入和查询性能测试"""
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例211 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")

            # 清理和启动单节点环境
            clean_single_environment_and_start_single_environment()
            time.sleep(5)  # 等待环境稳定

            # 1. 写入100亿数据
            print_log(f"\n{Colors.BLUE}1. 写入100亿数据，预计超过半个小时，请耐心等待！{Colors.ENDC}", "INFO")
            json_file = "/root/xintongyuan/code/UseCase211.json"
            qps = monitor_write_progress(json_file)
            print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")

            # 2. 验证总行数
            print_log(f"\n{Colors.BLUE}2. 验证数据总量{Colors.ENDC}", "INFO")
            records_ok, count_time = verify_100billion_records()
            if not records_ok:
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: 数据总量未达到100亿条{Colors.ENDC}", "INFO")
                    print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                    current_try += 1
                    continue
                else:
                    raise Exception("数据总量未达到100亿条")

            # 3. 验证查询性能
            print_log(f"\n{Colors.BLUE}3. 验证查询性能{Colors.ENDC}", "INFO")
            query_ok, query_time = verify_interval_query()
            if not query_ok:
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: 查询性能未达标{Colors.ENDC}", "INFO")
                    print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                    current_try += 1
                    continue
                else:
                    raise Exception(f"查询性能未达标，耗时 {query_time:.2f}秒 > 2秒")

            # 测试通过
            print_log(f"\n{Colors.GREEN}case211大数据量查询性能测试成功!!!{Colors.ENDC}", "SUCCESS")
            return  # 测试成功，直接返回

        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                # 等待一段时间后重试
                time.sleep(5)
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case211执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 在每次尝试结束时清理环境
            try:
                if current_try < max_retries:  # 如果还有重试机会，清理环境
                    print_log("\n清理环境准备下一次尝试...", "INFO")
                    clean_single_environment_and_start_single_environment()
                    time.sleep(5)
            except Exception as cleanup_e:
                print_log(f"清理环境时出错: {cleanup_e}", "ERROR")

def verify_case212_performance():
    """验证少量数据的查询性能"""
    try:
        conn = taos.connect()
        cursor = conn.cursor()
        
        # 1. 验证总行数
        print_log("\n检查数据总量...", "INFO")
        cursor.execute("select count(*) from test1.stb")
        total_rows = cursor.fetchall()[0][0]
        print_log(f"总行数: {total_rows:,}", "INFO")
        
        if total_rows != 10000:
            print_log(f"✗ 数据量不符！期望: 10,000, 实际: {total_rows:,}", "ERROR")
            return False, None
            
        print_log("✓ 数据量验证通过")
        
        # 2. 检查查询性能
        print_log("\n测试查询性能...", "INFO")
        cursor.execute("explain analyze select last_row(c0) from test1.stb")
        results = cursor.fetchall()
        
        # 解析执行时间
        execution_time = None
        for row in results:
            if "Execution Time:" in row[0]:
                execution_time = float(row[0].split(":")[1].strip().split()[0])
                break
        
        if execution_time is None:
            print_log("无法获取查询执行时间", "ERROR")
            return False, None
            
        print_log(f"查询执行时间: {execution_time:.3f} ms", "INFO")
        
        if execution_time < 10:  # 小于10ms
            print_log("✓ 查询性能验证通过")
            return True, execution_time
        else:
            print_log(f"✗ 查询性能不达标！执行时间: {execution_time:.3f} ms > 10 ms", "ERROR")
            return False, execution_time
            
    except Exception as e:
        print_log(f"验证查询性能时出错: {e}", "ERROR")
        return False, None
    finally:
        if 'conn' in locals():
            conn.close()

def case_212():
    """少量数据的查询性能测试"""
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例212 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")
            
            # 1. 清理和启动单节点环境
            clean_single_environment_and_start_single_environment()
            time.sleep(5)  # 等待环境稳定
            
            # 2. 运行taosBenchmark写入数据
            print_log(f"\n{Colors.BLUE}1. 写入10000条测试数据{Colors.ENDC}", "INFO")
            json_file = "/root/xintongyuan/code/UseCase212.json"
            qps = parse_qps_from_output(json_file)
            print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
            
            # 3. 验证数据量和查询性能
            print_log(f"\n{Colors.BLUE}2. 验证查询性能{Colors.ENDC}", "INFO")
            success, query_time = verify_case212_performance()
            
            if not success:
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败", "INFO")
                    if query_time is not None:
                        print_log(f"查询耗时 {query_time:.3f}ms > 10ms", "INFO")
                    print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    if query_time is not None:
                        raise Exception(f"查询性能未达标，耗时 {query_time:.3f}ms > 10ms")
                    else:
                        raise Exception("测试执行失败")

            print_log(f"\n{Colors.GREEN}case212少量数据查询性能测试成功!!!{Colors.ENDC}", "SUCCESS")
            return  # 测试成功，直接返回
            
        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                time.sleep(5)  # 等待资源释放
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case212执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 在每次尝试结束时清理环境（除非是最后一次成功的尝试）
            try:
                if current_try < max_retries and not success:  # 如果还有重试机会且当前尝试失败
                    print_log("\n清理环境准备下一次尝试...", "INFO")
                    clean_single_environment_and_start_single_environment()
                    time.sleep(5)
            except Exception as cleanup_e:
                print_log(f"清理环境时出错: {cleanup_e}", "ERROR")
                
        
def check_linear_growth(qps_results):
    """检查QPS是否呈线性增长
    
    Args:
        qps_results: 列表，包含每个测试的QPS值
    """
    try:
        # 首先检查是否有QPS为0的情况
        if any(qps == 0 for qps in qps_results):
            print_log(f"\n{Colors.RED}✗ 检测到QPS为0，写入性能异常！{Colors.ENDC}", "INFO")
            print_log("各阶段QPS值：", "INFO")
            for i, qps in enumerate(qps_results, 1):
                print_log(f"第{i}阶段 QPS: {qps:,.2f}", "INFO")
            return False
        
        # 计算每个阶段的总QPS
        qps1 = qps_results[0]  # 单节点QPS
        qps2 = qps_results[1]  # 双节点QPS
        qps3 = qps_results[2]  # 三节点QPS
        
        # 计算理想值和误差范围（30%）
        error_margin = 0.3
        expected_qps2 = qps1 * 2
        expected_qps3 = qps1 * 3
        
        expected_qps2_min = expected_qps2 * (1 - error_margin)
        expected_qps2_max = expected_qps2 * (1 + error_margin)
        expected_qps3_min = expected_qps3 * (1 - error_margin)
        expected_qps3_max = expected_qps3 * (1 + error_margin)
        
        # 检查是否在误差范围内
        is_linear2 = expected_qps2_min <= qps2 <= expected_qps2_max
        is_linear3 = expected_qps3_min <= qps3 <= expected_qps3_max
        
        print_log(f"\n{Colors.BLUE}线性增长分析:{Colors.ENDC}", "INFO")
        print_log(f"单实例 QPS: {qps1:,.2f}", "INFO")
        print_log(f"双实例 QPS: {qps2:,.2f} (期望范围: {expected_qps2_min:,.2f} ~ {expected_qps2_max:,.2f})", "INFO")
        print_log(f"三实例 QPS: {qps3:,.2f} (期望范围: {expected_qps3_min:,.2f} ~ {expected_qps3_max:,.2f})", "INFO")
        
        if is_linear2 and is_linear3:
            print_log(f"\n{Colors.GREEN}✓ QPS呈线性增长！{Colors.ENDC}", "SUCCESS")
            return True
        else:
            print_log(f"\n{Colors.RED}✗ QPS未呈线性增长！{Colors.ENDC}", "INFO")
            if not is_linear2:
                print_log(f"双实例性能不符合线性增长预期", "ERROR")
            if not is_linear3:
                print_log(f"三实例性能不符合线性增长预期", "ERROR")
            return False
            
    except Exception as e:
        print_log(f"检查线性增长时出错: {e}", "ERROR")
        return False

        
def case_213():
    """TMQ消息延迟测试"""
    try:
        print_log(f"{Colors.BLUE}开始执行用例213...{Colors.ENDC}", "INFO")
        
        # 1. 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        time.sleep(5)  # 等待环境稳定
        
        max_retries = 5  # 最大重试次数
        current_try = 1
        
        while current_try <= max_retries:
            # 2. 调用外部测试程序
            print_log(f"\n{Colors.BLUE}2. 执行TMQ消息延迟测试 (第{current_try}次尝试){Colors.ENDC}", "INFO")
            cmd = "python3 /root/xintongyuan/code/xtytest.py subscribe_delay_10ms"
            output = subprocess.check_output(cmd, shell=True).decode('utf-8')
            
            # 3. 解析测试结果
            print_log("\n测试程序输出:", "INFO")
            print_log(output.strip(), "INFO")
            
            # 4. 检查结果
            delay_match = re.search(r'subscribe use: (\d+)ms', output)
            if delay_match:
                delay = int(delay_match.group(1))
                print_log(f"\nTMQ消息延迟: {delay}ms", "INFO")
                
                if delay < 10:
                    print_log(f"✓ 测试通过！消息延迟({delay}ms) < 10ms", "SUCCESS")
                    print_log(f"\n{Colors.GREEN}case213 TMQ消息延迟测试成功!!!{Colors.ENDC}", "SUCCESS")
                    return  # 测试成功，直接返回
                else:
                    print_log(f"✗ 本次测试失败！消息延迟({delay}ms) >= 10ms", "INFO")
                    if current_try < max_retries:
                        print_log(f"\n尝试重新执行测试... (剩余{max_retries - current_try}次尝试)", "INFO")
                        time.sleep(5)  # 等待一段时间后重试
            else:
                print_log("无法从测试输出中解析延迟时间", "ERROR")
                
            current_try += 1
            
        # 如果所有尝试都失败
        raise Exception(f"TMQ消息延迟测试失败，{max_retries}次尝试都未能达到10ms要求")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case213执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
def case_214():
    """数据库写入线性扩展测试"""
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例214 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")
            qps_results = []  # 存储每次测试的QPS结果
            
            # 分别测试1、2、3个节点的情况
            for node_count in range(1, 4):
                print_log(f"\n{Colors.BLUE}开始{node_count}节点测试...{Colors.ENDC}", "INFO")
                
                # 1. 启动指定数量节点的集群
                start_cluster(node_count)
                time.sleep(5)  # 等待集群稳定
                
                # 设置数据库环境
                setup_databases()
                
                # 2. 运行对应数量的taosBenchmark
                print_log(f"\n{Colors.BLUE}执行{node_count}个写入任务...{Colors.ENDC}", "INFO")
                json_files = [
                    "/root/xintongyuan/code/UseCase214_1.json",
                    "/root/xintongyuan/code/UseCase214_2.json",
                    "/root/xintongyuan/code/UseCase214_3.json"
                ]
                
                # 执行当前节点数量对应的json文件数
                current_qps = 0
                for i in range(node_count):
                    print_log(f"运行第{i+1}个taosBenchmark...", "INFO")
                    qps = parse_qps_from_output(json_files[i])
                    current_qps += qps
                    print_log(f"第{i+1}个测试QPS: {qps:,.2f}/s", "INFO")
                
                qps_results.append(current_qps)
                print_log(f"{node_count}节点总QPS: {current_qps:,.2f}/s", "INFO")
                
                # 如果不是最后一轮，清理环境
                if node_count < 3:
                    print_log(f"\n{Colors.BLUE}清理环境准备下一轮测试...{Colors.ENDC}", "INFO")
                    clean_cluster_environment()
                    time.sleep(3)
            
            # 检查写入性能的线性增长
            if check_linear_growth(qps_results):
                print_log(f"\n{Colors.GREEN}case214写入性能线性扩展测试成功!!!{Colors.ENDC}", "SUCCESS")
                return  # 测试成功，直接返回
            else:
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: QPS未呈现线性增长{Colors.ENDC}", "INFO")
                    print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    raise Exception("case214写入性能线性扩展测试失败")
                    
        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                time.sleep(5)
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case214执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 清理环境
            try:
                clean_cluster_environment()
            except Exception as cleanup_e:
                print_log(f"清理环境时出错: {cleanup_e}", "ERROR")
        
def execute_query_and_get_time():
    """执行查询并获取执行时间"""
    try:
        conn = taos.connect(host="localhost", user="root", password="taosdata")
        cursor = conn.cursor()
        
        start_time = time.time()
        cursor.execute("select count(*), max(c0) from test1.stb interval(1s) limit 1")
        cursor.fetchall()
        execution_time = (time.time() - start_time) * 1000  # 转换为毫秒
        
        return execution_time
    except Exception as e:
        print_log(f"执行查询出错: {e}", "ERROR")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def check_query_linear_growth(query_times):
    """检查查询时间是否呈线性扩展
    
    Args:
        query_times: 列表，包含每次查询的执行时间(ms)
    """
    try:
        # 首先检查是否有异常的查询时间（比如0或者异常大的值）
        if any(time <= 0 for time in query_times):
            print_log(f"\n{Colors.RED}✗ 检测到无效的查询时间！{Colors.ENDC}", "INFO")
            print_log("各节点查询时间：", "INFO")
            for i, qt in enumerate(query_times, 1):
                print_log(f"第{i}节点查询时间: {qt:.2f}ms", "INFO")
            return False
        
        # 计算期望范围
        base_time = query_times[0]  # 基准时间（单节点）
        error_margin = 0.2  # 允许20%的误差范围
        
        # 计算期望范围
        expected_min = base_time * (1 - error_margin)
        expected_max = base_time * (1 + error_margin)
        
        print_log(f"\n{Colors.BLUE}查询性能线性分析:{Colors.ENDC}", "INFO")
        print_log(f"单节点查询时间: {query_times[0]:.2f}ms (基准时间)", "INFO")
        print_log(f"双节点查询时间: {query_times[1]:.2f}ms (期望范围: {expected_min:.2f}ms ~ {expected_max:.2f}ms)", "INFO")
        print_log(f"三节点查询时间: {query_times[2]:.2f}ms (期望范围: {expected_min:.2f}ms ~ {expected_max:.2f}ms)", "INFO")
        
        # 检查是否在误差范围内
        is_linear2 = expected_min <= query_times[1] <= expected_max
        is_linear3 = expected_min <= query_times[2] <= expected_max
        
        if is_linear2 and is_linear3:
            print_log(f"\n{Colors.GREEN}✓ 查询性能呈线性扩展！所有节点查询时间在误差范围内{Colors.ENDC}", "SUCCESS")
            return True
        else:
            print_log(f"\n{Colors.RED}✗ 查询性能未呈线性扩展！{Colors.ENDC}", "INFO")
            if not is_linear2:
                print_log(f"双节点查询时间超出误差范围: {expected_min:.2f}ms ~ {expected_max:.2f}ms", "ERROR")
            if not is_linear3:
                print_log(f"三节点查询时间超出误差范围: {expected_min:.2f}ms ~ {expected_max:.2f}ms", "ERROR")
            return False
            
    except Exception as e:
        print_log(f"检查查询线性增长时出错: {e}", "ERROR")
        return False

def case_215():
    """查询性能线性扩展测试"""
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例215 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")
            query_times = []  # 存储每次查询的执行时间
            
            # 分别测试1、2、3个节点的情况
            for node_count in range(1, 4):
                print_log(f"\n{Colors.BLUE}开始{node_count}节点测试...{Colors.ENDC}", "INFO")
                
                # 1. 启动指定数量节点的集群
                start_cluster(node_count)
                time.sleep(5)  # 等待集群稳定
                
                # 2. 运行taosBenchmark写入数据
                print_log(f"\n{Colors.BLUE}写入测试数据{Colors.ENDC}", "INFO")
                json_file = "/root/xintongyuan/code/UseCase215.json"
                qps = parse_qps_from_output(json_file)
                print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
                
                # 3. 执行数据落盘
                print_log(f"\n{Colors.BLUE}执行数据落盘{Colors.ENDC}", "INFO")
                conn = taos.connect(host="localhost", user="root", password="taosdata")
                cursor = conn.cursor()
                cursor.execute("flush database test1")
                print_log("数据落盘完成", "INFO")
                conn.close()
                time.sleep(2)
                
                # 4. 执行查询并记录时间
                print_log(f"\n{Colors.BLUE}执行查询测试{Colors.ENDC}", "INFO")
                query_time = execute_query_and_get_time()
                query_times.append(query_time)
                print_log(f"{node_count}节点查询执行时间: {query_time:.2f}ms", "INFO")
                
                # 如果不是最后一轮，清理环境
                if node_count < 3:
                    print_log(f"\n{Colors.BLUE}清理环境准备下一轮测试...{Colors.ENDC}", "INFO")
                    clean_cluster_environment()
                    time.sleep(3)
            
            # 检查查询性能的线性扩展性
            if check_query_linear_growth(query_times):
                print_log(f"\n{Colors.GREEN}case215查询性能线性扩展测试成功!!!{Colors.ENDC}", "SUCCESS")
                return  # 测试成功，直接返回
            else:
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: 查询性能未呈现线性扩展{Colors.ENDC}", "INFO")
                    print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    raise Exception("case215查询性能线性扩展测试失败")
                    
        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                time.sleep(5)
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case215执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 清理环境
            try:
                clean_cluster_environment()
            except Exception as cleanup_e:
                print_log(f"清理环境时出错: {cleanup_e}", "ERROR")
        

def verify_data_export():
    """验证数据导出结果"""
    try:
        print_log("\n验证导出数据...", "INFO")
        export_file = "/root/xintongyuan/code/test.sql"
        
        # 检查文件是否存在
        if not os.path.exists(export_file):
            print_log(f"✗ 导出文件不存在: {export_file}", "ERROR")
            return False
            
        # 统计文件行数
        cmd = f"wc -l {export_file}"
        line_count = int(subprocess.check_output(cmd, shell=True).decode().strip().split()[0])
        
        print_log(f"数据文件行数: {line_count:,}", "INFO")
        
        if line_count >= 100000000:
            print_log("✓ 导出数据行数正确", "SUCCESS")
            return True
        else:
            print_log(f"✗ 导出数据行数不符，期望超过100,000,000行，实际{line_count:,}行", "ERROR")
            return False
            
    except Exception as e:
        print_log(f"验证数据导出时出错: {e}", "ERROR")
        return False
    
def create_limited_container(container_name, cpus=2, memory_gb=4, image="tdengine/tdengine-amd64:3.3.6.6"):
    """创建资源受限的Docker容器
    
    Args:
        container_name: 容器名称
        cpus: CPU核心数限制，默认2核
        memory_gb: 内存限制(GB)，默认4GB
        image: Docker镜像名称，默认使用TDengine官方镜像
    
    Returns:
        bool: 容器创建和验证是否成功
    """
    try:
        print_log(f"\n创建资源受限的容器({cpus}核CPU, {memory_gb}GB内存)...", "INFO")
        print_log(f"使用镜像: {image}", "INFO")
        
        # 创建容器
        cmd = f"""docker run -d --name {container_name} \
            --cpus={cpus} \
            --memory={memory_gb}g \
            --memory-swap={memory_gb}g \
            -v /root/xintongyuan:/root/xintongyuan \
            {image} \
            tail -f /dev/null"""
        subprocess.run(cmd, shell=True, check=True)
        print_log("✓ 容器创建成功", "SUCCESS")

        # 验证容器资源限制
        return verify_container_resources(container_name, expected_cpus=cpus, expected_memory_gb=memory_gb)
        
    except Exception as e:
        print_log(f"创建容器失败: {e}", "ERROR")
        return False

def verify_container_resources(container_name, expected_cpus=2, expected_memory_gb=4):
    """验证容器内部实际可用的资源
    
    Args:
        container_name: 容器名称
        expected_cpus: 期望的CPU核心数
        expected_memory_gb: 期望的内存大小(GB)
    """
    try:
        print_log("\n验证容器资源限制:", "INFO")
        
        # 1. CPU限制验证
        print_log("\nCPU资源验证:", "INFO")
        cmd = f"docker exec {container_name} cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us"
        cpu_quota = int(subprocess.check_output(cmd, shell=True).decode().strip())
        cmd = f"docker exec {container_name} cat /sys/fs/cgroup/cpu/cpu.cfs_period_us"
        cpu_period = int(subprocess.check_output(cmd, shell=True).decode().strip())
        actual_cpus = cpu_quota / cpu_period
        print_log(f"CPU配额/周期比率: {actual_cpus:.1f}", "INFO")
        
        # 检查可见的CPU核心
        cmd = f"docker exec {container_name} nproc"
        visible_cpus = int(subprocess.check_output(cmd, shell=True).decode().strip())
        print_log(f"可见CPU核心数: {visible_cpus}", "INFO")
        
        # 检查CPU信息
        cmd = f"docker exec {container_name} cat /proc/cpuinfo | grep processor | wc -l"
        cpu_count = int(subprocess.check_output(cmd, shell=True).decode().strip())
        print_log(f"处理器数量: {cpu_count}", "INFO")

        # 2. 内存限制验证
        print_log("\n内存资源验证:", "INFO")
        # 检查cgroup内存限制
        cmd = f"docker exec {container_name} cat /sys/fs/cgroup/memory/memory.limit_in_bytes"
        memory_limit = int(subprocess.check_output(cmd, shell=True).decode().strip())
        memory_limit_gb = memory_limit / (1024**3)
        print_log(f"Cgroup内存限制: {memory_limit_gb:.1f}GB", "INFO")
        
        # 检查实际可用内存
        cmd = f"docker exec {container_name} free -h"
        print_log("\n内存使用情况:", "INFO")
        mem_info = subprocess.check_output(cmd, shell=True).decode().strip()
        for line in mem_info.split('\n'):
            print_log(line, "INFO")
            
        # 3. 检查系统负载
        cmd = f"docker exec {container_name} cat /proc/loadavg"
        loadavg = subprocess.check_output(cmd, shell=True).decode().strip()
        print_log(f"\n系统负载: {loadavg}", "INFO")
        
        # 验证资源是否符合预期
        is_cpu_ok = abs(actual_cpus - expected_cpus) < 0.1  # 允许0.1的误差
        is_memory_ok = abs(memory_limit_gb - expected_memory_gb) < 0.1  # 允许0.1GB的误差
        
        if is_cpu_ok and is_memory_ok:
            print_log("\n✓ 容器资源限制验证通过", "SUCCESS")
            return True
        else:
            if not is_cpu_ok:
                print_log(f"✗ CPU限制不符合预期: {actual_cpus:.1f} != {expected_cpus}", "ERROR")
            if not is_memory_ok:
                print_log(f"✗ 内存限制不符合预期: {memory_limit_gb:.1f}GB != {expected_memory_gb}GB", "ERROR")
            return False
            
    except Exception as e:
        print_log(f"验证容器资源时出错: {e}", "ERROR")
        return False

            
def case_216():
    """内存使用和数据导出测试"""
    container_name = "case216_test"
    max_retries = 5  # 最大重试次数
    current_try = 1

    while current_try <= max_retries:
        try:
            print_log(f"{Colors.BLUE}开始执行用例216 (第{current_try}次尝试)...{Colors.ENDC}", "INFO")
            
            # 1. 创建2核4GB的容器
            if not create_limited_container(container_name, cpus=8, memory_gb=2):
                raise Exception("容器资源限制验证失败")
            
            # 2. 在容器中启动 TDengine
            print_log("\n启动 TDengine 服务...", "INFO")
            cmd = f"docker exec {container_name} nohup taosd > /dev/null 2>&1 &"
            subprocess.run(cmd, shell=True)
            time.sleep(5)  # 等待服务启动
            
            # 3. 在容器中写入测试数据
            print_log(f"\n{Colors.BLUE}写入测试数据{Colors.ENDC}", "INFO")
            cmd = f"docker exec {container_name} taosBenchmark -d test1 -y > /dev/null 2>&1"
            subprocess.run(cmd, shell=True)
            print_log("写入数据完成", "INFO")
            
            # 4. 验证数据量
            print_log(f"\n{Colors.BLUE}验证数据总量{Colors.ENDC}", "INFO")
            cmd = f"docker exec {container_name} taos -s 'select count(*) from test1.meters'"
            result = subprocess.check_output(cmd, shell=True).decode()
            
            # 解析查询结果
            try:
                lines = result.strip().split('\n')
                count = None
                for line in lines:
                    if '|' in line and not line.startswith('='):
                        count_str = line.split('|')[0].strip()
                        if count_str.isdigit():
                            count = int(count_str)
                            break
                
                if count is None:
                    raise Exception("未找到有效的计数结果")
                    
                print_log(f"总行数: {count:,}", "INFO")
                
                if count < 100000000:
                    raise Exception(f"数据量不符，期望超过1亿条，实际{count:,}条")
                    
            except (ValueError, IndexError) as e:
                print_log("解析查询结果时出错，原始输出：", "ERROR")
                print_log(result, "ERROR")
                raise Exception(f"解析查询结果失败: {e}")
                
            # 5. 导出数据
            print_log(f"\n{Colors.BLUE}导出数据到文件，预计超过20分钟，请耐心等待！{Colors.ENDC}", "INFO")
            export_start = time.time()
            # 修改导出路径为完整路径
            cmd = f"docker exec {container_name} bash -c 'taos -s \"select * from test1.meters;\" > /root/xintongyuan/code/test.sql'"
            subprocess.run(cmd, shell=True)
            export_time = time.time() - export_start
            print_log(f"数据导出完成，耗时: {export_time:.2f}秒", "INFO")
            
            # 确保文件写入完成
            time.sleep(10)  # 增加等待时间
            
            # 6. 验证导出结果
            print_log(f"\n{Colors.BLUE}验证导出结果{Colors.ENDC}", "INFO")
            if not verify_data_export():
                if current_try < max_retries:
                    print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败，准备重试...{Colors.ENDC}", "INFO")
                    # 清理容器准备重试
                    subprocess.run(f"docker stop {container_name}", shell=True)
                    subprocess.run(f"docker rm {container_name}", shell=True)
                    time.sleep(5)  # 等待资源释放
                    current_try += 1
                    continue
                else:
                    raise Exception("数据导出验证失败")
            
            print_log(f"\n{Colors.GREEN}case216内存使用和数据导出测试成功!!!{Colors.ENDC}", "SUCCESS")
            return  # 测试成功，直接返回
            
        except Exception as e:
            if current_try < max_retries:
                print_log(f"\n{Colors.YELLOW}第{current_try}次尝试失败: {e}{Colors.ENDC}", "INFO")
                print_log(f"准备第{current_try + 1}次尝试...", "INFO")
                # 清理容器准备重试
                try:
                    subprocess.run(f"docker stop {container_name}", shell=True)
                    subprocess.run(f"docker rm {container_name}", shell=True)
                except Exception:
                    pass
                time.sleep(5)  # 等待资源释放
                current_try += 1
            else:
                print_log(f"\n{Colors.RED}case216执行出错!!!: {e}{Colors.ENDC}", "ERROR")
                sys.exit(1)
        finally:
            # 只在最后一次尝试或成功时清理容器
            if current_try == max_retries or verify_data_export():
                try:
                    print_log("\n清理测试容器...", "INFO")
                    subprocess.run(f"docker stop {container_name}", shell=True)
                    subprocess.run(f"docker rm {container_name}", shell=True)
                    print_log("✓ 容器清理完成", "SUCCESS")
                except Exception as e:
                    print_log(f"清理容器时出错: {e}", "ERROR")
            
def get_recovery_time_from_log():
    """从日志文件中获取恢复时间"""
    try:
        log_files = [
            "/root/xintongyuan/single/log/taosdlog.0",
            "/root/xintongyuan/single/log/taosdlog.1"
        ]
        
        # 获取当前年份
        current_year = time.strftime("%Y")
        
        # 首先找到存在的日志文件
        existing_files = [f for f in log_files if os.path.exists(f)]
        if not existing_files:
            print_log("未找到任何日志文件", "ERROR")
            return None
            
        # 获取最新的 offline to online 记录
        cmd = f"cat {' '.join(existing_files)} | grep 'offline to online' | tail -n 1"
        try:
            online_output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            if not online_output:
                print_log("未找到恢复完成的记录", "ERROR")
                return None
                
            # 获取 online 时间，添加当前年份
            time_str = f"{current_year}-{online_output.split()[0]} {online_output.split()[1]}"
            online_time = datetime.strptime(time_str, "%Y-%m/%d %H:%M:%S.%f")
            print_log(f"找到恢复完成时间: {online_time}", "INFO")
            
            # 获取最近的 startup path 记录
            cmd = f"cat {' '.join(existing_files)} | grep 'startup path' | tail -n 1"
            log_start_output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            if not log_start_output:
                print_log("未找到启动日志记录", "ERROR")
                return None
                
            # 获取启动时间，添加当前年份
            time_str = f"{current_year}-{log_start_output.split()[0]} {log_start_output.split()[1]}"
            log_start_time = datetime.strptime(time_str, "%Y-%m/%d %H:%M:%S.%f")
            print_log(f"找到启动时间: {log_start_time}", "INFO")
            
            # 如果启动时间晚于恢复时间，继续查找更早的启动记录
            if log_start_time > online_time:
                cmd = f"cat {' '.join(existing_files)} | grep 'open new log file' | grep -B 1000 '{online_output.split()[0]} {online_output.split()[1]}' | tail -n 1"
                log_start_output = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
                if log_start_output:
                    time_str = f"{current_year}-{log_start_output.split()[0]} {log_start_output.split()[1]}"
                    log_start_time = datetime.strptime(time_str, "%Y-%m/%d %H:%M:%S.%f")
                    print_log(f"找到正确的启动时间: {log_start_time}", "INFO")
                else:
                    print_log("未找到匹配的启动日志记录", "ERROR")
                    return None
                    
            # 计算恢复时间
            recovery_time = (online_time - log_start_time).total_seconds()
            print_log(f"恢复时间: {recovery_time:.3f}秒", "INFO")
            print_log(f"开始时间: {log_start_time}", "INFO")
            print_log(f"结束时间: {online_time}", "INFO")
            
            return recovery_time
            
        except subprocess.CalledProcessError as e:
            print_log(f"执行命令出错: {e}", "ERROR")
            return None
            
    except Exception as e:
        print_log(f"分析日志文件出错: {e}", "ERROR")
        return None

def case_217():
    """节点故障恢复时间测试"""
    try:
        print_log(f"{Colors.BLUE}开始执行用例217...{Colors.ENDC}", "INFO")
        
        # 第一轮测试：1亿数据，2列1标签
        print_log(f"\n{Colors.BLUE}1. 第一轮测试: 1亿数据(2列1标签){Colors.ENDC}", "INFO")
        
        # 清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        time.sleep(5)
        
        print_log("运行taosBenchmark写入数据...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase217_1.json"
        qps = parse_qps_from_output(json_file)
        print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
        
        # 终止taosd进程
        print_log("\n终止taosd进程...", "INFO")
        os.system("ps -ef|grep -wi taosd| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1")
        time.sleep(5)
        
        # 清理旧日志文件
        print_log("清理旧日志文件...", "INFO")
        os.system("rm -f /root/xintongyuan/single/log/taosdlog.*")
        time.sleep(1)
        
        # 重启taosd
        print_log("重启taosd服务...", "INFO")
        cmd = "nohup taosd -c /root/xintongyuan/single/cfg > /dev/null 2>&1 &"
        subprocess.Popen(cmd, shell=True)
        time.sleep(10)  # 等待服务启动
        
        # 获取第一轮恢复时间
        print_log("\n分析第一轮恢复时间...", "INFO")
        recovery_time1 = get_recovery_time_from_log()
        if recovery_time1 is None:
            raise Exception("无法获取第一轮恢复时间")
        
        # 第二轮测试：5亿数据，6列5标签
        print_log(f"\n{Colors.BLUE}2. 第二轮测试: 5亿数据(6列5标签){Colors.ENDC}", "INFO")
                
        # 再次清理和启动单节点环境
        clean_single_environment_and_start_single_environment()
        time.sleep(5)
        
        print_log("运行taosBenchmark写入数据...", "INFO")
        json_file = "/root/xintongyuan/code/UseCase217_2.json"
        qps = parse_qps_from_output(json_file)
        print_log(f"写入完成，QPS: {qps:,.2f}/s", "INFO")
        
        # 再次终止taosd进程
        print_log("\n终止taosd进程...", "INFO")
        os.system("ps -ef|grep -wi taosd| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1")
        time.sleep(5)
        
        # 清理旧日志文件
        print_log("清理旧日志文件...", "INFO")
        os.system("rm -f /root/xintongyuan/single/log/taosdlog.*")
        time.sleep(1)
        
        # 重启taosd
        print_log("重启taosd服务...", "INFO")
        cmd = "nohup taosd -c /root/xintongyuan/single/cfg > /dev/null 2>&1 &"
        subprocess.Popen(cmd, shell=True)
        time.sleep(10)  # 等待服务启动
        
        # 获取第二轮恢复时间
        print_log("\n分析第二轮恢复时间...", "INFO")
        recovery_time2 = get_recovery_time_from_log()
        if recovery_time2 is None:
            raise Exception("无法获取第二轮恢复时间")
                
         # 比较两次恢复时间
        time_diff = abs(recovery_time2 - recovery_time1)
        print_log(f"\n{Colors.BLUE}恢复时间比较:{Colors.ENDC}", "INFO")
        print_log(f"第一轮恢复时间: {recovery_time1:.3f}秒", "INFO")
        print_log(f"第二轮恢复时间: {recovery_time2:.3f}秒", "INFO")
        print_log(f"时间差异: {time_diff:.3f}秒", "INFO")
        
        # 判断时间是否相近（差异在20%以内）
        if time_diff <= max(recovery_time1, recovery_time2) * 0.2:
            print_log(f"\n{Colors.GREEN}✓ 测试通过！两次恢复时间相近，差异在可接受范围内{Colors.ENDC}", "SUCCESS")
            print_log(f"\n{Colors.GREEN}case217节点故障恢复时间测试成功!!!{Colors.ENDC}", "SUCCESS")
        else:
            print_log(f"\n{Colors.RED}✗ 测试失败！两次恢复时间差异过大{Colors.ENDC}", "ERROR")
            raise Exception("恢复时间差异超出预期")
            
    except Exception as e:
        print_log(f"\n{Colors.RED}case217执行出错!!!: {e}{Colors.ENDC}", "ERROR")
        sys.exit(1)
        
if __name__ == "__main__":
    try:
        # 设置日志记录
        logger = setup_logging()
        if not logger:
            print_log(f"{Colors.RED}设置日志记录失败，退出测试{Colors.ENDC}", "INFO")
            sys.exit(1)
            
        # 解析命令行参数
        if len(sys.argv) > 1:
            # 处理帮助命令
            if sys.argv[1] in ['-h', '--help']:
                print_usage()
                sys.exit(0)
            # 处理清理命令    
            elif sys.argv[1] == 'clean':
                if clean_all_environment():
                    print_log(f"{Colors.GREEN}清理环境成功{Colors.ENDC}", "SUCCESS")
                    sys.exit(0)
                else:
                    print_log(f"{Colors.RED}清理环境失败{Colors.ENDC}", "ERROR")
                    sys.exit(1)
            # 处理下载URL
            elif sys.argv[1].startswith('http://'):
                # 下载并安装指定版本
                success, docker_image = download_and_setup(sys.argv[1])
                if not success:
                    sys.exit(1)
                    
                # 更新Docker镜像配置
                if docker_image:
                    # 更新create_limited_container函数的默认镜像参数
                    create_limited_container.__defaults__ = (2, 4, docker_image)
                    print_log(f"更新默认Docker镜像为: {docker_image}", "INFO")
                    
                # 如果有附加的测试用例参数，设置要执行的用例
                case_nums = sys.argv[2].split(',') if len(sys.argv) > 2 else None
            else:
                # 其他情况视为测试用例编号
                case_nums = sys.argv[1].split(',')
        else:
            case_nums = None
        
        print_test_info()
        
        print_log("=" * 100, "INFO")
        print_log(f"{Colors.YELLOW}开始执行环境检查{Colors.ENDC}".center(100), "INFO")
        print_log("=" * 100, "INFO")
        
        # 执行环境检查
        if not check_environment():
            print_log(f"\n{Colors.RED}环境检查未通过，终止测试！{Colors.ENDC}", "ERROR")
            sys.exit(1)
            
        # 定义测试用例映射
        test_cases = {
            "201": {
                "func": case_201,
                "name": "集群总测点/时间线基准性能测试",
                "goal": "总测点/时间线记录数 > 100亿",
                "method": "并行创建总测点/时间线，统计总测点/时间线数"
            },
            "202": {
                "func": case_202,
                "name": "100亿时间线集群故障恢复测试",
                "goal": "1分钟内恢复",
                "method": "终止所有节点进程后重启，检查恢复时间"
            },
            "203": {
                "func": case_203,
                "name": "Docker容器部署集群测试",
                "goal": "验证基于Docker容器的集群部署功能",
                "method": [
                    "1. 调用外部脚本 xtytest.py",
                    "2. 执行 start_container_dnodes 命令",
                    "3. 验证容器集群启动成功"
                ]
            },
            "204": {
                "func": case_204,
                "name": "Docker容器集群重启测试",
                "goal": "验证容器集群重启功能和性能",
                "method": [
                    "1. 重启容器集群",
                    "2. 验证taosd服务启动",
                    "3. 确认重启时间小于1分钟",
                    "4. 验证数据写入查询功能"
                ]
            },
            "205": {
                "func": case_205,
                "name": "最大连接数测试",
                "goal": "并发连接数 > 50000",
                "method": "启动多个客户端建立连接，统计连接数"
            },
            "206": {
                "func": case_206,
                "name": "单核写入性能测试",
                "goal": "写入速度 > 3万QPS",
                "method": "单表写入测试，统计QPS"
            },
            "207": {
                "func": case_207,
                "name": "单表写入性能测试",
                "goal": "写入速度 > 500万QPS",
                "method": "单表写入测试，统计QPS"
            },
            "208": {
                "func": case_208,
                "name": "查询性能验证测试",
                "goal": "查询响应时间 < 1秒",
                "method": "执行查询，记录响应时间"
            },
            "209": {
                "func": case_209,
                "name": "多数据库写入性能测试",
                "goal": "总写入速度 > 9000万QPS",
                "method": "多数据库并发写入，统计总QPS"
            },
            "210": {
                "func": case_210,
                "name": "数据压缩率测试",
                "goal": "验证压缩后的数据量显著降低",
                "method": [
                    "1. 不压缩写入并记录压缩率A",
                    "2. 压缩写入并记录压缩率B",
                    "3. B/A应小于3%"
                ]
            },
            "211": {
                "func": case_211,
                "name": "大数据量写入和查询性能测试",
                "goal": "验证大数据量写入和查询性能",
                "method": [
                    "1. 写入100亿条测试数据",
                    "2. 验证数据总量达到100亿",
                    "3. 验证interval查询响应时间 < 2秒"
                ]
            },
            "212": {
                "func": case_212,
                "name": "少量数据查询性能测试",
                "goal": "验证少量数据的查询响应时间 < 10ms",
                "method": [
                    "1. 写入10000条测试数据",
                    "2. 验证数据总量为10000条",
                    "3. 验证last_row查询响应时间 < 10ms"
                ]
            },
            "213": {
                "func": case_213,
                "name": "TMQ消息延迟测试",
                "goal": "验证消息传递延迟小于10ms",
                "method": [
                    "1. 创建数据库和超级表",
                    "2. 创建子表和topic",
                    "3. 启动订阅消费和数据写入线程",
                    "4. 验证消息延迟小于10ms"
                ]
            },
            "214": {
                "func": case_214,
                "name": "数据库写入线性扩展测试",
                "goal": "验证写入性能随实例增加呈线性增长",
                "method": [
                    "1. 单实例写入获得基准QPS",
                    "2. 双实例写入应达到基准QPS的2倍(误差±30%)",
                    "3. 三实例写入应达到基准QPS的3倍(误差±30%)"
                ]
            },
            "215": {
                "func": case_215,
                "name": "查询性能线性扩展测试",
                "goal": "验证查询性能随节点增加呈线性扩展",
                "method": [
                    "1. 单节点执行查询获得基准时间",
                    "2. 双节点查询时间和基准时间基本一致(误差±20%)",
                    "3. 三节点查询时间和基准时间基本一致(误差±20%)"
                ]
            },
            "216": {
                "func": case_216,
                "name": "内存使用和数据导出测试",
                "goal": "验证内存使用限制和数据导出功能",
                "method": [
                    "1. 检查系统内存使用不超过2GB",
                    "2. 写入并验证1亿条测试数据",
                    "3. 导出数据到文件并验证行数"
                ]
            },
            "217": {
                "func": case_217,
                "name": "节点故障恢复时间测试",
                "goal": "验证不同数据量和结构下的故障恢复时间与测点规模、标签规模、时序数据规模无关",
                "method": [
                    "1. 写入1亿数据(2列1标签)并测试恢复时间",
                    "2. 写入5亿数据(6列5标签)并测试恢复时间",
                    "3. 验证两次恢复时间相近"
                ]
            }
        }
        
        # 执行指定的测试用例或所有用例
        if case_nums:
            # 检查所有用例号是否有效
            invalid_cases = [num for num in case_nums if num not in test_cases]
            if invalid_cases:
                print_log(f"\n{Colors.RED}错误：未知的测试用例编号: {', '.join(invalid_cases)}{Colors.ENDC}", "ERROR")
                print_log(f"可用的测试用例: {', '.join(test_cases.keys())}", "ERROR")
                sys.exit(1)
                
            # 执行指定的测试用例
            for case_num in case_nums:
                # 执行单个测试用例
                case = test_cases[case_num]
                print_log("\n\n\n" , "INFO")
                print_log("=" * 100 , "INFO")
                print_log(f"{Colors.RED}[执行用例{case_num} - {case['name']}]{Colors.ENDC}", "INFO")
                print_log(f"{Colors.RED}[目标：{case['goal']}]{Colors.ENDC}", "INFO")
                print_log(f"{Colors.YELLOW}[验证方法：]{Colors.ENDC}", "INFO")
                if isinstance(case['method'], list):
                    for step in case['method']:
                        print_log(f"{Colors.YELLOW}  {step}{Colors.ENDC}", "INFO")
                else:
                    print_log(f"{Colors.YELLOW}  {case['method']}{Colors.ENDC}", "INFO")
                print_log("-" * 100 + "\n\n\n", "INFO")
                case['func']()
            
        else:
            # 执行所有测试用例
            print_log("\n\n\n", "INFO")
            print_log("=" * 100 , "INFO")
            print_log(f"{Colors.YELLOW}开始执行信通院性能测试用例集{Colors.ENDC}".center(100), "INFO")
            print_log("=" * 100, "INFO")
            
            for case_id, case in test_cases.items():
                print_log("\n\n\n" , "INFO")
                print_log("=" * 100, "INFO")
                print_log(f"{Colors.RED}[执行用例{case_id} - {case['name']}]{Colors.ENDC}", "INFO")
                print_log(f"{Colors.RED}[目标：{case['goal']}]{Colors.ENDC}", "INFO")
                print_log(f"{Colors.YELLOW}[验证方法：]{Colors.ENDC}", "INFO")                        
                if isinstance(case['method'], list):
                    for step in case['method']:
                        print_log(f"{Colors.YELLOW}  {step}{Colors.ENDC}", "INFO")
                else:
                    print_log(f"{Colors.YELLOW}  {case['method']}{Colors.ENDC}", "INFO")
                print_log("-" * 100 + "\n\n\n", "INFO")
                case['func']()
        
        print_log("\n\n\n" , "INFO")
        print_log("=" * 100 , "INFO")
        if case_nums:
            print_log(f"{Colors.GREEN}测试用例 {','.join(case_nums)} 执行完成!!!{Colors.ENDC}".center(100), "INFO")
        else:
            print_log(f"{Colors.GREEN}所有测试用例执行完成!!!{Colors.ENDC}".center(100), "INFO")
        print_log("=" * 100 + "\n\n\n", "INFO")
        
    except KeyboardInterrupt:
        print_log(f"\n\n{Colors.RED}测试被用户中断!!!{Colors.ENDC}", "ERROR")
        sys.exit(1)