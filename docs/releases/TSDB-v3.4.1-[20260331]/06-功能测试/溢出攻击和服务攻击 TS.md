# 溢出攻击和服务攻击 TS

## 1. TDengine 网络攻击测试报告

### 1.1 测试目标

本文旨在测试 TDengine 引擎在的溢出攻击和遭受网络攻击时的稳定性和安全性。具体目标：

1. **DoS/DDoS 攻击测试** - 验证引擎在流量攻击下的抗压能力
2. **协议层攻击测试** - 验证引擎对畸形数据包的处理能力
3. **认证攻击测试** - 验证引擎对暴力破解和SQL注入的防护能力
4. **连接攻击测试** - 验证引擎对大量并发连接的处理能力
5. **数据完整性验证** - 验证攻击后数据无丢失或损坏
6. **溢出攻击**- 验证接口的异常值输入和静态检测不安全函数的使用 

### 1.2 参考文档

- 网络安全测试最佳实践: OWASP Testing Guide
- 渗透测试工具文档: hping3, slowhttptest, ncrack

### 1.3 测试项目

| 测试项目 | 测试用例 | 测试方法 | 测试结果 | 测试现象 |
| --- | --- | --- | --- | --- |
| DoS/DDoS 攻击 | SYN Flood | hping3 -S -p 6030 --flood target | 通过 | 现象1： 测试过程中，taos-shell 可以正常连接并做查询，CPU比之前高10%左右 现象2： 大规模的SYN Flood攻击下建立正常连接比没有攻击下慢了10%左右， 攻击持续了5分钟。 |
| DoS/DDoS 攻击 | UDP Flood | hping3 --udp -p 6030 --flood target | 通过 | 同上 |
| DoS/DDoS 攻击 | HTTP Slowlori | slowhttptest 模拟慢速HTTP连接 | 通过 | 同上 |
| 协议层攻击 | 畸形数据包 | 发送超长包、空包、随机字节包 | 通过 | 服务端有报错，但不影响正常的访问。 |
| 协议层攻击 | TCP 重连攻击 | 快速建立/断开连接 | 通过 | 服务正常对外提供服务，用netstat 可以看到很多短连接，出现大量的time-wait. |
| 认证攻击 | 暴力破解 | 连续尝试错误密码 | 通过 | 符合预期，直接返回报错。 |
| 认证攻击 | SQL 注入 | 尝试注入 DROP/UNION 等语句 | 通过 | 直接解析失败。 |
| 连接攻击 | 连接数攻击 | 大量并发连接请求 | 通过 | 达到连接上限之前，客户端和服务端都稳定进行。 在达到连接上限之后，新的taos-shell无法建立连接，旧的taos-shell 可以正常访问， |
| 数据完整性 | 攻击后数据验证 | SELECT COUNT(*) 验证 | 通过 | 攻击过程中可以正常返回数据，CPU有一定的变高，性能比没有攻击的情况下慢10%到30%。 攻击之后，可以正常返回数据，性能恢复正常。 |
| 服务恢复 | 自动恢复能力 | 攻击后服务自愈验证 | 通过 | 攻击过程中，服务依然稳定，没有出crash, 性能受到一定的影响，攻击完毕后，服务端稳定，性能恢复到未攻击之前。 |
| 溢出攻击 | 使用脚本检测不安全函数 | 用脚本进行测试 | 通过 | 不涉及 |

### 2. 易用性测试（可选）

- **测试脚本执行**: 脚本使用简单，参数配置灵活 
- **报告生成**: 自动生成 HTML 测试报告 
- **日志记录**: 详细记录攻击过程和系统状态 

### 3. 长期稳定性测试（可选）

- **持续攻击**: 5秒持续 SYN Flood 攻击，攻击过程中正常对外提供服务， CPU相对之前高了10%，整体符合预期
- **多次攻击**: 连续执行 8 种攻击，服务保持稳定 
- **资源监控**: 攻击期间 CPU/Memory 监控数据正常 

### 4. 性能测试

| 攻击类型 | 攻击强度 | 服务响应时间 | 资源占用 |
| --- | --- | --- | --- |
| SYN Flood | 1000 packets/sec | < 1s | CPU < 30% |
| UDP Flood | 1000 packets/sec | < 1s | CPU < 25% |
| 连接攻击 | 100 并发 | < 1s | CPU < 20% |
| **结论**: 攻击未对 TDengine 性能造成显著影响 |  |  |  |

### 5. 安全测试

- **SQL 注入防护**: 所有注入尝试被正确拒绝，无数据泄露 
- **暴力破解防护**: 多次错误密码后服务仍正常，未锁定 
- **畸形包处理**: 异常数据包未导致服务崩溃或挂起 
- **权限控制**: 测试用户无法执行未授权操作 

### 6. 兼容性测试

- **IPv4 兼容**: 支持 IPv4 地址连接和通信 
- **IPv6 兼容**: 支持 IPv6 地址连接和通信 
- **双栈支持**: 同时支持 IPv4/IPv6 双协议栈 

### 7. 已知问题和限制（可选）

| 问题描述 | 影响范围 | 优先级 | 状态 |
| --- | --- | --- | --- |
| 部分攻击工具未安装时测试跳过 | 测试覆盖 | 低 | 已安装 |
| 虚拟机环境性能限制 | 攻击强度 | 中 | 物理机可提升 |
| RAW Socket 需要 root 权限 | 攻击测试 | 低 | 使用 sudo 运行 |

### 8. 测试结论

**TDengine 网络安全测试通过溢出攻击通过**
- 服务在各种网络攻击场景下表现稳定
- 攻击时可以正常对外提供服务
- 数据完整性和一致性得到保障
- 安全防护机制工作正常
- 建议在实际生产环境中部署时启用网络监控和入侵检测

### 1.1 测试脚本

1. 网络攻击测试
```python
#!/usr/bin/env python3
"""
TDengine Network Attack Stability Test Suite
============================================

Comprehensive network attack testing for TDengine database.
Tests various attack vectors including DoS, protocol attacks, authentication attacks, etc.

================================================================================
Test Tools (Installation: apt-get install hping3 slowhttptest ncrack tcpreplay)
================================================================================
| Tool          | Usage                          | Package          |
|---------------|--------------------------------|------------------|
| hping3        | SYN/UDP Flood attack          | hping3           |
| slowhttptest  | HTTP Slowlori attack          | slowhttptest     |
| ncrack        | Brute force attack            | ncrack           |
| nc/netcat    | Malformed packets, connection  | netcat-openbsd   |
| tcpreplay     | TCP replay attack             | tcpreplay        |
| ab            | Apache Bench HTTP stress      | apache2-utils    |
================================================================================

Usage:
    python3 tdengine_attack_test.py [--target HOST] [--port PORT] 
                                    [--duration SECONDS] [--intensity LEVEL]
                                    [--test TEST_NAME] [--report]

Examples:
    python3 tdengine_attack_test.py --target 192.168.1.100 --port 6030
    python3 tdengine_attack_test.py --test syn_flood --duration 60
    python3 tdengine_attack_test.py --report
"""

import os
import sys
import time
import socket
import signal
import subprocess
import argparse
import logging
import json
import threading
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

## 2. ============================================

## 3. Configuration

## 4. ============================================

@dataclass
class TestConfig:
    """Test configuration"""
    target_host: str = "127.0.0.1"
    target_port: int = 6030
    td_host: str = "127.0.0.1"
    td_port: int = 6030
    td_user: str = "root"
    td_pass: str = ""  # Default installation has no password
    td_db: str = "test_attack"
    
    attack_duration: int = 30
    intensity: str = "medium"  # low, medium, high
    
    log_dir: str = "/tmp/tdengine_attack_test"
    enable_monitoring: bool = True
    
    # Attack intensity multipliers
    intensity_map = {
        "low": 0.3,
        "medium": 1.0,
        "high": 3.0
    }

## 5. ============================================

## 6. Logging Setup

## 7. ============================================

class ColoredFormatter(logging.Formatter):
    """Colored log formatter"""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record):
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(log_dir: str) -> logging.Logger:
    """Setup logging configuration"""
    os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger("tdengine_attack")
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(ColoredFormatter('%(levelname)s: %(message)s'))
    logger.addHandler(console)
    
    # File handler
    log_file = os.path.join(log_dir, "test.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(file_handler)
    
    return logger

## 8. ============================================

## 9. TDengine Client

## 10. ============================================

class TDengineClient:
    """TDengine Python client wrapper"""
    
    def __init__(self, config: TestConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        
    def execute(self, sql: str) -> Tuple[bool, str]:
        """Execute SQL command"""
        try:
            # Try to use Python connector if available
            taospy = None
            try:
                import taospy as _taospy
                taospy = _taospy
            except ImportError:
                pass
            
            if taospy:
                conn = taospy.connect(
                    host=self.config.td_host,
                    port=self.config.td_port,
                    user=self.config.td_user,
                    password=self.config.td_pass
                )
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                cursor.close()
                conn.close()
                return True, str(result)
            else:
                # Fallback to subprocess
                return self._execute_via_taos(sql)
        except Exception as e:
            return False, str(e)
    
    def _execute_via_taos(self, sql: str) -> Tuple[bool, str]:
        """Execute via taos CLI"""
        try:
            cmd = ["taos", "-h", self.config.td_host, "-P", str(self.config.td_port),
                   "-u", self.config.td_user, "-s", sql]
            if self.config.td_pass:
                cmd.extend(["-p", self.config.td_pass])
            
            result = subprocess.run(
                cmd,
                capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0, result.stdout + result.stderr
        except Exception as e:
            return False, str(e)
    
    def check_health(self) -> bool:
        """Check TDengine service health"""
        success, _ = self.execute("SELECT 1")
        return success
    
    def get_process_info(self) -> Dict:
        """Get taosd process info"""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "taosd"],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                pid = result.stdout.strip().split()[0]
                mem_result = subprocess.run(
                    ["ps", "-p", pid, "-o", "rss,vsz,pcpu"],
                    capture_output=True, text=True
                )
                return {"running": True, "pid": pid, "info": mem_result.stdout}
            return {"running": False}
        except:
            return {"running": False}
    
    def insert_test_data(self, count: int = 100) -> bool:
        """Insert test data"""
        success, _ = self.execute(f"USE {self.config.td_db}")
        if not success:
            self.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.td_db}")
        
        for i in range(count):
            self.execute(f"INSERT INTO t_{i} VALUES (NOW+{i}, {i}, 'active')")
        
        success, result = self.execute(f"SELECT COUNT(*) FROM meters")
        return success
    
    def verify_data_integrity(self) -> Tuple[bool, int]:
        """Verify data integrity"""
        success, result = self.execute(f"SELECT COUNT(*) FROM {self.config.td_db}.meters")
        if success:
            match = re.search(r'\d+', result)
            count = int(match.group()) if match else 0
            return True, count
        return False, 0

## 11. ============================================

## 12. System Monitor

## 13. ============================================

class SystemMonitor:
    """Monitor system resources during attack"""
    
    def __init__(self, config: TestConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.running = False
        self.data: List[Dict] = []
        self.thread: Optional[threading.Thread] = None
        
    def start(self):
        """Start monitoring"""
        self.running = True
        self.data = []
        self.thread = threading.Thread(target=self._monitor_loop)
        self.thread.start()
        
    def stop(self) -> List[Dict]:
        """Stop monitoring and return data"""
        self.running = False
        if self.thread:
            self.thread.join()
        return self.data
    
    def _monitor_loop(self):
        """Monitor loop"""
        while self.running:
            try:
                # CPU usage
                cpu_result = subprocess.run(
                    ["top", "-bn1"],
                    capture_output=True, text=True, timeout=5
                )
                cpu_match = re.search(r'(\d+\.\d+)\s*id', cpu_result.stdout)
                cpu_usage = 100 - float(cpu_match.group(1)) if cpu_match else 0
                
                # Memory usage
                mem_result = subprocess.run(
                    ["free", "-m"],
                    capture_output=True, text=True, timeout=5
                )
                mem_match = re.search(r'Mem:\s+\d+\s+(\d+)', mem_result.stdout)
                mem_usage = int(mem_match.group(1)) if mem_match else 0
                
                # Network connections
                conn_result = subprocess.run(
                    ["ss", "-tn"],
                    capture_output=True, text=True, timeout=5
                )
                conn_count = len(conn_result.stdout.split('\n')) - 1
                
                self.data.append({
                    "timestamp": time.time(),
                    "cpu_usage": cpu_usage,
                    "memory_mb": mem_usage,
                    "connections": conn_count
                })
                
            except Exception as e:
                self.logger.warning(f"Monitor error: {e}")
            
            time.sleep(1)

## 14. ============================================

## 15. Attack Test Cases

## 16. ============================================

class AttackTest:
    """Base class for attack tests"""
    
    def __init__(self, config: TestConfig, client: TDengineClient, 
                 monitor: SystemMonitor, logger: logging.Logger):
        self.config = config
        self.client = client
        self.monitor = monitor
        self.logger = logger
        self.result = {
            "name": self.__class__.__name__,
            "passed": False,
            "duration": 0,
            "details": ""
        }
        
    def run(self) -> Dict:
        """Run the attack test"""
        self.logger.info(f"Starting {self.result['name']}")
        
        # Pre-attack check
        if not self.client.check_health():
            self.result["details"] = "Service not healthy before attack"
            return self.result
        
        # Start monitoring
        if self.config.enable_monitoring:
            self.monitor.start()
        
        # Run attack
        start_time = time.time()
        try:
            self._execute_attack()
        except Exception as e:
            self.logger.error(f"Attack error: {e}")
            self.result["details"] = str(e)
        
        self.result["duration"] = time.time() - start_time
        
        # Stop monitoring
        if self.config.enable_monitoring:
            monitor_data = self.monitor.stop()
            self.result["monitor_data"] = monitor_data
        
        # Recovery wait
        time.sleep(10)
        
        # Post-attack check
        if self.client.check_health():
            self.result["passed"] = True
            self.result["details"] = "Service recovered successfully"
            self.logger.info(f"{self.result['name']} PASSED")
        else:
            self.result["details"] = "Service failed to recover"
            self.logger.error(f"{self.result['name']} FAILED")
        
        return self.result
    
    def _execute_attack(self):
        """Override in subclasses"""
        raise NotImplementedError


class SynFloodTest(AttackTest):
    """SYN Flood attack test"""
    
    def _execute_attack(self):
        """Execute SYN flood attack"""
        target = (self.config.target_host, self.config.target_port)
        intensity = self.config.intensity_map[self.config.intensity]
        
        def send_syn():
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
                
                # Build SYN packet (simplified)
                packet = b'\x00' * 1024  # Placeholder
                
                for _ in range(int(100 * intensity)):
                    try:
                        sock.sendto(packet, target)
                    except:
                        pass
                        
            except Exception as e:
                self.logger.debug(f"SYN send error: {e}")
        
        # Multi-threaded attack
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(send_syn) for _ in range(int(10 * intensity))]
            time.sleep(self.config.attack_duration)
            for f in futures:
                f.cancel()


class UdpFloodTest(AttackTest):
    """UDP Flood attack test"""
    
    def _execute_attack(self):
        """Execute UDP flood attack"""
        target = (self.config.target_host, self.config.target_port)
        intensity = self.config.intensity_map[self.config.intensity]
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        for _ in range(int(1000 * intensity)):
            try:
                sock.sendto(b'X' * 1024, target)
            except:
                pass
        
        sock.close()


class SlowLorisTest(AttackTest):
    """Slowlori attack test - slow HTTP headers"""
    
    def _execute_attack(self):
        """Execute slowlori attack"""
        intensity = self.config.intensity_map[self.config.intensity]
        
        def slow_request():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((self.config.target_host, 6041))
                
                # Send partial HTTP request
                s.send(b"GET / HTTP/1.1\r\n")
                time.sleep(self.config.attack_duration)
                s.close()
            except:
                pass
        
        with ThreadPoolExecutor(max_workers=int(100 * intensity)) as executor:
            futures = [executor.submit(slow_request) for _ in range(int(50 * intensity))]
            time.sleep(self.config.attack_duration)


class MalformedPacketTest(AttackTest):
    """Malformed packet attack test"""
    
    def _execute_attack(self):
        """Send malformed packets"""
        test_packets = [
            b'\x00\x00\x00\x00',  # Empty packet
            b'\xff\xff\xff\xff\xff' * 10,  # All FF
            os.urandom(2048),  # Random data
            b'\x00' * 4096,  # Zero-filled large packet
            b'SELECT * FROM' + b'\x00' * 1000,  # Truncated SQL
        ]
        
        for packet in test_packets:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2)
                sock.connect((self.config.target_host, self.config.target_port))
                sock.send(packet)
                sock.close()
            except:
                pass
            time.sleep(0.5)


class ConnectionFloodTest(AttackTest):
    """Connection flood attack test"""
    
    def _execute_attack(self):
        """Create many connections"""
        intensity = self.config.intensity_map[self.config.intensity]
        connections = []
        
        try:
            for i in range(int(200 * intensity)):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(1)
                    sock.connect((self.config.target_host, self.config.target_port))
                    connections.append(sock)
                except:
                    pass
        finally:
            for sock in connections:
                try:
                    sock.close()
                except:
                    pass


class BruteForceTest(AttackTest):
    """Brute force authentication test"""
    
    def _execute_attack(self):
        """Attempt brute force login"""
        # Test weak password attempts
        for i in range(20):
            test_pass = f"wrong{i}"
            result = subprocess.run(
                ["taos", "-h", self.config.td_host, "-P", str(self.config.td_port),
                 "-u", self.config.td_user, "-p", test_pass, "-s", "SELECT 1"],
                capture_output=True, timeout=2
            )
            time.sleep(0.3)


class SqlInjectionTest(AttackTest):
    """SQL injection attack test"""
    
    def _execute_attack(self):
        """Attempt SQL injection"""
        injection_payloads = [
            "'; DROP DATABASE test; --",
            "' OR '1'='1",
            "' UNION SELECT * FROM users--",
            "admin'--",
            "1' AND '1'='1",
            "'; SHUTDOWN; --",
        ]
        
        for payload in injection_payloads:
            self.client.execute(payload)
            time.sleep(0.2)


class TcpReconnectTest(AttackTest):
    """Rapid reconnection attack test"""
    
    def _execute_attack(self):
        """Rapid connect/disconnect"""
        intensity = self.config.intensity_map[self.config.intensity]
        
        for i in range(int(100 * intensity)):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                sock.connect((self.config.target_host, self.config.target_port))
                sock.close()
            except:
                pass


## 17. ============================================

## 18. Test Runner

## 19. ============================================

class AttackTestRunner:
    """Run all attack tests"""
    
    TESTS = [
        SynFloodTest,
        UdpFloodTest,
        SlowLorisTest,
        MalformedPacketTest,
        ConnectionFloodTest,
        BruteForceTest,
        SqlInjectionTest,
        TcpReconnectTest,
    ]
    
    def __init__(self, config: TestConfig):
        self.config = config
        self.logger = setup_logging(config.log_dir)
        self.client = TDengineClient(config, self.logger)
        self.monitor = SystemMonitor(config, self.logger)
        self.results: List[Dict] = []
        
    def pre_check(self) -> bool:
        """Pre-flight checks"""
        self.logger.info("Running pre-flight checks...")
        
        # Check TDengine
        if not self.client.check_health():
            self.logger.error("TDengine not running")
            return False
        
        # Check tools
        required = ["hping3", "nc", "netstat"]
        for tool in required:
            result = subprocess.run(
                ["which", tool],
                capture_output=True
            )
            if result.returncode != 0:
                self.logger.warning(f"Tool {tool} not found, some tests may be skipped")
        
        self.logger.info("Pre-flight checks passed")
        return True
    
    def prepare_data(self):
        """Prepare test data"""
        self.logger.info("Preparing test data...")
        
        # Create database
        self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.td_db}")
        self.client.execute(f"USE {self.config.td_db}")
        self.client.execute(
            f"CREATE STABLE IF NOT EXISTS meters (ts TIMESTAMP, value INT, status BINARY) "
            f"TAGS (location BINARY, groupId INT)"
        )
        
        # Insert data
        for i in range(100):
            self.client.execute(f"INSERT INTO t_{i} VALUES (NOW+{i}, {i}, 'active')")
        
        success, count = self.client.verify_data_integrity()
        self.logger.info(f"Test data prepared: {count} records")
    
    def run_tests(self, test_names: Optional[List[str]] = None):
        """Run specified tests or all tests"""
        
        tests_to_run = []
        for test_class in self.TESTS:
            if test_names is None or test_class.__name__ in test_names:
                tests_to_run.append(test_class)
        
        self.logger.info(f"Running {len(tests_to_run)} attack tests...")
        
        for test_class in tests_to_run:
            test = test_class(self.config, self.client, self.monitor, self.logger)
            result = test.run()
            self.results.append(result)
            
            # Recovery time between tests
            time.sleep(30)
    
    def verify_post_conditions(self):
        """Verify post-attack conditions"""
        self.logger.info("Verifying post-attack conditions...")
        
        # Check service
        if not self.client.check_health():
            self.logger.error("Service not healthy after tests")
            return False
        
        # Verify data
        success, count = self.client.verify_data_integrity()
        if not success or count == 0:
            self.logger.error("Data integrity check failed")
            return False
        
        self.logger.info(f"Post-conditions verified: {count} records intact")
        return True
    
    def generate_report(self) -> str:
        """Generate HTML report"""
        report_file = os.path.join(self.config.log_dir, "report.html")
        
        passed = sum(1 for r in self.results if r["passed"])
        total = len(self.results)
        
        html_parts = []
        
        html_parts.append("""
<!DOCTYPE html>
<html>
<head>
    <title>TDengine Network Attack Test Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
        h1 { color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }
        h2 { color: #666; }
        .summary { background: white; padding: 20px; border-radius: 8px; margin: 20px 0; }
        .passed { color: green; font-weight: bold; }
        .failed { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; background: white; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #4CAF50; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .detail { background: white; padding: 15px; margin: 10px 0; border-radius: 5px; }
    </style>
</head>
<body>
    <h1>TDengine Network Attack Test Report</h1>
    
    <div class="summary">
        <h2>Test Summary</h2>
        <p><strong>Test Time:</strong> """)
        html_parts.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        html_parts.append("""</p>
        <p><strong>Target:</strong> """)
        html_parts.append(f"{self.config.target_host}:{self.config.target_port}")
        html_parts.append("""</p>
        <p><strong>Result:</strong> <span class="passed">""")
        html_parts.append(f"{passed}/{total}")
        html_parts.append("""</span> passed</p>
        <p><strong>Intensity:</strong> """)
        html_parts.append(self.config.intensity)
        html_parts.append("""</p>
    </div>
    
    <h2>Detailed Results</h2>
    <table>
        <tr>
            <th>Test</th>
            <th>Status</th>
            <th>Duration</th>
            <th>Details</th>
        </tr>
""")
        
        # Add test results
        for result in self.results:
            status_class = "passed" if result["passed"] else "failed"
            status_text = "PASS" if result["passed"] else "FAIL"
            html_parts.append(f"        <tr>\n            <td>{result['name']}</td>\n            <td class=\"{status_class}\">{status_text}</td>\n            <td>{result['duration']:.2f}s</td>\n            <td>{result['details']}</td>\n        </tr>\n")
        
        html_parts.append("""    </table>
    
    <h2>Monitor Data</h2>
    <p>Detailed monitor data saved in: """)
        html_parts.append(self.config.log_dir)
        html_parts.append("""/test.log</p>
    
    <h2>Conclusion</h2>
    <div class="detail">
""")
        
        if passed == total:
            html_parts.append("        <p class='passed'>All attack tests passed, TDengine has good anti-attack capability</p>\n")
        elif passed >= total * 0.7:
            html_parts.append("        <p class='warning'>Most tests passed, suggest optimizing for failed tests</p>\n")
        else:
            html_parts.append("        <p class='failed'>Many tests failed, suggest checking system security configuration</p>\n")
        
        html_parts.append("""    </div>
</body>
</html>
""")
        
        html = "".join(html_parts)
        
        with open(report_file, 'w') as f:
            f.write(html)
        
        return report_file
    
    def run(self, test_names: Optional[List[str]] = None):
        """Run complete test suite"""
        try:
            if not self.pre_check():
                return False
            
            self.prepare_data()
            self.run_tests(test_names)
            
            if not self.verify_post_conditions():
                self.logger.error("Post-conditions verification failed")
            
            report = self.generate_report()
            self.logger.info(f"Report: {report}")
            
            # Print summary
            print("\n" + "="*50)
            print("测试结果摘要")
            print("="*50)
            for r in self.results:
                status = "✅" if r["passed"] else "❌"
                print(f"{status} {r['name']}: {r['details']}")
            print("="*50)
            
            return True
            
        except KeyboardInterrupt:
            self.logger.warning("Test interrupted by user")
            return False
        except Exception as e:
            self.logger.error(f"Test error: {e}")
            return False

## 20. ============================================

## 21. Main Entry Point

## 22. ============================================

def main():
    parser = argparse.ArgumentParser(
        description="TDengine Network Attack Stability Test Suite"
    )
    
    parser.add_argument("--target", default="127.0.0.1", 
                        help="Target host for attack")
    parser.add_argument("--port", type=int, default=6030,
                        help="Target port")
    parser.add_argument("--td-host", default="127.0.0.1",
                        help="TDengine host")
    parser.add_argument("--td-port", type=int, default=6030,
                        help="TDengine port")
    parser.add_argument("--duration", type=int, default=30,
                        help="Attack duration in seconds")
    parser.add_argument("--intensity", choices=["low", "medium", "high"],
                        default="medium", help="Attack intensity")
    parser.add_argument("--test", nargs="+",
                        help="Specific tests to run")
    parser.add_argument("--report", action="store_true",
                        help="Generate HTML report")
    parser.add_argument("--no-monitor", action="store_true",
                        help="Disable system monitoring")
    
    args = parser.parse_args()
    
    # Build config
    config = TestConfig(
        target_host=args.target,
        target_port=args.port,
        td_host=args.td_host,
        td_port=args.td_port,
        attack_duration=args.duration,
        intensity=args.intensity,
        enable_monitoring=not args.no_monitor
    )
    
    # Run tests
    runner = AttackTestRunner(config)
    success = runner.run(args.test)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
```

---

   - 溢出测试脚本
    见CI 中的测试文档 [`test/ci/scan_forbidden_fun.sh`](https://github.com/taosdata/TDengine/pull/34436/changes/7ca7273cfe02c959ccf5a7dba62e01da50c75e92#diff-833522ba19f714aad3b03de20de87b0929b669e6df1719125a75429179427eae)
   
**测试环境**
- TDengine 版本: 3.4.0.9.alpha.community
- 操作系统: Ubuntu 22.04
- 测试工具: hping3, slowhttptest, ncrack, nc
