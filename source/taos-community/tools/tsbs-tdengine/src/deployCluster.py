#
# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#



from logging import log
import sys
import os
import getopt
import socket
import time
import subprocess
import shutil
from datetime import datetime
from typing import Dict, List, Optional
from baseStep import BaseStep
from outLog import log

import taos

"""
TDengine Standalone Cluster Setup Module

This module provides functionality to create and start a TDengine cluster
with specified number of DNODEs, MNODEs, SNODEs without depending on frame modules.

Usage:
    python3 createCluster.py -N 3 -M 2 -S 2
    python3 createCluster.py --dnodes 3 --mnodes 2 --snodes 2 --path /tmp/tdengine
"""




# ==================== Logging Module ====================
class Logger:
    """Simple logger for cluster setup"""
    
    def __init__(self, log_file: Optional[str] = None):
        self.log_file = log_file
        
    def _log(self, level: str, msg: str):
        """Internal log method"""
        log.out(f"{level}: {msg}")
        
        if self.log_file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            log_msg = f"{timestamp} [{level}] {msg}"            
            with open(self.log_file, 'a') as f:
                f.write(log_msg + '\n')
                
    def info(self, msg: str):
        """Log info message"""
        self._log("INFO", msg)
        
    def debug(self, msg: str):
        """Log debug message"""
        self._log("DEBUG", msg)
        
    def notice(self, msg: str):
        """Log notice message"""
        self._log("NOTICE", msg)
        
    def success(self, msg: str):
        """Log success message"""
        self._log("SUCCESS", msg)
        
    def error(self, msg: str):
        """Log error message"""
        self._log("ERROR", msg)
        
    def exit(self, msg: str, exit_code: int = 1):
        """Log error and exit"""
        self.error(msg)
        sys.exit(exit_code)


# ==================== DNode Configuration ====================
class DNodeConfig:
    """Configuration for a single DNode"""
    
    def __init__(self, index: int, fqdn: str, first_ep: str, second_ep: str, 
                 base_port: int = 6030, base_path: str = "/tmp/tdengine",
                 level: int = 1, disk: int = 1):
        self.index = index
        self.fqdn = fqdn
        self.first_ep = first_ep
        self.second_ep = second_ep
        self.port = base_port + (index - 1) * 100
        self.base_path = base_path
        self.level = level
        self.disk = disk
        
        # Paths
        self.cfg_dir = os.path.join(self.base_path, f"dnode{index}", "cfg")
        self.data_dir = os.path.join(self.base_path, f"dnode{index}", "data")
        self.log_dir = os.path.join(self.base_path, f"dnode{index}", "log")
        self.cfg_file = os.path.join(self.cfg_dir, "taos.cfg")
        
    def create_directories(self):
        """Create necessary directories"""
        for dir_path in [self.cfg_dir, self.log_dir]:
            os.makedirs(dir_path, exist_ok=True)
            
        # Create multi-level data directories
        if self.level == 1 and self.disk == 1:
            # Use simple 'data' directory for default configuration
            data_path = os.path.join(self.base_path, f"dnode{self.index}", "data")
            os.makedirs(data_path, exist_ok=True)
        else:
            # Use multi-level naming for custom configurations
            for lvl in range(self.level):
                for dsk in range(self.disk):
                    data_path = os.path.join(self.base_path, f"dnode{self.index}", f"data{lvl}{dsk}")
                    os.makedirs(data_path, exist_ok=True)
            
    def generate_config(self, update_dict: Optional[Dict] = None) -> str:
        """Generate taos.cfg configuration"""
        config_lines = [
            f"fqdn {self.fqdn}",
            "monitor 0",
            "maxShellConns 30000",
            "locale en_US.UTF-8",
            "charset UTF-8",
            "asyncLog 0",
            "DebugFlag 135",
            "numOfLogLines 100000000",
            "statusInterval 1",
            "enableQueryHb 1",
            "supportVnodes 1024",
            "telemetryReporting 0",
            f"firstEp {self.first_ep}",
            f"serverPort {self.port}",
            f"secondEp {self.second_ep}",
        ]
        
        # Add data directory configuration
        if self.level == 1 and self.disk == 1:
            # Use simple 'data' directory for default configuration
            data_path = os.path.join(self.base_path, f"dnode{self.index}", "data")
            config_lines.append(f"dataDir {data_path}")
        else:
            # Add multi-level data directory configuration with proper format
            # Format: dataDir {path} {level} {is_primary}
            # First disk (disk 0) of each level is primary
            is_primary = 1
            for lvl in range(self.level):
                for dsk in range(self.disk):
                    data_path = os.path.join(self.base_path, f"dnode{self.index}", f"data{lvl}{dsk}")
                    config_lines.append(f"dataDir {data_path} {lvl} {is_primary}")
                    is_primary = 0
        
        config_lines.append(f"logDir {self.log_dir}")
        
        # Add custom configurations
        if update_dict:
            for key, value in update_dict.items():
                config_lines.append(f"{key} {value}")
                
        return '\n'.join(config_lines) + '\n'  # Add trailing newline
        
    def write_config(self, update_dict: Optional[Dict] = None):
        """Write configuration file"""
        config_content = self.generate_config(update_dict)
        with open(self.cfg_file, 'w') as f:
            f.write(config_content)


# ==================== TaosAdapter Manager ====================
class TaosAdapterManager:
    """Manager for TaosAdapter operations - only used when -B flag is set"""
    
    def __init__(self, base_path: str, logger: Optional[Logger] = None):
        self.base_path = base_path
        self.logger = logger or Logger()
        self.adapter_path: Optional[str] = None
        self.process: Optional[subprocess.Popen] = None
        
    def _find_taosadapter(self) -> str:
        """Find taosadapter executable"""
        possible_paths = [
            "/usr/bin/taosadapter",
            "/usr/local/bin/taosadapter",
            os.path.expanduser("~/TDinternal/debug/build/bin/taosadapter"),
            "./build/bin/taosadapter",
        ]
        
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return path
                
        result = subprocess.run(['which', 'taosadapter'], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
            
        raise FileNotFoundError("taosadapter executable not found")
        
    def start(self) -> bool:
        """Start taosadapter"""
        # Lazy loading: only find taosadapter when actually needed
        if self.adapter_path is None:
            try:
                self.adapter_path = self._find_taosadapter()
            except FileNotFoundError as e:
                self.logger.error(str(e))
                return False
        
        if self.process and self.process.poll() is None:
            self.logger.info("TaosAdapter is already running")
            return True
            
        try:
            cmd = f"nohup {self.adapter_path} > {self.base_path}/taosadapter.log 2>&1 &"
            self.logger.info(f"Starting TaosAdapter: {cmd}")
            
            self.process = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            time.sleep(2)
            
            # Check if running on default port 6041
            if self._is_adapter_running():
                self.logger.success("TaosAdapter started successfully on port 6041")
                return True
            else:
                self.logger.error("TaosAdapter failed to start")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to start TaosAdapter: {e}")
            return False
            
    def _is_adapter_running(self) -> bool:
        """Check if TaosAdapter is running"""
        cmd = "lsof -i tcp:6041 | grep LISTEN"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0
        
    def stop(self):
        """Stop taosadapter"""
        subprocess.run("pkill -9 taosadapter", shell=True, capture_output=True)
        self.logger.info("TaosAdapter stopped")


# ==================== DNode Manager ====================
class DNodeManager:
    """Manager for DNode operations"""
    
    def __init__(self, fqdn: str, base_path: str, level: int = 1, disk: int = 1,
                 taosd_path: Optional[str] = None, logger: Optional[Logger] = None):
        self.fqdn = fqdn
        self.base_path = base_path
        self.level = level
        self.disk = disk
        self.taosd_path = taosd_path or self._find_taosd()
        self.logger = logger or Logger()
        self.dnodes: List[DNodeConfig] = []
        self.processes: Dict[int, subprocess.Popen] = {}
        
    def _find_taosd(self) -> str:
        """Find taosd executable"""
        possible_paths = [
            "/usr/bin/taosd",
            "/usr/local/bin/taosd",
            os.path.expanduser("~/TDinternal/debug/build/bin/taosd"),
            "./build/bin/taosd",
        ]
        
        for path in possible_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return path
                
        result = subprocess.run(['which', 'taosd'], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip()
            
        raise FileNotFoundError("taosd executable not found")
        
    def create_dnode(self, index: int, first_ep: str, second_ep: str, 
                     update_dict: Optional[Dict] = None) -> DNodeConfig:
        """Create a DNode configuration"""
        dnode = DNodeConfig(
            index=index,
            fqdn=self.fqdn,
            first_ep=first_ep,
            second_ep=second_ep,
            base_path=self.base_path,
            level=self.level,
            disk=self.disk
        )
        
        dnode.create_directories()
        dnode.write_config(update_dict)
        
        self.dnodes.append(dnode)
        self.logger.info(f"DNode {index} configured at {dnode.cfg_dir}")
        self.logger.info(f"  Data levels: {self.level}, Disks per level: {self.disk}")
        
        return dnode
        
    def start_dnode(self, index: int) -> bool:
        """Start a specific DNode"""
        dnode = next((d for d in self.dnodes if d.index == index), None)
        if not dnode:
            self.logger.error(f"DNode {index} not found")
            return False
            
        # Check if DNode is already running
        if index in self.processes:
            if self.processes[index].poll() is None:
                self.logger.info(f"DNode {index} is already running")
                return True
        
        # Start DNode with nohup to run in background
        cmd = f"setsid nohup {self.taosd_path} -c {dnode.cfg_dir} > /dev/null 2>&1 &"
        self.logger.info(f"Starting DNode {index}: {cmd}")
        
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid  # create new session
        )
        
        self.processes[index] = process
        time.sleep(1)
        
        # Verify DNode started successfully
        if self._is_dnode_running(dnode.port):
            self.logger.success(f"DNode {index} started successfully on port {dnode.port}")
            return True
        else:
            self.logger.error(f"DNode {index} failed to start")
            return False
            
    def _is_dnode_running(self, port: int) -> bool:
        """Check if DNode is running on specified port"""
        cmd = f"lsof -i tcp:{port} | grep LISTEN"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.returncode == 0
        
    def stop_dnode(self, index: int):
        """Stop a specific DNode"""
        if index in self.processes:
            process = self.processes[index]
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
                self.logger.info(f"DNode {index} stopped")
                
    def stop_all(self):
        """Stop all DNODEs"""
        self.logger.info("Stopping all DNODEs...")
        subprocess.run("pkill -9 taosd", shell=True, capture_output=True)
        
        # Loop to verify all processes are stopped
        max_wait_time = 10  # Maximum wait time in seconds
        check_interval = 1  # Check every 1 second
        elapsed_time = 0
        
        while elapsed_time < max_wait_time:
            time.sleep(check_interval)

            # Check if any taosd process is still running
            result = subprocess.run(
                "echo `pidof taosd`",
                shell=True,
                capture_output=True,
                text=True
            )            
            if not result.stdout.strip():
                # No taosd processes found
                self.logger.success(f"All DNODEs stopped successfully (took {elapsed_time:.1f}s)")
                self.processes.clear()
                return
            else:
                print(f"Still running taosd pid:{result.stdout.strip()}")
            
            # Still running, wait and retry
            elapsed_time += check_interval
            
            if elapsed_time % 2 == 0:  # Log every 2 seconds
                self.logger.debug(f"Waiting for DNODEs to stop... ({elapsed_time:.0f}s)")
        
        # Timeout - force kill again and warn
        self.logger.error(f"Some DNODEs did not stop within {max_wait_time}s, forcing shutdown...")
        subprocess.run("pkill -9 taosd", shell=True, capture_output=True)
        time.sleep(1)
        self.processes.clear()
        self.logger.info("All DNODEs stopped (forced)")
        
    def clean_data(self):
        """Clean all data directories"""
        self.logger.info("Cleaning data directories...")
        if os.path.exists(self.base_path):
            shutil.rmtree(self.base_path)
            self.logger.info(f"Removed {self.base_path}")


# ==================== Cluster Manager ====================
class ClusterManager:
    """Manager for TDengine cluster operations"""
    
    def __init__(self, fqdn: str, base_path: str, level: int = 1, disk: int = 1,
                 logger: Optional[Logger] = None):
        self.fqdn = fqdn
        self.base_path = base_path
        self.level = level
        self.disk = disk
        self.logger = logger or Logger()
        self.dnode_manager = DNodeManager(fqdn, base_path, level, disk, logger=self.logger)
        self.adapter_manager: Optional[TaosAdapterManager] = None  # Lazy initialization
        self.conn: Optional[taos.TaosConnection] = None
        
    def _ensure_adapter_manager(self):
        """Ensure adapter manager is initialized (only when needed)"""
        if self.adapter_manager is None:
            self.adapter_manager = TaosAdapterManager(self.base_path, logger=self.logger)
        
    def create_cluster(self, dnode_nums: int, mnode_nums: int, 
                      update_dict: Optional[Dict] = None,
                      use_previous: bool = False) -> bool:
        """Create a cluster with specified configuration"""
        
        if dnode_nums < 1:
            self.logger.exit("At least 1 DNode is required")
            
        if mnode_nums > dnode_nums:
            self.logger.exit(f"MNode count ({mnode_nums}) cannot exceed DNode count ({dnode_nums})")
            
        if use_previous:
            self.logger.info("Using previous cluster, skipping creation")
            return True
            
        self.logger.info(f"Creating cluster: {dnode_nums} DNODEs, {mnode_nums} MNODEs")
        self.logger.info(f"Storage: {self.level} levels, {self.disk} disks per level")
        
        self.dnode_manager.stop_all()
        # clear self.base_path
        self.dnode_manager.clean_data()
        
        first_ep = f"{self.fqdn}:6030"
        second_ep = f"{self.fqdn}:6130" if dnode_nums > 1 else first_ep
        
        for i in range(1, dnode_nums + 1):
            self.dnode_manager.create_dnode(
                index=i,
                first_ep=first_ep,
                second_ep=second_ep,
                update_dict=update_dict
            )
            
        for i in range(1, dnode_nums + 1):
            if not self.dnode_manager.start_dnode(i):
                self.logger.exit(f"Failed to start DNode {i}")
                
        time.sleep(2)
        return True
        
    def connect(self, timeout: int = 10) -> taos.TaosConnection:
        """Connect to the cluster"""
        cfg_path = os.path.join(self.base_path, "dnode1", "cfg")
        
        self.logger.info(f"Connecting to {self.fqdn} with config {cfg_path}")
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self.conn = taos.connect(
                    host=self.fqdn,
                    config=cfg_path
                )
                self.logger.success("Connected to cluster")
                return self.conn
            except Exception as e:
                self.logger.debug(f"Connection attempt failed: {e}")
                time.sleep(1)
                
        self.logger.exit("Failed to connect to cluster")
        
    def create_dnodes_in_cluster(self, dnode_nums: int):
        """Create DNODEs in cluster (execute SQL)"""
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        
        for i in range(2, dnode_nums + 1):
            port = 6030 + (i - 1) * 100
            endpoint = f"{self.fqdn}:{port}"
            
            try:
                sql = f"CREATE DNODE '{endpoint}'"
                self.logger.info(f"Executing: {sql}")
                cursor.execute(sql)
                time.sleep(1)
            except Exception as e:
                self.logger.notice(f"Create DNode {i} warning: {e}")
                
        cursor.close()
        
    def create_mnodes_in_cluster(self, mnode_nums: int):
        """Create MNODEs in cluster"""
        if not self.conn:
            self.connect()
            
        if mnode_nums <= 1:
            self.logger.info("Only 1 MNode needed (default on DNode 1)")
            return
            
        cursor = self.conn.cursor()
        
        for i in range(2, mnode_nums + 1):
            try:
                sql = f"CREATE MNODE ON DNODE {i}"
                self.logger.info(f"Executing: {sql}")
                cursor.execute(sql)
                time.sleep(2)
            except Exception as e:
                self.logger.notice(f"Create MNode {i} warning: {e}")
                
        cursor.close()
        
    def create_snodes_in_cluster(self, snode_nums: int):
        """Create SNODEs in cluster"""
        if not self.conn:
            self.connect()
            
        if snode_nums <= 0:
            self.logger.info("No SNODEs to create")
            return
            
        self.logger.info(f"Creating {snode_nums} SNODEs...")
        cursor = self.conn.cursor()
        
        for i in range(1, snode_nums + 1):
            try:
                sql = f"CREATE SNODE ON DNODE {i}"
                self.logger.info(f"Executing: {sql}")
                cursor.execute(sql)
                time.sleep(2)
            except Exception as e:
                self.logger.notice(f"Create SNode {i} warning: {e}")
                
        cursor.close()
        
    def start_taosadapter(self) -> bool:
        """Start TaosAdapter - only called when -B flag is used"""
        self._ensure_adapter_manager()
        return self.adapter_manager.start()
        
    def check_cluster_status(self, check_snodes: bool = False) -> bool:
        """Check cluster status"""
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        
        try:
            # Check DNODEs
            cursor.execute("SHOW DNODES")
            dnodes = cursor.fetchall()
            
            self.logger.info(f"\nCluster Status - DNODEs ({len(dnodes)}):")
            for dnode in dnodes:
                status = "✓" if dnode[4] == "ready" else "✗"
                self.logger.info(f"  {status} DNode {dnode[0]}: {dnode[1]} - {dnode[4]}")
                
            # Check MNODEs
            cursor.execute("SHOW MNODES")
            mnodes = cursor.fetchall()
            
            self.logger.info(f"\nCluster Status - MNODEs ({len(mnodes)}):")
            for mnode in mnodes:
                status = "✓" if mnode[3] == "ready" else "✗"
                role_icon = "★" if mnode[2] == "leader" else "☆"
                self.logger.info(f"  {status} MNode {mnode[0]}: {mnode[1]} - {role_icon} {mnode[2]} - {mnode[3]}")
                
            # Check SNODEs if requested
            if check_snodes:
                try:
                    cursor.execute("SHOW SNODES")
                    snodes = cursor.fetchall()
                    
                    self.logger.info(f"\nCluster Status - SNODEs ({len(snodes)}):")
                    for snode in snodes:
                        self.logger.info(f"  ✓ SNode {snode[0]}: {snode[1]} - ReplicaId: {snode[3]}")
                except Exception as e:
                    self.logger.debug(f"No SNODEs or query failed: {e}")
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to check cluster status: {e}")
            return False
        finally:
            cursor.close()
            
    def wait_for_cluster_ready(self, timeout: int = 30) -> bool:
        """Wait for all DNODEs to be ready"""
        if not self.conn:
            self.connect()
            
        cursor = self.conn.cursor()
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                cursor.execute("SHOW DNODES")
                dnodes = cursor.fetchall()
                
                all_ready = all(dnode[4] == "ready" for dnode in dnodes)
                
                if all_ready:
                    self.logger.success(f"All {len(dnodes)} DNODEs are ready")
                    return True
                    
                time.sleep(1)
                
            except Exception as e:
                self.logger.debug(f"Check status failed: {e}")
                time.sleep(1)
                
        cursor.close()
        self.logger.error("Cluster failed to become ready within timeout")
        return False

# ==================== DeployCluster ====================

class DeployCluster(BaseStep):
    def __init__(self):
        self.update_cfg_dict = {}
        self.logger = Logger("./cluster.log")
        self.fqdn = socket.gethostname()

    def create_cluster(self, dnode = 1 , mnode =1, snode =1, level=1, disk=1, use_previous=False):
        print("create cluster with %d dnodes and %d mnodes" % (dnode, mnode))
        
        # param        
        self.dnodes = dnode  # Changed from 3 to 1
        self.mnodes = mnode  # Changed from 2 to 1
        self.snodes = snode
        self.level = level
        self.disk = disk
        self.use_previous = use_previous

        # init
        self.base_path = os.path.expanduser("./td_cluster")
        self.start_adapter = False
            
        self.logger.info("=" * 30)
        self.logger.info("TDengine Standalone Cluster Setup")
        self.logger.info("=" * 30)
        self.logger.info(f"Configuration:")
        self.logger.info(f"  DNODEs: {self.dnodes}")
        self.logger.info(f"  MNODEs: {self.mnodes}")
        self.logger.info(f"  SNODEs: {self.snodes}")
        self.logger.info(f"  Storage Levels: {self.level}")
        self.logger.info(f"  Disks per Level: {self.disk}")
        self.logger.info(f"  FQDN: {self.fqdn}")
        self.logger.info(f"  Base Path: {self.base_path}")
        self.logger.info(f"  Use Previous Cluster: {self.use_previous}")
        self.logger.info(f"  Start TaosAdapter: {self.start_adapter}")
        self.logger.info("=" * 30)
        
        try:
            self.cluster_mgr = ClusterManager(
                fqdn=self.fqdn,
                base_path=self.base_path,
                level=self.level,
                disk=self.disk,
                logger=self.logger
            )
            
            # Create or use existing cluster
            self.cluster_mgr.create_cluster(
                dnode_nums=self.dnodes,
                mnode_nums=self.mnodes,
                update_dict=self.update_cfg_dict,
                use_previous=self.use_previous
            )
            
            # Connect to cluster
            self.cluster_mgr.connect()
            
            if not self.use_previous:
                # Create DNODEs in cluster
                if self.dnodes > 1:
                    self.cluster_mgr.create_dnodes_in_cluster(self.dnodes)
                    
                # Create MNODEs
                if self.mnodes > 0:
                    self.cluster_mgr.create_mnodes_in_cluster(self.mnodes)
                    
                # Wait for cluster ready
                self.cluster_mgr.wait_for_cluster_ready()
                
                # Create SNODEs
                if self.snodes > 0:
                    self.cluster_mgr.create_snodes_in_cluster(self.snodes)
            
            # Start TaosAdapter if requested - only initialized when -B flag is used
            if self.start_adapter:
                self.cluster_mgr.start_taosadapter()
            
            # Check final status
            self.cluster_mgr.check_cluster_status(check_snodes=(self.snodes > 0))
            
            # Print summary
            self.logger.info("=" * 30)
            self.logger.success("✓ Cluster setup completed!")
            self.logger.info("=" * 30)
            self.logger.info("\nCluster Summary:")
            self.logger.info(f"  Total DNODEs: {self.dnodes}")
            self.logger.info(f"  Total MNODEs: {self.mnodes}")
            self.logger.info(f"  Total SNODEs: {self.snodes}")
            self.logger.info(f"  Storage: {self.level} levels × {self.disk} disks")
            if self.start_adapter:
                self.logger.info(f"  TaosAdapter: Running on port 6041")
            self.logger.info("\nNext Steps:")
            self.logger.info(f"  1. Connect: taos -h {self.fqdn}")
            self.logger.info(f"  2. Config: {self.base_path}/dnode1/cfg/taos.cfg")
            self.logger.info(f"  3. Logs: {self.base_path}/dnode*/log/")
            self.logger.info("=" * 30)
            
        except Exception as e:
            self.logger.exit(f"Setup failed: {e}")   
        
cluster = DeployCluster()