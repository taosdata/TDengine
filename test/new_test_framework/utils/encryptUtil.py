"""
TDengine Encryption Utility for Test Framework

This module provides utilities for managing encryption keys using the taosk tool.
It is designed to be used in test cases that require encrypted databases.

Author: TDengine Test Team
Date: 2025-12-29
"""

import os
import subprocess
import shutil
from .log import tdLog
from .epath import binPath


class EncryptKeyManager:
    """Manager for encryption key generation and validation"""
    
    @staticmethod
    def find_taosk_tool():
        """
        Locate the taosk tool in the system
        
        Search order:
        1. Build directory (debug/release)
        2. Installation directory (/usr/bin)
        3. System PATH
        4. Environment variable TAOSK_PATH
        
        Returns:
            str: Full path to taosk tool, or None if not found
        """
        # Try build directory
        try:
            bin_path = binPath()
            taosk_build = os.path.join(bin_path, 'taosk')
            if os.path.exists(taosk_build):
                return taosk_build
        except:
            pass
        
        # Try installation directory
        if os.path.exists('/usr/bin/taosk'):
            return '/usr/bin/taosk'
        
        # Try PATH
        taosk_in_path = shutil.which('taosk')
        if taosk_in_path:
            return taosk_in_path
        
        # Try environment variable
        taosk_env = os.getenv('TAOSK_PATH')
        if taosk_env and os.path.exists(taosk_env):
            return taosk_env
        
        return None
    
    @staticmethod
    def get_data_dir_from_config(cfg_path):
        """
        Read dataDir configuration from taos.cfg
        
        Args:
            cfg_path: Path to taosd configuration directory
            
        Returns:
            str: Data directory path (default: /var/lib/taos)
        """
        taos_cfg = os.path.join(cfg_path, 'taos.cfg')
        data_dir = '/var/lib/taos'
        
        if os.path.exists(taos_cfg):
            try:
                with open(taos_cfg, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith('dataDir'):
                            parts = line.split(maxsplit=1)
                            if len(parts) > 1:
                                data_dir = parts[1].strip()
                                break
            except Exception as e:
                tdLog.warning(f"Failed to read config file: {e}")
        
        return data_dir
    
    @staticmethod
    def check_keys_exist(cfg_path):
        """
        Check if encryption key files already exist
        
        Args:
            cfg_path: Path to taosd configuration directory
            
        Returns:
            bool: True if key files exist, False otherwise
        """
        data_dir = EncryptKeyManager.get_data_dir_from_config(cfg_path)
        
        master_key = os.path.join(data_dir, "dnode/config/master.bin")
        derived_key = os.path.join(data_dir, "dnode/config/derived.bin")
        
        exists = os.path.exists(master_key) or os.path.exists(derived_key)
        
        if exists:
            tdLog.info(f"Found encryption key files in {data_dir}")
            if os.path.exists(master_key):
                tdLog.info(f"  - {master_key}")
            if os.path.exists(derived_key):
                tdLog.info(f"  - {derived_key}")
        
        return exists
    
    @staticmethod
    def generate_keys(cfg_path, svr_key=None, db_key=None, 
                     generate_config=True, generate_meta=True,
                     generate_data=True, data_key=None,
                     force=False):
        """
        Generate encryption keys using taosk tool
        
        All keys are optional. If not specified, taosk will auto-generate them.
        
        Args:
            cfg_path: Path to taosd configuration directory
            svr_key: Server key (optional, auto-generated if None)
            db_key: Database key (optional, auto-generated if None)
            generate_config: Whether to generate config key (default: True)
            generate_meta: Whether to generate metadata key (default: True)
            generate_data: Whether to generate data key (default: True)
            data_key: Data encryption key (optional, auto-generated if None)
            force: Force regeneration even if keys exist
            
        Returns:
            tuple: (success: bool, message: str)
        """
        try:
            # Find taosk tool
            taosk_path = EncryptKeyManager.find_taosk_tool()
            if not taosk_path:
                msg = "taosk tool not found, please compile enterprise/src/plugins/taosk"
                tdLog.error(msg)
                return False, msg
            
            tdLog.info(f"Using taosk tool: {taosk_path}")
            
            # Check if keys already exist
            if not force and EncryptKeyManager.check_keys_exist(cfg_path):
                msg = "Encryption keys already exist, skipping generation"
                tdLog.info(msg)
                return True, msg
            
            # Build taosk command
            cmd = [taosk_path, '-c', cfg_path]
            
            # Add server key (optional - auto-generated if not specified)
            if svr_key:
                cmd.extend(['--encrypt-server', svr_key])
            else:
                cmd.append('--encrypt-server')  # Auto-generate
            
            # Add database key (optional - auto-generated if not specified)
            if db_key:
                cmd.extend(['--encrypt-database', db_key])
            else:
                cmd.append('--encrypt-database')  # Auto-generate
            
            # Optional keys
            if generate_config:
                cmd.append('--encrypt-config')
            
            if generate_meta:
                cmd.append('--encrypt-metadata')
            
            if generate_data:
                if data_key:
                    cmd.extend(['--encrypt-data', data_key])
                else:
                    cmd.append('--encrypt-data')
            
            tdLog.info(f"Executing: {' '.join(cmd)}")
            
            # Execute taosk command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                msg = f"Keys generated successfully"
                tdLog.success(msg)
                if result.stdout:
                    tdLog.info(f"Output: {result.stdout}")
                return True, msg
            elif "already exist" in result.stderr.lower() or "file exists" in result.stderr.lower():
                msg = "Encryption keys already exist"
                tdLog.info(msg)
                return True, msg
            else:
                msg = f"Failed to generate keys: {result.stderr}"
                tdLog.error(msg)
                return False, msg
                
        except subprocess.TimeoutExpired:
            msg = "taosk command execution timeout"
            tdLog.error(msg)
            return False, msg
        except Exception as e:
            msg = f"Exception while generating keys: {str(e)}"
            tdLog.error(msg)
            return False, msg
    
    @staticmethod
    def cleanup_keys(cfg_path):
        """
        Remove encryption key files
        
        Args:
            cfg_path: Path to taosd configuration directory
        """
        data_dir = EncryptKeyManager.get_data_dir_from_config(cfg_path)
        
        master_key = os.path.join(data_dir, "dnode/config/master.bin")
        derived_key = os.path.join(data_dir, "dnode/config/derived.bin")
        
        try:
            if os.path.exists(master_key):
                os.remove(master_key)
                tdLog.info(f"Removed {master_key}")
            
            if os.path.exists(derived_key):
                os.remove(derived_key)
                tdLog.info(f"Removed {derived_key}")
                
        except Exception as e:
            tdLog.error(f"Failed to cleanup keys: {str(e)}")


def generate_encrypt_keys(cfg_path, svr_key=None, db_key=None, **kwargs):
    """
    Convenience function for generating encryption keys
    
    This is a simplified wrapper around EncryptKeyManager.generate_keys()
    for common use cases in test cases.
    
    All keys are optional. If not specified, taosk will auto-generate them.
    
    Args:
        cfg_path: Path to taosd configuration directory
        svr_key: Server key (optional, auto-generated if None)
        db_key: Database key (optional, auto-generated if None)
        **kwargs: Additional options (generate_config, generate_meta, generate_data, data_key, force)
        
    Returns:
        tuple: (success: bool, message: str)
        
    Examples:
        # Auto-generate all keys
        success, msg = generate_encrypt_keys(cfg_path='/etc/taos')
        
        # Specify server and database keys
        success, msg = generate_encrypt_keys(
            cfg_path='/etc/taos',
            svr_key='1234567890',
            db_key='mydbkey'
        )
        
        # Full control
        success, msg = generate_encrypt_keys(
            cfg_path='/etc/taos',
            svr_key='my_svr',
            db_key='my_db',
            generate_config=True,
            generate_meta=True,
            generate_data=True,
            data_key='my_data_key'
        )
        
        if not success:
            tdLog.exit(f"Failed to generate keys: {msg}")
    """
    return EncryptKeyManager.generate_keys(cfg_path, svr_key, db_key, **kwargs)


def check_encrypt_keys_exist(cfg_path):
    """
    Check if encryption key files exist
    
    Args:
        cfg_path: Path to taosd configuration directory
        
    Returns:
        bool: True if key files exist
        
    Example:
        if not check_encrypt_keys_exist('/etc/taos'):
            generate_encrypt_keys('/etc/taos', '1234567890')
    """
    return EncryptKeyManager.check_keys_exist(cfg_path)



