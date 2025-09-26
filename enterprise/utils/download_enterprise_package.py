#!/usr/bin/env python3
# -*- coding: utf-8 -*-
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

"""
Enterprise Package Downloader for TDinternal
Downloads enterprise version packages from internal network
"""

import os
import sys
import platform
import subprocess
import argparse
from pathlib import Path


class EnterprisePackageDownloader:
    """Enterprise package downloader for TDinternal compatibility testing"""
    
    def __init__(self, base_url=None):
        """
        Initialize the downloader
        
        Args:
            base_url (str): Base URL for enterprise package downloads
        """
        # Internal network URL for enterprise packages
        self.base_url = base_url or "http://192.168.1.131/data/nas/TDengine"
        self.download_path = "/usr/local/src/"
        
        # Supported enterprise versions
        self.supported_versions = [
            "3.3.3.0", "3.3.4.0", "3.3.5.0", "3.3.6.0"
        ]
    
    def get_package_name(self, version, package_type="enterprise"):
        """
        Generate package name based on version and platform
        
        Args:
            version (str): Version string (e.g., "3.3.5.0")
            package_type (str): Package type ("enterprise" or "server")
            
        Returns:
            str: Package filename
        """
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            arch = "Linux-arm64"
        else:
            arch = "Linux-x64"
            
        # Enterprise package naming convention
        if package_type == "enterprise":
            package_name = f"TDengine-enterprise-{version}-{arch}.tar.gz"
        else:
            package_name = f"TDengine-server-{version}-{arch}.tar.gz"
            
        return package_name
    
    def download_package(self, version, package_type="enterprise", force_download=False):
        """
        Download enterprise package for specified version
        
        Args:
            version (str): Version to download
            package_type (str): Package type to download
            force_download (bool): Force re-download even if file exists
            
        Returns:
            str: Path to downloaded package
        """
        if version not in self.supported_versions:
            raise ValueError(f"Unsupported version {version}. Supported: {self.supported_versions}")
        
        package_name = self.get_package_name(version, package_type)
        package_path = os.path.join(self.download_path, package_name)
        
        # Construct URL based on internal network structure
        # URL format: http://192.168.1.131/data/nas/TDengine/3.3/v3.3.5.0/enterprise/
        version_major = ".".join(version.split(".")[:2])  # e.g., "3.3" from "3.3.5.0"
        download_url = f"{self.base_url}/{version_major}/v{version}/{package_type}/{package_name}"
        
        # Check if package already exists
        if Path(package_path).exists() and not force_download:
            print(f"Package {package_name} already exists at {package_path}")
            return package_path
        
        # Create download directory if it doesn't exist
        os.makedirs(self.download_path, exist_ok=True)
        
        # Download package
        print(f"Downloading {package_name} from {download_url}")
        download_cmd = f"cd {self.download_path} && wget -O {package_name} '{download_url}'"
        
        result = os.system(download_cmd)
        if result != 0:
            raise RuntimeError(f"Failed to download package from {download_url}")
        
        # Verify download
        if not Path(package_path).exists():
            raise RuntimeError(f"Package download failed: {package_path} not found")
        
        print(f"Successfully downloaded {package_name} to {package_path}")
        return package_path
    
    def install_package(self, package_path, install_options="-e no"):
        """
        Install downloaded enterprise package
        
        Args:
            package_path (str): Path to the package file
            install_options (str): Installation options
        """
        if not Path(package_path).exists():
            raise FileNotFoundError(f"Package not found: {package_path}")
        
        package_name = os.path.basename(package_path)
        package_dir = package_name.split("-Linux-")[0]
        
        print(f"Installing package {package_name}")
        
        # Extract and install
        install_cmd = (
            f"cd {self.download_path} && "
            f"tar xf {package_name} > /dev/null 2>&1 && "
            f"cd {package_dir} && "
            f"./install.sh {install_options} > /dev/null 2>&1"
        )
        
        result = os.system(install_cmd)
        if result != 0:
            raise RuntimeError(f"Failed to install package {package_name}")
        
        print(f"Successfully installed {package_name}")
    
    def download_and_install(self, version, package_type="enterprise", install_options="-e no"):
        """
        Download and install enterprise package in one step
        
        Args:
            version (str): Version to download and install
            package_type (str): Package type
            install_options (str): Installation options
            
        Returns:
            str: Path to installed package
        """
        package_path = self.download_package(version, package_type)
        self.install_package(package_path, install_options)
        return package_path


def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description="Download TDinternal enterprise packages")
    parser.add_argument("version", help="Version to download (e.g., 3.3.5.0)")
    parser.add_argument("--type", default="enterprise", choices=["enterprise", "server"],
                       help="Package type to download")
    parser.add_argument("--base-url", help="Base URL for downloads")
    parser.add_argument("--install", action="store_true", help="Install after download")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    parser.add_argument("--install-options", default="-e no", help="Installation options")
    
    args = parser.parse_args()
    
    try:
        downloader = EnterprisePackageDownloader(args.base_url)
        
        if args.install:
            package_path = downloader.download_and_install(
                args.version, args.type, args.install_options
            )
        else:
            package_path = downloader.download_package(
                args.version, args.type, args.force
            )
        
        print(f"Operation completed successfully: {package_path}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main() 