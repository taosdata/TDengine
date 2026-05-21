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
import re
import subprocess
import argparse
from pathlib import Path


def _create_so_symlinks(lib_dir: str) -> None:
    """Create unversioned .so symlinks inside lib_dir.

    Packages ship versioned files (e.g. libtaos.so.3.3.7.9) but binaries link
    against the unversioned name (libtaos.so) or the soname (libtaos.so.1).
    Without these symlinks the dynamic linker ignores LD_LIBRARY_PATH and falls
    back to the system library.

    For each  libXXX.so.<version>  file:
      1. Create libXXX.so  (unversioned)
      2. Read the ELF SONAME and create that symlink too (e.g. libtaos.so.1)
    """
    import subprocess as _sp
    p = Path(lib_dir)
    for versioned in p.glob("lib*.so.*"):
        if not versioned.is_file():
            continue
        name = versioned.name
        dot_so = name.find(".so.")
        if dot_so == -1:
            continue
        # 1. Unversioned symlink: libtaos.so -> libtaos.so.3.3.7.9
        unversioned = p / (name[: dot_so + 3])
        if not unversioned.exists():
            unversioned.symlink_to(versioned.name)
        # 2. SONAME symlink: libtaos.so.1 -> libtaos.so.3.3.5.0
        try:
            out = _sp.run(
                ["readelf", "-d", str(versioned)],
                capture_output=True, text=True, timeout=10
            ).stdout
            for line in out.splitlines():
                if "SONAME" in line:
                    m = re.search(r'\[([^\]]+)\]', line)
                    if m:
                        soname_link = p / m.group(1)
                        if not soname_link.exists():
                            soname_link.symlink_to(versioned.name)
                    break
        except Exception:
            pass


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
            "3.3.3.0", "3.3.4.0", "3.3.5.0", "3.3.6.0",
            "3.3.7.9", "3.3.8.5", "3.3.8.6"
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
            if tuple(map(int, version.split('.'))) >= (3, 3, 6, 16):
                package_name = f"TDengine-tsdb-enterprise-{version}-{arch}.tar.gz".lower()
            else:
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
        
        package_name: str = os.path.basename(package_path)
        package_dir = re.split("-linux-", package_name, maxsplit=1, flags=re.IGNORECASE)[0]
        
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

    def download_and_extract(self, version, package_type="enterprise"):
        """
        Download and extract enterprise package without running install.sh.
        Safe for use in compatibility tests where the system binaries must not
        be replaced.

        Note: also creates unversioned .so symlinks inside the lib/driver dir so
        that LD_LIBRARY_PATH is effective (packages ship libtaos.so.X.Y.Z but
        binaries link against the unversioned libtaos.so).

        Returns:
            tuple: (bin_dir, lib_dir) absolute paths inside the extracted tree
        """
        package_path = self.download_package(version, package_type)
        package_name = os.path.basename(package_path)
        package_dir = re.split("-linux-", package_name, maxsplit=1, flags=re.IGNORECASE)[0]
        extract_dir = os.path.join(self.download_path, package_dir)

        if not Path(extract_dir).exists():
            print(f"Extracting {package_name} to {self.download_path}")
            result = os.system(
                f"cd {self.download_path} && tar xf {package_name} > /dev/null 2>&1"
            )
            if result != 0:
                raise RuntimeError(f"Failed to extract package {package_name}")
        else:
            print(f"Package already extracted at {extract_dir}")

        # The outer archive contains a nested package.tar.gz that holds bin/.
        # Extract it if bin/ is not yet present.
        bin_dir = os.path.join(extract_dir, "bin")
        inner_package = os.path.join(extract_dir, "package.tar.gz")
        if not Path(bin_dir).exists():
            if not Path(inner_package).exists():
                raise RuntimeError(f"Neither bin/ nor package.tar.gz found in: {extract_dir}")
            print(f"Extracting inner package.tar.gz in {extract_dir}")
            result = os.system(
                f"cd {extract_dir} && tar xf package.tar.gz > /dev/null 2>&1"
            )
            if result != 0:
                raise RuntimeError(f"Failed to extract inner package.tar.gz in {extract_dir}")

        # Libraries live in driver/ (not lib/) for this package layout.
        lib_dir = os.path.join(extract_dir, "driver")
        if not Path(lib_dir).exists():
            # Fallback: some older packages may use lib/
            lib_dir = os.path.join(extract_dir, "lib")

        if not Path(bin_dir).exists():
            raise RuntimeError(f"bin/ not found in extracted package: {bin_dir}")
        if not Path(lib_dir).exists():
            raise RuntimeError(f"Neither driver/ nor lib/ found in extracted package: {extract_dir}")

        # Create unversioned symlinks (e.g. libtaos.so -> libtaos.so.3.3.7.9) so that
        # LD_LIBRARY_PATH=lib_dir is effective.  The binaries link against "libtaos.so"
        # (unversioned), but packages only ship the versioned filename.
        _create_so_symlinks(lib_dir)

        print(f"Package extracted: bin={bin_dir}, lib={lib_dir}")
        return bin_dir, lib_dir

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
