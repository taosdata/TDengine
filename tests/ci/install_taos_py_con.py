import subprocess
import pkg_resources
import sys
import argparse

def get_installed_version(package_name):
    try:
        return pkg_resources.get_distribution(package_name).version
    except pkg_resources.DistributionNotFound:
        return None

def install_package(package_name, version):
    subprocess.check_call([sys.executable, "-m", "pip", "install", f"{package_name}=={version}"])

def main(packages):
    for package, latest_version in packages.items():
        installed_version = get_installed_version(package)
        if installed_version != latest_version:
            print(f"Installing {package} version {latest_version} (current version: {installed_version})")
            install_package(package, latest_version)
        else:
            print(f"{package} is already up-to-date (version {installed_version})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Install specified versions of packages.')
    parser.add_argument('packages', nargs='+', help='List of packages with versions (e.g., taospy==2.7.16 taos-ws-py==0.3.3)')
    args = parser.parse_args()

    packages = {}
    for package in args.packages:
        name, version = package.split('==')
        packages[name] = version

    main(packages)