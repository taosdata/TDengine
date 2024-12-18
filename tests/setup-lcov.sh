#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 -v <version>"
    echo "Example: $0 -v 1.14"
}

function download_lcov() {
    local version=$1
    local url="https://github.com/linux-test-project/lcov/releases/download/v${version}/lcov-${version}.tar.gz"
    echo "Downloading lcov version ${version} from ${url}..."
    curl -LO ${url}
    tar -xzf lcov-${version}.tar.gz
    echo "lcov version ${version} downloaded and extracted."
}

function install_lcov() {
    echo -e "\nInstalling..."
    local version=$1
    cd lcov-${version}
    sudo make uninstall && sudo make install
    cd ..
    echo "lcov version ${version} installed."
}

function verify_lcov() {
    echo -e "\nVerify installation..."
    lcov --version
}

function main() {
    if [[ "$#" -ne 2 ]]; then
        usage
        exit 1
    fi

    while getopts "v:h" opt; do
        case ${opt} in
            v)
                version=${OPTARG}
                download_lcov ${version}
                install_lcov ${version}
        		verify_lcov
                ;;
            h)
                usage
                exit 0
                ;;
            *)
                usage
                exit 1
                ;;
        esac
    done
}

main "$@"