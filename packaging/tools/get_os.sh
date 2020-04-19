#!/bin/bash
#
# This file is used to install TAOS time-series database on linux systems. The operating system 
# is required to use systemd to manage services at boot

set -e
# set -x

# run uname get kernel os info
function get_kernel() {
    read -ra uname <<< "$(uname -srm)"
    kernel_name="${uname[0]}" #Darwin,Linux,Windows..
    kernel_release="${uname[1]}" #version
    kernel_machine="${uname[2]}" # x86_64
    ARC="${uname[2]}" # x86_64
}

get_kernel

function get_os() {
    # !!Must run get_kernel first
    case $kernel_name in
        Linux|GNU*)
            KERNEL=Linux
            OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
            len=$(echo ${#OS})
            len=$((len-2))
            OS=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
        ;;
        
        CYGWIN*|MSYS*|MINGW*)
            # MINGW32_NT,MINGW64_NT... , set OS the same name, check ARC for CPU
            OS=Windows
        ;;

        Darwin)   
            OS=Darwin   # use kernel name
            KERNEL=Darwin
            OS_NAME="$(sw_vers -productName)" # OS: Mac OS X
            OS_VER="$(sw_vers -productVersion)" # Version :10.15.3
        ;;

        SunOS)    
            OS=Solaris 
        ;;

        *BSD|DragonFly|Bitrig)
            OS=BSD
        ;;

        *)
            # other kernel like HP-UX, AIX,etc.
            OS="$kernel_name"
        ;;
    esac
}

get_os

echo -ne $OS

