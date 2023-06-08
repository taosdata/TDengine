#!/bin/sh

set -e
# set -x

# OPTIONS=`getopt  --options 'ho:r:e:f' --longoptions 'help,os:,arch:,repo:,force' -- "$@"`
FORCE=0
current_os=`uname`
usage() {
  cat << EOF
Usage:
   $0
   $0 -h|--help
   $0 [--os <os>] [--arch <arch>] [--repo <repo>]

Build taoskeeper in latest tag.

-h, --help                                  Display help

-o, --os <os>                               Run script in verbose mode. Will print out each step of execution.
-r, --arch <arch>                           Remove TDinsight dashboard, TDengine data source and the plugin.
-f, --force                                 Force rebuild and pack
-e, --repo                                  Code repositories
EOF
}

prepare_repo_taoskeeperinternal() {
  ([ -d build-taoskeeper ] && [ -d build-taoskeeper/.git ] && cd build-taoskeeper/ && git pull) || \
    (rm -rf build-taoskeeper && git clone https://github.com/taosdata/$REPO.git -b 3.0 build-taoskeeper && cd build-taoskeeper)
}

prepare_repo_taoskeeper() {
  ([ -d build-taoskeeper ] && [ -d build-taoskeeper/.git ] && cd build-taoskeeper/ && git pull) || \
    (rm -rf build-taoskeeper && git clone https://github.com/taosdata/$REPO.git build-taoskeeper && cd build-taoskeeper)
}

checkout_latest_tag() {
  cd build-taoskeeper
  latest="v1.0.9"

   if [ "$REPO" = "taoskeeper" ]; then
     git checkout $latest
   fi
   echo $latest
}

build_binary() {
  if [ "$FORCE" = "0" ] && [ -s taoskeeper ]; then
    true
  else
    if [ "$REPO" = "taoskeeperinternal" ]; then
      go build -ldflags="-s -w -X 'github.com/taosdata/taoskeeperinternal/version.Version=$latest'" -o taoskeeper main.go
    elif [ "$REPO" = "taoskeeper" ]; then
      go build -ldflags="-s -w -X 'github.com/taosdata/taoskeeper/version.Version=$latest'" -o taoskeeper main.go
          # if os != darwin, use upx to compress binary
      if [ "$current_os" != "Darwin" ]; then
         upx taoskeeper > /dev/null 2>&1 || :
      fi
    fi
#    upx taoskeeper > /dev/null 2>&1 || :   
  fi
  readlink -f taoskeeper
}

# eval set -- "$OPTIONS"
while getopts "ho:r:e:f" arg
do
  case $arg in
  h)
    usage
    exit 0
    ;;
  o)
    export GOOS=$OPTARG
    ;;
  r)
    export GOARCH=$OPTARG
    ;;
  e)
    export REPO=$OPTARG
    ;;
  f)
    export FORCE=1
    ;;
  ?)
    break
    ;;
  esac
done

set -e
if [ "$REPO" = "taoskeeperinternal" ]; then
  prepare_repo_taoskeeperinternal > /dev/null
elif [ "$REPO" = "taoskeeper" ]; then
  prepare_repo_taoskeeper > /dev/null
fi
checkout_latest_tag > /dev/null
build_binary
