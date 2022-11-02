#!/bin/sh

# set -x

options=$(getopt -l "help,os:,arch:,repo:,force" -o "ho:r:e:f" -a -- "$@")
FORCE=0

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

prepare_repo() {
  ([ -d build-taoskeeper ] && [ -d build-taoskeeper/.git ] && cd build-taoskeeper/ && git pull) || \
    (rm -rf build-taoskeeper && git clone https://github.com/taosdata/$REPO.git build-taoskeeper && cd build-taoskeeper)
}

checkout_latest_tag() {
  cd build-taoskeeper
  latest=$(git tag --sort=-taggerdate|grep -o 'v.*'|head -n1)
  if [ "$latest" = "" ]; then
    latest="3.0"
  else
    git checkout $latest
  fi
  echo $latest
}

build_binary() {
  if [ "$FORCE" = "0" ] && [ -s taoskeeper ]; then
    true
  else
    go build -ldflags="-s -w"
    upx taoskeeper > /dev/null 2>&1 || :
  fi
  readlink -f taoskeeper
}

eval set -- "$options"

while true; do
  case $1 in
  -h | --help)
    usage
    exit 0
    ;;
  -o | --os)
    shift
    export GOOS=$1
    ;;
  -r | --arch)
    shift
    export GOARCH=$1
    ;;
  -e | --repo)
    shift
    export REPO=$1
    ;;
  -f | --force)
    shift
    export FORCE=1
    ;;
  --)
    shift
    break
    ;;
  esac
  shift
done

set -e
prepare_repo > /dev/null
checkout_latest_tag > /dev/null
build_binary
