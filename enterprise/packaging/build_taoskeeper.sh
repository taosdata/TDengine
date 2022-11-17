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
  latest=$(git tag --sort=-taggerdate|head -n1)
  if [ "$latest" = "" ]; then
    latest=$(git log --pretty=format:"%h"|head -n1)
  else
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
    fi
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
if [ "$REPO" = "taoskeeperinternal" ]; then
  prepare_repo_taoskeeperinternal > /dev/null
elif [ "$REPO" = "taoskeeper" ]; then
  prepare_repo_taoskeeper > /dev/null
fi
checkout_latest_tag > /dev/null
build_binary
