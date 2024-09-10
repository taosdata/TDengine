#!/bin/sh

set -e
# set -x

# OPTIONS=`getopt  --options 'ho:r:e:f' --longoptions 'help,os:,arch:,repo:,force' -- "$@"`
FORCE=0
current_os=`uname`
cusName="TDengine"
cusEmail="support@taosdata.com"
cusPrompt="taos"

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
-t, --tag                                   tag
-N, --cusName                               Custom name
-M, --cusEmail                              Custom Email
-P, --cusPrompt                             Custom Prompt
EOF
}

prepare_repo_taoskeeperinternal() {
  ([ -d build-taoskeeper ] && [ -d build-taoskeeper/.git ] && cd build-taoskeeper/ && git pull) || \
    (rm -rf build-taoskeeper && git clone https://github.com/taosdata/$REPO.git build-taoskeeper && cd build-taoskeeper)
}

prepare_repo_taoskeeper() {
  ([ -d build-taoskeeper ] && [ -d build-taoskeeper/.git ] && cd build-taoskeeper/ && git pull) || \
    (rm -rf build-taoskeeper && git clone https://github.com/taosdata/$REPO.git build-taoskeeper && cd build-taoskeeper)
}

checkout_latest_tag() {
  cd build-taoskeeper

  git fetch
  # get tag  
  if [ -n "$TAG" ] && [ $(git tag -l "$TAG") ]; then
    latest=$TAG
  else
    latest=main
  fi

  echo $latest

  latestv=$(echo "$TAG" | sed 's/ver-//')

  gitinfo=`git rev-parse HEAD`
  buildinfo=`date +"%F %T %:z"`

  git checkout $latest
  echo $latest
}

build_binary() {
  if [ "$FORCE" = "0" ] && [ -s taoskeeper ]; then
    true
  else
    if [ "$REPO" = "taoskeeperinternal" ]; then
      if [ "${cusName}" = "TDengine" ]  && [ "${cusEmail}" = "support@taosdata.com" ] && [ "${cusPrompt}" = "taos" ]; then
        go build -ldflags="-s -w -X 'github.com/taosdata/taoskeeperinternal/version.Version=$latestv' -X 'github.com/taosdata/taoskeeperinternal/version.Gitinfo=$gitinfo' -X 'github.com/taosdata/taoskeeperinternal/version.BuildInfo=$buildinfo'" -o taoskeeper main.go
      else
        go build -ldflags="-s -w -X 'github.com/taosdata/taoskeeperinternal/version.CUS_NAME=$cusName' -X 'github.com/taosdata/taoskeeperinternal/version.CUS_EMAIL=$cusEmail' -X 'github.com/taosdata/taoskeeperinternal/version.CUS_PROMPT=$cusPrompt' -X 'github.com/taosdata/taoskeeper/version.CUS_NAME=$cusName' -X 'github.com/taosdata/taoskeeper/version.CUS_EMAIL=$cusEmail' -X 'github.com/taosdata/taoskeeper/version.CUS_PROMPT=$cusPrompt' -X 'github.com/taosdata/taoskeeperinternal/version.Version=$latestv' -X 'github.com/taosdata/taoskeeperinternal/version.Gitinfo=$gitinfo' -X 'github.com/taosdata/taoskeeperinternal/version.BuildInfo=$buildinfo'" -o taoskeeper main.go
      fi
    elif [ "$REPO" = "taoskeeper" ]; then
      go build -ldflags="-s -w -X 'github.com/taosdata/taoskeeper/version.Version=$latestv' -X 'github.com/taosdata/taoskeeper/version.Gitinfo=$gitinfo' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=$buildinfo'" -o taoskeeper main.go
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
while getopts "ho:r:e:f:t:N:M:P:" arg
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
  t)
    export TAG=$OPTARG
    ;;
  N)
    export cusName=$OPTARG
    ;;
  M)
    export cusEmail=$OPTARG
    ;;
  P)
    export cusPrompt=$OPTARG
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
