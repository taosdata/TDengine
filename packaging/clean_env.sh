#!/bin/bash

CUR_DIR=$(pwd)
SHELL_DIR=$(dirname $(readlink -f "$0"))
ENTERPRISE_DIR=$SHELL_DIR/../..
COMMUNITY_DIR=$SHELL_DIR/..
TOOLS_DIR=$COMMUNITY_DIR/src/kit/taos-tools

cd $ENTERPRISE_DIR
git checkout -- .
if [[ -e enterprise/src/plugins/taosainternal/taosadapter ]]; then
  rm -f enterprise/src/plugins/taosainternal/taosadapter
fi
if [[ -e enterprise/src/plugins/taosainternal/upx.tar.xz ]]; then
  rm -f enterprise/src/plugins/taosainternal/upx.tar.xz
fi

cd $COMMUNITY_DIR
git checkout -- .
if [[ -e src/plugins/taosadapter/taosadapter ]]; then
  rm -f src/plugins/taosadapter/taosadapter
fi
if [[ -e src/plugins/taosadapter/upx.tar.xz ]]; then
  rm -f src/plugins/taosadapter/upx.tar.xz
fi

cd $TOOLS_DIR
git checkout -- .
if [[ -e packaging/tools/install-khtools.sh ]]; then
  rm -f packaging/tools/install-khtools.sh
fi
if [[ -e packaging/tools/uninstall-khtools.sh ]]; then
  rm -f packaging/tools/uninstall-khtools.sh
fi
if [[ -e packaging/tools/install-prodbtools.sh ]]; then
  rm -f packaging/tools/install-prodbtools.sh
fi
if [[ -e packaging/tools/uninstall-prodbtools.sh ]]; then
  rm -f packaging/tools/uninstall-prodbtools.sh
fi
if [[ -e packaging/tools/install-powertools.sh ]]; then
  rm -f packaging/tools/install-powertools.sh
fi
if [[ -e packaging/tools/uninstall-powertools.sh ]]; then
  rm -f packaging/tools/uninstall-powertools.sh
fi

rm -rf $COMMUNITY_DIR/debug/*
rm -rf $COMMUNITY_DIR/release/*
if [[ -e $COMMUNITY_DIR/rpms ]]; then
  rm -rf $COMMUNITY_DIR/rpms
fi
if [[ -e $COMMUNITY_DIR/debs ]]; then
  rm -rf $COMMUNITY_DIR/debs
fi

cd $CUR_DIR
