#!/bin/bash

function usage() {
    echo "$0"
    echo -e "\t -w work dir"
    echo -e "\t -o old package"
    echo -e "\t -n new package"
    echo -e "\t -c client package"
    echo -e "\t -h help"
}

WORK_DIR=/var/data/jenkins/workspace
OLD_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.4.0.4-Linux-x64.tar.gz
NEW_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.4.0.5-Linux-x64.tar.gz
CLI_PACKAGE=$WORK_DIR/package/TDengine-enterprise-client-2.4.0.5-Linux-x64.tar.gz

WORK_DIR=/home/tang/work
OLD_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.2.2.10-Linux-x64.tar.gz
NEW_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.2.2.11-Linux-x64.tar.gz
CLI_PACKAGE=$WORK_DIR/package/TDengine-enterprise-client-2.2.2.11-Linux-x64.tar.gz

WORK_DIR=/home/tang/work
OLD_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.0.22.3-Linux-x64.tar.gz
NEW_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.0.22.4-Linux-x64.tar.gz
CLI_PACKAGE=$WORK_DIR/package/TDengine-enterprise-client-2.0.22.4-Linux-x64.tar.gz

WORK_DIR=/home/tang/work
OLD_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.4.0.4-Linux-x64.tar.gz
NEW_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.4.0.6-Linux-x64.tar.gz
CLI_PACKAGE=$WORK_DIR/package/TDengine-enterprise-client-2.4.0.6-Linux-x64.tar.gz

WORK_DIR=/home/tang/work
OLD_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.0.20.0-Linux-x64.tar.gz
NEW_PACKAGE=$WORK_DIR/package/TDengine-enterprise-server-2.0.20.5-Linux-x64.tar.gz
CLI_PACKAGE=$WORK_DIR/package/TDengine-enterprise-client-2.0.20.5-Linux-x64.tar.gz

./run_test.sh -w $WORK_DIR -o $OLD_PACKAGE -n $NEW_PACKAGE -c $CLI_PACKAGE
