#!/bin/bash

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

function print_color() {
    local color="$1"
    local message="$2"
    echo -e "${color}${message}${NC}"
}

function printHelp() {
    echo "Usage: $(basename $0) [options]"
    echo
    echo "Options:"
    echo "    -d [Project dir]            Project directory (default: outermost project directory)"
    echo "                                    e.g., -d /home/TDinternal/community"
    echo "    -f [Capture gcda dir]       Capture gcda directory (default: <project dir>/debug)"
    echo "    -b [Coverage branch]        Covevrage branch (default:3.0)"
    exit 0
}

# Find the project/tdengine/build/capture directory 
function get_DIR() {
    today=`date +"%Y%m%d"`
    if [ -z "$PROJECT_DIR" ]; then
        CODE_DIR=$(dirname $0)
        cd $CODE_DIR
        CODE_DIR=$(pwd)
        if [[ "$CODE_DIR" == *"/community/"*  ]]; then
            PROJECT_DIR=$(realpath ../..)
            TDENGINE_DIR="$PROJECT_DIR"
            BUILD_DIR="$PROJECT_DIR/debug"
            CAPTURE_GCDA_DIR="$BUILD_DIR"
        else
            PROJECT_DIR=$(realpath ..)
            TDENGINE_DIR="$PROJECT_DIR"
            BUILD_DIR="$PROJECT_DIR/debug"
            CAPTURE_GCDA_DIR="$BUILD_DIR"
        fi
    elif [[ "$PROJECT_DIR" == *"/TDinternal" ]]; then
        TDENGINE_DIR="$PROJECT_DIR/community"
        BUILD_DIR="$PROJECT_DIR/debug"
        CAPTURE_GCDA_DIR="$BUILD_DIR"
    elif [[ "$PROJECT_DIR" == *"/TDengine" ]]; then
        TDENGINE_DIR="$PROJECT_DIR"
        BUILD_DIR="$PROJECT_DIR/debug"
        CAPTURE_GCDA_DIR="$BUILD_DIR"
    fi
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDENGINE_DIR

    # collect data
    lcov -d "$CAPTURE_GCDA_DIR" -capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info --quiet > /dev/null 2>&1

    # remove exclude paths 
    lcov --remove coverage.info \
        '*/contrib/*' '*/test/*' '*/packaging/*' '*/taos-tools/deps/*' '*/taosadapter/*' '*/TSZ/*' \
        '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c' '/*/enterprise/*' '*/docs/*' '*/sim/*'\
        '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
        '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
        '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.cc' '*/strftime.c' '*/localtime.c'\
        '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/version.c' '*/tencode.h' \
        '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c' '*/debug/*' '*/tests/*'\
        '*/tsdbFile.c' '*/tsdbUpgrade.c' '*/tsdbFS.c' '*/tsdbReaderWriter.c' \ 
        '*/sql.c' '*/sql.y' '*/smaSnapshot.c' '*/smaCommit.c'\
        --rc lcov_branch_coverage=1  -o coverage.info --quiet  > /dev/null 2>&1

    # generate result
    echo "generate result"
    lcov -l --rc lcov_branch_coverage=1 coverage.info    > /dev/null 2>&1

    sed -i 's/\/home\/TDinternal\/sql.c/\/home\/TDinternal\/community\/source\/libs\/parser\/src\/sql.c/g' coverage.info
    sed -i 's/\/home\/TDinternal\/sql.y/\/home\/TDinternal\/community\/source\/libs\/parser\/inc\/sql.y/g' coverage.info

    # push result to coveralls.io
    echo "push result to coveralls.io"
    /usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage.info > coverall.log 2>&1
}


######################
# main entry
######################

# Initialization parameter
PROJECT_DIR=""
CAPTURE_GCDA_DIR=""
BRANCH=""

# Parse command line parameters
while getopts "hd:b:f:" arg; do
  case $arg in
    d)
      PROJECT_DIR=$OPTARG
      ;;
    b)
      BRANCH=$OPTARG
      ;;
    f)
      CAPTURE_GCDA_DIR=$OPTARG
      ;;
    h)
      printHelp
      ;;
    ?)
      echo "Usage: ./$(basename $0) -h"
      exit 1
      ;;
  esac
done

# Show all parameters
print_color "$GREEN" "Run coverage test on workflow!"
get_DIR
echo "PROJECT_DIR = $PROJECT_DIR"
echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "BUILD_DIR = $BUILD_DIR"
echo "CAPTURE_GCDA_DIR = $CAPTURE_GCDA_DIR"

lcovFunc
print_color "$GREEN" "End of coverage test on workflow!"
