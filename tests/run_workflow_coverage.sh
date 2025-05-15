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
    echo "    -d [TDengine dir]           Project directory (default: outermost project directory)"
    echo "                                    e.g., -d /home/TDinternal/community"
    echo "    -f [Capture gcda dir]       Capture gcda directory (default: <project dir>/debug)"
    echo "    -b [Coverage branch]        Covevrage branch (default:3.0)"
    exit 0
}

function lcovFunc {
    echo "collect data by lcov"
    cd $TDENGINE_DIR

    # collect data
    lcov -d "$CAPTURE_GCDA_DIR" -capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $TDENGINE_DIR -o coverage.info 


    # remove exclude paths 
    lcov --remove coverage.info \
        '*/contrib/*' '*/test/*' '*/passwdTest.c'  '*/taosc_test/*' '*/taoscTest.cpp' '*/packaging/*' '*/taos-tools/deps/*' '*/taosadapter/*' '*/TSZ/*' \
        '*/AccessBridgeCalls.c' '*/ttszip.c' '*/dataInserter.c' '*/tlinearhash.c' '*/tsimplehash.c' '*/tsdbDiskData.c' '/*/enterprise/*' '*/docs/*' '*/sim/*'\
        '*/texpr.c' '*/runUdf.c' '*/schDbg.c' '*/syncIO.c' '*/tdbOs.c' '*/pushServer.c' '*/osLz4.c'\
        '*/tbase64.c' '*/tbuffer.c' '*/tdes.c' '*/texception.c' '*/examples/*' '*/tidpool.c' '*/tmempool.c'\
        '*/clientJniConnector.c' '*/clientTmqConnector.c' '*/version.cc' '*/strftime.c' '*/localtime.c'\
        '*/tthread.c' '*/tversion.c'  '*/ctgDbg.c' '*/schDbg.c' '*/qwDbg.c' '*/version.c' '*/tencode.h' \
        '*/shellAuto.c' '*/shellTire.c' '*/shellCommand.c' '*/debug/*' '*/tests/*'\
        '*/tsdbFile.c' '*/tsdbUpgrade.c' '*/tsdbFS.c' '*/tsdbReaderWriter.c' '*/tests/script/api/passwdTest.c'\ 
        '*/sql.c' '*/sql.y' '*/smaSnapshot.c' '*/smaCommit.c'  'contrib/*' '*test*' '*tests*' '*/tests/taosc_test/taoscTest.cpp'\
        '*/cJSON.c' '*/lz4.c' '*/contrib/lz4/*' '*/contrib/zlib/*'  '*/contrib/pcre2/*' '*/contrib/libux/*' '*/contrib/libxml2/*' \
        '*/streamsessionnonblockoperator.c' '*/streameventnonblockoperator.c' '*/streamstatenonblockoperator.c' '*/streamfillnonblockoperator.c' \
        '*/streamclient.c' '*/cos_cp.c' '*/cos.c' '*/trow.c' '*/trow.h' '*/tsdbSnapshot.c' '*/smaTimeRange.c' \
        '*/metaSma.c' '*/mndDump.c' '*/td_block_blob_client.cpp' \
        --rc lcov_branch_coverage=1 -o coverage.info

    # generate result
    echo "generate result"
    lcov -l --rc lcov_branch_coverage=1 coverage.info    

    sed -i 's/\/home\/TDinternal\/community\/sql.c/\/home\/TDinternal\/community\/source\/libs\/parser\/src\/sql.c/g' coverage.info
    sed -i 's/\/home\/TDinternal\/community\/sql.y/\/home\/TDinternal\/community\/source\/libs\/parser\/inc\/sql.y/g' coverage.info

    # push result to coveralls.io
    echo "push result to coveralls.io"
    # push result to https://coveralls.io/
    # /usr/local/bin/coveralls-lcov -t WOjivt0JCvDfqHDpyBQXtqhYbOGANrrps -b $BRANCH $TDENGINE_DIR/coverage.info 

    # push result to https://app.codecov.io/
    pip install codecov
    codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f $TDENGINE_DIR/coverage.info  -b $BRANCH
    # codecov -t b0e18192-e4e0-45f3-8942-acab64178afe -f coverage.info  -b $BRANCH -X gcov #如果覆盖率数据已经由其他工具（如 lcov）生成，可以通过 -X gcov 禁用 gcov 的自动收集，以避免冲突或冗余。
}


######################
# main entry
######################

# Initialization parameter
TDINTRENAL_DIR="/home/TDinternal"
TDENGINE_DIR="/home/TDinternal/community"
CAPTURE_GCDA_DIR="/home/TDinternal/community/debug"
BRANCH=""

# Parse command line parameters
while getopts "hd:b:f:" arg; do
  case $arg in
    d)
      TDINTRENAL_DIR=$OPTARG
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

echo "TDINTRENAL_DIR = $TDINTRENAL_DIR"
echo "TDENGINE_DIR = $TDENGINE_DIR"
echo "CAPTURE_GCDA_DIR = $CAPTURE_GCDA_DIR"
echo "BRANCH = $BRANCH"

lcovFunc

COVERAGE_INFO="$TDENGINE_DIR/coverage.info"
OUTPUT_DIR="$CAPTURE_GCDA_DIR/coverage_report"
# Generate local HTML reports
genhtml "$COVERAGE_INFO"  --branch-coverage --function-coverage --output-directory "$OUTPUT_DIR"


print_color "$GREEN" "End of coverage test on workflow!"

echo "For more details: https://app.codecov.io/github/taosdata/TDengine"