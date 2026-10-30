#!/bin/bash

# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set +e
# set -x

if [[ "$OSTYPE" == "darwin"* ]]; then
  TD_OS="Darwin"
else
  OS=$(cat /etc/*-release | grep "^NAME=" | cut -d= -f2)
  len=$(echo ${#OS})
  len=$((len - 2))
  TD_OS=$(echo -ne ${OS:1:${len}} | cut -d" " -f1)
fi

if [[ "$TD_OS" == "Alpine" ]]; then
  echo -e "os is Alpine,skip check Asan"
  exit 0
fi
unset LD_PRELOAD
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR"/../ || exit 1
SCRIPT_DIR=$(pwd)

IN_TDINTERNAL="community"
if [[ "${SCRIPT_DIR}" == *"taos-community"* ]]; then
  TOP_DIR="${TEST_CODE_DIR}/../../../"
elif [[ "${SCRIPT_DIR}" == *"${IN_TDINTERNAL}"* ]]; then
  TOP_DIR="${TEST_CODE_DIR}/../../"
else
  TOP_DIR="${TEST_CODE_DIR}/../"
fi

if [[ -n "$WORK_DIR" ]]; then
  echo "WORK_DIR: $WORK_DIR"
  TAOS_DIR=$WORK_DIR  
elif [[ -n "$SIM_DIR" ]]; then
  echo "SIM_DIR: $SIM_DIR"
  TAOS_DIR=$SIM_DIR
else
  TAOS_DIR=$(pwd)/sim
fi
LOG_DIR=$TAOS_DIR/asan

# 无法使用 grep -c 代替 grep  |wc -l , 因为 grep -c 会统计每个文件的数量，无法统计总数
# shellcheck disable=SC2126
error_num=$(cat "${LOG_DIR}"/*.asan | grep "ERROR" | wc -l)

archOs=$(arch)

# ---------- 1. 集中白名单，以后要加只改这里 ----------
# 1a. 命中即忽略：块里只要有任一帧命中这些已知无害的 TDengine 符号，整块不计。
#     这里只放「已知无害的调用点」。不要放通用分配器：taosMemCalloc 是引擎的 malloc
#     对应物，几乎每条引擎泄漏栈里都有它，放在这一档等于把整类泄漏豁免掉。
ignore_pat='transDumpFromBuffer|cliHandleResp'

# 1a'. 通用分配器（taosMemCalloc）按「管道帧」处理：它只说明分配走了引擎内存池，
#      本身不是泄漏现场。放进 plumbing 的效果是——栈里只有它加上第三方/sanitizer 帧
#      （模块被剥符号、或静态链进来的第三方代码）时仍然豁免；一旦出现引擎源码帧，
#      整块重新计入。这样既保留了原来的豁免意图，又不会把「分配器 + 引擎代码」的
#      真泄漏一起放掉。
allocator_pat='taosMemCalloc'

# 1b. 纯第三方栈：OpenSSL 被链进 libtaos.so 后，它自己的一次性 config/error-state
# 初始化（OPENSSL_init_crypto -> CONF_modules_load_file_ex -> ERR_set_mark ->
# CRYPTO_zalloc）会被 LSan 报成 Direct leak。整条分配链都在 libcrypto 里，没有一帧是
# TDengine 代码；TDengine 只是触发者。
#
# 判定方式：块里所有帧都属于「第三方管道」（libcrypto/libssl 符号、只有 module+offset
# 的 libcrypto/libssl 帧、sanitizer 自己的 malloc 包装、glibc 的 pthread_once、以及引擎
# 的通用分配器 taosMemCalloc）时才忽略。
# 只要出现一帧不认识的——尤其是 TDengine 源码帧——整块仍然计入。
openssl_internal_pat="CRYPTO_|ERR_|BIO_(new_file|meth_new)|def_load|NCONF_load|CONF_modules_load_file_ex|ossl_|OPENSSL_|sk_[A-Za-z0-9_]+_deep_copy|EVP_|RAND_|SSL_CTX_|inner_(evp|ossl)_|evp_(generic_fetch|cipher_|rand_)|ssl_(evp_md_fetch|load_ciphers)|provider_|algorithm_|construct_evp_method|drbg_|rand_new_|seed_src_|pgtls_|initialize_SSL|aes_[A-Za-z0-9_]+_newctx"
# libpq.so.5 may bundle OpenSSL symbols under the taos-community build path.
# Keep this filter tied to libpq so TDengine-owned frames remain visible.
libpq_openssl_internal_pat="(X509_[A-Za-z0-9_]+|by_file_ctrl_ex).*/libpq\\.so\\.5"
# libcrypto/libssl 里被剥掉符号的帧只打印成 "(libcrypto.so.3+0x1234)"，没有函数名，
# 所以按模块名单独放行。
#
# Oracle Instant Client 同理：OCISessionBegin 内部会把协议协商串缓存进
# kpuSetMatchingProtocol 的进程级状态（taosd 实测 49 字节；FIPS 随机数上下文、
# DBGC 通知、sltsqKeyAdd 等也是同一类），客户端库退出时不会释放，LSan 就在 taosd
# 退出时把它们报成 Direct leak。整条分配链都在 libclntsh/libclntshcore/libnnz 内，
# 没有一帧 TDengine 代码——我们只是触发者。按模块放行，语义同 libcrypto：栈里一旦
# 出现引擎源码帧（例如 provider 自己 OCIHandleAlloc 出来的句柄没释放），整块仍然计入。
oracle_client_module_pat="[/(](libclntsh|libclntshcore|libnnz|libociei|libsqora|libaio)[^/]*\\.so"
# The driver manager and the ODBC driver sit between the engine and the
# Instant Client in every Oracle stack (libodbc.so.2 -> libsqora.so -> ...);
# they are third-party plumbing too, exactly like the Instant Client modules.
odbc_manager_module_pat="[/(](libodbc|libodbcinst)[^/]*\\.so"
third_party_module_pat="[/(](libcrypto|libssl)\\.so|${oracle_client_module_pat}|${odbc_manager_module_pat}"
plumbing_pat="asan_malloc_linux|__interceptor_|__pthread_once|${allocator_pat}|${third_party_module_pat}|${openssl_internal_pat}|${libpq_openssl_internal_pat}"

# ---------- 2. 一次性读出内容，避免多次 cat ----------
asan=$(cat "${LOG_DIR}"/*.asan)

# ---------- 3. 统计 ----------
# 原实现把白名单 grep 作用在 "Direct leak of N byte(s) ... allocated from:" 这一行上，
# 而白名单里的符号名只会出现在随后的调用帧里，于是 grep -Ev 永远命中不到——白名单
# 是死代码（叠加上面已修的负向先行断言 bug，等于每一次泄漏都会被计入）。
# 现在按「泄漏块」统计：块头连同它的全部调用帧一起判白名单，三条指标共用同一份名单，
# 避免对同一批帧给出相反结论。
leak_stats=$(
  # 正则用环境变量传给 awk：awk 的 -v 会对值做转义处理（"\(" -> "("、"\." -> "."），
  # 会把转义吃掉导致括号不配对、正则连编译都过不了。ENVIRON 不做转义。
  CHECKASAN_BENIGN_PAT="$ignore_pat" CHECKASAN_PLUMBING_PAT="$plumbing_pat" \
  awk '
    BEGIN {
      benign   = ENVIRON["CHECKASAN_BENIGN_PAT"]
      plumbing = ENVIRON["CHECKASAN_PLUMBING_PAT"]
      header = ""; frames = ""; ignored = 0; allplumbing = 1
      report_started = 0; counted = 0; blocks_seen = 0
    }
    function flush() {
      if (header == "") return
      blocks_seen = 1
      if (ignored == 0 && allplumbing == 1 && frames != "") ignored = 1
      if (header ~ /Direct leak of 32 byte/)      { if (ignored == 0) byte32++ }
      else if (header ~ /Direct leak/)            { if (ignored == 0) non_32++ }
      else if (header ~ /Indirect leak/)          { if (ignored == 0) indirect++ }
      if (ignored == 0) counted = 1
      header = ""; frames = ""; ignored = 0; allplumbing = 1
    }
    function close_report() {
      # error_num 用 grep "ERROR" 统计，LeakSanitizer 报告头那行也被算了一次，这里
      # 把「整份报告全是白名单泄漏」的那些报告数出来，好从 error_num 里减回去；
      # 报告里只要有一块没被白名单，就不减，宁严勿松。
      #
      # 必须先 flush：最后一个块只有在这里才会结算，否则 counted 读到的还是
      # 「上一块之前」的状态，最后一块是真泄漏的那份报告会被误判成全白名单，
      # error_num 就少算了。
      # blocks_seen 要求报告里真的出现过块头：一份没有块头的报告（LSan 输出格式
      # 变化、只有 SUMMARY 等）不能被当成「全白名单」而减掉，宁多报不漏报。
      flush()
      if (report_started == 1 && counted == 0 && blocks_seen == 1) benign_reports++
      report_started = 0; counted = 0; blocks_seen = 0
    }
    /ERROR: LeakSanitizer/ { close_report(); report_started = 1; next }
    /^(Direct|Indirect) leak of / {
      flush()
      header = $0
      if ($0 ~ benign) ignored = 1
      next
    }
    /^[[:space:]]+#[0-9]+/ {
      frames = frames $0 "\n"
      if ($0 ~ benign) ignored = 1
      if ($0 !~ plumbing) allplumbing = 0
      next
    }
    END {
      flush(); close_report()
      printf "%d %d %d %d\n", non_32 + 0, byte32 + 0, indirect + 0, benign_reports + 0
    }
  ' <<< "$asan"
)
if [[ -n "$leak_stats" ]]; then
  # non_32: 真泄漏（非 32 字节且非白名单）
  # byte32: 32 字节且非白名单
  # indirect_leak: 间接泄漏且非白名单
  # benign_reports: 整份报告只有白名单泄漏的 LeakSanitizer 报告数
  read -r non_32 byte32 indirect_leak benign_reports <<< "$leak_stats"
  if [[ ${benign_reports:-0} -gt 0 ]]; then
    echo "ignore ${benign_reports} LeakSanitizer report(s) whose frames are all whitelisted"
    error_num=$(( error_num - benign_reports ))
    [[ $error_num -lt 0 ]] && error_num=0
  fi
else
  # 解析器没输出（awk 报错/无输入）：退回到不带白名单的保守计数，宁可多报不可漏报。
  echo "WARN: leak block parser produced no output, falling back to plain counts"
  non_32=$(grep -E "Direct leak" <<< "$asan" | grep -v "Direct leak of 32 byte" | wc -l)
  byte32=$(grep -E "Direct leak of 32 byte" <<< "$asan" | wc -l)
  indirect_leak=$(grep -E "Indirect leak" <<< "$asan" | wc -l)
fi

# ---------- 4. 判断 ----------
if [[ $archOs == *aarch64* ]]; then
  echo "arm64 check mem leak"
  if [[ $non_32 -eq 0 && $byte32 -eq $error_num ]]; then
    echo "reset error_num to 0, ignore: __cxa_thread_atexit_impl leak"
    error_num=0
  fi
else
  echo "os check mem leak"
  # 非 arm64 只关心真泄漏
  [[ $non_32 -eq 0 ]] && echo "no memory leak detected"
fi
memory_leak=${non_32}

# shellcheck disable=SC2126
# tmq_vtable intentionally triggers tAssert in error test cases (case_non_snapshot_stb_topic_error).
# tAssert emits two consecutive lines:
#   E UTL FATAL tAssert at file .../tmq_vtable.c:72 exit:1
#   E UTL FATAL obtained 8 stack frames          <- matched by grep -w "stack"
# The "obtained N stack frames" line carries no file name, so grep -v "tmq_vtable" misses it.
# Use awk to suppress that line only when it immediately follows a tmq_vtable tAssert line.
python_error=$(
  awk '/tmq_vtable/{skip=1;next} skip&&/obtained [0-9]+ stack frames/{skip=0;next} {skip=0}' \
    "${LOG_DIR}"/*.info \
  | grep -w "stack" | wc -l
)

# Use "#" "0x" and "TDinternal" to match python taos error log
# #1 0x7f480ff14839  (/usr/local/lib/python3.9/site-packages/taosws/taosws.abi3.so+0x269839)
# TD-37832: taosws.abi3.so is a memory leak referenced by the third-party library of taosws, which can be ignored for the time being. If it is resolved, the ignore condition can be removed. @qevolg 2025-09-19
# grep -E "TDinternal|TDengine|/taosws/" for check all taos lib and repo related code,example: 
#/home/TDinternal/community/source/libs/scalar/src/sclfunc.c
#/home/TDengine/source/common/src/tdataformat.c
#/root/chr/test_taosd/lib/python3.12/site-packages/taosws/taosws.abi3.so+0x1b2fe4
# shellcheck disable=SC2126
# ignore gcov-instrumented noasan test binaries (sml_test/tmq_get_meta_json/replay_test etc.) running under ASAN:
# when these noasan binaries exit, __gcov_open/__gcov_exit tries to write coverage data and SEGVs.
# this is NOT a product bug — it's an artifact of running gcov-compiled binaries via LD_PRELOAD ASAN.
# openssl_internal_pat / libpq_openssl_internal_pat 已在「集中白名单」里定义，这里用同一份。
python_taos_error=$(
  cat "${LOG_DIR}"/*.info |
  grep -E  "#[0-9]+ 0x[0-9a-f]+ .*(TDinternal|TDengine|/taosws/|/mnt/tsdb/source/taos-community/)" |
  # 只统计带源码行的帧：形如 "  #1 0x... in func path/file.c:123"。
  # 形如 "  #1 0x... in func (libfoo.so+0x123)" 的是 module+offset 帧——库被剥离符号、
  # 或静态链进来的第三方代码（libuv、cJSON 等）都会这样打印。它们不是 TDengine 源码，
  # 却会因为模块路径落在 taos-community 构建目录而被算进来，所以先排除。
  grep -E  "\.(c|h|cc|cpp|cxx|hh|hpp|hxx|go):[0-9]+" |
  # 与 memory_leak 共用同一份白名单（这里把通用分配器也算进来：它的帧只是一次分配的
  # 中转，不是 python 侧的问题），避免两条指标对同一批帧给出相反结论。
  grep -E -v "${ignore_pat}|${allocator_pat}" |
  grep -E -v "venv|taosws.abi3.so|__gcov|gcov_do_dump|_GLOBAL__sub_D|sml_test|tmq_get_meta_json|replay_test|tmq_sim|tmq_taosx_ci|${openssl_internal_pat}|${libpq_openssl_internal_pat}" |
  wc -l
)

# ignore

# TD-20368
# /root/TDengine/contrib/zlib/trees.c:873:5: runtime error: null pointer passed as argument 2, which is declared to never be null

# TD-20494 TD-20452
# /root/TDengine/source/libs/scalar/src/sclfunc.c:735:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'signed char'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:790:11: runtime error: 3.4e+38 is outside the range of representable values of type 'long int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:772:11: runtime error: 3.52344e+09 is outside the range of representable values of type 'int'
# /root/TDengine/source/libs/scalar/src/sclfunc.c:753:11: runtime error: 4.75783e+11 is outside the range of representable values of type 'short int'

# TD-20569
# /root/TDengine/source/libs/function/src/builtinsimpl.c:856:29: runtime error: signed integer overflow: 9223372036854775806 + 9223372036854775805 cannot be represented in type 'long int'
# /root/TDengine/source/libs/scalar/src/sclvector.c:1075:66: runtime error: signed integer overflow: 9223372034707292160 + 1668838476672 cannot be represented in type 'long int'
# /root/TDengine/source/common/src/tdataformat.c:1876:7: runtime error: signed integer overflow: 8252423483843671206 + 2406154664059062870 cannot be represented in type 'long int'
# /home/chr/TDengine/source/libs/scalar/src/filter.c:3149:14: runtime error: applying non-zero offset 18446744073709551615 to null pointer

# /home/chr/TDengine/source/libs/scalar/src/filter.c:3149:14: runtime error: applying non-zero offset 18446744073709551615 to null pointer
# /home/TDinternal/community/source/libs/scalar/src/sclvector.c:1109:66: runtime error: signed integer overflow: 9223372034707292160 + 1676867897049 cannot be represented in type 'long int'

#0 0x7f2d64f5a808 in __interceptor_malloc ../../../../src/libsanitizer/asan/asan_malloc_linux.cc:144
#1 0x7f2d63fcf459 in strerror /build/glibc-SzIz7B/glibc-2.31/string/strerror.c:38

# TD-29953
#/home/TDinternal/community/utils/TSZ/sz/src/sz_double.c:388:59: runtime error: 2.64021e+25 is outside the range of representable values of type 'long unsigned int'
#/home/TDinternal/community/utils/TSZ/sz/src/sz_float.c:407:59: runtime error: 5.76041e+19 is outside the range of representable values of type 'long unsigned int'
#/home/TDinternal/community/source/libs/scalar/src/sclfunc.c:808:11: runtime error: -3.40401e+18 is outside the range of representable values of type 'int'
# shellcheck disable=SC2126
runtime_error=$(
  cat "${LOG_DIR}"/*.asan | 
  grep "runtime error" | 
  grep -E -v "trees.c:873|sclfunc.c.*outside the range of representable values of type|signed integer overflow|strerror.c|asan_malloc_linux.cc|asan_malloc_linux.cpp|sclvector.c|sclfunc.c:808|sz_double.c:388|sz_float.c:407:59" |
  wc -l
)

echo -e "\033[44;32;1m"asan error_num: $error_num"\033[0m"
echo -e "\033[44;32;1m"asan memory_leak: $memory_leak"\033[0m"
echo -e "\033[44;32;1m"asan indirect_leak: $indirect_leak"\033[0m"
echo -e "\033[44;32;1m"asan runtime error: $runtime_error"\033[0m"
echo -e "\033[44;32;1m"asan python error: $python_error"\033[0m"
echo -e "\033[44;32;1m"asan python taos error: $python_taos_error"\033[0m"

let "errors=$error_num+$memory_leak+$indirect_leak+$runtime_error+$python_error+$python_taos_error"

if [ $errors -eq 0 ]; then
  echo -e "\033[44;32;1m"no asan errors"\033[0m"
  exit 0
else
  echo -e "\033[44;31;1m"asan total errors: $errors"\033[0m"
  if [ "$python_error" -ne 0 ] || [ "$python_taos_error" -ne 0 ] ; then
    cat "${LOG_DIR}"/*.info |grep "#" | grep -wE "TDinternal|taos-community"
  fi
  cat "${LOG_DIR}"/*.asan |grep "#" | grep -wE "TDinternal|taos-community"
  exit 1
fi
