# =========================================================
# Deps options
# =========================================================

option(
    BUILD_TEST
    "If build unit tests using googletest"
    OFF
)

# TODO: tackle 'undefined pthread_atfork referenced by libuv.a' issue found on CentOS7.9/ubuntu 18
option(TD_PTHREAD_TWEAK "tweaking pthread experimentally, especially for CentOS7.9 or ubuntu 18" OFF)

# NOTE: these are not boolean options, but are very much useful
# TAOSADAPTER_BUILD_OPTIONS
if(NOT DEFINED TAOSADAPTER_BUILD_OPTIONS)
  set(TAOSADAPTER_BUILD_OPTIONS "" CACHE STRING "go build options to be used by taosadapter, separated by ':'" FORCE)
endif()

# TAOSADAPTER_GIT_TAG
# <tag/branch/commit-sha1>:[TRUE|FALSE]
# eg.: main
# stands for:
#      GIT_TAG main
#      GIT_SHALLOW TRUE
# eg.: ver-3.3.6.0
# stands for:
#      GIT_TAG ver-3.3.6.0
#      GIT_SHALLOW TRUE
# eg.: ba3e38da6cba08a555bd67369b1829cde3dd0348:FALSE
# stands for:
#      GIT_TAG ba3e38da6cba08a555bd67369b1829cde3dd0348
#      GIT_SHALLOW FALSE
# NOTE: if you specify branch other than main, please change this to FALSE
#       otherwise you might encounter the error like:
#       error: pathspec 'xxx' did not match any file(s) known to git
if(NOT DEFINED TAOSADAPTER_GIT_TAG)
  set(TAOSADAPTER_GIT_TAG "3.3.6" CACHE STRING "which tag/branch/commit-sha1 to checkout for taosadapter.git" FORCE)
endif()

# preprocess TAOSADAPTER_GIT_TAG
string(REPLACE ":" ";" _kv "${TAOSADAPTER_GIT_TAG}:TRUE") # NOTE: set GIT_SHALLOW to TRUE by default
list(GET _kv 0 _k)
list(GET _kv 1 _v)
set(TAOSADAPTER_GIT_TAG_NAME    "${_k}" CACHE STRING "" FORCE)
if(${_v})
  set(TAOSADAPTER_GIT_TAG_SHALLOW TRUE CACHE BOOL "" FORCE)
else()
  set(TAOSADAPTER_GIT_TAG_SHALLOW FALSE CACHE BOOL "" FORCE)
endif()

# TAOSWS_GIT_TAG
# eg.: main
if(NOT DEFINED TAOSWS_GIT_TAG)
    set(TAOSWS_GIT_TAG "3.3.6" CACHE STRING "which tag/branch/commit-sha1 to checkout for taosws(rust connector)" FORCE)
endif()

# preprocess TAOSWS_GIT_TAG
string(REPLACE ":" ";" _kv "${TAOSWS_GIT_TAG}:TRUE") # NOTE: set GIT_SHALLOW to TRUE by default
list(GET _kv 0 _k)
list(GET _kv 1 _v)
set(TAOSWS_GIT_TAG_NAME    "${_k}" CACHE STRING "" FORCE)
if(${_v})
    set(TAOSWS_GIT_TAG_SHALLOW TRUE CACHE BOOL "" FORCE)
else()
    set(TAOSWS_GIT_TAG_SHALLOW FALSE CACHE BOOL "" FORCE)
endif()

IF(${TD_WINDOWS})
    IF(NOT TD_ASTRA)
        MESSAGE("build pthread Win32")
        option(
                BUILD_PTHREAD
                "If build pthread on Windows"
                ON
            )

        MESSAGE("build gnu regex for Windows")
        option(
                BUILD_GNUREGEX
                "If build gnu regex on Windows"
                ON
            )

        MESSAGE("build iconv Win32")
        option(
                BUILD_WITH_ICONV
                "If build iconv on Windows"
                ON
            )

        MESSAGE("build msvcregex Win32")
        option(
                BUILD_MSVCREGEX
                "If build msvcregex on Windows"
                ON
            )

        MESSAGE("build wcwidth Win32")
        option(
                BUILD_WCWIDTH
                "If build wcwidth on Windows"
                ON
            )

        MESSAGE("build wingetopt Win32")
        option(
                BUILD_WINGETOPT
                    "If build wingetopt on Windows"
                ON
            )

        option(
                TDENGINE_3
                "TDengine 3.x for taos-tools"
                ON
            )

        option(
                BUILD_CRASHDUMP
                "If build crashdump on Windows"
                ON
            )
    ENDIF ()
ELSEIF (TD_DARWIN_64)
    IF(${BUILD_TEST})
        add_definitions(-DCOMPILER_SUPPORTS_CXX13)
    ENDIF ()
ENDIF ()

option(
    BUILD_WITH_LEMON
    "If build with lemon"
    ON
)

option(
    BUILD_WITH_UDF
    "If build with UDF"
    ON
)

IF(NOT TD_ASTRA)
    option(
            BUILD_GEOS
            "If build with geos"
            ON
        )

    option(
        BUILD_SHARED_LIBS
        ""
        OFF
        )

    option(
        RUST_BINDINGS
        "If build with rust-bindings"
        ON
        )

    option(
        BUILD_PCRE2
        "If build with pcre2"
        ON
    )

    option(
        JEMALLOC_ENABLED
        "If build with jemalloc"
        OFF
        )

    option(
        BUILD_SANITIZER
        "If build sanitizer"
        OFF
        )

    option(
        BUILD_ADDR2LINE
        "If build addr2line"
        OFF
        )

    option(
        BUILD_WITH_LEVELDB
        "If build with leveldb"
        OFF
    )

    option(
        BUILD_WITH_ROCKSDB
        "If build with rocksdb"
        ON
    )

    option(
        BUILD_WITH_LZ4
        "If build with lz4"
        ON
    )
ELSE ()

    option(
        BUILD_WITH_LZMA2
        "If build with lzma2"
        ON
    )

ENDIF ()

ADD_DEFINITIONS(-DUSE_AUDIT)
ADD_DEFINITIONS(-DUSE_GEOS)
ADD_DEFINITIONS(-DUSE_UDF)
ADD_DEFINITIONS(-DUSE_STREAM)
ADD_DEFINITIONS(-DUSE_PRCE2)
ADD_DEFINITIONS(-DUSE_RSMA)
ADD_DEFINITIONS(-DUSE_TSMA)
ADD_DEFINITIONS(-DUSE_TQ)
ADD_DEFINITIONS(-DUSE_TOPIC)
ADD_DEFINITIONS(-DUSE_MONITOR)
ADD_DEFINITIONS(-DUSE_REPORT)

IF(${TD_ASTRA_RPC})
    ADD_DEFINITIONS(-DTD_ASTRA_RPC)
ENDIF()

IF(${TD_LINUX})

option(
    BUILD_S3
    "If build with s3"
    ON
)

option(
    BUILD_WITH_S3
    "If build with s3"
    ON
)

option(
    BUILD_WITH_COS
    "If build with cos"
    OFF
)

option(
    BUILD_WITH_LZMA2
    "If build with lzma2"
    ON
)

ENDIF ()

# NOTE: only ON under TD_LINUX
option(
    BUILD_WITH_ANALYSIS
    "If build with analysis"
    ${TD_LINUX}
)

# NOTE: set option variable in this ways is not a good practice
IF(NOT TD_ENTERPRISE)
  MESSAGE("switch s3 off with community version")
  set(BUILD_S3 OFF)
  set(BUILD_WITH_S3 OFF)
  set(BUILD_WITH_COS OFF)
  set(BUILD_WITH_ANALYSIS OFF)
ENDIF ()

# NOTE: set option variable in this ways is not a good practice
IF(${BUILD_WITH_ANALYSIS})
    message("build with analysis")
    set(BUILD_S3 ON)
    set(BUILD_WITH_S3 ON)
ENDIF()

# NOTE: set option variable in this ways is not a good practice
IF(${TD_LINUX})
    set(BUILD_WITH_ANALYSIS ON)
ENDIF()

IF(${BUILD_S3})

  IF(${BUILD_WITH_S3})

    add_definitions(-DUSE_S3)
    # NOTE: BUILD_WITH_S3 does NOT coexist with BUILD_WITH_COS?
    option(BUILD_WITH_COS "If build with cos" OFF)

  ELSE ()

    # NOTE: BUILD_WITH_S3 does NOT coexist with BUILD_WITH_COS?
    option(BUILD_WITH_COS "If build with cos" ON)

  ENDIF ()

ELSE ()

  option(BUILD_WITH_S3 "If build with s3" OFF)

  option(BUILD_WITH_COS "If build with cos" OFF)

ENDIF ()

IF(${TAOSD_INTEGRATED})
    add_definitions(-DTAOSD_INTEGRATED)
ENDIF()

IF(${TD_AS_LIB})
    add_definitions(-DTD_AS_LIB)
ENDIF()

option(
    BUILD_WITH_SQLITE
    "If build with sqlite"
    OFF
)

option(
    BUILD_WITH_BDB
    "If build with BDB"
    OFF
)

option(
    BUILD_WITH_LUCENE
    "If build with lucene"
    off
)

option(
    BUILD_WITH_NURAFT
    "If build with NuRaft"
    OFF
)

IF(NOT TD_ASTRA)

option(
    BUILD_WITH_UV
    "If build with libuv"
    ON
)

option(
    BUILD_WITH_UV_TRANS
    "If build with libuv_trans "
    ON
)

IF(${TD_LINUX} MATCHES TRUE)

option(
    BUILD_DEPENDENCY_TESTS
    "If build dependency tests"
    ON
)

ENDIF ()

option(
    BUILD_DOCS
    "If use doxygen build documents"
    OFF
)

option(
   BUILD_WITH_INVERTEDINDEX
   "If use invertedIndex"
   ON
)
ENDIF ()

option(
   BUILD_RELEASE
   "If build release version"
   OFF
)

option(
   BUILD_CONTRIB
   "If build thirdpart from source"
   OFF
)

message(STATUS "BUILD_S3:${BUILD_S3}")
message(STATUS "BUILD_WITH_S3:${BUILD_WITH_S3}")
message(STATUS "BUILD_WITH_COS:${BUILD_WITH_COS}")

