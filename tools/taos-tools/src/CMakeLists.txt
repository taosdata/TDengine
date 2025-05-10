# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)

MESSAGE(STATUS "The current platform " ${CMAKE_SYSTEM_PROCESSOR} " is detected")

IF (CMAKE_SYSTEM_PROCESSOR MATCHES "(amd64)|(AMD64)|(x86_64)")
    MESSAGE(STATUS "The current platform is amd64")
    MESSAGE(STATUS "Set CPUTYPE to x64")
    SET(CURRENT_CPUTYPE "x64")
    SET(CURRENT_PLATFORM_ARCH_STR "amd64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)")
    MESSAGE(STATUS "The current platform is x86")
    MESSAGE(STATUS "Set CPUTYPE to x86")
    SET(CURRENT_CPUTYPE "x32")
    SET(CURRENT_PLATFORM_ARCH_STR "i386")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "armv7l")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_CPUTYPE "aarch32")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_PLATFORM_ARCH_STR "arm")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    SET(CURRENT_CPUTYPE "aarch64")
    MESSAGE(STATUS "Set CPUTYPE to aarch64")
    SET(CURRENT_PLATFORM_ARCH_STR "arm64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "mips64")
    SET(CURRENT_CPUTYPE "mips64")
    MESSAGE(STATUS "Set CPUTYPE to mips64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    SET(CURRENT_CPUTYPE "apple_m1")
    MESSAGE(STATUS "Set CPUTYPE to apple silicon m1")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "loongarch64")
    SET(CURRENT_CPUTYPE "loongarch64")
    MESSAGE(STATUS "Set CPUTYPE to loongarch64")
ENDIF ()

#
# collect --version information
#
MESSAGE("collect --version show info:")
# version
IF (DEFINED TD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${TD_VER_NUMBER}")
    MESSAGE(STATUS "version:${TD_VER_NUMBER}")
ELSE ()    
    # abort build
    MESSAGE(FATAL_ERROR "build taos-tools not found TD_VER_NUMBER define.")
ENDIF ()

# commit id
FIND_PACKAGE(Git)
IF(GIT_FOUND)
    # get
    EXECUTE_PROCESS(
        COMMAND git log -1 --format=%H
        WORKING_DIRECTORY ${TD_COMMUNITY_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_ID
        )    

    STRING(SUBSTRING "${GIT_COMMIT_ID}" 0 40 TAOSBENCHMARK_COMMIT_ID)
    SET(TAOSDUMP_COMMIT_ID "${TAOSBENCHMARK_COMMIT_ID}")

    # show
    MESSAGE(STATUS "taosdump commit id: ${TAOSDUMP_COMMIT_ID}")
    MESSAGE(STATUS "taosBenchmark commit id: ${TAOSBENCHMARK_COMMIT_ID}")
    # define
    ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_ID="${TAOSDUMP_COMMIT_ID}")
    ADD_DEFINITIONS(-DTAOSBENCHMARK_COMMIT_ID="${TAOSBENCHMARK_COMMIT_ID}")        
ELSE()
    MESSAGE(FATAL_ERROR "build taos-tools FIND_PACKAGE(Git) failed.")
ENDIF (GIT_FOUND)

# build info
SET(BUILD_INFO "${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}")
ADD_DEFINITIONS(-DBUILD_INFO="${BUILD_INFO}")
MESSAGE(STATUS "build:${BUILD_INFO}")
MESSAGE("")

MESSAGE("Current system is: ${CMAKE_SYSTEM_NAME}")

IF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
    ADD_DEFINITIONS(-I${CMAKE_CURRENT_SOURCE_DIR}/../../../../community/include/util)
ENDIF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  ADD_DEFINITIONS(-DLINUX)
ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  ADD_DEFINITIONS(-DDARWIN)
ELSE ()
  ADD_DEFINITIONS(-DWINDOWS)
ENDIF ()
ADD_EXECUTABLE(taosBenchmark
  benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c
  benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c
  benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c
  toolsSys.c toolsString.c ../../src/pub.c
)

ADD_EXECUTABLE(taosdump
  taosdump.c dumpUtil.c toolsSys.c toolstime.c
  toolsDir.c toolsString.c ../../src/pub.c
)
DEP_ext_avro(taosdump)
DEP_ext_zlib(taosdump)
DEP_ext_jansson(taosdump)
DEP_ext_snappy(taosdump)
DEP_ext_xz(taosdump)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  DEP_ext_iconv(taosdump)
  DEP_ext_msvcregex(taosdump)
  DEP_ext_pthread(taosdump)

  DEP_ext_pthread(taosBenchmark)
  DEP_ext_msvcregex(taosBenchmark)
  target_include_directories(taosdump PRIVATE "$<TARGET_PROPERTY:libargp,SOURCE_DIR>/argp")
  target_link_libraries(taosdump PRIVATE libargp)
ENDIF ()
target_link_libraries(taosdump PRIVATE toolscJson)
TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE toolscJson)

TARGET_LINK_LIBRARIES(taosdump PRIVATE taos)
TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE taos)

if(NOT WIN32)
  target_link_libraries(taosdump PRIVATE stdc++)
  target_link_libraries(taosBenchmark PRIVATE stdc++)
  target_link_libraries(taosBenchmark PRIVATE m)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  check_include_file("argp.h" _have_argp "-I${BREW_PREFIX}/include")
  if(NOT _have_argp)
    message(FATAL_ERROR "please run:brew install argp-standalone")
  endif()
  # brew install argp-standalone
  target_include_directories(taosBenchmark PRIVATE ${BREW_PREFIX}/include)
  target_include_directories(taosdump PRIVATE ${BREW_PREFIX}/include)
  target_link_libraries(taosdump PRIVATE ${BREW_PREFIX}/lib/libargp.a)
endif()

