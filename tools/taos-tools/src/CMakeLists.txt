CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

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

# freemine: #
# freemine: # build proj
# freemine: #
# freemine: LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/build/lib ${CMAKE_BINARY_DIR}/build/lib64)
# freemine: LINK_DIRECTORIES(/usr/lib /usr/lib64)
# freemine: INCLUDE_DIRECTORIES(/usr/local/taos/include)

MESSAGE("Current system is: ${CMAKE_SYSTEM_NAME}")

IF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
    ADD_DEFINITIONS(-I${CMAKE_CURRENT_SOURCE_DIR}/../../../../community/include/util)
ENDIF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

# freemine: IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
# freemine:     SET(OS_ID "")
# freemine: 
# freemine:     IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
# freemine:         ADD_DEFINITIONS(-DLINUX)
# freemine: 
# freemine:         EXECUTE_PROCESS (
# freemine:                 COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
# freemine:                 OUTPUT_VARIABLE OS_ID
# freemine:         )
# freemine:         MESSAGE(STATUS "The current OS is " ${OS_ID} "")
# freemine: 
# freemine:         ADD_LIBRARY(jansson STATIC IMPORTED)
# freemine:         SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION
# freemine:             "${CMAKE_BINARY_DIR}/build/lib/libjansson.a")
# freemine: 
# freemine:         ADD_LIBRARY(snappy STATIC IMPORTED)
# freemine:         SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION
# freemine:             "${CMAKE_BINARY_DIR}/build/lib/libsnappy.a")
# freemine: 
# freemine:         ADD_EXECUTABLE(taosdump taosdump.c dumpUtil.c wsdump.c toolstime.c toolsSys.c toolsDir.c toolsString.c)
# freemine:         ADD_DEPENDENCIES(deps-snappy apache-avro)
# freemine:         ADD_DEPENDENCIES(taosdump deps-jansson)
# freemine:         ADD_DEPENDENCIES(taosdump deps-snappy)
# freemine:         ADD_EXECUTABLE(taosBenchmark benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
# freemine:     ELSE ()
# freemine:         INCLUDE_DIRECTORIES(/usr/local/include)
# freemine:         ADD_DEFINITIONS(-DDARWIN)
# freemine:         LINK_DIRECTORIES(/usr/local/lib)
# freemine:         SET(OS_ID "Darwin")
# freemine: 
# freemine:         ADD_EXECUTABLE(taosBenchmark benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
# freemine:     ENDIF ()
# freemine: 
# freemine:     # websocket
# freemine:     IF (${WEBSOCKET})
# freemine:         ADD_DEFINITIONS(-DWEBSOCKET)
# freemine:         # INCLUDE_DIRECTORIES(/usr/local/include/)
# freemine:         # SET(WEBSOCKET_LINK_FLAGS "-ltaosws")
# freemine: 
# freemine:         IF (${CMAKE_PROJECT_NAME} STREQUAL "taos-tools")
# freemine:             MESSAGE("libtaosws.so need to be installed first")
# freemine:         ELSE ()
# freemine:             DEP_ext_taosws(taosBenchmark)
# freemine:             IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
# freemine:                 DEP_ext_taosws(taosdump)
# freemine:             ELSE ()
# freemine:                 MESSAGE("TODO: taosdump for macOS is WIP")
# freemine:             ENDIF ()
# freemine:         ENDIF ()
# freemine:     ELSE ()
# freemine:         SET(WEBSOCKET_LINK_FLAGS "")
# freemine:     ENDIF ()
# freemine: 
# freemine:     IF (${TOOLS_COVER} MATCHES "true")
# freemine:         MESSAGE("${Green} build with coverage ${ColourReset}")
# freemine:         SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
# freemine:         SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
# freemine:     ENDIF ()
# freemine: 
# freemine:     # sanitizer
# freemine:     IF (${BUILD_SANITIZER})
# freemine:         MESSAGE("${Yellow} Enable memory sanitize by BUILD_SANITIZER ${ColourReset}")
# freemine:         IF (${OS_ID} MATCHES "Darwin")
# freemine:             SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
# freemine:         ELSE ()
# freemine:             SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
# freemine:         ENDIF ()
# freemine:     ELSE ()
# freemine:         SET(TOOLS_SANITIZE_FLAG "")
# freemine:     ENDIF ()
# freemine: 
# freemine:     # TOOLS_BUILD_TYPE
# freemine:     IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
# freemine:         IF ((${TOOLS_SANITIZE} MATCHES "true") OR (${BUILD_SANITIZER}))
# freemine:             MESSAGE("${Yellow} Enable memory sanitize by TOOLS_SANITIZE ${ColourReset}")
# freemine:             IF (${OS_ID} MATCHES "Darwin")
# freemine:                 SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
# freemine:             ELSE ()
# freemine:                 SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
# freemine:             ENDIF ()
# freemine:         ELSE ()
# freemine:             SET(TOOLS_SANITIZE_FLAG "")
# freemine:         ENDIF ()
# freemine: 
# freemine:         IF (CMAKE_C_COMPILER_ID MATCHES "GNU")
# freemine:             MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
# freemine:             IF (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
# freemine:                 SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 ${TOOLS_SANITIZE_FLAG} -O0 -g3 -DDEBUG0ww0 -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS}")
# freemine:             ELSE()
# freemine:                 MESSAGE("${Yellow} GCC version is equal or less than 4.8.5, which does not support memory sanitizer. Please upgrade to a higher version if you want to use it. ${ColourReset}")
# freemine:                 SET(CMAKE_C_FLAGS "-std=c99 -std=gnu11 -O0 -g3 -DDEBUG ${GCC_COVERAGE_COMPILE_FLAGS}")
# freemine:             ENDIF()
# freemine:         ELSE ()
# freemine:             MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
# freemine:             SET(CMAKE_C_FLAGS "-std=c99 -std=gnu11 -O0 -g3 -DDEBUG ${WEBSOCKET_LINK_FLAGS}")
# freemine:         ENDIF ()
# freemine: 
# freemine:         IF (${OS_ID} MATCHES "alpine")
# freemine:             MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
# freemine:             FIND_LIBRARY(LIBZ_LIBRARY z)
# freemine:             MESSAGE(${LIBZ_LIBRARY})
# freemine: 
# freemine:             FIND_LIBRARY(LIBZ_LIBRARY z)
# freemine:             MESSAGE(${ARGP_LIBRARY})
# freemine: 
# freemine:             TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS})
# freemine:             TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m argp $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS})
# freemine:         ELSEIF(${OS_ID} MATCHES "Darwin")
# freemine:             ADD_LIBRARY(argp STATIC IMPORTED)
# freemine:             IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
# freemine:                 SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
# freemine:                 INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
# freemine:             ELSE ()
# freemine:                 SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
# freemine:                 INCLUDE_DIRECTORIES(/usr/local/include/)
# freemine:             ENDIF ()
# freemine:             TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson argp ${WEBSOCKET_LINK_FLAGS})
# freemine:         ElSE ()
# freemine:             MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
# freemine:             TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:             TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:         ENDIF()
# freemine: 
# freemine:     ELSE ()
# freemine:         MESSAGE("${Green} RELEASE mode use static avro library to link for release ${ColourReset}")
# freemine:         IF (CMAKE_C_COMPILER_ID MATCHES "GNU")
# freemine:             MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
# freemine:             IF (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
# freemine:                 IF (${OS_ID} MATCHES "alpine")
# freemine:                     SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE -Wformat=2 -Wno-stringop-truncation -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
# freemine:                 ELSE ()
# freemine:                     SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE -Werror -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
# freemine:                 ENDIF ()
# freemine:             ELSE ()
# freemine:                 SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS}")
# freemine:             ENDIF ()
# freemine:         ELSE()
# freemine:             SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE")
# freemine:         ENDIF ()
# freemine: 
# freemine:         IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
# freemine:             #            ADD_LIBRARY(jansson STATIC IMPORTED)
# freemine:             #SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/jansson/lib/libjansson.a")
# freemine: 
# freemine:             # ADD_LIBRARY(snappy STATIC IMPORTED)
# freemine:             # SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/snappy/lib/libsnappy.a")
# freemine: 
# freemine:             # ADD_LIBRARY(avro STATIC IMPORTED)
# freemine:             # SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/avro-c/lib/libavro.a")
# freemine:             #            TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy lzma z pthread)
# freemine:             ADD_LIBRARY(argp STATIC IMPORTED)
# freemine:             IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
# freemine:                 SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
# freemine:                 INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
# freemine:             ELSE ()
# freemine:                 SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
# freemine:                 INCLUDE_DIRECTORIES(/usr/local/include/)
# freemine:             ENDIF ()
# freemine: 
# freemine:             TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE taos pthread m toolscJson argp ${WEBSOCKET_LINK_FLAGS})
# freemine:         ELSE ()
# freemine:             EXECUTE_PROCESS (
# freemine:                 COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
# freemine:                 OUTPUT_VARIABLE OS_ID
# freemine:             )
# freemine:             MESSAGE(STATUS "src's cmake detected current OS is " ${OS_ID} "")
# freemine: 
# freemine:             IF (${OS_ID} MATCHES "arch")
# freemine:                 MESSAGE(STATUS "${OS_ID} linux has no libatomic.a, use dynamic lib instead of")
# freemine:             ELSE ()
# freemine:                 EXECUTE_PROCESS (
# freemine:                     COMMAND gcc -print-file-name=libatomic.a
# freemine:                     OUTPUT_VARIABLE LIBATOMIC_PATH
# freemine:                 )
# freemine:                 STRING(STRIP "${LIBATOMIC_PATH}" LIBATOMIC_PATH)
# freemine:                 MESSAGE(STATUS "libatomic.a path: ${LIBATOMIC_PATH}")
# freemine:                 ADD_LIBRARY(atomic STATIC IMPORTED)
# freemine:                 SET_PROPERTY(TARGET atomic PROPERTY IMPORTED_LOCATION "${LIBATOMIC_PATH}")
# freemine:             ENDIF ()
# freemine: 
# freemine:             ADD_LIBRARY(avro STATIC IMPORTED)
# freemine:             SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION
# freemine:                 "${CMAKE_BINARY_DIR}/build/lib/libavro.a")
# freemine: 
# freemine:             EXECUTE_PROCESS (
# freemine:                     COMMAND gcc -print-file-name=libstdc++.a
# freemine:                     OUTPUT_VARIABLE LIBSTDCXX_PATH
# freemine:             )
# freemine:             STRING(STRIP "${LIBSTDCXX_PATH}" LIBSTDCXX_PATH)
# freemine:             MESSAGE("libstdc++.a path:" ${LIBSTDCXX_PATH})
# freemine:             ADD_LIBRARY(stdc++ STATIC IMPORTED)
# freemine:             SET_PROPERTY(TARGET stdc++ PROPERTY IMPORTED_LOCATION "${LIBSTDCXX_PATH}")
# freemine: 
# freemine:             EXECUTE_PROCESS (
# freemine:                     COMMAND gcc -print-file-name=libz.a
# freemine:                     OUTPUT_VARIABLE LIBZ_PATH
# freemine:             )
# freemine:             STRING(STRIP "${LIBZ_PATH}" LIBZ_PATH)
# freemine:             MESSAGE("libza path:" ${LIBZ_PATH})
# freemine:             ADD_LIBRARY(libz-static STATIC IMPORTED)
# freemine:             SET_PROPERTY(TARGET libz-static PROPERTY IMPORTED_LOCATION "${LIBZ_PATH}")
# freemine: 
# freemine:             IF (${OS_ID} MATCHES "alpine")
# freemine:                 MESSAGE("alpine TARGET_LINK_LIBRARIES")
# freemine:                 FIND_LIBRARY(ARGP_LIBRARY argp)
# freemine:                 MESSAGE(${ARGP_LIBRARY})
# freemine: 
# freemine:                 FIND_LIBRARY(LIBZ_LIBRARY z)
# freemine:                 MESSAGE(${LIBZ_LIBRARY})
# freemine: 
# freemine:                 TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma atomic pthread $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:                 TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:             ELSE()
# freemine:                 TARGET_LINK_LIBRARIES(taosdump PRIVATE taos avro jansson snappy stdc++ lzma libz-static atomic pthread ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:                 TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE taos pthread m toolscJson ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
# freemine:             ENDIF()
# freemine:         ENDIF ()
# freemine: 
# freemine:     ENDIF ()
# freemine: ELSE ()
# freemine: ENDIF ()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  ADD_DEFINITIONS(-DLINUX)
ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  ADD_DEFINITIONS(-DDARWIN)
ELSE ()
  ADD_DEFINITIONS(-DWINDOWS)
ENDIF ()
# freemine: SET(CMAKE_C_STANDARD 11)
# freemine: SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /utf-8")
# freemine: SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /utf-8")
ADD_EXECUTABLE(taosBenchmark
  benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c
  benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c
  benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c
  toolsString.c toolsSys.c toolsString.c
)

ADD_EXECUTABLE(taosdump
  taosdump.c dumpUtil.c wsdump.c toolsSys.c toolstime.c
  toolsDir.c toolsString.c
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

IF (${WEBSOCKET})
    # freemine: INCLUDE_DIRECTORIES(/usr/local/include/)
    # freemine: SET(WEBSOCKET_LINK_FLAGS "taosws.lib")
    # freemine: IF (${CMAKE_PROJECT_NAME} STREQUAL "taos-tools")
    # freemine:     MESSAGE("taosws.lib need to be installed first")
    # freemine: ELSE ()
    # freemine: ENDIF ()
    DEP_ext_taosws(taosBenchmark)
    DEP_ext_taosws(taosdump)
ENDIF ()

# freemine: IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
# freemine:     TARGET_LINK_LIBRARIES(taosdump taos ${WEBSOCKET_LINK_FLAGS})
# freemine: ELSE ()
# freemine:     #SET(CMAKE_C_FLAGS "/w /D_WIN32 /DWIN32 /Zi /D NDEBUG /MTd")
# freemine:     #       SET(CMAKE_C_FLAGS "/permissive- /GS /GL /Gy /Zc:wchar_t /Zi /Gm- /O2 /Zc:inline /fp:precise /D \"RELEASE\" /D \"NDEBUG\" /D \"_CONSOLE\" /D \"_UNICODE\" /D \"UNICODE\" /errorReport:prompt /Zc:forScope /Gd /Oi /MTd /FC /EHsc /nologo /diagnostics:column")
# freemine:     #       SET(CMAKE_CXX_FLAGS "/permissive- /GS /GL /Gy /Zc:wchar_t /Zi /Gm- /O2 /sdl /Zc:inline /fp:precise /D \"NDEBUG\" /D \"_CONSOLE\" /D \"_UNICODE\" /D \"UNICODE\" /errorReport:prompt /Zc:forScope /Gd /Oi /MTd /FC /EHsc /nologo /diagnostics:column")
# freemine:     #TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlibstatic ${WEBSOCKET_LINK_FLAGS} msvcrt.lib ucrtd.lib)
# freemine: ENDIF ()

TARGET_LINK_LIBRARIES(taosdump PRIVATE taos)
TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE taos)

if(NOT WIN32)
  target_link_libraries(taosdump PRIVATE stdc++)
  target_link_libraries(taosBenchmark PRIVATE stdc++)
  target_link_libraries(taosBenchmark PRIVATE m)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  check_include_file("argp.h" _have_argp "-I/usr/local/include")
  if(NOT _have_argp)
    message(FATAL_ERROR "please run:brew install argp-standalone")
  endif()
  # brew install argp-standalone
  target_include_directories(taosBenchmark PRIVATE /usr/local/include)
  target_include_directories(taosdump PRIVATE /usr/local/include)
  target_link_libraries(taosdump PRIVATE /usr/local/lib/libargp.a)
endif()

