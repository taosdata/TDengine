CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/inc)

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

FIND_PACKAGE(Git)
IF(GIT_FOUND)
    IF (EXISTS "${CMAKE_CURRENT_LIST_DIR}/../VERSION")
        MESSAGE("Found VERSION file")
        EXECUTE_PROCESS(
            COMMAND grep "^taosdump" "${CMAKE_CURRENT_LIST_DIR}/../VERSION"
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_FULLTAG
            )
        EXECUTE_PROCESS(
            COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. log --pretty=oneline -n 1 HEAD"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_COMMIT_SHA1
            )
        EXECUTE_PROCESS(
            COMMAND grep "^taosbenchmark" "${CMAKE_CURRENT_LIST_DIR}/../VERSION"
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSBENCHMARK_FULLTAG
            )
        EXECUTE_PROCESS(
            COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. log --pretty=oneline -n 1 HEAD"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSBENCHMARK_COMMIT_SHA1
            )
    ELSE ()
        MESSAGE("Use git tag")
        EXECUTE_PROCESS(
            COMMAND sh -c "git for-each-ref --sort=taggerdate --format '%(tag)' refs/tags|grep taosdump|tail -1"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE TAG_RESULT
            OUTPUT_VARIABLE TAOSDUMP_FULLTAG
            )
        EXECUTE_PROCESS(
            COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. log --pretty=oneline -n 1 ${TAOSDUMP_FULLTAG}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_COMMIT_SHA1
            )
        EXECUTE_PROCESS(
            COMMAND sh -c "git for-each-ref --sort=taggerdate --format '%(tag)' refs/tags|grep taosbenchmark|tail -1"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE TAG_RESULT
            OUTPUT_VARIABLE TAOSBENCHMARK_FULLTAG
            )
        EXECUTE_PROCESS(
            COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. log --pretty=oneline -n 1 ${TAOSBENCHMARK_FULLTAG}"
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSBENCHMARK_COMMIT_SHA1
            )
    ENDIF ()
    EXECUTE_PROCESS(
        COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. status -z -s ${CMAKE_CURRENT_LIST_DIR}/taosdump.c"
        RESULT_VARIABLE RESULT
        OUTPUT_VARIABLE TAOSDUMP_STATUS
        ERROR_QUIET
        )

    # version    
    IF (DEFINED TD_VER_NUMBER)
        # use tdengine version
        SET(TAOSBENCHMARK_TAG ${TD_VER_NUMBER})   
        SET(TAOSDUMP_TAG ${TD_VER_NUMBER})
        MESSAGE(STATUS "use TD_VER_NUMBER version: " ${TD_VER_NUMBER})
    ELSE ()    
        # use internal version
        EXECUTE_PROCESS(
            COMMAND sh "-c" "echo '${TAOSDUMP_FULLTAG}' | awk -F '-' '{print $2}'"
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_TAG
            )
        MESSAGE(STATUS "taosdump use origin version: " ${TAOSDUMP_TAG})    
        EXECUTE_PROCESS(
            COMMAND sh "-c" "echo '${TAOSBENCHMARK_FULLTAG}' | awk -F '-' '{print $2}'"
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSBENCHMARK_TAG
            )
        MESSAGE(STATUS "taosBenchmark use origin version: " ${TAOSBENCHMARK_TAG})        
    ENDIF ()

    EXECUTE_PROCESS(
        COMMAND sh -c "git --git-dir=${CMAKE_CURRENT_LIST_DIR}/../.git --work-tree=${CMAKE_CURRENT_LIST_DIR}/.. status -z -s ${CMAKE_CURRENT_LIST_DIR}/bench*.c"
        RESULT_VARIABLE RESULT
        OUTPUT_VARIABLE TAOSBENCHMARK_STATUS
        ERROR_QUIET
        )
    IF ("${TAOSDUMP_COMMIT_SHA1}" STREQUAL "")
        SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${TAOSDUMP_COMMIT_SHA1}" 0 40 TAOSDUMP_COMMIT_SHA1)
        STRING(STRIP "${TAOSDUMP_COMMIT_SHA1}" TAOSDUMP_COMMIT_SHA1)
    ENDIF ()
    IF ("${TAOSDUMP_TAG}" STREQUAL "")
        SET(TAOSDUMP_TAG "0.1.0")
    ELSE ()
        STRING(STRIP "${TAOSDUMP_TAG}" TAOSDUMP_TAG)
    ENDIF ()

    IF ("${TAOSBENCHMARK_COMMIT_SHA1}" STREQUAL "")
        SET(TAOSBENCHMARK_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${TAOSBENCHMARK_COMMIT_SHA1}" 0 40 TAOSBENCHMARK_COMMIT_SHA1)
        STRING(STRIP "${TAOSBENCHMARK_COMMIT_SHA1}" TAOSBENCHMARK_COMMIT_SHA1)
    ENDIF ()
    IF ("${TAOSBENCHMARK_TAG}" STREQUAL "")
        SET(TAOSBENCHMARK_TAG "0.1.0")
    ELSE ()
        STRING(STRIP "${TAOSBENCHMARK_TAG}" TAOSBENCHMARK_TAG)
    ENDIF ()
ELSE()
    MESSAGE("Git not found")
    SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    SET(TAOSBENCHMARK_COMMIT_SHA1 "unknown")
    SET(TAOSDUMP_TAG "0.1.0")
    SET(TAOSDUMP_STATUS "unknown")
    SET(TAOSBENCHMARK_STATUS "unknown")
ENDIF (GIT_FOUND)

STRING(STRIP "${TAOSDUMP_STATUS}" TAOSDUMP_STATUS)
STRING(STRIP "${TAOSBENCHMARK_STATUS}" TAOSBENCHMARK_STATUS)

IF (TAOSDUMP_STATUS MATCHES "M")
    SET(TAOSDUMP_STATUS "modified")
ELSE()
    SET(TAOSDUMP_STATUS "")
ENDIF ()

IF (TAOSBENCHMARK_STATUS MATCHES "M")
    SET(TAOSBENCHMARK_STATUS "modified")
ELSE()
    SET(TAOSBENCHMARK_STATUS "")
ENDIF ()

MESSAGE("")
MESSAGE("taosdump last tag: ${TAOSDUMP_TAG}")
MESSAGE("taosdump commit: ${TAOSDUMP_COMMIT_SHA1}")
MESSAGE("taosdump status: ${TAOSDUMP_STATUS}")
MESSAGE("")
MESSAGE("taosBenchmark last tag: ${TAOSBENCHMARK_TAG}")
MESSAGE("taosBenchmark commit: ${TAOSBENCHMARK_COMMIT_SHA1}")
MESSAGE("taosBenchmark status: ${TAOSBENCHMARK_STATUS}")
MESSAGE("")

ADD_DEFINITIONS(-DTAOSDUMP_TAG="${TAOSDUMP_TAG}")
ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_SHA1="${TAOSDUMP_COMMIT_SHA1}")
ADD_DEFINITIONS(-DTAOSDUMP_STATUS="${TAOSDUMP_STATUS}")
ADD_DEFINITIONS(-DTAOSBENCHMARK_TAG="${TAOSBENCHMARK_TAG}")
ADD_DEFINITIONS(-DTAOSBENCHMARK_COMMIT_SHA1="${TAOSBENCHMARK_COMMIT_SHA1}")
ADD_DEFINITIONS(-DTAOSBENCHMARK_STATUS="${TAOSBENCHMARK_STATUS}")

LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/build/lib ${CMAKE_BINARY_DIR}/build/lib64)
LINK_DIRECTORIES(/usr/lib /usr/lib64)
INCLUDE_DIRECTORIES(/usr/local/taos/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/avro/lang/c/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/src)

if(NOT DEFINED TD_VER_COMPATIBLE)
    SET(TD_VER_COMPATIBLE "3.0.0.0")
ENDIF()

IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/pthread)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/msvcregex)
    ADD_DEFINITIONS(-DTD_VER_COMPATIBLE_3_0_0_0)
ENDIF ()

MESSAGE("Current system is: ${CMAKE_SYSTEM_NAME}")

IF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
    ADD_DEFINITIONS(-I${CMAKE_CURRENT_SOURCE_DIR}/../../../../community/include/util)
ENDIF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    SET(OS_ID "")

    IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
        ADD_DEFINITIONS(-DLINUX)

        EXECUTE_PROCESS (
                COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
                OUTPUT_VARIABLE OS_ID
        )
        MESSAGE(STATUS "The current OS is " ${OS_ID} "")

        ADD_LIBRARY(jansson STATIC IMPORTED)
        SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION
            "${CMAKE_BINARY_DIR}/build/lib/libjansson.a")

        ADD_LIBRARY(snappy STATIC IMPORTED)
        SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION
            "${CMAKE_BINARY_DIR}/build/lib/libsnappy.a")

        ADD_EXECUTABLE(taosdump taosdump.c toolstime.c toolsSys.c toolsDir.c toolsString.c)
        ADD_DEPENDENCIES(deps-snappy apache-avro)
        ADD_DEPENDENCIES(taosdump deps-jansson)
        ADD_DEPENDENCIES(taosdump deps-snappy)
        IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchTmq.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
        ELSE()
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
        ENDIF()
    ELSE ()
        INCLUDE_DIRECTORIES(/usr/local/include)
        ADD_DEFINITIONS(-DDARWIN)
        LINK_DIRECTORIES(/usr/local/lib)
        SET(OS_ID "Darwin")

        IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchTmq.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
        ELSE()
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
        ENDIF()
    ENDIF ()

    IF (${WEBSOCKET})
        ADD_DEFINITIONS(-DWEBSOCKET)
        INCLUDE_DIRECTORIES(/usr/local/include/)
        SET(WEBSOCKET_LINK_FLAGS "-ltaosws")

        IF (${CMAKE_PROJECT_NAME} STREQUAL "taos-tools")
            MESSAGE("libtaosws.so need to be installed first")
        ELSE ()
            ADD_DEPENDENCIES(taosBenchmark taosws-rs)
            IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
                ADD_DEPENDENCIES(taosdump taosws-rs)
            ELSE ()
                MESSAGE("TODO: taosdump for macOS is WIP")
            ENDIF ()
        ENDIF ()
    ELSE ()
        SET(WEBSOCKET_LINK_FLAGS "")
    ENDIF ()

    IF (${TOOLS_COVER} MATCHES "true")
        MESSAGE("${Green} build with coverage ${ColourReset}")
        SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
        SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
    ENDIF ()

    IF (${BUILD_SANITIZER})
        MESSAGE("${Yellow} Enable memory sanitize by BUILD_SANITIZER ${ColourReset}")
        IF (${OS_ID} MATCHES "Darwin")
            SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
        ELSE ()
            SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
        ENDIF ()
    ELSE ()
        SET(TOOLS_SANITIZE_FLAG "")
    ENDIF ()

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        IF (${TOOLS_SANITIZE} MATCHES "true")
            MESSAGE("${Yellow} Enable memory sanitize by TOOLS_SANITIZE ${ColourReset}")
            IF (${OS_ID} MATCHES "Darwin")
                SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
            ELSE ()
                SET(TOOLS_SANITIZE_FLAG "-fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
            ENDIF ()
        ELSE ()
            SET(TOOLS_SANITIZE_FLAG "")
        ENDIF ()

        IF (CMAKE_C_COMPILER_ID MATCHES "GNU")
            MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
            IF (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
                SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 ${TOOLS_SANITIZE_FLAG} -O0 -g3 -DDEBUG0ww0 -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS}")
            ELSE()
                MESSAGE("${Yellow} GCC version is equal or less than 4.8.5, which does not support memory sanitizer. Please upgrade to a higher version if you want to use it. ${ColourReset}")
                SET(CMAKE_C_FLAGS "-std=c99 -std=gnu11 -O0 -g3 -DDEBUG ${GCC_COVERAGE_COMPILE_FLAGS}")
            ENDIF()
        ELSE ()
            MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
            SET(CMAKE_C_FLAGS "-std=c99 -std=gnu11 -O0 -g3 -DDEBUG ${WEBSOCKET_LINK_FLAGS}")
        ENDIF ()

        IF (${OS_ID} MATCHES "alpine")
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            FIND_LIBRARY(LIBZ_LIBRARY z)
            MESSAGE(${LIBZ_LIBRARY})

            FIND_LIBRARY(LIBZ_LIBRARY z)
            MESSAGE(${ARGP_LIBRARY})

            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS})
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m argp $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS})
        ELSEIF(${OS_ID} MATCHES "Darwin")
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/)
            ENDIF ()
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson argp ${WEBSOCKET_LINK_FLAGS})
        ElSE ()
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
        ENDIF()

    ELSE ()
        MESSAGE("${Green} RELEASE mode use static avro library to link for release ${ColourReset}")
        IF (CMAKE_C_COMPILER_ID MATCHES "GNU")
            MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
            IF (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
                IF (${OS_ID} MATCHES "alpine")
                    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -O3 -DRELEASE -Wformat=2 -Wno-stringop-truncation -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
                ELSE ()
                    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -O3 -DRELEASE -Werror -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
                ENDIF ()
            ELSE ()
                SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -O3 -DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS}")
            ENDIF ()
        ELSE()
            SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -O3 -DRELEASE")
        ENDIF ()

        IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
            #            ADD_LIBRARY(jansson STATIC IMPORTED)
            #SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/jansson/lib/libjansson.a")

            # ADD_LIBRARY(snappy STATIC IMPORTED)
            # SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/snappy/lib/libsnappy.a")

            # ADD_LIBRARY(avro STATIC IMPORTED)
            # SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/avro-c/lib/libavro.a")
            #            TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy lzma z pthread)
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/)
            ENDIF ()

            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson argp ${WEBSOCKET_LINK_FLAGS})
        ELSE ()
            EXECUTE_PROCESS (
                COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
                OUTPUT_VARIABLE OS_ID
            )
            MESSAGE(STATUS "src's cmake detected current OS is " ${OS_ID} "")

            IF (${OS_ID} MATCHES "arch")
                MESSAGE(STATUS "${OS_ID} linux has no libatomic.a, use dynamic lib instead of")
            ELSE ()
                EXECUTE_PROCESS (
                    COMMAND gcc -print-file-name=libatomic.a
                    OUTPUT_VARIABLE LIBATOMIC_PATH
                )
                STRING(STRIP "${LIBATOMIC_PATH}" LIBATOMIC_PATH)
                MESSAGE(STATUS "libatomic.a path: ${LIBATOMIC_PATH}")
                ADD_LIBRARY(atomic STATIC IMPORTED)
                SET_PROPERTY(TARGET atomic PROPERTY IMPORTED_LOCATION "${LIBATOMIC_PATH}")
            ENDIF ()

            ADD_LIBRARY(avro STATIC IMPORTED)
            SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION
                "${CMAKE_BINARY_DIR}/build/lib/libavro.a")

            EXECUTE_PROCESS (
                    COMMAND gcc -print-file-name=libstdc++.a
                    OUTPUT_VARIABLE LIBSTDCXX_PATH
            )
            STRING(STRIP "${LIBSTDCXX_PATH}" LIBSTDCXX_PATH)
            MESSAGE("libstdc++.a path:" ${LIBSTDCXX_PATH})
            ADD_LIBRARY(stdc++ STATIC IMPORTED)
            SET_PROPERTY(TARGET stdc++ PROPERTY IMPORTED_LOCATION "${LIBSTDCXX_PATH}")

            EXECUTE_PROCESS (
                    COMMAND gcc -print-file-name=libz.a
                    OUTPUT_VARIABLE LIBZ_PATH
            )
            STRING(STRIP "${LIBZ_PATH}" LIBZ_PATH)
            MESSAGE("libza path:" ${LIBZ_PATH})
            ADD_LIBRARY(libz-static STATIC IMPORTED)
            SET_PROPERTY(TARGET libz-static PROPERTY IMPORTED_LOCATION "${LIBZ_PATH}")

            IF (${OS_ID} MATCHES "alpine")
                MESSAGE("alpine TARGET_LINK_LIBRARIES")
                FIND_LIBRARY(ARGP_LIBRARY argp)
                MESSAGE(${ARGP_LIBRARY})

                FIND_LIBRARY(LIBZ_LIBRARY z)
                MESSAGE(${LIBZ_LIBRARY})

                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma atomic pthread $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
                TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
            ELSE()
                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma libz-static atomic pthread ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
                TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson ${WEBSOCKET_LINK_FLAGS} ${GCC_COVERAGE_LINK_FLAGS})
            ENDIF()
        ENDIF ()

    ENDIF ()
ELSE ()
    ADD_DEFINITIONS(-DWINDOWS)
    SET(CMAKE_C_STANDARD 11)
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /utf-8")
    SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /utf-8")
    IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
        ADD_EXECUTABLE(taosBenchmark benchMain.c benchTmq.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsString.c toolsSys.c toolsString.c)
    ELSE ()
        ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
    ENDIF ()

    ADD_EXECUTABLE(taosdump taosdump.c toolsSys.c toolstime.c toolsDir.c toolsString.c)
    ADD_DEPENDENCIES(apache-avro tools-zlib)
    ADD_DEPENDENCIES(apache-avro deps-jansson)
    ADD_DEPENDENCIES(apache-avro deps-snappy)
    ADD_DEPENDENCIES(taosdump deps-jansson)
    ADD_DEPENDENCIES(taosdump deps-snappy)
    ADD_DEPENDENCIES(taosdump deps-libargp)
    ADD_DEPENDENCIES(taosdump apache-avro)

    IF (${WEBSOCKET})
        INCLUDE_DIRECTORIES(/usr/local/include/)
        SET(WEBSOCKET_LINK_FLAGS "taosws.lib")
        IF (${CMAKE_PROJECT_NAME} STREQUAL "taos-tools")
            MESSAGE("taosws.lib need to be installed first")
        ELSE ()
            ADD_DEPENDENCIES(taosBenchmark taosws-rs)
            ADD_DEPENDENCIES(taosdump taosws-rs)
        ENDIF ()
    ELSE ()
        SET(WEBSOCKET_LINK_FLAGS "")
    ENDIF ()

    target_include_directories(
        taosdump
        PUBLIC "${TD_SOURCE_DIR}/contrib/pthread"
        PUBLIC "${TD_SOURCE_DIR}/contrib/iconv"
    )

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlib ${WEBSOCKET_LINK_FLAGS})
    ELSE ()
        #SET(CMAKE_C_FLAGS "/w /D_WIN32 /DWIN32 /Zi /D NDEBUG /MTd")
        #       SET(CMAKE_C_FLAGS "/permissive- /GS /GL /Gy /Zc:wchar_t /Zi /Gm- /O2 /Zc:inline /fp:precise /D \"RELEASE\" /D \"NDEBUG\" /D \"_CONSOLE\" /D \"_UNICODE\" /D \"UNICODE\" /errorReport:prompt /Zc:forScope /Gd /Oi /MTd /FC /EHsc /nologo /diagnostics:column")
        #       SET(CMAKE_CXX_FLAGS "/permissive- /GS /GL /Gy /Zc:wchar_t /Zi /Gm- /O2 /sdl /Zc:inline /fp:precise /D \"NDEBUG\" /D \"_CONSOLE\" /D \"_UNICODE\" /D \"UNICODE\" /errorReport:prompt /Zc:forScope /Gd /Oi /MTd /FC /EHsc /nologo /diagnostics:column")
        #TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlibstatic ${WEBSOCKET_LINK_FLAGS} msvcrt.lib ucrtd.lib)
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlibstatic ${WEBSOCKET_LINK_FLAGS})
    ENDIF ()

    TARGET_LINK_LIBRARIES(taosBenchmark taos msvcregex pthread toolscJson ${WEBSOCKET_LINK_FLAGS})
ENDIF ()
