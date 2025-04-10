CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/inc)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/os)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/util)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/libs/decimal)


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

#
# build proj
#
LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/build/lib ${CMAKE_BINARY_DIR}/build/lib64)
LINK_DIRECTORIES(/usr/lib /usr/lib64)
INCLUDE_DIRECTORIES(/usr/local/taos/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/avro/lang/c/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../../inc)

INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/pthread)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/msvcregex)

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

        ADD_EXECUTABLE(taosdump taosdump.c ../../src/pub.c dumpUtil.c toolstime.c toolsSys.c toolsDir.c toolsString.c)
        ADD_DEPENDENCIES(deps-snappy apache-avro)
        ADD_DEPENDENCIES(taosdump deps-jansson)
        ADD_DEPENDENCIES(taosdump deps-snappy)
        ADD_EXECUTABLE(taosBenchmark benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c ../../src/pub.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
    ELSE ()
        INCLUDE_DIRECTORIES(/usr/local/include)
        ADD_DEFINITIONS(-DDARWIN)
        LINK_DIRECTORIES(/usr/local/lib)
        SET(OS_ID "Darwin")

        ADD_EXECUTABLE(taosBenchmark benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c ../../src/pub.c benchUtilDs.c benchSys.c toolstime.c toolsSys.c toolsString.c)
    ENDIF ()

    IF (${TOOLS_COVER} MATCHES "true")
        MESSAGE("${Green} build with coverage ${ColourReset}")
        SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
        SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
    ENDIF ()

    # sanitizer
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

    # TOOLS_BUILD_TYPE
    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        IF ((${TOOLS_SANITIZE} MATCHES "true") OR (${BUILD_SANITIZER}))
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
            SET(CMAKE_C_FLAGS "-std=c99 -std=gnu11 -O0 -g3 -DDEBUG ")
        ENDIF ()

        IF (${OS_ID} MATCHES "alpine")
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            FIND_LIBRARY(LIBZ_LIBRARY z)
            MESSAGE(${LIBZ_LIBRARY})

            FIND_LIBRARY(LIBZ_LIBRARY z)
            MESSAGE(${ARGP_LIBRARY})

            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> )
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m argp $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}> )
        ELSEIF(${OS_ID} MATCHES "Darwin")
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/)
            ENDIF ()
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson argp )
        ElSE ()
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread m  ${GCC_COVERAGE_LINK_FLAGS})
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson  ${GCC_COVERAGE_LINK_FLAGS})
        ENDIF()

    ELSE ()
        MESSAGE("${Green} RELEASE mode use static avro library to link for release ${ColourReset}")
        IF (CMAKE_C_COMPILER_ID MATCHES "GNU")
            MESSAGE("Compiler is: ${CMAKE_C_COMPILER_ID}, version: ${CMAKE_C_COMPILER_VERSION}")
            IF (CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
                IF (${OS_ID} MATCHES "alpine")
                    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE -Wformat=2 -Wno-stringop-truncation -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
                ELSE ()
                    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE -Werror -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation ${GCC_COVERAGE_COMPILE_FLAGS} ${TOOLS_SANITIZE_FLAG}")
                ENDIF ()
            ELSE ()
                SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS}")
            ENDIF ()
        ELSE()
            SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -g -O3 -DRELEASE")
        ENDIF ()

        IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/)
            ENDIF ()

            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson argp )
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

                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma atomic pthread $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}>  ${GCC_COVERAGE_LINK_FLAGS})
                TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson $<$<BOOL:${LIBZ_LIBRARY}>:${LIBZ_LIBRARY}> $<$<BOOL:${ARGP_LIBRARY}>:${ARGP_LIBRARY}>  ${GCC_COVERAGE_LINK_FLAGS})
            ELSE()
                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma libz-static atomic pthread  ${GCC_COVERAGE_LINK_FLAGS})
                TARGET_LINK_LIBRARIES(taosBenchmark taos pthread m toolscJson  ${GCC_COVERAGE_LINK_FLAGS})
            ENDIF()
        ENDIF ()

    ENDIF ()

    TARGET_LINK_LIBRARIES(taosBenchmark z)

ELSE ()
    ADD_DEFINITIONS(-DWINDOWS)
    SET(CMAKE_C_STANDARD 11)
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /utf-8 ")
    SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /utf-8 ")
    ADD_EXECUTABLE(taosBenchmark benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c benchCommandOpt.c benchUtil.c ../../src/pub.c benchUtilDs.c benchSys.c toolstime.c toolsString.c toolsSys.c toolsString.c)

    ADD_EXECUTABLE(taosdump taosdump.c ../../src/pub.c dumpUtil.c toolsSys.c toolstime.c toolsDir.c toolsString.c)
    ADD_DEPENDENCIES(apache-avro tools-zlib)
    ADD_DEPENDENCIES(apache-avro deps-jansson)
    ADD_DEPENDENCIES(apache-avro deps-snappy)
    ADD_DEPENDENCIES(taosdump deps-jansson)
    ADD_DEPENDENCIES(taosdump deps-snappy)
    ADD_DEPENDENCIES(taosdump deps-libargp)
    ADD_DEPENDENCIES(taosdump apache-avro)
    ADD_DEPENDENCIES(taosBenchmark tools-zlib)

    target_include_directories(
        taosdump
        PUBLIC "${TD_SOURCE_DIR}/contrib/pthread"
        PUBLIC "${TD_SOURCE_DIR}/contrib/iconv"
    )

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlib )
    ELSE ()
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy pthread libargp.lib zlibstatic )
    ENDIF ()

    TARGET_LINK_LIBRARIES(taosBenchmark taos msvcregex pthread toolscJson )

    TARGET_LINK_LIBRARIES(taosBenchmark zlibstatic)

ENDIF ()

