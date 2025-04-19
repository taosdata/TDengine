set(CMAKE_VERBOSE_MAKEFILE FALSE)
set(TD_BUILD_TAOSA_INTERNAL FALSE)
set(TD_BUILD_KEEPER_INTERNAL FALSE)

# set output directory
SET(TD_BUILD_DIR ${PROJECT_BINARY_DIR}/build)
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/bin)
SET(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/lib)
if(${TD_WINDOWS})
    # adapt to the rule of DLL searching on Windows
    SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/bin)
else()
    # better set rpath to $ORIGIN/../lib accordingly
    SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TD_BUILD_DIR}/lib)
endif()
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
MESSAGE(STATUS "Project library files output path: " ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY})

IF(NOT DEFINED TD_GRANT)
    SET(TD_GRANT FALSE)
ENDIF()

IF(NOT DEFINED BUILD_WITH_RAND_ERR)
    SET(BUILD_WITH_RAND_ERR FALSE)
ELSE()
    SET(BUILD_WITH_RAND_ERR TRUE)
ENDIF()

IF("${WEBSOCKET}" MATCHES "true")
    SET(TD_WEBSOCKET TRUE)
    MESSAGE("Enable websocket")
    ADD_DEFINITIONS(-DWEBSOCKET)
ELSE()
    SET(TD_WEBSOCKET FALSE)
ENDIF()

IF("${BUILD_HTTP}" STREQUAL "")
    IF(TD_LINUX)
        IF(TD_ARM_32)
            SET(TD_BUILD_HTTP TRUE)
        ELSE()
            SET(TD_BUILD_HTTP TRUE)
        ENDIF()
    ELSEIF(TD_DARWIN)
        SET(TD_BUILD_HTTP TRUE)
    ELSE()
        SET(TD_BUILD_HTTP TRUE)
    ENDIF()
ELSEIF(${BUILD_HTTP} MATCHES "false")
    SET(TD_BUILD_HTTP FALSE)
ELSEIF(${BUILD_HTTP} MATCHES "true")
    SET(TD_BUILD_HTTP TRUE)
ELSEIF(${BUILD_HTTP} MATCHES "internal")
    SET(TD_BUILD_HTTP FALSE)
    SET(TD_BUILD_TAOSA_INTERNAL TRUE)
ELSE()
    SET(TD_BUILD_HTTP TRUE)
ENDIF()

IF(TD_BUILD_HTTP)
    ADD_DEFINITIONS(-DHTTP_EMBEDDED)
ENDIF()

IF("${BUILD_KEEPER}" STREQUAL "")
    SET(TD_BUILD_KEEPER FALSE)
ELSEIF(${BUILD_KEEPER} MATCHES "false")
    SET(TD_BUILD_KEEPER FALSE)
ELSEIF(${BUILD_KEEPER} MATCHES "true")
    SET(TD_BUILD_KEEPER TRUE)
ELSEIF(${BUILD_KEEPER} MATCHES "internal")
    SET(TD_BUILD_KEEPER FALSE)
    SET(TD_BUILD_KEEPER_INTERNAL TRUE)
ELSE()
    SET(TD_BUILD_KEEPER FALSE)
ENDIF()

IF("${BUILD_TOOLS}" STREQUAL "")
    IF(TD_LINUX)
        IF(TD_ARM_32)
            SET(BUILD_TOOLS "false")
        ELSEIF(TD_ARM_64)
            SET(BUILD_TOOLS "false")
        ELSE()
            SET(BUILD_TOOLS "false")
        ENDIF()
    ELSEIF(TD_DARWIN)
        SET(BUILD_TOOLS "false")
    ELSE()
        SET(BUILD_TOOLS "false")
    ENDIF()
ENDIF()

IF("${BUILD_TOOLS}" MATCHES "false")
    MESSAGE("${Yellow} Will _not_ build taos_tools! ${ColourReset}")
    SET(TD_TAOS_TOOLS FALSE)
ELSE()
    MESSAGE("")
    MESSAGE("${Green} Will build taos_tools! ${ColourReset}")
    MESSAGE("")
    SET(TD_TAOS_TOOLS TRUE)
ENDIF()

SET(TAOS_LIB taos)
SET(TAOS_LIB_STATIC taos_static)
SET(TAOS_NATIVE_LIB taosnative)
SET(TAOS_NATIVE_LIB_STATIC taosnative_static)

# build TSZ by default
IF("${TSZ_ENABLED}" MATCHES "false")
    set(VAR_TSZ "" CACHE INTERNAL "global variant empty")
ELSE()
    # define add
    MESSAGE(STATUS "build with TSZ enabled")
    ADD_DEFINITIONS(-DTD_TSZ)
    set(VAR_TSZ "TSZ" CACHE INTERNAL "global variant tsz")
ENDIF()

IF(TD_WINDOWS)
    MESSAGE("${Yellow} set compiler flag for Windows! ${ColourReset}")

    IF(${CMAKE_BUILD_TYPE} MATCHES "Release")
        MESSAGE("${Green} will build Release version! ${ColourReset}")
        # NOTE: let cmake to choose default compile options
        message(STATUS "do NOT forget to remove the following line and check if it works or not!!!")
        SET(COMMON_FLAGS "/W3 /D_WIN32 /DWIN32 /Zi- /O2 /GL /MD")
    ELSE()
        MESSAGE("${Green} will build Debug version! ${ColourReset}")
        # NOTE: let cmake to choose default compile options
        # SET(COMMON_FLAGS "/w /D_WIN32 /DWIN32 /Zi /MDd")
    ENDIF()

    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /MANIFEST:NO /FORCE:MULTIPLE")

    # IF (MSVC AND (MSVC_VERSION GREATER_EQUAL 1900))
    # SET(COMMON_FLAGS "${COMMON_FLAGS} /Wv:18")
    # ENDIF ()
    IF(CMAKE_DEPFILE_FLAGS_C)
        SET(CMAKE_DEPFILE_FLAGS_C "")
    ENDIF()

    IF(CMAKE_DEPFILE_FLAGS_CXX)
        SET(CMAKE_DEPFILE_FLAGS_CXX "")
    ENDIF()

    IF(CMAKE_C_FLAGS_DEBUG)
        SET(CMAKE_C_FLAGS_DEBUG "" CACHE STRING "" FORCE)
    ENDIF()

    IF(CMAKE_CXX_FLAGS_DEBUG)
        SET(CMAKE_CXX_FLAGS_DEBUG "" CACHE STRING "" FORCE)
    ENDIF()

    # ref: https://learn.microsoft.com/en-us/cpp/error-messages/compiler-warnings/compiler-warning-level-1-c4819?view=msvc-170
    set(_c_cxx_flags_list
        /W3 /WX
        /wd4311      # 'variable' : pointer truncation from 'type' to 'type'
        /wd4312      # 'operation' : conversion from 'type1' to 'type2' of greater size
        /wd4022      # 'function' : pointer mismatch for actual parameter 'number'
        /wd4013      # 'function' undefined; assuming extern returning int
        /wd4090      # 'operation' : different 'modifier' qualifiers
        /wd4996      # Your code uses a function, class member, variable, or typedef that's marked deprecated
        /wd4819      # The file contains a character that cannot be represented in the current code page (number)
        /wd4101      # The local variable is never used
        /wd4244      # 'argument' : conversion from 'type1' to 'type2', possible loss of data
        /wd4267      # 'var' : conversion from 'size_t' to 'type', possible loss of data
        /wd4098      # 'function' : void function returning a value
        /wd4047      # 'operator' : 'identifier1' differs in levels of indirection from 'identifier2'
        /wd4133      # 'expression': incompatible types - from 'type1' to 'type2'
        /wd4715      # 'function' : not all control paths return a value
        /wd4018      # 'token' : signed/unsigned mismatch
        /wd4716      # 'function' must return a value
        /wd4305      # 'conversion': truncation from 'type1' to 'type2'
        /wd4102      # 'label' : unreferenced label
        /wd4146      # unary minus operator applied to unsigned type, result still unsigned
        /wd4005      # 'identifier' : macro redefinition
        /wd4273      # 'function' : inconsistent DLL linkage
        /wd4068      # unknown pragma
        /wd4003      # not enough arguments for function-like macro invocation 'identifier'
        /wd4081      # expected 'token1'; found 'token2'
        /wd4113      # 'identifier1' differs in parameter lists from 'identifier2'
        /wd4477      # 'function' : format string 'string' requires an argument of type 'type', but variadic argument number has type 'type'
        /wd4293      # 'operator' : shift count negative or too big, undefined behavior
        /wd4805      # 'operation' : unsafe mix of type 'type' and type 'type' in operation
        /wd4334      # 'operator': result of 32-bit shift implicitly converted to 64 bits (was 64-bit shift intended?)
        /wd4307      # 'operator' : signed integral constant overflow
        /wd4200      # nonstandard extension used: zero-sized array in struct/union
                     # C++ only: This member will be ignored by a defaulted constructor or copy/move assignment operator
        /wd4309      # 'conversion' : truncation of constant value
        /wd4028      # formal parameter 'number' different from declaration
    )
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")

ELSE()
    IF(${TD_DARWIN})
        set(CMAKE_MACOSX_RPATH 0)
    ENDIF()

    set(_c_cxx_flags_list
      -Wno-unused-result
    )
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS} ${_c_cxx_flags}")

    IF(${COVER} MATCHES "true")
        MESSAGE(STATUS "Test coverage mode, add extra flags")
        SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
        SET(GCC_COVERAGE_LINK_FLAGS "-lgcov --coverage")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
    ENDIF()

    # disable all assert
    IF((${DISABLE_ASSERT} MATCHES "true") OR(${DISABLE_ASSERTS} MATCHES "true"))
        ADD_DEFINITIONS(-DDISABLE_ASSERT)
        MESSAGE(STATUS "Disable all asserts")
    ENDIF()

    INCLUDE(CheckCCompilerFlag)

    IF(TD_ARM_64 OR TD_ARM_32)
        SET(COMPILER_SUPPORT_SSE42 false)
    ELSEIF(("${CMAKE_C_COMPILER_ID}" MATCHES "Clang") OR("${CMAKE_C_COMPILER_ID}" MATCHES "AppleClang"))
        SET(COMPILER_SUPPORT_SSE42 true)
        MESSAGE(STATUS "Always enable sse4.2 for Clang/AppleClang")
    ELSE()
        CHECK_C_COMPILER_FLAG("-msse4.2" COMPILER_SUPPORT_SSE42)
    ENDIF()

    IF(TD_ARM_64 OR TD_ARM_32)
        SET(COMPILER_SUPPORT_FMA false)
        SET(COMPILER_SUPPORT_AVX false)
        SET(COMPILER_SUPPORT_AVX2 false)
        SET(COMPILER_SUPPORT_AVX512F false)
        SET(COMPILER_SUPPORT_AVX512BMI false)
        SET(COMPILER_SUPPORT_AVX512VL false)
    ELSE()
        CHECK_C_COMPILER_FLAG("-mfma" COMPILER_SUPPORT_FMA)
        CHECK_C_COMPILER_FLAG("-mavx" COMPILER_SUPPORT_AVX)
        CHECK_C_COMPILER_FLAG("-mavx2" COMPILER_SUPPORT_AVX2)
        CHECK_C_COMPILER_FLAG("-mavx512f" COMPILER_SUPPORT_AVX512F)
        CHECK_C_COMPILER_FLAG("-mavx512vbmi" COMPILER_SUPPORT_AVX512BMI)
        CHECK_C_COMPILER_FLAG("-mavx512vl" COMPILER_SUPPORT_AVX512VL)
    ENDIF()

    IF(COMPILER_SUPPORT_SSE42)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2")
    ENDIF()

    IF("${SIMD_SUPPORT}" MATCHES "true")
        IF(COMPILER_SUPPORT_FMA)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mfma")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mfma")
            MESSAGE(STATUS "FMA instructions is ACTIVATED")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
            MESSAGE(STATUS "AVX instructions is ACTIVATED")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX2)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx2")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2")
            MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
        ENDIF()
    ENDIF()

    IF("${SIMD_AVX512_SUPPORT}" MATCHES "true")
        IF(COMPILER_SUPPORT_AVX512F AND COMPILER_SUPPORT_AVX512BMI)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx512f -mavx512vbmi")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512f -mavx512vbmi")
            MESSAGE(STATUS "avx512f/avx512bmi enabled by compiler")
        ENDIF()

        IF(COMPILER_SUPPORT_AVX512VL)
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mavx512vl")
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx512vl")
            MESSAGE(STATUS "avx512vl enabled by compiler")
        ENDIF()
    ENDIF()

    # build mode
    SET(CMAKE_C_FLAGS_REL "${CMAKE_C_FLAGS} -Werror -Werror=return-type -fPIC -O3 -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation -Wno-format-y2k")
    SET(CMAKE_CXX_FLAGS_REL "${CMAKE_CXX_FLAGS} -Werror -Wno-reserved-user-defined-literal -Wno-literal-suffix -Werror=return-type -fPIC -O3 -Wformat=2 -Wno-format-nonliteral -Wno-format-truncation -Wno-format-y2k")

    IF(${BUILD_SANITIZER})
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}     -Werror -Werror=return-type -fPIC -gdwarf-2 -fsanitize=address -fsanitize=undefined -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment -g3 -Wformat=0")

        # SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix -Werror=return-type -fPIC -gdwarf-2 -fsanitize=address -fsanitize=undefined -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment -g3 -Wformat=0")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-literal-suffix -Werror=return-type -fPIC -gdwarf-2 -fsanitize=address -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment -g3 -Wformat=0")
        MESSAGE(STATUS "Compile with Address Sanitizer!")
    ELSEIF(${BUILD_RELEASE})
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")
    elseif(TD_LINUX)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -fPIC -g3 -gdwarf-2 -Wno-format-truncation -Wno-write-strings -Wno-format-overflow -Wno-stringop-overread")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -fPIC -g3 -gdwarf-2 -Wno-format-truncation -Wno-write-strings -Wno-format-overflow -Wno-conversion-null -Wno-stringop-overread")
    elseif(TD_DARWIN)
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror -Werror=return-type -fPIC -g3 -gdwarf-2 -Wformat=2 -Wno-format-nonliteral -Wno-format-y2k -Wno-deprecated-declarations")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror -Werror=return-type -fPIC -g3 -gdwarf-2 -Wno-reserved-user-defined-literal -Wformat=2 -Wno-format-nonliteral -Wno-format-y2k -Wno-deprecated-declarations -Wno-literal-conversion -Wno-writable-strings -Wno-unused-value -Wno-format -Wno-null-conversion")
    ELSE()
        message(FATAL_ERROR "not implemented yet")
    ENDIF()
ENDIF()

IF(TD_LINUX_64)
    # NOTE: need to test
    IF(${JEMALLOC_ENABLED})
        MESSAGE(STATUS "JEMALLOC Enabled")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=attributes")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=attributes")
        SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
        ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib)
    ELSE()
        MESSAGE(STATUS "JEMALLOC Disabled")
        SET(LINK_JEMALLOC "")
    ENDIF()
ENDIF()
