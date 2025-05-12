aux_source_directory(src OS_SRC)

if(NOT ${TD_WINDOWS})
    add_definitions(-DTHREAD_SAFE=1)
    aux_source_directory(src/timezone OS_TZ)
    add_library(os STATIC ${OS_SRC} ${OS_TZ})
    DEP_ext_tz(os)
else()
    add_library(os STATIC ${OS_SRC})
endif(NOT ${TD_WINDOWS})
if(${BUILD_MSVCREGEX})
    DEP_ext_msvcregex(os)
endif()
if(${BUILD_PTHREAD})
    DEP_ext_pthread(os)
else()
    target_link_libraries(os PUBLIC Threads::Threads)
endif()
if(${BUILD_WITH_ICONV})
    DEP_ext_iconv(os)
endif()
if(${BUILD_WCWIDTH})
    DEP_ext_wcwidth(os)
endif()
if(${BUILD_CRASHDUMP})
    DEP_ext_crashdump(os)
endif()
target_include_directories(
    os
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PUBLIC "${TD_SOURCE_DIR}/include"
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

# iconv
# NOTE: since we use DEP_ext_iconv under windows
#       reconsider using find_path or not later!!!
#       btw, on macos, `brew install iconv` is an prerequisite
if(TD_WINDOWS)
    find_path(IconvApiIncludes iconv.h "${TD_SOURCE_DIR}/contrib/iconv")
else()
    find_path(IconvApiIncludes iconv.h PATHS)
endif(TD_WINDOWS)

if(NOT IconvApiIncludes)
    add_definitions(-DDISALLOW_NCHAR_WITHOUT_ICONV)
endif()

if(USE_TD_MEMORY)
    add_definitions(-DUSE_TD_MEMORY)
endif()

if(BUILD_WITH_RAND_ERR)
    add_definitions(-DBUILD_WITH_RAND_ERR)
endif()

if(BUILD_ADDR2LINE)
    if(NOT TD_WINDOWS)
        target_link_libraries(
            os PUBLIC addr2line dl
        )
    endif()

    add_definitions(-DUSE_ADDR2LINE)
endif()

if(CHECK_STR2INT_ERROR)
    add_definitions(-DTD_CHECK_STR_TO_INT_ERROR)
endif()
if(NOT ${TD_WINDOWS})
    target_link_libraries(os PUBLIC pthread)
endif()
DEP_ext_zlib(os)
if(TD_WINDOWS)
    target_link_libraries(
        os PUBLIC ws2_32 winmm dbghelp version KtmW32 Iphlpapi Userenv
    )
elseif(TD_DARWIN_64)
    find_library(CORE_FOUNDATION_FRAMEWORK CoreFoundation)
    target_link_libraries(os PUBLIC ${CORE_FOUNDATION_FRAMEWORK})
    find_library(SYSTEM_CONFIGURATION_FRAMEWORK SystemConfiguration)
    target_link_libraries(os PUBLIC ${SYSTEM_CONFIGURATION_FRAMEWORK})
    target_link_libraries(
        os PUBLIC dl m iconv
    )
elseif(TD_ALPINE)
    target_link_libraries(
        os PUBLIC dl m rt unwind
    )
else()
    target_link_libraries(
        os PUBLIC dl m rt
    )
endif()


if(JEMALLOC_ENABLED)
    add_dependencies(os jemalloc)
endif()

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})

