aux_source_directory(src OS_SRC)
add_library(os STATIC ${OS_SRC})
target_include_directories(
    os
    PUBLIC "${TD_SOURCE_DIR}/include/os"
    PUBLIC "${TD_SOURCE_DIR}/include"
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PUBLIC "${TD_SOURCE_DIR}/contrib/pthread"
    PUBLIC "${TD_SOURCE_DIR}/contrib/iconv"
    PUBLIC "${TD_SOURCE_DIR}/contrib/msvcregex"
)
# iconv
if(TD_WINDOWS)
    find_path(IconvApiIncludes iconv.h "${TD_SOURCE_DIR}/contrib/iconv")
else()
    find_path(IconvApiIncludes iconv.h PATHS)
endif(TD_WINDOWS)
if(NOT IconvApiIncludes)
    add_definitions(-DDISALLOW_NCHAR_WITHOUT_ICONV) 
endif ()
if(USE_TD_MEMORY)
    add_definitions(-DUSE_TD_MEMORY) 
endif ()
if(BUILD_ADDR2LINE)
    if(NOT TD_WINDOWS)
        target_include_directories(
            os
            PUBLIC "${TD_SOURCE_DIR}/contrib/libdwarf/src/lib/libdwarf"
        )
        target_link_libraries(
            os PUBLIC addr2line dl z
        )
    endif()
    add_definitions(-DUSE_ADDR2LINE)
endif ()
if(CHECK_STR2INT_ERROR)
    add_definitions(-DTD_CHECK_STR_TO_INT_ERROR)
endif()
target_link_libraries(
    os
    PUBLIC pthread
    PUBLIC zlibstatic
)
if(TD_WINDOWS)
    target_link_libraries(
        os PUBLIC ws2_32 iconv msvcregex wcwidth winmm crashdump dbghelp version KtmW32
    )
elseif(TD_DARWIN_64)
    find_library(CORE_FOUNDATION_FRAMEWORK CoreFoundation)
    target_link_libraries(os  PUBLIC ${CORE_FOUNDATION_FRAMEWORK})
    find_library(SYSTEM_CONFIGURATION_FRAMEWORK SystemConfiguration)
    target_link_libraries(os  PUBLIC  ${SYSTEM_CONFIGURATION_FRAMEWORK})
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

IF (JEMALLOC_ENABLED)
    target_link_libraries(os PUBLIC -L${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
ENDIF ()

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
