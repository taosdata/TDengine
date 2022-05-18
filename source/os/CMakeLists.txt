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
find_path(IconvApiIncludes iconv.h PATHS)
if(NOT IconvApiIncludes)
    add_definitions(-DDISALLOW_NCHAR_WITHOUT_ICONV) 
endif ()
if(USE_TD_MEMORY)
    add_definitions(-DUSE_TD_MEMORY) 
endif ()
if(BUILD_ADDR2LINE)
    target_include_directories(
        os
        PUBLIC "${TD_SOURCE_DIR}/contrib/libdwarf/src/lib/libdwarf"
    )
    add_definitions(-DUSE_ADDR2LINE)
    target_link_libraries(
        os PUBLIC addr2line dl z
    )
endif ()
target_link_libraries(
    os PUBLIC pthread
)
if(NOT TD_WINDOWS)
    target_link_libraries(
        os PUBLIC dl m rt
    )
else()
    target_link_libraries(
        os PUBLIC ws2_32 iconv msvcregex wcwidth winmm
    )
endif(NOT TD_WINDOWS)
