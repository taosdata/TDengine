aux_source_directory(src OS_SRC)
if(NOT ${TD_WINDOWS})
    add_definitions(-DTHREAD_SAFE=1)
    aux_source_directory(src/timezone OS_TZ)
    add_library(os STATIC ${OS_SRC} ${OS_TZ})
else()
    add_library(os STATIC ${OS_SRC})
endif(NOT ${TD_WINDOWS})
if(${FALLBACK})            # {
    target_include_directories(
        os
        PUBLIC "${TD_SOURCE_DIR}/include/os"
        PUBLIC "${TD_SOURCE_DIR}/include"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
        PUBLIC "${TD_SOURCE_DIR}/contrib/pthread"
        PUBLIC "${TD_SOURCE_DIR}/contrib/iconv"
        PUBLIC "${TD_SOURCE_DIR}/contrib/msvcregex"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    )
else()                     # }{
    target_include_directories(
        os
        PUBLIC "${TD_SOURCE_DIR}/include/os"
        PUBLIC "${TD_SOURCE_DIR}/include"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    )
endif()                    # }
# iconv
if(TD_WINDOWS)
    if(${FALLBACK})        # {
        find_path(IconvApiIncludes iconv.h "${TD_SOURCE_DIR}/contrib/iconv")
    else()                 # }{
        if(${BUILD_WITH_ICONV})
            set(IconvApiIncludes TRUE)
        endif()
    endif()                # }
else()
    find_path(IconvApiIncludes iconv.h PATHS)
endif(TD_WINDOWS)
if(NOT IconvApiIncludes)
    add_definitions(-DDISALLOW_NCHAR_WITHOUT_ICONV) 
endif ()
if(USE_TD_MEMORY)
    add_definitions(-DUSE_TD_MEMORY) 
endif ()
if(BUILD_WITH_RAND_ERR)
    add_definitions(-DBUILD_WITH_RAND_ERR) 
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
if(${FALLBACK})             # {
    target_link_libraries(
        os
        PUBLIC pthread
        PUBLIC zlibstatic
    )
else()                      # }{
    DEP_ext_zlib(os)
    if(${TD_WINDOWS})        # {
        DEP_ext_msvcregex(os)
        DEP_ext_pthread(os)
        if(${BUILD_WITH_ICONV})
            DEP_ext_iconv(os)
        endif()
    else()                   # }{
        target_link_libraries(
            os
            PUBLIC pthread
        )
    endif()                  # }

    if(${BUILD_TEST})
        # freemine: DEP_ext_gtest(os)
        # freemine: DEP_ext_stub(os)
        # freemine: target_include_directories(
        # freemine:     os
        # freemine:     PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
        # freemine: )
    endif()

    if(NOT ${TD_WINDOWS})
        DEP_ext_tz(os)
        add_dependencies(os ext_tz_builder)
    endif()

    set(_taos_h            "${CMAKE_BINARY_DIR}/build/include/taos.h")
    set(_taoserror_h       "${CMAKE_BINARY_DIR}/build/include/taoserror.h")
    add_custom_command(
        OUTPUT ${_taos_h} ${_taoserror_h}
        DEPENDS ${CMAKE_SOURCE_DIR}/include/client/taos.h
                ${CMAKE_SOURCE_DIR}/include/util/taoserror.h
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${CMAKE_SOURCE_DIR}/include/client/taos.h ${_taos_h}
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${CMAKE_SOURCE_DIR}/include/util/taoserror.h ${_taoserror_h}
        VERBATIM
    )
    add_custom_target(
        copy_taos_headers
        DEPENDS ${_taos_h}
                ${_taoserror_h}
    )
    add_dependencies(os copy_taos_headers)

    if(NOT ${TD_WINDOWS})         # {
        target_link_libraries(
            os
            PUBLIC pthread
        )
    endif()                       # }
endif()                     # }
if(TD_WINDOWS)
    if(${FALLBACK})         # {
        target_link_libraries(
            os PUBLIC ws2_32 iconv msvcregex wcwidth winmm crashdump dbghelp version KtmW32
        )
    else()                  # }{
        DEP_ext_wcwidth(os)
        DEP_ext_pcre2(os)
        DEP_ext_crashdump(os)
        target_link_libraries(
            os PUBLIC ws2_32 version KtmW32 Dbghelp Winmm Iphlpapi Userenv
        )
    endif()                 # }
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

#if(NOT ${TD_WINDOWS})
#    find_library(tz libtz.a "${TD_SOURCE_DIR}/contrib/tz")
#    target_link_libraries(os PUBLIC ${tz})
#endif(NOT ${TD_WINDOWS})

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
