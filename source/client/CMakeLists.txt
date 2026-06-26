aux_source_directory(src CLIENT_SRC)

if(TD_ENTERPRISE)
    LIST(APPEND CLIENT_SRC ${TD_ENTERPRISE_DIR}/source/plugins/view/src/clientView.c)
endif()

if(TD_WINDOWS)
    configure_file(src/taosnative.rc.in "${CMAKE_CURRENT_BINARY_DIR}/taosnative.rc")
    add_library(${TAOS_NATIVE_LIB} SHARED ${CLIENT_SRC} "${CMAKE_CURRENT_BINARY_DIR}/taosnative.rc")
else()
    add_library(${TAOS_NATIVE_LIB} SHARED ${CLIENT_SRC})
endif()

if(TD_DARWIN)
    target_compile_options(${TAOS_NATIVE_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
endif()


message(STATUS "Debugging ............................")
message(STATUS "TAOS_NATIVE_LIB:${TAOS_NATIVE_LIB}")
message(STATUS "TD_SOURCE_DIR:${TD_SOURCE_DIR}")


target_include_directories(
    ${TAOS_NATIVE_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
set(TAOSD_MODULE "")
if(BUILD_TAOSD_INTEGRATED)
    set(TAOSD_MODULE "taosd")
endif()
target_link_libraries(
    ${TAOS_NATIVE_LIB}
    INTERFACE api
    PRIVATE util common transport monitor nodes parser command planner catalog scheduler function qcom geometry ${TAOSD_MODULE} decimal extconnector
    PUBLIC os
)

add_library(${TAOS_NATIVE_LIB_STATIC} STATIC ${CLIENT_SRC})

if(TD_DARWIN)
    target_compile_options(${TAOS_NATIVE_LIB_STATIC} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    ${TAOS_NATIVE_LIB_STATIC}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)
target_link_libraries(
    ${TAOS_NATIVE_LIB_STATIC}
    INTERFACE api
    PUBLIC os util common transport monitor nodes parser command planner catalog scheduler function qcom geometry decimal
)

# libgsasl: application-layer SCRAM-SHA-256 authentication on the CONNECT handshake
if(${BUILD_LIBGSASL} AND ${TD_LINUX})
    DEP_ext_gsasl(${TAOS_NATIVE_LIB})
    DEP_ext_gsasl(${TAOS_NATIVE_LIB_STATIC})
    add_definitions(-DUSE_LIBGSASL)
endif()

if(BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif()

ADD_SUBDIRECTORY(wrapper)
