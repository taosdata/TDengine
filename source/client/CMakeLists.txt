aux_source_directory(src CLIENT_SRC)

if(TD_ENTERPRISE)
    LIST(APPEND CLIENT_SRC ${TD_ENTERPRISE_DIR}/src/plugins/view/src/clientView.c)
endif()

if(TD_WINDOWS)
    add_library(${TAOS_LIB} SHARED ${CLIENT_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/src/taos.rc.in)
else()
    add_library(${TAOS_LIB} SHARED ${CLIENT_SRC})
endif()

if(${TD_DARWIN})
    target_compile_options(${TAOS_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

INCLUDE_DIRECTORIES(jni)

target_include_directories(
    ${TAOS_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    ${TAOS_LIB}
    INTERFACE api
    PRIVATE os util common transport monitor nodes parser command planner catalog scheduler function qcom geometry decimal
)

if(TD_WINDOWS)
    INCLUDE_DIRECTORIES(jni/windows)
    INCLUDE_DIRECTORIES(jni/windows/win32)
    INCLUDE_DIRECTORIES(jni/windows/win32/bridge)
else()
    INCLUDE_DIRECTORIES(jni/linux)
endif()

set_target_properties(
    ${TAOS_LIB}
    PROPERTIES
    CLEAN_DIRECT_OUTPUT
    1
)

set_target_properties(
    ${TAOS_LIB}
    PROPERTIES
    VERSION ${TD_VER_NUMBER}
    SOVERSION 1
)

add_library(${TAOS_LIB_STATIC} STATIC ${CLIENT_SRC})

if(${TD_DARWIN})
    target_compile_options(${TAOS_LIB_STATIC} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    ${TAOS_LIB_STATIC}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    ${TAOS_LIB_STATIC}
    INTERFACE api
    PRIVATE os util common transport monitor nodes parser command planner catalog scheduler function qcom geometry decimal
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
