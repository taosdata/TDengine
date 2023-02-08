aux_source_directory(src CLIENT_SRC)

if(TD_WINDOWS)
    add_library(taos SHARED ${CLIENT_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/src/taos.rc.in)
else()
    add_library(taos SHARED ${CLIENT_SRC})
endif()

INCLUDE_DIRECTORIES(jni)

target_include_directories(
    taos
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos
    INTERFACE api
    PRIVATE os util common transport nodes parser command planner catalog scheduler function qcom
)

if(TD_DARWIN_ARM64)
    target_link_libraries(
        taos
        PRIVATE  "-arch x86_64"
    )
endif()

if(TD_WINDOWS)
    INCLUDE_DIRECTORIES(jni/windows)
    INCLUDE_DIRECTORIES(jni/windows/win32)
    INCLUDE_DIRECTORIES(jni/windows/win32/bridge)
else()
    INCLUDE_DIRECTORIES(jni/linux)
endif()

set_target_properties(
    taos
    PROPERTIES
    CLEAN_DIRECT_OUTPUT
    1
)

set_target_properties(
    taos
    PROPERTIES
    VERSION ${TD_VER_NUMBER}
    SOVERSION 1
)

add_library(taos_static STATIC ${CLIENT_SRC})
target_include_directories(
    taos_static
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos_static
    INTERFACE api
    PRIVATE os util common transport nodes parser command planner catalog scheduler function qcom
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
