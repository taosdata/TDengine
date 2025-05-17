aux_source_directory(src WRAPPER_SRC)

if(TD_WINDOWS)
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/../src/taos.rc.in)
else()
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC})
endif()

if(${TD_DARWIN})
    target_compile_options(${TAOS_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

# jni include
INCLUDE_DIRECTORIES(jni)
if(TD_WINDOWS)
    INCLUDE_DIRECTORIES(jni/windows)
    INCLUDE_DIRECTORIES(jni/windows/win32)
    INCLUDE_DIRECTORIES(jni/windows/win32/bridge)
else()
    INCLUDE_DIRECTORIES(jni/linux)
endif()

target_include_directories(
    ${TAOS_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    ${TAOS_LIB}
    PUBLIC os util
)

add_library(${TAOS_LIB_STATIC} STATIC ${WRAPPER_SRC})

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
    PUBLIC os util
)

# if(${BUILD_TEST})
# ADD_SUBDIRECTORY(test)
# endif(${BUILD_TEST})
