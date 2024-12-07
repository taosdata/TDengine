aux_source_directory(src WRAPPER_SRC)

if(TD_WINDOWS)
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/../src/taos.rc.in)
else()
    add_library(${TAOS_LIB} SHARED ${WRAPPER_SRC})
endif()

if(${TD_DARWIN})
    target_compile_options(${TAOS_LIB} PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    ${TAOS_LIB}
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    ${TAOS_LIB}
    PUBLIC os_for_wrapper
)

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
    PUBLIC os_for_wrapper
)

# if(${BUILD_TEST})
# ADD_SUBDIRECTORY(test)
# endif(${BUILD_TEST})
