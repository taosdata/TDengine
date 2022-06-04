aux_source_directory(src CLIENT_SRC)
if(TD_WINDOWS)
    add_library(thintaos SHARED ${CLIENT_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/src/taos.rc.in)
else()
    add_library(thintaos SHARED ${CLIENT_SRC})
endif ()
target_include_directories(
    thintaos
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    thintaos
    INTERFACE api
    PRIVATE os util common transport
)
if(TD_WINDOWS)
    set_target_properties(thintaos
        PROPERTIES 
        LINK_FLAGS 
        /DEF:${CMAKE_CURRENT_SOURCE_DIR}/src/taos.def
    )
endif ()

set_target_properties(
    thintaos
    PROPERTIES
    VERSION ${TD_VER_NUMBER}
    SOVERSION ${TD_VER_NUMBER}
)

add_library(thintaos_static STATIC ${CLIENT_SRC})
target_include_directories(
    thintaos_static
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    thintaos_static
    INTERFACE api
    PRIVATE os util common transport
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})