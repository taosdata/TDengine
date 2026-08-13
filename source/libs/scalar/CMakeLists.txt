aux_source_directory(src SCALAR_SRC)

add_library(scalar STATIC ${SCALAR_SRC})
target_include_directories(
    scalar
    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/totp/inc"
)

target_link_libraries(scalar
    PRIVATE os
    PRIVATE util
    PRIVATE common
    PRIVATE nodes
    PRIVATE function
    PRIVATE qcom
    PRIVATE parser
    PRIVATE geometry
    PRIVATE decimal
    PRIVATE totp
)

if(UNIX AND NOT APPLE)
    # scalar and parser reference each other when statically linked by tests.
    # Ask CMake to rescan the link interface enough times for GNU ld.
    set_property(TARGET scalar PROPERTY LINK_INTERFACE_MULTIPLICITY 3)
endif()

if(BUILD_TEST)
    ADD_SUBDIRECTORY(test)
endif()
