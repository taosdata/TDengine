add_executable(api2_test api2_test.c)
add_dependencies(api2_test taos)

target_link_directories(api2_test PRIVATE
    ${CMAKE_BINARY_DIR}/build/lib
    )
target_include_directories(api2_test PRIVATE
    ${CMAKE_SOURCE_DIR}/include/client
    )
target_link_libraries(api2_test taos)

add_test(
    NAME api2_test
    COMMAND api2_test
)

