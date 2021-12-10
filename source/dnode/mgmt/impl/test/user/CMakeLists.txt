add_executable(dndTestUser "")

target_sources(dndTestUser
    PRIVATE
    "user.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestUser
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestUser
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dndTestUser
    COMMAND dndTestUser
)
