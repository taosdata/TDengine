add_executable(dndTestAcct "")

target_sources(dndTestAcct
    PRIVATE
    "acct.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestAcct
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestAcct
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dndTestAcct
    COMMAND dndTestAcct
)
