add_executable(dndTestShow "")

target_sources(dndTestShow
    PRIVATE
    "show.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestShow
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestShow
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

enable_testing()

add_test(
    NAME dndTestShow
    COMMAND dndTestShow
)
