add_executable(dndTestProfile "")

target_sources(dndTestProfile
    PRIVATE
    "profile.cpp"
    "../sut/deploy.cpp"
)

target_link_libraries(
    dndTestProfile
    PUBLIC dnode
    PUBLIC util
    PUBLIC os
    PUBLIC gtest_main
)

target_include_directories(dndTestProfile
    PUBLIC
    "${CMAKE_SOURCE_DIR}/include/server/dnode/mgmt"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    "${CMAKE_CURRENT_SOURCE_DIR}/../sut"
)

add_test(
    NAME dndTestProfile
    COMMAND dndTestProfile
)
