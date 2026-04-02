add_executable(bdbTest "")
target_sources(
    bdbTest PRIVATE
    "bdbTest.c"
)

target_link_libraries(bdbTest bdb)
