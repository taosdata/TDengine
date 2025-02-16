if(NOT ${FALLBACK})      # {
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }

add_executable(walTest "")
target_sources(walTest
    PRIVATE
    "walMetaTest.cpp"
)
target_include_directories(walTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/wal"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

if(${FALLBACK})               # {
    target_link_libraries(walTest
        wal
        gtest_main
    )
else()                        # }{
    target_link_libraries(walTest
        PUBLIC wal
    )
    DEP_ext_gtest(walTest)
endif()                       # }
enable_testing()
add_test(
    NAME wal_test
    COMMAND walTest
)
