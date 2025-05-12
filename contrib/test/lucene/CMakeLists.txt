add_executable(luceneTest "")
target_sources(luceneTest
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp"
)
target_link_libraries(luceneTest lucene++)