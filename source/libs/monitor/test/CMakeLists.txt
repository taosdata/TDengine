enable_testing()

aux_source_directory(. MONITOR_TEST_SRC)
add_executable(monitorTest ${MONITOR_TEST_SRC})
if(${FALLBACK})                            # {
  target_link_libraries(
    monitorTest
    PUBLIC monitor
    PUBLIC gtest_main
  )
else()                                     # }{
  target_link_libraries(
    monitorTest
    PUBLIC monitor
  )
  DEP_ext_gtest(monitorTest)
  DEP_ext_cjson_LIB(monitorTest)
endif()                                    # }

add_test(
    NAME monitorTest
    COMMAND monitorTest
)
