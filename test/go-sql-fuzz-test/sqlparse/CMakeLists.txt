cmake_minimum_required(VERSION 3.16)

project(sqlparser NONE)

find_program(GO_EXECUTABLE go REQUIRED)

enable_testing()

add_test(
  NAME go_test_all
  COMMAND ${CMAKE_COMMAND} -E env
          GOCACHE=/tmp/gocache
          GOMODCACHE=/tmp/gomodcache
          ${GO_EXECUTABLE} test ./... -count=1
)
set_tests_properties(go_test_all PROPERTIES WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

add_custom_target(
  test-all
  COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
  USES_TERMINAL
)
