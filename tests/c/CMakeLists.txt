macro(build_test name)
  add_executable(${name} ${name}.c)
  add_dependencies(${name} taos)

  target_link_directories(${name} PRIVATE
      ${CMAKE_BINARY_DIR}/build/lib
      )
  target_include_directories(${name} PRIVATE
      ${CMAKE_SOURCE_DIR}/include/client
      )
  target_link_libraries(${name} taos)

  add_test(
      NAME ${name}
      COMMAND ${name}
  )
endmacro()

build_test(api2_test)
build_test(bm)

