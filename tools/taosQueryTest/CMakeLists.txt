# taosQueryTest owns its implementation and unit-test roots.  The parent
# tools directory decides whether optional tools and unit tests are enabled.
add_subdirectory(src)

if(TD_LINUX AND BUILD_TEST)
  add_subdirectory(test)
endif()
