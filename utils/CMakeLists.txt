add_subdirectory(tsim)

if(${BUILD_TEST})
  add_subdirectory(test/c)
endif()

# ADD_SUBDIRECTORY(comparisonTest/tdengine)
