MESSAGE(STATUS "build geometry unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)

  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
  ADD_EXECUTABLE(geomTest ${SOURCE_LIST})

  if(${FALLBACK})                        # {
    TARGET_LINK_LIBRARIES(
      geomTest
      PUBLIC os util gtest qcom nodes geometry scalar function scalar
    )
  else()                                 # }{
    TARGET_LINK_LIBRARIES(
      geomTest
      PUBLIC os util qcom nodes geometry scalar function scalar
    )
    DEP_ext_gtest(geomTest)
    DEP_ext_cjson_LIB(geomTest)
  endif()                                # }

  add_test(
    NAME geomTest
    COMMAND geomTest
  )
ENDIF()
