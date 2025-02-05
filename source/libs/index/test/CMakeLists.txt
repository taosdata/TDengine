IF(NOT TD_DARWIN)
  add_executable(idxTest "")
  add_executable(idxFstTest "")
  add_executable(idxFstUT "")
  add_executable(idxUtilUT "")
  add_executable(idxJsonUT "")
  add_executable(idxFstUtilUT "")

  target_sources(idxTest
    PRIVATE
    "indexTests.cc"
  )
  target_sources(idxFstTest
    PRIVATE
    "fstTest.cc"
  )

  target_sources(idxFstUT
    PRIVATE
    "fstUT.cc"
  )
  target_sources(idxUtilUT
    PRIVATE
    "utilUT.cc"
  )

  target_sources(idxJsonUT
    PRIVATE
    "jsonUT.cc"
  )
  target_sources(idxFstUtilUT
    PRIVATE
    "fstUtilUT.cc"
  )

  target_include_directories(idxTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(idxFstTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_sources(idxJsonUT
    PRIVATE
    "jsonUT.cc"
  )
  target_include_directories(idxTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(idxFstTest
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(idxFstUT
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(idxUtilUT
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  target_include_directories(idxJsonUT
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(idxFstUtilUT
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )
  target_include_directories(idxJsonUT
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/index"
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  if(${FALLBACK})                            # {
    target_link_libraries(idxTest
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxTest
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxTest)
  endif()                                    # }
  if(${FALLBACK})                            # {
    target_link_libraries(idxFstTest
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxFstTest
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxFstTest)
  endif()                                    # }

  if(${FALLBACK})                            # {
    target_link_libraries(idxFstUT
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxFstUT
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxFstUT)
  endif()                                    # }

  if(${FALLBACK})                            # {
    target_link_libraries(idxUtilUT
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxUtilUT
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxUtilUT)
  endif()                                    # }

  if(${FALLBACK})                            # {
    target_link_libraries(idxJsonUT
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxJsonUT
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxJsonUT)
  endif()                                    # }
  if(${FALLBACK})                            # {
    target_link_libraries(idxFstUtilUT
      os
      util
      common
      gtest_main
      index
    )
  else()                                     # }{
    target_link_libraries(idxFstUtilUT
      PUBLIC
      os
      util
      common
      index
    )
    DEP_ext_gtest(idxFstUtilUT)
  endif()                                    # }

  add_test(
    NAME idxJsonUT
    COMMAND idxJsonUT
  )
  add_test(
    NAME idxFstUtilUT
    COMMAND idxFstUtilUT
  )

  add_test(
    NAME idxTest
    COMMAND idxTest
  )
  add_test(
    NAME idxUtilUT
    COMMAND idxUtilUT
  )
  add_test(
    NAME idxFstUT
    COMMAND idxFstUT
  )
  add_test(
    NAME idxFstTest
    COMMAND idxFstTest
  )
ENDIF()
