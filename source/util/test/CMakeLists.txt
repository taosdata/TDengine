CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

if(NOT ${FALLBACK})      # {
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }

if(${FALLBACK})      # {
  FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
  FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
  FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)
  IF(HEADER_GTEST_INCLUDE_DIR AND(LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
      MESSAGE(STATUS "gTest library found, build unit test")

      INCLUDE_DIRECTORIES(${HEADER_GTEST_INCLUDE_DIR})
      AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

      LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/trefTest.c)
      LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/terrorTest.cpp)
      ADD_EXECUTABLE(utilTest ${SOURCE_LIST})
      TARGET_LINK_LIBRARIES(utilTest util common os gtest pthread)

      LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/cacheTest.cpp)
      LIST(APPEND SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/hashTest.cpp)
      ADD_EXECUTABLE(hashTest ${SOURCE_LIST})
      TARGET_LINK_LIBRARIES(hashTest util common os gtest pthread)

      LIST(APPEND BIN_SRC ${CMAKE_CURRENT_SOURCE_DIR}/trefTest.c)
      ADD_EXECUTABLE(trefTest ${BIN_SRC})
      TARGET_LINK_LIBRARIES(trefTest common util)
  ENDIF()
else()               # }{
  # TODO: freemine: better approach later!!!
  #       utility source files mixed with main-embedded-source files
  #       what a ugly approach (aux_source_directory, list remove and list append)!!!
  #       check with TDengine guys to confirm what are util-test-files and what are utility-files
  ### AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/trefTest.c)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/terrorTest.cpp)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/hashTest.cpp)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/hashTest.cpp)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/memPoolTest.cpp)
  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/terrorTest.cpp)

  ### ADD_EXECUTABLE(utilTest ${SOURCE_LIST})
  ### target_include_directories(utilTest
  ###   PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
  ### )
  ### TARGET_LINK_LIBRARIES(utilTest
  ###   PUBLIC os util common
  ### )
  ### DEP_ext_gtest(utilTest)
  ### DEP_ext_stub(utilTest)

  ### LIST(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/cacheTest.cpp)
  ### LIST(APPEND SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/hashTest.cpp)
  ### ADD_EXECUTABLE(hashTest ${SOURCE_LIST})
  ### TARGET_LINK_LIBRARIES(hashTest util common os gtest pthread)

  ### LIST(APPEND BIN_SRC ${CMAKE_CURRENT_SOURCE_DIR}/trefTest.c)
  ### ADD_EXECUTABLE(trefTest ${BIN_SRC})
  ### TARGET_LINK_LIBRARIES(trefTest common util)
endif()              # }

# IF (TD_LINUX)
# ADD_EXECUTABLE(trefTest ./trefTest.c)
# TARGET_LINK_LIBRARIES(trefTest util common)
# ENDIF ()
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)

add_executable(heapTest "heapTest.cpp")
if(${FALLBACK})               # {
    target_link_libraries(heapTest os util gtest_main)
else()                        # }{
    target_link_libraries(heapTest PUBLIC os util)
    DEP_ext_gtest(heapTest)
endif()                       # }
add_test(
    NAME heapTest
    COMMAND heapTest
)

# arrayTest
add_executable(arrayTest "arrayTest.cpp")
if(${FALLBACK})               # {
    target_link_libraries(arrayTest os util gtest_main)
else()                        # }{
    target_link_libraries(arrayTest PUBLIC os util)
    DEP_ext_gtest(arrayTest)
endif()                       # }
add_test(
    NAME arrayTest
    COMMAND arrayTest
)

# # freelistTest
# add_executable(freelistTest "")
# target_sources(freelistTest
# PRIVATE
# "freelistTest.cpp"
# )
# target_link_libraries(freelistTest os util gtest gtest_main)

# # encodeTest
# add_executable(encodeTest "encodeTest.cpp")
# target_link_libraries(encodeTest os util gtest gtest_main)

# cfgTest
add_executable(cfgTest "cfgTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(cfgTest os util gtest_main)
else()                           # }{
    target_link_libraries(cfgTest PUBLIC os util)
    DEP_ext_gtest(cfgTest)
endif()                          # }
add_test(
    NAME cfgTest
    COMMAND cfgTest
)

# bloomFilterTest
add_executable(bloomFilterTest "bloomFilterTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(bloomFilterTest os util gtest_main)
else()                           # }{
    target_link_libraries(bloomFilterTest PUBLIC os util)
    DEP_ext_gtest(bloomFilterTest)
endif()                          # }
add_test(
    NAME bloomFilterTest
    COMMAND bloomFilterTest
)

# taosbsearchTest
add_executable(taosbsearchTest "taosbsearchTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(taosbsearchTest os util gtest_main)
else()                           # }{
    target_link_libraries(taosbsearchTest PUBLIC os util)
    DEP_ext_gtest(taosbsearchTest)
endif()                          # }
add_test(
    NAME taosbsearchTest
    COMMAND taosbsearchTest
)

# trbtreeTest
add_executable(rbtreeTest "trbtreeTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(rbtreeTest os util gtest_main)
else()                           # }{
    target_link_libraries(rbtreeTest PUBLIC os util)
    DEP_ext_gtest(rbtreeTest)
endif()                          # }
add_test(
    NAME rbtreeTest
    COMMAND rbtreeTest
)

# pageBufferTest
add_executable(pageBufferTest "pageBufferTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(pageBufferTest os util gtest_main)
else()                           # }{
    target_link_libraries(pageBufferTest PUBLIC os util)
    DEP_ext_gtest(pageBufferTest)
endif()                          # }
add_test(
    NAME pageBufferTest
    COMMAND pageBufferTest
)

# talgoTest
add_executable(talgoTest "talgoTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(talgoTest os util gtest_main)
else()                           # }{
    target_link_libraries(talgoTest PUBLIC os util)
    DEP_ext_gtest(talgoTest)
endif()                          # }
add_test(
    NAME talgoTest
    COMMAND talgoTest
)

# tbaseCodecTest
add_executable(tbaseCodecTest "tbaseCodecTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(tbaseCodecTest os util common gtest_main)
else()                           # }{
    target_link_libraries(tbaseCodecTest PUBLIC os util common)
    DEP_ext_gtest(tbaseCodecTest)
endif()                          # }
add_test(
    NAME tbaseCodecTest
    COMMAND tbaseCodecTest
)

# bufferTest
add_executable(bufferTest "bufferTest.cpp")
if(${FALLBACK})               # {
    target_link_libraries(bufferTest os util gtest_main)
else()                        # }{
    target_link_libraries(bufferTest PUBLIC os util)
    DEP_ext_gtest(bufferTest)
endif()                       # }
add_test(
    NAME bufferTest
    COMMAND bufferTest
)

add_executable(regexTest "regexTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(regexTest os util gtest_main)
else()                           # }{
    target_link_libraries(regexTest PUBLIC os util)
    DEP_ext_gtest(regexTest)
endif()                          # }
add_test(
    NAME regexTest
    COMMAND regexTest
)

add_executable(logTest "log.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(logTest os util common gtest_main)
else()                           # }{
    target_link_libraries(logTest PUBLIC os util common)
    DEP_ext_gtest(logTest)
endif()                          # }
add_test(
    NAME logTest
    COMMAND logTest
)
target_include_directories(
  logTest
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_executable(decompressTest "decompressTest.cpp")
if(${FALLBACK})                  # {
    target_link_libraries(decompressTest os util common gtest_main)
else()                           # }{
    target_link_libraries(decompressTest PUBLIC os util common)
    DEP_ext_gtest(decompressTest)
endif()                          # }
add_test(
    NAME decompressTest
    COMMAND decompressTest
)


IF($TD_LINUX)
    add_executable(utilTests "utilTests.cpp")
    target_link_libraries(utilTests os util common gtest_main)
    add_test(
        NAME utilTests
        COMMAND utilTests
    )
ENDIF()

if(${TD_LINUX})
    # terrorTest
    add_executable(terrorTest "terrorTest.cpp")
    if(${FALLBACK})               # {
        target_link_libraries(terrorTest os util common gtest_main)
    else()                        # }{
        target_link_libraries(terrorTest PUBLIC os util common)
        DEP_ext_gtest(terrorTest)
    endif()                       # }
    add_test(
        NAME terrorTest
        COMMAND terrorTest
    )

    # config
    SET(ERR_TBL_FILE ${CMAKE_CURRENT_SOURCE_DIR}/errorCodeTable.ini)
    add_custom_command(TARGET terrorTest POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${ERR_TBL_FILE} $<TARGET_FILE_DIR:terrorTest>
    )

    # memPoolTest
    add_executable(memPoolTest "memPoolTest.cpp")
    if(${FALLBACK})               # {
        target_link_libraries(memPoolTest os util common gtest_main)
    else()                        # }{
        target_link_libraries(memPoolTest PUBLIC os util common)
        DEP_ext_gtest(memPoolTest)
        DEP_ext_stub(memPoolTest)
        target_include_directories(memPoolTest PUBLIC ${ext_stub_stub} ${ext_stub_addr_any})
    endif()                       # }
    add_test(
        NAME memPoolTest
        COMMAND memPoolTest
    )

endif()
