CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

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

# IF (TD_LINUX)
# ADD_EXECUTABLE(trefTest ./trefTest.c)
# TARGET_LINK_LIBRARIES(trefTest util common)
# ENDIF ()
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/src/util/inc)
INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)

add_executable(test_heap "heapTest.cpp")
target_link_libraries(test_heap os util gtest_main)
add_test(
  NAME test_heap
  COMMAND test_heap
)

# arrayTest
add_executable(test_array "arrayTest.cpp")
target_link_libraries(test_array os util gtest_main)
add_test(
  NAME test_array
  COMMAND test_array
)

# test_freelist
# add_executable(test_freelist "freelistTest.cpp")
# target_link_libraries(test_freelist os util gtest gtest_main)

# test_encode
# add_executable(test_encode "encodeTest.cpp")
# target_link_libraries(test_encode os util gtest gtest_main)

# test_cfg
add_executable(test_cfg "cfgTest.cpp")
target_link_libraries(test_cfg os util gtest_main)
add_test(
  NAME test_cfg
  COMMAND test_cfg
)

# test_bloomfilter
add_executable(test_bloomfilter "bloomFilterTest.cpp")
target_link_libraries(test_bloomfilter os util gtest_main)
add_test(
  NAME test_bloomfilter
  COMMAND test_bloomfilter
)

# test_taosbsearch
add_executable(test_taosbsearch "taosbsearchTest.cpp")
target_link_libraries(test_taosbsearch os util gtest_main)
add_test(
  NAME test_taosbsearch
  COMMAND test_taosbsearch
)

# test_rbtree
add_executable(test_rbtree "trbtreeTest.cpp")
target_link_libraries(test_rbtree os util gtest_main)
add_test(
  NAME test_rbtree
  COMMAND test_rbtree
)

# test_pagebuffer
add_executable(test_pagebuffer "pageBufferTest.cpp")
target_link_libraries(test_pagebuffer os util gtest_main)
add_test(
  NAME test_pagebuffer
  COMMAND test_pagebuffer
)

# test_algo
add_executable(test_algo "talgoTest.cpp")
target_link_libraries(test_algo os util gtest_main)
add_test(
  NAME test_algo
  COMMAND test_algo
)

# test_tbasecodec
add_executable(test_tbasecodec "tbaseCodecTest.cpp")
target_link_libraries(test_tbasecodec os util common gtest_main)
add_test(
  NAME test_tbasecodec
  COMMAND test_tbasecodec
)

# test_buffer
add_executable(test_buffer "bufferTest.cpp")
target_link_libraries(test_buffer os util gtest_main)
add_test(
  NAME test_buffer
  COMMAND test_buffer
)

add_executable(test_regex "regexTest.cpp")
target_link_libraries(test_regex os util gtest_main)
add_test(
  NAME test_regex
  COMMAND test_regex
)

add_executable(test_log "log.cpp")
target_link_libraries(test_log os util common gtest_main)
add_test(
  NAME test_log
  COMMAND test_log
)
target_include_directories(
  test_log
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

add_executable(test_decompress "decompressTest.cpp")
target_link_libraries(test_decompress os util common gtest_main)
add_test(
  NAME test_decompress
  COMMAND test_decompress
)

IF($TD_LINUX)
  add_executable(test_util "utilTests.cpp")
  target_link_libraries(test_util os util common gtest_main)
  add_test(
    NAME test_util
    COMMAND test_util
  )
ENDIF()

if(${TD_LINUX})
  # test_terror
  add_executable(test_terror "terrorTest.cpp")
  target_link_libraries(test_terror os util common gtest_main)
  add_test(
    NAME test_terror
    COMMAND test_terror
  )

  # config
  SET(ERR_TBL_FILE ${CMAKE_CURRENT_SOURCE_DIR}/test_errorcode.ini)
  add_custom_command(TARGET test_terror POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${ERR_TBL_FILE} $<TARGET_FILE_DIR:test_terror>
  )

  # test_mempool
  add_executable(test_mempool "memPoolTest.cpp")
  target_link_libraries(test_mempool os util common gtest_main)
  add_test(
    NAME test_mempool
    COMMAND test_mempool
  )
endif()
