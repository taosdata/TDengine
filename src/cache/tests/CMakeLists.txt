AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

FIND_PATH(HEADER_GTEST_INCLUDE_DIR gtest.h /usr/include/gtest /usr/local/include/gtest)
FIND_LIBRARY(LIB_GTEST_STATIC_DIR libgtest.a /usr/lib/ /usr/local/lib /usr/lib64)
FIND_LIBRARY(LIB_GTEST_SHARED_DIR libgtest.so /usr/lib/ /usr/local/lib /usr/lib64)

IF (HEADER_GTEST_INCLUDE_DIR AND (LIB_GTEST_STATIC_DIR OR LIB_GTEST_SHARED_DIR))
    MESSAGE(STATUS "gTest library found, build unit test")

    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)

    INCLUDE_DIRECTORIES(/usr/include /usr/local/include)
    LINK_DIRECTORIES(/usr/lib /usr/local/lib)

    add_executable(cacheTests ${SOURCE_LIST})
    target_link_libraries(cacheTests cache tutil common gtest pthread gtest )

    add_test(NAME unit COMMAND ${CMAKE_CURRENT_BINARY_DIR}/cacheTests)
ENDIF()

