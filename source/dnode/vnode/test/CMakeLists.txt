MESSAGE(STATUS "tq unit test")

# GoogleTest requires at least C++11
if(${FALLBACK})          # {
    SET(CMAKE_CXX_STANDARD 11)
else()                   # }{
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }

IF(NOT TD_WINDOWS)
    add_executable(tqTest tqTest.cpp)
    target_include_directories(tqTest
            PUBLIC
            "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    if(${FALLBACK})               # {
            TARGET_LINK_LIBRARIES(
                    tqTest
                    PUBLIC os util common vnode gtest_main
            )
    else()                        # }{
            TARGET_LINK_LIBRARIES(
                    tqTest
                    PUBLIC os util common vnode
            )
            DEP_ext_gtest(tqTest)
    endif()                       # }

    enable_testing()

    add_test(
            NAME tq_test
            COMMAND tqTest
    )
ENDIF()

# ADD_EXECUTABLE(tsdbSmaTest tsdbSmaTest.cpp)
# TARGET_LINK_LIBRARIES(
#         tsdbSmaTest
#         PUBLIC os util common vnode gtest_main
# )

# TARGET_INCLUDE_DIRECTORIES(
#         tsdbSmaTest
#         PUBLIC "${TD_SOURCE_DIR}/include/common"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )
