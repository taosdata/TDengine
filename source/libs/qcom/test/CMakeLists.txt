
MESSAGE(STATUS "build qcom unit test")

# GoogleTest requires at least C++11
if(${FALLBACK})          # {
    SET(CMAKE_CXX_STANDARD 11)
else()                   # }{
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(queryUtilTest ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
        queryUtilTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/qcom/inc"
)

if(${FALLBACK})                   # {
        TARGET_LINK_LIBRARIES(
                queryUtilTest
                PUBLIC os util gtest qcom common transport
        )
else()                            # }{
        TARGET_LINK_LIBRARIES(
                queryUtilTest
                PUBLIC os util qcom common transport
        )
        DEP_ext_gtest(queryUtilTest)
endif()                           # }

