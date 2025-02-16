MESSAGE(STATUS "build command unit test")

# GoogleTest requires at least C++11
if(${FALLBACK})          # {
    SET(CMAKE_CXX_STANDARD 11)
else()                   # }{
    SET(CMAKE_CXX_STANDARD 14)
endif()                  # }
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(commandTest ${SOURCE_LIST})

TARGET_INCLUDE_DIRECTORIES(
        commandTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/command/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/command/inc"
)

if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                commandTest
                PUBLIC os util common nodes parser catalog transport gtest function qcom
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                commandTest
                PUBLIC os util common nodes parser catalog transport function qcom
        )
        DEP_ext_gtest(commandTest)
endif()                          # }
