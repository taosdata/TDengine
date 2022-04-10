
MESSAGE(STATUS "build planner unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(plannerTest
        ${SOURCE_LIST}
        "${SOURCE_LIST}/../../../parser/test/mockCatalog.cpp"
        "${SOURCE_LIST}/../../../parser/test/mockCatalogService.cpp"
)

TARGET_LINK_LIBRARIES(
        plannerTest
        PUBLIC os util common nodes planner parser catalog transport gtest function qcom
)

TARGET_INCLUDE_DIRECTORIES(
        plannerTest
        PUBLIC "${TD_SOURCE_DIR}/include/libs/planner/"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/planner/inc"
        PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/test"
)
