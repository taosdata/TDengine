
MESSAGE(STATUS "build scheduler unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(schedulerTest ${SOURCE_LIST})
        DEP_ext_gtest(schedulerTest)
        DEP_ext_cppstub(schedulerTest)

        IF(TD_GRANT)
                TARGET_LINK_LIBRARIES(
                        schedulerTest
                        PUBLIC os util common catalog transport qcom ${TAOS_NATIVE_LIB_STATIC} planner scheduler grant
                )
        ELSE()
                TARGET_LINK_LIBRARIES(
                        schedulerTest
                        PUBLIC os util common catalog transport qcom ${TAOS_NATIVE_LIB_STATIC} planner scheduler
                )
        ENDIF()

        TARGET_INCLUDE_DIRECTORIES(
                schedulerTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/scheduler/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/scheduler/inc"
        )
        add_test(
                NAME schedulerTest
                COMMAND schedulerTest
        )
ENDIF()
