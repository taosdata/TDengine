
MESSAGE(STATUS "build planner unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

    ADD_EXECUTABLE(plannerTest
        ${SOURCE_LIST}
        "${SOURCE_LIST}/../../../parser/test/mockCatalog.cpp"
        "${SOURCE_LIST}/../../../parser/test/mockCatalogService.cpp"
    )
    DEP_ext_gtest(plannerTest)
    DEP_ext_cppstub(plannerTest)
    if(${BUILD_WINGETOPT})
        DEP_ext_wingetopt(plannerTest)
    endif()


    IF (TD_GRANT)
        TARGET_LINK_LIBRARIES(
                plannerTest
                PUBLIC os util common nodes planner parser catalog transport function qcom grant
        )
    ELSE ()
        TARGET_LINK_LIBRARIES(
                plannerTest
                PUBLIC os util common nodes planner parser catalog transport function qcom
        )
    ENDIF()

    TARGET_INCLUDE_DIRECTORIES(
            plannerTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/planner/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/planner/inc"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/test"
    )

    add_test(
        NAME plannerTest
        COMMAND plannerTest
    )
ENDIF ()
