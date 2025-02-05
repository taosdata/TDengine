
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

    IF (TD_GRANT)
        TARGET_LINK_LIBRARIES(
                plannerTest
                PUBLIC os util common nodes planner parser catalog transport gtest function qcom grant
        )
    ELSE ()
        if(${FALLBACK})               # {
            TARGET_LINK_LIBRARIES(
                    plannerTest
                    PUBLIC os util common nodes planner parser catalog transport gtest function qcom
            )
        else()                        # }{
            TARGET_LINK_LIBRARIES(
                    plannerTest
                    PUBLIC os util common nodes planner parser catalog transport function qcom
            )
            DEP_ext_gtest(plannerTest)
            if(${TD_WINDOWS})           # {
                    DEP_ext_wingetopt(plannerTest)
            endif()                     # }
            DEP_ext_stub(plannerTest)
            TARGET_INCLUDE_DIRECTORIES(plannerTest
                    PUBLIC ${ext_stub_stub} ${ext_stub_addr_any}
            )
        endif()                       # }
    ENDIF()

    TARGET_INCLUDE_DIRECTORIES(
            plannerTest
            PUBLIC "${TD_SOURCE_DIR}/include/libs/planner/"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/planner/inc"
            PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/test"
    )


    if(${BUILD_WINGETOPT})
        if(${FALLBACK})          # {
            target_include_directories(
                plannerTest
                PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
            )
            target_link_libraries(plannerTest PUBLIC wingetopt)
        endif()                  # }
    endif()

    add_test(
        NAME plannerTest
        COMMAND plannerTest
    )
ENDIF ()
