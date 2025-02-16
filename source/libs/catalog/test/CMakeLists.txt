
MESSAGE(STATUS "build catalog unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        if(${FALLBACK})          # {
            SET(CMAKE_CXX_STANDARD 11)
        else()                   # }{
            SET(CMAKE_CXX_STANDARD 14)
        endif()                  # }
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(catalogTest ${SOURCE_LIST})
        if(${FALLBACK})               # {
                TARGET_LINK_LIBRARIES(
                        catalogTest
                        PUBLIC os util common nodes catalog transport gtest qcom ${TAOS_LIB_STATIC}
                )
        else()                        # }{
                TARGET_LINK_LIBRARIES(
                        catalogTest
                        PUBLIC os util common nodes catalog transport qcom ${TAOS_LIB_STATIC}
                )
                DEP_ext_gtest(catalogTest)
                DEP_ext_stub(catalogTest)
                target_include_directories(catalogTest PUBLIC ${ext_stub_stub} ${ext_stub_addr_any})
        endif()                       # }

        TARGET_INCLUDE_DIRECTORIES(
                catalogTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/catalog/inc"
        )

        add_test(
            NAME catalogTest
            COMMAND catalogTest
        )
ENDIF()
