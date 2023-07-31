
MESSAGE(STATUS "build catalog unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(catalogTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
                catalogTest
                PUBLIC os util common nodes catalog transport gtest qcom taos_static
        )

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
