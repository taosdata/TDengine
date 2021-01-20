PROJECT(TDengine)

include_directories(${PRJ_HEADER_PATH} ${TD_COMMUNITY_DIR}/src/inc  ${TD_COMMUNITY_DIR}/src/client/inc/
        ${TD_COMMUNITY_DIR}/src/query/inc  ${TD_COMMUNITY_DIR}/src/util/inc ../../tests/test/perftest/inc
        ${TD_COMMUNITY_DIR}/src/common/inc ${TD_OS_DIR}/inc)

#add_executable(loadnsort ./perftest/loadnsort.c )
#target_link_libraries(loadnsort taos pthread)

#add_executable(asynctest ./cdriver/asynctest.c )
#target_link_libraries(asynctest taos pthread)

#add_executable(perftest ./perftest/taosperf.c ./perftest/perftestcommon.c)
#target_link_libraries(perftest taos pthread tutil)

add_executable(percentiletest ./perftest/percentileTest.c)
target_link_libraries(percentiletest query common taos_static tutil)

#add_executable(sqltest ./perftest/sqlTest.c ./perftest/testCommon.c)
#target_link_libraries(sqltest taos tutil)

#add_executable(asttest ./perftest/astTest.c ./perftest/testCommon.c)
#target_link_libraries(asttest taos tutil)

#add_executable(localmerge ./perftest/localMergeTest.c)
#target_link_libraries(localmerge taos tutil)

add_executable(querytest ./perftest/queryTest.c ./perftest/testCommon.c)
target_link_libraries(querytest taos_static query common tutil)

#add_executable(functiontest ./perftest/sqlfunctionTest.c)
#target_link_libraries(functiontest taos tutil)

#add_executable(sqlparseTest ./perftest/sqlparseTest.c ./perftest/testCommon.c)
#target_link_libraries(sqlparseTest taos tutil)

#add_executable(metricInterpoTest ./perftest/metricInterpolationTest.c ./perftest/testCommon.c)
#target_link_libraries(metricInterpoTest taos tutil)

#add_executable(multiclientest ./perftest/perftestbase.cpp ./perftest/perftestcommon.c)
#target_link_libraries(multiclientest taos pthread)

FIND_PATH(
        HEADER_MYSQL_INCLUDE_DIR
        mysql.h
        /usr/include/mysql
        /usr/local/include/mysql
)

find_library(LIB_MYSQL_STATIC_DIR
        libmysqlclient.a
        /usr/lib/x86_64-linux-gnu
        /usr/lib64
        )

FIND_FILE(mysql.h /usr/include /usr/include/mysql)

IF (HEADER_MYSQL_INCLUDE_DIR AND LIB_MYSQL_STATIC_DIR)
    message(STATUS "Found mysql include header")
    include_directories(${HEADER_MYSQL_INCLUDE_DIR})
    link_directories(/usr/lib/x86_64-linux-gnu)

    add_executable(mysqltest ./perftest/mysqlperf.c ./perftest/perftestcommon.c)
    target_link_libraries(mysqltest mysqlclient pthread)
ENDIF()

