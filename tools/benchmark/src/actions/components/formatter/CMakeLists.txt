cmake_minimum_required(VERSION 3.10)

project(components_formatter LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库

include_directories(C:/TDengine/include)
include_directories(/usr/include)

# 添加主库
add_library(components_formatter STATIC 
    src/FormatterFactory.cpp
)

# 包含头文件目录
target_include_directories(components_formatter PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../config/build)
target_link_libraries(components_formatter PUBLIC
    actions_config
    utils
    taos
)

# 添加测试
# test SqlDatabaseFormatter
add_executable(TestSqlDatabaseFormatter
    test/TestSqlDatabaseFormatter.cpp
)
target_link_libraries(TestSqlDatabaseFormatter PRIVATE 
    components_formatter
)
add_test(NAME TestSqlDatabaseFormatter COMMAND TestSqlDatabaseFormatter)

# test SqlSuperTableFormatter
add_executable(TestSqlSuperTableFormatter
    test/TestSqlSuperTableFormatter.cpp
)
target_link_libraries(TestSqlSuperTableFormatter PRIVATE 
    components_formatter
)
add_test(NAME TestSqlSuperTableFormatter COMMAND TestSqlSuperTableFormatter)

# test SqlChildTableFormatter
add_executable(TestSqlChildTableFormatter
    test/TestSqlChildTableFormatter.cpp
)
target_link_libraries(TestSqlChildTableFormatter PRIVATE 
    components_formatter
)
add_test(NAME TestSqlChildTableFormatter COMMAND TestSqlChildTableFormatter)

# test SqlInsertDataFormatter
add_executable(TestSqlInsertDataFormatter
    test/TestSqlInsertDataFormatter.cpp
)
target_link_libraries(TestSqlInsertDataFormatter PRIVATE 
    components_formatter
)
add_test(NAME TestSqlInsertDataFormatter COMMAND TestSqlInsertDataFormatter)
