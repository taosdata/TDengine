cmake_minimum_required(VERSION 3.10)

project(insert_generator LANGUAGES CXX)

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
add_library(insert_generator STATIC 
    src/RowDataGenerator.cpp
    src/RateLimiter.cpp
    src/TableDataManager.cpp
    src/TableNameManager.cpp
)

# 包含头文件目录
target_include_directories(insert_generator PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/generator/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/reader/csv/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/generator/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../components/reader/csv/build)
target_link_libraries(insert_generator PUBLIC
    actions_config
    components_generator
    reader_csv
    utils
)

# 添加测试
# Test RowDataGenerator
add_executable(TestRowDataGenerator
    test/TestRowDataGenerator.cpp
)
target_link_libraries(TestRowDataGenerator PRIVATE 
    insert_generator
)
add_test(NAME TestRowDataGenerator COMMAND TestRowDataGenerator)

# Test TableDataManager
add_executable(TestTableDataManager
    test/TestTableDataManager.cpp
)
target_link_libraries(TestTableDataManager PRIVATE 
    insert_generator
)
add_test(NAME TestTableDataManager COMMAND TestTableDataManager)

# Test TableNameManager
add_executable(TestTableNameManager
    test/TestTableNameManager.cpp
)
target_link_libraries(TestTableNameManager PRIVATE 
    insert_generator
)
add_test(NAME TestTableNameManager COMMAND TestTableNameManager)
