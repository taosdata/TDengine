cmake_minimum_required(VERSION 3.10)

project(actions_generator LANGUAGES CXX)

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
add_library(actions_generator STATIC 
    src/TableNameGenerator.cpp
    src/TimestampGenerator.cpp
    src/ColumnGeneratorFactory.cpp
    src/RandomColumnGenerator.cpp
    src/OrderColumnGenerator.cpp
    src/FunctionColumnGenerator.cpp
    src/RowGenerator.cpp
)

# 包含头文件目录
target_include_directories(actions_generator PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../config/build)
target_link_libraries(actions_generator PUBLIC
    actions_config
    utils
)

# 添加测试
# Test TimestampGenerator
add_executable(TestTimestampGenerator
    test/TestTimestampGenerator.cpp
)
target_link_libraries(TestTimestampGenerator PRIVATE 
    actions_generator
)
add_test(NAME TestTimestampGenerator COMMAND TestTimestampGenerator)

# Test TableNameGenerator
add_executable(TestTableNameGenerator
    test/TestTableNameGenerator.cpp
)
target_link_libraries(TestTableNameGenerator PRIVATE 
    actions_generator
)
add_test(NAME TestTableNameGenerator COMMAND TestTableNameGenerator)

# Test RandomColumnGenerator
add_executable(TestRandomColumnGenerator
    test/TestRandomColumnGenerator.cpp
)
target_link_libraries(TestRandomColumnGenerator PRIVATE 
    actions_generator
)
add_test(NAME TestRandomColumnGenerator COMMAND TestRandomColumnGenerator)

# Test ColumnGeneratorFactory
add_executable(TestColumnGeneratorFactory
    test/TestColumnGeneratorFactory.cpp
)
target_link_libraries(TestColumnGeneratorFactory PRIVATE 
    actions_generator
)
add_test(NAME TestColumnGeneratorFactory COMMAND TestColumnGeneratorFactory)

# Test RowGenerator
add_executable(TestRowGenerator
    test/TestRowGenerator.cpp
)
target_link_libraries(TestRowGenerator PRIVATE 
    actions_generator
)
add_test(NAME TestRowGenerator COMMAND TestRowGenerator)

