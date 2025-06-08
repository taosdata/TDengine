cmake_minimum_required(VERSION 3.10)

project(actions_config LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库


# 添加主库
add_library(actions_config STATIC 
    src/ConnectionInfo.cpp
    src/ColumnType.cpp
    src/ColumnConfig.cpp
)

# 包含头文件目录
target_include_directories(actions_config PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../utils/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../utils/build)
target_link_libraries(actions_config PUBLIC 
    utils
)

# 添加测试
# test ConnectionInfo
add_executable(TestConnectionInfo
    test/TestConnectionInfo.cpp
)
target_link_libraries(TestConnectionInfo PRIVATE 
    actions_config
)
add_test(NAME TestConnectionInfo COMMAND TestConnectionInfo)

# test ColumnConfig
add_executable(TestColumnConfig
    test/TestColumnConfig.cpp
)
target_link_libraries(TestColumnConfig PRIVATE 
    actions_config
)
add_test(NAME TestColumnConfig COMMAND TestColumnConfig)

# test ColumnConfigInstance
add_executable(TestColumnConfigInstance
    test/TestColumnConfigInstance.cpp
)
target_link_libraries(TestColumnConfigInstance PRIVATE 
    actions_config
)
add_test(NAME TestColumnConfigInstance COMMAND TestColumnConfigInstance)

