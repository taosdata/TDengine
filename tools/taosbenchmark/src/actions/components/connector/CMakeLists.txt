cmake_minimum_required(VERSION 3.10)

project(actions_connector LANGUAGES CXX)

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
add_library(actions_connector STATIC 
    src/DatabaseConnector.cpp
    src/NativeConnector.cpp
    src/WebsocketConnector.cpp
    src/RestfulConnector.cpp
)

# 包含头文件目录
target_include_directories(actions_connector PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../config/inc
)

# 添加测试
# test NativeConnector
add_executable(TestNativeConnector
    test/TestNativeConnector.cpp
)
target_link_libraries(TestNativeConnector PRIVATE 
    actions_connector
    taos
)
add_test(NAME TestNativeConnector COMMAND TestNativeConnector)