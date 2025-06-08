cmake_minimum_required(VERSION 3.10)

project(utils LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加主库
add_library(utils STATIC
    src/StringUtils.cpp
)

# 包含头文件目录
target_include_directories(utils PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)
