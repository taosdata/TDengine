cmake_minimum_required(VERSION 3.10)

project(actions_reader LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加子目录
add_subdirectory(csv)

# 编译静态库
add_library(actions_reader STATIC reader.cpp)

# 链接 csv 静态库到 actions_reader
target_link_libraries(actions_reader PUBLIC
    reader_csv
)

# 包含头文件目录
target_include_directories(actions_reader PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/csv/inc
)
