cmake_minimum_required(VERSION 3.10)

project(insert_pipeline LANGUAGES CXX)

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
add_library(insert_pipeline STATIC 
    src/DataQueue.cpp
)

# 包含头文件目录
target_include_directories(insert_pipeline PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../config/build)
target_link_libraries(insert_pipeline PUBLIC
    actions_config
)


# 添加测试
# test DataQueue
add_executable(TestDataQueue
    test/TestDataQueue.cpp
)
target_link_libraries(TestDataQueue PRIVATE 
    insert_pipeline
)
add_test(NAME TestDataQueue COMMAND TestDataQueue)