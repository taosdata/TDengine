cmake_minimum_required(VERSION 3.10)

project(engine LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库


# 添加主库
add_library(engine STATIC 
    src/JobDAG.cpp
    src/ThreadSafeQueue.cpp
    src/StepExecutionStrategy.cpp
    src/JobScheduler.cpp
)

# 包含头文件目录
target_include_directories(engine PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/base
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../parameter/build)
target_link_libraries(engine PUBLIC parameter)

# TestJobScheduler
add_executable(TestJobScheduler 
    test/TestJobScheduler.cpp
)

target_link_libraries(TestJobScheduler PRIVATE 
    parameter
    engine
    yaml-cpp
)

add_test(NAME TestJobScheduler COMMAND TestJobScheduler)
