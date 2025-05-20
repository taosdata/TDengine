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
    JobDAG.cpp 
    ThreadSafeQueue.cpp 
    JobScheduler.cpp
)

# 包含头文件目录
target_include_directories(engine PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../parameter
)

# 链接依赖库
target_link_libraries(engine PRIVATE 
    parameter
)
# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../parameter/build)
target_link_libraries(engine PRIVATE parameter)

# TestConfigData
# add_executable(TestConfigData 
#     test/TestConfigData.cpp
# )

# target_link_libraries(TestConfigData PRIVATE 
#     parameter
# )

# add_test(NAME TestConfigData COMMAND TestConfigData)

# 添加可执行文件
# add_executable(parameter main.cpp)