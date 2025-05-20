cmake_minimum_required(VERSION 3.10)

project(parameter LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 启用测试支持
enable_testing()

# 添加调试符号信息
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")

# 添加第三方库
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(yaml-cpp REQUIRED)

# 添加主库
add_library(parameter STATIC 
    ConfigData.cpp 
    ParameterContext.cpp
)

# 包含头文件目录
target_include_directories(parameter PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# 链接 nlohmann/json 和 yaml-cpp 库
target_link_libraries(parameter PUBLIC 
    nlohmann_json::nlohmann_json 
    yaml-cpp
)

# TestConfigData
add_executable(TestConfigData 
    test/TestConfigData.cpp
)

target_link_libraries(TestConfigData PRIVATE 
    parameter
)

add_test(NAME TestConfigData COMMAND TestConfigData)

# TestParameterContext
add_executable(TestParameterContext 
    test/TestParameterContext.cpp
)

target_link_libraries(TestParameterContext PRIVATE 
    parameter
    nlohmann_json::nlohmann_json
    yaml-cpp
)

add_test(NAME TestParameterContext COMMAND TestParameterContext)

# 添加可执行文件
# add_executable(parameter main.cpp)