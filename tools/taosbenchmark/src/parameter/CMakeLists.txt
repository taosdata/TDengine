cmake_minimum_required(VERSION 3.10)

project(ParameterContext LANGUAGES CXX)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 添加 nlohmann/json 库
find_package(nlohmann_json 3.2.0 REQUIRED)


# 添加主库
add_library(ParameterContext STATIC ParameterContext.cpp)

# 包含头文件目录
target_include_directories(ParameterContext PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# 链接 nlohmann/json 库
target_link_libraries(ParameterContext PRIVATE nlohmann_json::nlohmann_json)


# 添加测试可执行文件
add_executable(ParameterContextTest
    test/ParameterContextTest.cpp
)

# 链接主库和依赖
target_link_libraries(ParameterContextTest
    PRIVATE
    ParameterContext
    nlohmann_json::nlohmann_json
)

# # 添加可执行文件
# add_executable(ParameterContext main.cpp)
