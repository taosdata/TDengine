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
    src/ParameterContext.cpp
)

# 包含头文件目录
target_include_directories(parameter PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../workflow/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../actions/config/build)
target_link_libraries(parameter PUBLIC 
    nlohmann_json::nlohmann_json 
    yaml-cpp
    actions_config
)

# 添加测试
# test ParseYAML
add_executable(TestParseYAML 
    test/TestParseYAML.cpp
)
target_link_libraries(TestParseYAML PRIVATE 
    yaml-cpp
)
add_test(NAME TestParseYAML COMMAND TestParseYAML)

# test ParameterContext
add_executable(TestParameterContext 
    test/TestParameterContext.cpp
)
target_link_libraries(TestParameterContext PRIVATE 
    parameter
    nlohmann_json::nlohmann_json
    yaml-cpp
)
add_test(NAME TestParameterContext COMMAND TestParameterContext)
