cmake_minimum_required(VERSION 3.10)

project(reader_csv LANGUAGES CXX)

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

# 添加目标对象
add_library(reader_csv STATIC
    src/CSVUtils.cpp
    src/CSVReader.cpp
    src/TableNameCSV.cpp
    src/TagsCSV.cpp
    src/ColumnsCSV.cpp
)

# 包含头文件目录
target_include_directories(reader_csv PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../../utils/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../config/inc
    ${CMAKE_CURRENT_SOURCE_DIR}/../../generator/inc
)

# 链接依赖库
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../../utils/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../config/build)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../../components/generator/build)
target_link_libraries(reader_csv PUBLIC
    actions_config
    actions_generator
    utils
)

# 添加测试
# Test CSVReader
add_executable(TestCSVReader
    test/TestCSVReader.cpp
)
target_link_libraries(TestCSVReader PRIVATE 
    reader_csv
)
add_test(NAME TestCSVReader COMMAND TestCSVReader)

# Test TableNameCSV
add_executable(TestTableNameCSV
    test/TestTableNameCSV.cpp
)
target_link_libraries(TestTableNameCSV PRIVATE 
    reader_csv
)
add_test(NAME TestTableNameCSV COMMAND TestTableNameCSV)

# Test TagsCSV
add_executable(TestTagsCSV
    test/TestTagsCSV.cpp
)
target_link_libraries(TestTagsCSV PRIVATE 
    reader_csv
)
add_test(NAME TestTagsCSV COMMAND TestTagsCSV)

# Test ColumnsCSV
add_executable(TestColumnsCSV
    test/TestColumnsCSV.cpp
)
target_link_libraries(TestColumnsCSV PRIVATE 
    reader_csv
)
add_test(NAME TestColumnsCSV COMMAND TestColumnsCSV)

