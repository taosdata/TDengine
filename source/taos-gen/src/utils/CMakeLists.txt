# Create library target
add_library(utils STATIC
  src/LogUtils.cpp
  src/StringUtils.cpp
  src/TimestampUtils.cpp
  src/ProcessUtils.cpp
  src/Barrier.cpp
  src/Latch.cpp
  src/SignalManager.cpp
  src/ScopedEnvVar.cpp
)

# Set include directories
target_include_directories(utils
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/inc
)

# Find cctz package
find_package(cctz REQUIRED)

# GCC < 8 ships <filesystem> under <experimental/filesystem> and requires
# explicit linking with -lstdc++fs.
set(_fs_lib "")
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8)
  set(_fs_lib stdc++fs)
endif()

# Link dependent libraries
target_link_libraries(utils
  PUBLIC
    spdlog::spdlog
    Threads::Threads
    Iconv::Iconv
    fmt::fmt
    cctz::cctz
    ${_fs_lib}
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()