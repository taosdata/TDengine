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

# Link dependent libraries
target_link_libraries(utils
  PUBLIC
    spdlog::spdlog
    Threads::Threads
    Iconv::Iconv
    fmt::fmt
    cctz::cctz
)

# Enable tests
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()