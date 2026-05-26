cmake_minimum_required(VERSION 3.4...3.18)
project(${SKBUILD_PROJECT_NAME} VERSION ${SKBUILD_PROJECT_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fsanitize=address")

if (SKBUILD)
  find_package(pybind11 CONFIG REQUIRED)
else ()
  add_subdirectory(extern/pybind11)
endif()	

include_directories(extern/plog/include)

include_directories(src)

add_library(taospyudf SHARED src/taospyudf.cpp src/taospyudf.h)
target_link_libraries(taospyudf PUBLIC pybind11::embed)

# dladdr / Dl_info are GNU extensions; on glibc they are only declared from
# <dlfcn.h> when _GNU_SOURCE is set.  macOS exposes them unconditionally and
# Windows doesn't compile that code path at all.
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_compile_definitions(taospyudf PRIVATE _GNU_SOURCE)
  target_link_libraries(taospyudf PRIVATE ${CMAKE_DL_LIBS})
endif ()

if (SKBUILD)
  install(TARGETS taospyudf DESTINATION taospyudf)
endif ()

# Legacy convenience install for direct (non-pip) builds. Only meaningful
# on Unix-like systems where the libdir is on the default loader path.
# Windows users should install via ``pip install taospyudf`` (the wheel
# places the DLL inside the Python package directory).
# Honor CMAKE_INSTALL_PREFIX (default /usr/local) and the multiarch-aware
# libdir from GNUInstallDirs so distros / packagers can relocate the install.
if (NOT WIN32 AND NOT SKBUILD)
  include(GNUInstallDirs)
  install(TARGETS taospyudf DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif ()
