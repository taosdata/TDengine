from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, save
import os


_COS_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(cos_c_sdk C)

find_package(CURL REQUIRED)
find_package(apr REQUIRED)
find_package(aprutil REQUIRED)
find_package(mxml REQUIRED)

file(GLOB COS_SOURCES "cos_c_sdk/*.c")

add_library(cos_c_sdk_static STATIC ${COS_SOURCES})

target_include_directories(cos_c_sdk_static PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/cos_c_sdk
)

target_link_libraries(cos_c_sdk_static PUBLIC
  CURL::libcurl
  apr::apr
  aprutil::aprutil
  mxml::mxml
)

install(TARGETS cos_c_sdk_static
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

install(DIRECTORY cos_c_sdk/
  DESTINATION include/cos_c_sdk
  FILES_MATCHING PATTERN "*.h"
)
"""


class CosCSdkConan(ConanFile):
    name = "cos-c-sdk"
    version = "5.0.16"
    license = "Apache-2.0"  # informational
    url = "https://github.com/tencentyun/cos-c-sdk-v5"
    description = "Tencent COS C SDK v5"
    topics = ("cos", "tencent", "storage")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def requirements(self):
        self.requires("libcurl/8.2.1")
        self.requires("apr/1.7.6")
        self.requires("apr-util/1.6.3")
        self.requires("mxml/2.12")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            f"https://github.com/tencentyun/cos-c-sdk-v5/archive/refs/tags/v{self.version}.tar.gz",
            strip_root=True,
        )

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_SHARED_LIBS"] = bool(self.options.shared)
        tc.cache_variables["CMAKE_POSITION_INDEPENDENT_CODE"] = bool(self.options.get_safe("fPIC", True))
        tc.generate()

    def build(self):
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _COS_CMAKELISTS)

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["cos_c_sdk_static"]
        self.cpp_info.set_property("cmake_file_name", "cos-c-sdk")
        self.cpp_info.set_property("cmake_target_name", "cos-c-sdk::cos-c-sdk")
