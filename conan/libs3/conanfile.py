from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, save
import os


_LIBS3_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(libs3 C)

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

add_library(libs3
  src/bucket.c src/bucket_metadata.c src/error_parser.c src/general.c
  src/object.c src/request.c src/request_context.c
  src/response_headers_handler.c src/service_access_logging.c
  src/service.c src/simplexml.c src/util.c src/multipart.c
)

target_include_directories(libs3 PUBLIC inc)

target_link_libraries(libs3 PUBLIC
  CURL::libcurl
  OpenSSL::SSL
  OpenSSL::Crypto
  LibXml2::LibXml2
  ZLIB::ZLIB
  Threads::Threads
)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  find_library(CORE_FOUNDATION CoreFoundation)
  find_library(SYSTEM_CONFIGURATION SystemConfiguration)
  target_link_libraries(libs3 PRIVATE ${CORE_FOUNDATION} ${SYSTEM_CONFIGURATION})
  target_link_libraries(libs3 PRIVATE iconv)
endif()

if(UNIX AND NOT APPLE)
  target_link_libraries(libs3 PRIVATE m dl)
endif()

set_target_properties(libs3 PROPERTIES
  PUBLIC_HEADER "inc/libs3.h"
)

install(TARGETS libs3
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
  PUBLIC_HEADER DESTINATION include
)
"""


class Libs3Conan(ConanFile):
    name = "libs3"
    version = "4.1"
    license = "BSD-2-Clause"  # informational
    url = "https://github.com/bji/libs3"
    description = "libs3 - S3 client library"
    topics = ("s3", "aws", "storage")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def requirements(self):
        self.requires("libcurl/8.2.1")
        self.requires("openssl/3.6.0")
        self.requires("zlib/1.3.1")
        self.requires("libxml2/2.15.0")

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
        # Pin to the same commit used by cmake/external.cmake (ext_libs3)
        get(
            self,
            "https://github.com/bji/libs3/archive/98f667b248a7288c1941582897343171cfdf441c.tar.gz",
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
        # Replace upstream build files with a deterministic CMakeLists
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _LIBS3_CMAKELISTS)

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["libs3"]
        self.cpp_info.set_property("cmake_file_name", "libs3")
        self.cpp_info.set_property("cmake_target_name", "libs3::libs3")
