from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get, save
import os


_WCWIDTH_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(wcwidth C)
add_library(wcwidth STATIC wcwidth.c)
install(TARGETS wcwidth ARCHIVE DESTINATION lib)
install(FILES wcwidth.h DESTINATION include)
"""


class WcwidthCjkConan(ConanFile):
    name = "wcwidth-cjk"
    version = "master"
    license = "MIT"  # informational
    url = "https://github.com/fumiyas/wcwidth-cjk"
    description = "wcwidth implementation (CJK aware)"
    topics = ("wcwidth", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("wcwidth-cjk recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            "https://github.com/fumiyas/wcwidth-cjk/archive/a1b1e2c346a563f6538e46e1d29c265bdd5b1c9a.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _WCWIDTH_CMAKELISTS)
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["wcwidth"]
        self.cpp_info.set_property("cmake_file_name", "wcwidth-cjk")
        self.cpp_info.set_property("cmake_target_name", "wcwidth-cjk::wcwidth-cjk")
