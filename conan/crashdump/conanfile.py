from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get, save
import os


_CRASHDUMP_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(crashdump C)

add_executable(dumper dumper/dumper.c)
target_link_libraries(dumper Dbghelp)
install(TARGETS dumper RUNTIME DESTINATION bin)

add_library(crashdump STATIC crasher/crasher.c)
install(TARGETS crashdump ARCHIVE DESTINATION lib)
install(FILES crasher/crasher.h DESTINATION include)
"""


class CrashdumpConan(ConanFile):
    name = "crashdump"
    version = "master"
    license = "MIT"  # informational
    url = "https://github.com/Arnavion/crashdump"
    description = "Windows crash dump helper"
    topics = ("crashdump", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("crashdump recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            "https://github.com/Arnavion/crashdump/archive/149b43c10debdf28a2c50d79dee5ff344d83bd06.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _CRASHDUMP_CMAKELISTS)
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["crashdump"]
        self.cpp_info.system_libs = ["Dbghelp"]
        self.cpp_info.set_property("cmake_file_name", "crashdump")
        self.cpp_info.set_property("cmake_target_name", "crashdump::crashdump")
