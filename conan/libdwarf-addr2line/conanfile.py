from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, save
import os


_ADDR2LINE_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(addr2line C)

find_package(libdwarf REQUIRED)

add_executable(addr2line addr2line.c)
target_include_directories(addr2line PRIVATE ${libdwarf_INCLUDE_DIRS})
target_link_libraries(addr2line PRIVATE libdwarf::libdwarf)

install(TARGETS addr2line RUNTIME DESTINATION bin)
"""


class LibdwarfAddr2lineConan(ConanFile):
    name = "libdwarf-addr2line"
    version = "0.3.1"
    license = "LGPL-2.1-or-later"  # informational
    url = "https://github.com/davea42/libdwarf-addr2line"
    description = "addr2line tool based on libdwarf"

    settings = "os", "compiler", "build_type", "arch"

    def requirements(self):
        self.requires("libdwarf/0.3.1")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            "https://github.com/davea42/libdwarf-addr2line/archive/9d76b420f9d1261fa7feada3a209e605f54ba859.tar.gz",
            strip_root=True,
        )

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        # Use a minimal CMakeLists (the upstream repo is CMake-based but we want deterministic behavior)
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _ADDR2LINE_CMAKELISTS)

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        # Tool-only package
        self.cpp_info.bindirs = ["bin"]
        self.cpp_info.libdirs = []
        self.cpp_info.includedirs = []
        self.cpp_info.set_property("cmake_file_name", "libdwarf-addr2line")
        self.cpp_info.set_property("cmake_target_name", "libdwarf-addr2line::libdwarf-addr2line")
