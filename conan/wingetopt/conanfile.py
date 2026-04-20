from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import get


class WingetoptConan(ConanFile):
    name = "wingetopt"
    version = "master"
    license = "MIT"  # informational
    url = "https://github.com/alex85k/wingetopt"
    description = "getopt() implementation for Windows"
    topics = ("getopt", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("wingetopt recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            "https://github.com/alex85k/wingetopt/archive/e8531ed21b44f5a723c1dd700701b2a58ce3ea01.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        try:
            cmake.install()
        except Exception:
            pass

    def package_info(self):
        self.cpp_info.libs = ["wingetopt"]
        self.cpp_info.set_property("cmake_file_name", "wingetopt")
        self.cpp_info.set_property("cmake_target_name", "wingetopt::wingetopt")
