from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import get, copy
import os


class WinIconvConan(ConanFile):
    name = "win-iconv"
    version = "0.0.8"
    license = "MIT"  # informational
    url = "https://github.com/win-iconv/win-iconv"
    description = "iconv implementation for Windows"
    topics = ("iconv", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("win-iconv recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        # Pin to the commit used by cmake/external.cmake (ext_iconv)
        get(
            self,
            "https://github.com/win-iconv/win-iconv/archive/9f98392dfecadffd62572e73e9aba878e03496c4.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_SHARED"] = False
        tc.cache_variables["BUILD_STATIC"] = True
        tc.cache_variables["CMAKE_C_FLAGS"] = "/wd4267"
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

        copy(self, "iconv.h", src=self.source_folder, dst=os.path.join(self.package_folder, "include"), keep_path=False)
        copy(self, "*.lib", src=self.build_folder, dst=os.path.join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["iconv"]
        self.cpp_info.set_property("cmake_file_name", "win-iconv")
        self.cpp_info.set_property("cmake_target_name", "win-iconv::win-iconv")
