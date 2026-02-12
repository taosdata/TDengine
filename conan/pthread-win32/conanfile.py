from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import get, copy
import os


class PthreadWin32Conan(ConanFile):
    name = "pthread-win32"
    version = "3.0.3.1"
    license = "LGPL-2.1-or-later"  # informational
    url = "https://github.com/GerHobbelt/pthread-win32"
    description = "POSIX threads for Windows"
    topics = ("pthread", "windows")

    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        if self.settings.os != "Windows":
            raise Exception("pthread-win32 recipe is intended for Windows only")

    def layout(self):
        cmake_layout(self)

    def source(self):
        # Pin to the commit used by cmake/external.cmake (ext_pthread)
        get(
            self,
            "https://github.com/GerHobbelt/pthread-win32/archive/3309f4d6e7538f349ae450347b02132ecb0606a7.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_SHARED_LIBS"] = True
        # Matches external.cmake suppression
        tc.cache_variables["CMAKE_C_FLAGS"] = "/wd4244"
        tc.cache_variables["CMAKE_CXX_FLAGS"] = "/wd4244"
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        # Try CMake install if supported; otherwise copy common outputs.
        cmake = CMake(self)
        try:
            cmake.install()
        except Exception:
            pass

        copy(self, "pthread.h", src=os.path.join(self.source_folder, "include"), dst=os.path.join(self.package_folder, "include"), keep_path=False)
        copy(self, "*.lib", src=self.build_folder, dst=os.path.join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dll", src=self.build_folder, dst=os.path.join(self.package_folder, "bin"), keep_path=False)

    def package_info(self):
        # ExternalProject expects pthreadVC3
        self.cpp_info.libs = ["pthreadVC3"]
        self.cpp_info.set_property("cmake_file_name", "pthread-win32")
        self.cpp_info.set_property("cmake_target_name", "pthread-win32::pthread-win32")
