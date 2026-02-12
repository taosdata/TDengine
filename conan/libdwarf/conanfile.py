from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get
import os


class LibdwarfConan(ConanFile):
    name = "libdwarf"
    version = "0.3.1"
    license = "LGPL-2.1-or-later"  # upstream project licensing varies; keep informational
    url = "https://github.com/davea42/libdwarf-code"
    description = "libdwarf for DWARF debug information processing"
    topics = ("dwarf", "debug")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False], "with_libelf": [True, False]}
    default_options = {"shared": False, "fPIC": True, "with_libelf": True}

    def requirements(self):
        # ext_dwarf is built with zlib available.
        self.requires("zlib/1.3.1")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.get_safe("shared"):
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self)

    def source(self):
        get(
            self,
            f"https://github.com/davea42/libdwarf-code/archive/refs/tags/libdwarf-{self.version}.tar.gz",
            strip_root=True,
        )

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.cache_variables["CMAKE_INSTALL_LIBDIR"] = "lib"
        tc.cache_variables["BUILD_SHARED_LIBS"] = bool(self.options.shared)
        tc.cache_variables["CMAKE_POSITION_INDEPENDENT_CODE"] = True
        tc.cache_variables["DO_TESTING"] = False
        tc.cache_variables["DWARF_WITH_LIBELF"] = bool(self.options.with_libelf)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

        # Ensure headers are available as <dwarf.h> and <libdwarf.h>
        # (TDengine source includes them without a subdir)
        hdr_src = os.path.join(self.source_folder, "src", "lib", "libdwarf")
        copy(self, "dwarf.h", src=hdr_src, dst=os.path.join(self.package_folder, "include"), keep_path=False)
        copy(self, "libdwarf.h", src=hdr_src, dst=os.path.join(self.package_folder, "include"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["dwarf"]
        # libdwarf commonly needs libelf on Linux when built with libelf support.
        if self.settings.os in ["Linux", "FreeBSD"] and self.options.with_libelf:
            self.cpp_info.system_libs.append("elf")
        self.cpp_info.set_property("cmake_file_name", "libdwarf")
        self.cpp_info.set_property("cmake_target_name", "libdwarf::libdwarf")
