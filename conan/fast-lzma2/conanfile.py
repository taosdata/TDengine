from conan import ConanFile
from conan.tools.files import copy, get
import os


class FastLzma2Conan(ConanFile):
    name = "fast-lzma2"
    version = "1.0.1"
    license = "BSD-3-Clause"
    url = "https://github.com/conor42/fast-lzma2"
    description = "Fast LZMA2 Library - an optimized LZMA2 compression algorithm"
    topics = ("compression", "lzma2", "fast-lzma2")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    # Pin upstream source to a specific commit (avoid relying on local vendored sources)
    _commit = "ded964d203cabe1a572d2c813c55e8a94b4eda48"

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        # This is a C library, remove C++ related settings
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def source(self):
        # Fetch sources from GitHub at a pinned commit.
        # Using an archive URL avoids requiring git during build.
        get(
            self,
            f"https://github.com/conor42/fast-lzma2/archive/{self._commit}.tar.gz",
            strip_root=True,
        )

    def build(self):
        # Enter source code directory
        source_folder = self.source_folder

        # Build make command
        cflags = "-Wall -O2 -pthread"
        asflags = "-Wa,--noexecstack"
        if self.options.get_safe("fPIC"):
            cflags += " -fPIC"

        # Adjust compilation options based on build_type
        if self.settings.build_type == "Debug":
            cflags = cflags.replace("-O2", "-O0 -g")

        # Execute make compilation
        self.run(
            f'make CFLAGS="{cflags}" ASFLAGS="{asflags}" CC={self.settings.get_safe("compiler", default="gcc")} libfast-lzma2',
            cwd=source_folder,
        )

    def package(self):
        source_folder = self.source_folder

        # Copy license files
        copy(
            self,
            "LICENSE",
            src=source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        copy(
            self,
            "COPYING",
            src=source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )

        # Copy header files
        copy(
            self,
            "fast-lzma2.h",
            src=source_folder,
            dst=os.path.join(self.package_folder, "include"),
        )
        copy(
            self,
            "fl2_errors.h",
            src=source_folder,
            dst=os.path.join(self.package_folder, "include"),
        )

        # Copy library files
        if self.options.shared:
            # Shared library
            if self.settings.os == "Windows":
                copy(
                    self,
                    "*.dll",
                    src=source_folder,
                    dst=os.path.join(self.package_folder, "bin"),
                    keep_path=False,
                )
            elif self.settings.os == "Macos":
                copy(
                    self,
                    "*.dylib*",
                    src=source_folder,
                    dst=os.path.join(self.package_folder, "lib"),
                    keep_path=False,
                )
            else:  # Linux
                copy(
                    self,
                    "*.so*",
                    src=source_folder,
                    dst=os.path.join(self.package_folder, "lib"),
                    keep_path=False,
                )
        else:
            # Static library
            copy(
                self,
                "*.a",
                src=source_folder,
                dst=os.path.join(self.package_folder, "lib"),
                keep_path=False,
            )

    def package_info(self):
        self.cpp_info.libs = ["fast-lzma2"]

        # Add system library dependencies
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("pthread")

        # Set library name
        if self.settings.os == "Windows" and self.options.shared:
            self.cpp_info.defines.append("FL2_DLL_IMPORT=1")
