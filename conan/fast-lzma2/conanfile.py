from conan import ConanFile
from conan.tools.files import copy, get, replace_in_file
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

        # Patch upstream Makefile for macOS (Darwin ld doesn't support ELF-style -soname)
        makefile = os.path.join(source_folder, "Makefile")
        if os.path.isfile(makefile) and self.settings.os == "Macos":
            # Build a proper dylib when shared=True, and make the linker invocation portable.
            replace_in_file(
                self,
                makefile,
                "SONAME:=libfast-lzma2.so.1",
                "SONAME:=libfast-lzma2.1.dylib",
                strict=False,
            )
            replace_in_file(
                self,
                makefile,
                "REAL_NAME:=libfast-lzma2.so.1.0",
                "REAL_NAME:=$(SONAME)",
                strict=False,
            )
            replace_in_file(
                self,
                makefile,
                "LINKER_NAME=libfast-lzma2.so",
                "LINKER_NAME=libfast-lzma2.dylib",
                strict=False,
            )
            replace_in_file(
                self,
                makefile,
                "-Wl,-soname,$(SONAME)",
                "-Wl,-install_name,@rpath/$(SONAME)",
                strict=False,
            )
            replace_in_file(
                self,
                makefile,
                " -shared -pthread ",
                " -dynamiclib -pthread ",
                strict=False,
            )

        # Build make command
        cflags = "-Wall -O2 -pthread"
        if self.options.get_safe("fPIC"):
            cflags += " -fPIC"

        # Adjust compilation options based on build_type
        if self.settings.build_type == "Debug":
            cflags = cflags.replace("-O2", "-O0 -g")

        # Execute make compilation
        # NOTE: Conan's `settings.compiler` is a compiler *family* (e.g. "apple-clang"),
        # not necessarily an executable name available in PATH. Prefer the build env's
        # CC (if provided), otherwise let `make` pick the default compiler (usually `cc`).
        cc = os.getenv("CC")
        cmd = f'make CFLAGS="{cflags}"'
        if cc:
            cmd += f" CC={cc}"
        cmd += " libfast-lzma2"
        self.run(cmd, cwd=source_folder)

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
