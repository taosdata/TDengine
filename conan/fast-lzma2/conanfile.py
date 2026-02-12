from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.files import copy, get
import os


class FastLzma2Conan(ConanFile):
    name = "fast-lzma2"
    version = "1.0.1"
    license = "BSD-3-Clause"
    url = "https://github.com/conor42/fast-lzma2"
    description = "Fast LZMA2 Library - an optimized LZMA2 compression algorithm"
    topics = ("compression", "lzma2", "fast-lzma2")

    def _src_dir(self):
        """Return the directory containing the upstream Makefile.

        Depending on how the sources are exported, they might live in:
        - <source_folder>/fast-lzma2
        - <source_folder>
        - <source_folder>/fast-lzma2/fast-lzma2 (observed in some Conan export/copy layouts)
        """
        candidates = [
            os.path.join(self.source_folder, "fast-lzma2"),
            self.source_folder,
            os.path.join(self.source_folder, "fast-lzma2", "fast-lzma2"),
        ]
        for d in candidates:
            if os.path.isfile(os.path.join(d, "Makefile")):
                return d
        raise ConanInvalidConfiguration(
            "fast-lzma2 sources not found: expected a Makefile in one of: "
            + ", ".join(candidates)
        )

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    def export_sources(self):
        # Export bundled source tree.
        # NOTE:
        # - Conan's pattern matching can miss root-level files like "Makefile" when using only "**/*".
        # - Exclude .git to avoid exporting VCS metadata.
        #
        # Keep export layout flat (export_sources_folder/Makefile, etc.). This avoids an extra
        # directory layer that can appear in Conan's source→build copy step on CI.
        src_dir = os.path.join(self.recipe_folder, "fast-lzma2")
        dst_dir = self.export_sources_folder
        excludes = [".git/*", ".git/**"]

        copy(self, "Makefile", src=src_dir, dst=dst_dir, keep_path=False, excludes=excludes)
        copy(self, "*", src=src_dir, dst=dst_dir, keep_path=True, excludes=excludes)
        copy(self, "**/*", src=src_dir, dst=dst_dir, keep_path=True, excludes=excludes)

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
        # Prefer bundled sources via export_sources(); fallback to downloading upstream tarball
        # if sources are missing (e.g. mis-exported recipe in CI cache).
        candidates = [
            os.path.join(self.source_folder, "Makefile"),
            os.path.join(self.source_folder, "fast-lzma2", "Makefile"),
            os.path.join(self.source_folder, "fast-lzma2", "fast-lzma2", "Makefile"),
        ]
        if any(os.path.isfile(p) for p in candidates):
            return

        get(
            self,
            f"https://github.com/conor42/fast-lzma2/archive/v{self.version}.tar.gz",
            strip_root=True,
        )

    def build(self):
        source_folder = self._src_dir()

        # Build make command
        cflags = "-Wall -O2 -pthread"
        if self.options.get_safe("fPIC"):
            cflags += " -fPIC"

        # Adjust compilation options based on build_type
        if self.settings.build_type == "Debug":
            cflags = cflags.replace("-O2", "-O0 -g")

        # Execute make compilation
        # NOTE: don't force CC here; conan profiles / environment may override it.
        self.run(
            f'make CFLAGS="{cflags}" libfast-lzma2',
            cwd=source_folder,
        )

    def package(self):
        source_folder = self._src_dir()

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
