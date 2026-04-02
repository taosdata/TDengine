from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get
import os


class AvroCConan(ConanFile):
    name = "avro-c"
    version = "1.11.3"
    license = "Apache-2.0"
    url = "https://github.com/apache/avro"
    description = "Apache Avro is a data serialization system (C implementation)"
    topics = ("avro", "serialization", "apache")

    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
    }

    # No need to export sources for this recipe

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        # This is a C library, remove C++ related settings
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def requirements(self):
        # Avro C depends on zlib, jansson, and snappy
        self.requires("zlib/1.3.1")
        self.requires("jansson/2.14")
        self.requires("snappy/1.2.1")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def source(self):
        # Download from Apache Avro GitHub repository
        # Use the commit hash from external.cmake: 7b106b12ae22853c977259710d92a237d76f2236
        get(
            self,
            "https://github.com/apache/avro/archive/7b106b12ae22853c977259710d92a237d76f2236.tar.gz",
            strip_root=True,
        )

    def generate(self):
        tc = CMakeToolchain(self)
        # Apply patches and configurations similar to external.cmake
        tc.cache_variables["CMAKE_INSTALL_LIBDIR"] = "lib"
        tc.cache_variables["CMAKE_BUILD_TYPE"] = str(self.settings.build_type)
        tc.cache_variables["BUILD_SHARED_LIBS"] = self.options.shared
        
        # Point to Conan dependencies (Conan will handle this automatically via CMakeDeps)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        # Build from lang/c subdirectory
        cmake.configure(build_script_folder=os.path.join(self.source_folder, "lang", "c"))
        cmake.build()

    def package(self):
        # Copy license
        copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["avro"]
        
        # Set library name
        if self.settings.os == "Windows":
            if not self.options.shared:
                self.cpp_info.defines.append("AVRO_STATIC")
        
        # Add system libraries if needed
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.extend(["m", "pthread"])
