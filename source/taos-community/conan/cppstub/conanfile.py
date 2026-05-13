from conan import ConanFile
from conan.tools.files import copy, download
import os


class CppStubConan(ConanFile):
    name = "cppstub"
    version = "1.0.0"
    license = "MIT"
    url = "https://github.com/coolxv/cpp-stub"
    description = "A simple and easy-to-use C++ stub library for unit testing"
    topics = ("cpp", "stub", "testing", "mock")
    
    settings = "os", "compiler", "build_type", "arch"
    no_copy_source = True
    
    def export_sources(self):
        # Export source code directory if available locally
        copy(self, "*", src=os.path.join(self.recipe_folder, "cppstub"),
             dst=os.path.join(self.export_sources_folder, "cppstub"))

    def source(self):
        # If source code is not provided via export_sources, download from GitHub
        # For now, we rely on export_sources
        pass

    def package_id(self):
        # This is a header-only library
        self.info.clear()

    def build(self):
        # Header-only library, no build needed
        pass

    def package(self):
        source_folder = os.path.join(self.source_folder, "cppstub")
        
        # Copy license files if available
        for license_file in ["LICENSE", "COPYING", "README.md"]:
            lic_path = os.path.join(source_folder, license_file)
            if os.path.exists(lic_path):
                copy(self, license_file, src=source_folder, 
                     dst=os.path.join(self.package_folder, "licenses"))
        
        # Copy header files
        copy(self, "stub.h", src=os.path.join(source_folder, "src"), 
             dst=os.path.join(self.package_folder, "include"))
        
        # Copy platform-specific addr_any.h
        if self.settings.os == "Linux":
            platform_dir = "src_linux"
        elif self.settings.os == "Macos":
            platform_dir = "src_darwin"
        elif self.settings.os == "Windows":
            platform_dir = "src_win"
        else:
            platform_dir = "src_linux"  # Default to Linux
        
        copy(self, "addr_any.h", 
             src=os.path.join(source_folder, platform_dir),
             dst=os.path.join(self.package_folder, "include"))

    def package_info(self):
        # Header-only library, no libs to link
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        
        # Just include directories
        self.cpp_info.includedirs = ["include"]
