from conans import ConanFile, MSBuild

class CyrusSaslConan(ConanFile):
    version = "2.1.26"
    license = "BSD-with-attribution"
    url = "https://github.com/Ri0n/cyrus-sasl.git"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources="../../../*"

    name = "cyrus-sasl-sasl2"
    description = "Simple Authentication and Security Layer (SASL)"
    options = {"shared": [True, False]}
    default_options = "shared=True"
    requires = "OpenSSL/1.0.2o@conan/stable"

    def build(self):
        msbuild = MSBuild(self)
        msbuild.build("win32\\cyrus-sasl-core.sln")

    def package(self):
        self.copy("*.h", dst="include\sasl", src="include")
        self.copy("*sasl2*.lib", dst="lib", keep_path=False)
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["sasl2.lib"]
