from conans import ConanFile, MSBuild

class CyrusSaslGssapi2Conan(ConanFile):
    version = "2.1.26"
    license = "BSD-with-attribution"
    url = "https://github.com/Ri0n/cyrus-sasl.git"
    settings = "os", "compiler", "build_type", "arch"
    exports_sources="../../../*"

    name = "cyrus-sasl-gssapiv2"
    description = "Cyrus SASL GSSAPIv2 plugin"
    options = {"shared": [True]}
    default_options = "shared=True"
    build_requires = "OpenSSL/1.0.2o@conan/stable"
    requires = "krb5-gssapi/1.16.1@rion/stable"

    def build(self):
        msbuild = MSBuild(self)
        msbuild.build("win32\\cyrus-sasl-gssapiv2.sln")

    def package(self):
        self.copy("*.dll", dst="bin", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
