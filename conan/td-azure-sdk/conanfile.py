from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, save
import os


_AZURE_CMAKELISTS = r"""
cmake_minimum_required(VERSION 3.16)
project(td_azure_sdk CXX)

find_package(CURL REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(LibXml2 REQUIRED)
find_package(ZLIB REQUIRED)

set(AZURE_SDK_LIBRARY_DIR sdk)

file(GLOB AZURE_SDK_SRC
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/credentials/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/cryptography/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/http/curl/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/io/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/src/tracing/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/src/private/*.cpp"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/src/*.cpp"
)

set(AZURE_SDK_INCLUDES
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/inc/"
)

add_library(td_azure_sdk STATIC ${AZURE_SDK_SRC})

target_compile_definitions(td_azure_sdk PRIVATE BUILD_CURL_HTTP_TRANSPORT_ADAPTER)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_compile_features(td_azure_sdk PRIVATE cxx_std_20)
endif()

target_include_directories(td_azure_sdk PUBLIC ${AZURE_SDK_INCLUDES})

target_link_libraries(td_azure_sdk PRIVATE
  CURL::libcurl
  OpenSSL::SSL
  OpenSSL::Crypto
  LibXml2::LibXml2
  ZLIB::ZLIB
)

install(TARGETS td_azure_sdk
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

install(
  DIRECTORY
    ${AZURE_SDK_LIBRARY_DIR}/core/azure-core/inc/azure
    ${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/inc/azure
    ${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/inc/azure
    ${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/inc/azure
  DESTINATION include
)
"""


class TdAzureSdkConan(ConanFile):
    name = "td-azure-sdk"
    version = "12.13.0-beta.1"
    license = "MIT"  # informational; upstream components vary
    url = "https://github.com/Azure/azure-sdk-for-cpp"
    description = "Subset of Azure SDK for C++ used by TDengine shared storage"
    topics = ("azure", "storage")

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def requirements(self):
        self.requires("libcurl/>=8.2.1")
        self.requires("openssl/>=3.0.0")
        self.requires("zlib/1.3.1")
        self.requires("libxml2/2.15.0")

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def layout(self):
        cmake_layout(self)

    def source(self):
        # Pin to the tag used by cmake/external.cmake (ext_azure)
        get(
            self,
            "https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-blobs_12.13.0-beta.1.tar.gz",
            strip_root=True,
        )

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.cache_variables["BUILD_SHARED_LIBS"] = bool(self.options.shared)
        tc.cache_variables["CMAKE_POSITION_INDEPENDENT_CODE"] = bool(self.options.get_safe("fPIC", True))
        tc.generate()

    def build(self):
        save(self, os.path.join(self.source_folder, "CMakeLists.txt"), _AZURE_CMAKELISTS)

        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["td_azure_sdk"]
        self.cpp_info.set_property("cmake_file_name", "td-azure-sdk")
        self.cpp_info.set_property("cmake_target_name", "td-azure-sdk::td-azure-sdk")
