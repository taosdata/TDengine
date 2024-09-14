# lib_azure_sdk
set(AZURE_DIR "${TD_CONTRIB_DIR}/azure-sdk-for-cpp-azure-storage-blobs_12.13.0-beta.1")
set(AZURE_SDK_LIBRARY_DIR "${AZURE_DIR}/sdk")

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

file(GLOB AZURE_SDK_UNIFIED_SRC
    ${AZURE_SDK_SRC}
)

set(AZURE_SDK_INCLUDES
    "${AZURE_SDK_LIBRARY_DIR}/core/azure-core/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/identity/azure-identity/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-common/inc/"
    "${AZURE_SDK_LIBRARY_DIR}/storage/azure-storage-blobs/inc/"
)

add_library(_azure_sdk STATIC ${AZURE_SDK_UNIFIED_SRC})
target_compile_definitions(_azure_sdk PRIVATE BUILD_CURL_HTTP_TRANSPORT_ADAPTER)

 target_include_directories(
		_azure_sdk 
                 PUBLIC "$ENV{HOME}/.cos-local.2/include"
         )

find_library(CURL_LIBRARY curl $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
find_library(XML2_LIBRARY xml2 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
#find_library(CURL_LIBRARY curl)
#find_library(XML2_LIBRARY xml2)
find_library(SSL_LIBRARY ssl $ENV{HOME}/.cos-local.2/lib64 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
find_library(CRYPTO_LIBRARY crypto $ENV{HOME}/.cos-local.2/lib64 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
#find_library(CoreFoundation_Library CoreFoundation)
#find_library(SystemConfiguration_Library SystemConfiguration)

target_link_libraries(
		_azure_sdk 
                 PRIVATE ${CURL_LIBRARY}
                 PRIVATE ${SSL_LIBRARY}
                 PRIVATE ${CRYPTO_LIBRARY}
		 #PRIVATE xml2
		 PRIVATE ${XML2_LIBRARY}
# PRIVATE ${CoreFoundation_Library}
# PRIVATE ${SystemConfiguration_Library}
 PRIVATE zlib
)

# Originally, on Windows azure-core is built with bcrypt and crypt32 by default
if (TARGET OpenSSL::SSL)
    target_link_libraries(_azure_sdk PRIVATE OpenSSL::Crypto OpenSSL::SSL)
endif()

# Originally, on Windows azure-core is built with winhttp by default
if (TARGET td_contrib::curl)
    target_link_libraries(_azure_sdk PRIVATE td_contrib::curl)
endif()

target_include_directories(_azure_sdk SYSTEM BEFORE PUBLIC ${AZURE_SDK_INCLUDES})
add_library(td_contrib::azure_sdk ALIAS _azure_sdk)
