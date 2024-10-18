set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_executable (
     azure-test
     main.cpp
)

find_library(CURL_LIBRARY curl $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
find_library(XML2_LIBRARY xml2 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
#find_library(XML2_LIBRARY xml2)
find_library(SSL_LIBRARY ssl $ENV{HOME}/.cos-local.2/lib64 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
find_library(CRYPTO_LIBRARY crypto $ENV{HOME}/.cos-local.2/lib64 $ENV{HOME}/.cos-local.2/lib NO_DEFAULT_PATH)
#find_library(CoreFoundation_Library CoreFoundation)
#find_library(SystemConfiguration_Library SystemConfiguration)

 target_link_libraries(
   azure-test
 PRIVATE _azure_sdk
 PRIVATE ${CURL_LIBRARY}
 PRIVATE ${XML2_LIBRARY}
 PRIVATE ${SSL_LIBRARY}
 PRIVATE ${CRYPTO_LIBRARY}
 PRIVATE dl
 PRIVATE pthread
)
