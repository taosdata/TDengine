# openssl_test
add_executable(openssl_test opensslTest.c)
target_link_libraries(openssl_test ssl crypto)
