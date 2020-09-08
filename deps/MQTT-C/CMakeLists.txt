CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# MQTT-C build options
option(MQTT_C_OpenSSL_SUPPORT "Build MQTT-C with OpenSSL support?" OFF)
option(MQTT_C_MbedTLS_SUPPORT "Build MQTT-C with mbed TLS support?" OFF)
option(MQTT_C_EXAMPLES "Build MQTT-C examples?" ON)
option(MQTT_C_TESTS "Build MQTT-C tests?" OFF)

list (APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# MQTT-C library
ADD_LIBRARY(mqttc STATIC 
    src/mqtt_pal.c
    src/mqtt.c
)
TARGET_INCLUDE_DIRECTORIES(mqttc PUBLIC include)
target_link_libraries(mqttc PUBLIC 
    $<$<C_COMPILER_ID:MSVS>:ws2_32>
)


# Configure with OpenSSL support
if(MQTT_C_OpenSSL_SUPPORT)
    find_package(OpenSSL REQUIRED)
    target_link_libraries(mqttc INTERFACE OpenSSL::SSL)
    target_compile_definitions(mqttc PUBLIC MQTT_USE_BIO)
endif()

# Configure with mbed TLS support
if(MQTT_C_MbedTLS_SUPPORT)
    find_package(MbedTLS REQUIRED)
    TARGET_INCLUDE_DIRECTORIES(mqttc PUBLIC ${MBEDTLS_INCLUDE_DIRS})
    target_link_libraries(mqttc INTERFACE ${MBEDTLS_LIBRARY})
    target_compile_definitions(mqttc PUBLIC MQTT_USE_MBEDTLS)
endif()

# Build examples
if(MQTT_C_EXAMPLES)
    find_package(Threads REQUIRED)

    if(MQTT_C_OpenSSL_SUPPORT)
        add_executable(bio_publisher examples/bio_publisher.c)
        target_link_libraries(bio_publisher Threads::Threads mqttc)

        add_executable(openssl_publisher examples/openssl_publisher.c)
        target_link_libraries(openssl_publisher Threads::Threads mqttc)
    elseif(MQTT_C_MbedTLS_SUPPORT)
        add_executable(mbedtls_publisher examples/mbedtls_publisher.c)
        target_link_libraries(mbedtls_publisher Threads::Threads mqttc ${MBEDX509_LIBRARY} ${MBEDCRYPTO_LIBRARY})
    else()
        add_executable(simple_publisher examples/simple_publisher.c)
        target_link_libraries(simple_publisher Threads::Threads mqttc)

        add_executable(simple_subscriber examples/simple_subscriber.c)
        target_link_libraries(simple_subscriber Threads::Threads mqttc)

        add_executable(reconnect_subscriber examples/reconnect_subscriber.c)
        target_link_libraries(reconnect_subscriber Threads::Threads mqttc)
    endif()
endif()

# Build tests
if(MQTT_C_TESTS)    
    find_path(CMOCKA_INCLUDE_DIR cmocka.h)
    find_library(CMOCKA_LIBRARY cmocka)
    if((NOT CMOCKA_INCLUDE_DIR) OR (NOT CMOCKA_LIBRARY))
        message(FATAL_ERROR "Failed to find cmocka! Add cmocka's install prefix to CMAKE_PREFIX_PATH to resolve this error.")
    endif()

    add_executable(tests tests.c)
    target_link_libraries(tests ${CMOCKA_LIBRARY} mqttc)
    TARGET_INCLUDE_DIRECTORIES(tests PRIVATE ${CMOCKA_INCLUDE_DIR})
endif()

# Install includes and library
# install(TARGETS mqttc 
#     DESTINATION lib
# )
# install(DIRECTORY include/
#    DESTINATION include)
