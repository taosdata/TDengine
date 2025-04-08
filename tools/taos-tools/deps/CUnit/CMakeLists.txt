project(CUnit C)

set(CUNIT_VERSION_MAJOR 3)
set(CUNIT_VERSION_MINOR 2)
set(CUNIT_VERSION_PATCH 7)
set(CUNIT_VERSION "${CUNIT_VERSION_MAJOR}.${CUNIT_VERSION_MINOR}.${CUNIT_VERSION_PATCH}")
set(CUNIT_RELEASE cunity)

if(NOT DEFINED CUNIT_DISABLE_TESTS)
  set(CUNIT_DISABLE_TESTS FALSE)
endif()
if(NOT DEFINED CUNIT_DISABLE_EXAMPLES)
  set(CUNIT_DISABLE_EXAMPLES FALSE)
endif()



cmake_minimum_required (VERSION 3.2)

if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
  # repeat here so consumers get more warnings
  set(CMAKE_C_FLAGS " -Wall -Werror -Werror=strict-prototypes")
endif()


if (MSVC)
  # force assert() to work on release mode windows
  set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /UNDEBUG")
  message(STATUS "CMAKE_C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}")
endif()

FILE(GLOB headerfiles *.h)

configure_file(CUnit/CUnit.h.in ${CMAKE_CURRENT_LIST_DIR}/CUnit/CUnit.h @ONLY)


# Package Generator  #######################################################
set(CPACK_GENERATOR TGZ DEB)
set(CPACK_PACKAGE_NAME "cunit")
set(CPACK_PACKAGE_DESCRIPTION "CUnit")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "The CUnity fork of the CUnit unit test framework")
set(CPACK_PACKAGE_VERSION_MAJOR "${CUNIT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${CUNIT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${CUNIT_VERSION_PATCH}")
set(CPACK_PACKAGE_VENDOR "CUnity")
set(CPACK_PACKAGE_CONTACT "inorton@gmail.com")
set(CPACK_PACKAGE_FILE_NAME "cunit-${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
include (CPack)

FILE(GLOB Basic Sources/Basic/*.c)
FILE(GLOB Automated Sources/Automated/*.c)
FILE(GLOB Framework Sources/Framework/*.c)
FILE(GLOB Console Sources/Console/*.c)
FILE(GLOB Test Sources/Test/*.c)
FILE(GLOB CI Sources/CI/*.c)

set(cunit_srcs
  ${Basic}
  ${Automated}
  ${CI}
  ${Framework}
  ${Console}
  )

set(cunit_libs "")

if(UNIX AND NOT APPLE AND NOT CMAKE_SYSTEM_NAME MATCHES "Linux")
  list(APPEND cunit_libs m)
endif()


add_library(cunit STATIC ${cunit_srcs})
target_link_libraries(cunit
  PUBLIC
    ${cunit_libs})
target_include_directories(cunit
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>
    $<INSTALL_INTERFACE:include>)

if(NOT CUNIT_DISABLE_TESTS)
  # for internal test program
  add_executable(cunit_test
    ${cunit_srcs}
    ${Test}
    )

  add_test(NAME 
    cunit_test COMMAND $<TARGET_FILE:cunit_test>)

  target_include_directories(cunit_test
    PRIVATE
          Sources/Framework
          Sources/Test
          .
    )
  target_link_libraries(cunit_test
    ${cunit_libs})
  target_compile_definitions(cunit_test
    PRIVATE
      CUNIT_BUILD_TESTS
      MEMTRACE
      CUNIT_DO_NOT_DEFINE_UNLESS_BUILDING_TESTS)
endif()

if(CUNIT_CMAKE_INSTALL STREQUAL yes)
  install(DIRECTORY CUnit DESTINATION include/CUnit)
  install(TARGETS cunit DESTINATION lib EXPORT cunit-targets)
  install(EXPORT cunit-targets DESTINATION CUnit/share/cmake FILE CUnitTargets.cmake)
  install(FILES CUnitConfig.cmake DESTINATION CUnit/share/cmake)
endif()
