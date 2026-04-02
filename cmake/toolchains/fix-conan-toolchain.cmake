# Strip CMAKE_GENERATOR_PLATFORM and CMAKE_GENERATOR_TOOLSET from conan_toolchain.cmake
# These are only valid for Visual Studio multi-config generators.
file(READ "${INPUT_FILE}" _content)
string(REGEX REPLACE "set\\(CMAKE_GENERATOR_PLATFORM[^\n]*\n" "" _content "${_content}")
string(REGEX REPLACE "set\\(CMAKE_GENERATOR_TOOLSET[^\n]*\n" "" _content "${_content}")
file(WRITE "${INPUT_FILE}" "${_content}")
