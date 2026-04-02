###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

if(HAVE_CARGO)
  if(TODBC_WINDOWS)
    if(TODBC_X86)
        set(RUST_TARGET "i686-pc-windows-msvc")
    else()
        set(RUST_TARGET "x86_64-pc-windows-msvc")
    endif()
    message("RUST_TARGET: ${RUST_TARGET}")

    add_custom_target(rust_test_build ALL
        COMMAND ${CARGO_NAME} build --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target=${RUST_TARGET} --target-dir ${CMAKE_CURRENT_BINARY_DIR})

    if(NOT FAKE_TAOS)
        add_test(NAME rust_test
            COMMAND ${CARGO_NAME} run --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target=${RUST_TARGET} --target-dir ${CMAKE_CURRENT_BINARY_DIR})
    endif()

    add_test(NAME ${CARGO_NAME}_test
        COMMAND ${CARGO_NAME} test --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target=${RUST_TARGET} --target-dir ${CMAKE_CURRENT_BINARY_DIR})
  else()
    add_custom_target(rust_test_build ALL
        COMMAND env RUST_BACKTRACE=1 ${CARGO_NAME} build --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})

    add_test(NAME rust_test
        COMMAND env RUST_BACKTRACE=1 ${CARGO_NAME} run --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})

    add_test(NAME ${CARGO_NAME}_test
        COMMAND env RUST_BACKTRACE=1 ${CARGO_NAME} test --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml --target-dir ${CMAKE_CURRENT_BINARY_DIR})
  endif()

  if(ENABLE_VALGRIND)
    add_test(NAME Vrust_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/debug/main)
  endif()
endif()

