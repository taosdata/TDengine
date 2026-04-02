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

macro(tod_find_prog name version prog ver_arg ver_regex var_type)
  find_program(${name} NO_CACHE NAMES ${prog})
  if (${name})
    set(${name}_NAME ${prog})
    if ("${var_type}" STREQUAL "OUTPUT_VARIABLE")
      execute_process(COMMAND ${prog} ${ver_arg} ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ${name}_VERSION)
    else ()
      execute_process(COMMAND ${prog} ${ver_arg} OUTPUT_QUIET ERROR_STRIP_TRAILING_WHITESPACE ERROR_VARIABLE ${name}_VERSION)
    endif ()
    if(NOT "${${name}_VERSION}" STREQUAL "")
      string(REGEX REPLACE ${ver_regex} "\\1" ${name}_VERSION_ONLY ${${name}_VERSION})

      if(NOT "${${name}_VERSION_ONLY}" STREQUAL "")
        if (${${name}_VERSION_ONLY} VERSION_GREATER_EQUAL ${version})
          set(HAVE_${name} ON)
          message(STATUS "${Green}${${name}_VERSION} found: `${${name}}`, please be noted, ${prog} v${version} and above are expected compatible${ColorReset}")
        endif()
      endif()
    endif()
  endif ()
endmacro()

macro(set_colorful)
  string(ASCII 27 Esc)
  set(ColorReset  "${Esc}[m")
  set(ColorBold   "${Esc}[1m")
  set(Red         "${Esc}[31m")
  set(Green       "${Esc}[32m")
  set(Yellow      "${Esc}[33m")
  set(Blue        "${Esc}[34m")
  set(Magenta     "${Esc}[35m")
  set(Cyan        "${Esc}[36m")
  set(White       "${Esc}[37m")
  set(BoldRed     "${Esc}[1;31m")
  set(BoldGreen   "${Esc}[1;32m")
  set(BoldYellow  "${Esc}[1;33m")
  set(BoldBlue    "${Esc}[1;34m")
  set(BoldMagenta "${Esc}[1;35m")
  set(BoldCyan    "${Esc}[1;36m")
  set(BoldWhite   "${Esc}[1;37m")
endmacro()

macro(check_requirements)
  set(TAOS_ODBC_LOCAL_REPO ${CMAKE_SOURCE_DIR}/.externals)
  include(CheckSymbolExists)
  include(ExternalProject)

  ## prepare `cjson`
  set(CJSON_INSTALL_PATH ${TAOS_ODBC_LOCAL_REPO}/install/cjson)
  ExternalProject_Add(ex_cjson
      GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
      GIT_TAG v1.7.15
      GIT_SHALLOW TRUE
      PREFIX "${TAOS_ODBC_LOCAL_REPO}/build/cjson"
      CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${CJSON_INSTALL_PATH}"
      CMAKE_ARGS "-DBUILD_SHARED_LIBS:BOOL=OFF"
      CMAKE_ARGS "-DENABLE_CJSON_TEST:BOOL=OFF"
      )

  ## prepare `iconv`
  if(TODBC_WINDOWS AND USE_WIN_ICONV)
    set(ICONV_INSTALL_PATH ${TAOS_ODBC_LOCAL_REPO}/install/iconv)
    ExternalProject_Add(ex_iconv
        GIT_REPOSITORY https://github.com/win-iconv/win-iconv.git
        GIT_TAG v0.0.8
        GIT_SHALLOW TRUE
        PREFIX "${TAOS_ODBC_LOCAL_REPO}/build/iconv"
        CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${ICONV_INSTALL_PATH}"
        CMAKE_ARGS "-DBUILD_STATIC:BOOL=ON"
        CMAKE_ARGS "-DBUILD_SHARED:BOOL=OFF"
        )
  endif()

  if(BUILD_TAOSWSD_EXPERIMENTAL)
    ## prepare `libwebsockets`
    set(LIBWEBSOCKETS_INSTALL_PATH ${TAOS_ODBC_LOCAL_REPO}/install)
    ExternalProject_Add(ex_libwebsockets
        GIT_REPOSITORY https://github.com/warmcat/libwebsockets.git
        GIT_TAG v4.3.2
        GIT_SHALLOW TRUE
        PREFIX "${TAOS_ODBC_LOCAL_REPO}/build/libwebsockets"
        CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${LIBWEBSOCKETS_INSTALL_PATH}"
        CMAKE_ARGS "-DLWS_WITH_MINIMAL_EXAMPLES:BOOL=ON"
        )
  endif()

  ## set required lib path
  set(REQUIRED_LIB_PATH "")
  if(TODBC_WINDOWS)
    if(TODBC_X86)
      set(REQUIRED_LIB_PATH "C:/TDengine/taos_odbc/x86/lib")
    else()
      set(REQUIRED_LIB_PATH "C:/TDengine/taos_odbc/x64/lib")
    endif()
  elseif(TODBC_DARWIN)
    set(REQUIRED_LIB_PATH "")
  else()
    set(REQUIRED_LIB_PATH "")
  endif()

  ## check `taos`
  set(TAOS_LIB_NAME "taos")
  if(NOT FAKE_TAOS)
    find_library(TAOS NAMES ${TAOS_LIB_NAME} PATHS ${REQUIRED_LIB_PATH})
    if(${TAOS} STREQUAL TAOS-NOTFOUND)
      message(FATAL_ERROR "${Red}`libtaos.so/libtaos.dylib/taos.dll/taos.lib` is required but not found, you may refer to https://github.com/taosdata/TDengine${ColorReset}")
    endif()

    set(CMAKE_REQUIRED_LIBRARIES ${TAOS_LIB_NAME})
    if(TODBC_DARWIN)
      set(CMAKE_REQUIRED_INCLUDES /usr/local/include)
      set(CMAKE_REQUIRED_LINK_OPTIONS -L/usr/local/lib)
    elseif(TODBC_WINDOWS)
      set(CMAKE_REQUIRED_INCLUDES C:/TDengine/include)
      set(CMAKE_REQUIRED_LINK_OPTIONS /LIBPATH:${REQUIRED_LIB_PATH})
    endif()
    check_symbol_exists(taos_query "taos.h" HAVE_TAOS)
    if(NOT HAVE_TAOS)
      message(FATAL_ERROR "${Red}`taos.h` is required but not found, you may refer to https://github.com/taosdata/TDengine${ColorReset}")
    endif()
  endif()

  ## check `taosws`
  set(TAOSWS_LIB_NAME "taosws")
  find_library(TAOSWS NAMES ${TAOSWS_LIB_NAME} PATHS ${REQUIRED_LIB_PATH})
  if(${TAOSWS} STREQUAL TAOSWS-NOTFOUND)
    set(ErrorMessage "`libtaosws.so/libtaosws.dylib/taosws.dll/taosws.lib` is not found, you may refer to https://github.com/taosdata/TDengine")
    if(TODBC_WINDOWS AND TODBC_X86)
      message(FATAL_ERROR "${Red}${ErrorMessage}${ColorReset}")
    else()
      message(STATUS "${Yellow}${ErrorMessage}${ColorReset}")
    endif()
  else()
    set(CMAKE_REQUIRED_LIBRARIES ${TAOSWS_LIB_NAME})
    if(TODBC_DARWIN)
      set(CMAKE_REQUIRED_INCLUDES /usr/local/include)
      set(CMAKE_REQUIRED_LINK_OPTIONS -L/usr/local/lib)
    elseif(TODBC_WINDOWS)
      set(CMAKE_REQUIRED_INCLUDES C:/TDengine/include)
      set(CMAKE_REQUIRED_LINK_OPTIONS /LIBPATH:${REQUIRED_LIB_PATH})
    endif()
    if(WITH_TAOSWS)
      check_symbol_exists(ws_query "taosws.h" HAVE_TAOSWS)
      if(NOT HAVE_TAOSWS)
        message(STATUS "${Yellow}"
                       "`taosws.h` is not found, you may refer to https://github.com/taosdata/TDengine"
                       "${ColorReset}")
      endif()
    endif()
  endif()

  if(TODBC_DARWIN)
    ## check `iconv`
    find_package(Iconv)
    if(NOT Iconv_FOUND)
      message(FATAL_ERROR "${Red}you need to install `iconv` first${ColorReset}")
    endif()
  endif()

  ## check `flex`
  find_package(FLEX)
  if(NOT FLEX_FOUND)
    message(FATAL_ERROR "${Red}you need to install `flex` first${ColorReset}")
  endif()
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_C_COMPILER_VERSION VERSION_LESS 5.0.0)
    message(FATAL_ERROR "${Red}gcc 4.8.0 will complain too much about flex-generated code, we just bypass building ODBC driver in such case${ColorReset}")
  endif()

  ## check `bison`
  find_package(BISON)
  if(NOT BISON_FOUND)
    message(FATAL_ERROR "${Red}you need to install `bison` first${ColorReset}")
  endif()
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_C_COMPILER_VERSION VERSION_LESS 5.0.0)
    message(FATAL_ERROR "${Red}gcc 4.8.0 will complain too much about bison-generated code, we just bypass building ODBC driver in such case${ColorReset}")
  endif()

  if(NOT TODBC_WINDOWS)
    ## check `odbcinst`
    find_program(TAOS_ODBC_ODBCINST_INSTALLED NAMES odbcinst)
    if(NOT TAOS_ODBC_ODBCINST_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under macOS by typing: brew install unixodbc${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}odbcinst is not installed yet, you may install it under Ubuntu by typing: sudo apt install odbcinst${ColorReset}")
      endif()
    endif()

    ## check `isql`
    find_program(TAOS_ODBC_ISQL_INSTALLED NAMES isql)
    if(NOT TAOS_ODBC_ISQL_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under macOS by typing: brew install unixodbc${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under Ubuntu by typing: sudo apt install unixodbc${ColorReset}")
      endif()
    endif()

    ## check `pkg-config`
    find_program(TAOS_ODBC_PKG_CONFIG_INSTALLED NAMES pkg-config)
    if(NOT TAOS_ODBC_PKG_CONFIG_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}pkg-config is not installed yet, you may install it under macOS by typing: brew install pkg-config${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}pkg-config is not installed yet, you may install it under Ubuntu by typing: sudo apt install pkg-config${ColorReset}")
      endif()
    endif()

    ## get `odbc/odbcinst` info via `pkg-config`
    execute_process(COMMAND pkg-config --variable=includedir odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_INCLUDE_DIRECTORY)
    execute_process(COMMAND pkg-config --variable=libdir odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_LIBRARY_DIRECTORY)
    execute_process(COMMAND pkg-config --libs-only-L odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_LINK_OPTIONS)

    execute_process(COMMAND pkg-config --variable=includedir odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_INCLUDE_DIRECTORY)
    execute_process(COMMAND pkg-config --variable=libdir odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_LIBRARY_DIRECTORY)
    execute_process(COMMAND pkg-config --libs-only-L odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_LINK_OPTIONS)
  endif()

  if(NOT TODBC_WINDOWS)
    set(CMAKE_REQUIRED_LIBRARIES odbc odbcinst)
    set(CMAKE_REQUIRED_INCLUDES ${ODBC_INCLUDE_DIRECTORY} ${ODBCINST_INCLUDE_DIRECTORY})
    set(CMAKE_REQUIRED_LINK_OPTIONS ${ODBC_LINK_OPTIONS} ${ODBCINST_LINK_OPTIONS})
  else()
    set(CMAKE_REQUIRED_LIBRARIES odbc32 odbccp32 legacy_stdio_definitions)
  endif()

  ## check `sql.h`
  if(NOT TODBC_WINDOWS)
    check_symbol_exists(SQLExecute "sql.h" HAVE_ODBC_DEV)
  else()
    check_symbol_exists(SQLExecute "windows.h;sql.h" HAVE_ODBC_DEV)
  endif()
  if(NOT HAVE_ODBC_DEV)
    message(FATAL_ERROR "${Red}odbc requirement not satisfied, please install unixodbc-dev. Check detail in ${CMAKE_BINARY_DIR}/CMakeFiles/CMakeError.log${ColorReset}")
  endif()

  ## check `odbcinst.h`
  if(NOT TODBC_WINDOWS)
    check_symbol_exists(SQLConfigDataSource "odbcinst.h" HAVE_ODBCINST_DEV)
  else()
    check_symbol_exists(SQLConfigDataSource "windows.h;odbcinst.h" HAVE_ODBCINST_DEV)
  endif()
  if(NOT HAVE_ODBCINST_DEV)
    message(FATAL_ERROR "${Red}odbc requirement not satisfied, check detail in ${CMAKE_BINARY_DIR}/CMakeFiles/CMakeError.log${ColorReset}")
  endif()

  ## check `git`
  tod_find_prog(GIT 2.0.0 git "--version" "git version (.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_GIT)
    message(STATUS "${Yellow}"
                   "`git 2.0.0 or above` not found, git info is unavailable"
                   "${ColorReset}")
  endif ()

  ## check `valgrind`
  tod_find_prog(VALGRIND 3.18 valgrind "--version" "valgrind-(.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_VALGRIND)
    if (ENABLE_VALGRIND_TEST)
      message(FATAL_ERROR
        "${Red}"
        "`valgrind 3.18 or above` not found, "
        "thus valgrind-related-test-cases would be eliminated, you may refer to https://valgrind.org/"
        "${ColorReset}")
    endif()
  endif ()
  set(ENABLE_VALGRIND ${ENABLE_VALGRIND_TEST})
  message(STATUS "Enable valgrind: ${ENABLE_VALGRIND}")

  ## check `node`
  tod_find_prog(NODEJS 12 node "--version" "v(.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_NODEJS)
    message(STATUS "${Yellow}"
                   "`node v12 or above` not found, "
                   "thus node-related-test-cases would be eliminated, you may refer to https://nodejs.org/"
                   "${ColorReset}")
  endif ()

  ## check `python3`
  tod_find_prog(PYTHON3 3.10 python "--version" "Python (.*)" OUTPUT_VARIABLE)
  if (NOT HAVE_PYTHON3)
    tod_find_prog(PYTHON3 3.10 python3 "--version" "Python (.*)" OUTPUT_VARIABLE)
    if (NOT HAVE_PYTHON3)
      message(STATUS "${Yellow}"
                     "`python 3.10 or above` not found, "
                     "thus python3-related-test-cases would be eliminated, you may refer to https://www.python.org/"
                     "${ColorReset}")
    endif ()
  endif ()

  ## check `go`
  tod_find_prog(GO 1.17 go "version" ".*go([0-9\.]+).*" OUTPUT_VARIABLE)
  if (NOT HAVE_GO)
    message(STATUS "${Yellow}"
                   "`go v1.17 or above` not found, "
                   "thus go-related-test-cases would be eliminated, you may refer to https://go.dev/"
                   "${ColorReset}")
  endif ()

  ## NOTE: check http-proxy sort of issues
  ## check `rustc`
  tod_find_prog(RUSTC 1.63 rustc "--version" "rustc ([0-9\.]+).*" OUTPUT_VARIABLE)
  if (NOT HAVE_RUSTC)
    message(STATUS "${Yellow}"
                   "`rustc v1.63 or above` not found, "
                   "thus rustc-related-test-cases would be eliminated, you may refer to https://rust-lang.org/"
                   "${ColorReset}")
  endif ()

  ## check `cargo`
  tod_find_prog(CARGO 1.63 cargo "--version" "cargo ([0-9\.]+).*" OUTPUT_VARIABLE)
  if (NOT HAVE_CARGO)
    message(STATUS "${Yellow}"
                   "`cargo v1.63 or above` not found, "
                   "thus cargo-related-test-cases would be eliminated, you may refer to https://rust-lang.org/"
                   "${ColorReset}")
  endif ()

  ## check `erlang`
  tod_find_prog(ERLANG 12.2 erl "-version" ".* ([0-9\.]+).*" ERROR_VARIABLE)
  if (NOT HAVE_ERLANG)
    message(STATUS "${Yellow}"
                   "`erlang v12.2 or above` not found, "
                   "thus erlang-related-test-cases would be eliminated, you may refer to https://erlang.org/"
                   "${ColorReset}")
  endif ()

  ## check `haskell`
  tod_find_prog(HASKELL_CABAL 3.6 cabal "--version" ".* version ([0-9\.]+).*" OUTPUT_VARIABLE)
  if (NOT HAVE_HASKELL_CABAL)
    message(STATUS "${Yellow}"
                   "`HASKELL cabal v3.6 or above` not found, "
                   "thus haskell-related-test-cases would be eliminated, you may refer to https://www.haskell.org/ or https://www.haskell.org/ghcup/"
                   "${ColorReset}")
  endif ()

  ## check `common lisp`
  tod_find_prog(COMMON_LISP_SBCL 2.1.11 sbcl "--version" ".* ([0-9\.]+).*" OUTPUT_VARIABLE)
  if (NOT HAVE_COMMON_LISP_SBCL)
    message(STATUS "${Yellow}"
                   "`common lisp SBCL v2.1.11 or above` not found, "
                   "thus common-lisp-related-test-cases would be eliminated, you may refer to https://lisp-lang.org/ or https://lisp-lang.org/learn/getting-started/"
                   "${ColorReset}")
  endif ()

  ## check `R`
  tod_find_prog(R_SCRIPT 4.3.0 Rscript "--version" ".* ([0-9\.]+) .*" OUTPUT_VARIABLE)
  if (NOT HAVE_R_SCRIPT)
    message(STATUS "${Yellow}"
                   "`Rscript v4.3.0 or above` not found, "
                   "thus R-script-related-test-cases would be eliminated, you may refer to https://www.r-project.org/"
                   "${ColorReset}")
  endif ()

  ## check `mysql`
  if (ENABLE_MYSQL_TEST)
    tod_find_prog(MYSQL 8.0.32 mysql "--version" "mysql[ ]+Ver[ ]+([0-9\.]+).*" OUTPUT_VARIABLE)
    if (NOT HAVE_MYSQL)
      message(STATUS "${Yellow}"
                     "`mysql 8.0.32 or above` not found, "
                     "thus mysql-related-test-cases would be eliminated, you may refer to https://www.mysql.com/"
                     "${ColorReset}")
    endif ()
  endif()

  ## check `sqlite3`
  if(ENABLE_SQLITE3_TEST)
    tod_find_prog(SQLITE3 3.37 sqlite3 "--version" "([0-9\.]+).*" OUTPUT_VARIABLE)
    if (NOT HAVE_SQLITE3)
      message(STATUS "${Yellow}"
                     "`sqlite3 3.37 or above` not found, "
                     "thus sqlite3-related-test-cases would be eliminated, you may refer to https://www.sqlite.org/"
                     "${ColorReset}")
    endif ()
  endif ()

endmacro()

macro(parser_gen _name)
    set(_parser          ${_name}_parser)
    set(_scanner         ${_name}_scanner)
    set(_src_path        ${CMAKE_CURRENT_SOURCE_DIR})
    set(_dst_path        ${CMAKE_CURRENT_BINARY_DIR})
    set(_src_name        ${_src_path}/${_name})
    set(_dst_name        ${_dst_path}/${_name})
    set(_y               ${_src_name}.y)
    set(_l               ${_src_name}.l)
    set(_tab_c           ${_src_name}_tab.c)
    set(_dst_tab_c       ${_dst_name}.tab.c)
    set(_dst_lex_c       ${_dst_name}.lex.c)
    set(_dst_lex_h       ${_dst_name}.lex.h)

    BISON_TARGET(${_parser} ${_y} ${_dst_tab_c}
        COMPILE_FLAGS "-Wcounterexamples --warnings=error -Dapi.prefix={${_name}_yy}")
    FLEX_TARGET(${_scanner} ${_l} ${_dst_lex_c}
        COMPILE_FLAGS "--header-file=${_dst_lex_h} --prefix=${_name}_yy")
    ADD_FLEX_BISON_DEPENDENCY(${_scanner} ${_parser})

    set(_bison_outputs ${BISON_${_parser}_OUTPUTS})
    set(_flex_outputs ${FLEX_${_scanner}_OUTPUTS})
    set(_outputs ${_bison_outputs} ${_flex_outputs})

    set_source_files_properties(${_tab_c}
            PROPERTY COMPILE_FLAGS "-I${_dst_path}")
    set_source_files_properties(${_tab_c}
            PROPERTY OBJECT_DEPENDS "${_outputs}")

    unset(_bison_outputs)
    unset(_flex_outputs)
    unset(_outputs)

    unset(_parser)
    unset(_scanner)
    unset(_src_path)
    unset(_dst_path)
    unset(_src_name)
    unset(_dst_name)
    unset(_y)
    unset(_l)
    unset(_tab_c)
    unset(_dst_tab_c)
    unset(_dst_lex_c)
    unset(_dst_lex_h)
endmacro()

