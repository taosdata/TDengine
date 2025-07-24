PROJECT(TDengine)

# ADD_SUBDIRECTORY(admin)
# ADD_SUBDIRECTORY(account)
ADD_SUBDIRECTORY(grant)

# ADD_SUBDIRECTORY(tsdb)
ADD_SUBDIRECTORY(storage)

# ADD_SUBDIRECTORY(module)
ADD_SUBDIRECTORY(dnode)

ADD_SUBDIRECTORY(web)

if("${CUS_PROMPT}" STREQUAL "")
  MESSAGE("CUS_PROMPT EMPTY set taos")
  SET(TAOSA_CUS_PROMPT "taos")
ELSE()
  MESSAGE("CUS_PROMPT:${CUS_PROMPT}")
  SET(TAOSA_CUS_PROMPT "${CUS_PROMPT}")
ENDIF()

if("${CUS_NAME}" STREQUAL "")
  MESSAGE("CUS_NAME EMPTY set TDengine")
  SET(TAOSA_CUS_NAME "TDengine")
ELSE()
  MESSAGE("CUS_NAME:${CUS_NAME}")
  SET(TAOSA_CUS_NAME "${CUS_NAME}")
ENDIF()

IF(TD_BUILD_TAOSA_INTERNAL)
  MESSAGE("")
  MESSAGE("${Green} use taosainternal as httpd ${ColourReset}")

  SET(taos_version ${TD_VER_NUMBER})
  set(_cgo_cflags_list
          "-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client"
  )
  set(_cgo_ldflags_list
          "-L${CMAKE_BINARY_DIR}/build/lib ${_rpath}"
  )
  set(_ldflags_list
          "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}'"
          "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
          "-X 'github.com/taosdata/taosadapter/v3/version.CUS_NAME=${TAOSA_CUS_NAME}'"
          "-X 'github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${TAOSA_CUS_PROMPT}'"
  )
  string(JOIN " " _cgo_cflags ${_cgo_cflags_list})
  string(JOIN " " _cgo_ldflags ${_cgo_ldflags_list})
  string(JOIN " " _ldflags ${_ldflags_list})


  INCLUDE(ExternalProject)
  if(${TD_LINUX})
      set(ext_taosadapter_static taosadapter.a)
      set(taosadapter_exe        taosadapter)
      set(_rpath                 "-Wl,-rpath=\$ORIGIN/../lib")
      set(_shell_cmd             sh -c "go build -a -o ${taosadapter_exe} -ldflags \"${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=$(cd ${TD_INTERNALS_BASE_DIR}/build/ext_taosadapter/src/ext_taosadapter && git rev-parse HEAD)'\"")
  elseif(${TD_DARWIN})
      set(ext_taosadapter_static taosadapter.a)
      set(taosadapter_exe        taosadapter)
      set(_rpath                 "-Wl,-rpath,\$ORIGIN/../lib")
      set(_shell_cmd             sh -c "go build -a -o ${taosadapter_exe} -ldflags \"${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=$(cd ${TD_INTERNALS_BASE_DIR}/build/ext_taosadapter/src/ext_taosadapter && git rev-parse HEAD)'\"")
  elseif(${TD_WINDOWS})
      set(ext_taosadapter_static taosadapter.lib)
      set(taosadapter_exe        taosadapter.exe)
      set(_rpath                 "")
      set(_shell_cmd             cmd /c for /f %i in ('git -C ${TD_INTERNALS_BASE_DIR}/build/ext_taosadapter/src/ext_taosadapter rev-parse HEAD') do go build -a -o ${taosadapter_exe} -ldflags "${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=%i'")
  endif()


  include(ExternalProject)

  ExternalProject_Add(taosainternal
    PREFIX "taosainternal"
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosainternal
    BUILD_ALWAYS off
    DEPENDS ${TAOS_NATIVE_LIB}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
    PATCH_COMMAND
    COMMAND git clean -f -d
    BUILD_COMMAND
    COMMAND go mod tidy
    COMMAND "${CMAKE_COMMAND}" -E env "CGO_CFLAGS=${_cgo_cflags}"
                                      "CGO_LDFLAGS=${_cgo_ldflags}"
            ${_shell_cmd}

    INSTALL_COMMAND
    COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter"
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${taosadapter_exe} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${taosadapter_exe}
    COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter.toml"
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_INTERNALS_BASE_DIR}/build/ext_taosadapter/src/ext_taosadapter/example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.toml
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_INTERNALS_BASE_DIR}/build/ext_taosadapter/src/ext_taosadapter/taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.service
  )
  ADD_DEPENDENCIES(taosainternal ext_taosadapter)
ENDIF()

IF("${BUILD_TAOSX}" MATCHES "true")
  SET(TD_BULID_TAOSX TRUE)
  MESSAGE("Enable taosx")
  ADD_DEFINITIONS(-DBUILD_TAOSX)
ELSE()
  SET(TD_BULID_TAOSX FALSE)
ENDIF()

IF("${CUS_PROMPT}" STREQUAL "")
  MESSAGE("CUS_PROMPT EMPTY set: ${CUS_PROMPT}")
  SET(TAOSX_NAME "taosx")
ELSE()
  MESSAGE("CUS_PROMPT:${CUS_PROMPT}")
  SET(TAOSX_NAME "${CUS_PROMPT}x")
ENDIF()

SET(OUTPUT_SERVICE_DIR ${CMAKE_BINARY_DIR}/build/share/srv)
FILE(MAKE_DIRECTORY ${OUTPUT_SERVICE_DIR})

IF(TD_BULID_TAOSX)
  include(ExternalProject)
  set(ENV{TAOS_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib)
  set(ENV{LD_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LD_LIBRARY_PATH})
  set(ENV{LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LIBRARY_PATH})

  IF(BUILD_SANITIZER)
    SET(TAOSX_BULID_COMMAND cargo make taosx)
    SET(TAOSX_BINARY target/release/taosx)
  ELSE()
    SET(TAOSX_BULID_COMMAND LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} cargo make taosx)
    SET(TAOSX_BINARY target/release/taosx)
  ENDIF()

  IF(TD_LINUX)
    IF(TD_ALPINE)
      ExternalProject_Add(taosx
        PREFIX taosx
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
        BUILD_ALWAYS off
        DEPENDS taos
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND
        COMMAND git status
        BUILD_COMMAND
        COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} VER_NUMBER=${VER_NUMBER} cargo make taosx
        INSTALL_COMMAND
        COMMAND cmake -E copy target/release/taosx ${CMAKE_BINARY_DIR}/build/bin
        COMMAND cmake -E copy target/${TAOSX_NAME}.service ${OUTPUT_SERVICE_DIR}
      )
      ExternalProject_Add(taosx-agent
        PREFIX taosx
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
        BUILD_ALWAYS off
        DEPENDS taos
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND
        COMMAND git status
        BUILD_COMMAND
        COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} VER_NUMBER=${VER_NUMBER} cargo build --release -p taosx-agent
        INSTALL_COMMAND
        COMMAND cmake -E copy target/release/taosx-agent ${CMAKE_BINARY_DIR}/build/bin
        COMMAND cmake -E copy target/${TAOSX_NAME}-agent.service ${OUTPUT_SERVICE_DIR}
      )
    ELSE()
      ExternalProject_Add(taosx
        PREFIX taosx
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
        BUILD_ALWAYS off
        DEPENDS taos
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND
        COMMAND git status
        BUILD_COMMAND
        COMMAND CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} ${TAOSX_BULID_COMMAND}
        INSTALL_COMMAND
        COMMAND cmake -E copy ${TAOSX_BINARY} ${CMAKE_BINARY_DIR}/build/bin
        COMMAND cmake -E copy target/${TAOSX_NAME}.service ${OUTPUT_SERVICE_DIR}
      )
      ExternalProject_Add(taosx-agent
        PREFIX taosx
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
        BUILD_ALWAYS off
        DEPENDS taos
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND
        COMMAND git status
        BUILD_COMMAND
        COMMAND CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} cargo build -p taosx-agent --release
        INSTALL_COMMAND
        COMMAND cmake -E copy ${TAOSX_BINARY}-agent ${CMAKE_BINARY_DIR}/build/bin
        COMMAND cmake -E copy target/${TAOSX_NAME}-agent.service ${OUTPUT_SERVICE_DIR}
      )
    ENDIF()
  ELSEIF(TD_WINDOWS)
    ExternalProject_Add(taosx
      PREFIX taosx
      SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
      BUILD_ALWAYS off
      DEPENDS taos
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND
      COMMAND git status
      BUILD_COMMAND
      COMMAND set LIBPATH=$ENV{LIBRARY_PATH} && set LIB=$ENV{LIBRARY_PATH} && set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} && set VER_NUMBER=${VER_NUMBER} && cargo build --release
      INSTALL_COMMAND
      COMMAND cmake -E copy target/release/taosx.exe ${CMAKE_BINARY_DIR}/build/bin
    )
    ExternalProject_Add(taosx-agent
      PREFIX taosx
      SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosx
      BUILD_ALWAYS off
      DEPENDS taos
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND
      COMMAND git status
      BUILD_COMMAND
      COMMAND set LIBPATH=$ENV{LIBRARY_PATH} && set LIB=$ENV{LIBRARY_PATH} && set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} && set VER_NUMBER=${VER_NUMBER} && cargo build --release -p taosx-agent
      INSTALL_COMMAND
      COMMAND cmake -E copy target/release/taosx-agent.exe ${CMAKE_BINARY_DIR}/build/bin
    )
  ENDIF()
ENDIF()

IF(TD_BUILD_KEEPER_INTERNAL)
  MESSAGE("")
  MESSAGE("${Green} build taoskeeper, current platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

  EXECUTE_PROCESS(
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
    COMMAND git rev-parse HEAD
    OUTPUT_VARIABLE taoskeeper_commit_sha1
  )

  IF("${taoskeeper_commit_sha1}" STREQUAL "")
    SET(taoskeeper_commit_sha1 "unknown")
  ELSE()
    STRING(STRIP "${taoskeeper_commit_sha1}" taoskeeper_commit_sha1)
  ENDIF()

  SET(taos_version ${TD_VER_NUMBER})
  MESSAGE("${Green} taoskeeper will use ${taos_version} and commit ${taoskeeper_commit_sha1} as version ${ColourReset}")
  MESSAGE(" current source dir is ${CMAKE_CURRENT_SOURCE_DIR}")

  set(_ldflags_list
    "-X 'github.com/taosdata/taoskeeper/version.IsEnterprise=true'"
    "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}'"
    "-X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}'"
    "-X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}'"
    "-X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
    "-X 'github.com/taosdata/taoskeeper/version.CUS_NAME=${TAOSA_CUS_NAME}'"
    "-X 'github.com/taosdata/taoskeeper/version.CUS_PROMPT=${TAOSA_CUS_PROMPT}'"
    "-X 'github.com/taosdata/taoskeeper/version.CUS_PROMPT=${TAOSA_CUS_PROMPT}'"
    "-X 'github.com/taosdata/taoskeeper/version.TD_PRODUCT_NAME=${TD_PRODUCT_NAME}'"
  )
  string(JOIN " " _ldflags ${_ldflags_list})

  IF(TD_WINDOWS)
    MESSAGE("Building taoskeeper on Windows")
    INCLUDE(ExternalProject)
    ExternalProject_Add(taoskeeper
      PREFIX "taoskeeper"
      SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
      BUILD_ALWAYS off
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND cmake -E echo "taoskeeper doesn't need cmake for configuration."
      PATCH_COMMAND
      COMMAND git clean -f -d
      BUILD_COMMAND
      COMMAND go build -a -ldflags "${_ldflags}"
      INSTALL_COMMAND
      COMMAND cmake -E echo "Copy taoskeeper.exe"
      COMMAND cmake -E copy taoskeeper.exe ${CMAKE_BINARY_DIR}/build/bin
      COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
      COMMAND cmake -E echo "Copy taoskeeper.toml"
      COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/config/taoskeeper_enterprise.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
      COMMAND cmake -E echo "Copy taoskeeper.service"
      COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/
    )
  ELSE()
    IF(TD_DARWIN)
      MESSAGE("Building taoskeeper on macOS")
    ELSE()
      MESSAGE("Building taoskeeper on Linux")
    ENDIF()

    INCLUDE(ExternalProject)
    ExternalProject_Add(taoskeeper
      PREFIX "taoskeeper"
      SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
      BUILD_ALWAYS off
      BUILD_IN_SOURCE 1
      CONFIGURE_COMMAND cmake -E echo "taoskeeper no need cmake to config"
      PATCH_COMMAND
      COMMAND git clean -f -d
      BUILD_COMMAND
      COMMAND go build -a -ldflags "${_ldflags}"
      INSTALL_COMMAND
      COMMAND cmake -E echo "Copy taoskeeper"
      COMMAND cmake -E copy taoskeeper ${CMAKE_BINARY_DIR}/build/bin/
      COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
      COMMAND cmake -E echo "Copy taoskeeper.toml"
      COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/config/taoskeeper_enterprise.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
      COMMAND cmake -E echo "Copy taoskeeper.service"
      COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/
    )
  ENDIF()
ENDIF()
