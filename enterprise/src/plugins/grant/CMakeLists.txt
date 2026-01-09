PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/os)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/util)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/common)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/client)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/dnode/mgmt)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/dnode/mnode)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/monitor)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/transport)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/scheduler)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/dnode/mnode/impl/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/qcom)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/catalog)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/planner)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/nodes)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/sync)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/wal)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/tfs)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/libs/stream)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/dnode/vnode/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/include/dnode/mnode/sdb)
INCLUDE_DIRECTORIES(${TD_ENTERPRISE_DIR}/src/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/mnode/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/dnode/inc)
INCLUDE_DIRECTORIES(inc)

LIST(APPEND LIB_SRC ./src/grantMain.c)
LIST(APPEND LIB_SRC ./src/mndGrant.c)

# AUTH_SERVER 编译选项：控制是否编译授权服务器功能
# Windows 平台自动禁用授权服务器功能（由于线程模型和网络接口差异）
IF(TD_WINDOWS)
  MESSAGE(STATUS "AUTH_SERVER disabled on Windows platform")
ELSEIF(DEFINED AUTH_SERVER AND AUTH_SERVER)
  ADD_DEFINITIONS(-DAUTH_SERVER=1)
  LIST(APPEND LIB_SRC ./src/authServer.c)
  LIST(APPEND LIB_SRC ./src/dbRest.c)
  MESSAGE(STATUS "AUTH_SERVER enabled: authServer.c dbRest.c will be compiled")
ELSE()
  MESSAGE(STATUS "AUTH_SERVER disabled: authServer.c dbRest.c will be excluded")
ENDIF()

LIST(APPEND LIB_SRC ./src/authClient.c)

IF(BUILD_WITH_CFG_GRANTS)
  ADD_DEFINITIONS(-DGRANTS_CFG)

  # ADD_LIBRARY(grant STATIC ${LIB_SRC} ./src/wmachine.c)
ENDIF()

IF(${ASSERT_NOT_CORE})
  ADD_DEFINITIONS(-DASSERT_NOT_CORE)
ENDIF(${ASSERT_NOT_CORE})

IF(${TD_INDUSTRY})
  ADD_DEFINITIONS(-DTD_INDUSTRY)

  FOREACH(FEATURE
      STREAM
      SUBSCRIPTION
      AUDIT
      CSV
      VIEW
      MULTI_TIER_STORAGE
      DATA_BAK_RESTORE
      DATA_SYNC
      SHARED_STORAGE
      ACTIVE_ACTIVE
      DUAL_REPLICA_HA
      DB_ENCRYPTION
      TD_GPT)
    IF(TD_FUNC_${FEATURE})
      ADD_DEFINITIONS(-DTD_FUNC_${FEATURE})
    ENDIF()
  ENDFOREACH()

  FOREACH(DATAIN
      OPC_DA
      OPC_UA
      PI
      KAFKA
      INFLUXDB
      MQTT
      AVEVAHISTORIAN
      OPENTSDB
      TDENGINE_2_6
      TDENGINE_3_0
      MYSQL
      POSTGRES
      ORACLE
      MSSQL
      MONGODB
      CSV
      SPARKPLUGB
      ORC
      KINGHISTORIAN
      PULSAR)
    IF(TD_DATAIN_${DATAIN})
      ADD_DEFINITIONS(-DTD_DATAIN_${DATAIN})
    ENDIF()
  ENDFOREACH()

  FOREACH(FEATURE BASIC VERSION_CTRL DATA_FORECAST DATA_DETECT DATA_QUALITY AI_CHAT_GEN)
    IF(IDMP_FUNC_${FEATURE})
      ADD_DEFINITIONS(-DIDMP_FUNC_${FEATURE})
    ENDIF()
  ENDFOREACH()
ENDIF(${TD_INDUSTRY})

IF(TD_ENTERPRISE)
  IF(TD_LINUX_64)
    IF(TD_NINGSI)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_ningsi.o)
    ELSEIF(TD_ARM_64)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_arm64.o)
    ELSEIF(TD_ARM_32)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_arm32.o)
    ELSEIF(TD_ALPINE)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_alpine64.o)
    ELSEIF(TD_MIPS_64)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_mips64.o)
    ELSEIF(TD_LOONGARCH_64)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_loongarch64.o)
    ELSE()
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_linux64.o)
    ENDIF()
  ELSEIF(TD_DARWIN_64)
    IF(TD_ARM_64)
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_darwin_m1.o)
    ELSE()
      ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_darwin.o)
    ENDIF()
  ELSEIF(TD_ARM_64)
    ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_arm64.o)
  ELSEIF(TD_ARM_32)
    ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_arm32.o)
  ELSEIF(TD_WINDOWS_64)
    ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_windows64.o)
  ELSEIF(TD_WINDOWS_32)
    ADD_LIBRARY(grant STATIC ${LIB_SRC} ${TD_ENTERPRISE_DIR}/contrib/grant-lib/machine_windows32.o)
  ELSEIF(TD_DARWIN)
    ADD_LIBRARY(grant STATIC ${LIB_SRC} ./src/wmachine.c)
  ELSE()
    MESSAGE(FATAL_ERROR "grant-lib error: unknown platform")
  ENDIF()
ENDIF()

TARGET_LINK_LIBRARIES(grant util sdb mnode)

IF(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
ENDIF(${BUILD_TEST})
