# =========================================================
# Deps options
# =========================================================

option(BUILD_CLOUD "Build cloud edition" OFF)
option(BUILD_CFG_GRANTS "Build with config grants" OFF)

set(BUILD_GRANT_VALUE "" CACHE STRING "Grant value")
set(BUILD_GRANT_DNODES "" CACHE STRING "Grant dnodes limit")
set(BUILD_GRANT_TIMESERIES "" CACHE STRING "Grant timeseries limit")

set(TD_ENTERPRISE TRUE)
set(TD_ACCOUNT TRUE)
set(TD_ADMIN TRUE)
set(TD_VNODE_PLUGINS TRUE)
set(TD_MQTT FALSE)
set(TD_STORAGE TRUE)
set(TD_TOPIC TRUE)
set(TD_MODULE TRUE)
set(TD_MEM_CHECK FALSE)
set(TD_PAGMODE_LITE FALSE)
set(TD_SOMODE_STATIC FALSE)
set(TD_POWER FALSE)
set(TD_GODLL FALSE)
set(TD_PRIVILEGE TRUE)
set(TD_GRANT TRUE)
set(TD_DM_MODULE TRUE)

if(TD_LINUX_64 AND NOT TD_NINGSI)
  set(TD_USB_DONGLE TRUE)
else()
  set(TD_USB_DONGLE FALSE)
endif()

if(TD_WINDOWS OR TD_DARWIN)
  set(TD_SOMODE_STATIC TRUE)
endif()
