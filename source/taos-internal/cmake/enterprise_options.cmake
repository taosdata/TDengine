# =========================================================
# Deps options
# =========================================================

set(TD_ENTERPRISE TRUE)
set(TD_ACCOUNT TRUE)
set(TD_ADMIN TRUE)
set(TD_VNODE_PLUGINS TRUE)
set(TD_MQTT FALSE)
set(TD_STORAGE TRUE)
set(TD_TOPIC TRUE)
set(TD_MODULE TRUE)
set(TD_COVER FALSE)
set(TD_MEM_CHECK FALSE)
set(TD_PAGMODE_LITE FALSE)
set(TD_SOMODE_STATIC FALSE)
set(TD_POWER FALSE)
set(TD_GODLL FALSE)
set(TD_PRIVILEGE TRUE)
set(TD_GRANT TRUE)
set(BUILD_DM_MODULE TRUE)

if(TD_LINUX_64 AND NOT TD_NINGSI)
  set(TD_USB_DONGLE TRUE)
else()
  set(TD_USB_DONGLE FALSE)
endif()

if(TD_WINDOWS OR TD_DARWIN)
  set(TD_SOMODE_STATIC TRUE)
endif()

option(
  BUILD_CLOUD
  "If build cloud"
  OFF
)

option(
  BUILD_WITH_CFG_GRANTS
  "If build cfg grants"
  OFF
)
