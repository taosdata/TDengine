_kill_service_of() {
  _service=$1
  pid=$(ps -ef | grep "$_service" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid || :
  fi
}

_clean_service_on_systemd_of() {
  _service=$1
  _service_config="${service_config_dir}/${_service}.service"
  if systemctl is-active --quiet ${_service}; then
    echo "taoskeeper is running, stopping it..."
    ${csudo}systemctl stop ${_service} &>/dev/null || echo &>/dev/null
  fi
  ${csudo}systemctl disable ${_service} &>/dev/null || echo &>/dev/null
  ${csudo}rm -f ${_service_config}
}
_clean_service_on_sysvinit_of() {
  _service=$1
  if pidof ${_service} &>/dev/null; then
    echo "${_service} is running, stopping it..."
    ${csudo}service ${_service} stop || :
  fi
  if ((${initd_mod} == 1)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}chkconfig --del ${_service} || :
    fi
  elif ((${initd_mod} == 2)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}insserv -r ${_service} || :
    fi
  elif ((${initd_mod} == 3)); then
    if [ -e ${service_config_dir}/${_service} ]; then
      ${csudo}update-rc.d -f ${_service} remove || :
    fi
  fi

  ${csudo}rm -f ${service_config_dir}/${_service} || :

  if $(which init &>/dev/null); then
    ${csudo}init q || :
  fi
}

_clean_service_of() {
  _service=$1
  if ((${service_mod} == 0)); then
    _clean_service_on_systemd_of $_service
  elif ((${service_mod} == 1)); then
    _clean_service_on_sysvinit_of $_service
  else
    _kill_service_of $_service
  fi
}

remove_taoskeeper() {
  # remove taoskeeper bin
  _clean_service_of taoskeeper
  [ -e "${bin_link_dir}/taoskeeper" ] && ${csudo}rm -rf ${bin_link_dir}/taoskeeper
  [ -e "${cfg_link_dir}/metrics.toml" ] || ${csudo}rm -rf ${cfg_link_dir}/metrics.toml
  echo -e "${GREEN}taosKeeper is removed successfully!${NC}"
}

remove_taoskeeper
