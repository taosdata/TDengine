## =========================== Install taosKeeper ======================================
echo "Install taoskeeper as a standalone service"
exists=$(tar -tf $tarName | grep bin/taoskeeper)
if [ "$exists" = "" ]; then
  exit
else
  ${csudo}tar -x --strip-components 2 -C ${bin_link_dir} --overwrite -f $tarName ./bin/taoskeeper
  ${csudo}tar -x --strip-components 2 -C ${service_config_dir}/ --overwrite -f $tarName ./cfg/taoskeeper.service
  if [ -f "${configDir}/keeper.toml" ]; then
    ${csudo}tar -x --strip-components 2 -C /tmp --overwrite -f $tarName ./cfg/keeper.toml
    ${csudo}mv /tmp/keeper.toml ${configDir}/keeper.toml.new
  else
    ${csudo}tar -x --strip-components 2 -C ${configDir}/ --overwrite -f $tarName ./cfg/keeper.toml
  fi
  command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true

  echo "taoskeeper is installed, enable it by \`systemctl enable taoskeeper\`"
  echo ""
fi
