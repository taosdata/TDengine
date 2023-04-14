## =========================== Install taosKeeper ======================================
echo "Install keeper as a standalone service"
exists=$(tar -tf $tarName | grep bin/taoskeeper)
if [ "$exists" = "" ]; then
  exit
else
  ${csudo}tar -C ${bin_link_dir} --strip-components 2 -xzf $tarName ./bin/taoskeeper > /dev/null
  ${csudo}tar -C ${service_config_dir}/ --strip-components 2 -xzf $tarName ./cfg/taoskeeper.service > /dev/null
  if [ -f "${configDir}/keeper.toml" ]; then
    ${csudo}tar -C /tmp --strip-components 2 -xzf $tarName ./cfg/keeper.toml > /dev/null
    ${csudo}mv /tmp/keeper.toml ${configDir}/keeper.toml.new
  else
    ${csudo}tar -C ${configDir}/ --strip-components 2 -xzf $tarName ./cfg/keeper.toml
  fi
  command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true

  echo "taoskeeper is installed, enable it by \`systemctl enable taoskeeper\`"
  echo ""
fi
