## =========================== Install taosKeeper ======================================
echo "Install keeper as a standalone service"
exists=$(tar -tf $tarName | grep bin/taoskeeper)
if [ "$exists" = "" ]; then
  exit
else
  ${csudo}tar -C ${bin_link_dir} --strip-components 2 -xzf $tarName ./bin/taoskeeper > /dev/null
  ${csudo}tar -C ${service_config_dir}/ --strip-components 2 -xzf $tarName ./cfg/taoskeeper.service > /dev/null
  if [ -f "${configDir}/keeper.toml" ]; then
    echo "The file keeper.toml will be renamed to taoskeeper.toml"
    ${csudo}tar -C /tmp --strip-components 2 -xzf $tarName ./cfg/taoskeeper.toml > /dev/null
    ${csudo}mv /tmp/taoskeeper.toml ${configDir}/taoskeeper.toml.new
    ${csudo}mv ${configDir}/keeper.toml ${configDir}/taoskeeper.toml
  elif [ -f "${configDir}/taoskeeper.toml" ]; then
    # "taoskeeper.toml exists,new config is taoskeeper.toml.new" 
    ${csudo}tar -C /tmp --strip-components 2 -xzf $tarName ./cfg/taoskeeper.toml > /dev/null
    ${csudo}mv /tmp/taoskeeper.toml ${configDir}/taoskeeper.toml.new
  else
    ${csudo}tar -C ${configDir}/ --strip-components 2 -xzf $tarName ./cfg/taoskeeper.toml
  fi
  command -v systemctl >/dev/null 2>&1 && ${csudo}systemctl daemon-reload >/dev/null 2>&1 || true

  echo "taoskeeper is installed, enable it by \`systemctl enable taoskeeper\`"
  echo ""
fi
