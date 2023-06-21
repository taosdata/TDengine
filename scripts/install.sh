SUDO=$(command -v sudo)
$SUDO cp -f bin/* /usr/local/bin/
[ -d "/usr/local/taosX/" ] || $SUDO mkdir -p /usr/local/taosX/
$SUDO cp -rf plugins /usr/local/taosX/
$SUDO cp etc/systemd/* /etc/systemd/system/
$SUDO systemctl daemon-reload
