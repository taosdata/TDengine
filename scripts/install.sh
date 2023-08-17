SUDO=$(command -v sudo)
$SUDO cp -f bin/* /usr/local/bin/
[ -d "/usr/local/taosx/" ] || $SUDO mkdir -p /usr/local/taosx/
$SUDO cp -rf plugins /usr/local/taosx/
$SUDO cp etc/systemd/* /etc/systemd/system/
$SUDO systemctl daemon-reload
