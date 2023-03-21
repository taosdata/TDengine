lua_header_installed=`apt-cache policy liblua5.3-dev|grep Installed|grep none > /dev/null; echo $?`
if [ "$lua_header_installed" = "0" ]; then
  echo "If need, please input root password to install liblua5.3-dev for build the connector.."
  sudo apt install -y liblua5.3-dev
fi

gcc -g -std=c99 lua_connector.c -fPIC -shared -o luaconnector.so -Wall -ltaos -I/usr/include/lua5.3 -I../../include/client

