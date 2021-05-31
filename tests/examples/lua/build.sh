lua_header_installed=`apt-cache policy liblua5.1-0-dev|grep Installed|grep none > /dev/null; echo $?`
if [ "$lua_header_installed" = "0" ]; then
  sudo apt install -y liblua5.1-0-dev
fi

gcc -std=c99 lua_connector.c -fPIC -shared -o luaconnector.so -Wall -ltaos -I/usr/include/lua5.1

