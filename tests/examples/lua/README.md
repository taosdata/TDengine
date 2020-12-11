# TDengine driver connector for Lua

It's a Lua implementation for [TDengine](https://github.com/taosdata/TDengine), an open-sourced big data platform designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring. You may need to install Lua5.3 .

## Lua Dependencies
- Lua: 
```
https://www.lua.org/
```

## Run with Lua Sample

Build driver lib:
```
./build.sh
```
Run lua sample:
```
lua test.lua
```

## OpenResty Dependencies
- OpenResty: 
```
http://openresty.org
```
## Run with OpenResty Sample
**This section demonstrates how to get binary file for connector. To be convenient for trial, an connector has been put into OpenResty work directory.
Because of difference on C API between Lua5.3 and Lua5.1, the files needed by connector for OpenResty are stored in local source directory and configured in script build.sh.** 

Build driver lib:
```
cd lua51
./build.sh
```
Run OpenResty sample:
```
cd ..
cd OpenResty
sudo openresty -p .
curl http://127.0.0.1:7000/api/test
```

