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

Build driver lib:
```
cd lua51
./build.sh
```
Run OpenResty sample:
```
openresty -p .
curl http://127.0.0.1:7000/api/test
```

