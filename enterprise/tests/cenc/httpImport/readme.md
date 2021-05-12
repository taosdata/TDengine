## 从 HTTP 流中导出 miniseed 原始数据到 TQueue 中的工具
### 依赖

```bash
libtaos.so
libmseed.so
libcurl.so # 如果 libmseed 支持从网络 URL 读取数据
```

### 编译方法

**注意**
注释掉 Makefile 中的“-DHTTP_IMPORT_DEBUG”会向数据库中写入数据，否则会写到一个文件中，可用作测试用

```bash
make
```

**注意**
如果编译时是以动态库方式链接 libmseed，那么执行这两个程序前，需要设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
