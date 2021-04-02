## 在原始 miniseed 文件和 TQueue 之间导入导出数据的工具
### 依赖

```bash
libtaos.so
libmseed.so
libcurl.so # 如果 libmseed 支持从网络 URL 读取数据
```

### 编译方法

```bash
make
```

### rawfileToDatabase
将 miniseed 文件中的数据导出到 TQueue 中，不解析数据，但是存储的数据是原始数据经过 base64 编码的数据。

### databaseToRawfile
将 TQueue 中的数据导出到 miniseed 文件中，不解析数据，但是导出过程中会将 base64 编码的数据解码。

**注意**
执行这两个程序前，需要设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
