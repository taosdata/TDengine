## filterPicker

### 可执行文件 subscribe
可通过参数指定要消费的 TQueue 和要写入的结果表，可通过 ./subscribe -help 查看用法

### 依赖

```bash
libtaos.so
libmseed.so
libcurl.so # 如果 libmseed 支持从网络 URL 读取数据
```

### 编译

```bash
make
```

**注意**
执行这个程序前，需要设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
