## 消费 TQueue 中的地震原始数据并将延迟数据写入结果表
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

**注意**
执行这两个程序前，需要设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
