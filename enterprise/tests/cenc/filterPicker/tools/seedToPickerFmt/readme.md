## 将原始 miniseed 数据转换为 filterPicker 能处理的数据的工具
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

### 使用方法

```bash
./seedToPickerFmt -i infile -o outfile
```

**注意**
执行这两个程序前，需要设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
