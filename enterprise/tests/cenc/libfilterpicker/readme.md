## 为上层应用提供 udf 接口的 filterPicker 动态库

### 动态库名称

```bash
libfilterpicker.so
```

### 提供的头文件

```bash
callback_udf_func.h
```

### 提供的接口

```bash
void callback_udf_func(char *data, char type, int numOfRows, long long *ts, char *dataOutput, char *tsOutput, int *numOfOutput, SUdfInit *buf);

参数说明:
data: 地震采样数据，类型由后边的 type 说明
type: 指明地震采样数据的类型，目前只支持 sizeof(float) 的值
numOfRows: 地震采样数据的条数
ts: 地震采样数据的时间戳
dataOutput: 处理后得到采样数据结果
tsOutput: dataOutput 对应的时间戳
numOfOutput: 得到的采样数据结果条数
buf: 目前暂时没用
```

### 编译方法

```bash
make
```

### 安装

```bash
make install
```

安装目录为 /usr/local/lib

### 卸载
```bash
make uninstall
```

**注意**
执行链接了本库的程序时，需要首先设置 LD_LIBRARY_PATH 环境变量：

```bash
source setenv.sh
```
