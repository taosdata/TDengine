## Nginx 源代码选择
本模块需要使用 [Nginx](http://nginx.org) 的源码进行编译，推荐使用最新的稳定版本 [nginx-1.18.0](http://nginx.org/download/nginx-1.18.0.tar.gz) 进行编译。

## 软件依赖
### CentOS 系统

```bash
pcre-devel
zlib-devel
openssl
```

### Ubuntu 系统

```bash
libpcre3-dev
zlib1g-dev
openssl
```

无论哪个平台，都要依赖 [libmseed](https://github.com/iris-edu/libmseed)，需要自行编译。如果 libmseed 要支持网络功能，还需要 [libcurl](https://curl.se/libcurl)，可以使用系统已经编译好的二进制库文件： 

### CentOS 系统

```bash
libcurl-devel
```

### Ubuntu 系统

```bash
libcurl4-openssl-dev
```

## 将本模块添加到 Nginx
解压已经下载好的 Nginx 源代码压缩包，进入源代码根目录，执行以下命令（假设本模块的目录与 Nginx 的源代码目录处于同一级目录下）：

```bash
./configure --add-module=../nginx-http-seed-module
```

如果没有错误，就可以执行以下的步骤了。

## 编译和安装 Nginx
**注意**：安装 Nginx 需要 root 权限。

```bash
make && make install
```

如果没有错误，那么 Nginx 就被安装到 /usr/local/nginx 目录下了，可执行文件的路径是 /usr/local/nginx/sbin/nginx。

## 修改 Nginx 的配置文件
本模块只有两个配置项，要启动读取 seed 文件并发送的功能，只需要添加如下的配置即可：

```bash
    location /somedir {
        seed; # 开启读取 seed 文件的功能
    }
```

还有个参数可以控制读取 Nginx 读取文件的时间间隔，如下所示：

```bash
    location /somedir {
        seed;
        interval 2ms; # 控制读取 seed 文件的间隔，目前能设置的最小间隔就是 2ms
    }
```