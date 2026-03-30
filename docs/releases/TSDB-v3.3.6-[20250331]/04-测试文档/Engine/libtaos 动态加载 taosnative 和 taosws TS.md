# libtaos 动态加载 taosnative 和 taosws TS

## 1. 测试目标

JIRA: [TS-5663](https://jira.taosdata.com:18080/browse/TS-5663)
libtaos.so 动态加载 libtaosnative.so 和 libtaosws.so 之后，无兼容性问题，性能无变化

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.12.02 | 1.0 | 关胜亮 | Init doc |
| 2024.1.13 | 1.1 | 关胜亮 | Update taosinternal to taosnative |

## 3. 测试范围

1. 验证 Linux、Mac、Windows 平台下，tao shell、taosdump、taosbenchmark、libtaos.so、安装脚本、make install 脚本、tsim 测试、python 测试正常运转
2. 验证原生链接下，调用 libtaos.so 和 libtaosnative.so 时 taosBenchmark 性能没有显著变化

## 4. 测试结论

通过

## 5. 已知问题和限制

各语言连接器都需要升级，使其在 mative 模式下使用的不是 websocket 连接方式

## 6. 测试环境

安装了 Windows, Linux, macOS 的个人笔记本

## 7. 测试数据

用 taosBenchmark 模拟数据

## 8. 测试用例

### 8.1 增加 taos_connect_dsn、taos_connect_dsn_auth 函数

```sql {wrap}
DLL_EXPORT TAOS *taos_connect_dsn(const char *dsn, const char *user, const char *pass, const char *db);

DLL_EXPORT TAOS *taos_connect_dsn_auth(const char *dsn, const char *user, const char *auth, const char *db);
```

待 libtaosws 库改造完成后测试，在 taos shell 中会被调用，然后再行测试

### 8.2 Linux 平台

#### 8.2.1 TDengine 仓库

##### 8.2.1.1 未设 WEBSOCKET 选项

编译选项：cmake .. -DBUILD_TEST=true

###### 8.2.1.1.1 make install

检查链接库文件：libtaos.so、libtaosnative.so
```sql
root@test:/usr/local/taos/driver# ls -lht
total 58M
-rwxrwxrwx 1 root root  50M Jan 13 12:43 libtaosnative.so.3.3.5.0.alpha
-rwxrwxrwx 1 root root 8.0M Jan 13 12:43 libtaos.so.3.3.5.0.alpha

root@test:/usr/lib# ls -lht libtaos*
lrwxrwxrwx 1 root root 27 Jan 13 12:43 libtaosnative.so -> /usr/lib/libtaosnative.so.1
lrwxrwxrwx 1 root root 53 Jan 13 12:43 libtaosnative.so.1 -> /usr/local/taos/driver/libtaosnative.so.3.3.5.0.alpha
lrwxrwxrwx 1 root root 21 Jan 13 12:43 libtaos.so -> /usr/lib/libtaos.so.1
lrwxrwxrwx 1 root root 47 Jan 13 12:43 libtaos.so.1 -> /usr/local/taos/driver/libtaos.so.3.3.5.0.alpha
```

###### 8.2.1.1.2 libtaos.so

载入顺序：先在 debug/lib 目录寻找，然后系统目录，通过 taos shell 程序验证
1. taos_options 默认 native 选项，载入 libtaosnative.so
   - libtaosnative.so 存在：验证通过
  ```sql
  root@test:~/TDengine/debug/build/bin# taos 
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - libtaosnative.so 不存在
  ```sql
  root@test:~/TDengine/debug/build/bin# taos 
  failed to load libtaosnative.so since No such file or directory [0x80FF0002]
  failed to init shell since No such file or directory [0x80FF0002]
  ```

1. taos_options 设置 native 时，载入 libtaosnative.so
   - libtaosnative.so 存在：验证通过
  ```sql
  root@test:~/TDengine/debug/build/bin# taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - libtaosnative.so 不存在
  ```sql
  root@test:~/TDengine/debug/build/bin# taos -v 0
  failed to load libtaosnative.so since No such file or directory [0x80FF0002]
  failed to init shell since No such file or directory [0x80FF0002]
  ```

1. taos_options 设置 websocket 时，载入 libtaosws.so，通过软连接方式进行验证
   - libtaosws.so 存在：验证通过
  ```sql
  root@test:~/TDengine/debug# ./taos -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - libtaosws.so 不存在
  ```sql
  root@test:~/TDengine/debug# ./taos -v 1
  failed to load libtaosws.so since No such file or directory [0x80FF0002]
  failed to init shell since No such file or directory [0x80FF0002]
  ```

###### 8.2.1.1.3 taos shell 

1. 依赖 libtaos.so，不依赖 libtaosnative.so 和 libtaosws.so
```sql
root@test:~/TDengine/debug# ldd ./taos
        linux-vdso.so.1 (0x00007fffad22e000)
        libtaos.so.1 => /root/TDengine/debug/build/lib/libtaos.so.1 (0x00007f1a57524000)
        libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007f1a572ef000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007f1a57208000)
        libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007f1a571e8000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f1a56fbf000)
        /lib64/ld-linux-x86-64.so.2 (0x00007f1a57c2e000)
```

1. 连接方式
   - 默认使用 native 选项，载入 libtaosnative.so
  ```sql
  root@test:~/TDengine/debug# ./taos
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  0, native 时，载入 libtaosnative.so
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  root@test:~/TDengine/debug# ./taos -v native
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  1, websocket 时，载入 libtaosws.so
  ```sql
  root@test:~/TDengine/debug# ./taos -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  root@test:~/TDengine/debug# ./taos -v websocket
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v 输入错误选项时，报错
  ```sql
  root@test:~/TDengine/debug# ./taos -v xyz
  invalid input xyz for option v
  ```

   - 设置 dsn 时，先使用 dsn
  ```sql
  # 未指定 -v 选项时
  root@test:~/TDengine/debug# ./taos -E xyz
  DSN option not support in native connection mode.
  
  # 指定 -v 0 时，报告 native 不支持 dsn 选项
  root@test:~/TDengine/debug# ./taos -E xyz -v 0
  DSN option not support in native connection mode.
  
  # 指定 -v 1 时，libtaosws.lib 不存在时，报告载入 websocket 失败
  root@test:~/TDengine/debug# ./taos -E xyz -v 1
  failed to load libtaosws.so since No such file or directory [0x80FF0002]
  failed to init shell since No such file or directory [0x80FF0002]
  
  # 指定 -v 1 时，libtaosws.lib 存在时，正常
  root@test:~/TDengine/debug# ./taos -E xyz -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  ```

   - 未设置 dsn 时，依次使用环境变量 TDENGINE_CLOUD_DSN、TDENGINE_DSN 
  ```sql
  # 指定 -v 1 时，libtaosws.lib 存在时
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  xyzabc
  
  root@test:~/TDengine/debug# ./taos -v 1
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=123456
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN                     
  123456
  
  root@test:~/TDengine/debug# ./taos -v1
  Use the environment variable TDENGINE_CLOUD_DSN:123456 as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN  
  
  root@test:~/TDengine/debug# ./taos -v1
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.   
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN  
                  
  ```

   - 未设置环境变量时，使用 host 和 port
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0 -h 192.168.2.233
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Unable to establish connection [0x8000000B]
  
  
  root@test:~/TDengine/debug# ./taos -v 0 -P 6043
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Unable to establish connection [0x8000000B]
  ```

   - dsn、环境变量、host、port 都未设置时，使用 NULL、-1 或者配置文件中的 配置项
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

1. 基本功能
   - native 模式连接数据库
  通过
   - native 模式下，网络测试功能可用（虽然不依赖连接模式）
  ```sql
  taos -n server
  taos -n client
  ```

   - -C 估计不能用
  ```sql
  taos -C
  ```

   - --help 显示正确
  ```sql
  root@test:~/TDengine/debug# ./taos --help
  Usage: taos [OPTION...] 
  
  ……
  -v, --driver=DRIVER        How to access the database, 0|websocket for WebSocket, 1|native for native, default is 1.
   ……
  ```

   - 错误码返回
  ```sql
  root@test:~/TDengine/debug# ./taos -p
  Enter password: 
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Authentication failure [0x80000357]
  ```

###### 8.2.1.1.4 tsim

1. 依赖 libtaosnative.so
```sql
root@test:~/TDengine/debug/build/bin# ldd tsim
        linux-vdso.so.1 (0x00007fff427a5000)
        libtaosnative.so.1 => /root/TDengine/debug/build/lib/libtaosnative.so.1 (0x00007f54870de000)
```

1. 运行测试用例
```sql
root@test:~/TDengine/tests/script# ./test.sh -f tsim/db/basic2.sim 

正常运行
```

###### 8.2.1.1.5 python 

修改了 test.py 脚本，设定 taos_options 的 driver 为 native，可通过用例
```sql
from taos.cinterface import *
taos.taos_options(6, "native")

system-test
./pytest.sh python3 ./test.py -f 2-query/floor.py

army
./pytest.sh python3 ./test.py -f query/function/concat.py

develop-test # 不能运行
python3 ./test.py -f 2-query/pseudo_column.py

```

##### 8.2.1.2 设定 WEBSOCKET 选项

编译选项：cmake .. -DBUILD_TEST=true -DWEBSOCKET=true

###### 8.2.1.2.1 make install

检查链接库文件：libtaos.so、libtaosnative.so
```sql
root@test:/usr/local/taos/driver# ls -lht
total 52M
-rwxrwxrwx 1 root root  11M Dec  5 06:12 libtaosws.so
-rwxrwxrwx 1 root root  41M Dec  5 06:12 libtaosnative.so.3.3.4.8.alpha
-rwxrwxrwx 1 root root 1.3M Dec  5 06:12 libtaos.so.3.3.4.8.alpha

root@test:/usr/local/taos/driver# cd /usr/lib
root@test:/usr/lib# ls -lht libtaos*
lrwxrwxrwx 1 root root 35 Dec  5 06:12 libtaosws.so -> /usr/local/taos/driver/libtaosws.so
lrwxrwxrwx 1 root root 29 Dec  5 06:12 libtaosnative.so -> /usr/lib/libtaosnative.so.1
lrwxrwxrwx 1 root root 55 Dec  5 06:12 libtaosnative.so.1 -> /usr/local/taos/driver/libtaosnative.so.3.3.4.8.alpha
lrwxrwxrwx 1 root root 21 Dec  5 06:12 libtaos.so -> /usr/lib/libtaos.so.1
lrwxrwxrwx 1 root root 47 Dec  5 06:12 libtaos.so.1 -> /usr/local/taos/driver/libtaos.so.3.3.4.8.alpha
```

###### 8.2.1.2.2 libtaos.so

载入顺序：先在 debug/lib 目录寻找，然后系统目录，通过 taos shell 程序验证
1. taos_options 默认 native 选项，载入 libtaosws.so
   - libtaosws.so 存在：验证通过
  ```sql
  root@test:~/TDengine/debug# ./taos
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  ```

   - libtaosws.so 不存在
  ```sql
  root@test:/usr/local/taos/driver# taos
  failed to load libtaosws.so since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

1. taos_options 设置 native 时，载入 libtaosnative.so
   - libtaosnative.so 存在：验证通过
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - libtaosnative.so 不存在
  ```sql
  root@test:/usr/local/taos/driver# taos -v 0
  failed to load libtaosnative.so since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

1. taos_options 设置 websocket 时，载入 libtaosws.so，通过软连接方式进行验证
   - libtaosws.so 存在：验证通过
  ```sql
  root@test:/usr/local/taos/driver# taos -v 1
  failed to load libtaosws.so since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

   - libtaosws.so 不存在
  ```sql
  root@test:~/TDengine/debug# ./taos -v 1
  failed to load libtaosws.so since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

###### 8.2.1.2.3 taos shell 

1. 依赖 libtaos.so，不依赖 libtaosnative.so 和 libtaosws.so
```sql
root@test:~/TDengine/debug# ldd ./taos
        linux-vdso.so.1 (0x00007fffad22e000)
        libtaos.so.1 => /root/TDengine/debug/build/lib/libtaos.so.1 (0x00007f1a57524000)
        libstdc++.so.6 => /lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007f1a572ef000)
        libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007f1a57208000)
        libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007f1a571e8000)
        libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f1a56fbf000)
        /lib64/ld-linux-x86-64.so.2 (0x00007f1a57c2e000)
```

1. 连接方式
   - 默认使用 native 选项，载入 libtaosnative.so
  ```sql
  root@test:~/TDengine/debug# ./taos
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  0, native 时，载入 libtaosnative.so
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  root@test:~/TDengine/debug# ./taos -v native
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  root@test:~/TDengine/debug# ./taos -v native
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  1, websocket 时，载入 libtaosws.so
  ```sql
  root@test:~/TDengine/debug# ./taos -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  root@test:~/TDengine/debug# ./taos -v websocket
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v 输入错误选项时，报错
  ```sql
  root@test:~/TDengine/debug# ./taos -v xyz
  invalid input xyz for option v
  ```

   - 设置 dsn 时，先使用 dsn
  ```sql
  # 未指定 -v 选项时
  root@test:~/TDengine/debug# ./taos -E xyz
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  
  # 指定 -v 0 时，报告 native 不支持 dsn 选项
  root@test:~/TDengine/debug# ./taos -E xyz -v 0
  DSN option not support in native connection mode.
  
  # 指定 -v 1 时，libtaosws.lib 不存在时，报告载入 websocket 失败
  root@test:~/TDengine/debug# ./taos -E xyz -v 1
  failed to load libtaosws.so since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  
  # 指定 -v 1 时，libtaosws.lib 存在时，正常
  root@test:~/TDengine/debug# ./taos -E xyz -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  ```

   - 未设置 dsn 时，依次使用环境变量 TDENGINE_CLOUD_DSN、TDENGINE_DSN 
  ```sql
  # 指定 -v 1 时，libtaosws.lib 存在时
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  xyzabc
  
  root@test:~/TDengine/debug# ./taos
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=123456
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN                     
  123456
  
  root@test:~/TDengine/debug# ./taos
  Use the environment variable TDENGINE_CLOUD_DSN:123456 as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN  
  
  root@test:~/TDengine/debug# ./taos
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.   
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN  
                  
  ```

   - 未设置环境变量时，使用 host 和 port
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0 -h 192.168.2.233
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Unable to establish connection [0x8000000B]
  
  
  root@test:~/TDengine/debug# ./taos -v 0 -P 6043
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Unable to establish connection [0x8000000B]
  ```

   - dsn、环境变量、host、port 都未设置时，使用 NULL、-1 或者配置文件中的 配置项
  ```sql
  root@test:~/TDengine/debug# ./taos -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

1. 基本功能
   - native 模式连接数据库
  通过
   - native 模式下，网络测试功能可用（虽然不依赖连接模式）
  ```sql
  taos -n server
  taos -n client
  ```

   - -C 估计不能用
  ```sql
  taos -C
  ```

   - --help 显示正确
  ```sql
  root@test:~/TDengine/debug# ./taos --help
  Usage: taos [OPTION...] 
  
  ……
  -v, --driver=DRIVER        How to access the database, 0|websocket for WebSocket, 1|native for native, default is 0.
   ……
  ```

   - 错误码返回
  ```sql
  root@test:~/TDengine/debug# ./taos -p
  Enter password: 
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Authentication failure [0x80000357]
  ```

###### 8.2.1.2.4 tsim

1. 依赖 libtaosnative.so
```sql
root@test:~/TDengine/debug/build/bin# ldd tsim
        linux-vdso.so.1 (0x00007fff427a5000)
        libtaosnative.so.1 => /root/TDengine/debug/build/lib/libtaosnative.so.1 (0x00007f54870de000)
```

1. 运行测试用例
```sql
root@test:~/TDengine/tests/script# ./test.sh -f tsim/db/basic2.sim 

正常运行
```

###### 8.2.1.2.5 python 

修改了 test.py 脚本，设定 taos_options 的 driver 为 native，可通过用例
```sql
from taos.cinterface import *
taos.taos_options(6, "native")

system-test
./pytest.sh python3 ./test.py -f 2-query/floor.py

army
./pytest.sh python3 ./test.py -f query/function/concat.py

develop-test # 不能运行
python3 ./test.py -f 2-query/pseudo_column.py

```

#### 8.2.2 Taos-tools 仓库（未测试）

##### 8.2.2.1 未设 WEBSOCKET 选项

###### 8.2.2.1.1 taosdump

###### 8.2.2.1.2 taosBenchmark

##### 8.2.2.2 设定 WEBSOCKET 选项

###### 8.2.2.2.1 taosdump

###### 8.2.2.2.2 taosBenchmark

### 8.3 Windows 测试项

#### 8.3.1 TDengine 仓库

##### 8.3.1.1 未设 WEBSOCKET 选项

cmake .. -G "NMake Makefiles" -DBUILD_TEST=0 -DBUILD_TOOLS=0  -DBUILD_CONTRIB=on -DCMAKE_CXX_STANDARD=17

###### 8.3.1.1.1 make install

检查头文件：taosnative.h 、taos.h
```python
C:\TDengine\include>dir
 驱动器 C 中的卷是 OS
 卷的序列号是 CE24-5741

 C:\TDengine\include 的目录

2024/12/06  19:26    <DIR>          .
2024/12/06  19:31    <DIR>          ..
2024/12/05  13:52            16,617 taos.h
2024/12/05  13:54            73,299 taoserror.h
2024/12/05  13:52             5,015 taosnative.h
2024/12/05  13:52            13,445 taosudf.h
               4 个文件        108,376 字节
               2 个目录  3,904,102,400 可用字节
```

检查链接库文件：taos.dll、taosnative.dll
```python
C:\TDengine\driver>dir
 驱动器 C 中的卷是 OS
 卷的序列号是 CE24-5741

 C:\TDengine\driver 的目录

2024/12/06  19:26    <DIR>          .
2024/12/06  19:31    <DIR>          ..
2024/12/06  19:31         2,182,144 taos.dll
2024/12/06  19:04            32,952 taos.lib
2024/12/06  19:31        34,681,856 taosnative.dll
2024/12/06  19:04            80,910 taosnative.lib
2024/12/06  19:03         2,344,864 taosnative_static.lib
               5 个文件     39,322,726 字节
               2 个目录  3,903,717,376 可用字节
```

###### 8.3.1.1.2 taos.dll

1. taos_options 默认 native 选项，载入 taosnative.dll
   - taosnative.dll 存在：验证通过
  ```python
  D:\TDengine\debug>taos -h 192.168.2.249
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  Server is TDengine Community Edition, ver:3.3.4.8.alpha and will never expire.
  
  taos>
  ```

   - taosnative.dll 不存在
  ```sql
  D:\TDengine\debug>taos -h 192.168.2.249
  failed to load taosnative.dll since The dynamic link library was not loaded [0x8000010B]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

1. taos_options 设置 native 时，载入 taosnative.dll
   - taosnative.dll 存在：验证通过
  ```python
  D:\TDengine\debug>taos -h 192.168.2.249 -v 0
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  Server is TDengine Community Edition, ver:3.3.4.8.alpha and will never expire.
  
  taos>
  ```

   - taosnative.dll 不存在
  ```sql
  D:\TDengine\debug>taos -h 192.168.2.249 -v 0
  failed to load taosnative.dll since The dynamic link library was not loaded [0x8000010B]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

1. taos_options 设置 websocket 时，载入 taosws.dll，通过软连接方式进行验证
   - taosws.dll 存在，设置成功
  ```python
  D:\TDengine\debug>taos -h 192.168.2.249 -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  Server is TDengine Community Edition, ver:3.3.4.8.alpha and will never expire.
  
  taos>
  ```

   - taosws.dll 不存在
  ```sql
  D:\TDengine\debug>taos -h 192.168.2.249 -v 1
  failed to load taosnative.dll since The dynamic link library was not loaded [0x8000010B]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

###### 8.3.1.1.3 taos shell 

1. 依赖 taos.dll，不依赖 taosnative.dll 和 taosws.dll 
2. 连接方式
   - 默认使用 native 选项，载入 taosnative.dll
   - -v  0, native 时，载入 taosnative.dll
   - -v  1, websocket 时，载入 taosws.dll
   - -v 输入错误选项时，报错
   - 设置 dsn 时，先使用 dsn
   - 未设置 dsn 时，依次使用环境变量 TDENGINE_CLOUD_DSN、TDENGINE_DSN 
   - 未设置环境变量时，使用 host 和 port
   - dsn、环境变量、host、port 都未设置时，使用 NULL、-1 或者配置文件中的 配置项
3. 基本功能
   - native 模式连接数据库
   - native 模式下，网络测试功能可用（不依赖连接模式）
   - -C 参数可用
   - --help 显示正确
   - 错误码返回
  ```sql
  D:\TDengine\debug>taos -h 192.168.2.249 -p123
  
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Authentication failure [0x80000357]
  
  To view possible causes and suggested actions for error codes, see
  "Error Code Reference" in the TDengine online documentation.
  ```

##### 8.3.1.2 设定 WEBSOCKET 选项（未测试）

#### 8.3.2 taos-tools 仓库（未测试）

##### 8.3.2.1 未设 WEBSOCKET 选项

###### 8.3.2.1.1 taosdump

###### 8.3.2.1.2 taosBenchmark

##### 8.3.2.2 设定 WEBSOCKET 选项

###### 8.3.2.2.1 taosdump

###### 8.3.2.2.2 taosBenchmark

### 8.4 Mac 测试项

#### 8.4.1 TDengine 仓库

##### 8.4.1.1 未设 WEBSOCKET 选项

编译选项：cmake .. -DBUILD_TEST=true

###### 8.4.1.1.1 make install

检查头文件：taosnative.h 、taos.h
```sql
guanshengliang@localhost include % ls -lh /usr/local/include                               
total 0
lrwxr-xr-x  1 root  wheel    55B 12  1 11:15 taos.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/taos.h
lrwxr-xr-x  1 root  wheel    58B 12  1 11:15 taosdef.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/taosdef.h
lrwxr-xr-x  1 root  wheel    60B 12  1 11:15 taoserror.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/taoserror.h
lrwxr-xr-x  1 root  wheel    63B 12  1 11:15 taosnative.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/taosnative.h
lrwxr-xr-x  1 root  wheel    58B 12  1 11:15 taosudf.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/taosudf.h
lrwxr-xr-x  1 root  wheel    55B 12  1 11:15 tdef.h -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/tdef.h

guanshengliang@localhost include % ls -lh /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/
total 280
-rw-r--r--  1 root  wheel    16K 12  1 12:53 taos.h
-rw-r--r--  1 root  wheel   2.6K 12  1 12:53 taosdef.h
-rw-r--r--  1 root  wheel    70K 12  1 12:53 taoserror.h
-rw-r--r--  1 root  wheel   4.8K 12  1 12:53 taosnative.h
-rw-r--r--  1 root  wheel    13K 12  1 12:53 taosudf.h
-rw-r--r--  1 root  wheel    23K 12  1 12:53 tdef.h
```

检查链接库文件：libtaos.dylib、libtaosnative.dylib
```sql
guanshengliang@localhost include % ls -lh /usr/local/lib
total 0
lrwxr-xr-x  1 root  wheel    75B 12  1 11:16 libtaos.1.dylib -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/driver/libtaos.3.3.4.3.alpha.dylib
lrwxr-xr-x  1 root  wheel    30B 12  1 11:16 libtaos.dylib -> /usr/local/lib/libtaos.1.dylib
lrwxr-xr-x  1 root  wheel    83B 12  1 11:16 libtaosnative.1.dylib -> /usr/local/Cellar/tdengine/3.3.4.3.alpha/driver/libtaosnative.3.3.4.3.alpha.dylib
lrwxr-xr-x  1 root  wheel    38B 12  1 11:16 libtaosnative.dylib -> /usr/local/lib/libtaosnative.1.dylib

guanshengliang@localhost include % ls -lh /usr/local/Cellar/tdengine/3.3.4.3.alpha/driver/
total 101016
-rwxrwxrwx  1 root  wheel   441K 12  1 11:16 libtaos.3.3.4.3.alpha.dylib
-rwxrwxrwx  1 root  wheel    49M 12  1 11:16 libtaosnative.3.3.4.3.alpha.dylib
```

###### 8.4.1.1.2 libtaos.dylib

载入顺序：先在 debug/lib 目录寻找，然后系统目录，通过 taos shell 程序验证
1. taos_options 默认 native 选项，载入 libtaosnative.dylib
   - libtaosnative.dylib 存在：验证通过
   - libtaosnative.dylib 不存在
  ```sql
  failed to load libtaosnative.dylib since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010b]
  ```

1. taos_options 设置 native 时，载入 libtaosnative.dylib
   - libtaosnative.dylib 存在：验证通过
   - libtaosnative.dylib 不存在
  ```sql
  failed to load libtaosnative.dylib since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010b]
  ```

1. taos_options 设置 websocket 时，载入 libtaosws.dylib，通过软连接方式进行验证
   - libtaosws.dylib 存在，设置成功
   - libtaosws.dylib 不存在
  ```sql
  failed to load libtaosws.dylib since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010b
  ```

###### 8.4.1.1.3 taos shell 

1. 依赖 libtaos.dylib，不依赖 libtaosnative.dylib 和 libtaosws.dylib
```sql
guanshengliang@localhost debug % otool -L taos
taos:
        /Users/guanshengliang/TDengine/debug/build/lib/libtaos.1.dylib (compatibility version 1.0.0, current version 3.3.4)

```

1. 连接方式
   - 默认使用 native 选项，载入 libtaosnative.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos          
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.3.alpha 
  ```

   - -v  0, native 时，载入 libtaosnative.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 0        
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.3.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v native
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.3.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v native  
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.3.alpha 
  ```

   - -v  1, websocket 时，载入 libtaosws.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.3.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v WebSocket
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.3.alpha 
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v websocket
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.3.alpha 
  ```

   - -v 输入错误选项时，报错
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v xyz
  invalid input xyz for option v
  ```

   - 设置 dsn 时，先使用 dsn
  ```sql
  # 未指定 -v 选项时
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz
  DSN option not support in native connection mode.
  
  # 指定 -v 0 时，报告 native 不支持 dsn 选项
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz
  DSN option not support in native connection mode.
  
  # 指定 -v 1 时，libtaosws.lib 不存在时，报告载入 websocket 失败
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz -v 1
  failed to load libtaosws.dylib since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010b]
  
  # 指定 -v 1 时，libtaosws.lib 存在时，正常
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz -v 1
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.3.alpha 
  ```

   - 未设置 dsn 时，依次使用环境变量 TDENGINE_CLOUD_DSN、TDENGINE_DSN 
  ```sql
  # 指定 -v 1 时，libtaosws.lib 存在时
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 1
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=123456
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN                     
  
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 1                     
  Use the environment variable TDENGINE_CLOUD_DSN:123456 as the input for the DSN option. 
  ```

   - 未设置环境变量时，使用 host 和 port
  通过
   - dsn、环境变量、host、port 都未设置时，使用 NULL、-1 或者配置文件中的 配置项
  通过
1. 基本功能
   - native 模式连接数据库
  通过
   - native 模式下，网络测试功能可用（虽然不依赖连接模式）
  ```sql
  taos -n server
  taos -n client
  ```

   - -C 估计不能用
  ```sql
  taos -C
  ```

   - --help 显示正确
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos --help
  Usage: taos [OPTION...] 
  
    ……
    -v,  How to access the database, '0 and websocket' for WebSocket, '1 or native' for native, default is '1'.
    ……
  ```

   - 错误码返回
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -p 
  
  Enter password: 
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Authentication failure [0x80000357]
  
  To view possible causes and suggested actions for error codes, see 
  "Error Code Reference" in the TDengine online documentation.
  ```

###### 8.4.1.1.4 tsim

1. 依赖 libtaosnative.dylib
```sql
guanshengliang@guanshengliangdeMacBook-Air bin % otool -L tsim 
tsim:
        /Users/guanshengliang/TDengine/debug/build/lib/libtaosnative.1.dylib (compatibility version 1.0.0, current version 3.3.4) 
```

1. 运行测试用例
```sql
guanshengliang@guanshengliangdeMacBook-Air script % ./test.sh -f tsim/db/basic2.sim 

正常运行
```

###### 8.4.1.1.5 python 

修改了 test.py 脚本，设定 taos_options 的 driver 为 native，可通过用例
```sql
from taos.cinterface import *
taos.taos_options(6, "native")

system-test
./pytest.sh python3 ./test.py -f 2-query/floor.py

army
./pytest.sh python3 ./test.py -f query/function/concat.py

develop-test # 不能运行
python3 ./test.py -f 2-query/pseudo_column.py

```

##### 8.4.1.2 设定 WEBSOCKET 选项

编译选项：cmake .. -DBUILD_TEST=true -DWEBSOCKET=true

###### 8.4.1.2.1 make install

检查头文件：taosnative.h 、taos.h
```sql
guanshengliang@guanshengliangdeMacBook-Air include % ls -lht

total 0
lrwxr-xr-x  1 root  wheel    58B 12  4 16:56 taosudf.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taosudf.h
lrwxr-xr-x  1 root  wheel    55B 12  4 16:56 tdef.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/tdef.h
lrwxr-xr-x  1 root  wheel    60B 12  4 16:56 taoserror.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taoserror.h
lrwxr-xr-x  1 root  wheel    58B 12  4 16:56 taosdef.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taosdef.h
lrwxr-xr-x  1 root  wheel    63B 12  4 16:56 taosnative.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taosnative.h
lrwxr-xr-x  1 root  wheel    55B 12  4 16:56 taos.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taos.h
lrwxr-xr-x  1 root  wheel    57B 12  4 16:56 taosws.h -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/include/taosws.h

guanshengliang@localhost include % ls -lh /usr/local/Cellar/tdengine/3.3.4.3.alpha/include/
guanshengliang@guanshengliangdeMacBook-Air lib % ls -lh /usr/local/Cellar/tdengine/3.3.4.8.alpha/include

total 320
-rw-r--r--  1 root  wheel    16K 12  4 16:56 taos.h
-rw-r--r--  1 root  wheel   2.6K 12  4 16:56 taosdef.h
-rw-r--r--  1 root  wheel    70K 12  4 16:56 taoserror.h
-rw-r--r--  1 root  wheel   4.8K 12  4 16:56 taosnative.h
-rw-r--r--  1 root  wheel    13K 12  4 16:56 taosudf.h
-rw-r--r--  1 root  wheel    17K 12  4 16:56 taosws.h
-rw-r--r--  1 root  wheel    23K 12  4 16:56 tdef.h
```

检查链接库文件：libtaos.dylib、libtaosnative.dylib
```sql
guanshengliang@localhost include % ls -lh /usr/local/lib
total 0
total 0
lrwxr-xr-x  1 root  wheel    63B 12  4 16:56 libtaosws.dylib -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/driver/libtaosws.dylib
lrwxr-xr-x  1 root  wheel    38B 12  4 16:56 libtaosnative.dylib -> /usr/local/lib/libtaosnative.1.dylib
lrwxr-xr-x  1 root  wheel    30B 12  4 16:56 libtaos.dylib -> /usr/local/lib/libtaos.1.dylib
lrwxr-xr-x  1 root  wheel    83B 12  4 16:56 libtaosnative.1.dylib -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/driver/libtaosnative.3.3.4.8.alpha.dylib
lrwxr-xr-x  1 root  wheel    75B 12  4 16:56 libtaos.1.dylib -> /usr/local/Cellar/tdengine/3.3.4.8.alpha/driver/libtaos.3.3.4.8.alpha.dylib

guanshengliang@guanshengliangdeMacBook-Air lib % ls -lh /usr/local/Cellar/tdengine/3.3.4.8.alpha/driver/
 
total 118992
-rwxrwxrwx  1 root  wheel   442K 12  4 16:56 libtaos.3.3.4.8.alpha.dylib
-rwxrwxrwx  1 root  wheel    49M 12  4 16:56 libtaosnative.3.3.4.8.alpha.dylib
-rwxrwxrwx  1 root  wheel   8.5M 12  4 16:56 libtaosws.dylib
```

###### 8.4.1.2.2 libtaos.dylib

载入顺序：先在 debug/lib 目录寻找，然后系统目录，通过 taos shell 程序验证
1. taos_options 默认 native 选项，载入 libtaosws.dylib
   - libtaosws.dylib 存在：验证通过
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - libtaosws.dylib 不存在
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos                                    
  
  failed to load libtaosws.dylib since No such file or directory [0x80ff0002]
  failed to init shell since The dynamic link library was not loaded [0x8000010B]
  ```

1. taos_options 设置 native 时，载入 libtaosnative.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 0
  
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

1. taos_options 设置 websocket 时，载入 libtaosws.dylib，通过软连接方式进行验证
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 1
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

###### 8.4.1.2.3 taos shell 

1. 依赖 libtaos.dylib，不依赖 libtaosnative.dylib 和 libtaosws.dylib
```sql
guanshengliang@localhost debug % otool -L taos
taos:
        /Users/guanshengliang/TDengine/debug/build/lib/libtaos.1.dylib (compatibility version 1.0.0, current version 3.3.4)

```

1. 连接方式
   - 默认使用 native 选项，载入 libtaosnative.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  0, native 时，载入 libtaosnative.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 0
  
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v  1, websocket 时，载入 libtaosws.dylib
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -v 1
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  ```

   - -v 输入错误选项时，报错
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz
  
  invalid input xyz for option v
  ```

   - 设置 dsn 时，先使用 dsn
  ```sql
  # 未指定 -v 选项时
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Operation not supported [0x80000100]
  
  # 指定 -v 0 时，报告 native 不支持 dsn 选项
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz -v 0
  
  DSN option not support in native connection mode.
  
  # 指定 -v 1 时，libtaosws.lib 存在时，正常
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -E xyz -v 1
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Operation not supported [0x80000100]
  ```

   - 未设置 dsn 时，依次使用环境变量 TDENGINE_CLOUD_DSN、TDENGINE_DSN 
  ```sql
  # 指定 -v 1 时，libtaosws.lib 存在时
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  xyzabc
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos
  
  Use the environment variable TDENGINE_DSN:xyzabc as the input for the DSN option.
  
  Welcome to the TDengine Command Line Interface, WebSocket Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Operation not supported [0x80000100]
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_DSN=0
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_DSN                     
  
  guanshengliang@guanshengliangdeMacBook-Air debug % export TDENGINE_CLOUD_DSN=123456
  guanshengliang@guanshengliangdeMacBook-Air debug % echo $TDENGINE_CLOUD_DSN                     
  
  
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos                    
  Use the environment variable TDENGINE_CLOUD_DSN:123456 as the input for the DSN option. 
  ```

   - 未设置环境变量时，使用 host 和 port
  通过
   - dsn、环境变量、host、port 都未设置时，使用 NULL、-1 或者配置文件中的 配置项
  通过
1. 基本功能
   - native 模式连接数据库
  通过
   - native 模式下，网络测试功能可用（虽然不依赖连接模式）
  ```sql
  taos -n server
  taos -n client
  ```

   - -C 估计不能用
  ```sql
  taos -C
  ```

   - --help 显示正确
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos --help
  Usage: taos [OPTION...] 
  
    ……
    -v,  How to access the database, '0 and websocket' for WebSocket, '1 or native' for native, default is '1'.
    ……
  ```

   - 错误码返回
  ```sql
  guanshengliang@guanshengliangdeMacBook-Air debug % ./taos -p 
  
  Enter password: 
  Welcome to the TDengine Command Line Interface, Native Client Version:3.3.4.8.alpha 
  Copyright (c) 2024 by TDengine, all rights reserved.
  
  failed to connect to server, reason: Authentication failure [0x80000357]
  
  To view possible causes and suggested actions for error codes, see 
  "Error Code Reference" in the TDengine online documentation.
  ```

###### 8.4.1.2.4 tsim

1. 依赖 libtaosnative.dylib
```sql
guanshengliang@guanshengliangdeMacBook-Air bin % otool -L tsim 
tsim:
        /Users/guanshengliang/TDengine/debug/build/lib/libtaosnative.1.dylib (compatibility version 1.0.0, current version 3.3.4) 
```

1. 运行测试用例
```sql
guanshengliang@guanshengliangdeMacBook-Air script % ./test.sh -f tsim/db/basic2.sim 

正常运行
```

###### 8.4.1.2.5 python 

修改了 test.py 脚本，设定 taos_options 的 driver 为 native，可通过用例
```sql
from taos.cinterface import *
taos.taos_options(6, "native")

system-test
./pytest.sh python3 ./test.py -f 2-query/floor.py

army
./pytest.sh python3 ./test.py -f query/function/concat.py

develop-test # 不能运行
python3 ./test.py -f 2-query/pseudo_column.py

```

#### 8.4.2 Taos-tools 仓库aos-tools 仓库（未测试）

##### 8.4.2.1 未设 WEBSOCKET 选项

###### 8.4.2.1.1 taosdump

###### 8.4.2.1.2 taosBenchmark

##### 8.4.2.2 设定 WEBSOCKET 选项

###### 8.4.2.2.1 taosdump

###### 8.4.2.2.2 taosBenchmark

##### 8.4.2.3 未设 WEBSOCKET 选项

###### 8.4.2.3.1 taosdump

###### 8.4.2.3.2 taosBenchmark

##### 8.4.2.4 设定 WEBSOCKET 选项

###### 8.4.2.4.1 taosdump

###### 8.4.2.4.2 taosBenchmark

### 8.5 Taosadapter 载入 libtaosnative.so（未测试）

### 8.6 其他语言连接器（未测试）

1. 原生接口需要默认设置 taos_options
2. 原生接口是否还起作用

### 8.7 安装脚本（已修改，但未测试）

#### 8.7.1 Linux

#### 8.7.2 Windows

#### 8.7.3 Mac

## 9. 待讨论

无

## 10. Jira（可选）

无

## 11. 测试计划（可选）

无

## 12. 风险评估

无

## 13. 测试备忘（可选）

无

## 14. 参考文档（可选）

[客户端版本兼容性解决方案](https://taosdata.feishu.cn/wiki/VTEuwbf6DiDIHCkAsxRcH0t7nUg)
