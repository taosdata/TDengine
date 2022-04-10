---
title: 客户端及应用驱动配置
---

TDengine 系统的前台交互客户端应用程序为 taos，以及应用驱动，它与 taosd 共享同一个配置文件 taos.cfg。运行 taos 时，使用参数-c 指定配置文件目录，如 taos -c /home/cfg，表示使用/home/cfg/目录下的 taos.cfg 配置文件中的参数，缺省目录是/etc/taos。更多 taos 的使用方法请见帮助信息 `taos --help`。本节主要说明 taos 客户端应用在配置文件 taos.cfg 文件中使用到的参数。

**2.0.10.0 之后版本支持命令行以下参数显示当前客户端参数的配置**

```bash
taos -C  或  taos --dump-config
```

客户端及应用驱动配置参数列表及解释

- firstEp: taos 启动时，主动连接的集群中第一个 taosd 实例的 end point, 缺省值为 localhost:6030。

- secondEp: taos 启动时，如果 firstEp 连不上，将尝试连接 secondEp。

- locale：系统区位信息及编码格式。

  默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过 API 设置。

  TDengine 为存储中文、日文、韩文等非 ASCII 编码的宽字符，提供一种专门的字段类型 nchar。写入 nchar 字段的数据将统一采用 UCS4-LE 格式进行编码并发送到服务器。需要注意的是，编码正确性是客户端来保证。因此，如果用户想要正常使用 nchar 字段来存储诸如中文、日文、韩文等非 ASCII 字符，需要正确设置客户端的编码格式。

  客户端的输入的字符均采用操作系统当前默认的编码格式，在 Linux 系统上多为 UTF-8，部分中文系统编码则可能是 GB18030 或 GBK 等。在 docker 环境中默认的编码是 POSIX。在中文版 Windows 系统中，编码则是 CP936。客户端需要确保正确设置自己所使用的字符集，即客户端运行的操作系统当前编码字符集，才能保证 nchar 中的数据正确转换为 UCS4-LE 编码格式。

  在 Linux 中 locale 的命名规则为: <语言>\_<地区>.<字符集编码> 如：zh_CN.UTF-8，zh 代表中文，CN 代表大陆地区，UTF-8 表示字符集。字符集编码为客户端正确解析本地字符串提供编码转换的说明。Linux 系统与 Mac OSX 系统可以通过设置 locale 来确定系统的字符编码，由于 Windows 使用的 locale 中不是 POSIX 标准的 locale 格式，因此在 Windows 下需要采用另一个配置参数 charset 来指定字符编码。在 Linux 系统中也可以使用 charset 来指定字符编码。

- charset：字符集编码。

  默认值：系统中动态获取，如果自动获取失败，需要用户在配置文件设置或通过 API 设置。

  如果配置文件中不设置 charset，在 Linux 系统中，taos 在启动时候，自动读取系统当前的 locale 信息，并从 locale 信息中解析提取 charset 编码格式。如果自动读取 locale 信息失败，则尝试读取 charset 配置，如果读取 charset 配置也失败，则中断启动过程。

  在 Linux 系统中，locale 信息包含了字符编码信息，因此正确设置了 Linux 系统 locale 以后可以不用再单独设置 charset。例如：

  ```
  locale zh_CN.UTF-8
  ```

  在 Windows 系统中，无法从 locale 获取系统当前编码。如果无法从配置文件中读取字符串编码信息，taos 默认设置为字符编码为 CP936。其等效在配置文件中添加如下配置：

  ```
  charset CP936
  ```

  如果需要调整字符编码，请查阅当前操作系统使用的编码，并在配置文件中正确设置。

  在 Linux 系统中，如果用户同时设置了 locale 和字符集编码 charset，并且 locale 和 charset 的不一致，后设置的值将覆盖前面设置的值。

  ```
  locale zh_CN.UTF-8
  charset GBK
  ```

  则 charset 的有效值是 GBK。

  ```
  charset GBK
  locale zh_CN.UTF-8
  ```

  charset 的有效值是 UTF-8。

  日志的配置参数，与 server 的配置参数完全一样。

- timezone

  默认值：动态获取当前客户端运行系统所在的时区。

  为应对多时区的数据写入和查询问题，TDengine 采用 Unix 时间戳(Unix Timestamp)来记录和存储时间戳。Unix 时间戳的特点决定了任一时刻不论在任何时区，产生的时间戳均一致。需要注意的是，Unix 时间戳是在客户端完成转换和记录。为了确保客户端其他形式的时间转换为正确的 Unix 时间戳，需要设置正确的时区。

  在 Linux 系统中，客户端会自动读取系统设置的时区信息。用户也可以采用多种方式在配置文件设置时区。例如：

  ```
  timezone UTC-8
  timezone GMT-8
  timezone Asia/Shanghai
  ```

  均是合法的设置东八区时区的格式。但需注意，Windows 下并不支持 `timezone Asia/Shanghai` 这样的写法，而必须写成 `timezone UTC-8`。

  时区的设置对于查询和写入 SQL 语句中非 Unix 时间戳的内容（时间戳字符串、关键词 now 的解析）产生影响。例如：

  ```sql
  SELECT count(*) FROM table_name WHERE TS<'2019-04-11 12:01:08';
  ```

  在东八区，SQL 语句等效于

  ```sql
  SELECT count(*) FROM table_name WHERE TS<1554955268000;
  ```

  在 UTC 时区，SQL 语句等效于

  ```sql
  SELECT count(*) FROM table_name WHERE TS<1554984068000;
  ```

  为了避免使用字符串时间格式带来的不确定性，也可以直接使用 Unix 时间戳。此外，还可以在 SQL 语句中使用带有时区的时间戳字符串，例如：RFC3339 格式的时间戳字符串，2013-04-12T15:52:01.123+08:00 或者 ISO-8601 格式时间戳字符串 2013-04-12T15:52:01.123+0800。上述两个字符串转化为 Unix 时间戳不受系统所在时区的影响。

  启动 taos 时，也可以从命令行指定一个 taosd 实例的 end point，否则就从 taos.cfg 读取。

- maxBinaryDisplayWidth

  Shell 中 binary 和 nchar 字段的显示宽度上限，超过此限制的部分将被隐藏。默认值：30。可在 taos shell 中通过命令 set max_binary_display_width nn 动态修改此选项。
