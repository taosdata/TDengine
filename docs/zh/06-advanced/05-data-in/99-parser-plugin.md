---
title: "数据解析插件"
sidebar_label: "数据解析插件"
---

接入消息中间件时，如果使用 json/regex 等模式解析器无法满足解析需求，同时使用 UDT（自定义解析器） 也无法满足性能需求的情况，可以自定义数据解析插件。

## 插件概述

taosX Parser 插件是一个要求用 C/Rust 语言开发的 C ABI 兼容动态库，该动态库要实现约定的 API 并编译为在 taosX 所在运行环境中能够正确运行的动态库，然后复制到约定位置由 taosX 在运行时加载，并在处理数据的 Parsing 阶段调用。

## 插件部署

编译插件，需要在和目标环境兼容的环境中编译。

编译好插件后可将插件的动态库复制到插件目录下，taosX 启动时后，系统首次使用插件时初始化加载插件。可以在 explorer 的 kafka 或者 mqtt 数据接入配置页面中，检查是否加载成功。如下图，如果加载成功，则在解析器选择列表中展示出来。

插件目录在 `taosx.toml` 配置文件中复用 plugins 配置，追加`/parsers`作为插件安装路径，默认值在 UNIX 环境下为 `/usr/local/taos/plugins/parsers`，在 Windows 下为 `C:\TDengine\plugins\parsers`。

![](./plugin-01.png)

## 插件接口规范

| 函数签名     | 描述     | 参数说明     | 返回值 |
| -------- | -------- | -------- | ----------- |
| const char* parser_name() | 插件名，用于前端显示。 | 无 | 字符串 |
| const char* parser_version() | 版本，方便定位问题。 | 无 | 字符串 |
| struct parser_resp_t { <br> char* e; // Error if null. <br>  void* p;  // Success if contains. <br> } <br> parser_resp_t parser_new(char* ctx, uint32_t len); | 使用用户自定义配置生成解析器对象或返回错误信息。| char* ctx: 用户自定义配置字符串。<br> uint32_t len: 该字符串的二进制长度（不含 `\0`）。 | 返回值为结构体。<br> struct parser_resp_t { <br>  char* e;  // Error if null. <br> void* p;  // Success if contains. <br> } <br> 当创建对象失败时，第一个指针 e 不为 NULL。<br> 当创建成功时，e 为 NULL，p 为解析器对象。 |
| const char* parser_mutate( <br> void* parser, <br> const uint8_t* in_ptr, uint32_t in_len, <br> const void* uint8_t* out_ptr, uint32_t* out_len <br> ) | 使用解析器对象对输入 payload 进行解析，返回结果为 JSON 格式 [u8] 。<br> 返回的 JSON 将使用默认的 JSON 解析器进行完全解码（展开根数组和所有的对象）。 | void* parser: parser_new 生成的对象指针。<br> const uint8_t* in_ptr, uint32_t in_len：输入 Payload 的指针和 bytes 长度（不含 `\0`）。 <br>  const void* uint8_t* out_ptr, uint32_t * out_len：输出 JSON 字符串的指针和长度（不含 `\0`）。当 out_ptr 指向为空时，表示输出为空。当 out_ptr 不为空时，应用（taosx）在使用完毕后应当释放该内存空间。| 字符串指针。<br> 当调用成功时，返回值为 NULL。 <br> 当调用失败时，返回错误信息字符串。 |
| void parser_free(void* parser); | 释放解析器对象内存。 | void* parser: parser_new 生成的对象指针。 | 无。 |
