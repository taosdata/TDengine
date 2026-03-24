# taosX 插件机制

## 1. 背景

河北电力项目在数据 data in 场景中提出了无法在产品中满足的需求，对接收到的数据无法通过数据自身进行处理，要依据一个巨大的外部数据集对接收到的数据进行过滤，然后再对数据进行丢弃或选择不同的处理方式。该数据集还是不定时变化的。
**根本原因**：这是一种糟糕的数据设计，数据自身不具备自释义能力。举例：从我们的身份证号就可以知道是哪里人，通过某一位就知道性别，这就是自释义的数据结构。但此例中分配的设备 ID 没有自释义能力，要通过查询外部数据集才能知道该设备的类型。
**困境**：taosX 产品中不应该出现这样完全用于某个特定项目的代码，taosX 不应该有代码去加载这样的数据集并生成内部数据结构。但为了完成项目，需要解决这个问题。

## 2. 解决方案

将检查和加载该数据集的方法插件化，为此我们需要实现通用化的插件机制，在 taosX 配置项中增加配置如下
```toml {wrap}
external_plugin = /path/to/so # 实现了外部插件的动态连接库的地址
plugin_parameters = "" # 这个字符串按空格分隔后作为参数列表传递给 init() 方法
plugin_export_methods = [(udt_method, plugin_method)] # udt_mthod 是由 UDT 脚本调用的方法，其实质由 plugin_method 实现
plugin_init_method = "" # 初始化方法，启动加载后调用，可配置，避免符号冲突
plugin_cleanup_mthod = "" # 退出时调用的清除方法，可配置，避免符号冲突
```

其中，
- external_plugin 指定一个由任意语言编写（最好是 Rust）的动态库，例如名为 hebeipower.so
- export_methods 为一个数组，其中每个元素为一个 pair，pair 的第一个元素指定在 UDT 程序中调用的方法，第二个元素为由 external_plugin 所提供的方法，比如 UDT 中调用的方法名为 "check_device_id"，plugin 中提供的方法名为 "init_device_set"，taosX 在启动时加载该动态库，对外暴露 "init_device_set"，并将 UDT 方法  ”check_device_id" 注册到 "init_device_set" 上。
- init() 和 cleanup() 方法是必须实现的，由 taosX 在启动和退出时调用，为避免符号冲突，也作为配置项
- plugin_parameters 是传递给 init() 函数的参数列表

## 3. 具体案例

以河北电力为例，配置如下 
```toml {wrap}
external_plugin = /usr/lib/hebeipower.so
plugin_parameters = /usr/local/taosx/device.txt
export_methods = [(check, check_device_id)]
plugin_init_method = init
plugin_cleanup_method = cleanup
```

1. hebeipower.so 实现：
   - init() function：该函数调用 prepare_device_set() 方法，并设置一个 timer 定时调用一个回调函数 check_file_change()
   - check_file_change() 如果发现文件有变化，则再次调用  prepare_device_set() 方法；否则什么也不做
   - prepare_device_set() 方法对 device ID 的文件加载和初始化成 hashMap
   - check_device_id() 判断  device ID 是否存在
   - cleanup() 方法：taosX 退出时调用 
2. taosX 启动时：
   - dlopen(external_plugin)
   - init()
  1. 
  export check_device_id
  engine.register(check, check_device_id)
1. taosX 退出时
   - cleanup()

2. 当在 UDT 中需要检查 device ID 时，只负责调用  check()
