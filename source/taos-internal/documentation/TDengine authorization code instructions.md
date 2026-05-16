# 授权码获得方式及使用说明

## 生成机器码

TDengine机器码是长度为24的字符串，形如`XrCxPZM8eY92YwQPRjnA7m50`，通过获取系统的硬件信息摘要而成。
硬盘、内存、网卡变化时，机器码不变，但CPU、主板的更改会导致机器码发生变化。购买阿里云、华为云等云服务厂商提供的云服务器时，如果升降器配置，机器码会发生变化。
通过执行如下命令，生成机器码
```
sudo taosd -k
```
执行成功后，输出结果如下
```
machine code: XrCxPZM8eY92YwQPRjnA7m50 
```

## 获取授权码

与TDengine的商务联系，签订销售合同并提供机器码后，可以获取授权码。
授权码与机器码是绑定的，根据授权信息的不同，一个机器码可对应多个授权码，但每个授权码仅对应唯一的机器码。授权码长度为96的字符串，格式如下
```
noCxtq+nz5lJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2dhJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djGIj5StnQ3ZsAv9UOKFOPN
```
授权码包含如下信息

+ 授权类型：正式版（official）、试用版（trial）
+ 过期时间：单位为天
+ 存储空间：单位为GB
+ 写入速度：单位为Point per Second
+ 时间线：单位为点
+ 查询时间：单位为小时
+ 数据库数量
+ 用户数量
+ 连接数量
+ 流计算数量
+ 账户数量
+ 物理节点个数
+ CPU核数
+ 授权码使用方法
+ 在`/etc/taos/taos.cfg`中增加一行`activeCode <授权码>`
+ 重启数据库服务 `systemctl restart taosd`
+ 在shell程序中，执行`show grants`语句查看授权信息