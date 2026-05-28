---
sidebar_label: '任务：可视化管理'
title: 可视化管理
toc_max_heading_level: 4
---

taosExplorer 是一个可视化工具，使用户可以通过浏览器，以直观地方式使用和管理 TDeninge.

1. 打开浏览器，访问 taosExplorer 的地址，默认端口为 `6060`，如果您在本地运行 TDengine, 可以直接访问 [http://localhost:6060](http://localhost:6060).
2. 进入“TDengine 管理系统”页面，输入用户名和密码（默认为：`root/taosdata`），点击“登录”按钮，即可登录。
3. 登录后，您将进入“数据浏览器”页面。在这里，您可以查看数据库、超级表、子表等信息，并执行 SQL 查询。

![taosExplorer 数据浏览器](./01-download-and-install/assets/explorer.png)

除此以外，在“编程”页面，可以查看 TDengine 所支持的各种编程语言（包括：Java, Go, Python, JavaScript/Node.js, C#, Rust, R 等）创建连接的方式，所有的示例代码都可以通过“复制/粘贴”一键执行；在“工具”页面，列举了能够与 TDengine 进行交互的各种工具，包括：Grafana, Seeq, Looker Studio, PowerBI, 永洪 BI, Superset, Excel, Tableau 等，您可以按照页面上提示的步骤，快速地创建出可视化报表和仪表盘。

:::tip

通过点击 taosExplorer 界面的右上角的 ? 图标，无需联网，即可方便地查看 TDengine 的官方文档。

:::
