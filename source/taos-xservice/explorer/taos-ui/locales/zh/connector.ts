export default {
  desc: '通过封装 SQL 为 REST 请求的 {0} 连接器来连接。',
  bottom1: '客户端连接完成。',
  bottom2: '想了解如何写入和查询数据，请参考链接',
  bottomand: '和',
  bottom3: '想了解如何通过 REST API 写入和查询数据，请参考',
  bottom3end: '。',
  java: {
    step1: '增加依赖包',
    step2: '配置',
    step3: '代码示例',
    step3depdesc: '在 “pom.xml” 文件中添加 Spring Boot 和 TDengine Java connector 的依赖：',
    step3confdesc: '在 “application.yml” 文件中添加以下配置：',
    step3mybatisdesc1:
      '定义一个名为 MeterMapper 的接口，它使用 MyBatis 框架在 TDengine 数据库的超级表和 Java 对象之间进行映射：',
    step3mybatisdesc2: '在 “src/main/resources/mapper” 文件夹中创建 “MeterMapper.xml”，文件中添加以下 SQL 映射：',
    step3href: '使用 Spring 进行更多查询和插入 TDengine Cloud 实例的示例代码，请参考',
    step3desc: '下面的代码首先从环境变量获取 JDBC URL，然后创建标准的 JDBC Connection 对象。'
  },
  go: {
    step1: '初始化模块',
    step1desc: '您需要按照下面的代码生成 Go 的样例模块：',
    step2: '增加依赖包',
    step2desc: '在 Go 项目目录中的 go.mod 文件中增加到 driver-go 的依赖：',
    step3: '配置',
    step4: '建立连接',
    step4desc: '复制下面代码到 main.go 文件中：',
    step4desc1: '然后执行下面命令下载依赖包：',
    step4desc2: '最后测试连接：'
  },
  python: {
    step1: '安装连接器',
    'step1-1': `
        <h3>安装前准备</h3>
        您必须先安装 Python3 和 Pip3。
        <ol>
        <li>安装 Python。新版本 taospy 包要求 Python 3.6.2+。早期版本 taospy 包要求 Python 3.7+。taos-ws-py 包要求 Python 3.7+。如果系统上还没有 Python 可参考<a target="_blank" href="https://wiki.python.org/moin/BeginnersGuide/Download">Python Beginners Guide</a>安装。</li>
        <li>安装 Pip3。大部分情况下 Python 的安装包都自带了 pip 工具， 如果没有请参考<a target="_blank" href="https://pypi.org/project/pip/">pip documentation</a>安装。</li>
        </ol>
        `,
    'step1-2': `<h3>用 Pip 安装</h3>如果以前安装过旧版本的 Python 连接器, 请提前卸载。`,
    'step1-2-1': `安装最新或指定版本<code>taospy</code> or <code>taos-ws-py</code>, 在终端里面执行下面的命令。`,
    'step1-3': '安装验证',
    'step1-3-1': '对于 REST 连接，只需验证是否能成功导入<code>taosrest</code> 模块。可在 Python 交互式 Shell 中输入：',
    'step1-3-2':
      '对于 WebSocket 连接，只需验证是否能成功导入 <code>taosws</code> 模块。可在 Python 交互式 Shell 中输入：',

    step2: '配置',
    step3: '建立连接',
    step3desc:
      '请复制下面代码到您的编辑器中然后运行它。如果您正在使用 Jupyter 并假设您已经按照 Jupyter 的指南完成准备，请复制下面代码到您的浏览器的 Jupter 编辑器里面。'
  },
  node: { step1: '安装连接器', step2: '配置', step3: '建立连接' },
  csharp: {
    step1: '创建项目',
    step11desc: '增加 C# TDengine Driver 这个类库。',
    step12desc: '增加下面的 ItemGroup 和 Task 到您的项目文件中。',
    step2: '配置',
    step3: '建立连接',
    step31desc: '整个项目文件如下：',
    step32desc: '整个 C# 文件如下：'
  },
  rust: {
    desc: '通过封装 SQL 为 Websocket 请求的 taos 连接器来连接。',
    step1: '建立项目',
    step2: '增加依赖包',
    step2desc: '在 Cargo.toml 文件在添加依赖：',
    step3: '配置',
    step4: '建立连接',
    step41desc: '复制下面的代码到 main.rs 文件中：',
    step42desc: '然后您可以执行 cargo run 来测试建立的连接。'
  },
  r: {
    step1: '安装RJDBC库',
    step11desc: '首先该库需要依赖Java环境，请先从Oracle官方网站下载适合您操作系统的JDK，并按照安装指南进行安装。',
    step12desc: '然后在 R 控制台中执行以下命令来安装RJDBC库：',
    step13desc: '最后到下载地址去下载最新的 ',
    step13desc1: 'TDengine JDBC 驱动程序',
    step13desc2: '到本地计算机的一个合适位置：',
    step2: '配置',
    step21desc: '然后在 R 脚本中加载 RJDBC 和其他必要的库：',
    step22desc: '最后设置 JDBC 驱动程序和 TDengine JDBC URL：',
    step23desc:
      '注意：请替换“[path]”为实际 TDengine JDBC 驱动程序下载到的系统绝对路径，同时替换“taos-jdbcdriver-X.X.X-dist.jar”为实际下载的驱动程序完整文件名称。',
    step3: '建立连接',
    step31desc: '首先按照下面程序加载 JDBC 驱动程序：',
    step32desc: '然后您可以执行下面程序创建和 TDengine Cloud 实例连接：'
  },
  rest: {
    desc: '这个部分，我们会介绍如何使用 REST API 向 TDengine Cloud 写入数据。',
    step1: '配置',
    step2: '插入',
    step2desc: '请按照下面的命令通过命令行工具 curl 往数据库 test 的表 d1001 中插入数据：',
    step3: '查询',
    step3desc: '请按照下面的命令通过命令行工具 curl 从数据库 information_schema 的表 ins_databases 中查询数据：'
  },
  odbc: {
    desc: 'TDengine ODBC 是为 TDengine 实现的 ODBC 驱动程序，支持 Windows 系统的应用（如 ',
    desc1: ' 等）通过 ODBC 标准接口可以轻松访问 TDengine Cloud 的实例。',
    desc2:
      'TDengine ODBC 提供了两种连接方式，原生连接和 WebSocket 连接。但是您必须使用 WebSocket 连接访问 TDengine Cloud 的实例。',
    step1: '安装',
    step1full: '安装 ODBC 连接器',
    step11desc1: '仅支持 Windows 平台。Windows 上需要安装过 VC 运行时库，可在此下载安装 ',
    step11desc2: 'VC 运行时库',
    step11desc3: ' 。如果已经安装 VS 开发工具可忽略。',
    step12desc1: '下载和安装 ',
    step12desc2: 'TDengine Windows 客户端安装包',
    step12desc3: ' 。',
    step2: '配置',
    step2full: '配置 ODBC 数据源',
    step21desc:
      'Windows 操作系统的【开始】菜单搜索打开【ODBC 数据源(64 位)】管理工具（注意不要选择ODBC 数据源(32 位)）。',
    step22desc: '选中【用户 DSN】标签页，通过【添加(D)】按钮进入“创建数据源”界面。',
    step23desc:
      '选择想要添加的数据源，然后选择【TDengine】，点击完成，进入 TDengine ODBC 数据源配置页面，填写如下必要信息：',
    step23desc1: '【DSN】：',
    step23desc2: '数据源名称，必填，比如“MyTDengine”',
    step23desc3: '【连接类型】：',
    step23desc4: '选中【Websocket】',
    step23desc5: '【URL】：',
    step23desc6: '【数据库】：',
    step23desc7: '可选，填写需要连接的数据库，比如“test”',
    step24desc: '点击【测试连接】按钮测试连接情况，如果成功，会提示“成功连接到\n{0}”。',
    step3: '样例',
    step31desc:
      '您可以通过 Power BI 来使用 TDengine ODBC 驱动直接访问 TDengine Cloud 服务里面的一个实例。更多详情请参考“工具”菜单的“Power BI”页面。'
  }
};
