import {$IS_COMMUNITY, GRAFANA_GDS} from '@/utils/init';

export default {
  docs: {
    taosxAgent: {
      1: `taosx-agent 用于在部分数据接入场景，如 Pi、OPC UA、OPC DA 等对访问数据源有一定限制或者网络环境特殊的场景下，可以将 taosx-agent 部署在靠近数据源的环境中甚至与数据源在相同的服务器上，由 taosx-agent 负责从数据源读取数据并发送给 taosX。<br/><br/>请您通过这个地址 <a href="{linuxDL}">Linux</a> 或者 <a href="{windowDL}">Windows</a> 下载 taosx-agent 的下载包到本地环境。对于Linux系统，请将下载的文件解压到指定的文件夹中，然后执行文件夹中的 <code>install.sh</code>文件。对于Windows，请双击下载的文件安装taox-agent，然后在系统环境的路径变量中添加<code>C:\\TDengine </code>。<br/><br/>打开命令行，请执行下面的命令来检查 taosx-agent 是否安装成功。`,
      2: `请您输入代理的唯一名称，系统将为它生成一个连接令牌。`,
      // 3: `重要提示：请在单击“下一步”按钮之前将端点和生成的令牌保存到本地文件。TDengine Cloud 不会在线保存生成的令牌，一旦您单击“下一步”，您将无法检索此令牌，并且必须创建一个新的代理。<br/><br/>
      // 为了确保您的 TDx 代理正常工作，您必须对<code>agent.toml</code>文件进行更改。此文件可在以下目录中找到：<br/>
      3: `重要提示： 点击 "下一步 "按钮前，请将端点和生成的令牌保存到本地文件中。如果丢失，您将无法找回，并必须创建一个新的代理。<br/><br/>
      为确保代理正常运行，请将端点和生成的令牌复制到 <code>agent.toml</code> 文件中。该文件可在以下目录中找到：<br/>
      Linux： <code>/etc/taos</code><br/>
Windows： <code>C:\\TDengine\\cfg\\</code>`,
      4: `请您在命令行中执行以下命令。`,
      5: `请您在命令行中执行以下命令来检查代理运行状态。`,
      6: `<a target='_blank' href='{agenturl}'>代理配置文档</a>`,
      7: '检查代理是否连接正常',
      8: '正常',
      9: '失败',
      10: '正在检查',
      11: `请通过以下方式检查代理日志：`,
      12: '从日志里看是否自己能修复问题。如果无法解决，请向 TDengine 团队报告。'
      //     7: `如果代理令牌错误，服务将直接退出，您可以使用以下命令在Linux上检查日志：

      // <code>journalctl -u taosx-agent</code>`,
      //     8: `在Windows上，您可以在以下位置检查日志文件：

      // <code>C:\\Program Files\\taosX\\log\\agent\\</code>`,
      //     9: '在资源管理器中刷新代理状态，以检查代理是否正确连接。当代理成功连接时，代理的状态将显示为"Idle"。'
    },
    connector: {
      desc: '通过封装 SQL 为 REST 请求的 {0} 连接器来连接。',
      bottom1: '客户端连接完成。',
      bottom2: '想了解如何写入和查询数据，请参考链接',
      bottom2_1: '写入数据',
      bottom2_2: '查询数据',
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
        step3href: '使用 Spring 进行更多查询和插入 TDengine 实例的示例代码，请参考',
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
        step1desc: '首先您需要安装最新的 taospy 模块，Python要求是 Python 3.6+。请在终端中执行下面的命令：',
        step2: '配置',
        step3: '建立连接',
        step3desc:
          '请复制下面代码到您的编辑器中然后运行它。如果您正在使用 Jupyter 并假设您已经按照 Jupyter 的指南完成准备，请复制下面代码到您的浏览器的 Jupter 编辑器里面。',
        step41Title: '第一步：安装',
        step41Desc:
          '对于在 Python 中熟悉 Jupyter 的用户, 需要现在您的环境中准备好 TDengine Python 连接器和 Jupyter 。 如果您还没有这样做，请运行下面的命令：',
        step42Title: '第二步：配置',
        step42Desc:
          '为了让 Jupyter 连接上 TDengine  的实例，您需要先启动 Jupyter，然后设置好环境变量。我们以 Linux 终端作为例子：',
        step43Title: '第二步：建立连接',
        step43Desc:
          '一旦 jupyter lab 启动好后， Jupyter lab 服务就会自动连接上您的浏览器中。然后您可以创建一个新的 notebook ，复制下面代码并运行它。'
      },
      node: {
        step1: '安装连接器',
        step2: '配置',
        step3: '建立连接'
      },
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
      rest: {
        desc: '这个部分，我们会介绍如何使用 REST API 向 TDengine  写入数据。',
        step1: '配置',
        step2: '插入',
        step2desc: '请按照下面的命令通过命令行工具 curl 往数据库 test 的表 d1001 中插入数据：',
        step3: '查询',
        step3desc: '请按照下面的命令通过命令行工具 curl 从数据库 information_schema 的表 ins_databases 中查询数据：'
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
      odbc: {
        desc: 'TDengine ODBC 是为 TDengine 实现的 ODBC 驱动程序，支持 Windows 系统的应用（如 ',
        desc1: ' 等）通过 ODBC 标准接口可以轻松访问 TDengine Cloud 的实例。',
        step1: '安装',
        step1full: '安装 ODBC 连接器',
        step11desc1: '仅支持 Windows 平台。Windows 上需要安装过 VC 运行时库，可在此下载安装 ',
        step11desc2: 'VC 运行时库',
        step11desc3: '。如果已经安装 VS 开发工具可忽略。',
        step12desc1: '下载和安装 ',
        step12desc3: '。',
        step12desc2: 'TDengine Windows 客户端安装包',
        step2: '配置',
        step2full: '配置 ODBC 数据源',
        step21desc:
          'Windows 操作系统的【开始】菜单搜索打开【ODBC 数据源(64 位)】管理工具（注意不要选择 ODBC 数据源(32 位)）。',
        step22desc: '选中【用户 DSN】标签页，通过【添加(D)】按钮进入“创建数据源”界面。',
        step23desc:
          '选择想要添加的数据源，然后选择【TDengine】，点击完成，进入 TDengine ODBC 数据源配置页面，填写如下必要信息：',
        step23desc1: '【DSN】：',
        step23desc2: '数据源名称，必填，比如“MyTDengine”',
        step23desc3: '【连接类型】：',
        step23desc4: '选中【Websocket】',
        step23desc5: '【URL】：',
        step23desc6: '【数据库】：',
        step23desc7: '需要连接的数据库，可选，比如“test”',
        step23desc8: '【服务地址】：',
        step23desc9: '输入 TDengine 的服务地址，例如 192.168.1.100:6041（暂不支持云服务）',
        step23desc10: '【用户名】：',
        step23desc11: '输入用户名，如果不填，默认为 root',
        step23desc12: '【密码】：',
        step23desc13: '输入用户密码，如果不填，默认为 taosdata',
        step24desc: '点击【测试连接】按钮测试连接情况，如果成功，会提示“成功连接到\n{0}”。'
      }
    },
    party: {
      prometheus: {
        title: 'Prometheus',
        desc: '配置 Prometheus 往 TDengine  写入和读取数据。',
        totaldesc1:
          'Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。        ',
        totaldesc2:
          'Prometheus 提供了 `remote_write` 和 `remote_read` 接口来利用其它数据库产品作为它的存储引擎。为了让 Prometheus 生态圈的用户能够利用 TDengine 的高效写入和查询，TDengine 也提供了对这两个接口的支持。',
        step1: '前置条件',
        step1desc:
          '登录到 TDengine +“按钮添加一个名称是”prometheus_data“使用默认参数的数据库。然后执行 `show databases` SQL确认数据库确实被成功创建出来。',
        step2: '安装 Prometheus',
        step2desc: '假设您使用的是 amd64 架构的 Linux 操作系统：',
        step21: '下载',
        step22: '解压和重命名',
        step23: '改变目录为 prometheus',
        step2end: '然后 Prometheus 就会被安装到当前目录. 想了解更多 Prometheus 安装选型，请参考',
        step2doc: '官方文档',
        step3: '配置 Prometheus',
        step3desc:
          '可以通过编辑 Prometheus 配置文件 `prometheus.yml` 来设置 Prometheus （如果您完全按照上面的步骤执行，您可以在当前目录找到 prometheus.xml 文件）。',
        step3desc1: '配置完成后，Prometheus 会从自己的 HTTP 指标端点收集数据并存储到 TDengine  里面。',
        step4: '启动 Prometheus',
        step4desc: '之后 Prometheus 应该已经启动好。同时也启动了一个 Web 服务器',
        step4desc1:
          '。如果您想从浏览器访问这个 Web 服务器， 可以根据您的网络环境修改 `localhost` 为正确的主机名，FQDN 或者 IP 地址。',
        step5: '验证远程写入',
        step5desc: '登录 TDengine  Prometheus 收集的指标数据。',
        step5desc1: 'TDengine 会根据一定规则自动为子表名创建唯一的 IDs。'
      },
      telegraf: {
        title: 'Telegraf',
        desc: '配置 Telegraf 往 TDengine  写入指标。',
        totaldesc1:
          'Telegraf 是一款十分流行的指标采集开源软件。在数据采集和平台监控系统中，Telegraf 可以采集多种组件的运行信息，而不需要自己手写脚本定时采集，降低数据获取的难度。',
        totaldesc2:
          '只需要将 Telegraf 的输出配置增加指向 taosAdapter 对应的 url 并修改若干配置项即可将 Telegraf 的数据写入到 TDengine 中。将 Telegraf 的数据存在到 TDengine 中可以充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。',
        step1: '前置条件',
        step1desc:
          '要将 Telegraf 数据写入 TDengine  ，需要首先手动创建一个数据库。登录到 TDengine  ，在左边的菜单点击”数据浏览器“，然后再点击”数据库“标签旁边的”+“按钮添加一个名称是”telegraf“使用默认参数的数据库。',
        step2: '安装 Telegraf',
        step2desc: '假设您使用的是 Ubuntu 操作系统：',
        step2desc1: '安装结束以后，Telegraf 服务应该已经启动。请先停止它：',
        step2end: '想了解更多其他平台的安装说明，请参考',
        step2doc: '官方文档',
        step3: '配置',
        step3desc: '在您的终端命令行里面执行下面的命令来保存 TDengine  的令牌和URL为环境变量：',
        step3desc1: '然后运行下面的命令来生成 telegraf.conf 文件。',
        step3desc2: '编辑”outputs.http“部分。',
        step3desc3:
          '配置完成后 Telegraf 会开始收集CPU和内容的数据并发送到 TDengine 的数据库”telegraf“。”telegraf“数据库必须先通过 TDengine  创建。',
        step4: '启动 Telegraf',
        step4desc: '使用新生的 telegraf.conf 文件启动 Telegraf。',
        step5: '验证',
        step5desc: '通过下面命令检查 `weather` 数据库 `telegraf` 被创建出来：',
        step5desc1: '检查 `weather` 超级表 cpu 和 mem 被创建出来：',
        step5desc2: 'Telegraf 收集了目前系统正在运行的指标。您还可以启用',
        step5desc2input: '输入插件',
        step5desc2insert: '来插入',
        step5desc2format: '其他格式',
        step5desc2end: '的数据到 Telegraf 中，然后再写入 TDengine。',
        step5desc3:
          'TDengine 接收 influxdb 格式数据默认生成的子表名是根据规则生成的唯一 ID 值。用户如需指定生成的表名，可以通过在 taos.cfg 里配置 smlChildTableName 参数来指定。如果通过控制输入数据格式，即可利用 TDengine 这个功能指定生成的表名。举例如下：配置 smlChildTableName=tname 插入数据为 st,tname=cpu1,t1=4 c1=3 1626006833639000000 则创建的表名为 cpu1。如果多行数据 tname 相同，但是后面的 tag_set 不同，则使用第一行自动建表时指定的 tag_set，其他的行会忽略）。请参考',
        step5desc3end: 'TDengine 无模式写入参考指南'
      },
      influxdb: {
        title: 'InfluxDB 行协议',
        desc: '这一部分主要介绍如何通过REST接口的无模式 {0}往 TDengine  写入数据。',
        step1: '配置',
        step1desc: '在您的终端命令行运行下面的命令来设置 TDengine  的令牌和URL为环境变量：',
        step2: '插入',
        step2desc:
          '您可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 的接口地址 `<cloud_url>/influxdb/v1/write` 往 TDengine 里面写入兼容 InfluxDB 的数据。访问地址如下：',
        step2desc1: '支持 InfluxDB 查询参数如下：',
        step2desc2: '`db` 指定 TDengine 使用的数据库名',
        step2desc3: '`precision` TDengine 使用的时间精度',
        step2desc3ns: '纳秒',
        step2desc3u: '微妙',
        step2desc3ms: '毫秒',
        step2desc3s: '秒',
        step2desc3m: '分',
        step2desc3h: '小时',
        step3: '示例',
        step31: '写入样例',
        step32: '使用 SQL 查询样例',
        step32desc: '`measurement` 是超级表名。',
        step32desc1: '您可以像这样通过标签过滤数据：`where host= "host1"`。'
      },
      opentsdbjson: {
        title: 'OpenTSDB JSON 协议',
        step1: '配置',
        step2: '插入',
        step2desc:
          '您可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 的接口地址 `<cloud_url>/opentsdb/v1/put` 往 TDengine 里面写入兼容 OpenTSDB 的数据。访问地址如下：',
        step3: '示例',
        step31: '写入样例',
        step32: '使用 SQL 查询样例',
        step32desc: '`meter_current` 是超级表名。',
        step32desc1: '您可以像这样通过标签过滤数据：`where groupid=2`。'
      },
      opentsdbtelnet: {
        title: 'OpenTSDB Telnet 协议',
        step1: '配置',
        step2: '插入',
        step3: '示例',
        step31: '写入样例',
        step32: '使用 SQL 查询样例',
        step32desc: '`sys` 是超级表名。',
        step32desc1: '您可以像这样通过标签过滤数据：`where host="web01"`。'
      }
    },
    dataout: {
      dump: {
        desc: '创建可序列化的数据备份。',
        step1: '简介',
        step1desc:
          'taosdump 是一个支持从运行中的 TDengine 集群备份数据并将备份的数据恢复到相同或另一个运行中的 TDengine 集群中的工具应用程序。',
        step1desc1:
          'taosdump 可以用数据库、超级表或普通表作为逻辑数据单元进行备份，也可以对数据库、超级表和普通表中指定时间段内的数据记录进行备份。使用时可以指定数据备份的目录路径，如果不指定位置，taosdump 默认会将数据备份到当前目录。',
        step1desc2: '使用时可以指定数据备份的目录路径，如果不指定位置，taosdump 默认会将数据备份到当前目录。',
        step1desc3:
          '如果指定的位置已经有数据文件，taosdump 会提示用户并立即退出，避免数据被覆盖。这意味着同一路径只能被用于一次备份。如果看到相关提示，请小心操作。',
        step1desc4:
          'taosdump 是一个逻辑备份工具，它不应被用于备份任何原始数据、环境设置、硬件信息、服务端配置或集群的拓扑结构。taosdump 使用',
        step1desc5: '作为数据文件格式来存储备份数据。',
        step2: '安装',
        step2desc: '使用 taosdump，您需要下载并安装(',
        step2desc1: '。注意，在安装 taosTools 之前，请首先下载和安装 ',
        step2desc2: '解压下载的包并安装。',
        step2desc3: '设置环境变量',
        step3: '常用使用场景',
        step31: 'taosdump 备份数据',
        step31desc: '备份所有数据库：指定 `-A` 或 `--all-databases` 参数；',
        step31desc1: '备份多个指定数据库：使用 `-D db1,db2,...` 参数；',
        step31desc2:
          '备份指定数据库中的某些超级表或普通表：使用 `dbname stbname1 stbname2 tbname1 tbname2 ...` 参数，注意这种输入序列第一个参数为数据库名称，且只支持一个数据库，第二个和之后的参数为该数据库中的超级表或普通表名称，中间以空格分隔；',
        step31desc3:
          '备份系统 log 库：TDengine 集群通常会包含一个系统数据库，名为 `log`，这个数据库内的数据为 TDengine 自我运行的数据，taosdump 默认不会对 log 库进行备份。如果有特定需求对 log 库进行备份，可以使用 `-a` 或 `--allow-sys` 命令行参数。',
        step31desc4:
          '“宽容”模式备份：taosdump 1.4.1 之后的版本提供 `-n` 参数和 `-L` 参数，用于备份数据时不使用转义字符和“宽容”模式，可以在表名、列名、标签名没使用转义字符的情况下减少备份数据时间和备份数据占用空间。如果不确定符合使用 `-n` 和 `-L` 条件时请使用默认参数进行“严格”模式进行备份。转义字符的说明请参考',
        step31desc5: '官方文档',
        step31desc6: '。',
        step32: 'taosdump 恢复数据',
        step32desc:
          '恢复指定路径下的数据文件：使用 `-i` 参数加上数据文件所在路径。如前面提及，不应该使用同一个目录备份不同数据集合，也不应该在同一路径多次备份同一数据集，否则备份数据会造成覆盖或多次备份。',
        step4: '详细命令行参数列表',
        step4desc: '以下为 taosdump 详细命令行参数列表：'
      }
    },
    virtual: {
      grafana: {
        desc: `${GRAFANA_GDS} 能够与开源数据可视化系统  Grafana 快速集成搭建数据监测报警系统，整个过程无需任何代码开发，${GRAFANA_GDS} 中数据表的内容可以在仪表盘(DashBoard)上进行可视化展现。关于 ${GRAFANA_GDS} 插件的使用您可以在 GitHub 中了解更多。`,
        topdesc: `${GRAFANA_GDS} 能够与开源数据可视化系统 `,
        topdesc1: ` 快速集成搭建数据监测报警系统，整个过程无需任何代码开发。${GRAFANA_GDS} 中数据表的内容可以在仪表盘(DashBoard)上进行可视化展现。关于 ${GRAFANA_GDS} 插件的使用您可以在 `,
        topdesc2: ' 中了解更多。',
        step1: '安装 Grafana',
        step1desc: `目前 ${GRAFANA_GDS} 支持 Grafana 7.5 以上的版本。请您到 Grafana 官网下载安装包`,
        step2: `安装 ${GRAFANA_GDS} 插件`,
        step2desc: '使用 grafana-cli 命令行工具 进行插件安装。',
        script1: `如果本地访问 Github 比较方便，可以从 Linux 终端运行下面的脚本来安装 ${GRAFANA_GDS} 数据源插件。`,
        script2: `安装结束以后，请重启 <code>grafana-server</code>。`,
        step3: '添加数据源',
        step3desc: `在打开的 Grafana 数据源配置页面中，复制下面列出的主机和令牌值，然后粘贴到 Grafana 的相应输入框中。`,
        step3desc1: 'Host:',
        step3desc2: 'Token:',
        step3desc3: `输入密码登陆 ${GRAFANA_GDS}，然后点击 <code>Save & Test</code> 按钮来验证 ${GRAFANA_GDS} 是否能够工作。`,
        step4: '使用 Grafana',
        step4desc: `请创建一个新的仪表盘，或者导入存在的仪表盘来展示 ${GRAFANA_GDS} 里面的数据。`,
        step4desc1: '同时更多细节请参考',
        step4desc2: '文档',
        step4desc3: '。'
      },
      gds: {
        desc: `Looker Studio可以快速访问 ${GRAFANA_GDS} 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘.`,
        topdesc: '使用',
        topconnector: '第三方连接器',
        topdesc1: `，Looker Studio可以快速访问 ${GRAFANA_GDS} 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘。整个过程不需要任何的代码编写过程。可以分享报表和仪表盘给不同的个人，团队以及全世界，还可以跟其他人员实时协作，另外在任何的网页里面嵌入您的报表。`,
        topdesc2: `更多使用 Looker Studio 和 ${GRAFANA_GDS} 集成可以参考`,
        topdesc3: '。',
        step1: '选择数据源',
        step1desc: '目前的',
        step1desc1: ' 连接器',
        step1desc2: ` 支持两种不同的数据源：${GRAFANA_GDS} Server 和 ${GRAFANA_GDS} 。首先选择”${GRAFANA_GDS} “类型然后点击”下一步“。`,
        step2: '连接器配置',
        step21: '必须的配置',
        step21desc: `${GRAFANA_GDS}  URL：`,
        step211: `${GRAFANA_GDS} 令牌：`,
        step212: '数据库',
        step212desc: '数据库的名称，该数据库包含您想查询数据和创建报表的的表，可以是一般表，超级表或者子表。',
        step213: '表',
        step213desc: '您希望查询数据和执行报表的表的名称',
        step213desc1: '注意',
        step213desc2: ' 可以获取的最大记录行数是1000000。',
        step22: '可选配置',
        step221: '查询从开始日期到结束日期的数据',
        step221desc:
          '在页面上面配置您的连接器的两个时间输入框，这两个时间过滤条件是用来过滤大量数据的。时间输入框的格式是”YYYY-MM-DD HH:MM:SS“。比如：',
        step221desc1:
          '查询结果的开始时间戳是由 `start date` 定义的。加上这个条件，您不会获取到在 `start date` 时间戳之前的数据。',
        step221desc2:
          '`end time`输入框表明查询结束的时间戳。因此，在结束时间戳之后的数据也获取不到。这些条件是利用 SQL 的 where 语句来实现的。比如：',
        step221desc3: '事实上，您可通过一些过滤器来加快报表加载数据的速度。',
        step221desc4: `在配置完成以后，点击"CONNECT"按钮，您就会连接上您的具有给定数据库和表的”${GRAFANA_GDS}  “。`,
        step3: '创建报表和仪表盘',
        step3desc: `使用交互式仪表盘和优美报表解锁您的 ${GRAFANA_GDS} 数据能力，`,
        step3desc1: '更多详情请参考',
        step3desc2: '文档',
        step3desc3: '。'
      }
    },
    tool: {
      cli: {
        desc: 'TDengine 的交互式命令行工具',
        topdesc: 'TDengine 命令行程序（以下简称 TDengine CLI）是用户操作 TDengine 实例并与之交互的最简洁最常用的方式。',
        step1: '安装',
        step1desc: '运行 TDengine CLI 来访问 TDengine  ，请首先下载和安装最新的 ',
        step1desc1: 'TDengine 客户端安装包',
        step1desc2: '（',
        step1desc3: '，',
        step1desc4: '）。',
        step2: '配置',
        step2desc: '在您的 Linux 终端里面执行下面的命令设置 TDengine 的 DSN 为环境变量：',
        step2desc1: '在您的 Windows CMD 里面执行下面的命令设置 TDengine  的 DSN 为环境变量：',
        step2desc2: '或者在您的 Windows PowerShell 里面执行下面的命令设置 TDengine  的 DSN 为环境变量：',
        step2desc3: '在您的 Mac 里面执行下面的命令设置 TDengine  的 DSN 为环境变量：',
        step3: '建立连接',
        step3desc: '如果您已经设置了环境变量，您只需要立即执行 `taos` 命令就可以访问 TDengine  实例。',
        step3desc1: '如果已经设置了环境变量，要访问TDengine，可以执行下面的命令：',
        step4: '使用 TDengine CLI',
        step4desc:
          '如果成功连接上 TDengine 服务，TDengine CLI 会显示一个欢迎的消息和版本信息。如果失败了，TDengine CLI 会打印失败消息。TDengine CLI 打印的成功消息如下：',
        step4desc1: '进入 TDengine CLI 以后，您就可以执行大量的 SQL 命令来进行插入，查询或者进行管理。',
        step4desc2: '官方文档',
        step4desc3: '。'
      },
      benchmark: {
        desc: 'taosBenchmark 是一个用于测试 TDengine 产品性能的工具',
        step1: '简介',
        step1desc:
          'taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具。taosBenchmark 可以测试 TDengine 的插入、查询和订阅等功能的性能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表的数量、每张子表的数据量、插入数据的时间间隔、taosBenchmark 的工作线程数量、是否以及如何插入乱序数据等。为了兼容过往用户的使用习惯，安装包提供 了 taosdemo 作为 taosBenchmark 的软链接。',
        step1desc1:
          '在使用 TDengine  的时候，请注意，没有授权的用户是没有办法通过任何工具包括 taosBenchmark 来创建数据库的。只能通过 TDengine  的数据浏览器来创建数据库。这个文档中提到的任何创建数据库的内容请忽略，并在 TDengine  里面手动创建数据库。',
        step2: '安装',
        step2desc: '使用 taosBenchmark，您可以下载和安装(',
        step2desc1: ' 或者',
        step2desc2: 'TDengine client 安装包',
        step2desc3: '解压下载包并安装。',
        step3: '运行',
        step31: '配置和运行方式',
        step31desc: '运行下面命令来设置 TDengine  的 DSN 环境变量：',
        step31desc1: '用户只能使用一个命令行参数 `-f <json file>` 指定配置文件。',
        step31desc2:
          'taosBenchmark 支持对 TDengine 做完备的性能测试，其所支持的 TDengine 功能分为三大类：写入、查询和订阅。这三种功能之间是互斥的，每次运行 taosBenchmark 只能选择其中之一。值得注意的是，所要测试的功能类型在使用命令行配置方式时是不可配置的，命令行配置方式只能测试写入性能。若要测试 TDengine 的查询和订阅性能，必须使用配置文件的方式，通过配置文件中的参数 `filetype` 指定所要测试的功能类型。',
        step31desc3: '在运行 taosBenchmark 之前要确保 TDengine 集群已经在正确运行。',
        step32: '使用配置文件运行',
        step32desc: 'taosBenchmark 安装包中提供了配置文件的示例，位于 `<install_directory>/examples` 下',
        step32desc1: '使用如下命令行即可运行 taosBenchmark 并通过配置文件控制其行为。',
        step33: '示例配置文件',
        step34: '插入场景 JSON 配置文件示例',
        step35: '查询场景 JSON 配置文件示例',
        step4: '配置文件参数',
        step41: '通用配置参数',
        step41desc: '本节所列参数适用于所有功能模式。',
        step41desc1:
          '：要测试的功能，可选值为 `insert`, `query` 和 `subscribe`。分别对应插入、查询和订阅功能。每个配置文件中只能指定其中之一。',
        step41desc2: '：TDengine 客户端配置文件所在的目录，默认路径是 /etc/taos 。',
        step41desc3: '：指定要连接的 TDengine 服务端的 FQDN，默认值为 localhost。',
        step41desc4: '：要连接的 TDengine 服务器的端口号，默认值为 6030。',
        step41desc5: '：用于连接 TDengine 服务端的用户名，默认为 root。',
        step41desc6: '：用于连接 TDengine 服务端的密码，默认值为 taosdata。',
        step42: '插入场景配置参数',
        step42desc: '插入场景下 `filetype` 必须设置为 `insert`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)',
        step43: '流式计算相关配置参数',
        step43desc: '创建流式计算的相关参数在 json 配置文件中的 `stream` 中配置，具体参数如下。',
        step43desc1: '：流式计算的名称，必填项。',
        step43desc2: '：流式计算对应的超级表名称，必填项。',
        step43desc3: '：流式计算的sql语句，必填项。',
        step43desc4: '：流式计算的触发模式，可选项。',
        step43desc5: '：流式计算的水印，可选项。',
        step43desc6: '：是否创建流式计算，可选项为 "yes" 或者 "no", 为 "no" 时不创建。',
        step44: '超级表相关配置参数',
        step44desc: '创建超级表时的相关参数在 json 配置文件中的 `super_tables` 中配置，具体参数如下。',
        step44desc1: '：超级表名，必须配置，没有默认值。',
        step44desc2: '：子表是否已经存在，默认值为 "no"，可选值为 "yes" 或 "no"。',
        step44desc3: '：子表的数量，默认值为 10。',
        step44desc4: '：子表名称的前缀，必选配置项，没有默认值。',
        step44desc5: '：超级表和子表名称中是否包含转义字符，默认值为 "no"，可选值为 "yes" 或 "no"。',
        step44desc6:
          '：仅当 insert_mode 为 taosc, rest, stmt 并且 childtable_exists 为 "no" 时生效，该参数为 "yes" 表示 taosBenchmark 在插入数据时会自动创建不存在的表；为 "no" 则表示先提前建好所有表再进行插入。',
        step44desc7:
          '：创建子表时每批次的建表数量，默认为 10。注：实际的批数不一定与该值相同，当执行的 SQL 语句大于支持的最大长度时，会自动截断再执行，继续创建。',
        step44desc8:
          '：数据的来源，默认为 taosBenchmark 随机产生，可以配置为 "rand" 和 "sample"。为 "sample" 时使用 sample_file 参数指定的文件内的数据。',
        step44desc9:
          '：插入模式，可选项有 taosc, rest, stmt, sml, sml-rest, 分别对应普通写入、restful 接口写入、参数绑定接口写入、schemaless 接口写入、restful schemaless 接口写入 (由 taosAdapter 提供)。默认值为 taosc 。',
        step44desc10:
          '：指定是否持续写入，若为 "yes" 则 insert_rows 失效，直到 Ctrl + C 停止程序，写入才会停止。默认值为 "no"，即写入指定数量的记录后停止。注：即使在持续写入模式下 insert_rows 失效，但其也必须被配置为一个非零正整数。',
        step44desc11:
          '：使用行协议插入数据，仅当 insert_mode 为 sml 或 sml-rest 时生效，可选项为 `line`, `telnet`, `json`。',
        step44desc12:
          '：telnet 模式下的通信协议，仅当 insert_mode 为 sml-rest 并且 line_protocol 为 telnet 时生效。如果不配置，则默认为 http 协议。',
        step44desc13: '：每个子表插入的记录数，默认为 0 。',
        step44desc14:
          '：仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表时的偏移量，即从第几个子表开始。',
        step44desc15: '：仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表的上限。',
        step44desc16:
          '：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。',
        step44desc17:
          '：指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。',
        step44desc18:
          '：若该值为正数 n 时， 则仅向前 n 列写入，仅当 insert_mode 为 taosc 和 rest 时生效，如果 n 为 0 则是向全部列写入。',
        step44desc19: '：指定乱序数据的百分比概率，其值域为 [0,50]。默认为 0，即没有乱序数据。',
        step44desc20:
          '：指定乱序数据的时间戳回退范围。所生成的乱序时间戳为非乱序情况下应该使用的时间戳减去这个范围内的一个随机值。仅在 `-O/--disorder` 指定的乱序数据百分比大于 0 时有效。',
        step44desc21: '：每个子表中插入数据的时间戳步长，单位与数据库的 `precision` 一致，默认值是 1。',
        step44desc22: '：每个子表的时间戳起始值，默认值是 now。',
        step44desc23: '：样本数据文件的类型，现在只支持 "csv" 。',
        step44desc24:
          '：指定 csv 格式的文件作为数据源，仅当 data_source 为 sample 时生效。若 csv 文件内的数据行数小于等于 prepared_rand，那么会循环读取 csv 文件数据直到与 prepared_rand 相同；否则则会只读取 prepared_rand 个数的行的数据。也即最终生成的数据行数为二者取小。',
        step44desc25:
          '：仅当 data_source 为 sample 时生效，表示 sample_file 指定的 csv 文件内是否包含第一列时间戳，默认为 no。 若设置为 yes， 则使用 csv 文件第一列作为时间戳，由于同一子表时间戳不能重复，生成的数据量取决于 csv 文件内的数据行数相同，此时 insert_rows 失效。',
        step44desc26:
          '：仅当 insert_mode 为 taosc, rest 的模式下生效。 最终的 tag 的数值与 childtable_count 有关，如果 csv 文件内的 tag 数据行小于给定的子表数量，那么会循环读取 csv 文件数据直到生成 childtable_count 指定的子表数量；否则则只会读取 childtable_count 行 tag 数据。也即最终生成的子表数量为二者取小。',
        step45: 'TSMA 配置参数',
        step45desc: '指定 TSMA 的配置参数在 `super_tables` 中的 `tsmas` 中，具体参数如下。',
        step45desc1: '：指定 tsma 的名字，必选项。',
        step45desc2: '：指定 tsma 的函数，必选项。',
        step45desc3: '：指定 tsma 的时间间隔，必选项。',
        step45desc4: '：指定 tsma 的窗口时间位移，必选项。',
        step45desc5: '：指定 tsma 的创建语句结尾追加的自定义配置，可选项。',
        step45desc6: '：指定当插入多少行时创建 tsma，可选项，默认为 0。',
        step46: '标签列与数据列配置参数',
        step46desc: '指定超级表标签列与数据列的配置参数分别在 `super_tables` 中的 `columns` 和 `tag` 中。',
        step46desc1:
          '：指定列类型，可选值请参考 TDengine 支持的数据类型。注：JSON 数据类型比较特殊，只能用于标签，当使用 JSON 类型作为 tag 时有且只能有这一个标签，此时 count 和 len 代表的意义分别是 JSON tag 内的 key-value pair 的个数和每个 KV pair 的 value 的值的长度，value 默认为 string。',
        step46desc2:
          '：指定该数据类型的长度，对 NCHAR，BINARY 和 JSON 数据类型有效。如果对其他数据类型配置了该参数，若为 0 ， 则代表该列始终都是以 null 值写入；如果不为 0 则被忽略。',
        step46desc3: '：指定该类型列连续出现的数量，例如 "count"：4096 即可生成 4096 个指定类型的列。',
        step46desc4:
          '：列的名字，若与 count 同时使用，比如 "name"："current", "count":3, 则 3 个列的名字分别为 current, current_2. current_3。',
        step46desc5: '：数据类型的 列/标签 的最小值。生成的值将大于或等于最小值。',
        step46desc6: '：数据类型的 列/标签 的最大值。生成的值将小于最小值。',
        step46desc7: '：nchar/binary 列/标签的值域，将从值中随机选择。',
        step46desc8: '将该列加入 SMA 中，值为 "yes" 或者 "no"，默认为 "no"。',
        step47: '插入行为配置参数',
        step47desc: '：插入数据的线程数量，默认为 8。',
        step47desc1: '：建表的线程数量，默认为 8。',
        step47desc2: '：预先建立的与 TDengine 服务端之间的连接的数量。若不配置，则与所指定的线程数相同。',
        step47desc3: '：结果输出文件的路径，默认值为 ./output.txt。',
        step47desc4: '：开关参数，要求用户在提示后确认才能继续。默认值为 false 。',
        step47desc5:
          '：启用交错插入模式并同时指定向每个子表每次插入的数据行数。交错插入模式是指依次向每张子表插入由本参数所指定的行数并重复这个过程，直到所有子表的数据都插入完成。默认值为 0， 即向一张子表完成数据插入后才会向下一张子表进行数据插入。在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。',
        step47desc6:
          '：指定交错插入模式的插入间隔，单位为 ms，默认值为 0。 只有当 `-B/--interlace-rows` 大于 0 时才起作用。意味着数据插入线程在为每个子表插入隔行扫描记录后，会等待该值指定的时间间隔后再进行下一轮写入。在 `super_tables` 中也可以配置该参数，若配置则以 `super_tables` 中的配置为高优先级，覆盖全局设置。',
        step47desc7:
          '：每次向 TDengine 请求写入的数据行数，默认值为 30000 。当其设置过大时，TDengine 客户端驱动会返回相应的错误信息，此时需要调低这个参数的设置以满足写入要求。',
        step47desc8: '：生成的随机数据中唯一值的数量。若为 1 则表示所有数据都相同。默认值为 10000 。',
        step48: '查询场景配置参数',
        step48desc:
          '查询场景下 `filetype` 必须设置为 `query`。查询场景可以通过设置 `kill_slow_query_threshold` 和 `kill_slow_query_interval` 参数来控制杀掉慢查询语句的执行，threshold 控制如果 exec_usec 超过指定时间的查询将被 taosBenchmark 杀掉，单位为秒；interval 控制休眠时间，避免持续查询慢查询消耗 CPU ，单位为秒。其它通用参数详见[通用配置参数](#通用配置参数)。',
        step49: '执行指定查询语句的配置参数',
        step49desc: '查询子表或者普通表的配置参数在 `specified_table_query` 中设置。',
        step49desc1: '：查询时间间隔，单位是秒，默认值为 0。',
        step49desc2: '：执行查询 SQL 的线程数，默认值为 1。',
        step49desc3: '：执行的 SQL 命令，必填。',
        step49desc4: '：保存查询结果的文件，未指定则不保存。',
        step410: '查询超级表的配置参数',
        step410desc: '查询超级表的配置参数在 `super_table_query` 中设置。',
        step410desc1: '：指定要查询的超级表的名称，必填。',
        step410desc2: '：查询时间间隔，单位是秒，默认值为 0。',
        step410desc3: '：执行查询 SQL 的线程数，默认值为 1。',
        step410desc4:
          '：执行的 SQL 命令，必填；对于超级表的查询 SQL，在 SQL 命令中保留 "xxxx"，程序会自动将其替换为超级表的所有子表名。替换为超级表中所有的子表名。',
        step410desc5: '：保存查询结果的文件，未指定则不保存。'
      }
    },
    topic: {
      topdesc: `您可以按照下面的步骤消费组织的 ${GRAFANA_GDS} 实例 中的主题。`,
      python: {
        step1: '安装模块',
        step1desc: '首先您需要安装 `taos-ws-py` 模块，版本需要大于 `0.2.1` 。在您的终端执行下面的命令。',
        step1desc1: '您需要先安装 `Python3`。'
      },
      go: {
        step1: '初始化',
        step1desc: '您按照下面命令可以生成 Go 示例模块并加上 `driver-go` 依赖：'
      },
      rust: {
        step1: '创建项目',
        step1desc: '您按照下面命令可以创建 Rust 项目：',
        step1desc1: '然后把依赖包加到 `Cargo.toml` 文件中：'
      },
      createProject: '创建项目',
      step1desc: '您按照下面命令可以创建 {0} 项目：',
      step1desc1: '然后把依赖包加到 `{0}` 文件中：',
      step2: '配置',
      step3: '创建消费者',
      step3desc: '您可以按照下面的代码来创建消费者：',
      step4: '订阅主题',
      step4desc: '您可以按照下面的代码来订阅这个共享的主题 `{0}`： ',
      step5: '关闭消费者',
      step5desc: '如果您想从这个共享的主题 `{0}` 中取消订阅消息，您可以按照下面的代码关闭这个消费者：',
      step6: '完整实例',
      step6desc: '下面是如何消费这个共享主题 `{0}` 的完整的代码示例：',
      enddesc: '了解更多数据订阅的内容，请参考',
      enddesc1: '。',
      enddesc2: '数据订阅',
      defaultTopic: '主题'
    },
    dashboard: {
      topdesc: `要监控 ${GRAFANA_GDS} 运行状态并在出现问题时获得警报，请使用`,
      topdesc1: `。${GRAFANA_GDS} 可以与 Grafana 顺利集成，无需一行代码。 `,

      topdesc2: `关于 ${GRAFANA_GDS} 插件的使用您可以在 `,
      topdesc3: '中了解更多。',
      step1: '安装 Grafana',
      step1desc: `目前 ${GRAFANA_GDS} 支持 Grafana 7.5 以上的版本。请您到 Grafana 官网下载安装包`,
      pluginsdesc: `在浏览器打开 Grafana 后点击三个横条图标，然后再点击 <code>Connections</code>。在弹出页面的搜索栏内搜索 ${GRAFANA_GDS}，然后会弹出 "${GRAFANA_GDS} Data Source"。最后点击 “Install” 按钮安装 ${GRAFANA_GDS} 插件。安装完成后，就可以立即添加 ${GRAFANA_GDS} 数据源。`,
      plugin1desc: `1. 在浏览器打开 Grafana 后点击三个横条图标，然后再点击 <code>Connections</code>。`,
      plugin2desc: `2. 在弹出页面的搜索栏内搜索 ${GRAFANA_GDS}，然后会弹出 "${GRAFANA_GDS} Data Source"。 `,
      plugin3desc: `3. 最后点击 <strong>Install</strong> 按钮安装 ${GRAFANA_GDS} 插件。`,
      plugin4desc: `4. 安装完成后，就可以立即添加 ${GRAFANA_GDS} 数据源。`,
      script1: `如果本地访问 Github 比较方便，可以从 Linux 终端运行下面的脚本来安装 ${GRAFANA_GDS} 数据源插件。`,
      script2: `安装结束以后，请重启 <code>grafana-server</code>。`,
      step2: `安装 ${GRAFANA_GDS} 插件`,
      step3desc1: 'Host:',
      step3desc2: 'User:',
      step3desc3: `输入密码登陆 ${GRAFANA_GDS}，然后点击 <code>Save & Test</code> 按钮来验证 ${GRAFANA_GDS} 是否能够工作。`,
      step2desc: `请复制下面的脚本命令来为数据源安装设置 \`${GRAFANA_GDS}_URL\` 和 \`${GRAFANA_GDS}_TOKEN\` 的环境变量：`,
      step2desc1: `从 Linux 终端运行下面的脚本来安装 ${GRAFANA_GDS} 数据源插件。`,
      step2desc2: '安装结束以后，请重启 grafana-server。',
      step3: '添加数据源',
      step3desc: `在 Grafana 数据源配置页面中，复制如下所示的主机和用户，并将其输入相应的输入框。`,
      step4: '使用 Grafana',
      step4desc: `请创建一个新的仪表盘，或者导入存在的仪表盘来展示 ${GRAFANA_GDS} 里面的数据`,
      step4desc1: '同时更多细节请参考',
      step4desc2: '文档',
      step4desc3: '。',

      monitortip: '请遵循以下步骤用Grafana去监控TDengine的运行状态',
      dashboarddesc: `我们建议在此处使用最新的<a href='https://grafana.com/'>Grafana</a> 8 或 9 版本。您可以在任何<a href='https://grafana.com/docs/grafana/latest/setup-grafana/installation/#supported-operating-systems'>支持的操作系统</a>中，按照 <a href='https://grafana.com/docs/grafana/latest/setup-grafana/installation/'>Grafana官方文档安装说明</a>  安装 <a href='https://grafana.com/'>Grafana</a>。`,

      tab1: '基于 Debian 或 Ubuntu 系统',
      tab2: '基于 CentOS / RHEL 系统',
      tab2sub: '或者用 RPM 安装',
      pluginname2: '手动设置 TDinsight',
      pluginname1: '自动部署 TDinsight',
      plugin1: '从 GitHub 安装 TDengine 最新版数据源插件。',
      plugin2: `我们提供了一个自动化安装脚本 <code>TDinsight.sh</code> 脚本以便用户快速进行安装配置。<br/>

      您可以通过 wget 或其他工具下载该脚本：`,
      pluginsub2: `这个脚本会自动下载最新的<a href='https://github.com/taosdata/grafanaplugin/releases/tag/v3.3.2'>Grafana TDengine 数据源插件</a> 和 <a href='https://github.com/taosdata/grafanaplugin/blob/master/dashboards/TDinsightV3.json'>TDinsight 仪表盘</a> ，将命令行选项中的可配置参数转为 <a href='https://grafana.com/docs/grafana/latest/administration/provisioning/'>Grafana Provisioning</a> 配置文件，以进行自动化部署及更新等操作。利用该脚本提供的告警设置选项，你还可以获得内置的阿里云短信告警通知支持。`,

      logingrafana: `在 Web 浏览器中打开默认的 Grafana 网址：<code>http://localhost:3000</code>。 默认用户名/密码都是 <code>admin</code>。Grafana 会要求在首次登录后更改密码。`,

      nav: `指向 <strong>Configurations -> Data Sources</strong> 菜单，然后点击 <strong>Add data source</strong> 按钮。`,
      subsearch: '搜索并选择<strong>TDengine</strong>。',
      settingtd: `配置 TDengine 数据源。例如：<code>http://localhost:6041</code>`,
      savetest: "保存并测试，正常情况下会报告 'TDengine Data source is working'。",

      import: `在配置 TDengine 数据源界面，点击 <strong>Dashboards</strong> tab。`,
      cont1: '选择 <code>TDengine for 3.x</code>，并点击 <code>import</code>。',
      cont2: `导入完成后，在搜索界面已经出现了 <strong>TDinsight for 3.x</strong> dashboard。`,
      cont3: '进入 TDinsight for 3.x dashboard 后，选择 taosKeeper 中设置的记录监控指标的数据库。',
      cont4: '然后可以看到监控结果。',

      step5: '添加Dashboard',
      desc51: `1. 数据源工作后，单击数据源配置页面上的 <code>Dashboards</code> 选项卡。`,
      desc52: `2. 选择 <code>TDinsight for 3.x</code> 点击导入。`,
      desc53: `3. 单击三个水平条图标，然后单击 <code>Dashboards</code>，搜索 <code>TDinsight</code>，然后单击它。`,
      desc54: `4. 现在你可以看到完整的仪表盘。`
    },
    tools: {
      is: ' 是',
      seeq: {
        desc: 'Seeq 是专门为分析流程数据而设计，同时它可以与历史数据或者其他存储平台中的时序数据一起用于所有垂直行业。TDengine 可以通过 JDBC 连接器作为数据源添加到 Seeq 中。完成数据源配置后，Seeq 就能从 TDengine 读取数据，并提供数据展示、分析和预测等功能。',
        topdesc: '',
        topdesc1:
          ' 是专门为分析流程数据而设计，同时它可以与历史数据或者其他存储平台中的时序数据一起用于所有垂直行业。TDengine 可以通过 JDBC 连接器作为数据源添加到 Seeq 中。完成数据源配置后，Seeq 就能从 TDengine 读取数据，并提供数据展示、分析和预测等功能。',
        step1: '前置条件',
        step1desc: '安装 Seeq Server 和 Seeq Data Lab 软件，请从官方下载地址下载安装 ',
        step1desc1: ' 。',
        step2: '安装 TDengine Java 连接器',
        step2desc: '获取 Seeq 数据地址配置。在 Linux 上，可以执行下面的命令获取：',
        step2desc11: '首先从 ',
        step2desc12: ' 可以下载最新的 TDengine Java 连接器（目前的版本是 ',
        step2desc13: '），然后复制下载的 JAR 文件到这个文件目录 the_directory_found_in_step_1/plugins/lib/ 。',
        step2desc2: '重启 Seeq 服务器。在 Linux 上，可以执行下面的命令：',
        step3: '添加 TDengine 数据源',
        step3full: '把 TDengine 数据源添加到 Seeq 数据源',
        step3desc: '打开 Seeq，以 admin 用户登录，然后打开 Administration，点击“Add Data Source”',
        step3desc1: '对于连接器，请选择 SQL connector v2',
        step3desc2: '在“Additional Configuration”的输入框, 请复制和粘贴下面的内容：',
        step3desc3: '对于“QueryDefintions”，请参考下面的例子来完成您自己的查询定义。',
        step4: '智能电表样例',
        step4full: '导入大量时序数据：智能电表样例',
        step4desc:
          'TDengine 有自己独特的数据模型。它要求使用超级表作为模板，为每个数据采集点创建一个表。每个表最多可关联 128 个标签（静态属性）。一个数据库可能包含一百万甚至十亿个表。通过 Seeq 中的变量，您可以通过直接查询超级表而不是单个表，将超级表下的所有时序数据（表）导入到 Seeq 中。此外，您还可以将存储在 TDengine 中的表的相关标签导入 Seeq 中，这样您就可以通过搜索这些标签轻松找到想查询的时序数据。',
        step4desc1: '根据 TDengine 文档中的经典智能电表样例，可以使用下面配置来搜索超级表 meters 下的所有时序数据。',
        step4desc2: '在上面的例子中，tablename、location 和 groupid 可以通过下面的 SQL 语句获取：',
        step4desc3:
          '查询结果将分配给变量 tablename、location 和 groupid。根据查询结果，Seeq 会将此查询配置扩展为多个时间序列。',
        step4desc4:
          'TDengine 支持多数据列，您可以使用 Seeq 变量为每一列生成一个时间序列。更多关于 Seeq 变量的信息，请查阅 ',
        step4desc41: 'Seeq 文档',
        step4desc42: '。'
      },
      powerbi: {
        desc: ' 是由 Microsoft 提供的一种商业分析工具。通过配置使用 ODBC 连接器，Power BI 可以快速的访问 TDengine。您可以将标签数据、原始时序数据或按时间聚合后的时序数据从 TDengine 导入到 Power BI，制作报表或仪表盘，整个过程不需要任何的代码编写过程。',
        step1: '前置',
        step1full: '前置条件',
        step1desc: '安装完成 Power BI Desktop 软件并可以运行（如未安装，请从',
        step1desc1: '官方地址',
        step1desc2: '下载最新的 Windows X64 版本）。',
        step1desc3: 'TDengine 服务端软件已经安装并运行。',
        step2: '安装 ODBC',
        step2full: '安装 ODBC 连接器',
        step3: '配置 ODBC',
        step3full: '配置 ODBC 数据源',
        step4: '导入数据',
        step4full: '导入 TDengine 数据到 Power BI',
        step4desc:
          '打开 Power BI 并登录后，通过如下步骤添加数据源，“主页” -> “获取数据” -> “其他” -> “ODBC” -> “连接”。',
        step4desc1:
          '选择刚才创建的数据源名称，比如“MyTDengine”，点击“确定”按钮。在弹出的“ODBC 驱动程序”对话框中，在左边的菜单里面选择“默认或自定义”，点击“连接”按钮，可以连接到配置好的数据源。在进入“导航器”后，可以浏览对应数据库的数据表并加载。',
        step4desc2: '如果需要输入 SQL 语句，可以点击“高级选项”，在展开的对话框中输入并加载数据。',
        step4desc3:
          '为了更好的使用 Power BI 分析 TDengine 中的数据，您需要理解维度、度量、时序、相关性的概念，然后通过自定义的 SQL 语句导入数据。',
        step4desc4:
          '维度：通常是分类（文本）数据，描述设备、测点、型号等类别信息。在 TDengine 的超级表中，使用标签列存储数据的维度信息，可以通过形如 select distinct tbname, tag1, tag2 from supertable 的 SQL 语法快速获得维度信息。',
        step4desc5:
          '度量：可以用于进行计算的定量（数值）字段， 常见计算有求和、平均值和最小值等。如果测点的采集频率为秒，那么一年就有 31,536,000 条记录，把这些数据全部导入 Power BI 会严重影响其执行效率。在 TDengine 中，您可以使用数据切分查询、窗口切分查询等语法，结合与窗口相关的伪列，把降采样后的数据导入到 Power BI 中，具体语法参考 ',
        step4desc6: 'TDengine 特色查询功能介绍',
        step4desc7: '。',
        step4desc8:
          '窗口切分查询：比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值，这种场景下可以使用窗口子句来获得需要的降采样查询结果，对应的 SQL 语句形如 select tbname, _wstart date，avg(temperature) temp from table interval(10m) ，其中 _wstart 是伪列，表示时间窗口起始时间，10m 表示时间窗口的持续时间，avg(temperature) 表示时间窗口内的聚合值。',
        step4desc9:
          '数据切分查询：如果需要同时获取很多温度传感器的聚合数值，可对数据进行切分，然后在切分出的数据空间内再进行一系列的计算，对应的 SQL 语法参考 partition by part_list。数据切分子句最常见的用法就是在超级表查询中，按标签将子表数据进行切分，将每个子表的数据独立出来，形成一条条独立的时间序列，方便各种时序场景的统计分析。',
        step4desc10:
          '时序：在绘制曲线或者按照时间聚合数据时，通常需要引入日期表。日期表可以从 Excel 表格中导入，也可以在 TDengine 中执行 SQL 语句获取，例如 select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)，其中 fill 字句表示数据缺失情况下的填充模式，伪列_wstart 则为要获取的日期列。',
        step4desc11:
          '相关性：告诉数据之间如何关联，度量和维度可以通过 tbname 列关联在一起，日期表和度量则可以通过 date 列关联，配合形成可视化报表。',
        step5: '样例',
        step5full: '智能电表样例',
        step5desc:
          'TDengine 有自己独特的数据模型，它使用超级表作为模板，为每个设备创建一个表，每个表最多可创建 4096 个数据列和 128 个标签列。在',
        step5desc0:
          '中，假如一个电表每秒产生一条记录，一天就有 86,400 条记录，一年就有 31,536,000 条记录，1000 个电表将占用 600 GB 原始磁盘空间。因此，Power BI 更多的应用方式是将标签列映射为维度列，数据列的聚合结果导入为度量列，最终为关键决策制定者提供所需的指标。',
        step5desc1: '导入维度数据：在 Power BI 中导入表的标签列，取名为 tags，SQL 如下：',
        step5desc2:
          '导入度量数据：在 Power BI 中，按照 1 小时的时间窗口，导入每个电表的电流均值、电压均值、相位均值，取名为 data，SQL 如下：',
        step5desc3:
          '建立维度和度量的关联关系：在 Power BI 中，打开模型视图，建立表 tags 和 data 的关联关系，将 tbname 设置为关联数据列。之后，就可以在柱状图、饼图等控件中使用这些数据。更多有关 Power BI 构建视觉效果的信息，请查询 ',
        step5desc4: 'Power BI 文档',
        step5desc5: '。'
      },
      yonghongbi: {
        name: '永洪 BI',
        desc: '永洪一站式大数据 BI 平台',
        desc1:
          ' 为各种规模的企业提供灵活易用的全业务链的大数据分析解决方案，让每一位用户都能使用这一平台轻松发掘大数据价值，获取深度洞察力。TDengine 可以通过 JDBC 连接器作为数据源添加到永洪 BI 中。完成数据源配置后，永洪 BI 就能从 TDengine 中读取数据，并提供数据展示、分析和预测等功能。',
        step1: '前置条件',
        step11desc: 'Yonghong Desktop Basic 已经安装并运行（如果未安装，请到 ',
        step11desc1: '永洪科技官方下载页面',
        step11desc2: ' 下载）。',
        step12desc: 'TDengine 已经安装并运行，并确保在 TDengine 服务端启动了 taosadapter 服务。',
        step2: '安装驱动',
        step2full: '安装 JDBC 连接器',
        step2desc: '从 ',
        step2desc1: ' 下载最新的 TDengine JDBC 连接器（目前的版本是 ',
        step2desc2: '），并安装在 BI 工具运行的机器上。',
        step3: '配置',
        step3full: '配置 TDengine JDBC 数据源',
        step31desc: '在打开的 Yonghong Desktop BI 工具中点击“添加数据源”，选择 SQL 数据源中的“GENERIC”类型。',
        step32desc:
          '点击“选择自定义驱动”，在“驱动管理”对话框中，点击“驱动列表”旁边的“+”，输入名称“MyTDengine”。然后点击“上传文件”按钮上传刚刚下载的 TDengine JDBC 连接器文件"taos-jdbcdriver-3.2.7-dist.jar"，并选择“com.taosdata.jdbc.rs.RestfulDriver”驱动，最后点击“确定”按钮完成驱动添加。',
        step33desc: '然后请复制下面的内容到“URL”字段：',
        step34desc: '接着在“认证方式”选择“无身份认证”。',
        step35desc: '在数据源的高级设置中，修改“Quote符号”的值为反引号“`”。',
        step36desc: '点击“测试连接”，弹出“测试成功”的对话框。点击“保存”按钮，输入“MyTDengine”来保存 TDengine 数据源。',
        step4: '创建数据集',
        step4full: '创建 TDengine 数据集',
        step41desc: '在 BI 工具中点击“添加数据源”，展开刚刚创建的数据源，并浏览 TDengine 中的超级表。',
        step42desc: '您可以将超级表的数据全部加载到 BI 工具中，也可以通过自定义 SQL 语句导入部分数据。',
        step43desc:
          '当勾选“数据库内计算”时，BI 工具将不再缓存 TDengine 的时序数据，并在处理查询时将 SQL 请求发送给 TDengine 直接处理。',
        step44desc:
          '当导入数据后，BI 工具会自动将数值类型设置为“度量”列，将文本类型设置为“维度”列。而在 TDengine 的超级表中，采用普通列作为数据的度量，采用标签列作为数据的维度，因此您可能需要在创建数据集时更改部分列的属性。TDengine 在支持标准 SQL 的基础之上，还提供了一系列满足时序业务场景需求的特色查询语法，例如数据切分查询、窗口切分查询等，具体参考 ',
        step44desc1: 'TDengine 特色查询功能介绍',
        step44desc2:
          '。通过使用这些特色查询，当 BI 工具将 SQL 查询发送到 TDengine 数据库时，可以大大提高数据访问速度，减少网络传输带宽。',
        step45desc:
          '在 BI 工具中，您可以创建“参数”并在 SQL 语句中使用，通过手动、定时的方式动态执行这些 SQL 语句，即可实现可视化报告的刷新效果。如下 SQL 语句：',
        step45desc0: '可以从 TDengine 实时读取数据，其中：',
        step45desc1: '：表示时间窗口起始时间。',
        step45desc2: '：表示时间窗口内的聚合值。',
        step45desc3:
          '：表示在 SQL 语句中引入名称为 interval 的参数，当 BI 工具查询数据时，会给 interval 参数赋值，如果取值为 1m，则表示按照 1 分钟的时间窗口降采样数据。',
        step45desc4:
          '：该参数用来指定查询的数据表名称，当在 BI 工具中把某个“下拉参数组件”的 ID 也设置为 metric 时，该“下拉参数组件”的被选择项将会和该参数绑定在一起，实现动态选择的效果。',
        step45desc5: '：这两个参数用来表示查询数据集的时间范围，可以与“文本参数组件”绑定。',
        step45desc6:
          '可以在 BI 工具的“编辑参数”对话框中修改“参数”的数据类型、数据范围、默认取值，并在“可视化报告”中动态设置这些参数的值。',
        step5: '制作报告',
        step5full: '制作可视化报告',
        step51desc: '在永洪 BI 工具中点击“制作报告”，创建画布。',
        step52desc: '拖动可视化组件到画布中，例如“表格组件”。',
        step53desc: '在“数据集”侧边栏中选择待绑定的数据集，将数据列中的“维度”和“度量”按需绑定到“表格组件”。',
        step54desc: '点击“保存”后，即可查看报告。',
        step55desc: '更多有关永洪 BI 工具的信息，请查询其 ',
        step55desc1: ' 帮助文档',
        step55desc2: ' 。'
      },
      superset: {
        name: 'Superset',
        desc: '一个现代的企业级商业智能（BI）Web 应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，它拥有活跃的社区和丰富的生态系统。Superset 提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。',
        topdesc: '',
        topdesc1: ' 通过 TDengine 的 Python 连接器, ‌Superset‌ 可支持 TDengine 数据源并提供数据展现、分析等功能。',
        step1: '前置',
        step1full: '前置条件',
        step1desc: 'Apache Superset v2.1.0 或以上版本安装完成并可运行 (如未安装，请参考 ',
        step1desc1: '官方文档',
        step1desc2: ' ）。',
        step2: '安装 TDengine Python 连接器',
        step2full: '安装 TDengine Python 连接器',
        step2desc: 'TDengine Python 连接器从 `v2.1.21` 开始自带 Superset 连接驱动，安装程序会把连接驱动安装到 Superset 相应目录下并向 Superset 提供数据源服务。',
        step2desc1: 'Superset 与 TDengine 之间使用 WebSocket 协议连接，所以需另安装支持 WebSocket 连接协议的组件 `taos-ws-py`(版本要求 0.3.8 及以上) , 全部安装脚本如下：',
        step3: '配置 TDengine 数据源',
        step3full: '配置 TDengine 数据源',
        step3desc: '启动 Superset 服务之后，在浏览器中访问服务地址（例如：http://localhost:8088）并登录， 详细参考 ',
        step31desc1: 'Superset 安装文档',
        step31desc2: '。',
        step32desc1: '在 Superset 浏览器器页面中点击右侧的 "Setting" → "Database Connections" → "+DATABASE" (若下拉列表中无 “TDengine” 项，请确认安装顺序，确保先安装 Superset，再安装 TDengine Python 连接器）。',
        step33desc: '在弹出的 “Connect a database” 对话框中，填写如下必要信息：',
        step33desc1: '【Display Name】：',
        step33desc2: '数据源显示的名称，必填，比如 “MyTDengine”',
        step33desc3: '【SQLAlchemy URI】】：',
        step34desc: '点击 “TEST CONNECTION” 测试连接是否成功，测试通过后点击 “CONNECT” 按钮，完成连接。',
        step4: '导入数据',
        step4full: '导入 TDengine 数据到 Superset',
        step4desc: 'TDengine 数据源与其它数据源使用上无差别，这里简单介绍下数据查询：',
        step4desc1: '在 Superset Web 页面上点击右上角 “+” 号按钮，选择 “SQL query”, 进入查询页面。 ',
        step4desc2: '在查询页面的左上角 “DATABASE” 下拉列表中选择前面已创建好的数据源, 比如 “MyTDengine”。',
        step4desc3: '在 “SCHEMA” 下拉列表，选择要操作的数据库名（系统库不显示）。',
        step4desc4: '在 “SEE TABLE SCHEMA” 下拉列表，选择要操作的超级表名或普通表名（子表不显示）后， 会在下方显示选定表的 SCHEMA 信息。',
        step4desc5: '在上方的 SQL 编辑器区域可输入符合 TDengine 语法的 SQL 语句后，点击 “Run” 按钮执行。',
        step4desc6: '在上方的 SQL 编辑器区域内点击 “Sava” 按钮旁边的 “v” 按钮后，选择 “Sava dataset” 按钮进行保存。',
        step5: '样例',
        step5full: '数据分析',
        step5desc1: '在 Superset Web 页面上点击 “Datasets” 菜单，打开 “Datasets” 页面。',
        step5desc2: '在 “Datasets” 页面上点击刚才保存的 Datasets, 打开 “Chart” 页面。',
        step5desc3: '在 “Chart” 页面的左侧第二列选择横纵坐标的字段。',
        step5desc4: '选择好后点 “UPDATE CHART”，图表就生成好了。',
        step5desc5: '更多有关 Superset 的使用，请查询其 ',
        step5desc6: ' Superset 文档',
        step5desc7: '。'
      },
      tableau: {
        name: 'Tableau',
        desc: '一款知名的商业智能工具，它支持多种数据源，可方便地连接、导入和整合数据。并且可以通过直观的操作界面，让用户创建丰富多样的可视化图表，并具备强大的分析和筛选功能，为数据决策提供有力支持。用户可通过 TDengine ODBC Connector 将标签数据、原始时序数据或者经时间聚合后的时序数据从 TDengine 导入到 Tableau，用以制作报表或仪表盘，且整个过程无需编写任何代码。',
        step1: '前置',
        step1full: '前置条件',
        step1desc: 'Tableau 桌面版完成安装并可以运行（如未安装，请从 ',
        step1desc1: '官方地址',
        step1desc2: ' 下载最新的 Windows X64 版本）。',
        step2: '安装 ODBC',
        step2full: '安装 ODBC 连接器',
        step3: '配置 ODBC',
        step3full: '配置 ODBC 数据源',
        step23desc1: '填写需要连接的数据库，必填，比如 “test”',
        step4: '导入数据',
        step4full: '导入 TDengine 数据到 Tableau',
        step4desc: '打开 Tableau 之后在其连接页面中搜索 “ODBC”，并选择 “其他数据库 (ODBC)”。',
        step4desc1: '点击 DSN 单选框，选择刚才创建的数据源名称，比如 “MyTDengine”，接着点击 ”连接“ 按钮。待连接成功后，删除字符串附加部分的内容，最后点击 ”登录“ 按钮即可。',
        step4desc2: '在工作簿页面中，选择已连接的数据源，并点击数据库的下拉列表，选择需要进行数据分析的数据库。',
        step4desc3: '点击表选项中的 ”查找“ 按钮，即可将该数据库下的所有表显示出来。拖动需要分析的表到右侧区域，即可显示出表结构。',
        step4desc4: '点击下方的 ”立即更新“ 按钮，即可将表中的数据展示出来。',
        step5: '样例',
        step5full: '数据分析',
        step5desc1: '在工作簿页面中点击 “工作表”，弹出 “数据分析” 页面。',
        step5desc2: '在 “数据分析” 侧边栏中会展示出表的所有字段。',
        step5desc3: '将字段按照 “维度” 和 “度量” 拖动到右侧列行的 “表格组件“ 上，下方即可展示出图表。',
        step5desc4: '更多有关 Tableau 工具的信息，请查询其 ',
        step5desc5: ' Tableau 文档',
        step5desc6: ' 。'
      },
      excel: {
        name: 'Excel',
        desc: '微软公司（Microsoft）开发的一款功能强大且应用广泛的电子表格软件。通过配置使用 ODBC 连接器，Excel 可以快速访问 TDengine 的数据。用户可以将标签数据、原始时序数据或按时间聚合后的时序数据从 TDengine 导入到 Excel，用以制作报表整个过程不需要任何代码编写过程。',
        step1: '前置',
        step1full: '前置条件',
        step1desc: 'Excel 完成安装并运行, 如未安装，请下载并安装, 具体操作请参考 ',
        step1desc1: 'Microsoft 官方文档',
        step1desc2: '。',
        step2: '安装 ODBC',
        step2full: '安装 ODBC 连接器',
        step3: '配置 ODBC',
        step3full: '配置 ODBC 数据源',
        step4: '导入数据',
        step4full: '导入 TDengine 数据到 Excel',
        step4desc: '在 Windows 系统环境下启动 Excel，之后选择 “数据” -> “获取数据” -> “自其他源” -> “从ODBC”。',
        step4desc1: '在弹出窗口的 “数据源名称(DSN)” 下拉列表中选择需要连接的数据源后，点击 “确定” 按钮。',
        step4desc2: '在弹出的 “ODBC 驱动程序” 窗口中选择 “默认自定义” 菜单后点 “连接” 按钮。',
        step4desc3: '在弹出的 “导航器” 对话框中，选择要加载的库表, 并点击 “加载” 完成数据加载。',
        step5: '样例',
        step5full: '数据分析',
        step5desc1: '在已导入数据的 Excel 工作表里，选中所需的数据区域。',
        step5desc2: '在 Excel 菜单栏中找到并点击【插入】选项卡，选择需要的图表类型。',
        step5desc3: 'Excel 会立即在工作表中生成一个基于所选数据的图表。',
        step5desc4: '更多有关 Execl 的使用，请查询其 ',
        step5desc5: ' Excel 文档',
        step5desc6: ' 。'
      }
    },
    connectorTip: `使用您选择的编程语言<a target='_blank' href='${$IS_COMMUNITY ? 'https://docs.taosdata.com' : '/docs'}/taos-sql/select/'>使用SQL</a>查询数据。`,
    docConfig: {
      title: '配置',
      content: `请在您的终端先执行命令来保存 ${GRAFANA_GDS}  的{0}为系统环境变量：`,
      url: '网关URL和令牌',
      dsn: 'DSN连接字符串',
      tmq: 'TMQ连接字符串',
      endpoint: '网络终端和令牌',
      bottom: '另外，您也可以把环境变量设置到您开发工具的运行配置里面。'
    }
  }
};
