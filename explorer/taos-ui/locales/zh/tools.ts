export default {
  cli: {
    desc: 'TDengine 的交互式命令行工具',
    topdesc: 'TDengine 命令行程序（以下简称 TDengine CLI）是用户操作 TDengine 实例并与之交互的最简洁最常用的方式。',
    step1: '安装',
    step1desc: '运行 TDengine CLI 来访问 TDengine Cloud ，请首先下载和安装最新的 ',
    step1desc1: 'TDengine 客户端安装包',
    step1desc2: '（',
    step1desc3: '，',
    step1desc4: '）。',
    step2: '配置',
    step2desc: '在您的 Linux 终端里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：',
    step2desc1: '在您的 Windows CMD 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：',
    step2desc2: '或者在您的 Windows PowerShell 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：',
    step2desc3: '在您的 Mac 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：',
    step3: '建立连接',
    step3desc: '如果您已经设置了环境变量，您只需要立即执行 `taos` 命令就可以访问 TDengine Cloud 实例。',
    step3desc1:
      '如果您没有设置 TDengine Cloud 实例的环境变量，或者您想访问其他 TDengine Cloud 实例，您可以使用下面的命令 `taos -E <DSN>`来执行：',
    step4: '使用 TDengine CLI',
    step4desc:
      '如果成功连接上 TDengine 服务，TDengine CLI 会显示一个欢迎的消息和版本信息。如果失败了，TDengine CLI 会打印失败消息。TDengine CLI 打印的成功消息如下：',
    step4desc1: '进入 TDengine CLI 以后，您就可以执行大量的 SQL 命令来进行插入，查询或者进行管理。详情请参考',
    step4desc2: '官方文档',
    step4desc3: '。'
  },
  benchmark: {
    desc: 'taosBenchmark 是一个用于测试 TDengine 产品性能的工具',
    step1: '简介',
    step1desc:
      'taosBenchmark (曾用名 taosdemo ) 是一个用于测试 TDengine 产品性能的工具。taosBenchmark 可以测试 TDengine 的插入、查询和订阅等功能的性能，它可以模拟由大量设备产生的大量数据，还可以灵活地控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表的数量、每张子表的数据量、插入数据的时间间隔、taosBenchmark 的工作线程数量、是否以及如何插入乱序数据等。为了兼容过往用户的使用习惯，安装包提供 了 taosdemo 作为 taosBenchmark 的软链接。',
    step1desc1:
      '在使用 TDengine Cloud 的时候，请注意，没有授权的用户是没有办法通过任何工具包括 taosBenchmark 来创建数据库的。只能通过 TDengine Cloud 的数据浏览器来创建数据库。这个文档中提到的任何创建数据库的内容请忽略，并在 TDengine Cloud 里面手动创建数据库。',
    step2: '安装',
    step2desc: '使用 taosBenchmark 工具来访问 TDengine Cloud 的实例，您首先需要下载和安装',
    step3: '运行',
    step3desc: '下面分成两个部分详细阐述如何运行 taosBenchmark 工具：',
    step31: '运行方式',
    step31desc: '用户只能使用一个命令行参数 `-f <json file>` 指定配置文件。',
    step31desc1:
      'taosBenchmark 支持对 TDengine 做完备的性能测试，其所支持的 TDengine 功能分为三大类：写入、查询和订阅。这三种功能之间是互斥的，每次运行 taosBenchmark 只能选择其中之一。值得注意的是，所要测试的功能类型在使用命令行配置方式时是不可配置的，命令行配置方式只能测试写入性能。若要测试 TDengine 的查询和订阅性能，必须使用配置文件的方式，通过配置文件中的参数 `filetype` 指定所要测试的功能类型。',
    step31desc2: '在运行 taosBenchmark 之前要确保 TDengine Cloud 实例是运行中的状态。',
    step32: '插入场景',
    step32desc:
      'taosBenchmark 工具通过下面的 json 文件中的“filetype”属性设置成“insert”，可以远程把数据写入到 TDengine Cloud 的实例。',
    step32desc1: '请注意：下面配置文件中的数据库需要首先在“数据浏览器”创建出来。',
    step33: '查询场景',
    step33desc:
      'taosBenchmark 工具通过下面的 json 文件设置查询的类型，可以远程把连接到 TDengine Cloud 的实例进行查询测试。',
    step4: '参数详解',
    step4full: '配置文件参数详解',
    step4desc: '下面分成三部分详细解释每一个配置文件的参数：',
    step41: '基本参数详解',
    step41desc:
      '本节所列参数适用于所有功能模式。因为是和 TDengine Cloud 实例进行交互，所以不用设置“host”，“port”，“user”和“password”属性。',
    step41desc1:
      '：要测试的功能，可选值为 `insert`, `query` 和 `subscribe`。分别对应插入、查询和订阅功能。每个配置文件中只能指定其中之一。',
    step41desc2: '：TDengine 客户端配置文件所在的目录，默认路径是 /etc/taos 。',
    step42: '插入场景配置参数',
    step42desc: '插入场景下 `filetype` 必须设置为 `insert`，该参数及其它通用参数详见[通用配置参数](#通用配置参数)',
    step43: '流式计算配置参数',
    step43desc: '创建流式计算的相关参数在 json 配置文件中的 `stream` 中配置，具体参数如下。',
    step43desc1: '：流式计算的名称，必填项。',
    step43desc2: '：流式计算对应的超级表名称，必填项。',
    step43desc3: '：流式计算的sql语句，必填项。',
    step43desc4: '：流式计算的触发模式，可选项。',
    step43desc5: '：流式计算的水印，可选项。',
    step43desc6: '：是否创建流式计算，可选项为 "yes" 或者 "no", 为 "no" 时不创建。',
    step44: '#### 超级表相关配置参数',
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
    step44desc14: '：仅当 childtable_exists 为 yes 时生效，指定从超级表获取子表列表时的偏移量，即从第几个子表开始。',
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
  },
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
    step2desc: '使用 taosdump，您需要下载并安装 ',
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
  },
  grafana: {
    desc: 'TDengine 能够与开源数据可视化系统 Grafana 集成来构件一个数据的监控和告警系统。整个过程无需进行任何代码开发，您就可以通过可视化你存储在 TDengine 的数据并展示在仪表盘里面。关于 TDengine 插件的使用您可以在Github中了解更多。',
    topdesc: 'TDengine 能够与开源数据可视化系统 ',
    topdesc1:
      ' 快速集成搭建数据监测报警系统，整个过程无需任何代码开发，TDengine 中数据表的内容可以在仪表盘(Dashboard)上进行可视化展现。关于 TDengine 插件的使用您可以在 ',
    topdesc3: ' 中了解更多。',
    step1: '安装 Grafana',
    step1desc: '目前 TDengine 支持 Grafana 7.5 以上的版本。请您到 Grafana 官网下载安装包（',
    step1desc1: '）。',
    step2: '安装 TDengine 插件',
    step2desc: '在浏览器打开 Grafana 后点击三个横条图标，然后再点击 “Connections”。',
    step2desc11: '在弹出页面的搜索栏内搜索 TDengine，然后会弹出 "TDengine Data Source"。',
    step2desc12: '最后点击 “Install” 按钮安装 TDengine 插件。',
    step2desc13: '安装完成后，就可以立即添加 TDengine 数据源。',
    step2desc2: '如果本地访问 Github 比较方便，可以从 Linux 终端运行下面的脚本来安装 TDengine 数据源插件。',
    step2desc3: '安装结束以后，请重启 grafana-server。',
    step3: '添加数据源',
    step3desc: '在打开的 Grafana 数据源配置页面中，复制下面列出的主机和令牌值，然后粘贴到 Grafana 的相应输入框中。',
    step3desc1: 'Host:',
    step3desc2: 'Token:',
    step3desc3: '然后点击 "Save & Test" 按钮来验证 TDengine 是否能够工作。',
    step4: '使用 Grafana',
    step4desc: '请创建一个新的仪表盘，或者导入存在的仪表盘来展示 TDengine 里面的数据。同时更多细节请参考',
    step4desc2: '文档',
    step4desc3: '。'
  },
  gds: {
    desc: 'Google Data Studio可以快速访问 TDengine， 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘.',
    topdesc: '使用',
    topconnector: '第三方连接器',
    topdesc1:
      '，Google Data Studio可以快速访问 TDengine， 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘。整个过程不需要任何的代码编写过程。可以分享报表和仪表盘给不同的个人，团队以及全世界，还可以跟其他人员实时协作，另外在任何的网页里面嵌入您的报表。',
    topdesc2: '更多使用 Data Studio 和 TDengine 集成可以参考',
    topdesc3: '。',
    step1: '选择数据源',
    step1desc: '目前的',
    step1desc1: ' 连接器',
    step1desc2:
      ' 支持两种不同的数据源：TDengine Server 和 TDengine Cloud。首先选择”TDengine Cloud“类型然后点击“下一步”。',
    step2: '连接器配置',
    step21: '必须的配置',
    step21desc: 'TDengine Cloud URL：',
    step211: 'TDengine Cloud 令牌：',
    step212: '数据库',
    step212desc: '数据库的名称，该数据库包含您想查询数据和创建报表的的表，可以是一般表，超级表或者子表。',
    step213: '表',
    step213desc: '您希望查询数据和执行报表的表的名称',
    step213desc1: '注意',
    step213desc2: ' 可以获取的最大记录行数是1000000。',
    step22: '可选配置',
    step221: '查询从开始日期到结束日期的数据',
    step221desc:
      '在页面上面配置您的连接器的两个时间输入框，这两个时间过滤条件是用来过滤大量数据的。时间输入框的格式是“YYYY-MM-DD HH:MM:SS”。比如：',
    step221desc1:
      '查询结果的开始时间戳是由 `start date` 定义的。加上这个条件，您不会获取到在 `start date` 时间戳之前的数据。',
    step221desc2:
      '`end time`输入框表明查询结束的时间戳。因此，在结束时间戳之后的数据也获取不到。这些条件是利用 SQL 的 where 语句来实现的。比如：',
    step221desc3: '事实上，您可通过一些过滤器来加快报表加载数据的速度。',
    step221desc4: '在配置完成以后，点击“CONNECT”按钮，您就会连接上您的具有给定数据库和表的“TDengine Cloud”。',
    step3: '创建报表和仪表盘',
    step3desc: '使用交互式仪表盘和优美报表解锁您的 TDengine 数据能力，',
    step3desc1: '更多详情请参考',
    step3desc2: '文档',
    step3desc3: '。'
  },
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
    desc: ' 是由 Microsoft 提供的一种商业分析工具。通过配置使用 ODBC 连接器，Power BI 可以快速的访问 TDengine Cloud 的实例。您可以将标签数据、原始时序数据或按时间聚合后的时序数据从 TDengine Cloud 导入到 Power BI，制作报表或仪表盘，整个过程不需要任何的代码编写过程。',
    step1: '前置',
    step1full: '前置条件',
    step1desc: '安装完成 Power BI Desktop 软件并可以运行（如未安装，请从 ',
    step1desc1: '官方地址',
    step1desc2: ' 下载最新的 Windows X64 版本）。',
    step2: '安装 ODBC',
    step2full: '安装 ODBC 连接器',
    step3: '配置 ODBC',
    step3full: '配置 ODBC 数据源',
    step4: '导入数据',
    step4full: '导入 TDengine 数据到 Power BI',
    step4desc: '打开 Power BI 并登录后，通过如下步骤添加数据源，“主页” -> “获取数据” -> “其他” -> “ODBC” -> “连接”。',
    step4desc1:
      '选择刚才创建的数据源名称，比如“MyTDengine”，点击“确定”按钮。在弹出的“ODBC 驱动程序”对话框中，在左边的菜单里面选择“默认或自定义”，点击“连接”按钮，可以连接到配置好的数据源。在进入“导航器”后，可以浏览对应数据库的数据表并加载。',
    step4desc2: '如果需要输入 SQL 语句，可以点击“高级选项”，在展开的对话框中输入并加载数据。',
    step4desc3:
      '为了更好的使用 Power BI 分析 TDengine 中的数据，您需要理解维度、度量、时序、相关性的概念，然后通过自定义的 SQL 语句导入数据。',
    step4desc4label: '维度',
    step4desc4:
      '：通常是分类（文本）数据，描述设备、测点、型号等类别信息。在 TDengine 的超级表中，使用标签列存储数据的维度信息，可以通过形如 `select distinct tbname, tag1, tag2 from supertable` 的 SQL 语法快速获得维度信息。',
    step4desc5label: '度量',
    step4desc5:
      '：可以用于进行计算的定量（数值）字段， 常见计算有求和、平均值和最小值等。如果测点的采集频率为秒，那么一年就有 31,536,000 条记录，把这些数据全部导入 Power BI 会严重影响其执行效率。在 TDengine 中，您可以使用数据切分查询、窗口切分查询等语法，结合与窗口相关的伪列，把降采样后的数据导入到 Power BI 中，具体语法参考 ',
    step4desc6: 'TDengine 特色查询功能介绍',
    step4desc7: ' 。',
    step4desc8label: '窗口切分查询',
    step4desc8:
      '：比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值，这种场景下可以使用窗口子句来获得需要的降采样查询结果，对应的 SQL 语句形如 `select tbname, _wstart date，avg(temperature) temp from table interval(10m)` ，其中 _wstart 是伪列，表示时间窗口起始时间，10m 表示时间窗口的持续时间，avg(temperature) 表示时间窗口内的聚合值。',
    step4desc9label: '数据切分查询',
    step4desc9:
      '：如果需要同时获取很多温度传感器的聚合数值，可对数据进行切分，然后在切分出的数据空间内再进行一系列的计算，对应的 SQL 语法参考 partition by part_list。数据切分子句最常见的用法就是在超级表查询中，按标签将子表数据进行切分，将每个子表的数据独立出来，形成一条条独立的时间序列，方便各种时序场景的统计分析。',
    step4desc10label: '时序',
    step4desc10:
      '：在绘制曲线或者按照时间聚合数据时，通常需要引入日期表。日期表可以从 Excel 表格中导入，也可以在 TDengine 中执行 SQL 语句获取，例如 `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`，其中 fill 字句表示数据缺失情况下的填充模式，伪列_wstart 则为要获取的日期列。',
    step4desc11label: '相关性',
    step4desc11:
      '：告诉数据之间如何关联，度量和维度可以通过 tbname 列关联在一起，日期表和度量则可以通过 date 列关联，配合形成可视化报表。',
    step5: '样例',
    step5full: '智能电表样例',
    step5desc:
      'TDengine 有自己独特的数据模型，它使用超级表作为模板，为每个设备创建一个表，每个表最多可创建 4096 个数据列和 128 个标签列。在 ',
    step5desc01: '智能电表样例',
    step5desc02:
      ' 中，假如一个电表每秒产生一条记录，一天就有 86,400 条记录，一年就有 31,536,000 条记录，1000 个电表将占用 600 GB 原始磁盘空间。因此，Power BI 更多的应用方式是将标签列映射为维度列，数据列的聚合结果导入为度量列，最终为关键决策制定者提供所需的指标。',
    step5desc1: '导入维度数据：在 Power BI 中导入表的标签列，取名为 tags，SQL 如下：',
    step5desc2:
      '导入度量数据：在 Power BI 中，按照 1 小时的时间窗口，导入每个电表的电流均值、电压均值、相位均值，取名为 data，SQL 如下：',
    step5desc3:
      '建立维度和度量的关联关系：在 Power BI 中，打开模型视图，建立表 tags 和 data 的关联关系，将 tbname 设置为关联数据列。之后，就可以在柱状图、饼图等控件中使用这些数据。更多有关 Power BI 构建视觉效果的信息，请查询 ',
    step5desc4: 'Power BI 文档',
    step5desc5: '。'
  },
  yonghongbi: {
    desc: '永洪一站式大数据 BI 平台',
    desc1:
      ' 为各种规模的企业提供灵活易用的全业务链的大数据分析解决方案，让每一位用户都能使用这一平台轻松发掘大数据价值，获取深度洞察力。TDengine Cloud 的实例可以通过 JDBC 连接器作为数据源添加到永洪 BI 中。完成数据源配置后，永洪 BI 就能从 TDengine Cloud 实例中读取数据，并提供数据展示、分析和预测等功能。',
    step1: '前置条件',
    step11desc: 'Yonghong Desktop Basic 已经安装并运行（如果未安装，请到 ',
    step11desc1: '永洪科技官方下载页面',
    step11desc2: ' 下载）。',
    step2: '安装驱动',
    step2full: '安装 JDBC 连接器',
    step2desc: '从 ',
    step2desc1: ' 下载最新的 TDengine JDBC 连接器（目前的版本是 ',
    step2desc2: '），并安装在 BI 工具运行的机器上',
    step3: '配置',
    step3full: '配置 TDengine JDBC 数据源',
    step31desc: '在打开的 Yonghong Desktop BI 工具中点击“添加数据源”，选择 SQL 数据源中的“GENERIC”类型。',
    step32desc:
      '点击“选择自定义驱动”，在“驱动管理”对话框中，点击“驱动列表”旁边的“+”，输入名称“MyTDengine”。然后点击“上传文件”按钮上传刚刚下载的 TDengine JDBC 连接器文件"taos-jdbcdriver-3.2.7-dist.jar"，并选择“com.taosdata.jdbc.rs.RestfulDriver”驱动，最后点击“确定”按钮完成驱动添加。',
    step33desc: '然后请复制下面的内容到“URL”字段',
    step34desc: '接着在“认证方式”中选择“无身份认证”。',
    step35desc: '在数据源的高级设置中，修改“Quote符号”的值为反引号“`”。',
    step36desc: '点击“测试连接”，弹出“测试成功”的对话框。点击“保存”按钮，输入“MyTDengine”来保存 TDengine 数据源。',
    step4: '创建数据集',
    step4full: '创建 TDengine 数据集',
    step41desc: '在 BI 工具中点击“添加数据集”，展开刚刚创建的数据源，并浏览 TDengine 中的超级表。',
    step42desc: '您可以将超级表的数据全部加载到 BI 工具中，也可以通过自定义 SQL 语句导入部分数据。',
    step43desc:
      '当勾选“数据库内计算”时，BI 工具将不再缓存 TDengine 的时序数据，并在处理查询时将 SQL 请求发送给 TDengine 直接处理。',
    step44desc:
      '当导入数据后，BI 工具会自动将数值类型设置为“度量”列，将文本类型设置为“维度”列。而在 TDengine 的超级表中，采用普通列作为数据的度量，采用标签列作为数据的维度，因此您可能需要在创建数据集时更改部分列的属性。TDengine 在支持标准 SQL 的基础之上，还提供了一系列满足时序业务场景需求的特色查询语法，例如数据切分查询、窗口切分查询等，具体参考 ',
    step44desc1: 'TDengine 特色查询功能介绍',
    step44desc2:
      '。通过使用这些特色查询，当 BI 工具将 SQL 查询发送到 TDengine 数据库时，可以大大提高数据访问速度，减少网络传输带宽。',
    step45desc:
      '在 BI 工具中，您可以创建“参数”并在 SQL 语句中使用，通过手动、定时的方式动态执行这些 SQL 语句，即可实现可视化报告的刷新效果。如下 SQL 语句   select _wstart ws, count(*) cnt from supertable where tbname=?{metric} and ts >= ?{from} and ts < ?{to} interval(?{interval})    可以从 TDengine 实时读取数据，其中',
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
  }
};
