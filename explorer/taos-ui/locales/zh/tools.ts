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
    step4full: '参数详解',
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
    step3: '常用场景',
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
    step4: '参数列表',
    step4desc: '以下为 taosdump 详细命令行参数列表：'
  },
  grafana: {
    desc: 'TDengine 能够与开源数据可视化系统 Grafana 集成来构件一个数据的监控和告警系统。整个过程无需进行任何代码开发，您就可以通过可视化你存储在 TDengine 的数据并展示在仪表盘里面。关于 TDengine 插件的使用您可以在Github中了解更多。',
    topdesc: 'TDengine 能够与开源数据可视化系统 ',
    topdesc1:
      ' 快速集成搭建数据监测报警系统，整个过程无需任何代码开发，TDengine 中数据表的内容可以在仪表盘(Dashboard)上进行可视化展现。关于 TDengine 插件的使用您可以在 ',
    topdesc3: ' 中了解更多。',
    step1: '前置条件',
    step1desc: '请确保 Grafana 已经安装，目前 TDengine 支持 Grafana 7.5 以上的版本。参考网址（',
    step1desc1: '）。',
    step2: '安装 TDengine 插件',
    step2link: 'https://www.taosdata.com/assets-download/grafana-plugin/tdengine-datasource.zip',
    step2desc:
      '使用 grafana-cli 命令行工具 进行插件安装，安装后需要重启 Grafana。对于 Linux/Mac，在终端中执行下面命令：',
    step2desc1:
      '对于 Windows，首先请确保插件安装目录存在（默认是 Grafana 安装目录下 data/plugins）, 然后在 Grafana 安装目录的 bin 目录下以管理员账号执行下面的命令：',
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
    step4link: 'https://docs.taosdata.com/third-party/visual/grafana/#%E5%88%9B%E5%BB%BA-dashboard',
    step4desc3: '。'
  },
  perspective: {
    desc: ' 是一款开源且强大的数据可视化库，由 Prospective.co 开发，运用 WebAssembly 和 Web Workers 技术，在 Web 应用中实现交互式实时数据分析，能在浏览器端提供高性能可视化能力。借助它，开发者可构建实时更新的仪表盘、图表等，用户能轻松与数据交互，按需求筛选、排序及挖掘数据。其灵活性高，适配多种数据格式与业务场景；速度快，处理大规模数据也能保障交互流畅；易用性佳，新手和专业开发者都能快速搭建可视化界面。',
    desc1:
      '在数据连接方面，Perspective 通过 TDengine 的 Python 连接器，完美支持 TDengine 数据源，可高效获取其中海量时序数据等各类数据，并提供展示复杂图表、深度统计分析和趋势预测等实时功能，助力用户洞察数据价值，为决策提供有力支持，是构建对实时数据可视化和分析要求高的应用的理想选择。',
    step1: '简介',
    step1full: '简介',
    step2: '安装驱动',
    step2full: '安装驱动',
    step2desc1: '安装 Python 3.10 及以上版本（如未安装，可参考 ',
    step2desc2: 'Python 安装',
    step2desc3: '）。',
    step2desc4: '安装最新版本的 TDengine Python 连接器，安装命令如下：',
    step3: '配置数据源',
    step3full: '配置数据源',
    step3desc:
      '启动一个 Perspective 的 Python 服务器，该服务器会从 TDengine 读取数据，并通过 Tornado WebSocket 将数据流式传输到一个 Perspective 表中。',
    step3desc1: '启动一个 Perspective 的 Python 服务器。',
    step3desc2: '建立与 TDengine 的连接。',
    step3desc3: '创建一个 Perspective 表(表结构需要与 TDengine 数据库中表的类型保持匹配)。',
    step3desc4: '调用 Tornado.PeriodicCallback 函数来启动定时任务，进而实现对 Perspective 表数据的更新，示例代码如下：',
    step3desc5: '查看源码',
    step4: '可视化展示',
    step4full: '可视化展示',
    step4desc:
      '编写 HTML 文件将 Perspective Viewer 嵌入到 HTML 页面中。它通过 WebSocket 连接到 Perspective 服务器，并根据图表配置显示实时数据。',
    step4desc1: '配置展示的图表以及数据分析的规则。',
    step4desc2: '与 Perspective Python 服务器建立 Websocket 连接。',
    step4desc3: '引入 Perspective 的 js 库，通过 WebSocket 连接到 Perspective 服务器，随后加载数据并进行展示。',
    step4desc4: '查看源码',
    step4desc5: '更多有关 Perspective 信息，请参考 ',
    step4desc6: '与 Perspective 集成',
    step4desc7: ' 。'
  },
  gds: {
    desc: 'Google Looker Studio可以快速访问 TDengine， 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘.',
    topdesc: '使用',
    topconnector: '第三方连接器',
    topdesc1:
      '，Google Looker Studio 可以快速访问 TDengine， 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘。整个过程不需要任何的代码编写过程。可以分享报表和仪表盘给不同的个人，团队以及全世界，还可以跟其他人员实时协作，另外在任何的网页里面嵌入您的报表。',
    topdesc2: '更多使用 Google Looker Studio 和 TDengine 集成可以参考',
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
    step213desc2: ' 可以获取的最大记录行数是 1000000。',
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
    step2: '安装驱动',
    step2desc: '获取 Seeq 数据地址配置。在 Linux 上，可以执行下面的命令获取：',
    step2desc11: '首先从 ',
    step2desc12: ' 可以下载最新的 TDengine Java 连接器（目前的版本是 ',
    step2desc13: '），然后复制下载的 JAR 文件到这个文件目录 the_directory_found_in_step_1/plugins/lib/ 。',
    step2desc2: '重启 Seeq 服务器。在 Linux 上，可以执行下面的命令：',
    step3: '配置数据源',
    step3full: '配置数据源',
    step3desc: '打开 Seeq，以 admin 用户登录，然后打开 Administration，点击“Add Data Source”',
    step3desc1: '对于连接器，请选择 SQL connector v2',
    step3desc2: '在“Additional Configuration”的输入框, 请复制和粘贴下面的内容：',
    step3desc3: '对于“QueryDefinitions”，请参考下面的例子来完成您自己的查询定义。',
    step4: '数据分析',
    step4full: '数据分析',
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
    step1: '前置条件',
    step1full: '前置条件',
    step1desc: '安装完成 Power BI Desktop 软件并可以运行（如未安装，请从 ',
    step1desc1: '官方地址',
    step1desc2: ' 下载最新的 Windows X64 版本）。',
    step2: '安装 ODBC',
    step2full: '安装 ODBC',
    step3: '配置 ODBC',
    step3full: '配置 ODBC',
    step4: '数据准备',
    step4full: '数据准备',
    step4desc: '打开 Power BI 并登录后，通过如下步骤添加数据源，“主页” -> “获取数据” -> “其他” -> “ODBC” -> “连接”。',
    step4desc1:
      '选择刚才创建的数据源名称，比如 “MyTDengine”，点击“确定”按钮。在弹出的“ODBC 驱动程序”对话框中，在左边的菜单里面选择“默认或自定义”，点击“连接”按钮，可以连接到配置好的数据源。在进入“导航器”后，可以浏览对应数据库的数据表并加载。',
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
    step5: '数据分析',
    step5full: '数据分析',
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
    step2full: '安装驱动',
    step2desc: '从 ',
    step2desc1: ' 下载最新的 TDengine JDBC 连接器（目前的版本是 ',
    step2desc2: '），并安装在 BI 工具运行的机器上。',
    step3: '配置数据源',
    step3full: '配置数据源',
    step31desc: '在打开的 Yonghong Desktop BI 工具中点击“添加数据源”，选择 SQL 数据源中的“GENERIC”类型。',
    step32desc:
      '点击“选择自定义驱动”，在“驱动管理”对话框中，点击“驱动列表”旁边的“+”，输入名称“MyTDengine”。然后点击“上传文件”按钮上传刚刚下载的 TDengine JDBC 连接器文件"taos-jdbcdriver-3.6.3-dist.jar"，并选择“com.taosdata.jdbc.ws.WebSocketDriver”驱动，最后点击“确定”按钮完成驱动添加。',
    step33desc: '然后请复制下面的内容到“URL”字段',
    step34desc: '接着在“认证方式”中选择“无身份认证”。',
    step35desc: '在数据源的高级设置中，修改“Quote符号”的值为反引号“`”。',
    step36desc: '点击“测试连接”，弹出“测试成功”的对话框。点击“保存”按钮，输入“MyTDengine”来保存 TDengine 数据源。',
    step4: '数据准备',
    step4full: '数据准备',
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
    step5full: '制作报告',
    step51desc: '在永洪 BI 工具中点击“制作报告”，创建画布。',
    step52desc: '拖动可视化组件到画布中，例如“表格组件”。',
    step53desc: '在“数据集”侧边栏中选择待绑定的数据集，将数据列中的“维度”和“度量”按需绑定到“表格组件”。',
    step54desc: '点击“保存”后，即可查看报告。',
    step55desc: '更多有关永洪 BI 工具的信息，请查询其 ',
    step55desc1: ' 帮助文档',
    step55desc2: ' 。'
  },
  superset: {
    desc: ' 是一个现代的企业级商业智能（BI）Web 应用程序，主要用于数据探索和可视化。它由 Apache 软件基金会支持，是一个开源项目，它拥有活跃的社区和丰富的生态系统。Superset 提供了直观的用户界面，使得创建、分享和可视化数据变得简单，同时支持多种数据源和丰富的可视化选项‌。',
    topdesc: ' 通过 TDengine 的 Python 连接器, ‌Superset‌ 可支持 TDengine 数据源并提供数据展现、分析等功能。',
    setup: '安装',
    end: '。',

    step1: '前置条件',
    step1full: '前置条件',
    step11desc: 'Apache Superset v2.1.0 或以上版本已安装，安装参考 ',
    step11desc1: '官方文档',
    step12desc: 'Python 连接器 taospy 2.7.18 或以上版本已 ',
    step13desc: 'Python 连接器（WebSocket）taos-ws-py 0.3.9 或以上版本已 ',

    step2: '配置数据源',
    step2full: '配置数据源',
    step21desc: '进入新建数据库连接页“Superset”->“Setting”->“Database Connections”->“+DATABASE”。',
    step22desc: '选择其它数据库连接，“SUPPORTED DATABASES”下拉列表中选择最后一项“Other”。',
    step23desc: '在“DISPLAY NAME”中填写连接名称，任意填写即可。',
    step24desc: '“SQLALCHEMY URI”中填写连接字符串，请填写以下内容：',
    step25desc: '点击“TEST CONNECTION”测试连接是否成功，测试通过后点击“CONNECT”按钮保存配置。',

    step3: '数据准备',
    step3full: '数据准备',
    step3desc: 'TDengine 数据源与其它数据源使用上无差别，这里介绍下数据集准备：',
    step3desc1: '在 Superset Web 页面上点击右上角 “+” 号按钮，选择 “SQL query”, 进入查询页面。 ',
    step3desc2: '在查询页面的左上角 “DATABASE” 下拉列表中选择前面已创建好的数据源。',
    step3desc3: '在 “SCHEMA” 下拉列表，选择要操作的数据库名（系统库不显示）。',
    step3desc4:
      '在 “SEE TABLE SCHEMA” 下拉列表，选择要操作的超级表名或普通表名（子表不显示）后， 会在下方显示选定表的 SCHEMA 信息。',
    step3desc5: '在上方的 SQL 编辑器区域可输入符合 TDengine 语法的 SQL 语句后，点击 “Run” 按钮执行。',
    step3desc6: '在上方的 SQL 编辑器区域内点击 “SAVE” 按钮旁边的 “v” 按钮后，选择 “Save dataset” 按钮进行保存数据集。',

    step4: '数据分析',
    step4full: '数据分析',
    step4desc1: '在 Superset Web 页面上点击 “Datasets” 菜单，打开 “Datasets” 页面。',
    step4desc2: '在 “Datasets” 页面上点击刚才保存的 Datasets, 打开 “Chart” 页面。',
    step4desc3: '在 “Chart” 页面的左侧第二列选择横纵坐标的字段。',
    step4desc4: '选择好后点 “UPDATE CHART”，图表就生成好了。',
    step4desc5: '更多有关 Superset 的使用，请查询其 ',
    step4desc6: ' Superset 文档',
    step4desc7: '。'
  },
  tableau: {
    desc: ' 是一款知名的商业智能工具，它支持多种数据源，可方便地连接、导入和整合数据。并且可以通过直观的操作界面，让用户创建丰富多样的可视化图表，并具备强大的分析和筛选功能，为数据决策提供有力支持。用户可通过 TDengine ODBC Connector 将标签数据、原始时序数据或者经时间聚合后的时序数据从 TDengine 导入到 Tableau，用以制作报表或仪表盘，且整个过程无需编写任何代码。',
    step1: '前置条件',
    step1full: '前置条件',
    step1desc: 'Tableau 桌面版完成安装并可以运行（如未安装，请从 ',
    step1desc1: '官方地址',
    step1desc2: ' 下载最新的 Windows X64 版本）。',
    step2: '安装 ODBC',
    step2full: '安装 ODBC',
    step3: '配置 ODBC',
    step3full: '配置 ODBC',
    step23desc1: '填写需要连接的数据库，必填，比如 “test”',
    step4: '导入数据',
    step4full: '导入数据',
    step4desc: '打开 Tableau 之后在其连接页面中搜索 “ODBC”，并选择 “其他数据库 (ODBC)”。',
    step4desc1:
      '点击 DSN 单选框，选择刚才创建的数据源名称，比如 “MyTDengine”，接着点击 ”连接“ 按钮。待连接成功后，删除字符串附加部分的内容，最后点击 ”登录“ 按钮即可。',
    step4desc2: '在工作簿页面中，选择已连接的数据源，并点击数据库的下拉列表，选择需要进行数据分析的数据库。',
    step4desc3:
      '点击表选项中的 ”查找“ 按钮，即可将该数据库下的所有表显示出来。拖动需要分析的表到右侧区域，即可显示出表结构。',
    step4desc4: '点击下方的 ”立即更新“ 按钮，即可将表中的数据展示出来。',
    step5: '数据分析',
    step5full: '数据分析',
    step5desc1: '在工作簿页面中点击 “工作表”，弹出 “数据分析” 页面。',
    step5desc2: '在 “数据分析” 侧边栏中会展示出表的所有字段。',
    step5desc3: '将字段按照 “维度” 和 “度量” 拖动到右侧列行的 “表格组件“ 上，下方即可展示出图表。',
    step5desc4: '更多有关 Tableau 工具的信息，请查询其 ',
    step5desc5: ' Tableau 文档',
    step5desc6: ' 。'
  },
  excel: {
    desc: ' 是微软公司（Microsoft）开发的一款功能强大且应用广泛的电子表格软件。通过配置使用 ODBC 连接器，Excel 可以快速访问 TDengine 的数据。用户可以将标签数据、原始时序数据或按时间聚合后的时序数据从 TDengine 导入到 Excel，用以制作报表整个过程不需要任何代码编写过程。',
    step1: '前置条件',
    step1full: '前置条件',
    step1desc: 'Excel 完成安装并运行, 如未安装，请下载并安装, 具体操作请参考 ',
    step1desc1: 'Microsoft 官方文档',
    step1desc2: '。',
    step2: '安装 ODBC',
    step2full: '安装 ODBC',
    step3: '配置 ODBC',
    step3full: '配置 ODBC',
    step4: '导入数据',
    step4full: '导入数据',
    step4desc: '在 Windows 系统环境下启动 Excel，之后选择 “数据” -> “获取数据” -> “自其他源” -> “从ODBC”。',
    step4desc1: '在弹出窗口的 “数据源名称(DSN)” 下拉列表中选择需要连接的数据源后，点击 “确定” 按钮。',
    step4desc2: '在弹出的 “ODBC 驱动程序” 窗口中选择 “默认自定义” 菜单后点 “连接” 按钮。',
    step4desc3: '在弹出的 “导航器” 对话框中，选择要加载的库表, 并点击 “加载” 完成数据加载。',
    step5: '分析数据',
    step5full: '数据分析',
    step5desc1: '在已导入数据的 Excel 工作表里，选中所需的数据区域。',
    step5desc2: '在 Excel 菜单栏中找到并点击【插入】选项卡，选择需要的图表类型。',
    step5desc3: 'Excel 会立即在工作表中生成一个基于所选数据的图表。',
    step5desc4: '更多有关 Execl 的使用，请查询其 ',
    step5desc5: ' Excel 文档',
    step5desc6: ' 。'
  },
  finebi: {
    desc: '帆软是一家专注于商业智能与数据分析领域的科技企业，凭借自主研发的 FineBI 和 FineReport 两款核心产品在行业内占据重要地位。帆软的 BI 工具广泛应用于各类企业，帮助用户实现数据的可视化分析、报表生成和数据决策支持。',
    desc1:
      '通过 TDengine Java connector 连接器，FineBI 可以快速访问 TDengine 的数据。用户可以在 FineBI 中直接连接 TDengine 数据库，获取时序数据进行分析并制作可视化报表，整个过程不需要任何代码编写过程。',

    step1: '前置条件',
    step1full: '前置条件',
    step11desc: 'FineBI 已经安装（如果未安装，请从 ',
    step11desc1: '官方地址',
    step11desc2: ' 下载）。',
    step12desc: '下载 fine_conf_entity 插件用于支持允许添加JDBC驱动，',
    step12desc1: '下载地址',
    step12desc2: '。',

    step2: '安装驱动',
    step2full: '安装驱动',
    step2desc: '从 ',
    step2desc1: ' 下载 TDengine JDBC 连接器文件 taos-jdbcdriver-3.4.0-dist.jar 或以上版本。',

    step3: '配置数据源',
    step3full: '配置数据源',
    step31desc: '在 FineBI 服务端 db.script 配置文件中，找到 SystemConfig.driverUpload 配置项并将其修改为 true。',
    step31desc1: 'Liunx/Mac 系统：配置文件路径是 /usr/local/FineBI6.1/webapps/webroot/WEB-INF/embed/finedb/db.script。',
    step31desc2: 'Windows 系统：配置文件路径是安装目录下 webapps/webroot/WEB-INF/embed/finedb/db.script。',
    step32desc: '启动 FineBI 服务，在浏览器中输入 http://ip:37799/webroot/decision, 其中 ip 是 FineBI 服务端 ip 地址。',
    step33desc:
      '打开 FineBI Web 页面登录后，点击【管理系统】->【插件管理】，在右侧的【应用商城】中点击【从本地安装】选择已下载的 fine_conf_entity 插件进行安装。',
    step34desc:
      '点击【管理系统】->【数据连接】->【数据连接管理】，在右侧页面中点击【驱动管理】按钮打开配置页面，点击【新建驱动】按钮并在弹出窗口中输入名称（比如 tdengine-websocket），进行 JDBC 驱动配置。',
    step35desc:
      '在驱动配置页面中点击【上传文件】按钮，选择已下载的 TDengine Java Connector（比如 taos-jdbcdriver-3.4.0-dist.jar）进行上传，上传完成后在【驱动】的下拉列表中选择 com.taosdata.jdbc.ws.WebSocketDriver，并点击【保存】。',
    step36desc:
      '在 “数据连接管理” 页面中，点击【新建数据连接】按钮，随后点击 “其他” ，在右侧页面中点击 “其他JDBC” 进行连接配置。',
    step37desc:
      '在配置页面，先输入数据连接名称，在【驱动】选项中选择 “自定义”，并从下拉列表里选取已配置的驱动“com.taosdata.jdbc.ws.WebSocketDriver”，“数据连接 URL”填写下面内容：',
    step37desc1: '说明：参数 fineBIDialect=mysql，表示使用 MySQL 数据库方言规则。',
    step38desc:
      '完成上述设置后，点击页面右上角的【测试连接】进行连接测试，待验证成功后，点击【保存】，即可完成整个配置流程。',

    step4: '导入数据',
    step4full: '导入数据',
    step41desc:
      '点击【公共数据】在右侧页面中点击【新建文件夹】即可创建一个文件夹（比如 TDengine）， 接着在文件夹的右侧点击【+】按钮，可创建 “数据库表” 数据集或 “SQL数据集”。',
    step41desc1:
      '点击 “数据库表”，打开数据库选表页面，在左侧 “数据连接” 中选择已创建的连接，则在右侧会显示当前连接的数据库中的所有表，选择需要加载的表（比如 meters），点击【确定】即可显示 meters 表中的数据。',
    step41desc2:
      '点击 “SQL数据集”，打开 SQL 数据集的配置页面，首先输入表名（用于在 FineBI 页面显示），接着在 “数据来自数据连接” 下拉列表中选择已创建的连接， 之后输入 SQL 语句并点击预览即可看到查询结果，最后点击【确定】SQL 数据集即可创建成功。',

    step5: '数据分析',
    step5full: '数据分析',
    step51desc:
      '点击【公共数据】在右侧页面中点击【新建文件夹】即可创建一个文件夹（比如 TDengine）， 接着在文件夹的右侧点击【+】按钮，可创建 “数据库表” 数据集或 “SQL数据集”。',
    step51desc1: '在分析主题页面选择数据集（比如 meters）后点击【确定】按钮，即可完成数据集关联。',
    step51desc2: '点击分析主题页面下方的【组件】标签，打开图表配置页面, 拖动字段到横轴或纵轴即可展示出图表。'
  },
  ssrs: {
    desc: 'SSRS 是微软旗下一个强大的报表制作分发产品。',
    brief:
      '(SSRS) 作为微软 SQL Server 数据库平台内置组件，为企业级报表制作、浏览及管理提供强大支持。与微软旗下另一可制作灵活多样报表工具 Power BI 相比，SSRS 更适合于制作传统固定格式报表。',
    endmark: '。',

    step1: '前置条件',
    step1full: '前置条件',
    step1pre1: '本示例需准备一台服务器两台客户端，搭建 SSRS 示例环境，准备如下：',

    step11: 'SSRS 服务器',
    step11item1: '要求 Windows 操作系统。',
    step11item2: '安装 TDengine 3.3.3.0 或以上 Windows 客户端版（默认安装 TDengine ODBC 驱动）。',
    step11item3: '安装 Microsoft SQL Server 2022 且数据库服务正常运行，',
    step11item4: '安装 Microsoft SQL Server 2022 Reporting Service 且报表服务正常运行，',
    step11item5: '配置 Microsoft SQL Server 2022 Reporting Service 使用 IP 地址提供对外服务并记录对外服务 URLs。',
    step11link1: '下载安装',

    step12: '报表制作客户端',
    step12item1: '要求 Windows 操作系统。',
    step12item2: '安装 TDengine 3.3.3.0 或以上 Windows 客户端版（默认安装 TDengine ODBC 驱动）。',
    step12item3: '安装 Microsoft Report Builder（32 位），提供报表开发服务，',
    step12item4: '配置 Microsoft Report Builder 上报报表服务器地址，填写前面记录的对外服务 URLs。',
    step12link1: '下载安装',

    step13: '办公客户端',
    step13item1: '操作系统不限。',
    step13item2: '网络要求可连接至 SSRS 服务器。',
    step13item3: '安装任意一款浏览器软件。',

    step2: '配置数据源',
    step2full: '配置数据源',
    step2pre1: 'SSRS 通过 ODBC 访问 TDengine 数据源，配置步骤如下：',

    step21: 'SSRS 服务器配置 ODBC 数据源。',
    step21pre1:
      '打开 ODBC 数据源管理器（64 位），选择“System DSN”->“Add...”->“TDengine”->“Finish”，弹出配置窗口中按如下填写：',
    step21item1: '* DSN：填写“TDengine”。',
    step21item2: '* Connect type：选择“WebSocket”。',
    step21item3: '* URL：请填写以下内容，内容是网站根据您登录账号自动生成带 TOKEN 授权的 URL。',
    step21item4: '* User/Password： 不用填写。',
    step21end1: '点击“Test Connection”，连接成功表示配置正确，点击“OK”保存配置。',

    step22: '报表制作 Window 客户端配置 ODBC 数据源。',
    step22pre1:
      '打开 ODBC 数据源管理器（32 位），选择“System DSN”->“Add...”->“TDengine”->“Finish”，弹出 ODBC 数据源配置窗口，内容填写请与上一步相同。',
    step22end1: '点击“Test Connection”，连接成功表示配置正确，点击“OK”保存配置。',

    step23: 'Report Builder 创建数据源连接。',
    step23pre1:
      '启动 Report Builder，左侧区域内“Data Source”项上点右键，点击“Add Data Source...”菜单，弹出窗口填写内容如下：',
    step23item1: '* Name：填写数据源名称。',
    step23item2: '* 数据源方式：选择第二项“Use a connection embedded in my report”。',
    step23item3: '* Select Connection type：选择“ODBC”数据源。',
    step23item4: '* Connection string：填写“Dsn=TDengine”。',
    step23end1: '点击“Test Connection”，连接成功表示配置正确，点击“OK”保存配置。',

    step3: '数据分析',
    step3full: '数据分析',

    step31: '场景介绍',
    step31pre1:
      '某小区有 500 台智能电表，数据存储在 TDengine 数据库中，电力公司要求数据运营部门制作一张报表，能够分页浏览此小区每台智能电表最后一次上报电压及电流值，同时要求报表可在公司内任一台办公电脑上查看。',
    step31end1:
      '开发人员使用微软提供的 SSRS 报表服务完成此项工作，使用 Report Builder 制作好报表，上传至报表服务器后供相关人员浏览。',

    step32: '数据准备',
    step32pre1:
      '创建一张超级表，500 子表，每子表代表一台智能电表，生成电压数据在 198 ~ 235 内波动，电流在 10A ~ 30A 内波动。',

    step33: '制作报表',
    step331: '打开 Report Builder 开始制作报表。',
    step332: '创建新数据集。',
    step332pre1: '左侧区域内“DataSource”->“DataSource1”->“Add Dataset...”，弹出窗口中填写内容如下：',
    step332item1: '* Name：填写数据集名称。',
    step332item2: '* 数据集方式：选择第二项“Use a dataset embedded im my report”。',
    step332item3: '* Data source：选择前面创建好的“DataSource1”。',
    step332item4: '* Query type：选择“text”类型查询，填写如下查询分析 SQL：',

    step333: '制作报表页面。',
    step333pre1:
      '菜单“Insert”->“Table”->“Insert Table”，插入空表格，用鼠标把左侧“DataSet1”中数据列用拖到右侧报表制作区域内放置到自己想要展示的列上。',

    step334: '预览，点击菜单“Home”->“Run”按钮，预览报表效果。',
    step335: '退出预览，点击工具栏左侧第一个图标“Design”关闭预览，回到设计界面继续设计。',

    step34: '发送报表',
    step341: '保存报表到服务器上，点击“File”菜单->“Save”。',
    step342: '点击“File”菜单->“Publish Report Parts” 开始上传报表使用的数据源。',
    step343: '弹出窗口中点击第一项“Publish all report parts with default settings”，完成上传。',

    step35: '浏览报表',
    step35pre1: '报表保存至服务器后，报表即已被共享，可在任意客户端通过浏览器访问报表。',
    step351: '查看报表浏览地址。',
    step351pre1: '报表浏览地址在 SSRS 服务器配置窗口->“Web Service URL”->“Report Server Web Service URLs”->“URLs”中。',
    step352: '输入访问授权。',
    step352pre1:
      '客户端第一次访问报表数据时，会弹出授权窗口要求登录，输入报表服务器操作系统登录账号即可，登录后打开报表浏览页面。',
    step353: '分页浏览报表。',
    step353pre1: '点击“meters”，会分页展示小区内所有智能电表最新采集数据。',

    step36: '管理报表',
    step36pre1: '对 SSRS 服务器上报表进行管理，可参考',
    step36link1: '微软官网文档',

    docend:
      '以上流程，我们使用了 SSRS 开发了基于 TDengine 数据源的一个简单报表制作、分发、浏览系统，更多丰富的报表还有待您的进一步开发。'
  },
  nodered: {
    desc: 'Node-RED 是一个强大的 IoT 领域低代码可视化编程工具。',
    brief1:
      '是由 IBM 开发的基于 Node.js 的开源可视化编程工具，通过图形化界面组装连接各种节点，实现物联网设备、API 及在线服务的连接。同时支持多协议、跨平台，社区活跃，适用于智能家居、工业自动化等场景的事件驱动应用开发，其主要特点是低代码、可视化。',
    brief2:
      'TDengine 与 Node-RED 深度融合为工业 IoT 场景提供全栈式解决方案。通过 Node-RED 的 MQTT/OPC UA/Modbus 等协议节点，实现 PLC、传感器等设备毫秒级数据采集。同时 Node-RED 中基于 TDengine 的毫秒级实时查询结果，触发继电器动作、阀门开合等物理控制，实现更实时的联动控制。',
    brief3: 'node-red-node-tdengine 是 TDengine 为 Node-RED 开发的官方插件，由两个节点组成：',
    briefitem1: 'tdengine-operator：提供 SQL 语句执行能力，可实现数据写入/查询/元数据管理等功能。',
    briefitem2: 'tdengine-consumer：提供数据订阅消费能力，可实现从指定订阅服务器消费指定 TOPIC 的功能。',

    endmark: '。',

    step1: '前置条件',
    step1pre1: '准备以下环境：',

    step1item1: 'Node-RED 3.0.0 及以上版本，',
    step1item2: 'Node.js 语言连接器 3.1.8 及以上版本，可从',
    step1item3: 'node-red-node-tdengine 插件最新版本，可从',
    step11link1: 'Node-RED 安装',
    step12link1: 'npmjs.com 安装',
    step13link1: 'npmjs.com 安装',

    step2: '配置数据源',
    step2pre1: '插件数据源在节点属性中配置，通过 Node.js 语言连接器连接数据源，配置步骤如下',

    step21: '启动 Node-RED 服务，使用浏览器进入 Node-RED 主页。',
    step22: '画布左侧在节点选择区域选择 tdengine-operator 或 tdengine-consumer 节点拖至画布。',
    step23: '双击画布中选中节点，弹出属性设置窗口，在“数据库连接串(URI)”中填写下内容：',
    step24: '配置完成后，点击右上角“部署”按钮，节点状态为绿色，表示数据源配置正确且连接正常。',

    step3: '使用示例',

    step31: '场景准备',

    step311: '场景介绍',
    step311pre1:
      '某生产车间有多台智能电表，电表每一秒产生一条数据，数据准备存储在 TDengine 数据库中，要求实时输出每分钟各智能电表平均电流、电压及用电量，同时要对电流 > 25A 或电压 > 230V 负载过大设备进行报警。',
    step311pre2: '我们使用 Node-RED + TDengine 来实现需求：',
    step311item1: '使用 Inject + function 节点模拟设备产生数据。',
    step311item2: 'tdengine-operator 节点负责写入数据。',
    step311item3: '实时统计使用 tdengine-operator 节点查询功能。',
    step311item4: '过载报警使用 tdengine-consumer 订阅功能。',
    step311sec1: '假设：',
    step311secitem1: 'TDengine：已拥有云服务账号',
    step311secitem2: '模拟设备：三台（d0，d1，d2）。',

    step312: '数据建模',
    step312pre1: '使用数据库管理工具 taos-CLI，为采集数据进行手工建模，采用一张设备一张表建模思路：',
    step312item1: '创建超级表：meters。 ',
    step312item2: '创建子表：d0，d1，d2。',
    step312pre2: '建模 SQL 如下：',

    step32: '业务处理',

    step321: '数据采集',
    step321pre1:
      '示例使用生成随机数方式模拟真实设备生产数据，tdengine-operator 节点配置 TDengine 数据源连接信息，并把数据写入 TDengine，同时使用 debug 节点监控写入成功数据量并展示于界面。',
    step321pre2: '操作步骤如下：',

    step3211: '- 增加写入节点',
    step3211item1: '在节点选择区域选择 tdengine-operator 节点，拖动至画布中。',
    step3211item2: '双击节点打开属性设置，名称填写“td-writer”，数据库项右侧点击“+”号图标。',
    step3211item3: '弹出窗口中，名称填写“db-server”，连接类型选择使用字符串连接，输入：',
    step3211item4: '点击“添加”并返回。',

    step3212: '- 模拟设备产生数据',
    step3212item1: '在节点选择区域选择 function 节点，拖动至画布 td-writer 节点前。',
    step3212item2: '双击节点打开属性设置，名称填写“write d0”，下面选项卡选择“运行函数”，填写如下内容后保存并返回画布。',
    step3212item3: '在节点选择区域选择 inject 节点，拖动至画布“write d0”节点前。',
    step3212item4:
      '双击节点打开属性设置，名称填写‘inject1’，下拉列表中选择“周期性执行”，周期选择每隔 1 秒，保存并返回画布。',
    step3212item5: '重复 1 ~ 4 步完成另外两台设备 (d1，d2) 流程。',

    step3213: '- 增加信息输出',
    step3213item1: '在节点选择区域选择 debug 节点，拖动至画布 td-writer 节点后。',
    step3213item2: '双击节点打开属性设置，勾选“节点状态”，下拉列表中选择消息数量。',

    step321secpre1: '以上节点增加完成后，依次把上面节点按顺序连接起来，形成一条流水线，数据采集流程制作完成。',
    step321secpre2: '点击右上角“部署”按钮发布修改内容，运行成功后可以看到：',
    step321secitem1: 'td-writer 节点状态变成绿色，表示流程工作正常。',
    step321secitem2: 'debug 节点下的数字表示成功采集次数。',
    step321secpre3: '向下游节点输出写入成功结果，若失败抛出异常：',

    step322: '数据查询',
    step322pre1:
      '查询流程由三个节点（inject/tdengine-operator/debug）组成，完成每分钟实时输出各智能电表平均电流、电压及用电量需求。由 inject 节点完成触发查询请求，结果输出至下游 debug 节点中，节点上显示查询执行成功数量。',
    step322pre2: '操作步骤如下：',
    step322item1: '将 inject 节点拖动至画布中，双击节点设置属性，名称填“query”, msg.topic 填写并保存并返回画布：',
    step322item2:
      '将 tdengine-operator 节点拖动至画布中，双击节点设置属性，“数据库”选择前面已创建好的数据源“db-server”，保存并返回画布。',
    step322item3:
      '将 debug 节点拖动至画布中，双击节点设置属性，勾选“节点状态”，下拉列表中选择“消息数量”，保存并返回画布。',
    step322item4: '依次把以上节点按顺序连接起来，点击“部署”按钮发布修改内容。',
    step322pre3: '流程启动成功后：',
    step322secitem1: 'td-reader 节点状态变成“绿色”，表示流程工作正常。',
    step322secitem2: 'debug 节点显示查询成功次数。',
    step322pre4: '向下游节点输出查询结果，若失败抛出异常：',

    step323: '数据订阅',
    step323pre1:
      '数据订阅流程由两个节点（tdengine-consumer/debug）组成，实现过载告警。debug 节点展示向下游节点推送数据次数，生产中可把 debug 节点更换为处理订阅数据的功能节点。',
    step323pre2: '操作步骤如下：',
    step323item1: '使用 taos-CLI 手工创建订阅主题”topic_overload“,  SQL 如下：',
    step323item2: 'tdengine-consumer 节点拖动至画布中，双击节点设置属性，填写如下内容后保存并返回画布。',
    step323item2opt1: '名称：td-consumer',
    step323item2opt2: '订阅服务器(URI)：',
    step323item2opt3: '用户名：不填写',
    step323item2opt4: '密码：不填写',
    step323item2opt5: '订阅主题：topic_overload',
    step323item2opt6: '消费开始位置：latest',
    step323item2opt7: '其它项保持默认',
    step323item3:
      '将 debug 节点拖动至画布中，双击节点设置属性，勾选“节点状态”，下拉列表中选择“消息数量”，保存并返回画布。',
    step323item4: '依次把以上节点按顺序连接起来，点击”部署“按钮发布修改内容。',
    step323pre3: '流程启动成功后可看到 td-consumer 节点状态变成“绿色”表示流程工作正常，debug 节点数字表示消费次数。',
    step323pre4: '向下游节点推送的过载设备警告信息，若失败抛出异常：',

    step33: '异常捕获',
    step33pre1: '在数据采集、查询及订阅流程中，发生错误均按抛出异常机制来处理，需建立异常监控流程：',
    step33item1: '将 catch 节点拖动至画布中。',
    step33item2: '双击节点打开属性设置，名称填写“catch all except”，捕获范围选择“所有节点”。',
    step33item3: '将 debug 节点拖动至画布 catch all except 节点后。',
    step33item4: '双击节点设置属性，勾选“节点状态”，下拉列表中选择“消息数量”，保存并返回画布。',
    step33item5: '依次把以上节点按顺序连接起来，点击”部署“按钮发布修改内容。',
    step33pre2: '流程启动后监控所有节点异常产生：',
    step33secitem1: 'debug 节点展示发生异常数量。',
    step33secitem2: '可通过 NODE-RED 日志系统查看异常详细。',

    step4: '总结',
    step4pre1: '本文通过工业监控场景展示了：',
    step4item1: 'Node-RED 与 TDengine 的三种集成模式：',
    step4item1opt1: '数据采集（tdengine-operator 写入）',
    step4item1opt2: '实时查询（tdengine-operator 查询）',
    step4item1opt3: '事件驱动（tdengine-consumer 订阅）',
    step4item2: '完整的错误处理机制',
    step4item3: '生产环境部署参考方案',

    docend: '本文侧重从示例角度介绍，全部功能文档请在 Node-RED 节点在线文档中获取。'
  }
};
