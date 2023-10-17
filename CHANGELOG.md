# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.7] - 2023-10-10

**Full Changelog**: [v1.0.0...v1.2.7](https://github.com/taosdata/taosx/compare/v1.0.0...v1.2.7)

### Bug Fixes

- *docs*: Update docs-en ([9a27f22](9a27f227c997dee46c543211b3bb04024fb8f32c))
- *docs*: Use docs-en/ instead of docs-en/index ([1f9b94e](1f9b94eeab77145c17f8632b6a71cc5c2c75a4ed))
- *pi*: Fix pi submit no response ([1de57e0](1de57e00d2393878eac930442b12637ad156be93))
- *replication*: Raise error when dsn is invalid ([66f1154](66f1154d3ed3d99583122880da01f46842f8c831))
- *replication*: Fix error message even success ([e33f383](e33f383496602281bd083db59d62d36583ce8e7f))
- *tmq*: Fix data source dsn broken with cloud dsn ([94adc58](94adc58883a524c0a95c5eb3c996b1942acd9325))

- Hidden some tips ([50d9dd9](50d9dd9b198df1c5c0cf4bd191a24142b060fb92))
- Csv完整功能，自定义header ([2e5993a](2e5993a768ddec5be1fdfae8fafa3e5d006499e5))
- 调整create at 样式 ([30b0f9b](30b0f9ba60d7ccfc4a61f8545f476b2c2f584d18))
- 修改csv的bug ([9110bcf](9110bcfb3ef562f48a036998d4cf0180b12d122c))
- 处理0的值 ([6f1e1f7](6f1e1f718f4dd0310c0f977a220067161d666069))
- Kafka keep 参数 ([f9ffc0c](f9ffc0c688841f3376fdf3c35dd82c28206c3204))
- Csv的布局，windwos路径验证 ([ffc936e](ffc936e8ea13847cd8e92cf971ef965e35f2c947))
- Csv using and name ([d835683](d835683d2cebf5757d568fcf13fab6ff4aa08a7d))
- Fix some ui ([ef1af15](ef1af15441fdfc46768d84d2db126177593d654e))
- Dashboard webp ([ed125dc](ed125dcbb08a66b57d9f58c319563f31acdc9c78))
- Optimize tips and agent ui ([b9ba901](b9ba901c2ff199e42afde58c09f12d808a1226bd))
- Ui的补充修改 ([357aa8f](357aa8fc992597eb3b17e2e07d6419529b646237))
- Hidden linux code ([e46aa5a](e46aa5a8763330324bb65fb2ffb86112540e1b06))
- Optimize csv ([806a51d](806a51d2f06e1dbe7de81679742ff96afe87d6a9))
- Conflict ([6e2d71b](6e2d71ba4871d474de532051fa81baf60c5021f0))
- Add csv normal table ([796a0a5](796a0a5e6f04d1fd6aa445211e62c09546dc336c))
- 数据库名必填 ([f25c741](f25c74104ee2fe0f01edb3045edbf30b6c0398af))
- Fix two columns in csv and optimize target db position ([8d9994b](8d9994bbee0c5cd929d84a7112f9c4e1446b30bd))
- Fix file type and sort column ([0390ef1](0390ef161822310d66352f7fe0710b24ce34b197))
- Influxdb提交参数 ([170f3e9](170f3e9152fbd4ecefa60233fd1608f91d86a46a))
- Influxdb提交参数 ([eb1a347](eb1a34741de968f9e71111fa22b2005236b1155f))
- Change the task api function name ([43c4dd0](43c4dd03fd91183c87c592001970c8f79076e59b))
- Add explorer search ([4cf7152](4cf71525baa1ef787e0e2d773552d76d7ea01e04))
- Fix csv tip and explorer grid ([7fb308b](7fb308b63235f50ffe7388a4c9f6a4b2d46ec7d9))
- Fix csv directory ([4cb8fcb](4cb8fcb292e934bdc11b04140ba05ff1165834a9))
- Copy a task ([dd64791](dd647917c40f5b2ed8367b1ffc653a01285cdf1d))
- Fix merge ([1a45bb5](1a45bb5111157396fc9d64f507649c6d75098c5b))
- Add opentsdb ([7833f87](7833f8786b98c6415bfc11696a0246b795e107ff))
- Fix copy task when status is running ([7ac6b7b](7ac6b7b28b92a97a46c2a7818cb1147826b8b2bc))
- 创建数据库样式 ([9971266](99712666e40128386bfab2de9e62e1835a73d423))
- Oem ([870f421](870f421550329720686b8311efebdc1df9913743))
- Sql 编辑器优化 ([7084d4e](7084d4e3adeb9ff1e3073296eaee65f7251b74ff))
- 更改开始/结束时间交互 ([24c37e3](24c37e3c6a5b371944bfa7fa8433ed7a95fbf973))
- Add metrics ([204bc31](204bc31cab75711f07bf1203d27182977e7ef053))
- Fix bigint ([0d2a14b](0d2a14b4c154630a2dbfb3edfb9cebc5b404dd1f))
- Fix /rest/sql bigint ([381df7a](381df7aba76a4d4624346ee107220df6d675a060))
- 更改 offset 为 soffset ([a92ac71](a92ac71fccb502493b80209da315caccc5814407))
- 调整 limi 位置 ([2e0a206](2e0a206e9226ebdc5cd9d67d8669e0ce08eab627))
- Add empty metrics tip ([9551e03](9551e0317f529598f169965014b1cdf307dfea4b))
- 修改校验错误 ([d0bdc18](d0bdc18f101246a36c24edfc85c25109015c2319))
- Range 增加引号 ([450f46b](450f46b15d6214796675f7d91c868473db90869c))
- Range1 增加引号 ([3dd6908](3dd6908581441b55fffaba1a6a01e973b1d6037c))
- Filter db types that do not require agent ([799f87c](799f87c821b8d07131dc2a788e3a5296cfc6eea5))
- Typos in src/lang/en.js ([1ec54d5](1ec54d5ab3988434dc7511b55ea98baffc309fa2))
- Fix between and is null ([71d730d](71d730d478cb6bd3843074c125953748075b8dee))
- Fix is null ([b52e3d7](b52e3d7a9eafcca17e015c899a7aff098766f50b))
- 修改 docs url ([606ad76](606ad7636af37d7062621c80a1e3269ef50db09e))
- 调整 kafka 中英文 ([09ef411](09ef4119cb2e845794459f651f1d684467e57984))
- 修改文档路径 ([f07b478](f07b478a990fa89677be824a86b028b6151cc327))
- Update docs-en ([531f618](531f618a248755e363a57dcd3d33676a78f00705))
- Update docs-en ([e8405d9](e8405d9ff6ac1df0cd7c19a99ca8cf912a4b3742))
- Update docs-en ([bd1e771](bd1e771493079039b2811d3addd0aac9069f15cd))
- Translate chinese to english ([6e5b285](6e5b2850d681850369d4189bd2c1afcd02977f1d))
- Change sample ([1db9e96](1db9e9658a840f30fb323d72aa43e3a3393234f3))
- Fix agent deacription ([8bb70cb](8bb70cba292f457b3c5e507cf964ce8570b1485d))
- Fix cookie ([5dda154](5dda1540505ff8701da9f17885b20cdf396703a7))
- Fix messagebox ([f5032c0](f5032c090fe8ed80555b16b7765bc5399aae8de7))
- Fix create db dialog ([0af69dd](0af69dd7ac75c85f430e9a4f2a2bc0a0deefbb6a))
- Ignore empty active code. #[TD-26351](https://jira.taosdata.com:18080/browse/TD-26351) ([dcdc56b](dcdc56b47dcf0534bcf0cd3ae6ea58490f1446b2))
- Ignore empty active code. #[TD-26351](https://jira.taosdata.com:18080/browse/TD-26351) ([9797330](9797330a458f635f7982ea3d3c17bf1c7dbf2163))
- Add template csv file ([c8acb3e](c8acb3e0789e9c6ca79a32d8b4e00863bf7e7de9))
- Csv template file ([15994a0](15994a046b5ad2c77795010ec2ad59aaf4ffe1dd))
- Modify the comparison of result ([ac57e5e](ac57e5ea0bbebe84b334ad26a1ce3cb5ff6c91f8))
- Modify the comparison of result ([d56920a](d56920ab8653a3d55571587aede61212c27693ab))
- Modify the comparison of result. #[TD-26351](https://jira.taosdata.com:18080/browse/TD-26351) ([ccae517](ccae517c1f5eb62fcb1f23c49c277d386d828d75))
- Modify the comparison of result ([516e1f1](516e1f1d771a55d640c1838e0d7d0c5367d6625b))
- Fix no english ([2975503](29755038867555daa97b5ac46b8db6ce823bb4ec))
- Download template opc file ([1974a44](1974a44cfb245319a5fd64ee0ac7d45cafd54439))
- Fix mqtt zone ([05ce13b](05ce13b302ea9019cdcc294349f729efd6176693))
- Fix opc collect mode and payload ([b5397a2](b5397a23f0356553197811217f65bc100b4d60e6))
- Fix login direct ([7071fc6](7071fc64d311ea6115ae1bde46dbee42c1c41c63))
- Fix direct ([a509614](a509614da30c3a21aba1ea497b9c51703806638c))
- 修改跳转和pibackfill中文版时间选择 ([c38e971](c38e971bfee7ec48ffb310e153e0ccc475d98a55))
- Fix opc regex ([ef56571](ef56571fc0af68190f4fc46bc7fc12fc30162b2b))
- Pi 参数 ([a84ad4e](a84ad4e2dbcc8f332d4b0688bb43902b02c9b6ee))
- Pi 参数 ([ccb6294](ccb6294d18fc8ed01e8905bc42c27ec0edceaf45))
- Dataset tab 样式 ([f7b8c5f](f7b8c5f6b57fe31b69ac05d395db34b01da00078))
- Dataset tab 样式 ([25565aa](25565aaf8868857fdc36396e2bcbf47d8f857a82))
- Upload url ([5f437f0](5f437f0ae5475c9b9865967741f545d0b08b814f))
- Upload url ([73b80db](73b80db25139e35788038c0d0a7dcf007596be03))
- Dataset ui ([880566b](880566b4e94f3fb286c32b9ad6d3c09aac0d99c9))
- Dataset ui ([ba56e8e](ba56e8ea944c9d318ff75c57349ba9e28b93daba))
- Fix agent version ([8957fe8](8957fe898d2a23f1569820e1d090fada250ab59f))
- Fix agent version ([cc8dba7](cc8dba79a65925118acc708e4876191751e6fdb4))
- 中文版 dataset ([85e25cc](85e25cc863b70a0391c9d358baa96b9ed12b68b0))
- Oem TaosX hidden ([4b2c2e1](4b2c2e1cc7bbb5f05a122405dd460f828f2cc996))
- Oem taosx tip ([2cd97f3](2cd97f3f8c068ca7eef2efb9a7eb43f869d7f12b))
- Fix echarts x axis and metric tips ([985ef71](985ef715d7da80e8597c5479c991b898f2f0833e))
- Do not compare active code. ([325ae71](325ae71d936503f14d826e0150672764cf2cd3c3))
- Do not compare active code. ([e3d721a](e3d721a4f6e00722a9589f49e0446d24231ecda7))
- Opc ui ([3e1dd5b](3e1dd5bd1580f8c509c073c95d9b9df33982aac9))
- Ui conflict ([a34ae13](a34ae1399b58763d5ef8bf62c00a7fb3cff6f989))
- Conflict ([fe1b453](fe1b453ef79a0e31f5f5fa6da335030f746d5f42))
- Conflict ([4e90f7f](4e90f7fa5837182bbfa5014930e0ff4a82b32f1f))
- Conflict ([d6a99ee](d6a99ee2181fff3cde7dec2ee2a33e6c5e555b61))
- 传输速度加单位MB ([9e85a0e](9e85a0e845044d3a5d8abcaa7d9c7693376e9611))
- Upload/download url ([0a1b632](0a1b63297a624305f79e08dfcf87b9806156c34b))
- Fix ue ([30187dc](30187dc9837fc6f90d05b2e5049279f7bf1b8e0b))
- Dns 参数 ([5f32c4e](5f32c4ed4bfd7eb92e18dbf44f6ce427023c552f))
- Fix ui ([3290646](32906461814e7b4bdefe11821fb65c11e43de4fe))
- 新增编辑数据源名称、代理、数据库等参数 ([309538a](309538ad4453376deb7e173b0b290e6263097544))
- Opcua ([a1afe72](a1afe72d610a97d8f630e142f2e96250108585ba))
- Opc fix ([ed30f7b](ed30f7b004d059db3c056a76eb2802ab4bfde88b))
- Opentsdb dns ([697bf8c](697bf8ceccc12b512878eae13aaf3eda9ab21401))
- Datain loading ui ([1ab0975](1ab097595880ce4f0133f4562a03276fff0455da))
- Fix agent ([40ba7cd](40ba7cd63f7c605270e61980b68315a3066fcd1b))
- Dataset radio value ([8567ad2](8567ad257214f963e36d22e2b35103ff5ff6de81))
- Dataset radio value ([a55c7e0](a55c7e0ec16abfd60fd393c299017303fa4ab862))
- Fix edit direct ([6969767](69697670d03a92590b4208602c153131783ee0c1))
- Can not edit type in editing status ([f77674d](f77674d3b81ce847d1c38448d8ed3ee25c7ed222))
- 去除空格 ([abe508d](abe508dea877e447698a6ec8e1f622d76696ca15))
- AllPoint does not show csv in use ([b41526e](b41526eff337476aa85843db9c460509b5ffbf87))
- Add tip when download all points ([206f2be](206f2bef7c89501b74447fe6e08ae8e5d68d1b67))


### Documentation


- Add changelog for v1.1.0 ([d6cb56f](d6cb56fd90c8ef4d2f99c567ce10d0a48d427726))
- Update en docs ([166d9fb](166d9fbabcfa1200fe43e85935ebe4fff19814e0))


### Enhancements

- *tmq*: Update subscription data source ui ([e717052](e71705207aa0beb39ce84bcd1db042497a47c5a9))

- Support new oem param CUS_CONFIG ([d331d7b](d331d7b7fbe19abbe0bc1c490c2f8c54030c53fc))
- Bind server to ipv6 address ([7b65281](7b65281887ca116d8d135c0d864ac309c62df2aa))


### Features

- *server*: Add /docs/ route ([50e07cc](50e07cc6c05b31479dd4fa33aea656cd5efc5d5c))
- *server*: Add /docs-en/ route ([7c88986](7c88986dc968804208cee37e4748cb609b8c10c1))
- *test*: Add docs-en ([81aedcb](81aedcb3ae8022b97a172fe305ed567a5cb77915))

- Add topic_suffix filed ([967daf8](967daf82b67fede20901746f73d84959a11287e8))
- 去掉 topic_suffix ([14fe48d](14fe48dbcabd4bdf23110e331acc669eae47ec56))
- Dataset 配置增加 csv ([b553617](b5536173a6e58962b21e2b6d446aaf3286d9306e))
- Dataset 配置增加 csv ([108f394](108f3944482ecdc28cfff252d023954011dcd757))
- Add versin in profile ([6c3844c](6c3844ce45ba9990b56b9b96390bf02c6dff40e4))
- Add dsn info and tips for copy dsn ([4e8a0f1](4e8a0f1d8b07f5c44f5d59036779c3b9afe0bd9c))
- Dataset 中文文案 ([59d45f6](59d45f6b796285d6d6c262b08c0bcd3314980078))
- Datain submit/edit ux ([ed92880](ed92880783fc612b5fb6614667404fc060606006))


### Refactor


- Modify error hint ([7a32641](7a3264196bcdf786147814cead001cdd98a18a48))


## [1.0.0] - 2023-08-01

**Full Changelog**: [v0.2.0...v1.0.0](https://github.com/taosdata/taosx/compare/v0.2.0...v1.0.0)

### Bug Fixes

- *server*: Fix taosx API payload overflow error ([bd59ff7](bd59ff79e94549117b1dfa370ba3556d2ce7e332))
- *server*: Fix taosx API payload overflow error ([570bddf](570bddf48eca1bf189f9b74fff909cf05b00d417))
- *server*: Fix /rest/sql tz not work ([f10b49c](f10b49cd22ff7dd59747ab368488ae8c3b4f2e2d))
- *server*: Fix /rest/sql tz not work ([6590cff](6590cff3b7edab53b64303a3bbce2e9f249899fd))
- *server*: /upload API multipart boundary not found in production ([5a19337](5a193371318ed2abfa551374fa6cbc536da55ad2))
- *server*: Fix /api/x error with 200 code, use upstream status code instead ([f1dbb30](f1dbb30e76d8898c8bbc0aece7685cd75c612b32))
- *windows*: Fix windows config dir ([e6550ad](e6550add812b10a38d9cc71fd88249a26c56916d))

- Node ~14 does not have replaceAll function ([18deb6e](18deb6ea2f6706f3c07ee7c39a8e2d618fa89c06))
- /rest/upload and /rest/sql fix ([3475c9a](3475c9a622e737333e702c6ebf52b76482c04f0e))
- Fix configuration file path error on linux ([d012c6a](d012c6ab9358970a2889eecf50d9e604201ada28))
- Fix /rest/sql redirect timeout ([add77a7](add77a7960c27e9a6b110d499ec53493561de3e1))
- Arg parse remove default value ([563f7e6](563f7e6835b036760116d994613bb2d6a951182c))
- Fix systemd service error ([3b027b4](3b027b47e42c5dd41c4f67c117aa7e6262ac5484))
- Fix default value in /api/-/profile ([056cd38](056cd380574ca692098072dbb2c75cc91c72c404))
- Fix --version name error for OEM ([e6163ef](e6163ef74830d1acd383c2bb434af36a90e6d512))
- [TD-23547](https://jira.taosdata.com:18080/browse/TD-23547) 数据订阅示例代码页面 UI 不对 ([9a48db7](9a48db721e719867772f1e76010909f60973f9cb))
- 配置国际化 ([30464dd](30464dddfadf3da6ab465d0c171dddbdbe4bbe6e))
- [TD-23748](https://jira.taosdata.com:18080/browse/TD-23748) 增加用户名密码错误提示 ([14cefb2](14cefb2aa734ceead6d4e7f1b8bedad837308d57))
- [TD-23744](https://jira.taosdata.com:18080/browse/TD-23744) [TD-23742](https://jira.taosdata.com:18080/browse/TD-23742) 增加创建用户错误提示 ([1ef13f1](1ef13f17b3436e0c2b14d546fea5ee7f8b67ecef))
- [TD-23760](https://jira.taosdata.com:18080/browse/TD-23760) [TD-23762](https://jira.taosdata.com:18080/browse/TD-23762) [TD-23764](https://jira.taosdata.com:18080/browse/TD-23764) 增加国际化 ([81b47d6](81b47d6dbdca727b609d85f3d43f2bdd978b33a0))
- [TS-3111](https://jira.taosdata.com:18080/browse/TS-3111) 取消超时限制 ([d35b0ff](d35b0ff2b0e8441eb21819deac5ad59068d55837))
- 弹框警告、操作提示增加中英文国际化 ([9e986dd](9e986dd76f69ac38e32addf910fd82a53ccdad72))
- Apply new information_schema user privileges and fix timeout (#77) ([3b61425](3b6142582d2d8afec4cf65633c53e842bdcff202))
- Fix toml file and gitignore ([7b34bdf](7b34bdfc710c5278d63943700b1ec12dfdc1a2e0))
- Ignore the env file ([bf0cfc4](bf0cfc4a89b33059fbb901e8cf3d05064caccd6b))
- Ignore the env.dev file ([21b5908](21b590857d7f77eab8c4dfb25c403b3c6f8d9e66))
- [TD-23557](https://jira.taosdata.com:18080/browse/TD-23557) revoke user subscript ([5591c4b](5591c4b39fda08d5a8ba3f2ac3128a75d0d613d2))
- Resolve conflict ([977c27d](977c27d81c622055bef7b6331e5ffdbec9059929))
- Delete MODE function from AggregationFn ([9da36a0](9da36a0feb927997561d1991bc0ebc05419a9684))
- Delete MODE function from aggregation  (#84) ([3a0c83e](3a0c83eb8f70f253667c98279995b963eacc938d))
- Fix [TS-3261](https://jira.taosdata.com:18080/browse/TS-3261) and the button in sql query ([51f8da0](51f8da046cd5598c1a2c20ca70f797bb9bad7f7a))
- Fix TS3261 ([3051af6](3051af6444cc179c461465456354b551cbc6ad83))
- OPC support username authentication [TS-2361](https://jira.taosdata.com:18080/browse/TS-2361) (#85) ([81c42c5](81c42c5f9c45e494c3de8236660655fbd09268a7))
- [TD-23818](https://jira.taosdata.com:18080/browse/TD-23818) [TD-23821](https://jira.taosdata.com:18080/browse/TD-23821) [TD-23822](https://jira.taosdata.com:18080/browse/TD-23822) [TD-23825](https://jira.taosdata.com:18080/browse/TD-23825) ([d8654c2](d8654c220f488ce5f36b1a501f1f5a39e3fc99f3))
- [TS-3223](https://jira.taosdata.com:18080/browse/TS-3223) add placeholder & fix tips (#90) ([08df96e](08df96e5c1697eac1cf9efcb5156424eea781daf))
- [TD-23875](https://jira.taosdata.com:18080/browse/TD-23875) 创建表增加错误提示 ([a6c4fc1](a6c4fc14831857c5f50c79b73e0fb3bc890f760f))
- Add new feature of agent([TD-23796](https://jira.taosdata.com:18080/browse/TD-23796)) and fix bugs of [TD-23877](https://jira.taosdata.com:18080/browse/TD-23877)/23886/23878/23862/23863/23860 ([202cd3b](202cd3b1beacfa30956257b0e69197ad83a148bf))
- Fix bug [TD-2893](https://jira.taosdata.com:18080/browse/TD-2893) ([ef12540](ef1254015492ad48a0176fa60c10bdb8d04d52be))
- Fix [TS-3201](https://jira.taosdata.com:18080/browse/TS-3201),fix tags value of subtable and nortable ([52f8753](52f8753bf4e5a431f8028ced8e45307b01616e4c))
- Fix [TS-3201](https://jira.taosdata.com:18080/browse/TS-3201) and [TD-23877](https://jira.taosdata.com:18080/browse/TD-23877) ([161be18](161be1832597fd6e41a6e0391d649d122314405c))
- 流计算国际化、样式、必填提示 ([b34ca8f](b34ca8fd3fec914b00061913b54223206978eae4))
- [TD-23930](https://jira.taosdata.com:18080/browse/TD-23930) 用户无法禁用 ([cfdbfd9](cfdbfd95639a7ed61cc95981e43aefec37edff7f))
- 创建数据库参数调整 ([aad8bc2](aad8bc2b9f626dd5b79a3865ad096bdbcc25e89a))
- [TD-23900](https://jira.taosdata.com:18080/browse/TD-23900) 去除个人收藏 ([cc2c746](cc2c7461f3fa84c3a25a26ed06602cb476537c0d))
- Add new feature of ignore expired in stream,fix the agent in datasource and inplement localization on some dialogs ([847ff89](847ff89f5d3b5fce5b01da077eb1fb76305e26ad))
- Fix duplicate key ([6c61255](6c61255569c1c542300a6e43b0fa81c5be45004d))
- Delete error code in addStream ([145aa21](145aa211e01a28c65a467e7c24f40a48824e47f1))
- Release baseurl ([26b50e7](26b50e74484b15180328c13adfb87f2ade0bfbf3))
- Fix adding columns when editing stable,add warning message when uoloading csv ([4879414](487941487bcda772ac3e334308cffe9f260d54ca))
- Delete infulxdb basurl ([cd1e2cc](cd1e2cc59d6ae49e1a09dc196cc6a89e8facdce1))
- [TD-23998](https://jira.taosdata.com:18080/browse/TD-23998) 流计算sql 过长显示超出 ([74a3577](74a3577a8d6015fda3ebb63c186dcc3d87474e39))
- Hidden tmq datasource when adding new agent ([d9512cf](d9512cf6e3518b004a9f55b5dc8d0f606ac16f76))
- Add influxdb and fix the edit bug  of tmq in datasource ([9febbde](9febbdea6ea6802a401a887b9a4dffe944c9c765))
- [TD-23852](https://jira.taosdata.com:18080/browse/TD-23852) 创建数据库参数增加校验 ([e33e0ea](e33e0ea81f1f7f63dfdf50342738f95c02f49e24))
- TABLE_PREFIX、TABLE_SUFFIX 提示错误 ([9ef7275](9ef7275ef0909b43425d094ae7f4168b64750e37))
- [TD-23999](https://jira.taosdata.com:18080/browse/TD-23999) 不选分区设置，无法创建流计算 ([c046229](c0462298a4347fb6e6a514c9d2ec18479b150a47))
- [TD-24050](https://jira.taosdata.com:18080/browse/TD-24050) 子表的信息无法显示 ([09e3a4f](09e3a4f8f66425b556ae45ecdaf279970411b60a))
- [TD-24026](https://jira.taosdata.com:18080/browse/TD-24026) SQL语句创建新主题时，报错信息不正确 ([3029a2b](3029a2b3a2b3e74f0a3405e765f461eb08b01a94))
- 数据写入Select 增加属性allow-create ([218b8dc](218b8dc04f63d9c67fbc52af3b8ef124399b9b7e))
- Fix datasource edit status,fix dashboard direct url,add agent id when adding datasource,add localization ([480b1cc](480b1cc23fdc135bf4cb6697ad2147761fe868fa))
- Add agentid when adding opc/influxdb datasource,delete excess files and codes,test influxdb ([4c7ef05](4c7ef059c0b5e48374d3726184a35589db4f887c))
- 解决冲突 ([78a36c3](78a36c316b1b9c0b825c73024adf19ae6d0a1058))
- [TD-24049](https://jira.taosdata.com:18080/browse/TD-24049) SQL预览格式 ([95f3fe4](95f3fe4c8f05484e8b71b844a7b1f9a4404f18c8))
- 数据写入一些优化 ([3461ccf](3461ccf51e7c0e3b5c643a6d82cfb53edf08b84b))
- Data in bug ([08c2045](08c2045ee540bfaec0920bb07d7a66d64581c3ea))
- Fix mqtt style,add new data type(bool) in mqtt and show the right ui ([173ddb4](173ddb45f3d70bdd4bf3627beb17f44311baf141))
- 流计算、订阅本地化 ([4093cea](4093cea2077873f182cfbf8fe1cb62e14cea4d2b))
- Fix conflict on opcUI ([e52ccb5](e52ccb58e4614b5f5cbb6d195344d82f7d946266))
- Conflict ([67a03c6](67a03c64b40903ef2be173c5e59c0c5cc87b6217))
- Opc ua 参数 ([ea7462f](ea7462f69f489c5457cb3b86547d22b9281fbc96))
- 订阅相关国际化中英文 ([6c93939](6c93939890a216148404fe3f960141fb21205a2a))
- 用户相关国际话中英文 ([5ca4792](5ca47924e3e6ab800636382b469da34402e66621))
- 中英文国际化、操作提示、校验输入内容 ([43400c3](43400c3e1cdd2a86a99f3ac8312020ea17515f2c))
- Fix specail characters in creating topic,add mesage tip when deleting database with stream,fix table columns bug in tree explorer,optimize agent ([e88d748](e88d7483054f925b3449a20989e511ece0037048))
- 数据浏览器权限、校验 ([37edf94](37edf94eea49d8e62040a5b166881f6ed731b5da))
- Pi 参数 ([defcec1](defcec11b43271060b5ded3084f18b91a029c140))
- Fix influxdb required items and add new tips when adding,fix datasource ui ,allowing adding sub table in stable ([78e8816](78e8816307e034322a85736a39875a6de8ce45ff))
- Set target database required in datasource ([a8f3df6](a8f3df6016bc8ff0ba82d5e061c0f145f78c38ee))
- 数据浏览器 ([af7b043](af7b043e15fdeb326785a701b54a97fad99ae36a))
- Fix replication when non-root add  privilege, test pibackfile ([bde4a20](bde4a202c9eb35959d73000b6de31a4407671df1))
- Conflict datasource ([1183429](11834297c93031ef3cd116bf5b693a55a862805f))
- Fix opcui style ([f30603e](f30603e1e0074e4434f240e2d013d6304c3287ef))
- Fix max_delay sql,add interval_offset,add watermark unit in stream ,fix sql when granting  privilege ([b2db2c6](b2db2c6ff8af5a3a904bb58ba2ab9154462a5d2b))
- 流计算新增参数、订阅校验 ([e6ce2af](e6ce2af73dc34ce0a9bc1d974eee93674991c377))
- Conflict privilege/add.vue ([42d8211](42d82118992c9c1e32513cd9215cce01156900ca))
- Fix create_time format,fix agent expire time ([5296821](5296821677b1956900aee0c1694ce2b0ea9fc811))
- Add agent style ([658bd07](658bd073b2c82fde2bcba28a44bc82b61ae58446))
- 调试mqtt parser的编辑，勿删，不合并 ([5c9f61d](5c9f61d820039a41b20bbd72ebfb401e36dbbd49))
- Change parser type to cast,fix agent expire_time ([67b538b](67b538b3ede8a5583ba716412c69374f07f27eff))
- 修改操作表等相关bug ([fcb0796](fcb0796aea213eae898dc116d1674634444aa8ba))
- 备份、数据同步增加国际话中英文 ([ef308af](ef308af7ed72a1727fb7a3a9f42eb8c41a4823bd))
- Clear cache between adding and editing when operating mqtt parser,add mqtt username dsn ([361e9fb](361e9fbe1c342e25eaa0158b7bb687d1f28a484d))
- Fix agent expire time and set right disabled status,optimize mqtt dropdown list css style ([15eac4e](15eac4e5e2032cfa7a7648ede593da54ae34bea1))
- Fix agent editing,only adding  mqtt parser when fetching mqtt api ([dffddff](dffddff28d4e73a073ca20f1487e1ed16e653979))
- Add agent expire time tips and fix related style,mandtory add permisiion must select at least one ([a0f1523](a0f1523cc590e3b2a578d5ae3efdbd91585893ed))
- Add datasource localization,add message tip when changing task status with expired agent ([ecb8373](ecb8373bba744e8a84c47e23b9b27d6e4264cc4c))
- Copy 按钮国际化 ([a6d0c7d](a6d0c7df8b4791b59b4e3f7c1fde243d74ced1aa))
- 联调数据源 ([5c786d1](5c786d1380de466c772a876a0f8173e500c8f73d))
- Refactor mqtt parser and optimiz details ([8c5075a](8c5075a3bd49a21b92c043d7664632a0fb332f0c))
- Add mqtt domain name vertification,add error messagebox ([33eeb65](33eeb658bc36778b64f8f213f2ed416bb6447e93))
- Add mqtt domain name vertification,add error messagebox,resolve conflict ([f93855b](f93855b3894e48d61e4852e87efc3ef5831d11f5))
- Add mqtt domain name vertification,add error messagebox,resolve conflict ([1badc41](1badc417fcf617d317053c94226bc7b25cd22bb8))
- Modify api synchronization calls and error message ([f806cc3](f806cc36f6cb342f558ef68526b4034d8558984c))
- Conflict ([ee2fe5a](ee2fe5a1ff3c172e3fa3f48d028ebbe36fb7a4ab))
- Fix nested routes error when refresh ([66126fd](66126fd980d7f129211224ceef1f0fbf9fb0688b))
- Add primary key in mqtt,fix certification and fix some bugs about mqtt ([e3bb694](e3bb694f2f32d7542dbdcb0e2e7837b4db2c7878))
- Add opc config,fix mqtt bugs ([2d24d01](2d24d01cda9ee75ccaec1cb36c756f5a4ac7dbf2))
- Licence ui ([69dfbc6](69dfbc66fcc6a192455043cac6abf0127164465c))
- Licence table ui ([92a9000](92a90005c83fae7c536d04e11190cd35c8dcb7de))
- Add disable operation in edit mode in mqtt,[TD-24770](https://jira.taosdata.com:18080/browse/TD-24770) ([74861a2](74861a26d93d9a6b46e0f41ae43fca9556a555c5))
- Add opc nodes required tip,optimize the OPC DA and UA new/additional functionality and data echo feature,add task id in datasource table ([d0318ac](d0318ac75b2db39b648542a83c334fc788bf2917))
- Add topic required tip in mqtt ([8d5f1f5](8d5f1f5bf40328bb3f2507ac1aaa9d75542b37ad))
- Fix mqtt requied tip ,modify the initialized columns when add mqtt ([0410654](041065405538a783cbd35ca24e133dbad6e812d6))
- Restore 参数 ([44f7683](44f7683b7b1c5825e0c6f849b6169fe4d501a2af))
- Dataset 增加必填标识 ([ecb8145](ecb8145480d1f8015f331dace8c3fb6f16f4671a))
- Resolve the issue of data caching when adding and editing OPC records ([82a22f8](82a22f81b863be6f4e27771b03200f55fe1634b7))
- Fix mqtt ssl label to SSL/TLS ([3fd592e](3fd592ef8aeb166a1bb67b308996286fa6cd7bfc))
- Update cfg file path ([94a221f](94a221f0e6cc9008fe334d2dc28595087ef57fdb))
- Allow regular user to log in when sysinfo is 0 ([791dc42](791dc42621c7cd862010c9edc4cd6731f1233f5c))
- 优化mqtt ([b224492](b2244927ce48c56eeb92316d733b28ae779bcabd))
- Follow latest packaging rule ([754577a](754577a5bd5b17d16e1e0a066f9ffac4a0dc289f))
- Refactor MQTT and optimize interface calls and return prompts. ([78e482d](78e482d6e4d948a6eebe3e61ce34f8dbad950979))
- Add editable datasource name,add infuxdb target db precision ([8848b8d](8848b8d80a29a5a0843cbef2df960cf9abaf0606))
- OPC使用空格做列目标 ([7ce43f0](7ce43f0b161fc8919ccc91bc56545ca7694169cd))
- Add target db percision in influxdb ([5ce59c3](5ce59c37a11bad63deaea8f45aba582b69b6eba1))
- 修改influxdb的ns数据库提示到列表页面，同时删除老版本的mqtt config文件 ([9df1730](9df1730e5331e2397f7d039d97417804e10c3fe1))
- Conflict ([6eab8f8](6eab8f88c29d8f9c9673af75f83e04d2338c815c))
- 修改running状态下的task可查看不可编辑 ([a5eed5f](a5eed5f2bd1dcddebdb7dac409cbabdeb35cdb5d))
- Running状态下的task可查看不可编辑 ([366606f](366606f9f3ac9929e1cf72a2a7dcf49a2a6b00e9))
- 区分adapter和taosd的错误提示，优化接口调用方式 ([69ba7ff](69ba7ff8285a636bcb366059eeee42e5ef5e9aa1))
- Add single task refresh function ([cc2a899](cc2a899b48175fa142f7a45bd7a77757e10cf9d1))
- Add single task refresh function ([8891843](88918432d7cc0a9c18d6271dca65bb491edd491e))
- Allow datasource name to edit in datain ([b8f3994](b8f3994f2fdd8773680e43f256fbcc2dda9aff20))
- Fix cors security problem ([2e26b08](2e26b08980ccb489d40c325f68ebc5c82e636b3b))
- Fix cors security problem ([710b492](710b4925165906ab3edb65e650477939a9db40b0))
- Cors allow origin same to the host ([5045202](5045202887738947eb3f58aaf72cd3de0daca03e))
- Cors allow origin same to the host ([38d7fff](38d7fff8294701fd963b9a8d88095d661f2a2042))
- 修改整体ui,未完待续，勿删 ([3499583](3499583f4672d26237aa43d1a1c92a19431723b8))
- 修改字体大小，层级，高度 ([35320fd](35320fd062fc7ffa4e7a01d454034a0a8e06e9b0))
- Fix some ui ([372e3b5](372e3b523ef3fa8a4e8434c21a6b47cee999b8e3))
- Fix oem ui ([be38e60](be38e60bad8cb09c5309e4a6b2db64c02473e503))
- Fix oem ui ([0ebc957](0ebc957cf8f27b3908b9247f2971ed393c6b36c1))
- 修改cluster地址导致程序版本错乱 ([f72a9cc](f72a9cc1bda50aa3afc1eefb32a1cbad648fe807))
- Add steps when adding agent ([6d5e32d](6d5e32d24ac3e2ac6819263d8e53418e5cc7f4cc))
- Fix add agent ([0185395](018539530e039cbac59929cd8b5b3ffe2180a567))
- Fix agent style ([7c16505](7c16505e9a80c35331eb3f7bbab946ee549f49d3))
- Conflict ([4a29e94](4a29e94ca4aa84fa0bc96db88efd5b44c11d3e8c))
- Influxdb 获取 bucket 增加校验 ([361ce78](361ce78f569baa430b2200a308bdef977fda13ac))
- Optimize agent ui ([39202eb](39202eb32c0147d6404cb2849ff1a7923f79ee2f))
- Fix agent content ([0a6e97b](0a6e97bf65ff0acaaba628709e21238b8d9286e4))
- Fix agent url ([7ba7f5c](7ba7f5c00a63fa6c7d4c9c725a84a7cd742c2fae))
- Description style ([e0ef5f3](e0ef5f38e6f6418e7b19ca13b30d9fad705b7de2))
- Fix version ([523f801](523f80166b8215f3cd3d24ab1de0e975ff6c006c))
- Add timezone ([0e0baa5](0e0baa5928f07ba3a32a9fccb3711821451e5c34))
- Highlight token in agent ([a11e5ee](a11e5ee9ffa933835565011e508e1499dce331fb))
- 增加登录提示 ([ba2ca00](ba2ca009d19288405108530af43f8cf7fb262f18))
- 文件地址前面添加@符号 ([40b9f28](40b9f28ee2509b583bed3697fd8e65d505630c40))
- Use direct error desc in response in case of errors ([8e0d73d](8e0d73d518e56685c514d0490c3d1a71d0d6b6d9))
- Add mqtt file prefix ([40570a0](40570a0f12f000727e6a1d086fd3865dacf1fe63))
- Add api tip and file type ([a0cc39d](a0cc39d45156d94e98f990526ee5bc88d6d0b415))


### Enhancements


- Optimize api module (#22) ([6ecb97d](6ecb97d243491c923050678613afb0fd107704b9))
- Optimize cache for modals, fix data source edit (#24) ([a04631e](a04631ef83640a7243abf30f7b60f6bb5ec858e5))
- Oem support based on CUS_* environments (#25) ([c3631be](c3631be61d302ef6496f90abb280b017c5c5781c))
- Remove ansi color prints when not a tty ([a0b309c](a0b309c2de3db07f67be35720f5284deca578f9f))
- Resize response body size, modify return type (#81) ([d11f8a7](d11f8a781ec24855dcd67e130dce1e12ee02e056))
- Support print git commit id when execute -V ([f953af1](f953af1f6c45ad1ef99815d9c24446e265b8a8d8))


### Features

- *server*: Add systemd unit file to target ([a6c7937](a6c7937b7afd5f23c9dd66c40520e19ded27d9e3))
- *server*: Add `grpc` for taosx endpoint in /api/-/profile ([c568fdd](c568fddcf844929a40d32579c3c68ab2ba9bfba5))

- Support /rest/sql directly in explorer API ([5e2081c](5e2081cf551e45abcc012f1d48f84e1143797859))
- Add explorer.toml file ([975eda1](975eda1b75b2011d7f4f42d7842d24115241fd68))
- Split opa to opocua and opcda (#64) ([74fa512](74fa5127663976a694fe9305c564b778b9dcf9c2))
- Refactor message box (#66) ([d2ef90b](d2ef90b1e22a6b56526dadb88a4c7b02bbb27a84))
- Add dashboard and improve backquote support (#73) ([be59da8](be59da8cf0916927e2e61613453832f3a21a6d28))
- [TD-23557](https://jira.taosdata.com:18080/browse/TD-23557) 共享主题增加操作功能 ([28404c8](28404c829cc58888fff36d28a617bef607483bd8))
- Add eight class function to stream and topic module, classify them according to the supported data types ([74feb40](74feb40df450f9939f55f1fd64f9f9c4ae6fa8dd))
- [TS-3223](https://jira.taosdata.com:18080/browse/TS-3223) 创建数据库，支持参数补齐 (#87) ([69c81f6](69c81f65cbbea406dd6dd5b54f39157bad5d14de))
- Hidden agents ([d39f3bd](d39f3bdbcc63f0194cdfc03848f302fafa7d5ac1))
- Hidden agent ([deabd60](deabd60255387ba97cef703e81ba289cffcfc499))
- [TD-23630](https://jira.taosdata.com:18080/browse/TD-23630) 支持对 Pi 数据源选择数据点位 ([66023a6](66023a61efdca214f73480f878c4999149c0136d))
- Show the agent module ([6d965a1](6d965a1fab0b8493efec09d78d065fff6edb3148))
- [TD-24006](https://jira.taosdata.com:18080/browse/TD-24006) 对条件集窗口进行改进 ([d45e10c](d45e10c019d69e45091389c55348891efc55d9e7))
- OPC UA/DA 的数据源选择数据点位 ([1a81c85](1a81c8521df3ad688ad83056c98bce6d9d287e17))
- Pibackfill ([d550b05](d550b05333238d8ef2f638b794e3581a4535d810))
- Add license renew API ([c53603e](c53603e0aad763716e1fdcf65522efe757124196))
- Extend editing function of mqtt parser and optimize related functions and fix some bugs ([69c3437](69c3437194ae3c6bb97ce245915e43c92e2c1425))
- 重构mqtt parser，重新梳理主键，tag合column之间的关系和交互 ([42ba7cf](42ba7cff8e341f9e489ea0daafc3fa6dab0eaf9e))
- Add cors option in config file ([8de63bd](8de63bd510218da58cded1ad2f2c1e3bef450b5c))
- Add cors option in config file ([9b0260b](9b0260bf763ca4c362b7631346097127f9a06040))
- Add dataout ui ([accbb05](accbb055f8fbecd4eb04ff72bd69579b1181f2b1))
- Csv opc mqtt upload file ([2a1a823](2a1a8239ca6dfb1da88e45a015a9b093db41394e))
- Csv opc mqtt upload file ([f4db1aa](f4db1aa6dcaef3865548310a061a6d2c4bb82925))


### Performance


- 优化许可证和代理 ([43e3e01](43e3e0125140b4362416194b2b639515a4193ac4))
- 代理优化 ([26fee9c](26fee9c6fbad7de22a033b9f26f3c588d9cc24e8))


### Refactor


- Modify default timeout from 5 sec to intmax (#76) ([f9ec5b5](f9ec5b58b9971120cf03229317953c7c8f352fb9))
- Modify timeout ([1ac4861](1ac4861a211ec7485638373d067bba275daa0382))
- Sync status code with taosx ([7ecfa21](7ecfa21b6bbfb3a80cc208a3f893c980cbefcfa9))
- Remove import ([55d21ee](55d21ee19120414ccc6a6561885f2bd3d59f59e4))
- Wrap taosx all status code with ok ([8d59e63](8d59e637a35a0956de5297d52f1f087add05a2c8))
- Set content type ([2157f13](2157f13df12f2354b702ae9c5cf3492237c0fd0b))
- Modify explorer default config path ([9f6ed70](9f6ed708f41635e9caa16197b49130472769691a))


### Build


- Add windows service file ([c12f727](c12f727ba79e69f0d5cddebd9da9153053e6faaa))


## [0.2.0] - 2023-02-28

### Bug Fixes

- *server*: Tasks api content type error ([d11adbf](d11adbfca80a8623f2a6a8455e9ae8388fa32a52))

- Fix read configuration error ([2acbcdf](2acbcdfa7b72f4428f6a67bf48345b8b2b7f6f68))
- Grant topic ([5c25ad8](5c25ad89a40af67a77574f407764f8270f84b176))
- Windows build ([47b7537](47b75372e2eb3a71c3119f1ff41e0f1bc2179f72))
- [TD-22850](https://jira.taosdata.com:18080/browse/TD-22850) ([d200d50](d200d5054e12580d23efc8046c74ea0efce077a9))
- Fix replication content-type error ([ab4207c](ab4207cf01314b327568e3fc1e08395cd27f93fc))


### Documentation


- Update README ([e7e9236](e7e9236a78c98a7820f6e3b767e2f1d5d74958f2))
- Correct dsn in programming pages ([2913472](2913472aa384645015e16292ab9cb22110a57a9e))


### Enhancements


- Support vue route refresh on server ([b0ce43c](b0ce43c6da79274bc309e7c017dbc43b00116e1a))
- Init github workflow ([96cfdb7](96cfdb76e9d672ed437ca770b92baaaaae302e69))


### Features

- *server*: Add cluster/dashboard configuration options ([38399ca](38399cab7b975e7df36a80014150f6dc8badf917))

- Add rust embed server for explorer ([e399887](e39988737c3be77604a300d3a2ef7dcff12366eb))
- Apply configurations by order: file, env, cli ([d3d4c93](d3d4c93942c9e5470c4e313ea17dc7a31f0043bf))
- Add --version flag ([602f1d5](602f1d59ac3c100fe3bab6644c13dba761ad8a70))
- Support taosx API ([2a9b30e](2a9b30e12622c50f847e7392105dcc8961126197))
- Use configured taosx api ([68d2238](68d2238f59ce85965252e3c64ab9d0edb94f36c1))
- Cors: allow * ([f615158](f61515854dca0992a97410a2dfabb0554320682c))
- Support CUS_NAME/CUS_PROMPT environment ([512b274](512b274377fe630cec3a36f4604d8f179d79004b))
- Support README in out dir ([f4dc75d](f4dc75d8245aff60317797669b080b99deab9641))


### Performance


- Update words ([d498d0e](d498d0efab3f64c43816a16c32c344b864683ebb))


### Refactor


- Modify linux aarch64 build ([756b5f9](756b5f94906939fed5b3f5b57aadd97526a56df0))
- Add sudo to install package ([cebdbd6](cebdbd657187487de69c15716dc006c3c3be0d10))
- Modify linux-aarch64 build ([8ba7539](8ba7539cbda4038aa515e0017fac64602c1c7b74))


### Dep


- Add lodash as dependency ([f289418](f289418cc31ea82355e109996a66b33d53a77ae1))


