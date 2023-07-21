# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.3] - 2023-07-21

**Full Changelog**: [v1.0.2...1.0.3](https://github.com/taosdata/taosx/compare/v1.0.2...1.0.3)

### Bug Fixes

- *legacy*: Fix unexpected error in legacy sync ([4652132](465213251a9f6761668d93ad8f889c74a8980f46))
- *legacy*: Fix memory increasing problem in syncing ([4eff0fb](4eff0fb4d105c530fe57b576dd12c8fb910b1fa3))

- Influxdb log typo ([96d4b3f](96d4b3f92d58d8498a62afb7ffc102e18a73f2a9))
- Pi log typo ([ee7b0bb](ee7b0bb3472d023ae66db7d5492c37e007996b0c))
- Opc log typo ([e1aded1](e1aded1d2878be0272af3583387fe149ed8c27bd))


### Enhancements


- Set log keep days by env ([05e4c5a](05e4c5a15983845629cdf664414f394a60a8979a))


## [1.0.2] - 2023-07-10

**Full Changelog**: [v1.0.0...v1.0.2](https://github.com/taosdata/taosx/compare/v1.0.0...v1.0.2)

### Bug Fixes

- *legacy*: Websocket closed when syncing will cause channel closed ([2a40af3](2a40af31242a18bd67caaaae206641ba520d55cd))
- *legacy*: Fix channel close on websocket ([e613dfc](e613dfc5f643608959350137945dd60bd42636ab))

- Reinstall warning and readme ([82b3b8c](82b3b8ce2eea3be4f2a5e3f8c7a0976f0640ce4f))
- Config path on windows ([5168a84](5168a84e406a6eb2b3cf49c9c1ba2f305274a28b))
- Replace config path to "taosX" ([094fdd5](094fdd5e325d3794aa6263ff17663e1d4865cf58))
- Redo ([6693560](66935608ab10cab4ace22997d9831624c086158a))
- Update path ([e345617](e3456172fb24d432beb2ac9a714961b703ce13c1))
- Uninstall taosx from control panel ([c285831](c28583108f60c93c6ece949bba1b0391597a0ad6))
- Fix [TD-24903](https://jira.taosdata.com:18080/browse/TD-24903) opcua regex not work ([280e40c](280e40c95fd3b9441fc49b76c0607c085d2291cd))
- Fix [TD-24918](https://jira.taosdata.com:18080/browse/TD-24918) and [TD-24919](https://jira.taosdata.com:18080/browse/TD-24919) ([c947f53](c947f5355ca849c75abdc46f5bbea99c5afd98ff))
- Git config safe.directory after checkout ([15b20dd](15b20dd3c604839f98418a1438ab992c81dc4d57))
- Install git before exec checkout ([000a9ac](000a9aca7305f513df68b79118fce6837ebbd263))
- Update taos to v0.8.13 ([97cf14b](97cf14b0778586ff45e140e1720a4e27e138a47e))
- Fix bug in calculating offset. #[TD-24990](https://jira.taosdata.com:18080/browse/TD-24990) ([7d11459](7d1145986bdf8c6e29eb6caced96bf73ec707881))
- Do not track transfer metrics if cluster id not valid ([6e175af](6e175af78dd950adf87c93150a48c5c3a797aa2b))
- Specifiy tag name when use insert into using ([092cdb8](092cdb8fd2c91c5edeb7b3b1070d8f6d072b40c4))
- [TD-25061](https://jira.taosdata.com:18080/browse/TD-25061)/[TD-25040](https://jira.taosdata.com:18080/browse/TD-25040) fix index error ([05bb6a7](05bb6a7cef13b4a5f8afc841d0545d693010c8a0))
- Piconnector parse big toml file failed ([aaf0115](aaf01159275f63ba82c83c23ddfc8012543c9168))
- Local time ([d68f744](d68f744572bcb84a9cab83265745fd6fc22b2916))
- Use timer clone ([bc84f81](bc84f81a93a2b1a0bd1383e0cd7830f7737d43eb))
- Timer utc offset ([c3c9f57](c3c9f579fa38d3430a774e35b3fd1a0a37017a30))
- Time local offset ([9d56a51](9d56a51c3aab4ad60735359bd48faf98ecba89fe))
- Use chrono timezone offset ([c97038e](c97038e79dc62501af3344f7a2001e5ff8439ebe))
- Remove time feature ([34d690a](34d690abce072f714097893c619ca976edab2722))
- Remove unused import ([1a39ee5](1a39ee5468c497feb324fc1b6724ed9ffe4c196c))
- Fix configDir and libraryPath verify fail ([34d949c](34d949ce9c4a0ea5347b9a3763672b64154df6c0))


### Enhancements

- *mqtt*: Reduce CPU usage ([c08fa4c](c08fa4c7323bb416fb1393c14c314e0f53166894))
- *serve*: Improve data source parameters ([3c61a7c](3c61a7cad5743292ac5431ec65843a5a52aaaebd))
- *serve*: Add short_description field in params/groups ([35168ff](35168ff08e3c58528d1f3b9b33950df2c45b3e67))

- [TD-24921](https://jira.taosdata.com:18080/browse/TD-24921) "code" desc ([89500bf](89500bf29e7d96826db9cc42d9105e6a846b49a2))
- Refactor lush message batch insert ([a781dd1](a781dd16303f8c1c0d0269320dd6c3f0a9b2eb65))


### Features


- Verify taos dsn in legacy mode strictly ([33d8a3d](33d8a3decfa15f4a1b2fc72c6cc6de250d21e9b8))


### Refactor


- Opc config add points config ([9fe2f7b](9fe2f7b10f1aa1764cc21423535690be7f7cac99))
- Remove release temporily ([36e5b43](36e5b43e1974304e3bf77ab7cbc0e568a81e31b8))
- Add print ([ae15ea2](ae15ea239a8d479bd15eac4d3439226723935958))
- Modify git config exec location ([c0d2402](c0d2402f68c95faed98032a100d95c6865742f7c))
- Modify safe.directory set ([b306542](b306542dde4f6672f293710ea62058637a23d823))
- Modfiy lush message insert ([c6ab372](c6ab3725bb21b86fba982c626251d805da814239))
- Disable git safe.directory ([09cf1ab](09cf1ab7c69c7e4103970b48b4e1a2958b62e789))
- Add print ([313838b](313838b6b1fda2ebe8a2973ca11094604de63d2f))
- Reset git safe.directory ([aeebb66](aeebb66197d5d5483c3425e906b7fb6f12949c59))
- Modfiy log level ([94b016b](94b016bbed0fb6bcdee878b3673e698a686e9d47))
- Remove cache temporary ([71f49d2](71f49d21f802843e526fedf0a20eecf95927a22c))
- Use rust-cache ([a0be253](a0be2531b41e24959a08b7aa330c1ec8677bf3ca))
- Remove set safe.directory '*' (not work) ([cdf0160](cdf0160bb8926e3e8b41628a0b89ae0216ee3d1b))
- Recover release ([3382787](33827873f5877c7269237a29f6e1a83b11f6ee79))
- Set safe.directory $PWD ([47b2d3a](47b2d3a61d33af4bbbeb379e343631e9265d17ab))
- Use $PWD instead of /__w/taosx/taosx ([1b68ca5](1b68ca515106280721d864bd5d07bd4fa34bdbf6))
- Modify log print ([43e647a](43e647ad1ad8245df5cee1fcf1663821badf7e2d))


## [1.0.0] - 2023-06-21

**Full Changelog**: [v0.5.1...v1.0.0](https://github.com/taosdata/taosx/compare/v0.5.1...v1.0.0)

### Bug Fixes

- *build*: Use consist db params to fix sqlx migration error ([0fed271](0fed271ed58322908cc11cd5292dcadbde1b9cb5))
- *parser*: Tracing json parse errors instead of panic ([c9a3da8](c9a3da883fc2fcb478038010d269cbc41fc73d24))
- *serve*: Fix update agent with name error ([eec0ae7](eec0ae782ab1ac4de722e8aa62d018828f011e5e))

- Fix compile error when using native-tls ([50d8264](50d82642d13d666791e19392f7e9894915b14162))
- Fix mem leak in connector (#182) ([b2119d1](b2119d13b2d1eaec913931ba103615c37be63503))
- [TD-23843](https://jira.taosdata.com:18080/browse/TD-23843) agent patch support chinese (#184) ([80a980a](80a980aba0948bb2c45290233fa18f6fc82f88d7))
- Fix for subscribe ([3afca26](3afca2641db95331f6830cf76201f625b580eeb9))
- Fix move compile error ([62ac9f2](62ac9f2bf6f83725dce3d16cc5c0be02b2a6f95c))
- Fix pi dataset key error ([64fb493](64fb4936e36b73b48f6af36410aca774aebc61b2))
- Avoid stmt init multiple times in one writer ([4bee6d4](4bee6d40cbd9e90a7d5181d939975e97dcbc1d07))
- Fix cancellation not work for OPC on Linux ([bbe0083](bbe00831b124de95e9a651f5fdc57f59f5e4e862))
- Avoid infinite loop when IPC task stopped ([b45dde3](b45dde3902cd431be911561f6e1930e2bc4943ba))
- Fix dataset detail empty value ([c9e861c](c9e861c3ee369d3efc73b688af8e6832ca061356))
- Fix compile error ([8c3cb71](8c3cb71189a0bc68c644606a23b42014ee6ef4d2))
- Fix compile error on windows ([08ffe8a](08ffe8a4085ff5168ca1d2d5964ffd7d8d91f456))
- Fix for reading limit ([f6bee9d](f6bee9d90d3771e16dccac9a1c625890d17b6517))
- Default database path to /var/lib/taosx (#201) ([6869bef](6869bef2de8cc0d228a755108b0ef29942c96281))
- Fix stable not create for lush message when use agent ([69eb75b](69eb75bb759053f1d3ddefb9dce0a12ac2480143))
- Envent handler error ([033dee1](033dee13562619cfb142329710ce51bedf932eac))
- Fix deadlock when call remove ([c29ac45](c29ac4564fb30698fbbd723198ae51271c047cea))
- Fix param error ([ff51e81](ff51e81389a0de4020b2653ebaa92ede3df37712))
- Validate dsn before creating ([e40bbfc](e40bbfc8f813f66e0bc826ce596387dcf7f8fa44))
- Fix hint definition returned min/max not exist ([7ec10db](7ec10db90a2ad4a6df46aadda87f1f663ac76fe6))
- Fix point list param error ([024607f](024607fa698b371c25684f2ccc0948b1fe9c832f))
- Add editable&selectable for Target struct ([3b58d8d](3b58d8d30f70c130d0755a4d6b4abb5478693ea1))
- Remove labels feild ([f66af7a](f66af7ab7b44186e50bab0b448631977c0f7763d))
- Maxbackfilldays change to int ([a7b746e](a7b746e600a00ebd1c5a50821a3af48c49292bc4))
- Time range error ([db34234](db34234f3cc02b4aacbdd9f2e68c1c9054e2bb0c))
- Fix opc dataset category is none ([50a6840](50a68400829eb10711994f0f962fb54596c378e1))
- Fix for get all points for da ([5ae3ff1](5ae3ff15df86154377201de057824ccd40d617f1))
- Fix field reading error ([077d161](077d16134662c9bef77975a2ee7550956f3d184e))
- Fix systemd environment error for taosx ([3e2b524](3e2b52418414287ccf7fd5a9f300eafafda7a44c))
- Fix API example error ([8837615](883761593c9da018c82091953407aa5be6e150f4))
- Parser can be updated via /tasks/:id ([73cda0d](73cda0d49a133091f6ede53218073230b12cc27c))
- Change opc path ([dcdd3de](dcdd3de4fcdf71abbeabeb68906655dd265ce745))
- Fix consumer subscribe hangout in less CPUs environment ([1760758](1760758cb50857518620c4fd173b53fdd32230c1))
- Fix for opc ua connector ([ab7bb4f](ab7bb4fb0a7402a418a231934d6e65ef9b31064b))
- Fix for opc ua connector ([b0c6e8f](b0c6e8fd012b6b6c935b8c03afa9099d641c8edb))
- Fix windows tokio runtime dropping error ([fcf8934](fcf89344ee61c6fc55bc1ae3c077ebb1454939ad))
- Fix windows tokio runtime dropping error ([7bb5f39](7bb5f398f81ec98d53f6918fcde2bbe7d7cca351))
- Add args in mqtt test ([99a2c11](99a2c112b0ddbdb01d432a0b068e3dbf062891b2))
- Fix compile error ([7b7dc69](7b7dc69da6de5d20eae873db8d844f6f20aebe2f))
- Fix lush message insert error ([1031fd9](1031fd9b61154ba2e69c5a6aff929a354388ec1c))
- Fix to_string unsupported when type is bool ([011b775](011b775733caab5f8ac029488e391a63860b192b))
- Solve problems that cannot be stopped ([3c484cc](3c484ccc7ea6e086acb3557c57000173498420f1))
- Return empty node config instead of Err  to avoid taos panick when config node-config a file ([2e2ed98](2e2ed989b099c91a642397c5285af3842781d995))
- Fix bug for time format ([329a5e1](329a5e15cac9d5f63128b33f468c65fd48ef5aa3))
- Fix table write block ? ([05b9348](05b93480a70696ca4a507906d21830495dfa8fba))
- Fix parser none ([004601f](004601f3c646b411779c7147f2318eca494fd0bd))
- Task start and end time are parsed using the zero time zone. #[TD-24414](https://jira.taosdata.com:18080/browse/TD-24414) ([df4f93b](df4f93bcb4c5925bb222023a9a6b8de9912ee08b))
- Rename the name of the stable to 'bucket_measurement'. #[TD-24417](https://jira.taosdata.com:18080/browse/TD-24417) ([10035dc](10035dc7943548edbbc877eb53d4312b8bd23004))
- Replace varchar with nchar to solve the problem of Chinese characters. #[TD-24423](https://jira.taosdata.com:18080/browse/TD-24423) ([e24e9bc](e24e9bc695dd39a136b28f1bbfcbf2d5814e75fb))
- Solve the problem of variable numeric types. ([01ed83d](01ed83d5f548e11bc356de18cbeb47c5e430eea5))
- Solve the problem of time precision. #[TD-24407](https://jira.taosdata.com:18080/browse/TD-24407) ([6bc3b03](6bc3b03c2c502d45d51bd5f4a5dbe9a4dd9dfba4))
- Change SYSTEM_OUT to SYSTEM_ERR and fix the bug in arrow data. #[TD-24481](https://jira.taosdata.com:18080/browse/TD-24481) ([fe49ea5](fe49ea5ffc4da1552241226893948423bca67a21))
- Fix compile error ([f9e38ae](f9e38aef29e74c2736e44ca0cacfee259328edbf))
- Fix compile error ([2106534](2106534d7eec6764fcf6dff409437ade8e80d572))
- Fix the bug on timestamp and add fatjar. ([f9c37aa](f9c37aadc99b4146fdcc779acea19dcf707e3a06))
- Fix update agent fail ([21f72cc](21f72cc644714a4c51b87c2bed7e7001b9482d00))
- Remove useless methods ([52a5f71](52a5f713dcc1cca5c2fdf6b8098168b09acb2ef0))
- Modify field name ([0b66bf8](0b66bf8cdbc19d3568de3472494c1acedef9eb4b))
- Fix reason text in data replication ([9b69482](9b694825a56eab18c13ccea79c70b8083387053d))
- Modify packaging configuration ([1063b13](1063b132320d3fc9a7ea08d4e53c0834028ba481))
- Check version before get assignment ([523d62b](523d62b5a17be4cb661c2e36e1aee4b9ff33c753))
- Modify the parameters of the configuration page in explorer. #[TD-24544](https://jira.taosdata.com:18080/browse/TD-24544) ([cdffaab](cdffaabca13690e3a7a598a02af9c80dee7a4cb4))
- Fix username&password not set ([7e95802](7e95802c5f230610b964f5780f90f8e381b3f709))
- Modify to specify only one bucket. #[TD-24417](https://jira.taosdata.com:18080/browse/TD-24417) ([64ec68a](64ec68a264ac9310f802d9ccf60cd29f9788d3af))
- Delete register nodes ([ad3805d](ad3805dd6ae575781ca133e36acd98bf6bbddf5b))
- Remove redundant code for agent ([d41346c](d41346ca09d1e02227367b644daa94a19c9ce1ca))
- Translate in cn influxdb ([6c290db](6c290db788a817cd4a7d67e4516e8ec94e65790b))
- Fix parser parse timestamp/varchar/nchar fail error ([1c4e1ef](1c4e1efe34a3e5fed1800e324175f47a3d137e73))
- Fix example error ([c1060b6](c1060b6ebd13a5b04f2fe2ee651fd5904769c2d9))
- [TD-24649](https://jira.taosdata.com:18080/browse/TD-24649) muti vgroup ([9a47d2a](9a47d2a5bec223c053e29a8a044aa8e2bf69cc8e))
- Fix missing required claim exp error ([2c92cd6](2c92cd65a79f9f74538af0bfcaac8a4261e04e76))
- Stop task when agent abort ([a72cb36](a72cb365176a81ba862715ddff1cb4000fcfb6c6))
- Delete data type from opc points ([1e93a8b](1e93a8ba478cc3ae7c4c631460658104e1206531))
- Fix the logical bugs in the time window. #[TD-24612](https://jira.taosdata.com:18080/browse/TD-24612) ([384ec64](384ec64877c5cacea80dc9a794248cc19b5ec081))
- Support uppercase in cast types ([d70b6d7](d70b6d78f82385bcded248876921f3ca582bdeee))
- Do not create task when agent is not alive ([2a39e1f](2a39e1fb6a82ff44f6f4bafec59de246c50fb7fb))
- Fix mqtt username password missing ([ab6252a](ab6252a61360215efdb6367661384708237751e4))
- Default value of parameters for mqtt ([91b74dc](91b74dc1a8904ace6b8f414de01739ed15a253f3))
- Fix yaml config error ([3e540b7](3e540b77c55ca58bc58d18e6981fecc6ab6a803f))
- Add rotation for connector logs ([e1efc6e](e1efc6eb18436d72f9baaa447096c59c28ea2d53))
- Add mqtt rotation log ([9faf605](9faf605bf033a36c78d7d914e8097bd15927a1dd))
- Add influxdb rotation log ([afbbe60](afbbe6083e8a5182b111640462da2ace87ddeecc))
- Add pi rotation log ([5de552c](5de552caa3e434f611e3e41c09c0c6a69f20bfc3))
- Insert sql add column ([4513237](4513237da8b5797256c5a9012833e04c2ea3713c))
- Update std output ([c8f2c0a](c8f2c0a599c60d661323398b3435369b2db848b0))
- Fix the timezone issue and optimize the logging output.. #[TD-24612](https://jira.taosdata.com:18080/browse/TD-24612) ([cb5282f](cb5282f022c5bbb45598df337b55358a3022224f))
- Log not real time ([a113d28](a113d28aff80a6e7bb2ea0e22b4fafce58281d26))
- Update cfg path ([50b7ef9](50b7ef91162467410596f41c0baf37ee8a60566a))
- Delete register nodes ([28f0c1c](28f0c1c1d5a84dd48b1e202bdc6acf4bca773a9d))
- Delete data type from opc points ([1fe68eb](1fe68eb5c6682c9e0b15f98c8d878a10f6f7ce22))
- Change the output method of version information. ([5415d90](5415d90788c92f6f918f7e250e8f7e370b55f7e4))
- Add agent version to log ([256eb66](256eb66ac0d36724d0fbadadc80ddf3da322f0b8))
- Change the output method of version information. #[TD-24793](https://jira.taosdata.com:18080/browse/TD-24793) ([187425d](187425d2f02c6cce7c2c728060585785d0ad3299))
- Change the output method of version information. #[TD-24793](https://jira.taosdata.com:18080/browse/TD-24793) ([c15c121](c15c1211660e8479d5dd8ce130fd405722f268e3))
- Log fmt of version ([4f8cc80](4f8cc80880e817c867d0c1f917a5615a9dc5d00d))
- Add log of opc datasets ([1412b46](1412b466888de87e993a95c4031bfe44476debef))
- Format version output and fix for points ([defcdd1](defcdd122263563f25aaaaeb3de5c0571f15ede0))
- Format version output and fix for points ([c0271b2](c0271b25eea008cd1f0660e0e4e892782fa4bc28))
- Use local time in log ([00955c2](00955c22b0a2cb4a2c9f911159d6c791d3f9276d))
- Add err of plugin not found ([53f27dc](53f27dce9e8ad265471f0b87e9eff56a78401b77))
- Windows build error ([e2fdf3f](e2fdf3f406ac9fad3932cc1491b47df9d64939bd))
- Fix number as data source connector connections ([0cb88d0](0cb88d015eebddf792a6db6bdbfd2254d173a648))
- Add enterprise license validation for plugins ([05bc6fa](05bc6fa95f19cc378f199e4712faf7e8d5c15703))
- Fix parser alias not work and parse error when column not found in payload ([ccd2b95](ccd2b95ba7682a7fdb465e93d6d797b5e0eb9537))
- Refactor version print in stdout/log ([31be75d](31be75df1c781ec09f1c9947b21cf4548e95d78f))
- Fix parser add column with alias error ([70b0fcc](70b0fcc8cafe5066eb77834fe0834227ea0a10c1))
- Fix version output format ([c34eed1](c34eed13736d9cc19c3b473c469f3236ea356c71))
- Fix version output format ([36a5302](36a530248bea5edd91de738a82a0c8569b38be57))
- Unknown time ([f702d47](f702d4749e06c8688e29c54ab56456c1cc193983))
- Refactor license number validation ([92ef989](92ef98956252ffb0cd73afd4758cd458baa94af5))
- Support handle [0x0618] error code ([1cc09a7](1cc09a7d2d157d777753ad7667ee8983a6b154ef))
- Remove token in log ([6588964](658896475d83c91dab550d52f58beee7b8c95436))
- Add cargo rerun ([9d946e4](9d946e4e93bd415a266789549573b058fc98185c))
- Fmt log ([30e9133](30e9133a1bf7b4edbe3ab5251aca2c0cb089b628))
- Connector lincence number check error ([9d6067c](9d6067c59c6d501a6dfd6001c1658e9498a0e04a))
- Fix for opc da reader ([71cd76f](71cd76f35f916e80a329ae64bae6a0a8440a35aa))
- Fix for opc da reader ([0666e7b](0666e7b89936cde020cf499fe7f84a693cf01241))
- Remove token in influxdb log ([19c8e19](19c8e19e84990bfe0101710257893aec0d805031))
- Remove log token ([8ba706b](8ba706bc7d5a13e294bd8af7e1861ebd41bf9c4a))
- Typo of explorer ([b02864d](b02864d6e41b7bb0b094ed657fe7a2af18aba677))
- Dir mistake ([c081a93](c081a9360c473c90eeae430c5b65d34bd4b3eedd))
- [TD-24852](https://jira.taosdata.com:18080/browse/TD-24852) ([7e67f32](7e67f328768a813abdf09f68b82df7496fd78b14))
- Fix ParseIntError when version is "3.0.5.0.2023" ([0ec07d5](0ec07d548b68f8bf6c78efca79ad1969435547d9))
- Follow latest packaging rule ([b170f24](b170f24b2e61c697f24adf45bc2766b26186efeb))
- Remove token in log ([b28bcd0](b28bcd02dbc657974f3f756acb76e85a5d247123))
- Cluster-id not required in cloud ([43caaf8](43caaf866cee03507d542bfccc418fd5bc9c645f))
- Loop untail raw_block success when 0x0911 ([15f5535](15f5535c6a52d55edb5a10c99bbea55406f62a06))
- Split large query into chunks by a time unit ([05991ad](05991ad3cd03e8c42de2c631918f7e9a39913d5e))


### Documentation


- Add linux config file path ([4f499d1](4f499d1556cc7f74308f5363c9a5bd409b532f88))


### Enhancements

- *agent*: Support data sets API in agent ([c051165](c051165325bb7e86a92b9eb2fa0de4260f8e649c))
- *agent*: Add commit info and build time in --version output ([974b796](974b79651cd1582e11e06d4160b76092f12d170a))
- *connector::pi*: Batch handle (#180) ([548894a](548894a5d63bafd826fc8bb0fd41e0c31a7d3048))

- Add features and optimize code ([2f2950f](2f2950fb9df7693fe365de938c09ed75331721d1))
- Support add string between lines ([e86e88b](e86e88ba58f0d1fb891232b8bc373c7fc231ac4b))
- Support config point for pi ([73c65fa](73c65fad0c776ddc4742256732c6ee0bcd69657c))
- Add features and optimize code ([f23e98c](f23e98c51ea626325a6c24caaed347ab01de47b7))
- Change maven packaging method, and a few other changes ([fbfe670](fbfe6703992709d96618ac8e0cd095f7ab606248))
- Upgrade arrow version ([4d60d24](4d60d24688ddee0db7b7e647beccbcf9cf1862ee))
- Support debug param for opcua ([20d9197](20d91971ac1184afcabe792dbc5da65123c0c16b))
- Report error when checking enterprise edition ([3a41cb8](3a41cb88f533a040cd6bad40337bc751736fa390))
- Support offset and limit for pi daatase ([c9a517f](c9a517ff73b59a02adb59e80bde3d21c039d3b45))
- Optimize arrow writer ([346c54f](346c54fd0100760d424daef1d82131096e3501d3))
- Add a new data source 'influxdb' ([e0fbf8f](e0fbf8fb6b1912a15934451fe9647f343ed7d93c))
- Add features and optimize code ([140d14e](140d14edfdd4451f9947308d1f6579f62d1ecdff))
- Subscribe multiple consumers asynchronously (#197) ([b846593](b846593f5934b52a40cadc6063734912e6d81958))
- Support use received time as ts column ([7322ccb](7322ccb6a39dd6b114dd5e7cdf45d14333f27dff))
- Optimize monitoring data and fix tiny bugs. ([3694ffe](3694ffe15fac5f681c9b256a482f8c840dd40044))
- Support spawn mqtt ([e7fceba](e7fceba4cd524192ae0cd3f130bc53e873d83fb9))
- Mqtt plugin change ns to ms ([b959f96](b959f9678d7d16b52289740a0d495ba855ae6819))
- Opc datasets support ([802b254](802b2541b4a21cad692d4234b4b49eb60b2970bb))
- Add mqtt.yaml ([7a96dcf](7a96dcfe67ad866ba803311525dafbd6a065a3e7))
- List points with regex and limit ([30b2151](30b2151cfcf560bdeaf8198b6ef4f0205ec2d562))
- Read the specified toml file, some code optimizations and error modifications. ([21a8bf4](21a8bf423a3480ead2220e53e06b975d7f07f1a3))
- Add name and status ([2cc2ff7](2cc2ff736b39c713dac15ad90266aa30b19623ea))
- Log version on start up ([77e4e95](77e4e952358b4d9a2b81a6a4ce076df4203e9095))
- Add parsing fields and adjust influxdb.yaml ([edd9fd2](edd9fd211b4186f69245abe459984ff9ef2a1677))
- Datasource add pi-backfill.yaml ([8f4e507](8f4e50735dfe2f206681aa80aa61e4cb3aadfac2))
- Change opc binary name ([e24a1a5](e24a1a50b4c2356c164bf29fc376fc36b0287ed4))
- Support lush message contains null value ([7001b67](7001b679d1167ba2e71e7f486e14f86d7aa78008))
- Sort points ([5c09076](5c0907681b2ebce0d555a2c57fdbfec8da1a62ad))
- Add compiled version and modify type conversion bugs ([3be9599](3be9599e1450e1517e32d01f631a816381d30d6f))
- Set default timeout for tmq to 5s ([89b989d](89b989d8afe98cdadaabc37c708433dbf45b2c96))
- Support name and status column ([230b4d7](230b4d799c9e139e1db7d936ed511f6069a8bc8b))
- Mqtt plugin support version and build time ([bea972c](bea972c14a88e6e06d50e81ecb46ce2a26a570d2))
- Modify datasource description ([536c99b](536c99bc9d9654581394dcc1d03c6d52a50a6897))
- Print taosx version info when run or serve ([469fc30](469fc3082c3ebeaed39450b9e11546000a12ddbd))
- Modify log print ([fd8f284](fd8f284d7825c341873ae7f785b980c1ec0e71bb))
- Several optimizations ([147bdd9](147bdd9a26f765aca74eb15b096ea2dd440159cc))
- Add opc version ([3c78317](3c78317b0c86f98f68a5fac5b2bc0768ee9d66e5))
- Listening to random port & do not output log files. ([774e443](774e4435f6673c9faa8ba3d643bfeaf3f080bf4f))
- Modify startup command. ([0c2762c](0c2762c2224d705af8e0fc69410d8114b9ec4e72))
- Support mqtt v5.0 protocol ([12b2afb](12b2afbf480eb750d3e6170dfa6827e67dc6cfd8))
- Support mqtt v5.0 protocol ([aed7b67](aed7b67fd6a262d9a2a1ef3aee0e59cd22465570))
- Fix log ([8d52736](8d527366d55d103b86c07c25646d65ec1a938b9a))
- Change log format ([0e102a4](0e102a464019d5d88dd6aa2811ad59e8a9a9435c))
- Support arrow ns/us timestamp ([f6faccd](f6faccd5dc8124ad285f5babcf010fc278e903d4))
- Add log for agent ([8b3cbbc](8b3cbbcfadb8dd9c0b411a66e0fd63e03c2bc85c))
- Add log for opc ([d16d7dc](d16d7dc99afbdf9b9c31631e6cfe6b2f2fcd9bf6))
- Update log file create ([839dada](839dadab82eb3757999cf0ca6ad34db912511df4))
- Support errors for connectors from agent to srv ([d23ad83](d23ad839df56be535baa395232f2f73c4fd41ebf))
- Add logs home dir env ([5b0d060](5b0d060e719b196ee5cd1697d485894862c51ccd))
- Update agent log dir with env ([13dbb3b](13dbb3b5b70520ef57f21a8f424f7a6f005ea262))
- Update opc log dir with env ([57db583](57db5837caa51367aac3d2ba40f7d5e8a913f401))
- Add influxdb log ([7bfb0b7](7bfb0b7ac3c42c0964e456e6044cdd855b1feba6))
- Add mqtt log ([8f3993b](8f3993bf599341e76342047d1fb3f2d370f670a1))
- Add pi log ([4b77684](4b776845f674d6930282ca51878f2b52429a5bcf))
- Report connector error via grpc ([20af442](20af442a91477472767efc1ed62304accb558b44))
- Check agent name before save or update ([fa08b8e](fa08b8ed2594a08866c521edcd43c190b9ee3251))
- Zh localization ([dc6668a](dc6668a78c28908ee14aa48b0d09fd07017aaf40))
- Application exit when influxdb error ([154f078](154f078da85e514b38f33d0c70b4a5e4f314022f))
- Refactor mqtt UI with collapsible group ([406c973](406c97349851455f0d01c4288e6eb28be4e5a144))
- Should config opc_table_config when config opc task ([3a4f28c](3a4f28c7f8eeaf7457b1bbe544412cd19744d4e0))
- Opc support single column ([5ddab8d](5ddab8da4f89bd2c536d2c570b08946fd0449265))
- Support add column for opc table config ([8260b23](8260b2381fa613c43b8cb9faf81668908581839b))
- Support read file for parser ([2831414](283141455d6f78c502d152a547f9d3950f382912))
- Parser support bool type and add column or tag ([a25d1c5](a25d1c587eaf52692ffe09d5ef084042e400d3c4))
- Add log token ([7d876e7](7d876e74b9c4796fced6fd8431dd76757136f995))
- Linux pack and intall explore ([f9f9fbe](f9f9fbefd39ce6d5c9be1e2a470f0f8ecca6fb14))


### Features

- *agent*: Always retry when connection closed ([2bf839e](2bf839ea2cd656c381f6f34e234e659c136dbadf))
- *agent*: Add window service and toml file for Agent ([df847c6](df847c67656c622b6a2f71f4b09a178f856e2e5c))

- Add agent related API ([3d244b0](3d244b0da075e884e69d38c01fa4c65c70f53902))
- Support opcua subscribe and change timestamp for opua observe ([126340b](126340bea6b19b8ed8a0819766d3e8e95b0d20b6))
- Add taosx-agent CLI and RPC service/client ([b3a0eb3](b3a0eb3a09039093c158d72f15c7726bfbb5ffca))
- Support mqtt ([2defcef](2defcef79dc5b6cf4c52d7a2cd35f936eaab6c62))
- Add influxdb connector. ([dd4c417](dd4c417acb4aba77108ed867df9e3ee6b1335a15))
- Support pi backfill ([adf53c3](adf53c33f3aa86d300c2ff874cb96198ec031852))
- Support json parser for flat stream (MQTT payload) ([28ea638](28ea638f08dc74d31c68e4c78b466e509cf4a510))
- Support parser in /tasks API ([9501fd2](9501fd2748ff38b4e0de1684d1b9ac2ff930cfcc))
- Support parser in cli mode ([5b796a4](5b796a4ddedea41dbb09c6f565bb22e7e58fd04f))
- Get offsets ([c3f67a2](c3f67a27d9f34a816a88c0dcb903ba63245343fe))
- Tracking with connector usage and license ([7079da2](7079da29ce3d377fbfb256fb247f27dbefdcd128))
- Support partial columns in parser ([3f292aa](3f292aa67a436687f4641a0b5d2334f5f0e75ac6))
- Add parser field in mqtt data source ([464181d](464181d36c5964ac2ddebf5c6838ea2cc4cc3048))
- Release install uninstall taosx under Linux ([e882f5f](e882f5f6500822872b14dd4f510e8a8a1f34169d))
- Rename all plugins exe name ([9fd7d92](9fd7d92d16aef9de5d7315f28e4bd211022acadb))
- Add disable-enterprise-connector-validation feature ([5ebc04f](5ebc04fc7318977e0516297f5657183a4628ac6f))
- Support lang in /ds/in query ([ef38a14](ef38a14df8e332ffd676ac6a5427dc66b5e3e4b0))
- Adapt different schema in legacy mode ([900bd8d](900bd8d50a7012e883fa68fe148f1ec6658c63fa))
- Support jemallocator as global mallocator ([f818833](f81883349faedf758d5da7f50b9da60941beb5b0))


### Refactor


- Print error cause for stmt execute ([d2a4115](d2a4115228e6138630fea68d88cfc0bff09ac39a))
- Print StreamRead next error message ([fe45ce9](fe45ce917f7df3b955b80af5eb7c7e249b099c39))
- Modify dataset config in pi.yaml ([e55543e](e55543ed273d11b15409b08bbe05de0652ca19bc))
- Remove redundant code ([99b4867](99b4867a6b9842a4e78a03982fd372aae0832889))
- Modify error content ([2c085ef](2c085ef180123c3c5f4c629772c9d9444d9ad816))
- Modify hint from string to object and add field type ([af300c8](af300c8fc3462b0fab453da05222fc5309cb507f))
- Add opc param ([bc563ce](bc563cea9616d7a674cff59790d5fc949fa06319))
- Set limit is option ([a262ec5](a262ec561dbfa59b3d48be52fdce04d92be8ecb3))
- Remove redundant code ([328c784](328c784918fb20776b08c2130ee4735246c709f4))
- Remove redundant code ([7d35a94](7d35a94bf8e0af3ece712da4e22693a15aed7c05))
- Remove redundant test code ([b149ff6](b149ff68f29cef68fae8608a6e81bf7f076d087a))
- Datasource add mqtt.yaml ([43a3451](43a3451778868a3528f48f8877eb3e242ba89730))
- Modify mqtt.yaml ([4dc992f](4dc992ff3f37a76a3d926d8c5e936f4acdc9d457))
- Add options and format for opc dataset ([d3c915a](d3c915a5ede8b3511966ce75f6d674ecd32ae00c))
- Modify hint type ([e9ba25d](e9ba25d2a19953e02f3b15eb56e0146ca8784aa8))
- Modify pi+backfill to pibackfill ([7145881](714588137f9a3646cb3b3cd193789347da286b04))
- Run mode add pibackfill driver ([d15fab1](d15fab1e79f1874d5dcaa2816f74985caa87be42))
- Remove protocol ([6b4f613](6b4f61322012bbefb49c7134a1eb8868e7b1db46))
- Modify log level ([be4a197](be4a197a8d24cee0e086a17cd6511d1c11d61ad9))
- Support get pi point when use pibackfill ([d7d5b7a](d7d5b7a577981c006f7bcab3391caf7dedf79115))
- Modify offset ([1a63adf](1a63adff763a162357414e81b5dc43ec073ba34b))
- Modify datatime for pi/pibackfill toml file ([01534c2](01534c25406ed8cc9c45ff6a92ef78837910aa82))
- Add cfg ([d2f10d7](d2f10d7209e64446dc33fce0dbc880ed49eb293a))
- Modify time type from datetime to time ([5d05316](5d053163c0c2caa42ec6d3b104f58049d563cafe))
- Match result after send to channel ([3869943](38699432f66001f9cc94849bb960b9c5d48ab52f))
- Remove quote ([f62cd5c](f62cd5c941a0c19700d0b34283c01f2b8e68fe0f))
- Finish loop when send error occur ([f3b7108](f3b710842121f058be973541932111d27c5b273c))
- Modify mqtt description ([37fed70](37fed70e74ac8d2f3c35a254b1fbd651bbc34807))
- Add opc field min and max ([ba6d4f1](ba6d4f11c1fcf41893e3a2a3963fcbbe083441f2))
- Add example for mqtt topic config ([09c7ee6](09c7ee6de19dcd266947497504c4b4a832e0eb06))
- Mqtt config add version field ([1a20334](1a203345a55c37113e77a66a23062e0b85c8ec3d))
- Change mqtt value type to string ([929941b](929941b1849e9a2868ac3066f4ccbffdd5f62239))
- Udpate rust connector from version to git branch 3.0 ([3bcfba7](3bcfba74a0a1707c85877a347232480d3fe96878))
- Cast varchar to utf8 instead of binary to avoid arrow-json error ([36284b8](36284b8a5038492b88ab75ee7b82bbff75c59df6))
- Add username and password when choose certificates ([6570751](65707513cbbe1c0c1230fd06e47f8e4e853ba691))
- Modify connector description ([cd8b5b2](cd8b5b2b1c91b340254f9638fbf71ced53b61823))
- Should delete associated tasks before delete agent ([f62ab0e](f62ab0e4fb1416f35fc0f0e36b009ec2b44fc996))
- Add <br> for line break ([82719dd](82719dd74d77879157f298ae8b2026b0cf6e7433))
- Modify mqtt version and description ([c269baf](c269bafa3216fad24f77ab7314c7e38e037ffc4a))
- Check name if duplicated when create or update agent ([7dfaf51](7dfaf51d92753325196bafedbe574855ecc448b0))
- Use cluster id as check option ([5e241b4](5e241b40a687315d612b9573fcf1f36664f530dd))
- Modify mqtt cn yaml file ([d0056a0](d0056a0c3a5770bcf7916357580ec248ec888824))
- Modify pi yaml ([48f4241](48f4241a00dbc13db1d9a2d84bc203452a168d96))
- Mqtt remove username strict ([39d6806](39d68066532f14a310e9868e70d5533a86403018))
- Opc yaml config update ([f3bb6be](f3bb6be4c1089ac8ddd62d795fab9db147e91906))
- Add debug param to enable opc collector debug log ([254d458](254d458f2a98d4c5ca1b063da36c67840bae34c7))
- Data set required is true ([728b1ea](728b1eac7f1db19b752efd492faaaf7a094f71ac))
- Stop task when parser cannot find column config ([072a2c4](072a2c408061db4439fc5049a8c2a22b9504f6c1))
- [TD-24768](https://jira.taosdata.com:18080/browse/TD-24768), refactor error handle ([0c273ce](0c273ce1faf363d8ded214b3d0d8b8a058aaac78))
- Modify opc da nodes config, use address host instead ([5834ff7](5834ff73ab1a9fdf6f40c1dfcd2fe323a10f3655))
- Modify connector path and agent config path ([c10aeec](c10aeec023da4387a3c064616df678b18f21356d))


### Testing


- Add debug log ([043c893](043c89360ddab0f25ca780b5282d1b3b9ed2a5d2))


### Build


- Upgrade taos version ([59b2d59](59b2d59be387acea0a8b3d3ed30377511a578f64))


### Dep


- Update arrow to v38 ([70375fb](70375fb7f58e1fda53b6e0170bf3611f257bfdb6))
- Update taos connector to v0.8.2 ([3475fc0](3475fc025f02e5d4c2a4d35eba022ee6838aedc7))
- Update taos to v0.8.4 ([c8c4498](c8c4498e88f903fcff2e70c00cd175a6de36bd4d))
- Update taos v0.8.5 ([e7b33fe](e7b33fe4d0d80a3d2e5d5ece5dc178d8692201f5))
- Update outdated dependencies ([d950f2b](d950f2b900a254a10f66ff6128ef0b36c675309f))


### Ehn


- Print pi point value when config debug ([9acf6c2](9acf6c2207206f568c4cc38b56ef63a4ab165278))


## [0.5.1] - 2023-04-20

**Full Changelog**: [v0.5.0...v0.5.1](https://github.com/taosdata/taosx/compare/v0.5.0...v0.5.1)

### Bug Fixes

- *ipc*: May fix error that only accept one stream ([b53abc9](b53abc93d929b4e35490e879ffbe3e3475c82d3f))
- *legacy*: Fix hangout problem when using realtime mode ([be30cd0](be30cd0e1140e004fb3718507669e3d78c85f5a2))
- *legacy*: Fix hangout problem when using realtime mode ([282c3c1](282c3c1a8fd917bdfe7632d0628d75b672a46d97))
- *legacy*: Use a scheduler for data syncing ([14adf44](14adf443e85047eccfc67fc12f282fe74a48fcca))
- *pi*: Sql rest service handle multiple requests ([7b3bb96](7b3bb96f1722d535f633754a7d422809d95a3c7b))
- *unix*: Fix unit socket compile error ([ca04aaf](ca04aaf0da8766b73f47eb806a63eb07cd596d71))

- Fix stable already exists in legacy mode ([a9fff6d](a9fff6d3d32e35a33348a3e3b912e6b5d793e77a))
- Legacy compile error ([6e7843f](6e7843f0f46bcee22a26f00198cbf56657b47717))
- Fix CI error in case of taos v0.5.5 ([7e0bf45](7e0bf451a4bc360984c857cb4794cc8b411bad36))
- Attrs could be nullable for PI ([f8cda35](f8cda35a98f42f16fc6d58251265b609d78f92f8))
- Fix CI error in case of taos v0.5.5 ([c806b62](c806b62f69902e2bbbce7b2451440dff51d3f1db))
- Fix ctrl+c not work for pi to taos ([140f7b1](140f7b1e4432423c05f98315c9fdb107ad41c0ba))
- Fix windows file handle error ([e9b4ccd](e9b4ccdbedd2092402ce79b52d41eb1e494b0642))
- Fix remove temp file ([e764bc4](e764bc49e7e8ff5f489e5d3c5cac054fd73518e6))
- /ping 404 error ([b69b1d8](b69b1d8cfa9cb10d69c776b9742b0f8aab7af37c))
- Awc in spawn not valid ([988ace2](988ace2c35bc23224818ab113e4ce2f993a43be3))
- Fix create table error ([8f6112f](8f6112f40acb3669ff374e4ed8b816e8360f9f4e))
- Fix websocket connection error for /sql api ([c5315c6](c5315c653017a98345341bc2390760b7bebc1378))
- Update opc/pi data source definition yaml ([69fda6a](69fda6a107c50c15143c859b9d8058a7d5be5ff7))
- Fix backup/restore error ([9633365](96333658233fe8d0fce1a7babbc34c23aec47726))
- Fix pi log read error ([1fa643b](1fa643bdc6a85add6925f3e4ec45af4c4b4c4799))
- Fix opc read record error ([8e6ebe6](8e6ebe65f4cd0c706faa276bd248940e16e06813))
- Fix point read process ([a39e14f](a39e14f2f5bfb02378069e362f439b24f8361f42))
- Fix backup/restore error ([9c11c86](9c11c86d9505484e4ccda4be9e8a58244a64e40d))
- Fix pi data source parameters ([8b0de13](8b0de1338235c1f9492920784184c4d191d4cccc))
- Fix varchar/nchar/binary column type check failed ([1aaeace](1aaeacebbe916205228e52312bbba2610e46671a))
- Fix opc da config error ([72764e9](72764e9560b0449d3bb89c99d02155b01adb2c15))
- Fix struct build in test ([41f4762](41f476257a8dc244e9b8326df453a55f32df515f))
- Disable-enterprise-only-validation feature fix ([9a09542](9a09542d150bdaa7d9321cfec15cc69bc03fce60))
- Fix endpoint parse error ([7439204](7439204b09c4ac805948d5eb3d8c5f122ca04625))
- Fix nchar convert error ([105316e](105316e0fd1a07ecacebd267da77a272d73b0945))
- Fix lifetime error ([dd4f225](dd4f225098140f6e6914af9eef9ecbd2ce902c26))
- Fix from_utf8 error ([36e5970](36e5970d3ca6080005be76dd076fe118e599c865))
- Fix systemd environment file not exist error ([27cc5b2](27cc5b29a3a18d0c99ba3bb5fb4e95f45f6ab21e))
- Empty stable not created in legacy mode (#157) ([518f19e](518f19e82d95ec0b44ac3623805651e9386c7fb2))
- Fix task cancellation cause taosx stopped (#167) ([36b4328](36b4328d6c175721e7ba08d1ce32d36515ee04dd))
- Fix build error (#169) ([31fd490](31fd4904c6da288b751a6e0a10ab38057aef99e8))
- Opc upgrade arrow (#166) ([707c8db](707c8db2463ae8ab3a4dc9ef2b903394266ceb38))
- Catch exception when packaging ([866481e](866481ec06c73fcc4d5d2a33a2d99411d271922e))
- Fix  task state error (#173) ([e4e75ff](e4e75ff167da54b691ccd5a8c99c4dbcc49b67d7))
- Monitor del (#174) ([9ba9f5c](9ba9f5cc0c386a8249ad4235a8713054e0cf2d36))
- Upgrade arrow (#175) ([b5bebe4](b5bebe4017141a18da297691b6b7feeb882ac406))
- Description error ([8cda267](8cda2677026520731930f0e4e715d25894eb27a1))
- Upgrade arrow version ([3d249ba](3d249baf6028498d2448dd24d23856f831aaad07))


### Enhancements

- *legacy*: Use consist metadata for both schema and records ([4a64dad](4a64dad32420f9837c347cf5b0a6184b865e65f9))
- *legacy*: Improve legacy metrics ([e3b5cdf](e3b5cdf7ff3defb4e6bd1ca4d389c1fc01f60fe5))
- *legacy*: Use consist metadata for both schema and records ([ffdb196](ffdb196d2009748645718892f278d88e2d56d161))
- *legacy*: Improve legacy metrics ([ef836b7](ef836b7bfac5675b8f451f9b68ef5497b2f05355))
- *legacy*: Apply scheduler in both meta and data sync ([1a5abb1](1a5abb1403a066f4046cb1ef8519da0e176aece2))
- *legacy*: Apply scheduler in both meta and data sync ([6f8e150](6f8e1506731ea979000359868ed345c8789e9a51))
- *replication*: Use write_raw where possible ([a840f7c](a840f7cdaeb96c3c79188315ae3bfc0f42596a0b))

- Collect child process output ([e87d6eb](e87d6eb0f1a405f32e2b7d3abf37477868d7b82d))
- Collect pi output ([d450e82](d450e821b78cbb52891b60718a5a104de6ff6097))
- Support csv file config in opc dsn ([8460a34](8460a3456b0617e98437bdd65678e5ff4e7e214e))
- Modify table procesing rules ([bc7c5ee](bc7c5ee0a2211dfe2ad4256d1885698095710440))
- Add query erro log ([60f913c](60f913c8681f5edc570b6bd9fa52f4c4d1253b88))
- Add config param TDDataBase for pi ([e5ad479](e5ad47998234423c16ed2c77b936e9f14f2fb58e))
- Support empty ua.nodes/da.tags when build OPCConfig ([6fdc141](6fdc1417360851a136fe7e09b1432eb397f94867))
- Support opc.yaml config endpoint ([2c5bc28](2c5bc28b9401ad2db48e269551adcffa45efccaf))
- Support opc.yaml config endpoint ([e929a66](e929a66c1afa4eaf7d8631d6e0170932b8122989))
- Support get pi points ([961c17b](961c17be8ca9e2e48b73eeba8410020d1667adbf))
- Terminate child process when work thread stopped (#153) ([bdf9441](bdf94413faea352b0f7e939224f18e63f19ce199))
- Add features and optimize code ([30e44d6](30e44d6a6069141b80b92bc477abfc6ef8b6c5d1))
- Use optin feature as default (#163) ([da65f36](da65f368f71f40bc93f946ec67488503768365c5))
- Add features and optimize code ([007d775](007d775df581dfbe276ca2271d0a332893f8abbe))
- Add example for flat message ([e2b68a9](e2b68a97121d73a900e68703741b7002bbca21fc))
- Remove unnecessary reference to AF ([aa2d182](aa2d182aea656fa34a3edc345be38541b2c575c5))


### Features

- *ipc*: Add .NET demo project of Arrow IPC writer ([e414e37](e414e379161bcee22b08452bdecfcceee34a7f36))
- *ipc*: Support create child tables in batch ([6a039a7](6a039a7bb448cf85f3a47490bc30facd3ee4dffa))
- *legacy*: Add `failes-to` option for failed table names ([9671778](967177899d6819d31f14eec44cded665de95431b))
- *legacy*: Support `@file` in `stables` and `tables` parameters ([bd864c4](bd864c40771f00a576616f826731eca6624f0561))
- *legacy*: Syncing table schema concurrently ([c127d85](c127d8503c593eb89b152eaed797c887bb3c6d4f))
- *legacy*: Sync table data concurrently ([72dbd15](72dbd153899bfedfd36c8c271535b2a621566667))
- *legacy*: Add `failes-to` option for failed table names ([bb99377](bb993779c08e32ec622a8c93adf09822a7d0603c))
- *legacy*: Support `@file` in `stables` and `tables` parameters ([4b9fc6d](4b9fc6d025cb954e27a4eb8956bee5ddf013eef0))
- *legacy*: Syncing table schema concurrently ([0ba021f](0ba021fbcd259c1ff4dc3d741ea2d06746a28a57))
- *legacy*: Sync table data concurrently ([9fb8dcf](9fb8dcfa94bf03826e59c058f5505057aa23b38b))
- *systemd*: Support environment file for taosx service ([aa2c5a3](aa2c5a3415045b5e9a6d4bb406898c36376e7c84))

- Trigger with cron-like schedule ([07fc193](07fc193f93b6148488b67189612bb163d0f21223))
- Limit batch size and interval in legacy mode ([21ffc1c](21ffc1c43e40d1da418dadd6fdedd2f794f25eda))
- Ipc reader/writer based on arrow-ipc ([154fe59](154fe591b0019b7f6cc092ef1f4864dc57c1159a))
- Add tcp listener for windows & unix ([ea8c112](ea8c1128a27d8876cacb43a628afd65cb6367afb))
- Support all types of tags ([9cb30e3](9cb30e3ed70c4d3b643c0fec82315a71ca46297f))
- Support primitive types of columns ([b92d01a](b92d01aabb40f28a0c289446be722edf7f6a274b))
- Support binary input of columns ([ed87da3](ed87da304ea48d24498a1e23d1a0766215dc5cc7))
- Print metrics when done ([66124ac](66124ac6b0db04528c9935170abdf855598a9564))
- Support table_name column ([0114dbf](0114dbfc25406118941c1f1ed7deed2fbca1ba90))
- Print metrics when done ([64d0594](64d05944f62543cfa1a907d68a8a6f6aad75905a))
- Support pi to taos ([79903fe](79903feb24080154069b22765840f732695b8f83))
- Add pi to taos support in CLI mode ([e8eefa6](e8eefa60215b4ec7896463a1f7199a2274f7932a))
- Parse any value from str ([bfaae68](bfaae6823b72c14e1093e3880211f83cc24092c4))
- Support opc to taos ([51548a8](51548a85286d7683112414db84559f4024d528e4))
- Add /ds/in/sets endpoint for listing dataset collections ([562ec03](562ec03f13722e6d53ff980f53d04fab4ebe08f4))
- Use CUS_NAME/PROMPT env for OEM labeling ([5eaa5a6](5eaa5a64b5889d4c9074b92b6465154a58189b23))
- Add taosx-influxdb project codes. ([79b8a0b](79b8a0b9e9bb2e45daed6042afa8feabdb7cef2e))
- Split opc ua/da to different data sources ([4c07c64](4c07c647d2e10dc68d809710cad21009a89a4772))
- Add pi/opc and packaging scripts (#154) ([0a0b94f](0a0b94f91d17cf153d6e9d5272921a89ad27727c))
- Support process flat message ([45ea754](45ea7549673f3ef0fa6044cf6646b275c6e13700))
- Support -f mqtt -t taos(todo start mqtt program and transfer data to transformer) ([88fe6ae](88fe6aeabd2e4e57cfb6bc3316ab7942044b5139))


### Refactor


- Add with_context for Err ([b2bf07e](b2bf07e16acb6ad53b31a8488319a45105c06171))
- Modify example add more row ([ba04e91](ba04e91da81025c8ee029521b2d6395b0167054f))
- Use stream module ([cce50b4](cce50b412984a085b69318cdf989605b5dba91a7))
- Support point data insert ([7538cbf](7538cbfe5b2085cf3a174056d2fdae696b48f391))
- Add point stream example ([3fdf02c](3fdf02c55763fda0d884a54839cdc1a352ba38c1))
- Add with_context for Err ([2b9f110](2b9f110e2c2a839b71c6c4620809bc74ee76735e))
- Close remove warn ([4f41fb2](4f41fb2adeece9dc3eef22a72324ac797110ed4b))
- Cli add opc to taos ([eba0e7a](eba0e7a95778483be0a463bd3b28efc8213c45b7))
- Modify param required ([f67dd9b](f67dd9b6ca10b222b6e081f59ea800560197dcda))
- Construct endpoint with add and subject ([653008b](653008b4346b4bf961a9301e0ab8dbf8b5a0c374))
- Construct endpoint with add and subject ([178b7c9](178b7c9447256f6b49135ef3a2ddeb751c619957))
- Modify fn get_string_vec_from_param_or_file return type ([4d24543](4d2454308619c3c109fa6c8381e0978d3e18199b))
- Support read ca/cert/ca_key file ([b893304](b8933049fc88320e92f12fcb508c578fb6b9524e))


### Dep


- Remove palaver ([2134d24](2134d248babc640702a3e665fc265d906a33356b))
- Update taos v0.5.3 ([f0c6842](f0c6842de87d14ea61212f19b82c01d224836dcf))
- Update taos to v0.5.10 ([7f9db13](7f9db13a5e8c48ab69a6a1c1e9d8a4619bed6a5c))
- Update taos v0.5.3 ([89e1bdd](89e1bdd7e5351094fbce1f0ca789fb8633e24d12))
- Update taos to v0.6.0 ([3e14e4d](3e14e4d6d20a74c335b95206fb886d3d14e8e365))
- Update taos v0.7.0 ([85605c5](85605c5ce621d1ff1aef8a66ca1b47a9fe564a89))
- Update taos v0.7.2 to fix windows bug ([4b151bf](4b151bf3c4863684d249ff1ba412b7807871e566))
- Update taos v0.7.5 ([83ac3ac](83ac3ac8c6236f5880ae5fafc1710e8efaeac494))


### Readme


- Install path ([d9671c6](d9671c633c51f7fd682c0062ad93058262bc339c))


### Tmp


- Should not be pushed ([1cfa974](1cfa974fc76b251c9d3f86c04753149ac36fa0ec))
- Pi to taos ([2c119bb](2c119bb142b80b735c52ef1b85d917b500f61b85))


### Typo


- Feild -> field ([96ef718](96ef71883ddecc87856da2d7242ac13bb92d64b7))


## [0.5.0] - 2023-02-28

**Full Changelog**: [v0.4.2...v0.5.0](https://github.com/taosdata/taosx/compare/v0.4.2...v0.5.0)

### Bug Fixes

- *legacy*: Fix prepare error when result is emtpy ([6b805e3](6b805e3097e301fd56aa724e09f6ec00f6b243d1))
- *serve*: Fix label filters ([029d29d](029d29db21142c410534ae6e77997c02764922c1))

- Do not insert into tasks when fail with clear ([b79108c](b79108c390be783170bfc3968d89cc3c72578ad8))
- From main to 3.0 messages error handling ([1f6181b](1f6181b9afdb4d5abeffd82846fa741ca4b69775))
- Fix macos compile error. unknown field in &Process ([96e9724](96e97244e551a3ae4ca173b9c8a9cacf6ca9a9ff))
- Fix labels filter conflicts with stream_type query ([6f3b0bf](6f3b0bfa64e06260dbc7e01814f8c500b917c890))
- Set default connection timeout to 5s ([e99e481](e99e48190433585114daf7129ee55ac88bc63179))


### Features

- *docker*: Update TDengine to 3.0.2.6 ([92c89ff](92c89ff78ef50e32007ca3eab255f07686ab1a37))
- *serve*: Support task name and trigger settings ([462e85e](462e85e9f71a7be0cb5cd160091e4191d6580e8a))

- Support `select-with-stable` and `tables` params ([898b79b](898b79be99acf55583a6b2a4aa9f45bd724653f9))
- Add data source input tasks API ([faa4043](faa4043a9b8aae01fcb1e5601e298bbc2bfb78ac))
- Expand task detail with datasource definition ([2512928](251292888140e83a60fb08ba66e56c119466fe6d))


### Refactor


- Add labels to tasks api ([c1af2d1](c1af2d1afc50cf5216d1354b00f7c3b65a53fb67))
- Remove stream_type, from_cluster, to_cluster. ([0d00a25](0d00a2598c9f00f0331d0872aaeecb803654142f))
- Apply grants checking for cloud and enterprise ([1c3c367](1c3c367ff358b03637f5f2e71ec0e4bea94b0826))
- Do not check database exist ([e611388](e6113880e81802f66fcc23def1f04ba07696a18a))
- Authentication definition changes ([9a7360c](9a7360c102e2693e062d4b66c02adda5c789ed4d))
- Remove username/password in options ([8e61903](8e619036db5e4e63576a240785ea6aa548639447))


## [0.4.2] - 2023-02-13

**Full Changelog**: [v0.4.0...v0.4.2](https://github.com/taosdata/taosx/compare/v0.4.0...v0.4.2)

### Bug Fixes


- Fix database locked error ([3c56e23](3c56e238a27c9d480594e60c3cc3e249a353ae23))
- Force dynamic linking in musl target ([907d2e9](907d2e92feaec7e02c50bbad8a0e5a2d93154ff5))
- Fix .env error on windows ([e1ce433](e1ce433c38520c41bc166cf4d795035b03fb52e5))
- Retry write raw in case 0x032C ([7bbf11a](7bbf11a534d33499d77f1c8715e1386f381cfff8))
- Apply new version of taos-query ([e8feac4](e8feac4fec8be467aa1c2f8469a12a19032dd455))


### Dep


- Upgrade to utoipa v3 ([fa978bf](fa978bfffa90d1c650006c5483890295d618e750))
- Update dependencies ([59f53cb](59f53cb1c8791177e41acafd69a89226bacc3a55))


## [0.4.0] - 2023-01-10

**Full Changelog**: [v0.3.3...v0.4.0](https://github.com/taosdata/taosx/compare/v0.3.3...v0.4.0)

### Bug Fixes

- *lagacy*: Fix stmt wal size limit error for legacy sync ([4e122f6](4e122f6d289e2c4ee5b1fb08aef39e80dfa9c66a))
- *metrics*: Use sysinfo for all-platform metrics collection ([83768ad](83768ad2a0ae9a667af3b84d7fb242faa5a6ccaa))

- Upgrade parquet to v28 ([42d2592](42d25926184180460c6d51ac3e39754b51f30d68))
- Fix 0x030B data expired error in 2.x ([ad83395](ad833958e0279ff474b0b6f9b8b0df07b8ce2c65))
- Fix max sql len limit ([ab14320](ab14320eec00fabc7792dae9c2ec3f69a199076c))
- Fix table schema sync in legacy mode ([fe0cf00](fe0cf0090325c0b50b0048ef8f710d8406ba328a))
- Fix segmentfault at exit ([a424d46](a424d46a0c843450c1b1405673695ba328daaaff))
- Fix sync error with delete from tables ([d2d3fe2](d2d3fe2fcca688706ead9b49cb72e403d9707d13))
- Fix sqlite url error on windows ([118946b](118946b0f07bb020b604f560e114e498a09db822))
- Fix table name escape to solve errors 0x0362 ([80d3bd4](80d3bd45f868556530d472283b3876b10f02fc2f))
- Fix grant check failed in 2.6 ([8871929](8871929f9c6f262de72c9595664708693341c6c2))
- Fix wal size limit error not catched in scope ([10cacaa](10cacaad5860e70b2656cee7b9ae0945b6b11f25))
- Add batch-size option for legacy write to 2.6 ([54889b9](54889b9554838c4550351980b62e0e437c48a830))
- Check handler finished when start a task ([34cfaa4](34cfaa407d59af733ea3928ba58e90ba899c46db))


### Enhancements

- *serve*: Support tasks filter by `stream_type` ([fb8043c](fb8043cde3c3443a5fbfe7b13ff705641f544e5f))



### Features

- *serve*: Support `after_delete` action for tmq to local ([fab76c0](fab76c00368d2e6f17d2957e435e1ee3bd4117ff))

- Support multiple workers for legacy sync ([9d88f20](9d88f204d972d1912be3a218ffd48e7fd4d09bf2))
- Support enterprise only validation ([3d4f76f](3d4f76f040d926b075b4a196bfe22a51e4549eca))
- Add feature gate `disable-enterprise-only-validation` ([0fe672b](0fe672b3a7246a5e24bf881798a21c3339599535))
- Support stopAt task for tmq to local ([5a82acb](5a82acb87dd51dc7a5265e0764efa98374dfa695))


### Dep


- Shadow-rs v0.19.0 ([004d7a3](004d7a3e0c1534d3da7377e79e5f541326c51543))
- Update shadow-rs to v0.20.0 ([d27a241](d27a241ab37268a64584f6d16489819d360b2273))


## [0.3.3] - 2022-12-10

**Full Changelog**: [v0.3.2...v0.3.3](https://github.com/taosdata/taosx/compare/v0.3.2...v0.3.3)

### Bug Fixes


- 3.0.2.0/3.0.1.x compatible ([c7865be](c7865be434fc403b15c63163f7b2b9a41c334ebc))


### Enhancements


- Use native-tls by default feature ([0572ad1](0572ad1901c0fc91a169e3d74211137e2eb742c9))


## [0.3.2] - 2022-12-10

**Full Changelog**: [v0.3.1...v0.3.2](https://github.com/taosdata/taosx/compare/v0.3.1...v0.3.2)

### Refactor


- Support both native-tls and rustls ([58987c4](58987c4bade7427246d91b819d81d5fbf57228b4))


## [0.3.1] - 2022-12-10

**Full Changelog**: [v0.3.0...v0.3.1](https://github.com/taosdata/taosx/compare/v0.3.0...v0.3.1)

### Bug Fixes


- Update to 3.0.2.0 in docker image ([e366f82](e366f821ce5a417a1d173286f04d2f1df0b2b1fa))


## [0.3.0] - 2022-12-10

**Full Changelog**: [v0.2.1...v0.3.0](https://github.com/taosdata/taosx/compare/v0.2.1...v0.3.0)

### Bug Fixes


- Fix panic when clear target failed ([a63da7e](a63da7e71613d6781d2940e8f07e6d24cab1b0a1))
- Fix compile error when target/ deleted ([5f4335a](5f4335ae51e61929f07fa00e8182739cd92e1608))
- Fix sync override with partial updates ([d5cb7ed](d5cb7eded3a2d44c18bf6ec89a302900bd567dc4))
- Fix can not use keyword as database cases ([0e94b25](0e94b25660fdd15cc608d3d871c3532576e63cdb))
- Support utf8 table names in sync ([edbf6bd](edbf6bd420e32f8957b71f6d716667787f57181e))
- Add records per second, points per seconds in metrics output ([498f433](498f4338caaa199ce17b0683188d0da4419770de))
- Fix v3 to v2 subscription error ([0b32a1a](0b32a1aafe604db0ac42b550bf2496788b2cf10b))
- Fix timed out error when use large vgroups ([0bf61de](0bf61de0485962e5c405899d184044900faaf27b))
- Scc v0.12.0 ([f9311fd](f9311fdd8ad49565e671af209d3aed80c086e52b))
- Use dashmap instead of scc ([3dbc96c](3dbc96c2a33b583ac2eee887d30435e164f6589f))
- Remove use db in tmq2taos ([21e8c34](21e8c34a380faa6701bd14abb714006a4efe2da9))
- Drop consumers after all tasks done ([6ebf3ef](6ebf3efc0f380b5a3debf408fade5c97da0868dd))


### Features


- Use rustls instead of native tls ([72cae69](72cae69217071ed493ef2725fd7d085c0e771bb6))
- Print performance metrics at the end ([e5ce5f5](e5ce5f5ff7c119edf1c47c89227a8513795fbfe5))
- Support almost realtime synchronization in legacy mode ([b851de9](b851de90f998a69d27b20929516224b97a2e777d))


## [0.2.1] - 2022-11-18

**Full Changelog**: [v0.1.1...v0.2.1](https://github.com/taosdata/taosx/compare/v0.1.1...v0.2.1)

### Bug Fixes

- *serve*: Decide to connect or not by error handling ([0dc31d7](0dc31d7f341b35c99eb721a9942c01d95d4995c0))
- *serve*: Fix websocket connection with 401 unexpected error with HTTP ([6cbc772](6cbc7727cc2457c6d91ef8ea74ca1898badf2930))
- *serve*: Default listen to 0.0.0.0:6050 ([38053ab](38053ab9a98697053dbdb8653a005da610b4c2c8))

- Support timeout=never with websocket ([3d10b60](3d10b6011968c3509aafe757cd0c6b1c9d8dc182))
- Let tmq tasks cancelable ([78c8db6](78c8db69561fd9ec43c3a62d0b11e81651c21290))
- Fix cancellation unexpected errors ([441dc9d](441dc9d8d5ebcfc528646e4b0dc84d222aa931f8))


### Documentation


- Add API documents for task start/stop ([be04505](be04505f3a1e18377c9613b1d9b7e7566cc66719))


### Enhancements


- Re-connect when websocket connection closed unexpectly ([7e0ab44](7e0ab4442724a452cbd733966229174bafd1db28))


### Features

- *serve*: Try re-run tasks when timeout=never ([da704e8](da704e8f29a23d20987a38d67f44abdcd1c8450f))
- *serve*: Add /tasks/count api, also work for HEAD /tasks ([61bfd8c](61bfd8c3b858da4f3301bdba31db8417c10ab75e))
- *serve*: Invode all unfinished tasks after restart ([b78b7b9](b78b7b918f7b9064217a0daeaf8ed4170b61ab17))
- *serve*: Support clear target database in create api ([d96b9d1](d96b9d1a445c3ad7aa4cad9bee306a4e31c5a433))
- *serve*: Support oneshot topic for task ([2ae2e4f](2ae2e4ffaf3db91a75513f99e9d95e904a44c339))
- *serve*: Add PATCH task/:id for update a task ([9c6e8af](9c6e8affb8e5fbc4d65dd6853b6bf2332c6b0f04))

- Add Dockerfile for taosx/serve:0.2.0 ([c9496e4](c9496e4cf0dac861696fd6ad9549a8660db523e5))


### Refactor


- Task schema change and tasks/ api search filters ([7401aa4](7401aa40b49ea2ab36ea8348187e8f2456f858e3))


## [0.1.1] - 2022-10-31

**Full Changelog**: [v0.1.0...v0.1.1](https://github.com/taosdata/taosx/compare/v0.1.0...v0.1.1)

### Bug Fixes


- Fix cargo install error in build.rs ([1426488](14264884a6cf2045e5f5d9c49a500c2d585e7135))


## [0.1.0] - 2022-10-29

### <merge>[taos]


- <merge from main> ([f854615](f854615ecc7ac2717a6f7316fa4177aeabe06306))


### Bug Fixes

- *libtaosws*: Fix example error with old version of gcc ([73dbfad](73dbfadb461c74e48ac8dc7355436747f810914f))
- *local*: Create database when target not exist ([146d102](146d10269b20f7c55d954c73d2b32ef4d3a4aa14))
- *local*: Fix meta data inconsistant when loading multi files ([357d55c](357d55c8110a12e1b9c00a013be6aba29aa86cab))
- *mdsn*: Fix dsn parsing with tmq support ([90c0523](90c05237cdfe5ae88b0bf2e55c48b814b1097939))
- *mdsn*: Fix database characters error while parse db with _ ([68e9708](68e9708e026939abe5a3cca0ca8070a5106638db))
- *mdsn*: Fix `dirver:///` with empty db name error ([ba23f3f](ba23f3f9a36a379138f0bca9a66deb5a349edcd6))
- *query*: Mark async dependencies optional ([62cc86c](62cc86c9aa971ab481b20549f2ef18f4bbd29561))
- *query*: Fix nchar view error when there're more than one ([fb1e441](fb1e441bcf8e15a865840a0ac1bd37fc4e5a2f72))
- *replicate*: Fix replication error for child table only ([be7cb75](be7cb757b7b99b86c9a0e72c0dde25eca1dc6908))
- *taos-sys*: Update taos C bindings ([6dbd198](6dbd198b7ca2905b564d7511f6a892d6317c8bb4))
- *taosx*: Add missed other data type in creating sql ([e1099b7](e1099b7ac6d5dee958a3af06e309f5a76cd0d425))
- *taosx*: Allocate tasks for threads ([a3ef4ac](a3ef4ac94265f7dfdbd3d720e8f89dc47d875af3))
- *test*: Fix taos-ws second-test coredump ([ccf0cc7](ccf0cc7ea00ffbc1e8080284fb5f17b7aef11411))
- *transform*: Fix template rename error ([09b8242](09b8242321b2c746874aeb058d7e88a74faa4a65))
- *ws*: Fix raw block error for TDengine 2.x ([6fe2bbd](6fe2bbd4cee748ed97d0de44eefcc37351059799))
- *ws*: Use optional dependencies to reduce size ([1868295](18682952d5e5aad5a777ffc297313d69f49c280e))
- *ws*: Fix v2 error when use with show-databases ([8bed0ed](8bed0edd88cdaae7c5c17487d71d3c2701d58c54))
- *ws*: Not block on ws_get_server_info ([69a811a](69a811a75b0125a5e6f0fafc81f009075a903a24))
- *ws*: Use ws_errno/errstr in ws_fetch_block ([798d29a](798d29a748178f3bb2c269a39c8b7e57c80cbae2))
- *ws*: Fix hang error when connection closed ([59cf1e6](59cf1e6e5e7ab80bc4d78562cf6648f02e1ed02a))
- *ws*: Fix stmt coredump with non-null values bind ([988ba2e](988ba2e02052750972736ff0189bd63dd5bb9d49))
- *ws-sys*: Fix duplicate defines error with taos.h ([8a5864e](8a5864ebf47e8ef718e63342ba35789ad0b4e842))

- Fix taos_result_block in v3 ([ac9045c](ac9045c25d9b487443769e46c07d9dcc9860b02a))
- Workers = 0 error ([a405fde](a405fde639d137f772e242cfea72b2f464df2c8b))
- Update tmq error handling and offset changes ([f35c781](f35c7814ac692d81bb2df550841c5cd163a4bc09))
- Handle ctrl-c ([887b51b](887b51b327a55bc4e448e700476355d0e78f6727))
- Refine dev pipeline ([0b35884](0b3588478102e1eafc39ed85592fee86612571e5))
- Fix database/table name error for normal tables ([0b16941](0b1694163b76f9741fbfba21cd6b2e5ac072641e))
- Support subscription of stable or normal/child table ([14a0c5a](14a0c5a8618861f57d08fdbd01d10261272f0e4e))
- Fix table name not exist error instead of panic ([1379229](13792292ced0e9af9d593f92daf24b868b6500e8))
- Fix tags lost using with `tmq:///db.child_table` subscription ([5d07a8b](5d07a8b3b5009f125870ff678bc6d540c2076feb))
- Fix rename tables error ([b8855f1](b8855f14f32029446d5e25c8355bd0fb96eeb276))
- Fix data lost when backup multiple times ([60ec8e9](60ec8e9f6328d48d000908f566719af33c1351f6))
- Cloud service can't get create database sql ([a483190](a4831904bbecca57f94cf74c5bf94ca968d2e6b4))
- Set vgroups to default 2 when permission error ([adf51a8](adf51a83733799c6d69a7ba37c42335a18393031))
- Sync database parameters when target not exists ([6221b47](6221b4738bdcf0abecd8ce67b3a509a3f4132c20))
- Fix coredump when failed to connect ([361ec0b](361ec0bdef4adb55e61501cc09a710af9691016d))
- Fix write to cloud error ([d936a8f](d936a8f5108de114c278469516d3333a2aaed518))


### Documentation

- *macros*: Add document for c_cfg and test macros ([6cbaa32](6cbaa32eba8d7b0b9cfe4181de60937e06380793))

- Update README document ([ddf9e73](ddf9e738cc591c9111c6fdae91e12a357c8b0f69))
- Update README for table sync/backup ([0257a8d](0257a8d7f25653ce7adc6177839c15be69fa9578))
- Fix username:password typo (unexpected `@` in `root@taosdata`) ([0c9efc7](0c9efc76236618b4870c2b5968e282951023fb4d))
- Apply document refinement suggestions from jeff ([db064d5](db064d50a7cd603cc7ffe910ce90273005bc9254))
- Refine transformation doc in README ([14accc7](14accc7374e6930e8567e7fd05561247e5eea1ea))


### Enhancements

- *tmq*: Tmq API improvement and stream/sink test ([5c89e3d](5c89e3d39595f644817c93849c6278be62ceb32a))

- Add docs and helper methods to C ([e4c3447](e4c344764d9155fa2adcebe777948b234421444d))


### Features

- *libtaosws*: Add ws_get_server_info ([c348602](c3486020e74cc2f699cc1cd2ae70f8537a1055c1))
- *macros*: Add test macro to simplify tests ([1bee64e](1bee64e5da4bebae54b70dead61b4d84e23518cd))
- *macros*: Use crate/taos wiseness ([2224ca8](2224ca80632d36ecc1a44b2df28a9c2e982f88c7))
- *mdsn*: Add a dsn parser for taosx ([820bdb5](820bdb5c68fe030bbf0d6ca06f1b75e0bcd87e90))
- *mdsn*: DSN parse and display ([608afaa](608afaa25498b9397eee1a071f7d33fac230bf15))
- *metrics*: Add process metrics ([72af4cb](72af4cbe42fbc04f7adea82928c066f20360d793))
- *query*: Stream basic ([c0cfd82](c0cfd8296c846634808a1649d1cf9119a726eae4))
- *query*: Add columns_iter in block trait ([9ede3e3](9ede3e3d231a156e1e5a88f2b184d97e484977cd))
- *query*: Add (query|exec)_exec methods for async query ([15a5501](15a55013af8419da5c0fafbc80156acc6d6e17f6))
- *query*: Compatible to 3.0 query/stmt api ([0ef98fa](0ef98fa5c4ccc9d4482aa10585f266ff93c79769))
- *sync*: Add sync subcommand ([87468e4](87468e49d70c368cbc8e3b4936ebabe3a23bd8b2))
- *taos*: Support r2d2 pool and stmt ([a7ae84c](a7ae84ce4a3af7c3b67314c9ce15e9683a7b8f39))
- *taos*: Support serde ([bee71e0](bee71e00dd14ca5a19457574c43a0620469ebb25))
- *taos*: Support deserialization of json tag ([1449137](14491373d97865744a2ec8e5e251bffdb1553c71))
- *taos*: Support desrialize with serde ([e9132cb](e9132cb2538dfa0dd2e3b6a034946564b4cc4e39))
- *taos-error*: Separate error crate for both rest/native ([7501437](750143776059e73ea5a47f8cf53ed4382df8b2af))
- *taosx*: Refactor backup and restore ([7b48b1d](7b48b1d05847a3287a9c24cf7296196aa2851458))
- *taosx*: Support CSV/Parquet export ([54af092](54af092b9f9c9c019a1a0faba3455bd017e0b5d0))
- *taosx*: Export query result to CSV/Parquet. ([ca47155](ca47155056357050e4e476ee9afcd06613f4086b))
- *traits*: Add helper methods for connection ([0202bf4](0202bf40b72bedb85c6e5f0f5cf9850e00f935cf))
- *ws*: Add C STMT API for libtaosws ([9dbd208](9dbd20864910a562ac3471a52b7d45cda7eda289))
- *ws*: Improve libtaosws api ([fb6854d](fb6854d3c7ffad7d639ac8f56cccdd5d50c97073))
- *ws*: Add ws_take_timing for taosc execution cost ([8fcae0f](8fcae0f010ab1e9ef1742522e9143ca9461752c2))
- *ws*: Add ws_stop_query and support write raw block ([160cbe8](160cbe8c2f623927175f1caa491f5e9995655a53))

- <refactor parquet schema and d/serialize(WIP)> ([28e3fe2](28e3fe2c7e0f0111ea6b80c3d2ebbc41ad2e4755))
- Stmt API for both 2.4/3.0 ([ff119aa](ff119aa69cb786668d7f3083c4a71cffd441629e))
- Add sync subcommand to tasox cli ([6ec0dd4](6ec0dd47c5ebb10b3a5f2a89d39bf46b404fccd3))
- Add transformer ([128607f](128607f93ebe0aab5d896dbc2b087bf1baa31ea2))
- Add --transform in sync cli interface ([6390e1d](6390e1dbb23415ff9025dd88eac7fd591d790204))
- Add raw block api ([f5a6820](f5a6820f99bca702f0b1dac845b1dcd12f17b327))
- Stmt on websocket ([474ab87](474ab87831c62fe86b24f14551ad81a6f5b9e655))
- Backup/restore from TDengine to local files or opposite. ([c4a65ad](c4a65ad1defd0aac2bb3c45a8794f1b6a11642bf))
- Finish REST API ([0af928a](0af928a84371a129d63cbed466e262b4e590bfb5))
- Add --debug option for file:line debug prints ([0798ea6](0798ea6630045771d986905569f194aca7c401dc))
- Expose `metrics/` endpoint to OpenAPI schema ([c220798](c220798dc650717fe13b89b8be4cbdd4b98191ed))
- Support transformation while replicating ([904ce7d](904ce7d588c639c6d500d08026d74e99c0721b71))
- Support 2.x to 3.0 migration ([852d085](852d085ae6379c039b863c0f1ebefef2e6944e8e))


### Performance


- Reduce binary size ([e93b1ce](e93b1cef2208f21d62610d91ba3b2b526848be30))


### Refactor

- *query*: Refactor query lifetime ([1ee3228](1ee32286937d3f3aa93cf4deecfafaab281c61e5))
- *query*: Async query ([97e4397](97e43972b8e07b08425a1946c0d00d514a235e8e))
- *query*: Make query result queryable even for non-query sqls ([0656636](065663601b1047373ef12380bddae4d6181046de))
- *query*: Use the same type for both async/sync result set ([51dff8c](51dff8c493b7ba7ab105f7f4a61ed22f2359b8cb))
- *query*: New interface for query block ([3abfabd](3abfabd50fb33b58eb060598a660afcf2536b36d))
- *query*: Refactor query interface, apply changes to native/ws ([d9a1e9e](d9a1e9ea9d66ccfc77f8cb58315eff1b0e90d956))
- *taos*: Tmq consume with existing ResultSet ([669acaa](669acaa18d90514745e20c49b1725fd1ae20f2a9))
- *taosx*: Fix backup/restore in case of taos changes ([64a47a1](64a47a19339417019cefb25f48ee4ac53f1921e8))
- *tmq*: Update tmq message API [[TD-14604](https://jira.taosdata.com:18080/browse/TD-14604)] ([380616e](380616e53ab252cef501b469e05f66dc8d321297))
- *ws*: Rename library name to libtaosws ([ec23c4f](ec23c4fa4d2b25dfcf102cdd04b2416fc989ed5a))
- *ws*: Use stable channel ([552b332](552b332a3f12e932379235c29856f3d8096081e9))
- *ws*: Refactor error no/str in libtaosws ([8d5bf3d](8d5bf3d9fffd4ad45f2d1f5892731c769f7925d0))

- Separate common traits and types for both taosc and rest ([02744a7](02744a7da1e128e4cb926a8a6e3492794af07cd9))
- Use taos-query in taos-sys ([1da7e58](1da7e58920973b1715620c14a4bb26b9fd581453))
- Taos-query trait refactor ([e3fced3](e3fced381a778958c6152d4f8edaae6747b2b796))
- Use rust typed field struct ([e3a3de9](e3a3de9a054be33cd0a8e82ddd2f50b2a1365805))
- Use field ptr ([15db540](15db54039654a164c279664df20beb9e96ae87a3))
- Improve public method ([d947d49](d947d495d1d40a2d55b701cedaf6a277489bc4e1))
- Remove TaosResult for phase 1 ([b5cfb80](b5cfb80e07640d35083f40499bc7021e924193c8))
- Use query for all cases, add prelude mod ([07aff18](07aff188efc3126ccf9f896c9e9e9365f1bf585b))
- Tmq consume fields for each block, commit API improvements ([a56e6ae](a56e6ae6873465e536034e9a41804b0c9936cb49))
- Minor changes of taos-error and taos-ws ([6bea2cb](6bea2cb62f540cf90117f1d871550f4bf51af9ae))
- Raw block layout ([537fcc6](537fcc6471286c5e4e5f4d79b9223a0610c40c5c))
- Select tbname, tags from stable changes apply ([ca7281f](ca7281f5a5f127a92c6aef4e1a0a3e6c7a5aabf4))
- Api abstract for query and tmq ([b5cf452](b5cf4526dae32cd02fa13db48d816b65d8846cb2))
- Fix query layout ([a7a2813](a7a2813cc009b830ef5d8f43d697d83863b592e8))
- Taosx backup/restore/sync workflow draft ([2974d7e](2974d7e5776e0da6dc756374bf0139c9c4184523))
- Internal changes in case of raw block data structure changed ([0f20033](0f20033655a4c54bcb7b545d049d6af4ff8a00ed))
- Update to taos v0.3 and adapt metadata message type ([b2ea716](b2ea716ccc825072d643bb32598ae125e9c1d909))


### Dep


- Update dependencies (zip, parquet, sqlx) ([665fbf5](665fbf540acae00eb6fbb0adebdd77fcbdaff53b))


### Deps


- Upgrade parqute to 12, remove libtaos/taos-sys ([f2450ca](f2450cadad121e738e30dcc7250eda20a5734772))


