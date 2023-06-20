<a name="v1.0.0"></a>
## v1.0.0 TDengine Data Replication Toolkit (2023-06-20)


#### Features

*   support jemallocator as global mallocator ([f8188334](https://github.com/taosdata/taosx.git/commit/f81883349faedf758d5da7f50b9da60941beb5b0))
*   adapt different schema in legacy mode ([900bd8d5](https://github.com/taosdata/taosx.git/commit/900bd8d50a7012e883fa68fe148f1ec6658c63fa))
*   support lang in /ds/in query ([ef38a14d](https://github.com/taosdata/taosx.git/commit/ef38a14df8e332ffd676ac6a5427dc66b5e3e4b0))
*   add disable-enterprise-connector-validation feature ([5ebc04fc](https://github.com/taosdata/taosx.git/commit/5ebc04fc7318977e0516297f5657183a4628ac6f))
*   rename all plugins exe name ([9fd7d92d](https://github.com/taosdata/taosx.git/commit/9fd7d92d16aef9de5d7315f28e4bd211022acadb))
*   add parser field in mqtt data source ([464181d3](https://github.com/taosdata/taosx.git/commit/464181d36c5964ac2ddebf5c6838ea2cc4cc3048))
*   support partial columns in parser ([3f292aa6](https://github.com/taosdata/taosx.git/commit/3f292aa67a436687f4641a0b5d2334f5f0e75ac6))
*   tracking with connector usage and license ([7079da29](https://github.com/taosdata/taosx.git/commit/7079da29ce3d377fbfb256fb247f27dbefdcd128))
*   get offsets ([c3f67a27](https://github.com/taosdata/taosx.git/commit/c3f67a27d9f34a816a88c0dcb903ba63245343fe))
*   support parser in cli mode ([5b796a4d](https://github.com/taosdata/taosx.git/commit/5b796a4ddedea41dbb09c6f565bb22e7e58fd04f))
*   support parser in /tasks API ([9501fd27](https://github.com/taosdata/taosx.git/commit/9501fd2748ff38b4e0de1684d1b9ac2ff930cfcc))
*   support json parser for flat stream (MQTT payload) ([28ea638f](https://github.com/taosdata/taosx.git/commit/28ea638f08dc74d31c68e4c78b466e509cf4a510))
*   support pi backfill ([adf53c33](https://github.com/taosdata/taosx.git/commit/adf53c33f3aa86d300c2ff874cb96198ec031852))
*   add influxdb connector. ([dd4c417a](https://github.com/taosdata/taosx.git/commit/dd4c417acb4aba77108ed867df9e3ee6b1335a15))
*   support mqtt ([2defcef7](https://github.com/taosdata/taosx.git/commit/2defcef79dc5b6cf4c52d7a2cd35f936eaab6c62))
*   add taosx-agent CLI and RPC service/client ([b3a0eb3a](https://github.com/taosdata/taosx.git/commit/b3a0eb3a09039093c158d72f15c7726bfbb5ffca))
*   support opcua subscribe and change timestamp for opua observe ([126340be](https://github.com/taosdata/taosx.git/commit/126340bea6b19b8ed8a0819766d3e8e95b0d20b6))
*   add agent related API ([3d244b0d](https://github.com/taosdata/taosx.git/commit/3d244b0da075e884e69d38c01fa4c65c70f53902))
*   add taosx-influxdb project codes. ([79b8a0b9](https://github.com/taosdata/taosx.git/commit/79b8a0b9e9bb2e45daed6042afa8feabdb7cef2e))
* **agent:**
  *  add window service and toml file for Agent ([df847c67](https://github.com/taosdata/taosx.git/commit/df847c67656c622b6a2f71f4b09a178f856e2e5c))
  *  always retry when connection closed ([2bf839ea](https://github.com/taosdata/taosx.git/commit/2bf839ea2cd656c381f6f34e234e659c136dbadf))

#### Bug Fixes

*   split large query into chunks by a time unit ([05991ad3](https://github.com/taosdata/taosx.git/commit/05991ad3cd03e8c42de2c631918f7e9a39913d5e))
*   loop untail raw_block success when 0x0911 ([15f5535c](https://github.com/taosdata/taosx.git/commit/15f5535c6a52d55edb5a10c99bbea55406f62a06))
*   remove token in log ([b28bcd02](https://github.com/taosdata/taosx.git/commit/b28bcd02dbc657974f3f756acb76e85a5d247123))
*   follow latest packaging rule ([b170f24b](https://github.com/taosdata/taosx.git/commit/b170f24b2e61c697f24adf45bc2766b26186efeb))
*   fix ParseIntError when version is "3.0.5.0.2023" ([0ec07d54](https://github.com/taosdata/taosx.git/commit/0ec07d548b68f8bf6c78efca79ad1969435547d9))
*   TD-24852 ([7e67f328](https://github.com/taosdata/taosx.git/commit/7e67f328768a813abdf09f68b82df7496fd78b14))
*   dir mistake ([c081a936](https://github.com/taosdata/taosx.git/commit/c081a9360c473c90eeae430c5b65d34bd4b3eedd))
*   typo of explorer ([b02864d6](https://github.com/taosdata/taosx.git/commit/b02864d6e41b7bb0b094ed657fe7a2af18aba677))
*   remove log token ([8ba706bc](https://github.com/taosdata/taosx.git/commit/8ba706bc7d5a13e294bd8af7e1861ebd41bf9c4a))
*   remove token in influxdb log ([19c8e19e](https://github.com/taosdata/taosx.git/commit/19c8e19e84990bfe0101710257893aec0d805031))
*   fix for opc da reader ([0666e7b8](https://github.com/taosdata/taosx.git/commit/0666e7b89936cde020cf499fe7f84a693cf01241))
*   connector lincence number check error ([9d6067c5](https://github.com/taosdata/taosx.git/commit/9d6067c59c6d501a6dfd6001c1658e9498a0e04a))
*   fmt log ([30e9133a](https://github.com/taosdata/taosx.git/commit/30e9133a1bf7b4edbe3ab5251aca2c0cb089b628))
*   add cargo rerun ([9d946e4e](https://github.com/taosdata/taosx.git/commit/9d946e4e93bd415a266789549573b058fc98185c))
*   remove token in log ([65889647](https://github.com/taosdata/taosx.git/commit/658896475d83c91dab550d52f58beee7b8c95436))
*   support handle [0x0618] error code ([1cc09a7d](https://github.com/taosdata/taosx.git/commit/1cc09a7d2d157d777753ad7667ee8983a6b154ef))
*   refactor license number validation ([92ef9895](https://github.com/taosdata/taosx.git/commit/92ef98956252ffb0cd73afd4758cd458baa94af5))
*   unknown time ([f702d474](https://github.com/taosdata/taosx.git/commit/f702d4749e06c8688e29c54ab56456c1cc193983))
*   fix version output format ([36a53024](https://github.com/taosdata/taosx.git/commit/36a530248bea5edd91de738a82a0c8569b38be57))
*   fix parser add column with alias error ([70b0fcc8](https://github.com/taosdata/taosx.git/commit/70b0fcc8cafe5066eb77834fe0834227ea0a10c1))
*   refactor version print in stdout/log ([31be75df](https://github.com/taosdata/taosx.git/commit/31be75df1c781ec09f1c9947b21cf4548e95d78f))
*   fix parser alias not work and parse error when column not found in payload ([ccd2b95b](https://github.com/taosdata/taosx.git/commit/ccd2b95ba7682a7fdb465e93d6d797b5e0eb9537))
*   add enterprise license validation for plugins ([05bc6fa9](https://github.com/taosdata/taosx.git/commit/05bc6fa95f19cc378f199e4712faf7e8d5c15703))
*   fix number as data source connector connections ([0cb88d01](https://github.com/taosdata/taosx.git/commit/0cb88d015eebddf792a6db6bdbfd2254d173a648))
*   windows build error ([e2fdf3f4](https://github.com/taosdata/taosx.git/commit/e2fdf3f406ac9fad3932cc1491b47df9d64939bd))
*   add err of plugin not found ([53f27dce](https://github.com/taosdata/taosx.git/commit/53f27dce9e8ad265471f0b87e9eff56a78401b77))
*   use local time in log ([00955c22](https://github.com/taosdata/taosx.git/commit/00955c22b0a2cb4a2c9f911159d6c791d3f9276d))
*   format version output and fix for points ([c0271b25](https://github.com/taosdata/taosx.git/commit/c0271b25eea008cd1f0660e0e4e892782fa4bc28))
*   add log of opc datasets ([1412b466](https://github.com/taosdata/taosx.git/commit/1412b466888de87e993a95c4031bfe44476debef))
*   log fmt of version ([4f8cc808](https://github.com/taosdata/taosx.git/commit/4f8cc80880e817c867d0c1f917a5615a9dc5d00d))
*   change the output method of version information. #TD-24793 ([c15c1211](https://github.com/taosdata/taosx.git/commit/c15c1211660e8479d5dd8ce130fd405722f268e3))
*   change the output method of version information. #TD-24793 ([187425d2](https://github.com/taosdata/taosx.git/commit/187425d2f02c6cce7c2c728060585785d0ad3299))
*   add agent version to log ([256eb66a](https://github.com/taosdata/taosx.git/commit/256eb66ac0d36724d0fbadadc80ddf3da322f0b8))
*   change the output method of version information. ([5415d907](https://github.com/taosdata/taosx.git/commit/5415d90788c92f6f918f7e250e8f7e370b55f7e4))
*   delete data type from opc points ([1fe68eb5](https://github.com/taosdata/taosx.git/commit/1fe68eb5c6682c9e0b15f98c8d878a10f6f7ce22))
*   delete register nodes ([28f0c1c1](https://github.com/taosdata/taosx.git/commit/28f0c1c1d5a84dd48b1e202bdc6acf4bca773a9d))
*   update cfg path ([50b7ef91](https://github.com/taosdata/taosx.git/commit/50b7ef91162467410596f41c0baf37ee8a60566a))
*   log not real time ([a113d28a](https://github.com/taosdata/taosx.git/commit/a113d28aff80a6e7bb2ea0e22b4fafce58281d26))
*   fix the timezone issue and optimize the logging output.. #TD-24612 ([cb5282f0](https://github.com/taosdata/taosx.git/commit/cb5282f022c5bbb45598df337b55358a3022224f))
*   update std output ([c8f2c0a5](https://github.com/taosdata/taosx.git/commit/c8f2c0a599c60d661323398b3435369b2db848b0))
*   insert sql add column ([4513237d](https://github.com/taosdata/taosx.git/commit/4513237da8b5797256c5a9012833e04c2ea3713c))
*   add pi rotation log ([5de552ca](https://github.com/taosdata/taosx.git/commit/5de552caa3e434f611e3e41c09c0c6a69f20bfc3))
*   add influxdb rotation log ([afbbe608](https://github.com/taosdata/taosx.git/commit/afbbe6083e8a5182b111640462da2ace87ddeecc))
*   add mqtt rotation log ([9faf605b](https://github.com/taosdata/taosx.git/commit/9faf605bf033a36c78d7d914e8097bd15927a1dd))
*   add rotation for connector logs ([e1efc6eb](https://github.com/taosdata/taosx.git/commit/e1efc6eb18436d72f9baaa447096c59c28ea2d53))
*   fix yaml config error ([3e540b77](https://github.com/taosdata/taosx.git/commit/3e540b77c55ca58bc58d18e6981fecc6ab6a803f))
*   default value of parameters for mqtt ([91b74dc1](https://github.com/taosdata/taosx.git/commit/91b74dc1a8904ace6b8f414de01739ed15a253f3))
*   fix mqtt username password missing ([ab6252a6](https://github.com/taosdata/taosx.git/commit/ab6252a61360215efdb6367661384708237751e4))
*   do not create task when agent is not alive ([2a39e1fb](https://github.com/taosdata/taosx.git/commit/2a39e1fb6a82ff44f6f4bafec59de246c50fb7fb))
*   support uppercase in cast types ([d70b6d78](https://github.com/taosdata/taosx.git/commit/d70b6d78f82385bcded248876921f3ca582bdeee))
*   fix the logical bugs in the time window. #TD-24612 ([384ec648](https://github.com/taosdata/taosx.git/commit/384ec64877c5cacea80dc9a794248cc19b5ec081))
*   stop task when agent abort ([a72cb365](https://github.com/taosdata/taosx.git/commit/a72cb365176a81ba862715ddff1cb4000fcfb6c6))
*   fix missing required claim exp error ([2c92cd65](https://github.com/taosdata/taosx.git/commit/2c92cd65a79f9f74538af0bfcaac8a4261e04e76))
*   TD-24649 muti vgroup ([9a47d2a5](https://github.com/taosdata/taosx.git/commit/9a47d2a5bec223c053e29a8a044aa8e2bf69cc8e))
*   fix example error ([c1060b6e](https://github.com/taosdata/taosx.git/commit/c1060b6ebd13a5b04f2fe2ee651fd5904769c2d9))
*   fix parser parse timestamp/varchar/nchar fail error ([1c4e1efe](https://github.com/taosdata/taosx.git/commit/1c4e1efe34a3e5fed1800e324175f47a3d137e73))
*   translate in cn influxdb ([6c290db7](https://github.com/taosdata/taosx.git/commit/6c290db788a817cd4a7d67e4516e8ec94e65790b))
*   remove redundant code for agent ([d41346ca](https://github.com/taosdata/taosx.git/commit/d41346ca09d1e02227367b644daa94a19c9ce1ca))
*   modify to specify only one bucket. #TD-24417 ([64ec68a2](https://github.com/taosdata/taosx.git/commit/64ec68a264ac9310f802d9ccf60cd29f9788d3af))
*   fix username&password not set ([7e95802c](https://github.com/taosdata/taosx.git/commit/7e95802c5f230610b964f5780f90f8e381b3f709))
*   modify the parameters of the configuration page in explorer. #TD-24544 ([cdffaabc](https://github.com/taosdata/taosx.git/commit/cdffaabca13690e3a7a598a02af9c80dee7a4cb4))
*   check version before get assignment ([523d62b5](https://github.com/taosdata/taosx.git/commit/523d62b5a17be4cb661c2e36e1aee4b9ff33c753))
*   modify packaging configuration ([1063b132](https://github.com/taosdata/taosx.git/commit/1063b132320d3fc9a7ea08d4e53c0834028ba481))
*   fix reason text in data replication ([9b694825](https://github.com/taosdata/taosx.git/commit/9b694825a56eab18c13ccea79c70b8083387053d))
*   modify field name ([0b66bf8c](https://github.com/taosdata/taosx.git/commit/0b66bf8cdbc19d3568de3472494c1acedef9eb4b))
*   remove useless methods ([52a5f713](https://github.com/taosdata/taosx.git/commit/52a5f713dcc1cca5c2fdf6b8098168b09acb2ef0))
*   fix update agent fail ([21f72cc6](https://github.com/taosdata/taosx.git/commit/21f72cc644714a4c51b87c2bed7e7001b9482d00))
*   fix the bug on timestamp and add fatjar. ([f9c37aad](https://github.com/taosdata/taosx.git/commit/f9c37aadc99b4146fdcc779acea19dcf707e3a06))
*   fix compile error ([2106534d](https://github.com/taosdata/taosx.git/commit/2106534d7eec6764fcf6dff409437ade8e80d572))
*   fix compile error ([f9e38aef](https://github.com/taosdata/taosx.git/commit/f9e38aef29e74c2736e44ca0cacfee259328edbf))
*   change SYSTEM_OUT to SYSTEM_ERR and fix the bug in arrow data. #TD-24481 ([fe49ea5f](https://github.com/taosdata/taosx.git/commit/fe49ea5ffc4da1552241226893948423bca67a21))
*   solve the problem of time precision. #TD-24407 ([6bc3b03c](https://github.com/taosdata/taosx.git/commit/6bc3b03c2c502d45d51bd5f4a5dbe9a4dd9dfba4))
*   solve the problem of variable numeric types. ([01ed83d5](https://github.com/taosdata/taosx.git/commit/01ed83d5f548e11bc356de18cbeb47c5e430eea5))
*   replace varchar with nchar to solve the problem of Chinese characters. #TD-24423 ([e24e9bc6](https://github.com/taosdata/taosx.git/commit/e24e9bc695dd39a136b28f1bbfcbf2d5814e75fb))
*   rename the name of the stable to 'bucket_measurement'. #TD-24417 ([10035dc7](https://github.com/taosdata/taosx.git/commit/10035dc7943548edbbc877eb53d4312b8bd23004))
*   task start and end time are parsed using the zero time zone. #TD-24414 ([df4f93bc](https://github.com/taosdata/taosx.git/commit/df4f93bcb4c5925bb222023a9a6b8de9912ee08b))
*   fix parser none ([004601f3](https://github.com/taosdata/taosx.git/commit/004601f3c646b411779c7147f2318eca494fd0bd))
*   fix table write block ? ([05b93480](https://github.com/taosdata/taosx.git/commit/05b93480a70696ca4a507906d21830495dfa8fba))
*   fix bug for time format ([329a5e15](https://github.com/taosdata/taosx.git/commit/329a5e15cac9d5f63128b33f468c65fd48ef5aa3))
*   return empty node config instead of Err  to avoid taos panick when config node-config a file ([2e2ed989](https://github.com/taosdata/taosx.git/commit/2e2ed989b099c91a642397c5285af3842781d995))
*   solve problems that cannot be stopped ([3c484ccc](https://github.com/taosdata/taosx.git/commit/3c484ccc7ea6e086acb3557c57000173498420f1))
*   fix to_string unsupported when type is bool ([011b7757](https://github.com/taosdata/taosx.git/commit/011b775733caab5f8ac029488e391a63860b192b))
*   fix lush message insert error ([1031fd9b](https://github.com/taosdata/taosx.git/commit/1031fd9b61154ba2e69c5a6aff929a354388ec1c))
*   fix compile error ([7b7dc69d](https://github.com/taosdata/taosx.git/commit/7b7dc69da6de5d20eae873db8d844f6f20aebe2f))
*   add args in mqtt test ([99a2c112](https://github.com/taosdata/taosx.git/commit/99a2c112b0ddbdb01d432a0b068e3dbf062891b2))
*   fix windows tokio runtime dropping error ([7bb5f398](https://github.com/taosdata/taosx.git/commit/7bb5f398f81ec98d53f6918fcde2bbe7d7cca351))
*   fix windows tokio runtime dropping error ([fcf89344](https://github.com/taosdata/taosx.git/commit/fcf89344ee61c6fc55bc1ae3c077ebb1454939ad))
*   fix for opc ua connector ([b0c6e8fd](https://github.com/taosdata/taosx.git/commit/b0c6e8fd012b6b6c935b8c03afa9099d641c8edb))
*   fix for opc ua connector ([ab7bb4fb](https://github.com/taosdata/taosx.git/commit/ab7bb4fb0a7402a418a231934d6e65ef9b31064b))
*   fix consumer subscribe hangout in less CPUs environment ([1760758c](https://github.com/taosdata/taosx.git/commit/1760758cb50857518620c4fd173b53fdd32230c1))
*   change opc path ([dcdd3de4](https://github.com/taosdata/taosx.git/commit/dcdd3de4fcdf71abbeabeb68906655dd265ce745))
*   parser can be updated via /tasks/:id ([73cda0d4](https://github.com/taosdata/taosx.git/commit/73cda0d49a133091f6ede53218073230b12cc27c))
*   fix API example error ([88376159](https://github.com/taosdata/taosx.git/commit/883761593c9da018c82091953407aa5be6e150f4))
*   fix systemd environment error for taosx ([3e2b5241](https://github.com/taosdata/taosx.git/commit/3e2b52418414287ccf7fd5a9f300eafafda7a44c))
*   fix field reading error ([077d1613](https://github.com/taosdata/taosx.git/commit/077d16134662c9bef77975a2ee7550956f3d184e))
*   fix for get all points for da ([5ae3ff15](https://github.com/taosdata/taosx.git/commit/5ae3ff15df86154377201de057824ccd40d617f1))
*   fix opc dataset category is none ([50a68400](https://github.com/taosdata/taosx.git/commit/50a68400829eb10711994f0f962fb54596c378e1))
*   time range error ([db34234f](https://github.com/taosdata/taosx.git/commit/db34234f3cc02b4aacbdd9f2e68c1c9054e2bb0c))
*   maxbackfilldays change to int ([a7b746e6](https://github.com/taosdata/taosx.git/commit/a7b746e600a00ebd1c5a50821a3af48c49292bc4))
*   remove labels feild ([f66af7ab](https://github.com/taosdata/taosx.git/commit/f66af7ab7b44186e50bab0b448631977c0f7763d))
*   add editable&selectable for Target struct ([3b58d8d3](https://github.com/taosdata/taosx.git/commit/3b58d8d30f70c130d0755a4d6b4abb5478693ea1))
*   fix point list param error ([024607fa](https://github.com/taosdata/taosx.git/commit/024607fa698b371c25684f2ccc0948b1fe9c832f))
*   fix hint definition returned min/max not exist ([7ec10db9](https://github.com/taosdata/taosx.git/commit/7ec10db90a2ad4a6df46aadda87f1f663ac76fe6))
*   validate dsn before creating ([e40bbfc8](https://github.com/taosdata/taosx.git/commit/e40bbfc8f813f66e0bc826ce596387dcf7f8fa44))
*   fix param error ([ff51e813](https://github.com/taosdata/taosx.git/commit/ff51e81389a0de4020b2653ebaa92ede3df37712))
*   fix deadlock when call remove ([c29ac456](https://github.com/taosdata/taosx.git/commit/c29ac4564fb30698fbbd723198ae51271c047cea))
*   envent handler error ([033dee13](https://github.com/taosdata/taosx.git/commit/033dee13562619cfb142329710ce51bedf932eac))
*   fix stable not create for lush message when use agent ([69eb75bb](https://github.com/taosdata/taosx.git/commit/69eb75bb759053f1d3ddefb9dce0a12ac2480143))
*   default database path to /var/lib/taosx (#201) ([6869bef2](https://github.com/taosdata/taosx.git/commit/6869bef2de8cc0d228a755108b0ef29942c96281))
*   fix for reading limit ([f6bee9d9](https://github.com/taosdata/taosx.git/commit/f6bee9d90d3771e16dccac9a1c625890d17b6517))
*   fix compile error on windows ([08ffe8a4](https://github.com/taosdata/taosx.git/commit/08ffe8a4085ff5168ca1d2d5964ffd7d8d91f456))
*   fix compile error ([8c3cb711](https://github.com/taosdata/taosx.git/commit/8c3cb71189a0bc68c644606a23b42014ee6ef4d2))
*   fix dataset detail empty value ([c9e861c3](https://github.com/taosdata/taosx.git/commit/c9e861c3ee369d3efc73b688af8e6832ca061356))
*   avoid infinite loop when IPC task stopped ([b45dde39](https://github.com/taosdata/taosx.git/commit/b45dde3902cd431be911561f6e1930e2bc4943ba))
*   fix cancellation not work for OPC on Linux ([bbe00831](https://github.com/taosdata/taosx.git/commit/bbe00831b124de95e9a651f5fdc57f59f5e4e862))
*   avoid stmt init multiple times in one writer ([4bee6d40](https://github.com/taosdata/taosx.git/commit/4bee6d40cbd9e90a7d5181d939975e97dcbc1d07))
*   fix pi dataset key error ([64fb4936](https://github.com/taosdata/taosx.git/commit/64fb4936e36b73b48f6af36410aca774aebc61b2))
*   fix move compile error ([62ac9f2b](https://github.com/taosdata/taosx.git/commit/62ac9f2bf6f83725dce3d16cc5c0be02b2a6f95c))
*   fix for subscribe ([3afca264](https://github.com/taosdata/taosx.git/commit/3afca2641db95331f6830cf76201f625b580eeb9))
*   TD-23843 agent patch support chinese (#184) ([80a980ab](https://github.com/taosdata/taosx.git/commit/80a980aba0948bb2c45290233fa18f6fc82f88d7))
*   fix mem leak in connector (#182) ([b2119d13](https://github.com/taosdata/taosx.git/commit/b2119d13b2d1eaec913931ba103615c37be63503))
*   fix compile error when using native-tls ([50d82642](https://github.com/taosdata/taosx.git/commit/50d82642d13d666791e19392f7e9894915b14162))
* **build:**  use consist db params to fix sqlx migration error ([0fed271e](https://github.com/taosdata/taosx.git/commit/0fed271ed58322908cc11cd5292dcadbde1b9cb5))
* **parser:**  tracing json parse errors instead of panic ([c9a3da88](https://github.com/taosdata/taosx.git/commit/c9a3da883fc2fcb478038010d269cbc41fc73d24))
* **serve:**  fix update agent with name error ([eec0ae78](https://github.com/taosdata/taosx.git/commit/eec0ae782ab1ac4de722e8aa62d018828f011e5e))



<a name="v0.5.1"></a>
## v0.5.1 TDengine Data Replication Toolkit (2023-04-20)


#### Bug Fixes

*   upgrade arrow version ([3d249baf](https://github.com/taosdata/taosx.git/commit/3d249baf6028498d2448dd24d23856f831aaad07))
*   description error ([8cda2677](https://github.com/taosdata/taosx.git/commit/8cda2677026520731930f0e4e715d25894eb27a1))
*   upgrade arrow (#175) ([b5bebe40](https://github.com/taosdata/taosx.git/commit/b5bebe4017141a18da297691b6b7feeb882ac406))
*   monitor del (#174) ([9ba9f5cc](https://github.com/taosdata/taosx.git/commit/9ba9f5cc0c386a8249ad4235a8713054e0cf2d36))
*   fix  task state error (#173) ([e4e75ff1](https://github.com/taosdata/taosx.git/commit/e4e75ff167da54b691ccd5a8c99c4dbcc49b67d7))
*   catch exception when packaging ([866481ec](https://github.com/taosdata/taosx.git/commit/866481ec06c73fcc4d5d2a33a2d99411d271922e))
*   opc upgrade arrow (#166) ([707c8db2](https://github.com/taosdata/taosx.git/commit/707c8db2463ae8ab3a4dc9ef2b903394266ceb38))
*   fix build error (#169) ([31fd4904](https://github.com/taosdata/taosx.git/commit/31fd4904c6da288b751a6e0a10ab38057aef99e8))
*   fix task cancellation cause taosx stopped (#167) ([36b4328d](https://github.com/taosdata/taosx.git/commit/36b4328d6c175721e7ba08d1ce32d36515ee04dd))
*   empty stable not created in legacy mode (#157) ([518f19e8](https://github.com/taosdata/taosx.git/commit/518f19e82d95ec0b44ac3623805651e9386c7fb2))
*   fix systemd environment file not exist error ([27cc5b29](https://github.com/taosdata/taosx.git/commit/27cc5b29a3a18d0c99ba3bb5fb4e95f45f6ab21e))
*   fix from_utf8 error ([36e5970d](https://github.com/taosdata/taosx.git/commit/36e5970d3ca6080005be76dd076fe118e599c865))
*   fix lifetime error ([dd4f2250](https://github.com/taosdata/taosx.git/commit/dd4f225098140f6e6914af9eef9ecbd2ce902c26))
*   fix nchar convert error ([105316e0](https://github.com/taosdata/taosx.git/commit/105316e0fd1a07ecacebd267da77a272d73b0945))
*   fix endpoint parse error ([7439204b](https://github.com/taosdata/taosx.git/commit/7439204b09c4ac805948d5eb3d8c5f122ca04625))
*   disable-enterprise-only-validation feature fix ([9a09542d](https://github.com/taosdata/taosx.git/commit/9a09542d150bdaa7d9321cfec15cc69bc03fce60))
*   fix struct build in test ([41f47625](https://github.com/taosdata/taosx.git/commit/41f476257a8dc244e9b8326df453a55f32df515f))
*   fix opc da config error ([72764e95](https://github.com/taosdata/taosx.git/commit/72764e9560b0449d3bb89c99d02155b01adb2c15))
*   fix varchar/nchar/binary column type check failed ([1aaeaceb](https://github.com/taosdata/taosx.git/commit/1aaeacebbe916205228e52312bbba2610e46671a))
*   fix pi data source parameters ([8b0de133](https://github.com/taosdata/taosx.git/commit/8b0de1338235c1f9492920784184c4d191d4cccc))
*   fix backup/restore error ([9c11c86d](https://github.com/taosdata/taosx.git/commit/9c11c86d9505484e4ccda4be9e8a58244a64e40d))
*   fix point read process ([a39e14f2](https://github.com/taosdata/taosx.git/commit/a39e14f2f5bfb02378069e362f439b24f8361f42))
*   fix opc read record error ([8e6ebe65](https://github.com/taosdata/taosx.git/commit/8e6ebe65f4cd0c706faa276bd248940e16e06813))
*   fix pi log read error ([1fa643bd](https://github.com/taosdata/taosx.git/commit/1fa643bdc6a85add6925f3e4ec45af4c4b4c4799))
*   fix backup/restore error ([96333658](https://github.com/taosdata/taosx.git/commit/96333658233fe8d0fce1a7babbc34c23aec47726))
*   update opc/pi data source definition yaml ([69fda6a1](https://github.com/taosdata/taosx.git/commit/69fda6a107c50c15143c859b9d8058a7d5be5ff7))
*   fix websocket connection error for /sql api ([c5315c65](https://github.com/taosdata/taosx.git/commit/c5315c653017a98345341bc2390760b7bebc1378))
*   fix create table error ([8f6112f4](https://github.com/taosdata/taosx.git/commit/8f6112f40acb3669ff374e4ed8b816e8360f9f4e))
*   awc in spawn not valid ([988ace2c](https://github.com/taosdata/taosx.git/commit/988ace2c35bc23224818ab113e4ce2f993a43be3))
*   /ping 404 error ([b69b1d8c](https://github.com/taosdata/taosx.git/commit/b69b1d8cfa9cb10d69c776b9742b0f8aab7af37c))
*   fix remove temp file ([e764bc49](https://github.com/taosdata/taosx.git/commit/e764bc49e7e8ff5f489e5d3c5cac054fd73518e6))
*   fix windows file handle error ([e9b4ccdb](https://github.com/taosdata/taosx.git/commit/e9b4ccdbedd2092402ce79b52d41eb1e494b0642))
*   fix ctrl+c not work for pi to taos ([140f7b1e](https://github.com/taosdata/taosx.git/commit/140f7b1e4432423c05f98315c9fdb107ad41c0ba))
*   fix CI error in case of taos v0.5.5 ([c806b62f](https://github.com/taosdata/taosx.git/commit/c806b62f69902e2bbbce7b2451440dff51d3f1db))
*   attrs could be nullable for PI ([f8cda35a](https://github.com/taosdata/taosx.git/commit/f8cda35a98f42f16fc6d58251265b609d78f92f8))
*   fix CI error in case of taos v0.5.5 ([7e0bf451](https://github.com/taosdata/taosx.git/commit/7e0bf451a4bc360984c857cb4794cc8b411bad36))
*   legacy compile error ([6e7843f0](https://github.com/taosdata/taosx.git/commit/6e7843f0f46bcee22a26f00198cbf56657b47717))
*   fix stable already exists in legacy mode ([a9fff6d3](https://github.com/taosdata/taosx.git/commit/a9fff6d3d32e35a33348a3e3b912e6b5d793e77a))
* **ipc:**  may fix error that only accept one stream ([b53abc93](https://github.com/taosdata/taosx.git/commit/b53abc93d929b4e35490e879ffbe3e3475c82d3f))
* **legacy:**
  *  use a scheduler for data syncing ([14adf443](https://github.com/taosdata/taosx.git/commit/14adf443e85047eccfc67fc12f282fe74a48fcca))
  *  fix hangout problem when using realtime mode ([282c3c1a](https://github.com/taosdata/taosx.git/commit/282c3c1a8fd917bdfe7632d0628d75b672a46d97))
  *  fix hangout problem when using realtime mode ([be30cd0e](https://github.com/taosdata/taosx.git/commit/be30cd0e1140e004fb3718507669e3d78c85f5a2))
* **pi:**  sql rest service handle multiple requests ([7b3bb96f](https://github.com/taosdata/taosx.git/commit/7b3bb96f1722d535f633754a7d422809d95a3c7b))
* **unix:**  fix unit socket compile error ([ca04aaf0](https://github.com/taosdata/taosx.git/commit/ca04aaf0da8766b73f47eb806a63eb07cd596d71))

#### Features

*   support -f mqtt -t taos(todo start mqtt program and transfer data to transformer) ([88fe6aea](https://github.com/taosdata/taosx.git/commit/88fe6aeabd2e4e57cfb6bc3316ab7942044b5139))
*   support process flat message ([45ea7549](https://github.com/taosdata/taosx.git/commit/45ea7549673f3ef0fa6044cf6646b275c6e13700))
*   add pi/opc and packaging scripts (#154) ([0a0b94f9](https://github.com/taosdata/taosx.git/commit/0a0b94f91d17cf153d6e9d5272921a89ad27727c))
*   split opc ua/da to different data sources ([4c07c647](https://github.com/taosdata/taosx.git/commit/4c07c647d2e10dc68d809710cad21009a89a4772))
*   use CUS_NAME/PROMPT env for OEM labeling ([5eaa5a64](https://github.com/taosdata/taosx.git/commit/5eaa5a64b5889d4c9074b92b6465154a58189b23))
*   add /ds/in/sets endpoint for listing dataset collections ([562ec03f](https://github.com/taosdata/taosx.git/commit/562ec03f13722e6d53ff980f53d04fab4ebe08f4))
*   support opc to taos ([51548a85](https://github.com/taosdata/taosx.git/commit/51548a85286d7683112414db84559f4024d528e4))
*   parse any value from str ([bfaae682](https://github.com/taosdata/taosx.git/commit/bfaae6823b72c14e1093e3880211f83cc24092c4))
*   add pi to taos support in CLI mode ([e8eefa60](https://github.com/taosdata/taosx.git/commit/e8eefa60215b4ec7896463a1f7199a2274f7932a))
*   support pi to taos ([79903feb](https://github.com/taosdata/taosx.git/commit/79903feb24080154069b22765840f732695b8f83))
*   print metrics when done ([64d05944](https://github.com/taosdata/taosx.git/commit/64d05944f62543cfa1a907d68a8a6f6aad75905a))
*   support table_name column ([0114dbfc](https://github.com/taosdata/taosx.git/commit/0114dbfc25406118941c1f1ed7deed2fbca1ba90))
*   print metrics when done ([66124ac6](https://github.com/taosdata/taosx.git/commit/66124ac6b0db04528c9935170abdf855598a9564))
*   support binary input of columns ([ed87da30](https://github.com/taosdata/taosx.git/commit/ed87da304ea48d24498a1e23d1a0766215dc5cc7))
*   support primitive types of columns ([b92d01aa](https://github.com/taosdata/taosx.git/commit/b92d01aabb40f28a0c289446be722edf7f6a274b))
*   support all types of tags ([9cb30e3e](https://github.com/taosdata/taosx.git/commit/9cb30e3ed70c4d3b643c0fec82315a71ca46297f))
*   add tcp listener for windows & unix ([ea8c1128](https://github.com/taosdata/taosx.git/commit/ea8c1128a27d8876cacb43a628afd65cb6367afb))
*   ipc reader/writer based on arrow-ipc ([154fe591](https://github.com/taosdata/taosx.git/commit/154fe591b0019b7f6cc092ef1f4864dc57c1159a))
* **ipc:**
  *  support create child tables in batch ([6a039a7b](https://github.com/taosdata/taosx.git/commit/6a039a7bb448cf85f3a47490bc30facd3ee4dffa))
  *  add .NET demo project of Arrow IPC writer ([e414e379](https://github.com/taosdata/taosx.git/commit/e414e379161bcee22b08452bdecfcceee34a7f36))
* **legacy:**
  *  sync table data concurrently ([9fb8dcfa](https://github.com/taosdata/taosx.git/commit/9fb8dcfa94bf03826e59c058f5505057aa23b38b))
  *  syncing table schema concurrently ([0ba021fb](https://github.com/taosdata/taosx.git/commit/0ba021fbcd259c1ff4dc3d741ea2d06746a28a57))
  *  support `@file` in `stables` and `tables` parameters ([4b9fc6d0](https://github.com/taosdata/taosx.git/commit/4b9fc6d025cb954e27a4eb8956bee5ddf013eef0))
  *  add `failes-to` option for failed table names ([bb993779](https://github.com/taosdata/taosx.git/commit/bb993779c08e32ec622a8c93adf09822a7d0603c))
  *  sync table data concurrently ([72dbd153](https://github.com/taosdata/taosx.git/commit/72dbd153899bfedfd36c8c271535b2a621566667))
  *  syncing table schema concurrently ([c127d850](https://github.com/taosdata/taosx.git/commit/c127d8503c593eb89b152eaed797c887bb3c6d4f))
  *  support `@file` in `stables` and `tables` parameters ([bd864c40](https://github.com/taosdata/taosx.git/commit/bd864c40771f00a576616f826731eca6624f0561))
  *  add `failes-to` option for failed table names ([96717789](https://github.com/taosdata/taosx.git/commit/967177899d6819d31f14eec44cded665de95431b))
* **systemd:**  support environment file for taosx service ([aa2c5a34](https://github.com/taosdata/taosx.git/commit/aa2c5a3415045b5e9a6d4bb406898c36376e7c84))



<a name="v0.5.0"></a>
## v0.5.0 TDengine Data Replication Toolkit (2023-02-28)


#### Bug Fixes

*   set default connection timeout to 5s ([e99e4819](https://github.com/taosdata/taosx.git/commit/e99e48190433585114daf7129ee55ac88bc63179))
*   fix labels filter conflicts with stream_type query ([6f3b0bfa](https://github.com/taosdata/taosx.git/commit/6f3b0bfa64e06260dbc7e01814f8c500b917c890))
*   fix macos compile error. unknown field in &Process ([96e97244](https://github.com/taosdata/taosx.git/commit/96e97244e551a3ae4ca173b9c8a9cacf6ca9a9ff))
*   from main to 3.0 messages error handling ([1f6181b9](https://github.com/taosdata/taosx.git/commit/1f6181b9afdb4d5abeffd82846fa741ca4b69775))
*   do not insert into tasks when fail with clear ([b79108c3](https://github.com/taosdata/taosx.git/commit/b79108c390be783170bfc3968d89cc3c72578ad8))
* **legacy:**  fix prepare error when result is emtpy ([6b805e30](https://github.com/taosdata/taosx.git/commit/6b805e3097e301fd56aa724e09f6ec00f6b243d1))
* **serve:**  fix label filters ([029d29db](https://github.com/taosdata/taosx.git/commit/029d29db21142c410534ae6e77997c02764922c1))

#### Features

*   expand task detail with datasource definition ([25129288](https://github.com/taosdata/taosx.git/commit/251292888140e83a60fb08ba66e56c119466fe6d))
*   add data source input tasks API ([faa4043a](https://github.com/taosdata/taosx.git/commit/faa4043a9b8aae01fcb1e5601e298bbc2bfb78ac))
*   support `select-with-stable` and `tables` params ([898b79be](https://github.com/taosdata/taosx.git/commit/898b79be99acf55583a6b2a4aa9f45bd724653f9))
* **docker:**  update TDengine to 3.0.2.6 ([92c89ff7](https://github.com/taosdata/taosx.git/commit/92c89ff78ef50e32007ca3eab255f07686ab1a37))
* **serve:**  support task name and trigger settings ([462e85e9](https://github.com/taosdata/taosx.git/commit/462e85e9f71a7be0cb5cd160091e4191d6580e8a))



<a name="v0.4.2"></a>
## v0.4.2 TDengine Data Replication Toolkit (2023-02-13)


#### Bug Fixes

*   apply new version of taos-query ([e8feac4f](https://github.com/taosdata/taosx.git/commit/e8feac4fec8be467aa1c2f8469a12a19032dd455))
*   retry write raw in case 0x032C ([7bbf11a5](https://github.com/taosdata/taosx.git/commit/7bbf11a534d33499d77f1c8715e1386f381cfff8))
*   fix .env error on windows ([e1ce433c](https://github.com/taosdata/taosx.git/commit/e1ce433c38520c41bc166cf4d795035b03fb52e5))
*   force dynamic linking in musl target ([907d2e92](https://github.com/taosdata/taosx.git/commit/907d2e92feaec7e02c50bbad8a0e5a2d93154ff5))
*   fix database locked error ([3c56e238](https://github.com/taosdata/taosx.git/commit/3c56e238a27c9d480594e60c3cc3e249a353ae23))



<a name="v0.4.0"></a>
## v0.4.0 TDengine Data Replication Toolkit (2023-01-10)


#### Bug Fixes

*   check handler finished when start a task ([34cfaa40](https://github.com/taosdata/taosx.git/commit/34cfaa407d59af733ea3928ba58e90ba899c46db))
*   add batch-size option for legacy write to 2.6 ([54889b95](https://github.com/taosdata/taosx.git/commit/54889b9554838c4550351980b62e0e437c48a830))
*   fix wal size limit error not catched in scope ([10cacaad](https://github.com/taosdata/taosx.git/commit/10cacaad5860e70b2656cee7b9ae0945b6b11f25))
*   fix grant check failed in 2.6 ([8871929f](https://github.com/taosdata/taosx.git/commit/8871929f9c6f262de72c9595664708693341c6c2))
*   fix table name escape to solve errors 0x0362 ([80d3bd45](https://github.com/taosdata/taosx.git/commit/80d3bd45f868556530d472283b3876b10f02fc2f))
*   fix sqlite url error on windows ([118946b0](https://github.com/taosdata/taosx.git/commit/118946b0f07bb020b604f560e114e498a09db822))
*   fix sync error with delete from tables ([d2d3fe2f](https://github.com/taosdata/taosx.git/commit/d2d3fe2fcca688706ead9b49cb72e403d9707d13))
*   fix segmentfault at exit ([a424d46a](https://github.com/taosdata/taosx.git/commit/a424d46a0c843450c1b1405673695ba328daaaff))
*   fix table schema sync in legacy mode ([fe0cf009](https://github.com/taosdata/taosx.git/commit/fe0cf0090325c0b50b0048ef8f710d8406ba328a))
*   fix max sql len limit ([ab14320e](https://github.com/taosdata/taosx.git/commit/ab14320eec00fabc7792dae9c2ec3f69a199076c))
*   fix 0x030B data expired error in 2.x ([ad833958](https://github.com/taosdata/taosx.git/commit/ad833958e0279ff474b0b6f9b8b0df07b8ce2c65))
*   upgrade parquet to v28 ([42d25926](https://github.com/taosdata/taosx.git/commit/42d25926184180460c6d51ac3e39754b51f30d68))
* **lagacy:**  fix stmt wal size limit error for legacy sync ([4e122f6d](https://github.com/taosdata/taosx.git/commit/4e122f6d289e2c4ee5b1fb08aef39e80dfa9c66a))
* **metrics:**  use sysinfo for all-platform metrics collection ([83768ad2](https://github.com/taosdata/taosx.git/commit/83768ad2a0ae9a667af3b84d7fb242faa5a6ccaa))

#### Features

*   support stopAt task for tmq to local ([5a82acb8](https://github.com/taosdata/taosx.git/commit/5a82acb87dd51dc7a5265e0764efa98374dfa695))
*   add feature gate `disable-enterprise-only-validation` ([0fe672b3](https://github.com/taosdata/taosx.git/commit/0fe672b3a7246a5e24bf881798a21c3339599535))
*   support enterprise only validation ([3d4f76f0](https://github.com/taosdata/taosx.git/commit/3d4f76f040d926b075b4a196bfe22a51e4549eca))
*   support multiple workers for legacy sync ([9d88f204](https://github.com/taosdata/taosx.git/commit/9d88f204d972d1912be3a218ffd48e7fd4d09bf2))
* **serve:**  support `after_delete` action for tmq to local ([fab76c00](https://github.com/taosdata/taosx.git/commit/fab76c00368d2e6f17d2957e435e1ee3bd4117ff))



<a name="v0.3.3"></a>
## v0.3.3 TDengine Data Replication Toolkit (2022-12-10)


#### Bug Fixes

*   3.0.2.0/3.0.1.x compatible ([c7865be4](https://github.com/taosdata/taosx.git/commit/c7865be434fc403b15c63163f7b2b9a41c334ebc))



<a name="v0.3.2"></a>
## v0.3.2 TDengine Data Replication Toolkit (2022-12-10)




<a name="v0.3.1"></a>
## v0.3.1 TDengine Data Replication Toolkit (2022-12-10)


#### Bug Fixes

*   update to 3.0.2.0 in docker image ([e366f821](https://github.com/taosdata/taosx.git/commit/e366f821ce5a417a1d173286f04d2f1df0b2b1fa))



<a name="v0.3.0"></a>
## v0.3.0 TDengine Data Replication Toolkit (2022-12-10)


#### Features

*   support almost realtime synchronization in legacy mode ([b851de90](https://github.com/taosdata/taosx.git/commit/b851de90f998a69d27b20929516224b97a2e777d))
*   print performance metrics at the end ([e5ce5f5f](https://github.com/taosdata/taosx.git/commit/e5ce5f5ff7c119edf1c47c89227a8513795fbfe5))
*   use rustls instead of native tls ([72cae692](https://github.com/taosdata/taosx.git/commit/72cae69217071ed493ef2725fd7d085c0e771bb6))

#### Bug Fixes

*   drop consumers after all tasks done ([6ebf3efc](https://github.com/taosdata/taosx.git/commit/6ebf3efc0f380b5a3debf408fade5c97da0868dd))
*   remove use db in tmq2taos ([21e8c34a](https://github.com/taosdata/taosx.git/commit/21e8c34a380faa6701bd14abb714006a4efe2da9))
*   use dashmap instead of scc ([3dbc96c2](https://github.com/taosdata/taosx.git/commit/3dbc96c2a33b583ac2eee887d30435e164f6589f))
*   scc v0.12.0 ([f9311fdd](https://github.com/taosdata/taosx.git/commit/f9311fdd8ad49565e671af209d3aed80c086e52b))
*   fix timed out error when use large vgroups ([0bf61de0](https://github.com/taosdata/taosx.git/commit/0bf61de0485962e5c405899d184044900faaf27b))
*   fix v3 to v2 subscription error ([0b32a1aa](https://github.com/taosdata/taosx.git/commit/0b32a1aafe604db0ac42b550bf2496788b2cf10b))
*   add records per second, points per seconds in metrics output ([498f4338](https://github.com/taosdata/taosx.git/commit/498f4338caaa199ce17b0683188d0da4419770de))
*   support utf8 table names in sync ([edbf6bd4](https://github.com/taosdata/taosx.git/commit/edbf6bd420e32f8957b71f6d716667787f57181e))
*   fix can not use keyword as database cases ([0e94b256](https://github.com/taosdata/taosx.git/commit/0e94b25660fdd15cc608d3d871c3532576e63cdb))
*   fix sync override with partial updates ([d5cb7ede](https://github.com/taosdata/taosx.git/commit/d5cb7eded3a2d44c18bf6ec89a302900bd567dc4))
*   fix compile error when target/ deleted ([5f4335ae](https://github.com/taosdata/taosx.git/commit/5f4335ae51e61929f07fa00e8182739cd92e1608))
*   fix panic when clear target failed ([a63da7e7](https://github.com/taosdata/taosx.git/commit/a63da7e71613d6781d2940e8f07e6d24cab1b0a1))



<a name="v0.2.1"></a>
## v0.2.1 TDengine Data Replication Toolkit (2022-11-18)


#### Bug Fixes

*   fix cancellation unexpected errors ([441dc9d8](https://github.com/taosdata/taosx.git/commit/441dc9d8d5ebcfc528646e4b0dc84d222aa931f8))
*   let tmq tasks cancelable ([78c8db69](https://github.com/taosdata/taosx.git/commit/78c8db69561fd9ec43c3a62d0b11e81651c21290))
*   support timeout=never with websocket ([3d10b601](https://github.com/taosdata/taosx.git/commit/3d10b6011968c3509aafe757cd0c6b1c9d8dc182))
* **serve:**
  *  default listen to 0.0.0.0:6050 ([38053ab9](https://github.com/taosdata/taosx.git/commit/38053ab9a98697053dbdb8653a005da610b4c2c8))
  *  fix websocket connection with 401 unexpected error with HTTP ([6cbc7727](https://github.com/taosdata/taosx.git/commit/6cbc7727cc2457c6d91ef8ea74ca1898badf2930))
  *  decide to connect or not by error handling ([0dc31d7f](https://github.com/taosdata/taosx.git/commit/0dc31d7f341b35c99eb721a9942c01d95d4995c0))

#### Features

*   add Dockerfile for taosx/serve:0.2.0 ([c9496e4c](https://github.com/taosdata/taosx.git/commit/c9496e4cf0dac861696fd6ad9549a8660db523e5))
* **serve:**
  *  add PATCH task/:id for update a task ([9c6e8aff](https://github.com/taosdata/taosx.git/commit/9c6e8affb8e5fbc4d65dd6853b6bf2332c6b0f04))
  *  support oneshot topic for task ([2ae2e4ff](https://github.com/taosdata/taosx.git/commit/2ae2e4ffaf3db91a75513f99e9d95e904a44c339))
  *  support clear target database in create api ([d96b9d1a](https://github.com/taosdata/taosx.git/commit/d96b9d1a445c3ad7aa4cad9bee306a4e31c5a433))
  *  invode all unfinished tasks after restart ([b78b7b91](https://github.com/taosdata/taosx.git/commit/b78b7b918f7b9064217a0daeaf8ed4170b61ab17))
  *  add /tasks/count api, also work for HEAD /tasks ([61bfd8c3](https://github.com/taosdata/taosx.git/commit/61bfd8c3b858da4f3301bdba31db8417c10ab75e))
  *  try re-run tasks when timeout=never ([da704e8f](https://github.com/taosdata/taosx.git/commit/da704e8f29a23d20987a38d67f44abdcd1c8450f))



<a name="v0.2.0"></a>
## v0.2.0 TDengine Data Replication Toolkit (2022-11-18)


#### Features

* **serve:**
  *  add PATCH task/:id for update a task ([9c6e8aff](https://github.com/taosdata/taosx.git/commit/9c6e8affb8e5fbc4d65dd6853b6bf2332c6b0f04))
  *  support oneshot topic for task ([2ae2e4ff](https://github.com/taosdata/taosx.git/commit/2ae2e4ffaf3db91a75513f99e9d95e904a44c339))
  *  support clear target database in create api ([d96b9d1a](https://github.com/taosdata/taosx.git/commit/d96b9d1a445c3ad7aa4cad9bee306a4e31c5a433))
  *  invode all unfinished tasks after restart ([b78b7b91](https://github.com/taosdata/taosx.git/commit/b78b7b918f7b9064217a0daeaf8ed4170b61ab17))
  *  add /tasks/count api, also work for HEAD /tasks ([61bfd8c3](https://github.com/taosdata/taosx.git/commit/61bfd8c3b858da4f3301bdba31db8417c10ab75e))
  *  try re-run tasks when timeout=never ([da704e8f](https://github.com/taosdata/taosx.git/commit/da704e8f29a23d20987a38d67f44abdcd1c8450f))

#### Bug Fixes

*   fix cancellation unexpected errors ([441dc9d8](https://github.com/taosdata/taosx.git/commit/441dc9d8d5ebcfc528646e4b0dc84d222aa931f8))
*   let tmq tasks cancelable ([78c8db69](https://github.com/taosdata/taosx.git/commit/78c8db69561fd9ec43c3a62d0b11e81651c21290))
*   support timeout=never with websocket ([3d10b601](https://github.com/taosdata/taosx.git/commit/3d10b6011968c3509aafe757cd0c6b1c9d8dc182))
* **serve:**
  *  fix websocket connection with 401 unexpected error with HTTP ([6cbc7727](https://github.com/taosdata/taosx.git/commit/6cbc7727cc2457c6d91ef8ea74ca1898badf2930))
  *  decide to connect or not by error handling ([0dc31d7f](https://github.com/taosdata/taosx.git/commit/0dc31d7f341b35c99eb721a9942c01d95d4995c0))



<a name="v0.1.1"></a>
## v0.1.1 TDengine Data Replication Toolkit (2022-10-31)


#### Bug Fixes

*   fix cargo install error in build.rs ([14264884](https://github.com/taosdata/taosx.git/commit/14264884a6cf2045e5f5d9c49a500c2d585e7135))



<a name="v0.1.0"></a>
## v0.1.0 TDengine Data Replication Toolkit (2022-10-29)


#### Features

*   support 2.x to 3.0 migration ([852d085a](https://github.com/taosdata/taosx.git/commit/852d085ae6379c039b863c0f1ebefef2e6944e8e))
*   support transformation while replicating ([904ce7d5](https://github.com/taosdata/taosx.git/commit/904ce7d588c639c6d500d08026d74e99c0721b71))
*   expose `metrics/` endpoint to OpenAPI schema ([c220798d](https://github.com/taosdata/taosx.git/commit/c220798dc650717fe13b89b8be4cbdd4b98191ed))
*   add --debug option for file:line debug prints ([0798ea66](https://github.com/taosdata/taosx.git/commit/0798ea6630045771d986905569f194aca7c401dc))
*   finish REST API ([0af928a8](https://github.com/taosdata/taosx.git/commit/0af928a84371a129d63cbed466e262b4e590bfb5))
*   backup/restore from TDengine to local files or opposite. ([c4a65ad1](https://github.com/taosdata/taosx.git/commit/c4a65ad1defd0aac2bb3c45a8794f1b6a11642bf))
*   stmt on websocket ([474ab878](https://github.com/taosdata/taosx.git/commit/474ab87831c62fe86b24f14551ad81a6f5b9e655))
*   add raw block api ([f5a6820f](https://github.com/taosdata/taosx.git/commit/f5a6820f99bca702f0b1dac845b1dcd12f17b327))
*   add --transform in sync cli interface ([6390e1db](https://github.com/taosdata/taosx.git/commit/6390e1dbb23415ff9025dd88eac7fd591d790204))
*   add transformer ([128607f9](https://github.com/taosdata/taosx.git/commit/128607f93ebe0aab5d896dbc2b087bf1baa31ea2))
*   add sync subcommand to tasox cli ([6ec0dd47](https://github.com/taosdata/taosx.git/commit/6ec0dd47c5ebb10b3a5f2a89d39bf46b404fccd3))
*   stmt API for both 2.4/3.0 ([ff119aa6](https://github.com/taosdata/taosx.git/commit/ff119aa69cb786668d7f3083c4a71cffd441629e))
* **libtaosws:**  add ws_get_server_info ([c3486020](https://github.com/taosdata/taosx.git/commit/c3486020e74cc2f699cc1cd2ae70f8537a1055c1))
* **macros:**
  *  use crate/taos wiseness ([2224ca80](https://github.com/taosdata/taosx.git/commit/2224ca80632d36ecc1a44b2df28a9c2e982f88c7))
  *  add test macro to simplify tests ([1bee64e5](https://github.com/taosdata/taosx.git/commit/1bee64e5da4bebae54b70dead61b4d84e23518cd))
* **mdsn:**
  *  DSN parse and display ([608afaa2](https://github.com/taosdata/taosx.git/commit/608afaa25498b9397eee1a071f7d33fac230bf15))
  *  add a dsn parser for taosx ([820bdb5c](https://github.com/taosdata/taosx.git/commit/820bdb5c68fe030bbf0d6ca06f1b75e0bcd87e90))
* **metrics:**  add process metrics ([72af4cbe](https://github.com/taosdata/taosx.git/commit/72af4cbe42fbc04f7adea82928c066f20360d793))
* **query:**
  *  compatible to 3.0 query/stmt api ([0ef98fa5](https://github.com/taosdata/taosx.git/commit/0ef98fa5c4ccc9d4482aa10585f266ff93c79769))
  *  add (query|exec)_exec methods for async query ([15a55013](https://github.com/taosdata/taosx.git/commit/15a55013af8419da5c0fafbc80156acc6d6e17f6))
  *  add columns_iter in block trait ([9ede3e3d](https://github.com/taosdata/taosx.git/commit/9ede3e3d231a156e1e5a88f2b184d97e484977cd))
  *  stream basic ([c0cfd829](https://github.com/taosdata/taosx.git/commit/c0cfd8296c846634808a1649d1cf9119a726eae4))
* **sync:**  add sync subcommand ([87468e49](https://github.com/taosdata/taosx.git/commit/87468e49d70c368cbc8e3b4936ebabe3a23bd8b2))
* **taos:**
  *  support desrialize with serde ([e9132cb2](https://github.com/taosdata/taosx.git/commit/e9132cb2538dfa0dd2e3b6a034946564b4cc4e39))
  *  support deserialization of json tag ([14491373](https://github.com/taosdata/taosx.git/commit/14491373d97865744a2ec8e5e251bffdb1553c71))
  *  support serde ([bee71e00](https://github.com/taosdata/taosx.git/commit/bee71e00dd14ca5a19457574c43a0620469ebb25))
  *  support r2d2 pool and stmt ([a7ae84ce](https://github.com/taosdata/taosx.git/commit/a7ae84ce4a3af7c3b67314c9ce15e9683a7b8f39))
* **taos-error:**  separate error crate for both rest/native ([75014377](https://github.com/taosdata/taosx.git/commit/750143776059e73ea5a47f8cf53ed4382df8b2af))
* **taosx:**
  *  export query result to CSV/Parquet. ([ca471550](https://github.com/taosdata/taosx.git/commit/ca47155056357050e4e476ee9afcd06613f4086b))
  *  support CSV/Parquet export ([54af092b](https://github.com/taosdata/taosx.git/commit/54af092b9f9c9c019a1a0faba3455bd017e0b5d0))
  *  refactor backup and restore ([7b48b1d0](https://github.com/taosdata/taosx.git/commit/7b48b1d05847a3287a9c24cf7296196aa2851458))
* **traits:**  add helper methods for connection ([0202bf40](https://github.com/taosdata/taosx.git/commit/0202bf40b72bedb85c6e5f0f5cf9850e00f935cf))
* **ws:**
  *  add ws_stop_query and support write raw block ([160cbe8c](https://github.com/taosdata/taosx.git/commit/160cbe8c2f623927175f1caa491f5e9995655a53))
  *  add ws_take_timing for taosc execution cost ([8fcae0f0](https://github.com/taosdata/taosx.git/commit/8fcae0f010ab1e9ef1742522e9143ca9461752c2))
  *  improve libtaosws api ([fb6854d3](https://github.com/taosdata/taosx.git/commit/fb6854d3c7ffad7d639ac8f56cccdd5d50c97073))
  *  add C STMT API for libtaosws ([9dbd2086](https://github.com/taosdata/taosx.git/commit/9dbd20864910a562ac3471a52b7d45cda7eda289))

#### Performance

*   reduce binary size ([e93b1cef](https://github.com/taosdata/taosx.git/commit/e93b1cef2208f21d62610d91ba3b2b526848be30))

#### Bug Fixes

*   fix write to cloud error ([d936a8f5](https://github.com/taosdata/taosx.git/commit/d936a8f5108de114c278469516d3333a2aaed518))
*   fix coredump when failed to connect ([361ec0bd](https://github.com/taosdata/taosx.git/commit/361ec0bdef4adb55e61501cc09a710af9691016d))
*   sync database parameters when target not exists ([6221b473](https://github.com/taosdata/taosx.git/commit/6221b4738bdcf0abecd8ce67b3a509a3f4132c20))
*   set vgroups to default 2 when permission error ([adf51a83](https://github.com/taosdata/taosx.git/commit/adf51a83733799c6d69a7ba37c42335a18393031))
*   cloud service can't get create database sql ([a4831904](https://github.com/taosdata/taosx.git/commit/a4831904bbecca57f94cf74c5bf94ca968d2e6b4))
*   fix data lost when backup multiple times ([60ec8e9f](https://github.com/taosdata/taosx.git/commit/60ec8e9f6328d48d000908f566719af33c1351f6))
*   fix rename tables error ([b8855f14](https://github.com/taosdata/taosx.git/commit/b8855f14f32029446d5e25c8355bd0fb96eeb276))
*   fix tags lost using with `tmq:///db.child_table` subscription ([5d07a8b3](https://github.com/taosdata/taosx.git/commit/5d07a8b3b5009f125870ff678bc6d540c2076feb))
*   fix table name not exist error instead of panic ([13792292](https://github.com/taosdata/taosx.git/commit/13792292ced0e9af9d593f92daf24b868b6500e8))
*   support subscription of stable or normal/child table ([14a0c5a8](https://github.com/taosdata/taosx.git/commit/14a0c5a8618861f57d08fdbd01d10261272f0e4e))
*   fix database/table name error for normal tables ([0b169416](https://github.com/taosdata/taosx.git/commit/0b1694163b76f9741fbfba21cd6b2e5ac072641e))
*   refine dev pipeline ([0b358847](https://github.com/taosdata/taosx.git/commit/0b3588478102e1eafc39ed85592fee86612571e5))
*   handle ctrl-c ([887b51b3](https://github.com/taosdata/taosx.git/commit/887b51b327a55bc4e448e700476355d0e78f6727))
*   update tmq error handling and offset changes ([f35c7814](https://github.com/taosdata/taosx.git/commit/f35c7814ac692d81bb2df550841c5cd163a4bc09))
*   workers = 0 error ([a405fde6](https://github.com/taosdata/taosx.git/commit/a405fde639d137f772e242cfea72b2f464df2c8b))
*   fix taos_result_block in v3 ([ac9045c2](https://github.com/taosdata/taosx.git/commit/ac9045c25d9b487443769e46c07d9dcc9860b02a))
* **libtaosws:**  fix example error with old version of gcc ([73dbfadb](https://github.com/taosdata/taosx.git/commit/73dbfadb461c74e48ac8dc7355436747f810914f))
* **local:**
  *  fix meta data inconsistant when loading multi files ([357d55c8](https://github.com/taosdata/taosx.git/commit/357d55c8110a12e1b9c00a013be6aba29aa86cab))
  *  create database when target not exist ([146d1026](https://github.com/taosdata/taosx.git/commit/146d10269b20f7c55d954c73d2b32ef4d3a4aa14))
* **mdsn:**
  *  fix `dirver:///` with empty db name error ([ba23f3f9](https://github.com/taosdata/taosx.git/commit/ba23f3f9a36a379138f0bca9a66deb5a349edcd6))
  *  fix database characters error while parse db with _ ([68e9708e](https://github.com/taosdata/taosx.git/commit/68e9708e026939abe5a3cca0ca8070a5106638db))
  *  fix dsn parsing with tmq support ([90c05237](https://github.com/taosdata/taosx.git/commit/90c05237cdfe5ae88b0bf2e55c48b814b1097939))
* **query:**
  *  fix nchar view error when there're more than one ([fb1e441b](https://github.com/taosdata/taosx.git/commit/fb1e441bcf8e15a865840a0ac1bd37fc4e5a2f72))
  *  mark async dependencies optional ([62cc86c9](https://github.com/taosdata/taosx.git/commit/62cc86c9aa971ab481b20549f2ef18f4bbd29561))
* **replicate:**  fix replication error for child table only ([be7cb757](https://github.com/taosdata/taosx.git/commit/be7cb757b7b99b86c9a0e72c0dde25eca1dc6908))
* **taos-sys:**  update taos C bindings ([6dbd198b](https://github.com/taosdata/taosx.git/commit/6dbd198b7ca2905b564d7511f6a892d6317c8bb4))
* **taosx:**
  *  allocate tasks for threads ([a3ef4ac9](https://github.com/taosdata/taosx.git/commit/a3ef4ac94265f7dfdbd3d720e8f89dc47d875af3))
  *  add missed other data type in creating sql ([e1099b7a](https://github.com/taosdata/taosx.git/commit/e1099b7ac6d5dee958a3af06e309f5a76cd0d425))
* **test:**  fix taos-ws second-test coredump ([ccf0cc7e](https://github.com/taosdata/taosx.git/commit/ccf0cc7ea00ffbc1e8080284fb5f17b7aef11411))
* **transform:**  fix template rename error ([09b82423](https://github.com/taosdata/taosx.git/commit/09b8242321b2c746874aeb058d7e88a74faa4a65))
* **ws:**
  *  fix stmt coredump with non-null values bind ([988ba2e0](https://github.com/taosdata/taosx.git/commit/988ba2e02052750972736ff0189bd63dd5bb9d49))
  *  fix hang error when connection closed ([59cf1e6e](https://github.com/taosdata/taosx.git/commit/59cf1e6e5e7ab80bc4d78562cf6648f02e1ed02a))
  *  use ws_errno/errstr in ws_fetch_block ([798d29a7](https://github.com/taosdata/taosx.git/commit/798d29a748178f3bb2c269a39c8b7e57c80cbae2))
  *  not block on ws_get_server_info ([69a811a7](https://github.com/taosdata/taosx.git/commit/69a811a75b0125a5e6f0fafc81f009075a903a24))
  *  fix v2 error when use with show-databases ([8bed0edd](https://github.com/taosdata/taosx.git/commit/8bed0edd88cdaae7c5c17487d71d3c2701d58c54))
  *  use optional dependencies to reduce size ([18682952](https://github.com/taosdata/taosx.git/commit/18682952d5e5aad5a777ffc297313d69f49c280e))
  *  fix raw block error for TDengine 2.x ([6fe2bbd4](https://github.com/taosdata/taosx.git/commit/6fe2bbd4cee748ed97d0de44eefcc37351059799))
* **ws-sys:**  fix duplicate defines error with taos.h ([8a5864eb](https://github.com/taosdata/taosx.git/commit/8a5864ebf47e8ef718e63342ba35789ad0b4e842))



