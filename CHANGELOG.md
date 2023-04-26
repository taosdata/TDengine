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



