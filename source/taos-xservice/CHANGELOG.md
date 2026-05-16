# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2024-02-27

**Full Changelog**: [v1.4.0...v1.5.0](https://github.com/taosdata/taosx/compare/v1.4.0...v1.5.0)

### Bug Fixes

- *agent*: Start heartbeat task many times (#1376) ([c637228](c637228f0c33a82d55c8ffb4ebeb8b0f6bdd0218))
- *agent*: Wait util all received data written ([c2a8e00](c2a8e00f425ce8b28c4941f7d0e21ef5029e340f))
- *agent*: Explicit stopping state when use agent ipc ([8470b57](8470b573d1031f4fe04c6d70d061a1858e140eff))
- *api*: Fix status decode error in case of "waken" ([ec2fe3f](ec2fe3f913a4e5cc339e470998708379300012e1))
- *csv*: Csv task should fail when error ([e545535](e5455356da139238c5d6e45373117a08502a142f))
- *csv*: Csv task should fail when error ([6bd9831](6bd983155262c96a79d6ec82d2319182c1db61d2))
- *historian*: Use sql instead of write_raw_block* api ([562daef](562daef40202b0f88d3f08f52547be1639f8d05d))
- *ipc*: Fix sql with NULL values ([00eabb8](00eabb8a9fc45cf3518d49c8239fc3f4dc8216d6))
- *ipc*: Agent put stream send error ([8703f5c](8703f5c77045de6750c693dc5365973b74a3ad3a))
- *kafka*: Advanced options read_concurrency hidden in en yaml file ([ec05e5c](ec05e5c2bdb8f13872f51d5f25df69b3b14b85c7))
- *legacy*: Use describe to build sql when 0x2600 ([85dc4da](85dc4da39cc4114c5bf5cd8b4ab2df1980c93d2f))
- *legacy*: Sql validation priority is less than if not exist ([4b37e4a](4b37e4a4574e1593af81cf18d20de757e4e30a93))
- *legacy*: Fix 2.6 websocket connection error since 1.4.0 ([eaabe64](eaabe64f2ec72014b1d3dc6bd89c55ff53a8f3c1))
- *legacy*: Fix 2.6 websocket connection error since 1.4.0 ([407449f](407449f0b16a600acae8e7be575cb065e622b6c5))
- *legacy*: Fix large table fallback creation case sensitive ([73b924d](73b924d114b7abead4d4ecac374b03a111dd7069))
- *legacy*: Fix large table fallback creation case sensitive ([c8727c4](c8727c478868fdd214f6646006b1e4724be67ca5))
- *legacy*: Fix table not exist error when sync table from 2.x ([8863b86](8863b863526d1ef94932aaf2c04b98d8030b19bd))
- *legacy*: Fix table not exist error when sync table from 2.x ([2e3b603](2e3b603a5bb68b563e2b450f3441c317f730bc75))
- *legacy*: Migrate large table error with 0x2601 ([f2b8527](f2b8527d95194f21dfa93677fef05290fba98f4a))
- *legacy*: Fix result is nil error in case of sparse ([3334aea](3334aea719cea923c31c51e8490e54eaea85b63f))
- *legacy*: Fix result is nil error in case of sparse ([efab944](efab9446a6d986ee27a48f0b43677436e6d7d47d))
- *legacy*: New table without data will be synced ([0f24cf7](0f24cf7c5c8494a88efa184d114a9f278ad980c2))
- *legacy*: Fix blocking condition in scheduler worker ([17aea66](17aea66ee5ea007d027dcc7b8d4178345385fb14))
- *legacy*: Duplicate realtime tasks ([bccf2c2](bccf2c21fe52fc521b3ecc07749d8fc3c2381440))
- *legacy*: Duplicate restro tasks ([7a1005b](7a1005bf86714fd259c46fb931a879adec86a8dc))
- *legacy*: Duplacte tasks were genreated ([b166d5f](b166d5f2507eca841693b41f4d68db113bbb5409))
- *local*: Fix local backup restore database not exist ([d60365c](d60365cae17c12b38ad498a08aee425324416da9))
- *log*: Avoid bug of file-rotate library ([cfb6870](cfb687090dce2f22d23e9d52bd3a2e1fbf48f5a3))
- *metrics*: Total execute time is wrong ([714b18f](714b18f16baac1803c4283eff972f44d26da2719))
- *metrics*: Ipc total_process_batches is wrong ([4068682](40686821e7f6c476afed41ab21e3dcd0f24e2572))
- *metrics*: Should not close session when no metrics found ([70993ad](70993ad604ffd005e72f89a6dcb51b28ae46daeb))
- *metrics*: Save metrics error ([cd8c644](cd8c644b6f9b764d528acc13970707fa209bf146))
- *metrics*: Description error ([86145d8](86145d893879abdb70c6acb604017a73e7b5ebc6))
- *metrics*: Failed to reset  success_blocks ([9ba656c](9ba656cd022dff0d24926856adb8b4297f642d2c))
- *metrics*: Received_batches and process_records someitmes is wrong ([70a500c](70a500c447e7a2e1e639dfdf88a1389b062b8e6e))
- *metrics*: Written_points is wrong for lush message ([e569ea2](e569ea24b234847046d79d82344093c7e00ba883))
- *metrics*: Local_to_taos cause panic ([13a8c91](13a8c91c224b43b3801572696aa148d8596f85bf))
- *metrics*: Written_points may be wrong for flat message ([4873948](4873948ace1a6b3668032fd2d9dbad51578def12))
- *metrics*: Created_stable  is always 0 for point message ([08a9182](08a918259ccfa8fa3203e052a5c846ce67619aad))
- *metrics*: Column number is wrong ([c8a8766](c8a8766a0cc770b5aa494c8599d79e6dac3589db))
- *metrics*: Metrics processed_records is not correct ([3a9c813](3a9c8131cb8a9d8938978b37a469f80627821dff))
- *metrics*: Can't delete metrics.json ([2a99c5e](2a99c5e340d54397efa1c41df9ff3fa0e4fab5d6))
- *metrics*: Auto-save thread not exit when task done ([570f3c5](570f3c500a1b4770e7f58c752ad8eef506a8fb6a))
- *metrics*: Failed_sqls is not correct for opc task ([9e57c3c](9e57c3c0e9217f0c2e792ff4faafdf416cadb0c8))
- *metrics*: Not reset failed_batches ([aff5bdc](aff5bdcef6ca4294783c14fc8b73306f26b24ace))
- *metrics*: Execute time is not correct ([a9d7a81](a9d7a81c2014af0b7003c648f762df1bb6b82651))
- *metrics*: Not auto update metrics for watting tasks ([7da1f84](7da1f84d527344a55ce6208d7cb7929a6600c86c))
- *metrics*: Auto save metrics not start ([b3ce477](b3ce477c18dd66eabca9f81b836cee05713fe78d))
- *metrics*: Execute time not correct ([1ba0e53](1ba0e5325dcd9153eca65a25bffcaccd8e6f83fa))
- *monitor*: Fix some process metrics ([e128e52](e128e52f17673fd4926fd84c37738c55e84319ef))
- *monitor*: Disk io rate not accurate ([c7e8645](c7e8645faea753179c401d3aeb77b774d55b7185))
- *monitor*: Datasoruce name is wrong for pibackfill ([41e4110](41e411026ea2da49cf5aed5678fb0cad22a333d3))
- *monitor*: Cpu usage should less than 100 ([9d2706e](9d2706e310bb91918b83ebe0b00a68ed4cfd018c))
- *monitor*: Agent cpu usage is not correct ([8d8c904](8d8c904e788555716314618f7ee5d075b8a857d8))
- *mqtt*: Report in batches to avoid exceeding the length limit ([8ccc422](8ccc422183d8fec16be17e522a144dc32a904d54))
- *opc*: Fix opc point id pattern match ([4489216](4489216c6048eededab9c27fac3bc88b6827fe38))
- *opc*: Fix opcua BadTimestampsToReturn error ([7db326d](7db326d5e10208d112a2251bcdf2c7e9f2830aff))
- *opc*: Now opc will automatically extend column length ([aadd080](aadd0808790de2b896ce938b3d22b60fecd87e8b))
- *opc*: Fix opcua BadTimestampsToReturn error ([6898a51](6898a51dbfa193d3af089f8b6023a9246a8b61ea))
- *opc*: Fix opc da add tags and add ci test ([e851ae0](e851ae07b177f3048e3f4fe8a9bad927f6c19746))
- *opc*: Fix opc ua server ([accd133](accd1335b02af3d567da8f6bf1fdfe076d4b894b))
- *opc*: Fix opcua test endpoint ([b15c4c0](b15c4c0f92987d78f3afad6dad1482eccf73ecaf))
- *opc*: Fix log typo ([c2c8ed1](c2c8ed17d556cb350cebe61c4e1c53ec5c22259d))
- *opc*: Fix log typo ([ea6a613](ea6a6135c81ee4d99efcb449af07122e37b38014))
- *opc*: Subscribe in batches based on MaxNodesPerBrowse ([296eed1](296eed1138c7f19f834249f1a21db4257d40a384))
- *opc*: Upload read completion time as receiving time ([4814b53](4814b531ef77096376f58198c805dbf835de28b3))
- *opc*: Fix message list hungry test ([339d94e](339d94e8f03590aaa27c8ca8fa06d53a3ad05e05))
- *opc*: Fix opcua browse and get value panic ([490e460](490e4606d4c0fbe9cba19429e0d97e2cfc2a9154))
- *opc*: Fix opcua browse and get value panic ([74bccaa](74bccaa28f1aa8afa787e214a0f4f877c1fe415f))
- *opc*: Close the previous arrow reporter when creating the arrow reporter fails ([fbbf372](fbbf3722072b0cfc56d359eaaa50b1bb3531f1b1))
- *opc*: Delayed retry after connection failure ([c2f1219](c2f1219b03979ce2ae3d5430069dee003f05733c))
- *opc*: Security_mode and certificert params is not remove when parse as options ([515fd31](515fd316593af0318f1798c6e53949e3fedb9525))
- *opc*: Ignore subscription error ([7eb7b69](7eb7b69d0ad8d5da03c9e97a140034809a9b0871))
- *opc*: Fix max body size ([ab6fbf2](ab6fbf28d7114e53063dad53a29a2d027c8d03f5))
- *opcda*: Fix endpoint description to clearify the requirement ([69a475a](69a475a9f5728272498dee9416fcee46a169ea5c))
- *pi*: Make system name optional in case of data archive only ([e1ac6b2](e1ac6b272486428f498c43844c77d03af7778d46))
- *serve*: Limit max activities per task/agent ([c20eea8](c20eea87e11d6d0fcde4528832bbc2b51d878521))
- *serve*: Limit max activities per task/agent ([c429eda](c429eda7ad638e1e44a3aa4fd726e0b2d98c1115))
- *serve*: Fix ipc handler greater than 1 cause stopping stuck ([c15e4b4](c15e4b42199efa8bf213039a58c699e1441413f4))
- *serve*: Suspend tasks when license expired ([86352cc](86352cc6e3200662f865c833b43becbe4235b9e5))
- *serve*: Expire time compatible with seconds or days ([a763d4b](a763d4b34731cafb63e53350c629f8365f594523))
- *serve*: Csv completed task should not be ticked ([e829ae6](e829ae67acb89009dee9b27d70e4ca19b9c50af8))
- *serve*: Fix failed count always be 0 ([8fe5f52](8fe5f52888cfea40b159c076a8e3da6e31d04def))
- *tmq*: Fix write_raw_fails metric ([711d1e9](711d1e9bd2e1ec3439a3218f4b6b3e5fe4a39313))
- *tmq*: Automatically migrate schema when sync data ([d97745e](d97745e8c4cd3e6ec9d9a887a1b2bcd65c25ec74))
- *transform*: Regex matching with binary str error ([4849a18](4849a1857de38410cda478e1cfe9ea225c3649be))
- *transform*: Regex matching with binary str error ([153f16c](153f16c8903d669194e87584889f1cac7baef22d))
- *transform*: Fix nchar type writen error with 0x0118 ([fdb6630](fdb6630012e1e285390f28bf352e8ebba21baf83))
- *transform*: Fix null values invisible in modeled output ([29eb805](29eb80541b9590cd4781f76e5e26a54ca018e8ae))
- *transform*: Fix flat stream simulation precision error ([83c26eb](83c26eb257187da93d9698a1c36ccfa18d8ed425))

- Maximum nofile limit in systemd unit to fix resource unavailable ([df79345](df79345fd1ce356411589dbcb158981385fbc84c))
- Sync history exit since connection timeout ([7967fca](7967fcaa10299e331a569478ee3c19bad0b938cf))
- Connect reset by peer ([cbee3dd](cbee3ddb7dcf25daec3794caa0dbf1f1830cb9dd))
- Tcp stream set keep alive ([d4e0a89](d4e0a892be88bc2ca0030c2b22de1030cb1992d9))
- Use local timezone to convert naivedatetime to utc timestamp ([f107234](f1072345edd44308f481796723e9a2d19ef7e6e5))
- Sync-history and sync-live add ack reader ([1a1233a](1a1233ac04b22a0933e99e49b5407042093e4fc6))
- Use local datetime to query ([76419e5](76419e5599c784016672c748bdb2e13e14cac6d4))
- Add sync-live debug log ([3f7ab79](3f7ab79531d4b33e6f8bc8a3a90bbc8bb1aed07b))
- Sync-history add ack reader#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([4252938](4252938a7fdfb604a3276a1d291195d14b4f6bf4))
- Errors caused by overmuch connections ([ac7215d](ac7215df0a4f6cfdfc3b44f5963c6f60c4a3db71))
- Manually setting timeout of influxql. ([5d85068](5d85068f4998f6dadb3769a05f8718e52d78dae4))
- Pi connector only use point mode ([0df8b68](0df8b6842451beace482fdb7c49e91750fc69feb))
- Template observer error ([bb26125](bb261259e4f7bb7d0ca627443026b74c8706e301))
- Fix bugs on connection closing ([792fab5](792fab557784212dc42d6930bc5fe998e9d3bb88))
- Performance optimization, resolve errors when there are too many subtables ([18ff057](18ff057181ac6756c975e359ffc3534bca469bc2))
- Batch_timeout unit is second for opc#[TD-28020](https://jira.taosdata.com:18080/browse/TD-28020) ([3e06f4a](3e06f4a339d7b1107ffa2c81adad759e6f801e05))
- Batch_timeout unit is second for opc#[TD-28020](https://jira.taosdata.com:18080/browse/TD-28020) ([b87686d](b87686d373637edfc726da03e211ebf9ce2b6390))
- Fix 0x2603 table not exist error for flat stream ([debd6c0](debd6c06c9ecb87c03af80f1377475e180ee9da2))
- Fix 0x2603 table not exist error for flat stream ([ff245fc](ff245fcaea4f1815d173cb03f4bf950415653ead))
- Check connection number limit ([1eb91fa](1eb91fa9b31818458676275e7bdd2d8ada855f27))
- Compile error ([9c81cca](9c81cca707ff50d74bce9758871789f803bde20c))
- Fomat check error ([5beb879](5beb8792bfcb1122d4d4423f2fd22c5b7bb26340))
- Format check error ([4fdafb1](4fdafb1cf7b70e28b2e972b0c7ae50509b7e3ca6))
- Typo ([eb1b078](eb1b0787922c2df500edc177efd62631bc49d691))
- Add ipc ack reader for kafka source#[TD-27829](https://jira.taosdata.com:18080/browse/TD-27829) ([60cd436](60cd4364dd887f7d4aff3982bc79c7a6a1f66ff1))
- Kafka source set ssl#[TD-25437](https://jira.taosdata.com:18080/browse/TD-25437) ([7ee4ddf](7ee4ddf5514e61479c118d7e769fcdd577750f15))
- Compile error on linux ([ff76669](ff766693a5859900f6ec15a3945908a34378d6ac))
- Catch the exception of recycling connection ([a105361](a105361d778f9c6c4b83669333a2d31cb6ac21da))
- Catch exception of creating DataThread ([2c02eca](2c02ecab739142a2949a07a2a3ce750561b6e7c9))
- Compile error ([a70bcda](a70bcdae6fafcf27877f2be68a300271ecc7b97b))
- Rename historian to avevaHistorian ([60c70d6](60c70d68dd27f96f67be4aed8fd2697b7fc17c54))
- Metrics thread panic since task_id is None#[TD-28159](https://jira.taosdata.com:18080/browse/TD-28159) ([37d475c](37d475cce3ca70a82a3efd27bddb997842ec05ed))
- Spelling error in documents ([3b8be0c](3b8be0c3e73cd3147a88b699dbbee45e5e77bda3))
- Remove unsupported advanced options in UI and add tagListSize in collection config ([21a619d](21a619d48d5155a98ed2adec899b55d9e79e093a))
- Compile error ([cb4ce05](cb4ce055cdd7a413805d238028350f967bfb8f7f))
- Only connection config needed for validation check#[TD-28268](https://jira.taosdata.com:18080/browse/TD-28268) ([512c7c8](512c7c82bbec9232cc8c34f5ac83d81fff2da77c))
- Only connection config needed for validation check#[TD-28268](https://jira.taosdata.com:18080/browse/TD-28268) ([1512d71](1512d714281c20e8ab0f363899e52ff91a607096))
- Close [TD-28268](https://jira.taosdata.com:18080/browse/TD-28268) ([dab8a0f](dab8a0fc96f913c407885563820ccdc0392775cb))
- Close [TD-28268](https://jira.taosdata.com:18080/browse/TD-28268) ([a7cb639](a7cb63980c549b08982956cdf58bae2f94599710))
- Do not check the lisence of opentsdb and historian ([901bd86](901bd86ea2607bbfe78745fc471d2627f1fae8c3))
- Read_concurrency hidden is false#[TD-28201](https://jira.taosdata.com:18080/browse/TD-28201) ([536a254](536a2544ca96d09b2e747d4133de9dd3c0fb99e9))
- Format code with 'cargo fmt' ([30eaadf](30eaadf4e5a3dc2e1225951857d61729322a4fa2))
- Optimize valid source ([4e5cafa](4e5cafabddbab2c04e4e5fcf649da95619e14ca2))
- Backup strore cause unknown data source ([6d30d94](6d30d94fb4bde90e7f05e2bfc942b0756db81491))
- Backup strore cause unknown data source ([971712c](971712c26578c43475065629c65b08bc2a51f307))
- Compatible with local to tmq ([ef34ec3](ef34ec3d16535449b0d22529e99550515e5133eb))
- Compatible with local to tmq ([730759f](730759fbc7f54728372c2a63ee98c628364fa7b5))
- Set default value when concurrency is zero ([f9d8158](f9d8158a77e015ce4e050ac7a458621ce18072c5))
- Drop the result set when the traversal is complete#[TS-4466](https://jira.taosdata.com:18080/browse/TS-4466) ([544afbc](544afbc684eb2b8081f477f9d7429d7c4b071d77))
- Typo ([8060ba3](8060ba371ebddead911ec02a51a649e4db86e233))
- Typos and warnings ([6e13166](6e13166506cfc28ca0bc7aa5904232a5ec734c68))
- The result of influxdb is empty ([80226d6](80226d697ec88b9a54838466e5dd19b2776c847f))
- Check duplicate element name ([35928be](35928be27b078dfb1be56e1a90a0e185997c3da6))
- Exec sp_columns TABLE_NAME may return none when database is not specified ([f875394](f87539423b5eb1a477481e34072e2ca408292c17))
- Support dot replacer for table name ([ab81bfb](ab81bfb2219d9b9f9858f43b90905f44c82f2148))
- Replace oem info when needed#[TD-28526](https://jira.taosdata.com:18080/browse/TD-28526) ([26ef8e8](26ef8e8bbf0581279291a35601fcbe028f9f3f8c))
- Replace oem info when needed#[TD-28526](https://jira.taosdata.com:18080/browse/TD-28526) ([2826427](28264273a1fb439421761f59c0cbad9fb134ce28))
- Check authorization in validation ([103d8b7](103d8b7f071c61a5adfb00a605d8a546473b9337))
- Divide by zero#[TD-28487](https://jira.taosdata.com:18080/browse/TD-28487) ([eee5828](eee5828af872400305fba8b8712089f20986546f))
- Metrics description ([a2dfac2](a2dfac2b537fd1d749fd8c55d8b458c74cfd5fdd))
- Replace dot in table name#[TS-4388](https://jira.taosdata.com:18080/browse/TS-4388) ([3cd773c](3cd773c13b553ef5f3284696047e2cf50333e0d3))
- Remove invalid advanced options in opentsdb and influxdb#[TD-27175](https://jira.taosdata.com:18080/browse/TD-27175) ([fcd4641](fcd4641f311283652c982220fed7447e08d28c32))
- Af element table not used ([34394f6](34394f66f7e6a8ed7e35ee2f19fb87eb48bd6f45))
- Af element table not used ([e3ff1a4](e3ff1a4efa91db3ebb3eb61c240d141472fc483c))
- Backfill without af ([54b2958](54b2958e547e74e6030515db24a6e76d2ed6e7a0))
- Backfill without af ([5b981b4](5b981b445b79cd580cf59cfcf32645507d271e67))
- Default rawdata path#[TD-28596](https://jira.taosdata.com:18080/browse/TD-28596) ([e18a2d8](e18a2d868b4d304df1769cc11bb9b1309258208f))
- Stopping task can be deleteted ([63db6ab](63db6abaa1c883b297ecb9f4b5d5856d31d3aaa3))
- Tcp connection error when use 1000 connections ([a539e7f](a539e7ff57de63845ab792bc5a4210287b71b15f))
- Hidden batch_timeout for InfluxDB and OpenTSDB ([fe7506a](fe7506a01bc70f5e61575ebd7288fb0512718f0e))
- Code format ([270225a](270225a1155c09e52b1bdecf76f691bfd8e299bc))
- Infer sample data schema#[TD-28669](https://jira.taosdata.com:18080/browse/TD-28669) ([4896611](4896611eefacf1bfe2b6614f584592efb05614d6))
- Reduce concurrency to 128 for opc ([848da43](848da438e5abda7c18348a92b80c5c6e40020641))
- Add timeout for parsing paths ([19669cf](19669cf773a82e728e5da13f83fb2f8660edf68d))
- Code restyle ([ca90662](ca90662f7f5188d310e09a11ca75c0f4350fe13f))
- When type is none, use stable_prefix 'opc'#[TD-28755](https://jira.taosdata.com:18080/browse/TD-28755) ([43cab74](43cab743b4df0140b87b3ddb2f33f35b171b47b1))
- Rts and ts transform do not support h for hour#[TD-28753](https://jira.taosdata.com:18080/browse/TD-28753) ([4a3d8c0](4a3d8c04960606fb87c6068b0951d0db8cfb5f2b))
- Modify docker ignore files list ([3e95846](3e95846bd6df80af7785b96d159291d39cc95f16))
- Modify docker ignore files list ([2717cd1](2717cd116a094b9440e584e8489fc7c4fcabd827))
- Typo ([b173297](b173297462806f4f513c7babc482db3c5b35366d))


### Enhancements

- *agent*: Use RollingFileAppender ([8c63f12](8c63f12c34d8f982bbfc1a33494dd8efcc30079f))
- *data in*: Text optimization ([9540fcd](9540fcd3d31142776d3a26903be64e719ae1cc13))
- *legacy*: Add timeout for source edition check ([5b7962d](5b7962de26c912eef9d245f28ddcef1218942d1d))
- *legacy*: Refine log of worker ([b1b576c](b1b576c970f2da685066ad8fc700be66cb37660a))
- *metrics*: Use milliseconds as unit of current_execute_time ([7abb4fd](7abb4fd11a15dc30f7cad1fabb8bff2ee15f0558))
- *metrics*: Unified metrics names ([6859467](6859467bc9d0ce70d0e1f0b10377bc69d39de1cb))
- *metrics*: Refiine metrics of TMQ sources ([6bd86c6](6bd86c63dc6bbf55b25a8cf71b6f6932c90a1550))
- *metrics*: IPC metrics enhancement ([361bcb9](361bcb990e7acb5f005330910b0660b6d313ffe2))
- *metrics*: Init metrics for task with agent ([e9465a3](e9465a32e1495d30bae7a2434cec95a1e6b1d0ff))
- *metrics*: Replace MetricDB with MetricsStore ([1d2aa04](1d2aa04dc842bc88ff324e383f691ebb2d0740f3))
- *metrics*: Refine description ([0d5fd18](0d5fd18f3b1ef00bedbceb26df0d8419e4d3986c))
- *metrics*: Modify discription ([f3d6dd4](f3d6dd4012e56ce2d3a870b28d7b8e47dfbfbbee))
- *metrics*: Rename processed_records to processed_rows ([37ad5f9](37ad5f9ef8b9becf9749b60a9ce52327f5d3f108))
- *metrics*: Allow default metric value when deserializing for compatibility ([660540a](660540a6f8f6f817f399cd910fb3f163038b0b99))
- *metrics*: Modify task-metrics api ([76e7435](76e7435bfd88cd892b24aead44f40c61e2d53631))
- *metrics*: Update  metrics to 0.22.0 ([c4a0ad6](c4a0ad6b216bb250358bee7c0576a467b0e9b788))
- *metrics*: Enhance tmq write_raw_fails statistics ([1b08981](1b089812c576666e204b0dcd222330eebe1d63dc))
- *metrics*: Change description of written_points ([cf04378](cf0437857f2ae6afeadee6fbe7204666aa172532))
- *metrics*: Change log level to trace ([b1aafbe](b1aafbe9ffc2668ac3bcec7a943031dd23c6d7e8))
- *metrics*: Refine description ([2b10fc7](2b10fc7030789299c2c128ca06ba09e609b8a061))
- *monitor*: Change log level ([0b76060](0b760602f0fe205bc4bd3bfc9e3661d51eedb02f))
- *monitor*: Change log level ([303349a](303349ae8835b516d4c7f4e93abbc95ff4d132fc))
- *monitor*: Restrict monitor interval from 1 to 10 ([9dd3951](9dd39518049f9e1fb419ec10c359b2972e520b37))
- *monitor*: Retrict monitor_interval in config file ([7f0a561](7f0a56100919ca5ee09a03989b216226e122d2d0))
- *mqtt*: Support mqtt dump ([afa0bc3](afa0bc39b83652a9dc0859f1f9b22e76077021a5))
- *opc*: Get namespaces when checking connection ([702206c](702206c8d43c7a7645c8ae69cb02bfab94b1aa2d))
- *opc*: Concurrent browse ([078c8dd](078c8dd89dc317766b87a13e8fd7077ea49f6ae3))
- *opc*: Add browse filter ([6763dce](6763dcea4cd058152b131507b194816d8115deff))
- *opc*: Return namespace list on opc server when check connection ([ca7a3e8](ca7a3e8978cf0ac591eff10ff7932e2c941d39d5))
- *opc*: Add async api for download opc datapoint csv file ([8780756](878075698a0b449ead0e62ccc82c251bdd180b72))
- *opc*: DataSourceValidation add namespace property ([fe7a6db](fe7a6db74401f0e15814e1f4441290782298fcfe))
- *opc*: Opc ua yaml, modify connect options ([01e68b6](01e68b6201712371d39eaa5feddff7f3a05d7b0a))
- *opc*: Opcua yaml, add point filter conditions. ([8277115](827711581fcd3f6e730503dbd2ea846c975b8546))
- *opc*: Modify opc csv template file, add val transform ([4b88dfe](4b88dfe3fbcfdf93c41a994c262314fa02033b3e))
- *opc*: Cargo fmt my code ([c0a1788](c0a17880489fc4983e6f3bf17f38bf08f81ebecc))
- *opc*: Dsn from_str, must import std::str::FromStr ([f27405c](f27405c956f447e665332a7759c22dc54984e96b))
- *opc*: Modify  opc point page api, use object as the result data, not an arry ([732939a](732939afcd54c3c68a266347d66449f6a916f351))
- *opc*: Cargo fmt ([ed6e9fc](ed6e9fc8c8cff8df8d09075c3ef2ce9ad91e4621))
- *opc*: Modify api, download empty template ([2f95660](2f95660d79fa2a2c46dbb7c2105dbc1a6830e040))
- *opc*: Secure channel certificate and authenticate certificate ([4b5646a](4b5646a3105ed19921a09f172afc91b444c97397))
- *opc*: Cargo fmt ([3c52489](3c52489c1e16d7046aac9877d768a809b5a11cbf))
- *opc*: Certificate params add to toml file ([0292a15](0292a1574f5201d6b221ff5bd854288ca47ea45c))
- *opc*: Support secure channel certificate and auth certificate ([5ed9c31](5ed9c3154fd7f9e2fcc9a0884923ff70576af7ca))
- *opc*: Add certificate tool ([6ddef59](6ddef59138703eefad07821a545e1ab543645daa))
- *opc*: Namespace empty string check ([98b3aff](98b3affe3d4eb493b407171b9b75db551b935fa9))
- *opc*: Add log for debug ([337606a](337606a4433564f27c128b35612e90b89544c808))
- *opc*: Modify debug info, avoid misunderstand as the data sync task ([6edb66e](6edb66eb8ed46170cdce4a3a3308d9eeb23eed81))
- *opc*: Commit error file, roback ([a326404](a3264046a21defe5b109e2712f6f9af2a3a5878d))
- *opc*: Point data page api, 1-base ([337083f](337083f533d7f1cb5636e2e03556b6f271b50a96))
- *opc*: Modify the name of security mode ([91fc710](91fc7101c41917480c784f83bdb95b50b15f12e9))
- *opc*: All of 'opc ua' write as OPC UA ([1eadd7a](1eadd7a97e5f0cbd69ba6247ab48f664d86326ac))
- *opc*: Try to get server capabilities ([f2362c7](f2362c78cea91659640714923dd73af3fb20e06c))
- *opc*: Text specification, leave a blank between chinese char and alpha ([c10bad9](c10bad97b52e19aba8f3da62a527aa2067e7fe1c))
- *opc*: Opcda root, use string[] ([676bfef](676bfefc0c76e4c0a3d5d29e218886d0b21e3501))
- *opc*: Cargo fmt ([35526d6](35526d61b69d1fb44f9bcfd502c2b8fbca36dd50))
- *opc*: Encode and decode the certificate file when use agent ([d571c48](d571c489363e20cf167e053cb83f703a92a50d62))
- *opc*: Validate with agent and certificate file ([13504d2](13504d2ed5a18f04dff30eb138766c894cd2003b))
- *opc*: Log the opc config file content given to opc connector ([f2efbac](f2efbac350676a1438a126878cce32caad82261b))
- *opc*: Opc task agent show parse certificate file ([3f196f8](3f196f890f25982ae24890453663eee78e8d8d44))
- *opc*: Point  id and name show be safe for csv ([ebdf804](ebdf804f9321ad5f3f3c77e623c50ad4f0a2a90d))
- *opc*: Cargo fmt ([091dcd9](091dcd95667b227b14772759197e052a6943fab9))
- *opc*: Temp file for both csv file config and select point config ([738a500](738a50079ba06cffc9b110be341988d62e14a419))
- *opc*: Csv template title, sn, varchar ([820c612](820c61211c979faa68fc6aa413209094d6633eb4))
- *opc*: Modify the expression text on opc config page ([5420e8e](5420e8ec349bb4c91ff1c4c347b510e6ce6b2485))
- *opc*: Misspelling in opcua/opcda yaml ([df3c9a4](df3c9a43f7f8a18b33a2e029df5017b270d1e066))
- *opc*: Remove the redundant description ([2271d29](2271d29e76d55c89c83bf81ad753f7894cb2b4a7))
- *opc*: Optimize text prompts ([648f719](648f719f551ac1a244328ff5fb03fe4418372db1))
- *opc*: Optimize text for opcda ([32d81a0](32d81a076db74c852f3435c1a0c16e82973b4502))
- *opc*: Opc csv header ([5f7d50b](5f7d50b1c7c5e34ab282d4a49359b0e1ccc0c3d2))
- *opcda*: Add filter for data points ([dbef3a3](dbef3a38857eab4648309bcefe671f97eb81eda5))
- *serve*: Expire duration with seconds ([08de334](08de3341ace68cec347f200912b1b621499408b6))
- *tmq*: Optimize the description for params ([ec2bf69](ec2bf6916583863fd3a076cbf777e3cd68ad0c3f))
- *tracing*: Remove mut bound for RequestID::next ([45b9e68](45b9e680f37a536530762b936845683b3d863a3d))
- *tracing*: Enh: make req_id start with 1 ([8d7eeab](8d7eeab5d7857963dd9f6a1deeb94e284729ed5f))
- *transform*: Use new_null(n) instead of empty string ([8449966](8449966fe2a1bed6e6392f9314ca3e9937175f81))
- *transform*: Add test case to timestamp expression ([e779875](e779875ae4d9ecc1c8e623fb092b074e60cb9000))

- Exception log ([6cca070](6cca070ffddae1d2151a9c8b5ef4605a629cd6a0))
- Rename a function ([23899b3](23899b3e82a73c9f64d02dd7c5234f64d51cdcee))
- Make req_id start with 1 ([bcec58d](bcec58de1fc477ffbef473ce5e9f38cf628e1523))
- Optimize example file ([6eea869](6eea869aa2deb01d096a308b1690cac0043c0796))
- Refine metric names ([775d75f](775d75fd47b3563845c38f878db4976002f472ca))
- Performance optimization #[TD-28007](https://jira.taosdata.com:18080/browse/TD-28007) ([654a309](654a3093c3f8891713fcb3451fbb524fa020246c))
- Refine error log ([ac11c89](ac11c8906b7b11b4eb8816d3af48e8d8ed4d5578))
- Metrics description ([46ded86](46ded86c9b0e5421c336a759fa766a356cbe38cb))
- Rename symbole ([99aebff](99aebff70031df1f320178f43a0b12aa96f51d57))
- Delete or comment out some code ([1f1748c](1f1748c08f3e79b6e1185fd78f035ce4b9621a7c))
- Performance optimization ([6d92d53](6d92d530dd1184ed7906354a366a61bd61e99538))
- Delete or comment out some code ([287d619](287d6190eb26c905f4489f09ce65710603871f1a))
- Log level configable ([c72bd5d](c72bd5d68ce614312d61f2080386758e3a8a53d3))
- Print config file path on start ([efd610e](efd610ee8fd0c957ae0c0b05c6f066fd92b90065))
- Format code ([bbe4bf1](bbe4bf1a3737109f7c0739f94f23468c7429cb96))


### Features

- *compression*: Log out compression when agent start up ([f8511f1](f8511f1aec3bcf0b9dacff4b0cf44fe5e9021e7e))
- *deploy*: Enable taoskeeper in taosx/integrated docker image (#1378) ([f4c00fd](f4c00fd67ae0bc7a8790247f0649c32ffd699ee7))
- *legacy*: Add `sparse` option for legacy data source ([dfdb9f7](dfdb9f792d6bfdf9642bcf9d0c1bb2e4e29eceb7))
- *legacy*: Support new tables data migration when syncing ([9f28b33](9f28b33a5e6d89d3fbab53876f39f04b33cd271f))
- *legacy*: Expose schema-polling-interval in explorer ([a79d3c7](a79d3c723f19d9fac8119634385a9d248c0c86aa))
- *metrics*: Define  TMQMetrics ([e79715a](e79715a50c8a50fea85b54845eb9ece3995c0f05))
- *metrics*: Add IPCMetrics ([06cdaa2](06cdaa2d87f3ad442da8defb548bda5e60fa3496))
- *metrics*: Auto save ipc metrics ([3f2e9a9](3f2e9a9f5aaf4256ffa475fac8ba3ac9e7df2205))
- *metrics*: Auto-save metrics in scheduler ([0c39f13](0c39f13a315a42684bc6c0592e42d5c1df7f22d5))
- *metrics*: Add taosx-metrics crate ([24e7ed7](24e7ed705cbb548e8bfa657e2190d953e23c9607))
- *metrics*: Encode and decode metrics events ([20b33ba](20b33ba2f8e53bc3d743f0a1afd2124d8ae2150a))
- *metrics*: Add prometheus compatible render ([93f304f](93f304ff80c59f6ae7f8641ea71e7d3fc0d215f1))
- *monitor*: Finish monitor framework ([3f88ebd](3f88ebdb3db08babf38fcef2eef85565d226fecf))
- *monitor*: Add taosx process metrics ([1146c18](1146c1895da569e6076ae328ce4f1799dea6c50a))
- *monitor*: Add agent metrics ([5719c82](5719c823c9075d05a6e9d1c3bf2d7f7a231258ce))
- *monitor*: Add util method for collecting connector metrics ([b2f1d8a](b2f1d8aa984d0eeecc5ae2781fc47bc3fc844879))
- *monitor*: Collect sub process metrics ([4999f6f](4999f6fcc0bfccf1badc84685d7e8bb51df928c9))
- *monitor*: Push metrics to taoskeeper ([4188d26](4188d260818155ebd271f9af1d83cd8a73a5a1ac))
- *monitor*: Add tag ds_name to connector metrics ([9fbf549](9fbf5490c8e9d54286ea03c03eb85435c71ac458))
- *mqtt*: Support advanced options ([35c0d87](35c0d872ef25d70a5365d0202545a0e9f64ab05c))
- *taosx*: Add gzip compression support for taosx-agent ([3f25a21](3f25a21d76582f3c0e23e6f299a67a5530bba84c))
- *taosx*: Add gzip compression support for taosx-agent (#1359) ([09d2ee4](09d2ee4da2c1d7a20acf7c87c2d0f744cc5bc07d))
- *tmq*: Support replica param ([474d466](474d466ed2b2deaf17c118ed8cab0ee4a45dd6e3))
- *transform*: Support extract in mutate ([ae059c1](ae059c19f70eec51a519f6bccf167467c816185c))
- *transform*: Support timezone output in transform sim ([51fafbf](51fafbfff833533ed07c64b94490b553fbf8c6d8))
- *transform*: Support timestamp expression ([7a65362](7a65362a41ab6d0cfc5cc56f84450350038eeb21))

- Keep ts order when generating config from csv#[TS-3997](https://jira.taosdata.com:18080/browse/TS-3997) ([e83812c](e83812c1f119a218f5cc4bb01cc3b1a3bf919bbd))
- Opc ts_col and receive_ts_col keep order ([5a40ae0](5a40ae05b613dd791916d27d16e5bdb02fb885ef))
- Add historian data source ([ec8b762](ec8b76290a3758899bb7240bc8ddbdbd89129357))
- New metrics mechanism and it's apply to lagecy ([0133534](0133534220d62aa48028c18dd208f7cb6740078c))
- Keep order of ts_col and receive_ts_col in csv config file#[TS-3997](https://jira.taosdata.com:18080/browse/TS-3997) ([61c5611](61c5611098c2c9f55427c79ba0be0bb8c2a0ecdc))
- Historian support advenced options: batch_size, write_concurrency#[TD-27740](https://jira.taosdata.com:18080/browse/TD-27740) ([1951409](1951409ae6f44f9fd90322f6191f77090e8a8d3a))
- Support write_raw_data for historian#[TD-27740](https://jira.taosdata.com:18080/browse/TD-27740) ([7b11308](7b1130852da9c272bfc569995783d5df43c4142e))
- Use custom oem name in datasources ([639c045](639c04543bd4a83398efdf1064aa9a6795eb8fe3))
- Support setting log level ([1146a43](1146a437e5098373adfdf7175455b389461226cd))
- Keep raw data for historian#[TD-27740](https://jira.taosdata.com:18080/browse/TD-27740) ([2568b01](2568b013b97f7631934d674a6f1d700f4856cd95))
- Kafka use task_id parameter ([19a68fb](19a68fbf0fecba51f856fabca827c3cd9f086c3c))
- Add debug flag in version print ([d3bbaa0](d3bbaa01e28d6cb1c6dab2f2774d7b2c03389744))
- Print configuration information at startup ([0a71aaa](0a71aaa2d1596745b35f6b9f9564626884086119))
- Add debug flag in version print ([a82b314](a82b3140f74d597fe1636849d1ca09f10cd5f845))
- Add debug flag in version print ([7bccd8e](7bccd8e8ebbd42fe447bf646005e2b1fdfbc36e7))
- Add debug flag in version print ([353a80c](353a80c52831de938b037fd6e2284ae5a9392bc1))
- Add debug flag in version print ([cddb679](cddb679d6b8f781691f3bf61d3717c758643ad18))
- Kafka support read_concurrency#[TD-27739](https://jira.taosdata.com:18080/browse/TD-27739) ([b60033a](b60033a29f5b6807dbb9806759e31d319b96e6bd))
- Kafka support read_concurrency#[TD-27739](https://jira.taosdata.com:18080/browse/TD-27739) ([3516225](3516225b89a35ebdeeae704fad7cd7ac39de3a9e))
- Rename historian to avevaHistorian#[TS-4398](https://jira.taosdata.com:18080/browse/TS-4398) ([dcaddf7](dcaddf79a47863630a14cb11c94c9bed47628f90))
- Check jdk before execute commant ([51613d7](51613d7b54f7a74b6595f7dc683ec28508d55fcc))
- Csv support advanced options#[TD-27741](https://jira.taosdata.com:18080/browse/TD-27741) ([190034b](190034bd7b8a11e96940edc8257a4c7eb42cc550))
- AvevaHistorian tags support wildcard *#[TD-27909](https://jira.taosdata.com:18080/browse/TD-27909) ([0fb166c](0fb166cb1edf8d40a9c29dd48626f19dec95df44))
- Bump MSRV to 1.75.0 ([e7f4bdd](e7f4bdd35995f5ef89c5f6d69534514513bc80fd))
- Move DsSampleIn into taosx_core ([19b0a4e](19b0a4e028e6e816197061457fbd2b680a4ab021))
- AvevaHistorian support get sample#[TS-4227](https://jira.taosdata.com:18080/browse/TS-4227) ([2b6a64b](2b6a64b11c9e164a0c0eb3b6897968d9274ac04e))
- End to end coverage report ([c19b9bf](c19b9bf3006fbf59ca09032f643283a815ee8c54))
- Query data source for sample data#[TS-4227](https://jira.taosdata.com:18080/browse/TS-4227) ([4880d71](4880d716ff5592bc429a27047cecf19f96d75cc6))
- Support new csv template#[TD-28370](https://jira.taosdata.com:18080/browse/TD-28370) ([376a5ad](376a5ad34145b7e1ac867294b99acba0848c343f))
- Support break points for avevaHistorian#[TS-4355](https://jira.taosdata.com:18080/browse/TS-4355) ([c23c8b7](c23c8b746a16cd504cfdda25a1e29637b6e0f96e))
- Set empty when cast notfound ([4800521](4800521ba133ca35338278dae376f4af8b8ce584))
- Active-standby replica management command ([9c8f82b](9c8f82bb42a2f1c26cd2b26ac1818ab0b6d68185))
- Support value,ts,rts transform#[TD-28370](https://jira.taosdata.com:18080/browse/TD-28370) ([14dcf15](14dcf15f3abd7dd99897dfd9f243388e940662c1))
- Csv transform#[TD-28370](https://jira.taosdata.com:18080/browse/TD-28370) ([294dec1](294dec196d5d8180c204c58e001e3eb0a080582c))


### Performance

- *agent*: Improve agent put stream performance ([bcd2016](bcd2016709ae2b1783a104b10909ad2cb1cdeb49))



### Refactor

- *metrics*: API for get task metrics ([a8668e5](a8668e5faaa355212b9d7669af129090935e1af9))
- *serve*: Better logging for task creation ([32e7b5f](32e7b5ff89d3a5551fdf63e8d7bc9f4fbdbdf4e9))

- Return query stream only#[TS-4227](https://jira.taosdata.com:18080/browse/TS-4227) ([1ad8d62](1ad8d62ff17b2bcab4fb4e92f4ed302e035b355e))
- Update dependencies ([25a788d](25a788d4baa4411022ff637cb76389cbfd9aeeb5))
- Update dependencies ([1893466](1893466c9b28249ca5423b7191051043e9ef00be))
- Dynamic processing columns#[TS-4227](https://jira.taosdata.com:18080/browse/TS-4227) ([8407c62](8407c621e0ed9fd842d0e07426f6a53c6c0f4e62))
- Check enterprise edition in target only ([d21c519](d21c519bd7261bb325913e42e573ad743a38017c))


### Testing


- Add test cases for transformer ([fb995a2](fb995a25b09ff06079b6596994634bb807be4937))
- Add test cases for transformer ([a532dd4](a532dd4b69c28531e79e9df070988e5107db80e2))
- Add junit tests, generate coverage report on github workflow ([189b397](189b3971709917aa23ac72cd4b7f1d423ff7ea04))
- Junit for opentsdb ([571b4c6](571b4c6a9de5979d206bf55692095410ab545179))
- Delete invalid functions ([c6c22c3](c6c22c349b5cc95ae1af388f74dd77729af196f8))


### Debug


- Add debug log ([0dc64f0](0dc64f0d440ebf49078701f04653dec29e8615d2))
- Realtime legacy to taos ([ece3ee4](ece3ee45fdfdb99991ec3a893596ee86fa7292ec))
- Add more debug log to realtime ([d4e6fd5](d4e6fd53ce5b76573c09932f883dfdfaa1c7d63b))
- Debug realtime task ([2840557](2840557de556de8cf3455606e45e12d3ec1be4a6))


### Fmt


- Format ([4f28c27](4f28c27ddcf42b1797a75ea23aec9989026545a7))
- Kafka use KAFKA_ID ([0df820c](0df820c50b7af340f37acf3e4cc2ff35dbbc2082))


### Impv


- Use ts if receive_ts_col and ts_col not exist#[TD-27842](https://jira.taosdata.com:18080/browse/TD-27842) ([75edf91](75edf914f674334d5610149c0c3b3b7e48418dfb))


### Packaging


- Fix error for packaging agent ([95d684f](95d684febb57a2c3d5acb41d7dca215936b2e13d))
- Fix error for packaging agent ([2c5f31a](2c5f31afaeb579cc92a96f2fcc5a7f5f14f790fc))
- The dockerization for taosx ([e9c977a](e9c977a231fe44ba6e156278f184de6393c5bd97))
- Do repo check and pull & use rsync -u ([a4726b3](a4726b358208c3bdd47af2d9af63e92d00270493))
- The dockerization for taosx ([bf2dcea](bf2dceaf6058397c404bf2c24f7ecb7234699942))
- Modify nas path of tdengine ([004814f](004814f0fb999bc505bcf1ab3750725f0f1f499f))
- Fix bug in start.sh ([0e7b8e8](0e7b8e817984102573e30ff4048f2c42a2898e6c))
- Modify for agent on windows ([b7a5039](b7a50395286fdad10307876ce58e7c624055b50d))
- Modify for agent on windows ([47643d5](47643d5faf692434fa0757dca0b411e15b5481f6))


### Refine

- *metrics*: Extract common metrics ([0815885](0815885421a1f8d6b7411a3f26915a63fd7a0b66))



## [1.4.0] - 2023-12-01

**Full Changelog**: [v1.3.0...v1.4.0](https://github.com/taosdata/taosx/compare/v1.3.0...v1.4.0)

### Bug Fixes

- *agent*: Fix unexpected exit issue when use with pi ([8bb2813](8bb281330d8442c8940049d7ae4367516eee037f))
- *agent*: Fix unexpected exit issue when use with pi ([6397d18](6397d18f2842ddfc420784d83e0c78d7af4be1d0))
- *agent*: Fix agent exe path in windows service ([2779cc4](2779cc4bd2ba7ad02775390798d1d86fe9b151c0))
- *agent*: Fix agent exe path in windows service ([4f20c26](4f20c26c88a7972efe67972e6b6dcecb9a480de7))
- *api*: Fix delete task timeout issue ([9333dcf](9333dcf5b14c3a92ae9e330270e5883e72a5fff4))
- *api*: Fix api time cost too long ([4abc30c](4abc30c1eb9e5e3b0a94731dab6392b30d814ea1))
- *cloud*: Fix cloud image data dir error ([e8fa93e](e8fa93e089997e09c79b6deac34cea81e211b079))
- *cloud*: Fix cloud image data dir error ([c17020b](c17020b55c907f19731438f820eeae23bc69035f))
- *csv*: Fix csv open file error when create task ([b543ba5](b543ba5c27b5b83836b3a3bacf4004e9b7efcb9b))
- *grpc*: Ensure only one connection for each agent to proceed ([613ae33](613ae331ea592e4006eed2e731ebc61427fcb44b))
- *init*: Set status as suspended for tasks in queued ([8241df5](8241df598e9ec7755d56403de459352c22d218ec))
- *init*: Set status as suspended for tasks in queued ([94ac8c8](94ac8c8949f74ae2d2deaf757d9606db08695f7f))
- *ipc*: Fix ipc socket close error on windows ([28b9554](28b9554c5a3260646360d19d90b8d78538bb3de4))
- *legacy*: Legacy can be stopped via scheduler ([814c16e](814c16e138606dcc46c2f873080c8407abfedee4))
- *mqtt*: Fix log_level required issue ([25c83fb](25c83fb142672bf06da1abfeda6896eb10034a6c))
- *opc*: Check opc ua subscription failed ([cd0b49e](cd0b49e4a9271611bee40eaabf0e8e91e7bcdf3f))
- *opc*: Negative number when converting uint to int ([6d6f0d1](6d6f0d118178182cef8bbda986f9722a52ed100b))
- *opc*: Fix opc upload collect time ([ec73dbe](ec73dbe1c9ecf4832b3dd9c47934400cc9ae12b9))
- *opc*: Fix opc upload collect time and subscription in batches ([dc4980e](dc4980ef8c61ad4536c2170aa5f311c336608bf0))
- *opc*: Opc csv config parse error ([1310c4b](1310c4b62dfb81c86eb47ac61c463b7fd7ca40d8))
- *opc*: Fix opc connection check error ([3250b2e](3250b2ed4160ccbb525ce026d8ea9b20b2e0c57e))
- *opc*: Fix opc value field being nullable ([73050e5](73050e5693df92865205761c13fa8bf88c432f35))
- *opc*: Fix opc performance issue with batch size default ([a9d4ddb](a9d4ddba46a2ca886d5f5f10548f6741785cbee4))
- *opc*: Fix opc da memory leak ([6298dc6](6298dc6f2e2841886c8c75ea4b2e993d0a4b4170))
- *opc*: Fix csv config empty did not raise error ([710a4fe](710a4fe4b896d87c26cfe8120f4960234ec79691))
- *scheduler*: Ignore unexpected suspended state ([ff866d0](ff866d0e3ea0fab4eee32c9286c005c2f3039a31))
- *serve*: Fix status resuemd to running ([bd244e8](bd244e8da392afce505bb8f94efd63fedb79a134))
- *tmq*: Fix stable name not exist error in target ([422c4a5](422c4a50f431453437a9b77c9b2f73efbea50db9))
- *tmq*: Fix stable name not exist error in target ([930ac1f](930ac1f8e7ef0f185744e1878e7f1d9b0b49d4bd))
- *transform*: Fix json path extraction error for bool ([2b28e92](2b28e928b4696e403434946411c8810779bf1f5f))
- *transform*: Fix json path with array index cause column not found ([a50a65f](a50a65fe853b066e31aabf80fa952e23f382583b))
- *transform*: Fix `as` not work in constant value mapper ([de38f6f](de38f6f801ff009b9bf0714c56ff06625c5f1b0b))
- *transform*: Require non empty `expr` expression ([7516f10](7516f10f5a704fd40a58e58cd51c968df9cf9553))
- *transform*: Fix split extractor error ([e795c8d](e795c8d6eadf16792b6f81677a38db459473b2b3))
- *transform*: Fix json path extract wihtout type cast ([7d84163](7d8416382fc07b97f15132ecb48cdccacfde3816))
- *transform*: Replace old field with parsed field ([5b64130](5b64130c599f67eee2466202dc05c3a2147ab48d))
- *transform*: Support unnamed capture groups in regex ([9f2e623](9f2e623915cae651a1a48e7ef761281fb6ad75c4))
- *transform*: Support simple cast like float to int in json ([092af46](092af46de10552b32e24e82afc682ca9a8a2b51a))
- *transform*: Support timestamp cast from integer strings ([689dc53](689dc53146bb7b80a3722400b1fccee3eb430620))
- *transform*: Fix join with separator result error ([ccc63bd](ccc63bd7f5bd4d9c6d3acadc8d7736b4cd73c384))
- *transform*: Require tags exist when stable set in model ([18929fd](18929fdeb9985fe98d7398ff06b29b254ed635fd))
- *transform*: Fix regex transform error when named group duplicate with field name ([9e2cb7f](9e2cb7f04573135b4e1730528bec8969d6d81473))

- Do not suspend task when agent disconnected ([ca09e78](ca09e78f20d8527b3cb26d2c8bceadcf7b0e9db2))
- Do not suspend task when agent disconnected ([f41f40f](f41f40fb4203589373d695a3620a17ba7066930d))
- Use termiate instead of kill for child process stop ([6dc8541](6dc854143e677c6c0a1d138c5040400fc183b817))
- Use termiate instead of kill for child process stop ([0dd1f49](0dd1f495808ff38e8c37672bb97a0e58b482e536))
- Fix stop cause status error when update with agent ([a98e642](a98e642059ebff00e4ee964ea7f95a903712c082))
- Fix stop cause status error when update with agent ([1cecbaa](1cecbaab23e9405eedd63715717b32a4edbd52a9))
- Username and password is required in historian source#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([9eceeba](9eceeba1d6461543e259e56424921f56e86f589e))
- Historian source config ([2fe989a](2fe989a081aecfa085b1a2b238a88c6b62de1504))
- Historian source config ([b777055](b777055fae7165917b08147a71566718077259d6))
- BeginDateTime and endDateTime has no value#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([b41447b](b41447bcd08575c12b1a96c79e19b90309362293))
- Lock log dir to /data/taosx/log in image ([b5343b0](b5343b01d4fba66409f1218df42d3ccfadc0bcfe))
- Metrics error for flat message ([016413f](016413fe53a624cb8b5536c7d24ee50ee7f6f2af))
- Check enterprise license when creating and starting task#[TD-26228](https://jira.taosdata.com:18080/browse/TD-26228) ([c3b6270](c3b627032c8bab7ed0cf0804a0804aac36d2fd58))
- Adjust mqtt config parameters#[TD-27199](https://jira.taosdata.com:18080/browse/TD-27199) ([110abcc](110abccfb9b8f1d4930fb5d2b16bd391cbadce84))
- Enterprise check when creating task#[TD-26228](https://jira.taosdata.com:18080/browse/TD-26228) ([6397e74](6397e743cd9e0bce3f9eaba157b2be9d75a4d94d))
- Update task could remove agent ([0a932d0](0a932d09a63d8ef5124f53f55dae6e89388e938c))
- Modify for oem ([6c236bc](6c236bc1fc4e14b6161ffdead95944595caa6937))
- Modify for oem ([5abffbb](5abffbbde43accb7a901399084f0dfd08180003a))
- Write speed too low#[TD-26849](https://jira.taosdata.com:18080/browse/TD-26849) ([2ff8e48](2ff8e4862b85f547f9ca3594325eda6aaec5648b))
- Remove ipv6 local port check to fix gcp issue ([b4e439c](b4e439c846a5e6a13135d53a8aed30b127274110))
- Fix some opc bugs ([416c5f9](416c5f912f39d8577a3d116a7b7e7431dfa2adf9))
- Write speed too low#[TD-26849](https://jira.taosdata.com:18080/browse/TD-26849) ([d9cbaa2](d9cbaa2eb8bbd74b869fcee0742a0accc79a9720))
- Remove ipv6 local port check to fix gcp issue ([7361d84](7361d84bbf1a7b318bb0b2c0e0ed08f677eb77ff))
- Update task could remove agent ([cf8f333](cf8f33337086336b463cbc2e995a4b9dbac8a7f9))
- Remove secret ([9020764](9020764d2ddeae9efa2bbde01a65394b74182714))
- Remove secret ([663a151](663a1516d22a48acaf22945c102a9d490c91996d))
- Verbose in subCommand ([ab17beb](ab17beb4bb453f52268f3c42223bfe9e5866cfaa))
- Opc advance options ([38fde9e](38fde9ef8d070e996b5b8fe5007c9ea5fc8eb7b7))
- Fix -vv/-qq behaviers ([65006bc](65006bce22177a58b3d82547ada2f4d9a23c86cb))
- Should config opc table config#[TD-27422](https://jira.taosdata.com:18080/browse/TD-27422) ([52f1815](52f18154bf69b1683f0343524fff9111a1a8f855))
- Data TraceId error ([4341d4d](4341d4d59ad4b63a3fbfdf3c1733e141ec93de0d))
- Created stables count is not correct for opc ([18eb8f5](18eb8f5f2fec20dc191d4c9bc8ed26fc982e472d))
- Typo ([4935fc9](4935fc9893ddad588ca73effbe60073f30d128cf))
- Get metrics from db cause panic ([95ffdcd](95ffdcdc90864942c5f3695d2f80b98dc833a86d))
- Enterprise check error in 2.6 ([e5810db](e5810dbb473c002241106a34d866ff7c510cc8ab))
- Enterprise check error in 2.6 ([1a3918a](1a3918a0b56d98f2b1f2d1c4b6b525cb71f46d8d))
- Use task_id as group id in kafka source#[TD-27312](https://jira.taosdata.com:18080/browse/TD-27312) ([548876e](548876e188ff296d0c14a5bd1cfa5061db71e963))
- Remove duplicated config in mqtt source ([b975dc0](b975dc0fbeeb31d61fd287c219109c0a72eb330b))
- Agent ts may be later than local ([f8bc2e5](f8bc2e58b4841c1aac05236d41a22dc53bed631b))
- Close#[TD-27312](https://jira.taosdata.com:18080/browse/TD-27312) ([68b172f](68b172f15403eb66538f165bad9b97382a0620e2))
- Compile error ([8ac2731](8ac2731f1eda46936fc08463f37fc2d560fa0f8c))
- Improve error tips in case target database not exist ([2543f7b](2543f7bdc7af00c02a688cd396eaeee18e21831b))
- Consumer hang since use sync api#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([5b1df79](5b1df793666c1db00dbdaa7ec736d839e7b51443))
- Add tracing info in kafka source#[TD-27359](https://jira.taosdata.com:18080/browse/TD-27359) ([1d747fd](1d747fdebca01aa2d5b2db4feba2424631149a4a))
- Consumer add ack reader#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([fa4e5cd](fa4e5cd311b6a403ae222f9a053f3f81bbc0f867))
- Fix parser with sample input ([10e90a1](10e90a1aae770df6089465b81c72c45b699b79c9))
- Update taos version ([ba33142](ba331427d639660846edd132e2f0d2b56f72d35e))
- Migrate task hang#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([e38a162](e38a162ad52b122cd567e92ebd16074617511a45))
- Opcda parameter mapping replace TagName#[TD-27526](https://jira.taosdata.com:18080/browse/TD-27526) ([ccc6220](ccc622097d9801e3f87b6e67eb94a050ffa7da49))
- Replace invalid characters in TagName ([14b3bcd](14b3bcdcc4e4cb9c616152cf608628777e08623b))
- Add http timeout ([6e553ca](6e553ca81bd3fe2af9231c88232ab991a4160167))
- Missing description of metrics.csv.files ([c52e049](c52e049c98630b938bbf69bb2281e8b03b37bed0))
- Remove spaces in command ([2c0cc6e](2c0cc6e11b459edaf8b3364fdfb4df2c8e5f1346))
- Remove spaces in command ([d2bc620](d2bc620c12c462972382520d93110316ff334648))
- Check csv file before creating or updating task ([c1277b1](c1277b17fcf8e34388b7f20d47658767de1bf0ae))


### Enhancements

- *datasource*: Add type definition in parser.fields of datasource ([2375af7](2375af7efd16a570a8e7d0ea5fd274bb24270025))
- *opc*: Remove unnecessary get all tags ([4f9972b](4f9972b81824dc2c32683a1b0fa622849a8af3ea))
- *opc*: Opc da collection adds milliseconds ([0a79007](0a7900794a618cec8de712f29da90731e501dd7d))
- *opc*: Optimize logs ([f9fb0f8](f9fb0f8bea5a2855f5dd2b66c5fc554596c8c8b8))
- *opc*: Optimize logs ([40a16c9](40a16c9e8323abeac67a841792f1831a14a9d20c))
- *opc*: Enable null data ([cc91325](cc91325dc00085e1efacfccd067b55905fceb769))
- *tmq*: Auto create table if not exist ([e475f19](e475f19a0b2878457e05be75f5e2caef245ef73d))
- *tmq*: Auto create table if not exist ([2749f7f](2749f7fcafcce71076beb701eafdcac844409d4b))

- Handle get task metrics error ([6cbe348](6cbe3485a6eb50312b075aea6aea738e1c0b759e))
- Change ws heart beat interval ([8d95a82](8d95a82efe062b657e639f6ac61739789c481cfe))
- Add metrics desc ([1500ecd](1500ecd3cd61462beeb509f8a02f99b8d1c7e149))
- Refine tracing log ([8b1beab](8b1beab2383e7435c5d0e928522cb0f45fb96b64))
- Remove wrong metric ([d40642d](d40642dffc074d037bc1329d43cced66c5cca15c))
- Update rust-connector version ([efdb26a](efdb26ab4795e3dcadf1062ed2c90a06c947161d))
- Update rust-connector version on branch 3.0 ([00cfc95](00cfc957000812b6608d0bde52f5a4a551fcc115))
- Add span to request handlers ([4536b1c](4536b1cf958dc4671bc5ba117013e4b49d52a19f))
- Add req_id to handle_lush_message_init ([0bcda84](0bcda8411b435d8139e1cc029f27dca386e3a23e))
- Print settings on start ([d58a3b3](d58a3b3e701475f02a5ab080aab5eda192336825))
- Print more settings ([25d4ea6](25d4ea6923ca1a4e23e426e9496d433ad73bf485))
- Print configs on starting ([8be4f1b](8be4f1b6d77e5f89ef27ea833aa90acb9ec0fafa))
- Refine tracing log ([fa2a2f6](fa2a2f6613010f1e907eca91f364f9e39bbed2ea))
- Do not raise task not found error when not in scheduler ([d902126](d90212648c0f99c979efc760753f5a2c31958e1b))
- Use consist order with json object field order ([e2a4bf9](e2a4bf901f857142d767caa3441d2d663fd82814))
- Change metric speed to f64 ([3988bd1](3988bd1f815b603354f986ebdf35fa773fce9a7c))


### Features

- *api*: Add in_scheduler tasks filter ([5045e84](5045e8485103b0715328156b0a9adff0a70072eb))
- *transform*: Support parsing int/bool from string ([340f643](340f643a954d773b42b45fb78eae5c3fc33099a8))
- *transform*: Support cast mapper { cast: "old-name", as: "new-type" } ([8f4f127](8f4f127cf8854a2e914ffc718910a653a79bdb11))
- *transform*: Support string function .truncate/.replace ([aa982c4](aa982c4dbf46fee30ae934f69f3aaf6dc5169eaa))

- Remove git branch from build info ([c01d921](c01d9212e8be56329de4ff943071e9ea00cb467a))
- Modify error tips ([fd4d325](fd4d325cc41ef76d44134c0cd3be01513e42b1eb))
- Add websocke api for task metrics ([f06ae2d](f06ae2d2edd2372930870a9c18d36c318133563a))
- Add rhai eval engine ([65a34ac](65a34ac923ad472f52949da86304d8a152a8eb77))
- Separate advanced options for mqtt ([1bb37c8](1bb37c8fecb566869e54c7e06fc48156ae74b266))
- Modify log path ([e317165](e3171650fe3322a619de0c9e418b31e16e5b20f5))
- Adjust influxdb and opentsdb config parameters. ([8a75ee8](8a75ee82a62a122ffc5ea44ba360efafe146d8bb))
- Mertics persistence ([97c50a3](97c50a398169e0901f56a057df07ea056b687912))
- Clear mertics db when delete task ([be6d47f](be6d47f652684d644a7b46ca31fda7351e32134d))
- Revert parameter delay ([286fb8f](286fb8fd22f850edf4681955a059b258d737d3f5))
- Add collection mode to historian source#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([df0501f](df0501fa9d958f8f92e71f27800dad13cb306d48))
- BeginDateTime is requried#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([35d8cb4](35d8cb4c1b09d78e291cc1a26f549605800e9971))
- Table is required#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([aca0616](aca0616f90f6b162f901e9c93bbc4ce514c3d18e))
- TimeWindow for migrate and retrieveInterval for synchronize#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([8659231](865923168510f92a24e34dc09b9f3da97cf2c756))
- Default value of timeWindow and retrieveInterval ([9f8ef05](9f8ef055311f0d585d549a77fb49f78aa0569f0d))
- Modify connector config filepath ([dafdd4b](dafdd4b854b775b1d5ca434b710677f7a0b3efd3))
- Modify connector config filepath ([d34d239](d34d239aa2c51c11adf3024cee588116cdeed2be))
- Reset lagacy_to_taos metrics ([08c5032](08c50325fd2d4985b024f26819da9a7e02d20923))
- Support filter ([1bfc7d0](1bfc7d0475e7de44110df6208b1ea38f7c96c2fe))
- Modify test expression ([f7292bf](f7292bf65a17381c78b2fd856818d76123c0dfd4))
- Synchronize mode#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([4fd0422](4fd04221e4d9000674151abdc7a2afb332f8e78f))
- Modify connector config filepath ([8802fdc](8802fdc1cc51d154557b07cafb5295f7678b78b8))
- Modify connector config filepath ([3a136ac](3a136ac1771f85fe52426f0d07d3990a6d68ce22))
- Get task metrics from db ([d198167](d198167e9ec0b53f66792f245c485940c01ee7e2))
- Add lagacy metrics total_tables ([f21d756](f21d7564cef28cb7d16d17c56ec42e94460b7f0b))
- Add sample taosx.toml ([6291caa](6291caa783e6166a8a304aeb0adb201536eadca8))
- Add sample taosx.toml ([9569f7f](9569f7fe10ddcfa02a6c95082136e8f2cb25689e))
- Add transform sample in/out api ([30f4e1b](30f4e1bd9a1e6f291f6960eebc56668f784a1c6a))
- Migrate history#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([2b0cc03](2b0cc03bb870038bdc671c1c6580a86f5070a875))
- Histoty sync and live sync#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([17f9002](17f90028e67dfdb556e8ffd16f6657f576f061a6))
- Copy taosx.toml in install.sh ([c37e9a8](c37e9a850e37f72eeeac781b11d1b13810bb671c))
- Remove git branch from build info ([dab7e5f](dab7e5f120093d551086b61248aebca4af701fb8))
- Add req_id for all writing methods ([ccd9b84](ccd9b8485ae6f5d9d846c998dbdf958450565bcf))
- Add comments to taosx.toml ([96720ac](96720ac30788e9ed747778266b3d9dc91772d3f4))
- Constant value builder and generator value builder#[TD-27179](https://jira.taosdata.com:18080/browse/TD-27179) ([4e0a161](4e0a16167a680005d556c541a2eb1cc7d5a5cb69))
- Add metric ipc.stream.received_batches ([59038db](59038dbf59aa5ddb63a60ef23f6d68f4cfe6bae5))
- Transformer.map support constant, expr, format, generator, join and sum.[TD-27179](https://jira.taosdata.com:18080/browse/TD-27179) ([75606e2](75606e2e2940fd20d0b2a5b237361f581e05b22a))
- Support split ([0fd3057](0fd30572027691fe501e773f3fabf8c7b3a3a5b9))
- Support sample length with csv file parsing ([f714fba](f714fba6c1014756e976f12751777af43890b4d5))
- Improve performance#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([4601a0d](4601a0d7935a39da4df7961a43f8f892483b7b4a))
- Sync history, migrate history, sync live test#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([e764586](e76458606e711bf1ef3390b9589c5e0b1dd0d7a2))


### Refactor

- *opc*: Remake opc connector ([f9f3eb1](f9f3eb14dd6984d8cb24928f775d63d53314d4f0))

- Add TransformExt for inner implements ([8d34663](8d34663023b97a243c2776c3bc5c51664c9d96a9))
- Add draft mutate transform support ([059171e](059171e056b5deff2129ec5c5122786ba545a4e1))
- Opc source ([f376b9d](f376b9d42c8cc7390806d7d94a3dd4470ed5edcd))
- Opc source ([c8d5ab6](c8d5ab6c88b42200674009431936d23fd757b3ae))
- Opcua and opcda advanced options#[TD-27198](https://jira.taosdata.com:18080/browse/TD-27198) ([c2e77ac](c2e77ac82d8b4860046b73781585204f9bdb9565))
- Kafka source configurations#[TD-27197](https://jira.taosdata.com:18080/browse/TD-27197) ([c99c10e](c99c10ee0da19bfecb19e344fe55bbf2788456ae))


### Testing


- Add csv config file parser test case ([de65137](de6513798ab6566ee3826f6cea5c9d883c99651a))


### Fmt


- Opc source ([0355117](0355117a05ab47fcc6cc9a450a28257264ffbd54))


### Revert


- Metrics of record_per_second ([ce75c8c](ce75c8c40bdb4524a020b64ed7a72a16ae5744ec))


## [1.3.0] - 2023-11-07

**Full Changelog**: [v1.2.8...v1.3.0](https://github.com/taosdata/taosx/compare/v1.2.8...v1.3.0)

### Bug Fixes

- *agent*: Improve agent task handling ([4a74aaf](4a74aaf5ea41ebff6cc0224b1664c91a94f935dc))
- *agent*: Task can still be queued when agent disconnected ([5d70e3d](5d70e3d680c512afa5beeb295e3118f39802a458))
- *opcua*: Remove collect.limit option in opcua ([d64b18f](d64b18f94e8929c8443b0cd1c7a666e71814d2d6))
- *serve*: Fix dsn check action to agent ([bb14a02](bb14a027f9e7b0be9362c3acbd9c6d9feef07a2a))

- Scheduler suspending cause hangout ([ab8d498](ab8d4983df53a98a9851b821bd575dde5b7220cb))
- Apply -v verbosity flags over log_level in config ([35f06ad](35f06add41f72f17313ba42581b358678c3b35de))
- Fix stopping timeout issue ([21249f7](21249f739b78dba0d8d355bf4c3cb75c6466d25b))
- Notify errors instead of interrupt ([45e5f41](45e5f418ecbf19f41ea869cf8da51f11decb4a0c))
- Crate private type leak ([e9e22e7](e9e22e7ccfa2f2b7625ca95bc46b3bea62072997))
- Set agent transferring status after task running ([ae9a12b](ae9a12bde0651f4d9cfcecc997e67fbd48469337))
- Fix action name error ([45acb34](45acb3453c53666d7d9fd4708290070e277db381))
- Add task breakpoints in scheduler (#992) ([8236940](82369400fb3d09b5ffab772a8b167d2e073fa7b9))
- Report error when agent not alive in dsn validation ([fe731b0](fe731b0f76d2cc5af8ee8d45044e52c657608784))
- Supsending right for running mqtt ([fb0aa84](fb0aa8461cb32d9b7585f6278f31efe05330afb1))
- Opc killed will cause error ([01205d7](01205d7eeaf75092e73ebfca5aa1189977f13bdb))
- Report validation error when opcda not on windows ([2776c9b](2776c9b6fb38d3cf53c34d84fbcca5ede53cc949))
- Decode json message in agent check#[TD-27121](https://jira.taosdata.com:18080/browse/TD-27121) ([38890d5](38890d5051531ed6dc76d29091df4dcac801fe63))
- Influxdb/opentsb will be interrupted ([12d5617](12d5617e1d0c44c278cddd1d6d3e7822ad98b21a))
- Agent status is now only offline/online ([b97057f](b97057f30bd56a7caef82afd0c5275cc3fad2a9b))
- Kill opc task but task not failed ([c405ca8](c405ca8cf864c413250fbca0c0a264681ccd2248))
- Close temp file to aviod error in pi#[TD-26928](https://jira.taosdata.com:18080/browse/TD-26928) ([ab9f525](ab9f525f5eb0860e0c2249c8e16a1049ec67541d))
- Close temp file to aviod error in pi#[TD-26928](https://jira.taosdata.com:18080/browse/TD-26928) ([015a2ed](015a2edbd407f0e98c1684d371cd2b2bfc2637d7))
- Pi and pibackfill need keep temp file exist#[TD-26928](https://jira.taosdata.com:18080/browse/TD-26928) ([f3c87a3](f3c87a38a5ea2ca6c19e13f668676adc8aebdf23))
- Abort kafka consumer when task stopped ([7c6b7cc](7c6b7ccee8259b55255936a4bada5a6691551a1c))
- Compatible pi dsn check for since/message as reason ([4799f51](4799f51fc9247a823965b42ff07cbd1163932086))
- Fix opc collector killed hanging in ipc+agent ([75a8a9e](75a8a9ed02bf5aa00915c5e9b980a0620610b864))
- Change online/offline to connected/disconnected ([cc5c8dc](cc5c8dc7908061266b231483aa73afc47a17db75))
- Fix pi cause error when killed by user ([3d2cfaa](3d2cfaa3a148842ba01e09930f1d771bc0b3ab81))
- Fix breakpoints not updated when resume from interrupted state ([4a1db2c](4a1db2c72c591b0250baf9ff5d8672430660dcef))
- AFDatabaseName is option#[TD-27143](https://jira.taosdata.com:18080/browse/TD-27143) ([3c27f5e](3c27f5ebe2d175afe151e4e4c62c38bf6edd4872))
- Pi and pibackfill message#[TD-27143](https://jira.taosdata.com:18080/browse/TD-27143) ([f2d5be7](f2d5be7311742899bb2c92ceac0abc0a96ad19ab))
- Fix duplicated value key in influxdb/opentsdb ([788c7b8](788c7b83c84ba6088d9d59bf2a70dae997419971))
- Stop taosadapter will cause interrupted ([3993255](399325520ca41d748a327b30d3d7b5b1b9fdbf41))
- Database error: status variant not match ([55c84d6](55c84d6ce9564917a22374f0c6c2ed39b4056276))
- StartDateTime and endDateTime's type is time ([672a71a](672a71a78152b1f2bae04462d8153db591ab92ca))


### Enhancements


- Allow default x-trace-id for backward compatibility ([d0a0729](d0a0729b24af8ff1106d7d1e738cca028ebb1262))
- Rename  metric related consts ([0d08fb1](0d08fb1e8e6886401be45b3060a1501e7e7db4d8))


### Features


- Add oem args in release.py ([fe2ccd8](fe2ccd8c3d57ffed9716e480a9f64b84a8f7ff9a))
- Add oem args in release.py ([697c329](697c32925839e910d4014bfbac26ae81c6f86bf3))
- Modify docs of oem ([6bf372f](6bf372fda5a739b45b76fade56b92a06ec482a25))
- Modify docs of oem ([597170f](597170fdb6ef22ccc1f108e517de3d2b4ae0c814))
- Add a mode of only build agent ([89cb407](89cb407fc9b3019a36746a38d6d80d87d9e54535))
- Historian source yaml#[TD-25998](https://jira.taosdata.com:18080/browse/TD-25998) ([1ba41bb](1ba41bbf1d703496b2aed5b06b0c3a96c6eaa8b4))
- Add api /metric/description ([2200dcf](2200dcf8192b5f277201a9f70c3e08307a94e1dd))


### Refactor


- Opc source ([20d715b](20d715b51d77c73fd173b323809bd32e54831a22))
- Opc souce ([50f10d3](50f10d30fec85dc6abdd7c4dae2a1e4ebdf6ee3f))


## [1.2.8] - 2023-11-02

**Full Changelog**: [v1.2.7...v1.2.8](https://github.com/taosdata/taosx/compare/v1.2.7...v1.2.8)

### Bug Fixes

- *agent*: Fix stop with agent cause error ([b341350](b341350d6b8318b64c5e4cabda6d6cd9cda14ce5))
- *agent*: Fix resuming behavior with agent ([7185966](71859667d75870d0eb31a80eaf3e8942dcaf31b3))
- *build*: Fix mqtt build error under go 1.20.6 ([3ae7374](3ae73740b5b492fdace38389e12556914b374d97))
- *legacy*: Add native protocol option in yaml ([c5d4805](c5d4805686bce46bfb5b8965ec7fa9c3370a4df2))
- *legacy*: Fix connection reuse when select 1 not trustful ([6f6af11](6f6af11a436bb513efd4e321454280dd0f414997))
- *lush*: Fix lush stream compare tag value error ([314b70a](314b70a89d4ba4b5c63cb4d2a00eee70dbd65f44))
- *metrics*: Ipc.stream.records ([b35e63c](b35e63c81e9c85dbe0a21fb1c109007c7dda74f5))
- *mqtt*: Change Mqtt to MQTT ([d090bfb](d090bfb3dd394f925304490949a369e1ba121faf))
- *mqtt*: Fix flat stream write error with channel closed ([00650f2](00650f261392ca730308edb14678993afb514f95))
- *opc*: Fix disconnect error with opc + agent ([451bd0a](451bd0adc06ef0aba3cd22923936f6379e17a8d9))
- *opc*: Fix ack error to opc connector ([4be5799](4be5799954c7490442ae78b0091e49adc8ecd635))
- *scheduler*: Fix scheduler stop/stop in agent inconsist ([b3dea72](b3dea727264e3f44c18481ccf79c90145cc7a843))
- *serve*: Fix task status not change when task updated ([97b72cd](97b72cd8a70eb3d560b3f2707ff51c8ac562cbbe))
- *serve*: Run a task twice cause actix-web thread panic ([a8e4810](a8e4810ca98e6c5733f93495e1344224a2c92229))
- *serve*: Make stop operation atomicly ([9b9e3f8](9b9e3f8b2043e32b7f2b1dcadd24634e101ab65f))
- *serve*: Fix ctrl-c handler for task suspending ([bf51411](bf51411f92b1cda0a9d5ef693718d5c56e2e1633))
- *serve*: Fix trigger deserialization error in creation ([6b09425](6b09425c809f3d5f891047bbbdc755a3b575d7ad))
- *serve*: Connector licence expired cause error ([8ddb422](8ddb422198cf0ad1d62ff31305ea7ca8c7836bee))
- *serve*: Fix license number check error ([85da490](85da49041daceaec2f84be47e3ee05eadbe2fa69))
- *serve*: Fix start stopped task error ([c1276b0](c1276b0b01748d7f8c8348aed1ad123a0b5fa9c9))
- *serve*: Fix filemeta path check ([ba3e0b9](ba3e0b95866916d297d95b658e576b29feff9b30))
- *serve*: Fix filemeta path check ([e120cf9](e120cf97ae4390c708ca6341354a9ddd74c80bc9))
- *serve*: Fix failed status task can't delete ([1d1b8d7](1d1b8d7db58dcddcd7f5f7d4539c2a9341a5e445))
- *serve*: Fix dsn validation before task creation ([ad331e3](ad331e304acecf8d82552728ff008bdf79c6a667))
- *serve*: Fix failed/interrupted state error ([a6fa3b4](a6fa3b4885677ad1ef70da22ac5b12d221021dc5))
- *sink*: Fix second time unit convert in flat/point stream ([5486340](548634080536477602603bfd07176dd7fd9ca16e))
- *tmq*: Resume when 0xE003 channel closed ([9c753ab](9c753ab1920cfb0c302369de795ee2df491bdf36))
- *tmq*: Fix upper-case topic for database or stable ([53734cc](53734cc4b918debf7d7c414721356565aa0e044e))
- *tmq*: Fix upper-case topic name subscrition error ([190acac](190acacb28aa8de50684941757899e179ea08472))

- Compile errors and warnings ([f4097a3](f4097a3bde8156601635de50ea90252fe92a193a))
- Improvement opc ua get all points ([5464971](5464971fa301d16783cc644361f9b46475026266))
- Fix compile error ([dd9d02c](dd9d02c2d77d3a81d5f4bb71681d5a0ee6c46a26))
- Fix the bug caused by the InfluxDB interface, avoid duplicate buckets. ([9dda40c](9dda40ccd8254977aa4f6830679ccb8dbd15cabf))
- Agent status ([3df6551](3df6551b54230441f18c52902f41c6ff4a69cbeb))
- Set fetch_max_bytes_per_partition to solve MessageSizeTooLarge Error#[TD-26709](https://jira.taosdata.com:18080/browse/TD-26709) ([8fb56aa](8fb56aaca47f592ef3b06795f563a5250e28bb34))
- Filter points ([5f33d4a](5f33d4aaaf6696938ca37b1ca9ec1a0c65d27f3f))
- Sled maybe panic with muti threads ([fe6320c](fe6320cfa3382347abb5bec7233b78c3d20c4715))
- Ignore error when browse nodes for ua ([518e3fe](518e3feb4c6f5436a1c20b6ff12d02d22264a4e6))
- Ignore error when tag not exists for da ([9abd4e7](9abd4e7dbc75476c541b712ed928245ceac9bf33))
- Build error ([ea0b0b3](ea0b0b302c213f43c2a6f12e8b20cde06f961d5d))
- Use  for integer parameters and  for duration parameters#[TD-26856](https://jira.taosdata.com:18080/browse/TD-26856) ([4e3b9f6](4e3b9f647b1f8e435920fdd33e26815b2aa0a48e))
- Modify the path of opc and mqtt in docker ([7f2de1e](7f2de1e922cd2b0a9637cfec735861f77c41a8de))
- Set breakpoints after chunk ack ([e0fbb96](e0fbb9641e15283fbe8230f37248464f0313bd81))
- Split reader and sender for opcda and check nil when subscribe in opcua ([1c12581](1c12581e4f7a210bfaaf951c8cd72042adf83456))
- Modify the path of opc and mqtt in docker ([d3fbdf4](d3fbdf4fe3737c2d5c3eb59ace7a07b1650eb946))
- Upgrade gopcua ([257863f](257863fbfe4d6986b5f38987a2b0a4d4f0eee550))
- Pending when check data source validation#[TD-26916](https://jira.taosdata.com:18080/browse/TD-26916) ([2340366](23403662044482dbc48d6354717c448040d6b8c3))
- Consistent with the Chinese description in the data subscription#[TD-26912](https://jira.taosdata.com:18080/browse/TD-26912) ([6d6a2c5](6d6a2c560138f63a4db70b5d62936917967c8de1))
- Consistent with the Chinese description in the data subscription#[TD-26912](https://jira.taosdata.com:18080/browse/TD-26912) ([a69a892](a69a8923f326dc47c1c9ec60e200fefa5363f081))
- Use async taos_query.server_version#[TD-26913](https://jira.taosdata.com:18080/browse/TD-26913) ([7b5cdbb](7b5cdbb85601ff6b85aa1f3283e9fbdd2c244d0c))
- Legacy data loss ([1e844cd](1e844cdb413b8958365379788258b9d0207538c9))
- Add err details ([3c18aec](3c18aec607a3a066b31437a7ce232678a1d38965))
- Modify paths of taosx ([a50e86f](a50e86f3bbab52088deae44455060c8f805bfc8a))
- Use earliest as the default offset.reset#[TD-26946](https://jira.taosdata.com:18080/browse/TD-26946) ([d198161](d19816108e338991209f4effed4893cd84a14066))
- Modify args ([917802c](917802cb71654de5ed0955691859235172ff16a0))
- Add task id ([417ef81](417ef81aa4180b62854db66a09362ed6dcdbf9a5))
- Add log ([abf3953](abf395317220523227f128e62211207186e0c802))
- Init-paths-in-agent ([d7a7952](d7a7952da2505ade067f2f9f8309dc5e9567ce20))
- Modify the name of breakpoints ([b5a18c1](b5a18c1f56b6b9822de3b5e47bad0d2f5decb1d7))
- Breakpoints_set panic (#922) ([a0fd22f](a0fd22fccce6c2f06626d4d5fb0725dd9e5b0fe3))
- Set parameters global ([0d4d523](0d4d52304882ec1a03a3b4d4b2df5f64a81417f9))
- Download path ([99f6cd4](99f6cd484f2073c384328f8d41043ae7ff0e6c2e))
- Executable filepath in Windows ([d3b0f80](d3b0f80c82d462f77bfa49a2e5b9150223e8ac4a))
- Host and port is required in kafka source boostrap_servers#[TD-26921](https://jira.taosdata.com:18080/browse/TD-26921) ([4fb1816](4fb18169bef78c972b4692673b7f5c38978f611d))
- Tmq is different with tao in dsn#[TD-26936](https://jira.taosdata.com:18080/browse/TD-26936) ([4ff15a4](4ff15a4856a2420e362761516710eb02f8a7df73))
- Conf parameter is wrong#[TD-26997](https://jira.taosdata.com:18080/browse/TD-26997) ([3be63b1](3be63b1a67b357137cc00546473d3e37f4f4da20))
- If group.id is empty, use test_tmq_is_valid ([07b3315](07b3315fd1ecca97efe137ef48efc046e641e491))
- Sled file read error ([83da649](83da649934826c5264d832c4b3346955b09f5851))
- Stop task via agent in queued state failed ([45756e1](45756e1ac3925d7b07309e2ae241cea8183b68d1))
- Subject is required in tmq dsn#[TD-26936](https://jira.taosdata.com:18080/browse/TD-26936) ([5a1aab1](5a1aab146f3930645a97ecd025e7cd44fa37fd0c))
- Parse time error ([3f79b65](3f79b65aca474f38783d9c7afd40ca9f7ac324bd))
- Parse time error ([12ce0cb](12ce0cb2616a40a023a09c2465312bd6ae6e66ce))
- Fix serve command line parsing with config ([9ab50bd](9ab50bd51d5c0efc6bf905b7023b2bb8b6707bc3))
- Return erro when TDengine is a 2.x instance#[TD-26935](https://jira.taosdata.com:18080/browse/TD-26935) ([11e95a7](11e95a7775af381c0e13e13d05c1cae5a6d331a7))
- Non exist topic in tmq dsn#[TD-26937](https://jira.taosdata.com:18080/browse/TD-26937) ([0f28dab](0f28dab242b604329f51cf16d43d2ce1b97f9d63))
- Add default deserialization for resume strategy ([bc20c77](bc20c77fac1b31a217d826dd0e2ab114da14c9ff))
- Merge error ([74b3d43](74b3d43c48ad0f9ea073b4ffd33c00da9ca8ef1d))
- Add timeout for ds/in/validate#[TD-26940](https://jira.taosdata.com:18080/browse/TD-26940) ([ce888bb](ce888bbf75ec22db4f93440af0acb0060c7831c4))
- Compile ([99097d6](99097d69361925491fcaaa904b584540fd2f23cb))
- Connect_time and request_timeout is required in opc source#[TD-26997](https://jira.taosdata.com:18080/browse/TD-26997) ([d9ff44a](d9ff44a3f26c97b99a2715390212a970badd56cb))
- Via is option ([428ea6c](428ea6cc79881d576d081ab4db7c6d5e98f74562))
- Fix --version/-V error ([1a7b0c0](1a7b0c05258c27b17d298f36a8b02400e4680f1c))
- Get /ds/in/validate return 200 status code#[TD-27051](https://jira.taosdata.com:18080/browse/TD-27051) ([0db2845](0db28456a7e3e1bfb0855273758ac008be9e0caf))
- Disable delete when task is running ([42c866a](42c866a7ec03770e6a0182ec658ff7b6baf94735))
- Refactor error message before edition check ([438f1fd](438f1fd2e78b94bad0fd2d204f75f2ceb39fbfe7))
- Fix update task with trigger error ([2433f93](2433f93f2f4110876f9a737e49c0490840f59715))
- Change timeout of /ds/in/validate to 20 sec#[TD-27067](https://jira.taosdata.com:18080/browse/TD-27067) ([9f73066](9f73066986eef990a5947ca910c53aad9767c506))
- Remove native protocol in taos.yaml#[TD-27020](https://jira.taosdata.com:18080/browse/TD-27020) ([e84a125](e84a12559f4e2fbed3492fe3bc85b850a2710069))
- Influxdb/opentsdb panic ([5856b29](5856b29a02e40700b8a4a91501e9df06bc3a72ba))
- Interrupt task when transfer break (#967) ([c50a3a8](c50a3a8f071e26eb7030887b0c7b76f3f1ceab40))
- Fix task removal when completed ([b9ff480](b9ff480ec7e27e803f18d5e1eb8114ab259ed8a9))
- Uploaded files not found while running task ([03b69fe](03b69fe4056c096b9fc6f8aa0b2e3f039babd5ad))
- Fix trigger deserialization error in agent ([c563952](c563952977b2764b5300ce29e184c207d73009d9))
- Lush message table err ([002d02e](002d02e2ec474c29f84a21967e4cd1cf57052c03))
- Insert table name only once ([6162ab0](6162ab049a0dd4fac1f3576a6d6a944da851929a))
- Fix bugs in version and args ([10bb55b](10bb55be9bfed6d2a961c1e58e5a342874c635c6))
- Fix bugs in version and args ([a4ff1c3](a4ff1c3d5dec4c0188a76ed482e84024cae00447))
- Check dsn with agent#[TD-26928](https://jira.taosdata.com:18080/browse/TD-26928) ([28bfd0c](28bfd0cc9f15ba0f5b60d8ab0de7274ccbc459f1))
- Fix explorer version ([026846d](026846d538cc5ca0d5acccfb06c8bb1eb5971c17))
- Fix explorer version ([8b5a9f9](8b5a9f99f10c98bc3af951e565c2e181f1840a0d))


### Enhancements

- *log*: Remove module path ([52fceda](52fceda71d1e2834f36fdb620b7097d8020b77b2))
- *mqtt*: Add connection check ([430494c](430494cb885a62818cb4e9dfeca13e3424d44d05))
- *opc*: Add connect check ([28f4103](28f41037531717c06c1c6bbcc5cb7924120f2830))
- *tmq*: Explicitly set offset/snapshot parameters ([8e3cfb1](8e3cfb14d9a766066767eb91beb96a09bee14ec6))

- Support down csv template ([1c4c902](1c4c902a471e52cb513d54eedef63f8d89ef3c11))
- Support down csv template ([2e779f2](2e779f2ca7188e1ed35e3477d39aa029d0452332))
- Add influx resume ([4e69c05](4e69c05cf7fe53768ff9c33ce58d32de978307c4))
- Fix taskopts args ([8ce69e0](8ce69e075f78e9c50ef5a1d52d9d5807053f9a23))
- Fix agent breakpoints ([b2790cf](b2790cfd8b9262871d3c788ba1ea62f4eb66febf))
- Extract function init_tracing ([1beef86](1beef862e8325538d5c8c9ebd28d6f367267b55d))
- Remove file and line number in log ([8635454](86354540219cc73bd0347a44b14bdd09aeb8f911))
- Change default log location on linux ([c08958e](c08958e4a61b09aa920e4e4e49a399aee5c12c49))
- Add breakpoint in task ([7e9cdd2](7e9cdd219e77bcd73f2c80b322523e56e9f63d80))
- Pass breakpoints to taskOpts in agent ([3242ac0](3242ac0192a5b6359f87d5971f519497dca8c804))
- Adjust log format ([d307df3](d307df358592446257ad9d9cef3ed166d37a5ebf))
- Add log to check breakpoints ([f7f3ecf](f7f3ecf7bb811593b2ef7e0108224ce247f3a08e))
- Change order ([1c338c8](1c338c8114ce905c5858052167811e0f19677621))
- Refine structure of main ([99b714f](99b714fe2211f93c64eb4880d8d70620fe955ed6))
- Optimize imports ([b6265c5](b6265c53e4e3eddc6f7cc4b7bc62574c6079b0b4))
- Reformat code ([3d97c1a](3d97c1a171975550a886d0db797d4d573a0050c8))
- Reformat code ([bdf2e0e](bdf2e0e7c65d641dd3e2ecd730132c50451f0046))
- Reformat code ([1f07366](1f07366eee591790f25401e2449cc8ad5d7352dd))
- Support special point_id like ns=3;s=Special_\"!§$%&/()=?`´\\+~*'#_-:.;,<>|@^°€µ{[]} ([ca1fa34](ca1fa345f7c193ff61dbd6d37925f77054036b66))
- Print log level on start ([68d295d](68d295d605399e6db5158ff0db7da26e3a24b06e))
- Remove year from timestamp in log ([81ad757](81ad757ae648ddc3355127a1d29f7d21d6a84411))
- Tracing for ipc_flat_stream_reader ([fe22924](fe22924b624124f1ab984c750f7c762c26271d28))
- Add task_id ([1728313](1728313f22c39ad293a13f6c5223954994e32748))
- Use TaosXLayer for agent log ([9d2860b](9d2860bc1ffb02c768f2d02e30ce1f44dc9d69c4))
- Add placeholder or value for data source config#[TD-26939](https://jira.taosdata.com:18080/browse/TD-26939) ([c702edb](c702edbee87268165cea8deb1df220ac1ee91480))
- Use TaosXLayer for agent log ([94f08fb](94f08fba5360a27f58543420c437b82b32e9b1d5))


### Features


- Check validation for taos#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([bc59aeb](bc59aeb81a13fe2fa549700ccfb8fab12352782c))
- Modify datasource field description ([22827bf](22827bf8d3b9035c6e607e5f73e5a7fefc58f5a9))
- Do not allow same name for a cluster ([06e538e](06e538e216c88bf689f523cd9e648873bbddd7d8))
- Refactor source in ruuners#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([37a40d7](37a40d7bd3234a82a966bfc9724c2a07b294f6aa))
- Optimize the code of validation ([fd337e3](fd337e3e9a84f0578790dc8d91cf40b8b57bf4cc))
- Kafka source add task parameters#[TD-26709](https://jira.taosdata.com:18080/browse/TD-26709) ([8d48679](8d48679d83e36c7cf82d062c71b45fee0222a413))
- Modify datasource field description. ([f7c09a5](f7c09a5b98c5cb45e1b34f783d99079b78013a6b))
- Modify datasource field description. ([aef0ab9](aef0ab95b56cf31ccda17d992d41555d114c56f0))
- Add test cases#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([0b8ebd8](0b8ebd88c2684bbd4a7a8e1d175941de7c76d91a))
- Automatically add dbrp. ([5940aaa](5940aaaaa2f94c85aa5c2e6baf534d7c1aa37f3a))
- Edit running task immediately effect ([782d87b](782d87b5afc363df249aff8c9df3edafa53bd342))
- Add api select opentsdb offsets ([697ca10](697ca1057ac112a508e252b80982516180b13477))
- Add TaosXLayer for tracing ([abeb91f](abeb91fd46155955fc74a080c135eaa5474d9fb3))
- Enable set trace id anywhere ([abe770b](abe770bbba1d92562259df82ded2e1115ada595e))
- Restyle of configuration ([941c25a](941c25aa53013f90dc4ab26962494f9e89063cbf))
- Use td version instead of package version ([a6bee20](a6bee2018b14f3d1024d33c56c1ee46d4bccd0c3))
- Use td version instead of package version ([4c8d130](4c8d130978a74988df8189bfeea8eeed6c7302c0))
- Log rest api error response ([e996f65](e996f65e3845163cfde57b69f505fa73189bf55c))
- Use customized RootSpanBuilder ([28056da](28056dad4dc9756624c97c363887aaf3a58015ac))
- Use td version instead of package version ([294c607](294c607f166087a0f2f9b3de26af510abaac7d19))
- Use /usr/local/taos instead of taosx ([b02b37d](b02b37de62fee6d3de220f3da8d34bb61271165a))
- Edit running task immediately effect ([505e1c2](505e1c22b0966d1fa8cbb100be3aa2476d460666))
- Optimize for ui ([1c182cd](1c182cd03dc4ee54c955360974648577ddabb33c))
- Pi source is_valid and refactor#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([569174d](569174d1bad5fe17d47860cbc07a11103d397ef8))
- Add global function  attach_trace_id ([9dd0ab2](9dd0ab2bfc97b6f382d78f0a237856d1ff5c9c00))
- Refactor task scheduler and error handler ([399c5bc](399c5bcc2f201f765818d826b79592e7ac84be2e))
- Mqtt is_valid test#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([fe207f1](fe207f158d8074b8cc4b6edc39da7dda318f4c77))
- Mqtt is_valid test#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([db92b97](db92b97dab6e986f0f42fe8364b7e1716025542d))
- Opc source is_valid#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([475e390](475e3900300f4b594f843028a0ecda3f8a08bcd7))
- Add default value for protocol(main) ([67093ec](67093ec3542b482752723cb08934ec34ebc177df))
- Add data trace id for tasks via agent ([0d072f3](0d072f3506ff0d1f444391bc47bd73cd98d5b034))
- Add data trace id for task without agent ([41deae3](41deae39753aae75b3357f41b1a6319f5968382f))
- Merge some commits to main ([e934d30](e934d306ac877602620fe9a9a9872baa3348ea98))
- transform data-trace-id via app metadata ([ba23939](ba23939c28d36bd801be2083dccda52618917355))


### Refactor

- *pi*: Backfill start/end time alternatives in yaml ([0112e3e](0112e3e73c00d3129a571f5971886818b6d87be9))

- Validate taos&tmq source#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([4aab760](4aab76050172e19f420799e599c969facb90f0b6))
- Format ([945ff44](945ff44a6514d098d29cea9d0e8c4cb1860bc406))
- Influxdb and opentsdb source check is_valid#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([f342b46](f342b467e7639fcfbee4dd8c6645c873c8268391))
- Influxdb source ([ce99f96](ce99f960c80b3f454614369dbf9fc34b4c18c0e8))
- File log format ([73d6316](73d631687191a51830b171cbaa134b83e83c3bf6))
- Influxdb and opentsdb source ([5b6b3c3](5b6b3c3b51797c169c9dc855d06dd24b96fca42a))
- Solve warning ([0a9772f](0a9772f50ad0bdf04a38b2077bfeb61dee15692f))
- Mqtt source and is_valid#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([219880c](219880c42b4670472a08ea8f2f1168470d4041c3))
- Mqtt source ([455a052](455a0523a2c5f7db9235412fb669c55a30fcacab))
- Pi source ([d523522](d52352298df658093c3b886ca4a0f0f61680a24d))
- Move test cases into data source ([a5b0c46](a5b0c4680366c2ec48c64a571bd57ea080cc8b85))
- Rerun-if-changed ([535dd3a](535dd3ab7f7fd43cdb67e196f59aa0d8bc26632a))


### Testing


- RootSpanBuilder and actix-web ([db40518](db405184d3e62574e4cae72fcde97f3b3f4f47b9))


### Debug


- Add log ([24e7863](24e7863c1bece353bfa75ac22fcf837ce5ae5db8))
- Add log ([f079e63](f079e63f54118f7007c97b32bbc94f289d988dd2))


## [1.2.7] - 2023-10-10

**Full Changelog**: [v1.2.6...v1.2.7](https://github.com/taosdata/taosx/compare/v1.2.6...v1.2.7)

### Bug Fixes

- *agent*: Cannot resume tasks with agent in status cancelled ([4aa2046](4aa2046f5375de5002138500eb356505e5b0787b))
- *api*: Fix panic when create with invalid dsn ([8bed630](8bed630d31ade0f7dc5c0f959e92f9fdf3feeecd))
- *legacy*: Fix cancellation ops in legacy to taos ([213531b](213531b62a27f01c220241b12b1d3afb03e24991))
- *pi*: Produce false 0x2653 error when error resolved ([79d6109](79d61094ec4535004552edaa56b11e5a7386d6b8))
- *pi*: Support all points mode ([704b56b](704b56be1b6e6ca798a4146459a383cb482f584a))
- *tmq*: Use a fully version of endpoint ([f27097b](f27097ba4822e0809ec3a7e64863180a554fa3ba))

- Agent auto exit without err lgo ([1715fb5](1715fb5770d70a840f9bab3018fa52ec23d6b7bf))
- Ignore sigpipe ([712c01b](712c01b775ea368d4696d65fec882521f5c9b484))
- Realtime data migration#[TD-26429](https://jira.taosdata.com:18080/browse/TD-26429) ([55b058d](55b058d0b81a74a4b518e7ca0bb85a6018dc451b))
- Compile error ([b8705d6](b8705d64043328bc8814c1771446270461a18f30))
- Optimize code ([3746ce2](3746ce2d6fc690bfdaae456a21ad684c76b91355))
- Fix value not exists ([6156922](615692256babfa9197fd3b1ae992218ea165ff01))
- Fix confilct ([97c4b6e](97c4b6efdba87bb17d7141c70295f4f98259636b))
- Fix opc select all points error ([f2188c9](f2188c98a15e2ec135946a649cc575f78fe0c8df))
- Fix opc select all points error ([0d28b5b](0d28b5b8708748fcd0118dead511e6e9467a717d))
- Add log and fix string type case ([167d04e](167d04e4fd4957771ae2fd57d5d3491346006837))
- Merge conflict ([e1fe62e](e1fe62ed3456935b31230ed1989bc7dc16fa9923))
- Fix value name error ([aa80af6](aa80af6eff2952f63d591f37c83184cca5f9f83d))
- Fix value name error ([d42ff5d](d42ff5d98f55fde30058fdb9ed003912b4c39f10))


### Documentation


- Update data source description in en language ([af6dd65](af6dd654165a77a4a9d03c850e46c57d48f3b401))


### Enhancements

- *ds*: Data source host tips for agent ([8dc350e](8dc350e7e6f0cf481664072b7bb6f1b2bf3f0ae0))
- *pi*: Use consist data sets in data source yaml ([b5584a5](b5584a50b36effb1ae0a8a7a1f690353474d33eb))

- Support download all nodes as a csv file ([6357ed0](6357ed02fd0e33e938356f0df50ecc3712a5bf33))
- Support download all nodes as a csv file ([60d89b4](60d89b477e3f3e2adb633b7a46a9c136cc6020e7))
- Support select all points from opc ([2f8e0be](2f8e0be20b5b5f5f549384e82c937b2731ccc525))
- Support select all points from opc ([9fe6fd7](9fe6fd7d10fe675a6971b04306248cd1df3099e7))
- Offsets api return json ([2493820](2493820a8aaa7924d39b4f8e605f01bcc5ae340e))


### Features


- Update breakpoints dir ([9425997](94259971790ddb4e089f721c68ed9d6cc23688c5))
- Use env::current_dir ([f2102fc](f2102fc6ae2577d937ab513ce8f3a37702897e10))
- Support breakpoint continuation. #[TD-26424](https://jira.taosdata.com:18080/browse/TD-26424) ([6771b82](6771b82b8475f3b31634fe7acd5fb17b9a5c9467))
- Support breakpoint continuation. #[TD-26425](https://jira.taosdata.com:18080/browse/TD-26425) ([1944b27](1944b275a0ed4c1073ef489dda69686d9c5e82cb))
- Use breakpoints to resume ([2bbf5de](2bbf5ded8e5bf2736fc6b92ea2a05bbba47c3a8d))
- Change tolerance to delay. ([7329da9](7329da93f51014f87fcc16300710105fdaec222e))
- Configure performance params on the page. #[TD-26643](https://jira.taosdata.com:18080/browse/TD-26643) ([2b61f10](2b61f1092c4f2cf471a394a0f5c8540a85fe0d0f))
- Update get offsets api ([83ceea7](83ceea79b7b3a6f45f41fbe744a8d6bfff9400b0))
- Impl is_valid for kafka source #[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([3d48a1b](3d48a1be1c152e4c67e7929b048a3463b9f4d539))
- Modify parameter read window ([cd57846](cd57846ee1f16f71c53599fbbfca05f5b89afe7f))
- Change tolerance to delay. ([8fe3708](8fe37083069b25b819540ddf192db59fec3d7904))
- Configure performance params on the page. ([1493c37](1493c3748aa688eef55f2a08b3b6e9aa3a05d55a))
- Modify parameter read window ([5fb60f8](5fb60f8c6c8a0c2dc43c8548462220f8338eb7df))
- Check validation for historian source#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([85f675d](85f675d2c0bef6bb4c329ba07b3d6809ca324281))
- Modify field descriptions ([1f24dda](1f24ddad17ea88175ca4dc67783d73408e8149d5))


### Refactor

- *agent*: Rename busy status to transferring ([f17b4c3](f17b4c3e2789d1fd293d3fc9eb4d1107b7e5fde0))

- Opc desc amend ([bd4398d](bd4398d338e552dacd5f66f6992f47388ad9a6d1))
- Opc desc amend ([7f8fc51](7f8fc51ad66ba711a6aeaf3397838d10e0937ba4))
- Opc desc/display amend ([93ad37b](93ad37b371014e21a340b0b164a2c594a83432dd))
- Update some data sources(tmq,taos,kafka) ([5298e41](5298e4160ba979d060f1733e902f38ae4bf01724))
- Modify pibackfill yaml config ([ac5a847](ac5a847d070e793f2afcc805eed7d6a165b82b94))
- Modify pibackfill yaml config ([0c42c53](0c42c5324101e9626782b3ff6fa1287b4e5bd229))
- Modify data set all points file download ([eb803d9](eb803d935d7ca598317fe5c378e7303f81044a97))
- Modify download_all_point_csv_file  post to get ([0b24838](0b24838bd891ab50ae5d032b7ca305d086b18c80))
- Add categories param ([dce2c0e](dce2c0e5c56cf30445a356a7c2271f4e90547cc7))
- Param type modify ([ce6a4fd](ce6a4fd023df4110f9c8137dee2290fd4bdb4d80))
- Opc ua yaml modify ([b61374b](b61374b44b2b52e3df9a249e0df9e470e22de303))
- Modify param desc ([9052c07](9052c07986ceca8f84f6aa9840d6aea3859ba3e6))
- Remove table config ([ff1ecd4](ff1ecd4600cb67b0a29a5e3b12d534178107bebc))
- Modify data set all points file download ([37a7d94](37a7d94fbe8ad37eecb891195d2f0a92a6243fcf))
- Modify download_all_point_csv_file  post to get ([bb68207](bb68207bdd884a4853fa8f13ce161cd08fadeeca))
- Add categories param ([e1d54be](e1d54be5c309bb317c2a8b1a7a108bdf429e28ca))
- Param type modify ([2f30f66](2f30f66bef4ab87aebc09c82668099ca16698b10))
- Opc ua yaml modify ([461f33c](461f33c7f422e623ed9fbc6b9fee38fd08a04a68))
- Modify param desc ([c288173](c28817352f82bb4a2a793f8a09cc42005b6ecf80))
- Remove table config ([a742239](a74223985e4189a0e4e6cc9a8f9e4685e0f68034))
- Historian source#[TD-25744](https://jira.taosdata.com:18080/browse/TD-25744) ([1e3f628](1e3f628a79565a8be9954745e94ae17c42d9558d))


### Revert


- Busy timeout to 30 ([9addd9b](9addd9b3c3e3e9220e212e8421416c1199244f3f))


## [1.2.6] - 2023-09-27

**Full Changelog**: [v1.2.5...v1.2.6](https://github.com/taosdata/taosx/compare/v1.2.5...v1.2.6)

### Bug Fixes

- *agent*: Fix agent transfer file with \n ([9d24d72](9d24d72782788cec212efc3d871233c09f6b2a3f))

- Fix opc page description ([1094b5a](1094b5aad423a9c60be4f26e450dd3312bbae948))


### Features

- *pi*: Support config file ([cb35cfb](cb35cfb5bd455a4a111cad8126ea75ce2da1f9ab))



### Refactor


- Opc yaml desc modify ([e0442e0](e0442e00d3130028c078bb241e227a303d32a857))
- Opc yaml desc modify ([0d5b07d](0d5b07d9c1c8028c3ced995e22fe4122b69c49ba))


## [1.2.5] - 2023-09-26

**Full Changelog**: [v1.2.4...v1.2.5](https://github.com/taosdata/taosx/compare/v1.2.4...v1.2.5)

### Bug Fixes

- *agent*: Fix pi datasets unexpected value at line .. error ([0a72625](0a7262519584277b43f6c0decf63008ef758ba5a))
- *legacy*: Sync stable schema cause conflict transactions ([cf5894b](cf5894b891a572f3f096253725a3e1f7cd315dc1))
- *pi*: Fix mut requires in older rustc ([f8f7ea2](f8f7ea286643f67bdc4e65a4f232ef1bfb7f9a5f))
- *serve*: Fix agent tasks set as cancelled error ([db14086](db140863151e9d052d9cb7719e30bf2241071f3c))

- Assert enterprise edition ([1bb276d](1bb276d1bc83485476a3fd2b2c82ddb76898e842))
- Update assert_enterprise msg ([8baf82d](8baf82d431e28a594fc37b470d0de96cf167a93c))
- Assert enterprise edition ([2fd5ed2](2fd5ed2fe3879f5b43180bb62772a2238e7a3975))
- Update assert_enterprise msg ([19ac0ae](19ac0aeafa6685f38f9c1b49c1acdc4c33282222))
- Fix mem leak with native connections ([c0fe976](c0fe97645d3ba4815750430a4eca1d68c4b7de00))
- Sync schema error for some specific 2.x versions ([a95c29e](a95c29ec6a31616b56fbc8d2543c57d1ce6c6378))
- Fix opc dump and add log ([4f4524d](4f4524d839510c5dd516d7d5c578c9584197edbc))
- Table not exist occurs since retry time is 2#[TD-26508](https://jira.taosdata.com:18080/browse/TD-26508) ([ccdf70e](ccdf70e9a253aa3b3e92719abbb7b5081a34a90e))
- Table not exist occurs since retry time is 2#[TD-26508](https://jira.taosdata.com:18080/browse/TD-26508) ([add39d1](add39d19e0d9f2f04e75e9e84c028d6751c617d0))


### Enhancements

- *dev*: Add clean-all and build-sanitizer task [skip ci] ([fe1f887](fe1f8876a94f25e9a43e5badaa26187dd6c9d9ee))
- *pi*: Improve pi datasets error message ([06f26cb](06f26cbc293bcc209a5c02a52c4ee13eb4a0fbb1))

- Improve error logs in pi datasets ([5db8af9](5db8af981204737d5a6d80098d6a3788ce1a6ff0))


### Features

- *legacy*: Support remap options in target ([898e418](898e418069458c74037b86bd356eb977e0188a12))

- Add breakpoints api ([5290473](529047375f56490f27819b44af52c663c32aadf5))


### Refactor

- *pi*: Update pi data source yaml (#780) ([91fb6ef](91fb6ef437be9f3e298a0ab6f2ba3f1a6b990d66))
- *pi*: Update pi data source yaml (#781) ([9fb8325](9fb83259d57bbdc11ef354ed79ce79f57cf043cc))

- Make tokio-console optional by compile features ([9e0c106](9e0c106100845e3a59ef3c826aa7764231c0c17e))
- Use opentelemetry-otlp instead jaeger ([9bc70ce](9bc70ce2ef866526e82d9b181d5dfea4fee8014c))
- Pi yaml refactor ([c7bbbb3](c7bbbb34aaa6c5cfa9fd6fd6c59b2e7cf755e0a6))
- Pi yaml refactor ([b286b10](b286b105f7267020c0fe1bb4be584a4d06b7f362))
- Pi dataset header modify ([93ddaa3](93ddaa3288704400a0b89fca0ba4cb79a0288f1f))
- Pi dataset header modify ([fb4a1cc](fb4a1cc52718907d54a0990b608507d04c2c27d3))


### Ref

- *serve*: Default timeout for datasets to 60s ([b4cf729](b4cf72982e12d2ccf1e7232debbb52fce265b26f))



## [1.2.4] - 2023-09-20

**Full Changelog**: [v1.2.3...v1.2.4](https://github.com/taosdata/taosx/compare/v1.2.3...v1.2.4)

### Bug Fixes

- *csv*: Fix write with nchar oom ([fb4bdf1](fb4bdf173c48ad057ac6702efe28dfa97c61971e))
- *legacy*: Try to fix migration oom ([5fbaa4d](5fbaa4d334b084a24be63e7cb9956f4084da2ff2))
- *serve*: Fix legacy data source options default value error ([a61c56d](a61c56d59ec413778db7caf90b265d1fcc513781))
- *serve*: Fix metrics display error for multiple starts ([6985239](698523998a6751f97242f1181697116a83ed6a48))
- *serve*: Fix using files path ([efed071](efed0712e760151d22cfe793544d040a1bee6186))

- Fix opc process still running aftere stopped ([59160e3](59160e3924de165ef23c6cc3020e94ef6b6b705d))
- Break write loop when encount unrecoverable error ([81df63d](81df63dc8d27ca63d7f73dbe85a16e176ef177b1))
- Break write loop when encount unrecoverable error ([82bcf9f](82bcf9fd9057da32a5008c43bc86c4fa7a5653f7))
- Fix list datasets hanging out when use with opcda ([f60c98f](f60c98faaa6709624d851e5bfcdde200d14b0e97))


### Documentation


- Improve post-install tips ([a1ae88c](a1ae88cb8f7a299353a7b912a83a336d2b203d49))


### Enhancements

- *legacy*: Print errors to stdout if fails-to not set ([58155df](58155dfcbab51ee4adda380276f1f897a722ff29))



### Refactor


- [TS-3927](https://jira.taosdata.com:18080/browse/TS-3927) opc dump default keep set to 10 ([934917d](934917dc64f711eb3f83d5bede6f076b23514d0c))
- [TD-26269](https://jira.taosdata.com:18080/browse/TD-26269) modify default value for opcua ([e8fb0fd](e8fb0fd14b8e2a06efeb28cf3744b62d3249857d))
- Opc csv config column modify ([9e69797](9e6979749f2c7762b234b759d80bef28ff43fa59))
- Modify log print ([d3bce32](d3bce32a4b34eb8817c9d2d9005a59cfdbd74d4f))
- Modify opcua desc ([58e63c7](58e63c7997c807e8d4b38d6c5db192f5f88e6971))
- Keep backward compatibility ([2ed5f53](2ed5f53fe7a3f2dfcb0ce337227bbea6d7e2c67a))
- [TS-3927](https://jira.taosdata.com:18080/browse/TS-3927) opc dump default keep set to 10 ([6ae6a32](6ae6a32377c821a18b8780115446d4634c362724))
- [TD-26269](https://jira.taosdata.com:18080/browse/TD-26269) modify default value for opcua ([463082b](463082bca53cee07fd5d107d714ad66439fc081b))
- Opc csv config column modify ([c60558e](c60558efc7026d862d2dcef644759d1c29f243fa))
- Modify opcua desc ([2d52a0e](2d52a0e89d8053361692e52f7f247aca6b89882a))
- Keep backward compatibility ([a4b4074](a4b40745f9a0ef94883fc984f344b4de11f2d406))
- Opc table config field modify ([46ba1ad](46ba1adf53c1236aee409676bce79fd87c4b9098))
- Opc table config field modify ([4a2c741](4a2c74102ba2bc8f4760d1f4270eb4a361c803c7))
- Modify opc table config desc ([145e417](145e417fcd2bcd664dece45f829284e3ae60e9ac))
- Modify opc table config desc ([2bb126c](2bb126c455d84e46ecd7723a21ffde259b3d394e))


## [1.2.3] - 2023-09-17

**Full Changelog**: [v1.2.2...v1.2.3](https://github.com/taosdata/taosx/compare/v1.2.2...v1.2.3)

### Bug Fixes

- *agent*: Ignore edition check in agent ([06f7248](06f7248eae5870b346286fc20362217a57502cc6))
- *cloud*: Fix enterprise edition check on cloud directly (#736) ([bccabc0](bccabc064058bb220ded7498fa5bef8600cf6063))
- *opc*: Fix channel closed when taosadapter restart ([4a5a1ac](4a5a1ac2747d1bc8b672641e1457ad7312616379))
- *opc*: Fix error tracing with specific sql ([c155c9b](c155c9b1c72d21ab6bf7608dedbbf9c586391192))

- Use tini to fix sigterm problem in docker/k8s ([514bdbc](514bdbc41f5d18645db5fe9dfefb855f2e0fa300))


### Documentation

- *mqtt*: Improve mqtt payload display docs ([b739116](b73911662c8cd84bbb826184ef5c5bbd17f3655c))



### Enhancements


- Shuffle to writer by node id ([e2c9af2](e2c9af2474f3dda8fa90b1a06bbf8b354da83f92))


## [1.2.2] - 2023-09-15

**Full Changelog**: [v1.2.1...v1.2.2](https://github.com/taosdata/taosx/compare/v1.2.1...v1.2.2)

### Bug Fixes

- *agent*: Fix agent keep stop on windows (#730) ([47ab244](47ab244b6cacc4f08d179bcf1ab3efb3be344672))
- *grpc*: Break stream when flight error raise ([885af00](885af00b36106791e9a23a579c02fd90c71c02d3))
- *grpc*: Set tasks as cancelled when agent disconnected (#731) ([4c637a5](4c637a567a56a263b5417cba0be760741bf04bd1))
- *legacy*: Fix add column error ([0f04a96](0f04a968ffcfea61da4345c10de975f706129c89))
- *opcda*: Fix opcda collecting interval parameter (#734) ([d6812c3](d6812c382a1c201054bd8f003ea25be47eee0201))

- Simplify opc connector on create arrow writer ([7b20adf](7b20adf1f940c0e5791c1df1f7296cb51384b69a))
- Use separate runtimes to fix pending issue ([1ef9291](1ef92919c68053c54845bd90ebd96281bec5e325))
- Opc cause whole program hanging ([aeda527](aeda527c19a2fbf71a3034b44ea8ad476dd39e1b))
- Change reader to paralle (#727) ([3cb5d95](3cb5d9555a56fc79d656a3af74f6cf8c14778155))


### Documentation


- Update release.py to auto integrate with docs ([59eb201](59eb201d9f06aec9a42aaba1351faf1c5e6e7f90))


### Enhancements


- Support print metrics in run mode ([7a693f0](7a693f0861aed4c65a1d4536c62e047313d0ac2a))
- Add more cargo-make tasks ([33e8f65](33e8f65f02d9c0cda6e4a414be6136cbba5532c9))


### Features

- *grpc*: Abort grpc connection when version not match (#733) ([dbdf37f](dbdf37fab0d5327e046405bb7931538e90714a7b))
- *legacy*: Add workers for query and concurrent-limit for write ([ef676bc](ef676bc31d6ab59a444b3a2317d118f59eb7055d))

- Add range for tolerance. [TD-26190](https://jira.taosdata.com:18080/browse/TD-26190) ([57b881a](57b881af49a15f764195c0144dcafb90a901fc65))
- Add range for tolerance. [TD-26190](https://jira.taosdata.com:18080/browse/TD-26190) ([d38397d](d38397db28e1cb73a5ddab4a0e4240429fac93f0))


### Refactor


- Opc desc modify ([1ff1704](1ff1704f66cb2fddf11e0d29274da7b94518741b))
- Opc desc modify ([0031995](00319959e3089009e4dc0ff05c962d29b135cae4))


## [1.2.1] - 2023-09-12

**Full Changelog**: [v1.2.0...v1.2.1](https://github.com/taosdata/taosx/compare/v1.2.0...v1.2.1)

### Bug Fixes

- *agent*: Fix ipc forward stream broken error ([7f28d1d](7f28d1d132232654be2fa4253488574242ae3dfc))
- *legacy*: Add column when cause 0x263F [TS-3937](https://jira.taosdata.com:18080/browse/TS-3937) (#705) ([ee092fd](ee092fd950ef678b135db62736439d983ff14bc9))
- *legacy*: Fix select tbname 0x0362 error when stable special ([307337b](307337becee0cd735c6b9f386afda225cefa2a5b))
- *legacy*: Correct fail-to time range ([7cb5fcd](7cb5fcd4619a6f4fc75575ed897f0f2e375ef679))
- *package*: Error occurs when multi build (#701) ([390a5e1](390a5e1808702b529eb64fea67aa334720fda5d4))
- *serve*: Persist fallback to std::fs::copy when failed ([136b853](136b853d554297a600c12fa516e523526f4663f0))
- *serve*: Fix invalid value "Cancelled" for enum Status ([4eaf948](4eaf948135c581037f0c7c50b8d1621a89752bfc))
- *serve*: Stop task cause 404 ([0895cc4](0895cc4b4c5534e63e32369d6dbb1baefb9a96be))

- Fix agent keep live issue (#699) ([a6c4fb4](a6c4fb4bac4364c7ee9fd90b791f1ac415fd0a5f))
- Clap short option confilct (#703) ([f8f5291](f8f52918b46da7158c43c190b25328b8a1b50bd8))
- Clap short option confilct (#704) ([405f900](405f900138ee665f17b60ef882623ed8cf5b6b61))
- Change default opc observe interval to 10s ([2eb2642](2eb2642ae907efdc52e58ba19b785cc1e5b153cf))
- Try to fix pending issue ([b6d99f5](b6d99f5def8a341a36d77ab9f2600b0b8eb993a4))


### Enhancements

- *legacy*: Add time range in fails-to output ([9dd93a8](9dd93a8aa9ae970ea70774c46411b3e0d7f9bc0a))
- *opc*: Fix opc dump path (#693) ([04962fa](04962fa94e920cfd1fb0154cb40ce13a49ca0253))
- *serve*: Stop task fast with a timeout wait ([0e7e5db](0e7e5db3a54b497af7637c97c55715be84ea9f28))

- Install optimization (#697) ([20c9607](20c9607f1d2ec599ea669011d2afe08ebc6b7963))


### Features

- *source*: Add historian data source #[TS-3802](https://jira.taosdata.com:18080/browse/TS-3802) (#664) ([a04d189](a04d1895a1e39bce8ffc909a0ee00abb0ec34eda))



### Refactor


- Add schedular for metrics print when execute run ([be3c813](be3c813c293370f0c202eafaa61ef47155cb72ff))
- Add print function ([44eab83](44eab83369c03376a4c246561134f53c1808a33d))


## [1.2.0] - 2023-09-06

**Full Changelog**: [v1.1.0...v1.2.0](https://github.com/taosdata/taosx/compare/v1.1.0...v1.2.0)

### Bug Fixes

- *agent*: Set tcp stream blocking mode (#622) ([0e95066](0e950662b61413fa7d576f3f2bbe366c8aaca3b1))
- *agent*: Fix ipc read error in agent (#645) ([6670316](6670316ce8bc3d8020d5b365bd07cc8b07400ed0))
- *agent*: Fix error context when target database name error ([74b19e3](74b19e3104694f82cb74a6f4c0fa3a6575f18c1a))
- *agent*: Check agent alive status error when agent stopped (#684) ([4a593ce](4a593ceac3c3a277e45166a39f1805bf3fd09eba))
- *csv*: Fix csv import in-completed (#603) ([f86f4d0](f86f4d001c9ab1e139213e3a4c0200d8ed9d9304))
- *csv*: Fix csv import in-completed (#602) ([a08d9be](a08d9be598cd87b37245cb8a720e06ec16a7a138))
- *csv*: Fail fast while write with IPC (#615) ([2a7598a](2a7598a901e35916c1e20664b9ab06443fcffa8d))
- *csv*: Fail fast while write with IPC (#616) ([88b0b0f](88b0b0f59c2d2a2b056d6fd2eb5115ba1316dad0))
- *influxdb*: Add protocol option to support https #[TD-25628](https://jira.taosdata.com:18080/browse/TD-25628) (#588) ([d316d78](d316d78c0d2061a34492bfb2881925324e8deb34))
- *influxdb*: Modify the content of the data source page. #[TD-25677](https://jira.taosdata.com:18080/browse/TD-25677) (#618) ([4de1cf7](4de1cf7316c1d5e6b52c2c6dc1a710b41d8e81bd))
- *influxdb*: Fix tiny bugs. #[TD-25792](https://jira.taosdata.com:18080/browse/TD-25792) (#617) ([8e6f4bf](8e6f4bf961c14110ee8b29d7a4c85a4c2b43cc66))
- *influxdb*: Fix the bug of the name of measurement with dot. #[TD-25887](https://jira.taosdata.com:18080/browse/TD-25887) (#643) ([3028b78](3028b78b537a4ce5ae901b3bebd0e00278b797bc))
- *influxdb*: Special handling for influxdb cloud. #[TD-25842](https://jira.taosdata.com:18080/browse/TD-25842) (#642) ([066a2f9](066a2f902a11dfbfa6c5cac3e879f1f9fd4b21bf))
- *influxdb/opentsdb*: Modify tolerance type. [TD-26147](https://jira.taosdata.com:18080/browse/TD-26147) (#690) ([225c10a](225c10a1bc7d474780be0af1ee24b81793e235ec))
- *ipc*: Retry ipc tcp forward #[TS-3800](https://jira.taosdata.com:18080/browse/TS-3800) (#599) ([2dd3c73](2dd3c734f41dbf49d695d096fd6a5b5d881c357e))
- *ipc*: Fix lush ipc writing error (#619) ([c276564](c276564da23474a529e77500fe47310a28b4ccbd))
- *ipc*: Fix lush ipc writing erro (#621) ([64db9d1](64db9d1846328ffa513fde0e446481d471f0fec4))
- *kafka*: Group consuming not work #[TD-25653](https://jira.taosdata.com:18080/browse/TD-25653) (#591) ([5b22611](5b226115387aa1c685376aa3a20198de399c3d4a))
- *kafka*: Group consuming not work #[TD-25653](https://jira.taosdata.com:18080/browse/TD-25653) (#592) ([3937cf0](3937cf025b4ee901955a00bfb396c7e67d41e388))
- *kafka*: Fix td to kafka fail on topic exist (#641) ([44c3ca4](44c3ca4423223e4189365ac268411d733f2a3998))
- *opentsdb*: Fix error message when target database removed ([932fb29](932fb2936596d473fbcb5942f5939567fad3adff))
- *opentsdb*: Filter empty dps. #[TD-25906](https://jira.taosdata.com:18080/browse/TD-25906) (#653) ([263e70e](263e70e9b71e1710289651dfa53efadad7c515e0))
- *opentsdb*: Add datasource validate. #[TD-25750](https://jira.taosdata.com:18080/browse/TD-25750) #[TD-25755](https://jira.taosdata.com:18080/browse/TD-25755) (#679) ([da3ae1f](da3ae1f97b73f949a1d39d1f35600e0df37b6f62))
- *pi*: Fix time zone parsing in toml (#631) ([2797292](2797292934fece476fee3459898c9961ac40558e))
- *serve*: Resume task when connection closed in tmq (#586) ([c87462e](c87462eb75d03bcbf4b9096b30fd05427334f787))
- *serve*: Fix serve mode hangout (#610) ([b7be616](b7be6167fa2735523719a9e1a603e57a49a5c5d2))
- *serve*: Fix serve mode hangout (#611) ([ee73499](ee734995cff6b68c4d991fcd43f479b89a59f487))

- Fix token param check error ([d749351](d749351b604b92f5fcf7bb11216acbf8cb6e6208))
- Fix token param check error ([5ae5a9f](5ae5a9f6f97d2907fc56fdfc34962570c8d88b2a))
- Fix cloud image error when start a task ([83e0ee6](83e0ee60155b2ac941d295b164a9f2b75219208e))
- Add protocol option. #[TD-25628](https://jira.taosdata.com:18080/browse/TD-25628) (#593) ([16d5d3e](16d5d3e1c12a6fb316781a5b37bcfcb4e93f10c5))
- Fix error hint (#597) ([ff1d614](ff1d614a0e880e34e07908b7026053feb56a5c44))
- Fix error hint (#598) ([f135fe4](f135fe4c123439d4ffc931d3c10d1fb463e9cfe5))
- Fix get string content from value error (#606) ([3cd5426](3cd54267fd5f6acf86bc7f02d45fdae1e2db89c1))
- Fix get string content from value error (#607) ([37276de](37276de6a62876fe1273f85809a1a51f8957143e))
- Fix panic "call blocking only when running on the multi-threaded runtime" (#613) ([104a3e0](104a3e01c114a0d0dddafdfdb3c130ba942260e6))
- Fix panic "call blocking only when running on the multi-threaded runtime" (#614) ([10ba932](10ba9329db2db0d9d6179b1117b52a3d9be0b09a))
- Use universal enterprise edition check method (#626) ([38c00a6](38c00a60e1d0e2493338f2eb6b9e8f77e4ab997a))
- Use universal enterprise edition check method (#625) ([ced66db](ced66dbc2a23f8b4e607e7b7e240ffae8ffc3e2e))
- Fix possible blocking while build ipc reader (#640) ([8a252ac](8a252ac0c4e3d3d4e8cdf83e25acf48ae380ba00))
- Fix possible blocking while build ipc reader (#639) ([374d1e8](374d1e80d30e30dd9fc1e0fd862c992eec29b114))
- Check timezone range. [TD-25889](https://jira.taosdata.com:18080/browse/TD-25889) (#646) ([f82db93](f82db93b02628a4750f2bc277020849d9f08fe5d))
- Update retry wait time and add log info ([c60d4c5](c60d4c5760430ef62c5f4fc3b287d7bd72d82b9b))
- Fix [TD-25957](https://jira.taosdata.com:18080/browse/TD-25957) file code error ([1942092](1942092e0aa4e132d95a8b1676990d885c5e24e9))
- Fix [TD-25958](https://jira.taosdata.com:18080/browse/TD-25958) check error ([58142c6](58142c6ebd9bfde8576755698a5643dcb3832d67))
- Change taos to async taos ([59ca2f0](59ca2f0aba4bf9256f5bbf6658777bf4f1b3a863))
- Update connector version (#654) ([1598784](1598784a0e4bfb697d706809a6395153d9c00601))
- Fix server run on windows (#655) ([9503aff](9503aff943501fc004c760bde0fd33fa3f5ff056))
- Change task_id to topic_suffix and delete taos in config (#657) ([8e957c1](8e957c1a1652f4ac861e409f7497542d318d7856))
- Modify the time accuracy of the returned data. ([28a27c3](28a27c371ca39f0da968aa01e10cfb764aba9913))
- Modify statistic, it is a two-layer dataset. ([2b5ca9e](2b5ca9ee8e8b1e995d9f559b86ce5ec003c00178))
- Fix metrics not right when connector use multiple conn ([54a988b](54a988beceb22e52e0604a97e9db978d58589100))
- Fix child table metrics in lush message process ([0ca2de5](0ca2de5b7fede400f6a8f969f32c7162e1e0df45))
- Fix kafka sinker batch size ([70cb2a5](70cb2a5198617d809a8bb399d666b3578a35bd59))
- Fix for error throwing ([6bdc067](6bdc067abe71ae01b269dd548667831d7dc7ea40))
- Fix [TD-26097](https://jira.taosdata.com:18080/browse/TD-26097), time cost < 1s ([7d69d61](7d69d6152771240b9e0b439a6175d8302edbdd40))
- Fix for kafka address ([0321701](0321701822b8b40259d2ba76cc7942c323a8ab80))
- Fix ipc handler not completed ([0fe30e7](0fe30e7dc8ad8a1f5b36ab19c1e56fc587aab1b1))
- Metrics time lost when use agent ([33f07ca](33f07ca33747f7a74c533e431f909b9a724f2b8a))
- Tsdb not completed with agent (#681) ([057502c](057502c3b5652c25288bf0bad675488b72a8bee9))
- Add tolerance interval. [TD-26147](https://jira.taosdata.com:18080/browse/TD-26147) ([8f11a9f](8f11a9ff763ec57f43a0c453f1d36fad4fd5bf44))


### Enhancements

- *ipc*: Point message improvment (#600) ([56ce39f](56ce39fa687d2bb8a6caefa22d2f2c48a1bb4dd7))
- *ipc*: Point message improvment (#601) ([6e31880](6e31880c98fbf0e83433f7b1b58b0ea5cdeb346d))
- *opc*: Register node in parallel in subscribe mode (#582) ([7dfb3af](7dfb3afb450284dc8200111933af5524be4e2cc1))
- *opc*: Change arrow ack to lush (#635) ([b6fc7f4](b6fc7f4e4908455479f45f57af9c013121f47aef))
- *packaging*: Add post-install tips (#589) ([f82f5bf](f82f5bf1c873790c9257dc6a516d90ac27559d77))
- *tmq*: Improve tmq to td error message (#609) ([4ba5f1f](4ba5f1f303433c367c3a79e0a0273d6e5b403511))
- *tmq*: Improve tmq to td error message (#608) ([0a3cc90](0a3cc90afd1393b3d1b13b9b3d6c5ac94e5a12bb))

- Lush message support alter length [TD-25679](https://jira.taosdata.com:18080/browse/TD-25679) (#627) ([6cf2529](6cf2529ee360458c56e41067a68b1fc596abee59))
- Sink connector for kafka (#499) ([ad6ddad](ad6ddad8afce198c6f5eb23164e2f9679b6ed6f4))
- Handle 0x032c for flat message (#630) ([23bf58c](23bf58c9c99085a510b6cd5a265b2808768799bd))
- Improvements for tmq and ipc (#632) ([1e44bb7](1e44bb756b9b5b5455c3ba05b91850a46adc6f1e))
- Modify point message insert (#636) ([a933e3e](a933e3e289c18e07b3570f414d7ee6aee3259c86))
- Modify point message insert (#637) ([6edd156](6edd15682951d238f743be01bd0a116dd7e08d8c))
- Agent message support metrics ([6e1d71f](6e1d71f713ab9e636d07f59b1532f798b610174e))
- Wait for IPC stream listener task done (#656) ([c61e8d2](c61e8d2d25144504ac8dbd02a828018a0f189d20))
- Support metrics for tasks ([09148c8](09148c8e95345790d3ebd780a4892a203696d6f0))
- Kafka config ack timeout support duration config like (1s, 5m) ([719f56d](719f56d7b4e387df7d21262684ca1e28c49fc193))
- Kafka config ack timeout support duration config like (1s, 5m) ([cad0f22](cad0f22dfcfd1e0643a2b641b651e73435ae46cf))
- Add response statistic. [TD-25854](https://jira.taosdata.com:18080/browse/TD-25854) ([d1cb03f](d1cb03f4c894bd7e37e0de1c9c210eadec48428d))
- Metrics key sort (#683) ([b8967ad](b8967adad9f9125943222d7fb8f3c1c80ddcdd35))


### Features

- *influxdb*: Optimize time format and column type. #[TD-25662](https://jira.taosdata.com:18080/browse/TD-25662) (#596) ([662d8c7](662d8c7017cb9d0d443740c2c2d8352c4805a501))
- *kafka*: Check kafka is available#[TD-25752](https://jira.taosdata.com:18080/browse/TD-25752) (#623) ([553e425](553e425ca3132558b3bd401c62bec10acad734ca))

- Add opentsdb connector. #[TS-3770](https://jira.taosdata.com:18080/browse/TS-3770) (#612) ([7fcd101](7fcd101677b3e01ed4b05b5fe1914b67f84840d3))
- Enable tracing with open-telemetry ([8faa21c](8faa21c2d1efdcadd628935a7f4bb0a857c34496))
- Support metrics snapshot in server mode ([bb4772d](bb4772dcf0038ffa9347f6e84a1aeb713e1ff3c3))
- Use jemallocator by default in linux ([59ce247](59ce247f98757e9a1db62011095bef8859aad7b3))
- Add opentsdb to installer (#682) ([93b7ad5](93b7ad5d41699c2d48e64d4ca9ff172b230762da))
- Add --tracing-events option for tracing span ([1484b73](1484b736cd17a1d6c1966e935cc35834ef01d2b6))


### Refactor

- *kafka*: Error handle (#624) ([5602062](5602062a2b8461d2ddb6a29b9d7e8eb9146dff25))

- Error message modify ([58b8357](58b835778f9fb42ecca721c0fa1a390dabbf18b0))
- Error message modify ([eaacc5e](eaacc5eaa6a1d8913e3c621a29629cfbb7233f47))
- Modify name value (#511) ([2e67956](2e679565f482f3c15b994929222677fa0c2872d8))
- Opc metrics ([c623ba9](c623ba964a8f5a9ef7ff1b6f18d2bfc47916c93a))
- Remove table counter temp ([b233b2d](b233b2db13427e7d7d6b048f1056c84ea1e10b95))


### Dep


- Update taos v0.9.4 ([7477ce3](7477ce3fb17311d5591d970e0ada36fc5098fc07))


### Packaging


- Add post-install tips (#587) ([6788bbc](6788bbcbbcd534d1c1a586cbae70306f18816345))


## [1.1.0] - 2023-08-05

**Full Changelog**: [v1.0.4...v1.1.0](https://github.com/taosdata/taosx/compare/v1.0.4...v1.1.0)

### Bug Fixes

- *agent*: Fix negative duration delay from server ([3678841](3678841ef606db23f72bc9219c9684ec7cb97616))
- *agent*: Update activities when task cancelled/stopped ([319215a](319215a37c3fc87ba19959ee6b020b05707de6af))
- *core*: Fix regex parser error ([4db3535](4db3535d1b877a762605b9e6361517a2a715adeb))
- *core*: Improve error for multiple csv files with inconsist columns ([4e3aaf1](4e3aaf1e714f598b06335d1b21bf1923d2998f0e))
- *core*: Fix in-compatible for early version of local.toml ([d936317](d9363179ad554cda204018d7f0d5035bce8b4f96))
- *core*: Fix in-compatible for early version of local.toml ([5d3d0eb](5d3d0eb39814add687fe4eddaed1fe2d1a6ca4b4))
- *opc*: Report opc data sets error when failed ([4c2bb2a](4c2bb2a660dcf600fd40722247ee16dbae4e1fb8))
- *serve*: Fix filemeta api error message for csv ([d526ae3](d526ae3e58f5207cda5f51f57332bdb0c7af846b))
- *serve*: Improve error message in filemeta api ([46fdbe9](46fdbe9445999add078132ab776e1ceebaf76311))
- *serve*: Raise error when csv header is empty ([1f364eb](1f364eb5a2ffc9fccd209a7f1bf224d0d81555f8))

- Support unix timestamp in csv parsing ([f0b0790](f0b07905533783f3a58b9d075daa69f5a7897f08))
- Expect status updated to completed when task done ([d58a291](d58a2913f0d1fd0ed373fe6749623b1eb636024d))
- Csv data source localization ([19a443e](19a443e52b23e7a3bd15546c744b4d180847622a))
- Distinguish jdk versions. #[TD-25502](https://jira.taosdata.com:18080/browse/TD-25502) ([b7bc6aa](b7bc6aabd4d91c6f61ca2c6b635d84367ecc24e7))
- Update frequecy daily ([0be0866](0be086643ad429be5f5fdf71b9f1152a372f0869))
- Update frequecy daily ([5aff57a](5aff57a684d1bad86dbb82cdd22638b26ec8bfcf))
- Fix sql too long ([6069644](6069644bdcfeaa0190c8cb4bbf257c2fe08d43c9))
- Fix lush add tag error when use agent ([08447fb](08447fbb024ab1c86eaea753f15d57dbefb21226))
- CSV ns to ns, us to ns panic ([ed63c08](ed63c08e440dcbb35948a3ea70d0471ac8f6944f))
- Support kafka source to tdengine ([cebf0cd](cebf0cd66351a09012facae6527d4108f2082a5e))
- Fix varchar lenth not work ([23bb679](23bb679eba66a98b3a622793cb7a96179002b562))
- Add timeout parameter in kafka source configuration#[TD-25559](https://jira.taosdata.com:18080/browse/TD-25559) ([7c17875](7c17875d32e4f169f8f087d4fac2f1af7eb0a1f9))
- Timeout parameter should be str ([2ae6a09](2ae6a094b6ccc37ca080e7d41eefb270849e7bcf))
- Fix json meta deserialization error in tmq to td ([194e82c](194e82c7a78ec5429beee3ab47a87bcc742ca49f))
- Fix json meta deserialization error in tmq to td ([c1cf3fb](c1cf3fb37882dbcb5470113466f418d3d064339f))
- Influxdb jdk error cause panic ([ebc2e78](ebc2e78562d64baee8808a45c522acdcb9f7b772))
- Report IPC stream error to runners ([0d0ddff](0d0ddff3e8ccba0e3c5b54b163f46638b11cb9b7))
- Fix csv lost data ([939dfed](939dfed3b01fc7b38d3e176257062488137732f6))
- Fix rename table error ([53fe2ba](53fe2ba47f8e3ec0a1ebc78204927bf837c0a5ae))
- Fix max_frame_size too large ([a4b58e5](a4b58e547fc11adf0ea6542309013e9a446d0c91))
- Stop kafka task causes pending ([9114878](91148787a6093dd1f8f3b7d33fc636d5cb9bb0f4))
- Fix child table sync fail ([96c8119](96c81191185d7e7dea1a8656bc57af0262d3d94b))
- Use native-tls instead of rustls ([62f5963](62f5963bf53f4b4497b9bb911dc1d3144c89aecf))
- Packaging script fix ([7c71adf](7c71adf359223edd7f698fbbcb7a204329a32366))


### Enhancements


- Check duplicated column or tag ([3d57bdb](3d57bdb5c35e82e18d9a93f987f00dce4643c9e7))
- Support rename table with regex ([2a0992c](2a0992cc60335f1d419b2edeba3011438b5b0c01))


### Features


- Kafka source add ssl#[TD-25437](https://jira.taosdata.com:18080/browse/TD-25437) ([62f2776](62f2776f2d10ace55d30b459559fce90bf13fde1))


### Refactor


- [TD-25455](https://jira.taosdata.com:18080/browse/TD-25455) add min value for keep ([66257bf](66257bf530c783fd2cacc880507f9811af6522a3))
- Remove csv seq config temporary ([056b2a5](056b2a5dc11667b0065f2523b85022510db558f4))
- Fix replace not work ([15a6165](15a616507f8b701375cd78b06f1c02f2a59d5b77))
- Set Payload size to 100m, MultipartForm size to 2G ([de3bfb3](de3bfb3ba14bf5a07e6d2143a3e7dac94660f722))
- Size set to max ([9634ccc](9634cccc215bd52c60dbbb27c90180652fdaa608))
- Remove reduntant config ([0e29495](0e2949547f1f949f82a85729e70d9e602b93fe5c))
- Set max_frame_size ([9d76fb1](9d76fb18be607501e5b296ccda79151254e18e49))
- Set max_decoding_message_size and max_encoding_message_size ([69d8fba](69d8fbaa9bfd5b7bea946287a0854857f97ba8e5))


## [1.0.4] - 2023-07-28

**Full Changelog**: [v1.0.3...v1.0.4](https://github.com/taosdata/taosx/compare/v1.0.3...v1.0.4)

### Bug Fixes

- *csv*: Csv header/has_header/skip behaviors ([f878b80](f878b8000113c5ab74b8ad498498b3e92b89fa63))
- *serve*: Default use ws in explorer ([7ecd96e](7ecd96ea6abc66455bf2f51813fd4c0ffe28de51))
- *serve*: Fix legacy options in explorer ([c48ede5](c48ede5fbc107694cf5c45d600aa494dba74e744))
- *serve*: Assert task.force is true ([c1869e2](c1869e22360ed37dde335c79b752f76647171b4a))
- *serve*: Assert task.force is true ([ea1dc38](ea1dc386a8789e8c15197edfb2487453cf3ad2db))

- Add query parameter. #[TD-25314](https://jira.taosdata.com:18080/browse/TD-25314) ([113aba9](113aba97290f3d1434399edd881b1fd6e20d47a8))
- Fix [TS-3723](https://jira.taosdata.com:18080/browse/TS-3723) version parse error ([22a65f2](22a65f2507c8f2897e0e5a2a824b13d95315ba69))
- Fix [TS-3723](https://jira.taosdata.com:18080/browse/TS-3723) version parse error ([bc2dc2f](bc2dc2f718d2f85b675d739542af1c8479f700ec))
- Modify the format of the placeholder of time ([acd5d2b](acd5d2bcf60b7afbb468573e5df9cd10e6821fe4))
- Optimize error response. #[TD-25240](https://jira.taosdata.com:18080/browse/TD-25240) ([df0e585](df0e5856799705f264f2e0ab51684ac8c37e8c45))
- Modify to unified error. #[TD-25240](https://jira.taosdata.com:18080/browse/TD-25240) ([7a381cd](7a381cdc5eea338f8facd50c163a302364856c0b))
- Fix CSV stream writer error ([9fd0ebf](9fd0ebfa45095827cd9b8ef90e45855e49007717))
- Fix config check error when get dataset ([66f6a58](66f6a581e9d83341baf9c81aa9e0a5da4a797b4f))
- Fix for csv connector parse error when no header ([d10a58c](d10a58c54d0c0fafcf82d6a1a893070c18102420))
- Optimize error response. #[TD-25240](https://jira.taosdata.com:18080/browse/TD-25240) ([aaa5770](aaa5770e2b9d55a4248fa996978d0f7ec54e161a))
- Kafka source option should be an endpoint ([211eb27](211eb27a619b0c93ade00e7dc9138111776ebf7b))
- Fix for batch size in csv ([1593689](1593689c4be2034022976fede28635d5e0d9f7f7))
- Should'n use same key in datasource config, cause empty value when edit ([baf2737](baf2737f50f5b73dc571efdeadc10966ead69969))


### Enhancements


- Opc add dump config ([398af02](398af02e0e0d4c83993c3235ae0d1a6aa3443f23))
- Config log keep days in agent.toml ([2852328](285232818d146bb1416a0a3cb92f871b3c2dd363))
- Config log keep days in agent.toml ([84ab9a2](84ab9a2d4a70814b084988b6c135e5e5b15e0251))


### Features


- Github actions test. #[TD-24480](https://jira.taosdata.com:18080/browse/TD-24480) ([d4a73ff](d4a73ffa606ddca7a7471ca9f8fe1f0d5ca7eea8))
- Github actions test. #[TD-24480](https://jira.taosdata.com:18080/browse/TD-24480) ([d4c9f1c](d4c9f1c2e756a3c38c5ebe7ae38108dcfb500da9))
- Github actions test. #[TD-24480](https://jira.taosdata.com:18080/browse/TD-24480) ([4917ea9](4917ea99c0a3ef33aed1baf4e4429c4af831998f))
- Github actions test. #[TD-24480](https://jira.taosdata.com:18080/browse/TD-24480) ([37d31eb](37d31ebfdb59f34c27e3faddb51f351b9a00b9c0))
- Github actions test. #[TD-24480](https://jira.taosdata.com:18080/browse/TD-24480) ([849c60a](849c60a275387fb51ecfc2d27783dfe3ad0c306f))
- Change the beginTime and endTime to the format with timezone. #[TD-25355](https://jira.taosdata.com:18080/browse/TD-25355) ([6ab93dd](6ab93dd84b11e22e5639bf4aae82f0c97922fe63))
- Kafka source use SSL authentication ([dd0c24c](dd0c24c7d839f75608607b39523f368b393d58a5))
- Kafka source use SSL authentication ([d112ee8](d112ee8e93f387eda1454ddcdc1f533489b6b506))
- Improve agent activities awareness ([156bbd1](156bbd1ea6a5823b3ca634443d718e11fb873fd9))


### Refactor


- Modify param type ([ee30315](ee30315264983d0224dd6304141fdca123b4cbf9))
- Add opc csv config param ([8d2811f](8d2811fb66579a78caddcda2ccb7acaebc93594f))
- Modify error format ([861c314](861c314c889eadf7fe52b8045e19e7b727271185))
- Add desc for duration type in datasource ([8e586e4](8e586e4c1bfc36fdd205d7e8271974f3a7ad0a62))
- Desc and value modify ([4d5226a](4d5226af381a120a05b04e0701c3c9e8c877e712))
- Restore param use_csv_config ([dfcd8ae](dfcd8aea5cfbe806cc7c3d8ee5cc4db09aa58648))


## [1.0.3] - 2023-07-21

**Full Changelog**: [v1.0.2...v1.0.3](https://github.com/taosdata/taosx/compare/v1.0.2...v1.0.3)

### Bug Fixes

- *api*: Fix GET /agents api error ([29175f8](29175f83a84046b762bc518d584de3984fa5d618))
- *core*: Fix parser deserialization error ([84056bf](84056bf72b87fae9c6599eb9f88a47c3a817d523))
- *legacy*: Fix unexpected error in legacy sync ([4652132](465213251a9f6761668d93ad8f889c74a8980f46))
- *legacy*: Fix unexpected error in legacy sync ([acf177b](acf177b33d6dd59b2d200bbef25150c81fc7d03c))
- *legacy*: Fix memory increasing problem in syncing ([4eff0fb](4eff0fb4d105c530fe57b576dd12c8fb910b1fa3))
- *legacy*: Fix memory increasing problem in syncing ([91b53c3](91b53c335c0397c6b370d02a1b327af252007092))
- *serve*: Plugin definition should not use line-breaks for md str ([ac361b4](ac361b418bff7d5d580eaf0129685e0f58b81876))

- Fix for csv connector ([533ae46](533ae461b137fb253622bb4c312e6f96a34fada2))
- Read configuration with default value. #[TD-25144](https://jira.taosdata.com:18080/browse/TD-25144) ([c7e29b9](c7e29b9e30d4246a14d28d733dafc9f38e485efa))
- Fix tag add ([85a2187](85a2187d5d9f5c9a3bc20a5693704df52c8de660))
- Fix configDir and libraryPath verify fail ([af9df18](af9df180c3e5f9f275c5abbbfce55df2c1ce7ff4))
- Fix tag add ([656d8ad](656d8ad04b296f012ed2d3e8607f4af6e9d3ee78))
- Change the log to English. #[TD-25153](https://jira.taosdata.com:18080/browse/TD-25153) ([fe69201](fe692017551f6ac983796ec533d5a4da7f38dcd2))
- Fix authication param value set error when different AuthItem have same param ([21cdf59](21cdf59e58b0967c83df9ebbbf4ac683634d2202))
- Push updated schema information. #[TD-25216](https://jira.taosdata.com:18080/browse/TD-25216) ([c7135c2](c7135c2def1c8ab442408786edac44b1d18338eb))
- Add startup parameters to support jdk16+. #[TD-25189](https://jira.taosdata.com:18080/browse/TD-25189) ([0317b5a](0317b5a6b4b99f9ac1a1754e74bd3f16afd1927f))
- Fix opc message insert when use csv config ([3269de9](3269de96c63eb67f20372d40199c763aa7fc2a48))
- Remove print message ([356c513](356c513e9dd96fa7274de7c615d39df1b05df092))
- Influxdb log typo ([96d4b3f](96d4b3f92d58d8498a62afb7ffc102e18a73f2a9))
- Pi log typo ([ee7b0bb](ee7b0bb3472d023ae66db7d5492c37e007996b0c))
- Opc log typo ([e1aded1](e1aded1d2878be0272af3583387fe149ed8c27bd))
- Influxdb log typo ([33a047c](33a047c472acd02c7e910711a7f8855a7612aef5))
- Pi log typo ([3fd88b4](3fd88b475e2ec482acec21a72e5a3e25031213b9))
- Opc log typo ([a2314c8](a2314c8dbca0f1ce1435c9d325aa3968a255149d))
- Fix opc runtime error ([961041a](961041a8ae589c315aa82e8151ab3bb1075dd534))
- Fix test compile error ([f8a108d](f8a108de9bfc84741d42618dbda7c2ca9fbef679))
- Fix test compile error ([2123ad5](2123ad5be9eb559de90327fa58c83b1e9d097a47))
- Sort the data returned by getSchema. #[TD-25249](https://jira.taosdata.com:18080/browse/TD-25249) ([50f0e24](50f0e241e5e47ee13afdea24fbcbc84bf2b2fabb))
- Modify location. #[TD-25273](https://jira.taosdata.com:18080/browse/TD-25273) ([f09fda7](f09fda7e7c77fda49c472d592093d42cbd9f8d73))
- Delete the orgid of version 1.x . #[TD-25266](https://jira.taosdata.com:18080/browse/TD-25266) ([19dc18a](19dc18a02ad11fa7595b60ff2462776de8a10517))
- Delete the orgid of version 1.x . #[TD-25266](https://jira.taosdata.com:18080/browse/TD-25266) ([46ab724](46ab724c2ae2e1b336466971a25b62b4f46cafa9))
- Modify the description of the readWindow. #[TD-25257](https://jira.taosdata.com:18080/browse/TD-25257) ([55ec04f](55ec04fe856a2e30348b43486bb0256d88124cbc))
- Fix tag value match error ([305c79b](305c79b29f77d925f81a33ddbaeea4de2edef1f7))
- Fix decode invalid padding ([b30f4ee](b30f4eefc258820e3e9bed87a1691c614b459438))
- Fix enabled not word ([d2d3113](d2d31137145d176f77a83e393b50e90e78a8572a))
- Optimize error response. #[TD-25240](https://jira.taosdata.com:18080/browse/TD-25240) ([9e3ed73](9e3ed73c8f430c41a9e5b325c714569c8589dca5))
- Fix csv options hint ([b922804](b9228046d562f69e1961a8e0b7db91d2c3e52af1))
- Optimize query buckets. #[TD-25295](https://jira.taosdata.com:18080/browse/TD-25295) ([9bb66e6](9bb66e65d4ae24447054e3d7587eff0f4419d330))
- Optimize query buckets. #[TD-25295](https://jira.taosdata.com:18080/browse/TD-25295) ([ae7ed21](ae7ed218a2422e4bef79442e1eb9f69965b30ee7))
- Ignore when session not valid ([56478ed](56478ed81b1d641a7f46378be77907141feec57a))
- Ignore when session not valid ([1368845](13688453128ce8542a138fbeabaaa19a0a5a4572))


### Enhancements


- Support insert for csv config file ([823a987](823a9872ac3c498dac546f647621666b3fed1486))
- Set log keep days by env ([05e4c5a](05e4c5a15983845629cdf664414f394a60a8979a))
- Set log keep days by env ([bc8583e](bc8583edf25f8400e7e4ea4d3a4e65e2eebd73de))
- Dump opc data to log file ([a888414](a8884146c280527ddb794bb39070730c4982b6a5))
- Dump opc data to log file ([6f59ee3](6f59ee358173a01d0a7afb2551cc2d4a344c89f5))


### Features

- *data source*: Add legacy data source ([4d826cb](4d826cb2f508582b7063a187be81199fb3009342))

- Support set tag value after add tag for lush message ([2ec4b6f](2ec4b6fdf9ed85c45b855e1ec0a00ea615b5f21e))
- Taosx kafka source#[TD-22294](https://jira.taosdata.com:18080/browse/TD-22294) ([3f87596](3f87596b1dee089c1d39a741ad41d98ef1396941))
- Optimize ds/in for explorer. #[TD-25208](https://jira.taosdata.com:18080/browse/TD-25208) ([330ede0](330ede0a81f8b65c12420e948761b95b4f39d1de))
- Supoort download file ([622deea](622deea823dc502473423b24aa93cd0b55cfd345))
- Support replace param with file content when use agent start task ([de59cbc](de59cbcb0dec10d84c5cf55480c992c57bc098df))
- Add kafka data source ([b2682b9](b2682b98c9ae8839ebd10d27dde84669c70e11a9))


### Refactor


- Refactor file upload response and filemeta request ([5a72d3b](5a72d3b3a37adfd0ae9700f9df060f4249e13de4))
- Ref get_string_vec_from_param_or_file to async ([a7eb870](a7eb8708ed4dd1c825694d18724a62a4da631220))
- Support get opc table config from csv file ([7b1ac32](7b1ac324390ed6c35978d27d75a96c3e70404e22))
- Set default value ([62d717d](62d717d9718521ba24ba214cf1cc7f5f02ba63b4))
- Fix create stable and insert ([c06cfa0](c06cfa002f43af704facc97f568237d3f11019b9))
- Return file path with file name after upload ([a0d7926](a0d7926064aeef89b42a54d4722109c01f950c93))
- Modify print ([99e6b88](99e6b881c356ba2ed548bfff66110ce6664d895c))
- Remove redundant code ([0317a1e](0317a1e6dc5de9c59dc4a51b4688aa1581874836))
- Use table name config in csv config file ([341dbe7](341dbe7b4f9c557026252a2439e68c94db2d1f2b))


## [1.0.2] - 2023-07-10

**Full Changelog**: [v1.0.1...v1.0.2](https://github.com/taosdata/taosx/compare/v1.0.1...v1.0.2)

### Bug Fixes


- Piconnector parse big toml file failed ([aaf0115](aaf01159275f63ba82c83c23ddfc8012543c9168))
- Piconnector parse big toml file failed ([70c22f9](70c22f9902c9687da1d3abbff4c3cc5d057bf89c))
- Local time ([d68f744](d68f744572bcb84a9cab83265745fd6fc22b2916))
- Use timer clone ([bc84f81](bc84f81a93a2b1a0bd1383e0cd7830f7737d43eb))
- Timer utc offset ([c3c9f57](c3c9f579fa38d3430a774e35b3fd1a0a37017a30))
- Time local offset ([9d56a51](9d56a51c3aab4ad60735359bd48faf98ecba89fe))
- Use chrono timezone offset ([c97038e](c97038e79dc62501af3344f7a2001e5ff8439ebe))
- Remove time feature ([34d690a](34d690abce072f714097893c619ca976edab2722))
- Remove unused import ([1a39ee5](1a39ee5468c497feb324fc1b6724ed9ffe4c196c))
- Local time ([8eaa42a](8eaa42a163d9cc407cff92c589a743a52dee4b25))
- Use timer clone ([3083263](3083263f6699fa226bf1a10729754da2a985be4e))
- Timer utc offset ([bd2c7de](bd2c7de9c2dd0c2c58fdd3dda65a830eacffff1b))
- Time local offset ([cea01ce](cea01ce9a9e6604fce53f6f5c7e5664676e4afb6))
- Use chrono timezone offset ([190a68d](190a68d821efe3f06e3dde12af791cdc99fb73e7))
- Remove time feature ([b84574c](b84574c6fbf00d8814428c3c9d109d2a5560b535))
- Remove unused import ([8a427bf](8a427bf2c2862b02a7b84737306c5f87ee66ef6e))
- Modify the parameters of the query schema. #[TD-25157](https://jira.taosdata.com:18080/browse/TD-25157) ([fda613d](fda613d6dae322bfd296d6d8358de5eafc4791e6))
- Fix configDir and libraryPath verify fail ([34d949c](34d949ce9c4a0ea5347b9a3763672b64154df6c0))


### Enhancements

- *serve*: Improve data source parameters ([3c61a7c](3c61a7cad5743292ac5431ec65843a5a52aaaebd))
- *serve*: Improve data source parameters ([8c7552c](8c7552c4fff2039edb9454e9c135a3238a419526))
- *serve*: Add short_description field in params/groups ([35168ff](35168ff08e3c58528d1f3b9b33950df2c45b3e67))
- *serve*: Add short_description field in params/groups ([99714c8](99714c81b557bbe230533b37ba998e94af3bdba8))

- Support csv ([b4c6c22](b4c6c22d046981d8b0cebba1cb337df3986c6077))
- Support alter table from lush message ([b5b7f5f](b5b7f5f821207992f8f12d76cf8874696bb07acb))


### Features

- *serve*: Add csv data source ([7f24d11](7f24d11a1e4625056ea0f5575ea4f8b212a54069))

- Add get filemeat api ([aef45ae](aef45ae4a7c5fb9fc00a409e59b88205e0789210))
- Add parameters for influxdb. #[TD-25032](https://jira.taosdata.com:18080/browse/TD-25032) ([00daf37](00daf373499bb912f9e090aee7cc24a2955a1e9c))
- Verify taos dsn in legacy mode strictly ([33d8a3d](33d8a3decfa15f4a1b2fc72c6cc6de250d21e9b8))
- Verify taos dsn in legacy mode strictly ([db9ac98](db9ac986c771b98dc4770a712ce371dd6b134363))
- Add dataset api for influxdb. #[TD-25032](https://jira.taosdata.com:18080/browse/TD-25032) ([a62263f](a62263fff4ee5110209763fbd438c1bf5214ffeb))


### Refactor


- Support read certificate/private_key from file ([0d306f7](0d306f74c442c9e39f7fe08581683a6ea56f5e00))


## [1.0.1] - 2023-07-05

**Full Changelog**: [v1.0.0...v1.0.1](https://github.com/taosdata/taosx/compare/v1.0.0...v1.0.1)

### Bug Fixes

- *legacy*: Websocket closed when syncing will cause channel closed ([2a40af3](2a40af31242a18bd67caaaae206641ba520d55cd))
- *legacy*: Fix channel close on websocket ([e613dfc](e613dfc5f643608959350137945dd60bd42636ab))

- Reinstall warning and readme ([82b3b8c](82b3b8ce2eea3be4f2a5e3f8c7a0976f0640ce4f))
- Config path on windows ([5168a84](5168a84e406a6eb2b3cf49c9c1ba2f305274a28b))
- Replace config path to "taosX" ([094fdd5](094fdd5e325d3794aa6263ff17663e1d4865cf58))
- Redo ([6693560](66935608ab10cab4ace22997d9831624c086158a))
- Update path ([e345617](e3456172fb24d432beb2ac9a714961b703ce13c1))
- Fix build script in opc makefile ([7849e9a](7849e9ad67099d8b9e7b4f90216772d69e17b621))
- Uninstall taosx from control panel ([c285831](c28583108f60c93c6ece949bba1b0391597a0ad6))
- Fix [TD-24903](https://jira.taosdata.com:18080/browse/TD-24903) opcua regex not work ([280e40c](280e40c95fd3b9441fc49b76c0607c085d2291cd))
- Fix [TD-24918](https://jira.taosdata.com:18080/browse/TD-24918) and [TD-24919](https://jira.taosdata.com:18080/browse/TD-24919) ([c947f53](c947f5355ca849c75abdc46f5bbea99c5afd98ff))
- Cluster-id not required in cloud ([4a61688](4a616881c9365098dc993bd1d750350b854c9fd7))
- Git config safe.directory after checkout ([15b20dd](15b20dd3c604839f98418a1438ab992c81dc4d57))
- Install git before exec checkout ([000a9ac](000a9aca7305f513df68b79118fce6837ebbd263))
- Update taos to v0.8.13 ([97cf14b](97cf14b0778586ff45e140e1720a4e27e138a47e))
- Update taos to v0.8.13 ([e2ccd48](e2ccd485f43dbfcbbb161cacf7cf22b2ae01a03e))
- Fix bug in calculating offset. #[TD-24990](https://jira.taosdata.com:18080/browse/TD-24990) ([7d11459](7d1145986bdf8c6e29eb6caced96bf73ec707881))
- Fix bug in calculating offset. #[TD-24990](https://jira.taosdata.com:18080/browse/TD-24990) ([6b2b80b](6b2b80bd83d571b3bdd83e3785381b76d77c9b1f))
- Do not track transfer metrics if cluster id not valid ([6e175af](6e175af78dd950adf87c93150a48c5c3a797aa2b))
- Do not track transfer metrics if cluster id not valid ([98f0727](98f0727a2e792ad583a1bd8c748a5d4976f1bf21))
- Endtime is later than now. ([f48c1ad](f48c1ad700ea55a4c253a60c9595786365871473))
- Specifiy tag name when use insert into using ([092cdb8](092cdb8fd2c91c5edeb7b3b1070d8f6d072b40c4))
- Exit after completing the endTime. #[TD-25038](https://jira.taosdata.com:18080/browse/TD-25038) ([2ca1a8e](2ca1a8ed176ea395c5f6b1572800c5d1b4ba2bf9))
- [TD-25061](https://jira.taosdata.com:18080/browse/TD-25061)/[TD-25040](https://jira.taosdata.com:18080/browse/TD-25040) fix index error ([05bb6a7](05bb6a7cef13b4a5f8afc841d0545d693010c8a0))
- [TD-25061](https://jira.taosdata.com:18080/browse/TD-25061)/[TD-25040](https://jira.taosdata.com:18080/browse/TD-25040) fix index error ([96a5327](96a532786967af194e051079f054c45ed663c336))
- Specifiy tag name when use insert into using ([aa5619e](aa5619e1c923213a30b30a40cb7d4d0d1d2a4d2d))


### Enhancements

- *mqtt*: Reduce CPU usage ([c08fa4c](c08fa4c7323bb416fb1393c14c314e0f53166894))
- *mqtt*: Reduce CPU usage ([4470d59](4470d59fca0b9fc6ee710dc9de62b6110a8b736f))

- Regex in getting points supports matching id ([5d4ad07](5d4ad078eec89ce5c3d41ba59e91fdc5a99e3b74))
- [TD-24921](https://jira.taosdata.com:18080/browse/TD-24921) "code" desc ([89500bf](89500bf29e7d96826db9cc42d9105e6a846b49a2))
- Regex extractor ([55c926a](55c926a4f38ec26a71ed4c6ee6b448df9a3b33ed))
- Refactor lush message batch insert ([a781dd1](a781dd16303f8c1c0d0269320dd6c3f0a9b2eb65))
- [TD-24875](https://jira.taosdata.com:18080/browse/TD-24875) support update task name ([8e112b2](8e112b27acdc919b7f0131f89fd43ef7f1dcaa0f))
- Refactor lush message batch insert ([8083254](80832545b7be094f7c308882b0ff2edcb54a78f3))


### Features


- Process new added measurements. #[TD-24399](https://jira.taosdata.com:18080/browse/TD-24399) ([63e587a](63e587a3be02d0d5216a53f42e5139c26d039d59))
- Support configuration of fixed measurements. #[TD-24398](https://jira.taosdata.com:18080/browse/TD-24398) ([8b86937](8b869370ce951cfe412992c0a7f70c071bc20ca0))
- Support upload files ([9878342](9878342ec880fd7f6324e2c8284fd4a9dcb78589))
- Exit after completing the endTime. #[TD-25038](https://jira.taosdata.com:18080/browse/TD-25038) ([676d989](676d989f25533a2da325eb27eb801c07d02c96cb))
- Support for multiple versions of Influxdb. #[TD-24318](https://jira.taosdata.com:18080/browse/TD-24318) ([83ac428](83ac42837a61b6fd4f0ba9ab6a92aa258f635447))


### Refactor


- Opc config add points config ([9fe2f7b](9fe2f7b10f1aa1764cc21423535690be7f7cac99))
- Remove release temporily ([36e5b43](36e5b43e1974304e3bf77ab7cbc0e568a81e31b8))
- Add print ([ae15ea2](ae15ea239a8d479bd15eac4d3439226723935958))
- Modify git config exec location ([c0d2402](c0d2402f68c95faed98032a100d95c6865742f7c))
- Modify safe.directory set ([b306542](b306542dde4f6672f293710ea62058637a23d823))
- Return releative path after uploaded ([2be3ce5](2be3ce5f9c194001dbb1a3a9c3b7fc9df1934a51))
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
- Modfiy lush message insert ([950fa55](950fa5594d4c95295a7109487067b4b9ed8f879a))
- Modfiy log level ([a15643e](a15643ea2b9c394bc8a1fd48a137ec7676b956bd))
- Modify log print ([cdf1aa1](cdf1aa1b7eb44a4e17be3a0a4c3d78a99e15943c))


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


