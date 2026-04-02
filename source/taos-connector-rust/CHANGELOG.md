# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [taos-v0.10.1] - 2023-08-17

**Full Changelog**: [taos-v0.9.5...taos-v0.10.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.5...taos-v0.10.1)

### Bug Fixes


- Fix IsData trait impls for taos-sys ([4a764a3](4a764a398b8f5929084a165f457c44e752ee6a1a))


### Features


- Use async bind in async prelude ([3f9dd98](3f9dd988d01bcde4ae47313bfb9fdbe72b05c48e))


## [taos-v0.9.5] - 2023-08-16

**Full Changelog**: [taos-v0.9.4...taos-v0.9.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.4...taos-v0.9.5)

### Bug Fixes


- Now we support single-thread tokio runtime ([c89bfa8](c89bfa821a821cecb6e840a9b4038c3c93af619f))
- Use block_in_place under multi-thread runtime only ([b617d28](b617d285f68a9059d37080fef4002d8e8d0aac49))


### Enhancements


- Ws_affected_rows64, ws_stmt_set_sub_tbname ([881f471](881f4716f587be740a4c0b80d549bcb5ff49ea53))
- Ws_affected_rows64 ([681d84a](681d84a8d55a613d2de0dc27c44ef8d2a5fba75f))
- Add affected_rows_once ([c3a9ac1](c3a9ac19d3ddfd1e08b1dbabbac48590278bacce))


### Features


- Add is_native/is_ws in taos::Taos ([dbf8c0b](dbf8c0b3aec1ec51d38d01bf103f2d5cf9839fd1))


### Testing


- Add case for ws_stmt_set_sub_tbname ([4f02b51](4f02b51a836ab7ada54e010afa6843e85ab42285))
- Add case affected_row ([90698bc](90698bc80ffc8336843198a7adc733fe61c77821))
- Update case affected_row ([d582daa](d582daa77f09f506dc78eac9d62b0cf8f3d6dfaf))


### Dev


- Add query benchmark with critierion ([568a150](568a150d45ad9b1558d063aa9fc7953c8a2d9cf8))


## [taos-v0.9.4] - 2023-08-09

**Full Changelog**: [taos-v0.9.3...taos-v0.9.4](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.3...taos-v0.9.4)

### Bug Fixes

- *ws*: Fix some panics in websocket impls ([142a9e9](142a9e9e8806cd3425568179d0afbe0c23ea2487))



### Features


- Use default rustls for taos-ws-sys ([52e9b18](52e9b186a39cd13e15462b46621f89fbb40c148c))


## [taos-v0.9.3] - 2023-08-03

**Full Changelog**: [taos-v0.9.2...taos-v0.9.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.2...taos-v0.9.3)

### Bug Fixes

- *mdsn*: Fix unix path with spaces ([d5aa25f](d5aa25f3dd3ff07b29d90fa9b4d281739eb549ce))



### Documentation


- Add example code ([0cb8868](0cb886841f68c4af8c862c4c26a85a7c3be3cc29))
- Add example code: basic ([01788f9](01788f96a5d21d2faacd18636f977f3302af3b19))
- Add example code: stmt bind ([1dcf95b](1dcf95bd1ca8c4b3b85f5168d7718afc12ddc0f3))
- Add example code: stmt muti bind ([ed81b2d](ed81b2d8228b04886b30846847c900a5511f316a))
- Add example code: stmt muti bind ([c9b64fd](c9b64fd7b5e8565ef25ec705fe0509f6d7652422))
- Add ws-table-muti-bind ([69d4054](69d4054c42ac37de52f05561e3440434e70af833))
- Add example of get err string ([56400af](56400af2c3ba3ddfce6c128dbd41b80575e30a2f))


## [taos-v0.9.2] - 2023-07-27

**Full Changelog**: [taos-v0.9.1...taos-v0.9.2](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.1...taos-v0.9.2)

### Bug Fixes

- *sys*: Fix nchar display error ([2c291f1](2c291f1ceed52f546e08dad5d3efabc8caaf8cfb))
- *ws-sys*: Fix nchar to str ([00c465c](00c465c939717060a288cd8c59f563429f3fc35f))

- Reclaim vec item ([6ad6f81](6ad6f811db820be19e4286eb34396d4a259311d1))
- Fix csv dsn parse error when use unix glob ([f49a0e5](f49a0e595309a5bd5a4fc12c632f48a6bbeeacd8))


### Documentation


- Update git cliff config and CHANGELOG.md ([c8848c9](c8848c9b87febe0b4f3cab044f310c968d815052))


### Enhancements


- Add ws_stmt_reclaim_fields ([cb52f6e](cb52f6e9a38e47051387f586b87a5629a6554f06))
- Add error handler ([9e18dfa](9e18dfaafec4e435e99db125d737b9cc0fefe318))


### Features


- Add api get_col_fields and get_tag_fields ([83c0242](83c0242a080e9ef3b5b9f0d20e8581c7f3a1d9ae))
- Add c api stmt get fields ([552749d](552749d87053d69ea53d14e0ce657d05ef6b355d))
- Update C struct ([fdebcd7](fdebcd7767779da546596e207b4829a7e20596cf))


### Testing


- Add case for taos-ws-sys ([0624b6f](0624b6fdcc889b4c442cb164276e1d7ff8322a7f))
- Update case ([f1db56b](f1db56bf747ddb2028c719c9876685b201c9d965))
- Update case ([bb6f1d1](bb6f1d1f145763e82d29b1a447c00c3582e9e38d))


## [taos-v0.9.1] - 2023-07-18

**Full Changelog**: [taos-v0.9.0...taos-v0.9.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.9.0...taos-v0.9.1)

### Bug Fixes


- Table does not exist in enterprise checking #[TD-25253](https://jira.taosdata.com:18080/browse/TD-25253) ([316945f](316945fb7b261e3cec24f1b148fb42ebe0a4e8ad))


## [taos-v0.9.0] - 2023-07-12

**Full Changelog**: [taos-v0.8.15...taos-v0.9.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.15...taos-v0.9.0)

### Bug Fixes

- *optin*: Safe way to fetch raw block in both v2/v3 ([931a3f5](931a3f5cda145919f54b394d756388f66931f49a))

- Annotate of ws_get_server_info ([1d05e7b](1d05e7b1ad9e103791d055aaa1d8d790d4128861))
- Annotate of ws_get_server_info ([295cf45](295cf4535761e0e0d2d21b13508910bd7682b08e))
- Drop data in WsMaybeError ([db84de5](db84de5271bce8ec7d96ec3562748af958341cd9))
- Remove read ([4a4cdac](4a4cdac2ea020ce3ab834d4da8778ef95b0103f3))
- Drop data in WsMaybeError ([ced57b9](ced57b963f24d317567cee2425831af053842cf8))
- Remove read ([a40f0a8](a40f0a81e3ff9c146b7f875ce548a76848197bb2))
- Fix unexpected error string ([3e79cf1](3e79cf1db41ad034ad97b28c5fecaa6872fb253d))


### Enhancements


- Improve query errors ([2f4d84b](2f4d84b5f5d81f3a5055c890c9cb0c481b5dcbaf))


### Features


- Raise backtrace with taosc error codes ([2637b39](2637b39b4ab1269b543c6ee7203eca445b3c9bf4))


### Testing


- Add benchmark ([ee9a867](ee9a8675e2fef06501fea8e025a4bf798d5992cb))
- Rename bench file name ([c94e70c](c94e70cfba4cba35ec518da5d8881cdcd89bf218))
- Update case ([a610057](a610057ee627029443a8827a713c39aeffa2438b))
- Add bench reports ([9ce79e2](9ce79e2c6f1416e070ac75fcf2c3d928b87fd874))


## [taos-v0.8.15] - 2023-06-28

**Full Changelog**: [taos-v0.8.14...taos-v0.8.15](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.14...taos-v0.8.15)

### Bug Fixes


- Cloud as enterprise edition ([f93c339](f93c339f53498ee66573a010429c1dfd0470f8e9))


## [taos-v0.8.14] - 2023-06-27

**Full Changelog**: [taos-v0.8.13...taos-v0.8.14](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.13...taos-v0.8.14)

### Bug Fixes


- Treat cloud as enterprise ([68bdda9](68bdda9ec22d3cbd1ae1057d4dddf2e64577fd8a))


## [taos-v0.8.13] - 2023-06-27

**Full Changelog**: [taos-v0.8.12...taos-v0.8.13](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.12...taos-v0.8.13)

### Bug Fixes


- Cloud connection 400 bad request error ([6281978](6281978ca7933ec4965dd74ecdb958fe1185d00b))


## [taos-v0.8.12] - 2023-06-21

**Full Changelog**: [taos-v0.8.11...taos-v0.8.12](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.11...taos-v0.8.12)

### Bug Fixes


- Update free result ([36a7b41](36a7b41fcbe5e289dba6b54fb7ade603e373b857))
- Add thread scope ([5b37e63](5b37e63f51ee65470f87294ade500bd97178c438))
- Retry connect when 0x000B error ([5a885ab](5a885abb7c2160ec124d745a33ebfd9c859e8c0a))


### Enhancements


- Add sync implement for put schemaless ([5a57e5d](5a57e5d5ec05c5879d8da49ec2bf21d1ecdeff46))


## [taos-v0.8.11] - 2023-06-13

**Full Changelog**: [taos-v0.8.10...taos-v0.8.11](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.10...taos-v0.8.11)

### Bug Fixes

- *features*: `Offset` for `native` flag should be labelled as `taos_sys::tmq::Offset` ([1a1cc08](1a1cc080e141f5ab7e1a208aee44ef3aa0af37d1))



### Enhancements


- Add sync implement for put schemaless ([fe2ff37](fe2ff37c618b78b39ee263aa94bd16278595bf61))


### Features


- Add cast() method to ColumnView ([83d61c2](83d61c24760369b22eefbb12a6d104faefefa30e))


### Refactor


- There's no need for parsing the response of the tmq as error string here ([8a42a33](8a42a3307faf78b3683d3c126acdfe9d50b661c7))


### Build


- `private` is a deprecated attribute for cargo ([8c3de91](8c3de915fa6088b3ba4405fb77d965b74d9a7fc0))


## [taos-v0.8.10] - 2023-06-08

**Full Changelog**: [taos-v0.8.9...taos-v0.8.10](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.9...taos-v0.8.10)

### Bug Fixes


- Update log to debug ([1f69fe1](1f69fe14085753fa4bff2a3968e83d58380d79fc))
- Remove logs ([a53fe06](a53fe0619cff07782d1a8db24d17ef9c9bd42250))


## [taos-v0.8.9] - 2023-06-08

**Full Changelog**: [taos-v0.8.8...taos-v0.8.9](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.8...taos-v0.8.9)

### Bug Fixes


- Do not panic while ws retrun no assignment ([d6b1b86](d6b1b86086a5f959acb761f0cbd8bac99ec7ed5a))


### Enhancements


- Add assignment and seek to AsConsumer ([37e3f69](37e3f6910e1cceaa8ec1ac069749cc51b0251cf7))


### Testing


- Add case to verify Invalid parameters ([efa311c](efa311ce12568eeec7c8af30834537bf87ffce6c))
- Add sleep to wait create db ready ([64505f4](64505f41cf3f1c441e8cef07613193fe6d2930bb))
- Fix db name ([5a66d02](5a66d02acd585018f11384f301803c24265e30d8))
- Update db name ([f7cbfa8](f7cbfa834ec31f74dc67412b614a1ca080a85ada))
- Update case ([54a4848](54a4848071b6fd4e1a9a62ea750ee278b5b6a8be))


## [taos-v0.8.8] - 2023-06-01

**Full Changelog**: [taos-v0.8.5...taos-v0.8.8](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.5...taos-v0.8.8)

### Enhancements


- Dsn parse support param vlaue contains line break ([e17b0c1](e17b0c1999818c7a0ecea026b822021655c5f5b3))


### Refactor


- Upgrade mdsn version ([a3ba32c](a3ba32ce641183468c68ba30b1fa163099bfba52))


## [taos-v0.8.5] - 2023-05-30

**Full Changelog**: [taos-v0.8.4...taos-v0.8.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.4...taos-v0.8.5)

### Enhancements


- Seek_offset use dsn ([1f30982](1f30982b2ee05dec53612a5acb23f1e004ab4f3a))
- Columnview support from micros/nanos ([9987fac](9987fac455f2e3a55c4253ecdfca178c7ba19433))


## [taos-v0.8.4] - 2023-05-25

**Full Changelog**: [taos-v0.8.3...taos-v0.8.4](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.3...taos-v0.8.4)

### Bug Fixes


- [TD-24404](https://jira.taosdata.com:18080/browse/TD-24404) ([e8b3acf](e8b3acfc9f9d9aeab78752e095d70d0e0d062ea2))


### Enhancements


- Add messager for tmq ([a5c654b](a5c654b739314700d1a90f1d592d5384237cccdf))
- Ws impl assignments and seek_offset ([5f63949](5f639491fd06a2efeedecda39b75ff4c249d865b))


### Refactor


- Update Assignment args name ([cfc7e30](cfc7e300827c72d7fe3ecaa9fb45b52e02151e50))


### Testing


- Ws add case for assignments and seek offset ([972686c](972686c4d70ed42f9b9aa1877f6bf1bd7640e35f))
- Upate case of test_put ([626dbdd](626dbddc8076d4352b65fa1adaf96bcc09f0928e))
- Ws move args db from action insert to conn ([52aa82d](52aa82d032bcaa2836ded0554489baa512af0145))


## [taos-v0.8.3] - 2023-05-22

**Full Changelog**: [taos-v0.8.2...taos-v0.8.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.2...taos-v0.8.3)

### Bug Fixes


- Remove logger ([cdd1bda](cdd1bda6b3a676bea2091e75a92358fe6f9c3ce0))


### Enhancements


- Ws tmq unsubscribe ([17d1727](17d17273611770b9f1232cb13cc70a8adbe6495c))


### Features


- Ws add subscribe ([f873a1e](f873a1e1ad0532f10f4615837548b3f22ce4e3cc))


## [taos-v0.8.2] - 2023-05-17

**Full Changelog**: [taos-v0.8.1...taos-v0.8.2](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.1...taos-v0.8.2)

### Bug Fixes


- Connect error async ([b1a5a3f](b1a5a3f5f335fc885c1f68ee70a9d27f70633dca))
- Remove logger ([7f93ac5](7f93ac52828decad4647d281c8acd1f529e2002b))
- Remove logger ([18d0a4e](18d0a4e4bc60653a18b455561d8885c6cd0f3f59))
- Fix typo ([ee27625](ee2762559152695a908bbe07450dbcb06cd87092))
- Update tmq_get_topic_assignment with Option ([8cc4102](8cc4102973439bdc3c45bf59a1da542ae4b5f79b))
- Remove TSDB_DATA_TYPE_MAX in taosws ([895306f](895306fb649f81744c09b03a001b45f565360c12))


### Enhancements


- Add err detection ([123e8db](123e8dbef29efdb44239cc5fa4fb11df3de3134e))


### Features


- Get assignments and seek offset ([ee45fb8](ee45fb8c2ed18cbac5de3bb109aa83ed1d13379d))
- Taos-optin add assignments and offset seek ([db257c0](db257c0ba84f7f0bbe58795e9df7fc76d2c2a6d2))
- Add topic_assignment ([023e782](023e782a10af44ce93b4e890d16c2ad831a3d69a))
- Add block <-> views helper methods ([cbe6d53](cbe6d53a38ff0ad8f5d6939fb3d6cb1452dcdcfa))


### Testing


- Add case for asyn ws and native ([478371a](478371ae416471e92c0a8a5a793a494ddcf9901f))
- Update case ([d7e4ae3](d7e4ae3be0989a73e104a0312c6125b79641523f))
- Add case on taos-sys ([61e0634](61e0634de4757de6e72094d73d54d5d1417d13ca))
- Fix test error ([57775f5](57775f5c0de7e0f6e66f0bd883fd60e7696c3f8e))


## [taos-v0.8.1] - 2023-04-26

**Full Changelog**: [taos-v0.8.0...taos-v0.8.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.8.0...taos-v0.8.1)

### Bug Fixes

- *ws*: Fix get raw data in websocket ([18f6ec4](18f6ec4d251a67057f3fca0b21e12083446ca166))

- Is_enterprise_edition return result<bool> ([ce40d61](ce40d615cb41ea2803dc41f90a93713ccaeb71fa))


## [taos-v0.8.0] - 2023-04-24

**Full Changelog**: [taos-v0.7.6...taos-v0.8.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.6...taos-v0.8.0)

### Bug Fixes


- Taos_query_a_with_reqid should be optional ([08dd7dc](08dd7dc211c0235d8e79926a98ceb05cd29286f5))
- Fix test error for reqid ([3e6db89](3e6db89bed7b1ac720c6b621eea76be0143ea5b6))
- Fix memory leak for query and tmq ([794b3b5](794b3b5cd19061b80af5baf7a53de76fd210bf47))


### Enhancements


- Add fn sml put ([82b716a](82b716a4303464152a0b75247a08d0b51d62e17f))
- Support sml put taos-sys ([cc8a179](cc8a1797689e36f28a9a1258fe87f9e6745df11c))
- Support sml put taos-optin ([4724c93](4724c939abf2bcc5340afb4479ae0b8763938ee2))
- Remove db in sml struct ([7f6ce1f](7f6ce1f2ecd0b3767aee4094b27ba1a7b3d7c3bb))
- Specify db before insert ([096b13a](096b13a8bf47a7624733e70b189f2be68acd3f33))


## [taos-v0.7.6] - 2023-04-14

**Full Changelog**: [taos-v0.7.5...taos-v0.7.6](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.5...taos-v0.7.6)

### Bug Fixes


- Add args about wal to fix tmq test ([60006eb](60006eb71d44cb4e3b7a954c1dab0330cf0df728))
- Fix encoded username parsing error ([e3b922f](e3b922f020a7e69c9caa80421dbf38ce6e250444))


### Testing


- Add case ([1b6d278](1b6d27871ce3cce5377bd5f5a721bc3df74ac121))
- Update removelogger init ([fad433f](fad433f59ae24bd51c09e392ec4d97e79c08a767))
- Fix tmp case ([bc8d72f](bc8d72f1b315c474e7e382bd3152a23508717a81))
- Fix tmp ([9137d49](9137d4906c30cd333671f00b7eb7bfee600bcb4f))


## [taos-v0.7.5] - 2023-04-13

**Full Changelog**: [taos-v0.7.4...taos-v0.7.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.4...taos-v0.7.5)

### Bug Fixes


- Fix dead loop when table not exists ([aa89a02](aa89a0225ce9b015c434bed8bad13ad0407d7691))


## [taos-v0.7.4] - 2023-04-13

**Full Changelog**: [taos-v0.7.3...taos-v0.7.4](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.3...taos-v0.7.4)

### Enhancements


- Update sml data struct ([4c19d40](4c19d407d2f517986ea548c48c7e351a43921f90))
- Use trace level instead of error to avoid misleading ([e167b95](e167b950b821ad5a764b5daee26c3d1aed72e23d))


## [taos-v0.7.3] - 2023-04-13

**Full Changelog**: [taos-v0.7.2...taos-v0.7.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.2...taos-v0.7.3)

### Bug Fixes


- Fix ws_errno on NULL ptr ([974b67d](974b67d3870e316a01407863033a58b7bd316da4))
- Fix windows error when call json_meta ([f5c0870](f5c0870aa849aed3d8b1057543062b8aa0cd9c48))


### Enhancements


- Support schemaless ws ([8a765b0](8a765b06435ce9e6971e8f739a8914310ab28de8))
- Add props in sml data ([11de27b](11de27b9fe9acca36ff5f7af307b0518dfe7009e))


## [taos-v0.7.2] - 2023-04-10

**Full Changelog**: [taos-v0.7.1...taos-v0.7.2](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.1...taos-v0.7.2)

### Bug Fixes


- Fix windows ptr reading error while query failed ([90a3846](90a3846d6a5544053e370f56c96018097bcbfa88))


### Enhancements


- Add entry ([2b62c45](2b62c45a81f8fdaf5192938ef45889071bd4028b))
- Update todo ([4d413a4](4d413a4a3b8d762f98da73d9adb3f83e69e95c2f))
- Update todo ([bf81004](bf81004cec3333332a288b7adbf59cb9b12f3671))
- Update signature with Option<> ([2481c8c](2481c8cffabf3fcdb6d98ecbdcb0505184a3ae19))
- Update signature with #[cfg(taos_v3)] ([bfe3892](bfe3892375009567802f69b5f63af708d26865b5))
- Update signature with #[cfg(taos_req_id)] ([a4e8bab](a4e8babe7439a42379a37e57b35ffca17c669441))


### Testing


- Add case test req_id for ws ([8246f32](8246f32c6c9f23323ebdf361d6cb064dc791d6a1))
- Update env var name ([5f0a438](5f0a438cd1aa789eb28e4165e77c1c0714adab04))
- Update dns with env variable ([05b5b90](05b5b90ebbf25f327f804afbcb1ffcd709d3a6b2))
- Add cases with req id ([9e2afce](9e2afceb13df5cc655d1448d7b5b97768d1beae6))
- Update env ([ffbe4c7](ffbe4c7a0001642ba440428ab9e007817bb41901))
- Add cases with req id ([0280dc4](0280dc4f5e95f34ab4c0f5c54936430b509bf00f))
- Add cases to improve coverage ([77ead4f](77ead4f68d857b150acb6d42c4d22e25ba45cd19))


### End


- Add query_with_req_id for native ([bb642bc](bb642bca8e03fadb4cdbfcf02f9743b459c5cc91))
- Add query_with_req_id for native, optin package ([55588e2](55588e2b87ead4a16df53fa4bbb1b2ac7737a428))


## [taos-v0.7.1] - 2023-04-03

**Full Changelog**: [taos-v0.7.0...taos-v0.7.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.7.0...taos-v0.7.1)

### Bug Fixes


- Fix unaligned ptr reader ([76df290](76df290e5800cc7612c70617f05f9dc48dc05754))
- Fix docs building error in docs.rs ([f8af219](f8af219d32f718ee824db4b37596a6c5e91aa1d9))


## [taos-v0.7.0] - 2023-04-03

**Full Changelog**: [taos-v0.6.0...taos-v0.7.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.6.0...taos-v0.7.0)

### Bug Fixes

- *option*: Do not check errors for enable.heartbeat.background ([0f717a8](0f717a849903d887d2bcad6f6b90972049dd2546))

- Update mdsn to v0.2.10 ([1d358b5](1d358b5f9dab63124a127c11d8bcc34f8c441f23))
- Fix tmq config options ([01b6854](01b68548e8931c87fa5608ba24bd3547b32d75d1))


### Documentation

- *mdsn*: Add homepage/documentation/repository info to Cargo.toml ([db9513c](db9513cf7ae52a44b98e6bb9c1d3a09e4c2b87af))



## [taos-v0.5.12] - 2023-03-25

**Full Changelog**: [taos-v0.5.11...taos-v0.5.12](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.11...taos-v0.5.12)

### Bug Fixes


- Add ColumnView::from_json ([baee5d5](baee5d5bfbabcd3b117c8e34a920869c5fe38050))


## [taos-v0.5.10] - 2023-03-23

**Full Changelog**: [taos-v0.5.9...taos-v0.5.10](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.9...taos-v0.5.10)

### Enhancements


- Support req_id for ws ([78caa19](78caa19bbf1a5f358c921252d0a51116af9c5164))
- Support req_id for ws ([41941a2](41941a286e8e312cc0a474bb80fba781c35c993b))
- Support req_id for ws ([643992e](643992ea508908b3568992a409bf1385a78e813f))


### Features


- Add pool_builder for TBuiler trait ([8b7e77f](8b7e77f9571f5c60fdb5a63caa75a11b257cc1f3))


## [taos-v0.5.9] - 2023-03-19

**Full Changelog**: [taos-v0.5.8...taos-v0.5.9](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.8...taos-v0.5.9)

### Bug Fixes


- Fix column view concat ([80486e1](80486e1d8e359c5f23be93ab01efca48f7426c20))


## [taos-v0.5.8] - 2023-03-18

**Full Changelog**: [taos-v0.5.7...taos-v0.5.8](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.7...taos-v0.5.8)

### Bug Fixes


- Fix error for block stream ([47e84d4](47e84d41c1765982f7f4f4653499d27dbc110556))
- Use a smaller pool size as default ([4712a91](4712a9138ed3ccd88b372d23bce5d78c999e6d7b))


### Features


- Concat two column views with Add operator or .concat method ([094b8b6](094b8b6b28ff16fb33f38800982502fe04e809ae))


## [taos-v0.5.7] - 2023-03-17

**Full Changelog**: [taos-v0.5.5...taos-v0.5.7](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.5...taos-v0.5.7)

### Bug Fixes


- Fix != error in taosx ([c93422f](c93422f1c9a0163a81249710c2ff3ca9f1d0f27d))
- Fix in use fetch_rows_a for v2 ([0884f7f](0884f7f4c6f2292231bd552478d28af045279f68))


## [taos-v0.5.5] - 2023-03-16

**Full Changelog**: [taos-v0.5.4...taos-v0.5.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.4...taos-v0.5.5)

### Features


- Support PartialEq for borrowed and owned value ([661e0b8](661e0b8cc8dcd52017211e47c0d539551f7ddbb3))


## [taos-v0.5.3] - 2023-03-16

**Full Changelog**: [taos-v0.5.1...taos-v0.5.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.1...taos-v0.5.3)

### Bug Fixes

- *sys*: Parse version with 5-fragments like 3.0.3.0.1 ([2bb63dc](2bb63dc2212a63da7e37ab0da3f811b17891496b))

- Fix 0x032C error ([fd48d6d](fd48d6d35969d59a3e5b1aba29e06f73447a9892))
- Fix double free for stmt ([28cd48a](28cd48a20e0803074ac4e026f85661c56bda6b84))
- Santizer detect unaligned memory ([33c7f50](33c7f502a5972a642c7d3be8caf11ccf0d3f6a15))


### Enhancements


- Error logging in debug level for code handling ([aeedb0f](aeedb0f108d5b970f5e6273e3a2a99e758332a21))


## [taos-v0.5.1] - 2023-03-09

**Full Changelog**: [taos-v0.5.0...taos-v0.5.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.5.0...taos-v0.5.1)

### Bug Fixes

- *optin*: Fix metadata unimplemented ([28363ec](28363ec9b59d00dd352f4ed9ddeee61e1ea16f53))



## [taos-v0.5.0] - 2023-03-08

**Full Changelog**: [taos-v0.4.18...taos-v0.5.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.18...taos-v0.5.0)

### Bug Fixes

- *optin*: Fix memory leak in stmt bind ([29877ea](29877eabe54b484d22f0e827a1a9c94b080736c7))
- *pool*: Set connection timeout default to 5s ([7ece76f](7ece76fd29c4134e2954d5ea21ac4c547b873aa1))
- *py*: Fix token parsing for cloud ([0fa1922](0fa192270889eb2b0c1ffb919f09f751d8637f55))
- *py*: Fix query error with cloud token ([83ebaf0](83ebaf0166258d7617ce751d5c17dfa674605560))
- *sys*: Fix async runtime error under v2 ([b318e75](b318e755d54ec5d4d0aa9d1a434be4a6394824e3))
- *sys*: Fix memory leak in stmt bind ([1765c99](1765c9962450218beda339b0f15efa5edf8a3854))
- *sys*: Remove log::debug! macro in query future ([dc6b31f](dc6b31f48a2eb4d3df1e5f472e4081a3bc4671bd))
- *ws*: Unimpleted type when message_type = 3 ([a4f6141](a4f614182e31f17474cbb326727156ebf759b320))

- Fix unaligned pointer to support sanitizer build ([c5077d2](c5077d27c45db59fd392761c73091d3f00cbb6a3))
- Stmt mem leak for binary/nchar bind ([2810106](281010641a1634e4e8627b08d815fdccbfdc0c2f))
- Retry write meta when got code 0x2603 ([f16da13](f16da134e74ab3bf1681b110cccedabb762fab7d))
- Fix test error ([444be97](444be97b4103316f79d1d2a2e232f54ca2d01870))
- Impl to_string for bool type ([c2f2119](c2f2119f3adbae03e2de034a0297c76a39ba4898))
- Fix taos_query_a called twice ([78c7f22](78c7f22d0f141cb695f71b8c27f1dc7b8dbe0621))
- Optin server edition check ([3608eaf](3608eaf56c3b6e2f471580b28e45bfd94adda511))
- Fix macos compile error ([c4b6066](c4b606629c910687e14dfdef36e0d4813fdcb633))
- Fix edition version pattern (cloud) ([b20380e](b20380e5b187a36c723f6fb7bd7195d9ddb8c964))
- Macos compile error in optin feature ([97e6084](97e60844fec592bf93068529003685d97d0ee834))
- Fix async block called twice ([2f2ffce](2f2ffce6021a358069f4fd06eab0285c56ceacb6))
- Force Send/Sync for column view ([39adc7e](39adc7e2b5e7e966158ebaf9048b05917466982f))
- Fix double free when execute set_tag before bind (#142) ([354fd26](354fd2615fc963ef53b662ffab1d25c5d268b982))
- Fix warnings and syntax error after 3.0.3.0 ([59e4d5a](59e4d5a77ea4fb93c9e7024b5286536f89318a1f))


### Enhancements


- Use mimimum features set of tokio instead of full ([915964b](915964b5edc159fcc274b8a5640f86bae6b58e67))


### Features


- Use rustls with native roots by default ([f04e029](f04e029814110c4ec96e9424706a63553b631323))
- Do not move metga when use write_raw_meta. ([c00f14a](c00f14ac981b97b2c8583480f9ee4ca8a2e2c6e8))
- Pretty print block with prettytable-rs ([bd3c725](bd3c725f991f9939df30bf3594998c8644dc0b61))
- Expose taos-query at root of taos ([b05d76c](b05d76c81dd0d1c0f34aaf2980247d2f441dfce6))
- Add sesrver_version() and is_enterprise_edition() ([1b3e6fc](1b3e6fc7e1cb54dc60925a226b254099da8d54ed))


## [taos-v0.4.18] - 2023-01-08

**Full Changelog**: [taos-v0.4.17...taos-v0.4.18](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.17...taos-v0.4.18)

### Bug Fixes


- Fix create topic error ([4dc6862](4dc6862cf31775a0d41aa01c4f50046da67dd485))
- Fix create topic error for older version ([aadc3df](aadc3df3bcf50941e5fdb62150f9f4be86e31b83))
- Fix slice of null bits error ([391e00d](391e00d0748726af3e6ea78496896db7f7ac38b7))
- Add a macro to prepare fixing typo ([8e237bd](8e237bd569c19b99fca680173cdf93e8e3508f08))


### Features

- *taos-ws-py*: Support multiple python version by abc3-py37 ([ea7dcb5](ea7dcb5effe38b49b51709b2629952f6c2f5455c))

- Taosws PEP-249 fully support ([f078346](f07834603d2e51f9e730ef85d890bf6251a9e9f6))
- Support column view slice ([82a1745](82a1745bfb35700b9e6f9ef73148ccdfd2346d36))


### Testing


- Fix sql syntax error ([473a3e0](473a3e096d33e4a2ab8016fed406f92712f3210a))


### Dep


- Taos-query v0.3.13 ([cf19c8c](cf19c8c5c24ab5ad6e249c759f3942a21bd319f4))
- Update taos-optin v0.1.10 ([9c71cb7](9c71cb70cbadd185494a021b1faee403fa3c7937))
- Use taos-ws v0.3.17 ([75702b5](75702b5e391ef73e3b8e66dc9a3865b83ef2446e))


## [taos-v0.4.17] - 2022-12-24

**Full Changelog**: [taos-v0.4.16...taos-v0.4.17](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.16...taos-v0.4.17)

### Bug Fixes


- Fix complie errors in sanitizer mode ([6564e3d](6564e3dc82e2302128b4a3bbd322aa1fa9447dda))
- Fix tmq_free_raw lost that causes mem leak ([047486e](047486ee6ca103397d93c902fe066e2d40c3f60e))
- Fix mem leak in taos_query_a callback ([87a1ed2](87a1ed27c70e021272220700b68a01f80d351567))
- Fix DSN parsing with localhost and specified port ([83431f4](83431f45f82c33958dad4d7d7885f1353b3f152c))
- Default max pool size to 5000 ([9a7dff9](9a7dff9fd665f134e7e56a12391e913cad9acf38))
- Fix memory leak when query error ([c13b692](c13b69248e71a3be55eb8914f3ed62ae423b0445))


### Enhancements


- Support delete meta in json ([8e1d4f3](8e1d4f33995a45a331f23291137f9e4e4863a00c))


### Features


- Support write_raw_block_with_fields ([0ac7f8b](0ac7f8bfd052a240147d3411a58b98de8ae41a21))
- Add server_version method for connection object ([51dea9f](51dea9f84defca31844084d737efa49e1e874eca))


### Examples


- Add example for database creation ([f07734c](f07734c320e633e9aa3a62e6d5b4cee36c295c72))


## [taos-v0.4.16] - 2022-12-10

**Full Changelog**: [taos-v0.4.15...taos-v0.4.16](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.15...taos-v0.4.16)

### Bug Fixes


- Fix test error in CI ([e7ed2f0](e7ed2f074231af2bc664d1e93f02b618df6865c1))
- Support api changes since 3.0.2.0 ([7435b54](7435b543b6ab4a16eb6e44f9fdb40a2f1c4c381d))


## [taos-v0.4.15] - 2022-12-09

**Full Changelog**: [taos-v0.4.14...taos-v0.4.15](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.14...taos-v0.4.15)

### Bug Fixes


- Support write_raw_block_with_fields action ([4a8ccd5](4a8ccd5f215f24dfd78d19c5d59e80c66edb7ce4))


## [taos-v0.4.14] - 2022-12-09

**Full Changelog**: [taos-v0.4.13...taos-v0.4.14](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.13...taos-v0.4.14)

### Bug Fixes


- Use dlopen2 instead of libloading ([57c66b4](57c66b40a2b61fd0ffb1731a0e70137c61038d08))


## [taos-v0.4.13] - 2022-12-09

**Full Changelog**: [taos-v0.4.12...taos-v0.4.13](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.12...taos-v0.4.13)

### Bug Fixes


- Fix r2d2 timedout error with default pool options ([f6da615](f6da615abb2d0017b1b81474e6a736ef509391ca))
- Use spawn_blocking for tmq poll async ([d48d095](d48d0952e6d6002c9977679ec51952b7956b5331))


## [taos-v0.4.12] - 2022-12-06

**Full Changelog**: [taos-v0.4.11...taos-v0.4.12](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.11...taos-v0.4.12)

### Bug Fixes


- Produce exlicit error for write_raw_meta in 2.x ([d6dd846](d6dd84613cf1d78781cc144ba2f0f5a48579c2e3))


### Features

- *mdsn*: Support percent encoding of password ([137eecf](137eecf03e1a9ad1e2dfa93251e0ae64fc4bb58f))



## [taos-v0.4.11] - 2022-12-01

**Full Changelog**: [taos-v0.4.10...taos-v0.4.11](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.10...taos-v0.4.11)

### Bug Fixes

- *sys*: Fix compile error when _raw_block_with_fields not avalible ([3c65d97](3c65d974ee08f6ecba728295ec79399980efdc6f))

- Support `?key`-like params ([28e61a4](28e61a401996669d89150a228842727ffcf532d4))
- Support special database names in use_database method ([633e43a](633e43a647ce1b93aa3a6b1a8ad42fb240287fb2))
- Fix coredump when optin dropped ([7c6ef10](7c6ef101df52bb3c17c50adc40da479d9e55c9c6))
- Support utf-8 table names in describe ([4f0f3f9](4f0f3f906f5c538fd82604788bf2d6e09bb51055))


### Features


- Expose TaosPool type alias for r2d2 pool ([5a05049](5a05049bea93d2866870dbd96e3969a145026248))


## [taos-v0.4.10] - 2022-11-29

**Full Changelog**: [taos-v0.4.9...taos-v0.4.10](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.9...taos-v0.4.10)

### Bug Fixes


- Use from_timestamp_opt for the deprecation warning of chrono ([711e0f8](711e0f8fc73bf3b741781c4e113ec2ccb4c5434f))
- Use new write_raw_block_with_fields API if avaliable ([1efbb66](1efbb6627c0be0cbddb4baad0aace199ecfb2c87))


### Enhancements


- Expose fields method to public for RawBlock ([e5ae526](e5ae526f8f5baaff9eb8274bd55efa2e38de5b24))


## [taos-v0.4.9] - 2022-11-22

**Full Changelog**: [taos-v0.4.8...taos-v0.4.9](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.8...taos-v0.4.9)

### Bug Fixes

- *ws*: Fix websocket handout in release build ([9e9635d](9e9635dd91fb118cbc03123a3c80d590bb8bc9fd))

- Remove simd-json for mips64 compatibility ([6f61d9c](6f61d9cfd6c51ee77aa5fddd62d7bc12fcc43df6))


### Enhancements

- *ws*: Add features `rustls`/`native-tls` for ssl libs ([8e01a72](8e01a72cee1ea10cdfcb46e325ecc784955671a2))



### Testing


- Add test case for [TS-2035](https://jira.taosdata.com:18080/browse/TS-2035) ([2914d0a](2914d0a902ffe8b89bcf44666ce168daba2d37cb))
- Add test case for partial update block ([622f052](622f05205eb0bcb42ce595df50f58e10830175e7))


## [taos-v0.4.8] - 2022-11-11

**Full Changelog**: [taos-v0.4.7...taos-v0.4.8](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.7...taos-v0.4.8)

### Bug Fixes

- *ws*: Fix error catching for query failed ([4065780](4065780b0fc67b1d7f20576e34df51204eb01ebf))
- *ws*: Fix call blocking error in async runtime ([61ea7f0](61ea7f0c83cd017b62aee426db93a6dbd2df1f14))
- *ws*: Fix send error ([2276b20](2276b202027469088e21f4085a81cf2eafa01a8a))

- Use oneshot sender for async drop impls ([006e40a](006e40ab535ab51925bd361b24b8ced20781e906))


### Features


- DsnError::InvalidAddresses error changed ([5e1a4f1](5e1a4f16fbb7da7c673b595dbe3e0791ffa26317))


## [taos-v0.4.7] - 2022-11-02

**Full Changelog**: [taos-v0.4.6...taos-v0.4.7](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.6...taos-v0.4.7)

### Bug Fixes

- *ws*: Broadcast errors when connection broken ([c873d0e](c873d0edc059d01d29f93004b2fa04861d3bba47))

- Async future not cancellable when timeout=never ([f5db6cc](f5db6ccb997b510151b15b67a92a72fb76e449c8))


### Refactor


- Expose tokio in prelude mod ([612334d](612334d2eccda50b00e71392233e8ac450f7a1c5))


## [taos-v0.4.6] - 2022-11-01

**Full Changelog**: [taos-v0.4.5...taos-v0.4.6](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.5...taos-v0.4.6)

### Bug Fixes


- Remove initial poll since first msg not always none ([f37afa5](f37afa51574cd693cf40cf4f19078a4052876cce))


## [taos-v0.4.5] - 2022-10-31

**Full Changelog**: [taos-v0.4.4...taos-v0.4.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.4...taos-v0.4.5)

### Bug Fixes


- Cover timeout=never feature for websocket ([db54aa1](db54aa163cc6cddee5d87f5b2ffb313dc2bbb6ae))


## [taos-v0.4.4] - 2022-10-31

**Full Changelog**: [taos-v0.4.3...taos-v0.4.4](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.3...taos-v0.4.4)

### Bug Fixes


- Fix Stmt Send/Sync error under async/await ([bb3a758](bb3a758710f85111f47267502f9a21f87a1ada05))


## [taos-v0.4.3] - 2022-10-28

**Full Changelog**: [taos-v0.4.2...taos-v0.4.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.2...taos-v0.4.3)

### Bug Fixes


- Check tmq pointer is null ([1388bf0](1388bf06b9bcd30a3b1abfd46ee9dc320c5c57ff))


## [taos-v0.4.2] - 2022-10-28

**Full Changelog**: [taos-v0.4.1...taos-v0.4.2](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.1...taos-v0.4.2)

### Bug Fixes


- Fix drop errors when use optin libs ([20c1c87](20c1c87d30bed11f26d91a4026967ec9a50d944f))


## [taos-v0.4.1] - 2022-10-28

**Full Changelog**: [taos-v0.4.0...taos-v0.4.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.4.0...taos-v0.4.1)

### Features


- Refactor write_raw_* apis ([b91a4ad](b91a4ada4e98c514b795e0f46630faea673409c5))
- Add optin package for 2.x/3.x native comatible loading ([95ddc44](95ddc44e980da92267e694fecc97b74f1a08a124))
- Support optin native library loading by dlopen ([da91105](da911058ec55daf32f03a5852c8c0de45998acd1))


## [taos-v0.4.0] - 2022-10-25

**Full Changelog**: [taos-v0.3.1...taos-v0.4.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.3.1...taos-v0.4.0)

### Bug Fixes

- *ws*: Fix coredump when websocket already closed ([18da2fb](18da2fb6f6fd2a9560ffc8e84c96a45635314f1c))
- *ws*: Fix close on closed websocket connection ([2745f60](2745f6016311c81e5c15ff2a21ccc7fe608f408e))

- Remove select-rustc as dep ([4e3b0fd](4e3b0fd732d98e5b4f9ac7f30b0bfc7bf39e03be))
- Implicitly set protocol as ws when token param exists ([dfb3f4e](dfb3f4ef0c4cf879aadec0dff237b9a73f999991))


## [taos-v0.3.1] - 2022-10-09

**Full Changelog**: [taos-v0.3.0...taos-v0.3.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.3.0...taos-v0.3.1)

### Bug Fixes


- Expose Meta/Data struct ([e59f407](e59f407fd31e91c1aa3a3238933acc411c7d3770))


## [taos-v0.3.0] - 2022-09-22

**Full Changelog**: [taos-v0.2.12...taos-v0.3.0](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.12...taos-v0.3.0)

### Bug Fixes

- *taosws*: Fix coredump with cmd: `taos -c test/cfg -R` ([51ec0a5](51ec0a5dc1b393c8eed8d5e56df85e76820e05d1))

- Handle error on ws disconnected abnormally ([6ab74ff](6ab74ff84c7841a4a67e52b603d80f20308bd752))


### Features


- Add MetaData variant for tmq message. ([5d0a522](5d0a522c87ee4fe402b9a309137f804bf5583568))
  - **BREAKING**: MessageSet enum updated:


## [taos-v0.2.12] - 2022-09-17

**Full Changelog**: [taos-v0.2.11...taos-v0.2.12](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.11...taos-v0.2.12)

### Bug Fixes


- Public MetaAlter fields and AlterType enum ([9c10958](9c109585ccd27ae5994f63ed48692940e038b38c))


## [taos-v0.2.11] - 2022-09-16

**Full Changelog**: [taos-v0.2.10...taos-v0.2.11](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.10...taos-v0.2.11)

### Bug Fixes


- Catch error in case of action fetch_block ([562039b](562039b43b6d9b1446b35b731524041db0db19c5))
- Publish meta data structures ([9f33b4d](9f33b4dd9b777207909c8b526251c308b9b2af31))


### Performance

- *mdsn*: Improve dsn parse performance and reduce binary size ([e0586c1](e0586c1603159457e8a8ff42f7c2519b6608d211))



### Refactor


- Move taos as workspace member ([c2dbbc6](c2dbbc6443124acc574c3536a6c7fc0777565727))


## [taos-v0.2.10] - 2022-09-07

**Full Changelog**: [taos-v0.2.9...taos-v0.2.10](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.9...taos-v0.2.10)

### Bug Fixes


- Fix std::ffi::c_char not found in rust v1.63.0 ([1aa45e1](1aa45e100100683164ad9283e8b7d7d4dd0414ed))


## [taos-v0.2.9] - 2022-09-07

**Full Changelog**: [taos-v0.2.8...taos-v0.2.9](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.8...taos-v0.2.9)

### Bug Fixes


- Sync on async websocket query support ([5e62fd3](5e62fd36676e6fc063f9d506f301e37a8878181e))
- Fix arm64 specific compile error ([96ce1ba](96ce1ba018146e98e520808aa16f512d1a7904c8))


### Testing


- Fix test cases compile error ([0b6ad98](0b6ad9879fd891faa5dfc1024c7d513640f3b005))


## [taos-v0.2.8] - 2022-09-06

**Full Changelog**: [taos-v0.2.7...taos-v0.2.8](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.7...taos-v0.2.8)

### Bug Fixes

- *ws*: Fix data lost in large query set ([685e1ed](685e1edc9c2265c5b592c10c52175c5d5be11b3e))

- Fix unknown action 'close', use 'free_result' ([44ded92](44ded9212d0f1ff8f332b7a5cbe30698e84dcaa9))


## [taos-v0.2.7] - 2022-09-02

**Full Changelog**: [taos-v0.2.6...taos-v0.2.7](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.6...taos-v0.2.7)

### Bug Fixes


- Fix version action response error in websocket ([af8d06a](af8d06aad06718e251b4625f8b8b5ca91f8da3f7))


## [taos-v0.2.6] - 2022-09-01

**Full Changelog**: [taos-v0.2.5...taos-v0.2.6](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.5...taos-v0.2.6)

### Bug Fixes


- Fix websocket stmt set json tags error ([eca5562](eca556285c7fe0ea151e072a0d05748a8b4850aa))


## [taos-v0.2.5] - 2022-08-31

**Full Changelog**: [taos-v0.2.4...taos-v0.2.5](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.4...taos-v0.2.5)

### Bug Fixes

- *tmq*: Enable expriment snapshot by default ([20e8069](20e8069923465645b75320e0cda1cab768378896))

- Support . in database part ([ebd7ee5](ebd7ee56c516541c1f39f84b27adc5afacc85faf))
- Topics table rename ([cb695ae](cb695aeb38c573dc0a8541562a36b0dc2fea9c0f))


## [taos-v0.2.4] - 2022-08-30

**Full Changelog**: [taos-v0.2.3...taos-v0.2.4](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.3...taos-v0.2.4)

### Bug Fixes


- Remove unused log print ([adc4243](adc4243360d9b29481c7ea6d6d6065debeca5a6f))


## [taos-v0.2.3] - 2022-08-22

**Full Changelog**: [taos-v0.2.2...taos-v0.2.3](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.2...taos-v0.2.3)

### Testing


- Fix llvm-cov test error and report code coverage ([8f3018d](8f3018d486655dc2b11c3e70f7c4f0ed3b86ca34))


## [taos-v0.2.2] - 2022-08-11

**Full Changelog**: [taos-v0.2.1...taos-v0.2.2](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.1...taos-v0.2.2)

### Bug Fixes


- Fix derive error since rustc-1.65.0-nightly ([0d155a5](0d155a5d6d16e108d0845d868fc7196ef988c027))


## [taos-v0.2.1] - 2022-08-11

**Full Changelog**: [taos-v0.2.0...taos-v0.2.1](https://github.com/taosdata/taos-connector-rust/compare/taos-v0.2.0...taos-v0.2.1)

### Documentation


- Fix build error on docs.rs ([ce8f6c8](ce8f6c816331b05c8d0c244ef9ef1782d057472d))


## [taos-v0.2.0] - 2022-08-11

### Bug Fixes

- *C*: Use char instead of uint8_t in field names ([65e08ea](65e08ea1546aed7321cbcc44f283a74c1910f9cf))
- *query*: Remove debug info of json view ([1e7c053](1e7c053a857dde9ac7c3451d187ee1a38e0d157c))
- *query*: Fix websocket v2 float null ([35eabf7](35eabf7c16631fe0112384ea9fe1df8af4eaa0ef))
- *ws*: Not block on ws_get_server_info ([80d7309](80d7309db161e7bd0fa6c6e5a00127722657ed4b))
- *ws*: Use ws_errno/errstr in ws_fetch_block ([c27eeab](c27eeab5728d6912361a8ab668ed1d74a5fafb2b))
- *ws*: Add ws-related error codes, fix version detection ([96884b0](96884b06ad2649598325b15987c9260a6b16055a))
- *ws*: Fix fetch block unexpect exit ([6e1335f](6e1335f8b9a2c46ab26548070aa77aaf9e47813c))
- *ws*: Fix query errno when connection is not alive ([49f980c](49f980cdcaa2cb151884d712ecddccf33ac16f1f))
- *ws*: Fix stmt bind with non-null values coredump ([742f99f](742f99fedd7f205f3471a80fac38bbe85738a1c1))
- *ws*: Fix timing compatibility bug for v2 ([2f190df](2f190df78d35acfbb82cef1a8c312e3888e2e6df))
- *ws*: Fix stmt null error when buffer is empty ([7fb8574](7fb8574385ea871cbe81640357745328fc93b8db))
- *ws*: Fix bigint/unsigned-bigint precision bug ([35afaf2](35afaf2a0bacfec3f3c62e2b72c94397d63c2912))
- *ws-sys*: Fix ws_free_result with NULL ([f642d60](f642d6081df8230b3514db98b30ac1c68ffce1a5))

- Fix column length calculation for v2 block ([24c3377](24c33772ae8857292adc5fc67351fe359cf66d3c))
- Gcc 7 compile error ([89c1720](89c1720ee3073e2a6b7365d086a0432154bd0ac5))
- Taos-ws-sys/examples/show-databases.c ([2caa2dd](2caa2ddee2834dc87e8eb8a688252afc9e95f4af))
- Fix duplicate defines when use with taos.h ([430982a](430982a0c2c29a819ffc414d11f49f2d424ca3fe))
- .github/workflows/build.yml branch ([ba81246](ba81246badb2dfd5113a6d5364b4595aac902680))
- Remove usless file ([e725bcc](e725bcce340f8b16e2b6a079643b47b8fd0bc708))


### Documentation


- Add license and refine readme ([e80fef2](e80fef2aebf58217a38def12c90443b2705da28f))
- Add README and query/tmq examples ([de89305](de8930596c59081af9b643d2dc170394faaaf0e7))
- Add example for bind and r2d2 ([d1b8b27](d1b8b271c87a747fe9047386474609477c2c78b9))


### Enhancements

- *ws*: Feature gated ssl support for websocket ([b2c1a92](b2c1a9245ce9723f8eab24cc663f332df1e31325))



### Features

- *libtaosws*: Add ws_get_server_info ([9c2fad8](9c2fad82a1a9e45eea2a2784df1b5a7b9b8e03f3))
- *ws*: Add stmt API ([20837b2](20837b2fd617e2b3f4c801fa193de2570817abcd))
- *ws*: Add ws_take_timing method for taosc cost ([d420966](d420966ad85952bae1d00d79902620efeee490e4))
- *ws*: Add ws_stop_query, support write raw block ([13c3563](13c3563691e813c4fc9f4add4f4590b7d7f35880))

- Support python lib for websocket ([1f24401](1f2440127d534b0bb791d227a46d77cdffbcd857))
- Add main connector ([0ba8a17](0ba8a1701d4b8a30c89228807305ac41b23bc763))


### Refactor

- *query*: Refactor query interface ([dcb9c57](dcb9c57930dd35f029c7aedfe151d8d38b886dcf))
- *query*: New version of raw block ([31d1032](31d10327148079e8e86b47693db643cc6745277f))

- Use stable channel ([f7989f4](f7989f495c5f752df4d8e35e26d6cd04eb5c2a9a))
- Change ws_rs to ws_res to align with original naming style ([94e94cb](94e94cb6067c68ebdd13f045770bd2695ae27903))
- Merge with main branch ([aa71b8e](aa71b8e0681808fc40d53ff654ab6e21637d7e58))
- Fix nchar/json error, refactor error handling ([9d8c632](9d8c632c093ae838e066e23c3fdda52bafa1a403))
- Fix stmt bind with raw block error ([d45e37a](d45e37ac1125aba07b2efe48d8375e0b4af2a6bf))


