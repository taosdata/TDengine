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



