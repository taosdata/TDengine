<a name="0.3.0"></a>
## 0.3.0 TDengine (2023-04-03)


#### Features

*   add explorer.toml file ([975eda1b](975eda1b))
*   support /rest/sql directly in explorer API ([5e2081cf](5e2081cf))

#### Bug Fixes

*   fix configuration file path error on linux ([d012c6ab](d012c6ab))
*   /rest/upload and /rest/sql fix ([3475c9a6](3475c9a6))



<a name="0.2.0"></a>
## 0.2.0 TDengine (2023-02-28)


#### Bug Fixes

*   fix replication content-type error ([ab4207cf](ab4207cf))
*   TD-22850 ([d200d505](d200d505))
*   windows build ([47b75372](47b75372))
*   grant topic ([5c25ad89](5c25ad89))
*   fix read configuration error ([2acbcdfa](2acbcdfa))
*  修改所有删除的具体提示信息和programing的跳转逻辑 DATA IN ([30d00898](30d00898))
* **server:**  tasks api content type error ([d11adbfc](d11adbfc))

#### Features

*   support README in out dir ([f4dc75d8](f4dc75d8))
*   support CUS_NAME/CUS_PROMPT environment ([512b2743](512b2743))
*   cors: allow * ([f6151585](f6151585))
*   use configured taosx api ([68d2238f](68d2238f))
*   TD-22602 1. sort user 2. filter has no db or topic ([689c0e3f](689c0e3f))
*   TD-22602 add user database ([4a8a53c5](4a8a53c5))
*   support taosx API ([2a9b30e1](2a9b30e1))
*   add --version flag ([602f1d59](602f1d59))
*   apply configurations by order: file, env, cli ([d3d4c939](d3d4c939))
*   add rust embed server for explorer ([e3998873](e3998873))
* **server:**  add cluster/dashboard configuration options ([38399cab](38399cab))

#### Performance

*   update words ([d498d0ef](d498d0ef))



