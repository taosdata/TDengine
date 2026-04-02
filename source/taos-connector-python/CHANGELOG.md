# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Conventional Changelog](https://www.conventionalcommits.org/en/v1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## v2.8.9 - 2026-03-05

### Features:

- Data subscription supports token authentication

## v2.8.6 - 2025-10-27

### Features:

-  support sqlalchemy stmt query (#368)

### Bug Fixes:

-  fix memleak (#367)

## v2.8.5 - 2025-08-25

### Features:

-  support dbutils/sqlalchemy connection pool (#362)

### Enhancements:

-  ws sqlalchemy supports failover (#364)
-  ws support timezone (#361)

## v2.8.3 - 2025-07-24

### Features:

-  native support blob type (#357)

## v2.8.2 - 2025-06-27

### Bug Fixes:

-  moify timezone param set interface (#347)

## v2.8.1 - 2025-05-22

### Features:

- add connect property setting function to native connector

### Bug Fixes:

- all workflows cmake config
- delete token auth
- modify version to 2.8.0
- optimize null value checks (#317)
- password supports special characters
- remove superset driver TDengine.py
- test auth base64
- uniform python name
- workflows cmake config
- **(ws)**: update Cargo.lock

### Documents:

- add changelog for v2.7.22/v2.7.23
- python to Python

## v2.7.23 - 2025-03-24
- support DECIMAL data type

## v2.7.22 - 2025-03-24
- support Python 3.12 and above

### Bug Fixes:

- fix python 3.12 abandoned (#313)

## v2.7.21 - 2024-12-30


### Features:

-  Suport Stmt2 Write

## v2.7.19 - 2024-12-30

### Features:

-  Supported Apache SuperSet with TDengine Cloud Data Source

### Bug Fixes:

-  add mac build go upgrade 1.18
-  add pip install taos-ws-py in the document

### Tests:

-  token pass with argument index

### Documents:

-  2.7.18 modify bug fixed log
-  rewrite changelog with offical-website doc

## v2.7.18 - 2024-12-14

### Features:

-  support taos taosws taosrest protocol for superset

### Bug Fixes:

-  fix: case test_tmp death wait message
- **(taos-ws-py)**:  fix crypto provider error

### Enhancements:

-  double match to sqltypes.float reference sqlite rule

### Tests:

-  do nothing for tmp and sqlalchemy

### Documents:

-  optimize taospy documents on PyPi official website

## v2.7.16 - 2024-09-19

### Features:

- Add subscription configuration (session. timeout. ms, Max. roll. interval. ms).

## v2.7.15 - 2024-04-12

### Features:

- Added support for VARBINRY and GEOMETRY types.

## v2.7.14 - 2024-04-12

### Bug Fixes:

- Fix Known Issues.

## v2.7.13 - 2024-01-26

### Bug Fixes:

-  mem leak

### Enhancements:

-  add api committed, position
-  add taos.cfg
-  separate the build and test processe
-  start tdengine in job
-  update  cargo taos version
-  update step
- **(ws)**:  add tmq commit_offset
- **(ws)**:  add tmq list_topic

### Documents:

-  update dev cmd

## v2.7.12 - 2023-09-11

### Bug Fixes:

- fix decode binary in tmq

## v2.7.11 - 2023-09-08

### Bug Fixes:

- fix fetch block via ws
- fix varbinary for cursor
- return empty list when no data
- return empty list when no data in cursor
- taos.error: Database not specified

### Enhancements:

- add config decode_binary to special whether decode varchar or binary

## v2.7.10 - 2023-07-04

### Bug Fixes:

- delete duplicate code
- fix test case for 2.6

### Documents:

- remove examples

## v2.7.9 - 2023-06-25

### Bug Fixes:

- fix mem leak

### Enhancements:

- support assignment and seek
- support assignment and seek via ws
- support schemaless via ws
- support stmt via ws

## v2.7.8 - 2023-06-14

### Bug Fixes:

- tmq error refine (#182)

### Enhancements:

- add cursor.execute_many on native (#168)
- add unsubscribe fn

### Documents:

- update cmd
- update readme

## v2.7.7 - 2023-03-28

### Bug Fixes:

- recover TaosConnection init args (#150)
- there was a conflict between the minimum version of Python and t… (#144)
- update typing in schemaless_insert (#153)

### Enhancements:

- add req_id for rest api (#149)
- python connector support req_id (#147)
- support req id ws (#161)
- TD-19401 support schemaless raw (#159)
- update function signature (#148)

### Tests:

- add case for connect args (#151)
- add tear_down function, clean up database after test
- add workflow (#154)
- add workflow (#155)
- add workflow for TDengine, single case (#157)
- update function name
- update teardown with module level
- update wrong password test

### Documents:

- add examples (#163)
- update doc and examples with schemaless (#164)

## v2.7.6 - 2023-02-27

### Bug Fixes:

- add pandas-read-sql-tdengine-cloud.py (#134)
- fix for sqlalchemy warning (#135)

## v2.7.5 - 2023-02-23

### Bug Fixes:

- add default port for taosrest sqlalchemy (#133)

## v2.7.4 - 2023-02-22

### Bug Fixes:

- adapt to sqlachemy 2

## v2.7.3 - 2023-02-22

### Bug Fixes:

- fix rest and ws for cloud

### Enhancements:

- delete TaosConsumer and TaosTmq api (#129)

### Documents:

- update readme markdown for v2.7.2 to clarify python version requirement (#128)

## v2.7.2 - 2023-01-23

### Features:

- make taospy works with python 3.6 (#127)

## v2.7.1 - 2023-01-18

### Bug Fixes:

- fix new tmq consumer (#126)

## v2.7.0 - 2023-01-13

## v2.6.10 - 2022-12-19

### Bug Fixes:

- alchemy+rest support db name

### Enhancements:

- support vg id and fix code style (#116)

## v2.6.9 - 2022-12-08

### Bug Fixes:

- fix tmq result memory leak

## v2.6.8 - 2022-12-05

### Enhancements:

- column max length support up to 64k (#114)

## v2.6.7 - 2022-11-25

### Bug Fixes:

- should not call taos_free_result for stmt (#111)
- test.yml install dependency always" (#110)

### Documents:

- move topic drop to early phase in python tmq example (#109)

## v2.6.6 - 2022-11-20

### Bug Fixes:

- fix tmq crash (#108)

## v2.6.5 - 2022-11-11

### Bug Fixes:

- dependency constrait error with poetry 1.2
- fix `tmq_commit_cb` param and add test for ns precision (#100)
- github action load library issue on macos (#103)
- taosrest alchemy handle non-root user (#102)

### Tests:

- verify on macOS (#20)

### Documents:

- update examples/tmq.py (#99)

## v2.6.5 - 2022-11-10

### Bug Fixes:

- dependency constrait error with poetry 1.2
- fix `tmq_commit_cb` param and add test for ns precision (#100)
- github action load library issue on macos (#103)
- taosrest alchemy handle non-root user (#102)

### Tests:

- verify on macOS (#20)

### Documents:

- update examples/tmq.py (#99)

## v2.6.5 - 2022-11-10

### Bug Fixes:

- dependency constrait error with poetry 1.2
- fix `tmq_commit_cb` param and add test for ns precision (#100)
- github action load library issue on macos (#103)
- taosrest alchemy handle non-root user (#102)

### Tests:

- verify on macOS (#20)

### Documents:

- update examples/tmq.py (#99)

## v2.6.5 - 2022-11-10

### Bug Fixes:

- dependency constrait error with poetry 1.2
- fix `tmq_commit_cb` param and add test for ns precision (#100)
- github action load library issue on macos (#103)
- taosrest alchemy handle non-root user (#102)

### Tests:

- verify on macOS (#20)

### Documents:

- update examples/tmq.py (#99)

## v2.6.5 - 2022-11-10

## v2.6.5 - 2022-11-10

### Bug Fixes:

- dependency constrait error with poetry 1.2
- fix `tmq_commit_cb` param and add test for ns precision (#100)
- github action load library issue on macos (#103)
- taosrest alchemy handle non-root user (#102)

### Tests:

- verify on macOS (#20)

### Documents:

- update examples/tmq.py (#99)

## v2.6.4 - 2022-09-09

### Bug Fixes:

- **(rest)**: fix non timezone error when taosAdapter run in non-UTC env (#96)

## v2.6.3 - 2022-09-06

### Bug Fixes:

- auto detect response version instead of taos.IS_V3

### Tests:

- improve connector test coverage (#91)
- python function test (#92)

## v2.6.2 - 2022-08-18

### Bug Fixes:

- fix ci script error

## v2.6.1 - 2022-08-18

## v2.6.0 - 2022-08-17

### Features:

- add kafka tmq api
- latest supported python version bumped to v3.7

## v2.5.2 - 2022-08-11

### Bug Fixes:

- fix taos-ws-py python version dependency (#88)

## v2.5.1 - 2022-08-11

### Features:

- add taosws module
- **(rest)**: add timezone option

### Bug Fixes:

- avoid in 2.x
- do not test pandas api in 3.0
- remove duplicate (#82)

### Enhancements:

- **(rest)**: add test case

### Tests:

- add test case for tmq
- **(rest)**: add test case
- **(rest)**: refine doc
- timezone

### Documents:

- **(taosws)**: add examples using taosws module

## v2.5.0 - 2022-08-10

### Features:

- add taosws module

### Documents:

- **(taosws)**: add examples using taosws module

## v2.4.0 - 2022-07-18
### Features:

- taosrest support more query methods (#70)

### Bug Fixes:

- iter on TaosFields v3

### Tests:

- test rest cursor

### Documents:

- add build-doc.sh
- modify as comment
- update build-doc.sh
- update build-docs.sh

## v2.3.6 - 2022-06-10

### Enhancement:

- add tests/examples files in source package distribution (#69)

## v2.3.5 - 2022-06-07

### Bug Fixes:

- fix gbk encoding error while use cursor.log() in windows (#67)
- sqlalchemy use rest connection (#66)

## v2.3.4 - 2022-06-06

## v2.3.3 - 2022-06-06

### Bug Fixes:

- fix null value bind stmt for 3.0 (#60)
- nullptr

## v2.3.2 - 2022-05-23

### Features:

- compatible with 3.0 stmt
- support for cloud service token (#57)

### Bug Fixes:

- fix json tag error in query (#59)
- no exception subscribe error occur (#58)

### Tests:

- add test case for taos_stmt_set_tbname_tag
- test stmt bind passed in 3.0

## v2.3.1 - 2022-04-28

### Features:

- add  RestClient
- implement PEP249 Connections and Cursor API
- remove wsclient and refactor folder structure
- **(rest)**: adapter code for sqlalchemy and pandas
- **(restful)**: add some test cases
- **(rest)**: increase version number
- support 3.0

### Bug Fixes:

- cannot iter on TaosFields twice
- catch datetime overflow and pass
- decrease version number
- failed to convert rfc3339 format time string
- fix poetry publish error
- fix pytest error for v3 check
- fix set_tz function not found
- float is_null missed
- remove buggy line in taos_fetch_row
- restore taos_options
- rowcount is -1 for INSERT statement
- taos_query() result for 3.0
- typo
- typos
- use isnan() or is_null for float/double
- use taos_is_null()

### Enhancements:

- add time utile to parse rfc3339 format time string
- ident json
- **(RestClient)**: support timeout parameter
- **(rest)**: convert timestamp from 'str' to 'datetime'
- **(rest)**: export RestClient
- **(taos)**: make config method private
- **(taos)**: rename _config to _init_config
- use iso8601 to parse date

### Tests:

- add test cases for pandas
- test rowcount

### Documents:

- add docs to cursor.description
- add limitation of taosrest
- fix typo and align parameters order
- schemaless docs
- update manual
- update readme for taosres
- update user manual

## v2.3.0 - 2022-04-28

### Features:

- add  RestClient
- implement PEP249 Connections and Cursor API
- remove wsclient and refactor folder structure
- **(rest)**: adapter code for sqlalchemy and pandas
- **(restful)**: add some test cases
- **(rest)**: increase version number
- support 3.0

### Bug Fixes:

- cannot iter on TaosFields twice
- catch datetime overflow and pass
- decrease version number
- failed to convert rfc3339 format time string
- fix pytest error for v3 check
- fix set_tz function not found
- float is_null missed
- remove buggy line in taos_fetch_row
- restore taos_options
- rowcount is -1 for INSERT statement
- taos_query() result for 3.0
- typo
- typos
- use isnan() or is_null for float/double
- use taos_is_null()

### Enhancements:

- add time utile to parse rfc3339 format time string
- ident json
- **(RestClient)**: support timeout parameter
- **(rest)**: convert timestamp from 'str' to 'datetime'
- **(rest)**: export RestClient
- **(taos)**: make config method private
- **(taos)**: rename _config to _init_config
- use iso8601 to parse date

### Tests:

- add test cases for pandas
- test rowcount

### Documents:

- add docs to cursor.description
- add limitation of taosrest
- fix typo and align parameters order
- schemaless docs
- update manual
- update readme for taosres
- update user manual

## v2.2.5 - 2022-04-13

### Features:

- [TD-14696]:support timezone option when connect

## v2.2.4 - 2022-03-31

### Bug Fixes:

- [TD-14410]:add affected_rows property for stmt
- [TD-14410]:fix import error in taos 2.0/2.2
- [TD-14410]:use stmt.affected_rows in examples

## v2.2.3 - 2022-03-29

### Bug Fixes:

- [TD-14371]:remove unnecessary print() lines
- [TD-14382]:fix row_count property always be 0

## v2.2.2 - 2022-03-28

### Features:

- [TD-14210]:support sqlalchemy dialect plugin

### Bug Fixes:

- [TD-14358]:remove stream api

## v2.2.1 - 2022-01-12

### Bug Fixes:

- [TS-1107]:use typing as one of dev-dependencies

## v2.2.0 - 2021-12-23

### Bug Fixes:

- [TD-10875]:test for each git push
- [TD-12253]:fix syntax error in README

### Tests:

- [TD-10875]:add GitHub Actions test workflow on pr

### Documents:

- [TD-12250]:fix README confused titles

## v2.1.2 - 2021-11-05

### Bug Fixes:

- [TD-10828]:fix import taos error for TDengine 2.2 or 2.0
- [TD-10838]:fix ci error in release

### Documents:

- [TD-10828]:improve document for pip install

## v0.1.0 - 2021-09-09

### Features:

- [TD-2971]:make python connector support unsigned int. fix None determine.
- [TD-3048]:support lines/stream/query_a/stop_query/ and so on. (#7079)
- [TD-4647]:auto add column through schemaless line protocol
- [TD-4752]:python connector support nanosecond. (#6528)

### Bug Fixes:

- [TD-2915]:python connector cursor iter next() function. (#5875)
- [TD-3288]:solve python connector bigint and timestamp issue on 32bit.
- [TD-4048]:fix python connector error where ts is null (#6018)
- [TD-4160]:remove python connector soft links, fix tests and documents
- [TD-4370]:squashed commit of python connector changes in develop (#6246)
- [TD-4640]:fix CDLL error in macOs (#6430)
- [TD-4808]:fix python connector error where ts is null (#6025)
- [TD-5585]:arm32 python fromtimestamp error. (#7090)
- [TD-5892]:publish python connector to PyPI
- [TD-6169]:windows dll client can not quit.
- [TD-6231]:fix all none error of stmt multibind in python connector (#7482)
- [TD-6241]:fix unexpected nan while inserting None as float/doub… (#7486)
- [TD-6313]:improve error handling if loading taos failed in python (#7542)
- [TD-6313]:improve error handling if loading taos failed in python (#7550)

### Enhancements:

- [TD-182]:use single repo for python connector (#6036)

### Documents:

- [TD-4803]:fix repo name in python connector README (#6559)
- [TD-6449]:prefer english only in method documentations in python connector [ci skip] (#7711)

