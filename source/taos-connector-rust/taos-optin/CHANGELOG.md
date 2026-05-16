# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.1.11] - 2023-01-16

### Bug Fixes

- *(optin)* Fix memory leak in stmt bind- Stmt mem leak for binary/nchar bind


## [0.1.10] - 2023-01-08

### Features
- Taosws PEP-249 fully support


### Dep
- Taos-query v0.3.13


## [0.1.9] - 2022-12-24

### Bug Fixes
- Fix memory leak when query error


## [0.1.8] - 2022-12-21

### Bug Fixes
- Fix mem leak in taos_query_a callback


### Features
- Support write_raw_block_with_fields


## [0.1.7] - 2022-12-02

### Bug Fixes
- Produce exlicit error for write_raw_meta in 2.x


## [0.1.6] - 2022-12-01

### Bug Fixes
- Fix coredump when optin dropped


## [0.1.3] - 2022-10-28

### Bug Fixes
- Check tmq pointer is null


## [0.1.2] - 2022-10-28

### Bug Fixes
- Fix drop errors when use optin libs


## [0.1.1] - 2022-10-27

### Features
- Add optin package for 2.x/3.x native comatible loading


## [0.1.0] - 2022-10-27

### Features
- Add optin package for 2.x/3.x native comatible loading


