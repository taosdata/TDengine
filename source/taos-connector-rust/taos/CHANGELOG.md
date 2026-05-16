# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.4.18] - 2023-01-08

### Features
- Taosws PEP-249 fully support


### Dependencies

- Use taos-ws v0.3.17
- Use taos-optin v0.1.10
- Use taos-query v0.3.13


## [0.4.17] - 2022-12-21

### Examples
- Add example for database creation


## [0.4.11] - 2022-12-01

### Features
- Expose TaosPool type alias for r2d2 pool


## [0.4.9] - 2022-11-22

### Enhancements

- *(ws)* Add features `rustls`/`native-tls` for ssl libs

## [0.4.7] - 2022-11-02

### Bug Fixes

- *(ws)* Broadcast errors when connection broken

## [0.4.1] - 2022-10-27

### Features
- Support optin native library loading by dlopen
- Refactor write_raw_* apis


## [0.4.0] - 2022-10-25

### Bug Fixes
- Implicitly set protocol as ws when token param exists
- Expose Meta/Data struct


### Features
- Add MetaData variant for tmq message.
  - **BREAKING**: add MetaData variant for tmq message.


### Refactor
- Move taos as workspace member


## [0.3.1] - 2022-10-09

### Bug Fixes
- Expose Meta/Data struct


## [0.3.0] - 2022-09-22

### Features
- Add MetaData variant for tmq message.
  - **BREAKING**: add MetaData variant for tmq message.


## [0.2.11] - 2022-09-16

### Refactor
- Move taos as workspace member


