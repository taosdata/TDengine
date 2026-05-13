# Changelog

All notable changes to this project will be documented in this file.


The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.2.9] - 2022-12-06

### Features

- *(mdsn)* Support percent encoding of password

## [0.2.8] - 2022-12-01

### Bug Fixes
- Support `?key`-like params


## [0.2.7] - 2022-11-11

### Features
- DsnError::InvalidAddresses error changed


## [0.2.5] - 2022-09-16

### Performance

- *(mdsn)* Improve dsn parse performance and reduce binary size

## [0.2.4] - 2022-08-31

### Bug Fixes
- Support . in database part


## [0.2.3] - 2022-08-22

### Bug Fixes

- *(query)* Fix websocket v2 float null
- *(ws)* Fix bigint/unsigned-bigint precision bug

### Testing
- Fix llvm-cov test error and report code coverage


