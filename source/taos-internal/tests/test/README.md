# TDengine Enterprise Extension (taos-internal)

This repository contains the **enterprise extension plugins** for TDengine. It is **not independently compilable** — it is built as part of taos-community when the `-DBUILD_ENTERPRISE=ON` CMake option is enabled.

For build instructions, prerequisites, installation, running, and testing, please refer to the [taos-community README](../taos-community/README.md).

## 1. Purpose

taos-internal provides enterprise-only plugins that extend [TDengine OSS](../taos-community/) with additional capabilities for industrial customers:

- **Tiered Storage** — S3 and multi-media storage support
- **Grant/License Management** — node count, time series, and data intake licensing controls
- **Enhanced Authentication** — token, TOTP, and encryption modules
- **Privilege Management** — fine-grained access control
- **Administrative & Audit** — admin panel, audit logging
- **Data Node Extensions** — enterprise dnode/mnode/vnode/xnode plugins

For a full list of TDengine Enterprise features, please [check here](https://tdengine.com/enterprise/). The easiest way to experience TDengine Enterprise is through [TDengine Cloud](https://cloud.tdengine.com).

## 2. Directory Structure

```
taos-internal/
├── cmake/                # Enterprise CMake configuration
│   ├── enterprise_defines.cmake   # Brand customization & grant parameters
│   └── enterprise_options.cmake   # Enterprise build toggles
├── source/
│   └── plugins/          # Enterprise plugin modules
│       ├── grant/        # License/grant management
│       ├── storage/      # Tiered storage (S3, multi-media)
│       ├── dnode/        # Data node extensions
│       ├── mnode/        # Management node extensions
│       ├── vnode/        # Virtual node extensions
│       ├── xnode/        # Extension node plugins
│       ├── taosk/        # Encryption module
│       ├── token/        # Token authentication
│       ├── totp/         # TOTP authentication
│       ├── privilege/    # Privilege management
│       ├── admin/        # Admin panel
│       ├── audit/        # Audit logging
│       ├── crypt/        # Cryptographic utilities
│       ├── trans/        # Transaction extensions
│       ├── view/         # View extensions
│       ├── web/          # Web interface plugins
│       └── common/       # Shared enterprise utilities
├── packaging/            # Enterprise release & packaging scripts
├── tests/                # Enterprise-specific tests
└── docs/                 # Documentation (deprecated)
```

## 3. How It Is Built

taos-internal is **not a standalone project**. It is integrated into the taos-community build system via `cmake/enterprise.cmake`:

```cmake
# In taos-community CMakeLists.txt:
if(TD_ENTERPRISE)
  add_subdirectory("${TD_ENTERPRISE_DIR}" "${CMAKE_BINARY_DIR}/enterprise")
endif()
```

To build TDengine with enterprise features enabled:

```bash
cd taos-community
mkdir debug && cd debug
cmake .. -DBUILD_ENTERPRISE=ON
make -j$(nproc)
```

The `TD_ENTERPRISE_DIR` defaults to `../taos-internal` (sibling directory of taos-community).

## 4. Enterprise Packaging

Enterprise packaging scripts are located in the `packaging/` directory:

```bash
cd taos-internal/packaging
# version_number format: x.x.x.x[.x], e.g. 3.3.5.0
./new_ver_release.sh -n <version_number>
```

The generated installation packages can be found in `taos-community/release/`.

## 5. Enterprise Releasing

TDengine Enterprise installers are published to the corporate NAS server:

- NAS Server: `http://192.168.1.252:5000/`
- Directory: `/Release/TDengine/`

## 6. Documentation

- [TDengine Documentation](https://docs.tdengine.com) ([中文文档](https://docs.taosdata.com))
- [TDengine Enterprise](https://tdengine.com/enterprise/)
- [TDengine Cloud](https://cloud.tdengine.com)