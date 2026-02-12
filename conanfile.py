from conan import ConanFile


class TDengineConan(ConanFile):
    name = "tdengine"
    version = "3.0"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"

    # Options for conditional dependencies
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_test": [True, False],
        "with_jemalloc": [True, False],
        "with_geos": [True, False],
        "with_pcre2": [True, False],
        "with_uv": [True, False],
        "with_sqlite": [True, False],
        "with_s3": [True, False],
        "with_azure": [True, False],
        "with_cos": [True, False],
        "with_taosws": [True, False],
        "with_taos_tools": [True, False],
        "build_addr2line": [True, False],
    }

    default_options = {
        "shared": False,
        "fPIC": True,
        "with_test": True,
        "with_jemalloc": False,
        "with_geos": True,
        "with_pcre2": True,
        "with_uv": True,
        "with_sqlite": False,
        "with_s3": False,
        "with_azure": False,
        "with_cos": False,
        "with_taosws": False,
        "with_taos_tools": True,
        "build_addr2line": False,
        # Force static libraries for all dependencies
        "*:shared": False,
        "*:fPIC": True,
    }

    # Conan generators
    generators = "CMakeDeps", "CMakeToolchain"

    def layout(self):
        # Don't use cmake_layout to avoid nested build directories
        self.folders.generators = "generators"

    def requirements(self):
        """Define all dependencies (ConanCenter + local recipes under conan/)"""

        # tzcode (libtz.a) is linked by source/os on non-Windows
        if self.settings.os != "Windows":
            self.requires("tz/2025a")

        # Core compression libraries
        self.requires("zlib/1.3.1")
        self.requires("lz4/1.10.0")
        self.requires("xxhash/0.8.3")
        self.requires("xz_utils/5.8.1")  # LZMA
        self.requires("fast-lzma2/1.0.1")  # From conan/fast-lzma2

        # JSON libraries
        self.requires("cjson/1.7.18")

        # Networking
        self.requires("openssl/3.6.0")  # Using latest 3.x
        self.requires("libcurl/8.2.1")  # Compatible with Conan 2.19

        # Optional: libuv for transport
        if self.options.with_uv:
            self.requires("libuv/1.49.2")

        # Database/Storage
        self.requires("rocksdb/9.7.4")
        self.requires("cyrus-sasl/2.1.28")

        # Optional: jemalloc
        if self.options.with_jemalloc:
            self.requires("jemalloc/5.3.0")

        # Optional: sqlite
        if self.options.with_sqlite:
            self.requires("sqlite3/3.51.0")

        # Optional: geometry library
        if self.options.with_geos:
            self.requires("geos/3.12.2")

        # Optional: regex library
        if self.options.with_pcre2:
            self.requires("pcre2/10.44")

        # Optional: addr2line/backtrace support (libdwarf-based)
        if self.options.build_addr2line and self.settings.os != "Windows":
            self.requires("libdwarf/0.3.1")
            # Tool (optional)
            self.requires("libdwarf-addr2line/0.3.1")

        # Testing framework
        if self.options.with_test:
            self.requires("gtest/1.12.1")
            self.requires("cppstub/1.0.0")

        # Optional: taos-tools dependencies
        if self.options.with_taos_tools:
            self.requires("jansson/2.14")
            self.requires("snappy/1.2.1")
            # Apache Avro C - using local recipe in conan/avro-c
            self.requires("avro-c/1.11.3")

        # Shared storage dependencies (S3)
        if self.options.with_s3 and self.settings.os != "Windows":
            self.requires("libxml2/2.15.0")
            self.requires("libs3/4.1")
            if self.options.with_azure:
                self.requires("td-azure-sdk/12.13.0-beta.1")

        # COS (optional)
        if self.options.with_cos and self.settings.os != "Windows":
            self.requires("apr/1.7.6")
            self.requires("apr-util/1.6.3")
            self.requires("mxml/2.12")
            self.requires("cos-c-sdk/5.0.16")

        # taosws (optional, Rust-based)
        if self.options.with_taosws:
            self.requires("taosws/0.1.0")

        # Windows specific dependencies (local recipes)
        if self.settings.os == "Windows":
            self.requires("pthread-win32/3.0.3.1")
            self.requires("win-iconv/0.0.8")
            self.requires("libgnurx-msvc/master")
            self.requires("wcwidth-cjk/master")
            self.requires("wingetopt/master")
            self.requires("crashdump/master")

    def configure(self):
        """Configure options based on settings"""
        # Force static linking for all dependencies
        self.options["*"].shared = False

        # Cyrus SASL: ConanCenter recipe doesn't support building static libs with MSVC.
        # Use shared on Windows/MSVC to keep the dependency available.
        if self.settings.os == "Windows" and str(self.settings.compiler) == "msvc":
            self.options["cyrus-sasl"].shared = True

        # Configure dependency options
        if self.settings.os != "Windows":
            self.options["*"].fPIC = True

        # OpenSSL configuration
        self.options["openssl"].shared = False
        self.options["openssl"].no_deprecated = False

        # libcurl configuration
        self.options["libcurl"].shared = False
        self.options["libcurl"].with_ssl = "openssl"
        self.options["libcurl"].with_zlib = True

        # RocksDB configuration
        self.options["rocksdb"].shared = False
        self.options["rocksdb"].with_jemalloc = self.options.with_jemalloc
        self.options["rocksdb"].with_gflags = False
        self.options["rocksdb"].with_snappy = False
        self.options["rocksdb"].with_lz4 = True
        self.options["rocksdb"].with_zlib = True
        self.options["rocksdb"].with_zstd = False

        # zlib configuration
        self.options["zlib"].shared = False

        # lz4 configuration
        self.options["xxhash"].shared = False

        # lz4 configuration
        self.options["lz4"].shared = False

        # fast-lzma2 configuration
        self.options["fast-lzma2"].shared = False

        # cjson configuration
        self.options["cjson"].shared = False

        # gtest configuration
        if self.options.with_test:
            self.options["gtest"].shared = False

    def build_requirements(self):
        """Build-time dependencies"""
        pass
