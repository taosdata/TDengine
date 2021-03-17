
The library requires that C99 integer types are available on the
target computer.  Specifically the int8_t, int16_t, int32_t, int64_t
and their unsigned counterpart types.

## Unix, Linux, macOS

A simple `make` on most Unix-like systems should build the library.

The included Makefile should work for most Unix-like environments and
many make variants. It is known to work with GNU make, which, if not the
default, is sometimes installed as `gmake`.

The `CC`, `CFLAGS`, `LDFLAGS` and `CPPFLAGS` environment variables can be set
to control the build.

If the **LIBMSEED_URL** variable is defined during the build, the library will
be compiled with support for reading from URLs.  Currently, this support requires
that [libcurl](https://curl.haxx.se/) be installed on the target system.

The simplest way to build the library with URL support is to include `-DLIMSEED_URL` 
in the CFLAGS environment variable.  Or invoking the build as such:

```
CFLAGS+=" -DLIBMSEED_URL" make
```

By default a statically linked version of the library is built: **libmseed.a**,
with an accompanying header **libmseed.h**.

With GCC, clang or compatible build tools it is possible to build a shared
library with `make shared`.

A simple install method for the shared library can be invoked with
`make install`.  By default the installation destination is /usr/local.
The install destination may be specified using the **PREFIX** variable, for
example:

```
make install PREFIX=/path/to/install/
```

## Windows

On a Windows platform the library can be compiled by using the
NMake compatible Makefile.win (e.g. 'nmake -f Makefile.win').
The default target is a static library 'libmseed.lib'.
A libmseed.def file is included for use building and linking a DLL.
