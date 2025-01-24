AC_PREREQ([2.60])
AC_INIT([jansson], [2.14], [https://github.com/akheron/jansson/issues])

AC_CONFIG_AUX_DIR([.])
AM_INIT_AUTOMAKE([1.10 foreign])

AC_CONFIG_SRCDIR([src/value.c])
AC_CONFIG_HEADERS([jansson_private_config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_CXX
AC_PROG_LIBTOOL
AM_CONDITIONAL([GCC], [test x$GCC = xyes])

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([endian.h fcntl.h locale.h sched.h unistd.h sys/param.h sys/stat.h sys/time.h sys/types.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT32_T
AC_TYPE_UINT32_T
AC_TYPE_UINT16_T
AC_TYPE_UINT8_T
AC_TYPE_LONG_LONG_INT

AC_C_INLINE
case $ac_cv_c_inline in
    yes) json_inline=inline;;
    no) json_inline=;;
    *) json_inline=$ac_cv_c_inline;;
esac
AC_SUBST([json_inline])

# Checks for library functions.
AC_CHECK_FUNCS([close getpid gettimeofday localeconv open read sched_yield strtoll])

AC_MSG_CHECKING([for gcc __sync builtins])
have_sync_builtins=no
AC_TRY_LINK(
  [], [unsigned long val; __sync_bool_compare_and_swap(&val, 0, 1); __sync_add_and_fetch(&val, 1); __sync_sub_and_fetch(&val, 1);],
  [have_sync_builtins=yes],
)
if test "x$have_sync_builtins" = "xyes"; then
  AC_DEFINE([HAVE_SYNC_BUILTINS], [1],
    [Define to 1 if gcc's __sync builtins are available])
  json_have_sync_builtins=1
else
  json_have_sync_builtins=0
fi
AC_SUBST([json_have_sync_builtins])
AC_MSG_RESULT([$have_sync_builtins])

AC_MSG_CHECKING([for gcc __atomic builtins])
have_atomic_builtins=no
AC_TRY_LINK(
  [], [char l; unsigned long v; __atomic_test_and_set(&l, __ATOMIC_RELAXED); __atomic_store_n(&v, 1, __ATOMIC_RELEASE); __atomic_load_n(&v, __ATOMIC_ACQUIRE); __atomic_add_fetch(&v, 1, __ATOMIC_ACQUIRE); __atomic_sub_fetch(&v, 1, __ATOMIC_RELEASE);],
  [have_atomic_builtins=yes],
)
if test "x$have_atomic_builtins" = "xyes"; then
  AC_DEFINE([HAVE_ATOMIC_BUILTINS], [1],
    [Define to 1 if gcc's __atomic builtins are available])
  json_have_atomic_builtins=1
else
  json_have_atomic_builtins=0
fi
AC_SUBST([json_have_atomic_builtins])
AC_MSG_RESULT([$have_atomic_builtins])

case "$ac_cv_type_long_long_int$ac_cv_func_strtoll" in
     yesyes) json_have_long_long=1;;
     *) json_have_long_long=0;;
esac
AC_SUBST([json_have_long_long])

case "$ac_cv_header_locale_h$ac_cv_func_localeconv" in
     yesyes) json_have_localeconv=1;;
     *) json_have_localeconv=0;;
esac
AC_SUBST([json_have_localeconv])

# Features
AC_ARG_ENABLE([urandom],
  [AS_HELP_STRING([--disable-urandom],
    [Don't use /dev/urandom to seed the hash function])],
  [use_urandom=$enableval], [use_urandom=yes])

if test "x$use_urandom" = xyes; then
AC_DEFINE([USE_URANDOM], [1],
  [Define to 1 if /dev/urandom should be used for seeding the hash function])
fi

AC_ARG_ENABLE([windows-cryptoapi],
  [AS_HELP_STRING([--disable-windows-cryptoapi],
    [Don't use CryptGenRandom to seed the hash function])],
  [use_windows_cryptoapi=$enableval], [use_windows_cryptoapi=yes])

if test "x$use_windows_cryptoapi" = xyes; then
AC_DEFINE([USE_WINDOWS_CRYPTOAPI], [1],
  [Define to 1 if CryptGenRandom should be used for seeding the hash function])
fi

AC_ARG_ENABLE([initial-hashtable-order],
  [AS_HELP_STRING([--enable-initial-hashtable-order=VAL],
    [Number of buckets new object hashtables contain is 2 raised to this power. The default is 3, so empty hashtables contain 2^3 = 8 buckets.])],
  [initial_hashtable_order=$enableval], [initial_hashtable_order=3])
AC_DEFINE_UNQUOTED([INITIAL_HASHTABLE_ORDER], [$initial_hashtable_order],
  [Number of buckets new object hashtables contain is 2 raised to this power. E.g. 3 -> 2^3 = 8.])

AC_ARG_ENABLE([Bsymbolic],
  [AS_HELP_STRING([--disable-Bsymbolic],
    [Avoid linking with -Bsymbolic-function])],
  [], [with_Bsymbolic=check])

if test "x$with_Bsymbolic" != "xno" ; then
    AC_MSG_CHECKING([for -Bsymbolic-functions linker flag])
    saved_LDFLAGS="${LDFLAGS}"
    LDFLAGS=-Wl,-Bsymbolic-functions
    AC_TRY_LINK(
      [], [int main (void) { return 0; }],
      [AC_MSG_RESULT([yes])
       have_Bsymbolic=yes],
      [AC_MSG_RESULT([no])
       have_Bsymbolic=no]
    )
    LDFLAGS="${saved_LDFLAGS}"

    if test "x$with_Bsymbolic" = "xcheck" ; then
        with_Bsymbolic=$have_Bsymbolic;
    fi
    if test "x$with_Bsymbolic:x$have_Bsymbolic" = "xyes:xno" ; then
        AC_MSG_ERROR([linker support is required for -Bsymbolic])
    fi
fi

AS_IF([test "x$with_Bsymbolic" = "xyes"], [JSON_BSYMBOLIC_LDFLAGS=-Wl[,]-Bsymbolic-functions])
AC_SUBST(JSON_BSYMBOLIC_LDFLAGS)

# Enable symbol versioning on GNU libc
JSON_SYMVER_LDFLAGS=
AC_CHECK_DECL([__GLIBC__], [JSON_SYMVER_LDFLAGS=-Wl,--default-symver])
AC_SUBST([JSON_SYMVER_LDFLAGS])

AC_ARG_ENABLE([ossfuzzers],
  [AS_HELP_STRING([--enable-ossfuzzers],
    [Whether to generate the fuzzers for OSS-Fuzz])],
  [have_ossfuzzers=yes], [have_ossfuzzers=no])
AM_CONDITIONAL([USE_OSSFUZZERS], [test "x$have_ossfuzzers" = "xyes"])


AC_SUBST([LIB_FUZZING_ENGINE])
AM_CONDITIONAL([USE_OSSFUZZ_FLAG], [test "x$LIB_FUZZING_ENGINE" = "x-fsanitize=fuzzer"])
AM_CONDITIONAL([USE_OSSFUZZ_STATIC], [test -f "$LIB_FUZZING_ENGINE"])


if test x$GCC = xyes; then
    AC_MSG_CHECKING(for -Wno-format-truncation)
    wnoformat_truncation="-Wno-format-truncation"
    AS_IF([${CC} -Wno-format-truncation -Werror -S -o /dev/null -xc /dev/null > /dev/null 2>&1],
      [AC_MSG_RESULT(yes)],
      [AC_MSG_RESULT(no)
      wnoformat_truncation=""])

    AM_CFLAGS="-Wall -Wextra -Wdeclaration-after-statement -Wshadow ${wnoformat_truncation}"
fi
AC_SUBST([AM_CFLAGS])

AC_CONFIG_FILES([
        jansson.pc
        Makefile
        doc/Makefile
        src/Makefile
        src/jansson_config.h
        test/Makefile
        test/bin/Makefile
        test/ossfuzz/Makefile
        test/suites/Makefile
        test/suites/api/Makefile
])
AC_OUTPUT
