/* Private header for tzdb code.  */

#ifndef PRIVATE_H

#define PRIVATE_H

/*
** This file is in the public domain, so clarified as of
** 1996-06-05 by Arthur David Olson.
*/

/*
** This header is for use ONLY with the time conversion code.
** There is no guarantee that it will remain unchanged,
** or that it will remain at all.
** Do NOT copy it to any system include directory.
** Thank you!
*/

/* PORT_TO_C89 means the code should work even if the underlying
   compiler and library support only C89 plus C99's 'long long'
   and perhaps a few other extensions to C89.

   This macro is obsolescent, and the plan is to remove it along with
   associated code.  A good time to do that might be in the year 2029
   because RHEL 7 (whose GCC defaults to C89) extended life cycle
   support (ELS) is scheduled to end on 2028-06-30.  */
#ifndef PORT_TO_C89
# define PORT_TO_C89 0
#endif

/* SUPPORT_C89 means the tzcode library should support C89 callers
   in addition to the usual support for C99-and-later callers.
   This defaults to 1 as POSIX requires, even though that can trigger
   latent bugs in callers.  */
#ifndef SUPPORT_C89
# define SUPPORT_C89 1
#endif

#ifndef __STDC_VERSION__
# define __STDC_VERSION__ 0
#endif

/* Define true, false and bool if they don't work out of the box.  */
#if PORT_TO_C89 && __STDC_VERSION__ < 199901
# define true 1
# define false 0
# define bool int
#elif __STDC_VERSION__ < 202311
# include <stdbool.h>
#endif

#if __STDC_VERSION__ < 202311
# undef static_assert
# define static_assert(cond) extern int static_assert_check[(cond) ? 1 : -1]
#endif

/*
** zdump has been made independent of the rest of the time
** conversion package to increase confidence in the verification it provides.
** You can use zdump to help in verifying other implementations.
** To do this, compile with -DUSE_LTZ=0 and link without the tz library.
*/
#ifndef USE_LTZ
# define USE_LTZ 1
#endif

/* This string was in the Factory zone through version 2016f.  */
#define GRANDPARENTED	"Local time zone must be set--see zic manual page"

/*
** Defaults for preprocessor symbols.
** You can override these in your C compiler options, e.g. '-DHAVE_GETTEXT=1'.
*/

#if !defined HAVE__GENERIC && defined __has_extension
# if !__has_extension(c_generic_selections)
#  define HAVE__GENERIC 0
# endif
#endif
/* _Generic is buggy in pre-4.9 GCC.  */
#if !defined HAVE__GENERIC && defined __GNUC__ && !defined __STRICT_ANSI__
# define HAVE__GENERIC (4 < __GNUC__ + (9 <= __GNUC_MINOR__))
#endif
#ifndef HAVE__GENERIC
# define HAVE__GENERIC (201112 <= __STDC_VERSION__)
#endif

#if !defined HAVE_GETTEXT && defined __has_include
# if __has_include(<libintl.h>)
#  define HAVE_GETTEXT true
# endif
#endif
#ifndef HAVE_GETTEXT
# define HAVE_GETTEXT false
#endif

#ifndef HAVE_INCOMPATIBLE_CTIME_R
# define HAVE_INCOMPATIBLE_CTIME_R 0
#endif

#ifndef HAVE_LINK
# define HAVE_LINK 1
#endif /* !defined HAVE_LINK */

#ifndef HAVE_MALLOC_ERRNO
# define HAVE_MALLOC_ERRNO 1
#endif

#ifndef HAVE_POSIX_DECLS
# define HAVE_POSIX_DECLS 1
#endif

#ifndef HAVE_SETENV
# define HAVE_SETENV 1
#endif

#ifndef HAVE_STRDUP
# define HAVE_STRDUP 1
#endif

#ifndef HAVE_SYMLINK
# define HAVE_SYMLINK 1
#endif /* !defined HAVE_SYMLINK */

#if !defined HAVE_SYS_STAT_H && defined __has_include
# if !__has_include(<sys/stat.h>)
#  define HAVE_SYS_STAT_H false
# endif
#endif
#ifndef HAVE_SYS_STAT_H
# define HAVE_SYS_STAT_H true
#endif

#if !defined HAVE_UNISTD_H && defined __has_include
# if !__has_include(<unistd.h>)
#  define HAVE_UNISTD_H false
# endif
#endif
#ifndef HAVE_UNISTD_H
# define HAVE_UNISTD_H true
#endif

#ifndef NETBSD_INSPIRED
# define NETBSD_INSPIRED 1
#endif

#if HAVE_INCOMPATIBLE_CTIME_R
# define asctime_r _incompatible_asctime_r
# define ctime_r _incompatible_ctime_r
#endif /* HAVE_INCOMPATIBLE_CTIME_R */

/* Enable tm_gmtoff, tm_zone, and environ on GNUish systems.  */
#define _GNU_SOURCE 1
/* Fix asctime_r on Solaris 11.  */
#define _POSIX_PTHREAD_SEMANTICS 1
/* Enable strtoimax on pre-C99 Solaris 11.  */
#define __EXTENSIONS__ 1

/* On GNUish systems where time_t might be 32 or 64 bits, use 64.
   On these platforms _FILE_OFFSET_BITS must also be 64; otherwise
   setting _TIME_BITS to 64 does not work.  The code does not
   otherwise rely on _FILE_OFFSET_BITS being 64, since it does not
   use off_t or functions like 'stat' that depend on off_t.  */
#ifndef _TIME_BITS
# ifndef _FILE_OFFSET_BITS
#  define _FILE_OFFSET_BITS 64
# endif
# if _FILE_OFFSET_BITS == 64
#  define _TIME_BITS 64
# endif
#endif

/*
** Nested includes
*/

/* Avoid clashes with NetBSD by renaming NetBSD's declarations.
   If defining the 'timezone' variable, avoid a clash with FreeBSD's
   'timezone' function by renaming its declaration.  */
#define localtime_rz sys_localtime_rz
#define mktime_z sys_mktime_z
#define posix2time_z sys_posix2time_z
#define time2posix_z sys_time2posix_z
#if defined USG_COMPAT && USG_COMPAT == 2
# define timezone sys_timezone
#endif
#define timezone_t sys_timezone_t
#define tzalloc sys_tzalloc
#define tzfree sys_tzfree
#include <time.h>
#undef localtime_rz
#undef mktime_z
#undef posix2time_z
#undef time2posix_z
#if defined USG_COMPAT && USG_COMPAT == 2
# undef timezone
#endif
#undef timezone_t
#undef tzalloc
#undef tzfree

#include <stddef.h>
#include <string.h>
#if !PORT_TO_C89
# include <inttypes.h>
#endif
#include <limits.h>	/* for CHAR_BIT et al. */
#include <stdlib.h>

#include <errno.h>

#ifndef EINVAL
# define EINVAL ERANGE
#endif

#ifndef ELOOP
# define ELOOP EINVAL
#endif
#ifndef ENAMETOOLONG
# define ENAMETOOLONG EINVAL
#endif
#ifndef ENOMEM
# define ENOMEM EINVAL
#endif
#ifndef ENOTSUP
# define ENOTSUP EINVAL
#endif
#ifndef EOVERFLOW
# define EOVERFLOW EINVAL
#endif

#if HAVE_GETTEXT
# include <libintl.h>
#endif /* HAVE_GETTEXT */

#if HAVE_UNISTD_H
# include <unistd.h> /* for R_OK, and other POSIX goodness */
#endif /* HAVE_UNISTD_H */

/* SUPPORT_POSIX2008 means the tzcode library should support
   POSIX.1-2017-and-earlier callers in addition to the usual support for
   POSIX.1-2024-and-later callers; however, this can be
   incompatible with POSIX.1-2024-and-later callers.
   This macro is obsolescent, and the plan is to remove it
   along with any code needed only when it is nonzero.
   A good time to do that might be in the year 2034.
   This macro's name is SUPPORT_POSIX2008 because _POSIX_VERSION == 200809
   in POSIX.1-2017, a minor revision of POSIX.1-2008.  */
#ifndef SUPPORT_POSIX2008
# if defined _POSIX_VERSION && _POSIX_VERSION <= 200809
#  define SUPPORT_POSIX2008 1
# else
#  define SUPPORT_POSIX2008 0
# endif
#endif

#ifndef HAVE_DECL_ASCTIME_R
# if SUPPORT_POSIX2008
#  define HAVE_DECL_ASCTIME_R 1
# else
#  define HAVE_DECL_ASCTIME_R 0
# endif
#endif

#ifndef HAVE_STRFTIME_L
# if _POSIX_VERSION < 200809
#  define HAVE_STRFTIME_L 0
# else
#  define HAVE_STRFTIME_L 1
# endif
#endif

#ifndef USG_COMPAT
# ifndef _XOPEN_VERSION
#  define USG_COMPAT 0
# else
#  define USG_COMPAT 1
# endif
#endif

#ifndef HAVE_TZNAME
# if _POSIX_VERSION < 198808 && !USG_COMPAT
#  define HAVE_TZNAME 0
# else
#  define HAVE_TZNAME 1
# endif
#endif

#ifndef ALTZONE
# if defined __sun || defined _M_XENIX
#  define ALTZONE 1
# else
#  define ALTZONE 0
# endif
#endif

#ifndef R_OK
# define R_OK 4
#endif /* !defined R_OK */

#if PORT_TO_C89

/*
** Define HAVE_STDINT_H's default value here, rather than at the
** start, since __GLIBC__ and INTMAX_MAX's values depend on
** previously included files.  glibc 2.1 and Solaris 10 and later have
** stdint.h, even with pre-C99 compilers.
*/
#if !defined HAVE_STDINT_H && defined __has_include
# define HAVE_STDINT_H true /* C23 __has_include implies C99 stdint.h.  */
#endif
#ifndef HAVE_STDINT_H
# define HAVE_STDINT_H \
   (199901 <= __STDC_VERSION__ \
    || 2 < __GLIBC__ + (1 <= __GLIBC_MINOR__) \
    || __CYGWIN__ || INTMAX_MAX)
#endif /* !defined HAVE_STDINT_H */

#if HAVE_STDINT_H
# include <stdint.h>
#endif /* !HAVE_STDINT_H */

#ifndef HAVE_INTTYPES_H
# define HAVE_INTTYPES_H HAVE_STDINT_H
#endif
#if HAVE_INTTYPES_H
# include <inttypes.h>
#endif

/* Pre-C99 GCC compilers define __LONG_LONG_MAX__ instead of LLONG_MAX.  */
#if defined __LONG_LONG_MAX__ && !defined __STRICT_ANSI__
# ifndef LLONG_MAX
#  define LLONG_MAX __LONG_LONG_MAX__
# endif
# ifndef LLONG_MIN
#  define LLONG_MIN (-1 - LLONG_MAX)
# endif
# ifndef ULLONG_MAX
#  define ULLONG_MAX (LLONG_MAX * 2ull + 1)
# endif
#endif

#ifndef INT_FAST64_MAX
# if 1 <= LONG_MAX >> 31 >> 31
typedef long int_fast64_t;
#  define INT_FAST64_MIN LONG_MIN
#  define INT_FAST64_MAX LONG_MAX
# else
/* If this fails, compile with -DHAVE_STDINT_H or with a better compiler.  */
typedef long long int_fast64_t;
#  define INT_FAST64_MIN LLONG_MIN
#  define INT_FAST64_MAX LLONG_MAX
# endif
#endif

#ifndef PRIdFAST64
# if INT_FAST64_MAX == LONG_MAX
#  define PRIdFAST64 "ld"
# else
#  define PRIdFAST64 "lld"
# endif
#endif

#ifndef SCNdFAST64
# define SCNdFAST64 PRIdFAST64
#endif

#ifndef INT_FAST32_MAX
# if INT_MAX >> 31 == 0
typedef long int_fast32_t;
#  define INT_FAST32_MAX LONG_MAX
#  define INT_FAST32_MIN LONG_MIN
# else
typedef int int_fast32_t;
#  define INT_FAST32_MAX INT_MAX
#  define INT_FAST32_MIN INT_MIN
# endif
#endif

#ifndef INTMAX_MAX
# ifdef LLONG_MAX
typedef long long intmax_t;
#  ifndef HAVE_STRTOLL
#   define HAVE_STRTOLL true
#  endif
#  if HAVE_STRTOLL
#   define strtoimax strtoll
#  endif
#  define INTMAX_MAX LLONG_MAX
#  define INTMAX_MIN LLONG_MIN
# else
typedef long intmax_t;
#  define INTMAX_MAX LONG_MAX
#  define INTMAX_MIN LONG_MIN
# endif
# ifndef strtoimax
#  define strtoimax strtol
# endif
#endif

#ifndef PRIdMAX
# if INTMAX_MAX == LLONG_MAX
#  define PRIdMAX "lld"
# else
#  define PRIdMAX "ld"
# endif
#endif

#ifndef PTRDIFF_MAX
# define PTRDIFF_MAX MAXVAL(ptrdiff_t, TYPE_BIT(ptrdiff_t))
#endif

#ifndef UINT_FAST32_MAX
typedef unsigned long uint_fast32_t;
#endif

#ifndef UINT_FAST64_MAX
# if 3 <= ULONG_MAX >> 31 >> 31
typedef unsigned long uint_fast64_t;
#  define UINT_FAST64_MAX ULONG_MAX
# else
/* If this fails, compile with -DHAVE_STDINT_H or with a better compiler.  */
typedef unsigned long long uint_fast64_t;
#  define UINT_FAST64_MAX ULLONG_MAX
# endif
#endif

#ifndef UINTMAX_MAX
# ifdef ULLONG_MAX
typedef unsigned long long uintmax_t;
#  define UINTMAX_MAX ULLONG_MAX
# else
typedef unsigned long uintmax_t;
#  define UINTMAX_MAX ULONG_MAX
# endif
#endif

#ifndef PRIuMAX
# ifdef ULLONG_MAX
#  define PRIuMAX "llu"
# else
#  define PRIuMAX "lu"
# endif
#endif

#ifndef SIZE_MAX
# define SIZE_MAX ((size_t) -1)
#endif

#endif /* PORT_TO_C89 */

/* The maximum size of any created object, as a signed integer.
   Although the C standard does not outright prohibit larger objects,
   behavior is undefined if the result of pointer subtraction does not
   fit into ptrdiff_t, and the code assumes in several places that
   pointer subtraction works.  As a practical matter it's OK to not
   support objects larger than this.  */
#define INDEX_MAX ((ptrdiff_t) min(PTRDIFF_MAX, SIZE_MAX))

/* Support ckd_add, ckd_sub, ckd_mul on C23 or recent-enough GCC-like
   hosts, unless compiled with -DHAVE_STDCKDINT_H=0 or with pre-C23 EDG.  */
#if !defined HAVE_STDCKDINT_H && defined __has_include
# if __has_include(<stdckdint.h>)
#  define HAVE_STDCKDINT_H true
# endif
#endif
#ifdef HAVE_STDCKDINT_H
# if HAVE_STDCKDINT_H
#  include <stdckdint.h>
# endif
#elif defined __EDG__
/* Do nothing, to work around EDG bug <https://bugs.gnu.org/53256>.  */
#elif defined __has_builtin
# if __has_builtin(__builtin_add_overflow)
#  define ckd_add(r, a, b) __builtin_add_overflow(a, b, r)
# endif
# if __has_builtin(__builtin_sub_overflow)
#  define ckd_sub(r, a, b) __builtin_sub_overflow(a, b, r)
# endif
# if __has_builtin(__builtin_mul_overflow)
#  define ckd_mul(r, a, b) __builtin_mul_overflow(a, b, r)
# endif
#elif 7 <= __GNUC__
# define ckd_add(r, a, b) __builtin_add_overflow(a, b, r)
# define ckd_sub(r, a, b) __builtin_sub_overflow(a, b, r)
# define ckd_mul(r, a, b) __builtin_mul_overflow(a, b, r)
#endif

#if (defined __has_c_attribute \
     && (202311 <= __STDC_VERSION__ || !defined __STRICT_ANSI__))
# define HAVE___HAS_C_ATTRIBUTE true
#else
# define HAVE___HAS_C_ATTRIBUTE false
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(deprecated)
#  define ATTRIBUTE_DEPRECATED [[deprecated]]
# endif
#endif
#ifndef ATTRIBUTE_DEPRECATED
# if 3 < __GNUC__ + (2 <= __GNUC_MINOR__)
#  define ATTRIBUTE_DEPRECATED __attribute__((deprecated))
# else
#  define ATTRIBUTE_DEPRECATED /* empty */
# endif
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(fallthrough)
#  define ATTRIBUTE_FALLTHROUGH [[fallthrough]]
# endif
#endif
#ifndef ATTRIBUTE_FALLTHROUGH
# if 7 <= __GNUC__
#  define ATTRIBUTE_FALLTHROUGH __attribute__((fallthrough))
# else
#  define ATTRIBUTE_FALLTHROUGH ((void) 0)
# endif
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(maybe_unused)
#  define ATTRIBUTE_MAYBE_UNUSED [[maybe_unused]]
# endif
#endif
#ifndef ATTRIBUTE_MAYBE_UNUSED
# if 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define ATTRIBUTE_MAYBE_UNUSED __attribute__((unused))
# else
#  define ATTRIBUTE_MAYBE_UNUSED /* empty */
# endif
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(noreturn)
#  define ATTRIBUTE_NORETURN [[noreturn]]
# endif
#endif
#ifndef ATTRIBUTE_NORETURN
# if 201112 <= __STDC_VERSION__
#  define ATTRIBUTE_NORETURN _Noreturn
# elif 2 < __GNUC__ + (8 <= __GNUC_MINOR__)
#  define ATTRIBUTE_NORETURN __attribute__((noreturn))
# else
#  define ATTRIBUTE_NORETURN /* empty */
# endif
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(reproducible)
#  define ATTRIBUTE_REPRODUCIBLE [[reproducible]]
# endif
#endif
#ifndef ATTRIBUTE_REPRODUCIBLE
# define ATTRIBUTE_REPRODUCIBLE /* empty */
#endif

#if HAVE___HAS_C_ATTRIBUTE
# if __has_c_attribute(unsequenced)
#  define ATTRIBUTE_UNSEQUENCED [[unsequenced]]
# endif
#endif
#ifndef ATTRIBUTE_UNSEQUENCED
# define ATTRIBUTE_UNSEQUENCED /* empty */
#endif

/* GCC attributes that are useful in tzcode.
   __attribute__((const)) is stricter than [[unsequenced]],
   so the latter is an adequate substitute in non-GCC C23 platforms.
   __attribute__((pure)) is stricter than [[reproducible]],
   so the latter is an adequate substitute in non-GCC C23 platforms.  */
#if __GNUC__ < 3
# define ATTRIBUTE_CONST ATTRIBUTE_UNSEQUENCED
# define ATTRIBUTE_FORMAT(spec) /* empty */
# define ATTRIBUTE_PURE ATTRIBUTE_REPRODUCIBLE
#else
# define ATTRIBUTE_CONST __attribute__((const))
# define ATTRIBUTE_FORMAT(spec) __attribute__((format spec))
# define ATTRIBUTE_PURE __attribute__((pure))
#endif

/* Avoid GCC bug 114833 <https://gcc.gnu.org/bugzilla/show_bug.cgi?id=114833>.
   Remove this macro and its uses when the bug is fixed in a GCC release,
   because only the latest GCC matters for $(GCC_DEBUG_FLAGS).  */
#ifdef GCC_LINT
# define ATTRIBUTE_PURE_114833 ATTRIBUTE_PURE
#else
# define ATTRIBUTE_PURE_114833 /* empty */
#endif

#if (__STDC_VERSION__ < 199901 && !defined restrict \
     && (PORT_TO_C89 || defined _MSC_VER))
# define restrict /* empty */
#endif

/*
** Workarounds for compilers/systems.
*/

#ifndef EPOCH_LOCAL
# define EPOCH_LOCAL 0
#endif
#ifndef EPOCH_OFFSET
# define EPOCH_OFFSET 0
#endif
#ifndef RESERVE_STD_EXT_IDS
# define RESERVE_STD_EXT_IDS 0
#endif

#ifdef time_tz
# define defined_time_tz true
#else
# define defined_time_tz false
#endif

/* If standard C identifiers with external linkage (e.g., localtime)
   are reserved and are not already being renamed anyway, rename them
   as if compiling with '-Dtime_tz=time_t'.  */
#if !defined time_tz && RESERVE_STD_EXT_IDS && USE_LTZ
# define time_tz time_t
#endif

/*
** Compile with -Dtime_tz=T to build the tz package with a private
** time_t type equivalent to T rather than the system-supplied time_t.
** This debugging feature can test unusual design decisions
** (e.g., time_t wider than 'long', or unsigned time_t) even on
** typical platforms.
*/
#if defined time_tz || EPOCH_LOCAL || EPOCH_OFFSET != 0
# define TZ_TIME_T 1
#else
# define TZ_TIME_T 0
#endif

#if defined LOCALTIME_IMPLEMENTATION && TZ_TIME_T
static time_t sys_time(time_t *x) { return time(x); }
#endif

#if TZ_TIME_T

typedef time_tz tz_time_t;

# undef  asctime
# define asctime tz_asctime
# undef  ctime
# define ctime tz_ctime
# undef  difftime
# define difftime tz_difftime
# undef  gmtime
# define gmtime tz_gmtime
# undef  gmtime_r
# define gmtime_r tz_gmtime_r
# undef  localtime
# define localtime tz_localtime
# undef  localtime_r
# define localtime_r tz_localtime_r
# undef  localtime_rz
# define localtime_rz tz_localtime_rz
# undef  mktime
# define mktime tz_mktime
# undef  mktime_z
# define mktime_z tz_mktime_z
# undef  offtime
# define offtime tz_offtime
# undef  posix2time
# define posix2time tz_posix2time
# undef  posix2time_z
# define posix2time_z tz_posix2time_z
# undef  strftime
# define strftime tz_strftime
# undef  time
# define time tz_time
# undef  time2posix
# define time2posix tz_time2posix
# undef  time2posix_z
# define time2posix_z tz_time2posix_z
# undef  time_t
# define time_t tz_time_t
# undef  timegm
# define timegm tz_timegm
# undef  timelocal
# define timelocal tz_timelocal
# undef  timeoff
# define timeoff tz_timeoff
# undef  tzalloc
# define tzalloc tz_tzalloc
# undef  tzfree
# define tzfree tz_tzfree
# undef  tzset
# define tzset tz_tzset
# if SUPPORT_POSIX2008
#  undef  asctime_r
#  define asctime_r tz_asctime_r
#  undef  ctime_r
#  define ctime_r tz_ctime_r
# endif
# if HAVE_STRFTIME_L
#  undef  strftime_l
#  define strftime_l tz_strftime_l
# endif
# if HAVE_TZNAME
#  undef  tzname
#  define tzname tz_tzname
# endif
# if USG_COMPAT
#  undef  daylight
#  define daylight tz_daylight
#  undef  timezone
#  define timezone tz_timezone
# endif
# if ALTZONE
#  undef  altzone
#  define altzone tz_altzone
# endif

# if __STDC_VERSION__ < 202311
#  define DEPRECATED_IN_C23 /* empty */
# else
#  define DEPRECATED_IN_C23 ATTRIBUTE_DEPRECATED
# endif
DEPRECATED_IN_C23 char *asctime(struct tm const *);
DEPRECATED_IN_C23 char *ctime(time_t const *);
#if SUPPORT_POSIX2008
char *asctime_r(struct tm const *restrict, char *restrict);
char *ctime_r(time_t const *, char *);
#endif
ATTRIBUTE_CONST double difftime(time_t, time_t);
size_t strftime(char *restrict, size_t, char const *restrict,
		struct tm const *restrict);
# if HAVE_STRFTIME_L
size_t strftime_l(char *restrict, size_t, char const *restrict,
		  struct tm const *restrict, locale_t);
# endif
struct tm *gmtime(time_t const *);
struct tm *gmtime_r(time_t const *restrict, struct tm *restrict);
struct tm *localtime(time_t const *);
struct tm *localtime_r(time_t const *restrict, struct tm *restrict);
time_t mktime(struct tm *);
time_t time(time_t *);
time_t timegm(struct tm *);
void tzset(void);
#endif

#ifndef HAVE_DECL_TIMEGM
# if (202311 <= __STDC_VERSION__ \
      || defined __GLIBC__ || defined __tm_zone /* musl */ \
      || defined __FreeBSD__ || defined __NetBSD__ || defined __OpenBSD__ \
      || (defined __APPLE__ && defined __MACH__))
#  define HAVE_DECL_TIMEGM true
# else
#  define HAVE_DECL_TIMEGM false
# endif
#endif
#if !HAVE_DECL_TIMEGM && !defined timegm
time_t timegm(struct tm *);
#endif

#if !HAVE_DECL_ASCTIME_R && !defined asctime_r && SUPPORT_POSIX2008
extern char *asctime_r(struct tm const *restrict, char *restrict);
#endif

#ifndef HAVE_DECL_ENVIRON
# if defined environ || defined __USE_GNU
#  define HAVE_DECL_ENVIRON 1
# else
#  define HAVE_DECL_ENVIRON 0
# endif
#endif

#if !HAVE_DECL_ENVIRON
extern char **environ;
#endif

#if 2 <= HAVE_TZNAME + (TZ_TIME_T || !HAVE_POSIX_DECLS)
extern char *tzname[];
#endif
#if 2 <= USG_COMPAT + (TZ_TIME_T || !HAVE_POSIX_DECLS)
extern long timezone;
extern int daylight;
#endif
#if 2 <= ALTZONE + (TZ_TIME_T || !HAVE_POSIX_DECLS)
extern long altzone;
#endif

/*
** The STD_INSPIRED functions are similar, but most also need
** declarations if time_tz is defined.
*/

#ifndef STD_INSPIRED
# define STD_INSPIRED 0
#endif
#if STD_INSPIRED
# if TZ_TIME_T || !defined offtime
struct tm *offtime(time_t const *, long);
# endif
# if TZ_TIME_T || !defined timelocal
time_t timelocal(struct tm *);
# endif
# if TZ_TIME_T || !defined timeoff
#  define EXTERN_TIMEOFF
# endif
# if TZ_TIME_T || !defined time2posix
time_t time2posix(time_t);
# endif
# if TZ_TIME_T || !defined posix2time
time_t posix2time(time_t);
# endif
#endif

/* Infer TM_ZONE on systems where this information is known, but suppress
   guessing if NO_TM_ZONE is defined.  Similarly for TM_GMTOFF.  */
#if (200809 < _POSIX_VERSION \
     || defined __GLIBC__ \
     || defined __tm_zone /* musl */ \
     || defined __FreeBSD__ || defined __NetBSD__ || defined __OpenBSD__ \
     || (defined __APPLE__ && defined __MACH__))
# if !defined TM_GMTOFF && !defined NO_TM_GMTOFF
#  define TM_GMTOFF tm_gmtoff
# endif
# if !defined TM_ZONE && !defined NO_TM_ZONE
#  define TM_ZONE tm_zone
# endif
#endif

/*
** Define functions that are ABI compatible with NetBSD but have
** better prototypes.  NetBSD 6.1.4 defines a pointer type timezone_t
** and labors under the misconception that 'const timezone_t' is a
** pointer to a constant.  This use of 'const' is ineffective, so it
** is not done here.  What we call 'struct state' NetBSD calls
** 'struct __state', but this is a private name so it doesn't matter.
*/
#if NETBSD_INSPIRED
typedef struct state *timezone_t;
struct tm *localtime_rz(timezone_t restrict, time_t const *restrict,
			struct tm *restrict);
time_t mktime_z(timezone_t restrict, struct tm *restrict);
timezone_t tzalloc(char const *);
void tzfree(timezone_t);
# if STD_INSPIRED
#  if TZ_TIME_T || !defined posix2time_z
ATTRIBUTE_PURE time_t posix2time_z(timezone_t, time_t);
#  endif
#  if TZ_TIME_T || !defined time2posix_z
ATTRIBUTE_PURE time_t time2posix_z(timezone_t, time_t);
#  endif
# endif
#endif

/*
** Finally, some convenience items.
*/

#define TYPE_BIT(type) (CHAR_BIT * (ptrdiff_t) sizeof(type))
#define TYPE_SIGNED(type) (((type) -1) < 0)
#define TWOS_COMPLEMENT(t) ((t) ~ (t) 0 < 0)

/* Minimum and maximum of two values.  Use lower case to avoid
   naming clashes with standard include files.  */
#define max(a, b) ((a) > (b) ? (a) : (b))
#define min(a, b) ((a) < (b) ? (a) : (b))

/* Max and min values of the integer type T, of which only the bottom
   B bits are used, and where the highest-order used bit is considered
   to be a sign bit if T is signed.  */
#define MAXVAL(t, b)						\
  ((t) (((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))			\
	- 1 + ((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))))
#define MINVAL(t, b)						\
  ((t) (TYPE_SIGNED(t) ? - TWOS_COMPLEMENT(t) - MAXVAL(t, b) : 0))

/* The extreme time values, assuming no padding.  */
#define TIME_T_MIN_NO_PADDING MINVAL(time_t, TYPE_BIT(time_t))
#define TIME_T_MAX_NO_PADDING MAXVAL(time_t, TYPE_BIT(time_t))

/* The extreme time values.  These are macros, not constants, so that
   any portability problems occur only when compiling .c files that use
   the macros, which is safer for applications that need only zdump and zic.
   This implementation assumes no padding if time_t is signed and
   either the compiler lacks support for _Generic or time_t is not one
   of the standard signed integer types.  */
#if HAVE__GENERIC
# define TIME_T_MIN \
    _Generic((time_t) 0, \
	     signed char: SCHAR_MIN, short: SHRT_MIN, \
	     int: INT_MIN, long: LONG_MIN, long long: LLONG_MIN, \
	     default: TIME_T_MIN_NO_PADDING)
# define TIME_T_MAX \
    (TYPE_SIGNED(time_t) \
     ? _Generic((time_t) 0, \
		signed char: SCHAR_MAX, short: SHRT_MAX, \
		int: INT_MAX, long: LONG_MAX, long long: LLONG_MAX, \
		default: TIME_T_MAX_NO_PADDING)			    \
     : (time_t) -1)
enum { SIGNED_PADDING_CHECK_NEEDED
         = _Generic((time_t) 0,
		    signed char: false, short: false,
		    int: false, long: false, long long: false,
		    default: true) };
#else
# define TIME_T_MIN TIME_T_MIN_NO_PADDING
# define TIME_T_MAX TIME_T_MAX_NO_PADDING
enum { SIGNED_PADDING_CHECK_NEEDED = true };
#endif
/* Try to check the padding assumptions.  Although TIME_T_MAX and the
   following check can both have undefined behavior on oddball
   platforms due to shifts exceeding widths of signed integers, these
   platforms' compilers are likely to diagnose these issues in integer
   constant expressions, so it shouldn't hurt to check statically.  */
static_assert(! TYPE_SIGNED(time_t) || ! SIGNED_PADDING_CHECK_NEEDED
	      || TIME_T_MAX >> (TYPE_BIT(time_t) - 2) == 1);

/* If true, the value returned by an idealized unlimited-range mktime
   always fits into an integer type with bounds MIN and MAX.
   If false, the value might not fit.
   This macro is usable in #if if its arguments are.
   Add or subtract 2**31 - 1 for the maximum UT offset allowed in a TZif file,
   divide by the maximum number of non-leap seconds in a year,
   divide again by two just to be safe,
   and account for the tm_year origin (1900) and time_t origin (1970).  */
#define MKTIME_FITS_IN(min, max) \
  ((min) < 0 \
   && ((min) + 0x7fffffff) / 366 / 24 / 60 / 60 / 2 + 1970 - 1900 < INT_MIN \
   && INT_MAX < ((max) - 0x7fffffff) / 366 / 24 / 60 / 60 / 2 + 1970 - 1900)

/* MKTIME_MIGHT_OVERFLOW is true if mktime can fail due to time_t overflow
   or if it is not known whether mktime can fail,
   and is false if mktime definitely cannot fail.
   This macro is usable in #if, and so does not use TIME_T_MAX or sizeof.
   If the builder has not configured this macro, guess based on what
   known platforms do.  When in doubt, guess true.  */
#ifndef MKTIME_MIGHT_OVERFLOW
# if defined __FreeBSD__ || defined __NetBSD__ || defined __OpenBSD__
#  include <sys/param.h>
# endif
# if ((/* The following heuristics assume native time_t.  */ \
       defined_time_tz) \
      || ((/* Traditional time_t is 'long', so if 'long' is not wide enough \
	      assume overflow unless we're on a known-safe host.  */ \
	   !MKTIME_FITS_IN(LONG_MIN, LONG_MAX)) \
	  && (/* GNU C Library 2.29 (2019-02-01) and later has 64-bit time_t \
		 if __TIMESIZE is 64.  */ \
	      !defined __TIMESIZE || __TIMESIZE < 64) \
	  && (/* FreeBSD 12 r320347 (__FreeBSD_version 1200036; 2017-06-26), \
		 and later has 64-bit time_t on all platforms but i386 which \
		 is currently scheduled for end-of-life on 2028-11-30.  */ \
	      !defined __FreeBSD_version || __FreeBSD_version < 1200036 \
	      || defined __i386) \
	  && (/* NetBSD 6.0 (2012-10-17) and later has 64-bit time_t.  */ \
	      !defined __NetBSD_Version__ || __NetBSD_Version__ < 600000000) \
	  && (/* OpenBSD 5.5 (2014-05-01) and later has 64-bit time_t.  */ \
	      !defined OpenBSD || OpenBSD < 201405)))
#  define MKTIME_MIGHT_OVERFLOW true
# else
#  define MKTIME_MIGHT_OVERFLOW false
# endif
#endif
/* Check that MKTIME_MIGHT_OVERFLOW is consistent with time_t's range.  */
static_assert(MKTIME_MIGHT_OVERFLOW
	      || MKTIME_FITS_IN(TIME_T_MIN, TIME_T_MAX));

/*
** 302 / 1000 is log10(2.0) rounded up.
** Subtract one for the sign bit if the type is signed;
** add one for integer division truncation;
** add one more for a minus sign if the type is signed.
*/
#define INT_STRLEN_MAXIMUM(type) \
	((TYPE_BIT(type) - TYPE_SIGNED(type)) * 302 / 1000 + \
	1 + TYPE_SIGNED(type))

/*
** INITIALIZE(x)
*/

#ifdef GCC_LINT
# define INITIALIZE(x)	((x) = 0)
#else
# define INITIALIZE(x)
#endif

/* Whether memory access must strictly follow the C standard.
   If 0, it's OK to read uninitialized storage so long as the value is
   not relied upon.  Defining it to 0 lets mktime access parts of
   struct tm that might be uninitialized, as a heuristic when the
   standard doesn't say what to return and when tm_gmtoff can help
   mktime likely infer a better value.  */
#ifndef UNINIT_TRAP
# define UNINIT_TRAP 0
#endif

/* strftime.c sometimes needs access to timeoff if it is not already public.
   tz_private_timeoff should be used only by localtime.c and strftime.c.  */
#if (!defined EXTERN_TIMEOFF && ! (defined USE_TIMEX_T && USE_TIMEX_T) \
     && defined TM_GMTOFF && (200809 < _POSIX_VERSION || ! UNINIT_TRAP))
# ifndef timeoff
#  define timeoff tz_private_timeoff
# endif
# define EXTERN_TIMEOFF
#endif
#ifdef EXTERN_TIMEOFF
time_t timeoff(struct tm *, long);
#endif

#ifdef DEBUG
# undef unreachable
# define unreachable() abort()
#elif !defined unreachable
# ifdef __has_builtin
#  if __has_builtin(__builtin_unreachable)
#   define unreachable() __builtin_unreachable()
#  endif
# elif 4 < __GNUC__ + (5 <= __GNUC_MINOR__)
#  define unreachable() __builtin_unreachable()
# endif
# ifndef unreachable
#  define unreachable() ((void) 0)
# endif
#endif

/*
** For the benefit of GNU folk...
** '_(MSGID)' uses the current locale's message library string for MSGID.
** The default is to use gettext if available, and use MSGID otherwise.
*/

#if HAVE_GETTEXT
#define _(msgid) gettext(msgid)
#else /* !HAVE_GETTEXT */
#define _(msgid) msgid
#endif /* !HAVE_GETTEXT */

#if !defined TZ_DOMAIN && defined HAVE_GETTEXT
# define TZ_DOMAIN "tz"
#endif

#if HAVE_INCOMPATIBLE_CTIME_R
#undef asctime_r
#undef ctime_r
char *asctime_r(struct tm const *restrict, char *restrict);
char *ctime_r(time_t const *, char *);
#endif /* HAVE_INCOMPATIBLE_CTIME_R */

/* Handy macros that are independent of tzfile implementation.  */

enum {
  SECSPERMIN = 60,
  MINSPERHOUR = 60,
  SECSPERHOUR = SECSPERMIN * MINSPERHOUR,
  HOURSPERDAY = 24,
  DAYSPERWEEK = 7,
  DAYSPERNYEAR = 365,
  DAYSPERLYEAR = DAYSPERNYEAR + 1,
  MONSPERYEAR = 12,
  YEARSPERREPEAT = 400	/* years before a Gregorian repeat */
};

#define SECSPERDAY	((int_fast32_t) SECSPERHOUR * HOURSPERDAY)

#define DAYSPERREPEAT		((int_fast32_t) 400 * 365 + 100 - 4 + 1)
#define SECSPERREPEAT		((int_fast64_t) DAYSPERREPEAT * SECSPERDAY)
#define AVGSECSPERYEAR		(SECSPERREPEAT / YEARSPERREPEAT)

/* How many years to generate (in zic.c) or search through (in localtime.c).
   This is two years larger than the obvious 400, to avoid edge cases.
   E.g., suppose a rule applies from 2012 on with transitions
   in March and September, plus one-off transitions in November 2013,
   and suppose the rule cannot be expressed as a proleptic TZ string.
   If zic looked only at the last 400 years, it would set max_year=2413,
   with the intent that the 400 years 2014 through 2413 will be repeated.
   The last transition listed in the tzfile would be in 2413-09,
   less than 400 years after the last one-off transition in 2013-11.
   Two years is not overkill for localtime.c, as a one-year bump
   would mishandle 2023d's America/Ciudad_Juarez for November 2422.  */
enum { years_of_observations = YEARSPERREPEAT + 2 };

enum {
  TM_SUNDAY,
  TM_MONDAY,
  TM_TUESDAY,
  TM_WEDNESDAY,
  TM_THURSDAY,
  TM_FRIDAY,
  TM_SATURDAY
};

enum {
  TM_JANUARY,
  TM_FEBRUARY,
  TM_MARCH,
  TM_APRIL,
  TM_MAY,
  TM_JUNE,
  TM_JULY,
  TM_AUGUST,
  TM_SEPTEMBER,
  TM_OCTOBER,
  TM_NOVEMBER,
  TM_DECEMBER
};

enum {
  TM_YEAR_BASE = 1900,
  TM_WDAY_BASE = TM_MONDAY,
  EPOCH_YEAR = 1970,
  EPOCH_WDAY = TM_THURSDAY
};

#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

/*
** Since everything in isleap is modulo 400 (or a factor of 400), we know that
**	isleap(y) == isleap(y % 400)
** and so
**	isleap(a + b) == isleap((a + b) % 400)
** or
**	isleap(a + b) == isleap(a % 400 + b % 400)
** This is true even if % means modulo rather than Fortran remainder
** (which is allowed by C89 but not by C99 or later).
** We use this to avoid addition overflow problems.
*/

#define isleap_sum(a, b)	isleap((a) % 400 + (b) % 400)

#endif /* !defined PRIVATE_H */
