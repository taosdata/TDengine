# Make and install tzdb code and data.
# This file is in the public domain, so clarified as of
# 2009-05-17 by Arthur David Olson.
# Request POSIX conformance; this must be the first non-comment line.
.POSIX:
# On older platforms you may need to scrounge for POSIX conformance.
# For example, on Solaris 10 (2005) with Sun Studio 12 aka Sun C 5.9 (2007),
# use 'PATH=/usr/xpg4/bin:$PATH make CC=c99'.

# To affect how this Makefile works, you can run a shell script like this:
#
#	#!/bin/sh
#	make CC='gcc -std=gnu23' "$@"
#
# This example script is appropriate for a circa 2024 GNU/Linux system
# where a non-default setting enables this package's optional use of C23.
#
# Alternatively, you can simply edit this Makefile to tailor the following
# macro definitions.

###############################################################################
# Start of macros that one plausibly might want to tailor.

# Package name for the code distribution.
PACKAGE=	tzcode

# Version number for the distribution, overridden in the 'tarballs' rule below.
VERSION=	unknown

# Email address for bug reports.
BUGEMAIL=	tz@iana.org

# DATAFORM selects the data format.
# Available formats represent essentially the same data, albeit
# possibly with minor discrepancies that users are not likely to notice.
# To get new features and the best data right away, use:
#	DATAFORM=	vanguard
# To wait a while before using new features, to give downstream users
# time to upgrade zic (the default), use:
#	DATAFORM=	main
# To wait even longer for new features, use:
#	DATAFORM=	rearguard
# Rearguard users might also want "ZFLAGS = -b fat"; see below.
DATAFORM=		main

# Change the line below for your timezone (after finding the one you want in
# one of the $(TDATA) source files, or adding it to a source file).
# Alternatively, if you discover you've got the wrong timezone, you can just
# 'zic -l -' to remove it, or 'zic -l rightzone' to change it.
# Use the command
#	make zonenames
# to get a list of the values you can use for LOCALTIME.

LOCALTIME=	Factory

# The POSIXRULES macro controls interpretation of POSIX-like TZ
# settings like TZ='EET-2EEST' that lack DST transition rules.
# If POSIXRULES is '-', no template is installed; this is the default.
# Any other value for POSIXRULES is obsolete and should not be relied on, as:
# * It does not work correctly in popular implementations such as GNU/Linux.
# * It does not work even in tzcode, except for historical timestamps
#   that precede the last explicit transition in the POSIXRULES file.
#   Hence it typically does not work for current and future timestamps.
# If, despite the above, you want a template for handling these settings,
# you can change the line below (after finding the timezone you want in the
# one of the $(TDATA) source files, or adding it to a source file).
# Alternatively, if you discover you've got the wrong timezone, you can just
# 'zic -p -' to remove it, or 'zic -p rightzone' to change it.
# Use the command
#	make zonenames
# to get a list of the values you can use for POSIXRULES.

POSIXRULES=	-

# Also see TZDEFRULESTRING below, which takes effect only
# if POSIXRULES is '-' or if the template file cannot be accessed.


# Installation locations.
#
# The defaults are suitable for Debian, except that if REDO is
# posix_right or right_posix then files that Debian puts under
# /usr/share/zoneinfo/posix and /usr/share/zoneinfo/right are instead
# put under /usr/share/zoneinfo-posix and /usr/share/zoneinfo-leaps,
# respectively.  Problems with the Debian approach are discussed in
# the commentary for the right_posix rule (below).

# Destination directory, which can be used for staging.
# 'make DESTDIR=/stage install' installs under /stage (e.g., to
# /stage/etc/localtime instead of to /etc/localtime).  Files under
# /stage are not intended to work as-is, but can be copied by hand to
# the root directory later.  If DESTDIR is empty, 'make install' does
# not stage, but installs directly into production locations.
DESTDIR =

# Everything is installed into subdirectories of TOPDIR, and used there.
# TOPDIR should be empty (meaning the root directory),
# or a directory name that does not end in "/".
# TOPDIR should be empty or an absolute name unless you're just testing.
TOPDIR =

# The default local timezone is taken from the file TZDEFAULT.
TZDEFAULT = $(TOPDIR)/etc/localtime

# The subdirectory containing installed program and data files, and
# likewise for installed files that can be shared among architectures.
# These should be relative file names.
USRDIR = usr
USRSHAREDIR = $(USRDIR)/share

# "Compiled" timezone information is placed in the "TZDIR" directory
# (and subdirectories).
# TZDIR_BASENAME should not contain "/" and should not be ".", ".." or empty.
TZDIR_BASENAME=	zoneinfo
TZDIR = $(TOPDIR)/$(USRSHAREDIR)/$(TZDIR_BASENAME)

# The "tzselect" and (if you do "make INSTALL") "date" commands go in:
BINDIR = $(TOPDIR)/$(USRDIR)/bin

# The "zdump" command goes in:
ZDUMPDIR = $(BINDIR)

# The "zic" command goes in:
ZICDIR = $(TOPDIR)/$(USRDIR)/sbin

# Manual pages go in subdirectories of. . .
MANDIR = $(TOPDIR)/$(USRSHAREDIR)/man

# Library functions are put in an archive in LIBDIR.
LIBDIR = $(TOPDIR)/$(USRDIR)/lib


# Types to try, as an alternative to time_t.
TIME_T_ALTERNATIVES = $(TIME_T_ALTERNATIVES_HEAD) $(TIME_T_ALTERNATIVES_TAIL)
TIME_T_ALTERNATIVES_HEAD = int_least64_t.ck
TIME_T_ALTERNATIVES_TAIL = int_least32_t.ck uint_least32_t.ck \
  uint_least64_t.ck

# What kind of TZif data files to generate.  (TZif is the binary time
# zone data format that zic generates; see Internet RFC 9636.)
# If you want only POSIX time, with time values interpreted as
# seconds since the epoch (not counting leap seconds), use
#	REDO=		posix_only
# below.  If you want only "right" time, with values interpreted
# as seconds since the epoch (counting leap seconds), use
#	REDO=		right_only
# below.  If you want both sets of data available, with leap seconds not
# counted normally, use
#	REDO=		posix_right
# below.  If you want both sets of data available, with leap seconds counted
# normally, use
#	REDO=		right_posix
# below.  POSIX mandates that leap seconds not be counted; for compatibility
# with it, use "posix_only" or "posix_right".  Use POSIX time on systems with
# leap smearing; this can work better than unsmeared "right" time with
# applications that are not leap second aware, and is closer to unsmeared
# "right" time than unsmeared POSIX time is (e.g., 0.5 vs 1.0 s max error).

REDO=		posix_right

# Whether to put an "Expires" line in the leapseconds file.
# Use EXPIRES_LINE=1 to put the line in, 0 to omit it.
# The EXPIRES_LINE value matters only if REDO's value contains "right".
# If you change EXPIRES_LINE, remove the leapseconds file before running "make".
# zic's support for the Expires line was introduced in tzdb 2020a,
# and was modified in tzdb 2021b to generate version 4 TZif files.
# EXPIRES_LINE defaults to 0 for now so that the leapseconds file
# can be given to pre-2020a zic implementations and so that TZif files
# built by newer zic implementations can be read by pre-2021b libraries.
EXPIRES_LINE=	0

# To install data in text form that has all the information of the TZif data,
# (optionally incorporating leap second information), use
#	TZDATA_TEXT=	tzdata.zi leapseconds
# To install text data without leap second information (e.g., because
# REDO='posix_only'), use
#	TZDATA_TEXT=	tzdata.zi
# To avoid installing text data, use
#	TZDATA_TEXT=

TZDATA_TEXT=	leapseconds tzdata.zi

# For backward-compatibility links for old zone names, use
#	BACKWARD=	backward
# To omit these links, use
#	BACKWARD=

BACKWARD=	backward

# If you want out-of-scope and often-wrong data from the file 'backzone',
# but only for entries listed in the backward-compatibility file zone.tab, use
#	PACKRATDATA=	backzone
#	PACKRATLIST=	zone.tab
# If you want all the 'backzone' data, use
#	PACKRATDATA=	backzone
#	PACKRATLIST=
# To omit this data, use
#	PACKRATDATA=
#	PACKRATLIST=

PACKRATDATA=
PACKRATLIST=

# The name of a locale using the UTF-8 encoding, used during self-tests.
# The tests are skipped if the name does not appear to work on this system.

UTF8_LOCALE=	en_US.utf8

# Non-default libraries needed to link.
# On some hosts, this should have -lintl unless CFLAGS has -DHAVE_GETTEXT=0.
LDLIBS=

# Add the following to an uncommented "CFLAGS=" line as needed
# to override defaults specified in the source code or by the system.
# "-DFOO" is equivalent to "-DFOO=1".
#  -DDEPRECATE_TWO_DIGIT_YEARS for optional runtime warnings about strftime
#	formats that generate only the last two digits of year numbers
#  -DEPOCH_LOCAL if the 'time' function returns local time not UT
#  -DEPOCH_OFFSET=N if the 'time' function returns a value N greater
#	than what POSIX specifies, assuming local time is UT.
#	For example, N is 252460800 on AmigaOS.
#  -DHAVE_DECL_ASCTIME_R=0 if <time.h> does not declare asctime_r
#	on POSIX platforms predating POSIX.1-2024
#  -DHAVE_DECL_ENVIRON if <unistd.h> declares 'environ'
#  -DHAVE_DECL_TIMEGM=0 if <time.h> does not declare timegm
#  -DHAVE_DIRECT_H if mkdir needs <direct.h> (MS-Windows)
#  -DHAVE__GENERIC=0 if _Generic does not work*
#  -DHAVE_GETRANDOM if getrandom works (e.g., GNU/Linux),
#	-DHAVE_GETRANDOM=0 to avoid using getrandom
#  -DHAVE_GETTEXT if gettext works (e.g., GNU/Linux, FreeBSD, Solaris),
#	where LDLIBS also needs to contain -lintl on some hosts;
#	-DHAVE_GETTEXT=0 to avoid using gettext
#  -DHAVE_INCOMPATIBLE_CTIME_R if your system's time.h declares
#	ctime_r and asctime_r incompatibly with POSIX.1-2017 and earlier
#	(Solaris when _POSIX_PTHREAD_SEMANTICS is not defined).
#  -DHAVE_INTTYPES_H=0 if <inttypes.h> does not work*+
#  -DHAVE_LINK=0 if your system lacks a link function
#  -DHAVE_LOCALTIME_R=0 if your system lacks a localtime_r function
#  -DHAVE_LOCALTIME_RZ=0 if you do not want zdump to use localtime_rz
#	localtime_rz can make zdump significantly faster, but is nonstandard.
#  -DHAVE_MALLOC_ERRNO=0 if malloc etc. do not set errno on failure.
#  -DHAVE_POSIX_DECLS=0 if your system's include files do not declare
#	functions like 'link' or variables like 'tzname' required by POSIX
#  -DHAVE_SETENV=0 if your system lacks the setenv function
#  -DHAVE_SNPRINTF=0 if your system lacks the snprintf function+
#  -DHAVE_STDCKDINT_H=0 if neither <stdckdint.h> nor substitutes like
#	__builtin_add_overflow work*
#  -DHAVE_STDINT_H=0 if <stdint.h> does not work*+
#  -DHAVE_STRFTIME_L if <time.h> declares locale_t and strftime_l
#  -DHAVE_STRDUP=0 if your system lacks the strdup function
#  -DHAVE_STRTOLL=0 if your system lacks the strtoll function+
#  -DHAVE_SYMLINK=0 if your system lacks the symlink function
#  -DHAVE_SYS_STAT_H=0 if <sys/stat.h> does not work*
#  -DHAVE_TZSET=0 if your system lacks a tzset function
#  -DHAVE_UNISTD_H=0 if <unistd.h> does not work*
#  -DHAVE_UTMPX_H=0 if <utmpx.h> does not work*
#  -Dlocale_t=XXX if your system uses XXX instead of locale_t
#  -DMKTIME_MIGHT_OVERFLOW if mktime might fail due to time_t overflow
#  -DPORT_TO_C89 if tzcode should also run on mostly-C89 platforms+
#	Typically it is better to use a later standard.  For example,
#	with GCC 4.9.4 (2016), prefer '-std=gnu11' to '-DPORT_TO_C89'.
#	Even with -DPORT_TO_C89, the code needs at least one C99
#	feature (integers at least 64 bits wide) and maybe more.
#  -DRESERVE_STD_EXT_IDS if your platform reserves standard identifiers
#	with external linkage, e.g., applications cannot define 'localtime'.
#  -Dssize_t=int on hosts like MS-Windows that lack ssize_t
#  -DSUPPORT_C89=0 if the tzcode library should not support C89 callers
#	Although -DSUPPORT_C89=0 might work around latent bugs in callers,
#	it does not conform to POSIX.
#  -DSUPPORT_POSIX2008 if the library should support older POSIX callers+
#	However, this might cause problems in POSIX.1-2024-or-later callers.
#  -DSUPPRESS_TZDIR to not prepend TZDIR to file names; this has
#	security implications and is not recommended for general use
#  -DTHREAD_SAFE to make localtime.c thread-safe, as POSIX requires;
#	not needed by the main-program tz code, which is single-threaded.
#	Append other compiler flags as needed, e.g., -pthread on GNU/Linux.
#  -Dtime_tz=\"T\" to use T as the time_t type, rather than the system time_t
#	This is intended for internal use only; it mangles external names.
#  -DTZ_DOMAIN=\"foo\" to use "foo" for gettext domain name; default is "tz"
#  -DTZ_DOMAINDIR=\"/path\" to use "/path" for gettext directory;
#	the default is system-supplied, typically "/usr/lib/locale"
#  -DTZDEFRULESTRING=\",date/time,date/time\" to default to the specified
#	DST transitions for proleptic format TZ strings lacking them,
#	in the usual case where POSIXRULES is '-'.  If not specified,
#	TZDEFRULESTRING defaults to US rules for future DST transitions.
#	This mishandles some past timestamps, as US DST rules have changed.
#	It also mishandles settings like TZ='EET-2EEST' for eastern Europe,
#	as Europe and US DST rules differ.
#  -DTZNAME_MAXIMUM=N to limit time zone abbreviations to N bytes (default 254)
#  -DUNINIT_TRAP if reading uninitialized storage can cause problems
#	other than simply getting garbage data
#  -DUSE_LTZ=0 to build zdump with the system time zone library
#	Also set TZDOBJS=zdump.o and CHECK_TIME_T_ALTERNATIVES= below.
#  -DZIC_BLOAT_DEFAULT=\"fat\" to default zic's -b option to "fat", and
#	similarly for "slim".  Fat TZif files work around incompatibilities
#	and bugs in some TZif readers, notably older ones that
#	ignore or otherwise mishandle 64-bit data in TZif files;
#	however, fat TZif files may trigger bugs in newer TZif readers.
#	Slim TZif files are more efficient, and are the default.
#  -DZIC_MAX_ABBR_LEN_WO_WARN=3
#	(or some other number) to set the maximum time zone abbreviation length
#	that zic will accept without a warning (the default is 6)
#  -g to generate symbolic debugging info
#  -Idir to include from directory 'dir'
#  -O0 to disable optimization; other -O options to enable more optimization
#  -Uname to remove any definition of the macro 'name'
#  $(GCC_DEBUG_FLAGS) if you are using recent GCC and want lots of checking
#
# * Options marked "*" can be omitted if your compiler is C23 compatible.
# * Options marked "+" are obsolescent and are planned to be removed
#   once the code assumes C99 or later (say in the year 2029)
#   and POSIX.1-2024 or later (say in the year 2034).
#
# Select instrumentation via "make GCC_INSTRUMENT='whatever'".
GCC_INSTRUMENT = \
  -fsanitize=undefined -fsanitize-address-use-after-scope \
  -fsanitize-undefined-trap-on-error -fstack-protector
# Omit -fanalyzer from GCC_DEBUG_FLAGS, as it makes GCC too slow.
GCC_DEBUG_FLAGS = -DGCC_LINT -g3 -O3 \
  $(GCC_INSTRUMENT) \
  -Wall -Wextra \
  -Walloc-size-larger-than=100000 -Warray-bounds=2 \
  -Wbad-function-cast -Wbidi-chars=any,ucn -Wcast-align=strict -Wcast-qual \
  -Wdate-time \
  -Wdeclaration-after-statement -Wdouble-promotion \
  -Wduplicated-branches -Wduplicated-cond -Wflex-array-member-not-at-end \
  -Wformat=2 -Wformat-overflow=2 -Wformat-signedness -Wformat-truncation \
  -Wimplicit-fallthrough=5 -Winit-self -Wlogical-op \
  -Wmissing-declarations -Wmissing-prototypes \
  -Wmissing-variable-declarations -Wnested-externs \
  -Wnull-dereference \
  -Wold-style-definition -Woverlength-strings -Wpointer-arith \
  -Wshadow -Wshift-overflow=2 -Wstrict-overflow \
  -Wstrict-prototypes -Wstringop-overflow=4 \
  -Wstringop-truncation -Wsuggest-attribute=cold \
  -Wsuggest-attribute=const -Wsuggest-attribute=format \
  -Wsuggest-attribute=malloc \
  -Wsuggest-attribute=noreturn -Wsuggest-attribute=pure \
  -Wtrampolines -Wundef -Wunused-macros -Wuse-after-free=3 \
  -Wvariadic-macros -Wvla -Wwrite-strings \
  -Wno-format-nonliteral -Wno-sign-compare -Wno-type-limits
#
# If your system has a "GMT offset" field in its "struct tm"s
# (or if you decide to add such a field in your system's "time.h" file),
# add the name to a define such as
#	-DTM_GMTOFF=tm_gmtoff
# to the end of the "CFLAGS=" line.  If not defined, the code attempts to
# guess TM_GMTOFF from other macros; define NO_TM_GMTOFF to suppress this.
# Similarly, if your system has a "zone abbreviation" field, define
#	-DTM_ZONE=tm_zone
# and define NO_TM_ZONE to suppress any guessing.
# Although POSIX.1-2024 requires these fields and they are widely available
# on GNU/Linux and BSD systems, some older systems lack them.
#
# The next batch of options control support for external variables
# exported by tzcode.  In practice these variables are less useful
# than TM_GMTOFF and TM_ZONE.  However, most of them are standardized.
# #
# # To omit or support the external variable "tzname", add one of:
# #	-DHAVE_TZNAME=0 # do not support "tzname"
# #	-DHAVE_TZNAME=1 # support "tzname", which is defined by system library
# #	-DHAVE_TZNAME=2 # support and define "tzname"
# # to the "CFLAGS=" line.  Although "tzname" is required by POSIX.1-1988
# # and later, its contents are unspecified if you use a geographical TZ
# # and the variable is planned to be removed in a future POSIX edition.
# # If not defined, the code attempts to guess HAVE_TZNAME from other macros.
# # Warning: unless time_tz is also defined, HAVE_TZNAME=1 can cause
# # crashes when combined with some platforms' standard libraries,
# # presumably due to memory allocation issues.
# #
# # To omit or support the external variables "timezone" and "daylight", add
# #	-DUSG_COMPAT=0 # do not support
# #	-DUSG_COMPAT=1 # support, and variables are defined by system library
# #	-DUSG_COMPAT=2 # support and define variables
# # to the "CFLAGS=" line; "timezone" and "daylight" are inspired by Unix
# # Systems Group code and are required by POSIX.1-2008 and later (with XSI),
# # although their contents are unspecified if you use a geographical TZ
# # and the variables are planned to be removed in a future edition of POSIX.
# # If not defined, the code attempts to guess USG_COMPAT from other macros.
# #
# # To support the external variable "altzone", add
# #	-DALTZONE=0 # do not support
# #	-DALTZONE=1 # support "altzone", which is defined by system library
# #	-DALTZONE=2 # support and define "altzone"
# # to the end of the "CFLAGS=" line; although "altzone" appeared in
# # System V Release 3.1 it has not been standardized.
# # If not defined, the code attempts to guess ALTZONE from other macros.
#
# If you want functions that were inspired by early versions of X3J11's work,
# add
#	-DSTD_INSPIRED
# to the end of the "CFLAGS=" line.  This arranges for the following
# functions to be added to the time conversion library.
# "offtime" is like "gmtime" except that it accepts a second (long) argument
# that gives an offset to add to the time_t when converting it.
# I.e., "offtime" is like calling "localtime_rz" with a fixed-offset zone.
# "timelocal" is nearly equivalent to "mktime".
# "timeoff" is like "timegm" except that it accepts a second (long) argument
# that gives an offset to use when converting to a time_t.
# I.e., "timeoff" is like calling "mktime_z" with a fixed-offset zone.
# "posix2time" and "time2posix" are described in an included manual page.
# X3J11's work does not describe any of these functions.
# These functions may well disappear in future releases of the time
# conversion package.
#
# If you don't want functions that were inspired by NetBSD, add
#	-DNETBSD_INSPIRED=0
# to the end of the "CFLAGS=" line.  Otherwise, the functions
# "localtime_rz", "mktime_z", "tzalloc", and "tzfree" are added to the
# time library, and if STD_INSPIRED is also defined to nonzero the functions
# "posix2time_z" and "time2posix_z" are added as well.
# The functions ending in "_z" (or "_rz") are like their unsuffixed
# (or suffixed-by-"_r") counterparts, except with an extra first
# argument of opaque type timezone_t that specifies the timezone.
# "tzalloc" allocates a timezone_t value, and "tzfree" frees it.
#
# If you want to allocate state structures in localtime, add
#	-DALL_STATE
# to the end of the "CFLAGS=" line.  Storage is obtained by calling malloc.
#
# NIST-PCTS:151-2, Version 1.4, (1993-12-03) is a test suite put
# out by the National Institute of Standards and Technology
# which claims to test C and POSIX conformance.  If you want to pass PCTS, add
#	-DPCTS
# to the end of the "CFLAGS=" line.
#
# If you want strict compliance with XPG4 as of 1994-04-09, add
#	-DXPG4_1994_04_09
# to the end of the "CFLAGS=" line.  This causes "strftime" to always return
# 53 as a week number (rather than 52 or 53) for January days before
# January's first Monday when a "%V" format is used and January 1
# falls on a Friday, Saturday, or Sunday.
#
# POSIX says CFLAGS defaults to "-O 1".
# Uncomment the following line and edit its contents as needed.

#CFLAGS= -O 1


# The name of a POSIX-like library archiver, its flags, C compiler,
# linker flags, and 'make' utility.  Ordinarily the defaults suffice.
# The commented-out values are the defaults specified by POSIX.1-2024.
#AR = ar
#ARFLAGS = -rv
#CC = c17
#LDFLAGS =
#MAKE = make

# Where to fetch leap-seconds.list from.
leaplist_URI = \
  https://hpiers.obspm.fr/iers/bul/bulc/ntp/leap-seconds.list
# The file is generated by the IERS Earth Orientation Centre, in Paris.
leaplist_TZ = Europe/Paris

# The zic command and its arguments.

zic=		./zic
ZIC=		$(zic) $(ZFLAGS)

# To shrink the size of installed TZif files,
# append "-r @N" to omit data before N-seconds-after-the-Epoch.
# To grow the files and work around bugs in older applications,
# possibly at the expense of introducing bugs in newer ones,
# append "-b fat"; see ZIC_BLOAT_DEFAULT above.
# See the zic man page for more about -b and -r.
ZFLAGS=

# How to use zic to install TZif files.

ZIC_INSTALL=	$(ZIC) -d '$(DESTDIR)$(TZDIR)'

# The name of a POSIX-compliant 'awk' on your system.
# mawk 1.3.3 and Solaris 10 /usr/bin/awk do not work.
# Also, it is better (though not essential) if 'awk' supports UTF-8,
# and unfortunately mawk and busybox awk do not support UTF-8.
# Try AWK=gawk or AWK=nawk if your awk has the abovementioned problems.
AWK=		awk

# The full path name of a POSIX-compliant shell, preferably one that supports
# the Korn shell's 'select' statement as an extension.
# These days, Bash is the most popular.
# It should be OK to set this to /bin/sh, on platforms where /bin/sh
# lacks 'select' or doesn't completely conform to POSIX, but /bin/bash
# is typically nicer if it works.
KSHELL=		/bin/bash

# Name of curl <https://curl.haxx.se/>, used for HTML validation
# and to fetch leap-seconds.list from upstream.
# Set CURL=: to disable use of the Internet.
CURL=		curl

# Name of GNU Privacy Guard <https://gnupg.org/>, used to sign distributions.
GPG=		gpg

# This expensive test requires USE_LTZ.
# To suppress it, define this macro to be empty.
CHECK_TIME_T_ALTERNATIVES = check_time_t_alternatives

# SAFE_CHAR is a regular expression that matches a safe character.
# Some parts of this distribution are limited to safe characters;
# others can use any UTF-8 character.
# For now, the safe characters are a safe subset of ASCII.
# The caller must set the shell variable 'sharp' to the character '#',
# since Makefile macros cannot contain '#'.
# TAB_CHAR is a single tab character, in single quotes.
TAB_CHAR=	'	'
SAFE_CHARSET1=	$(TAB_CHAR)' !\"'$$sharp'$$%&'\''()*+,./0123456789:;<=>?@'
SAFE_CHARSET2=	'ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\^_`'
SAFE_CHARSET3=	'abcdefghijklmnopqrstuvwxyz{|}~'
SAFE_CHARSET=	$(SAFE_CHARSET1)$(SAFE_CHARSET2)$(SAFE_CHARSET3)
SAFE_CHAR=	'[]'$(SAFE_CHARSET)'-]'

# These non-alphabetic, non-ASCII printable characters are Latin-1,
# and so are likely displayable even in editors like XEmacs 21
# that have limited display capabilities.
UNUSUAL_OK_LATIN_1 = ¡¢£¤¥¦§¨©«¬®¯°±²³´¶·¸¹»¼½¾¿×÷
# Non-ASCII non-letters that OK_CHAR allows, as these characters are
# useful in commentary.
UNUSUAL_OK_CHARSET= $(UNUSUAL_OK_LATIN_1)

# Put this in a bracket expression to match spaces.
s = [:space:]

# OK_CHAR matches any character allowed in the distributed files.
# This is the same as SAFE_CHAR, except that UNUSUAL_OK_CHARSET and
# multibyte letters are also allowed so that commentary can contain a
# few safe symbols and people's names and can quote non-English sources.
# Other non-letters are limited to ASCII renderings for the
# convenience of maintainers using XEmacs 21.5.34, which by default
# mishandles Unicode characters U+0100 and greater.
OK_CHAR=	'[][:alpha:]$(UNUSUAL_OK_CHARSET)'$(SAFE_CHARSET)'-]'

# SAFE_LINE matches a line of safe characters.
# SAFE_SHARP_LINE is similar, except any OK character can follow '#';
# this is so that comments can contain non-ASCII characters.
# OK_LINE matches a line of OK characters.
SAFE_LINE=	'^'$(SAFE_CHAR)'*$$'
SAFE_SHARP_LINE='^'$(SAFE_CHAR)'*('$$sharp$(OK_CHAR)'*)?$$'
OK_LINE=	'^'$(OK_CHAR)'*$$'

# Flags to give 'tar' when making a distribution.
# Try to use flags appropriate for GNU tar.
GNUTARFLAGS= --format=pax --pax-option=delete=atime,delete=ctime \
  --numeric-owner --owner=0 --group=0 \
  --mode=go+u,go-w --sort=name
SETUP_TAR= \
  export LC_ALL=C && \
  if tar $(GNUTARFLAGS) --version >/dev/null 2>&1; then \
    TAR='tar $(GNUTARFLAGS)'; \
  else \
    TAR=tar; \
  fi

# Flags to give 'gzip' when making a distribution.
GZIPFLAGS=	-9n

# When comparing .tzs files, use GNU diff's -F'^TZ=' option if supported.
# This makes it easier to see which Zone has been affected.
SETUP_DIFF_TZS = \
  if diff -u -F'^TZ=' - - <>/dev/null >&0 2>&1; then \
    DIFF_TZS='diff -u -F^TZ='; \
  else \
    DIFF_TZS='diff -u'; \
  fi

# ':' on typical hosts; 'ranlib' on the ancient hosts that still need ranlib.
RANLIB=		:

# POSIX prohibits defining or using SHELL.  However, csh users on systems
# that use the user shell for Makefile commands may need to define SHELL.
#SHELL=		/bin/sh

# End of macros that one plausibly might want to tailor.
###############################################################################


TZCOBJS=	zic.o
TZDOBJS=	zdump.o localtime.o strftime.o
DATEOBJS=	date.o localtime.o strftime.o
LIBSRCS=	localtime.c asctime.c difftime.c strftime.c
LIBOBJS=	localtime.o asctime.o difftime.o strftime.o
HEADERS=	tzfile.h private.h
NONLIBSRCS=	zic.c zdump.c
NEWUCBSRCS=	date.c
SOURCES=	$(HEADERS) $(LIBSRCS) $(NONLIBSRCS) $(NEWUCBSRCS) \
			tzselect.ksh workman.sh
MANS=		newctime.3 newstrftime.3 newtzset.3 time2posix.3 \
			tzfile.5 tzselect.8 zic.8 zdump.8
MANTXTS=	newctime.3.txt newstrftime.3.txt newtzset.3.txt \
			time2posix.3.txt \
			tzfile.5.txt tzselect.8.txt zic.8.txt zdump.8.txt \
			date.1.txt
COMMON=		calendars CONTRIBUTING LICENSE Makefile \
			NEWS README SECURITY theory.html version
WEB_PAGES=	tz-art.html tz-how-to.html tz-link.html
CHECK_WEB_PAGES=theory.ck tz-art.ck tz-how-to.ck tz-link.ck
DOCS=		$(MANS) date.1 $(MANTXTS) $(WEB_PAGES)
PRIMARY_YDATA=	africa antarctica asia australasia \
		europe northamerica southamerica
YDATA=		$(PRIMARY_YDATA) etcetera
NDATA=		factory
TDATA_TO_CHECK=	$(YDATA) $(NDATA) backward
TDATA=		$(YDATA) $(NDATA) $(BACKWARD)
ZONETABLES=	zone.tab zone1970.tab zonenow.tab
TABDATA=	iso3166.tab $(TZDATA_TEXT) $(ZONETABLES)
LEAP_DEPS=	leapseconds.awk leap-seconds.list
TZDATA_ZI_DEPS=	ziguard.awk zishrink.awk version $(TDATA) \
		  $(PACKRATDATA) $(PACKRATLIST)
DSTDATA_ZI_DEPS= ziguard.awk $(TDATA) $(PACKRATDATA) $(PACKRATLIST)
DATA=		$(TDATA_TO_CHECK) backzone iso3166.tab leap-seconds.list \
			leapseconds $(ZONETABLES)
AWK_SCRIPTS=	checklinks.awk checknow.awk checktab.awk leapseconds.awk \
			ziguard.awk zishrink.awk
MISC=		$(AWK_SCRIPTS)
TZS_YEAR=	2050
TZS_CUTOFF_FLAG=	-c $(TZS_YEAR)
TZS=		to$(TZS_YEAR).tzs
TZS_NEW=	to$(TZS_YEAR)new.tzs
TZS_DEPS=	$(YDATA) localtime.c private.h \
			strftime.c tzfile.h zdump.c zic.c
TZDATA_DIST = $(COMMON) $(DATA) $(MISC)
# EIGHT_YARDS is just a yard short of the whole ENCHILADA.
EIGHT_YARDS = $(TZDATA_DIST) $(DOCS) $(SOURCES) tzdata.zi
ENCHILADA = $(EIGHT_YARDS) $(TZS)

# Consult these files when deciding whether to rebuild the 'version' file.
# This list is not the same as the output of 'git ls-files', since
# .gitignore is not distributed.
VERSION_DEPS= \
		calendars CONTRIBUTING LICENSE Makefile NEWS README SECURITY \
		africa antarctica asctime.c asia australasia \
		backward backzone \
		checklinks.awk checknow.awk checktab.awk \
		date.1 date.c difftime.c \
		etcetera europe factory iso3166.tab \
		leap-seconds.list leapseconds.awk localtime.c \
		newctime.3 newstrftime.3 newtzset.3 northamerica \
		private.h southamerica strftime.c theory.html \
		time2posix.3 tz-art.html tz-how-to.html tz-link.html \
		tzfile.5 tzfile.h tzselect.8 tzselect.ksh \
		workman.sh zdump.8 zdump.c zic.8 zic.c \
		ziguard.awk zishrink.awk \
		zone.tab zone1970.tab zonenow.tab

all:		tzselect zic zdump libtz.a $(TABDATA) \
		  vanguard.zi main.zi rearguard.zi

ALL:		all date $(ENCHILADA)

install:	all $(DATA) $(REDO) $(MANS)
		mkdir -p '$(DESTDIR)$(BINDIR)' \
			'$(DESTDIR)$(ZDUMPDIR)' '$(DESTDIR)$(ZICDIR)' \
			'$(DESTDIR)$(LIBDIR)' \
			'$(DESTDIR)$(MANDIR)/man3' '$(DESTDIR)$(MANDIR)/man5' \
			'$(DESTDIR)$(MANDIR)/man8'
		$(ZIC_INSTALL) -l $(LOCALTIME) \
			-p $(POSIXRULES) \
			-t '$(DESTDIR)$(TZDEFAULT)'
		cp -f $(TABDATA) '$(DESTDIR)$(TZDIR)/.'
		cp tzselect '$(DESTDIR)$(BINDIR)/.'
		cp zdump '$(DESTDIR)$(ZDUMPDIR)/.'
		cp zic '$(DESTDIR)$(ZICDIR)/.'
		install -C libtz.a '$(DESTDIR)$(LIBDIR)/.'
		$(RANLIB) '$(DESTDIR)$(LIBDIR)/libtz.a'
		cp -f newctime.3 newtzset.3 '$(DESTDIR)$(MANDIR)/man3/.'
		cp -f tzfile.5 '$(DESTDIR)$(MANDIR)/man5/.'
		cp -f tzselect.8 zdump.8 zic.8 '$(DESTDIR)$(MANDIR)/man8/.'

INSTALL:	ALL install date.1
		mkdir -p '$(DESTDIR)$(BINDIR)' '$(DESTDIR)$(MANDIR)/man1'
		cp date '$(DESTDIR)$(BINDIR)/.'
		cp -f date.1 '$(DESTDIR)$(MANDIR)/man1/.'

# Calculate version number from git, if available.
# Otherwise, use $(VERSION) unless it is "unknown" and there is already
# a 'version' file, in which case reuse the existing 'version' contents
# and append "-dirty" if the contents do not already end in "-dirty".
version:	$(VERSION_DEPS)
		{ (type git) >/dev/null 2>&1 && \
		  V=$$(git describe --match '[0-9][0-9][0-9][0-9][a-z]*' \
				--abbrev=7 --dirty) || \
		  if test '$(VERSION)' = unknown && read -r V <$@; then \
		    V=$${V%-dirty}-dirty; \
		  else \
		    V='$(VERSION)'; \
		  fi; } && \
		printf '%s\n' "$$V" >$@.out
		mv $@.out $@

# These files can be tailored by setting BACKWARD, PACKRATDATA, PACKRATLIST.
vanguard.zi main.zi rearguard.zi: $(DSTDATA_ZI_DEPS)
		$(AWK) \
		  -v DATAFORM=$(@:.zi=) \
		  -v PACKRATDATA='$(PACKRATDATA)' \
		  -v PACKRATLIST='$(PACKRATLIST)' \
		  -f ziguard.awk \
		  $(TDATA) $(PACKRATDATA) >$@.out
		mv $@.out $@
# This file has a version comment that attempts to capture any tailoring
# via BACKWARD, DATAFORM, PACKRATDATA, PACKRATLIST, and REDO.
tzdata.zi:	$(DATAFORM).zi version zishrink.awk
		read -r version <version && \
		  LC_ALL=C $(AWK) \
		    -v dataform='$(DATAFORM)' \
		    -v deps='$(DSTDATA_ZI_DEPS) zishrink.awk' \
		    -v redo='$(REDO)' \
		    -v version="$$version" \
		    -f zishrink.awk \
		    $(DATAFORM).zi >$@.out
		mv $@.out $@

tzdir.h:
		printf '%s\n' >$@.out \
		  '#ifndef TZDEFAULT' \
		  '# define TZDEFAULT "$(TZDEFAULT)" /* default zone */' \
		  '#endif' \
		  '#ifndef TZDIR' \
		  '# define TZDIR "$(TZDIR)" /* TZif directory */' \
		  '#endif'
		mv $@.out $@

version.h:	version
		read -r VERSION <version && printf '%s\n' \
		  'static char const PKGVERSION[]="($(PACKAGE)) ";' \
		  "static char const TZVERSION[]=\"$$VERSION\";" \
		  'static char const REPORT_BUGS_TO[]="$(BUGEMAIL)";' \
		  >$@.out
		mv $@.out $@

zdump:		$(TZDOBJS)
		$(CC) -o $@ $(CFLAGS) $(LDFLAGS) $(TZDOBJS) $(LDLIBS)

zic:		$(TZCOBJS)
		$(CC) -o $@ $(CFLAGS) $(LDFLAGS) $(TZCOBJS) $(LDLIBS)

leapseconds:	$(LEAP_DEPS)
		$(AWK) -v EXPIRES_LINE=$(EXPIRES_LINE) \
		  -f leapseconds.awk leap-seconds.list >$@.out
		mv $@.out $@

# Awk script to extract a Git-style author from leap-seconds.list comments.
EXTRACT_AUTHOR = \
  author_line { sub(/^.[[:space:]]*/, ""); \
      sub(/:[[:space:]]*/, " <"); \
      printf "%s>\n", $$0; \
      success = 1; \
      exit \
  } \
  /Questions or comments to:/ { author_line = 1 } \
  END { exit !success }

# Fetch leap-seconds.list from upstream.
fetch-leap-seconds.list:
		$(CURL) -OR $(leaplist_URI)

# Fetch leap-seconds.list from upstream and commit it to the local repository.
commit-leap-seconds.list: fetch-leap-seconds.list
		author=$$($(AWK) '$(EXTRACT_AUTHOR)' leap-seconds.list) && \
		date=$$(TZ=$(leaplist_TZ) stat -c%y leap-seconds.list) && \
		git commit --author="$$author" --date="$$date" -m'make $@' \
		  leap-seconds.list

# Arguments to pass to submakes.
# They can be overridden by later submake arguments.
INSTALLARGS = \
 BACKWARD='$(BACKWARD)' \
 DESTDIR='$(DESTDIR)' \
 PACKRATDATA='$(PACKRATDATA)' \
 PACKRATLIST='$(PACKRATLIST)' \
 TZDEFAULT='$(TZDEFAULT)' \
 TZDIR='$(TZDIR)' \
 ZIC='$(ZIC)'

INSTALL_DATA_DEPS = zic leapseconds tzdata.zi

posix_only: $(INSTALL_DATA_DEPS)
		$(ZIC_INSTALL) tzdata.zi

right_only: $(INSTALL_DATA_DEPS)
		$(ZIC_INSTALL) -L leapseconds tzdata.zi

# In earlier versions of this makefile, the other two directories were
# subdirectories of $(TZDIR).  However, this led to configuration errors.
# For example, with posix_right under the earlier scheme,
# TZ='right/Australia/Adelaide' got you localtime with leap seconds,
# but gmtime without leap seconds, which led to problems with applications
# like sendmail that subtract gmtime from localtime.
# Therefore, the other two directories are now siblings of $(TZDIR).
# You must replace all of $(TZDIR) to switch from not using leap seconds
# to using them, or vice versa.
right_posix:	right_only
		rm -fr '$(DESTDIR)$(TZDIR)-leaps'
		ln -s '$(TZDIR_BASENAME)' '$(DESTDIR)$(TZDIR)-leaps' || \
		  $(MAKE) $(INSTALLARGS) TZDIR='$(TZDIR)-leaps' right_only
		$(MAKE) $(INSTALLARGS) TZDIR='$(TZDIR)-posix' posix_only

posix_right:	posix_only
		rm -fr '$(DESTDIR)$(TZDIR)-posix'
		ln -s '$(TZDIR_BASENAME)' '$(DESTDIR)$(TZDIR)-posix' || \
		  $(MAKE) $(INSTALLARGS) TZDIR='$(TZDIR)-posix' posix_only
		$(MAKE) $(INSTALLARGS) TZDIR='$(TZDIR)-leaps' right_only

zones:		$(REDO)

# dummy.zd is not a real file; it is mentioned here only so that the
# top-level 'make' does not have a syntax error.
ZDS = dummy.zd
# Rule used only by submakes invoked by the $(TZS_NEW) rule.
# It is separate so that GNU 'make -j' can run instances in parallel.
$(ZDS): zdump
		./zdump -i $(TZS_CUTOFF_FLAG) "$$PWD/$(@:.zd=)" >$@

TZS_NEW_DEPS = tzdata.zi zdump zic
$(TZS_NEW): $(TZS_NEW_DEPS)
		rm -fr tzs$(TZS_YEAR).dir
		mkdir tzs$(TZS_YEAR).dir
		$(zic) -d tzs$(TZS_YEAR).dir tzdata.zi
		$(AWK) '/^L/{print "Link\t" $$2 "\t" $$3}' \
		   tzdata.zi | LC_ALL=C sort >$@.out
		x=$$($(AWK) '/^Z/{print "tzs$(TZS_YEAR).dir/" $$2 ".zd"}' \
				tzdata.zi \
		     | LC_ALL=C sort -t . -k 2,2) && \
		set x $$x && \
		shift && \
		ZDS=$$* && \
		$(MAKE) TZS_CUTOFF_FLAG="$(TZS_CUTOFF_FLAG)" \
		  ZDS="$$ZDS" $$ZDS && \
		sed 's,^TZ=".*\.dir/,TZ=",' $$ZDS >>$@.out
		rm -fr tzs$(TZS_YEAR).dir
		mv $@.out $@

# If $(TZS) exists but 'make tzs.ck' fails, a maintainer should inspect the
# failed output and fix the inconsistency, perhaps by running 'make force_tzs'.
$(TZS):
		touch $@

force_tzs:	$(TZS_NEW)
		cp $(TZS_NEW) $(TZS)

libtz.a:	$(LIBOBJS)
		rm -f $@
		$(AR) $(ARFLAGS) $@ $(LIBOBJS)
		$(RANLIB) $@

date:		$(DATEOBJS)
		$(CC) -o $@ $(CFLAGS) $(LDFLAGS) $(DATEOBJS) $(LDLIBS)

tzselect:	tzselect.ksh version
		read -r VERSION <version && sed \
		  -e "s'#!/bin/bash'#!"'$(KSHELL)'\' \
		  -e s\''\(AWK\)=[^}]*'\''\1=\'\''$(AWK)\'\'\' \
		  -e s\''\(PKGVERSION\)=.*'\''\1=\'\''($(PACKAGE)) \'\'\' \
		  -e s\''\(REPORT_BUGS_TO\)=.*'\''\1=\'\''$(BUGEMAIL)\'\'\' \
		  -e s\''\(TZDIR\)=[^}]*'\''\1=\'\''$(TZDIR)\'\'\' \
		  -e s\''\(TZVERSION\)=.*'\''\1=\'"'$$VERSION\\''" \
		  <$@.ksh >$@.out
		chmod +x $@.out
		mv $@.out $@

check: check_mild back.ck now.ck
check_mild: check_web check_zishrink \
  character-set.ck white-space.ck links.ck mainguard.ck \
  name-lengths.ck slashed-abbrs.ck sorted.ck \
  tables.ck ziguard.ck tzs.ck

# True if UTF8_LOCALE does not work;
# otherwise, false but with LC_ALL set to $(UTF8_LOCALE).
UTF8_LOCALE_MISSING = \
  { test ! '$(UTF8_LOCALE)' \
    || ! printf 'A\304\200B\n' \
         | LC_ALL='$(UTF8_LOCALE)' grep -q '^A.B$$' >/dev/null 2>&1 \
    || { export LC_ALL='$(UTF8_LOCALE)'; false; }; }

character-set.ck: $(ENCHILADA)
	$(UTF8_LOCALE_MISSING) || { \
		sharp='#' && \
		! grep -Env $(SAFE_LINE) $(MANS) date.1 $(MANTXTS) \
			$(MISC) $(SOURCES) $(WEB_PAGES) \
			CONTRIBUTING LICENSE README SECURITY \
			version tzdata.zi && \
		! grep -Env $(SAFE_LINE)'|^UNUSUAL_OK_'$(OK_CHAR)'*$$' \
			Makefile && \
		! grep -Env $(SAFE_SHARP_LINE) $(TDATA_TO_CHECK) backzone \
			leapseconds zone.tab && \
		! grep -Env $(OK_LINE) $(ENCHILADA); \
	}
	touch $@

white-space.ck: $(ENCHILADA)
	$(UTF8_LOCALE_MISSING) || { \
		enchilada='$(ENCHILADA)' && \
		patfmt=' \t|[\f\r\v]' && pat=$$(printf "$$patfmt\\n") && \
		! grep -En "$$pat|[$s]\$$" \
		    $${enchilada%leap-seconds.list*} \
		    $${enchilada#*leap-seconds.list}; \
	}
	touch $@

PRECEDES_FILE_NAME = ^(Zone|Link[$s]+[^$s]+)[$s]+
FILE_NAME_COMPONENT_TOO_LONG = $(PRECEDES_FILE_NAME)[^$s]*[^/$s]{15}

name-lengths.ck: $(TDATA_TO_CHECK) backzone
		:;! grep -En '$(FILE_NAME_COMPONENT_TOO_LONG)' \
			$(TDATA_TO_CHECK) backzone
		touch $@

mainguard.ck: main.zi
		test '$(PACKRATLIST)' || \
		  cat $(TDATA) $(PACKRATDATA) | diff -u - main.zi
		touch $@

PRECEDES_STDOFF = ^(Zone[$s]+[^$s]+)?[$s]+
STDOFF = [-+]?[0-9:.]+
RULELESS_SAVE = (-|$(STDOFF)[sd]?)
RULELESS_SLASHED_ABBRS = \
  $(PRECEDES_STDOFF)$(STDOFF)[$s]+$(RULELESS_SAVE)[$s]+[^$s]*/

slashed-abbrs.ck: $(TDATA_TO_CHECK)
		:;! grep -En '$(RULELESS_SLASHED_ABBRS)' $(TDATA_TO_CHECK)
		touch $@

CHECK_CC_LIST = { n = split($$1,a,/,/); for (i=2; i<=n; i++) print a[1], a[i]; }

sorted.ck: backward backzone
		$(AWK) '/^Link/ {printf "%.5d %s\n", g, $$3} !/./ {g++}' \
		  backward | LC_ALL=C sort -cu
		$(AWK) '/^Zone.*\// {print $$2}' backzone | LC_ALL=C sort -cu
		touch $@

back.ck: checklinks.awk $(TDATA_TO_CHECK)
		$(AWK) \
		  -v DATAFORM=$(DATAFORM) \
		  -v backcheck=backward \
		  -f checklinks.awk $(TDATA_TO_CHECK)
		touch $@

links.ck: checklinks.awk tzdata.zi
		$(AWK) \
		  -v DATAFORM=$(DATAFORM) \
		  -f checklinks.awk tzdata.zi
		touch $@

# Check timestamps from now through 28 years from now, to make sure
# that zonenow.tab contains all sequences of planned timestamps,
# without any duplicate sequences.  In theory this might require
# 2800+ years but that would take a long time to check.
CHECK_NOW_TIMESTAMP = $$(./date +%s)
CHECK_NOW_FUTURE_YEARS = 28
CHECK_NOW_FUTURE_SECS = $(CHECK_NOW_FUTURE_YEARS) * 366 * 24 * 60 * 60
now.ck: checknow.awk date tzdata.zi zdump zic zone1970.tab zonenow.tab
		rm -fr $@d
		mkdir $@d
		./zic -d $@d tzdata.zi
		now=$(CHECK_NOW_TIMESTAMP) && \
		  future=$$(($(CHECK_NOW_FUTURE_SECS) + $$now)) && \
		  ./zdump -i -t $$now,$$future \
		     $$(find "$$PWD/$@d"/????*/ -type f) \
		     >$@d/zdump-now.tab && \
		  ./zdump -i -t 0,$$future \
		     $$(find "$$PWD/$@d" -name Etc -prune \
			  -o -type f ! -name '*.tab' -print) \
		     >$@d/zdump-1970.tab
		$(AWK) \
		  -v zdump_table=$@d/zdump-now.tab \
		  -f checknow.awk zonenow.tab
		$(AWK) \
		  'BEGIN {print "-\t-\tUTC"} /^Zone/ {print "-\t-\t" $$2}' \
		  $(PRIMARY_YDATA) backward factory | \
		 $(AWK) \
		   -v zdump_table=$@d/zdump-1970.tab \
		   -f checknow.awk
		rm -fr $@d
		touch $@

tables.ck: checktab.awk $(YDATA) backward zone.tab zone1970.tab
		for tab in $(ZONETABLES); do \
		  test "$$tab" = zone.tab && links='$(BACKWARD)' || links=''; \
		  $(AWK) -f checktab.awk -v zone_table=$$tab $(YDATA) $$links \
		    || exit; \
		done
		touch $@

tzs.ck: $(TZS) $(TZS_NEW)
		if test -s $(TZS); then \
		  $(SETUP_DIFF_TZS) && $$DIFF_TZS $(TZS) $(TZS_NEW); \
		else \
		  cp $(TZS_NEW) $(TZS); \
		fi
		touch $@

check_web:	$(CHECK_WEB_PAGES)
.SUFFIXES: .ck .html
.html.ck:
		{ ! ($(CURL) --version) >/dev/null 2>&1 || \
		    $(CURL) -sS --url https://validator.w3.org/nu/ -F out=gnu \
		          -F file=@$<; } >$@.out && \
		  test ! -s $@.out || { cat $@.out; exit 1; }
		mv $@.out $@

ziguard.ck: rearguard.zi vanguard.zi ziguard.awk
		$(AWK) -v DATAFORM=rearguard -f ziguard.awk vanguard.zi | \
		  diff -u rearguard.zi -
		$(AWK) -v DATAFORM=vanguard -f ziguard.awk rearguard.zi | \
		  diff -u vanguard.zi -
		touch $@

# Check that zishrink.awk does not alter the data, and that ziguard.awk
# preserves main-format data.
check_zishrink: zishrink-posix.ck zishrink-right.ck
zishrink-posix.ck zishrink-right.ck: \
  zic leapseconds $(PACKRATDATA) $(PACKRATLIST) \
  $(TDATA) $(DATAFORM).zi tzdata.zi
		rm -fr $@d t-$@d shrunk-$@d
		mkdir $@d t-$@d shrunk-$@d
		case $@ in \
		  *right*) leap='-L leapseconds';; \
		  *) leap=;; \
		esac && \
		  $(ZIC) $$leap -d $@d $(DATAFORM).zi && \
		  $(ZIC) $$leap -d shrunk-$@d tzdata.zi && \
		  case $(DATAFORM),$(PACKRATLIST) in \
		    main,) \
		      $(ZIC) $$leap -d t-$@d $(TDATA) && \
		      $(AWK) '/^Rule/' $(TDATA) | \
			$(ZIC) $$leap -d t-$@d - $(PACKRATDATA) && \
		      diff -r $@d t-$@d;; \
		  esac
		diff -r $@d shrunk-$@d
		rm -fr $@d t-$@d shrunk-$@d
		touch $@

clean_misc:
		rm -fr *.ckd *.dir
		rm -f *.ck *.core *.o *.out core core.* \
		  date tzdir.h tzselect version.h zdump zic libtz.a
clean:		clean_misc
		rm -fr tzdb-*/
		rm -f *.zi $(TZS_NEW)

maintainer-clean: clean
		@echo 'This command is intended for maintainers to use; it'
		@echo 'deletes files that may need special tools to rebuild.'
		rm -f leapseconds version $(MANTXTS) $(TZS) *.asc *.tar.*

names:
		@echo $(ENCHILADA)

public: check public.ck $(CHECK_TIME_T_ALTERNATIVES) \
		tarballs signatures

date.1.txt:	date.1
newctime.3.txt:	newctime.3
newstrftime.3.txt: newstrftime.3
newtzset.3.txt:	newtzset.3
time2posix.3.txt: time2posix.3
tzfile.5.txt:	tzfile.5
tzselect.8.txt:	tzselect.8
zdump.8.txt:	zdump.8
zic.8.txt:	zic.8

$(MANTXTS):	workman.sh
		LC_ALL=C sh workman.sh $(@:.txt=) >$@.out
		mv $@.out $@

# Set file timestamps deterministically if possible,
# so that tarballs containing the timestamps are reproducible.
#
# '$(SET_TIMESTAMP_N) N DEST A B C ...' sets the timestamp of the
# file DEST to the maximum of the timestamps of the files A B C ...,
# plus N if GNU ls and touch are available.
SET_TIMESTAMP_N = sh -c '\
  n=$$0 dest=$$1; shift; \
  <"$$dest" && \
  if test $$n != 0 && \
     lsout=$$(ls -nt --time-style="+%s" "$$@" 2>/dev/null); then \
    set x $$lsout && \
    timestamp=$$(($$7 + $$n)) && \
    echo "+ touch -md @$$timestamp $$dest" && \
    touch -md @$$timestamp "$$dest"; \
  else \
    newest=$$(ls -t "$$@" | sed 1q) && \
    echo "+ touch -mr $$newest $$dest" && \
    touch -mr "$$newest" "$$dest"; \
  fi'
# If DEST depends on A B C ... in this Makefile, callers should use
# $(SET_TIMESTAMP_DEP) DEST A B C ..., for the benefit of any
# downstream 'make' that considers equal timestamps to be out of date.
# POSIX allows this 'make' behavior, and HP-UX 'make' does it.
# If all that matters is that the timestamp be reproducible
# and plausible, use $(SET_TIMESTAMP).
SET_TIMESTAMP = $(SET_TIMESTAMP_N) 0
SET_TIMESTAMP_DEP = $(SET_TIMESTAMP_N) 1

# Set the timestamps to those of the git repository, if available,
# and if the files have not changed since then.
# This uses GNU 'ls --time-style=+%s', which outputs the seconds count,
# and GNU 'touch -d@N FILE', where N is the number of seconds since 1970.
# If git or GNU is absent, don't bother to sync with git timestamps.
# Also, set the timestamp of each prebuilt file like 'leapseconds'
# to be the maximum of the files it depends on.
set-timestamps.out: $(EIGHT_YARDS)
		rm -f $@
		if (type git) >/dev/null 2>&1 && \
		   files=$$(git ls-files $(EIGHT_YARDS)) && \
		   touch -md @1 test.out; then \
		  rm -f test.out && \
		  for file in $$files; do \
		    if git diff --quiet HEAD $$file; then \
		      time=$$(TZ=UTC0 git log -1 \
			--format='tformat:%cd' \
			--date='format:%Y-%m-%dT%H:%M:%SZ' \
			$$file) && \
		      echo "+ touch -md $$time $$file" && \
		      touch -md $$time $$file; \
		    else \
		      echo >&2 "$$file: warning: does not match repository"; \
		    fi || exit; \
		  done; \
		fi
		$(SET_TIMESTAMP_DEP) leapseconds $(LEAP_DEPS)
		for file in $(MANTXTS); do \
		  $(SET_TIMESTAMP_DEP) $$file $${file%.txt} workman.sh || \
		    exit; \
		done
		$(SET_TIMESTAMP_DEP) version $(VERSION_DEPS)
		$(SET_TIMESTAMP_DEP) tzdata.zi $(TZDATA_ZI_DEPS)
		touch $@
set-tzs-timestamp.out: $(TZS)
		$(SET_TIMESTAMP_DEP) $(TZS) $(TZS_DEPS)
		touch $@

# The zics below ensure that each data file can stand on its own.
# We also do an all-files run to catch links to links.

public.ck: $(VERSION_DEPS)
		rm -fr $@d
		mkdir $@d
		ln $(VERSION_DEPS) $@d
		cd $@d \
		  && $(MAKE) CFLAGS='$(GCC_DEBUG_FLAGS)' TZDIR='$(TZDIR)' ALL
		for i in $(TDATA_TO_CHECK) \
		    tzdata.zi vanguard.zi main.zi rearguard.zi; \
		do \
		  $@d/zic -v -d $@d/zoneinfo $@d/$$i || exit; \
		done
		$@d/zic -v -d $@d/zoneinfo-all $(TDATA_TO_CHECK)
		:
		: Also check 'backzone' syntax.
		rm $@d/main.zi
		cd $@d && $(MAKE) PACKRATDATA=backzone main.zi
		$@d/zic -d $@d/zoneinfo main.zi
		rm $@d/main.zi
		cd $@d && \
		  $(MAKE) PACKRATDATA=backzone PACKRATLIST=zone.tab main.zi
		$@d/zic -d $@d/zoneinfo main.zi
		:
		rm -fr $@d
		touch $@

# Check that the code works under various alternative
# implementations of time_t.
check_time_t_alternatives: $(TIME_T_ALTERNATIVES)
$(TIME_T_ALTERNATIVES_TAIL): $(TIME_T_ALTERNATIVES_HEAD)
$(TIME_T_ALTERNATIVES): $(VERSION_DEPS)
		rm -fr $@d
		mkdir $@d
		ln $(VERSION_DEPS) $@d
		case $@ in \
		  *32_t*) range=-2147483648,2147483648;; \
		  u*) range=0,4294967296;; \
		  *) range=-4294967296,4294967296;; \
		esac && \
		wd=$$PWD && \
		zones=$$($(AWK) '/^[^#]/ { print $$3 }' <zone1970.tab) && \
		if test $@ = $(TIME_T_ALTERNATIVES_HEAD); then \
		  range_target=; \
		else \
		  range_target=to$$range.tzs; \
		fi && \
		(cd $@d && \
		  $(MAKE) TOPDIR="$$wd/$@d" \
		    CFLAGS='$(CFLAGS) -Dtime_tz='"'$(@:.ck=)'" \
		    REDO='$(REDO)' \
		    D="$$wd/$@d" \
		    TZS_YEAR="$$range" TZS_CUTOFF_FLAG="-t $$range" \
		    install $$range_target) && \
		test $@ = $(TIME_T_ALTERNATIVES_HEAD) || { \
		  (cd $(TIME_T_ALTERNATIVES_HEAD)d && \
		    $(MAKE) TOPDIR="$$wd/$@d" \
		      TZS_YEAR="$$range" TZS_CUTOFF_FLAG="-t $$range" \
		      D="$$wd/$@d" \
		      to$$range.tzs) && \
		  $(SETUP_DIFF_TZS) && \
		  $$DIFF_TZS $(TIME_T_ALTERNATIVES_HEAD)d/to$$range.tzs \
			  $@d/to$$range.tzs && \
		  if diff -q Makefile Makefile 2>/dev/null; then \
		    quiet_option='-q'; \
		  else \
		    quiet_option=''; \
		  fi && \
		    diff $$quiet_option -r $(TIME_T_ALTERNATIVES_HEAD)d/etc \
					   $@d/etc && \
		    diff $$quiet_option -r \
		      $(TIME_T_ALTERNATIVES_HEAD)d/usr/share \
		      $@d/usr/share; \
		}
		touch $@

TRADITIONAL_ASC = \
  tzcode$(VERSION).tar.gz.asc \
  tzdata$(VERSION).tar.gz.asc
REARGUARD_ASC = \
  tzdata$(VERSION)-rearguard.tar.gz.asc
ALL_ASC = $(TRADITIONAL_ASC) $(REARGUARD_ASC) \
  tzdb-$(VERSION).tar.lz.asc

tarballs rearguard_tarballs tailored_tarballs traditional_tarballs \
signatures rearguard_signatures traditional_signatures: \
  version set-timestamps.out rearguard.zi vanguard.zi
		read -r VERSION <version && \
		$(MAKE) AWK='$(AWK)' VERSION="$$VERSION" $@_version

# These *_version rules are intended for use if VERSION is set by some
# other means.  Ordinarily these rules are used only by the above
# non-_version rules, which set VERSION on the 'make' command line.
tarballs_version: traditional_tarballs_version rearguard_tarballs_version \
  tzdb-$(VERSION).tar.lz
rearguard_tarballs_version: \
  tzdata$(VERSION)-rearguard.tar.gz
traditional_tarballs_version: \
  tzcode$(VERSION).tar.gz tzdata$(VERSION).tar.gz
tailored_tarballs_version: \
  tzdata$(VERSION)-tailored.tar.gz
signatures_version: $(ALL_ASC)
rearguard_signatures_version: $(REARGUARD_ASC)
traditional_signatures_version: $(TRADITIONAL_ASC)

tzcode$(VERSION).tar.gz: set-timestamps.out
		$(SETUP_TAR) && \
		$$TAR -cf - \
		    $(COMMON) $(DOCS) $(SOURCES) | \
		  gzip $(GZIPFLAGS) >$@.out
		mv $@.out $@

tzdata$(VERSION).tar.gz: set-timestamps.out
		$(SETUP_TAR) && \
		$$TAR -cf - $(TZDATA_DIST) | \
		  gzip $(GZIPFLAGS) >$@.out
		mv $@.out $@

# Create empty files with a reproducible timestamp.
CREATE_EMPTY = TZ=UTC0 touch -mt 202010122253.00

# The obsolescent *rearguard* targets and related macros are present
# for backwards compatibility with tz releases 2018e through 2022a.
# They should go away eventually.  To build rearguard tarballs you
# can instead use 'make DATAFORM=rearguard tailored_tarballs'.
tzdata$(VERSION)-rearguard.tar.gz: rearguard.zi set-timestamps.out
		rm -fr $@.dir
		mkdir $@.dir
		ln $(TZDATA_DIST) $@.dir
		cd $@.dir && rm -f $(TDATA) $(PACKRATDATA) version
		for f in $(TDATA) $(PACKRATDATA); do \
		  rearf=$@.dir/$$f; \
		  $(AWK) -v DATAFORM=rearguard -f ziguard.awk $$f >$$rearf && \
		  $(SET_TIMESTAMP_DEP) $$rearf ziguard.awk $$f || exit; \
		done
		sed '1s/$$/-rearguard/' <version >$@.dir/version
		: The dummy pacificnew pacifies TZUpdater 2.3.1 and earlier.
		$(CREATE_EMPTY) $@.dir/pacificnew
		touch -mr version $@.dir/version
		$(SETUP_TAR) && \
		  (cd $@.dir && \
		   $$TAR -cf - \
			$(TZDATA_DIST) pacificnew | \
		     gzip $(GZIPFLAGS)) >$@.out
		mv $@.out $@

# Create a tailored tarball suitable for TZUpdater and compatible tools.
# For example, 'make DATAFORM=vanguard tailored_tarballs' makes a tarball
# useful for testing whether TZUpdater supports vanguard form.
# The generated tarball is not byte-for-byte equivalent to a hand-tailored
# traditional tarball, as data entries are put into 'etcetera' even if they
# came from some other source file.  However, the effect should be the same
# for ordinary use, which reads all the source files.
tzdata$(VERSION)-tailored.tar.gz: set-timestamps.out
		rm -fr $@.dir
		mkdir $@.dir
		: The dummy pacificnew pacifies TZUpdater 2.3.1 and earlier.
		if test $(DATAFORM) = vanguard; then \
		  pacificnew=; \
		else \
		  pacificnew=pacificnew; \
		fi && \
		cd $@.dir && \
		  $(CREATE_EMPTY) $(PRIMARY_YDATA) $(NDATA) backward \
		  $$pacificnew
		(grep '^#' tzdata.zi && echo && cat $(DATAFORM).zi) \
		  >$@.dir/etcetera
		touch -mr tzdata.zi $@.dir/etcetera
		sed -n \
		  -e '/^# *version  *\(.*\)/h' \
		  -e '/^# *ddeps  */H' \
		  -e '$$!d' \
		  -e 'g' \
		  -e 's/^# *version  *//' \
		  -e 's/\n# *ddeps  */-/' \
		  -e 's/ /-/g' \
		  -e 'p' \
		  <tzdata.zi >$@.dir/version
		touch -mr version $@.dir/version
		links= && \
		  for file in $(TZDATA_DIST); do \
		    test -f $@.dir/$$file || links="$$links $$file"; \
		  done && \
		  ln $$links $@.dir
		$(SETUP_TAR) && \
		  (cd $@.dir && \
		   $$TAR -cf - * | gzip $(GZIPFLAGS)) >$@.out
		mv $@.out $@

tzdb-$(VERSION).tar.lz: set-timestamps.out set-tzs-timestamp.out
		rm -fr tzdb-$(VERSION)
		mkdir tzdb-$(VERSION)
		ln $(ENCHILADA) tzdb-$(VERSION)
		$(SET_TIMESTAMP) tzdb-$(VERSION) tzdb-$(VERSION)/*
		$(SETUP_TAR) && \
		$$TAR -cf - tzdb-$(VERSION) | lzip -9 >$@.out
		mv $@.out $@

tzcode$(VERSION).tar.gz.asc: tzcode$(VERSION).tar.gz
tzdata$(VERSION).tar.gz.asc: tzdata$(VERSION).tar.gz
tzdata$(VERSION)-rearguard.tar.gz.asc: tzdata$(VERSION)-rearguard.tar.gz
tzdb-$(VERSION).tar.lz.asc: tzdb-$(VERSION).tar.lz
$(ALL_ASC):
		$(GPG) --armor --detach-sign $?

TYPECHECK_CFLAGS = $(CFLAGS) -DTYPECHECK -D__time_t_defined -D_TIME_T
typecheck: long-long.ck unsigned.ck
long-long.ck unsigned.ck: $(VERSION_DEPS)
		rm -fr $@d
		mkdir $@d
		ln $(VERSION_DEPS) $@d
		cd $@d && \
		  case $@ in \
		    long-long.*) i="long long";; \
		    unsigned.* ) i="unsigned" ;; \
		  esac && \
		  $(MAKE) \
		    CFLAGS="$(TYPECHECK_CFLAGS) \"-Dtime_t=$$i\"" \
		    TOPDIR="$$PWD" \
		    install
		$@d/zdump -i -c 1970,1971 Europe/Rome
		touch $@

zonenames:	tzdata.zi
		@$(AWK) '/^Z/ { print $$2 } /^L/ { print $$3 }' tzdata.zi

asctime.o:	private.h
date.o:		private.h
difftime.o:	private.h
localtime.o:	private.h tzdir.h tzfile.h
strftime.o:	localtime.c private.h tzdir.h tzfile.h
zdump.o:	private.h version.h
zic.o:		private.h tzdir.h tzfile.h version.h

.PHONY: ALL INSTALL all
.PHONY: check check_mild check_time_t_alternatives
.PHONY: check_web check_zishrink
.PHONY: clean clean_misc commit-leap-seconds.list dummy.zd
.PHONY: fetch-leap-seconds.list force_tzs
.PHONY: install maintainer-clean names
.PHONY: posix_only posix_right public
.PHONY: rearguard_signatures rearguard_signatures_version
.PHONY: rearguard_tarballs rearguard_tarballs_version
.PHONY: right_only right_posix signatures signatures_version
.PHONY: tarballs tarballs_version
.PHONY: traditional_signatures traditional_signatures_version
.PHONY: traditional_tarballs traditional_tarballs_version
.PHONY: tailored_tarballs tailored_tarballs_version
.PHONY: typecheck
.PHONY: zonenames zones
.PHONY: $(ZDS)
