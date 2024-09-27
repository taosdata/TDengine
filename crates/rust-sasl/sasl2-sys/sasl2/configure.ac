dnl configure.ac for the SASL library
dnl Rob Siemborski
dnl Rob Earhart
dnl $Id: configure.ac,v 1.224 2011/09/22 14:44:15 mel Exp $
dnl
dnl Copyright (c) 2001 Carnegie Mellon University.  All rights reserved.
dnl
dnl Redistribution and use in source and binary forms, with or without
dnl modification, are permitted provided that the following conditions
dnl are met:
dnl
dnl 1. Redistributions of source code must retain the above copyright
dnl    notice, this list of conditions and the following disclaimer. 
dnl
dnl 2. Redistributions in binary form must reproduce the above copyright
dnl    notice, this list of conditions and the following disclaimer in
dnl    the documentation and/or other materials provided with the
dnl    distribution.
dnl
dnl 3. The name "Carnegie Mellon University" must not be used to
dnl    endorse or promote products derived from this software without
dnl    prior written permission. For permission or any other legal
dnl    details, please contact  
dnl      Office of Technology Transfer
dnl      Carnegie Mellon University
dnl      5000 Forbes Avenue
dnl      Pittsburgh, PA  15213-3890
dnl      (412) 268-4387, fax: (412) 268-7395
dnl      tech-transfer@andrew.cmu.edu
dnl
dnl 4. Redistributions of any form whatsoever must retain the following
dnl    acknowledgment:
dnl    \"This product includes software developed by Computing Services
dnl     at Carnegie Mellon University (http://www.cmu.edu/computing/).\"
dnl
dnl CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
dnl THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
dnl AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
dnl FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
dnl WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
dnl AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
dnl OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
dnl

AC_PREREQ([2.69])

dnl
dnl REMINDER: When changing the version number here, please also update
dnl the values in win32/include/config.h and include/sasl.h as well.
dnl
AC_INIT([cyrus-sasl],[2.1.28],[https://github.com/cyrusimap/cyrus-sasl/issues],[cyrus-sasl],[https://www.cyrusimap.org])

AC_CONFIG_MACRO_DIR([m4])

dnl use ./config.cache as the default cache file.
dnl we require a cache file to successfully configure our build.
if test $cache_file = "/dev/null"; then
cache_file="./config.cache"
AC_CACHE_LOAD
fi

AC_CONFIG_AUX_DIR(config)
AC_CANONICAL_HOST
AC_CANONICAL_TARGET

AM_INIT_AUTOMAKE([1.11 tar-ustar dist-bzip2 foreign -Wno-portability subdir-objects])

AM_MAINTAINER_MODE

DIRS=""

AC_ARG_ENABLE(cmulocal,
              [AS_HELP_STRING([--enable-cmulocal],[enable local mods for CMU [[no]]])],
              [],
              enable_cmulocal=no)

AC_ARG_ENABLE(sample,
              [AS_HELP_STRING([--enable-sample],[compile sample code [[yes]]])],
              [],
              enable_sample=yes)

AC_ARG_ENABLE(obsolete_cram_attr,
              [AS_HELP_STRING([--enable-obsolete_cram_attr],[enable support for cmusaslsecretCRAM-MD5 auxprop property [[yes]]])],
              enable_obsolete_cram_attr=$enableval,
              enable_obsolete_cram_attr=yes)

AC_ARG_ENABLE(obsolete_digest_attr,
              [AS_HELP_STRING([--enable-obsolete_digest_attr],[enable support for cmusaslsecretDIGEST-MD5 auxprop property [[yes]]])],
              enable_obsolete_digest_attr=$enableval,
              enable_obsolete_digest_attr=yes)

AC_PROG_CC
AX_PROG_CC_FOR_BUILD
AC_PROG_CPP
AC_PROG_AWK
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_INSTALL
AC_USE_SYSTEM_EXTENSIONS

dnl check for -R, etc. switch
CMU_GUESS_RUNPATH_SWITCH

dnl xxx compatibility
AC_ARG_WITH(staticsasl)
if test "$with_staticsasl" = yes; then
	enable_shared=yes
	enable_static=yes
fi

save_target=$target
if test -z "$target"; then
	target="NONE"
fi

# new libtool
AC_DISABLE_STATIC([])
LT_INIT

target=$save_target

if test "$enable_static" = yes; then
	SASL_STATIC_LIB=libsasl2.a
else
	SASL_STATIC_LIB=
fi
AC_SUBST(SASL_STATIC_LIB)
SASL_STATIC_LIBS=

AC_ARG_ENABLE(staticdlopen, [  --enable-staticdlopen   try dynamic plugins when we are a static libsasl [[no]] ],
                enable_staticdlopen=$enableval,
                enable_staticdlopen=no)

if test "$enable_staticdlopen" = yes; then
  AC_DEFINE(TRY_DLOPEN_WHEN_STATIC,[],[Should we try to dlopen() plugins while statically compiled?])
fi

if test "$ac_cv_c_compiler_gnu" = yes; then
  CFLAGS="-Wall -W ${CFLAGS}"
fi

AC_ARG_WITH(purecov,[  --with-purecov          link with purecov])
if test "$with_purecov" = yes; then
  AC_CHECK_PROGS(PURECOV, purecov)
fi
AC_ARG_WITH(purify,[  --with-purify           link with purify])
if test "$with_purify" = yes; then
  AC_CHECK_PROGS(PURIFY, purify)
fi

AM_CONDITIONAL(SAMPLE, test "$enable_sample" = yes)

dnl call before we do the berkeley DB checks
CMU_SOCKETS

dnl we extracted this to config/sasldb.m4
SASL_DB_PATH_CHECK()
SASL_DB_CHECK()

# Do we not install the SASL DB man pages?
AM_CONDITIONAL(NO_SASL_DB_MANS, test "x$SASL_DB_MANS" = "x")

AC_ARG_ENABLE(keep_db_open, [  --enable-keep-db-open   keep handle to DB open for improved performance [[no]] ],
                keep_db_open=$enableval,
                keep_db_open=no)

# Disable if Berkeley DB and LMDB are not used
if test "$dblib" != berkeley -a "$dblib" != lmdb; then
  keep_db_open=no
fi

if test "$keep_db_open" = yes; then
  AC_DEFINE(KEEP_DB_OPEN,[],[Should we keep handle to DB open in SASLDB plugin?])
fi
AC_MSG_CHECKING(if DB handle is kept open in SASLDB)
AC_MSG_RESULT($keep_db_open)

AC_CHECK_LIB(dl, dlopen, SASL_DL_LIB="-ldl", SASL_DL_LIB="")
AC_SUBST(SASL_DL_LIB)

dnl /dev/random ?

AC_ARG_WITH(devrandom, [  --with-devrandom=PATH   set the path to pseudo random number generator [[/dev/urandom]] ],
  devrandom=$withval,
  devrandom=/dev/urandom)
AC_MSG_CHECKING(PRNG to use)
AC_MSG_RESULT($devrandom)
AC_DEFINE_UNQUOTED(SASL_DEV_RANDOM, "$devrandom", [File to use for source of randomness])

dnl Do we need leading underscores on our symbols?

AC_CHECK_PROGS(NM, nm)

AC_MSG_CHECKING(for underscore before symbols)
AC_CACHE_VAL(sasl_cv_uscore,[
    echo "int main(){int i=1; return 0;}
    void foo(){int i=6;}" > conftest.c
    ${CC} -o a.out conftest.c > /dev/null
    if (${NM} a.out | grep _foo) > /dev/null; then
      sasl_cv_uscore=yes
    else
      sasl_cv_uscore=no
    fi])
AC_MSG_RESULT($sasl_cv_uscore)
rm -f conftest.c a.out

if test $sasl_cv_uscore = yes; then
  if test $ac_cv_lib_dl_dlopen = yes ; then
	AC_MSG_CHECKING(whether dlsym adds the underscore for us)
	cmu_save_LIBS="$LIBS"
	LIBS="$LIBS $SASL_DL_LIB"
	AC_CACHE_VAL([sasl_cv_dlsym_adds_uscore],
	             [AC_RUN_IFELSE([AC_LANG_SOURCE([[
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
void foo() { int i=0;}
int main() { void *self, *ptr1, *ptr2; self=dlopen(NULL,RTLD_LAZY);
    if(self) { ptr1=dlsym(self,"foo"); ptr2=dlsym(self,"_foo");
    if(ptr1 && !ptr2) exit(0); } exit(1); } 
]])],[sasl_cv_dlsym_adds_uscore=yes],[sasl_cv_dlsym_adds_uscore=no],[AC_MSG_WARN(cross-compiler, we'll do our best)])])
	LIBS="$cmu_save_LIBS"
      AC_MSG_RESULT($sasl_cv_dlsym_adds_uscore)

      if test "$sasl_cv_dlsym_adds_uscore" = no; then
        AC_DEFINE(DLSYM_NEEDS_UNDERSCORE, [], [Do we need a leading _ for dlsym?])
      fi
  fi
fi

dnl See if we can provide a default logging function...
AC_CHECK_FUNCS(syslog)

AC_ARG_WITH(saslauthd, [  --with-saslauthd=DIR    enable use of the saslauth daemon using state dir DIR ],
		with_saslauthd=$withval,
		with_saslauthd=yes)
if test "$with_saslauthd" != no; then
  if test "$with_saslauthd" = yes; then
    with_saslauthd="/var/state/saslauthd"
  fi
  AC_DEFINE(HAVE_SASLAUTHD,[],[Include support for saslauthd?])
  AC_DEFINE_UNQUOTED(PATH_SASLAUTHD_RUNDIR, "$with_saslauthd",
		     [Where do we look for saslauthd's socket?])
fi
AM_CONDITIONAL(SASLAUTHD, test "$with_saslauthd" != no)
AC_MSG_CHECKING(if I should include saslauthd)
AC_MSG_RESULT($with_saslauthd)

AC_ARG_WITH(authdaemond, [  --with-authdaemond=PATH enable use of authdaemon with default socket=PATH [[yes]] ],
		with_authdaemon=$withval,
		with_authdaemon=yes)
if test "$with_authdaemon" != no; then
  if test "$with_authdaemon" = yes; then
    with_authdaemon="/dev/null"
  fi
  AC_DEFINE(HAVE_AUTHDAEMON,[],[Include support for Courier's authdaemond?])
  AC_DEFINE_UNQUOTED(PATH_AUTHDAEMON_SOCKET, "$with_authdaemon",
		     [Where do we look for Courier authdaemond's socket?])
fi
AC_MSG_CHECKING(to include Courier authdaemond support)
AC_MSG_RESULT($with_authdaemon)

AC_ARG_WITH(pwcheck,
[  --with-pwcheck=DIR     enable deprecated pwcheck daemon using statedir DIR ],
	with_pwcheck=$withval,
	with_pwcheck=no)
if test "$with_pwcheck" != no; then
   if test "$with_pwcheck" = yes; then
     with_pwcheck=/var/pwcheck
   fi
   AC_DEFINE(HAVE_PWCHECK,[],[Include Support for pwcheck daemon?])
   AC_DEFINE_UNQUOTED(PWCHECKDIR, "$with_pwcheck", [Location of pwcheck socket])
   AC_CHECK_FUNC(getspnam,PWCHECKMETH="getspnam",PWCHECKMETH="getpwnam")
   AC_SUBST(PWCHECKMETH)
fi
AM_CONDITIONAL(PWCHECK, test "$with_pwcheck" != no)
AC_MSG_CHECKING(if I should include pwcheck)
AC_MSG_RESULT($with_pwcheck)

AC_ARG_WITH(ipctype, [  --with-ipctype={unix,doors}    use ipctype [[unix]] ],
       with_ipctype=$withval,
       with_ipctype="unix")
IPCTYPE=$with_ipctype
AC_SUBST(IPCTYPE)
LIB_DOOR=
if test "$with_ipctype" = "doors"; then
   LIB_DOOR="-ldoor"
   AC_DEFINE(USE_DOORS,[],[use the doors IPC API for saslauthd?])
fi
AC_SUBST(LIB_DOOR)

AC_ARG_ENABLE(alwaystrue, [  --enable-alwaystrue     enable the alwaystrue password verifier (discouraged)],
		enable_alwaystrue=$enableval,
		enable_alwaystrue=no)
if test "$enable_alwaystrue" = yes; then
  AC_DEFINE(HAVE_ALWAYSTRUE, [], [Enable 'alwaystrue' password verifier?])
fi
AC_MSG_CHECKING(if I should include the alwaystrue verifier)
AC_MSG_RESULT($enable_alwaystrue)

dnl sasl_checkapop support
AC_ARG_ENABLE(checkapop, [  --enable-checkapop      enable use of sasl_checkapop [[yes]] ],
  checkapop=$enableval,
  checkapop=yes)

AC_MSG_CHECKING(if we should enable sasl_checkapop)
if test "$checkapop" != no; then
  AC_MSG_RESULT(enabled)
  AC_DEFINE(DO_SASL_CHECKAPOP, [], [should we support sasl_checkapop?])
else
  AC_MSG_RESULT(disabled)
fi

dnl CRAM-MD5
AC_ARG_ENABLE(cram, [  --enable-cram           enable CRAM-MD5 authentication [[yes]] ],
  cram=$enableval,
  cram=yes)

AC_MSG_CHECKING(CRAM-MD5)
if test "$cram" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS libcrammd5.la"
  if test "$enable_obsolete_cram_attr" = yes; then
    CPPFLAGS="$CPPFLAGS -DOBSOLETE_CRAM_ATTR=1"
  fi
  if test "$enable_static" = yes; then
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS cram.o"
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/cram.c"
    AC_DEFINE(STATIC_CRAMMD5, [], [Link CRAM-MD5 Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

CMU_HAVE_OPENSSL
AC_MSG_CHECKING(for OpenSSL)
AC_MSG_RESULT($with_openssl)

SASL_DES_CHK

dnl DIGEST-MD5
AC_ARG_ENABLE(digest, [  --enable-digest         enable DIGEST-MD5 authentication [[yes]] ],
  digest=$enableval,
  digest=yes)

if test "$digest" != no; then
  dnl In order to compile digest, we should look for need libdes.
  if test -d $digest; then
    CPPFLAGS="$CPPFLAGS -I$digest/include"
    LDFLAGS="$LDFLAGS -L$digest/lib"
  fi
  if test "$with_des" = no; then
    AC_MSG_WARN(No DES support for DIGEST-MD5)
  fi
fi

AC_MSG_CHECKING(DIGEST-MD5)
if test "$digest" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS libdigestmd5.la"
  if test "$enable_obsolete_digest_attr" = yes; then
    CPPFLAGS="$CPPFLAGS -DOBSOLETE_DIGEST_ATTR=1"
  fi
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/digestmd5.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS digestmd5.o"
    SASL_STATIC_LIBS="$SASL_STATIC_LIBS $LIB_DES"
    AC_DEFINE(STATIC_DIGESTMD5, [], [Link DIGEST-MD5 Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

dnl SCRAM
AC_ARG_ENABLE(scram, [  --enable-scram            enable SCRAM authentication [[yes]] ],
  scram=$enableval,
  scram=yes)

if test "$with_openssl" = no; then
  AC_MSG_WARN(OpenSSL not found -- SCRAM will be disabled)
  scram=no
fi

AC_MSG_CHECKING(SCRAM)
if test "$scram" != no; then
  AC_MSG_RESULT(enabled)
  SCRAM_LIBS="-lcrypto $LIB_RSAREF"

  SASL_MECHS="$SASL_MECHS libscram.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/scram.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS scram.o"
    AC_DEFINE(STATIC_SCRAM, [], [Link SCRAM Statically])
  fi

  AC_SUBST(SCRAM_LIBS)
else
  AC_MSG_RESULT(disabled)
fi

dnl OTP
AC_ARG_ENABLE(otp, [  --enable-otp            enable OTP authentication [[yes]] ],
  otp=$enableval,
  otp=yes)

if test "$with_openssl" = no; then
  AC_MSG_WARN(OpenSSL not found -- OTP will be disabled)
  otp=no
fi

AC_MSG_CHECKING(OTP)
if test "$otp" != no; then
  AC_MSG_RESULT(enabled)
  OTP_LIBS="-lcrypto $LIB_RSAREF"

  SASL_MECHS="$SASL_MECHS libotp.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/otp.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS otp.o"
    AC_DEFINE(STATIC_OTP, [], [Link OTP Statically])
  fi

  dnl Test for OPIE
  AC_ARG_WITH(opie, AS_HELP_STRING([--with-opie=PATH],[use OPIE (One Time Passwords in Everything) from PATH]))

  case "$with_opie" in
	""|yes) 
		AC_CHECK_LIB(opie, opiechallenge, [
			AC_CHECK_HEADER(opie.h, with_opie="yes",
					with_opie="no")],
			with_opie="no")
		;;
	*)
		if test -d $with_opie; then
		  CPPFLAGS="${CPPFLAGS} -I${with_opie}/include"
		  LDFLAGS="${LDFLAGS} -L${with_opie}/lib"
		else
		  with_opie="no"
		fi
		;;
  esac

  AC_MSG_CHECKING(for OPIE)
  AC_MSG_RESULT($with_opie)

  if test "$with_opie" != no; then
    AC_DEFINE(HAVE_OPIE,[],[Use OPIE for server-side OTP?])
    OTP_LIBS="$OTP_LIBS -lopie"
  fi

  AC_SUBST(OTP_LIBS)

else
  AC_MSG_RESULT(disabled)
fi

dnl SRP
AC_ARG_ENABLE(srp, [  --enable-srp            enable SRP authentication [[no]] ],
  srp=$enableval,
  srp=no)

if test "$with_openssl" = no; then
  AC_MSG_WARN(OpenSSL not found -- SRP will be disabled)
  srp=no
fi

AC_MSG_CHECKING(SRP)
if test "$srp" != no; then
  AC_MSG_RESULT(enabled)
  SRP_LIBS="-lcrypto $LIB_RSAREF"

  SASL_MECHS="$SASL_MECHS libsrp.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/srp.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS srp.o"
    AC_DEFINE(STATIC_SRP, [], [Link SRP Statically])
  fi

dnl srp_setpass support
  AC_ARG_ENABLE(srp_setpass, [  --enable-srp-setpass    enable setting SRP secrets with saslpasswd [[no]]],
      srp_setpass=$enableval,
      srp_setpass=no)

  AC_MSG_CHECKING(if we should enable setting SRP secrets with saslpasswd)
  if test "$srp_setpass" != no; then
    AC_MSG_RESULT(enabled)
    AC_DEFINE(DO_SRP_SETPASS, [], [should we support setpass() for SRP?])
  else
    AC_MSG_RESULT(disabled)
  fi

  AC_SUBST(SRP_LIBS)
else
  AC_MSG_RESULT(disabled)
fi

dnl Kerberos based Mechanisms
SASL_KERBEROS_V4_CHK
SASL_GSSAPI_CHK

if test "$gssapi" != "no"; then
  AC_DEFINE(STATIC_GSSAPIV2,[],[Link GSSAPI Statically])
  AC_DEFINE(HAVE_GSSAPI,[],[Include GSSAPI/Kerberos 5 Support])

  mutex_default="no"
  if test "$gss_impl" = "mit"; then
     mutex_default="yes"
  elif test "$gss_impl" = "heimdal"; then
     AC_DEFINE(KRB5_HEIMDAL,[],[Using Heimdal]) 
  fi

  AC_MSG_CHECKING(to use mutexes aroung GSS calls)
  AC_ARG_ENABLE(gss_mutexes, [  --enable-gss_mutexes     use mutexes around calls to the GSS library],
                use_gss_mutexes=$enableval,
                use_gss_mutexes=$mutex_default)
  if test $use_gss_mutexes = "yes"; then
     AC_DEFINE(GSS_USE_MUTEXES, [], [should we mutex-wrap calls into the GSS library?])
  fi
  AC_MSG_RESULT($use_gss_mutexes)
fi

SASL2_CRYPT_CHK

AC_ARG_ENABLE(sia, [  --enable-sia            enable SIA authentication [no] ],
  sia=$enableval,
  sia=no)
LIB_SIA=""
if test "$sia" != no; then
  if test -f /etc/sia/matrix.conf; then
    AC_DEFINE(HAVE_SIA,[],[Include SIA Support])
    LIB_SIA="-lsecurity -ldb -lm -laud"
  else
    AC_MSG_ERROR([No support for SIA found])
  fi
fi
AC_SUBST(LIB_SIA)

AC_ARG_ENABLE(auth-sasldb, [  --enable-auth-sasldb    enable experimental SASLdb authentication module [no] ],
  authsasldb=$enableval,
  authsasldb=no)
if test "$authsasldb" != no; then
  AC_DEFINE(AUTH_SASLDB,[],[Include SASLdb Support])
  SASL_DB_PATH_CHECK()
  SASL_DB_CHECK()
fi
AM_CONDITIONAL(AUTH_SASLDB, test "$authsasldb" != no)

AC_ARG_ENABLE(httpform, [  --enable-httpform       enable HTTP form authentication [[no]] ],
  httpform=$enableval,
  httpform=no)
if test "$httpform" != no; then
  AC_DEFINE(HAVE_HTTPFORM,[],[Include HTTP form Support])
fi

AC_ARG_WITH(pam, AS_HELP_STRING([--with-pam=DIR],[use PAM (rooted in DIR) [yes]]),,
	with_pam=yes)
if test "$with_pam" != no; then
  if test -d $with_pam; then
    CPPFLAGS="$CPPFLAGS -I${with_pam}/include"
    LDFLAGS="$LDFLAGS -L${with_pam}/lib"
  fi
  cmu_save_LIBS="$LIBS"
  AC_CHECK_LIB(pam, pam_start, [
  AC_CHECK_HEADER(security/pam_appl.h,,
		  with_pam=no)],
	with_pam=no, $SASL_DL_LIB)
  LIBS="$cmu_save_LIBS"
fi

AC_MSG_CHECKING(for PAM support)
AC_MSG_RESULT($with_pam)
LIB_PAM=""
if test "$with_pam" != no; then
  AC_DEFINE(HAVE_PAM,[],[Support for PAM?])
  LIB_PAM="-lpam"
fi
AC_SUBST(LIB_PAM)

dnl PLAIN
SASL_PLAIN_CHK

dnl ANONYMOUS
AC_ARG_ENABLE(anon, [  --enable-anon           enable ANONYMOUS authentication [[yes]] ],
  anon=$enableval,
  anon=yes)

AC_MSG_CHECKING(ANONYMOUS)
if test "$anon" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS libanonymous.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS anonymous.o"
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/anonymous.c"
    AC_DEFINE(STATIC_ANONYMOUS, [], [Link ANONYMOUS Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

dnl LOGIN
AC_ARG_ENABLE(login, [  --enable-login          enable unsupported LOGIN authentication [[no]] ],
  login=$enableval,
  login=no)

AC_MSG_CHECKING(LOGIN)
if test "$login" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS liblogin.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/login.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS login.o"
    AC_DEFINE(STATIC_LOGIN,[],[Link LOGIN Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

dnl NTLM
AC_ARG_ENABLE(ntlm, [  --enable-ntlm           enable unsupported NTLM authentication [[no]] ],
  ntlm=$enableval,
  ntlm=no)

if test "$with_openssl" = no; then
  AC_MSG_WARN(OpenSSL not found -- NTLM will be disabled)
  ntlm=no
fi

AC_MSG_CHECKING(NTLM)
if test "$ntlm" != no; then
  AC_MSG_RESULT(enabled)
  NTLM_LIBS="-lcrypto $LIB_RSAREF"
  AC_SUBST(NTLM_LIBS)

  SASL_MECHS="$SASL_MECHS libntlm.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/ntlm.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS ntlm.o"
    AC_DEFINE(STATIC_NTLM,[],[Link NTLM Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

dnl PASSDSS
AC_ARG_ENABLE(passdss, [  --enable-passdss        enable PASSDSS authentication (experimental) [[no]] ],
  passdss=$enableval,
  passdss=no)

if test "$with_openssl" = no; then
  AC_MSG_WARN(OpenSSL not found -- PASSDSS will be disabled)
  passdss=no
fi

AC_MSG_CHECKING(PASSDSS)
if test "$passdss" != no; then
  AC_MSG_RESULT(enabled)
  PASSDSS_LIBS="-lcrypto $LIB_RSAREF"
  AC_SUBST(PASSDSS_LIBS)

  SASL_MECHS="$SASL_MECHS libpassdss.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS passdss.o"
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/passdss.c"
    AC_DEFINE(STATIC_PASSDSS,[],[Link PASSDSS Statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi


AC_MSG_CHECKING(to include LDAP support)
AC_ARG_WITH(ldap, [  --with-ldap=DIR         use LDAP (in DIR) for saslauthd [no] ],
	with_ldap=$withval,
	with_ldap=no)
AC_MSG_RESULT($with_ldap)

if test -d $with_ldap; then
    CPPFLAGS="$CPPFLAGS -I${with_ldap}/include"
    CMU_ADD_LIBPATH(${with_ldap}/lib)
fi

LDAP_LIBS=""
if test "$with_ldap" != no; then
  AC_CHECK_LIB(ldap, ldap_initialize, [ AC_DEFINE(HAVE_LDAP,[],[Support for LDAP?])
                                        LDAP_LIBS="-lldap -llber"
					if test "$with_openssl" != "no"; then
					    LDAP_LIBS="$LDAP_LIBS -lcrypto $LIB_RSAREF"
					fi],,-llber)
fi
AC_SUBST(LDAP_LIBS)


dnl SQL
dnl This flag also changes the requirements of --with-mysql and --with-pgsql
dnl
dnl Desired behavior:
dnl
dnl doesn't require mysql or postgres if --disable-sql is chosen
dnl requires at least one (but not both) if --enable-sql is chosen

AC_ARG_ENABLE(sql, [  --enable-sql            enable SQL auxprop [[no]] ],
  sql=$enableval,
  sql=no)

AC_MSG_CHECKING(SQL)
if test "$sql" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS libsql.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/sql.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS sql.o"
    AC_DEFINE(STATIC_SQL,[],[Link SQL plugin statically])
  fi
else
  AC_MSG_RESULT(disabled)
fi

dnl MySQL
AC_ARG_WITH(mysql,  [  --with-mysql=PATH       use MySQL from PATH ],
  with_mysql=$withval,
  with_mysql=$sql)

# find location of library 
# presuming if one given then correct
if test "${with_mysql}" = "yes"; then
  with_mysql=notfound
  for mysqlloc in lib/mysql lib mysql/lib
  do
    if test -f ${prefix}/${mysqlloc}/libmysqlclient.a; then
      with_mysql="${prefix}"
      break
    elif test -f /usr/local/${mysqlloc}/libmysqlclient.a; then
      with_mysql="/usr/local"
      break
    elif test -f /usr/${mysqlloc}/libmysqlclient.a; then
      with_mysql="/usr"
      break
    fi
  done
fi

LIB_MYSQL=""

case "$with_mysql" in
    no) true;;
    notfound)
     save_LDFLAGS=$LDFLAGS
     LIB_MYSQL=`mysql_config --libs`
     LIB_MYSQL="-lmysqlclient"
     LDFLAGS="$LDFLAGS $LIB_MYSQL"
     # CPPFLAGS="${CPPFLAGS} `mysql_config --include`"
     AC_CHECK_LIB(mysqlclient, mysql_select_db,
           AC_DEFINE(HAVE_MYSQL, [], [Do we have mysql support?]),
           [AC_MSG_WARN(MySQL library mysqlclient does not work)
            with_mysql=no])
     LDFLAGS=$save_LDFLAGS
     ;;
    *)
     if test -d ${with_mysql}/lib/mysql; then
	CMU_ADD_LIBPATH_TO(${with_mysql}/lib/mysql, LIB_MYSQL)
     elif test -d ${with_mysql}/mysql/lib; then
	CMU_ADD_LIBPATH_TO(${with_mysql}/mysql/lib, LIB_MYSQL)
     elif test -d ${with_mysql}/lib; then
	CMU_ADD_LIBPATH_TO(${with_mysql}/lib, LIB_MYSQL)
     else
	CMU_ADD_LIBPATH_TO(${with_mysql}, LIB_MYSQL)
     fi

     LIB_MYSQL_DIR=$LIB_MYSQL
     LIB_MYSQL="$LIB_MYSQL -lmysqlclient"

     if test -d ${with_mysql}/include/mysql; then
         CPPFLAGS="${CPPFLAGS} -I${with_mysql}/include/mysql"
     elif test -d ${with_mysql}/mysql/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_mysql}/mysql/include"
     elif test -d ${with_mysql}/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_mysql}/include"
     elif test -d ${prefix}/include/mysql; then
         CPPFLAGS="${CPPFLAGS} -I${prefix}/include/mysql"
     else
         CPPFLAGS="${CPPFLAGS} -I${with_mysql}"
     fi

       save_LDFLAGS=$LDFLAGS
       LDFLAGS="$LDFLAGS $LIB_MYSQL_DIR"
       AC_CHECK_LIB(mysqlclient, mysql_select_db,
	   AC_DEFINE(HAVE_MYSQL, [], [Do we have mysql support?]),
           [AC_MSG_WARN(MySQL library mysqlclient does not work)
	    with_mysql=no])
       LDFLAGS=$save_LDFLAGS;;
         
esac
AC_SUBST(LIB_MYSQL)

dnl PgSQL
AC_ARG_WITH(pgsql,  [  --with-pgsql=PATH       use PostgreSQL from PATH ],
  with_pgsql=$withval,
  with_pgsql=$sql)

# find location of library 
# presuing if one given then correct
if test "${with_pgsql}" = "yes"; then
  with_pgsql=notfound
  for pgsqlloc in lib/pgsql lib pgsql/lib
  do
    if test -f ${prefix}/${pgsqlloc}/libpq.a; then
      with_pgsql="${prefix}"
      break
    elif test -f /usr/local/${pgsqlloc}/libpq.a; then
      with_pgsql="/usr/local"
      break
    elif test -f /usr/${pgsqlloc}/libpq.a; then
      with_pgsql="/usr"
      break
    fi
  done
fi

LIB_PGSQL=""

case "$with_pgsql" in
    no) true;;
    notfound)
     LIB_PGSQL="-lpq"
     # CPPFLAGS="${CPPFLAGS} -I`pg_config --includedir`"
     save_LDFLAGS=$LDFLAGS
     LDFLAGS="$LDFLAGS $LIB_PGSQL"
     AC_CHECK_LIB(pq, PQsetdbLogin, AC_DEFINE(HAVE_PGSQL,[],
         [Do we have Postgres support?]),
         [AC_MSG_WARN(PostgreSQL Library pq does not work)
          with_pgsql=no])
     LDFLAGS=$save_LDFLAGS
     ;;
    *)
     if test -d ${with_pgsql}/lib/pgsql; then
	CMU_ADD_LIBPATH_TO(${with_pgsql}/lib/pgsql, LIB_PGSQL)
     elif test -d ${with_pgsql}/pgsql/lib; then
	CMU_ADD_LIBPATH_TO(${with_pgsql}/pgsql/lib, LIB_PGSQL)
     elif test -d ${with_pgsql}/lib; then
	CMU_ADD_LIBPATH_TO(${with_pgsql}/lib, LIB_PGSQL)
     else
	CMU_ADD_LIBPATH_TO(${with_pgsql}, LIB_PGSQL)
     fi

     LIB_PGSQL_DIR=$LIB_PGSQL
     LIB_PGSQL="$LIB_PGSQL -lpq"

     if test -d ${with_pgsql}/include/pgsql; then
         CPPFLAGS="${CPPFLAGS} -I${with_pgsql}/include/pgsql"
     elif test -d ${with_pgsql}/pgsql/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_pgsql}/pgsql/include"
     elif test -d ${with_pgsql}/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_pgsql}/include"
     elif test -d ${prefix}/include; then
         CPPFLAGS="${CPPFLAGS} -I${prefix}/include"
     else
         CPPFLAGS="${CPPFLAGS} -I${with_pgsql}"
     fi


       save_LDFLAGS=$LDFLAGS
       LDFLAGS="$LDFLAGS $LIB_PGSQL_DIR"
       AC_CHECK_LIB(pq, PQsetdbLogin, AC_DEFINE(HAVE_PGSQL,[],
	   [Do we have Postgres support?]),
           [AC_MSG_WARN(PostgreSQL Library pq does not work)
	    with_pgsql=no])
       LDFLAGS=$save_LDFLAGS;;
         
esac
AC_SUBST(LIB_PGSQL)

dnl SQLite
AC_ARG_WITH(sqlite,  [  --with-sqlite=PATH       use SQLite from PATH ],
  with_sqlite=$withval,
  with_sqlite=$sql)

# find location of library
# presuing if one given then correct
if test "${with_sqlite}" = "yes"; then
  with_sqlite=notfound
  for sqliteloc in lib
  do
    if test -f ${prefix}/${sqliteloc}/libsqlite.a; then
      with_sqlite="${prefix}"
      break
    elif test -f /usr/local/${sqliteloc}/libsqlite.a; then
      with_sqlite="/usr/local"
      break
    elif test -f /usr/${sqliteloc}/libsqlite.a; then
      with_sqlite="/usr"
      break
    fi
  done
fi

LIB_SQLITE=""

case "$with_sqlite" in
    no) true;;
    notfound) AC_MSG_WARN(SQLite Library not found); true;;
    *)
     if test -d ${with_sqlite}/lib; then
         CMU_ADD_LIBPATH_TO(${with_sqlite}/lib, LIB_SQLITE)
     else
         CMU_ADD_LIBPATH_TO(${with_sqlite}, LIB_SQLITE)
     fi

     LIB_SQLITE_DIR=$LIB_SQLITE
     LIB_SQLITE="$LIB_SQLITE -lsqlite"

     if test -d ${with_sqlite}/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_sqlite}/include"
     else
         CPPFLAGS="${CPPFLAGS} -I${with_sqlite}"
     fi
       AC_CHECK_LIB(sqlite, sqlite_open, AC_DEFINE(HAVE_SQLITE,[],
	   [Do we have SQLite support?]),
           [AC_MSG_WARN(SQLite Library sqlite does not work)
	    with_sqlite=no], $LIB_SQLITE_DIR);;

esac
AC_SUBST(LIB_SQLITE)

dnl SQLite3
AC_ARG_WITH(sqlite3,  [  --with-sqlite3=PATH       use SQLite3 from PATH ],
  with_sqlite3=$withval,
  with_sqlite3=$sql)

# find location of library
# we assume that if one given then it is correct
if test "${with_sqlite3}" = "yes"; then
  with_sqlite3=notfound
  for sqlite3loc in lib
  do
    if test -f ${prefix}/${sqlite3loc}/libsqlite3.a; then
      with_sqlite3="${prefix}"
      break
    elif test -f /usr/local/${sqlite3loc}/libsqlite3.a; then
      with_sqlite3="/usr/local"
      break
    elif test -f /usr/${sqlite3loc}/libsqlite3.a; then
      with_sqlite3="/usr"
      break
    fi
  done
fi

LIB_SQLITE3=""

case "$with_sqlite3" in
    no) true;;
    notfound) AC_MSG_WARN(SQLite3 Library not found); true;;
    *)
     if test -d ${with_sqlite3}/lib; then
         CMU_ADD_LIBPATH_TO(${with_sqlite3}/lib, LIB_SQLITE3)
     else
         CMU_ADD_LIBPATH_TO(${with_sqlite3}, LIB_SQLITE3)
     fi

     LIB_SQLITE3_DIR=$LIB_SQLITE3
     LIB_SQLITE3="$LIB_SQLITE3 -lsqlite3"

     if test -d ${with_sqlite3}/include; then
         CPPFLAGS="${CPPFLAGS} -I${with_sqlite3}/include"
     else
         CPPFLAGS="${CPPFLAGS} -I${with_sqlite3}"
     fi
       AC_CHECK_LIB(sqlite3, sqlite3_open, AC_DEFINE(HAVE_SQLITE3,[],
	   [Do we have SQLite3 support?]),
           [AC_MSG_WARN(SQLite3 Library sqlite3 does not work)
	    with_sqlite3=no], $LIB_SQLITE3_DIR);;

esac
AC_SUBST(LIB_SQLITE3)

if test "$sql" = yes -a "$with_pgsql" = no -a "$with_mysql" = no -a "$with_sqlite" = no -a "$with_sqlite3" = no; then
    AC_MSG_ERROR([--enable-sql chosen but neither Postgres nor MySQL nor SQLite nor SQLite3 found])
fi

if test "$enable_shared" = yes; then
	AC_DEFINE(DO_DLOPEN,[],[Should we build a shared plugin (via dlopen) library?])
fi

dnl LDAPDB
AC_ARG_ENABLE(ldapdb, [  --enable-ldapdb         enable LDAPDB plugin [no] ],
  ldapdb=$enableval,
  ldapdb=no)
AC_MSG_CHECKING(LDAPDB)
if test "$ldapdb" != no; then
    AC_MSG_RESULT(enabled)

    if test "$with_ldap" = no; then
        AC_MSG_ERROR([Cannot enable LDAPDB plugin: You need to specify --with-ldap])
    fi

    save_CPPFLAGS=$CPPFLAGS
    save_LDFLAGS=$LDFLAGS

    if test -d $with_ldap; then
        CPPFLAGS="${CPPFLAGS} -I${with_ldap}/include"
        CMU_ADD_LIBPATH(${with_ldap}/lib)
    fi

    AC_CHECK_HEADERS(ldap.h lber.h)

    if test $ac_cv_header_ldap_h = yes -a $ac_cv_header_lber_h = yes; then
        CMU_OPENLDAP_API

        if test "$cmu_cv_openldap_api" = yes; then
            AC_CHECK_LIB(ldap, ldap_initialize, [ cmu_link_openldap="-lldap -llber" ], [ cmu_link_openldap=no ],-llber)
        fi
    fi

    if test "$cmu_cv_openldap_api" = no -o "$cmu_link_openldap" = no; then
        AC_MSG_ERROR([Cannot enable LDAPDB plugin: Could not locate OpenLDAP])
    else
        CMU_OPENLDAP_COMPAT

        if test "$cmu_cv_openldap_compat" = no; then
            AC_MSG_ERROR([Cannot enable LDAPDB plugin: OpenLDAP library located but incompatible])
        else
            LIB_LDAP=$cmu_link_openldap
            AC_SUBST(LIB_LDAP)

            SASL_MECHS="$SASL_MECHS libldapdb.la"
            if test "$enable_static" = yes; then
                SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/ldapdb.c"
                SASL_STATIC_OBJS="$SASL_STATIC_OBJS ldapdb.o"
                AC_DEFINE(STATIC_LDAPDB,[],[Link ldapdb plugin Statically])
            fi
        fi
    fi

    if test "$cmu_cv_openldap_compat" != yes; then
        CPPFLAGS=$save_CPPFLAGS
        LDFLAGS=$save_LDFLAGS
    fi
else
    AC_MSG_RESULT(disabled)
fi

AC_SUBST(SASL_MECHS)
AC_SUBST(SASL_STATIC_SRCS)
AC_SUBST(SASL_STATIC_OBJS)
AC_SUBST(SASL_STATIC_LIBS)

AC_ARG_WITH(plugindir, [  --with-plugindir=DIR    set the directory where plugins will
                          be found [[LIBDIR/sasl2]] ],
  plugindir=$withval,
  plugindir='${libdir}/sasl2')
AC_SUBST(plugindir)

AC_ARG_WITH(configdir, [   --with-configdir=DIR    set the directory where config files will
                          be found [PLUGINDIR:SYSCONFDIR/sasl2] ],
  configdir=$withval,
  configdir='${plugindir}:${sysconfdir}/sasl2')
AC_SUBST(configdir)

AC_ARG_WITH(rc4, [  --with-rc4              use rc4 routines [[yes]] ],
	with_rc4=$withval,
	with_rc4=yes)

if test "$with_rc4" != no; then
    AC_DEFINE(WITH_RC4,[],[Use RC4])
fi

building_for_macosx=no
case "$host_os" in
        darwin*)
AC_ARG_ENABLE(macos-framework, [  --disable-macos-framework       disable building and installing replacement SASL2 Framework for MacOS X-provided SASL Framework [[no]]],building_for_macosx=no,building_for_macosx=yes)
        ;;
esac
AM_CONDITIONAL(MACOSX, test "$building_for_macosx" = yes)
AM_CONDITIONAL(WINDOWS, test "$host_os" = "mingw32")

dnl dmalloc tests
AC_MSG_CHECKING(for dmalloc library)
AC_ARG_WITH(dmalloc, [  --with-dmalloc=DIR      with DMALLOC support (for test applications) [[no]] ],
	with_dmalloc=$withval,
	with_dmalloc=no)

DMALLOC_LIBS=""

if test "$with_dmalloc" != "no"; then
   if test "$with_dmalloc" = "yes"; then
	with_dmalloc="/usr/local"
   fi

   if test -r "$with_dmalloc/libdmalloc.a"; then
	DMALLOC_LIBS="$with_dmalloc/libdmalloc.a"
	AC_DEFINE(WITH_DMALLOC,[],[Linking against dmalloc?])
	AC_MSG_RESULT(yes)
   elif test -r "$with_dmalloc/lib/libdmalloc.a"; then
	DMALLOC_LIBS="$with_dmalloc/lib/libdmalloc.a"
	AC_DEFINE(WITH_DMALLOC,[],[Linking against dmalloc?])
	AC_MSG_RESULT(yes)
   else
	AC_MSG_ERROR(cannot find dmalloc library, please check your installation.)
   fi
else
   AC_MSG_RESULT(no)
fi

AC_SUBST(DMALLOC_LIBS)

dnl sfio tests
AC_MSG_CHECKING(for sfio library)
AC_ARG_WITH(sfio, [  --with-sfio=DIR         with SFIO support (for smtptest/libsfsasl) [[no]] ],
	with_sfio=$withval,
	with_sfio=no)

if test "$with_sfio" != "no"; then
   if test "$with_sfio" = "yes"; then
	with_sfio="/usr/local"
   fi

   AC_DEFUN([SFIO_INC_CHK],
	[if test -r "$with_sfio$1/sfio.h"; then SFIO_DIR=$with_sfio;
					      SFIO_INC_DIR=$with_sfio$1])

   AC_DEFUN([SFIO_LIB_CHK],[
		str="$SFIO_DIR/$1/libsfio.*"
		for i in `echo $str`; do
			if test -r $i; then
				SFIO_LIBDIR=$SFIO_DIR/$1
				break 2
			fi
		done
		])

   SFIO_INC_CHK()
   el[]SFIO_INC_CHK(/include)
   el[]SFIO_INC_CHK(/include/sfio)
   fi

   if test -z "$SFIO_DIR"; then
	AC_MSG_ERROR(Cannot find sfio.h, Please check your SFIO installation.)
   fi

   SFIO_LIB_CHK(lib)
   SFIO_LIB_CHK(lib/sfio)

   if test -z "$SFIO_LIBDIR"; then
	AC_MSG_ERROR(Cannot find sfio library, Please check your SFIO installation.)
   fi

   SFIO_INC_FLAGS="-I$SFIO_INC_DIR"
   SFIO_LIB_FLAGS="-L$SFIO_LIBDIR -lsfio"
   SMTPTEST_PROGRAM="smtptest"
   SASL_UTIL_LIBS_EXTRA=libsfsasl2.la
   SASL_UTIL_HEADERS_EXTRA=sfsasl.h

   AC_MSG_RESULT(yes)
else
   AC_MSG_RESULT(no)
   SFIO_INC_FLAGS=""
   SFIO_LIB_FLAGS=""
   SMTPTEST_PROGRAM=""
   SASL_UTIL_LIBS_EXTRA=""
   SASL_UTIL_HEADERS_EXTRA=""
fi

AC_SUBST(SFIO_INC_FLAGS)
AC_SUBST(SFIO_LIB_FLAGS)
AC_SUBST(SMTPTEST_PROGRAM)
AC_SUBST(SASL_UTIL_LIBS_EXTRA)
AC_SUBST(SASL_UTIL_HEADERS_EXTRA)

dnl check for getsubopt
sasl_cv_getsubopt=no
AC_CHECK_FUNC(getsubopt, [AC_DEFINE(HAVE_GETSUBOPT,[],
	[do we have getsubopt()?])], [sasl_cv_getsubopt=yes])
if test $sasl_cv_getsubopt = yes; then
	AC_LIBOBJ(getsubopt)
	GETSUBOPT="getsubopt.lo"
fi
AC_SUBST(GETSUBOPT)

dnl Check for snprintf
sasl_cv_snprintf=no
SNPRINTFOBJS=""
LTSNPRINTFOBJS=""
AC_CHECK_FUNC(snprintf, [AC_DEFINE(HAVE_SNPRINTF,[],[Does the system have snprintf()?])], [sasl_cv_snprintf=yes])
AC_CHECK_FUNC(vsnprintf, [AC_DEFINE(HAVE_VSNPRINTF,[],[Does the system have vsnprintf()?])], [sasl_cv_snprintf=yes])
if test $sasl_cv_snprintf = yes; then
       AC_LIBOBJ(snprintf)
       SNPRINTFOBJS="snprintf.o"
       LTSNPRINTFOBJS="snprintf.lo"
fi
AC_SUBST(SNPRINTFOBJS)
AC_SUBST(LTSNPRINTFOBJS)

dnl do we need to link in -lresolv?
AC_CHECK_LIB(resolv, inet_aton)

dnl Check for getaddrinfo
GETADDRINFOOBJS=""
sasl_cv_getaddrinfo=yes
IPv6_CHECK_FUNC(getaddrinfo, [IPv6_CHECK_FUNC(gai_strerror,
                [AC_DEFINE(HAVE_GETADDRINFO,[],[Do we have a getaddrinfo() function?])
                sasl_cv_getaddrinfo=no])])
if test $sasl_cv_getaddrinfo = yes; then
    AC_LIBOBJ(getaddrinfo)
    GETADDRINFOOBJS="getaddrinfo.o"
    LTGETADDRINFOOBJS="getaddrinfo.lo"
fi
AC_SUBST(GETADDRINFOOBJS)
AC_SUBST(LTGETADDRINFOOBJS)

dnl Check for getnameinfo
GETNAMEINFOOBJS=""
sasl_cv_getnameinfo=no
IPv6_CHECK_FUNC(getnameinfo,
		[AC_DEFINE(HAVE_GETNAMEINFO,[],[Do we have a getnameinfo() function?])], [sasl_cv_getnameinfo=yes])
if test $sasl_cv_getnameinfo = yes; then
	AC_LIBOBJ(getnameinfo)
        GETNAMEINFOOBJS="getnameinfo.o"
        LTGETNAMEINFOOBJS="getnameinfo.lo"
fi
AC_SUBST(GETNAMEINFOOBJS)
AC_SUBST(LTGETNAMEINFOOBJS)

LTLIBOBJS=`echo "$LIB@&t@OBJS" | sed 's,\.[[^.]]* ,.lo ,g;s,\.[[^.]]*$,.lo,'`
AM_CONDITIONAL(BUILD_LIBOBJ, test "$LTLIBOBJS" != "")
AC_SUBST(LTLIBOBJS)

AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_PID_T

AC_CHECK_HEADERS_ONCE([sys/time.h])

AC_HEADER_DIRENT
AC_HEADER_SYS_WAIT
AC_CHECK_HEADERS(crypt.h des.h dlfcn.h fcntl.h limits.h malloc.h paths.h strings.h sys/file.h sys/time.h syslog.h time.h unistd.h inttypes.h sys/uio.h sys/param.h sysexits.h stdarg.h varargs.h krb5.h)

IPv6_CHECK_SS_FAMILY()
IPv6_CHECK_SA_LEN()
IPv6_CHECK_SOCKLEN_T()

#AC_FUNC_MEMCMP
#AC_FUNC_VPRINTF
AC_CHECK_FUNCS(gethostname getdomainname getpwnam getspnam gettimeofday inet_aton memcpy memmem mkdir select socket strchr strdup strerror strspn strstr strtol jrand48 getpassphrase asprintf strlcat strlcpy)

if test $ac_cv_func_getspnam = yes; then
	AC_MSG_CHECKING(if getpwnam_r/getspnam_r take 5 arguments)
	AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <pwd.h>
#include <shadow.h>
		]], [[
struct passwd *pw;
struct passwd pwbuf;
char pwdata[512];
(void) getpwnam_r("bin", &pwbuf, pwdata, sizeof(pwdata), &pw);
		]])],[AC_MSG_RESULT(yes)
		 AC_DEFINE(GETXXNAM_R_5ARG, 1,
			[Define if your getpwnam_r()/getspnam_r()
			functions take 5 arguments])],[AC_MSG_RESULT(no)
	])
fi

if test $enable_cmulocal = yes; then
    AC_MSG_WARN(enabling CMU local kludges)
    AC_DEFINE(KRB4_IGNORE_IP_ADDRESS,[],[Ignore IP Address in Kerberos 4 tickets?])
    AC_DEFINE_UNQUOTED(PREFER_MECH, "KERBEROS_V4", [Force a preferred mechanism])
fi

AC_EGREP_HEADER(sockaddr_storage, sys/socket.h, [
		AC_DEFINE(HAVE_STRUCT_SOCKADDR_STORAGE,[],[Do we have struct sockaddr_stroage?])])

AC_SUBST(DIRS)

dnl documentation generation (sphinx, perl2rst)
AC_ARG_VAR(SPHINX_BUILD, [Location of sphinx-build])
AC_ARG_WITH([sphinx-build],
            AS_HELP_STRING([with-sphinx-build=(yes|no|PATH)], [Look for sphinx-build in PATH]),
            [with_sphinx_build=$withval],
            [with_sphinx_build=yes])
AS_CASE([$with_sphinx_build],
        [yes],  [AC_PATH_PROG(SPHINX_BUILD, sphinx-build)],
        [no],   [SPHINX_BUILD=''],
        [*],    [AC_PATH_PROG(SPHINX_BUILD, sphinx-build, [], [$with_sphinx_build])])
AS_IF([test -z "$SPHINX_BUILD"],
      [AC_MSG_WARN([No sphinx-build, won't be able to regenerate docs])])
AC_SUBST([SPHINX_BUILD])
AX_PROG_PERL_MODULES([Pod::POM::View::Restructured],[have_ppvr=yes],[AC_MSG_WARN([No Pod::POM::View::Restructured, won't be able to regenerate docs])])
AM_CONDITIONAL([HAVE_SPHINX_BUILD], [ test -n "$SPHINX_BUILD" -a x"$have_ppvr" = xyes])


AH_TOP([
/* acconfig.h - autoheader configuration input */
/* 
 * Copyright (c) 1998-2003 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact  
 *      Office of Technology Transfer
 *      Carnegie Mellon University
 *      5000 Forbes Avenue
 *      Pittsburgh, PA  15213-3890
 *      (412) 268-4387, fax: (412) 268-7395
 *      tech-transfer@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef CONFIG_H
#define CONFIG_H
])

AH_BOTTOM([

#define RETSIGTYPE void

/* Create a struct iovec if we need one */
#if !defined(_WIN32)
#if !defined(HAVE_SYS_UIO_H)
/* (win32 is handled in sasl.h) */
struct iovec {
    char *iov_base;
    long iov_len;
};
#else
#include <sys/types.h>
#include <sys/uio.h>
#endif
#endif

/* location of the random number generator */
#ifdef DEV_RANDOM
#undef DEV_RANDOM
#endif
#define DEV_RANDOM SASL_DEV_RANDOM

/* if we've got krb_get_err_txt, we might as well use it;
   especially since krb_err_txt isn't in some newer distributions
   (MIT Kerb for Mac 4 being a notable example). If we don't have
   it, we fall back to the krb_err_txt array */
#ifdef HAVE_KRB_GET_ERR_TEXT
#define get_krb_err_txt krb_get_err_text
#else
#define get_krb_err_txt(X) (krb_err_txt[(X)])
#endif

/* Make Solaris happy... */
#ifndef __EXTENSIONS__
#define __EXTENSIONS__
#endif

/* Make Linux happy... */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#define SASL_PATH_ENV_VAR "SASL_PATH"
#define SASL_CONF_PATH_ENV_VAR "SASL_CONF_PATH"

#include <stdlib.h>
#include <sys/types.h>
#ifndef WIN32
# include <sys/socket.h>
# include <netdb.h>
# include <netinet/in.h>
# ifdef HAVE_SYS_PARAM_H
#  include <sys/param.h>
# endif
#else /* WIN32 */
# include <winsock2.h>
#endif /* WIN32 */
#include <string.h>

#ifndef HAVE_SOCKLEN_T
typedef unsigned int socklen_t;
#endif /* HAVE_SOCKLEN_T */

#if !defined(HAVE_STRUCT_SOCKADDR_STORAGE) && !defined(WIN32)
#define	_SS_MAXSIZE	128	/* Implementation specific max size */
#define	_SS_PADSIZE	(_SS_MAXSIZE - sizeof (struct sockaddr))

struct sockaddr_storage {
	struct	sockaddr ss_sa;
	char		__ss_pad2[_SS_PADSIZE];
};
# define ss_family ss_sa.sa_family
#endif /* !HAVE_STRUCT_SOCKADDR_STORAGE */

#ifndef AF_INET6
/* Define it to something that should never appear */
#define	AF_INET6	AF_MAX
#endif

#ifndef HAVE_GETADDRINFO
#define	getaddrinfo	sasl_getaddrinfo
#define	freeaddrinfo	sasl_freeaddrinfo
#define	gai_strerror	sasl_gai_strerror
#endif

#ifndef HAVE_GETNAMEINFO
#define	getnameinfo	sasl_getnameinfo
#endif

#if !defined(HAVE_GETNAMEINFO) || !defined(HAVE_GETADDRINFO)
#include "gai.h"
#endif

#ifndef AI_NUMERICHOST   /* support glibc 2.0.x */
#define AI_NUMERICHOST  4
#define NI_NUMERICHOST  2
#define NI_NAMEREQD     4
#define NI_NUMERICSERV  8
#endif

#ifndef HAVE_SYSEXITS_H
#include "exits.h"
#else
#include "sysexits.h"
#endif

/* Get the correct time.h */
#if TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# if HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

#ifndef HIER_DELIMITER
#define HIER_DELIMITER '/'
#endif

#ifdef WIN32
#define SASL_ROOT_KEY "SOFTWARE\\Carnegie Mellon\\Project Cyrus\\SASL Library"
#define SASL_PLUGIN_PATH_ATTR "SearchPath"
#define SASL_CONF_PATH_ATTR "ConfFile"

#include <windows.h>
inline static unsigned int sleep(unsigned int seconds) {
       Sleep(seconds * 1000);
       return 0;
}
#endif

/* handy string manipulation functions */
#ifndef HAVE_STRLCPY
extern size_t saslauthd_strlcpy(char *dst, const char *src, size_t len);
#define strlcpy(x,y,z) saslauthd_strlcpy((x),(y),(z))
#endif
#ifndef HAVE_STRLCAT
extern size_t saslauthd_strlcat(char *dst, const char *src, size_t len);
#define strlcat(x,y,z) saslauthd_strlcat((x),(y),(z))
#endif
#ifndef HAVE_ASPRINTF
extern int asprintf(char **str, const char *fmt, ...);
#endif

#endif /* CONFIG_H */
])

AH_BOTTOM([#if defined __GNUC__ &&  __GNUC__ > 6
    #define GCC_FALLTHROUGH __attribute__((fallthrough));
#else
    #define GCC_FALLTHROUGH /* fall through */
#endif
])

AC_CONFIG_HEADERS(config.h)

AC_CONFIG_FILES([Makefile
libsasl2.pc
include/Makefile
saslauthd/Makefile
sasldb/Makefile
common/Makefile
plugins/Makefile
lib/Makefile
utils/Makefile
sample/Makefile
pwcheck/Makefile])
AC_OUTPUT

AC_MSG_NOTICE([

cyrus-sasl - $VERSION

• Prefix: $prefix
• Plugins: $SASL_MECHS

Now type 'make' to build $PACKAGE
])
