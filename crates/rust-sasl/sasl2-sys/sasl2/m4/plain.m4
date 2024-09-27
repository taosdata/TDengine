dnl Check for PLAIN (and therefore crypt)

AC_DEFUN([SASL_PLAIN_CHK],[
AC_REQUIRE([SASL2_CRYPT_CHK])

dnl PLAIN
 AC_ARG_ENABLE(plain, [  --enable-plain          enable PLAIN authentication [yes] ],
  plain=$enableval,
  plain=yes)

 PLAIN_LIBS=""
 if test "$plain" != no; then
  dnl In order to compile plain, we need crypt.
  if test "$cmu_have_crypt" = yes; then
    PLAIN_LIBS=$LIB_CRYPT
  fi
 fi
 AC_SUBST(PLAIN_LIBS)

 AC_MSG_CHECKING(PLAIN)
 if test "$plain" != no; then
  AC_MSG_RESULT(enabled)
  SASL_MECHS="$SASL_MECHS libplain.la"
  if test "$enable_static" = yes; then
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS plain.o"
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/plain.c"
    AC_DEFINE(STATIC_PLAIN,[],[Link PLAIN Staticly])
  fi
 else
  AC_MSG_RESULT(disabled)
 fi
])
