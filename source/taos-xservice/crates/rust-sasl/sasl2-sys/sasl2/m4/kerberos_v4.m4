dnl checking for kerberos 4 libraries (and DES)

AC_DEFUN([SASL_DES_CHK], [
AC_ARG_WITH(des, [  --with-des=DIR          with DES (look in DIR) [yes] ],
	with_des=$withval,
	with_des=yes)

LIB_DES=""
if test "$with_des" != no; then
  if test -d $with_des; then
    CPPFLAGS="$CPPFLAGS -I${with_des}/include"
    LDFLAGS="$LDFLAGS -L${with_des}/lib"
  fi

  if test "$with_openssl" != no; then
    dnl check for openssl installing -lcrypto, then make vanilla check
    AC_CHECK_LIB(crypto, des_cbc_encrypt, [
        AC_CHECK_HEADER(openssl/des.h, [AC_DEFINE(WITH_SSL_DES,[],[Use OpenSSL DES Implementation])
                                       LIB_DES="-lcrypto";
                                       with_des=yes],
                       with_des=no)],
        with_des=no, $LIB_RSAREF)

    dnl same test again, different symbol name
    if test "$with_des" = no; then
      AC_CHECK_LIB(crypto, DES_cbc_encrypt, [
        AC_CHECK_HEADER(openssl/des.h, [AC_DEFINE(WITH_SSL_DES,[],[Use OpenSSL DES Implementation])
                                       LIB_DES="-lcrypto";
                                       with_des=yes],
                       with_des=no)],
        with_des=no, $LIB_RSAREF)
    fi
  else
    with_des=no
  fi

  if test "$with_des" = no; then
    AC_CHECK_LIB(des, des_cbc_encrypt, [LIB_DES="-ldes";
                                        with_des=yes], with_des=no)
  fi

  if test "$with_des" = no; then
     AC_CHECK_LIB(des425, des_cbc_encrypt, [LIB_DES="-ldes425";
                                       with_des=yes], with_des=no)
  fi

  if test "$with_des" = no; then
     AC_CHECK_LIB(des524, des_cbc_encrypt, [LIB_DES="-ldes524";
                                       with_des=yes], with_des=no)
  fi

  if test "$with_des" = no; then
    dnl if openssl is around, we might be able to use that for des

    dnl if openssl has been compiled with the rsaref2 libraries,
    dnl we need to include the rsaref libraries in the crypto check
    LIB_RSAREF=""
    AC_CHECK_LIB(rsaref, RSAPublicEncrypt,
                 LIB_RSAREF="-lRSAglue -lrsaref"; cmu_have_rsaref=yes,
                 cmu_have_rsaref=no)

    AC_CHECK_LIB(crypto, des_cbc_encrypt, [
	AC_CHECK_HEADER(openssl/des.h, [AC_DEFINE(WITH_SSL_DES,[],[Use OpenSSL DES Implementation])
					LIB_DES="-lcrypto";
					with_des=yes],
			with_des=no)], 
        with_des=no, $LIB_RSAREF)
  fi
fi

if test "$with_des" != no; then
  AC_DEFINE(WITH_DES,[],[Use DES])
fi

AC_SUBST(LIB_DES)
])

AC_DEFUN([SASL_KERBEROS_V4_CHK], [
  AC_REQUIRE([SASL_DES_CHK])

  AC_ARG_ENABLE(krb4, [  --enable-krb4           enable KERBEROS_V4 authentication [[no]] ],
    krb4=$enableval,
    krb4=no)

  if test "$krb4" != no; then
    dnl In order to compile kerberos4, we need libkrb and libdes.
    dnl (We've already gotten libdes from SASL_DES_CHK)
    dnl we might need -lresolv for kerberos
    AC_CHECK_LIB(resolv,res_search)

    dnl if we were ambitious, we would look more aggressively for the
    dnl krb4 install
    if test -d ${krb4}; then
       AC_CACHE_CHECK(for Kerberos includes, cyrus_cv_krbinclude, [
         for krbhloc in include/kerberosIV include/kerberos include
         do
           if test -f ${krb4}/${krbhloc}/krb.h ; then
             cyrus_cv_krbinclude=${krb4}/${krbhloc}
             break
           fi
         done
         ])

       if test -n "${cyrus_cv_krbinclude}"; then
         CPPFLAGS="$CPPFLAGS -I${cyrus_cv_krbinclude}"
       fi
       LDFLAGS="$LDFLAGS -L$krb4/lib"
    fi

    if test "$with_des" != no; then
      AC_CHECK_HEADER(krb.h, [
        AC_CHECK_LIB(com_err, com_err, [
	  AC_CHECK_LIB(krb, krb_mk_priv,
                     [COM_ERR="-lcom_err"; SASL_KRB_LIB="-lkrb"; krb4lib="yes"],
                     krb4lib=no, $LIB_DES -lcom_err)], [
    	  AC_CHECK_LIB(krb, krb_mk_priv,
                     [COM_ERR=""; SASL_KRB_LIB="-lkrb"; krb4lib="yes"],
                     krb4lib=no, $LIB_DES)])], krb4="no")

      if test "$krb4" != "no" -a "$krb4lib" = "no"; then
	AC_CHECK_LIB(krb4, krb_mk_priv,
                     [COM_ERR=""; SASL_KRB_LIB="-lkrb4"; krb4=yes],
                     krb4=no, $LIB_DES)
      fi
      if test "$krb4" = no; then
          AC_WARN(No Kerberos V4 found)
      fi
    else
      AC_WARN(No DES library found for Kerberos V4 support)
      krb4=no
    fi
  fi

  if test "$krb4" != no; then
    cmu_save_LIBS="$LIBS"
    LIBS="$LIBS $SASL_KRB_LIB"
    AC_CHECK_FUNCS(krb_get_err_text)
    LIBS="$cmu_save_LIBS"
  fi

  AC_MSG_CHECKING(KERBEROS_V4)
  if test "$krb4" != no; then
    AC_MSG_RESULT(enabled)
    SASL_MECHS="$SASL_MECHS libkerberos4.la"
    SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/kerberos4.c"
    SASL_STATIC_OBJS="$SASL_STATIC_OBJS kerberos4.o"
    AC_DEFINE(STATIC_KERBEROS4,[],[User KERBEROS_V4 Staticly])
    AC_DEFINE(HAVE_KRB,[],[Do we have Kerberos 4 Support?])
    SASL_KRB_LIB="$SASL_KRB_LIB $LIB_DES $COM_ERR"
  else
    AC_MSG_RESULT(disabled)
  fi
  AC_SUBST(SASL_KRB_LIB)
])

