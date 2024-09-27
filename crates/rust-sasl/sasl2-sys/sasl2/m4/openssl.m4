dnl
dnl macros for configure.in to detect openssl
dnl

AC_DEFUN([CMU_HAVE_OPENSSL], [
AC_REQUIRE([CMU_FIND_LIB_SUBDIR])
AC_ARG_WITH(openssl,
	[AS_HELP_STRING([--with-openssl=DIR], [use OpenSSL from DIR])],
	with_openssl=$withval, with_openssl="yes")

	save_CPPFLAGS=$CPPFLAGS
	save_LDFLAGS=$LDFLAGS

	if test -d $with_openssl; then
	  CPPFLAGS="${CPPFLAGS} -I${with_openssl}/include"
	  CMU_ADD_LIBPATH(${with_openssl}/$CMU_LIB_SUBDIR)
	fi

case "$with_openssl" in
	no)
	  with_openssl="no";;
	*) 
	  with_openssl="yes"
	  dnl if openssl has been compiled with the rsaref2 libraries,
	  dnl we need to include the rsaref libraries in the crypto check
                LIB_RSAREF=""
	        AC_CHECK_LIB(rsaref, RSAPublicEncrypt,
			cmu_have_rsaref=yes;
			[AC_CHECK_LIB(RSAglue, RSAPublicEncrypt,
				LIB_RSAREF="-lRSAglue -lrsaref",
				LIB_RSAREF="-lrsaref")],
			cmu_have_rsaref=no)

		AC_CHECK_HEADER(openssl/evp.h, [
			AC_CHECK_LIB(crypto, EVP_DigestInit,
					[AC_CHECK_LIB(crypto, SHA512,
                                                      AC_DEFINE(HAVE_SHA512,[],
                                                      [Do we have SHA512?]))],
					with_openssl="no", $LIB_RSAREF)],
			with_openssl="no")
		;;
esac

	if test "$with_openssl" != "no"; then
		AC_DEFINE(HAVE_OPENSSL,[],[Do we have OpenSSL?])
	else
		CPPFLAGS=$save_CPPFLAGS
		LDFLAGS=$save_LDFLAGS
	fi
])
