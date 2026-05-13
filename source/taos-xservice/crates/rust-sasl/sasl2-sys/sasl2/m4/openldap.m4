dnl
dnl macros for configure.in to detect openldap
dnl

dnl
dnl Check for OpenLDAP version compatility
AC_DEFUN([CMU_OPENLDAP_API],
[AC_CACHE_CHECK([OpenLDAP api], [cmu_cv_openldap_api],[
    AC_EGREP_CPP(__openldap_api,[
#include <ldap.h>

#ifdef LDAP_API_FEATURE_X_OPENLDAP
char *__openldap_api = LDAP_API_FEATURE_X_OPENLDAP;
#endif
],      [cmu_cv_openldap_api=yes], [cmu_cv_openldap_api=no])])
])

dnl
dnl Check for OpenLDAP version compatility
AC_DEFUN([CMU_OPENLDAP_COMPAT],
[AC_CACHE_CHECK([OpenLDAP version], [cmu_cv_openldap_compat],[
    AC_EGREP_CPP(__openldap_compat,[
#include <ldap.h>

/* Require 2.1.27+ and 2.2.6+ */
#if LDAP_VENDOR_VERSION_MAJOR == 2  && LDAP_VENDOR_VERSION_MINOR == 1 && LDAP_VENDOR_VERSION_PATCH > 26
char *__openldap_compat = "2.1.27 or better okay";
#elif LDAP_VENDOR_VERSION_MAJOR == 2  && LDAP_VENDOR_VERSION_MINOR == 2 && LDAP_VENDOR_VERSION_PATCH > 5
char *__openldap_compat = "2.2.6 or better okay";
#elif LDAP_VENDOR_VERSION_MAJOR == 2  && LDAP_VENDOR_VERSION_MINOR > 2
char *__openldap_compat = "2.3 or better okay"
#endif
],      [cmu_cv_openldap_compat=yes], [cmu_cv_openldap_compat=no])])
])

