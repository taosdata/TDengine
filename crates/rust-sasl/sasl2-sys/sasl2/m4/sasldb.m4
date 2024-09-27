dnl Functions to check what database to use for libsasldb

dnl Berkeley DB specific checks first..

dnl Figure out what database type we're using
AC_DEFUN([SASL_DB_CHECK], [
cmu_save_LIBS="$LIBS"
AC_ARG_WITH(dblib,
  [AC_HELP_STRING([--with-dblib={berkeley|gdbm|lmdb|ndbm|none|auto_detect}],
  [set the DB library to use [[berkeley]]])],
  dblib=$withval,
  dblib=auto_detect)

CYRUS_BERKELEY_DB_OPTS()

SASL_DB_LIB=""

case "$dblib" in
dnl this is unbelievably painful due to confusion over what db-3 should be
dnl named.  arg.
  berkeley)
	CYRUS_BERKELEY_DB_CHK()
	CPPFLAGS="${CPPFLAGS} ${BDB_INCADD}"
	SASL_DB_INC=$BDB_INCADD
	SASL_DB_LIB="${BDB_LIBADD}"
	;;
  gdbm)
	AC_ARG_WITH(gdbm,[  --with-gdbm=PATH        use gdbm from PATH],
                    with_gdbm="${withval}")

        case "$with_gdbm" in
           ""|yes)
               AC_CHECK_HEADER(gdbm.h, [
			AC_CHECK_LIB(gdbm, gdbm_open, SASL_DB_LIB="-lgdbm",
                                           dblib="no")],
			dblib="no")
               ;;
           *)
               if test -d $with_gdbm; then
                 CPPFLAGS="${CPPFLAGS} -I${with_gdbm}/include"
                 LDFLAGS="${LDFLAGS} -L${with_gdbm}/lib"
                 SASL_DB_LIB="-lgdbm" 
               else
                 with_gdbm="no"
               fi
       esac
	;;
  lmdb)
    AC_CHECK_HEADER(lmdb.h, [
		AC_CHECK_LIB(lmdb, mdb_env_create, SASL_DB_LIB="-llmdb"; enable_keep_db_open=yes, dblib="no")],
		dblib="no")
	;;
  ndbm)
	dnl We want to attempt to use -lndbm if we can, just in case
	dnl there's some version of it installed and overriding libc
	AC_CHECK_HEADER(ndbm.h, [
			AC_CHECK_LIB(ndbm, dbm_open, SASL_DB_LIB="-lndbm", [
				AC_CHECK_FUNC(dbm_open,,dblib="no")])],
				dblib="no")
	;;
  auto_detect)
        dnl How about berkeley db?
	CYRUS_BERKELEY_DB_CHK()
	if test "$dblib" = no; then
	  dnl How about OpenLDAP's lmdb?
      AC_CHECK_HEADER(lmdb.h, [
		AC_CHECK_LIB(lmdb, mdb_env_create, SASL_DB_LIB="-llmdb"; enable_keep_db_open=yes, dblib="no")],
		dblib="no")
	fi
	if test "$dblib" = no; then
	  dnl How about ndbm?
	  AC_CHECK_HEADER(ndbm.h, [
		AC_CHECK_LIB(ndbm, dbm_open,
			     dblib="ndbm"; SASL_DB_LIB="-lndbm",
		   	     dblib="weird")],
		   dblib="no")
	  if test "$dblib" = "weird"; then
	    dnl Is ndbm in the standard library?
            AC_CHECK_FUNC(dbm_open, dblib="ndbm", dblib="no")
	  fi

	  if test "$dblib" = no; then
            dnl Can we use gdbm?
   	    AC_CHECK_HEADER(gdbm.h, [
		AC_CHECK_LIB(gdbm, gdbm_open, dblib="gdbm";
					     SASL_DB_LIB="-lgdbm", dblib="no")],
  			     dblib="no")
	  fi
	else
	  dnl we took Berkeley
	  CPPFLAGS="${CPPFLAGS} ${BDB_INCADD}"
	  SASL_DB_INC=$BDB_INCADD
          SASL_DB_LIB="${BDB_LIBADD}"
	fi
	;;
  none)
	;;
  no)
	;;
  *)
	AC_MSG_WARN([Bad DB library implementation specified;])
	AC_ERROR([Use either \"berkeley\", \"gdbm\", \"lmdb\", \"ndbm\" or \"none\"])
	dblib=no
	;;
esac
LIBS="$cmu_save_LIBS"

AC_MSG_CHECKING(DB library to use)
AC_MSG_RESULT($dblib)

SASL_DB_BACKEND="db_${dblib}.lo"
SASL_DB_BACKEND_STATIC="db_${dblib}.o allockey.o"
SASL_DB_BACKEND_STATIC_SRCS="\$(top_srcdir)/sasldb/db_${dblib}.c \$(top_srcdir)/sasldb/allockey.c"
SASL_DB_UTILS="saslpasswd2\$(EXEEXT) sasldblistusers2\$(EXEEXT)"
SASL_DB_MANS="saslpasswd2.8 sasldblistusers2.8"

case "$dblib" in
  gdbm) 
    SASL_MECHS="$SASL_MECHS libsasldb.la"
    AC_DEFINE(SASL_GDBM,[],[Use GDBM for SASLdb])
    ;;
  lmdb)
    SASL_MECHS="$SASL_MECHS libsasldb.la"
    AC_DEFINE(SASL_LMDB,[],[Use LMDB for SASLdb])
    ;;
  ndbm)
    SASL_MECHS="$SASL_MECHS libsasldb.la"
    AC_DEFINE(SASL_NDBM,[],[Use NDBM for SASLdb])
    ;;
  berkeley)
    SASL_MECHS="$SASL_MECHS libsasldb.la"
    AC_DEFINE(SASL_BERKELEYDB,[],[Use BerkeleyDB for SASLdb])
    ;;
  *)
    AC_MSG_WARN([Disabling SASL authentication database support])
    dnl note that we do not add libsasldb.la to SASL_MECHS, since it
    dnl will just fail to load anyway.
    SASL_DB_BACKEND="db_none.lo"
    SASL_DB_BACKEND_STATIC="db_none.o"
    SASL_DB_BACKEND_STATIC_SRCS="\$(top_srcdir)/sasldb/db_none.c"
    SASL_DB_UTILS=""
    SASL_DB_MANS=""
    SASL_DB_LIB=""
    ;;
esac

if test "$enable_static" = yes; then
    if test "$dblib" != "none"; then
      SASL_STATIC_SRCS="$SASL_STATIC_SRCS \$(top_srcdir)/plugins/sasldb.c $SASL_DB_BACKEND_STATIC_SRCS"
      SASL_STATIC_OBJS="$SASL_STATIC_OBJS sasldb.o $SASL_DB_BACKEND_STATIC"
      AC_DEFINE(STATIC_SASLDB,[],[Link SASLdb Staticly])
    else
      SASL_STATIC_OBJS="$SASL_STATIC_OBJS $SASL_DB_BACKEND_STATIC"
      SASL_STATIC_SRCS="$SASL_STATIC_SRCS $SASL_DB_BACKEND_STATIC_SRCS"
    fi
fi

AC_SUBST(SASL_DB_UTILS)
AC_SUBST(SASL_DB_MANS)
AC_SUBST(SASL_DB_BACKEND)
AC_SUBST(SASL_DB_BACKEND_STATIC)
AC_SUBST(SASL_DB_INC)
AC_SUBST(SASL_DB_LIB)
])

dnl Figure out what database path we're using
AC_DEFUN([SASL_DB_PATH_CHECK], [
AC_ARG_WITH(dbpath, [  --with-dbpath=PATH      set the DB path to use [/etc/sasldb2] ],
  dbpath=$withval,
  dbpath=/etc/sasldb2)
AC_MSG_CHECKING(DB path to use)
AC_MSG_RESULT($dbpath)
AC_DEFINE_UNQUOTED(SASL_DB_PATH, "$dbpath", [Path to default SASLdb database])])
