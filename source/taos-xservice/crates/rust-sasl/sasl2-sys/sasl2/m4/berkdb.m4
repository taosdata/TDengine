dnl $Id: berkdb.m4,v 1.24 2010/01/06 17:01:27 murch Exp $

AC_DEFUN([CMU_DB_INC_WHERE1], [
saved_CPPFLAGS=$CPPFLAGS
CPPFLAGS="$saved_CPPFLAGS -I$1"
AC_TRY_COMPILE([#include <db.h>],
[DB *db;
db_create(&db, NULL, 0);
db->open(db, "foo.db", NULL, DB_UNKNOWN, DB_RDONLY, 0644);],
ac_cv_found_db_inc=yes,
ac_cv_found_db_inc=no)
CPPFLAGS=$saved_CPPFLAGS
])

AC_DEFUN([CMU_DB_INC_WHERE], [
   for i in $1; do
      AC_MSG_CHECKING(for db headers in $i)
      CMU_DB_INC_WHERE1($i)
      CMU_TEST_INCPATH($i, db)
      if test "$ac_cv_found_db_inc" = "yes"; then
        ac_cv_db_where_inc=$i
        AC_MSG_RESULT(found)
        break
      else
        AC_MSG_RESULT(not found)
      fi
    done
])

#
# Test for lib files
#

AC_DEFUN([CMU_DB3_LIB_WHERE1], [
AC_REQUIRE([CMU_AFS])
AC_REQUIRE([CMU_KRB4])
saved_LIBS=$LIBS
  LIBS="$saved_LIBS -L$1 -ldb-3"
AC_TRY_LINK([#include <db.h>],
[db_env_create(NULL, 0);],
[ac_cv_found_db_3_lib=yes],
ac_cv_found_db_3_lib=no)
LIBS=$saved_LIBS
])
AC_DEFUN([CMU_DB4_LIB_WHERE1], [
AC_REQUIRE([CMU_AFS])
AC_REQUIRE([CMU_KRB4])
saved_LIBS=$LIBS
LIBS="$saved_LIBS -L$1 -ldb-4"
AC_TRY_LINK([#include <db.h>],
[db_env_create(NULL, 0);],
[ac_cv_found_db_4_lib=yes],
ac_cv_found_db_4_lib=no)
LIBS=$saved_LIBS
])

AC_DEFUN([CMU_DB_LIB_WHERE], [
   for i in $1; do
      AC_MSG_CHECKING(for db libraries in $i)
if test "$enable_db4" = "yes"; then
      CMU_DB4_LIB_WHERE1($i)
      CMU_TEST_LIBPATH($i, [db-4])
      ac_cv_found_db_lib=$ac_cv_found_db_4_lib
else
      CMU_DB3_LIB_WHERE1($i)
      CMU_TEST_LIBPATH($i, [db-3])
      ac_cv_found_db_lib=$ac_cv_found_db_3_lib
fi
      if test "$ac_cv_found_db_lib" = "yes" ; then
        ac_cv_db_where_lib=$i
        AC_MSG_RESULT(found)
        break
      else
        AC_MSG_RESULT(not found)
      fi
    done
])

AC_DEFUN([CMU_USE_DB], [
AC_REQUIRE([CMU_FIND_LIB_SUBDIR])
AC_ARG_WITH(db,
	[  --with-db=PREFIX      Compile with db support],
	[if test "X$with_db" = "X"; then
		with_db=yes
	fi])
AC_ARG_WITH(db-lib,
	[  --with-db-lib=dir     use db libraries in dir],
	[if test "$withval" = "yes" -o "$withval" = "no"; then
		AC_MSG_ERROR([No argument for --with-db-lib])
	fi])
AC_ARG_WITH(db-include,
	[  --with-db-include=dir use db headers in dir],
	[if test "$withval" = "yes" -o "$withval" = "no"; then
		AC_MSG_ERROR([No argument for --with-db-include])
	fi])
AC_ARG_ENABLE(db4,
	[  --enable-db4          use db 4.x libraries])
	
	if test "X$with_db" != "X"; then
	  if test "$with_db" != "yes"; then
	    ac_cv_db_where_lib=$with_db/$CMU_LIB_SUBDIR
	    ac_cv_db_where_inc=$with_db/include
	  fi
	fi

	if test "X$with_db_lib" != "X"; then
	  ac_cv_db_where_lib=$with_db_lib
	fi
	if test "X$ac_cv_db_where_lib" = "X"; then
	  CMU_DB_LIB_WHERE(/usr/athena/$CMU_LIB_SUBDIR /usr/$CMU_LIB_SUBDIR /usr/local/$CMU_LIB_SUBDIR)
	fi

	if test "X$with_db_include" != "X"; then
	  ac_cv_db_where_inc=$with_db_include
	fi
	if test "X$ac_cv_db_where_inc" = "X"; then
	  CMU_DB_INC_WHERE(/usr/athena/include /usr/local/include)
	fi

	AC_MSG_CHECKING(whether to include db)
	if test "X$ac_cv_db_where_lib" = "X" -o "X$ac_cv_db_where_inc" = "X"; then
	  ac_cv_found_db=no
	  AC_MSG_RESULT(no)
	else
	  ac_cv_found_db=yes
	  AC_MSG_RESULT(yes)
	  DB_INC_DIR=$ac_cv_db_where_inc
	  DB_LIB_DIR=$ac_cv_db_where_lib
	  DB_INC_FLAGS="-I${DB_INC_DIR}"
          if test "$enable_db4" = "yes"; then
	     DB_LIB_FLAGS="-L${DB_LIB_DIR} -ldb-4"
          else
	     DB_LIB_FLAGS="-L${DB_LIB_DIR} -ldb-3"
          fi
          dnl Do not force configure.in to put these in CFLAGS and LIBS unconditionally
          dnl Allow makefile substitutions....
          AC_SUBST(DB_INC_FLAGS)
          AC_SUBST(DB_LIB_FLAGS)
	  if test "X$RPATH" = "X"; then
		RPATH=""
	  fi
	  case "${host}" in
	    *-*-linux*)
	      if test "X$RPATH" = "X"; then
	        RPATH="-Wl,-rpath,${DB_LIB_DIR}"
	      else 
		RPATH="${RPATH}:${DB_LIB_DIR}"
	      fi
	      ;;
	    *-*-hpux*)
	      if test "X$RPATH" = "X"; then
	        RPATH="-Wl,+b${DB_LIB_DIR}"
	      else 
		RPATH="${RPATH}:${DB_LIB_DIR}"
	      fi
	      ;;
	    *-*-irix*)
	      if test "X$RPATH" = "X"; then
	        RPATH="-Wl,-rpath,${DB_LIB_DIR}"
	      else 
		RPATH="${RPATH}:${DB_LIB_DIR}"
	      fi
	      ;;
	    *-*-solaris2*)
	      if test "$ac_cv_prog_gcc" = yes; then
		if test "X$RPATH" = "X"; then
		  RPATH="-Wl,-R${DB_LIB_DIR}"
		else 
		  RPATH="${RPATH}:${DB_LIB_DIR}"
		fi
	      else
	        RPATH="${RPATH} -R${DB_LIB_DIR}"
	      fi
	      ;;
	  esac
	  AC_SUBST(RPATH)
	fi
	])



dnl ---- CUT HERE ---

dnl These are the Cyrus Berkeley DB macros.  In an ideal world these would be
dnl identical to the above.

dnl They are here so that they can be shared between Cyrus IMAPd
dnl and Cyrus SASL with relative ease.

dnl The big difference between this and the ones above is that we don't assume
dnl that we know the name of the library, and we try a lot of permutations
dnl instead.  We also assume that DB4 is acceptable.

dnl When we're done, there will be a BDB_LIBADD and a BDB_INCADD which should
dnl be used when necessary.  We should probably be smarter about our RPATH
dnl handling.

dnl Call these with BERKELEY_DB_CHK.

dnl We will also set $dblib to "berkeley" if we are successful, "no" otherwise.

dnl this is unbelievably painful due to confusion over what db-3 should be
dnl named and where the db-3 header file is located.  arg.
AC_DEFUN([CYRUS_BERKELEY_DB_CHK_LIB],
[
	BDB_SAVE_LDFLAGS=$LDFLAGS

	if test -d $with_bdb_lib; then
	    CMU_ADD_LIBPATH_TO($with_bdb_lib, LDFLAGS)
	    CMU_ADD_LIBPATH_TO($with_bdb_lib, BDB_LIBADD)
	else
	    BDB_LIBADD=""
	fi

	saved_LIBS=$LIBS
	    for dbname in ${with_bdb} \
	        db-5.2 db5.2 db52 \
	        db-5.1 db5.2 db51 \
	        db-5.0 db5.2 db50 \
	        db-4.8 db4.8 db48 \
	        db-4.7 db4.7 db47 \
	        db-4.6 db4.6 db46 \
	        db-4.5 db4.5 db45 \
	        db-4.4 db4.4 db44 \
	        db-4.3 db4.3 db43 \
	        db-4.2 db4.2 db42 \
	        db-4.1 db4.1 db41 \
	        db-4.0 db4.0 db40 db-4 db4 \
	        db-3.3 db3.3 db33 \
	        db-3.2 db3.2 db32 \
	        db-3.1 db3.1 db31 \
	        db-3.0 db3.0 db30 db-3 db3 \
	        db
	      do
	    LIBS="$saved_LIBS -l$dbname"
	    AC_TRY_LINK([#include <stdio.h>
#include <db.h>],
	    [db_create(NULL, NULL, 0);],
	    BDB_LIBADD="$BDB_LIBADD -l$dbname"; dblib="berkeley"; dbname=db,
            dblib="no")
	    if test "$dblib" = "berkeley"; then break; fi
          done
        if test "$dblib" = "no"; then
	    LIBS="$saved_LIBS -ldb"
	    AC_TRY_LINK([#include <stdio.h>
#include <db.h>],
	    [db_open(NULL, 0, 0, 0, NULL, NULL, NULL);],
	    BDB_LIBADD="$BDB_LIBADD -ldb"; dblib="berkeley"; dbname=db,
            dblib="no")
        fi
	LIBS=$saved_LIBS

	LDFLAGS=$BDB_SAVE_LDFLAGS
])

AC_DEFUN([CYRUS_BERKELEY_DB_OPTS],
[
AC_ARG_WITH(bdb-libdir,
	[  --with-bdb-libdir=DIR   Berkeley DB lib files are in DIR],
	with_bdb_lib=$withval,
	[ test "${with_bdb_lib+set}" = set || with_bdb_lib=none])
AC_ARG_WITH(bdb-incdir,
	[  --with-bdb-incdir=DIR   Berkeley DB include files are in DIR],
	with_bdb_inc=$withval,
	[ test "${with_bdb_inc+set}" = set || with_bdb_inc=none ])
])

AC_DEFUN([CYRUS_BERKELEY_DB_CHK],
[
	AC_REQUIRE([CYRUS_BERKELEY_DB_OPTS])

	cmu_save_CPPFLAGS=$CPPFLAGS

	if test -d $with_bdb_inc; then
	    CPPFLAGS="$CPPFLAGS -I$with_bdb_inc"
	    BDB_INCADD="-I$with_bdb_inc"
	else
	    BDB_INCADD=""
	fi

	dnl Note that FreeBSD puts it in a wierd place
        dnl (but they should use with-bdb-incdir)
        AC_CHECK_HEADER(db.h,
                        [CYRUS_BERKELEY_DB_CHK_LIB()],
                        dblib="no")

	CPPFLAGS=$cmu_save_CPPFLAGS
])
