AC_DEFUN([CMU_TEST_LIBPATH], [
changequote(<<, >>)
define(<<CMU_AC_CV_FOUND>>, translit(ac_cv_found_$2_lib, <<- *>>, <<__p>>))
changequote([, ])
if test "$CMU_AC_CV_FOUND" = "yes"; then
  if test \! -r "$1/lib$2.a" -a \! -r "$1/lib$2.so" -a \! -r "$1/lib$2.sl" -a \! -r "$1/lib$2.dylib"; then
    CMU_AC_CV_FOUND=no
  fi
fi
])

AC_DEFUN([CMU_TEST_INCPATH], [
changequote(<<, >>)
define(<<CMU_AC_CV_FOUND>>, translit(ac_cv_found_$2_inc, [ *], [_p]))
changequote([, ])
if test "$CMU_AC_CV_FOUND" = "yes"; then
  if test \! -r "$1/$2.h"; then
    CMU_AC_CV_FOUND=no
  fi
fi
])

dnl CMU_CHECK_HEADER_NOCACHE(HEADER-FILE, [ACTION-IF-FOUND [, ACTION-IF-NOT-FOUND]])
AC_DEFUN([CMU_CHECK_HEADER_NOCACHE],
[dnl Do the transliteration at runtime so arg 1 can be a shell variable.
ac_safe=`echo "$1" | sed 'y%./+-%__p_%'`
AC_MSG_CHECKING([for $1])
AC_TRY_CPP([#include <$1>], eval "ac_cv_header_$ac_safe=yes",
  eval "ac_cv_header_$ac_safe=no")
if eval "test \"`echo '$ac_cv_header_'$ac_safe`\" = yes"; then
  AC_MSG_RESULT(yes)
  ifelse([$2], , :, [$2])
else
  AC_MSG_RESULT(no)
ifelse([$3], , , [$3
])dnl
fi
])

AC_DEFUN([CMU_FIND_LIB_SUBDIR],
[dnl
AC_ARG_WITH([lib-subdir], AC_HELP_STRING([--with-lib-subdir=DIR],[Find libraries in DIR instead of lib]))
AC_CHECK_SIZEOF(long)
AC_CACHE_CHECK([what directory libraries are found in], [ac_cv_cmu_lib_subdir],
[test "X$with_lib_subdir" = "Xyes" && with_lib_subdir=
test "X$with_lib_subdir" = "Xno" && with_lib_subdir=
if test "X$with_lib_subdir" = "X" ; then
  ac_cv_cmu_lib_subdir=lib
  if test $ac_cv_sizeof_long -eq 4 ; then
    test -d /usr/lib32 && ac_cv_cmu_lib_subdir=lib32
  fi
  if test $ac_cv_sizeof_long -eq 8 ; then
    test -d /usr/lib64 && ac_cv_cmu_lib_subdir=lib64
  fi
else
  ac_cv_cmu_lib_subdir=$with_lib_subdir
fi])
AC_SUBST(CMU_LIB_SUBDIR, $ac_cv_cmu_lib_subdir)
])
