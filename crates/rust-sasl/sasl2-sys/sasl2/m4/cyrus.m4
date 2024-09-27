dnl
dnl Additional macros for configure.in packaged up for easier theft.
dnl tjs@andrew.cmu.edu 6-may-1998
dnl

dnl It would be good if ANDREW_ADD_LIBPATH could detect if something was
dnl already there and not redundantly add it if it is.

dnl add -L(arg), and possibly (runpath switch)(arg), to LDFLAGS
dnl (so the runpath for shared libraries is set).
AC_DEFUN([CMU_ADD_LIBPATH], [
  # this is CMU ADD LIBPATH
  if test "$andrew_cv_runpath_switch" = "none" ; then
	LDFLAGS="-L$1 ${LDFLAGS}"
  else
	LDFLAGS="-L$1 $andrew_cv_runpath_switch$1 ${LDFLAGS}"
  fi
])

dnl add -L(1st arg), and possibly (runpath switch)(1st arg), to (2nd arg)
dnl (so the runpath for shared libraries is set).
AC_DEFUN([CMU_ADD_LIBPATH_TO], [
  # this is CMU ADD LIBPATH TO
  if test "$andrew_cv_runpath_switch" = "none" ; then
	$2="-L$1 ${$2}"
  else
	$2="-L$1 ${$2} $andrew_cv_runpath_switch$1"
  fi
])

dnl runpath initialization
AC_DEFUN([CMU_GUESS_RUNPATH_SWITCH], [
   # CMU GUESS RUNPATH SWITCH
  AC_CACHE_CHECK(for runpath switch, andrew_cv_runpath_switch, [
    # first, try -R
    SAVE_LDFLAGS="${LDFLAGS}"
    LDFLAGS="-R /usr/lib"
    AC_TRY_LINK([],[],[andrew_cv_runpath_switch="-R"], [
  	LDFLAGS="-Wl,-rpath,/usr/lib"
    AC_TRY_LINK([],[],[andrew_cv_runpath_switch="-Wl,-rpath,"],
    [andrew_cv_runpath_switch="none"])
    ])
  LDFLAGS="${SAVE_LDFLAGS}"
  ])])
