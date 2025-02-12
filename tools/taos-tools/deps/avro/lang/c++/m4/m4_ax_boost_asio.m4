# ===========================================================================
#       https://www.gnu.org/software/autoconf-archive/ax_boost_asio.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_BOOST_ASIO
#
# DESCRIPTION
#
#   Test for Asio library from the Boost C++ libraries. The macro requires a
#   preceding call to AX_BOOST_BASE. Further documentation is available at
#   <https://www.randspringer.de/boost/index.html>.
#
#   This macro calls:
#
#     AC_SUBST(BOOST_ASIO_LIB)
#
#   And sets:
#
#     HAVE_BOOST_ASIO
#
# LICENSE
#
#   Copyright (c) 2008 Thomas Porschberg <thomas@randspringer.de>
#   Copyright (c) 2008 Pete Greenwell <pete@mu.org>
#
#   Copying and distribution of this file, with or without modification, are
#   permitted in any medium without royalty provided the copyright notice
#   and this notice are preserved. This file is offered as-is, without any
#   warranty.

#serial 7

AC_DEFUN([AX_BOOST_ASIO],
[
    AC_ARG_WITH([boost-asio],
    AS_HELP_STRING([--with-boost-asio@<:@=special-lib@:>@],
                   [use the ASIO library from boost - it is possible to specify a certain library for the linker
                        e.g. --with-boost-asio=boost_system-gcc41-mt-1_34 ]),
        [
        if test "$withval" = "no"; then
            want_boost="no"
        elif test "$withval" = "yes"; then
            want_boost="yes"
            ax_boost_user_asio_lib=""
        else
            want_boost="yes"
            ax_boost_user_asio_lib="$withval"
        fi
        ],
        [want_boost="yes"]
    )

    if test "x$want_boost" = "xyes"; then
        AC_REQUIRE([AC_PROG_CC])
        CPPFLAGS_SAVED="$CPPFLAGS"
        CPPFLAGS="$CPPFLAGS $BOOST_CPPFLAGS"
        export CPPFLAGS

        LDFLAGS_SAVED="$LDFLAGS"
        LDFLAGS="$LDFLAGS $BOOST_LDFLAGS"
        export LDFLAGS

        AC_CACHE_CHECK(whether the Boost::ASIO library is available,
                       ax_cv_boost_asio,
        [AC_LANG_PUSH([C++])
         AC_COMPILE_IFELSE(AC_LANG_PROGRAM([[ @%:@include <boost/asio.hpp>
                                            ]],
                                  [[

                                    boost::asio::io_service io;
                                    boost::system::error_code timer_result;
                                    boost::asio::deadline_timer t(io);
                                    t.cancel();
                                    io.run_one();
                                    return 0;
                                   ]]),
                             ax_cv_boost_asio=yes, ax_cv_boost_asio=no)
         AC_LANG_POP([C++])
        ])
        if test "x$ax_cv_boost_asio" = "xyes"; then
            AC_DEFINE(HAVE_BOOST_ASIO,,[define if the Boost::ASIO library is available])
            BN=boost_system
            if test "x$ax_boost_user_asio_lib" = "x"; then
                for ax_lib in $BN $BN-$CC $BN-$CC-mt $BN-$CC-mt-s $BN-$CC-s \
                              lib$BN lib$BN-$CC lib$BN-$CC-mt lib$BN-$CC-mt-s lib$BN-$CC-s \
                              $BN-mgw $BN-mgw $BN-mgw-mt $BN-mgw-mt-s $BN-mgw-s ; do
                    AC_CHECK_LIB($ax_lib, main, [BOOST_ASIO_LIB="-l$ax_lib" AC_SUBST(BOOST_ASIO_LIB) link_thread="yes" break],
                                 [link_thread="no"])
                done
            else
               for ax_lib in $ax_boost_user_asio_lib $BN-$ax_boost_user_asio_lib; do
                      AC_CHECK_LIB($ax_lib, main,
                                   [BOOST_ASIO_LIB="-l$ax_lib" AC_SUBST(BOOST_ASIO_LIB) link_asio="yes" break],
                                   [link_asio="no"])
                  done

            fi
            if test "x$link_asio" = "xno"; then
                AC_MSG_ERROR(Could not link against $ax_lib !)
            fi
        fi

        CPPFLAGS="$CPPFLAGS_SAVED"
        LDFLAGS="$LDFLAGS_SAVED"
    fi
])
