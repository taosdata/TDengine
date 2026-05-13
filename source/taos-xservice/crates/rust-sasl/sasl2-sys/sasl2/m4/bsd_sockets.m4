dnl bsd_sockets.m4--which socket libraries do we need? 
dnl Derrick Brashear
dnl from Zephyr

dnl Hacked on by Rob Earhart to not just toss stuff in LIBS
dnl It now puts everything required for sockets into LIB_SOCKET

AC_DEFUN([CMU_SOCKETS], [
	save_LIBS="$LIBS"
	AC_CHECK_HEADERS([sys/socket.h ws2tcpip.h])
	AC_CHECK_FUNC(socket, , [
		AC_CHECK_LIB(socket, socket, [LIB_SOCKET=-lsocket], [
			LIBS="$LIBS -lws2_32"
			AC_LINK_IFELSE([
				AC_LANG_PROGRAM([[
					#ifdef HAVE_SYS_SOCKET_H
					#	include <sys/socket.h>
					#endif
					#ifdef HAVE_WS2TCPIP_H
					#	include <ws2tcpip.h>
					#endif
				]], [[return socket(0, 0, 0);]])
			],
			[LIB_SOCKET=-lws2_32
			 AC_MSG_RESULT(yes)],
			[AC_MSG_ERROR([socket not found])])
		])
	])
	LIBS="$save_LIBS"
	AC_CHECK_FUNC(connect, :,
		[AC_CHECK_LIB(nsl, gethostbyname,
			     LIB_SOCKET="-lnsl $LIB_SOCKET")
		AC_CHECK_LIB(socket, connect,
			     LIB_SOCKET="-lsocket $LIB_SOCKET")]
	)
	LIBS="$LIB_SOCKET $save_LIBS"
	AC_CHECK_FUNC(res_search, :,
		[LIBS="-lresolv $LIB_SOCKET $save_LIBS"
		AC_TRY_LINK([[
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#ifdef HAVE_ARPA_NAMESER_COMPAT_H
#include <arpa/nameser_compat.h>
#endif
#include <resolv.h>]],[[
const char host[12]="openafs.org";
u_char ans[1024];
res_search( host, C_IN, T_MX, (u_char *)&ans, sizeof(ans));
return 0;
]], LIB_SOCKET="-lresolv $LIB_SOCKET")
        ])
	LIBS="$LIB_SOCKET $save_LIBS"
	AC_CHECK_FUNCS(dn_expand dns_lookup)
	LIBS="$save_LIBS"
	AC_SUBST(LIB_SOCKET)
	])
