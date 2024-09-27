/*
 * type functions split out of here to make things look nicer in the
 * various include files which need these definitions, as well as in
 * the util/ directories.
 */

#ifndef _KRB5_WIN_MAC_H
#define _KRB5_WIN_MAC_H

#if (defined(_MSDOS) || defined(_WIN32))
/* 
 * Machine-type definitions: PC Clone 386 running Microloss Windows
 */

#define ID_READ_PWD_DIALOG  10000
#define ID_READ_PWD_PROMPT  10001
#define ID_READ_PWD_PROMPT2 10002
#define ID_READ_PWD_PWD     10003

#ifdef RES_ONLY

#define APSTUDIO_HIDDEN_SYMBOLS
#include <windows.h>

#else

#if defined(_MSDOS)
	/* Windows 16 specific */
#define BITS16
#define SIZEOF_INT      2
#define SIZEOF_SHORT    2
#define SIZEOF_LONG     4

#ifndef KRB5_CALLCONV
#define KRB5_CALLCONV __far __export __pascal
#define KRB5_CALLCONV_C __far __export __cdecl
#define KRB5_EXPORTVAR __far __export
#define KRB5_DLLIMP
#endif /* !KRB5_CALLCONV */

#include <windows.h>
	
/*
 * The following defines are needed to make <windows.h> work
 * in stdc mode (/Za flag). Winsock.h needs <windows.h>.
 */
#ifndef FAR
#define FAR     __far
#define NEAR    __near
#endif

#ifndef _far
#define _far    __far
#define _near   __near
#define _pascal __pascal
#define _cdecl  __cdecl
#define _huge   __huge
#endif

#else
	/* Windows 32 specific */
#define SIZEOF_INT      4
#define SIZEOF_SHORT    2
#define SIZEOF_LONG     4

#include <windows.h>   /* always include this here, to get correct FAR and NEAR */

#define HAVE_LABS

#ifndef KRB5_CALLCONV
#  ifdef _MSC_VER
#    ifdef KRB5_DLL_FILE
#      define KRB5_DLLIMP __declspec(dllexport)
#    else
#      define KRB5_DLLIMP __declspec(dllimport)
#    endif
#    ifdef GSS_DLL_FILE
#      define GSS_DLLIMP __declspec(dllexport)
#    else
#      define GSS_DLLIMP __declspec(dllimport)
#    endif
#  else /* !_MSC_VER */
#    define KRB5_DLLIMP
#    define GSS_DLLIMP
#  endif
#  define KRB5_CALLCONV __stdcall
#  define KRB5_CALLCONV_C __cdecl
#  define KRB5_EXPORTVAR
#endif /* !KRB5_CALLCONV */

#endif /* _MSDOS */

#ifndef KRB5_SYSTYPES__
#define KRB5_SYSTYPES__
#include <sys/types.h>
typedef unsigned long	u_long;      /* Not part of sys/types.h on the pc */
typedef unsigned int	u_int;
typedef unsigned short	u_short;
typedef unsigned char	u_char;
#endif /* KRB5_SYSTYPES__ */

#ifndef MAXPATHLEN
#define MAXPATHLEN      256            /* Also for Windows temp files */
#endif

#define HAVE_NETINET_IN_H
#define MSDOS_FILESYSTEM
#define HAVE_STRING_H 
#define HAVE_SRAND
#define HAVE_ERRNO
#define HAVE_STRDUP
#define NO_USERID
#define NO_PASSWORD

#define WM_KERBEROS5_CHANGED "Kerberos5 Changed"
#ifdef KRB4
#define WM_KERBEROS_CHANGED "Kerberos Changed"
#endif

/* Kerberos Windows initialization file */
#define KERBEROS_INI    "kerberos.ini"
#ifdef CYGNUS
#define KERBEROS_HLP    "kerbnet.hlp"
#else
#define KERBEROS_HLP	"krb5clnt.hlp"
#endif
#define INI_DEFAULTS    "Defaults"
#define   INI_USER        "User"          /* Default user */
#define   INI_INSTANCE    "Instance"      /* Default instance */
#define   INI_REALM       "Realm"         /* Default realm */
#define   INI_POSITION    "Position"
#define   INI_OPTIONS     "Options"
#define   INI_DURATION    "Duration"   /* Ticket duration in minutes */
#define INI_EXPIRATION  "Expiration" /* Action on expiration (alert or beep) */
#define   INI_ALERT       "Alert"
#define   INI_BEEP        "Beep"
#define   INI_FILES       "Files"
#ifdef KRB4
#define   INI_KRB_CONF    "krb.conf"     /* Location of krb.conf file */
#define   DEF_KRB_CONF    "krb.conf"      /* Default name for krb.conf file */
#else
#define INI_KRB5_CONF   "krb5.ini"	/* From k5-config.h */
#define INI_KRB_CONF    INI_KRB5_CONF	/* Location of krb.conf file */
#define DEF_KRB_CONF    INI_KRB5_CONF	/* Default name for krb.conf file */
#define INI_TICKETOPTS  "TicketOptions" /* Ticket options */
#define   INI_FORWARDABLE  "Forwardable" /* get forwardable tickets */
#define INI_KRB_CCACHE  "krb5cc"       	/* From k5-config.h */
#endif
#define INI_KRB_REALMS  "krb.realms"    /* Location of krb.realms file */
#define DEF_KRB_REALMS  "krb.realms"    /* Default name for krb.realms file */
#define INI_RECENT_LOGINS "Recent Logins"    
#define INI_LOGIN       "Login"

#define HAS_ANSI_VOLATILE
#define HAS_VOID_TYPE
#define KRB5_PROVIDE_PROTOTYPES
#define HAVE_STDARG_H
#define HAVE_SYS_TYPES_H
#define HAVE_STDLIB_H

/* This controls which encryption routines libcrypto will provide */
#define PROVIDE_DES_CBC_MD5
#define PROVIDE_DES_CBC_CRC
#define PROVIDE_DES_CBC_RAW
#define PROVIDE_DES_CBC_CKSUM
#define PROVIDE_CRC32
#define PROVIDE_RSA_MD4
#define PROVIDE_RSA_MD5
/* #define PROVIDE_DES3_CBC_SHA */
/* #define PROVIDE_DES3_CBC_RAW */
/* #define PROVIDE_NIST_SHA */

/* Ugly. Microsoft, in stdc mode, doesn't support the low-level i/o
 * routines directly. Rather, they only export the _<function> version.
 * The following defines works around this problem. 
 */
#include <sys\types.h>
#include <sys\stat.h>
#include <fcntl.h>
#include <io.h>
#include <process.h>
#define THREEPARAMOPEN(x,y,z) open(x,y,z)
#ifndef _WIN32
#define O_RDONLY        _O_RDONLY
#define O_WRONLY        _O_WRONLY
#define O_RDWR          _O_RDWR
#define O_APPEND        _O_APPEND
#define O_CREAT         _O_CREAT
#define O_TRUNC         _O_TRUNC
#define O_EXCL          _O_EXCL
#define O_TEXT          _O_TEXT
#define O_BINARY        _O_BINARY
#define O_NOINHERIT     _O_NOINHERIT
#define stat            _stat
#define unlink          _unlink
#define lseek           _lseek
#define write           _write
#define open            _open
#define close           _close
#define read            _read
#define fstat           _fstat
#define mktemp          _mktemp
#define dup             _dup

#define getpid          _getpid
#endif

#ifdef NEED_SYSERROR
/* Only needed by util/et/error_message.c but let's keep the source clean */
#define sys_nerr        _sys_nerr
#define sys_errlist     _sys_errlist
#endif

/*
 * Functions with slightly different names on the PC
 */
#define strcasecmp   stricmp
#define strncasecmp  strnicmp

HINSTANCE get_lib_instance(void);

#endif /* !RES_ONLY */

#endif /* _MSDOS || _WIN32 */

#ifdef macintosh

#include <KerberosSupport/KerberosConditionalMacros.h>

#define USE_LOGIN_LIBRARY

#define KRB5_CALLCONV
#define KRB5_CALLCONV_C
#define KRB5_DLLIMP
#define GSS_DLLIMP
#ifndef FAR
#define FAR
#endif
#ifndef NEAR
#define NEAR
#endif

#define SIZEOF_INT 4
#define SIZEOF_SHORT 2
#define HAVE_SRAND
#define NO_PASSWORD
#define HAVE_LABS
/*#define ENOMEM 12*/
#include <ctype.h>

/*
 * Which encryption routines libcrypto will provide is controlled by
 * mac/libraries/KerberosHeaders.h.
 */

/* there is no <stat.h> for mpw */
#ifndef __MWERKS__
typedef unsigned long size_t;
typedef unsigned long	mode_t;
typedef unsigned long	ino_t;
typedef unsigned long	dev_t;
typedef short			nlink_t;
typedef unsigned long	uid_t;
typedef unsigned long	gid_t;
typedef long			off_t;

struct stat
{
	mode_t		st_mode;	/* File mode; see #define's below */
	ino_t		st_ino;		/* File serial number */
	dev_t		st_dev;		/* ID of device containing this file */
	nlink_t		st_nlink;	/* Number of links */
	uid_t		st_uid;		/* User ID of the file's owner */
	gid_t		st_gid;		/* Group ID of the file's group */
	dev_t		st_rdev;	/* Device type */
	off_t		st_size;	/* File size in bytes */
	unsigned long	st_atime;	/* Time of last access */
	unsigned long	st_mtime;	/* Time of last data modification */
	unsigned long	st_ctime;	/* Time of last file status change */
	long		st_blksize;	/* Optimal blocksize */
	long		st_blocks;	/* blocks allocated for file */
};

int stat(const char *path, struct stat *buf);
#endif

int fstat(int fildes, struct stat *buf);

#define EFBIG 1000

#define NOFCHMOD 1
#define NOCHMOD 1
#define _MACSOCKAPI_

#define THREEPARAMOPEN(x,y,z) open(x,y)
#else /* macintosh */
#define THREEPARAMOPEN(x,y,z) open(x,y,z)
#endif /* macintosh */

#ifndef KRB5_CALLCONV
#define KRB5_CALLCONV
#define KRB5_CALLCONV_C
#define KRB5_DLLIMP
#endif
#ifndef FAR
#define FAR
#endif
#ifndef NEAR
#define NEAR
#endif

#endif /* _KRB5_WIN_MAC_H */
