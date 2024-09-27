/* saslpasswd.c -- SASL password setting program
 * Rob Earhart
 */
/* 
 * Copyright (c) 1998-2016 Carnegie Mellon University.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The name "Carnegie Mellon University" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For permission or any other legal
 *    details, please contact  
 *      Carnegie Mellon University
 *      Center for Technology Transfer and Enterprise Creation
 *      4615 Forbes Avenue
 *      Suite 302
 *      Pittsburgh, PA  15213
 *      (412) 268-7393, fax: (412) 268-7395
 *      innovation@andrew.cmu.edu
 *
 * 4. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by Computing Services
 *     at Carnegie Mellon University (http://www.cmu.edu/computing/)."
 *
 * CARNEGIE MELLON UNIVERSITY DISCLAIMS ALL WARRANTIES WITH REGARD TO
 * THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS, IN NO EVENT SHALL CARNEGIE MELLON UNIVERSITY BE LIABLE
 * FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <config.h>
#include <stdio.h>
#include <assert.h>

#ifndef WIN32
#include <termios.h>
#include <unistd.h>

/* perror can't be used on Windows system calls, so we define a new macro to underline this */
#define	p_oserror(str)	    perror(str)

#else /* WIN32 */

#include <stdio.h>
#include <io.h>

#include <saslutil.h>
#if defined(__MINGW64_VERSION_MAJOR)
#include <getopt.h>
#else
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
#endif
/* perror can't be used on Windows system calls, so we define a new macro to underline this */
void p_oserror (const char *string);
#endif /*WIN32*/

#include <sasl.h>
#include <saslplug.h>

char myhostname[1025];

#define PW_BUF_SIZE 2048

const char *progname = NULL;
char *sasldb_path = NULL;

#ifdef WIN32

/* This is almost like _plug_get_error_message(), but uses malloc */
char * _get_error_message (
   DWORD error
)
{
    char * return_value;
    LPVOID lpMsgBuf;

    FormatMessage( 
	FORMAT_MESSAGE_ALLOCATE_BUFFER | 
	FORMAT_MESSAGE_FROM_SYSTEM | 
	FORMAT_MESSAGE_IGNORE_INSERTS,
	NULL,
	error,
	MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), /* Default language */
	(LPTSTR) &lpMsgBuf,
	0,
	NULL 
    );

    return_value = strdup (lpMsgBuf);

    LocalFree( lpMsgBuf );
    return (return_value);
}

/* perror() like function that works on OS error codes returned by GetLastError() */
void p_oserror (
    const char *message
)
{
/* Try to match perror() behaviour:
    string is printed first, followed by a colon, then by the system error message
    for the last library call that produced the error, and finally by a newline
    character. If string is a null pointer or a pointer to a null string, perror
    prints only the system error message.
 */
    if (message && *message) {
	fprintf (stderr, "%s: %s\n", message, _get_error_message(GetLastError()));
    } else {
	fprintf (stderr, "%s\n", _get_error_message(GetLastError()));
    }
}
#endif /* WIN32 */

void read_password(const char *prompt,
		   int flag_pipe,
		   char ** password,
		   unsigned *passlen)
{
  char buf[PW_BUF_SIZE];
#ifndef WIN32
  struct termios ts, nts;
  ssize_t n_read;
#else
  HANDLE hStdin;
  DWORD n_read, fdwMode, fdwOldMode;
  hStdin = GetStdHandle(STD_INPUT_HANDLE);
  if (hStdin == INVALID_HANDLE_VALUE) {
	  p_oserror(progname);
	  exit(-(SASL_FAIL));
  }
#endif /*WIN32*/

  if (! flag_pipe) {
    fputs(prompt, stdout);
    fflush(stdout);
#ifndef WIN32
    tcgetattr(STDIN_FILENO, &ts);
    nts = ts;
    nts.c_lflag &= ~(ECHO | ECHOE | ECHOK
#ifdef ECHOCTL
    | ECHOCTL
#endif
#ifdef ECHOPRT
    | ECHOPRT
#endif
#ifdef ECHOKE
    | ECHOKE
#endif
    );
    nts.c_lflag |= ICANON | ECHONL;
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &nts);
#else
  if (! GetConsoleMode(hStdin, &fdwOldMode)) {
	  p_oserror(progname);
	  exit(-(SASL_FAIL));
  }
  fdwMode = fdwOldMode & ~ENABLE_ECHO_INPUT;
  if (! SetConsoleMode(hStdin, fdwMode)) {
	  p_oserror(progname);
	  exit(-(SASL_FAIL));
  }
#endif /*WIN32*/
  }

#ifndef WIN32
  n_read = read(STDIN_FILENO, buf, PW_BUF_SIZE);
  if (n_read < 0) {
#else
  if (! ReadFile(hStdin, buf, PW_BUF_SIZE, &n_read, NULL)) {
#endif /*WIN32*/

    p_oserror(progname);
    exit(-(SASL_FAIL));
  }

  if (! flag_pipe) {
#ifndef WIN32
    tcsetattr(STDIN_FILENO, TCSANOW, &ts);
    if (0 < n_read && buf[n_read - 1] != '\n') {
      /* if we didn't end with a \n, echo one */
      putchar('\n');
      fflush(stdout);
    }
#else
    SetConsoleMode(hStdin, fdwOldMode);
    putchar('\n');
    fflush(stdout);
#endif /*WIN32*/
  }

  if (0 < n_read && buf[n_read - 1] == '\n') /* if we ended with a \n */
    n_read--;			             /* remove it */

#ifdef WIN32
  /*WIN32 will have a CR in the buffer also*/
  if (0 < n_read && buf[n_read - 1] == '\r') /* if we ended with a \r */
    n_read--;			             /* remove it */
#endif /*WIN32*/

  *password = malloc(n_read + 1);
  if (! *password) {
/* Can use perror() here even on Windows, as malloc is in std C library */
    perror(progname);
    exit(-(SASL_FAIL));
  }

  memcpy(*password, buf, n_read);
  (*password)[n_read] = '\0';	/* be nice... */
  *passlen = n_read;
}

void exit_sasl(int result, const char *errstr) __attribute__((noreturn));

void
exit_sasl(int result, const char *errstr)
{
  (void)fprintf(stderr, errstr ? "%s: %s: %s\n" : "%s: %s\n",
		progname,
		sasl_errstring(result, NULL, NULL),
		errstr);
  exit(result < 0 ? -result : result);
}

int good_getopt(void *context __attribute__((unused)), 
		const char *plugin_name __attribute__((unused)), 
		const char *option,
		const char **result,
		unsigned *len)
{
    if (sasldb_path && !strcmp(option, "sasldb_path")) {
	*result = sasldb_path;
	if (len)
	    *len = (unsigned) strlen(sasldb_path);
	return SASL_OK;
    }

    return SASL_FAIL;
}

static struct sasl_callback goodsasl_cb[] = {
    { SASL_CB_GETOPT, (sasl_callback_ft)&good_getopt, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

int
main(int argc, char *argv[])
{
  int flag_pipe = 0, flag_create = 0, flag_disable = 0, flag_error = 0;
  int flag_nouserpass = 0;
  int c;
  char *userid;
  char *password = NULL;
  char *verify = NULL;
  unsigned passlen = 0;
  unsigned verifylen;
  const char *errstr = NULL;
  int result;
  sasl_conn_t *conn;
  char *user_domain = NULL;
  char *appname = "saslpasswd";
  const char *sasl_implementation;
  int libsasl_version;
  int libsasl_major;
  int libsasl_minor;
  int libsasl_step;

#ifdef WIN32
  /* initialize winsock */
  WSADATA wsaData;

  result = WSAStartup( MAKEWORD(2, 0), &wsaData );
  if ( result != 0) {
    exit_sasl(SASL_FAIL, "WSAStartup");
  }
#endif

  memset(myhostname, 0, sizeof(myhostname));
  result = gethostname(myhostname, sizeof(myhostname)-1);
  if (result == -1) exit_sasl(SASL_FAIL, "gethostname");

  if (! argv[0])
    progname = "saslpasswd";
  else {
    progname = strrchr(argv[0], HIER_DELIMITER);
    if (progname)
      progname++;
    else
      progname = argv[0];
  }

  while ((c = getopt(argc, argv, "vpcdnf:u:a:h?")) != EOF)
    switch (c) {
    case 'p':
      flag_pipe = 1;
      break;
    case 'c':
      if (flag_disable)
	flag_error = 1;
      else
	flag_create = 1;
      break;
    case 'd':
      if (flag_create)
	flag_error = 1;
      else
	flag_disable = 1;
      break;
    case 'n':
	flag_nouserpass = 1;
	break;
    case 'u':
      user_domain = optarg;
      break;
    case 'f':
      sasldb_path = optarg;
      break;
    case 'a':
      appname = optarg;
      if (strchr(optarg, '/') != NULL) {
        (void)fprintf(stderr, "appname must not contain /\n");
        exit(-(SASL_FAIL));
      }
      break;
    case 'v':
      sasl_version (&sasl_implementation, &libsasl_version);
      libsasl_major = libsasl_version >> 24;
      libsasl_minor = (libsasl_version >> 16) & 0xFF;
      libsasl_step = libsasl_version & 0xFFFF;

      (void)fprintf(stderr, "\nThis product includes software developed by Computing Services\n"
	 "at Carnegie Mellon University (http://www.cmu.edu/computing/).\n\n"
	 "Built against SASL API version %u.%u.%u\n"
	 "LibSasl version %u.%u.%u by \"%s\"\n",
	 SASL_VERSION_MAJOR, SASL_VERSION_MINOR, SASL_VERSION_STEP,
	 libsasl_major, libsasl_minor, libsasl_step, sasl_implementation);
      exit(0);
      break;
    default:
      flag_error = 1;
      break;
    }

  if (optind != argc - 1)
    flag_error = 1;

  if (flag_error) {
    (void)fprintf(stderr,
	"\nThis product includes software developed by Computing Services\n"
	 "at Carnegie Mellon University (http://www.cmu.edu/computing/).\n\n"
	"%s: usage: %s [-v] [-c [-p] [-n]] [-d] [-a appname] [-f sasldb] [-u DOM] userid\n"
		  "\t-p\tpipe mode -- no prompt, password read on stdin\n"
		  "\t-c\tcreate -- ask mechs to create the account\n"
		  "\t-d\tdisable -- ask mechs to disable/delete the account\n"
		  "\t-n\tno userPassword -- don't set plaintext userPassword property\n"
		  "\t  \t                   (only set mechanism-specific secrets)\n"
		  "\t-f sasldb\tuse given file as sasldb\n"
		  "\t-a appname\tuse appname as application name\n"
		  "\t-u DOM\tuse DOM for user domain\n"
		  "\t-v\tprint version numbers and exit\n",
		  progname, progname);
    exit(-(SASL_FAIL));
  }

  userid = argv[optind];

  result = sasl_server_init(goodsasl_cb, appname);
  if (result != SASL_OK)
    exit_sasl(result, NULL);

  result = sasl_server_new("sasldb",
			   myhostname,
			   user_domain,
			   NULL,
			   NULL,
			   NULL,
			   0,
			   &conn);
  if (result != SASL_OK)
    exit_sasl(result, NULL);
 
#ifndef WIN32
  if (! flag_pipe && ! isatty(STDIN_FILENO))
    flag_pipe = 1;
#endif /*WIN32*/

  if (!flag_disable) {
      read_password("Password: ", flag_pipe, &password, &passlen);

      if (! flag_pipe) {
	  read_password("Again (for verification): ", flag_pipe, &verify,
		  &verifylen);
	  if (passlen != verifylen
	      || memcmp(password, verify, verifylen)) {
	      free(verify);
	      free(password);
	      fprintf(stderr, "%s: passwords don't match; aborting\n", 
		      progname);
	      exit(-(SASL_BADPARAM));
	  }
	  free(verify);
      }
  }

  result = sasl_setpass(conn,
			userid,
			password,
			passlen,
			NULL, 0,
			(flag_create ? SASL_SET_CREATE : 0)
			| (flag_disable ? SASL_SET_DISABLE : 0)
			| (flag_nouserpass ? SASL_SET_NOPLAIN : 0));
  free(password);

  if (result != SASL_OK && !flag_disable)
      exit_sasl(result, NULL);
  else {
      struct propctx *propctx = NULL;
      const char *delete_request[] = { "cmusaslsecretCRAM-MD5",
				       "cmusaslsecretDIGEST-MD5",
				       "cmusaslsecretPLAIN",
				       NULL };
      int ret = SASL_OK;
      /* Either we were setting and succeeded or we were disabling and
	 failed.  In either case, we want to wipe old entries */

      /* Delete the possibly old entries */
      /* We don't care if these fail */
      propctx = prop_new(0);
      if (!propctx) ret = SASL_FAIL;
      if (!ret) ret = prop_request(propctx, delete_request);
      if (!ret) {
	  ret = prop_set(propctx, "cmusaslsecretCRAM-MD5", NULL, 0);
	  ret = prop_set(propctx, "cmusaslsecretDIGEST-MD5", NULL, 0);
	  ret = prop_set(propctx, "cmusaslsecretPLAIN", NULL, 0);
	  ret = sasl_auxprop_store(conn, propctx, userid);
      }
      if (propctx) prop_dispose(&propctx);
  }
      
  if (result != SASL_OK)
/* errstr is currently always NULL */
    exit_sasl(result, errstr);

  sasl_dispose(&conn);
  sasl_done();

  return 0;
}
