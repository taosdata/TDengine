/* sample-client.c -- sample SASL client
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
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef WIN32
# include <winsock2.h>
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
__declspec(dllimport) int getsubopt(char **optionp, const char * const *tokens, char **valuep);
#else  /* WIN32 */
# include <netinet/in.h>
#endif /* WIN32 */
#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#ifdef macintosh
#include <sioux.h>
#include <parse_cmd_line.h>
#define MAX_ARGC (100)
int xxx_main(int argc, char *argv[]);
int main(void)
{
	char *argv[MAX_ARGC];
	int argc;
	char line[400];
	SIOUXSettings.asktosaveonclose = 0;
	SIOUXSettings.showstatusline = 1;
	argc=parse_cmd_line(MAX_ARGC,argv,sizeof(line),line);
	return xxx_main(argc,argv);
}
#define main xxx_main
#endif

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifndef HAVE_GETSUBOPT
int getsubopt(char **optionp, const char * const *tokens, char **valuep);
#endif

static const char *progname = NULL;
static int verbose;

#define SAMPLE_SEC_BUF_SIZE (2048)

#define N_CALLBACKS (16)

static const char
message[] = "Come here Watson, I want you.";

char buf[SAMPLE_SEC_BUF_SIZE];

static const char *bit_subopts[] = {
#define OPT_MIN (0)
  "min",
#define OPT_MAX (1)
  "max",
  NULL
};

static const char *ext_subopts[] = {
#define OPT_EXT_SSF (0)
  "ssf",
#define OPT_EXT_ID (1)
  "id",
  NULL
};

static const char *flag_subopts[] = {
#define OPT_NOPLAIN (0)
  "noplain",
#define OPT_NOACTIVE (1)
  "noactive",
#define OPT_NODICT (2)
  "nodict",
#define OPT_FORWARDSEC (3)
  "forwardsec",
#define OPT_NOANONYMOUS (4)
  "noanonymous",
#define OPT_PASSCRED (5)
  "passcred",
  NULL
};

static const char *ip_subopts[] = {
#define OPT_IP_LOCAL (0)
  "local",
#define OPT_IP_REMOTE (1)
  "remote",
  NULL
};

static sasl_conn_t *conn = NULL;

static void
free_conn(void)
{
  if (conn)
    sasl_dispose(&conn);
}

static int
sasl_my_log(void *context __attribute__((unused)),
	    int priority,
	    const char *message) 
{
  const char *label;

  if (! message)
    return SASL_BADPARAM;

  switch (priority) {
  case SASL_LOG_ERR:
    label = "Error";
    break;
  case SASL_LOG_NOTE:
    label = "Info";
    break;
  default:
    label = "Other";
    break;
  }

  fprintf(stderr, "%s: SASL %s: %s\n",
	  progname, label, message);

  return SASL_OK;
}

static int getrealm(void *context, 
		    int id,
		    const char **availrealms __attribute__((unused)),
		    const char **result)
{
  if (id!=SASL_CB_GETREALM) return SASL_FAIL;

  *result=(char *) context;
  
  return SASL_OK;
}

static int
getpath(void *context,
	const char ** path) 
{
  const char *searchpath = (const char *) context;

  if (! path)
    return SASL_BADPARAM;

  if (searchpath) {
      *path = searchpath;
  } else {
      *path = PLUGINDIR;
  }

  return SASL_OK;
}

static int
simple(void *context,
       int id,
       const char **result,
       unsigned *len)
{
  const char *value = (const char *)context;

  if (! result)
    return SASL_BADPARAM;

  switch (id) {
  case SASL_CB_USER:
    *result = value;
    if (len)
      *len = value ? (unsigned) strlen(value) : 0;
    break;
  case SASL_CB_AUTHNAME:
    *result = value;
    if (len)
      *len = value ? (unsigned) strlen(value) : 0;
    break;
  case SASL_CB_LANGUAGE:
    *result = NULL;
    if (len)
      *len = 0;
    break;
  default:
    return SASL_BADPARAM;
  }

  printf("returning OK: %s\n", *result);

  return SASL_OK;
}

#ifndef HAVE_GETPASSPHRASE
static char *
getpassphrase(const char *prompt)
{
  return getpass(prompt);
}
#endif /* ! HAVE_GETPASSPHRASE */

static int
getsecret(sasl_conn_t *conn,
	  void *context __attribute__((unused)),
	  int id,
	  sasl_secret_t **psecret)
{
  char *password;
  unsigned len;

  if (! conn || ! psecret || id != SASL_CB_PASS)
    return SASL_BADPARAM;

  password = getpassphrase("Password: ");
  if (! password)
    return SASL_FAIL;

  len = (unsigned) strlen(password);

  *psecret = (sasl_secret_t *) malloc(sizeof(sasl_secret_t) + len);
  
  if (! *psecret) {
    memset(password, 0, len);
    return SASL_NOMEM;
  }

  (*psecret)->len = len;
  strcpy((char *)(*psecret)->data, password);
  memset(password, 0, len);
    
  return SASL_OK;
}

static int
prompt(void *context __attribute__((unused)),
       int id,
       const char *challenge,
       const char *prompt,
       const char *defresult,
       const char **result,
       unsigned *len)
{
  if ((id != SASL_CB_ECHOPROMPT && id != SASL_CB_NOECHOPROMPT)
      || !prompt || !result || !len)
    return SASL_BADPARAM;

  if (! defresult)
    defresult = "";
  
  fputs(prompt, stdout);
  if (challenge)
    printf(" [challenge: %s]", challenge);
  printf(" [%s]: ", defresult);
  fflush(stdout);
  
  if (id == SASL_CB_ECHOPROMPT) {
    char *original = getpassphrase("");
    if (! original)
      return SASL_FAIL;
    if (*original)
      *result = strdup(original);
    else
      *result = strdup(defresult);
    memset(original, 0L, strlen(original));
  } else {
    char buf[1024];
    fgets(buf, 1024, stdin);
    if (buf[0]) {
      *result = strdup(buf);
    } else {
      *result = strdup(defresult);
    }
    memset(buf, 0L, sizeof(buf));
  }
  if (! *result)
    return SASL_NOMEM;

  *len = (unsigned) strlen(*result);
  
  return SASL_OK;
}

static void
sasldebug(int why, const char *what, const char *errstr)
{
  fprintf(stderr, "%s: %s: %s",
	  progname,
	  what,
	  sasl_errstring(why, NULL, NULL));
  if (errstr)
    fprintf(stderr, " (%s)\n", errstr);
  else
    putc('\n', stderr);
}

static void
saslfail(int why, const char *what, const char *errstr)
{
  sasldebug(why, what, errstr);
  free_conn();
  sasl_done();
  exit(EXIT_FAILURE);
}

static void
fail(const char *what)
{
  fprintf(stderr, "%s: %s\n",
	  progname, what);
  exit(EXIT_FAILURE);
}

static void
osfail()
{
  perror(progname);
  exit(EXIT_FAILURE);
}

static void
samp_send(const char *buffer,
	  unsigned length)
{
  char *buf;
  unsigned len, alloclen;
  int result;

  alloclen = ((length / 3) + 1) * 4 + 1;
  buf = malloc(alloclen);
  if (! buf)
    osfail();
  result = sasl_encode64(buffer, length, buf, alloclen, &len);
  if (result != SASL_OK)
    saslfail(result, "Encoding data in base64", NULL);
  printf("C: %s\n", buf);
  free(buf);
}

static unsigned
samp_recv()
{
  unsigned len;
  int result;
  
  if (! fgets(buf, SAMPLE_SEC_BUF_SIZE, stdin)) {
    fail("Unable to parse input");
  }

  if (strncmp(buf, "S: ", 3) != 0) {
    fail("Line must start with 'S: '");
  }

  len = strlen(buf);
  if (len > 0 && buf[len-1] == '\n') {
      buf[len-1] = '\0';
  }

  result = sasl_decode64(buf + 3, (unsigned) strlen(buf + 3), buf,
			 SAMPLE_SEC_BUF_SIZE, &len);
  if (result != SASL_OK)
    saslfail(result, "Decoding data from base64", NULL);
  buf[len] = '\0';
  printf("recieved %d byte message\n",len);
  if (verbose) { printf("got '%s'\n", buf); }
  return len;
}

int
main(int argc, char *argv[])
{
  int c = 0;
  int errflag = 0;
  int result;
  sasl_security_properties_t secprops;
  sasl_ssf_t extssf = 0;
  const char *ext_authid = NULL;
  char *options, *value;
  const char *data;
  const char *chosenmech;
  int serverlast = 0;
  unsigned len;
  int clientfirst = 1;
  sasl_callback_t callbacks[N_CALLBACKS], *callback;
  char *realm = NULL;
  char *mech = NULL,
    *iplocal = NULL,
    *ipremote = NULL,
    *searchpath = NULL,
    *service = "rcmd",
    *fqdn = "",
    *userid = NULL,
    *authid = NULL;
  sasl_ssf_t *ssf;
    
#ifdef WIN32
  /* initialize winsock */
    WSADATA wsaData;

    result = WSAStartup( MAKEWORD(2, 0), &wsaData );
    if ( result != 0) {
	saslfail(SASL_FAIL, "Initializing WinSockets", NULL);
    }
#endif

  progname = strrchr(argv[0], HIER_DELIMITER);
  if (progname)
    progname++;
  else
    progname = argv[0];

  /* Init defaults... */
  memset(&secprops, 0L, sizeof(secprops));
  secprops.maxbufsize = SAMPLE_SEC_BUF_SIZE;
  secprops.max_ssf = UINT_MAX;

  verbose = 0;
  while ((c = getopt(argc, argv, "vhldb:e:m:f:i:p:r:s:n:u:a:?")) != EOF)
    switch (c) {
    case 'v':
	verbose = 1;
	break;
    case 'b':
      options = optarg;
      while (*options != '\0')
	switch(getsubopt(&options, (const char * const *)bit_subopts, &value)) {
	case OPT_MIN:
	  if (! value)
	    errflag = 1;
	  else
	    secprops.min_ssf = atoi(value);
	  break;
	case OPT_MAX:
	  if (! value)
	    errflag = 1;
	  else
	    secprops.max_ssf = atoi(value);
	  break;
	default:
	  errflag = 1;
	  break;	  
	  }
      break;

    case 'l':
	serverlast = SASL_SUCCESS_DATA;
	break;
	
    case 'd':
	clientfirst = 0;
	break;

    case 'e':
      options = optarg;
      while (*options != '\0')
	switch(getsubopt(&options, (const char * const *)ext_subopts, &value)) {
	case OPT_EXT_SSF:
	  if (! value)
	    errflag = 1;
	  else
	    extssf = atoi(value);
	  break;
	case OPT_MAX:
	  if (! value)
	    errflag = 1;
	  else
	    ext_authid = value;
	  break;
	default:
	  errflag = 1;
	  break;
	  }
      break;

    case 'm':
      mech = optarg;
      break;

    case 'f':
      options = optarg;
      while (*options != '\0') {
	switch(getsubopt(&options, (const char * const *)flag_subopts, &value)) {
	case OPT_NOPLAIN:
	  secprops.security_flags |= SASL_SEC_NOPLAINTEXT;
	  break;
	case OPT_NOACTIVE:
	  secprops.security_flags |= SASL_SEC_NOACTIVE;
	  break;
	case OPT_NODICT:
	  secprops.security_flags |= SASL_SEC_NODICTIONARY;
	  break;
	case OPT_FORWARDSEC:
	  secprops.security_flags |= SASL_SEC_FORWARD_SECRECY;
	  break;
	case OPT_NOANONYMOUS:
	  secprops.security_flags |= SASL_SEC_NOANONYMOUS;
	  break;
	case OPT_PASSCRED:
	  secprops.security_flags |= SASL_SEC_PASS_CREDENTIALS;
	  break;
	default:
	  errflag = 1;
	  break;
	  }
	if (value) errflag = 1;
	}
      break;

    case 'i':
      options = optarg;
      while (*options != '\0')
	switch(getsubopt(&options, (const char * const *)ip_subopts, &value)) {
	case OPT_IP_LOCAL:
	  if (! value)
	    errflag = 1;
	  else
	    iplocal = value;
	  break;
	case OPT_IP_REMOTE:
	  if (! value)
	    errflag = 1;
	  else
	    ipremote = value;
	  break;
	default:
	  errflag = 1;
	  break;
	  }
      break;

    case 'p':
      searchpath = optarg;
      break;

    case 'r':
      realm = optarg;
      break;

    case 's':
      service=malloc(1000);
      strcpy(service,optarg);
      /*      service = optarg;*/
      printf("service=%s\n",service);
      break;

    case 'n':
      fqdn = optarg;
      break;

    case 'u':
      userid = optarg;
      break;

    case 'a':
      authid = optarg;
      break;

    default:			/* unknown flag */
      errflag = 1;
      break;
    }

  if (optind != argc) {
    /* We don't *have* extra arguments */
    errflag = 1;
  }

  if (errflag) {
    fprintf(stderr, "%s: Usage: %s [-b min=N,max=N] [-e ssf=N,id=ID] [-m MECH] [-f FLAGS] [-i local=IP,remote=IP] [-p PATH] [-s NAME] [-n FQDN] [-u ID] [-a ID]\n"
	    "\t-b ...\t#bits to use for encryption\n"
	    "\t\tmin=N\tminimum #bits to use (1 => integrity)\n"
	    "\t\tmax=N\tmaximum #bits to use\n"
	    "\t-e ...\tassume external encryption\n"
	    "\t\tssf=N\texternal mech provides N bits of encryption\n"
	    "\t\tid=ID\texternal mech provides authentication id ID\n"
	    "\t-m MECH\tforce use of MECH for security\n"
	    "\t-f ...\tset security flags\n"
	    "\t\tnoplain\t\trequire security vs. passive attacks\n"
	    "\t\tnoactive\trequire security vs. active attacks\n"
	    "\t\tnodict\t\trequire security vs. passive dictionary attacks\n"
	    "\t\tforwardsec\trequire forward secrecy\n"
	    "\t\tmaximum\t\trequire all security flags\n"
	    "\t\tpasscred\tattempt to pass client credentials\n"
	    "\t-i ...\tset IP addresses (required by some mechs)\n"
	    "\t\tlocal=IP;PORT\tset local address to IP, port PORT\n"
	    "\t\tremote=IP;PORT\tset remote address to IP, port PORT\n"
	    "\t-p PATH\tcolon-seperated search path for mechanisms\n"
	    "\t-r REALM\trealm to use"
	    "\t-s NAME\tservice name pass to mechanisms\n"
	    "\t-n FQDN\tserver fully-qualified domain name\n"
	    "\t-u ID\tuser (authorization) id to request\n"
	    "\t-a ID\tid to authenticate as\n"
	    "\t-d\tDisable client-send-first\n"
	    "\t-l\tEnable server-send-last\n",
	    progname, progname);
    exit(EXIT_FAILURE);
  }

  /* Fill in the callbacks that we're providing... */
  callback = callbacks;

  /* log */
  callback->id = SASL_CB_LOG;
  callback->proc = (sasl_callback_ft)&sasl_my_log;
  callback->context = NULL;
  ++callback;
  
  /* getpath */
  if (searchpath) {
    callback->id = SASL_CB_GETPATH;
    callback->proc = (sasl_callback_ft)&getpath;
    callback->context = searchpath;
    ++callback;
  }

  /* user */
  if (userid) {
    callback->id = SASL_CB_USER;
    callback->proc = (sasl_callback_ft)&simple;
    callback->context = userid;
    ++callback;
  }

  /* authname */
  if (authid) {
    callback->id = SASL_CB_AUTHNAME;
    callback->proc = (sasl_callback_ft)&simple;
    callback->context = authid;
    ++callback;
  }

  if (realm!=NULL)
  {
    callback->id = SASL_CB_GETREALM;
    callback->proc = (sasl_callback_ft)&getrealm;
    callback->context = realm;
    callback++;
  }

  /* password */
  callback->id = SASL_CB_PASS;
  callback->proc = (sasl_callback_ft)&getsecret;
  callback->context = NULL;
  ++callback;

  /* echoprompt */
  callback->id = SASL_CB_ECHOPROMPT;
  callback->proc = (sasl_callback_ft)&prompt;
  callback->context = NULL;
  ++callback;

  /* noechoprompt */
  callback->id = SASL_CB_NOECHOPROMPT;
  callback->proc = (sasl_callback_ft)&prompt;
  callback->context = NULL;
  ++callback;

  /* termination */
  callback->id = SASL_CB_LIST_END;
  callback->proc = NULL;
  callback->context = NULL;
  ++callback;

  if (N_CALLBACKS < callback - callbacks)
    fail("Out of callback space; recompile with larger N_CALLBACKS");

  result = sasl_client_init(callbacks);
  if (result != SASL_OK)
    saslfail(result, "Initializing libsasl", NULL);

  result = sasl_client_new(service,
			   fqdn,
			   iplocal,ipremote,
			   NULL,serverlast,
			   &conn);
  if (result != SASL_OK)
    saslfail(result, "Allocating sasl connection state", NULL);

  if(extssf) {
      result = sasl_setprop(conn,
			    SASL_SSF_EXTERNAL,
			    &extssf);

      if (result != SASL_OK)
	  saslfail(result, "Setting external SSF", NULL);
  }
  
  if(ext_authid) {
      result = sasl_setprop(conn,
			    SASL_AUTH_EXTERNAL,
			    &ext_authid);

      if (result != SASL_OK)
	  saslfail(result, "Setting external authid", NULL);
  }
  
  result = sasl_setprop(conn,
			SASL_SEC_PROPS,
			&secprops);

  if (result != SASL_OK)
    saslfail(result, "Setting security properties", NULL);

  puts("Waiting for mechanism list from server...");
  len = samp_recv();

  if (mech) {
    printf("Forcing use of mechanism %s\n", mech);
    strncpy(buf, mech, SAMPLE_SEC_BUF_SIZE);
    buf[SAMPLE_SEC_BUF_SIZE - 1] = '\0';
  }

  printf("Choosing best mechanism from: %s\n", buf);

  if(clientfirst) {
      result = sasl_client_start(conn,
				 buf,
				 NULL,
				 &data,
				 &len,
				 &chosenmech);
  } else {
      data = "";
      len = 0;
      result = sasl_client_start(conn,
				 buf,
				 NULL,
				 NULL,
				 0,
				 &chosenmech);
  }
  
  
  if (result != SASL_OK && result != SASL_CONTINUE) {
      printf("error was %s\n", sasl_errdetail(conn));
      saslfail(result, "Starting SASL negotiation", NULL);
  }

  printf("Using mechanism %s\n", chosenmech);
  strcpy(buf, chosenmech);
  if (data) {
    if (SAMPLE_SEC_BUF_SIZE - strlen(buf) - 1 < len)
      fail("Not enough buffer space");
    puts("Preparing initial.");
    memcpy(buf + strlen(buf) + 1, data, len);
    len += (unsigned) strlen(buf) + 1;
    data = NULL;
  } else {
    len = (unsigned) strlen(buf);
  }
  
  puts("Sending initial response...");
  samp_send(buf, len);

  while (result == SASL_CONTINUE) {
    puts("Waiting for server reply...");
    len = samp_recv();
    result = sasl_client_step(conn, buf, len, NULL,
			      &data, &len);
    if (result != SASL_OK && result != SASL_CONTINUE)
	saslfail(result, "Performing SASL negotiation", NULL);
    if (data && len) {
	puts("Sending response...");
	samp_send(data, len);
    } else if (result != SASL_OK || !serverlast) {
	samp_send("",0);
    }
    
  }
  puts("Negotiation complete");

  result = sasl_getprop(conn, SASL_USERNAME, (const void **)&data);
  if (result != SASL_OK)
    sasldebug(result, "username", NULL);
  else
    printf("Username: %s\n", data);

#define CLIENT_MSG1 "client message 1"
#define SERVER_MSG1 "srv message 1"

  result = sasl_getprop(conn, SASL_SSF, (const void **)&ssf);
  if (result != SASL_OK)
    sasldebug(result, "ssf", NULL);
  else
    printf("SSF: %d\n", *ssf);
 
 printf("Waiting for encoded message...\n");
 len=samp_recv();
 {
 	unsigned int recv_len;
 	const char *recv_data;
	result=sasl_decode(conn,buf,len,&recv_data,&recv_len);
 	if (result != SASL_OK)
	    saslfail(result, "sasl_decode", NULL);
	printf("recieved decoded message '%s'\n",recv_data);
	if(strcmp(recv_data,SERVER_MSG1)!=0)
	    saslfail(1,"recive decoded server message",NULL);
 }
  result=sasl_encode(conn,CLIENT_MSG1,sizeof(CLIENT_MSG1),
  	&data,&len);
  if (result != SASL_OK)
      saslfail(result, "sasl_encode", NULL);
  printf("sending encrypted message '%s'\n",CLIENT_MSG1);
  samp_send(data,len);

  free_conn();
  sasl_done();

#ifdef WIN32
  WSACleanup();
#endif
  return (EXIT_SUCCESS);
}
