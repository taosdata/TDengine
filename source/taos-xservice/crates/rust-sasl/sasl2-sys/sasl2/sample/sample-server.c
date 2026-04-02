/* sample-server.c -- sample SASL server
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

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef WIN32
# include <winsock2.h>
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
__declspec(dllimport) int getsubopt(char **optionp, const char * const *tokens, char **valuep);
#define HAVE_GETSUBOPT
#else /* WIN32 */
# include <netinet/in.h>
#endif /* WIN32 */
#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>

#ifndef HAVE_GETSUBOPT
int getsubopt(char **optionp, const char * const *tokens, char **valuep);
#endif

static const char *progname = NULL;
static int verbose;

/* Note: if this is changed, change it in samp_read(), too. */
#define SAMPLE_SEC_BUF_SIZE (2048)

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

char *mech = NULL,
  *iplocal = NULL,
  *ipremote = NULL,
  *searchpath = NULL,
  *service = "rcmd",
  *localdomain = NULL,
  *userdomain = NULL;
sasl_conn_t *conn = NULL;

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

static int
getpath(void *context __attribute__((unused)),
	char ** path) 
{
  if (! path)
    return SASL_BADPARAM;

  if (searchpath) {
    *path = searchpath;
  } else {
    *path = PLUGINDIR;
  }

  return SASL_OK;
}

static sasl_callback_t callbacks[] = {
  {
    SASL_CB_LOG, (sasl_callback_ft)&sasl_my_log, NULL
  }, {
    SASL_CB_GETPATH, (sasl_callback_ft)&getpath, NULL
  }, {
    SASL_CB_LIST_END, NULL, NULL
  }
};

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
  printf("S: %s\n", buf);
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

  if (strncmp(buf, "C: ", 3) != 0) {
    fail("Line must start with 'C: '");
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
  printf("got '%s'\n", buf);
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
  unsigned len, count;
  const char *data;
  int serverlast = 0;
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
  while ((c = getopt(argc, argv, "vlhb:e:m:f:i:p:s:d:u:?")) != EOF)
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

    case 'l':
	serverlast = SASL_SUCCESS_DATA;
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

    case 's':
      service = optarg;
      break;

    case 'd':
      localdomain = optarg;
      break;

    case 'u':
      userdomain = optarg;
      break;

    default:		
      errflag = 1;
      break;
    }

  if (optind != argc) {
    
    errflag = 1;
  }

  if (errflag) {
    fprintf(stderr, "%s: Usage: %s [-b min=N,max=N] [-e ssf=N,id=ID] [-m MECH] [-f FLAGS] [-i local=IP,remote=IP] [-p PATH] [-d DOM] [-u DOM] [-s NAME]\n"
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
	    "\t\tpasscred\tattempt to receive client credentials\n"
	    "\t-i ...\tset IP addresses (required by some mechs)\n"
	    "\t\tlocal=IP;PORT\tset local address to IP, port PORT\n"
	    "\t\tremote=IP;PORT\tset remote address to IP, port PORT\n"
	    "\t-p PATH\tcolon-seperated search path for mechanisms\n"
	    "\t-s NAME\tservice name to pass to mechanisms\n"
	    "\t-d DOM\tlocal server domain\n"
	    "\t-u DOM\tuser domain\n"
	    "\t-l\tenable server-send-last\n",
	    progname, progname);
    exit(EXIT_FAILURE);
  }

  result = sasl_server_init(callbacks, "sample");
  if (result != SASL_OK)
    saslfail(result, "Initializing libsasl", NULL);

  atexit(&sasl_done);

  result = sasl_server_new(service,
			   localdomain,
			   userdomain,
			   iplocal,
			   ipremote,
			   NULL,
			   serverlast,
			   &conn);
  if (result != SASL_OK)
    saslfail(result, "Allocating sasl connection state", NULL);
  
  atexit(&free_conn);

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

  if (mech) {
    printf("Forcing use of mechanism %s\n", mech);
    data = strdup(mech);
    if (! data)
      osfail();
    len = (unsigned) strlen(data);
    count = 1;
  } else {
    puts("Generating client mechanism list...");
    result = sasl_listmech(conn,
			   ext_authid,
			   NULL,
			   " ",
			   NULL,
			   &data,
			   &len,
			   &count);
    if (result != SASL_OK)
      saslfail(result, "Generating client mechanism list", NULL);
  }
  
  printf("Sending list of %d mechanism(s)\n", count);
  samp_send(data, len);

  if(mech) {
      free((void *)data);
  }

  puts("Waiting for client mechanism...");
  len = samp_recv();
  if (mech && strcasecmp(mech, buf))
    fail("Client chose something other than the mandatory mechanism");
  if (strlen(buf) < len) {
    /* Hmm, there's an initial response here */
    data = buf + strlen(buf) + 1;
    len = len - (unsigned) strlen(buf) - 1;
  } else {
    data = NULL;
    len = 0;
  }
  result = sasl_server_start(conn,
			     buf,
			     data,
			     len,
			     &data,
			     &len);
  if (result != SASL_OK && result != SASL_CONTINUE)
    saslfail(result, "Starting SASL negotiation", sasl_errstring(result,NULL,NULL));

  while (result == SASL_CONTINUE) {
    if (data) {
      puts("Sending response...");
      samp_send(data, len);
    } else
      fail("No data to send--something's wrong");
    puts("Waiting for client reply...");
    len = samp_recv();
    data = NULL;
    result = sasl_server_step(conn, buf, len,
			      &data, &len);
    if (result != SASL_OK && result != SASL_CONTINUE)
      saslfail(result, "Performing SASL negotiation", sasl_errstring(result,NULL,NULL));
  }
  puts("Negotiation complete");

  if(serverlast&&data) {
      printf("might need additional send:\n");
      samp_send(data,len);
  }

  result = sasl_getprop(conn, SASL_USERNAME, (const void **)&data);
  if (result != SASL_OK)
    sasldebug(result, "username", NULL);
  else
    printf("Username: %s\n", data ? data : "(NULL)");

  result = sasl_getprop(conn, SASL_DEFUSERREALM, (const void **)&data);
  if (result != SASL_OK)
    sasldebug(result, "realm", NULL);
  else
    printf("Realm: %s\n", data ? data : "(NULL)");

  result = sasl_getprop(conn, SASL_SSF, (const void **)&ssf);
  if (result != SASL_OK)
    sasldebug(result, "ssf", NULL);
  else
    printf("SSF: %d\n", *ssf);
#define CLIENT_MSG1 "client message 1"
#define SERVER_MSG1 "srv message 1"
  result=sasl_encode(conn,SERVER_MSG1,sizeof(SERVER_MSG1),
  	&data,&len);
  if (result != SASL_OK)
      saslfail(result, "sasl_encode", NULL);
  printf("sending encrypted message '%s'\n",SERVER_MSG1);
  samp_send(data,len);
  printf("Waiting for encrypted message...\n");
  len=samp_recv();
 {
 	unsigned int recv_len;
 	const char *recv_data;
	result=sasl_decode(conn,buf,len,&recv_data,&recv_len);
 	if (result != SASL_OK)
      saslfail(result, "sasl_encode", NULL);
    printf("recieved decoded message '%s'\n",recv_data);
    if(strcmp(recv_data,CLIENT_MSG1)!=0)
    	saslfail(1,"recive decoded server message",NULL);
 }

#ifdef WIN32
  WSACleanup();
#endif

  return (EXIT_SUCCESS);
}
