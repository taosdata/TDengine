/* smtpauth.c -- authenticate to SMTP server, then give normal protocol
 *
 * uses sfio
 *
 */

#include <config.h>

#include <sfio.h>
#include <sfio/stdio.h>
#include <ctype.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include <sasl.h>
#include <saslutil.h>

#include "sfsasl.h"

/* from OS: */
extern char *getpass();
extern struct hostent *gethostbyname();

static char *authname = NULL;
static char *username = NULL;
static char *realm = NULL;

extern char *optarg;
extern int optind;

int verbose = 0;
int emacs = 0;

int iptostring(const struct sockaddr *addr, socklen_t addrlen,
		     char *out, unsigned outlen) {
    char hbuf[NI_MAXHOST], pbuf[NI_MAXSERV];
    int niflags;

    if(!addr || !out) return SASL_BADPARAM;

    niflags = (NI_NUMERICHOST | NI_NUMERICSERV);
#ifdef NI_WITHSCOPEID
    if (addr->sa_family == AF_INET6)
	niflags |= NI_WITHSCOPEID;
#endif
    if (getnameinfo(addr, addrlen, hbuf, sizeof(hbuf), pbuf, sizeof(pbuf),
		    niflags) != 0)
	return SASL_BADPARAM;

    if(outlen < strlen(hbuf) + strlen(pbuf) + 2)
	return SASL_BUFOVER;

    snprintf(out, outlen, "%s;%s", hbuf, pbuf);

    return SASL_OK;
}

void usage(char *p)
{
    fprintf(stderr, "%s [-v] [-l] [-u username] [-a authname] [-s ssf] [-m mech] host[:port]\n", p);
    fprintf(stderr, " -v\tVerbose Output\n");
    fprintf(stderr, " -l\tLMTP semantics\n");
    exit(EX_USAGE);
}

#define ISGOOD(r) (((r) / 100) == 2)
#define TEMPFAIL(r) (((r) / 100) == 4)
#define PERMFAIL(r) (((r) / 100) == 5)
#define ISCONT(s) (s && (s[3] == '-'))

static int ask_code(const char *s)
{
    int ret = 0;
    
    if (s==NULL) return -1;

    if (strlen(s) < 3) return -1;

    /* check to make sure 0-2 are digits */
    if ((isdigit((int) s[0])==0) ||
	(isdigit((int) s[1])==0) ||
	(isdigit((int) s[2])==0))
    {
	return -1;
    }

    ret = ((s[0]-'0')*100)+((s[1]-'0')*10)+(s[2]-'0');
    
    return ret;
}

static void chop(char *s)
{
    char *p;

    assert(s);
    p = s + strlen(s) - 1;
    if (p[0] == '\n') {
	*p-- = '\0';
    }
    if (p >= s && p[0] == '\r') {
	*p-- = '\0';
    }
}

void interaction (int id, const char *prompt,
		  char **tresult, unsigned int *tlen)
{
    char result[1024];
    
    if (id==SASL_CB_PASS) {
	fprintf(stderr, "%s: ", prompt);
	*tresult = strdup(getpass("")); /* leaks! */
	*tlen=   strlen(*tresult);
	return;
    } else if (id==SASL_CB_USER) {
	if (username != NULL) {
	    strcpy(result, username);
	} else {
	    strcpy(result, getpwuid(getuid())->pw_name);
	}
    } else if (id==SASL_CB_AUTHNAME) {
	if (authname != NULL) {
	    strcpy(result, authname);
	} else {
	    strcpy(result, getpwuid(getuid())->pw_name);
	}
    } else if ((id==SASL_CB_GETREALM) && (realm != NULL)) {
      strcpy(result, realm);
    } else {
	int c;
	
	fprintf(stderr, "%s: ",prompt);
	fgets(result, sizeof(result) - 1, stdin);
	c = strlen(result);
	result[c - 1] = '\0';
    }

    *tlen = strlen(result);
    *tresult = (char *) malloc(*tlen+1); /* leaks! */
    memset(*tresult, 0, *tlen+1);
    memcpy((char *) *tresult, result, *tlen);
}

void fillin_interactions(sasl_interact_t *tlist)
{
    while (tlist->id != SASL_CB_LIST_END)
    {
	interaction(tlist->id, tlist->prompt,
		    (void *) &(tlist->result), 
		    &(tlist->len));
	tlist++;
    }
}

static sasl_callback_t callbacks[] = {
    { SASL_CB_GETREALM, NULL, NULL }, 
    { SASL_CB_USER, NULL, NULL }, 
    { SASL_CB_AUTHNAME, NULL, NULL }, 
    { SASL_CB_PASS, NULL, NULL }, 
    { SASL_CB_LIST_END, NULL, NULL }
};

static sasl_security_properties_t *make_secprops(int min,int max)
{
    sasl_security_properties_t *ret=(sasl_security_properties_t *)
	malloc(sizeof(sasl_security_properties_t));

    ret->maxbufsize = 8192;
    ret->min_ssf = min;
    ret->max_ssf = max;

    ret->security_flags = 0;
    ret->property_names = NULL;
    ret->property_values = NULL;

    return ret;
}

Sfio_t *debug;

int main(int argc, char **argv)
{
    char *mechlist = NULL;
    const char *mechusing = NULL;
    int minssf = 0, maxssf = 128;
    char *p;
    Sfio_t *server_in, *server_out;
    sasl_conn_t *conn = NULL;
    sasl_interact_t *client_interact = NULL;
    char in[4096];
    const char *out;
    unsigned int inlen, outlen;
    unsigned len;
    char out64[4096];
    int c;

    char *host;
    struct servent *service;
    int port;
    struct hostent *hp;
    struct sockaddr_in addr;
    char remote_ip[64], local_ip[64];
    int sock;

    char buf[1024];
    int sz;
    char greeting[1024];
    int code;
    int do_lmtp=0;
    int r = 0;

    debug = stderr;

    while ((c = getopt(argc, argv, "vElm:s:u:a:d:")) != EOF) {
	switch (c) {
	case 'm':
	    mechlist = optarg;
	    break;

	case 'l':
	    do_lmtp = 1;
	    break;

	case 's':
	    maxssf = atoi(optarg);
	    break;
	    
	case 'u':
	    username = optarg;
	    break;

	case 'a':
	    authname = optarg;
	    break;

	case 'v':
	    verbose++;
	    break;
	    
	case 'E':
	    emacs++;
	    break;

	case 'd':
	    sprintf(buf, "%s-%d", optarg, getpid());
	    debug = sfopen(NULL, buf, "w");
	    sfsetbuf(debug, NULL, 0);
	    break;

	case '?':
	default:
	    usage(argv[0]);
	    break;
	}
    }

    if (optind != argc - 1) {
	usage(argv[0]);
    }

    host = argv[optind];
    p = strchr(host, ':');
    if (p) {
	*p++ = '\0';
    } else {
	if(do_lmtp) {
	    p = "lmtp";
	} else {
	    p = "smtp";
	}
    }
    service = getservbyname(p, "tcp");
    if (service) {
	port = service->s_port;
    } else {
	port = atoi(p);
	if (!port) usage(argv[0]);
	port = htons(port);
    }

    if ((hp = gethostbyname(host)) == NULL) {
	perror("gethostbyname");
	exit(EX_NOHOST);
    }

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
	perror("socket");
	exit(EX_OSERR);
    }

    addr.sin_family = AF_INET;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = port;

    if (connect(sock, (struct sockaddr *) &addr, sizeof (addr)) < 0) {
	perror("connect");
	exit(EX_NOHOST);
    }

    server_in = sfnew(NULL, NULL, SF_UNBOUND, sock, SF_READ);
    server_out = sfnew(NULL, NULL, SF_UNBOUND, sock, SF_WRITE);

    /* read greeting */
    greeting[0] = '\0';
    for (;;) {
	sfsync(server_out);
	if (fgets(buf, sizeof(buf)-1, server_in)) {
	    if (greeting[0] == '\0') {
		strncpy(greeting, buf, sizeof(greeting) - 1);
	    }

	    if (verbose) fprintf(debug, "%s", buf);
	    code = ask_code(buf);
	    if (ISCONT(buf) && ISGOOD(code)) continue;
	} else {
	    code = 400;
	}
	break;
    }

    if (!ISGOOD(code)) goto done;

    /* EHLO */
    gethostname(buf, sizeof(buf)-1);
    if(do_lmtp) {
	if(verbose) fprintf(debug, "LHLO %s\r\n", buf);
	fprintf(server_out, "LHLO %s\r\n", buf);
    } else {
	if (verbose) fprintf(debug, "EHLO %s\r\n", buf);
	fprintf(server_out, "EHLO %s\r\n", buf);
    }
    
    /* read responses */
    for (;;) {
	sfsync(server_out);
	if (!fgets(buf, sizeof(buf)-1, server_in)) {
	    code = 400;
	    goto done;
	}
	if (verbose) fprintf(debug, "%s", buf);
	code = ask_code(buf);
	if (code == 250) {
	    /* we're only looking for AUTH */
	    if (!strncasecmp(buf + 4, "AUTH ", 5)) {
		chop(buf);
		if (!mechlist) mechlist = strdup(buf + 9);
	    }
	}
	if (ISCONT(buf) && ISGOOD(code)) {
	    continue;
	} else {
	    break;
	}
    }
    if (!ISGOOD(code)) goto done;

    /* attempt authentication */
    if (!mechlist) {
	if (verbose > 2) fprintf(debug, "no authentication\n");
	goto doneauth;
    }

    if (!r) r = sasl_client_init(callbacks);
    if (!r) {
	struct sockaddr_in saddr_r;
	int addrsize = sizeof(struct sockaddr_in);

	if (getpeername(sock, (struct sockaddr *) &saddr_r, &addrsize) < 0) {
	    perror("getpeername");
	    exit(EX_NOHOST);
	}
	r = iptostring((struct sockaddr *)&saddr_r,
		       sizeof(struct sockaddr_in), remote_ip, 64);
    }
    if (!r) {
	struct sockaddr_in saddr_l;
	int addrsize = sizeof(struct sockaddr_in);

	if (getsockname(sock, (struct sockaddr *) &saddr_l, &addrsize) < 0) {
	    perror("getsockname");
	    exit(EX_OSERR);
	}
	r = iptostring((struct sockaddr *)&saddr_l,
		       sizeof(struct sockaddr_in), local_ip, 64);
    }

    if (!r) {
	if(do_lmtp) {
	    r = sasl_client_new("lmtp", host, local_ip, remote_ip,
				NULL, 0, &conn);
	} else {
	    r = sasl_client_new("smtp", host, local_ip, remote_ip,
				NULL, 0, &conn);
	}
    }
    
    if (!r) {
	sasl_security_properties_t *secprops = make_secprops(minssf, maxssf);
	r = sasl_setprop(conn, SASL_SEC_PROPS, secprops);
	free(secprops);
    }
    
    if (!r) {
	do {
	    r = sasl_client_start(conn, mechlist,
				  &client_interact, &out, &outlen, &mechusing);
	    if (r == SASL_INTERACT) {
		fillin_interactions(client_interact);
	    }
	} while (r == SASL_INTERACT);

	if (r == SASL_OK || r == SASL_CONTINUE) {
	    if (outlen > 0) {
		r = sasl_encode64(out, outlen, out64, sizeof out64, NULL);
		if (!r) {
		    if (verbose) 
			fprintf(debug, "AUTH %s %s\r\n", mechusing, out64);
		    fprintf(server_out, "AUTH %s %s\r\n", mechusing, out64);
		}
	    } else {
		if (verbose) fprintf(debug, "AUTH %s\r\n", mechusing);
		fprintf(server_out, "AUTH %s\r\n", mechusing);
	    }
	} else {
	    fprintf(debug, "\nclient start failed: %s\n", sasl_errdetail(conn));
	}
	
    }

    /* jump to doneauth if we succeed */
    while (r == SASL_OK || r == SASL_CONTINUE) {
	sfsync(server_out);
	if (!fgets(buf, sizeof(buf)-1, server_in)) {
	    code = 400;
	    goto done;
	}
	if (verbose) fprintf(debug, "%s", buf);
	code = ask_code(buf);
	if (ISCONT(buf)) continue;
	if (ISGOOD(code)) {
	    if (code != 235) {
		/* weird! */
	    }
	    /* yay, we won! */
	    sfdcsasl(server_in, conn);
	    sfdcsasl(server_out, conn);
	    goto doneauth;
	} else if (code != 334) {
	    /* unexpected response */
	    break;
	}
	len = strlen(buf);
	if (len > 0 && buf[len-1] == '\n') {
	    buf[len-1] = '\0';
	}
	r = sasl_decode64(buf + 4, strlen(buf) - 6, in, 4096, &inlen);
	if (r != SASL_OK) break;
	
	do {
	    r = sasl_client_step(conn, in, inlen, &client_interact,
				 &out, &outlen);
	    if (r == SASL_INTERACT) {
		fillin_interactions(client_interact);
	    }
	} while (r == SASL_INTERACT);

	if (r == SASL_OK || r == SASL_CONTINUE) {
	    r = sasl_encode64(out, outlen, out64, sizeof out64, NULL);
	}
	if (r == SASL_OK) {
	    if (verbose) fprintf(debug, "%s\r\n", out64);
	    fprintf(server_out, "%s\r\n", out64);
	}
    }

    /* auth failed! */
    if (!r) {
	fprintf(debug, "%d authentication failed\n", code);
    } else {
	fprintf(debug, "400 authentication failed: %s\n", 
		sasl_errstring(r, NULL, NULL));
    }
    exit(EX_SOFTWARE);

 doneauth:
    /* ready for application */
    greeting[3] = '-';
    printf("%s", greeting);
    printf("220 %s %s\r\n", host, conn ? "authenticated" : "no auth");

    fcntl(0, F_SETFL, O_NONBLOCK);
    fcntl(sock, F_SETFL, O_NONBLOCK);
    sfset(stdin, SF_SHARE, 0);

    /* feed data back 'n forth */
    for (;;) {
	Sfio_t *flist[3];

    top:
	flist[0] = stdin;
	flist[1] = server_in;

	/* sfpoll */
	if (verbose > 5) fprintf(debug, "poll\n");
	r = sfpoll(flist, 2, -1);
	if (verbose > 5) fprintf(debug, "poll 2\n");

	while (r--) {
	    if (flist[r] == server_in) {
		do {
		    if (verbose > 5) fprintf(debug, "server!\n");
		    errno = 0;
		    sz = sfread(server_in, buf, sizeof(buf)-1);
		    if (sz == 0 && (errno == EAGAIN)) goto top;
		    if (sz <= 0) goto out;
		    buf[sz] = '\0';
		    if (verbose > 5) fprintf(debug, "server 2 '%s'!\n", buf);
		    sfwrite(stdout, buf, sz);
		} while (sfpoll(&server_in, 1, 0));
		sfsync(stdout);
	    } else if (flist[r] == stdin) {
		Sfio_t *p[1];

		p[0] = stdin;
		do {
		    if (verbose > 5) fprintf(debug, "stdin!\n");
		    errno = 0;
		    sz = sfread(stdin, buf, sizeof(buf)-1);
		    if (sz == 0 && (errno == EAGAIN)) goto top;
		    if (sz <= 0) goto out;
		    buf[sz] = '\0';
		    if (verbose > 5) fprintf(debug, "stdin 2 '%s'!\n", buf);
		    if (emacs) {
			int i;

			/* fix emacs stupidness */
			for (i = 0; i < sz - 1; i++) {
			    if (buf[i] == '\n' && buf[i+1] == '\n')
				buf[i++] = '\r';
			}
			if (buf[sz-2] != '\r' && buf[sz-1] == '\n') {
			    sfungetc(stdin, buf[sz--]);
			    buf[sz] = '\0';
			}

			if (verbose > 5) fprintf(debug, "emacs '%s'!\n", buf);
		    }
		    sfwrite(server_out, buf, sz);
		    if (verbose > 7) fprintf(debug, "stdin 3!\n");
		} while (sfpoll(p, 1, 0));
		sfsync(server_out);
	    } else {
		abort();
	    }
	}
    }
 out:
    if (verbose > 3) fprintf(debug, "exiting! %d %s\n", sz, strerror(errno));
    exit(EX_OK);

 done:
    if (ISGOOD(code)) {
	if (verbose > 1) fprintf(debug, "ok\n");
	exit(EX_OK);
    }
    if (TEMPFAIL(code)) {
	if (verbose > 1) fprintf(debug, "tempfail\n");
	exit(EX_TEMPFAIL);
    }
    if (PERMFAIL(code)) {
	if (verbose > 1) fprintf(debug, "permfail\n");
	exit(EX_UNAVAILABLE);
    }
    
    if (verbose > 1) fprintf(debug, "unknown failure\n");
    exit(EX_TEMPFAIL);
}
