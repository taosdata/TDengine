/* testsuite.c -- Stress the library a little
 * Rob Siemborski
 * Tim Martin
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

/*
 * To create a krb5 srvtab file given a krb4 srvtab
 *
 * ~/> ktutil
 * ktutil:  rst /etc/srvtab
 * ktutil:  wkt /etc/krb5.keytab
 * ktutil:  q
 */

/*
 * TODO [FIXME]:
 *  put in alloc() routines that fail occasionally.
 */

#include <config.h>

#include <stdio.h>
#include <stdlib.h>

#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>
#include <prop.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <time.h>
#include <string.h>
#include <ctype.h>
#ifndef WIN32
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/file.h>
#endif

#ifdef WIN32
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
__declspec(dllimport) int getsubopt(char **optionp, char * const *tokens, char **valuep);
#endif

char myhostname[1024+1];
#define MAX_STEPS 7 /* maximum steps any mechanism takes */

#define CLIENT_TO_SERVER "Hello. Here is some stuff"

#define REALLY_LONG_LENGTH  32000
#define REALLY_LONG_BACKOFF  2000

const char *username = "ken";
const char *nonexistant_username = "ABCDEFGHIJ";
const char *authname = "ken";
const char *proxyasname = "kenproxy";
const char *password = "1234";
sasl_secret_t * g_secret = NULL;
const char *cu_plugin = "INTERNAL";
char other_result[1024];

int proxyflag = 0;

static const char *gssapi_service = "host";

/* our types of failures */
typedef enum {
    NOTHING = 0,
    ONEBYTE_RANDOM,		/* replace one byte with something random */
    ONEBYTE_NULL,		/* replace one byte with a null */
    ONEBYTE_QUOTES,		/* replace one byte with a double quote 
				   (try to fuck with digest-md5) */
    ONLY_ONE_BYTE,		/* send only one byte */
    ADDSOME,			/* add some random bytes onto the end */
    SHORTEN,			/* shorten the string some */
    REASONABLE_RANDOM,		/* send same size but random */
    REALLYBIG,			/* send something absurdly large all random */
    NEGATIVE_LENGTH,		/* send negative length */
    CORRUPT_SIZE		/* keep this one last */
} corrupt_type_t;

const char *corrupt_types[] = {
    "NOTHING",
    "ONEBYTE_RANDOM",
    "ONEBYTE_NULL",
    "ONEBYTE_QUOTES",
    "ONLY_ONE_BYTE",
    "ADDSOME",
    "SHORTEN",
    "REASONABLE_RANDOM",
    "REALLYBIG",
    "NEGATIVE_LENGTH",
    "CORRUPT_SIZE"
};

void fatal(char *str)
{
    printf("Failed with: %s\n",str);
    exit(3);
}

/* interactions we support */
static sasl_callback_t client_interactions[] = {
  {
    SASL_CB_GETREALM, NULL, NULL
  }, {
    SASL_CB_USER, NULL, NULL
  }, {
    SASL_CB_AUTHNAME, NULL, NULL
  }, {
    SASL_CB_PASS, NULL, NULL    
  }, {
    SASL_CB_LIST_END, NULL, NULL
  }
};

int test_getrealm(void *context __attribute__((unused)), int id,
		  const char **availrealms __attribute__((unused)),
		  const char **result) 
{
    if(id != SASL_CB_GETREALM) fatal("test_getrealm not looking for realm");
    if(!result) return SASL_BADPARAM;
    *result = myhostname;
    return SASL_OK;
}

int test_getsecret(sasl_conn_t *conn __attribute__((unused)),
		   void *context __attribute__((unused)), int id,
		   sasl_secret_t **psecret) 
{
    if(id != SASL_CB_PASS) fatal("test_getsecret not looking for pass");
    if(!psecret) return SASL_BADPARAM;

    *psecret = g_secret;

    return SASL_OK;
}

int test_getsimple(void *context __attribute__((unused)), int id,
		   const char **result, unsigned *len) 
{
    if(!result) return SASL_BADPARAM;
    
    if (id==SASL_CB_USER && proxyflag == 0) {
	*result=(char *) username;
    } else if (id==SASL_CB_USER && proxyflag == 1) {
	*result=(char *) proxyasname;
    } else if (id==SASL_CB_AUTHNAME) {
	*result=(char *) authname;
    } else {
	printf("I want %d\n", id);
	fatal("unknown callback in test_getsimple");
    }

    if (len) *len = (unsigned) strlen(*result);
    return SASL_OK;
}

/* callbacks we support */
static sasl_callback_t client_callbacks[] = {
  {
    SASL_CB_GETREALM, (sasl_callback_ft)test_getrealm, NULL
  }, {
    SASL_CB_USER, (sasl_callback_ft)test_getsimple, NULL
  }, {
    SASL_CB_AUTHNAME, (sasl_callback_ft)test_getsimple, NULL
  }, {
    SASL_CB_PASS, (sasl_callback_ft)test_getsecret, NULL    
  }, {
    SASL_CB_LIST_END, NULL, NULL
  }
};

typedef void *foreach_t(char *mech, void *rock);

typedef struct tosend_s {
    corrupt_type_t type; /* type of corruption to make */
    int step; /* step it should send bogus data on */
    sasl_callback_t *client_callbacks; /* which client callbacks to use */
} tosend_t;

typedef struct mem_info 
{
    void *addr;
    size_t size;
    struct mem_info *next;
} mem_info_t;

int DETAILED_MEMORY_DEBUGGING = 0;

mem_info_t *head = NULL;

#ifndef WITH_DMALLOC

void *test_malloc(size_t size)
{
    void *out;
    mem_info_t *new_data;
    
    out = malloc(size);

    if(DETAILED_MEMORY_DEBUGGING)
	fprintf(stderr, "  %p = malloc(%u)\n", out, (unsigned) size);
    
    if(out) {
	new_data = malloc(sizeof(mem_info_t));
	if(!new_data) return out;

	new_data->addr = out;
	new_data->size = size;
	new_data->next = head;
	head = new_data;
    }

    return out;
}

void *test_realloc(void *ptr, size_t size)
{
    void *out;
    mem_info_t *cur;
    
    out = realloc(ptr, size);
    
    if(DETAILED_MEMORY_DEBUGGING)
	fprintf(stderr, "  %p = realloc(%p,%zd)\n",
		out, ptr, size);

    cur = head;
    
    while(cur) {
	if(cur->addr == ptr) {
	    cur->addr = out;
	    cur->size = size;
	    return out;
	}
	
	cur = cur->next;
    }
    
    if(DETAILED_MEMORY_DEBUGGING && cur == NULL) {
	fprintf(stderr,
		"  MEM WARNING: reallocing something we never allocated!\n");

	cur = malloc(sizeof(mem_info_t));
	if(!cur) return out;

	cur->addr = out;
	cur->size = size;
	cur->next = head;
	head = cur;
    }

    return out;
}

void *test_calloc(size_t nmemb, size_t size)
{
    void *out;
    mem_info_t *new_data;
    
    out = calloc(nmemb, size);

    if(DETAILED_MEMORY_DEBUGGING)    
	fprintf(stderr, "  %p = calloc(%zd, %zd)\n",
		out, nmemb, size);

    if(out) {
	new_data = malloc(sizeof(mem_info_t));
	if(!new_data) return out;

	new_data->addr = out;
	new_data->size = size;
	new_data->next = head;
	head = new_data;
    }
    
    return out;
}


void test_free(void *ptr)
{
    mem_info_t **prev, *cur;

    if(DETAILED_MEMORY_DEBUGGING)
	fprintf(stderr, "  free(%p)\n",
		ptr);

    prev = &head; cur = head;
    
    while(cur) {
	if(cur->addr == ptr) {
	    *prev = cur->next;
	    free(cur);
	    break;
	}
	
	prev = &cur->next;
	cur = cur->next;
    }

    if(DETAILED_MEMORY_DEBUGGING && cur == NULL) {
	fprintf(stderr,
		"  MEM WARNING: Freeing something we never allocated!\n");
    }

    free(ptr);
}

#endif /* WITH_DMALLOC */

int mem_stat() 
{
#ifndef WITH_DMALLOC
    mem_info_t *cur;
    size_t n;
    unsigned char *data;

    if(!head) {
	fprintf(stderr, "  All memory accounted for!\n");
	return SASL_OK;
    }
    
    fprintf(stderr, "  Currently Still Allocated:\n");
    for(cur = head; cur; cur = cur->next) {
	fprintf(stderr, "    %p (%5zd)\t", cur->addr, cur->size);
	for(data = (unsigned char *) cur->addr,
		n = 0; n < (cur->size > 12 ? 12 : cur->size); n++) {
	    if (isprint((int) data[n]))
		fprintf(stderr, "'%c' ", (char) data[n]);
	    else
		fprintf(stderr, "%02X  ", data[n] & 0xff);
	}
	if (n < cur->size)
	    fprintf(stderr, "...");
	fprintf(stderr, "\n");
    }
    return SASL_FAIL;
#else
    return SASL_OK;
#endif /* WITH_DMALLOC */
}


/************* End Memory Allocation functions ******/

/* my mutex functions */
int g_mutex_cnt = 0;

typedef struct my_mutex_s {

    int num;
    int val;
    
} my_mutex_t;

void *my_mutex_new(void)
{
    my_mutex_t *ret = (my_mutex_t *)malloc(sizeof(my_mutex_t));
    ret->num = g_mutex_cnt;
    g_mutex_cnt++;

    ret->val = 0;

    return ret;
}

int my_mutex_lock(my_mutex_t *m)
{
    if (m->val != 0)
    {
	fatal("Trying to lock a mutex already locked [single-threaded app]");
    }

    m->val = 1;
    return SASL_OK;
}

int my_mutex_unlock(my_mutex_t *m)
{
    if (m->val != 1)
    {
	fatal("Unlocking mutex that isn't locked");
    }

    m->val = 0;

    return SASL_OK;
}

void my_mutex_dispose(my_mutex_t *m)
{
    if (m==NULL) return;

    free(m);

    return;
}

int good_getopt(void *context __attribute__((unused)), 
		const char *plugin_name __attribute__((unused)), 
		const char *option,
		const char **result,
		unsigned *len)
{
    if (strcmp(option,"pwcheck_method")==0)
    {
	*result = "auxprop";
	if (len)
	    *len = (unsigned) strlen("auxprop");
	return SASL_OK;
    } else if (!strcmp(option, "auxprop_plugin")) {
	*result = "sasldb";
	if (len)
	    *len = (unsigned) strlen("sasldb");
	return SASL_OK;
    } else if (!strcmp(option, "sasldb_path")) {
	*result = "./sasldb";
	if (len)
	    *len = (unsigned) strlen("./sasldb");
	return SASL_OK;
    } else if (!strcmp(option, "canon_user_plugin")) {
	*result = cu_plugin;
	if (len)
	    *len = (unsigned) strlen(*result);
	return SASL_OK;
    }

    return SASL_FAIL;
}

static struct sasl_callback goodsasl_cb[] = {
    { SASL_CB_GETOPT, (sasl_callback_ft)&good_getopt, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

#if defined(DO_DLOPEN) && (defined(PIC) || (!defined(PIC) && defined(TRY_DLOPEN_WHEN_STATIC)))
int givebadpath(void * context __attribute__((unused)), 
		char ** path)
{
    int lup;
    *path = malloc(10000);    
    strcpy(*path,"/tmp/is/not/valid/path/");

    for (lup = 0;lup<1000;lup++)
	strcat(*path,"a/");

    return SASL_OK;
}

static struct sasl_callback withbadpathsasl_cb[] = {
    { SASL_CB_GETPATH, (sasl_callback_ft)&givebadpath, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};
#endif

int giveokpath(void * context __attribute__((unused)), 
		const char ** path)
{
    *path = "/tmp/";

    return SASL_OK;
}

static struct sasl_callback withokpathsasl_cb[] = {
    { SASL_CB_GETPATH, (sasl_callback_ft)&giveokpath, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

static struct sasl_callback emptysasl_cb[] = {
    { SASL_CB_LIST_END, NULL, NULL }
};

static int proxy_authproc(sasl_conn_t *conn,
			   void *context __attribute__((unused)),
			   const char *requested_user,
			   unsigned rlen __attribute__((unused)),
			   const char *auth_identity,
			   unsigned alen __attribute__((unused)),
			   const char *def_realm __attribute__((unused)),
			   unsigned urlen __attribute__((unused)),
			   struct propctx *propctx __attribute__((unused)))
{
    if(!strcmp(auth_identity, authname)
       && !strcmp(requested_user, proxyasname)) return SASL_OK;

    if(!strcmp(auth_identity, requested_user)) {
	printf("Warning: Authenticated name but DID NOT proxy (%s/%s)\n",
	       requested_user, auth_identity);
	return SASL_OK;
    }

    sasl_seterror(conn, SASL_NOLOG, "authorization failed: %s by %s",
		  requested_user, auth_identity);
    return SASL_BADAUTH;
}

static struct sasl_callback goodsaslproxy_cb[] = {
    { SASL_CB_PROXY_POLICY, (sasl_callback_ft)&proxy_authproc, NULL },
    { SASL_CB_GETOPT, (sasl_callback_ft)&good_getopt, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

char really_long_string[REALLY_LONG_LENGTH];

/*
 * Setup some things for test
 */
void init(unsigned int seed)
{
    int lup;
    int result;

    srand(seed);    

    for (lup=0;lup<REALLY_LONG_LENGTH;lup++)
	really_long_string[lup] = '0' + (rand() % 10);

    really_long_string[REALLY_LONG_LENGTH - rand() % REALLY_LONG_BACKOFF] = '\0';

    result = gethostname(myhostname, sizeof(myhostname)-1);
    if (result == -1) fatal("gethostname");

    sasl_set_mutex((sasl_mutex_alloc_t *) &my_mutex_new,
		   (sasl_mutex_lock_t *) &my_mutex_lock,
		   (sasl_mutex_unlock_t *) &my_mutex_unlock,
		   (sasl_mutex_free_t *) &my_mutex_dispose);

#ifndef WITH_DMALLOC
    sasl_set_alloc((sasl_malloc_t *)test_malloc,
		   (sasl_calloc_t *)test_calloc,
		   (sasl_realloc_t *)test_realloc,
		   (sasl_free_t *)test_free);
#endif

}

/*
 * Tests for sasl_server_init
 */

void test_init(void)
{
    int result;

    /* sasl_done() before anything */
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after sasl_done test");

    /* Try passing appname a really long string (just see if it crashes it)*/

    result = sasl_server_init(NULL,really_long_string);
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after long appname test");

    /* this calls sasl_done when it wasn't inited */
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after null appname test");

    /* try giving it a different path for where the plugins are */
    result = sasl_server_init(withokpathsasl_cb, "Tester");
    if (result!=SASL_OK) fatal("Didn't deal with ok callback path very well");
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after callback path test");

    /* and the client */
    result = sasl_client_init(withokpathsasl_cb);

    if (result!=SASL_OK)
	fatal("Client didn't deal with ok callback path very well");
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after client test");

#if defined(DO_DLOPEN) && (defined(PIC) || (!defined(PIC) && defined(TRY_DLOPEN_WHEN_STATIC)))
    /* try giving it an invalid path for where the plugins are */
    result = sasl_server_init(withbadpathsasl_cb, NULL);
    if (result==SASL_OK) fatal("Allowed invalid path");
    sasl_done();
    if(mem_stat() != SASL_OK) fatal("memory error after bad path test");
#endif

    /* and the client - xxx is this necessary?*/
#if 0
    result = sasl_client_init(withbadpathsasl_cb);

    if (result==SASL_OK)
	fatal("Client allowed invalid path");
    sasl_done();
#endif

    /* Now try to break all the sasl_server_* functions for not returning
       SASL_NOTINIT */

    if(sasl_global_listmech())
	fatal("sasl_global_listmech did not return NULL with no library initialized");

    if(sasl_server_new(NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL)
       != SASL_NOTINIT)
	fatal("sasl_server_new did not return SASL_NOTINIT");

/* Can't check this validly without a server conn, so this would be
   a hard one to tickle anyway */
#if 0    
    if(sasl_listmech(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_listmech did not return SASL_NOTINIT");
#endif

    if(sasl_server_start(NULL, NULL, NULL, 0, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_server_start did not return SASL_NOTINIT");

    if(sasl_server_step(NULL, NULL, 0, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_server_step did not return SASL_NOTINIT");
    
#ifdef DO_SASL_CHECKAPOP
    if(sasl_checkapop(NULL, NULL, 0, NULL, 0)
       != SASL_NOTINIT)
	fatal("sasl_checkapop did not return SASL_NOTINIT");
#endif    

    if(sasl_checkpass(NULL, NULL, 0, NULL, 0)
       != SASL_NOTINIT)
	fatal("sasl_checkpass did not return SASL_NOTINIT");
    
    if(sasl_user_exists(NULL, NULL, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_user_exists did not return SASL_NOTINIT");

    if(sasl_setpass(NULL, NULL, NULL, 0, NULL, 0, 0)
       != SASL_NOTINIT)
	fatal("sasl_setpass did not return SASL_NOTINIT");

    /* And sasl_client_*... */

    if(sasl_client_new(NULL, NULL, NULL, NULL, NULL, 0, NULL)
       != SASL_NOTINIT)
	fatal("sasl_client_new did not return SASL_NOTINIT");

    if(sasl_client_start(NULL, NULL, NULL, NULL, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_client_start did not return SASL_NOTINIT");

    if(sasl_client_step(NULL, NULL, 0, NULL, NULL, NULL)
       != SASL_NOTINIT)
	fatal("sasl_client_step did not return SASL_NOTINIT");

}


/* 
 * Tests sasl_listmech command
 */

void test_listmech(void)
{
    sasl_conn_t *saslconn, *cconn;
    int result;
    const char *str = NULL;
    unsigned plen;
    unsigned lup, flag;
    int pcount;
    const char **list;

    /* test without initializing library */
    result = sasl_listmech(NULL, /* conn */
			   NULL,
			   "[",
			   "-",
			   "]",
			   &str,
			   NULL,
			   NULL);

    /*    printf("List mech without library initialized: %s\n",sasl_errstring(result,NULL,NULL));*/
    if (result == SASL_OK) fatal("Failed sasl_listmech() with NULL saslconn");

    if (sasl_server_init(emptysasl_cb,"TestSuite")!=SASL_OK)
	fatal("can't sasl_server_init");
    if (sasl_client_init(client_interactions)!=SASL_OK)
	fatal("can't sasl_client_init");

    if (sasl_server_new("rcmd", myhostname,
			NULL, NULL, NULL, NULL, 0, 
			&saslconn) != SASL_OK)
	fatal("can't sasl_server_new");

    if (sasl_setprop(saslconn, SASL_AUTH_EXTERNAL, authname)!=SASL_OK)
	fatal("sasl_setprop(SASL_AUTH_EXTERNAL) failed");

    /* client new connection */
    if (sasl_client_new("rcmd",
			myhostname,
			NULL, NULL, NULL,
			0,
			&cconn)!= SASL_OK)
	fatal("sasl_client_new() failure");

    if (sasl_setprop(cconn, SASL_AUTH_EXTERNAL, authname)!=SASL_OK)
	fatal("sasl_setprop(SASL_AUTH_EXTERNAL) failed");

    /* try both sides */
    list = sasl_global_listmech();
    if(!list) fatal("sasl_global_listmech failure");

    printf(" [");
    flag = 0;
    for(lup = 0; list[lup]; lup++) {
	if(flag) printf(",");
	else flag++;
	printf("%s",list[lup]);
    }
    printf("]\n");

    /* try client side */
    result = sasl_listmech(cconn,
			   NULL,
			   " [",
			   ",",
			   "]",
			   &str,
			   NULL,
			   NULL);
    if(result == SASL_OK) {
	printf("Client mechlist:\n%s\n", str);
    } else {
	fatal("client side sasl_listmech failed");
    }

    /* Test with really long user */

    result = sasl_listmech(saslconn,
			   really_long_string,
			   "[",
			   "-",
			   "]",
			   &str,
			   NULL,
			   NULL);

    if (result != SASL_OK) fatal("Failed sasl_listmech() with long user");

    if (str[0]!='[') fatal("Failed sasl_listmech() with long user (didn't start with '['");

    result = sasl_listmech(saslconn,
			   really_long_string,
			   "[",
			   ",",
			   "]",
			   &str,
			   NULL,
			   NULL);

    if (result != SASL_OK) fatal("Failed sasl_listmech() with different params");

    printf("We have the following mechs:\n %s\n",str);

    /* Test with really long prefix */

    result = sasl_listmech(saslconn,
			   NULL,
			   really_long_string,
			   "-",
			   "]",
			   &str,
			   NULL,
			   NULL);

    if (result != SASL_OK) fatal("failed sasl_listmech() with long prefix");

    if (str[0]!=really_long_string[0]) fatal("failed sasl_listmech() with long prefix (str is suspect)");

    /* Test with really long suffix */

    result = sasl_listmech(saslconn,
			   NULL,
			   "[",
			   "-",
			   really_long_string,
			   &str,
			   NULL,
			   NULL);

    if (result != SASL_OK) fatal("Failed sasl_listmech() with long suffix");

    /* Test with really long seperator */

    result = sasl_listmech(saslconn,
			   NULL,
			   "[",
			   really_long_string,
			   "]",
			   &str,
			   NULL,
			   NULL);

    if (result != SASL_OK) fatal("Failed sasl_listmech() with long seperator");

    /* Test contents of output string is accurate */
    result = sasl_listmech(saslconn,
			   NULL,
			   "",
			   "%",
			   "",
			   &str,
			   &plen,
			   &pcount);

    if (result != SASL_OK) fatal("Failed sasl_listmech()");

    if (strlen(str)!=plen) fatal("Length of string doesn't match what we were told");
    
    for (lup=0;lup<plen;lup++)
	if (str[lup]=='%')
	    pcount--;

    pcount--;
    if (pcount != 0)
    {
	printf("mechanism string = %s\n",str);
	printf("Mechs left = %d\n",pcount);
	fatal("Number of mechs received doesn't match what we were told");
    }

    /* Call sasl done then make sure listmech doesn't work anymore */
    sasl_dispose(&saslconn);
    sasl_dispose(&cconn);
    sasl_done();

    result = sasl_listmech(saslconn,
			   NULL,
			   "[",
			   "-",
			   "]",
			   &str,
			   NULL,
			   NULL);

    if (result == SASL_OK) fatal("Called sasl_done but listmech still works\n");

}

/*
 * Perform tests on the random utilities
 */

void test_random(void)
{
    sasl_rand_t *rpool;
    int lup;
    char buf[4096];

    /* make sure it works consistantly */

    for (lup = 0;lup<10;lup++)
    {
	if (sasl_randcreate(&rpool) != SASL_OK) fatal("sasl_randcreate failed");
	sasl_randfree(&rpool);
    }

    /* try seeding w/o calling rand_create first */
    rpool = NULL;
    sasl_randseed(rpool, "seed", 4);

    /* try seeding with bad values */
    sasl_randcreate(&rpool);
    sasl_randseed(rpool, "seed", 0);
    sasl_randseed(rpool, NULL, 0);
    sasl_randseed(rpool, NULL, 4);    
    sasl_randfree(&rpool);

    /* try churning with bad values */
    sasl_randcreate(&rpool);
    sasl_churn(rpool, "seed", 0);
    sasl_churn(rpool, NULL, 0);
    sasl_churn(rpool, NULL, 4);    
    sasl_randfree(&rpool);

    /* try seeding with a lot of crap */
    sasl_randcreate(&rpool);
    
    for (lup=0;lup<(int) sizeof(buf);lup++)
    {
	buf[lup] = (char) (rand() % 256);	
    }
    sasl_randseed(rpool, buf, sizeof(buf));
    sasl_churn(rpool, buf, sizeof(buf));

    sasl_randfree(&rpool);
}

/*
 * Test SASL base64 conversion routines
 */

void test_64(void)
{
    char orig[4096];
    char enc[8192];
    unsigned encsize;
    int lup;

    /* make random crap and see if enc->dec produces same as original */
    for (lup=0;lup<(int) sizeof(orig);lup++)
	orig[lup] = (char) (rand() % 256);
    
    if (sasl_encode64(orig, sizeof(orig), enc, sizeof(enc), &encsize)!=SASL_OK) 
	fatal("encode64 failed when we didn't expect it to");

    if (sasl_decode64(enc, encsize, enc, 8192, &encsize)!=SASL_OK)
	fatal("decode64 failed when we didn't expect it to");
    
    if (encsize != sizeof(orig)) fatal("Now has different size");
    
    for (lup=0;lup<(int) sizeof(orig);lup++)
	if (enc[lup] != orig[lup])
	    fatal("enc64->dec64 doesn't match");

    /* try to get a SASL_BUFOVER */
    
    if (sasl_encode64(orig, sizeof(orig)-1, enc, 10, &encsize)!=SASL_BUFOVER)
	fatal("Expected SASL_BUFOVER");


    /* pass some bad params */
    if (sasl_encode64(NULL, 10, enc, sizeof(enc), &encsize)==SASL_OK)
	fatal("Said ok to null data");

    if (sasl_encode64(orig, sizeof(orig), enc, sizeof(enc), NULL)!=SASL_OK)
	fatal("Didn't allow null return size");

    /* New tests in 2.1.22 */
    for (lup=0;lup<(int) sizeof(orig);lup++) {
	enc[lup] = 'A';
    }

    if (sasl_decode64(enc, 3, orig, 8192, &encsize) != SASL_CONTINUE)
	fatal("decode64 succeded on a 3 byte buffer when it shouldn't have");

    enc[3] = '\r';
    enc[4] = '\n';

    if (sasl_decode64(enc, 4, orig, 8192, &encsize) == SASL_OK)
	fatal("decode64 succeded on a 4 byte buffer with a bare CR");

    if (sasl_decode64(enc, 5, orig, 8192, &encsize) == SASL_OK)
	fatal("decode64 succeded on a 5 byte buffer with CRLF");

    enc[2] = '=';
    enc[3] = '=';
    enc[4] = '=';

    if (sasl_decode64(enc, 4, orig, 8192, &encsize) != SASL_OK)
	fatal("decode64 failed on a 4 byte buffer with a terminating =");

    if (sasl_decode64(enc, 5, orig, 8192, &encsize) != SASL_BADPROT)
	fatal("decode64 did not return SASL_CONTINUE on a 5 byte buffer with a terminating =");

    /* Test for invalid character after the terminating '=' */
    enc[3] = '*';

    if (sasl_decode64(enc, 4, orig, 8192, &encsize) == SASL_OK)
	fatal("decode64 failed on a 4 byte buffer with invalid character a terminating =");

    /* Test for '=' in the middle of an encoded string */
    enc[3] = 'B';

    if (sasl_decode64(enc, 4, orig, 8192, &encsize) == SASL_OK)
	fatal("decode64 succeed on a 4 byte buffer with a data after a terminating =");

    if (sasl_decode64(enc, 0, orig, 8192, &encsize) != SASL_OK)
	fatal("decode64 should have succeeded on an empty buffer");
}

/* This isn't complete, but then, what in the testsuite is? */
void test_props(void) 
{
    int result;
    struct propval foobar[3];
    struct propctx *ctx, *dupctx;

    const char *requests[] = {
	"userPassword",
        "userName",
	"homeDirectory",
        "uidNumber",
        "gidNumber",
        NULL
    };

    const char *more_requests[] = {
	"a",
	"b",
	"c",
	"defghijklmnop",
	NULL
    };

    const char *short_requests[] = {
	"userPassword",
	"userName",
	"BAD",
	NULL
    };

    ctx = prop_new(2);
    if(!ctx) {
	fatal("no new prop context");
    }

    if(prop_request(NULL, requests) == SASL_OK)
	fatal("prop_request w/NULL context succeeded");
    if(prop_request(ctx, NULL) == SASL_OK)
	fatal("prop_request w/NULL request list succeeded");
    
    result = prop_request(ctx, requests);
    if(result != SASL_OK)
	fatal("prop request failed");

    /* set some values */
    prop_set(ctx, "uidNumber", really_long_string, 0);
    prop_set(ctx, "userPassword", "pw1", 0);
    prop_set(ctx, "userPassword", "pw2", 0);
    prop_set(ctx, "userName", "rjs3", 0);
    prop_set(ctx, NULL, "tmartin", 0);

    /* and request some more (this resets values) */
    prop_request(ctx, more_requests);

    /* and set some more... */
    prop_set(ctx, "c", really_long_string, 0);
    prop_set(ctx, "b", really_long_string, 0);
    prop_set(ctx, "userPassword", "pw1b", 0);
    prop_set(ctx, "userPassword", "pw2b", 0);
    prop_set(ctx, "userName", "rjs3b", 0);
    prop_set(ctx, NULL, "tmartinagain", 0);

    if(prop_set(ctx, "gah", "ack", 0) == SASL_OK) {
	printf("setting bad property name succeeded\n");
	exit(1);
    }

    result = prop_getnames(ctx, short_requests, foobar);
    if(result < 0)
	fatal("prop_getnames failed");

    if(strcmp(foobar[0].name, short_requests[0]))
	fatal("prop_getnames item 0 wrong name");
    if(strcmp(foobar[1].name, short_requests[1]))
	fatal("prop_getnames item 1 wrong name");
    if(foobar[2].name)
	fatal("prop_getnames returned an item 2");

    if(strcmp(foobar[0].values[0], "pw1b"))
	fatal("prop_getnames item 1a wrong value");
    if(strcmp(foobar[0].values[1], "pw2b"))
	fatal("prop_getnames item 1b wrong value");
    if(strcmp(foobar[1].values[0], "rjs3b"))
	fatal("prop_getnames item 2a wrong value");
    if(strcmp(foobar[1].values[1], "tmartinagain"))
	fatal("prop_getnames item 2b wrong value");

    result = prop_dup(ctx, &dupctx);
    if(result != SASL_OK)
	fatal("could not duplicate");

    prop_clear(ctx, 1);
    
    result = prop_getnames(ctx, short_requests, foobar);
    if(result < 0)
	fatal("prop_getnames failed second time");

    if(foobar[0].name)
	fatal("it appears that prop_clear failed");
    
    result = prop_getnames(dupctx, short_requests, foobar);
    if(result < 0)
	fatal("prop_getnames failed second time");

    if(!foobar[0].name)
	fatal("prop_clear appears to have affected dup'd context");
    
    prop_clear(dupctx, 0);

    result = prop_getnames(dupctx, short_requests, foobar);
    if(result < 0)
	fatal("prop_getnames failed second time");

    if(!foobar[0].name || strcmp(foobar[0].name, short_requests[0]))
	fatal("prop_clear appears to have cleared too much");

    prop_dispose(&ctx);
    prop_dispose(&dupctx);
    if(ctx != NULL)
	fatal("ctx not null after prop_dispose");
}

void interaction (int id, const char *prompt,
		  const char **tresult, unsigned int *tlen)
{
    if (id==SASL_CB_PASS) {
	*tresult=(char *) password;
    } else if (id==SASL_CB_USER && proxyflag == 0) {
	*tresult=(char *) username;
    } else if (id==SASL_CB_USER && proxyflag == 1) {
	*tresult=(char *) proxyasname;
    } else if (id==SASL_CB_AUTHNAME) {
	*tresult=(char *) authname;
    } else if ((id==SASL_CB_GETREALM)) {
	*tresult=(char *) myhostname;
    } else {
	size_t c;
	
	printf("%s: ",prompt);
	fgets(other_result, sizeof(other_result) - 1, stdin);
	c = strlen(other_result);
	other_result[c - 1] = '\0';
	*tresult=other_result;
    }

    *tlen = (unsigned int) strlen(*tresult);
}

void fillin_correctly(sasl_interact_t *tlist)
{
  while (tlist->id!=SASL_CB_LIST_END)
  {
    interaction(tlist->id, tlist->prompt,
		(void *) &(tlist->result), 
		&(tlist->len));
    tlist++;
  }

}

const sasl_security_properties_t security_props = {
    0,
    256,
    8192,
    0,
    NULL,
    NULL	    
};

void set_properties(sasl_conn_t *conn, const sasl_security_properties_t *props)
{
    if(!props) {
	if (sasl_setprop(conn, SASL_SEC_PROPS, &security_props) != SASL_OK)
	    fatal("sasl_setprop() failed - default properties");
    } else {
       	if (sasl_setprop(conn, SASL_SEC_PROPS, props) != SASL_OK)
	    fatal("sasl_setprop() failed");
    }

    if (sasl_setprop(conn, SASL_AUTH_EXTERNAL, authname)!=SASL_OK)
	fatal("sasl_setprop(SASL_AUTH_EXTERNAL) failed");
}

/*
 * This corrupts the string for us
 */
void corrupt(corrupt_type_t type, char *in, int inlen,
	     char **out, unsigned *outlen)
{
    unsigned lup;
    

    switch (type)
	{
	case NOTHING:
	    *out = in;
	    *outlen = inlen;
	    break;
	case ONEBYTE_RANDOM: /* corrupt one byte */

	    if (inlen>0)
		in[ (rand() % inlen) ] = (char) (rand() % 256);

	    *out = in;
	    *outlen = inlen;

	    break;
	case ONEBYTE_NULL:
	    if (inlen>0)
		in[ (rand() % inlen) ] = '\0';

	    *out = in;
	    *outlen = inlen;
	    break;
	case ONEBYTE_QUOTES:
	    if (inlen>0)
		in[ (rand() % inlen) ] = '"';

	    *out = in;
	    *outlen = inlen;
	    break;
	case ONLY_ONE_BYTE:
	    *out = (char *) malloc(1);
	    (*out)[0] = (char) (rand() % 256);
	    *outlen = 1;
	    break;

	case ADDSOME:
	    *outlen = inlen+ (rand() % 100);
	    *out = (char *) malloc(*outlen);
	    memcpy( *out, in, inlen);
	    
	    for (lup=inlen;lup<*outlen;lup++)
		(*out)[lup] = (char) (rand() %256);

	    break;

	case SHORTEN:
	    if (inlen > 0)
	    {
		*outlen = 0;
		while(*outlen == 0)
		    *outlen = (rand() % inlen);
		*out = (char *) malloc(*outlen);
		memcpy(*out, in, *outlen);
	    } else {
		*outlen = inlen;
		*out = in;
	    }
	    break;
	case REASONABLE_RANDOM:
	    *outlen = inlen;
	    if(*outlen != 0)
		*out = (char *) malloc(*outlen);
	    else
		*out = malloc(1);

	    for (lup=0;lup<*outlen;lup++)
		(*out)[lup] = (char) (rand() % 256);

	    break;
	case REALLYBIG:
	    *outlen = rand() % 50000;
	    *out = (char *) malloc( *outlen);
	    
	    for (lup=0;lup<*outlen;lup++)
		(*out)[lup] = (char) (rand() % 256);
	    
	    break;
	case NEGATIVE_LENGTH:

	    *out = in;
	    if (inlen == 0) inlen = 10;
	    *outlen = -1 * (rand() % inlen);
	    
	    break;
	default:
	    fatal("Invalid corruption type");
	    break;
	}
}

void sendbadsecond(char *mech, void *rock)
{
    int result, need_another_client = 0;
    sasl_conn_t *saslconn;
    sasl_conn_t *clientconn;
    const char *out, *dec, *out2;
    char *tmp;
    unsigned outlen, declen, outlen2;
    sasl_interact_t *client_interact=NULL;
    const char *mechusing;
    const char *service = "rcmd";
    int mystep = 0; /* what step in the authentication are we on */
    int mayfail = 0; /* we did some corruption earlier so it's likely to fail now */
    
    tosend_t *send = (tosend_t *)rock;

    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];
    int reauth = 1;

    printf("%s --> start\n",mech);
    
    if (strncmp(mech,"GSS",3)==0) service = gssapi_service;

    if (sasl_client_init(client_interactions)!=SASL_OK) fatal("Unable to init client");

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK) fatal("unable to init server");

    if ((hp = gethostbyname(myhostname)) == NULL) {
	perror("gethostbyname");
	fatal("can't gethostbyname");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

 reauth: /* loop back for reauth testing */
    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 23);

    /* client new connection */
    if (sasl_client_new(service,
			myhostname,
			buf, buf, NULL,
			0,
			&clientconn)!= SASL_OK) fatal("sasl_client_new() failure");

    set_properties(clientconn, NULL);

    if (sasl_server_new(service, myhostname, NULL,
			buf, buf, NULL, 0, 
			&saslconn) != SASL_OK) {
	fatal("can't sasl_server_new");
    }
    set_properties(saslconn, NULL);

    do {
	result = sasl_client_start(clientconn, mech,
				   &client_interact,
				   &out, &outlen,
				   &mechusing);

	if (result == SASL_INTERACT) fillin_correctly(client_interact);
	else if(result == SASL_CONTINUE) need_another_client = 1;
	else if(result == SASL_OK) need_another_client = 0;
    } while (result == SASL_INTERACT);
			       
    if (result < 0)
    {
	printf("%s - \n",sasl_errdetail(clientconn));
	fatal("sasl_client_start() error");
    }

    if (mystep == send->step && outlen)
    {
	memcpy(buf, out, outlen);
	corrupt(send->type, buf, outlen, &tmp, &outlen);
	out = tmp;
	mayfail = 1;
    }

    result = sasl_server_start(saslconn,
			       mech,
			       out,
			       outlen,
			       &out,
			       &outlen);

    if (mayfail)
    {
	if (result >= SASL_OK)
	    printf("WARNING: We did a corruption but it still worked\n");
	else {
	    goto done;
	}
    } else {
	if (result < 0) 
	{
	    printf("%s\n",sasl_errstring(result,NULL,NULL));
	    fatal("sasl_server_start() error");
	}
    }
    mystep++;

    while (result == SASL_CONTINUE) {

	if (mystep == send->step)
	{
	    memcpy(buf,out,outlen);
	    corrupt(send->type, buf, outlen, &tmp, &outlen);
	    out = tmp;
	    mayfail = 1;
	}

	do {
	    result = sasl_client_step(clientconn,
				      out, outlen,
				      &client_interact,
				      &out2, &outlen2);
	    
	    if (result == SASL_INTERACT)
		fillin_correctly(client_interact);
	    else if (result == SASL_CONTINUE)
		need_another_client = 1;
	    else if (result == SASL_OK)
		need_another_client = 0;
	} while (result == SASL_INTERACT);

	if (mayfail == 1)
	{
	    if (result >= 0)
		printf("WARNING: We did a corruption but it still worked\n");
	    else {
		goto done;
	    }
	} else {
	    if (result < 0) 
	    {
		printf("%s\n",sasl_errstring(result,NULL,NULL));
		fatal("sasl_client_step() error");
	    }
	}
	out=out2;
	outlen=outlen2;
	mystep++;

	if (mystep == send->step)
	{
	    memcpy(buf, out, outlen);
	    corrupt(send->type, buf, outlen, &tmp, &outlen);
	    out = tmp;
	    mayfail = 1;
	}

	result = sasl_server_step(saslconn,
				  out,
				  outlen,
				  &out,
				  &outlen);
	
	if (mayfail == 1)
	{
	    if (result >= 0)
		printf("WARNING: We did a corruption but it still worked\n");
	    else {
		goto done;
	    }
	} else {
	    if (result < 0) 
	    {
		printf("%s\n",sasl_errstring(result,NULL,NULL));
		fatal("sasl_server_step() error");
	    }
	}
	mystep++;

    }

    if(need_another_client) {
	result = sasl_client_step(clientconn,
				  out, outlen,
				  &client_interact,
				  &out2, &outlen2);
	if(result != SASL_OK)
	    fatal("client was not ok on last server step");
    }
    
    if (reauth) {
 	sasl_dispose(&clientconn);
 	sasl_dispose(&saslconn);
 
 	reauth = 0;
 	goto reauth;
    }

    /* client to server */
    result = sasl_encode(clientconn, CLIENT_TO_SERVER,
			 (unsigned) strlen(CLIENT_TO_SERVER), &out, &outlen);
    if (result != SASL_OK) fatal("Error encoding");

    if (mystep == send->step)
    {
	memcpy(buf, out, outlen);
	corrupt(send->type, buf, outlen, &tmp, &outlen);
	out = tmp;
	mayfail = 1;
    }

    result = sasl_decode(saslconn, out, outlen, &dec, &declen);

    if (mayfail == 1)
    {
	if (result >= 0)
	    printf("WARNING: We did a corruption but it still worked\n");
	else {
	    goto done;
	}
    } else {
	if (result < 0) 
	{
	    printf("%s\n",sasl_errstring(result,NULL,NULL));
	    fatal("sasl_decode() failure");
	}
    }
    mystep++;

    /* no need to do other direction since symetric */

    /* Just verify oparams */
    if(sasl_getprop(saslconn, SASL_USERNAME, (const void **)&out)
       != SASL_OK) {
	fatal("couldn't get server username");
	goto done;
    }
    if(sasl_getprop(clientconn, SASL_USERNAME, (const void **)&out2)
       != SASL_OK) {
	fatal("couldn't get client username");
	goto done;
    }
    if(strcmp(out,out2)) {
	fatal("client username does not match server username");
	goto done;
    }

    printf("%s --> %s (as %s)\n",mech,sasl_errstring(result,NULL,NULL),out);

 done:
    sasl_dispose(&clientconn);
    sasl_dispose(&saslconn);
    sasl_done();
}

/* Authenticate two sasl_conn_t's to eachother, validly.
 * used to test the security layer */
int doauth(char *mech, sasl_conn_t **server_conn, sasl_conn_t **client_conn,
           const sasl_security_properties_t *props,
	   sasl_callback_t *c_calls, int fail_ok)
{
    int result, need_another_client = 0;
    sasl_conn_t *saslconn;
    sasl_conn_t *clientconn;
    const char *out, *out2;
    unsigned outlen, outlen2;
    sasl_interact_t *client_interact=NULL;
    const char *mechusing;
    const char *service = "rcmd";
    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];

    if(!server_conn || !client_conn) return SASL_BADPARAM;
    
    if (strncmp(mech,"GSS",3)==0) service = gssapi_service;

    result = sasl_client_init((c_calls ? c_calls : client_interactions));
    if (result!=SASL_OK) {
	if(!fail_ok) fatal("Unable to init client");
	else return result;
    }

    if(proxyflag == 0) {
        result = sasl_server_init(goodsasl_cb,"TestSuite");
    } else {
        result = sasl_server_init(goodsaslproxy_cb,"TestSuite");
    }
    if(result != SASL_OK) {
       if(!fail_ok) fatal("unable to init server");
       else return result;
    }
	    

    if ((hp = gethostbyname(myhostname)) == NULL) {
	perror("gethostbyname");
	if(!fail_ok) fatal("can't gethostbyname");
	else return SASL_FAIL;
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    /* client new connection */
    result = sasl_client_new(service,
			     myhostname,
			     buf, buf, NULL,
			     0,
			     &clientconn);
    if(result != SASL_OK) {
	if(!fail_ok) fatal("sasl_client_new() failure");
	else return result;
    }

    /* Set the security properties */
    set_properties(clientconn, props);

    result = sasl_server_new(service, myhostname, NULL,
			     buf, buf, NULL, 0, 
			     &saslconn);
    if(result != SASL_OK) {
	if(!fail_ok) fatal("can't sasl_server_new");
	else return result;
    }
    set_properties(saslconn, props);

    do {
	result = sasl_client_start(clientconn, mech,
				   &client_interact,
				   &out, &outlen,
				   &mechusing);

	if (result == SASL_INTERACT) fillin_correctly(client_interact);
	else if(result == SASL_CONTINUE) need_another_client = 1;
	else if(result == SASL_OK) need_another_client = 0;
    } while (result == SASL_INTERACT);
			       
    if (result < 0)
    {
	if(!fail_ok) fatal("sasl_client_start() error");
	else return result;
    }

    result = sasl_server_start(saslconn,
			       mech,
			       out,
			       outlen,
			       &out,
			       &outlen);

    if (result < 0) 
    {
	if(!fail_ok) fatal("sasl_server_start() error");
	else return result;
    }

    while (result == SASL_CONTINUE) {
	do {
	    result = sasl_client_step(clientconn,
				      out, outlen,
				      &client_interact,
				      &out2, &outlen2);
	    
	    if (result == SASL_INTERACT)
		fillin_correctly(client_interact);
	    else if (result == SASL_CONTINUE)
		need_another_client = 1;
	    else if (result == SASL_OK)
		need_another_client = 0;
	} while (result == SASL_INTERACT);

	if (result < 0) 
	{
	    if(!fail_ok) fatal("sasl_client_step() error");
	    else return result;
	}

	out=out2;
	outlen=outlen2;

	result = sasl_server_step(saslconn,
				  out,
				  outlen,
				  &out,
				  &outlen);
	
	if (result < 0) 
	{
	    if(!fail_ok) fatal("sasl_server_step() error");
	    else return result;
	}

    }

    if(need_another_client) {
	if(!fail_ok) fatal("server-last not allowed, but need another client call");
	else return SASL_BADPROT;
    }

    *server_conn = saslconn;
    *client_conn = clientconn;
    
    return SASL_OK;
}

/* Authenticate two sasl_conn_t's to eachother, validly.
 * without allowing client-send-first */
int doauth_noclientfirst(char *mech, sasl_conn_t **server_conn,
			 sasl_conn_t **client_conn,
			 const sasl_security_properties_t *props,
			 sasl_callback_t *c_calls)
{
    int result, need_another_client = 0;
    sasl_conn_t *saslconn;
    sasl_conn_t *clientconn;
    const char *out, *out2;
    unsigned outlen, outlen2;
    sasl_interact_t *client_interact=NULL;
    const char *mechusing;
    const char *service = "rcmd";

    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];

    if(!server_conn || !client_conn) return SASL_BADPARAM;
    
    if (strncmp(mech,"GSS",3)==0) service = gssapi_service;


    if (sasl_client_init((c_calls ? c_calls : client_interactions))!=SASL_OK)
	fatal("Unable to init client");

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK)
        fatal("unable to init server");  

    if ((hp = gethostbyname(myhostname)) == NULL) {
	perror("gethostbyname");
	fatal("can't gethostbyname");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    /* client new connection */
    if (sasl_client_new(service,
			myhostname,
			buf, buf, NULL,
			0,
			&clientconn)!= SASL_OK) fatal("sasl_client_new() failure");

    /* Set the security properties */
    set_properties(clientconn, props);

    if (sasl_server_new(service, myhostname, NULL,
			buf, buf, NULL, 0, 
			&saslconn) != SASL_OK) {
	fatal("can't sasl_server_new");
    }
    set_properties(saslconn, props);

    do {
	result = sasl_client_start(clientconn, mech,
				   &client_interact,
				   NULL, NULL,
				   &mechusing);

	if (result == SASL_INTERACT) fillin_correctly(client_interact);
	else if(result == SASL_CONTINUE) need_another_client = 1;
	else if(result == SASL_OK) need_another_client = 0;
    } while (result == SASL_INTERACT);

    if (result < 0)
    {
	fatal("sasl_client_start() error");
    }	

    result = sasl_server_start(saslconn,
			       mech,
			       NULL,
			       0,
			       &out,
			       &outlen);

    if (result < 0) 
    {
	fatal("sasl_server_start() error");
    }

    while (result == SASL_CONTINUE) {
	do {
	    result = sasl_client_step(clientconn,
				      out, outlen,
				      &client_interact,
				      &out2, &outlen2);
	    
	    if (result == SASL_INTERACT)
		fillin_correctly(client_interact);
	    else if (result == SASL_CONTINUE)
		need_another_client = 1;
	    else if (result == SASL_OK)
		need_another_client = 0;
	} while (result == SASL_INTERACT);

	if (result < 0) 
	{
	    fatal("sasl_client_step() error");
	}

	out=out2;
	outlen=outlen2;

	result = sasl_server_step(saslconn,
				  out,
				  outlen,
				  &out,
				  &outlen);
	
	if (result < 0) 
	{
	    fatal("sasl_server_step() error");
	}

    }

    if(need_another_client) {
	fatal("server-last not allowed, but need another client call");
    }

    *server_conn = saslconn;
    *client_conn = clientconn;
    
    return SASL_OK;
}

/* Authenticate two sasl_conn_t's to eachother, validly.
 * used to test the security layer */
int doauth_serverlast(char *mech, sasl_conn_t **server_conn,
		      sasl_conn_t **client_conn,
		      const sasl_security_properties_t *props,
		      sasl_callback_t *c_calls)
{
    int result, need_another_client = 0;
    sasl_conn_t *saslconn;
    sasl_conn_t *clientconn;
    const char *out, *out2;
    unsigned outlen, outlen2;
    sasl_interact_t *client_interact=NULL;
    const char *mechusing;
    const char *service = "rcmd";

    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];

    if(!server_conn || !client_conn) return SASL_BADPARAM;
    
    if (strncmp(mech,"GSS",3)==0) service = gssapi_service;

    if (sasl_client_init((c_calls ? c_calls : client_interactions))!=SASL_OK)
	fatal("unable to init client");

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK)
	fatal("unable to init server");

    if ((hp = gethostbyname(myhostname)) == NULL) {
	perror("gethostbyname");
	fatal("can't gethostbyname");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    /* client new connection */
    if (sasl_client_new(service,
			myhostname,
			buf, buf, NULL,
			SASL_SUCCESS_DATA,
			&clientconn)!= SASL_OK) fatal("sasl_client_new() failure");

    /* Set the security properties */
    set_properties(clientconn, props);

    if (sasl_server_new(service, myhostname, NULL,
			buf, buf, NULL, SASL_SUCCESS_DATA, 
			&saslconn) != SASL_OK) {
	fatal("can't sasl_server_new");
    }
    set_properties(saslconn, props);

    do {
	result = sasl_client_start(clientconn, mech,
				   &client_interact,
				   &out, &outlen,
				   &mechusing);

	if (result == SASL_INTERACT) fillin_correctly(client_interact);
	else if(result == SASL_CONTINUE) need_another_client = 1;
	else if(result == SASL_OK) need_another_client = 0;
    } while (result == SASL_INTERACT);
	

    if (result < 0)
    {
	fatal("sasl_client_start() error");
    }			       

    result = sasl_server_start(saslconn,
			       mech,
			       out,
			       outlen,
			       &out,
			       &outlen);

    if (result < 0) 
    {
	fatal("sasl_server_start() error");
    }

    while (result == SASL_CONTINUE) {
	do {
	    result = sasl_client_step(clientconn,
				      out, outlen,
				      &client_interact,
				      &out2, &outlen2);
	    
	    if (result == SASL_INTERACT)
		fillin_correctly(client_interact);
	    else if (result == SASL_CONTINUE)
		need_another_client = 1;
	    else if (result == SASL_OK)
		need_another_client = 0;
	} while (result == SASL_INTERACT);

	if (result < 0) 
	{
	    fatal("sasl_client_step() error");
	}

	out=out2;
	outlen=outlen2;

	result = sasl_server_step(saslconn,
				  out,
				  outlen,
				  &out,
				  &outlen);
	
	if (result < 0) 
	{
	    fatal("sasl_server_step() error");
	}

    }

    if(need_another_client) {
	result = sasl_client_step(clientconn,
				  out, outlen,
				  &client_interact,
				  &out2, &outlen2);
	if(result != SASL_OK)
	    fatal("client was not ok on last server step");
    }

    *server_conn = saslconn;
    *client_conn = clientconn;
    
    return SASL_OK;
}

/* Authenticate two sasl_conn_t's to eachother, validly.
 * without allowing client-send-first */
int doauth_noclientfirst_andserverlast(char *mech, sasl_conn_t **server_conn,
				       sasl_conn_t **client_conn,
				       const sasl_security_properties_t *props,
				       sasl_callback_t *c_calls)
{
    int result, need_another_client = 0;
    sasl_conn_t *saslconn;
    sasl_conn_t *clientconn;
    const char *out, *out2;
    unsigned outlen, outlen2;
    sasl_interact_t *client_interact=NULL;
    const char *mechusing;
    const char *service = "rcmd";

    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];

    if(!server_conn || !client_conn) return SASL_BADPARAM;
    
    if (strncmp(mech,"GSS",3)==0) service = gssapi_service;

    if (sasl_client_init((c_calls ? c_calls : client_interactions))!=SASL_OK)
	fatal("unable to init client");

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK)
	fatal("unable to init server");

    if ((hp = gethostbyname(myhostname)) == NULL) {
	perror("gethostbyname");
	fatal("can't gethostbyname");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    /* client new connection */
    if (sasl_client_new(service,
			myhostname,
			buf, buf, NULL,
			SASL_SUCCESS_DATA,
			&clientconn)!= SASL_OK) fatal("sasl_client_new() failure");

    /* Set the security properties */
    set_properties(clientconn, props);

    if (sasl_server_new(service, myhostname, NULL,
			buf, buf, NULL, SASL_SUCCESS_DATA, 
			&saslconn) != SASL_OK) {
	fatal("can't sasl_server_new");
    }
    set_properties(saslconn, props);

    do {
	result = sasl_client_start(clientconn, mech,
				   &client_interact,
				   NULL, NULL,
				   &mechusing);

	if (result == SASL_INTERACT) fillin_correctly(client_interact);
	else if(result == SASL_CONTINUE) need_another_client = 1;
	else if(result == SASL_OK) need_another_client = 0;
    } while (result == SASL_INTERACT);

    if (result < 0)
    {
	fatal("sasl_client_start() error");
    }				       

    result = sasl_server_start(saslconn,
			       mech,
			       NULL,
			       0,
			       &out,
			       &outlen);

    if (result < 0) 
    {
	fatal("sasl_server_start() error");
    }

    while (result == SASL_CONTINUE) {
	do {
	    result = sasl_client_step(clientconn,
				      out, outlen,
				      &client_interact,
				      &out2, &outlen2);
	    
	    if (result == SASL_INTERACT)
		fillin_correctly(client_interact);
	    else if (result == SASL_CONTINUE)
		need_another_client = 1;
	    else if (result == SASL_OK)
		need_another_client = 0;
	} while (result == SASL_INTERACT);

	if (result < 0) 
	{
	    fatal("sasl_client_step() error");
	}

	out=out2;
	outlen=outlen2;

	result = sasl_server_step(saslconn,
				  out,
				  outlen,
				  &out,
				  &outlen);
	
	if (result < 0) 
	{
	    fatal("sasl_server_step() error");
	}

    }

    if(need_another_client) {
	result = sasl_client_step(clientconn,
				  out, outlen,
				  &client_interact,
				  &out2, &outlen2);
	if(result != SASL_OK)
	    fatal("client was not ok on last server step");
    }

    *server_conn = saslconn;
    *client_conn = clientconn;
    
    return SASL_OK;
}

void cleanup_auth(sasl_conn_t **client, sasl_conn_t **server) 
{
    sasl_dispose(client);
    sasl_dispose(server);
    sasl_done();
}


const sasl_security_properties_t int_only = {
    0,
    1,
    8192,
    0,
    NULL,
    NULL	    
};

const sasl_security_properties_t force_des = {
    0,
    55,
    8192,
    0,
    NULL,
    NULL	    
};

const sasl_security_properties_t force_rc4_56 = {
    0,
    56,
    8192,
    0,
    NULL,
    NULL	    
};

const sasl_security_properties_t force_3des = {
    0,
    112,
    8192,
    0,
    NULL,
    NULL	    
};


const sasl_security_properties_t no_int = {
    2,
    256,
    8192,
    0,
    NULL,
    NULL	    
};

const sasl_security_properties_t disable_seclayer = {
    0,
    256,
    0,
    0,
    NULL,
    NULL	    
};

void do_proxypolicy_test(char *mech, void *rock __attribute__((unused))) 
{
    sasl_conn_t *sconn, *cconn;
    const char *username;
    
    printf("%s --> start\n", mech);
    proxyflag = 1;
    if(doauth(mech, &sconn, &cconn, &security_props, NULL, 0) != SASL_OK) {
	fatal("doauth failed in do_proxypolicy_test");
    }

    if(sasl_getprop(sconn, SASL_USERNAME, (const void **)&username) != SASL_OK)
    {
	fatal("getprop failed in do_proxypolicy_test");
    }
    
    if(strcmp(username, proxyasname)) {
	printf("Warning: Server Authorization Name != proxyasuser\n");
    }

    cleanup_auth(&cconn, &sconn);
    proxyflag = 0;
    printf("%s --> successful result\n",mech);
}

void test_clientfirst(char *mech, void *rock) 
{
    sasl_conn_t *sconn, *cconn;
    tosend_t *tosend = (tosend_t *)rock;
    
    printf("%s --> start\n", mech);

    /* Basic crash-tests (none should cause a crash): */
    if(doauth(mech, &sconn, &cconn, &security_props, tosend->client_callbacks,
	      0) != SASL_OK) {
	fatal("doauth failed in test_clientfirst");
    }

    cleanup_auth(&cconn, &sconn);

    printf("%s --> successful result\n", mech);
}

void test_noclientfirst(char *mech, void *rock) 
{
    sasl_conn_t *sconn, *cconn;
    tosend_t *tosend = (tosend_t *)rock;
    
    printf("%s --> start\n", mech);

    /* Basic crash-tests (none should cause a crash): */
    if(doauth_noclientfirst(mech, &sconn, &cconn, &security_props,
	tosend->client_callbacks) != SASL_OK) {
	fatal("doauth failed in test_noclientfirst");
    }

    cleanup_auth(&cconn, &sconn);

    printf("%s --> successful result\n", mech);
}

void test_serverlast(char *mech, void *rock) 
{
    sasl_conn_t *sconn, *cconn;
    tosend_t *tosend = (tosend_t *)rock;
    
    printf("%s --> start\n", mech);

    /* Basic crash-tests (none should cause a crash): */
    if(doauth_serverlast(mech, &sconn, &cconn, &security_props,
			 tosend->client_callbacks) != SASL_OK) {
	fatal("doauth failed in test_serverlast");
    }

    cleanup_auth(&cconn, &sconn);

    printf("%s --> successful result\n", mech);
}


void test_noclientfirst_andserverlast(char *mech, void *rock) 
{
    sasl_conn_t *sconn, *cconn;
    tosend_t *tosend = (tosend_t *)rock;
    
    printf("%s --> start\n", mech);

    /* Basic crash-tests (none should cause a crash): */
    if(doauth_noclientfirst_andserverlast(mech, &sconn, &cconn,
					  &security_props,
					  tosend->client_callbacks)
       != SASL_OK) {
	fatal("doauth failed in test_noclientfirst_andserverlast");
    }

    cleanup_auth(&cconn, &sconn);

    printf("%s --> successful result\n", mech);
}

void testseclayer(char *mech, void *rock __attribute__((unused))) 
{
    sasl_conn_t *sconn, *cconn;
    int result;
    char buf[8192], buf2[8192];
    const char *txstring = "THIS IS A TEST";
    const char *out, *out2;
    char *tmp;
    const sasl_security_properties_t *test_props[7] =
                                          { &security_props,
					    &force_3des,
					    &force_rc4_56,
					    &force_des,
					    &int_only,
					    &no_int,
					    &disable_seclayer };
    const unsigned num_properties = 7;
    unsigned i;
    const sasl_ssf_t *this_ssf;
    unsigned outlen = 0, outlen2 = 0, totlen = 0;
    
    printf("%s --> security layer start\n", mech);

    for(i=0; i<num_properties; i++) {
        
    /* Basic crash-tests (none should cause a crash): */
    result = doauth(mech, &sconn, &cconn, test_props[i], NULL, 1);
    if(result == SASL_NOMECH && test_props[i]->min_ssf > 0) {
	printf("  Testing SSF: SKIPPED (requested minimum > 0: %d)\n",
	       test_props[i]->min_ssf);
	cleanup_auth(&sconn, &cconn);
	continue;
    } else if(result != SASL_OK) {
	fatal("doauth failed in testseclayer");
    }

    if(sasl_getprop(cconn, SASL_SSF, (const void **)&this_ssf) != SASL_OK) {
	fatal("sasl_getprop in testseclayer");
    }

    if(*this_ssf != 0 && !test_props[i]->maxbufsize) {
	fatal("got nonzero SSF with zero maxbufsize");
    }

    printf("  SUCCESS Testing SSF: %d (requested %d/%d with maxbufsize: %d)\n",
	   (unsigned)(*this_ssf),
	   test_props[i]->min_ssf, test_props[i]->max_ssf,
	   test_props[i]->maxbufsize);

    if(!test_props[i]->maxbufsize) {
	result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			     &out, &outlen);
	if(result == SASL_OK) {
	    fatal("got OK when encoding with zero maxbufsize");
	}
	result = sasl_decode(sconn, "foo", 3, &out, &outlen);
	if(result == SASL_OK) {
	    fatal("got OK when decoding with zero maxbufsize");
	}
	cleanup_auth(&sconn, &cconn);
	continue;
    }
    
    sasl_encode(NULL, txstring, (unsigned) strlen(txstring), &out, &outlen);
    sasl_encode(cconn, NULL, (unsigned) strlen(txstring), &out, &outlen);
    sasl_encode(cconn, txstring, 0, &out, &outlen);
    sasl_encode(cconn, txstring, (unsigned) strlen(txstring), NULL, &outlen);
    sasl_encode(cconn, txstring, (unsigned) strlen(txstring), &out, NULL);
    
    sasl_decode(NULL, txstring, (unsigned) strlen(txstring), &out, &outlen);
    sasl_decode(cconn, NULL, (unsigned) strlen(txstring), &out, &outlen);
    sasl_decode(cconn, txstring, 0, &out, &outlen);
    sasl_decode(cconn, txstring, (unsigned)-1, &out, &outlen);
    sasl_decode(cconn, txstring, (unsigned) strlen(txstring), NULL, &outlen);
    sasl_decode(cconn, txstring, (unsigned) strlen(txstring), &out, NULL);
    
    cleanup_auth(&sconn, &cconn);

    /* Basic I/O Test */
    if(doauth(mech, &sconn, &cconn, test_props[i], NULL, 0) != SASL_OK) {
	fatal("doauth failed in testseclayer");
    }

    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure");
    }

    result = sasl_decode(sconn, out, outlen, &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_decode failure");
    }    
    
    cleanup_auth(&sconn, &cconn);

    /* Split one block and reassemble */
    if(doauth(mech, &sconn, &cconn, test_props[i], NULL, 0) != SASL_OK) {
	fatal("doauth failed in testseclayer");
    }

    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure (2)");
    }

    memcpy(buf, out, 5);
    buf[5] = '\0';
    
    out += 5;

    result = sasl_decode(sconn, buf, 5, &out2, &outlen2);
    if(result != SASL_OK) {
	printf("Failed with: %s\n", sasl_errstring(result, NULL, NULL));
	fatal("sasl_decode failure part 1/2");
    }    

    memset(buf2, 0, 8192);
    if(outlen2) 
        memcpy(buf2, out2, outlen2);

    result = sasl_decode(sconn, out, outlen - 5, &out, &outlen);
    if(result != SASL_OK) {
	fatal("sasl_decode failure part 2/2");
    }

    strcat(buf2, out);
    if(strcmp(buf2, txstring)) {
	printf("Exptected '%s' but got '%s'\n", txstring, buf2);
	fatal("did not get correct string back after 2 sasl_decodes");
    }

    cleanup_auth(&sconn, &cconn);

    /* Combine 2 blocks */
    if(doauth(mech, &sconn, &cconn, test_props[i], NULL, 0) != SASL_OK) {
	fatal("doauth failed in testseclayer");
    }

    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure (3)");
    }

    memcpy(buf, out, outlen);

    tmp = buf + outlen;
    totlen = outlen;
    
    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure (4)");
    }

    memcpy(tmp, out, outlen);
    totlen += outlen;

    result = sasl_decode(sconn, buf, totlen, &out, &outlen);
    if(result != SASL_OK) {
	printf("Failed with: %s\n", sasl_errstring(result, NULL, NULL));
	fatal("sasl_decode failure (2 blocks)");
    }    

    sprintf(buf2, "%s%s", txstring, txstring);

    if(strcmp(out, buf2)) {
	fatal("did not get correct string back (2 blocks)");
    }

    cleanup_auth(&sconn, &cconn);

    /* Combine 2 blocks with 1 split */
    if(doauth(mech, &sconn, &cconn, test_props[i], NULL, 0) != SASL_OK) {
	fatal("doauth failed in testseclayer");
    }

    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out, &outlen);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure (3)");
    }

    memcpy(buf, out, outlen);

    tmp = buf + outlen;

    result = sasl_encode(cconn, txstring, (unsigned) strlen(txstring),
			 &out2, &outlen2);
    if(result != SASL_OK) {
	fatal("basic sasl_encode failure (4)");
    }

    memcpy(tmp, out2, 5);
    tmp[5] = '\0';
    outlen += 5;

    outlen2 -= 5;
    out2 += 5;

    result = sasl_decode(sconn, buf, outlen, &out, &outlen);
    if(result != SASL_OK) {
	printf("Failed with: %s\n", sasl_errstring(result, NULL, NULL));
	fatal("sasl_decode failure 1/2 (2 blocks, 1 split)");
    }    

    memset(buf2, 0, 8192);
    memcpy(buf2, out, outlen);

    tmp = buf2 + outlen;

    result = sasl_decode(sconn, out2, outlen2, &out, &outlen);
    if(result != SASL_OK) {
	printf("Failed with: %s\n", sasl_errstring(result, NULL, NULL));
	fatal("sasl_decode failure 2/2 (2 blocks, 1 split)");
    }

    memcpy(tmp, out, outlen);

    sprintf(buf, "%s%s", txstring, txstring);
    if(strcmp(buf, buf2)) {
	fatal("did not get correct string back (2 blocks, 1 split)");
    }

    cleanup_auth(&sconn, &cconn);
    
    } /* for each properties type we want to test */
     
    printf("%s --> security layer OK\n", mech);
    
}


/*
 * Apply the given function to each machanism 
 */

void foreach_mechanism(foreach_t *func, void *rock)
{
    const char *out;
    char *str, *start;
    sasl_conn_t *saslconn;
    int result;
    struct sockaddr_in addr;
    struct hostent *hp;
    unsigned len;
    char buf[8192];

    /* Get the list of mechanisms */
    sasl_done();

    if (sasl_server_init(emptysasl_cb,"TestSuite")!=SASL_OK)
	fatal("sasl_server_init failed in foreach_mechanism");

    if ((hp = gethostbyname(myhostname)) == NULL) {
        perror("gethostbyname");
        fatal("can't gethostbyname");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    if (sasl_server_new("rcmd", myhostname, NULL,
			buf, buf, NULL, 0,
			&saslconn) != SASL_OK) {
	fatal("sasl_server_new in foreach_mechanism");
    }

    if (sasl_setprop(saslconn, SASL_AUTH_EXTERNAL, authname)!=SASL_OK)
	fatal("sasl_setprop(SASL_AUTH_EXTERNAL) failed");

    result = sasl_listmech(saslconn,
			   NULL,
			   "",
			   "\n",
			   "",
			   &out,
			   &len,
			   NULL);

    if(result != SASL_OK) {
	fatal("sasl_listmech in foreach_mechanism");
    }
    
    memcpy(buf, out, len + 1);

    sasl_dispose(&saslconn);
    sasl_done();

    /* call the function for each mechanism */
    start = str = buf;
    while (*start != '\0')
    {
	while ((*str != '\n') && (*str != '\0'))
	    str++;

	if (*str == '\n')
	{
	    *str = '\0';
	    str++;
	}

	func(start, rock);

	start = str;
    }
}

void test_serverstart()
{
    int result;
    sasl_conn_t *saslconn;
    const char *out;
    unsigned outlen;
    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];

    if (sasl_server_init(emptysasl_cb,"TestSuite")!=SASL_OK)
	fatal("can't sasl_server_init in test_serverstart");

    if ((hp = gethostbyname(myhostname)) == NULL) {
        perror("gethostbyname");
        fatal("can't gethostbyname in test_serverstart");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    if (sasl_server_new("rcmd", myhostname, NULL,
			buf, buf, NULL, 0, 
			&saslconn) != SASL_OK) {
	fatal("can't sasl_server_new in test_serverstart");
    }


    /* Test null connection */
    result = sasl_server_start(NULL,
			       "foobar",
			       NULL,
			       0,
			       NULL,
			       NULL);
    
    if (result == SASL_OK) fatal("Said ok to null sasl_conn_t in sasl_server_start()");

    /* send plausible but invalid mechanism */
    result = sasl_server_start(saslconn,
			       "foobar",
			       NULL,
			       0,
			       &out,
			       &outlen);

    if (result == SASL_OK) fatal("Said ok to invalid mechanism");

    /* send really long and invalid mechanism */
    result = sasl_server_start(saslconn,
			       really_long_string,
			       NULL,
			       0,
			       &out,
			       &outlen);

    if (result == SASL_OK) fatal("Said ok to invalid mechanism");

    sasl_dispose(&saslconn);
    sasl_done();
}

void test_rand_corrupt(unsigned steps) 
{
    unsigned lup;
    tosend_t tosend;
    
    for (lup=0;lup<steps;lup++)
    {
	tosend.type = rand() % CORRUPT_SIZE;
	tosend.step = lup % MAX_STEPS;
	tosend.client_callbacks = NULL;

	printf("RANDOM TEST: (%s in step %d) (%d of %d)\n",corrupt_types[tosend.type],tosend.step,lup+1,steps);
	foreach_mechanism((foreach_t *) &sendbadsecond,&tosend);
    }
}

void test_proxypolicy() 
{
    foreach_mechanism((foreach_t *) &do_proxypolicy_test,NULL);
}

void test_all_corrupt() 
{
    tosend_t tosend;
    tosend.client_callbacks = NULL;

    /* Start just beyond NOTHING */
    for(tosend.type=1; tosend.type<CORRUPT_SIZE; tosend.type++) {
	for(tosend.step=0; tosend.step<MAX_STEPS; tosend.step++) {
	    printf("TEST: %s in step %d:\n", corrupt_types[tosend.type],
		   tosend.step);
	    foreach_mechanism((foreach_t *) &sendbadsecond, &tosend);
	}
    }
}

void test_seclayer() 
{
    foreach_mechanism((foreach_t *) &testseclayer, NULL);
}

void create_ids(void)
{
    sasl_conn_t *saslconn;
    int result;
    struct sockaddr_in addr;
    struct hostent *hp;
    char buf[8192];
#ifdef DO_SASL_CHECKAPOP
    int i;
    const char challenge[] = "<1896.697170952@cyrus.andrew.cmu.edu>";
    MD5_CTX ctx;
    unsigned char digest[16];
    char digeststr[33];
#endif

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK)
	fatal("can't sasl_server_init in create_ids");

    if ((hp = gethostbyname(myhostname)) == NULL) {
        perror("gethostbyname");
        fatal("can't gethostbyname in create_ids");
    }

    addr.sin_family = 0;
    memcpy(&addr.sin_addr, hp->h_addr, hp->h_length);
    addr.sin_port = htons(0);

    sprintf(buf,"%s;%d", inet_ntoa(addr.sin_addr), 0);

    if (sasl_server_new("rcmd", myhostname, NULL,
			buf, buf, NULL, 0,
			&saslconn) != SASL_OK)
	fatal("can't sasl_server_new in create_ids");
    
    /* Try to set password then check it */

    result = sasl_setpass(saslconn, username, password,
			  (unsigned) strlen(password),
			  NULL, 0, SASL_SET_CREATE);
    if (result != SASL_OK) {
	printf("error was %s (%d)\n",sasl_errstring(result,NULL,NULL),result);
	fatal("Error setting password. Do we have write access to sasldb?");
    }    
    
    result = sasl_checkpass(saslconn, username,
			    (unsigned) strlen(username),
			    password, (unsigned) strlen(password));
    if (result != SASL_OK) {
	fprintf(stderr, "%s\n", sasl_errdetail(saslconn));
	fatal("Unable to verify password we just set");
    }
    result = sasl_user_exists(saslconn, "imap", NULL, username);
    if(result != SASL_OK)
	fatal("sasl_user_exists did not find user");

    result = sasl_user_exists(saslconn, "imap", NULL,
			      nonexistant_username);
    if(result == SASL_OK)
	fatal("sasl_user_exists found nonexistant username");

    /* Test sasl_checkapop */
#ifdef DO_SASL_CHECKAPOP
    _sasl_MD5Init(&ctx);
    _sasl_MD5Update(&ctx,(const unsigned char *)challenge,strlen(challenge));
    _sasl_MD5Update(&ctx,(const unsigned char *)password,strlen(password));
    _sasl_MD5Final(digest, &ctx);
                            
    /* convert digest from binary to ASCII hex */
    for (i = 0; i < 16; i++)
      sprintf(digeststr + (i*2), "%02x", digest[i]);

    sprintf(buf, "%s %s", username, digeststr);
    
    result = sasl_checkapop(saslconn,
                            challenge, strlen(challenge),
                            buf, strlen(buf));
    if(result != SASL_OK)
        fatal("Unable to checkapop password we just set");
    /* End checkapop test */
#else /* Just check that checkapop is really turned off */
    if(sasl_checkapop(saslconn, NULL, 0, NULL, 0) == SASL_OK)
	fatal("sasl_checkapop seems to work but was disabled at compile time");
#endif

    /* now delete user and make sure can't find him anymore */
    result = sasl_setpass(saslconn, username, password,
			  (unsigned) strlen(password),
			  NULL, 0, SASL_SET_DISABLE);
    if (result != SASL_OK)
	fatal("Error disabling password. Do we have write access to sasldb?");

    result = sasl_checkpass(saslconn, username,
			    (unsigned) strlen(username),
			    password, (unsigned) strlen(password));
    if (result == SASL_OK) {
	printf("\n  WARNING: sasl_checkpass got SASL_OK after disableing\n");
	printf("           This is generally ok, just an artifact of sasldb\n");
	printf("           being an external verifier\n");
    }

#ifdef DO_SASL_CHECKAPOP
    /* And checkapop... */
    result = sasl_checkapop(saslconn,
                            challenge, strlen(challenge), 
                            buf, strlen(buf));
    if (result == SASL_OK) {
	printf("\n  WARNING: sasl_checkapop got SASL_OK after disableing\n");
	printf("           This is generally ok, just an artifact of sasldb\n");
	printf("           being an external verifier\n");
    }
#endif

    /* try bad params */
    if (sasl_setpass(NULL,username, password,
		     (unsigned) strlen(password),
		     NULL, 0, SASL_SET_CREATE)==SASL_OK)
	fatal("Didn't specify saslconn");
    if (sasl_setpass(saslconn,username, password, 0, NULL, 0, SASL_SET_CREATE)==SASL_OK)
	fatal("Allowed password of zero length");
    if (sasl_setpass(saslconn,username, password,
		     (unsigned) strlen(password), NULL, 0, 43)==SASL_OK)
	fatal("Gave weird code");

#ifndef SASL_NDBM
    if (sasl_setpass(saslconn,really_long_string,
		     password, (unsigned)strlen(password), 
		     NULL, 0, SASL_SET_CREATE)!=SASL_OK)
	fatal("Didn't allow really long username");
#else
    printf("WARNING: skipping sasl_setpass() on really_long_string with NDBM\n");
#endif

    if (sasl_setpass(saslconn,"bob",really_long_string,
		     (unsigned) strlen(really_long_string),NULL, 0,
		     SASL_SET_CREATE)!=SASL_OK)
	fatal("Didn't allow really long password");

    result = sasl_setpass(saslconn,"frank",
			  password, (unsigned) strlen(password), 
			  NULL, 0, SASL_SET_DISABLE);

    if ((result!=SASL_NOUSER) && (result!=SASL_OK))
	{
	    printf("error = %d\n",result);
	    fatal("Disabling non-existant didn't return SASL_NOUSER");
	}
    
    /* Now set the user again (we use for rest of program) */
    result = sasl_setpass(saslconn, username,
			  password, (unsigned) strlen(password),
			  NULL, 0, SASL_SET_CREATE);
    if (result != SASL_OK)
	fatal("Error setting password. Do we have write access to sasldb?");

    /* cleanup */
    sasl_dispose(&saslconn);
    sasl_done();
}

/*
 * Test the checkpass routine
 */

void test_checkpass(void)
{
    sasl_conn_t *saslconn;

    /* try without initializing anything */
    if(sasl_checkpass(NULL,
		      username,
		      (unsigned) strlen(username),
		      password,
		      (unsigned) strlen(password)) != SASL_NOTINIT) {
	fatal("sasl_checkpass() when library not initialized");
    }    

    if (sasl_server_init(goodsasl_cb,"TestSuite")!=SASL_OK)
	fatal("can't sasl_server_init in test_checkpass");

    if (sasl_server_new("rcmd", myhostname,
			NULL, NULL, NULL, NULL, 0, 
			&saslconn) != SASL_OK)
	fatal("can't sasl_server_new in test_checkpass");

    /* make sure works for general case */

    if (sasl_checkpass(saslconn, username, (unsigned) strlen(username),
		       password, (unsigned) strlen(password))!=SASL_OK)
	fatal("sasl_checkpass() failed on simple case");

    /* NULL saslconn */
    if (sasl_checkpass(NULL, username, (unsigned) strlen(username),
		   password, (unsigned) strlen(password)) == SASL_OK)
	fatal("Suceeded with NULL saslconn");

    /* NULL username -- should be OK if sasl_checkpass enabled */
    if (sasl_checkpass(saslconn, NULL, (unsigned) strlen(username),
		   password, (unsigned) strlen(password)) != SASL_OK)
	fatal("failed check if sasl_checkpass is enabled");

    /* NULL password */
    if (sasl_checkpass(saslconn, username, (unsigned) strlen(username),
		   NULL, (unsigned) strlen(password)) == SASL_OK)
	fatal("Suceeded with NULL password");

    sasl_dispose(&saslconn);
    sasl_done();
}



void notes(void)
{
    printf("NOTE:\n");
    printf("-For KERBEROS_V4 must be able to read srvtab file (usually /etc/srvtab)\n");
    printf("-For GSSAPI must be able to read srvtab (/etc/krb5.keytab)\n");
    printf("-For both KERBEROS_V4 and GSSAPI you must have non-expired tickets\n");
    printf("-For OTP (w/OPIE) must be able to read/write opiekeys (/etc/opiekeys)\n");
    printf("-For OTP you must have a non-expired secret\n");
    printf("-Must be able to read sasldb, which needs to be setup with a\n");
    printf(" username and a password (see top of testsuite.c)\n");
    printf("\n\n");
}

void usage(void)
{
    printf("Usage:\n" \
           " testsuite [-g name] [-s seed] [-r tests] -a -M\n" \
           "    g -- gssapi service name to use (default: host)\n" \
	   "    r -- # of random tests to do (default: 25)\n" \
	   "    a -- do all corruption tests (and ignores random ones unless -r specified)\n" \
	   "    n -- skip the initial \"do correctly\" tests\n"
	   "    h -- show this screen\n" \
           "    s -- random seed to use\n" \
	   "    M -- detailed memory debugging ON\n" \
           );
}

int main(int argc, char **argv)
{
    char c;
    int random_tests = -1;
    int do_all = 0;
    int skip_do_correct = 0;
    unsigned int seed = (unsigned int) time(NULL);
#ifdef WIN32
  /* initialize winsock */
    int result;
    WSADATA wsaData;

    result = WSAStartup( MAKEWORD(2, 0), &wsaData );
    if ( result != 0) {
	fatal("Windows sockets initialization failure");
    }
#endif

    while ((c = getopt(argc, argv, "Ms:g:r:han")) != EOF)
	switch (c) {
	case 'M':
	    DETAILED_MEMORY_DEBUGGING = 1;
	    break;
	case 's':
	    seed = atoi(optarg);
	    break;
	case 'g':
	    gssapi_service = optarg;
	    break;
	case 'r':
	    random_tests = atoi(optarg);
	    break;
	case 'a':
	    random_tests = 0;
	    do_all = 1;
	    break;
	case 'n':
	    skip_do_correct = 1;
	    break;
	case 'h':
	    usage();
	    exit(0);
	    break;
	default:
	    usage();
	    fatal("Invalid parameter\n");
	    break;
    }

    g_secret = malloc(sizeof(sasl_secret_t) + strlen(password));
    g_secret->len = (unsigned) strlen(password);
    strcpy((char *) g_secret->data, password);

    if(random_tests < 0) random_tests = 25;

    notes();

    init(seed);

#if 0 /* Disabled because it is borked */
    printf("Creating id's in mechanisms (not in sasldb)...\n");
    create_ids();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("Creating id's in mechanisms (not in sasldb)... ok\n");
#endif

    printf("Checking plaintext passwords... ");
    test_checkpass();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");

    printf("Random number functions... ");
    test_random();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");

    printf("Testing base64 functions... ");
    test_64();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");

    printf("Testing auxprop functions... ");
    test_props();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");

    printf("Tests of sasl_{server|client}_init()... ");
    test_init();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");
    
    printf("Testing sasl_listmech()... \n");
    test_listmech();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("Testing sasl_listmech()... ok\n");

    printf("Testing serverstart...");
    test_serverstart();
    if(mem_stat() != SASL_OK) fatal("memory error");
    printf("ok\n");

    if(!skip_do_correct) {
	tosend_t tosend;
	
	tosend.type = NOTHING;
	tosend.step = 500;
	tosend.client_callbacks = client_interactions;
	
	printf("Testing client-first/no-server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_clientfirst,&tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of client-first/no-server-last...ok\n");

	printf("Testing no-client-first/no-server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_noclientfirst, &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of no-client-first/no-server-last...ok\n");
	
	printf("Testing no-client-first/server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_noclientfirst_andserverlast,
			  &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of no-client-first/server-last...ok\n");

	printf("Testing client-first/server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_serverlast, &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of client-first/server-last...ok\n");

	tosend.client_callbacks = client_callbacks;
	printf("-=-=-=-=- And now using the callbacks interface -=-=-=-=-\n");

	printf("Testing client-first/no-server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_clientfirst,&tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of client-first/no-server-last...ok\n");

	printf("Testing no-client-first/no-server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_noclientfirst, &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of no-client-first/no-server-last...ok\n");
	
	printf("Testing no-client-first/server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_noclientfirst_andserverlast,
			  &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of no-client-first/server-last...ok\n");

	printf("Testing client-first/server-last correctly...\n");
	foreach_mechanism((foreach_t *) &test_serverlast, &tosend);
	if(mem_stat() != SASL_OK) fatal("memory error");
	printf("Test of client-first/server-last...ok\n");
    } else {
	printf("Testing client-first/no-server-last correctly...skipped\n");
	printf("Testing no-client-first/no-server-last correctly...skipped\n");
	printf("Testing no-client-first/server-last correctly...skipped\n");
	printf("Testing client-first/server-last correctly...skipped\n");
	printf("Above tests with callbacks interface...skipped\n");
    }
    
    /* FIXME: do memory tests below here on the things
     * that are MEANT to fail sometime. */
    if(do_all) {	
	printf("All corruption tests...\n");
	test_all_corrupt();
	printf("All corruption tests... ok\n");
    }
    
    if(random_tests) {
	printf("Random corruption tests...\n");
	test_rand_corrupt(random_tests);
	printf("Random tests... ok\n");
    } else {
	printf("Random tests... skipped\n");
    }

    printf("Testing Proxy Policy...\n");
    test_proxypolicy();
    printf("Tests of Proxy Policy...ok\n");

    printf("Testing security layer...\n");
    test_seclayer();
    printf("Tests of security layer... ok\n");

    printf("All tests seemed to go ok (i.e. we didn't crash)\n");

    free(g_secret);

    exit(0);
}
