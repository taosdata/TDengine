/* pluginviewer.c -- Plugin Viewer for CMU SASL
 * Alexey Melnikov, Isode Ltd.
 */
/* 
 * Copyright (c) 2004-2016 Carnegie Mellon University.  All rights reserved.
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
# include <winsock.h>
__declspec(dllimport) char *optarg;
__declspec(dllimport) int optind;
__declspec(dllimport) int getsubopt(char **optionp, const char * const *tokens, char **valuep);
#else  /* WIN32 */
# include <netinet/in.h>
#endif /* WIN32 */
#include <sasl.h>
#include <saslutil.h>
#include <saslplug.h>

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
/* SASL authentication methods (client or server side). NULL means all. */
static char *sasl_mech = NULL;
/* auxprop methods. NULL means all. */
static char *auxprop_mech = NULL;

#define N_CALLBACKS (16)

#define NOT_NULL	(void *) -1

#define SAMPLE_SEC_BUF_SIZE (2048)

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

/* Whitespace separated list of mechanisms to allow (e.g. 'plain otp').
   Used to restrict the mechanisms to a subset of the installed plugins.
   Default: NULL (i.e. all available) */
#define SASL_OPT_MECH_LIST		    "mech_list"
/* Name of canon_user plugin to use, default is "INTERNAL" */
#define SASL_OPT_CANON_USER_PLUGIN	    "canon_user_plugin"
/* Name of auxiliary plugin to use, you may specify a space-separated list
   of plugin names, and the plugins will be queried in order. Default is NULL (i.e. query all) */
#define SASL_OPT_AUXPROP_PLUGIN		    "auxprop_plugin"

static sasl_conn_t *server_conn = NULL;
static sasl_conn_t *client_conn = NULL;

static void
free_conn(void)
{
    if (server_conn) {
        sasl_dispose(&server_conn);
    }
    if (client_conn) {
        sasl_dispose(&client_conn);
    }
}

static int
sasl_my_log(void *context __attribute__((unused)),
	    int priority,
	    const char *message) 
{
    const char *label;

    if (! message) {
        return SASL_BADPARAM;
    }

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
getpath(void *context,
	const char ** path) 
{
    const char *searchpath = (const char *) context;

    if (! path) {
        return SASL_BADPARAM;
    }

    if (searchpath) {
        *path = searchpath;
    } else {
        *path = PLUGINDIR;
    }

    return SASL_OK;
}

static int
plugview_sasl_getopt (
    void *context __attribute__((unused)),
    const char *plugin_name __attribute__((unused)),
    const char *option,
    const char **result,
    unsigned *len
)
{
    if (strcasecmp (option, SASL_OPT_MECH_LIST) == 0) {
        /* Whitespace separated list of mechanisms to allow (e.g. 'plain otp').
           Used to restrict the mechanisms to a subset of the installed plugins.
           Default: NULL (i.e. all available) */
        if (result != NULL) {
	    *result = sasl_mech;
        }

        if (len != NULL) {
    /* This might be NULL, which means "all mechanisms" */
	    *len = sasl_mech ? strlen(sasl_mech) : 0;
        }
        return (SASL_OK);
    } 
    else {
        /* Unrecognized */
        return (SASL_FAIL);
    }
}

static void
sasldebug(int why, const char *what, const char *errstr)
{
    fprintf(stderr, "%s: %s: %s",
	    progname,
	    what,
	    sasl_errstring(why, NULL, NULL));
    if (errstr) {
        fprintf(stderr, " (%s)\n", errstr);
    } else {
        putc('\n', stderr);
    }
}

static void
saslfail(int why, const char *what, const char *errstr)
{
    sasldebug(why, what, errstr);
    free_conn();
    /* Call sasl_done twice - one for the client side SASL and
       one for the server side. */
    sasl_done();
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

/* Produce a space separated list of installed mechanisms */
static void
list_installed_server_mechanisms (
  server_sasl_mechanism_t *m,
  sasl_info_callback_stage_t stage,
  void *rock
)
{
    char ** list_of_mechs = (char **) rock;
    char * new_list;

    if (stage == SASL_INFO_LIST_START || stage == SASL_INFO_LIST_END) {
	return;
    }

    if (m->plug != NULL) {
	if (*list_of_mechs == NULL) {
	    *list_of_mechs = strdup(m->plug->mech_name);
	} else {
	    /* This is suboptimal, but works */
	    new_list = malloc (strlen(*list_of_mechs) + strlen(m->plug->mech_name) + 2);
	    if (new_list == NULL) {
		return;
	    }
	    sprintf (new_list, "%s %s", *list_of_mechs, m->plug->mech_name);
	    free (*list_of_mechs);
	    *list_of_mechs = new_list;
	}
    }
}

/* Produce a space separated list of installed mechanisms */
static void
list_installed_client_mechanisms (
  client_sasl_mechanism_t *m,
  sasl_info_callback_stage_t stage,
  void *rock
)
{
    char ** list_of_mechs = (char **) rock;
    char * new_list;

    if (stage == SASL_INFO_LIST_START || stage == SASL_INFO_LIST_END) {
	return;
    }

    if (m->plug != NULL) {
	if (*list_of_mechs == NULL) {
	    *list_of_mechs = strdup(m->plug->mech_name);
	} else {
	    /* This is suboptimal, but works */
	    new_list = malloc (strlen(*list_of_mechs) + strlen(m->plug->mech_name) + 2);
	    if (new_list == NULL) {
		return;
	    }
	    sprintf (new_list, "%s %s", *list_of_mechs, m->plug->mech_name);
	    free (*list_of_mechs);
	    *list_of_mechs = new_list;
	}
    }
}

/* Produce a space separated list of installed mechanisms */
static void
list_installed_auxprop_mechanisms (
  sasl_auxprop_plug_t *m,
  sasl_info_callback_stage_t stage,
  void *rock
)
{
    char ** list_of_mechs = (char **) rock;
    char * new_list;

    if (stage == SASL_INFO_LIST_START || stage == SASL_INFO_LIST_END) {
	return;
    }

    if (*list_of_mechs == NULL) {
	*list_of_mechs = strdup(m->name);
    } else {
	/* This is suboptimal, but works */
	new_list = malloc (strlen(*list_of_mechs) + strlen(m->name) + 2);
	if (new_list == NULL) {
	    return;
	}
	sprintf (new_list, "%s %s", *list_of_mechs, m->name);
	free (*list_of_mechs);
	*list_of_mechs = new_list;
    }
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
  const char *available_mechs = NULL;
  unsigned len;
  int count;
  sasl_callback_t callbacks[N_CALLBACKS], *callback;
  char *searchpath = NULL;
  char *service = "test";
  char * list_of_server_mechs = NULL;
  char * list_of_client_mechs = NULL;
  char * list_of_auxprop_mechs = NULL;
  int list_all_plugins = 1;             /* By default we list all plugins */
  int list_client_auth_plugins = 0;
  int list_server_auth_plugins = 0;
  int list_auxprop_plugins = 0;

#ifdef WIN32
  /* initialize winsock */
    WSADATA wsaData;

    result = WSAStartup( MAKEWORD(2, 0), &wsaData );
    if ( result != 0) {
	saslfail(SASL_FAIL, "Initializing WinSockets", NULL);
    }
#endif

    progname = strrchr(argv[0], HIER_DELIMITER);
    if (progname) {
        progname++;
    } else {
        progname = argv[0];
    }

    /* Init defaults... */
    memset(&secprops, 0L, sizeof(secprops));
    secprops.maxbufsize = SAMPLE_SEC_BUF_SIZE;
    secprops.max_ssf = UINT_MAX;

    while ((c = getopt(argc, argv, "acshb:e:m:f:p:x:?")) != EOF)
        switch (c) {
        case 'a':
	    list_auxprop_plugins = 1;
            list_all_plugins = 0;
	    break;

        case 'x':
            auxprop_mech = optarg;
            break;

        case 'c':
	    list_client_auth_plugins = 1;
            list_all_plugins = 0;
	    break;

        case 's':
	    list_server_auth_plugins = 1;
            list_all_plugins = 0;
	    break;

        case 'b':
            options = optarg;
            while (*options != '\0') {
	        switch(getsubopt(&options, (char * const *)bit_subopts, &value)) {
	        case OPT_MIN:
                    if (! value) {
	                errflag = 1;
                    } else {
	                secprops.min_ssf = atoi(value);
                    }
	            break;
	        case OPT_MAX:
                    if (! value) {
	                errflag = 1;
                    } else {
	                secprops.max_ssf = atoi(value);
                    }
	            break;
	        default:
	            errflag = 1;
	            break;	  
	        }
            }
            break;

        case 'e':
            options = optarg;
            while (*options != '\0') {
	        switch(getsubopt(&options, (char * const *)ext_subopts, &value)) {
	        case OPT_EXT_SSF:
                    if (! value) {
	                errflag = 1;
                    } else {
	                extssf = atoi(value);
                    }
	            break;
	        case OPT_MAX:
                    if (! value) {
	                errflag = 1;
                    } else {
	                ext_authid = value;
                    }
	            break;
	        default:
	            errflag = 1;
	            break;
	        }
            }
            break;

        case 'm':
            sasl_mech = optarg;
            break;

        case 'f':
            options = optarg;
            while (*options != '\0') {
	        switch(getsubopt(&options, (char * const *)flag_subopts, &value)) {
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

        case 'p':
            searchpath = optarg;
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
        fprintf(stderr, "%s: Usage: %s [-a] [-s] [-c] [-b min=N,max=N] [-e ssf=N,id=ID] [-m MECHS] [-x AUXPROP_MECH] [-f FLAGS] [-i local=IP,remote=IP] [-p PATH]\n"
	        "\t-a\tlist auxprop plugins\n"
                "\t-s\tlist server authentication (SASL) plugins\n"
                "\t-c\tlist client authentication (SASL) plugins\n"
	        "\t-b ...\t#bits to use for encryption\n"
	        "\t\tmin=N\tminimum #bits to use (1 => integrity)\n"
	        "\t\tmax=N\tmaximum #bits to use\n"
	        "\t-e ...\tassume external encryption\n"
	        "\t\tssf=N\texternal mech provides N bits of encryption\n"
	        "\t\tid=ID\texternal mech provides authentication id ID\n"
	        "\t-m MECHS\tforce to use one of MECHS SASL mechanism\n"
	        "\t-x AUXPROP_MECHS\tforce to use one of AUXPROP_MECHS auxprop plugins\n"
	        "\t-f ...\tset security flags\n"
	        "\t\tnoplain\t\tno plaintext password send during authentication\n"
	        "\t\tnoactive\trequire security vs. active attacks\n"
	        "\t\tnodict\t\trequire security vs. passive dictionary attacks\n"
	        "\t\tforwardsec\trequire forward secrecy\n"
	        "\t\tmaximum\t\trequire all security flags\n"
	        "\t\tpasscred\tattempt to pass client credentials\n"
#ifdef WIN32
	        "\t-p PATH\tsemicolon-separated search path for mechanisms\n",
#else
	        "\t-p PATH\tcolon-separated search path for mechanisms\n",
#endif
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

    /* getopt */
    /* NOTE: this will return "sasl_mech" option, however this HAS NO EFFECT
       on client side SASL plugins, which just never query this option */
    callback->id = SASL_CB_GETOPT;
    callback->proc = (sasl_callback_ft)&plugview_sasl_getopt;
    callback->context = NULL;
    ++callback;

    /* The following callbacks are for a client connection only.
    We reuse the same callbacks variable and the server side doesn't like
    proc == NULL. So we just put something there, != NULL! */
    callback->id = SASL_CB_AUTHNAME;
    callback->proc = NOT_NULL;
    callback->context = NULL;
    ++callback;


    callback->id = SASL_CB_PASS;
    callback->proc = NOT_NULL;
    callback->context = NULL;
    ++callback;

    /* termination */
    callback->id = SASL_CB_LIST_END;
    callback->proc = NULL;
    callback->context = NULL;
    ++callback;

    /* FIXME: In general case this is not going to work of course,
       as some plugins will need more callbacks then others. */
    if (N_CALLBACKS < callback - callbacks) {
        fail("Out of callback space; recompile with larger N_CALLBACKS");
    }

    result = sasl_client_init(callbacks);
    if (result != SASL_OK) {
        saslfail(result, "Initializing client side of libsasl", NULL);
    }

    result = sasl_server_init(callbacks, "pluginviewer");
    if (result != SASL_OK) {
        saslfail(result, "Initializing server side of libsasl", NULL);
    }

    if (list_all_plugins || list_auxprop_plugins) {
	list_of_auxprop_mechs = NULL;

	auxprop_plugin_info (NULL,  /* list all auxprop mechanisms */
			    &list_installed_auxprop_mechanisms,
			    (void *) &list_of_auxprop_mechs);

	printf ("Installed and properly configured auxprop mechanisms are:\n%s\n",
		(list_of_auxprop_mechs == NULL) ? "<none>" : list_of_auxprop_mechs);

	free (list_of_auxprop_mechs);

        
        auxprop_plugin_info (auxprop_mech, NULL, NULL);
    }

    /* TODO: add listing of canonicalization plugins, if needed. */

    if (list_all_plugins || list_server_auth_plugins) {
        /* SASL server plugins */
	/* List all loaded plugins first */
        list_of_server_mechs = NULL;

        sasl_server_plugin_info (NULL,  /* list all SASL mechanisms */
				 &list_installed_server_mechanisms,
				 (void *) &list_of_server_mechs);

        printf ("Installed and properly configured SASL (server side) mechanisms are:\n  %s\n", list_of_server_mechs);

        free (list_of_server_mechs);

	/* Now list plugins matching the criteria */
        result = sasl_server_new(service,
				/* Has to be any non NULL value */
			        "test.example.com",	/* localdomain */
			        NULL,			/* userdomain */
			        NULL,			/* iplocal */
			        NULL,			/* ipremote */
			        NULL,
			        0,
			        &server_conn);
        if (result != SASL_OK) {
            saslfail(result, "Allocating sasl connection state (server side)", NULL);
        }

        /* The following two options are required for SASL EXTERNAL */
        if (extssf) {
            result = sasl_setprop(server_conn,
			        SASL_SSF_EXTERNAL,
			        &extssf);

            if (result != SASL_OK) {
	        saslfail(result, "Setting external SSF", NULL);
            }
        }
          
        if (ext_authid) {
            result = sasl_setprop(server_conn,
			        SASL_AUTH_EXTERNAL,
			        &ext_authid);

            if (result != SASL_OK) {
	        saslfail(result, "Setting external authid", NULL);
            }
        }
          
        result = sasl_setprop(server_conn,
			    SASL_SEC_PROPS,
			    &secprops);

        if (result != SASL_OK) {
            saslfail(result, "Setting security properties", NULL);
        }

	/* NOTE - available_mechs must not be freed */
        result = sasl_listmech(server_conn,
			    ext_authid,
			    NULL,
			    " ",
			    NULL,
			    &available_mechs,
			    &len,
			    &count);
        if (result != SASL_OK) {
            saslfail(result, "Listing SASL mechanisms", NULL);
        }

	/* NOTE: available_mechs contains subset of sasl_mech */

        if (count > 0) {
            printf ("Available SASL (server side) mechanisms matching your criteria are:\n  %s\n", available_mechs);

	    /* Dump information about the requested SASL mechanism */
	    sasl_server_plugin_info (available_mechs, NULL, NULL);
        } else {
	    printf ("No server side SASL mechanisms matching your criteria found\n");
        }
    }

    if (list_all_plugins || list_client_auth_plugins) {
        /* SASL client plugins */
	/* List all loaded plugins first */
	list_of_client_mechs = NULL;

	sasl_client_plugin_info (NULL,  /* list all SASL mechanisms */
				 &list_installed_client_mechanisms,
				 (void *) &list_of_client_mechs);

	printf ("Installed and properly configured SASL (client side) mechanisms are:\n  %s\n",
		(list_of_client_mechs != NULL) ? list_of_client_mechs : "<none>");

	free (list_of_client_mechs);

	/* Now list plugins matching the criteria */
        result = sasl_client_new(service,
				/* Has to be any non NULL value */
			        "test.example.com",	/* fqdn */
			        NULL,			/* iplocal */
			        NULL,			/* ipremote */
			        NULL,
			        0,
			        &client_conn);

        if (result != SASL_OK) {
            saslfail(result, "Allocating sasl connection state (client side)", NULL);
        }

        /* The following two options are required for SSF */
        if (extssf) {
            result = sasl_setprop(client_conn,
			        SASL_SSF_EXTERNAL,
			        &extssf);

            if (result != SASL_OK) {
	        saslfail(result, "Setting external SSF", NULL);
            }
        }
          
        if (ext_authid) {
            result = sasl_setprop(client_conn,
			        SASL_AUTH_EXTERNAL,
			        &ext_authid);

            if (result != SASL_OK) {
	        saslfail(result, "Setting external authid", NULL);
            }
        }
          
        result = sasl_setprop(client_conn,
			    SASL_SEC_PROPS,
			    &secprops);

        if (result != SASL_OK) {
            saslfail(result, "Setting security properties", NULL);
        }

	/* NOTE - available_mechs must not be freed */
        result = sasl_listmech(client_conn,
			       ext_authid,
			       NULL,
			       " ",
			       NULL,
			       &available_mechs,
			       &len,
			       &count);
        if (result != SASL_OK) {
            saslfail(result, "Listing SASL mechanisms", NULL);
        }

        if (count > 0) {
            printf ("Available SASL (client side) mechanisms matching your criteria are:\n  %s\n", available_mechs);

	    /* Dump information about the requested SASL mechanism */
	    sasl_client_plugin_info (sasl_mech, NULL, NULL);
        } else {
	    printf ("No client side SASL mechanisms matching your criteria found\n");
        }
    }

    free_conn();
    /* Call sasl_done twice - one for the client side SASL and
       one for the server side. */
    sasl_done();
    sasl_done();

#ifdef WIN32
    WSACleanup();
#endif

    return (EXIT_SUCCESS);
}
