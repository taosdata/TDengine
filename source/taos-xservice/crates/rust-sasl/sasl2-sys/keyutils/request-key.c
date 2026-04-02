/* request-key.c: hand a key request off to the appropriate process
 *
 * Copyright (C) 2005 Red Hat, Inc. All Rights Reserved.
 * Written by David Howells (dhowells@redhat.com)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 *
 * /sbin/request-key <op> <key> <uid> <gid> <threadring> <processring> <sessionring> [<info>]
 *
 * Searches the specified session ring for a key indicating the command to run:
 *	type:	"user"
 *	desc:	"request-key:<op>"
 *	data:	command name, e.g.: "/home/dhowells/request-key-create.sh"
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <signal.h>
#include <syslog.h>
#include <unistd.h>
#include <dirent.h>
#include <limits.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <sys/select.h>
#include <sys/wait.h>
#include "keyutils.h"


struct parameters {
	key_serial_t	key_id;
	char		*op;
	char		*key_type;
	char		*key_desc;
	char		*callout_info;
	char		*key;
	char		*uid;
	char		*gid;
	char		*thread_keyring;
	char		*process_keyring;
	char		*session_keyring;
	int		len;
	int		oplen;
	int		ktlen;
	int		kdlen;
	int		cilen;

};

static int verbosity;
static int xlocaldirs;
static int xnolog;
static int debug_mode;
static char conffile[PATH_MAX + 1];
static int confline;
static int norecurse;

static char cmd[4096 + 2], cmd_conffile[PATH_MAX + 1];
static unsigned int cmd_wildness[4] = { UINT_MAX, UINT_MAX, UINT_MAX, UINT_MAX };
static int cmd_len, cmd_confline;

static void lookup_action(struct parameters *params)
	__attribute__((noreturn));
static void scan_conf_dir(struct parameters *params, const char *confdir);
static void scan_conf_file(struct parameters *params, int dirfd, const char *conffile);

static void execute_program(struct parameters *params,
			    char *cmdline)
	__attribute__((noreturn));

static void pipe_to_program(struct parameters *params,
			    char *prog,
			    char **argv)
	__attribute__((noreturn));

static int match(const char *pattern, int plen, const char *datum, int dlen,
		 unsigned int *wildness);

static void debug(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void debug(const char *fmt, ...)
{
	va_list va;

	if (verbosity) {
		va_start(va, fmt);
		vfprintf(stderr, fmt, va);
		va_end(va);

		if (!xnolog) {
			openlog("request-key", 0, LOG_AUTHPRIV);

			va_start(va, fmt);
			vsyslog(LOG_DEBUG, fmt, va);
			va_end(va);

			closelog();
		}
	}
}

static void error(const char *fmt, ...) __attribute__((noreturn, format(printf, 1, 2)));
static void error(const char *fmt, ...)
{
	va_list va;

	if (verbosity) {
		va_start(va, fmt);
		vfprintf(stderr, fmt, va);
		va_end(va);
	}

	if (!xnolog) {
		openlog("request-key", 0, LOG_AUTHPRIV);

		va_start(va, fmt);
		vsyslog(LOG_ERR, fmt, va);
		va_end(va);

		closelog();
	}

	exit(1);
}

#define file_error(FMT, ...)  error("%s: "FMT, conffile, ## __VA_ARGS__)
#define line_error(FMT, ...)  error("%s:%d: "FMT, conffile, confline, ## __VA_ARGS__)

static void oops(int x)
{
	error("Died on signal %d", x);
}

/*****************************************************************************/
/*
 *
 */
int main(int argc, char *argv[])
{
	struct parameters params;
	char *test_desc = "user;0;0;1f0000;debug:1234";
	char *buf;
	int ret, ntype, dpos, n, fd, opt;

	if (argc == 2 && strcmp(argv[1], "--version") == 0) {
		printf("request-key from %s (Built %s)\n",
		       keyutils_version_string, keyutils_build_string);
		return 0;
	}

	signal(SIGSEGV, oops);
	signal(SIGBUS, oops);
	signal(SIGPIPE, SIG_IGN);

	while (opt = getopt(argc, argv, "D:dlnv"),
	       opt != -1) {
		switch (opt) {
		case 'D':
			test_desc = optarg;
			break;
		case 'd':
			debug_mode = 1;
			break;
		case 'l':
			xlocaldirs = 1;
			break;
		case 'n':
			xnolog = 1;
			break;
		case 'v':
			verbosity++;
			break;
		}
	}

	argc -= optind;
	argv += optind;

	if (argc != 7 && argc != 8)
		error("Unexpected argument count: %d\n", argc);

	fd = open("/dev/null", O_RDWR);
	if (fd < 0)
		error("open");
	if (fd > 2) {
		close(fd);
	}
	else if (fd < 2) {
		ret = dup(fd);
		if (ret < 0)
			error("dup failed: %m\n");

		if (ret < 2 && dup(fd) < 0)
			error("dup failed: %m\n");
	}

	params.op		= argv[0];
	params.key		= argv[1];
	params.uid		= argv[2];
	params.gid		= argv[3];
	params.thread_keyring	= argv[4];
	params.process_keyring	= argv[5];
	params.session_keyring	= argv[6];
	params.callout_info	= argv[7];

	params.key_id = atoi(params.key);

	/* assume authority over the key
	 * - older kernel doesn't support this function
	 */
	if (!debug_mode) {
		ret = keyctl_assume_authority(params.key_id);
		if (ret < 0 && !(argc == 8 || errno == EOPNOTSUPP))
			error("Failed to assume authority over key %d (%m)\n",
			      params.key_id);
	}

	/* ask the kernel to describe the key to us */
	if (!debug_mode) {
		ret = keyctl_describe_alloc(params.key_id, &buf);
		if (ret < 0)
			goto inaccessible;
	} else {
		buf = strdup(test_desc);
	}

	/* extract the type and description from the key */
	debug("Key descriptor: \"%s\"\n", buf);
	ntype = -1;
	dpos = -1;

	n = sscanf(buf, "%*[^;]%n;%*d;%*d;%x;%n", &ntype, &n, &dpos);
	if (n != 1)
		error("Failed to parse key description\n");

	params.key_type = buf;
	params.key_type[ntype] = 0;
	params.key_desc = buf + dpos;

	debug("Key type: %s\n", params.key_type);
	debug("Key desc: %s\n", params.key_desc);

	/* get hold of the callout info */
	if (!params.callout_info) {
		void *tmp;

		if (keyctl_read_alloc(KEY_SPEC_REQKEY_AUTH_KEY, &tmp) < 0)
			error("Failed to retrieve callout info (%m)\n");

		params.callout_info = tmp;
	}

	debug("CALLOUT: '%s'\n", params.callout_info);

	/* determine the action to perform */
	params.oplen = strlen(params.op);
	params.ktlen = strlen(params.key_type);
	params.kdlen = strlen(params.key_desc);
	params.cilen = strlen(params.callout_info);
	lookup_action(&params);

inaccessible:
	error("Key %d is inaccessible (%m)\n", params.key_id);

} /* end main() */

/*****************************************************************************/
/*
 * determine the action to perform
 */
static void lookup_action(struct parameters *params)
{
	if (!xlocaldirs) {
		scan_conf_dir(params, "/etc/request-key.d");
		scan_conf_file(params, AT_FDCWD, "/etc/request-key.conf");
	} else {
		scan_conf_dir(params, "request-key.d");
		scan_conf_file(params, AT_FDCWD, "request-key.conf");
	}

	if (cmd_len > 0)
		execute_program(params, cmd);

	file_error("No matching action\n");
}

/*****************************************************************************/
/*
 * Scan the files in a configuration directory.
 */
static void scan_conf_dir(struct parameters *params, const char *confdir)
{
	struct dirent *d;
	DIR *dir;
	int l;

	debug("__ SCAN %s __\n", confdir);

	dir = opendir(confdir);
	if (!dir) {
		if (errno == ENOENT)
			return;
		error("Cannot open %s: %m\n", confdir);
	}

	while ((d = readdir(dir))) {
		if (d->d_name[0] == '.')
			continue;
		if (d->d_type != DT_UNKNOWN && d->d_type != DT_REG)
			continue;
		l = strlen(d->d_name);
		if (l < 5)
			continue;
		if (memcmp(d->d_name + l - 5, ".conf", 5) != 0)
			continue;
		scan_conf_file(params, dirfd(dir), d->d_name);
	}

	closedir(dir);
}

/*****************************************************************************/
/*
 * Scan the contents of a configuration file.
 */
static void scan_conf_file(struct parameters *params, int dirfd, const char *conffile)
{
	char buf[4096 + 2], *p, *q;
	FILE *conf;
	int fd;

	debug("__ read %s __\n", conffile);

	fd = openat(dirfd, conffile, O_RDONLY);
	if (fd < 0) {
		if (errno == ENOENT)
			return;
		error("Cannot open %s: %m\n", conffile);
	}

	conf = fdopen(fd, "r");
	if (!conf)
		error("Cannot open %s: %m\n", conffile);

	for (confline = 1;; confline++) {
		unsigned int wildness[4] = {};
		unsigned int len;

		/* read the file line-by-line */
		if (!fgets(buf, sizeof(buf), conf)) {
			if (feof(conf))
				break;
			file_error("error %m\n");
		}

		len = strlen(buf);
		if (len >= sizeof(buf) - 2)
			line_error("Line too long\n");

		/* ignore blank lines and comments */
		if (len == 1 || buf[0] == '#' || isspace(buf[0]))
			continue;

		buf[--len] = 0;
		p = buf;

		/* attempt to match the op */
		q = p;
		while (*p && !isspace(*p)) p++;
		if (!*p)
			goto syntax_error;
		*p = 0;

		if (!match(q, p - q, params->op, params->oplen, &wildness[0]))
			continue;

		p++;

		/* attempt to match the type */
		while (isspace(*p)) p++;
		if (!*p)
			goto syntax_error;

		q = p;
		while (*p && !isspace(*p)) p++;
		if (!*p)
			goto syntax_error;
		*p = 0;

		if (!match(q, p - q, params->key_type, params->ktlen, &wildness[1]))
			continue;

		p++;

		/* attempt to match the description */
		while (isspace(*p)) p++;
		if (!*p)
			goto syntax_error;

		q = p;
		while (*p && !isspace(*p)) p++;
		if (!*p)
			goto syntax_error;
		*p = 0;

		if (!match(q, p - q, params->key_desc, params->kdlen, &wildness[2]))
			continue;

		p++;

		/* attempt to match the callout info */
		while (isspace(*p)) p++;
		if (!*p)
			goto syntax_error;

		q = p;
		while (*p && !isspace(*p)) p++;
		if (!*p)
			goto syntax_error;
		*p = 0;

		if (!match(q, p - q, params->callout_info, params->cilen, &wildness[3]))
			continue;

		p++;

		/* we've got a match */
		while (isspace(*p)) p++;
		if (!*p)
			goto syntax_error;

		debug("%s:%d: Line matches '%s' (%u,%u,%u,%u)\n",
		      conffile, confline, p,
		      wildness[0], wildness[1], wildness[2], wildness[3]);

		if (wildness[0] < cmd_wildness[0] ||
		    (wildness[0] == cmd_wildness[0] &&
		     wildness[1] < cmd_wildness[1]) ||
		    (wildness[0] == cmd_wildness[0] &&
		     wildness[1] == cmd_wildness[1] &&
		     wildness[2] < cmd_wildness[2]) ||
		    (wildness[0] == cmd_wildness[0] &&
		     wildness[1] == cmd_wildness[1] &&
		     wildness[2] == cmd_wildness[2] &&
		     wildness[3] < cmd_wildness[3])
		    ) {
			memcpy(cmd_wildness, wildness, sizeof(cmd_wildness));
			cmd_len = len - (p - buf);
			cmd_confline = confline;
			debug("%s:%d: Prefer command (%u,%u,%u,%u)\n",
			      conffile, confline,
			      wildness[0], wildness[1], wildness[2], wildness[3]);
			memcpy(cmd, p, cmd_len + 1);
			strcpy(cmd_conffile, conffile);
		}
	}

	fclose(conf);
	return;

syntax_error:
	line_error("Syntax error\n");
}

/*****************************************************************************/
/*
 * attempt to match a datum to a pattern
 * - one asterisk is allowed anywhere in the pattern to indicate a wildcard
 * - returns true if matched, false if not
 * - adds the total number of chars skipped by wildcard to *_wildness
 */
static int match(const char *pattern, int plen, const char *datum, int dlen,
		 unsigned int *_wildness)
{
	const char *asterisk;
	int n;

	if (verbosity >= 2)
		debug("match(%*.*s,%*.*s)", plen, plen, pattern, dlen, dlen, datum);

	asterisk = memchr(pattern, '*', plen);
	if (!asterisk) {
		/* exact match only if no wildcard */
		if (plen == dlen && memcmp(pattern, datum, dlen) == 0)
			goto yes;
		goto no;
	}

	/* the datum mustn't be shorter than the pattern without the asterisk */
	if (dlen < plen - 1)
		goto no;

	n = asterisk - pattern;
	if (n == 0) {
		/* wildcard at beginning of pattern */
		pattern++;
		if (!*pattern)
			goto yes_wildcard; /* "*" matches everything */

		/* match the end of the datum */
		if (memcmp(pattern, datum + (dlen - (plen - 1)), plen - 1) == 0)
			goto yes_wildcard;
		goto no;
	}

	/* need to match beginning of datum for "abc*" and "abc*def" */
	if (memcmp(pattern, datum, n) != 0)
		goto no;

	if (!asterisk[1])
		goto yes_wildcard; /* "abc*" matches */

	/* match the end of the datum */
	asterisk++;
	n = plen - n - 1;
	if (memcmp(pattern, datum + (dlen - n), n) == 0)
		goto yes_wildcard;

no:
	if (verbosity >= 2)
		debug(" = no\n");
	return 0;

yes:
	if (verbosity >= 2)
		debug(" = yes (w=0)\n");
	return 1;

yes_wildcard:
	*_wildness += dlen - (plen - 1);
	if (verbosity >= 2)
		debug(" = yes (w=%u)\n", dlen - (plen - 1));
	return 1;

} /* end match() */

/*****************************************************************************/
/*
 * execute a program to deal with a key
 */
static void execute_program(struct parameters *params, char *cmdline)
{
	char *argv[256];
	char *prog, *p, *q;
	int argc, pipeit;

	debug("execute_program('%s','%s')\n", params->callout_info, cmdline);

	/* if the commandline begins with a bar, then we pipe the callout data into it and read
	 * back the payload data
	 */
	pipeit = 0;

	if (cmdline[0] == '|') {
		pipeit = 1;
		cmdline++;
	}

	/* extract the path to the program to run */
	prog = p = cmdline;
	while (*p && !isspace(*p)) p++;
//	if (!*p)
//		line_error("No command path\n");
//	*p++ = 0;
	if (*p)
		*p++ = 0;

	argv[0] = strrchr(prog, '/') + 1;

	/* extract the arguments */
	for (argc = 1; p; argc++) {
		while (isspace(*p)) p++;
		if (!*p)
			break;

		if (argc >= 254)
			line_error("Too many arguments\n");
		argv[argc] = q = p;

		while (*p && !isspace(*p)) p++;

		if (*p)
			*p++ = 0;
		else
			p = NULL;

		debug("argv[%d]: '%s'\n", argc, argv[argc]);

		if (*q != '%')
			continue;

		/* it's a macro */
		q++;
		if (!*q)
			line_error("Missing macro name\n");

		if (*q == '%') {
			/* it's actually an anti-macro escape "%%..." -> "%..." */
			argv[argc]++;
			continue;
		}

		/* single character macros */
		if (!q[1]) {
			switch (*q) {
			case 'o': argv[argc] = params->op;		continue;
			case 'k': argv[argc] = params->key;		continue;
			case 't': argv[argc] = params->key_type;	continue;
			case 'd': argv[argc] = params->key_desc;	continue;
			case 'c': argv[argc] = params->callout_info;	continue;
			case 'u': argv[argc] = params->uid;		continue;
			case 'g': argv[argc] = params->gid;		continue;
			case 'T': argv[argc] = params->thread_keyring;	continue;
			case 'P': argv[argc] = params->process_keyring;	continue;
			case 'S': argv[argc] = params->session_keyring;	continue;
			default:
				line_error("Unsupported macro\n");
			}
		}

		/* keysub macro */
		if (*q == '{') {
			key_serial_t keysub;
			void *tmp;
			char *ksdesc, *end, *subdata;
			int ret, loop;

			/* extract type and description */
			q++;
			ksdesc = strchr(q, ':');
			if (!ksdesc)
				line_error("Keysub macro lacks ':'\n");
			*ksdesc++ = 0;
			end = strchr(ksdesc, '}');
			if (!end)
				line_error("Unterminated keysub macro\n");

			*end++ = 0;
			if (*end)
				line_error("Keysub macro has trailing rubbish\n");

			debug("Keysub: %s key \"%s\"\n", q, ksdesc);

			if (!q[0])
				line_error("Keysub type empty\n");

			if (!ksdesc[0])
				line_error("Keysub description empty\n");

			/* look up the key in the requestor's keyrings, but fail immediately if the
			 * key is not found rather than invoking /sbin/request-key again
			 */
			keysub = request_key(q, ksdesc, NULL, 0);
			if (keysub < 0)
				line_error("Keysub key not found: %m\n");

			ret = keyctl_read_alloc(keysub, &tmp);
			if (ret < 0)
				line_error("Can't read keysub %d data: %m\n", keysub);
			subdata = tmp;

			for (loop = 0; loop < ret; loop++)
				if (!isprint(subdata[loop]))
					error("keysub %d data not printable ('%02hhx')\n",
					      keysub, subdata[loop]);

			argv[argc] = subdata;
			continue;
		}
	}

	if (argc == 0)
		line_error("No arguments\n");

	argv[argc] = NULL;

	if (verbosity) {
		char **ap;

		debug("%s %s\n", pipeit ? "PipeThru" : "Run", prog);
		for (ap = argv; *ap; ap++)
			debug("- argv[%td] = \"%s\"\n", ap - argv, *ap);
	}

	/* become the same UID/GID as the key requesting process */
	//setgid(atoi(xuid));
	//setuid(atoi(xgid));

	/* if the last argument is a single bar, we spawn off the program dangling on the end of
	 * three pipes and read the key material from the program, otherwise we just exec
	 */
	if (debug_mode) {
		printf("-- exec disabled --\n");
		exit(0);
	}

	if (pipeit)
		pipe_to_program(params, prog, argv);

	/* attempt to execute the command */
	execv(prog, argv);

	line_error("Failed to execute '%s': %m\n", prog);

} /* end execute_program() */

/*****************************************************************************/
/*
 * pipe the callout information to the specified program and retrieve the
 * payload data over another pipe
 */
static void pipe_to_program(struct parameters *params, char *prog, char **argv)
{
	char errbuf[512], payload[32768 + 1], *pp, *pc, *pe;
	int ipi[2], opi[2], epi[2], childpid;
	int ifl, ofl, efl, npay, ninfo, espace, tmp;

	debug("pipe_to_program(%s -> %s)", params->callout_info, prog);

	if (pipe(ipi) < 0 || pipe(opi) < 0 || pipe(epi) < 0)
		error("pipe failed: %m");

	childpid = fork();
	if (childpid == -1)
		error("fork failed: %m");

	if (childpid == 0) {
		/* child process */
		if (dup2(ipi[0], 0) < 0 ||
		    dup2(opi[1], 1) < 0 ||
		    dup2(epi[1], 2) < 0)
			error("dup2 failed: %m");
		close(ipi[0]);
		close(ipi[1]);
		close(opi[0]);
		close(opi[1]);
		close(epi[0]);
		close(epi[1]);

		execv(prog, argv);
		line_error("Failed to execute '%s': %m\n", prog);
	}

	/* parent process */
	close(ipi[0]);
	close(opi[1]);
	close(epi[1]);

#define TOSTDIN ipi[1]
#define FROMSTDOUT opi[0]
#define FROMSTDERR epi[0]

	ifl = fcntl(TOSTDIN, F_GETFL);
	ofl = fcntl(FROMSTDOUT, F_GETFL);
	efl = fcntl(FROMSTDERR, F_GETFL);
	if (ifl < 0 || ofl < 0 || efl < 0)
		error("fcntl/F_GETFL failed: %m");

	ifl |= O_NONBLOCK;
	ofl |= O_NONBLOCK;
	efl |= O_NONBLOCK;

	if (fcntl(TOSTDIN, F_SETFL, ifl) < 0 ||
	    fcntl(FROMSTDOUT, F_SETFL, ofl) < 0 ||
	    fcntl(FROMSTDERR, F_SETFL, efl) < 0)
		error("fcntl/F_SETFL failed: %m");

	ninfo = params->cilen;
	pc = params->callout_info;

	npay = sizeof(payload);
	pp = payload;

	espace = sizeof(errbuf);
	pe = errbuf;

	do {
		fd_set rfds, wfds;

		FD_ZERO(&rfds);
		FD_ZERO(&wfds);

		if (TOSTDIN != -1) {
			if (ninfo > 0) {
				FD_SET(TOSTDIN, &wfds);
			}
			else {
				close(TOSTDIN);
				TOSTDIN = -1;
				continue;
			}
		}

		if (FROMSTDOUT != -1)
			FD_SET(FROMSTDOUT, &rfds);

		if (FROMSTDERR != -1)
			FD_SET(FROMSTDERR, &rfds);

		tmp = TOSTDIN > FROMSTDOUT ? TOSTDIN : FROMSTDOUT;
		tmp = tmp > FROMSTDERR ? tmp : FROMSTDERR;
		tmp++;

		debug("select r=%d,%d w=%d m=%d\n", FROMSTDOUT, FROMSTDERR, TOSTDIN, tmp);

		tmp = select(tmp, &rfds, &wfds, NULL, NULL);
		if (tmp < 0)
			error("select failed: %m\n");

		if (TOSTDIN != -1 && FD_ISSET(TOSTDIN, &wfds)) {
			tmp = write(TOSTDIN, pc, ninfo);
			if (tmp < 0) {
				if (errno != EPIPE)
					error("write failed: %m\n");

				debug("EPIPE");
				ninfo = 0;
			}
			else {
				debug("wrote %d\n", tmp);

				pc += tmp;
				ninfo -= tmp;
			}
		}

		if (FROMSTDOUT != -1 && FD_ISSET(FROMSTDOUT, &rfds)) {
			tmp = read(FROMSTDOUT, pp, npay);
			if (tmp < 0)
				error("read failed: %m\n");

			debug("read %d\n", tmp);

			if (tmp == 0) {
				close(FROMSTDOUT);
				FROMSTDOUT = -1;
			}
			else {
				pp += tmp;
				npay -= tmp;

				if (npay == 0)
					error("Too much data read from query program\n");
			}
		}

		if (FROMSTDERR != -1 && FD_ISSET(FROMSTDERR, &rfds)) {
			char *nl;

			tmp = read(FROMSTDERR, pe, espace);
			if (tmp < 0)
				error("read failed: %m\n");

			debug("read err %d\n", tmp);

			if (tmp == 0) {
				close(FROMSTDERR);
				FROMSTDERR = -1;
				continue;
			}

			pe += tmp;
			espace -= tmp;

			while ((nl = memchr(errbuf, '\n', pe - errbuf))) {
				int n, rest;

				nl++;
				n = nl - errbuf;

				if (verbosity)
					fprintf(stderr, "Child: %*.*s", n, n, errbuf);

				if (!xnolog) {
					openlog("request-key", 0, LOG_AUTHPRIV);
					syslog(LOG_ERR, "Child: %*.*s", n, n, errbuf);
					closelog();
				}

				rest = pe - nl;
				if (rest > 0) {
					memmove(errbuf, nl, rest);
					pe -= n;
					espace += n;
				}
				else {
					pe = errbuf;
					espace = sizeof(errbuf);
				}
			}

			if (espace == 0) {
				int n = sizeof(errbuf);

				if (verbosity)
					fprintf(stderr, "Child: %*.*s", n, n, errbuf);

				if (!xnolog) {
					openlog("request-key", 0, LOG_AUTHPRIV);
					syslog(LOG_ERR, "Child: %*.*s", n, n, errbuf);
					closelog();
				}

				pe = errbuf;
				espace = sizeof(errbuf);
			}
		}

	} while (TOSTDIN != -1 || FROMSTDOUT != -1 || FROMSTDERR != -1);

	/* wait for the program to exit */
	if (waitpid(childpid, &tmp, 0) != childpid)
		error("wait for child failed: %m\n");

	/* if the process exited non-zero or died on a signal, then we call back in to ourself to
	 * decide on negation
	 * - this is not exactly beautiful but the quickest way of having configurable negation
	 *   settings
	 */
	if (WIFEXITED(tmp) && WEXITSTATUS(tmp) != 0) {
		if (norecurse)
			error("child exited %d\n", WEXITSTATUS(tmp));

		norecurse = 1;
		debug("child exited %d\n", WEXITSTATUS(tmp));
		params->op = "negate";
		lookup_action(params);
	}

	if (WIFSIGNALED(tmp)) {
		if (norecurse)
			error("child died on signal %d\n", WTERMSIG(tmp));

		norecurse = 1;
		params->op = "negate";
		lookup_action(params);
	}

	/* attempt to instantiate the key */
	debug("instantiate with %td bytes\n", pp - payload);

	if (keyctl_instantiate(params->key_id, payload, pp - payload, 0) < 0)
		error("instantiate key failed: %m\n");

	debug("instantiation successful\n");
	exit(0);

} /* end pipe_to_program() */
