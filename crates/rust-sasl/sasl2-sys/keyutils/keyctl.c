/* keyctl.c: key control program
 *
 * Copyright (C) 2005, 2011 Red Hat, Inc. All Rights Reserved.
 * Written by David Howells (dhowells@redhat.com)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */

#define _GNU_SOURCE
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <asm/unistd.h>
#include "keyutils.h"
#include <limits.h>
#include "keyctl.h"

static void try_wipe_memory(void *s, size_t n)
{
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2,25)
	explicit_bzero(s, n);
#endif
#endif
}

static nr void act_keyctl___version(int argc, char *argv[]);
static nr void act_keyctl_id(int argc, char *argv[]);
static nr void act_keyctl_show(int argc, char *argv[]);
static nr void act_keyctl_add(int argc, char *argv[]);
static nr void act_keyctl_padd(int argc, char *argv[]);
static nr void act_keyctl_request(int argc, char *argv[]);
static nr void act_keyctl_request2(int argc, char *argv[]);
static nr void act_keyctl_prequest2(int argc, char *argv[]);
static nr void act_keyctl_update(int argc, char *argv[]);
static nr void act_keyctl_pupdate(int argc, char *argv[]);
static nr void act_keyctl_newring(int argc, char *argv[]);
static nr void act_keyctl_revoke(int argc, char *argv[]);
static nr void act_keyctl_clear(int argc, char *argv[]);
static nr void act_keyctl_link(int argc, char *argv[]);
static nr void act_keyctl_unlink(int argc, char *argv[]);
static nr void act_keyctl_search(int argc, char *argv[]);
static nr void act_keyctl_read(int argc, char *argv[]);
static nr void act_keyctl_pipe(int argc, char *argv[]);
static nr void act_keyctl_print(int argc, char *argv[]);
static nr void act_keyctl_list(int argc, char *argv[]);
static nr void act_keyctl_rlist(int argc, char *argv[]);
static nr void act_keyctl_describe(int argc, char *argv[]);
static nr void act_keyctl_rdescribe(int argc, char *argv[]);
static nr void act_keyctl_chown(int argc, char *argv[]);
static nr void act_keyctl_chgrp(int argc, char *argv[]);
static nr void act_keyctl_setperm(int argc, char *argv[]);
static nr void act_keyctl_session(int argc, char *argv[]);
static nr void act_keyctl_instantiate(int argc, char *argv[]);
static nr void act_keyctl_pinstantiate(int argc, char *argv[]);
static nr void act_keyctl_negate(int argc, char *argv[]);
static nr void act_keyctl_timeout(int argc, char *argv[]);
static nr void act_keyctl_security(int argc, char *argv[]);
static nr void act_keyctl_new_session(int argc, char *argv[]);
static nr void act_keyctl_reject(int argc, char *argv[]);
static nr void act_keyctl_reap(int argc, char *argv[]);
static nr void act_keyctl_purge(int argc, char *argv[]);
static nr void act_keyctl_invalidate(int argc, char *argv[]);
static nr void act_keyctl_get_persistent(int argc, char *argv[]);
static nr void act_keyctl_dh_compute(int argc, char *argv[]);
static nr void act_keyctl_dh_compute_kdf(int argc, char *argv[]);
static nr void act_keyctl_dh_compute_kdf_oi(int argc, char *argv[]);
static nr void act_keyctl_restrict_keyring(int argc, char *argv[]);
static nr void act_keyctl_pkey_query(int argc, char *argv[]);
static nr void act_keyctl_pkey_encrypt(int argc, char *argv[]);
static nr void act_keyctl_pkey_decrypt(int argc, char *argv[]);
static nr void act_keyctl_pkey_sign(int argc, char *argv[]);
static nr void act_keyctl_pkey_verify(int argc, char *argv[]);
static nr void act_keyctl_move(int argc, char *argv[]);
static nr void act_keyctl_supports(int argc, char *argv[]);

static const struct command commands[] = {
	{ act_keyctl___version,	"--version",	"" },
	{ act_keyctl_add,	"add",		"[-x] <type> <desc> <data> <keyring>" },
	{ act_keyctl_chgrp,	"chgrp",	"<key> <gid>" },
	{ act_keyctl_chown,	"chown",	"<key> <uid>" },
	{ act_keyctl_clear,	"clear",	"<keyring>" },
	{ act_keyctl_describe,	"describe",	"<keyring>" },
	{ act_keyctl_dh_compute, "dh_compute",	"<private> <prime> <base>" },
	{ act_keyctl_dh_compute_kdf, "dh_compute_kdf", "<private> <prime> <base> <len> <hash_name>" },
	{ act_keyctl_dh_compute_kdf_oi, "dh_compute_kdf_oi", "[-x] <private> <prime> <base> <len> <hash_name>" },
	{ act_keyctl_get_persistent, "get_persistent", "<keyring> [<uid>]" },
	{ act_keyctl_id,	"id",		"<key>" },
	{ act_keyctl_instantiate, "instantiate","[-x] <key> <data> <keyring>" },
	{ act_keyctl_invalidate,"invalidate",	"<key>" },
	{ act_keyctl_link,	"link",		"<key> <keyring>" },
	{ act_keyctl_list,	"list",		"<keyring>" },
	{ act_keyctl_move,	"move",		"[-f] <key> <from_keyring> <to_keyring>" },
	{ act_keyctl_negate,	"negate",	"<key> <timeout> <keyring>" },
	{ act_keyctl_new_session, "new_session",	"[<name>]" },
	{ act_keyctl_newring,	"newring",	"<name> <keyring>" },
	{ act_keyctl_padd,	"padd",		"[-x] <type> <desc> <keyring>" },
	{ act_keyctl_pinstantiate, "pinstantiate","[-x] <key> <keyring>" },
	{ act_keyctl_pipe,	"pipe",		"<key>" },
	{ act_keyctl_pkey_query, "pkey_query",	"<key> <pass> [k=v]*" },
	{ act_keyctl_pkey_encrypt, "pkey_encrypt", "<key> <pass> <datafile> [k=v]*" },
	{ act_keyctl_pkey_decrypt, "pkey_decrypt", "<key> <pass> <datafile> [k=v]*" },
	{ act_keyctl_pkey_sign, "pkey_sign",	"<key> <pass> <datafile> [k=v]*" },
	{ act_keyctl_pkey_verify, "pkey_verify", "<key> <pass> <datafile> <sigfile> [k=v]*" },
	{ act_keyctl_prequest2,	"prequest2",	"<type> <desc> [<dest_keyring>]" },
	{ act_keyctl_print,	"print",	"<key>" },
	{ act_keyctl_pupdate,	"pupdate",	"[-x] <key>" },
	{ act_keyctl_purge,	"purge",	"<type>" },
	{ NULL,			"purge",	"[-p] [-i] <type> <desc>" },
	{ NULL,			"purge",	"-s <type> <desc>" },
	{ act_keyctl_rdescribe,	"rdescribe",	"<keyring> [sep]" },
	{ act_keyctl_read,	"read",		"<key>" },
	{ act_keyctl_reap,	"reap",		"[-v]" },
	{ act_keyctl_reject,	"reject",	"<key> <timeout> <error> <keyring>" },
	{ act_keyctl_request,	"request",	"<type> <desc> [<dest_keyring>]" },
	{ act_keyctl_request2,	"request2",	"<type> <desc> <info> [<dest_keyring>]" },
	{ act_keyctl_restrict_keyring, "restrict_keyring", "<keyring> [<type> [<restriction>]]" },
	{ act_keyctl_revoke,	"revoke",	"<key>" },
	{ act_keyctl_rlist,	"rlist",	"<keyring>" },
	{ act_keyctl_search,	"search",	"<keyring> <type> <desc> [<dest_keyring>]" },
	{ act_keyctl_security,	"security",	"<key>" },
	{ act_keyctl_session,	"session",	"" },
	{ NULL,			"session",	"- [<prog> <arg1> <arg2> ...]" },
	{ NULL,			"session",	"<name> [<prog> <arg1> <arg2> ...]" },
	{ act_keyctl_setperm,	"setperm",	"<key> <mask>" },
	{ act_keyctl_show,	"show",		"[-x] [<keyring>]" },
	{ act_keyctl_supports,	"supports",	"[<cap> | --raw]" },
	{ act_keyctl_timeout,	"timeout",	"<key> <timeout>" },
	{ act_keyctl_unlink,	"unlink",	"<key> [<keyring>]" },
	{ act_keyctl_update,	"update",	"[-x] <key> <data>" },
	{ act_keyctl_watch,	"watch",	"[-f<filters>] <key>" },
	{ act_keyctl_watch_add,	"watch_add",	"<fd> <key>" },
	{ act_keyctl_watch_rm,	"watch_rm",	"<fd> <key>" },
	{ act_keyctl_watch_session, "watch_session", "[-f<filters>] [-n <name>] <notifylog> <gclog> <fd> <prog> [<arg1> <arg2> ...]" },
	{ act_keyctl_watch_sync, "watch_sync",	"<fd>" },
	{ act_keyctl_test,	"--test",	"..." },
	{ NULL,			NULL,		NULL }
};

static int dump_key_tree(key_serial_t keyring, const char *name, int hex_key_IDs);
static void *read_file(const char *name, size_t *_size);

static uid_t myuid;
static gid_t mygid, *mygroups;
static int myngroups;
static int verbose;

/*****************************************************************************/
/*
 * handle an error
 */
void error(const char *msg)
{
	perror(msg);
	exit(1);
}

/*****************************************************************************/
/*
 * 
 */
int main(int argc, char *argv[])
{
	do_command(argc, argv, commands, "");
}

/*
 * Execute the appropriate subcommand.
 */
void do_command(int argc, char **argv,
		const struct command *commands, const char *group)
{
	const struct command *cmd, *best;
	int n;

	argv++;
	argc--;

	if (argc == 0)
		format();

	/* find the best fit command */
	best = NULL;
	n = strlen(*argv);

	for (cmd = commands; cmd->name; cmd++) {
		if (!cmd->action)
			continue;
		if (strlen(cmd->name) > n)
			continue;
		if (memcmp(cmd->name, *argv, n) != 0)
			continue;

		if (cmd->name[n] == 0) {
			/* exact match */
			best = cmd;
			break;
		}

		/* partial match */
		if (best) {
			fprintf(stderr, "Ambiguous %scommand\n", group);
			exit(2);
		}

		best = cmd;
	}

	if (!best) {
		fprintf(stderr, "Unknown %scommand\n", group);
		exit(2);
	}

	best->action(argc, argv);
}

/*****************************************************************************/
/*
 * display command format information
 */
void format(void)
{
	const struct command *cmd;

	fprintf(stderr, "Format:\n");

	for (cmd = commands; cmd->name; cmd++)
		fprintf(stderr, "  keyctl %s %s\n", cmd->name, cmd->format);

	fprintf(stderr, "\n");
	fprintf(stderr, "Key/keyring ID:\n");
	fprintf(stderr, "  <nnn>   numeric keyring ID\n");
	fprintf(stderr, "  @t      thread keyring\n");
	fprintf(stderr, "  @p      process keyring\n");
	fprintf(stderr, "  @s      session keyring\n");
	fprintf(stderr, "  @u      user keyring\n");
	fprintf(stderr, "  @us     user default session keyring\n");
	fprintf(stderr, "  @g      group keyring\n");
	fprintf(stderr, "  @a      assumed request_key authorisation key\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "<type> can be \"user\" for a user-defined keyring\n");
	fprintf(stderr, "If you do this, prefix the description with \"<subtype>:\"\n");

	exit(2);

} /* end format() */

/*****************************************************************************/
/*
 * Display version information
 */
static void act_keyctl___version(int argc, char *argv[])
{
	printf("keyctl from %s (Built %s)\n",
	       keyutils_version_string, keyutils_build_string);
	exit(0);
}

/*****************************************************************************/
/*
 * grab data from stdin
 */
static char *grab_stdin(size_t *_size)
{
	static char input[1024 * 1024 + 1];
	int n, tmp;

	n = 0;
	do {
		tmp = read(0, input + n, sizeof(input) - 1 - n);
		if (tmp < 0)
			error("stdin");

		if (tmp == 0)
			break;

		n += tmp;

	} while (n < sizeof(input));

	if (n >= sizeof(input)) {
		fprintf(stderr, "Too much data read on stdin\n");
		exit(1);
	}

	input[n] = '\0';
	*_size = n;

	return input;

} /* end grab_stdin() */

/*
 * Convert hex to binary if need be.
 */
void hex2bin(void **_data, size_t *_datalen, bool as_hex)
{
	unsigned char *buf, *q, h, l;
	char *p, *end;
	
	if (!as_hex || *_datalen == 0)
		return;

	q = buf = malloc(*_datalen / 2 + 2);
	if (!buf) {
		try_wipe_memory(*_data, *_datalen);
		error("malloc");
	}

	p = *_data;
	end = p + *_datalen;

	while (p < end) {
		if (isspace(*p)) {
			p++;
			continue;
		}
		if (end - p < 2) {
			fprintf(stderr, "Short hex doublet\n");
			goto ret_exit;
		}
		if (!isxdigit(p[0]) || !isxdigit(p[1])) {
			fprintf(stderr, "Bad hex doublet\n");
			goto ret_exit;
		}

		h = isdigit(p[0]) ? p[0] - '0' : tolower(p[0]) - 'a' + 0xa;
		l = isdigit(p[1]) ? p[1] - '0' : tolower(p[1]) - 'a' + 0xa;
		p += 2;
		*q++ = (h << 4) | l;
	}

	try_wipe_memory(*_data, *_datalen);

	*q = 0;
	*_data = buf;
	*_datalen = q - buf;
	return;

ret_exit:
	try_wipe_memory(*_data, *_datalen);
	try_wipe_memory(buf, q - buf);
	exit(1);
}

/*
 * Load the groups list and grab the process's UID and GID.
 */
static void grab_creds(void)
{
	static int inited;

	if (inited)
		return;
	inited = 1;

	/* grab my UID, GID and groups */
	myuid = geteuid();
	mygid = getegid();
	myngroups = getgroups(0, NULL);

	if (myuid == -1 || mygid == -1 || myngroups == -1)
		error("Unable to get UID/GID/#Groups\n");

	mygroups = calloc(myngroups, sizeof(gid_t));
	if (!mygroups)
		error("calloc");

	myngroups = getgroups(myngroups, mygroups);
	if (myngroups < 0)
		error("Unable to get Groups\n");
}

/*****************************************************************************/
/*
 * convert the permissions mask to a string representing the permissions we
 * have actually been granted
 */
static void calc_perms(char *pretty, key_perm_t perm, uid_t uid, gid_t gid)
{
	unsigned perms;
	gid_t *pg;
	int loop;

	grab_creds();

	perms = (perm & KEY_POS_ALL) >> 24;

	if (uid == myuid) {
		perms |= (perm & KEY_USR_ALL) >> 16;
		goto write_mask;
	}

	if (gid != -1) {
		if (gid == mygid) {
			perms |= (perm & KEY_GRP_ALL) >> 8;
			goto write_mask;
		}

		pg = mygroups;
		for (loop = myngroups; loop > 0; loop--, pg++) {
			if (gid == *pg) {
				perms |= (perm & KEY_GRP_ALL) >> 8;
				goto write_mask;
			}
		}
	}

	perms |= (perm & KEY_OTH_ALL);

write_mask:
	sprintf(pretty, "--%c%c%c%c%c%c",
		perms & KEY_OTH_SETATTR	? 'a' : '-',
		perms & KEY_OTH_LINK	? 'l' : '-',
		perms & KEY_OTH_SEARCH	? 's' : '-',
		perms & KEY_OTH_WRITE	? 'w' : '-',
		perms & KEY_OTH_READ	? 'r' : '-',
		perms & KEY_OTH_VIEW	? 'v' : '-');

} /* end calc_perms() */

/*****************************************************************************/
/*
 * Get a key or keyring ID.
 */
static void act_keyctl_id(int argc, char *argv[])
{
	key_serial_t key;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	key = keyctl_get_keyring_ID(key, 0);
	if (key < 0)
		error("keyctl_get_keyring_ID");

	printf("%d\n", key);
	exit(0);
}

/*****************************************************************************/
/*
 * show the parent process's session keyring
 */
static void act_keyctl_show(int argc, char *argv[])
{
	key_serial_t keyring = KEY_SPEC_SESSION_KEYRING;
	int hex_key_IDs = 0;

	if (argc >= 2 && strcmp(argv[1], "-x") == 0) {
		hex_key_IDs = 1;
		argc--;
		argv++;
	}

	if (argc > 2)
		format();

	if (argc == 2)
		keyring = get_key_id(argv[1]);

	dump_key_tree(keyring, argc == 2 ? "Keyring" : "Session Keyring", hex_key_IDs);
	exit(0);

} /* end act_keyctl_show() */

/*****************************************************************************/
/*
 * add a key
 */
static void act_keyctl_add(int argc, char *argv[])
{
	key_serial_t dest;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 5)
		format();

	data = argv[3];
	datalen = strlen(argv[3]);
	hex2bin(&data, &datalen, as_hex);

	dest = get_key_id(argv[4]);

	ret = add_key(argv[1], argv[2], data, datalen, dest);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("add_key");

	/* print the resulting key ID */
	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_add() */

/*****************************************************************************/
/*
 * add a key, reading from a pipe
 */
static void act_keyctl_padd(int argc, char *argv[])
{
	key_serial_t dest;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 4)
		format();

	data = grab_stdin(&datalen);
	hex2bin(&data, &datalen, as_hex);

	dest = get_key_id(argv[3]);

	ret = add_key(argv[1], argv[2], data, datalen, dest);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("add_key");

	/* print the resulting key ID */
	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_padd() */

/*****************************************************************************/
/*
 * request a key
 */
static void act_keyctl_request(int argc, char *argv[])
{
	key_serial_t dest;
	int ret;

	if (argc != 3 && argc != 4)
		format();

	dest = 0;
	if (argc == 4)
		dest = get_key_id(argv[3]);

	ret = request_key(argv[1], argv[2], NULL, dest);
	if (ret < 0)
		error("request_key");

	/* print the resulting key ID */
	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_request() */

/*****************************************************************************/
/*
 * request a key, with recourse to /sbin/request-key
 */
static void act_keyctl_request2(int argc, char *argv[])
{
	key_serial_t dest;
	int ret;

	if (argc != 4 && argc != 5)
		format();

	dest = 0;
	if (argc == 5)
		dest = get_key_id(argv[4]);

	ret = request_key(argv[1], argv[2], argv[3], dest);
	if (ret < 0)
		error("request_key");

	/* print the resulting key ID */
	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_request2() */

/*****************************************************************************/
/*
 * request a key, with recourse to /sbin/request-key, reading the callout info
 * from a pipe
 */
static void act_keyctl_prequest2(int argc, char *argv[])
{
	char *args[6];
	size_t datalen;

	if (argc != 3 && argc != 4)
		format();

	args[0] = argv[0];
	args[1] = argv[1];
	args[2] = argv[2];
	args[3] = grab_stdin(&datalen);
	args[4] = argv[3];
	args[5] = NULL;

	act_keyctl_request2(argc + 1, args);

} /* end act_keyctl_prequest2() */

/*****************************************************************************/
/*
 * update a key
 */
static void act_keyctl_update(int argc, char *argv[])
{
	key_serial_t key;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 3)
		format();

	data = argv[2];
	datalen = strlen(argv[2]);
	hex2bin(&data, &datalen, as_hex);

	key = get_key_id(argv[1]);

	ret = keyctl_update(key, data, datalen);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("keyctl_update");

	exit(0);

} /* end act_keyctl_update() */

/*****************************************************************************/
/*
 * update a key, reading from a pipe
 */
static void act_keyctl_pupdate(int argc, char *argv[])
{
	key_serial_t key;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);
	data = grab_stdin(&datalen);
	hex2bin(&data, &datalen, as_hex);

	ret = keyctl_update(key, data, datalen);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("keyctl_update");

	exit(0);

} /* end act_keyctl_pupdate() */

/*****************************************************************************/
/*
 * create a new keyring
 */
static void act_keyctl_newring(int argc, char *argv[])
{
	key_serial_t dest;
	int ret;

	if (argc != 3)
		format();

	dest = get_key_id(argv[2]);

	ret = add_key("keyring", argv[1], NULL, 0, dest);
	if (ret < 0)
		error("add_key");

	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_newring() */

/*****************************************************************************/
/*
 * revoke a key
 */
static void act_keyctl_revoke(int argc, char *argv[])
{
	key_serial_t key;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	if (keyctl_revoke(key) < 0)
		error("keyctl_revoke");

	exit(0);

} /* end act_keyctl_revoke() */

/*****************************************************************************/
/*
 * clear a keyring
 */
static void act_keyctl_clear(int argc, char *argv[])
{
	key_serial_t keyring;

	if (argc != 2)
		format();

	keyring = get_key_id(argv[1]);

	if (keyctl_clear(keyring) < 0)
		error("keyctl_clear");

	exit(0);

} /* end act_keyctl_clear() */

/*****************************************************************************/
/*
 * link a key to a keyring
 */
static void act_keyctl_link(int argc, char *argv[])
{
	key_serial_t keyring, key;

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);
	keyring = get_key_id(argv[2]);

	if (keyctl_link(key, keyring) < 0)
		error("keyctl_link");

	exit(0);

} /* end act_keyctl_link() */

/*
 * Attempt to unlink a key matching the ID
 */
static int act_keyctl_unlink_func(key_serial_t parent, key_serial_t key,
				  char *desc, int desc_len, void *data)
{
	key_serial_t *target = data;

	if (key == *target)
		return keyctl_unlink(key, parent) < 0 ? 0 : 1;
	return 0;
}

/*
 * Unlink a key from a keyring or from the session keyring tree.
 */
static void act_keyctl_unlink(int argc, char *argv[])
{
	key_serial_t keyring, key;
	int n;

	if (argc != 2 && argc != 3)
		format();

	key = get_key_id(argv[1]);

	if (argc == 3) {
		keyring = get_key_id(argv[2]);
		if (keyctl_unlink(key, keyring) < 0)
			error("keyctl_unlink");
	} else {
		n = recursive_session_key_scan(act_keyctl_unlink_func, &key);
		printf("%d links removed\n", n);
	}

	exit(0);
}

/*****************************************************************************/
/*
 * search a keyring for a key
 */
static void act_keyctl_search(int argc, char *argv[])
{
	key_serial_t keyring, dest;
	int ret;

	if (argc != 4 && argc != 5)
		format();

	keyring = get_key_id(argv[1]);

	dest = 0;
	if (argc == 5)
		dest = get_key_id(argv[4]);

	ret = keyctl_search(keyring, argv[2], argv[3], dest);
	if (ret < 0)
		error("keyctl_search");

	/* print the ID of the key we found */
	printf("%d\n", ret);
	exit(0);

} /* end act_keyctl_search() */

/*****************************************************************************/
/*
 * read a key
 */
static void act_keyctl_read(int argc, char *argv[])
{
	key_serial_t key;
	void *buffer;
	char *p;
	int ret, sep, col;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	/* read the key payload data */
	ret = keyctl_read_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_read_alloc");

	if (ret == 0) {
		printf("No data in key\n");
		exit(0);
	}

	/* hexdump the contents */
	printf("%u bytes of data in key:\n", ret);

	sep = 0;
	col = 0;
	p = buffer;

	do {
		if (sep) {
			putchar(sep);
			sep = 0;
		}

		printf("%02hhx", *p);
		p++;

		col++;
		if (col % 32 == 0)
			sep = '\n';
		else if (col % 4 == 0)
			sep = ' ';

	} while (--ret > 0);

	printf("\n");
	exit(0);

} /* end act_keyctl_read() */

/*****************************************************************************/
/*
 * read a key and dump raw to stdout
 */
static void act_keyctl_pipe(int argc, char *argv[])
{
	key_serial_t key;
	void *buffer;
	int ret;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	/* read the key payload data */
	ret = keyctl_read_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_read_alloc");

	if (ret > 0 && write(1, buffer, ret) < 0)
		error("write");
	exit(0);

} /* end act_keyctl_pipe() */

/*****************************************************************************/
/*
 * read a key and dump to stdout in printable form
 */
static void act_keyctl_print(int argc, char *argv[])
{
	key_serial_t key;
	void *buffer;
	char *p;
	int loop, ret;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	/* read the key payload data */
	ret = keyctl_read_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_read_alloc");

	/* see if it's printable */
	p = buffer;
	for (loop = ret; loop > 0; loop--, p++)
		if (!isprint(*p))
			goto not_printable;

	/* it is */
	printf("%s\n", (char *) buffer);
	exit(0);

not_printable:
	/* it isn't */
	printf(":hex:");
	p = buffer;
	for (loop = ret; loop > 0; loop--, p++)
		printf("%02hhx", *p);
	printf("\n");
	exit(0);

} /* end act_keyctl_print() */

/*****************************************************************************/
/*
 * list a keyring
 */
static void act_keyctl_list(int argc, char *argv[])
{
	key_serial_t keyring, key, *pk;
	key_perm_t perm;
	void *keylist;
	char *buffer, pretty_mask[9];
	uid_t uid;
	gid_t gid;
	int count, tlen, dpos, n, ret;

	if (argc != 2)
		format();

	keyring = get_key_id(argv[1]);

	/* read the key payload data */
	count = keyctl_read_alloc(keyring, &keylist);
	if (count < 0)
		error("keyctl_read_alloc");

	count /= sizeof(key_serial_t);

	if (count == 0) {
		printf("keyring is empty\n");
		exit(0);
	}

	/* list the keys in the keyring */
	if (count == 1)
		printf("1 key in keyring:\n");
	else
		printf("%u keys in keyring:\n", count);

	pk = keylist;
	do {
		key = *pk++;

		ret = keyctl_describe_alloc(key, &buffer);
		if (ret < 0) {
			printf("%9d: key inaccessible (%m)\n", key);
			continue;
		}

		uid = 0;
		gid = 0;
		perm = 0;

		tlen = -1;
		dpos = -1;

		n = sscanf((char *) buffer, "%*[^;]%n;%d;%d;%x;%n",
			   &tlen, &uid, &gid, &perm, &dpos);
		if (n != 3) {
			fprintf(stderr, "Unparsable description obtained for key %d\n", key);
			exit(3);
		}

		calc_perms(pretty_mask, perm, uid, gid);

		printf("%9d: %s %5d %5d %*.*s: %s\n",
		       key,
		       pretty_mask,
		       uid, gid,
		       tlen, tlen, buffer,
		       buffer + dpos);

		free(buffer);

	} while (--count);

	exit(0);

} /* end act_keyctl_list() */

/*****************************************************************************/
/*
 * produce a raw list of a keyring
 */
static void act_keyctl_rlist(int argc, char *argv[])
{
	key_serial_t keyring, key, *pk;
	void *keylist;
	int count;

	if (argc != 2)
		format();

	keyring = get_key_id(argv[1]);

	/* read the key payload data */
	count = keyctl_read_alloc(keyring, &keylist);
	if (count < 0)
		error("keyctl_read_alloc");

	count /= sizeof(key_serial_t);

	/* list the keys in the keyring */
	if (count <= 0) {
		printf("\n");
	}
	else {
		pk = keylist;
		for (; count > 0; count--) {
			key = *pk++;
			printf("%d%c", key, count == 1 ? '\n' : ' ');
		}
	}

	exit(0);

} /* end act_keyctl_rlist() */

/*****************************************************************************/
/*
 * describe a key
 */
static void act_keyctl_describe(int argc, char *argv[])
{
	key_serial_t key;
	key_perm_t perm;
	char *buffer;
	uid_t uid;
	gid_t gid;
	int tlen, dpos, n, ret;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	/* get key description */
	ret = keyctl_describe_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_describe_alloc");

	/* parse it */
	uid = 0;
	gid = 0;
	perm = 0;

	tlen = -1;
	dpos = -1;

	n = sscanf(buffer, "%*[^;]%n;%d;%d;%x;%n",
		   &tlen, &uid, &gid, &perm, &dpos);
	if (n != 3) {
		fprintf(stderr, "Unparsable description obtained for key %d\n", key);
		exit(3);
	}

	/* display it */
	printf("%9d:"
	       " %c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c%c"
	       " %5d %5d %*.*s: %s\n",
	       key,
	       perm & KEY_POS_SETATTR	? 'a' : '-',
	       perm & KEY_POS_LINK	? 'l' : '-',
	       perm & KEY_POS_SEARCH	? 's' : '-',
	       perm & KEY_POS_WRITE	? 'w' : '-',
	       perm & KEY_POS_READ	? 'r' : '-',
	       perm & KEY_POS_VIEW	? 'v' : '-',

	       perm & KEY_USR_SETATTR	? 'a' : '-',
	       perm & KEY_USR_LINK	? 'l' : '-',
	       perm & KEY_USR_SEARCH	? 's' : '-',
	       perm & KEY_USR_WRITE	? 'w' : '-',
	       perm & KEY_USR_READ	? 'r' : '-',
	       perm & KEY_USR_VIEW	? 'v' : '-',

	       perm & KEY_GRP_SETATTR	? 'a' : '-',
	       perm & KEY_GRP_LINK	? 'l' : '-',
	       perm & KEY_GRP_SEARCH	? 's' : '-',
	       perm & KEY_GRP_WRITE	? 'w' : '-',
	       perm & KEY_GRP_READ	? 'r' : '-',
	       perm & KEY_GRP_VIEW	? 'v' : '-',

	       perm & KEY_OTH_SETATTR	? 'a' : '-',
	       perm & KEY_OTH_LINK	? 'l' : '-',
	       perm & KEY_OTH_SEARCH	? 's' : '-',
	       perm & KEY_OTH_WRITE	? 'w' : '-',
	       perm & KEY_OTH_READ	? 'r' : '-',
	       perm & KEY_OTH_VIEW	? 'v' : '-',
	       uid, gid,
	       tlen, tlen, buffer,
	       buffer + dpos);

	exit(0);

} /* end act_keyctl_describe() */

/*****************************************************************************/
/*
 * get raw key description
 */
static void act_keyctl_rdescribe(int argc, char *argv[])
{
	key_serial_t key;
	char *buffer, *q;
	int ret;

	if (argc != 2 && argc != 3)
		format();
	if (argc == 3 && !argv[2][0])
		format();

	key = get_key_id(argv[1]);

	/* get key description */
	ret = keyctl_describe_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_describe");

	/* replace semicolon separators with requested alternative */
	if (argc == 3) {
		for (q = buffer; *q; q++)
			if (*q == ';')
				*q = argv[2][0];
	}

	/* display raw description */
	printf("%s\n", buffer);
	exit(0);

} /* end act_keyctl_rdescribe() */

/*****************************************************************************/
/*
 * change a key's ownership
 */
static void act_keyctl_chown(int argc, char *argv[])
{
	key_serial_t key;
	uid_t uid;
	char *q;

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);

	uid = strtoul(argv[2], &q, 0);
	if (*q) {
		fprintf(stderr, "Unparsable uid: '%s'\n", argv[2]);
		exit(2);
	}

	if (keyctl_chown(key, uid, -1) < 0)
		error("keyctl_chown");

	exit(0);

} /* end act_keyctl_chown() */

/*****************************************************************************/
/*
 * change a key's group ownership
 */
static void act_keyctl_chgrp(int argc, char *argv[])
{
	key_serial_t key;
	gid_t gid;
	char *q;

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);

	gid = strtoul(argv[2], &q, 0);
	if (*q) {
		fprintf(stderr, "Unparsable gid: '%s'\n", argv[2]);
		exit(2);
	}

	if (keyctl_chown(key, -1, gid) < 0)
		error("keyctl_chown");

	exit(0);

} /* end act_keyctl_chgrp() */

/*****************************************************************************/
/*
 * set the permissions on a key
 */
static void act_keyctl_setperm(int argc, char *argv[])
{
	key_serial_t key;
	key_perm_t perm;
	char *q;

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);
	perm = strtoul(argv[2], &q, 0);
	if (*q) {
		fprintf(stderr, "Unparsable permissions: '%s'\n", argv[2]);
		exit(2);
	}

	if (keyctl_setperm(key, perm) < 0)
		error("keyctl_setperm");

	exit(0);

} /* end act_keyctl_setperm() */

/*****************************************************************************/
/*
 * start a process in a new session
 */
static void act_keyctl_session(int argc, char *argv[])
{
	char *p, *q;
	int ret;

	argv++;
	argc--;

	/* no extra arguments signifies a standard shell in an anonymous
	 * session */
	p = NULL;
	if (argc != 0) {
		/* a dash signifies an anonymous session */
		p = *argv;
		if (strcmp(p, "-") == 0)
			p = NULL;

		argv++;
		argc--;
	}

	/* create a new session keyring */
	ret = keyctl_join_session_keyring(p);
	if (ret < 0)
		error("keyctl_join_session_keyring");

	fprintf(stderr, "Joined session keyring: %d\n", ret);

	/* run the standard shell if no arguments */
	if (argc == 0) {
		q = getenv("SHELL");
		if (!q)
			q = "/bin/sh";
		execl(q, q, NULL);
		error(q);
	}

	/* run the command specified */
	execvp(argv[0], argv);
	error(argv[0]);

} /* end act_keyctl_session() */

/*****************************************************************************/
/*
 * instantiate a key that's under construction
 */
static void act_keyctl_instantiate(int argc, char *argv[])
{
	key_serial_t key, dest;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 4)
		format();

	key = get_key_id(argv[1]);
	dest = get_key_id(argv[3]);
	data = argv[2];
	datalen = strlen(argv[2]);
	hex2bin(&data, &datalen, as_hex);

	ret = keyctl_instantiate(key, data, datalen, dest);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("keyctl_instantiate");

	exit(0);

} /* end act_keyctl_instantiate() */

/*****************************************************************************/
/*
 * instantiate a key, reading from a pipe
 */
static void act_keyctl_pinstantiate(int argc, char *argv[])
{
	key_serial_t key, dest;
	size_t datalen;
	void *data;
	bool as_hex = false;
	int ret;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);
	dest = get_key_id(argv[2]);
	data = grab_stdin(&datalen);
	hex2bin(&data, &datalen, as_hex);

	ret = keyctl_instantiate(key, data, datalen, dest);
	try_wipe_memory(data, datalen);
	if (ret < 0)
		error("keyctl_instantiate");

	exit(0);

} /* end act_keyctl_pinstantiate() */

/*****************************************************************************/
/*
 * negate a key that's under construction
 */
static void act_keyctl_negate(int argc, char *argv[])
{
	unsigned long timeout;
	key_serial_t key, dest;
	char *q;

	if (argc != 4)
		format();

	key = get_key_id(argv[1]);

	timeout = strtoul(argv[2], &q, 10);
	if (*q) {
		fprintf(stderr, "Unparsable timeout: '%s'\n", argv[2]);
		exit(2);
	}

	dest = get_key_id(argv[3]);

	if (keyctl_negate(key, timeout, dest) < 0)
		error("keyctl_negate");

	exit(0);

} /* end act_keyctl_negate() */

/*****************************************************************************/
/*
 * set a key's timeout
 */
static void act_keyctl_timeout(int argc, char *argv[])
{
	unsigned long timeout;
	key_serial_t key;
	char *q;

	if (argc != 3)
		format();

	key = get_key_id(argv[1]);

	timeout = strtoul(argv[2], &q, 10);
	if (*q) {
		fprintf(stderr, "Unparsable timeout: '%s'\n", argv[2]);
		exit(2);
	}

	if (keyctl_set_timeout(key, timeout) < 0)
		error("keyctl_set_timeout");

	exit(0);

} /* end act_keyctl_timeout() */

/*****************************************************************************/
/*
 * get a key's security label
 */
static void act_keyctl_security(int argc, char *argv[])
{
	key_serial_t key;
	char *buffer;
	int ret;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	/* get key description */
	ret = keyctl_get_security_alloc(key, &buffer);
	if (ret < 0)
		error("keyctl_getsecurity");

	printf("%s\n", buffer);
	exit(0);
}

/*****************************************************************************/
/*
 * install a new session keyring on the parent process
 */
static void act_keyctl_new_session(int argc, char *argv[])
{
	key_serial_t keyring;

	if (argc != 1 && argc != 2)
		format();

	if (keyctl_join_session_keyring(argv[1]) < 0)
		error("keyctl_join_session_keyring");

	if (keyctl_session_to_parent() < 0)
		error("keyctl_session_to_parent");

	keyring = keyctl_get_keyring_ID(KEY_SPEC_SESSION_KEYRING, 0);
	if (keyring < 0)
		error("keyctl_get_keyring_ID");

	/* print the resulting key ID */
	printf("%d\n", keyring);
	exit(0);
}

/*****************************************************************************/
/*
 * reject a key that's under construction
 */
static void act_keyctl_reject(int argc, char *argv[])
{
	unsigned long timeout;
	key_serial_t key, dest;
	unsigned long rejerr;
	char *q;

	if (argc != 5)
		format();

	key = get_key_id(argv[1]);

	timeout = strtoul(argv[2], &q, 10);
	if (*q) {
		fprintf(stderr, "Unparsable timeout: '%s'\n", argv[2]);
		exit(2);
	}

	if (strcmp(argv[3], "rejected") == 0) {
		rejerr = EKEYREJECTED;
	} else if (strcmp(argv[3], "revoked") == 0) {
		rejerr = EKEYREVOKED;
	} else if (strcmp(argv[3], "expired") == 0) {
		rejerr = EKEYEXPIRED;
	} else {
		rejerr = strtoul(argv[3], &q, 10);
		if (*q) {
			fprintf(stderr, "Unparsable error: '%s'\n", argv[3]);
			exit(2);
		}
	}

	dest = get_key_id(argv[4]);

	if (keyctl_reject(key, timeout, rejerr, dest) < 0)
		error("keyctl_negate");

	exit(0);
}

/*
 * Attempt to unlink a key if we can't read it for reasons other than we don't
 * have permission
 */
static int act_keyctl_reap_func(key_serial_t parent, key_serial_t key,
				char *desc, int desc_len, void *data)
{
	if (desc_len < 0 && errno != EACCES) {
		if (verbose)
			printf("Reap %d", key);
		if (keyctl_unlink(key, parent) < 0) {
			if (verbose)
				printf("... failed %m\n");
			return 0;
		} else {
			if (verbose)
				printf("\n");
			return 1;
		};
	}
	return 0;
}

/*
 * Reap the dead keys from the session keyring tree
 */
static void act_keyctl_reap(int argc, char *argv[])
{
	int n;

	if (argc > 1 && strcmp(argv[1], "-v") == 0) {
		verbose = 1;
		argc--;
		argv++;
	}

	if (argc != 1)
		format();

	n = recursive_session_key_scan(act_keyctl_reap_func, NULL);
	printf("%d keys reaped\n", n);
	exit(0);
}

struct purge_data {
	const char	*type;
	const char	*desc;
	size_t		desc_len;
	size_t		type_len;
	char		prefix_match;
	char		case_indep;
};

/*
 * Attempt to unlink a key matching the type
 */
static int act_keyctl_purge_type_func(key_serial_t parent, key_serial_t key,
				      char *raw, int raw_len, void *data)
{
	const struct purge_data *purge = data;
	char *p, *type;

	if (parent == 0 || !raw)
		return 0;

	/* type is everything before the first semicolon */
	type = raw;
	p = memchr(raw, ';', raw_len);
	if (!p)
		return 0;
	*p = 0;
	if (strcmp(type, purge->type) != 0)
		return 0;

	return keyctl_unlink(key, parent) < 0 ? 0 : 1;
}

/*
 * Attempt to unlink a key matching the type and description literally
 */
static int act_keyctl_purge_literal_func(key_serial_t parent, key_serial_t key,
					 char *raw, int raw_len, void *data)
{
	const struct purge_data *purge = data;
	size_t tlen;
	char *p, *type, *desc;

	if (parent == 0 || !raw)
		return 0;

	/* type is everything before the first semicolon */
	type = raw;
	p = memchr(type, ';', raw_len);
	if (!p)
		return 0;

	tlen = p - type;
	if (tlen != purge->type_len)
		return 0;
	if (memcmp(type, purge->type, tlen) != 0)
		return 0;

	/* description is everything after the last semicolon */
	p++;
	desc = memrchr(p, ';', raw + raw_len - p);
	if (!desc)
		return 0;
	desc++;

	if (purge->prefix_match) {
		if (raw_len - (desc - raw) < purge->desc_len)
			return 0;
	} else {
		if (raw_len - (desc - raw) != purge->desc_len)
			return 0;
	}

	if (purge->case_indep) {
		if (strncasecmp(purge->desc, desc, purge->desc_len) != 0)
			return 0;
	} else {
		if (memcmp(purge->desc, desc, purge->desc_len) != 0)
			return 0;
	}

	printf("%*.*s '%s'\n", (int)tlen, (int)tlen, type, desc);

	return keyctl_unlink(key, parent) < 0 ? 0 : 1;
}

/*
 * Attempt to unlink a key matching the type and description literally
 */
static int act_keyctl_purge_search_func(key_serial_t parent, key_serial_t keyring,
					char *raw, int raw_len, void *data)
{
	const struct purge_data *purge = data;
	key_serial_t key;
	int kcount = 0;

	if (!raw || memcmp(raw, "keyring;", 8) != 0)
		return 0;

	for (;;) {
		key = keyctl_search(keyring, purge->type, purge->desc, 0);
		if (keyctl_unlink(key, keyring) < 0)
			return kcount;
		kcount++;
	}
	return kcount;
}

/*
 * Purge matching keys from a keyring
 */
static void act_keyctl_purge(int argc, char *argv[])
{
	recursive_key_scanner_t func;
	struct purge_data purge = {
		.prefix_match	= 0,
		.case_indep	= 0,
	};
	int n = 0, search_mode = 0;

	argc--;
	argv++;
	while (argc > 0 && argv[0][0] == '-') {
		if (argv[0][1] == 's')
			search_mode = 1;
		else if (argv[0][1] == 'p')
			purge.prefix_match = 1;
		else if (argv[0][1] == 'i')
			purge.case_indep = 1;
		else
			format();
		argc--;
		argv++;
	}

	if (argc < 1)
		format();

	purge.type	= argv[0];
	purge.desc	= argv[1];
	purge.type_len	= strlen(purge.type);
	purge.desc_len	= purge.desc ? strlen(purge.desc) : 0;

	if (search_mode == 1) {
		if (argc != 2 || purge.prefix_match || purge.case_indep)
			format();
		/* purge all keys of a specific type and description, according
		 * to the kernel's comparator */
		func = act_keyctl_purge_search_func;
	} else if (argc == 1) {
		if (purge.prefix_match || purge.case_indep)
			format();
		/* purge all keys of a specific type */
		func = act_keyctl_purge_type_func;
	} else if (argc == 2) {
		/* purge all keys of a specific type with literally matching
		 * description */
		func = act_keyctl_purge_literal_func;
	} else {
		format();
	}

	n = recursive_session_key_scan(func, &purge);
	printf("purged %d keys\n", n);
	exit(0);
}

/*****************************************************************************/
/*
 * Invalidate a key
 */
static void act_keyctl_invalidate(int argc, char *argv[])
{
	key_serial_t key;

	if (argc != 2)
		format();

	key = get_key_id(argv[1]);

	if (keyctl_invalidate(key) < 0)
		error("keyctl_invalidate");

	exit(0);
}

/*****************************************************************************/
/*
 * Get the per-UID persistent keyring
 */
static void act_keyctl_get_persistent(int argc, char *argv[])
{
	key_serial_t dest, ret;
	uid_t uid = -1;
	char *q;

	if (argc != 2 && argc != 3)
		format();

	dest = get_key_id(argv[1]);

	if (argc > 2) {
		uid = strtoul(argv[2], &q, 0);
		if (*q) {
			fprintf(stderr, "Unparsable uid: '%s'\n", argv[2]);
			exit(2);
		}
	}

	ret = keyctl_get_persistent(uid, dest);
	if (ret < 0)
		error("keyctl_get_persistent");

	/* print the resulting key ID */
	printf("%d\n", ret);
	exit(0);
}

/*****************************************************************************/
/*
 * Perform Diffie-Hellman computation
 */
static void act_keyctl_dh_compute(int argc, char *argv[])
{
	key_serial_t priv, prime, base;
	void *buffer;
	char *p;
	int ret, sep, col;

	if (argc != 4)
		format();

	priv = get_key_id(argv[1]);
	prime = get_key_id(argv[2]);
	base = get_key_id(argv[3]);

	ret = keyctl_dh_compute_alloc(priv, prime, base, &buffer);
	if (ret < 0)
		error("keyctl_dh_compute_alloc");

	/* hexdump the contents */
	printf("%u bytes of data in result:\n", ret);

	sep = 0;
	col = 0;
	p = buffer;

	do {
		if (sep) {
			putchar(sep);
			sep = 0;
		}

		printf("%02hhx", *p);
		*p = 0x00;	/* zeroize buffer */
		p++;

		col++;
		if (col % 32 == 0)
			sep = '\n';
		else if (col % 4 == 0)
			sep = ' ';

	} while (--ret > 0);

	printf("\n");

	free(buffer);

	exit(0);
}

static void act_keyctl_dh_compute_kdf(int argc, char *argv[])
{
	key_serial_t private, prime, base;
	char *buffer;
	char *p;
	int ret, sep, col;
	unsigned long buflen = 0;

	if (argc != 6)
		format();

	private = get_key_id(argv[1]);
	prime = get_key_id(argv[2]);
	base = get_key_id(argv[3]);

	buflen = strtoul(argv[4], NULL, 10);
	if (buflen == ULONG_MAX)
		error("dh_compute: cannot convert generated length value");

	buffer = malloc(buflen);
	if (!buffer)
		error("dh_compute: cannot allocate memory");

	ret = keyctl_dh_compute_kdf(private, prime, base, argv[5], NULL,  0,
				    buffer, buflen);
	if (ret < 0)
		error("keyctl_dh_compute_kdf");

	/* hexdump the contents */
	printf("%u bytes of data in result:\n", ret);

	sep = 0;
	col = 0;
	p = buffer;

	do {
		if (sep) {
			putchar(sep);
			sep = 0;
		}

		printf("%02hhx", *p);
		*p = 0x00;	/* zeroize buffer */
		p++;

		col++;
		if (col % 32 == 0)
			sep = '\n';
		else if (col % 4 == 0)
			sep = ' ';

	} while (--ret > 0);

	printf("\n");

	free(buffer);

	exit(0);
}

static void act_keyctl_dh_compute_kdf_oi(int argc, char *argv[])
{
	key_serial_t private, prime, base;
	char *buffer;
	char *p;
	int ret, sep, col;
	unsigned long buflen = 0;
	size_t oilen;
	void *oi;
	bool as_hex = false;

	if (argc > 1 && strcmp(argv[1], "-x") == 0) {
		as_hex = true;
		argc--;
		argv++;
	}

	if (argc != 6)
		format();

	private = get_key_id(argv[1]);
	prime = get_key_id(argv[2]);
	base = get_key_id(argv[3]);

	buflen = strtoul(argv[4], NULL, 10);
	if (buflen == ULONG_MAX)
		error("dh_compute: cannot convert generated length value");

	buffer = malloc(buflen);
	if (!buffer)
		error("dh_compute: cannot allocate memory");

	oi = grab_stdin(&oilen);
	hex2bin(&oi, &oilen, as_hex);

	ret = keyctl_dh_compute_kdf(private, prime, base, argv[5], oi,  oilen,
				    buffer, buflen);
	try_wipe_memory(oi, oilen);
	if (ret < 0)
		error("keyctl_dh_compute_kdf");

	/* hexdump the contents */
	printf("%u bytes of data in result:\n", ret);

	sep = 0;
	col = 0;
	p = buffer;

	do {
		if (sep) {
			putchar(sep);
			sep = 0;
		}

		printf("%02hhx", *p);
		*p = 0x00;	/* zeroize buffer */
		p++;

		col++;
		if (col % 32 == 0)
			sep = '\n';
		else if (col % 4 == 0)
			sep = ' ';

	} while (--ret > 0);

	printf("\n");

	free(buffer);

	exit(0);
}

/*****************************************************************************/
/*
 * Restrict the keys that may be added to a keyring
 */
static void act_keyctl_restrict_keyring(int argc, char *argv[])
{
	key_serial_t keyring;
	char *type = NULL;
	char *restriction = NULL;
	long ret;

	if (argc < 2 || argc > 4)
		format();

	keyring = get_key_id(argv[1]);

	if (argc > 2)
		type = argv[2];

	if (argc == 4)
		restriction = argv[3];

	ret = keyctl_restrict_keyring(keyring, type, restriction);
	if (ret < 0)
		error("keyctl_restrict_keyring");

	exit(0);
}

/*
 * Parse public key operation info arguments.
 */
static void pkey_parse_info(char **argv, char info[4096])
{
	int i_len = 0;

	/* A number of key=val pairs can be provided after the main arguments
	 * to inform the kernel of things like encoding type and hash function.
	 */
	for (; *argv; argv++) {
		int n = strlen(argv[0]);

		if (!memchr(argv[0], '=', n)) {
			fprintf(stderr, "Option not in key=val form\n");
			exit(2);
		}
		if (n + 1 > 4096 - 1 - i_len) {
			fprintf(stderr, "Too many info options\n");
			exit(2);
		}

		if (i_len > 0)
			info[i_len++] = ' ';
		memcpy(info + i_len, argv[0], n);
		i_len += n;
	}

	info[i_len] = 0;
}

/*
 * Query a public key.
 */
static void act_keyctl_pkey_query(int argc, char *argv[])
{
	struct keyctl_pkey_query result;
	key_serial_t key;
	char info[4096];

	if (argc < 3)
		format();
	pkey_parse_info(argv + 3, info);

	key = get_key_id(argv[1]);
	if (strcmp(argv[2], "0") != 0) {
		fprintf(stderr, "Password passing is not yet supported\n");
		exit(2);
	}

	if (keyctl_pkey_query(key, info, &result) < 0)
		error("keyctl_pkey_query");

	printf("key_size=%u\n", result.key_size);
	printf("max_data_size=%u\n", result.max_data_size);
	printf("max_sig_size=%u\n", result.max_sig_size);
	printf("max_enc_size=%u\n", result.max_enc_size);
	printf("max_dec_size=%u\n", result.max_dec_size);
	printf("encrypt=%c\n", result.supported_ops & KEYCTL_SUPPORTS_ENCRYPT ? 'y' : 'n');
	printf("decrypt=%c\n", result.supported_ops & KEYCTL_SUPPORTS_DECRYPT ? 'y' : 'n');
	printf("sign=%c\n",    result.supported_ops & KEYCTL_SUPPORTS_SIGN    ? 'y' : 'n');
	printf("verify=%c\n",  result.supported_ops & KEYCTL_SUPPORTS_VERIFY  ? 'y' : 'n');
	exit(0);
}

/*
 * Encrypt a blob.
 */
static void act_keyctl_pkey_encrypt(int argc, char *argv[])
{
	struct keyctl_pkey_query result;
	key_serial_t key;
	size_t in_len;
	long out_len;
	void *in, *out;
	char info[4096];

	if (argc < 4)
		format();
	pkey_parse_info(argv + 4, info);

	key = get_key_id(argv[1]);
	if (strcmp(argv[2], "0") != 0) {
		fprintf(stderr, "Password passing is not yet supported\n");
		exit(2);
	}
	in = read_file(argv[3], &in_len);

	if (keyctl_pkey_query(key, info, &result) < 0)
		error("keyctl_pkey_query");

	out = malloc(result.max_dec_size);
	if (!out)
		error("malloc");

	out_len = keyctl_pkey_encrypt(key, info,
				      in, in_len, out, result.max_dec_size);
	if (out_len < 0)
		error("keyctl_pkey_encrypt");

	if (fwrite(out, out_len, 1, stdout) != 1)
		error("stdout");
	exit(0);
}

/*
 * Decrypt a blob.
 */
static void act_keyctl_pkey_decrypt(int argc, char *argv[])
{
	struct keyctl_pkey_query result;
	key_serial_t key;
	size_t in_len;
	long out_len;
	void *in, *out;
	char info[4096];

	if (argc < 4)
		format();
	pkey_parse_info(argv + 4, info);

	key = get_key_id(argv[1]);
	if (strcmp(argv[2], "0") != 0) {
		fprintf(stderr, "Password passing is not yet supported\n");
		exit(2);
	}
	in = read_file(argv[3], &in_len);

	if (keyctl_pkey_query(key, info, &result) < 0)
		error("keyctl_pkey_query");

	out = malloc(result.max_enc_size);
	if (!out)
		error("malloc");

	out_len = keyctl_pkey_decrypt(key, info,
				      in, in_len, out, result.max_enc_size);
	if (out_len < 0)
		error("keyctl_pkey_decrypt");

	if (fwrite(out, out_len, 1, stdout) != 1)
		error("stdout");
	exit(0);
}

/*
 * Create a signature
 */
static void act_keyctl_pkey_sign(int argc, char *argv[])
{
	struct keyctl_pkey_query result;
	key_serial_t key;
	size_t in_len;
	long out_len;
	void *in, *out;
	char info[4096];

	if (argc < 4)
		format();
	pkey_parse_info(argv + 4, info);

	key = get_key_id(argv[1]);
	if (strcmp(argv[2], "0") != 0) {
		fprintf(stderr, "Password passing is not yet supported\n");
		exit(2);
	}
	in = read_file(argv[3], &in_len);

	if (keyctl_pkey_query(key, info, &result) < 0)
		error("keyctl_pkey_query");

	out = malloc(result.max_sig_size);
	if (!out)
		error("malloc");

	out_len = keyctl_pkey_sign(key, info,
				   in, in_len, out, result.max_sig_size);
	if (out_len < 0)
		error("keyctl_pkey_sign");

	if (fwrite(out, out_len, 1, stdout) != 1)
		error("stdout");
	exit(0);
}

/*
 * Verify a signature.
 */
static void act_keyctl_pkey_verify(int argc, char *argv[])
{
	key_serial_t key;
	size_t data_len, sig_len;
	void *data, *sig;
	char info[4096];

	if (argc < 5)
		format();
	pkey_parse_info(argv + 5, info);

	key = get_key_id(argv[1]);
	if (strcmp(argv[2], "0") != 0) {
		fprintf(stderr, "Password passing is not yet supported\n");
		exit(2);
	}
	data = read_file(argv[3], &data_len);
	sig = read_file(argv[4], &sig_len);

	if (keyctl_pkey_verify(key, info,
			       data, data_len, sig, sig_len) < 0)
		error("keyctl_pkey_verify");
	exit(0);
}

/*
 * Move a key between keyrings.
 */
static void act_keyctl_move(int argc, char *argv[])
{
	key_serial_t key, from_keyring, to_keyring;
	unsigned int flags = KEYCTL_MOVE_EXCL;

	if (argc > 4) {
		if (strcmp("-f", argv[1]) == 0) {
			flags &= ~KEYCTL_MOVE_EXCL;
			argc--;
			argv++;
		}
	}

	if (argc != 4)
		format();

	key = get_key_id(argv[1]);
	from_keyring = get_key_id(argv[2]);
	to_keyring = get_key_id(argv[3]);

	if (keyctl_move(key, from_keyring, to_keyring, flags) < 0)
		error("keyctl_move");

	exit(0);
}

struct capability_def {
	const char	*name;	/* Textual name of capability */
	unsigned int	index;	/* Index in capabilities array */
	unsigned char	mask;	/* Mask on capabilities array element */
};

static const struct capability_def capabilities[] = {
	{ "capabilities",		0,	KEYCTL_CAPS0_CAPABILITIES },
	{ "persistent_keyrings",	0,	KEYCTL_CAPS0_PERSISTENT_KEYRINGS },
	{ "dh_compute",			0,	KEYCTL_CAPS0_DIFFIE_HELLMAN },
	{ "public_key",			0,	KEYCTL_CAPS0_PUBLIC_KEY },
	{ "big_key_type",		0,	KEYCTL_CAPS0_BIG_KEY },
	{ "key_invalidate",		0,	KEYCTL_CAPS0_INVALIDATE },
	{ "restrict_keyring",		0,	KEYCTL_CAPS0_RESTRICT_KEYRING },
	{ "move_key",			0,	KEYCTL_CAPS0_MOVE },
	{ "ns_keyring_name",		1,	KEYCTL_CAPS1_NS_KEYRING_NAME },
	{ "ns_key_tag",			1,	KEYCTL_CAPS1_NS_KEY_TAG },
	{ "notify",			1,	KEYCTL_CAPS1_NOTIFICATIONS },
	{}
};

/*
 * Detect/list capabilities.
 */
static void act_keyctl_supports(int argc, char *argv[])
{
	const struct capability_def *p;
	unsigned char caps[256];
	size_t len;
	int i;

	if (argc < 1 || argc > 2)
		format();

	len = keyctl_capabilities(caps, sizeof(caps));
	if (len < 0)
		error("keyctl_capabilities");

	if (argc == 1) {
		for (p = capabilities; p->name; p++)
			printf("have_%s=%c\n",
			       p->name,
			       (caps[p->index] & p->mask) ? '1' : '0');
		exit(0);
	} else {
		if (strcmp(argv[1], "--raw") == 0) {
			for (i = 0; i < len; i++)
				printf("%02x", caps[i]);
			printf("\n");
		}

		for (p = capabilities; p->name; p++)
			if (strcmp(argv[1], p->name) == 0)
				exit((caps[p->index] & p->mask) ? 0 : 1);
		exit(3);
	}
}

/*****************************************************************************/
/*
 * parse a key identifier
 */
key_serial_t get_key_id(char *arg)
{
	key_serial_t id;
	char *end;

	/* handle a special keyring name */
	if (arg[0] == '@') {
		if (strcmp(arg, "@t" ) == 0) return KEY_SPEC_THREAD_KEYRING;
		if (strcmp(arg, "@p" ) == 0) return KEY_SPEC_PROCESS_KEYRING;
		if (strcmp(arg, "@s" ) == 0) return KEY_SPEC_SESSION_KEYRING;
		if (strcmp(arg, "@u" ) == 0) return KEY_SPEC_USER_KEYRING;
		if (strcmp(arg, "@us") == 0) return KEY_SPEC_USER_SESSION_KEYRING;
		if (strcmp(arg, "@g" ) == 0) return KEY_SPEC_GROUP_KEYRING;
		if (strcmp(arg, "@a" ) == 0) return KEY_SPEC_REQKEY_AUTH_KEY;

		fprintf(stderr, "Unknown special key: '%s'\n", arg);
		exit(2);
	}

	/* handle a lookup-by-name request "%<type>:<desc>", eg: "%keyring:_ses" */
	if (arg[0] == '%') {
		char *type;

		arg++;
		if (!*arg)
			goto incorrect_key_by_name_spec;

		if (*arg == ':') {
			type = "keyring";
			arg++;
		} else {
			type = arg;
			arg = strchr(arg, ':');
			if (!arg)
				goto incorrect_key_by_name_spec;
			*(arg++) = '\0';
		}

		if (!*arg)
			goto incorrect_key_by_name_spec;

		id = find_key_by_type_and_desc(type, arg, 0);
		if (id == -1) {
			fprintf(stderr, "Can't find '%s:%s'\n", type, arg);
			exit(1);
		}
		return id;
	}

	/* handle a numeric key ID */
	id = strtoul(arg, &end, 0);
	if (*end) {
		fprintf(stderr, "Unparsable key: '%s'\n", arg);
		exit(2);
	}

	return id;

incorrect_key_by_name_spec:
	fprintf(stderr, "Incorrect key-by-name spec\n");
	exit(2);

} /* end get_key_id() */

/*
 * Read the contents of a file into a buffer and return it.
 */
static void *read_file(const char *name, size_t *_size)
{
	struct stat st;
	ssize_t r;
	void *p;
	int fd;

	fd = open(name, O_RDONLY);
	if (fd < 0)
		error(name);
	if (fstat(fd, &st) < 0)
		error(name);

	p = malloc(st.st_size);
	if (!p)
		error("malloc");
	r = read(fd, p, st.st_size);
	if (r == -1)
		error(name);
	if (r != st.st_size) {
		fprintf(stderr, "%s: Short read\n", name);
		exit(1);
	}
	if (close(fd) < 0)
		error(name);

	*_size = st.st_size;
	return p;
}

/*****************************************************************************/
/*
 * recursively display a key/keyring tree
 */
static int dump_key_tree_aux(key_serial_t key, int depth, int more, int hex_key_IDs)
{
	static char dumpindent[64];
	key_serial_t *pk;
	key_perm_t perm;
	size_t ringlen;
	void *payload;
	char *desc, type[255], pretty_mask[9];
	int uid, gid, ret, n, dpos, rdepth, kcount = 0;

	if (depth > 8 * 4)
		return 0;

	/* read the description */
	ret = keyctl_describe_alloc(key, &desc);
	if (ret < 0) {
		printf("%d: key inaccessible (%m)\n", key);
		return 0;
	}

	/* parse */
	type[0] = 0;
	uid = 0;
	gid = 0;
	perm = 0;

	n = sscanf(desc, "%[^;];%d;%d;%x;%n",
		   type, &uid, &gid, &perm, &dpos);

	if (n != 4) {
		fprintf(stderr, "Unparsable description obtained for key %d\n", key);
		exit(3);
	}

	/* and print */
	calc_perms(pretty_mask, perm, uid, gid);

	if (hex_key_IDs)
		printf("0x%08x %s  %5d %5d  %s%s%s: %s\n",
		       key,
		       pretty_mask,
		       uid, gid,
		       dumpindent,
		       depth > 0 ? "\\_ " : "",
		       type, desc + dpos);
	else
		printf("%10d %s  %5d %5d  %s%s%s: %s\n",
		       key,
		       pretty_mask,
		       uid, gid,
		       dumpindent,
		       depth > 0 ? "\\_ " : "",
		       type, desc + dpos);

	free(desc);

	/* if it's a keyring then we're going to want to recursively
	 * display it if we can */
	if (strcmp(type, "keyring") == 0) {
		/* read its contents */
		ret = keyctl_read_alloc(key, &payload);
		if (ret < 0)
			error("keyctl_read_alloc");

		ringlen = ret;
		kcount = ringlen / sizeof(key_serial_t);

		/* walk the keyring */
		pk = payload;
		while (ringlen >= sizeof(key_serial_t)) {
			key = *pk++;

			/* recurse into next keyrings */
			if (strcmp(type, "keyring") == 0) {
				if (depth == 0) {
					rdepth = depth;
					dumpindent[rdepth++] = ' ';
					dumpindent[rdepth] = 0;
				}
				else {
					rdepth = depth;
					dumpindent[rdepth++] = ' ';
					dumpindent[rdepth++] = ' ';
					dumpindent[rdepth++] = ' ';
					dumpindent[rdepth++] = ' ';
					dumpindent[rdepth] = 0;
				}

				if (more)
					dumpindent[depth + 0] = '|';

				kcount += dump_key_tree_aux(key,
							    rdepth,
							    ringlen - 4 >= sizeof(key_serial_t),
							    hex_key_IDs);
			}

			ringlen -= sizeof(key_serial_t);
		}

		free(payload);
	}

	return kcount;

} /* end dump_key_tree_aux() */

/*****************************************************************************/
/*
 * recursively list a keyring's contents
 */
static int dump_key_tree(key_serial_t keyring, const char *name, int hex_key_IDs)
{
	printf("%s\n", name);

	keyring = keyctl_get_keyring_ID(keyring, 0);
	if (keyring == -1)
		error("Unable to dump key");

	return dump_key_tree_aux(keyring, 0, 0, hex_key_IDs);

} /* end dump_key_tree() */
