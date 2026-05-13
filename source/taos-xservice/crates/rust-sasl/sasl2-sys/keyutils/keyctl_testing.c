/* Tests built into the keyctl program
 *
 * Copyright (C) 2019 Red Hat, Inc. All Rights Reserved.
 * Written by David Howells (dhowells@redhat.com)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public Licence
 * as published by the Free Software Foundation; either version
 * 2 of the Licence, or (at your option) any later version.
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

static nr void act_keyctl_test_limits(int, char *[]);
static nr void act_keyctl_test_limits2(int, char *[]);

static const struct command test_commands[] = {
	{ act_keyctl_test_limits,	"limits",	"" },
	{ act_keyctl_test_limits2,	"limits2",	"" },
	{ NULL,				NULL,		NULL }
};

static void test_format(void) __attribute__((noreturn));
static void test_format(void)
{
	const struct command *cmd;

	fprintf(stderr, "Format:\n");

	for (cmd = test_commands; cmd->name; cmd++)
		fprintf(stderr, "  keyctl --test %s %s\n", cmd->name, cmd->format);

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
}

/*
 * Provide some testing functions for "keyctl --test"
 */
void act_keyctl_test(int argc, char *argv[])
{
	if (argc < 2)
		test_format();

	do_command(argc, argv, test_commands, "test ");
}

/*
 * Test the limits of the type and description fields in add_user().
 */
static void act_keyctl_test_limits(int argc, char *argv[])
{
	key_serial_t key;
	char buf[8192];
	int i, nr_fail = 0;

	if (argc != 1)
		test_format();

	setvbuf(stdout, NULL, _IONBF, 0);

	for (i = 0; i < sizeof(buf); i++) {
		if (i % 32 == 0) {
			if (i != 0)
				putchar('\n');
			printf("TEST SIZE %d", i);
		}

		buf[i] = 0;

		putchar('.');
		if (add_key(buf, "wibble", "a", 1, KEY_SPEC_THREAD_KEYRING) == -1) {
			if (i == 0 || i >= 32) {
				if (errno != EINVAL) {
					putchar('\n');
					fprintf(stderr, "%d type failed: %m\n", i);
					nr_fail++;
				}
			} else {
				if (errno != ENODEV) {
					putchar('\n');
					fprintf(stderr, "%d type failed: %m\n", i);
					nr_fail++;
				}
			}
		} else {
			putchar('\n');
			fprintf(stderr, "%d type unexpectedly succeeded\n", i);
			nr_fail++;
		}

		putchar('_');
		key = add_key("user", buf, "a", 1, KEY_SPEC_THREAD_KEYRING);
		if (key == -1) {
			if (i == 0 || i >= 4096) {
				if (errno != EINVAL) {
					putchar('\n');
					fprintf(stderr, "%d desc failed: %m\n", i);
					nr_fail++;
				}
			} else {
				putchar('\n');
				fprintf(stderr, "%d desc wrong error: %m\n", i);
				nr_fail++;
			}
		} else {
			if (i == 0 || i >= 4096) {
				putchar('\n');
				fprintf(stderr, "%d desc unexpectedly succeeded\n", i);
				nr_fail++;
			}

			if (keyctl_unlink(key, KEY_SPEC_THREAD_KEYRING) == -1) {
				putchar('\n');
				fprintf(stderr, "Unlink failed: %m\n");
				nr_fail++;
			}
		}

		buf[i] = 'a';
		if (nr_fail > 20) {
			fprintf(stderr, "Aborting with too many failures\n");
			exit(1);
		}
	}

	putchar('\n');
	exit(nr_fail ? 1 : 0);
}

/*
 * Test the limits of the payload field in add_user().  The user-type will only
 * accept sizes in the range 1-32767 bytes, though add_key() will accept up to
 * just shy of 1MiB.
 */
static void act_keyctl_test_limits2(int argc, char *argv[])
{
	key_serial_t key;
	char buf[1030 * 1024];
	int i, nr_fail = 0;

	if (argc != 1)
		test_format();

	setvbuf(stdout, NULL, _IONBF, 0);
	memset(buf, 'a', sizeof(buf));

	for (i = 0; i < sizeof(buf); i++) {
		if (i % 2048 == 0) {
			if (i != 0)
				putchar('\n');
			printf("TEST SIZE %7d ", i);
		}

		if (i % (2048 / 32) == 0)
			putchar('.');

		key = add_key("user", "a", buf, i, KEY_SPEC_THREAD_KEYRING);
		if (key == -1) {
			if (i == 0 || i > 32767) {
				if (errno != EINVAL) {
					putchar('\n');
					fprintf(stderr, "%d desc failed: %m\n", i);
					nr_fail++;
				}
			} else if (errno == EDQUOT) {
				/* This might happen due to us creating keys
				 * really fast.
				 */
			} else {
				putchar('\n');
				fprintf(stderr, "%d desc wrong error: %m\n", i);
				nr_fail++;
			}
		} else {
			if (i == 0 || i > 32767) {
				putchar('\n');
				fprintf(stderr, "%d desc unexpectedly succeeded\n", i);
				nr_fail++;
			}

			if (keyctl_unlink(key, KEY_SPEC_THREAD_KEYRING) == -1) {
				putchar('\n');
				fprintf(stderr, "Unlink failed: %m\n");
				nr_fail++;
			}
		}

		if (nr_fail > 20) {
			fprintf(stderr, "Aborting with too many failures\n");
			exit(1);
		}
	}

	putchar('\n');
	exit(nr_fail ? 1 : 0);
}
