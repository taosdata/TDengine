/* Key watching facility.
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
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <ctype.h>
#include <errno.h>
#include <poll.h>
#include <getopt.h>
#include <sys/wait.h>
#include "keyutils.h"
#include <limits.h>
#include "keyctl.h"
#include "watch_queue.h"

#define MAX_MESSAGE_COUNT 256

static int consumer_stop;
static pid_t pid_con = -1, pid_cmd = -1;
static key_serial_t session;
static int watch_fd;
static int debug;

static struct watch_notification_filter filter = {
	.nr_filters	= 0,
	.filters = {
		/* Reserve a slot */
		[0]	= {
			.type			= WATCH_TYPE_KEY_NOTIFY,
		},
	},
};

static inline bool after_eq(unsigned int a, unsigned int b)
{
        return (signed int)(a - b) >= 0;
}

static void consumer_term(int sig)
{
	consumer_stop = 1;
}

static void saw_key_change(FILE *log, struct watch_notification *n,
			   unsigned int len)
{
	struct key_notification *k = (struct key_notification *)n;

	if (len != sizeof(struct key_notification))
		return;

	switch (n->subtype) {
	case NOTIFY_KEY_INSTANTIATED:
		fprintf(log, "%u inst\n", k->key_id);
		break;
	case NOTIFY_KEY_UPDATED:
		fprintf(log, "%u upd\n", k->key_id);
		break;
	case NOTIFY_KEY_LINKED:
		fprintf(log, "%u link %u\n", k->key_id, k->aux);
		break;
	case NOTIFY_KEY_UNLINKED:
		fprintf(log, "%u unlk %u\n", k->key_id, k->aux);
		break;
	case NOTIFY_KEY_CLEARED:
		fprintf(log, "%u clr\n", k->key_id);
		break;
	case NOTIFY_KEY_REVOKED:
		fprintf(log, "%u rev\n", k->key_id);
		break;
	case NOTIFY_KEY_INVALIDATED:
		fprintf(log, "%u inv\n", k->key_id);
		break;
	case NOTIFY_KEY_SETATTR:
		fprintf(log, "%u attr\n", k->key_id);
		break;
	}
}

/*
 * Handle removal notification.
 */
static void saw_removal_notification(FILE *gc, struct watch_notification *n,
				     unsigned int len)
{
	key_serial_t key = 0;
	unsigned int wp;

	wp = (n->info & WATCH_INFO_ID) >> WATCH_INFO_ID__SHIFT;

	if (len >= sizeof(struct watch_notification_removal)) {
		struct watch_notification_removal *r = (void *)n;
		key = r->id;
	}

	fprintf(gc, "%u gc\n", key);
	if (wp == 1)
		exit(0);
}

/*
 * Consume and display events.
 */
static __attribute__((noreturn))
int consumer(FILE *log, FILE *gc, int fd)
{
	unsigned char buffer[433], *p, *end;
	union {
		struct watch_notification n;
		unsigned char buf1[128];
	} n;
	ssize_t buf_len;

	setlinebuf(log);
	setlinebuf(gc);
	signal(SIGTERM, consumer_term);

	do {
		if (!consumer_stop) {
			struct pollfd pf[1];
			pf[0].fd = fd;
			pf[0].events = POLLIN;
			pf[0].revents = 0;

			if (poll(pf, 1, -1) == -1) {
				if (errno == EINTR)
					continue;
				error("poll");
			}
		}

		buf_len = read(fd, buffer, sizeof(buffer));
		if (buf_len == -1) {
			perror("read");
			exit(1);
		}

		if (buf_len == 0) {
			printf("-- END --\n");
			exit(0);
		}

		if (buf_len > sizeof(buffer)) {
			fprintf(stderr, "Read buffer overrun: %zd\n", buf_len);
			exit(4);
		}

		if (debug)
			fprintf(stderr, "read() = %zd\n", buf_len);

		p = buffer;
		end = buffer + buf_len;
		while (p < end) {
			size_t largest, len;

			largest = end - p;
			if (largest > 128)
				largest = 128;
			if (largest < sizeof(struct watch_notification)) {
				fprintf(stderr, "Short message header: %zu\n", largest);
				exit(4);
			}
			memcpy(&n, p, largest);

			if (debug)
				fprintf(stderr, "NOTIFY[%03tx]: ty=%06x sy=%02x i=%08x\n",
					p - buffer, n.n.type, n.n.subtype, n.n.info);

			len = n.n.info & WATCH_INFO_LENGTH;
			if (len < sizeof(n.n) || len > largest) {
				fprintf(stderr, "Bad message length: %zu/%zu\n", len, largest);
				exit(1);
			}

			switch (n.n.type) {
			case WATCH_TYPE_META:
				switch (n.n.subtype) {
				case WATCH_META_REMOVAL_NOTIFICATION:
					saw_removal_notification(gc, &n.n, len);
					break;
				case WATCH_META_LOSS_NOTIFICATION:
					fprintf(log, "-- LOSS --\n");
					break;
				default:
					if (debug)
						fprintf(stderr, "other meta record\n");
					break;
				}
				break;
			case WATCH_TYPE_KEY_NOTIFY:
				saw_key_change(log, &n.n, len);
				break;
			default:
				if (debug)
					fprintf(stderr, "other type\n");
				break;
			}

			p += len;
		}
	} while (!consumer_stop);

	fprintf(log, "Monitoring terminated\n");
	if (gc != log)
		fprintf(gc, "Monitoring terminated\n");
	exit(0);
}

/*
 * Open the watch device and allocate a buffer.
 */
static int open_watch(void)
{
	int pipefd[2], fd;

	if (pipe2(pipefd, O_NOTIFICATION_PIPE | O_NONBLOCK) == -1)
		error("pipe2");

	fd = pipefd[0];

	if (ioctl(fd, IOC_WATCH_QUEUE_SET_SIZE, MAX_MESSAGE_COUNT) == -1)
		error("/dev/watch_queue(size)");

	if (filter.nr_filters &&
	    ioctl(fd, IOC_WATCH_QUEUE_SET_FILTER, &filter) == -1)
		error("/dev/watch_queue(filter)");

	return fd;
}

/*
 * Parse a filter character representation into a subtype number.
 */
static bool parse_subtype(struct watch_notification_type_filter *t, char filter)
{
	static const char filter_mapping[] =
		"i" /* 0 NOTIFY_KEY_INSTANTIATED */
		"p" /* 1 NOTIFY_KEY_UPDATED */
		"l" /* 2 NOTIFY_KEY_LINKED */
		"n" /* 3 NOTIFY_KEY_UNLINKED */
		"c" /* 4 NOTIFY_KEY_CLEARED */
		"r" /* 5 NOTIFY_KEY_REVOKED */
		"v" /* 6 NOTIFY_KEY_INVALIDATED */
		"s" /* 7 NOTIFY_KEY_SETATTR */
		;
	const char *p;
	unsigned int st_bits;
	unsigned int st_index;
	unsigned int st_bit;
	int subtype;

	p = strchr(filter_mapping, filter);
	if (!p)
		return false;

	subtype = p - filter_mapping;
	st_bits = sizeof(t->subtype_filter[0]) * 8;
	st_index = subtype / st_bits;
	st_bit = 1U << (subtype % st_bits);
	t->subtype_filter[st_index] |= st_bit;
	return true;
}

/*
 * Parse filters.
 */
static void parse_watch_filter(char *str)
{
	struct watch_notification_filter *f = &filter;
	struct watch_notification_type_filter *t0 = &f->filters[0];

	f->nr_filters	= 1;
	t0->type	= WATCH_TYPE_KEY_NOTIFY;

	for (; *str; str++) {
		if (parse_subtype(t0, *str))
			continue;
		fprintf(stderr, "Unknown filter character '%c'\n", *str);
		exit(2);
	}
}

/*
 * Watch a key or keyring for changes.
 */
void act_keyctl_watch(int argc, char *argv[])
{
	key_serial_t key;
	int wfd, opt;

	while (opt = getopt(argc, argv, "f:"),
	       opt != -1) {
		switch (opt) {
		case 'f':
			parse_watch_filter(optarg);
			break;
		default:
			fprintf(stderr, "Unknown option\n");
			exit(2);
		}
	}

	argv += optind;
	argc -= optind;
	if (argc != 1)
		format();

	key = get_key_id(argv[0]);
	wfd = open_watch();

	if (keyctl_watch_key(key, wfd, 0x01) == -1)
		error("keyctl_watch_key");

	consumer(stdout, stdout, wfd);
}

/*
 * Add a watch on a key to the monitor created by watch_session.
 */
void act_keyctl_watch_add(int argc, char *argv[])
{
	key_serial_t key;
	int fd;

	if (argc != 3)
		format();

	fd = atoi(argv[1]);
	key = get_key_id(argv[2]);

	if (keyctl_watch_key(key, fd, 0x02) == -1)
		error("keyctl_watch_key");
	exit(0);
}

/*
 * Remove a watch on a key from the monitor created by watch_session.
 */
void act_keyctl_watch_rm(int argc, char *argv[])
{
	key_serial_t key;
	int fd;

	if (argc != 3)
		format();

	fd = atoi(argv[1]);
	key = get_key_id(argv[2]);

	if (keyctl_watch_key(key, fd, -1) == -1)
		error("keyctl_watch_key");
	exit(0);
}

static void exit_cleanup(void)
{
	pid_t me = getpid();
	int w;

	if (me != pid_cmd && me != pid_con) {
		keyctl_watch_key(session, watch_fd, -1);
		if (pid_cmd != -1) {
			kill(pid_cmd, SIGTERM);
			waitpid(pid_cmd, &w, 0);
		}
		if (pid_con != -1) {
			kill(pid_con, SIGTERM);
			waitpid(pid_con, &w, 0);
		}
	}
}

static void run_command(int argc, char *argv[], int wfd)
{
	char buf[16];

	pid_cmd = fork();
	if (pid_cmd == -1)
		error("fork");
	if (pid_cmd != 0)
		return;

	pid_cmd = -1;
	pid_con = -1;

	sprintf(buf, "%u", wfd);
	setenv("KEYCTL_WATCH_FD", buf, true);

	/* run the standard shell if no arguments */
	if (argc == 0) {
		const char *q = getenv("SHELL");
		if (!q)
			q = "/bin/sh";
		execl(q, q, NULL);
		error(q);
	}

	/* run the command specified */
	execvp(argv[0], argv);
	error(argv[0]);
}

/*
 * Open a logfiles.
 */
static FILE *open_logfile(const char *logfile)
{
	unsigned int flags;
	FILE *log;
	int lfd;

	log = fopen(logfile, "a");
	if (!log)
		error(logfile);

	lfd = fileno(log);
	flags = fcntl(lfd, F_GETFD);
	if (flags == -1)
		error("F_GETFD");
	if (fcntl(lfd, F_SETFD, flags | FD_CLOEXEC) == -1)
		error("F_SETFD");

	return log;
}

/*
 * Set up a new session keyring with a monitor that is exposed on an explicit
 * file descriptor in the program that it starts.
 */
void act_keyctl_watch_session(int argc, char *argv[])
{
	const char *session_name = NULL;
	const char *logfile, *gcfile, *target_fd;
	unsigned int flags;
	pid_t pid;
	FILE *log, *gc;
	int wfd, tfd, opt, w, e = 0, e2 = 0;

	while (opt = getopt(argc, argv, "+df:n:"),
	       opt != -1) {
		switch (opt) {
		case 'd':
			debug = 1;
			break;
		case 'f':
			parse_watch_filter(optarg);
			break;
		case 'n':
			session_name = optarg;
			break;
		default:
			fprintf(stderr, "Unknown option\n");
			exit(2);
		}
	}

	argv += optind;
	argc -= optind;

	if (argc < 4)
		format();

	logfile = argv[0];
	gcfile = argv[1];
	target_fd = argv[2];
	tfd = atoi(target_fd);
	if (tfd < 3 || tfd > 9) {
		fprintf(stderr, "The target fd must be between 3 and 9\n");
		exit(2);
	}

	wfd = open_watch();
	if (wfd != tfd) {
		if (dup2(wfd, tfd) == -1)
			error("dup2");
		close(wfd);
		wfd = tfd;
	}
	watch_fd = wfd;

	atexit(exit_cleanup);

	/* We want the fd to be inherited across a fork. */
	flags = fcntl(wfd, F_GETFD);
	if (flags == -1)
		error("F_GETFD");
	if (fcntl(wfd, F_SETFD, flags & ~FD_CLOEXEC) == -1)
		error("F_SETFD");

	log = open_logfile(logfile);
	gc = open_logfile(gcfile);

	pid_con = fork();
	if (pid_con == -1)
		error("fork");
	if (pid_con == 0) {
		pid_cmd = -1;
		pid_con = -1;
		consumer(log, gc, wfd);
	}

	/* Create a new session keyring and watch it. */
	session = keyctl_join_session_keyring(session_name);
	if (session == -1)
		error("keyctl_join_session_keyring");

	if (keyctl_watch_key(session, wfd, 0x01) == -1)
		error("keyctl_watch_key/session");

	fprintf(stderr, "Joined session keyring: %d\n", session);

	/* Start the command and then wait for it to finish and the
	 * notification consumer to clean up.
	 */
	run_command(argc - 3, argv + 3, wfd);
	close(wfd);
	wfd = -1;

	while (pid = wait(&w),
	       pid != -1) {
		if (pid == pid_cmd) {
			if (pid_con != -1)
				kill(pid_con, SIGTERM);
			if (WIFEXITED(w)) {
				e2 = WEXITSTATUS(w);
				pid_cmd = -1;
			} else if (WIFSIGNALED(w)) {
				e2 = WTERMSIG(w) + 128;
				pid_cmd = -1;
			} else if (WIFSTOPPED(w)) {
				raise(WSTOPSIG(w));
			}
		} else if (pid == pid_con) {
			if (pid_cmd != -1)
				kill(pid_cmd, SIGTERM);
			if (WIFEXITED(w)) {
				e = WEXITSTATUS(w);
				pid_con = -1;
			} else if (WIFSIGNALED(w)) {
				e = WTERMSIG(w) + 128;
				pid_con = -1;
			}
		}
	}

	if (e == 0)
		e = e2;
	exit(e);
}

/*
 * Wait for monitoring to synchronise.
 */
void act_keyctl_watch_sync(int argc, char *argv[])
{
	long ret;
	int wfd, count;

	if (argc != 2)
		format();

	wfd = atoi(argv[1]);

	ret = ioctl(wfd, PIPE_IOC_SYNC);
	if (ret == 0)
		exit(0);

	if (ret == -1 && errno != ENOTTY)
		error("ioctl(PIPE_IOC_SYNC)");

	for (;;) {
		ret = ioctl(wfd, FIONREAD, &count);
		if (ret == -1)
			error("ioctl(FIONREAD)");
		if (count == 0)
			break;
		usleep(200 * 1000);
	}

	exit(0);
}
