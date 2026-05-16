/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public Licence as published by
 * the Free Software Foundation; either version 2 of the Licence, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public Licence for more details.
 */
#define _GNU_SOURCE
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <arpa/inet.h>
#include <limits.h>
#include <resolv.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <syslog.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>
#include <keyutils.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>

#define	MAX_VLS			15	/* Max Volume Location Servers Per-Cell */
#define	INET_IP4_ONLY		0x1
#define	INET_IP6_ONLY		0x2
#define	INET_ALL		0xFF
#define ONE_ADDR_ONLY		0x100

/*
 * key.dns_resolver.c
 */
extern key_serial_t key;
extern int debug_mode;
extern unsigned mask;
extern unsigned int key_expiry;

#define N_PAYLOAD 256
extern struct iovec payload[N_PAYLOAD];
extern int payload_index;

extern __attribute__((format(printf, 1, 2), noreturn))
void error(const char *fmt, ...);
extern __attribute__((format(printf, 1, 2)))
void _error(const char *fmt, ...);
extern __attribute__((format(printf, 1, 2)))
void warning(const char *fmt, ...);
extern __attribute__((format(printf, 1, 2)))
void info(const char *fmt, ...);
extern __attribute__((noreturn))
void nsError(int err, const char *domain);
extern void _nsError(int err, const char *domain);
extern __attribute__((format(printf, 1, 2)))
void debug(const char *fmt, ...);

extern void append_address_to_payload(const char *addr);
extern void dump_payload(void);
extern int dns_resolver(const char *server_name, const char *port);

/*
 * dns.afsdb.c
 */
extern __attribute__((noreturn))
void afs_look_up_VL_servers(const char *cell, char *options);
