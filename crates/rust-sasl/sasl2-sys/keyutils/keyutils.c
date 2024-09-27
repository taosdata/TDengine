/* keyutils.c: key utility library
 *
 * Copyright (C) 2005,2011 Red Hat, Inc. All Rights Reserved.
 * Written by David Howells (dhowells@redhat.com)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or (at your option) any later version.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <dlfcn.h>
#include <sys/uio.h>
#include <errno.h>
#include <asm/unistd.h>
#include "keyutils.h"

const char keyutils_version_string[] = PKGVERSION;
const char keyutils_build_string[] = PKGBUILD;

#ifdef NO_GLIBC_KEYERR
static int error_inited;
static void (*libc_perror)(const char *msg);
static char *(*libc_strerror_r)(int errnum, char *buf, size_t n);
//static int (*libc_xpg_strerror_r)(int errnum, char *buf, size_t n);
#define RTLD_NEXT      ((void *) -1L)
#endif

#define __weak __attribute__((weak))

key_serial_t __weak add_key(const char *type,
			    const char *description,
			    const void *payload,
			    size_t plen,
			    key_serial_t ringid)
{
	return syscall(__NR_add_key,
		       type, description, payload, plen, ringid);
}

key_serial_t __weak request_key(const char *type,
				const char *description,
				const char * callout_info,
				key_serial_t destringid)
{
	return syscall(__NR_request_key,
		       type, description, callout_info, destringid);
}

static inline long __keyctl(int cmd,
			    unsigned long arg2,
			    unsigned long arg3,
			    unsigned long arg4,
			    unsigned long arg5)
{
	return syscall(__NR_keyctl,
		       cmd, arg2, arg3, arg4, arg5);
}

long __weak keyctl(int cmd, ...)
{
	va_list va;
	unsigned long arg2, arg3, arg4, arg5;

	va_start(va, cmd);
	arg2 = va_arg(va, unsigned long);
	arg3 = va_arg(va, unsigned long);
	arg4 = va_arg(va, unsigned long);
	arg5 = va_arg(va, unsigned long);
	va_end(va);

	return __keyctl(cmd, arg2, arg3, arg4, arg5);
}

key_serial_t keyctl_get_keyring_ID(key_serial_t id, int create)
{
	return keyctl(KEYCTL_GET_KEYRING_ID, id, create);
}

key_serial_t keyctl_join_session_keyring(const char *name)
{
	return keyctl(KEYCTL_JOIN_SESSION_KEYRING, name);
}

long keyctl_update(key_serial_t id, const void *payload, size_t plen)
{
	return keyctl(KEYCTL_UPDATE, id, payload, plen);
}

long keyctl_revoke(key_serial_t id)
{
	return keyctl(KEYCTL_REVOKE, id);
}

long keyctl_chown(key_serial_t id, uid_t uid, gid_t gid)
{
	return keyctl(KEYCTL_CHOWN, id, uid, gid);
}

long keyctl_setperm(key_serial_t id, key_perm_t perm)
{
	return keyctl(KEYCTL_SETPERM, id, perm);
}

long keyctl_describe(key_serial_t id, char *buffer, size_t buflen)
{
	return keyctl(KEYCTL_DESCRIBE, id, buffer, buflen);
}

long keyctl_clear(key_serial_t ringid)
{
	return keyctl(KEYCTL_CLEAR, ringid);
}

long keyctl_link(key_serial_t id, key_serial_t ringid)
{
	return keyctl(KEYCTL_LINK, id, ringid);
}

long keyctl_unlink(key_serial_t id, key_serial_t ringid)
{
	return keyctl(KEYCTL_UNLINK, id, ringid);
}

long keyctl_search(key_serial_t ringid,
		   const char *type,
		   const char *description,
		   key_serial_t destringid)
{
	return keyctl(KEYCTL_SEARCH, ringid, type, description, destringid);
}

long keyctl_read(key_serial_t id, char *buffer, size_t buflen)
{
	return keyctl(KEYCTL_READ, id, buffer, buflen);
}

long keyctl_instantiate(key_serial_t id,
			const void *payload,
			size_t plen,
			key_serial_t ringid)
{
	return keyctl(KEYCTL_INSTANTIATE, id, payload, plen, ringid);
}

long keyctl_negate(key_serial_t id, unsigned timeout, key_serial_t ringid)
{
	return keyctl(KEYCTL_NEGATE, id, timeout, ringid);
}

long keyctl_set_reqkey_keyring(int reqkey_defl)
{
	return keyctl(KEYCTL_SET_REQKEY_KEYRING, reqkey_defl);
}

long keyctl_set_timeout(key_serial_t id, unsigned timeout)
{
	return keyctl(KEYCTL_SET_TIMEOUT, id, timeout);
}

long keyctl_assume_authority(key_serial_t id)
{
	return keyctl(KEYCTL_ASSUME_AUTHORITY, id);
}

long keyctl_get_security(key_serial_t id, char *buffer, size_t buflen)
{
	return keyctl(KEYCTL_GET_SECURITY, id, buffer, buflen);
}

long keyctl_session_to_parent(void)
{
	return keyctl(KEYCTL_SESSION_TO_PARENT);
}

long keyctl_reject(key_serial_t id, unsigned timeout, unsigned error,
		   key_serial_t ringid)
{
	long ret = keyctl(KEYCTL_REJECT, id, timeout, error, ringid);

	/* fall back to keyctl_negate() if this op is not supported by this
	 * kernel version */
	if (ret == -1 && errno == EOPNOTSUPP)
		return keyctl_negate(id, timeout, ringid);
	return ret;
}

long keyctl_instantiate_iov(key_serial_t id,
			    const struct iovec *payload_iov,
			    unsigned ioc,
			    key_serial_t ringid)
{
	long ret = keyctl(KEYCTL_INSTANTIATE_IOV, id, payload_iov, ioc, ringid);

	/* fall back to keyctl_instantiate() if this op is not supported by
	 * this kernel version */
	if (ret == -1 && errno == EOPNOTSUPP) {
		unsigned loop;
		size_t bsize = 0, seg;
		void *buf, *p;

		if (!payload_iov || !ioc)
			return keyctl_instantiate(id, NULL, 0, ringid);
		for (loop = 0; loop < ioc; loop++)
			bsize += payload_iov[loop].iov_len;
		if (bsize == 0)
			return keyctl_instantiate(id, NULL, 0, ringid);
		p = buf = malloc(bsize);
		if (!buf)
			return -1;
		for (loop = 0; loop < ioc; loop++) {
			seg = payload_iov[loop].iov_len;
			p = memcpy(p, payload_iov[loop].iov_base, seg) + seg;
		}
		ret = keyctl_instantiate(id, buf, bsize, ringid);
		free(buf);
	}
	return ret;
}

long keyctl_invalidate(key_serial_t id)
{
	return keyctl(KEYCTL_INVALIDATE, id);
}

long keyctl_get_persistent(uid_t uid, key_serial_t id)
{
	return keyctl(KEYCTL_GET_PERSISTENT, uid, id);
}

long keyctl_dh_compute(key_serial_t priv, key_serial_t prime,
		       key_serial_t base, char *buffer, size_t buflen)
{
	struct keyctl_dh_params params = { .priv = priv,
					   .prime = prime,
					   .base = base };

	return keyctl(KEYCTL_DH_COMPUTE, &params, buffer, buflen, 0);
}

long keyctl_dh_compute_kdf(key_serial_t priv, key_serial_t prime,
			   key_serial_t base, char *hashname, char *otherinfo,
			   size_t otherinfolen, char *buffer, size_t buflen)
{
	struct keyctl_dh_params params = { .priv = priv,
					   .prime = prime,
					   .base = base };
	struct keyctl_kdf_params kdfparams = { .hashname = hashname,
					       .otherinfo = otherinfo,
					       .otherinfolen = otherinfolen };

	return keyctl(KEYCTL_DH_COMPUTE, &params, buffer, buflen, &kdfparams);
}

long keyctl_restrict_keyring(key_serial_t keyring, const char *type,
			     const char *restriction)
{
	return keyctl(KEYCTL_RESTRICT_KEYRING, keyring, type, restriction);
}

long keyctl_pkey_query(key_serial_t key_id,
		       const char *info,
		       struct keyctl_pkey_query *result)
{
	return keyctl(KEYCTL_PKEY_QUERY, key_id, NULL, info, result);
}

long keyctl_pkey_encrypt(key_serial_t key_id,
			 const char *info,
			 const void *data, size_t data_len,
			 void *enc, size_t enc_len)
{
	struct keyctl_pkey_params params = {
		.key_id		= key_id,
		.in_len		= data_len,
		.out_len	= enc_len,
	};

	return keyctl(KEYCTL_PKEY_ENCRYPT, &params, info, data, enc);
}

long keyctl_pkey_decrypt(key_serial_t key_id,
			 const char *info,
			 const void *enc, size_t enc_len,
			 void *data, size_t data_len)
{
	struct keyctl_pkey_params params = {
		.key_id		= key_id,
		.in_len		= enc_len,
		.out_len	= data_len,
	};

	return keyctl(KEYCTL_PKEY_DECRYPT, &params, info, enc, data);
}

long keyctl_pkey_sign(key_serial_t key_id,
		      const char *info,
		      const void *data, size_t data_len,
		      void *sig, size_t sig_len)
{
	struct keyctl_pkey_params params = {
		.key_id		= key_id,
		.in_len		= data_len,
		.out_len	= sig_len,
	};

	return keyctl(KEYCTL_PKEY_SIGN, &params, info, data, sig);
}

long keyctl_pkey_verify(key_serial_t key_id,
			const char *info,
			const void *data, size_t data_len,
			const void *sig, size_t sig_len)
{
	struct keyctl_pkey_params params = {
		.key_id		= key_id,
		.in_len		= data_len,
		.in2_len	= sig_len,
	};

	return keyctl(KEYCTL_PKEY_VERIFY, &params, info, data, sig);
}

long keyctl_move(key_serial_t id,
		 key_serial_t from_ringid,
		 key_serial_t to_ringid,
		 unsigned int flags)
{
	return keyctl(KEYCTL_MOVE, id, from_ringid, to_ringid, flags);
}

long keyctl_capabilities(unsigned char *buffer, size_t buflen)
{
	long n;

	n = keyctl(KEYCTL_CAPABILITIES, buffer, buflen);
	if (n != -1 || errno != EOPNOTSUPP)
		return n;

	/* Emulate the operation */
	if (buflen > 0) {
		memset(buffer, 0, buflen);

		errno = 0;
		keyctl_get_persistent(-1, 0);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_PERSISTENT_KEYRINGS;

		errno = 0;
		keyctl_dh_compute(0, 0, 0, NULL, 0);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_DIFFIE_HELLMAN;

		errno = 0;
		keyctl_pkey_query(0, NULL, NULL);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_PUBLIC_KEY;

		/* Can't emulate KEYCTL_CAPS0_BIG_KEY without a valid
		 * destination keyring.
		 */

		errno = 0;
		keyctl_invalidate(0);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_INVALIDATE;

		errno = 0;
		keyctl_restrict_keyring(0, NULL, NULL);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_RESTRICT_KEYRING;

		errno = 0;
		keyctl_move(0, 0, 0, 0);
		if (errno != EOPNOTSUPP)
			buffer[0] |= KEYCTL_CAPS0_MOVE;
	}

	return sizeof(unsigned char);
}

long keyctl_watch_key(key_serial_t id, int watch_queue_fd, int watch_id)
{
	return keyctl(KEYCTL_WATCH_KEY, id, watch_queue_fd, watch_id);
}

/*****************************************************************************/
/*
 * fetch key description into an allocated buffer
 * - resulting string is NUL terminated
 * - returns count not including NUL
 */
int keyctl_describe_alloc(key_serial_t id, char **_buffer)
{
	char *buf;
	long buflen, ret;

	ret = keyctl_describe(id, NULL, 0);
	if (ret < 0)
		return -1;

	for (;;) {
		buflen = ret;
		buf = malloc(buflen);
		if (!buf)
			return -1;

		ret = keyctl_describe(id, buf, buflen);
		if (ret < 0) {
			free(buf);
			return -1;
		}

		if (buflen >= ret)
			break;
		free(buf);
	}

	*_buffer = buf;
	return ret - 1;
}

/*****************************************************************************/
/*
 * fetch key contents into an allocated buffer
 * - resulting buffer has an extra NUL added to the end
 * - returns count (not including extraneous NUL)
 */
int keyctl_read_alloc(key_serial_t id, void **_buffer)
{
	char *buf;
	long buflen, ret;

	ret = keyctl_read(id, NULL, 0);
	if (ret < 0)
		return -1;

	for (;;) {
		buflen = ret;
		buf = malloc(buflen + 1);
		if (!buf)
			return -1;

		ret = keyctl_read(id, buf, buflen);
		if (ret < 0) {
			free(buf);
			return -1;
		}

		if (buflen >= ret)
			break;
		free(buf);
	}

	buf[ret] = 0;
	*_buffer = buf;
	return ret;
}

/*****************************************************************************/
/*
 * fetch key security label into an allocated buffer
 * - resulting string is NUL terminated
 * - returns count not including NUL
 */
int keyctl_get_security_alloc(key_serial_t id, char **_buffer)
{
	char *buf;
	long buflen, ret;

	ret = keyctl_get_security(id, NULL, 0);
	if (ret < 0)
		return -1;

	for (;;) {
		buflen = ret;
		buf = malloc(buflen);
		if (!buf)
			return -1;

		ret = keyctl_get_security(id, buf, buflen);
		if (ret < 0) {
			free(buf);
			return -1;
		}

		if (buflen >= ret)
			break;
		free(buf);
	}

	*_buffer = buf;
	return ret - 1;
}

/*****************************************************************************/
/*
 * fetch DH computation results into an allocated buffer
 * - resulting buffer has an extra NUL added to the end
 * - returns count (not including extraneous NUL)
 */
int keyctl_dh_compute_alloc(key_serial_t priv, key_serial_t prime,
			    key_serial_t base, void **_buffer)
{
	char *buf;
	long buflen, ret;

	ret = keyctl_dh_compute(priv, prime, base, NULL, 0);
	if (ret < 0)
		return -1;

	buflen = ret;
	buf = malloc(buflen + 1);
	if (!buf)
		return -1;

	ret = keyctl_dh_compute(priv, prime, base, buf, buflen);
	if (ret < 0) {
		free(buf);
		return -1;
	}

	buf[ret] = 0;
	*_buffer = buf;
	return ret;
}

/*
 * Depth-first recursively apply a function over a keyring tree
 */
static int recursive_key_scan_aux(key_serial_t parent, key_serial_t key,
				  int depth, recursive_key_scanner_t func,
				  void *data)
{
	key_serial_t *pk;
	key_perm_t perm;
	size_t ringlen;
	void *ring;
	char *desc, type[255];
	int desc_len, uid, gid, ret, n, kcount = 0;

	if (depth > 800)
		return 0;

	/* read the key description */
	desc = NULL;
	desc_len = keyctl_describe_alloc(key, &desc);
	if (desc_len < 0)
		goto do_this_key;

	/* parse */
	type[0] = 0;

	n = sscanf(desc, "%[^;];%d;%d;%x;", type, &uid, &gid, &perm);
	if (n != 4) {
		free(desc);
		desc = NULL;
		errno = -EINVAL;
		desc_len = -1;
		goto do_this_key;
	}

	/* if it's a keyring then we're going to want to recursively search it
	 * if we can */
	if (strcmp(type, "keyring") == 0) {
		/* read the keyring's contents */
		ret = keyctl_read_alloc(key, &ring);
		if (ret < 0)
			goto do_this_key;

		ringlen = ret;

		/* walk the keyring */
		pk = ring;
		for (ringlen = ret;
		     ringlen >= sizeof(key_serial_t);
		     ringlen -= sizeof(key_serial_t)
		     )
			kcount += recursive_key_scan_aux(key, *pk++, depth + 1,
							 func, data);

		free(ring);
	}

do_this_key:
	kcount += func(parent, key, desc, desc_len, data);
	free(desc);
	return kcount;
}

/*
 * Depth-first apply a function over a keyring tree
 */
int recursive_key_scan(key_serial_t key, recursive_key_scanner_t func, void *data)
{
	return recursive_key_scan_aux(0, key, 0, func, data);
}

/*
 * Depth-first apply a function over session keyring tree
 */
int recursive_session_key_scan(recursive_key_scanner_t func, void *data)
{
	key_serial_t session =
		keyctl_get_keyring_ID(KEY_SPEC_SESSION_KEYRING, 0);
	if (session > 0)
		return recursive_key_scan(session, func, data);
	return 0;
}

/*
 * Find a key by type and description
 */
key_serial_t find_key_by_type_and_desc(const char *type, const char *desc,
				       key_serial_t destringid)
{
	key_serial_t id, error;
	FILE *f;
	char buf[1024], typebuf[40], rdesc[1024], *kdesc, *cp;
	int n, ndesc, dlen;

	error = ENOKEY;

	id = request_key(type, desc, NULL, destringid);
	if (id >= 0 || errno == ENOMEM)
		return id;
	if (errno != ENOKEY)
		error = errno;

	dlen = strlen(desc);

	f = fopen("/proc/keys", "r");
	if (!f) {
		fprintf(stderr, "libkeyutils: Can't open /proc/keys: %m\n");
		return -1;
	}

	while (fgets(buf, sizeof(buf), f)) {
		cp = strchr(buf, '\n');
		if (*cp)
			*cp = '\0';

		ndesc = 0;
		n = sscanf(buf, "%x %*s %*u %*s %*x %*d %*d %s %n",
			   &id, typebuf, &ndesc);
		if (n == 2 && ndesc > 0 && ndesc <= cp - buf) {
			if (strcmp(typebuf, type) != 0)
				continue;
			kdesc = buf + ndesc;
			if (memcmp(kdesc, desc, dlen) != 0)
				continue;
			if (kdesc[dlen] != ':' &&
			    kdesc[dlen] != '\0' &&
			    kdesc[dlen] != ' ')
				continue;
			kdesc[dlen] = '\0';

			/* The key type appends extra stuff to the end of the
			 * description after a colon in /proc/keys.  Colons,
			 * however, are allowed in descriptions, so we need to
			 * make a further check. */
			n = keyctl_describe(id, rdesc, sizeof(rdesc) - 1);
			if (n == -1) {
				if (errno != ENOKEY)
					error = errno;
				if (errno == ENOMEM)
					break;
			}
			if (n >= sizeof(rdesc) - 1)
				continue;
			rdesc[n] = '\0';

			cp = strrchr(rdesc, ';');
			if (!cp)
				continue;
			cp++;
			if (strcmp(cp, desc) != 0)
				continue;

			fclose(f);

			if (destringid &&
			    keyctl_link(id, destringid) == -1)
				return -1;

			return id;
		}
	}

	fclose(f);
	errno = error;
	return -1;
}

#ifdef NO_GLIBC_KEYERR
/*****************************************************************************/
/*
 * initialise error handling
 */
static void error_init(void)
{
	char *err;

	error_inited = 1;

	dlerror();

	libc_perror = dlsym(RTLD_NEXT,"perror");
	if (!libc_perror) {
		fprintf(stderr, "Failed to look up next perror\n");
		err = dlerror();
		if (err)
			fprintf(stderr, "%s\n", err);
		abort();
	}

	//fprintf(stderr, "next perror at %p\n", libc_perror);

	libc_strerror_r = dlsym(RTLD_NEXT,"strerror_r");
	if (!libc_strerror_r) {
		fprintf(stderr, "Failed to look up next strerror_r\n");
		err = dlerror();
		if (err)
			fprintf(stderr, "%s\n", err);
		abort();
	}

	//fprintf(stderr, "next strerror_r at %p\n", libc_strerror_r);

#if 0
	libc_xpg_strerror_r = dlsym(RTLD_NEXT,"xpg_strerror_r");
	if (!libc_xpg_strerror_r) {
		fprintf(stderr, "Failed to look up next xpg_strerror_r\n");
		err = dlerror();
		if (err)
			fprintf(stderr, "%s\n", err);
		abort();
	}

	//fprintf(stderr, "next xpg_strerror_r at %p\n", libc_xpg_strerror_r);
#endif

} /* end error_init() */

/*****************************************************************************/
/*
 * overload glibc's strerror_r() with a version that knows about key errors
 */
char *strerror_r(int errnum, char *buf, size_t n)
{
	const char *errstr;
	int len;

	printf("hello\n");

	if (!error_inited)
		error_init();

	switch (errnum) {
	case ENOKEY:
		errstr = "Requested key not available";
		break;

	case EKEYEXPIRED:
		errstr = "Key has expired";
		break;

	case EKEYREVOKED:
		errstr = "Key has been revoked";
		break;

	case EKEYREJECTED:
		errstr = "Key was rejected by service";
		break;

	default:
		return libc_strerror_r(errnum, buf, n);
	}

	len = strlen(errstr) + 1;
	if (n > len) {
		errno = ERANGE;
		if (n > 0) {
			memcpy(buf, errstr, n - 1);
			buf[n - 1] = 0;
		}
		return NULL;
	}
	else {
		memcpy(buf, errstr, len);
		return buf;
	}

} /* end strerror_r() */

#if 0
/*****************************************************************************/
/*
 * overload glibc's strerror_r() with a version that knows about key errors
 */
int xpg_strerror_r(int errnum, char *buf, size_t n)
{
	const char *errstr;
	int len;

	if (!error_inited)
		error_init();

	switch (errnum) {
	case ENOKEY:
		errstr = "Requested key not available";
		break;

	case EKEYEXPIRED:
		errstr = "Key has expired";
		break;

	case EKEYREVOKED:
		errstr = "Key has been revoked";
		break;

	case EKEYREJECTED:
		errstr = "Key was rejected by service";
		break;

	default:
		return libc_xpg_strerror_r(errnum, buf, n);
	}

	len = strlen(errstr) + 1;
	if (n > len) {
		errno = ERANGE;
		if (n > 0) {
			memcpy(buf, errstr, n - 1);
			buf[n - 1] = 0;
		}
		return -1;
	}
	else {
		memcpy(buf, errstr, len);
		return 0;
	}

} /* end xpg_strerror_r() */
#endif

/*****************************************************************************/
/*
 *
 */
void perror(const char *msg)
{
	if (!error_inited)
		error_init();

	switch (errno) {
	case ENOKEY:
		fprintf(stderr, "%s: Requested key not available\n", msg);
		return;

	case EKEYEXPIRED:
		fprintf(stderr, "%s: Key has expired\n", msg);
		return;

	case EKEYREVOKED:
		fprintf(stderr, "%s: Key has been revoked\n", msg);
		return;

	case EKEYREJECTED:
		fprintf(stderr, "%s: Key was rejected by service\n", msg);
		return;

	default:
		libc_perror(msg);
		return;
	}

} /* end perror() */
#endif
