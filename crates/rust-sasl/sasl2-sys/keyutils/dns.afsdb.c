/*
 * DNS Resolver Module User-space Helper for AFSDB records
 *
 * Copyright (C) Wang Lei (wang840925@gmail.com) 2010
 * Authors: Wang Lei (wang840925@gmail.com)
 *
 * Copyright (C) David Howells (dhowells@redhat.com) 2018
 *
 * This is a userspace tool for querying AFSDB RR records in the DNS on behalf
 * of the kernel, and converting the VL server addresses to IPv4 format so that
 * they can be used by the kAFS filesystem.
 *
 * As some function like res_init() should use the static library, which is a
 * bug of libresolv, that is the reason for cifs.upcall to reimplement.
 *
 * To use this program, you must tell /sbin/request-key how to invoke it.  You
 * need to have the keyutils package installed and something like the following
 * lines added to your /etc/request-key.conf file:
 *
 * 	#OP    TYPE         DESCRIPTION CALLOUT INFO PROGRAM ARG1 ARG2 ARG3 ...
 * 	====== ============ =========== ============ ==========================
 * 	create dns_resolver afsdb:*     *            /sbin/key.dns_resolver %k
 *
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
#include "key.dns.h"

/*
 *
 */
static void afsdb_hosts_to_addrs(ns_msg handle, ns_sect section)
{
	char *vllist[MAX_VLS];	/* list of name servers	*/
	int vlsnum = 0;		/* number of name servers in list */
	int rrnum;
	ns_rr rr;
	int subtype, i, ret;
	unsigned int ttl = UINT_MAX, rr_ttl;

	debug("AFSDB RR count is %d", ns_msg_count(handle, section));

	/* Look at all the resource records in this section. */
	for (rrnum = 0; rrnum < ns_msg_count(handle, section); rrnum++) {
		/* Expand the resource record number rrnum into rr. */
		if (ns_parserr(&handle, section, rrnum, &rr)) {
			_error("ns_parserr failed : %m");
			continue;
		}

		/* We're only interested in AFSDB records */
		if (ns_rr_type(rr) == ns_t_afsdb) {
			vllist[vlsnum] = malloc(MAXDNAME);
			if (!vllist[vlsnum])
				error("Out of memory");

			subtype = ns_get16(ns_rr_rdata(rr));

			/* Expand the name server's domain name */
			if (ns_name_uncompress(ns_msg_base(handle),
					       ns_msg_end(handle),
					       ns_rr_rdata(rr) + 2,
					       vllist[vlsnum],
					       MAXDNAME) < 0)
				error("ns_name_uncompress failed");

			rr_ttl = ns_rr_ttl(rr);
			if (ttl > rr_ttl)
				ttl = rr_ttl;

			/* Check the domain name we've just unpacked and add it to
			 * the list of VL servers if it is not a duplicate.
			 * If it is a duplicate, just ignore it.
			 */
			for (i = 0; i < vlsnum; i++)
				if (strcasecmp(vllist[i], vllist[vlsnum]) == 0)
					goto next_one;

			/* Turn the hostname into IP addresses */
			ret = dns_resolver(vllist[vlsnum], NULL);
			if (ret) {
				debug("AFSDB RR can't resolve."
				      "subtype:%d, server name:%s, netmask:%u",
				      subtype, vllist[vlsnum], mask);
				goto next_one;
			}

			info("AFSDB RR subtype:%d, server name:%s, ip:%*.*s, ttl:%u",
			     subtype, vllist[vlsnum],
			     (int)payload[payload_index - 1].iov_len,
			     (int)payload[payload_index - 1].iov_len,
			     (char *)payload[payload_index - 1].iov_base,
			     ttl);

			/* prepare for the next record */
			vlsnum++;
			continue;

		next_one:
			free(vllist[vlsnum]);
		}
	}

	key_expiry = ttl;
	info("ttl: %u", key_expiry);
}

/*
 *
 */
static void srv_hosts_to_addrs(ns_msg handle, ns_sect section)
{
	char *vllist[MAX_VLS];	/* list of name servers	*/
	int vlsnum = 0;		/* number of name servers in list */
	int rrnum;
	ns_rr rr;
	int subtype, i, ret;
	unsigned short pref, weight, port;
	unsigned int ttl = UINT_MAX, rr_ttl;
	char sport[8];

	debug("SRV RR count is %d", ns_msg_count(handle, section));

	/* Look at all the resource records in this section. */
	for (rrnum = 0; rrnum < ns_msg_count(handle, section); rrnum++) {
		/* Expand the resource record number rrnum into rr. */
		if (ns_parserr(&handle, section, rrnum, &rr)) {
			_error("ns_parserr failed : %m");
			continue;
		}

		if (ns_rr_type(rr) == ns_t_srv) {
			vllist[vlsnum] = malloc(MAXDNAME);
			if (!vllist[vlsnum])
				error("Out of memory");

			subtype = ns_get16(ns_rr_rdata(rr));

			/* Expand the name server's domain name */
			if (ns_name_uncompress(ns_msg_base(handle),
					       ns_msg_end(handle),
					       ns_rr_rdata(rr) + 6,
					       vllist[vlsnum],
					       MAXDNAME) < 0) {
				_error("ns_name_uncompress failed");
				continue;
			}

			rr_ttl = ns_rr_ttl(rr);
			if (ttl > rr_ttl)
				ttl = rr_ttl;

			pref   = ns_get16(ns_rr_rdata(rr));
			weight = ns_get16(ns_rr_rdata(rr) + 2);
			port   = ns_get16(ns_rr_rdata(rr) + 4);
			info("rdata %u %u %u", pref, weight, port);

			sprintf(sport, "+%hu", port);

			/* Check the domain name we've just unpacked and add it to
			 * the list of VL servers if it is not a duplicate.
			 * If it is a duplicate, just ignore it.
			 */
			for (i = 0; i < vlsnum; i++)
				if (strcasecmp(vllist[i], vllist[vlsnum]) == 0)
					goto next_one;

			/* Turn the hostname into IP addresses */
			ret = dns_resolver(vllist[vlsnum], sport);
			if (ret) {
				debug("SRV RR can't resolve."
				      "subtype:%d, server name:%s, netmask:%u",
				      subtype, vllist[vlsnum], mask);
				goto next_one;
			}

			info("SRV RR subtype:%d, server name:%s, ip:%*.*s, ttl:%u",
			     subtype, vllist[vlsnum],
			     (int)payload[payload_index - 1].iov_len,
			     (int)payload[payload_index - 1].iov_len,
			     (char *)payload[payload_index - 1].iov_base,
			     ttl);

			/* prepare for the next record */
			vlsnum++;
			continue;

		next_one:
			free(vllist[vlsnum]);
		}
	}

	key_expiry = ttl;
	info("ttl: %u", key_expiry);
}

/*
 * Look up an AFSDB record to get the VL server addresses.
 */
static int dns_query_AFSDB(const char *cell)
{
	int	response_len;		/* buffer length */
	ns_msg	handle;			/* handle for response message */
	union {
		HEADER hdr;
		u_char buf[NS_PACKETSZ];
	} response;		/* response buffers */

	debug("Get AFSDB RR for cell name:'%s'", cell);

	/* query the dns for an AFSDB resource record */
	response_len = res_query(cell,
				 ns_c_in,
				 ns_t_afsdb,
				 response.buf,
				 sizeof(response));

	if (response_len < 0) {
		/* negative result */
		_nsError(h_errno, cell);
		return -1;
	}

	if (ns_initparse(response.buf, response_len, &handle) < 0)
		error("ns_initparse: %m");

	/* look up the hostnames we've obtained to get the actual addresses */
	afsdb_hosts_to_addrs(handle, ns_s_an);

	info("DNS query AFSDB RR results:%u ttl:%u", payload_index, key_expiry);
	return 0;
}

/*
 * Look up an SRV record to get the VL server addresses [RFC 5864].
 */
static int dns_query_VL_SRV(const char *cell)
{
	int	response_len;		/* buffer length */
	ns_msg	handle;			/* handle for response message */
	union {
		HEADER hdr;
		u_char buf[NS_PACKETSZ];
	} response;
	char name[1024];

	snprintf(name, sizeof(name), "_afs3-vlserver._udp.%s", cell);

	debug("Get VL SRV RR for name:'%s'", name);

	response_len = res_query(name,
				 ns_c_in,
				 ns_t_srv,
				 response.buf,
				 sizeof(response));

	if (response_len < 0) {
		/* negative result */
		_nsError(h_errno, cell);
		return -1;
	}

	if (ns_initparse(response.buf, response_len, &handle) < 0)
		error("ns_initparse: %m");

	/* look up the hostnames we've obtained to get the actual addresses */
	srv_hosts_to_addrs(handle, ns_s_an);

	info("DNS query VL SRV RR results:%u ttl:%u", payload_index, key_expiry);
	return 0;
}

/*
 * Instantiate the key.
 */
static __attribute__((noreturn))
void afs_instantiate(const char *cell)
{
	int ret;

	/* set the key's expiry time from the minimum TTL encountered */
	if (!debug_mode) {
		ret = keyctl_set_timeout(key, key_expiry);
		if (ret == -1)
			error("%s: keyctl_set_timeout: %m", __func__);
	}

	/* handle a lack of results */
	if (payload_index == 0)
		nsError(NO_DATA, cell);

	/* must include a NUL char at the end of the payload */
	payload[payload_index].iov_base = "";
	payload[payload_index++].iov_len = 1;
	dump_payload();

	/* load the key with data key */
	if (!debug_mode) {
		ret = keyctl_instantiate_iov(key, payload, payload_index, 0);
		if (ret == -1)
			error("%s: keyctl_instantiate: %m", __func__);
	}

	exit(0);
}

/*
 * Look up VL servers for AFS.
 */
void afs_look_up_VL_servers(const char *cell, char *options)
{
	/* Is the IP address family limited? */
	if (strcmp(options, "ipv4") == 0)
		mask = INET_IP4_ONLY;
	else if (strcmp(options, "ipv6") == 0)
		mask = INET_IP6_ONLY;

	if (dns_query_VL_SRV(cell) != 0)
		dns_query_AFSDB(cell);

	afs_instantiate(cell);
}
