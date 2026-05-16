/*
 * Copyright (c) 1991, by Sun Microsystems, Inc.
 */

#ifndef	_KERBEROS_ERROR_TABLE_H
#define	_KERBEROS_ERROR_TABLE_H

#pragma ident	"@(#)error_table.h	1.4	93/08/30 SMI"

#ifdef	__cplusplus
extern "C" {
#endif

typedef struct {
	char	**msgs;
	int	base;
	int	n_msgs;
} error_table;
extern error_table **_et_list;

#define	ERROR_CODE	"int"	/* type used for error codes */

#define	ERRCODE_RANGE	8	/* # of bits to shift table number */
#define	BITS_PER_CHAR	6	/* # bits to shift per character in name */

extern char *error_table_name();

#ifdef	__cplusplus
}
#endif

#endif	/* _KERBEROS_ERROR_TABLE_H */
