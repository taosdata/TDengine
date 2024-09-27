/* dbconverter-2.c -- convert libsasl v1 sasldb's to SASLv2 format
 * Rob Siemborski
 * based on SASLv1 sasldblistusers
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

#include <config.h>

#include <stdio.h>
#include <stdlib.h>

#include <sasl.h>
#include <saslplug.h>
#include "../sasldb/sasldb.h"

/* Cheating to make the utils work out right */
extern const sasl_utils_t *sasl_global_utils;
sasl_conn_t *globalconn;

typedef void *listcb_t(const char *, const char *, const char *,
		       const char *, unsigned);

void listusers_cb(const char *authid, const char *realm,
		  const char *mechanism, const char *secret,
		  unsigned seclen)
{
    char newPropBuffer[8192];

    if (!authid || !mechanism || !realm) {
	fprintf(stderr,"userlist callback has bad param");
	return;
    }

    /* the entries that just say the mechanism exists */
    if (strlen(authid)==0) return;

    printf("Converting: %s@%s (%s)...",authid,realm,mechanism);

    /* Maybe we have a plaintext password? */
    if(!strcmp(mechanism,"PLAIN-APOP")) {
	sprintf(newPropBuffer, "userPassword");
	/* Skip salt + NULL */
	secret = secret + 17;
	seclen -= 17;
    } else {
	sprintf(newPropBuffer, "cmusaslsecret%s", mechanism);
    }
    
    _sasldb_putdata(sasl_global_utils, globalconn,
		    authid, realm, newPropBuffer,
		    secret, seclen);

    printf("ok\n");
}

/*
 * List all users in database
 */

#if defined(SASL_GDBM)

#include <gdbm.h>
#include <fcntl.h>
#include <sys/stat.h>

int listusers(const char *path, listcb_t *cb)
{
    GDBM_FILE indb;
    datum dkey, nextkey, dvalue;

    indb = gdbm_open((char *)path, 0, GDBM_READER, S_IRUSR | S_IWUSR, NULL);

    if (!indb) {
	fprintf(stderr, "can't open %s\n", path);
	return 1;
    }

    memset(&dkey, 0, sizeof(datum));

    dkey = gdbm_firstkey(indb);

    while (dkey.dptr != NULL) {
	char *authid = dkey.dptr;
	char *realm  = dkey.dptr+strlen(authid)+1;
	char *tmp    = realm + strlen(realm)+1;
	char mech[1024];
	int len = dkey.dsize - (tmp - ((char *)dkey.dptr));

	if (len >= (int) sizeof mech) {
	    fprintf(stderr, "malformed database entry\n");
	    break;
	}
	memcpy(mech, tmp, len);
	mech[dkey.dsize - (tmp - dkey.dptr)] = '\0';

	dvalue = gdbm_fetch(indb, dkey);

	if (*authid && dvalue.dptr) {
	    /* don't check return values */
	    cb(authid,realm,mech,dvalue.dptr,dvalue.dsize);
	}

	nextkey=gdbm_nextkey(indb, dkey);
	dkey=nextkey;
    }

    gdbm_close(indb);
    return 0;
}

#elif defined(SASL_NDBM)

#include <ndbm.h>
#include <fcntl.h>
#include <sys/stat.h>

int listusers(const char *path, listcb_t *cb)
{
    DBM *indb;
    datum dkey, nextkey, dvalue;

    indb = dbm_open(path, O_RDONLY, S_IRUSR | S_IWUSR);

    if (!indb) {
	fprintf(stderr, "can't open %s\n", path);
	return 1;
    }

    dkey = dbm_firstkey(indb);

    while (dkey.dptr != NULL) {
	char *authid = dkey.dptr;
	char *realm  = dkey.dptr+strlen(authid)+1;
	char *tmp    = realm + strlen(realm)+1;
	char mech[1024];
	int len = dkey.dsize - (tmp - ((char *)dkey.dptr));

	if (len >= (int) sizeof mech) {
	    fprintf(stderr, "malformed database entry\n");
	    break;
	}
	memcpy(mech, tmp, len);
	mech[dkey.dsize - (tmp - ((char *)dkey.dptr))] = '\0';

	dvalue = dbm_fetch(indb, dkey);

	if (*authid && dvalue.dptr) {
	    /* don't check return values */
	    cb(authid,realm,mech,dvalue.dptr,dvalue.dsize);
	}

	nextkey=dbm_nextkey(indb);
	dkey=nextkey;
    }

    dbm_close(indb);
    return 0;
}

#elif defined(SASL_BERKELEYDB)

#include <db.h>

#define DB_VERSION_FULL ((DB_VERSION_MAJOR << 24) | (DB_VERSION_MINOR << 16) | DB_VERSION_PATCH)
/*
 * Open the database
 *
 */
static int berkeleydb_open(const char *path,DB **mbdb)
{
    int ret;

#if DB_VERSION_FULL < 0x03000000
    ret = db_open(path, DB_HASH, DB_CREATE, 0664, NULL, NULL, mbdb);
#else /* DB_VERSION_FULL < 0x03000000 */
    ret = db_create(mbdb, NULL, 0);
    if (ret == 0 && *mbdb != NULL)
    {
#if DB_VERSION_FULL >= 0x04010000
	ret = (*mbdb)->open(*mbdb, NULL, path, NULL, DB_HASH, DB_CREATE, 0664);
#else
	ret = (*mbdb)->open(*mbdb, path, NULL, DB_HASH, DB_CREATE, 0664);
#endif
	if (ret != 0)
	{
	    (void) (*mbdb)->close(*mbdb, 0);
	    *mbdb = NULL;
	}
    }
#endif /* DB_VERSION_FULL < 0x03000000 */

    if (ret != 0) {
	fprintf(stderr,"Error opening password file %s\n", path);
	return SASL_FAIL;
    }

    return SASL_OK;
}

/*
 * Close the database
 *
 */

static void berkeleydb_close(DB *mbdb)
{
    int ret;
    
    ret = mbdb->close(mbdb, 0);
    if (ret!=0) {
	fprintf(stderr,"error closing sasldb: %s",
		db_strerror(ret));
    }
}

int listusers(const char *path, listcb_t *cb)
{
    int result;
    DB *mbdb = NULL;
    DBC *cursor;
    DBT key, data;

    /* open the db */
    result=berkeleydb_open(path, &mbdb);
    if (result!=SASL_OK) goto cleanup;

    /* make cursor */
#if DB_VERSION_FULL < 0x03060000
    result = mbdb->cursor(mbdb, NULL,&cursor); 
#else
    result = mbdb->cursor(mbdb, NULL,&cursor, 0); 
#endif /* DB_VERSION_FULL < 0x03060000 */

    if (result!=0) {
	fprintf(stderr,"Making cursor failure: %s\n",db_strerror(result));
      result = SASL_FAIL;
      goto cleanup;
    }

    memset(&key,0, sizeof(key));
    memset(&data,0,sizeof(data));

    /* loop thru */
    result = cursor->c_get(cursor, &key, &data,
			   DB_FIRST);

    while (result != DB_NOTFOUND)
    {
	char *authid;
	char *realm;
	char *tmp;
	unsigned int len;
	char mech[1024];
	int numnulls = 0;
	unsigned int lup;

	/* make sure there are exactly 2 null's */
	for (lup=0;lup<key.size;lup++)
	    if (((char *)key.data)[lup]=='\0')
		numnulls++;

	if (numnulls != 2) {
	    fprintf(stderr,"warning: probable database corruption\n");
	    result = cursor->c_get(cursor, &key, &data, DB_NEXT);
	    continue;
	}

	authid = key.data;
	realm  = authid + strlen(authid)+1;
	tmp    = realm + strlen(realm)+1;
	len = key.size - (tmp - authid);

	/* make sure we have enough space of mech */
	if (len >=sizeof(mech)) {
	    fprintf(stderr,"warning: absurdly long mech name\n");
	    result = cursor->c_get(cursor, &key, &data, DB_NEXT);
	    continue;
	}

	memcpy(mech, tmp, key.size - (tmp - ((char *)key.data)));
	mech[key.size - (tmp - ((char *)key.data))] = '\0';

	if (*authid) {
	    /* don't check return values */
	    cb(authid,realm,mech,data.data,data.size);
	}

	result = cursor->c_get(cursor, &key, &data, DB_NEXT);
    }

    if (result != DB_NOTFOUND) {
	fprintf(stderr,"failure: %s\n",db_strerror(result));
	result = SASL_FAIL;
	goto cleanup;
    }

    result = cursor->c_close(cursor);
    if (result != 0) {
        result = SASL_FAIL;
        goto cleanup;
    }

    result = SASL_OK;

 cleanup:

    if (mbdb != NULL) berkeleydb_close(mbdb);
    return result;
}

#else 

/* ARGSUSED */

int listusers(const char *path __attribute__((unused)),
	      listcb_t *cb __attribute__((unused)))
{
    fprintf(stderr,"Unsupported DB format");
    exit(1);
}

#endif

char *db_new=SASL_DB_PATH;

int good_getopt(void *context __attribute__((unused)), 
		const char *plugin_name __attribute__((unused)), 
		const char *option,
		const char **result,
		unsigned *len)
{
    if (db_new && !strcmp(option, "sasldb_path")) {
	*result = db_new;
	if (len)
	    *len = strlen(db_new);
	return SASL_OK;
    }

    return SASL_FAIL;
}

static struct sasl_callback goodsasl_cb[] = {
    { SASL_CB_GETOPT, (int (*)(void))&good_getopt, NULL },
    { SASL_CB_LIST_END, NULL, NULL }
};

int main(int argc, char **argv)
{
    const char *db="/etc/sasldb";
    int result;

    if (argc > 1) {
	db = argv[1];
	if(argc > 2) {
	    db_new = argv[2];
	}
    }

    result = sasl_server_init(goodsasl_cb, "dbconverter");
    if (result != SASL_OK) {
	printf("couldn't init saslv2\n");
	return 1;
    }

    result = sasl_server_new("sasldb",
			     "localhost",
			     NULL,
			     NULL,
			     NULL,
			     NULL,
			     0,
			     &globalconn);
    if (result != SASL_OK) {
	printf("couldn't create globalconn\n");
	return 1;
    }

    if(_sasl_check_db(sasl_global_utils,globalconn) != SASL_OK) {
	printf("target DB %s is not OK\n", db_new);
	return 1;
    }
    
    printf("\nThis program will take the sasldb file specified on the\n"
           "command line and convert it to a new sasldb file in the default\n"
           "location (usually /etc/sasldb). It is STRONGLY RECOMMENDED that you\n"
           "backup sasldb before allowing this program to run\n\n"
	   "We are going to convert %s and our output will be in %s\n\n"
           "Press return to continue\n", db, db_new);

    getchar();

    listusers(db, (listcb_t *) &listusers_cb);

    sasl_dispose(&globalconn);
    sasl_done();
    
    exit(0);
}
