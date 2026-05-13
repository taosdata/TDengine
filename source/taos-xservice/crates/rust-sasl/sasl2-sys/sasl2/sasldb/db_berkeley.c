/* db_berkeley.c--SASL berkeley db interface
 * Rob Siemborski
 * Tim Martin
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

#include <db.h>

#include <sys/stat.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include "sasldb.h"

#define DB_VERSION_FULL ((DB_VERSION_MAJOR << 24) | (DB_VERSION_MINOR << 16) | DB_VERSION_PATCH)

static int db_ok = 0;
#if defined(KEEP_DB_OPEN)
static DB * g_db = NULL;
#endif

/*
 * Open the database
 */
static int berkeleydb_open(const sasl_utils_t *utils,
			   sasl_conn_t *conn,
			   int rdwr, DB **mbdb)
{
    const char *path = SASL_DB_PATH;
    int ret;
    int flags;
    void *cntxt;
    sasl_getopt_t *getopt;

#if defined(KEEP_DB_OPEN)
    if (g_db) {
	*mbdb = g_db;
	return SASL_OK;
    }
#endif

    if (utils->getcallback(conn, SASL_CB_GETOPT,
			   (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
	const char *p;
	if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	    && p != NULL && *p != 0) {
	    path = p;
	}
    }

    if (rdwr) flags = DB_CREATE;
    else flags = DB_RDONLY;
#if defined(KEEP_DB_OPEN)
#if defined(DB_THREAD)
    flags |= DB_THREAD;
#endif
#endif

#if DB_VERSION_FULL < 0x03000000
    ret = db_open(path, DB_HASH, flags, 0660, NULL, NULL, mbdb);
#else /* DB_VERSION_FULL < 0x03000000 */
    ret = db_create(mbdb, NULL, 0);
    if (ret == 0 && *mbdb != NULL)
    {
#if DB_VERSION_FULL >= 0x04010000 
	ret = (*mbdb)->open(*mbdb, NULL, path, NULL, DB_HASH, flags, 0660);
#else
	ret = (*mbdb)->open(*mbdb, path, NULL, DB_HASH, flags, 0660);
#endif
	if (ret != 0)
	{
	    (void) (*mbdb)->close(*mbdb, 0);
	    *mbdb = NULL;
	}
    }
#endif /* DB_VERSION_FULL < 0x03000000 */

    if (ret != 0) {
	if (rdwr == 0 && ret == ENOENT) {
	    /* File not found and we are only reading the data.
	       Treat as SASL_NOUSER. */
	    return SASL_NOUSER;
	}
	utils->log(conn, SASL_LOG_ERR,
		   "unable to open Berkeley db %s: %s",
		   path, db_strerror(ret));
	utils->seterror(conn, SASL_NOLOG, "Unable to open DB");
	return SASL_FAIL;
    }

#if defined(KEEP_DB_OPEN)
    /* Save the DB handle for later use */
    g_db = *mbdb;
#endif
    return SASL_OK;
}

/*
 * Close the database
 */
static void berkeleydb_close(const sasl_utils_t *utils, DB *mbdb)
{
    int ret;

#if defined(KEEP_DB_OPEN)
    /* Prevent other threads from reusing the same handle */
    /* msdb == g_db */    
    g_db = NULL;
#endif

    ret = mbdb->close(mbdb, 0);
    if (ret!=0) {
	/* error closing! */
	utils->log(NULL, SASL_LOG_ERR,
		   "error closing sasldb: %s",
		   db_strerror(ret));
    }
}


/*
 * Retrieve the secret from the database. 
 * 
 * Return SASL_NOUSER if the entry doesn't exist,
 * SASL_OK on success.
 *
 */
int _sasldb_getdata(const sasl_utils_t *utils,
		    sasl_conn_t *context,
		    const char *auth_identity,
		    const char *realm,
		    const char *propName,
		    char *out, const size_t max_out, size_t *out_len)
{
  int result = SASL_OK;
  char *key;
  size_t key_len;
  DBT dbkey, data;
  DB *mbdb = NULL;

  if(!utils) return SASL_BADPARAM;

  /* check parameters */
  if (!auth_identity || !realm || !propName || !out || !max_out) {
      utils->seterror(context, 0,
		      "Bad parameter in db_berkeley.c: _sasldb_getdata");
      return SASL_BADPARAM;
  }

  if (!db_ok) {
      utils->seterror(context, 0,
		      "Database not checked");
      return SASL_FAIL;
  }

  /* allocate a key */
  result = _sasldb_alloc_key(utils, auth_identity, realm, propName,
			     &key, &key_len);
  if (result != SASL_OK) {
      utils->seterror(context, 0,
		      "Could not allocate key in _sasldb_getdata");
      return result;
  }

  /* zero out */
  memset(&dbkey, 0, sizeof(dbkey));
  memset(&data, 0, sizeof(data));

  /* open the db */
  result = berkeleydb_open(utils, context, 0, &mbdb);
  if (result != SASL_OK) goto cleanup;

  /* create the key to search for */
  dbkey.data = key;
  dbkey.size = (u_int32_t) key_len;
  dbkey.flags = DB_DBT_USERMEM;
  data.flags = DB_DBT_MALLOC;

  /* ask berkeley db for the entry */
  result = mbdb->get(mbdb, NULL, &dbkey, &data, 0);

  switch (result) {
  case 0:
    /* success */
    break;

  case DB_NOTFOUND:
    result = SASL_NOUSER;
    utils->seterror(context, SASL_NOLOG,
		    "user: %s@%s property: %s not found in sasldb",
		    auth_identity,realm,propName);
    goto cleanup;
    break;
  default:
    utils->seterror(context, 0,
		    "error fetching from sasldb: %s",
		    db_strerror(result));
    result = SASL_FAIL;
    goto cleanup;
    break;
  }

  if(data.size > max_out + 1)
      return SASL_BUFOVER;

  if(out_len) *out_len = data.size;
  memcpy(out, data.data, data.size);
  out[data.size] = '\0';
  
 cleanup:

#if !defined(KEEP_DB_OPEN)
  if (mbdb != NULL) berkeleydb_close(utils, mbdb);
#endif

  utils->free(key);
  utils->free(data.data);

  return result;
}

/*
 * Put or delete an entry
 * 
 *
 */

int _sasldb_putdata(const sasl_utils_t *utils,
		    sasl_conn_t *context,
		    const char *authid,
		    const char *realm,
		    const char *propName,
		    const char *data_in, size_t data_len)
{
  int result = SASL_OK;
  char *key;
  size_t key_len;
  DBT dbkey;
  DB *mbdb = NULL;

  if (!utils) return SASL_BADPARAM;

  if (!authid || !realm || !propName) {
      utils->seterror(context, 0,
		      "Bad parameter in db_berkeley.c: _sasldb_putdata");
      return SASL_BADPARAM;
  }
  
  if (!db_ok) {
      utils->seterror(context, 0,
		      "Database not checked");   
      return SASL_FAIL;
  }

  result = _sasldb_alloc_key(utils, authid, realm, propName,
			     &key, &key_len);
  if (result != SASL_OK) {
       utils->seterror(context, 0,
		      "Could not allocate key in _sasldb_putdata");     
       return result;
  }

  /* open the db */
  result=berkeleydb_open(utils, context, 1, &mbdb);
  if (result!=SASL_OK) goto cleanup;

  /* create the db key */
  memset(&dbkey, 0, sizeof(dbkey));
  dbkey.data = key;
  dbkey.size = (u_int32_t) key_len;

  if (data_in) {   /* putting secret */
    DBT data;

    memset(&data, 0, sizeof(data));    

    data.data = (char *)data_in;
    if(!data_len) data_len = strlen(data_in);
    data.size = (u_int32_t) data_len;

    result = mbdb->put(mbdb, NULL, &dbkey, &data, 0);

    if (result != 0)
    {
      utils->log(NULL, SASL_LOG_ERR,
		 "error updating sasldb: %s", db_strerror(result));
      utils->seterror(context, SASL_NOLOG,
		      "Couldn't update db");
      result = SASL_FAIL;
      goto cleanup;
    }
  } else {        /* removing secret */
    result=mbdb->del(mbdb, NULL, &dbkey, 0);

    if (result != 0)
    {
      utils->log(NULL, SASL_LOG_ERR,
		 "error deleting entry from sasldb: %s", db_strerror(result));
      utils->seterror(context, SASL_NOLOG,
		      "Couldn't update db");
      if (result == DB_NOTFOUND)
	  result = SASL_NOUSER;
      else	  
	  result = SASL_FAIL;
      goto cleanup;
    }
  }

 cleanup:

#if !defined(KEEP_DB_OPEN)
  if (mbdb != NULL) berkeleydb_close(utils, mbdb);
#endif

  utils->free(key);

  return result;
}

LIBSASL_API int _sasl_check_db(const sasl_utils_t *utils,
		   sasl_conn_t *conn)
{
    const char *path = SASL_DB_PATH;
    int ret;
    void *cntxt;
    sasl_getopt_t *getopt;
    sasl_verifyfile_t *vf;

    if (!utils) return SASL_BADPARAM;

    if (utils->getcallback(conn, SASL_CB_GETOPT,
			   (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
	const char *p;
	if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	    && p != NULL && *p != 0) {
	    path = p;
	}
    }

    ret = utils->getcallback(conn, SASL_CB_VERIFYFILE,
			     (sasl_callback_ft *)&vf, &cntxt);
    if (ret != SASL_OK) {
	utils->seterror(conn, 0, "verifyfile failed");
	return ret;
    }

    ret = vf(cntxt, path, SASL_VRFY_PASSWD);

    if (ret == SASL_OK) {
	db_ok = 1;
    }

    if (ret == SASL_OK || ret == SASL_CONTINUE) {
        return SASL_OK;
    } else {
	return ret;
    }
}

#if defined(KEEP_DB_OPEN)
void sasldb_auxprop_free (void *glob_context,
                          const sasl_utils_t *utils)
{
    if (g_db != NULL) berkeleydb_close(utils, g_db);
}
#endif

typedef struct berkeleydb_handle 
{
    DB *mbdb;
    DBC *cursor;
} berkleyhandle_t;

LIBSASL_API sasldb_handle _sasldb_getkeyhandle(const sasl_utils_t *utils,
				   sasl_conn_t *conn) 
{
    int ret;
    DB *mbdb;
    berkleyhandle_t *handle;
    
    if(!utils || !conn) return NULL;

    if(!db_ok) {
	utils->seterror(conn, 0, "Database not OK in _sasldb_getkeyhandle");
	return NULL;
    }

    ret = berkeleydb_open(utils, conn, 0, &mbdb);

    if (ret != SASL_OK) {
	return NULL;
    }

    handle = utils->malloc(sizeof(berkleyhandle_t));
    if(!handle) {
#if !defined(KEEP_DB_OPEN)
	(void)mbdb->close(mbdb, 0);
#endif
	utils->seterror(conn, 0, "Memory error in _sasldb_gethandle");
	return NULL;
    }
    
    handle->mbdb = mbdb;
    handle->cursor = NULL;

    return (sasldb_handle)handle;
}

LIBSASL_API int _sasldb_getnextkey(const sasl_utils_t *utils __attribute__((unused)),
		       sasldb_handle handle, char *out,
		       const size_t max_out, size_t *out_len) 
{
    DB *mbdb;
    int result;
    berkleyhandle_t *dbh = (berkleyhandle_t *)handle;
    DBT key, data;

    if(!utils || !handle || !out || !max_out)
	return SASL_BADPARAM;

    mbdb = dbh->mbdb;

    memset(&key,0, sizeof(key));
    memset(&data,0,sizeof(data));

    if(!dbh->cursor) {
        /* make cursor */
#if DB_VERSION_FULL < 0x03060000
	result = mbdb->cursor(mbdb, NULL,&dbh->cursor); 
#else
	result = mbdb->cursor(mbdb, NULL,&dbh->cursor, 0); 
#endif /* DB_VERSION_FULL < 0x03000000 */

	if (result!=0) {
	    return SASL_FAIL;
	}

	/* loop thru */
	result = dbh->cursor->c_get(dbh->cursor, &key, &data,
				    DB_FIRST);
    } else {
	result = dbh->cursor->c_get(dbh->cursor, &key, &data,
				    DB_NEXT);
    }

    if (result == DB_NOTFOUND) return SASL_OK;

    if (result != 0) {
	return SASL_FAIL;
    }
    
    if (key.size > max_out) {
	return SASL_BUFOVER;
    }
    
    memcpy(out, key.data, key.size);
    if (out_len) *out_len = key.size;

    return SASL_CONTINUE;
}


LIBSASL_API int _sasldb_releasekeyhandle(const sasl_utils_t *utils,
			     sasldb_handle handle) 
{
    berkleyhandle_t *dbh = (berkleyhandle_t *)handle;
    int ret = 0;
    
    if (!utils || !dbh) return SASL_BADPARAM;

    if (dbh->cursor) {
	dbh->cursor->c_close(dbh->cursor);
    }

#if !defined(KEEP_DB_OPEN)
    /* This is almost the same berkeleydb_close(), except that
       berkeleydb_close logs a message on error and does not return
       any error */
    if (dbh->mbdb) {
	  ret = dbh->mbdb->close(dbh->mbdb, 0);
    }
#endif
    
    utils->free(dbh);
    
    if (ret) {
	return SASL_FAIL;
    } else {
	return SASL_OK;
    }
}
