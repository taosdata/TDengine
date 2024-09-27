/* db_lmdb.c--SASL OpenLDAP LMDB interface
 * Howard Chu
 */
/* 
 * Copyright (C) 2011-2012 Howard Chu, All rights reserved. <hyc@symas.com>
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

#include <lmdb.h>

#include <sys/stat.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include "sasldb.h"

static int db_ok = 0;
static MDB_env *db_env;
static MDB_dbi db_dbi;

#define KILO 1024

/*
 * Open the environment
 */
static int do_open(const sasl_utils_t *utils,
			 sasl_conn_t *conn,
			 int rdwr, MDB_txn **mtxn)
{
    const char *path = SASL_DB_PATH;
    void *cntxt;
    MDB_env *env;
    MDB_txn *txn;
    sasl_getopt_t *getopt;
    size_t mapsize = 0;
    int readers = 0;
    int ret;
    int flags;

    if (!db_env) {

	if (utils->getcallback(conn, SASL_CB_GETOPT,
			       (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
	    const char *p;
	    if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK
		&& p != NULL && *p != 0) {
		path = p;
	    }
	    if (getopt(cntxt, NULL, "sasldb_maxreaders", &p, NULL) == SASL_OK
		&& p != NULL && *p != 0) {
		readers = atoi(p);
	    }
	    if (getopt(cntxt, NULL, "sasldb_mapsize", &p, NULL) == SASL_OK
		&& p != NULL && *p != 0) {
		mapsize = atoi(p);
		mapsize *= KILO;
	    }
	}

	ret = mdb_env_create(&env);
	if (ret) {
	    utils->log(conn, SASL_LOG_ERR,
		       "unable to create MDB environment: %s",
		       mdb_strerror(ret));
	    utils->seterror(conn, SASL_NOLOG, "Unable to create MDB environment");
	    return SASL_FAIL;
	}

	if (readers) {
	    ret = mdb_env_set_maxreaders(env, readers);
	    if (ret) {
		utils->log(conn, SASL_LOG_ERR,
		       "unable to set MDB maxreaders: %s",
		       mdb_strerror(ret));
		utils->seterror(conn, SASL_NOLOG, "Unable to set MDB maxreaders");
		return SASL_FAIL;
	    }
	}

	if (mapsize) {
	    ret = mdb_env_set_mapsize(env, mapsize);
	    if (ret) {
		utils->log(conn, SASL_LOG_ERR,
		       "unable to set MDB mapsize: %s",
		       mdb_strerror(ret));
		utils->seterror(conn, SASL_NOLOG, "Unable to set MDB mapsize");
		return SASL_FAIL;
	    }
	}

	flags = MDB_NOSUBDIR;
	if (!rdwr) flags |= MDB_RDONLY;
	ret = mdb_env_open(env, path, flags, 0660);
	if (ret) {
	    mdb_env_close(env);
	    if (!rdwr && ret == ENOENT) {
		/* File not found and we are only reading the data.
		   Treat as SASL_NOUSER. */
		return SASL_NOUSER;
	    }
	    utils->log(conn, SASL_LOG_ERR,
		       "unable to open MDB environment %s: %s",
		       path, mdb_strerror(ret));
	    utils->seterror(conn, SASL_NOLOG, "Unable to open MDB environment");
	    return SASL_FAIL;
	}
    } else {
    	env = db_env;
    }

    ret = mdb_txn_begin(env, NULL, rdwr ? 0 : MDB_RDONLY, &txn);
    if (ret) {
    	mdb_env_close(env);
	utils->log(conn, SASL_LOG_ERR,
		   "unable to open MDB transaction: %s",
		   mdb_strerror(ret));
	utils->seterror(conn, SASL_NOLOG, "Unable to open MDB transaction");
	return SASL_FAIL;
    }

    if (!db_dbi) {
	ret = mdb_open(txn, NULL, 0, &db_dbi);
	if (ret) {
	    mdb_txn_abort(txn);
	    mdb_env_close(env);
	    utils->log(conn, SASL_LOG_ERR,
		       "unable to open MDB database: %s",
		       mdb_strerror(ret));
	    utils->seterror(conn, SASL_NOLOG, "Unable to open MDB database");
	    return SASL_FAIL;
	}
    }

    if (!db_env)
	db_env = env;
    *mtxn = txn;

    return SASL_OK;
}

/*
 * Close the environment
 */
static void do_close()
{
    mdb_env_close(db_env);
    db_env = NULL;
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
  MDB_val dbkey, data;
  MDB_txn *txn = NULL;

  if(!utils) return SASL_BADPARAM;

  /* check parameters */
  if (!auth_identity || !realm || !propName || !out || !max_out) {
      utils->seterror(context, 0,
		      "Bad parameter in db_lmdb.c: _sasldb_getdata");
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

  /* open the db */
  result = do_open(utils, context, 0, &txn);
  if (result != SASL_OK) goto cleanup;

  /* create the key to search for */
  dbkey.mv_data = key;
  dbkey.mv_size = key_len;

  /* ask MDB for the entry */
  result = mdb_get(txn, db_dbi, &dbkey, &data);

  switch (result) {
  case 0:
    /* success */
    break;

  case MDB_NOTFOUND:
    result = SASL_NOUSER;
    utils->seterror(context, SASL_NOLOG,
		    "user: %s@%s property: %s not found in sasldb",
		    auth_identity,realm,propName);
    goto cleanup;
    break;
  default:
    utils->seterror(context, 0,
		    "error fetching from sasldb: %s",
		    mdb_strerror(result));
    result = SASL_FAIL;
    goto cleanup;
    break;
  }

  if(data.mv_size > max_out + 1)
      return SASL_BUFOVER;

  if(out_len) *out_len = data.mv_size;
  memcpy(out, data.mv_data, data.mv_size);
  out[data.mv_size] = '\0';

 cleanup:

  mdb_txn_abort(txn);
  utils->free(key);

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
  MDB_val dbkey;
  MDB_txn *txn = NULL;

  if (!utils) return SASL_BADPARAM;

  if (!authid || !realm || !propName) {
      utils->seterror(context, 0,
		      "Bad parameter in db_lmdb.c: _sasldb_putdata");
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
  result=do_open(utils, context, 1, &txn);
  if (result!=SASL_OK) goto cleanup;

  /* create the db key */
  dbkey.mv_data = key;
  dbkey.mv_size = key_len;

  if (data_in) {   /* putting secret */
    MDB_val data;

    data.mv_data = (char *)data_in;
    if(!data_len) data_len = strlen(data_in);
    data.mv_size = data_len;

    result = mdb_put(txn, db_dbi, &dbkey, &data, 0);

    if (result != 0)
    {
      utils->log(NULL, SASL_LOG_ERR,
		 "error updating sasldb: %s", mdb_strerror(result));
      utils->seterror(context, SASL_NOLOG,
		      "Couldn't update db");
      result = SASL_FAIL;
      goto cleanup;
    }
  } else {        /* removing secret */
    result=mdb_del(txn, db_dbi, &dbkey, NULL);

    if (result != 0)
    {
      utils->log(NULL, SASL_LOG_ERR,
		 "error deleting entry from sasldb: %s", mdb_strerror(result));
      utils->seterror(context, SASL_NOLOG,
		      "Couldn't update db");
      if (result == MDB_NOTFOUND)
	  result = SASL_NOUSER;
      else
	  result = SASL_FAIL;
      goto cleanup;
    }
  }
  result = mdb_txn_commit(txn);
  if (result) {
      utils->log(NULL, SASL_LOG_ERR,
		 "error committing to sasldb: %s", mdb_strerror(result));
      utils->seterror(context, SASL_NOLOG,
		      "Couldn't update db");
      result = SASL_FAIL;
  }
  txn = NULL;

 cleanup:

  mdb_txn_abort(txn);
  utils->free(key);

  return result;
}

int _sasl_check_db(const sasl_utils_t *utils,
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
void sasldb_auxprop_free (void *glob_context __attribute__((unused)),
                          const sasl_utils_t *utils __attribute__((unused)))
{
    do_close();
}
#endif

sasldb_handle _sasldb_getkeyhandle(const sasl_utils_t *utils,
				   sasl_conn_t *conn)
{
    int ret;
    MDB_txn *txn;
    MDB_cursor *mc;

    if(!utils || !conn) return NULL;

    if(!db_ok) {
	utils->seterror(conn, 0, "Database not OK in _sasldb_getkeyhandle");
	return NULL;
    }

    ret = do_open(utils, conn, 0, &txn);

    if (ret != SASL_OK) {
	return NULL;
    }

    ret = mdb_cursor_open(txn, db_dbi, &mc);
    if (ret) {
	utils->seterror(conn, 0, "cursor_open failed in _sasldb_gekeythandle");
    	return NULL;
    }

    return (sasldb_handle)mc;
}

int _sasldb_getnextkey(const sasl_utils_t *utils __attribute__((unused)),
		       sasldb_handle handle, char *out,
		       const size_t max_out, size_t *out_len)
{
    int result;
    MDB_cursor *mc = (MDB_cursor *)handle;
    MDB_val key;

    if(!utils || !handle || !out || !max_out)
	return SASL_BADPARAM;

    result = mdb_cursor_get(mc, &key, NULL, MDB_NEXT);

    if (result == MDB_NOTFOUND) return SASL_OK;

    if (result != 0) {
	return SASL_FAIL;
    }

    if (key.mv_size > max_out) {
	return SASL_BUFOVER;
    }

    memcpy(out, key.mv_data, key.mv_size);
    if (out_len) *out_len = key.mv_size;

    return SASL_CONTINUE;
}


int _sasldb_releasekeyhandle(const sasl_utils_t *utils,
			     sasldb_handle handle)
{
    MDB_cursor *mc = (MDB_cursor *)handle;

    if (!utils || !handle) return SASL_BADPARAM;

    mdb_cursor_close(mc);

    return SASL_OK;
}
