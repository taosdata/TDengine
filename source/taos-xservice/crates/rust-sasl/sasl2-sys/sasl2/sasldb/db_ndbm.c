/* db_ndbm.c--SASL ndbm interface
 * Rob Siemborski
 * Rob Earhart
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
#include <ndbm.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include "sasldb.h"

static int db_ok = 0;

/* This provides a version of _sasl_db_getsecret and
 * _sasl_db_putsecret which work with ndbm. */
int _sasldb_getdata(const sasl_utils_t *utils,
		    sasl_conn_t *conn,
		    const char *authid,
		    const char *realm,
		    const char *propName,
		    char *out, const size_t max_out, size_t *out_len)
{
  int result = SASL_OK;
  char *key;
  size_t key_len;
  DBM *db;
  datum dkey, dvalue;
  void *cntxt;
  sasl_getopt_t *getopt;
  const char *path = SASL_DB_PATH;

  if (!utils) return SASL_BADPARAM;
  if (!authid || !propName || !realm || !out || !max_out) {
      utils->seterror(conn, 0,
		      "Bad parameter in db_ndbm.c: _sasldb_getdata");    
      return SASL_BADPARAM;
  }
  if (!db_ok) {
      utils->seterror(conn, 0, "Database not checked");
      return SASL_FAIL;
  }

  result = _sasldb_alloc_key(utils, authid, realm, propName,
			     &key, &key_len);
  if (result != SASL_OK) {
      utils->seterror(conn, 0,
		      "Could not allocate key in _sasldb_getdata");
      return result;
  }

  if (utils->getcallback(conn, SASL_CB_GETOPT,
                        (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
      const char *p;
      if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	  && p != NULL && *p != 0) {
          path = p;
      }
  }
  db = dbm_open(path, O_RDONLY, S_IRUSR | S_IWUSR);
  if (! db) {
      utils->seterror(cntxt, 0, "Could not open db `%s': %s",
		      path, strerror(errno));
      result = SASL_FAIL;
      goto cleanup;
  }
  dkey.dptr = key;
  dkey.dsize = key_len;
  dvalue = dbm_fetch(db, dkey);
  if (! dvalue.dptr) {
      utils->seterror(cntxt, SASL_NOLOG,
		      "user: %s@%s property: %s not found in sasldb %s",
		      authid, realm, propName, path);
      result = SASL_NOUSER;
      goto cleanup;
  }

  if((size_t)dvalue.dsize > max_out + 1) {
      utils->seterror(cntxt, 0, "buffer overflow");
      return SASL_BUFOVER;
  }

  if(out_len) *out_len = dvalue.dsize;
  memcpy(out, dvalue.dptr, dvalue.dsize); 
  out[dvalue.dsize] = '\0';

#if NDBM_FREE
  /* Note: not sasl_FREE!  This is memory allocated by ndbm,
   * which is using libc malloc/free. */
  free(dvalue.dptr);
#endif

 cleanup:
  utils->free(key);
  if(db)
    dbm_close(db);

  return result;
}

int _sasldb_putdata(const sasl_utils_t *utils,
		    sasl_conn_t *conn,
		    const char *authid,
		    const char *realm,
		    const char *propName,
		    const char *data, size_t data_len)
{
  int result = SASL_OK;
  char *key;
  size_t key_len;
  DBM *db;
  datum dkey;
  void *cntxt;
  sasl_getopt_t *getopt;
  const char *path = SASL_DB_PATH;

  if (!utils) return SASL_BADPARAM;

  if (!authid || !realm || !propName) {
      utils->seterror(conn, 0,
		      "Bad parameter in db_ndbm.c: _sasldb_putdata");
      return SASL_BADPARAM;
  }

  result = _sasldb_alloc_key(utils, authid, realm, propName,
			     &key, &key_len);
  if (result != SASL_OK) {
      utils->seterror(conn, 0,
		      "Could not allocate key in _sasldb_putdata"); 
      return result;
  }

  if (utils->getcallback(conn, SASL_CB_GETOPT,
			 (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
      const char *p;
      if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	  && p != NULL && *p != 0) {
          path = p;
      }
  }

  db = dbm_open(path,
		O_RDWR | O_CREAT,
		S_IRUSR | S_IWUSR);
  if (! db) {
      utils->seterror(conn, 0, "Could not open db `%s' for writing: %s",
		      path, strerror(errno));
      utils->log(conn, SASL_LOG_ERR,
		 "SASL error opening password file. "
		 "Do you have write permissions?\n");
      result = SASL_FAIL;
      goto cleanup;
  }
  dkey.dptr = key;
  dkey.dsize = key_len;
  if (data) {
    datum dvalue;
    dvalue.dptr = (void *)data;
    if(!data_len) data_len = strlen(data);
    dvalue.dsize = data_len;
    if (dbm_store(db, dkey, dvalue, DBM_REPLACE)) {
	utils->seterror(conn, 0,
			"Couldn't update record for %s@%s property %s "
			"in db %s: %s", authid, realm, propName, path,
			strerror(errno));
	result = SASL_FAIL;
    }
  } else {
      if (dbm_delete(db, dkey)) {
	  utils->seterror(conn, 0,
			  "Couldn't delete record for %s@%s property %s "
			  "in db %s: %s", authid, realm, propName, path,
			  strerror(errno));
	  result = SASL_NOUSER;
      }
  }
  dbm_close(db);

 cleanup:
  utils->free(key);

  return result;
}

#ifdef DBM_SUFFIX
#define SUFLEN (strlen(DBM_SUFFIX) + 1)
#else
#define SUFLEN 5
#endif

int _sasl_check_db(const sasl_utils_t *utils,
		   sasl_conn_t *conn)
{
    const char *path = SASL_DB_PATH;
    void *cntxt;
    sasl_getopt_t *getopt;
    sasl_verifyfile_t *vf;
    int ret = SASL_OK;
    char *db;

    if(!utils) return SASL_BADPARAM;

    if (utils->getcallback(conn, SASL_CB_GETOPT,
			   (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
	const char *p;
	if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	    && p != NULL && *p != 0) {
	    path = p;
	}
    }

    db = utils->malloc(strlen(path) + SUFLEN);

    if (db == NULL) {
	ret = SASL_NOMEM;
    }

    ret = utils->getcallback(NULL, SASL_CB_VERIFYFILE,
			     (sasl_callback_ft *)&vf, &cntxt);
    if(ret != SASL_OK) {
	utils->seterror(conn, 0,
			"No verifyfile callback");
	return ret;
    }

#ifdef DBM_SUFFIX
    if (ret == SASL_OK) {
	sprintf(db, "%s%s", path, DBM_SUFFIX);
	ret = vf(cntxt, db, SASL_VRFY_PASSWD);
    }
#else
    if (ret == SASL_OK) {
	sprintf(db, "%s.dir", path);
	ret = vf(cntxt, db, SASL_VRFY_PASSWD);
    }
    if (ret == SASL_OK) {
	sprintf(db, "%s.pag", path);
	ret = vf(cntxt, db, SASL_VRFY_PASSWD);
    }
#endif
    if (db) {
	utils->free(db);
    }
    if (ret == SASL_OK) {
	db_ok = 1;
    }

    if (ret == SASL_OK || ret == SASL_CONTINUE) {
	return SASL_OK;
    } else {
	utils->seterror(conn, 0,
			"Verifyfile failed");
	return ret;
    }
}

typedef struct ndbm_handle 
{
    DBM *db;
    datum dkey;
    int first;
} handle_t;

sasldb_handle _sasldb_getkeyhandle(const sasl_utils_t *utils,
				   sasl_conn_t *conn) 
{
    const char *path = SASL_DB_PATH;
    sasl_getopt_t *getopt;
    void *cntxt;
    DBM *db;
    handle_t *handle;
    
    if(!utils || !conn) return NULL;

    if(!db_ok) {
	utils->seterror(conn, 0, "Database not OK in _sasldb_getkeyhandle");
	return NULL;
    }

    if (utils->getcallback(conn, SASL_CB_GETOPT,
			   (sasl_callback_ft *)&getopt, &cntxt) == SASL_OK) {
	const char *p;
	if (getopt(cntxt, NULL, "sasldb_path", &p, NULL) == SASL_OK 
	    && p != NULL && *p != 0) {
	    path = p;
	}
    }

    db = dbm_open(path, O_RDONLY, S_IRUSR | S_IWUSR);

    if(!db) {
	utils->seterror(conn, 0, "Could not open db `%s': %s",
			path, strerror(errno));
	return NULL;
    }

    handle = utils->malloc(sizeof(handle_t));
    if(!handle) {
	utils->seterror(conn, 0, "no memory in _sasldb_getkeyhandle");
	dbm_close(db);
	return NULL;
    }
    
    handle->db = db;
    handle->first = 1;

    return (sasldb_handle)handle;
}

int _sasldb_getnextkey(const sasl_utils_t *utils __attribute__((unused)),
		       sasldb_handle handle, char *out,
		       const size_t max_out, size_t *out_len) 
{
    handle_t *dbh = (handle_t *)handle;
    datum nextkey;

    if(!utils || !handle || !out || !max_out)
	return SASL_BADPARAM;

    if(dbh->first) {
	dbh->dkey = dbm_firstkey(dbh->db);
	dbh->first = 0;
    } else {
	nextkey = dbm_nextkey(dbh->db);
	dbh->dkey = nextkey;
    }

    if(dbh->dkey.dptr == NULL)
	return SASL_OK;
    
    if((unsigned)dbh->dkey.dsize > max_out)
	return SASL_BUFOVER;
    
    memcpy(out, dbh->dkey.dptr, dbh->dkey.dsize);
    if(out_len) *out_len = dbh->dkey.dsize;
    
    return SASL_CONTINUE;
}

int _sasldb_releasekeyhandle(const sasl_utils_t *utils,
			     sasldb_handle handle) 
{
    handle_t *dbh = (handle_t *)handle;
    
    if(!utils || !dbh) return SASL_BADPARAM;

    if(dbh->db) dbm_close(dbh->db);

    utils->free(dbh);
    
    return SASL_OK;
}
