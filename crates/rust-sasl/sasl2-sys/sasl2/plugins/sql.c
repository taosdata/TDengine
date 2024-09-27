/*
**
** SQL Auxprop plugin
**
** Ken Murchison
** Maya Nigrosh -- original store() and txn support
** Simon Loader -- original mysql plugin
** Patrick Welche -- original pgsql plugin
**
**
*/

#include <config.h>

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "sasl.h"
#include "saslutil.h"
#include "saslplug.h"

#include <ctype.h>

#include "plugin_common.h"

#define sql_max(a, b) ((a) > (b) ? (a) : (b))
#define sql_len(input) ((input) ? strlen(input) : 0)
#define sql_exists(input) ((input) && (*input))

typedef struct sql_engine {
    const char *name;
    void *(*sql_open)(char *host, char *port, int usessl,
		      const char *user, const char *password,
		      const char *database, const sasl_utils_t *utils);
    int (*sql_escape_str)(char *to, const char *from);
    int (*sql_begin_txn)(void *conn, const sasl_utils_t *utils);
    int (*sql_commit_txn)(void *conn, const sasl_utils_t *utils);
    int (*sql_rollback_txn)(void *conn, const sasl_utils_t *utils);
    int (*sql_exec)(void *conn, const char *cmd, char *value, size_t size,
		    size_t *value_len, const sasl_utils_t *utils);
    void (*sql_close)(void *conn);
} sql_engine_t;

typedef struct sql_settings {
    const sql_engine_t *sql_engine;
    const char *sql_user;
    const char *sql_passwd;
    const char *sql_hostnames;
    const char *sql_database;
    const char *sql_select;
    const char *sql_insert;
    const char *sql_update;
    int sql_usessl;
} sql_settings_t;

static const char * SQL_BLANK_STRING = "";
static const char * SQL_WILDCARD = "*";
static const char * SQL_NULL_VALUE = "NULL";


#ifdef HAVE_MYSQL
#include <mysql.h>

static void *_mysql_open(char *host, char *port, int usessl,
			 const char *user, const char *password,
			 const char *database, const sasl_utils_t *utils)
{
    MYSQL *mysql;
    
    if (!(mysql = mysql_init(NULL))) {
	utils->log(utils->conn, SASL_LOG_ERR,
		   "sql plugin: could not execute mysql_init()");
	return NULL;
    }
    
    return mysql_real_connect(mysql, host, user, password, database,
			      port ? strtoul(port, NULL, 10) : 0, NULL,
			      usessl ? CLIENT_SSL : 0);
}

static int _mysql_escape_str(char *to, const char *from)
{
    return mysql_escape_string(to, from, strlen(from));
}

static int _mysql_exec(void *conn, const char *cmd, char *value, size_t size,
		       size_t *value_len, const sasl_utils_t *utils)
{
    MYSQL_RES *result;
    MYSQL_ROW row;
    int row_count, len;
    
    len = strlen(cmd);
    /* mysql_real_query() doesn't want a terminating ';' */
    if (cmd[len-1] == ';') len--;

    /* 
     *  Run the query. It is important to note that mysql_real_query
     *  will return success even if the sql statement 
     *  had an error in it. However, mysql_errno() will alsways
     *  tell us if there was an error. Therefore we can ignore
     *  the result from mysql_real_query and simply check mysql_errno()
     *  to decide if there was really an error.
     */
    (void)mysql_real_query(conn, cmd, len);

    if(mysql_errno(conn)) {
        utils->log(utils->conn, SASL_LOG_ERR, "sql query failed: %s",
		   mysql_error(conn));
	return -1;
    }

    /* see if we should expect some results */
    if (!mysql_field_count(conn)) {
	/* no results (BEGIN, COMMIT, DELETE, INSERT, UPDATE) */
	return 0;
    }

    /* get the results */
    result = mysql_store_result(conn);
    if (!result) {
	/* umm nothing found */
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	return -1;
    }

    /* quick row check */
    row_count = mysql_num_rows(result);
    if (!row_count) {
	/* umm nothing found */
	mysql_free_result(result);
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	return -1;
    }
    if (row_count > 1) {
	utils->log(utils->conn, SASL_LOG_WARN,
		   "sql plugin: found duplicate row for query %s", cmd);
    }
    
    /* now get the result set value and value_len */
    /* we only fetch one because we don't care about the rest */
    row = mysql_fetch_row(result);
    if (!row || !row[0]) {
	/* umm nothing found */
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	mysql_free_result(result);
	return -1;
    }
    if (value) {
	strncpy(value, row[0], size-2);
	value[size-1] = '\0';
	if (value_len) *value_len = strlen(value);
    }
    
    /* free result */
    mysql_free_result(result);
    mysql_next_result(conn);
    
    return 0;
}

static int _mysql_begin_txn(void *conn, const sasl_utils_t *utils)
{
    return _mysql_exec(conn,
#if MYSQL_VERSION_ID >= 40011
		       "START TRANSACTION",
#else
		       "BEGIN",
#endif
		       NULL, 0, NULL, utils);
}

static int _mysql_commit_txn(void *conn, const sasl_utils_t *utils)
{
    return _mysql_exec(conn, "COMMIT", NULL, 0, NULL, utils);
}

static int _mysql_rollback_txn(void *conn, const sasl_utils_t *utils)
{
    return _mysql_exec(conn, "ROLLBACK", NULL, 0, NULL, utils);
}

static void _mysql_close(void *conn)
{
    mysql_close(conn);
}
#endif /* HAVE_MYSQL */

#ifdef HAVE_PGSQL
#include <libpq-fe.h>

static void *_pgsql_open(char *host, char *port, int usessl,
			 const char *user, const char *password,
			 const char *database, const sasl_utils_t *utils)
{
    PGconn *conn = NULL;
    char *conninfo, *sep;
    
    /* create the connection info string */
    /* The 64 represents the number of characters taken by
     * the keyword tokens, plus a small pad
     */
    conninfo = utils->malloc(64 + sql_len(host) + sql_len(port)
			     + sql_len(user) + sql_len(password)
			     + sql_len(database));
    if (!conninfo) {
	MEMERROR(utils);
	return NULL;
    }
    
    /* add each term that exists */
    conninfo[0] = '\0';
    sep = "";
    if (sql_exists(host)) {
	strcat(conninfo, sep);
	strcat(conninfo, "host='");
	strcat(conninfo, host);
	strcat(conninfo, "'");
	sep = " ";
    }
    if (sql_exists(port)) {
	strcat(conninfo, sep);
	strcat(conninfo, "port='");
	strcat(conninfo, port);
	strcat(conninfo, "'");
	sep = " ";
    }
    if (sql_exists(user)) {
	strcat(conninfo, sep);
	strcat(conninfo, "user='");
	strcat(conninfo, user);
	strcat(conninfo, "'");
	sep = " ";
    }
    if (sql_exists(password)) {
	strcat(conninfo, sep);
	strcat(conninfo, "password='");
	strcat(conninfo, password);
	strcat(conninfo, "'");
	sep = " ";
    }
    if (sql_exists(database)) {
	strcat(conninfo, sep);
	strcat(conninfo, "dbname='");
	strcat(conninfo, database);
	strcat(conninfo, "'");
	sep = " ";
    }
    if (usessl) {
	strcat(conninfo, sep);
	strcat(conninfo, "requiressl='1'");
    }
    
    conn = PQconnectdb(conninfo);
    free(conninfo);
    
    if ((PQstatus(conn) != CONNECTION_OK)) {
	utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %s",
		   PQerrorMessage(conn));
	return NULL;
    }
    
    return conn;
}

static int _pgsql_escape_str(char *to, const char *from)
{
    return PQescapeString(to, from, strlen(from));
}

static int _pgsql_exec(void *conn, const char *cmd, char *value, size_t size,
		       size_t *value_len, const sasl_utils_t *utils)
{
    PGresult *result;
    int row_count;
    ExecStatusType status;
    
    /* run the query */
    result = PQexec(conn, cmd);
    
    /* check the status */
    status = PQresultStatus(result);
    if (status == PGRES_COMMAND_OK) {
	/* no results (BEGIN, COMMIT, DELETE, INSERT, UPDATE) */
	PQclear(result);
	return 0;
    }
    else if (status != PGRES_TUPLES_OK) {
	/* error */
	utils->log(utils->conn, SASL_LOG_DEBUG, "sql plugin: %s ",
		   PQresStatus(status));
	PQclear(result);
	return -1;
    }
    
    /* quick row check */
    row_count = PQntuples(result);
    if (!row_count) {
	/* umm nothing found */
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	PQclear(result);
	return -1;
    }
    if (row_count > 1) {
	utils->log(utils->conn, SASL_LOG_WARN,
		   "sql plugin: found duplicate row for query %s", cmd);
    }
    
    /* now get the result set value and value_len */
    /* we only fetch one because we don't care about the rest */
    if (value) {
	strncpy(value, PQgetvalue(result,0,0), size-2);
	value[size-1] = '\0';
	if (value_len) *value_len = strlen(value);
    }
    
    /* free result */
    PQclear(result);
    return 0;
}

static int _pgsql_begin_txn(void *conn, const sasl_utils_t *utils)
{
    return _pgsql_exec(conn, "BEGIN;", NULL, 0, NULL, utils);
}

static int _pgsql_commit_txn(void *conn, const sasl_utils_t *utils)
{
    return _pgsql_exec(conn, "COMMIT;", NULL, 0, NULL, utils);
}

static int _pgsql_rollback_txn(void *conn, const sasl_utils_t *utils)
{
    return _pgsql_exec(conn, "ROLLBACK;", NULL, 0, NULL, utils);
}

static void _pgsql_close(void *conn)
{
    PQfinish(conn);
}
#endif /* HAVE_PGSQL */

#ifdef HAVE_SQLITE
#include <sqlite.h>

static void *_sqlite_open(char *host __attribute__((unused)),
			  char *port __attribute__((unused)),
			  int usessl __attribute__((unused)),
			  const char *user __attribute__((unused)),
			  const char *password __attribute__((unused)),
			  const char *database, const sasl_utils_t *utils)
{
    int rc;
    sqlite *db;
    char *zErrMsg = NULL;

    db = sqlite_open(database, 0, &zErrMsg);
    if (db == NULL) {
	utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %s", zErrMsg);
	sqlite_freemem (zErrMsg);
	return NULL;
    }

    rc = sqlite_exec(db, "PRAGMA empty_result_callbacks = ON", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
	utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %s", zErrMsg);
	sqlite_freemem (zErrMsg);
	sqlite_close(db);
	return NULL;
    }

    return (void*)db;
}

static int _sqlite_escape_str(char *to, const char *from)
{
    char s;

    while ( (s = *from++) != '\0' ) {
	if (s == '\'' || s == '\\') {
	    *to++ = '\\';
	}
	*to++ = s;
    }
    *to = '\0';

    return 0;
}

static int sqlite_my_callback(void *pArg, int argc __attribute__((unused)),
			      char **argv,
			      char **columnNames __attribute__((unused)))
{
    char **result = (char**)pArg;

    if (argv == NULL) {
	*result = NULL;				/* no record */
    } else if (argv[0] == NULL) {
	*result = strdup(SQL_NULL_VALUE);	/* NULL IS SQL_NULL_VALUE */
    } else {
	*result = strdup(argv[0]);
    }

    return /*ABORT*/1;
}

static int _sqlite_exec(void *db, const char *cmd, char *value, size_t size,
		        size_t *value_len, const sasl_utils_t *utils)
{
    int rc;
    char *result = NULL;
    char *zErrMsg = NULL;

    rc = sqlite_exec((sqlite*)db, cmd, sqlite_my_callback, (void*)&result, &zErrMsg);
    if (rc != SQLITE_OK && rc != SQLITE_ABORT) {
	utils->log(utils->conn, SASL_LOG_DEBUG, "sql plugin: %s ", zErrMsg);
	sqlite_freemem (zErrMsg);
	return -1;
    }

    if (rc == SQLITE_OK) {
	/* no results (BEGIN, COMMIT, DELETE, INSERT, UPDATE) */
	return 0;
    }

    if (result == NULL) {
	/* umm nothing found */
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	return -1;
    }

    /* XXX: Duplication cannot be found by this method. */

    /* now get the result set value and value_len */
    /* we only fetch one because we don't care about the rest */
    if (value) {
	strncpy(value, result, size - 2);
	value[size - 1] = '\0';
	if (value_len) {
	    *value_len = strlen(value);
	}
    }

    /* free result */
    free(result);
    return 0;
}

static int _sqlite_begin_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite_exec(db, "BEGIN TRANSACTION", NULL, 0, NULL, utils);
}

static int _sqlite_commit_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite_exec(db, "COMMIT TRANSACTION", NULL, 0, NULL, utils);
}

static int _sqlite_rollback_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite_exec(db, "ROLLBACK TRANSACTION", NULL, 0, NULL, utils);
}

static void _sqlite_close(void *db)
{
    sqlite_close((sqlite*)db);
}
#endif /* HAVE_SQLITE */

#ifdef HAVE_SQLITE3
#include <sqlite3.h>

static void *_sqlite3_open(char *host __attribute__((unused)),
			  char *port __attribute__((unused)),
			  int usessl __attribute__((unused)),
			  const char *user __attribute__((unused)),
			  const char *password __attribute__((unused)),
			  const char *database, const sasl_utils_t *utils)
{
    int rc;
    sqlite3 *db;
    char *zErrMsg = NULL;

    rc = sqlite3_open(database, &db);
    if (SQLITE_OK != rc) {
    	if (db)
		utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %s",
			   sqlite3_errmsg(db));
	else
		utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %d", rc);
	sqlite3_close(db);
	return NULL;
    }

    rc = sqlite3_exec(db, "PRAGMA empty_result_callbacks = ON", NULL, NULL, &zErrMsg);
    if (rc != SQLITE_OK) {
    	if (zErrMsg) {
		utils->log(utils->conn, SASL_LOG_ERR, "sql plugin: %s",
			   zErrMsg);
		sqlite3_free(zErrMsg);
	} else
		utils->log(utils->conn, SASL_LOG_DEBUG, "sql plugin: %d", rc);
	sqlite3_close(db);
	return NULL;
    }

    return (void*)db;
}

static int _sqlite3_escape_str(char *to, const char *from)
{
    char s;

    while ( (s = *from++) != '\0' ) {
	if (s == '\'' || s == '\\') {
	    *to++ = '\\';
	}
	*to++ = s;
    }
    *to = '\0';

    return 0;
}

static int sqlite3_my_callback(void *pArg, int argc __attribute__((unused)),
			      char **argv,
			      char **columnNames __attribute__((unused)))
{
    char **result = (char**)pArg;

    if (argv == NULL) {
	*result = NULL;				/* no record */
    } else if (argv[0] == NULL) {
	*result = strdup(SQL_NULL_VALUE);	/* NULL IS SQL_NULL_VALUE */
    } else {
	*result = strdup(argv[0]);
    }

    return 0;
}

static int _sqlite3_exec(void *db,
			 const char *cmd,
			 char *value,
			 size_t size,
			 size_t *value_len,
			 const sasl_utils_t *utils)
{
    int rc;
    char *result = NULL;
    char *zErrMsg = NULL;

    rc = sqlite3_exec((sqlite3*)db, cmd, sqlite3_my_callback, (void*)&result, &zErrMsg);
    if (rc != SQLITE_OK) {
    	if (zErrMsg) {
	    utils->log(utils->conn, SASL_LOG_DEBUG, "sql plugin: %s", zErrMsg);
	    sqlite3_free(zErrMsg);
	} else {
	    utils->log(utils->conn, SASL_LOG_DEBUG, "sql plugin: %d", rc);
	}
	return -1;
    }

    if (value == NULL && rc == SQLITE_OK) {
	/* no results (BEGIN, COMMIT, DELETE, INSERT, UPDATE) */
	return 0;
    }

    if (result == NULL) {
	/* umm nothing found */
	utils->log(utils->conn, SASL_LOG_NOTE, "sql plugin: no result found");
	return -1;
    }

    /* XXX: Duplication cannot be found by this method. */

    /* now get the result set value and value_len */
    /* we only fetch one because we don't care about the rest */
    if (value) {
	strncpy(value, result, size - 2);
	value[size - 1] = '\0';
	if (value_len) {
	    *value_len = strlen(value);
	}
    }

    free(result);
    return 0;
}

static int _sqlite3_begin_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, 0, NULL, utils);
}

static int _sqlite3_commit_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite3_exec(db, "COMMIT TRANSACTION;", NULL, 0, NULL, utils);
}

static int _sqlite3_rollback_txn(void *db, const sasl_utils_t *utils)
{
    return _sqlite3_exec(db, "ROLLBACK TRANSACTION;", NULL, 0, NULL, utils);
}

static void _sqlite3_close(void *db)
{
    sqlite3_close((sqlite3*)db);
}
#endif /* HAVE_SQLITE3 */

static const sql_engine_t sql_engines[] = {
#ifdef HAVE_MYSQL
    { "mysql", &_mysql_open, &_mysql_escape_str,
      &_mysql_begin_txn, &_mysql_commit_txn, &_mysql_rollback_txn,
      &_mysql_exec, &_mysql_close },
#endif /* HAVE_MYSQL */
#ifdef HAVE_PGSQL
    { "pgsql", &_pgsql_open, &_pgsql_escape_str,
      &_pgsql_begin_txn, &_pgsql_commit_txn, &_pgsql_rollback_txn,
      &_pgsql_exec, &_pgsql_close },
#endif
#ifdef HAVE_SQLITE
    { "sqlite", &_sqlite_open, &_sqlite_escape_str,
      &_sqlite_begin_txn, &_sqlite_commit_txn, &_sqlite_rollback_txn,
      &_sqlite_exec, &_sqlite_close },
#endif
#ifdef HAVE_SQLITE3
    { "sqlite3", &_sqlite3_open, &_sqlite3_escape_str,
      &_sqlite3_begin_txn, &_sqlite3_commit_txn, &_sqlite3_rollback_txn,
      &_sqlite3_exec, &_sqlite3_close },
#endif
    { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL }
};

/*
**  Sql_create_statement
**   uses statement line and allocate memory to replace
**  Parts with the strings provided.
**   %<char> =  no change
**   %% = %
**   %u = user
**   %p = prop
**   %r = realm
**   %v = value of prop
**  e.g select %p from auth where user = %u and domain = %r;
**  Note: calling function must free memory.
**
*/

static char *sql_create_statement(const char *statement, const char *prop,
				  const char *user, const char *realm, 
				  const char *value,  
				  const sasl_utils_t *utils)
{
    const char *ptr, *line_ptr;
    char *buf, *buf_ptr;
    int filtersize;
    int ulen, plen, rlen, vlen;
    int numpercents=0;
    int biggest;
    size_t i;
    
    /* calculate memory needed for creating the complete query string. */
    ulen = (int)strlen(user);
    rlen = (int)strlen(realm);
    plen = (int)strlen(prop);
    vlen = (int)sql_len(value);
    
    /* what if we have multiple %foo occurrences in the input query? */
    for (i = 0; i < strlen(statement); i++) {
	if (statement[i] == '%') {
	    numpercents++;
	}
    }
    
    /* find the biggest of ulen, rlen, plen, vlen */
    biggest = sql_max(sql_max(ulen, rlen), sql_max(plen, vlen));
    
    /* plus one for the semicolon...and don't forget the trailing 0x0 */
    filtersize = (int)strlen(statement) + 1 + (numpercents*biggest)+1;
    
    /* ok, now try to allocate a chunk of that size */
    buf = (char *) utils->malloc(filtersize);
    
    if (!buf) {
	MEMERROR(utils);
	return NULL;
    }
    
    buf_ptr = buf;
    line_ptr = statement;
    
    /* replace the strings */
    while ( (ptr = strchr(line_ptr, '%')) ) {
	/* copy up to but not including the next % */
	memcpy(buf_ptr, line_ptr, ptr - line_ptr); 
	buf_ptr += ptr - line_ptr;
	ptr++;
	switch (ptr[0]) {
	case '%':
	    buf_ptr[0] = '%';
	    buf_ptr++;
	    break;
	case 'u':
	    memcpy(buf_ptr, user, ulen);
	    buf_ptr += ulen;
	    break;
	case 'r':
	    memcpy(buf_ptr, realm, rlen);
	    buf_ptr += rlen;
	    break;
	case 'p':
	    memcpy(buf_ptr, prop, plen);
	    buf_ptr += plen;
	    break;
	case 'v':
	    if (value != NULL) {
		memcpy(buf_ptr, value, vlen);
		buf_ptr += vlen;
	    }
	    else {
		utils->log(utils->conn, SASL_LOG_ERR,
			   "'%%v' shouldn't be in a SELECT or DELETE");
	    }
	    break;
	default:
	    buf_ptr[0] = '%';
	    buf_ptr[1] = ptr[0];
	    buf_ptr += 2;
	    break;
	}
	ptr++;
	line_ptr = ptr;
    }
    
    memcpy(buf_ptr, line_ptr, strlen(line_ptr)+1);
    /* Make sure the current portion of the statement ends with a semicolon */
    if (buf_ptr[strlen(buf_ptr-1)] != ';') {
	strcat(buf_ptr, ";");
    }

    return (buf);
}

/* sql_get_settings
 *
 * Get the auxprop settings and put them in the global context array
*/
static void sql_get_settings(const sasl_utils_t *utils, void *glob_context)
{
    sql_settings_t *settings;
    int r;
    const char *usessl, *engine_name;
    const sql_engine_t *e;
    
    settings = (sql_settings_t *) glob_context;
    
    r = utils->getopt(utils->getopt_context,"SQL", "sql_engine",
		      &engine_name, NULL);
    if (r || !engine_name) {
	engine_name = "mysql";
    }
    
    /* find the correct engine */
    e = sql_engines;
    while (e->name) {
	if (!strcasecmp(engine_name, e->name)) break;
	e++;
    }

    if (!e->name) {
	utils->log(utils->conn, SASL_LOG_ERR, "SQL engine '%s' not supported",
		   engine_name);
    }

    settings->sql_engine = e;

    r = utils->getopt(utils->getopt_context,"SQL","sql_user",
		      &settings->sql_user, NULL);
    if ( r || !settings->sql_user ) {
	settings->sql_user = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context,"SQL", "sql_passwd",
		      &settings->sql_passwd, NULL);
    if (r || !settings->sql_passwd ) {
	settings->sql_passwd = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context,"SQL", "sql_hostnames",
		      &settings->sql_hostnames, NULL);
    if (r || !settings->sql_hostnames ) {
	settings->sql_hostnames = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context,"SQL", "sql_database",
		      &settings->sql_database, NULL);
    if (r || !settings->sql_database ) {
	settings->sql_database = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context,"SQL", "sql_select",
		      &settings->sql_select, NULL);
    if (r || !settings->sql_select ) {
	/* backwards compatibility */
	r = utils->getopt(utils->getopt_context,"SQL", "sql_statement",
			  &settings->sql_select, NULL);
	if (r || !settings->sql_select) {
	    settings->sql_select = SQL_BLANK_STRING;
	}
    }

    r = utils->getopt(utils->getopt_context, "SQL", "sql_insert",
		      &settings->sql_insert, NULL);
    if (r || !settings->sql_insert) {
	settings->sql_insert = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context, "SQL", "sql_update",
		      &settings->sql_update, NULL);
    if (r || !settings->sql_update) {
	settings->sql_update = SQL_BLANK_STRING;
    }

    r = utils->getopt(utils->getopt_context, "SQL", "sql_usessl",
		  &usessl, NULL);
    if (r || !usessl) usessl = "no";

    if (*usessl == '1' || *usessl == 'y'  || *usessl == 't' ||
	(*usessl == 'o' && usessl[1] == 'n')) {
	settings->sql_usessl = 1;
    } else {
	settings->sql_usessl = 0;
    }
}

static void *sql_connect(sql_settings_t *settings, const sasl_utils_t *utils)
{
    void *conn = NULL;
    char *db_host_ptr = NULL;
    char *db_host = NULL;
    char *cur_host, *cur_port;

    /* loop around hostnames till we get a connection 
     * it should probably save the connection but for 
     * now we will just disconnect everytime
     */
    utils->log(utils->conn, SASL_LOG_DEBUG,
	       "sql plugin try and connect to a host\n");
    
    /* create a working version of the hostnames */
    _plug_strdup(utils, settings->sql_hostnames, &db_host_ptr, NULL);
    db_host = db_host_ptr;
    cur_host = db_host;
    while (cur_host != NULL) {
	db_host = strchr(db_host,',');
	if (db_host != NULL) {  
	    db_host[0] = '\0';

	    /* loop till we find some text */
	    while (!isalnum(db_host[0])) db_host++;
	}
	
	utils->log(utils->conn, SASL_LOG_DEBUG,
		   "sql plugin trying to open db '%s' on host '%s'%s\n",
		   settings->sql_database, cur_host,
		   settings->sql_usessl ? " using SSL" : "");
	
	/* set the optional port */
	if ((cur_port = strchr(cur_host, ':'))) *cur_port++ = '\0';
	
	conn = settings->sql_engine->sql_open(cur_host, cur_port,
					      settings->sql_usessl,
					      settings->sql_user,
					      settings->sql_passwd,
					      settings->sql_database,
					      utils);
	if (conn) break;
	
	utils->log(utils->conn, SASL_LOG_ERR,
		   "sql plugin could not connect to host %s", cur_host);
	
	cur_host = db_host;
    }

    if (db_host_ptr) utils->free(db_host_ptr);

    return conn;
}

static int sql_auxprop_lookup(void *glob_context,
			       sasl_server_params_t *sparams,
			       unsigned flags,
			       const char *user,
			       unsigned ulen) 
{
    char *userid = NULL;
    /* realm could be used for something clever */
    char *realm = NULL;
    const char *user_realm = NULL;
    const struct propval *to_fetch, *cur;
    char value[8192];
    size_t value_len;
    char *user_buf;
    char *query = NULL;
    char *escap_userid = NULL;
    char *escap_realm = NULL;
    sql_settings_t *settings;
    int verify_against_hashed_password;
    int saw_user_password = 0;
    void *conn = NULL;
    int do_txn = 0;
    int ret;
    
    if (!glob_context || !sparams || !user) return SASL_BADPARAM;
    
    /* setup the settings */
    settings = (sql_settings_t *) glob_context;
    
    sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
			"sql plugin Parse the username %s\n", user);
    
    user_buf = sparams->utils->malloc(ulen + 1);
    if (!user_buf) {
	ret = SASL_NOMEM;
	goto done;
    }
    
    memcpy(user_buf, user, ulen);
    user_buf[ulen] = '\0';
    
    if(sparams->user_realm) {
	user_realm = sparams->user_realm;
    } else {
	user_realm = sparams->serverFQDN;
    }
    
    if ((ret = _plug_parseuser(sparams->utils,
			       &userid,
			       &realm,
			       user_realm,
			       sparams->serverFQDN,
			       user_buf)) != SASL_OK ) {
	goto done;
    }
    
    /* just need to escape userid and realm now */
    /* allocate some memory */
    escap_userid = (char *)sparams->utils->malloc(strlen(userid)*2+1);
    escap_realm = (char *)sparams->utils->malloc(strlen(realm)*2+1);
    
    if (!escap_userid || !escap_realm) {
	ret = SASL_NOMEM;
	goto done;
    }
    
    /*************************************/
    
    /* find out what we need to get */
    /* this corrupts const char *user */
    to_fetch = sparams->utils->prop_get(sparams->propctx);
    if (!to_fetch) {
	ret = SASL_NOMEM;
	goto done;
    }

    conn = sql_connect(settings, sparams->utils);
    if (!conn) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
			    "sql plugin couldn't connect to any host\n");
	/* TODO: in the future we might want to extend the internal
	   SQL driver API to return a more detailed error */
	ret = SASL_FAIL;
	goto done;
    }
    
    /* escape out */
    settings->sql_engine->sql_escape_str(escap_userid, userid);
    settings->sql_engine->sql_escape_str(escap_realm, realm);

    verify_against_hashed_password = flags & SASL_AUXPROP_VERIFY_AGAINST_HASH;

    /* Assume that nothing is found */
    ret = SASL_NOUSER;
    for (cur = to_fetch; cur->name; cur++) {
	char *realname = (char *) cur->name;

	/* Only look up properties that apply to this lookup! */
	if (cur->name[0] == '*'
	    && (flags & SASL_AUXPROP_AUTHZID))
	    continue;
	if (!(flags & SASL_AUXPROP_AUTHZID)) {
	    if(cur->name[0] != '*')
		continue;
	    else
		realname = (char*)cur->name + 1;
	}
	
	/* If it's there already, we want to see if it needs to be
	 * overridden. userPassword is a special case, because it's value
	   is always present if SASL_AUXPROP_VERIFY_AGAINST_HASH is specified.
	   When SASL_AUXPROP_VERIFY_AGAINST_HASH is set, we just clear userPassword. */
	if (cur->values && !(flags & SASL_AUXPROP_OVERRIDE) &&
	    (verify_against_hashed_password == 0 ||
	     strcasecmp(realname, SASL_AUX_PASSWORD_PROP) != 0)) {
	    continue;
	} else if (cur->values) {
	    sparams->utils->prop_erase(sparams->propctx, cur->name);
	}

	if (strcasecmp(realname, SASL_AUX_PASSWORD_PROP) == 0) {
	    saw_user_password = 1;
	}

	if (!do_txn) {
	    do_txn = 1;
	    sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
				"begin transaction");
	    if (settings->sql_engine->sql_begin_txn(conn, sparams->utils)) {
		sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
				    "Unable to begin transaction\n");
	    }
	}
    
	sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
			    "sql plugin create statement from %s %s %s\n",
			    realname, escap_userid, escap_realm);
	
	/* create a statement that we will use */
	query = sql_create_statement(settings->sql_select,
				     realname,escap_userid,
				     escap_realm, NULL,
				     sparams->utils);
	if (query == NULL) {
	    ret = SASL_NOMEM;
	    break;
	}
	
	sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
			    "sql plugin doing query %s\n", query);
	
	value[0] = '\0';
	value_len = 0;
	/* run the query */
	if (!settings->sql_engine->sql_exec(conn, query, value, sizeof(value),
					    &value_len, sparams->utils)) {
	    sparams->utils->prop_set(sparams->propctx,
				     cur->name,
				     value,
				     (int)value_len);
	    ret = SASL_OK;
	}
	
	sparams->utils->free(query);
    }

    if (flags & SASL_AUXPROP_AUTHZID) {
	/* This is a lie, but the caller can't handle
	   when we return SASL_NOUSER for authorization identity lookup. */
	if (ret == SASL_NOUSER) {
	    ret = SASL_OK;
	}
    } else {
	if (ret == SASL_NOUSER && saw_user_password == 0) {
	    /* Verify user existence by checking presence of
	       the userPassword attribute */
	    if (!do_txn) {
		do_txn = 1;
		sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
				    "begin transaction");
		if (settings->sql_engine->sql_begin_txn(conn, sparams->utils)) {
		    sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
					"Unable to begin transaction\n");
		}
	    }

	    sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
				"sql plugin create statement from %s %s %s\n",
				SASL_AUX_PASSWORD_PROP,
				escap_userid,
				escap_realm);
    	
	    /* create a statement that we will use */
	    query = sql_create_statement(settings->sql_select,
					 SASL_AUX_PASSWORD_PROP,
					 escap_userid,
					 escap_realm,
					 NULL,
					 sparams->utils);
	    if (query == NULL) {
		ret = SASL_NOMEM;
	    } else {
		sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
				    "sql plugin doing query %s\n", query);
        	
		value[0] = '\0';
		value_len = 0;
		/* run the query */
		if (!settings->sql_engine->sql_exec(conn,
						    query,
						    value,
						    sizeof(value),
						    &value_len,
						    sparams->utils)) {
		    ret = SASL_OK;
		}
        	
		sparams->utils->free(query);
	    }
	}
    }


    if (do_txn) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
			    "commit transaction");
	if (settings->sql_engine->sql_commit_txn(conn, sparams->utils)) {
	    sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
				"Unable to commit transaction\n");
	    /* Failure of the commit is non fatal when reading values */
	}
    }
    
  done:
    if (escap_userid) sparams->utils->free(escap_userid);
    if (escap_realm) sparams->utils->free(escap_realm);
    if (conn) settings->sql_engine->sql_close(conn);
    if (userid) sparams->utils->free(userid);
    if (realm) sparams->utils->free(realm);
    if (user_buf) sparams->utils->free(user_buf);

    return (ret);
}

static int sql_auxprop_store(void *glob_context,
			     sasl_server_params_t *sparams,
			     struct propctx *ctx,
			     const char *user,
			     unsigned ulen) 
{
    char *userid = NULL;
    char *realm = NULL;
    const char *user_realm = NULL;
    int ret = SASL_FAIL;
    const struct propval *to_store, *cur;
    
    char *user_buf;
    char *statement = NULL;
    char *escap_userid = NULL;
    char *escap_realm = NULL;
    char *escap_passwd = NULL;
    const char *cmd;
    
    sql_settings_t *settings;
    void *conn = NULL;
    
    settings = (sql_settings_t *) glob_context; 

    /* just checking if we are enabled */
    if (!ctx &&
	sql_exists(settings->sql_insert) &&
	sql_exists(settings->sql_update)) return SASL_OK;
    
    /* make sure our input is okay */
    if (!glob_context || !sparams || !user) return SASL_BADPARAM;
    
    sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
			"sql plugin Parse the username %s\n", user);
    
    user_buf = sparams->utils->malloc(ulen + 1);
    if (!user_buf) {
	ret = SASL_NOMEM;
	goto done;
    }
    
    memcpy(user_buf, user, ulen);
    user_buf[ulen] = '\0';
    
    if (sparams->user_realm) {
	user_realm = sparams->user_realm;
    }
    else {
	user_realm = sparams->serverFQDN;
    }
    
    ret = _plug_parseuser(sparams->utils, &userid, &realm, user_realm,
			  sparams->serverFQDN, user_buf);
    if (ret != SASL_OK)	goto done;
    
    /* just need to escape userid and realm now */
    /* allocate some memory */
    
    escap_userid = (char *) sparams->utils->malloc(strlen(userid)*2+1);
    escap_realm = (char *) sparams->utils->malloc(strlen(realm)*2+1);
    
    if (!escap_userid || !escap_realm) {
	MEMERROR(sparams->utils);
	goto done;
    }
    
    to_store = sparams->utils->prop_get(ctx);
    
    if (!to_store) {
	ret = SASL_BADPARAM;
	goto done;
    }

    conn = sql_connect(settings, sparams->utils);
    if (!conn) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
			    "sql plugin couldn't connect to any host\n");
	goto done;
    }
    
    settings->sql_engine->sql_escape_str(escap_userid, userid);
    settings->sql_engine->sql_escape_str(escap_realm, realm);
    
    if (settings->sql_engine->sql_begin_txn(conn, sparams->utils)) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
			    "Unable to begin transaction\n");
    }
    for (cur = to_store; ret == SASL_OK && cur->name; cur++) {
	/* Free the buffer, current content is from previous loop. */
	if (escap_passwd) {
	    sparams->utils->free(escap_passwd);
	    escap_passwd = NULL;
	}

	if (cur->name[0] == '*') {
	    continue;
	}

	/* determine which command we need */
	/* see if we already have a row for this user */
	statement = sql_create_statement(settings->sql_select,
					 SQL_WILDCARD, escap_userid,
					 escap_realm, NULL,
					 sparams->utils);
	if (!settings->sql_engine->sql_exec(conn, statement, NULL, 0, NULL,
					    sparams->utils)) {
	    /* already have a row => UPDATE */
	    cmd = settings->sql_update;
	} else {
	    /* new row => INSERT */
	    cmd = settings->sql_insert;
	}
	sparams->utils->free(statement);

	if (cur->values[0]) {
	    escap_passwd = (char *)sparams->utils->malloc(strlen(cur->values[0])*2+1);
	    if (!escap_passwd) {
		ret = SASL_NOMEM;
		break;
	    }
	    settings->sql_engine->sql_escape_str(escap_passwd, cur->values[0]);
	}

	/* create a statement that we will use */
	statement = sql_create_statement(cmd, cur->name, escap_userid,
					 escap_realm,
					 escap_passwd ?
					 escap_passwd : SQL_NULL_VALUE,
					 sparams->utils);
	if (!statement) {
	    ret = SASL_NOMEM;
	    break;
	}
	
	{
	    char *log_statement =
		sql_create_statement(cmd, cur->name,
				     escap_userid,
				     escap_realm,
				     escap_passwd ?
				     "<omitted>" : SQL_NULL_VALUE,
				     sparams->utils);
	    sparams->utils->log(sparams->utils->conn, SASL_LOG_DEBUG,
				"sql plugin doing statement %s\n",
				log_statement);
	    sparams->utils->free(log_statement);
	}
	
	/* run the statement */
	if (settings->sql_engine->sql_exec(conn, statement, NULL, 0, NULL,
					   sparams->utils)) {
	    ret = SASL_FAIL;
	}
	
	sparams->utils->free(statement);
    }
    if (ret != SASL_OK) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
			    "Failed to store auxprop; aborting transaction\n");
	if (settings->sql_engine->sql_rollback_txn(conn, sparams->utils)) {
	    sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
				"Unable to rollback transaction\n");
	}
    }
    else if (settings->sql_engine->sql_commit_txn(conn, sparams->utils)) {
	sparams->utils->log(sparams->utils->conn, SASL_LOG_ERR,
			    "Unable to commit transaction\n");
    }
    
  done:
    if (escap_userid) sparams->utils->free(escap_userid);
    if (escap_realm) sparams->utils->free(escap_realm);
    if (escap_passwd) sparams->utils->free(escap_passwd);
    if (conn) settings->sql_engine->sql_close(conn);
    if (userid) sparams->utils->free(userid);
    if (realm) sparams->utils->free(realm);
    if (user_buf) sparams->utils->free(user_buf);
    
    return ret;
    
    /* do a little dance */
}


static void sql_auxprop_free(void *glob_context, const sasl_utils_t *utils)
{
    sql_settings_t *settings;
    
    settings = (sql_settings_t *)glob_context;
    
    if (!settings) return;
    
    utils->log(utils->conn, SASL_LOG_DEBUG, "sql freeing memory\n");
    
    utils->free(settings);
}

static sasl_auxprop_plug_t sql_auxprop_plugin = {
    0,			/* Features */
    0,			/* spare */
    NULL,		/* glob_context */
    sql_auxprop_free,	/* auxprop_free */
    sql_auxprop_lookup,	/* auxprop_lookup */
    "sql",		/* name */
    sql_auxprop_store	/* auxprop_store */
};

int sql_auxprop_plug_init(const sasl_utils_t *utils,
			  int max_version,
			  int *out_version,
			  sasl_auxprop_plug_t **plug,
			  const char *plugname __attribute__((unused))) 
{
    sql_settings_t *settings;
    
    if (!out_version || !plug) return SASL_BADPARAM;
    
    if (max_version < SASL_AUXPROP_PLUG_VERSION) return SASL_BADVERS;
    *out_version = SASL_AUXPROP_PLUG_VERSION;
    
    *plug = &sql_auxprop_plugin;
    
    settings = (sql_settings_t *) utils->malloc(sizeof(sql_settings_t));
    
    if (!settings) {
	MEMERROR(utils);
	return SASL_NOMEM;
    }
    
    memset(settings, 0, sizeof(sql_settings_t));
    sql_get_settings(utils, settings);
    
    if (!settings->sql_engine->name) return SASL_NOMECH;

    if (!sql_exists(settings->sql_select)) {
	utils->log(utils->conn, SASL_LOG_ERR, "sql_select option missing");
	utils->free(settings);	
	return SASL_NOMECH;
    }

    utils->log(utils->conn, SASL_LOG_DEBUG,
	       "sql auxprop plugin using %s engine\n",
	       settings->sql_engine->name);
    
    sql_auxprop_plugin.glob_context = settings;
    
    return SASL_OK;
}
