#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"

// refer: https://docs.oracle.com/cd/E17076_05/html/gsg/C/BerkeleyDB-Core-C-GSG.pdf

// Access Methods:
// 1. BTree
// 2. Hash
// 3. Queue
// 4. Recno

// Use secondary database work as index.
// Any attemp to write to a secondary database results in a non-zero status return.

static int idx_callback(DB *dbp, const DBT *keyp, const DBT *valuep, DBT *resvp);

#define USE_ENV 0

#define DESCRIPTION_SIZE 128
float money = 122.45;
char *description = "Grocery bill.";

typedef struct {
  int   id;
  char *family_name;
  char *surname;
} SPersion;

static void put_value(DB *dbp) {
  DBT key = {0};
  DBT value = {0};
  int ret;

  key.data = &money;
  key.size = sizeof(money);

  value.data = description;
  value.size = strlen(description) + 1;

  ret = dbp->put(dbp, NULL, &key, &value, DB_NOOVERWRITE);
  if (ret != 0) {
    fprintf(stderr, "Failed to put DB record: %s\n", db_strerror(ret));
  }
}

static void get_value(DB *dbp) {
  char desp[DESCRIPTION_SIZE];
  DBT  key = {0};
  DBT  value = {0};

  key.data = &money;
  key.size = sizeof(money);

  value.data = desp;
  value.ulen = DESCRIPTION_SIZE;
  value.flags = DB_DBT_USERMEM;

  dbp->get(dbp, NULL, &key, &value, 0);
  printf("The value is \"%s\"\n", desp);
}

int main(int argc, char const *argv[]) {
  DB *      dbp = NULL;
  DB *      sdbp = NULL;
  u_int32_t db_flags;
  DB_ENV *  envp = NULL;
  u_int32_t env_flags;
  int       ret;
  DBT       key = {0};
  DBT       value = {0};

#if USE_ENV
  // Initialize an env object and open it for
  ret = db_env_create(&envp, 0);
  if (ret != 0) {
    fprintf(stderr, "Error creating env handle: %s\n", db_strerror(ret));
    return -1;
  }

  env_flags = DB_CREATE | DB_INIT_MPOOL;

  ret = envp->open(envp, "./meta", env_flags, 0);
  if (ret != 0) {
    fprintf(stderr, "Error opening env handle: %s\n", db_strerror(ret));
    return -1;
  }
#endif

  // Initialize a DB handle and open the DB
  ret = db_create(&dbp, envp, 0);
  if (ret != 0) {
    exit(1);
  }

  ret = db_create(&sdbp, envp, 0);
  if (ret != 0) {
    exit(1);
  }

  ret = sdbp->set_flags(sdbp, DB_DUPSORT);
  if (ret != 0) {
    exit(1);
  }

  db_flags = DB_CREATE | DB_TRUNCATE;
  ret = dbp->open(dbp,       /* DB structure pointer */
                  NULL,      /* Transaction pointer */
                  "meta.db", /* On-disk file that holds the database */
                  NULL,      /* Optional logical database name */
                  DB_BTREE,  /* Database access method */
                  db_flags,  /* Open flags */
                  0);        /* File mode */
  if (ret != 0) {
    exit(1);
  }

  ret = sdbp->open(sdbp,       /* DB structure pointer */
                   NULL,       /* Transaction pointer */
                   "index.db", /* On-disk file that holds the database */
                   NULL,       /* Optional logical database name */
                   DB_BTREE,   /* Database access method */
                   db_flags,   /* Open flags */
                   0);         /* File mode */
  if (ret != 0) {
    exit(1);
  }

  // Associate the secondary database to the primary
  dbp->associate(dbp,          /* Primary database */
                 NULL,         /* TXN id */
                 sdbp,         /* Secondary database */
                 idx_callback, /* Callback used for key creation */
                 0);           /* Flags */

  {
    // Insert a key-value record
    put_value(dbp);

    // Read the key-value record
    get_value(dbp);
  }

  // Close the database
  if (sdbp != NULL) {
    sdbp->close(sdbp, 0);
  }

  if (dbp != NULL) {
    dbp->close(dbp, 0);
  }

  if (envp != NULL) {
    envp->close(envp, 0);
  }

  return 0;
}

static int idx_callback(DB *       sdbp,   /* secondary db handle */
                        const DBT *keyp,   /* primary db record's key */
                        const DBT *valuep, /* primary db record's value */
                        DBT *      skeyp   /* secondary db record's key*/
) {
  DBT *tmpdbt;

  tmpdbt = (DBT *)calloc(2, sizeof(DBT));

  {  // TODO
    tmpdbt[0].data = NULL;
    tmpdbt[0].size = 0;

    tmpdbt[1].data = NULL;
    tmpdbt[1].size = 0;
  }

  /**
   * DB_DBT_MULTIPLE means DBT references an array
   * DB_DBT_APPMALLOC means we dynamically allocated memory for the DBT's data field.
   */
  skeyp->flags = DB_DBT_MULTIPLE | DB_DBT_APPMALLOC;
  skeyp->size = 2;
  skeyp->data = tmpdbt;

  return 0;
}
