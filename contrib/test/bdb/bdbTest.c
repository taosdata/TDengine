#include <stdio.h>
#include <stdlib.h>

#include "db.h"

// refer: https://docs.oracle.com/cd/E17076_05/html/gsg/C/BerkeleyDB-Core-C-GSG.pdf

// Access Methods:
// 1. BTree
// 2. Hash
// 3. Queue
// 4. Recno

#define USE_ENV 0

int main(int argc, char const *argv[]) {
  DB *      dbp = NULL;
  u_int32_t db_flags;
  DB_ENV *  envp = NULL;
  u_int32_t env_flags;
  int       ret;
  DBT       key, value;  // Database records

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

  db_flags = DB_CREATE;
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

  // Close the database
  if (dbp != NULL) {
    dbp->close(dbp, 0);
  }

  if (envp != NULL) {
    envp->close(envp, 0);
  }

  return 0;
}
