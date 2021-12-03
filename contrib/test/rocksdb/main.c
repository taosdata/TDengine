#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>  // sysconf() - get CPU count
#include "rocksdb/c.h"

// const char DBPath[] = "/tmp/rocksdb_c_simple_example";
const char DBPath[] = "rocksdb_c_simple_example";
const char DBBackupPath[] = "/tmp/rocksdb_c_simple_example_backup";

int main(int argc, char const *argv[]) {
  rocksdb_t *              db;
  rocksdb_backup_engine_t *be;
  rocksdb_options_t *      options = rocksdb_options_create();
  rocksdb_options_set_create_if_missing(options, 1);

  // open DB
  char *err = NULL;
  db = rocksdb_open(options, DBPath, &err);

  // Write
  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  rocksdb_put(db, writeoptions, "key", 3, "value", 5, &err);

  // Read
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  rocksdb_readoptions_set_snapshot(readoptions, rocksdb_create_snapshot(db));
  size_t vallen = 0;
  char * val = rocksdb_get(db, readoptions, "key", 3, &vallen, &err);
  printf("val:%s\n", val);

  // Update
  // rocksdb_put(db, writeoptions, "key", 3, "eulav", 5, &err);

  // Delete
  rocksdb_delete(db, writeoptions, "key", 3, &err);

  // Read again
  val = rocksdb_get(db, readoptions, "key", 3, &vallen, &err);
  printf("val:%s\n", val);

  rocksdb_close(db);

  return 0;
}