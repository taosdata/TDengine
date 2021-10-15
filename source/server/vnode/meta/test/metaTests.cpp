#include <gtest/gtest.h>
#include <string.h>
#include <iostream>

#include "meta.h"

static STSchema *metaGetSimpleSchema() {
  STSchema *      pSchema = NULL;
  STSchemaBuilder sb;

  tdInitTSchemaBuilder(&sb, 0);
  tdAddColToSchema(&sb, TSDB_DATA_TYPE_TIMESTAMP, 0, 8);
  tdAddColToSchema(&sb, TSDB_DATA_TYPE_INT, 1, 4);

  pSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  return pSchema;
}

TEST(MetaTest, meta_open_test) {
  // Open Meta
  SMeta *meta = metaOpen(NULL);
  std::cout << "Meta is opened!" << std::endl;

  // Create 1000000 normal tables
  META_TABLE_OPTS_DECLARE(tbOpts)
  STSchema *pSchema = metaGetSimpleSchema();
  char      tbname[128];

  for (size_t i = 0; i < 1000000; i++) {
    sprintf(tbname, "ntb%ld", i);
    metaNormalTableOptsInit(&tbOpts, tbname, pSchema);
    metaCreateTable(meta, &tbOpts);
    metaTableOptsDestroy(&tbOpts);
  }

  // Close Meta
  metaClose(meta);
  std::cout << "Meta is closed!" << std::endl;

  // // Destroy Meta
  // metaDestroy("meta");
  // std::cout << "Meta is destroyed!" << std::endl;
}