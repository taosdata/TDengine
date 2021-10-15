#include <gtest/gtest.h>
#include <string.h>
#include <iostream>

#include "meta.h"

TEST(MetaTest, meta_open_test) {
  // Open Meta
  SMeta *meta = metaOpen(NULL);
  std::cout << "Meta is opened!" << std::endl;

#if 0
  // Create tables
  STableOpts      tbOpts;
  char            tbname[128];
  STSchema *      pSchema;
  STSchemaBuilder sb;
  tdInitTSchemaBuilder(&sb, 0);
  for (size_t i = 0; i < 10; i++) {
    tdAddColToSchema(&sb, TSDB_DATA_TYPE_TIMESTAMP, i, 8);
  }
  pSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);
  for (size_t i = 0; i < 1000000; i++) {
    sprintf(tbname, "tb%ld", i);
    metaTableOptsInit(&tbOpts, 0, tbname, pSchema);

    metaCreateTable(meta, &tbOpts);
  }
#endif

  // Close Meta
  metaClose(meta);
  std::cout << "Meta is closed!" << std::endl;

  // // Destroy Meta
  // metaDestroy("meta");
  // std::cout << "Meta is destroyed!" << std::endl;
}