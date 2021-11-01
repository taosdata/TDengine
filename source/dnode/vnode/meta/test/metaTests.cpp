#include <gtest/gtest.h>
#include <string.h>
#include <iostream>

#include "meta.h"

static STSchema *metaGetSimpleSchema() {
  STSchema *      pSchema = NULL;
  STSchemaBuilder sb = {0};

  tdInitTSchemaBuilder(&sb, 0);
  tdAddColToSchema(&sb, TSDB_DATA_TYPE_TIMESTAMP, 0, 8);
  tdAddColToSchema(&sb, TSDB_DATA_TYPE_INT, 1, 4);

  pSchema = tdGetSchemaFromBuilder(&sb);
  tdDestroyTSchemaBuilder(&sb);

  return pSchema;
}

static SKVRow metaGetSimpleTags() {
  SKVRowBuilder kvrb = {0};
  SKVRow        row;

  tdInitKVRowBuilder(&kvrb);
  int64_t ts = 1634287978000;
  int32_t a = 10;

  tdAddColToKVRow(&kvrb, 0, TSDB_DATA_TYPE_TIMESTAMP, (void *)(&ts));
  tdAddColToKVRow(&kvrb, 0, TSDB_DATA_TYPE_INT, (void *)(&a));

  row = tdGetKVRowFromBuilder(&kvrb);

  tdDestroyKVRowBuilder(&kvrb);

  return row;
}

TEST(MetaTest, DISABLED_meta_create_1m_normal_tables_test) {
  // Open Meta
  SMeta *meta = metaOpen(NULL);
  std::cout << "Meta is opened!" << std::endl;

  // Create 1000000 normal tables
  META_TABLE_OPTS_DECLARE(tbOpts);
  STSchema *pSchema = metaGetSimpleSchema();
  char      tbname[128];

  for (size_t i = 0; i < 1000000; i++) {
    sprintf(tbname, "ntb%ld", i);
    metaNormalTableOptsInit(&tbOpts, tbname, pSchema);
    metaCreateTable(meta, &tbOpts);
    metaTableOptsClear(&tbOpts);
  }

  tdFreeSchema(pSchema);

  // Close Meta
  metaClose(meta);
  std::cout << "Meta is closed!" << std::endl;

  // Destroy Meta
  metaDestroy("meta");
  std::cout << "Meta is destroyed!" << std::endl;
}

TEST(MetaTest, meta_create_1m_child_tables_test) {
  // Open Meta
  SMeta *meta = metaOpen(NULL);
  std::cout << "Meta is opened!" << std::endl;

  // Create a super tables
  tb_uid_t uid = 477529885843758ul;
  META_TABLE_OPTS_DECLARE(tbOpts);
  STSchema *pSchema = metaGetSimpleSchema();
  STSchema *pTagSchema = metaGetSimpleSchema();

  metaSuperTableOptsInit(&tbOpts, "st", uid, pSchema, pTagSchema);
  metaCreateTable(meta, &tbOpts);
  metaTableOptsClear(&tbOpts);

  tdFreeSchema(pSchema);
  tdFreeSchema(pTagSchema);

  // Create 1000000 child tables
  char   name[128];
  SKVRow row = metaGetSimpleTags();
  for (size_t i = 0; i < 1000000; i++) {
    sprintf(name, "ctb%ld", i);
    metaChildTableOptsInit(&tbOpts, name, uid, row);
    metaCreateTable(meta, &tbOpts);
    metaTableOptsClear(&tbOpts);
  }
  kvRowFree(row);

  // Close Meta
  metaClose(meta);
  std::cout << "Meta is closed!" << std::endl;

  // Destroy Meta
  metaDestroy("meta");
  std::cout << "Meta is destroyed!" << std::endl;
}