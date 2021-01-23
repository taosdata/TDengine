#include <gtest/gtest.h>
#include <sys/time.h>
#include <cassert>
#include <iostream>

#include "texpr.h"
#include "taosmsg.h"
#include "tsdb.h"
#include "tskiplist.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"

typedef struct ResultObj {
  int32_t numOfResult;
  char *  resultName[64];
} ResultObj;

static void initSchema(SSchema *pSchema, int32_t numOfCols);

static void initSchema_binary(SSchema *schema, int32_t numOfCols);

static SSkipList *createSkipList(SSchema *pSchema, int32_t numOfTags);
static SSkipList *createSkipList_binary(SSchema *pSchema, int32_t numOfTags);

static void dropMeter(SSkipList *pSkipList);

static void Right2LeftTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList);

static void Left2RightTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList);

static void IllegalExprTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList);

static void Left2RightTest_binary(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList);
static void Right2LeftTest_binary(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList);

void setValue(ResultObj *pResult, int32_t num, char **val) {
  pResult->numOfResult = num;
  for (int32_t i = 0; i < num; ++i) {
    pResult->resultName[i] = val[i];
  }
}

static void initSchema_binary(SSchema *schema, int32_t numOfCols) {
  schema[0].type = TSDB_DATA_TYPE_BINARY;
  schema[0].bytes = 8;
  strcpy(schema[0].name, "a");

  schema[1].type = TSDB_DATA_TYPE_DOUBLE;
  schema[1].bytes = 8;
  strcpy(schema[1].name, "b");

  schema[2].type = TSDB_DATA_TYPE_INT;
  schema[2].bytes = 20;
  strcpy(schema[2].name, "c");

  schema[3].type = TSDB_DATA_TYPE_BIGINT;
  schema[3].bytes = 8;
  strcpy(schema[3].name, "d");

  schema[4].type = TSDB_DATA_TYPE_SMALLINT;
  schema[4].bytes = 2;
  strcpy(schema[4].name, "e");

  schema[5].type = TSDB_DATA_TYPE_TINYINT;
  schema[5].bytes = 1;
  strcpy(schema[5].name, "f");

  schema[6].type = TSDB_DATA_TYPE_FLOAT;
  schema[6].bytes = 4;
  strcpy(schema[6].name, "g");

  schema[7].type = TSDB_DATA_TYPE_BOOL;
  schema[7].bytes = 1;
  strcpy(schema[7].name, "h");
}

static void initSchema(SSchema *schema, int32_t numOfCols) {
  schema[0].type = TSDB_DATA_TYPE_INT;
  schema[0].bytes = 8;
  strcpy(schema[0].name, "a");

  schema[1].type = TSDB_DATA_TYPE_DOUBLE;
  schema[1].bytes = 8;
  strcpy(schema[1].name, "b");

  schema[2].type = TSDB_DATA_TYPE_BINARY;
  schema[2].bytes = 20;
  strcpy(schema[2].name, "c");

  schema[3].type = TSDB_DATA_TYPE_BIGINT;
  schema[3].bytes = 8;
  strcpy(schema[3].name, "d");

  schema[4].type = TSDB_DATA_TYPE_SMALLINT;
  schema[4].bytes = 2;
  strcpy(schema[4].name, "e");

  schema[5].type = TSDB_DATA_TYPE_TINYINT;
  schema[5].bytes = 1;
  strcpy(schema[5].name, "f");

  schema[6].type = TSDB_DATA_TYPE_FLOAT;
  schema[6].bytes = 4;
  strcpy(schema[6].name, "g");

  schema[7].type = TSDB_DATA_TYPE_BOOL;
  schema[7].bytes = 1;
  strcpy(schema[7].name, "h");
}

// static void addOneNode(SSchema *pSchema, int32_t tagsLen, SSkipList *pSkipList,
//                       char *meterId, int32_t a, double b, char *c, int64_t d, int16_t e, int8_t f, float g,
//                       bool h, int32_t numOfTags) {
//  STabObj *pMeter = calloc(1, sizeof(STabObj));
//  pMeter->numOfTags = numOfTags;
//  pMeter->pTagData = calloc(1, tagsLen + TSDB_METER_ID_LEN);
//  strcpy(pMeter->meterId, meterId);
//
//  char *tags = pMeter->pTagData + TSDB_METER_ID_LEN;
//  int32_t offset = 0;
//
//  *(int32_t *) tags = a;
//
//  offset += pSchema[0].bytes;
//  *(double *) (tags + offset) = b;
//
//  offset += pSchema[1].bytes;
//  memcpy(tags + offset, c, 3);
//
//  offset += pSchema[2].bytes;
//  *(int64_t *) (tags + offset) = d;
//
//  offset += pSchema[3].bytes;
//  *(int16_t *) (tags + offset) = e;
//
//  offset += pSchema[4].bytes;
//  *(int8_t *) (tags + offset) = f;
//
//  offset += pSchema[5].bytes;
//  *(float *) (tags + offset) = g;
//
//  offset += pSchema[6].bytes;
//  *(int8_t *) (tags + offset) = h ? 1 : 0;
//
//  SSkipListKey pKey = SSkipListCreateKey(pSchema[0].type, tags, pSchema[0].bytes);
//  SSkipListPut(pSkipList, pMeter, &pKey, 1);
//}
//
// static void addOneNode_binary(SSchema *pSchema, int32_t tagsLen, SSkipList *pSkipList,
//                              char *meterId, int32_t a, double b, char *c, int64_t d, int16_t e, int8_t f, float g,
//                              bool h, int32_t numOfTags) {
//  STabObj *pMeter = calloc(1, sizeof(STabObj));
//  pMeter->numOfTags = numOfTags;
//  pMeter->pTagData = calloc(1, tagsLen + TSDB_METER_ID_LEN);
//  strcpy(pMeter->meterId, meterId);
//
//  char *tags = pMeter->pTagData + TSDB_METER_ID_LEN;
//  int32_t offset = 0;
//  memcpy(tags, c, pSchema[0].bytes);
//
//  offset += pSchema[0].bytes;
//  *(double *) (tags + offset) = b;
//
//  offset += pSchema[1].bytes;
//  *(int32_t *) (tags + offset) = a;
//
//  offset += pSchema[2].bytes;
//  *(int64_t *) (tags + offset) = d;
//
//  offset += pSchema[3].bytes;
//  *(int16_t *) (tags + offset) = e;
//
//  offset += pSchema[4].bytes;
//  *(int8_t *) (tags + offset) = f;
//
//  offset += pSchema[5].bytes;
//  *(float *) (tags + offset) = g;
//
//  offset += pSchema[6].bytes;
//  *(int8_t *) (tags + offset) = h ? 1 : 0;
//
//  SSkipListKey pKey = SSkipListCreateKey(pSchema[0].type, tags, pSchema[0].bytes);
//  SSkipListPut(pSkipList, pMeter, &pKey, 1);
//  SSkipListDestroyKey(&pKey);
//}

// static void dropMeter(SSkipList *pSkipList) {
//  SSkipListNode **pRes = NULL;
//  int32_t num = SSkipListIterateList(pSkipList, &pRes, NULL, NULL);
//  for (int32_t i = 0; i < num; ++i) {
//    SSkipListNode *pNode = pRes[i];
//    STabObj *pMeter = (STabObj *) pNode->pData;
//    free(pMeter->pTagData);
//    free(pMeter);
//    pNode->pData = NULL;
//  }
//  free(pRes);
//}

// static SSkipList *createSkipList(SSchema *pSchema, int32_t numOfTags) {
//  int32_t tagsLen = 0;
//  for (int32_t i = 0; i < numOfTags; ++i) {
//    tagsLen += pSchema[i].bytes;
//  }
//
//  SSkipList *pSkipList = SSkipListCreate(10, pSchema[0].type, 4);
//
//  addOneNode(pSchema, tagsLen, pSkipList, "tm0\0", 0, 10.5, "abc", 1000, -10000, -20, 1.0, true, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm1\0", 1, 20.5, "def", 1100, -10500, -30, 2.0, false, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm2\0", 2, 30.5, "ghi", 1200, -11000, -40, 3.0, true, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm3\0", 3, 40.5, "jkl", 1300, -11500, -50, 4.0, false, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm4\0", 4, 50.5, "mno", 1400, -12000, -60, 5.0, true, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm5\0", 5, 60.5, "pqr", 1500, -12500, -70, 6.0, false, 8);
//  addOneNode(pSchema, tagsLen, pSkipList, "tm6\0", 6, 70.5, "stu", 1600, -13000, -80, 7.0, true, 8);
//
//  return pSkipList;
//}
//
// static SSkipList *createSkipList_binary(SSchema *pSchema, int32_t numOfTags) {
//  int32_t tagsLen = 0;
//  for (int32_t i = 0; i < numOfTags; ++i) {
//    tagsLen += pSchema[i].bytes;
//  }
//
//  SSkipList *pSkipList = SSkipListCreate(10, pSchema[0].type, 4);
//
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm0\0", 0, 10.5, "abc", 1000, -10000, -20, 1.0, true, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm1\0", 1, 20.5, "def", 1100, -10500, -30, 2.0, false, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm2\0", 2, 30.5, "ghi", 1200, -11000, -40, 3.0, true, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm3\0", 3, 40.5, "jkl", 1300, -11500, -50, 4.0, false, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm4\0", 4, 50.5, "mno", 1400, -12000, -60, 5.0, true, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm5\0", 5, 60.5, "pqr", 1500, -12500, -70, 6.0, false, 8);
//  addOneNode_binary(pSchema, tagsLen, pSkipList, "tm6\0", 6, 70.5, "stu", 1600, -13000, -80, 7.0, true, 8);
//
//  return pSkipList;
//}

//static void testQueryStr(SSchema *schema, int32_t numOfCols, char *sql, SSkipList *pSkipList, ResultObj *pResult) {
//  tExprNode *pExpr = NULL;
//  tSQLBinaryExprFromString(&pExpr, schema, numOfCols, sql, strlen(sql));
//
//  char    str[512] = {0};
//  int32_t len = 0;
//  if (pExpr == NULL) {
//    printf("-----error in parse syntax:%s\n\n", sql);
//    assert(pResult == NULL);
//    return;
//  }
//
//  tSQLBinaryExprToString(pExpr, str, &len);
//  printf("expr is: %s\n", str);
//
//  SArray *result = NULL;
//  //  tExprTreeTraverse(pExpr, pSkipList, result, SSkipListNodeFilterCallback, &result);
//  //  printf("the result is:%lld\n", result.num);
//  //
//  //  bool findResult = false;
//  //  for (int32_t i = 0; i < result.num; ++i) {
//  //    STabObj *pm = (STabObj *)result.pRes[i];
//  //    printf("meterid:%s,\t", pm->meterId);
//  //
//  //    for (int32_t j = 0; j < pResult->numOfResult; ++j) {
//  //      if (strcmp(pm->meterId, pResult->resultName[j]) == 0) {
//  //        findResult = true;
//  //        break;
//  //      }
//  //    }
//  //    assert(findResult == true);
//  //    findResult = false;
//  //  }
//
//  printf("\n\n");
//  tExprTreeDestroy(&pExpr, NULL);
//}

#if 0
static void Left2RightTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList) {
  char str[256] = {0};

  char *t0[1] = {"tm0"};
  char *t1[1] = {"tm1"};
  char *sql = NULL;

  ResultObj res = {1, {"tm1"}};
  testQueryStr(schema, numOfCols, "a=1", pSkipList, &res);

  char *tt[1] = {"tm6"};
  setValue(&res, 1, tt);
  testQueryStr(schema, numOfCols, "a>=6", pSkipList, &res);

  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, "b<=10.6", pSkipList, &res);

  strcpy(str, "c<>'pqr'");
  char *t2[6] = {"tm0", "tm1", "tm2", "tm3", "tm4", "tm6"};
  setValue(&res, 6, t2);
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  strcpy(str, "c='abc'");
  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  char *t3[6] = {"tm1", "tm2", "tm3", "tm4", "tm5", "tm6"};
  setValue(&res, 6, t3);
  testQueryStr(schema, numOfCols, "d>1050", pSkipList, &res);

  char *t4[3] = {"tm4", "tm5", "tm6"};
  setValue(&res, 3, t4);
  testQueryStr(schema, numOfCols, "g>4.5980765", pSkipList, &res);

  char *t5[4] = {"tm0", "tm2", "tm4", "tm6"};
  setValue(&res, 4, t5);
  testQueryStr(schema, numOfCols, "h=true", pSkipList, &res);

  char *t6[3] = {"tm1", "tm3", "tm5"};
  setValue(&res, 3, t6);
  testQueryStr(schema, numOfCols, "h=0", pSkipList, &res);

  sql = "(((b<40)))\0";
  char *t7[3] = {"tm0", "tm1", "tm2"};
  setValue(&res, 3, t7);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);

  sql = "((a=1) or (a=10)) or ((b=12))";
  setValue(&res, 1, t1);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);

  sql = "((((((a>0 and a<2))) or a=6) or a=3) or (b=50.5)) and h=0";
  char *t8[2] = {"tm1", "tm3"};
  setValue(&res, 2, t8);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);

  char *tf[1] = {"tm6"};
  setValue(&res, 1, tf);
  testQueryStr(schema, numOfCols, "e = -13000", pSkipList, &res);

  char *ft[5] = {"tm0", "tm1", "tm2", "tm3", "tm4"};
  setValue(&res, 5, ft);
  testQueryStr(schema, numOfCols, "f > -65", pSkipList, &res);
}

void Right2LeftTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList) {
  ResultObj res = {1, {"tm1"}};
  testQueryStr(schema, numOfCols, "((1=a))", pSkipList, &res);

  char *t9[2] = {"tm0", "tm1"};
  setValue(&res, 2, t9);
  testQueryStr(schema, numOfCols, "1>=a", pSkipList, &res);

  char *t0[1] = {"tm0"};
  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, "10.6>=b", pSkipList, &res);

  char *t10[3] = {"tm1", "tm3", "tm5"};
  setValue(&res, 3, t10);
  testQueryStr(schema, numOfCols, "0=h", pSkipList, &res);
}

static void IllegalExprTest(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList) {
  testQueryStr(schema, numOfCols, "h=", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "h<", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "a=1 and ", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "and or", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "and a = 1 or", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "(())", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "(", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "(a", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "(a)", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "())", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "a===1", pSkipList, NULL);
  testQueryStr(schema, numOfCols, "a=1 and ", pSkipList, NULL);
}

static void Left2RightTest_binary(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList) {
  char  str[256] = {0};
  char *sql = NULL;

  char *t0[1] = {"tm0"};
  char *t1[1] = {"tm1"};

  ResultObj res = {1, {"tm0"}};
  strcpy(str, "a='abc'");
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  char *tt[1] = {"tm6"};
  setValue(&res, 1, tt);
  testQueryStr(schema, numOfCols, "c>=6", pSkipList, &res);

  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, "b<=10.6", pSkipList, &res);

  strcpy(str, "a<>'pqr'");
  char *t2[6] = {"tm0", "tm1", "tm2", "tm3", "tm4", "tm6"};
  setValue(&res, 6, t2);
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  strcpy(str, "a='abc'");
  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  char *t3[6] = {"tm1", "tm2", "tm3", "tm4", "tm5", "tm6"};
  setValue(&res, 6, t3);
  testQueryStr(schema, numOfCols, "d>1050", pSkipList, &res);

  char *t4[3] = {"tm4", "tm5", "tm6"};
  setValue(&res, 3, t4);
  testQueryStr(schema, numOfCols, "g>4.5980765", pSkipList, &res);

  char *t5[4] = {"tm0", "tm2", "tm4", "tm6"};
  setValue(&res, 4, t5);
  testQueryStr(schema, numOfCols, "h=true", pSkipList, &res);

  char *t6[3] = {"tm1", "tm3", "tm5"};
  setValue(&res, 3, t6);
  testQueryStr(schema, numOfCols, "h=0", pSkipList, &res);

  sql = "(((b<40)))\0";
  char *t7[3] = {"tm0", "tm1", "tm2"};
  setValue(&res, 3, t7);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);

  sql = "((c=1) or (c=10)) or ((b=12))\0";
  setValue(&res, 1, t1);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);

  sql = "((((((c>0 and c<2))) or c=6) or c=3) or (b=50.5)) and h=false\0";
  char *t8[2] = {"tm1", "tm3"};
  setValue(&res, 2, t8);
  testQueryStr(schema, numOfCols, sql, pSkipList, &res);
}

static void Right2LeftTest_binary(SSchema *schema, int32_t numOfCols, SSkipList *pSkipList) {
  char  str[256] = {0};
  char *sql = NULL;

  char *t0[1] = {"tm0"};
  char *t1[1] = {"tm1"};

  ResultObj res = {1, {"tm0"}};
  strcpy(str, "'abc'=a");
  testQueryStr(schema, numOfCols, str, pSkipList, &res);

  char *tt[1] = {"tm6"};
  setValue(&res, 1, tt);
  testQueryStr(schema, numOfCols, "6<=c", pSkipList, &res);

  setValue(&res, 1, t0);
  testQueryStr(schema, numOfCols, "10.6>=b", pSkipList, &res);

  strcpy(str, "'pqr'<>a");
  char *t2[6] = {"tm0", "tm1", "tm2", "tm3", "tm4", "tm6"};
  setValue(&res, 6, t2);
  testQueryStr(schema, numOfCols, str, pSkipList, &res);
}

namespace {
// two level expression tree
tExprNode *createExpr1() {
  auto *pLeft = (tExprNode*) calloc(1, sizeof(tExprNode));
  pLeft->nodeType = TSQL_NODE_COL;
  pLeft->pSchema = (SSchema*) calloc(1, sizeof(SSchema));
  
  strcpy(pLeft->pSchema->name, "col_a");
  pLeft->pSchema->type = TSDB_DATA_TYPE_INT;
  pLeft->pSchema->bytes = sizeof(int32_t);
  pLeft->pSchema->colId = 1;
  
  auto *pRight = (tExprNode*) calloc(1, sizeof(tExprNode));
  pRight->nodeType = TSQL_NODE_VALUE;
  pRight->pVal = (tVariant*) calloc(1, sizeof(tVariant));
  
  pRight->pVal->nType = TSDB_DATA_TYPE_INT;
  pRight->pVal->i64 = 12;
  
  auto *pRoot = (tExprNode*) calloc(1, sizeof(tExprNode));
  pRoot->nodeType = TSQL_NODE_EXPR;
  
  pRoot->_node.optr = TSDB_RELATION_EQUAL;
  pRoot->_node.pLeft = pLeft;
  pRoot->_node.pRight = pRight;
  pRoot->_node.hasPK = true;
  
  return pRoot;
}

// thress level expression tree
tExprNode* createExpr2() {
  auto *pLeft2 = (tExprNode*) calloc(1, sizeof(tExprNode));
  pLeft2->nodeType = TSQL_NODE_COL;
  pLeft2->pSchema = (SSchema*) calloc(1, sizeof(SSchema));
  
  strcpy(pLeft2->pSchema->name, "col_a");
  pLeft2->pSchema->type = TSDB_DATA_TYPE_BINARY;
  pLeft2->pSchema->bytes = 20;
  pLeft2->pSchema->colId = 1;
  
  auto *pRight2 = (tExprNode*) calloc(1, sizeof(tExprNode));
  pRight2->nodeType = TSQL_NODE_VALUE;
  pRight2->pVal = (tVariant*) calloc(1, sizeof(tVariant));
  
  pRight2->pVal->nType = TSDB_DATA_TYPE_BINARY;
  const char* v = "hello world!";
  pRight2->pVal->pz = strdup(v);
  pRight2->pVal->nLen = strlen(v);
  
  auto *p1 = (tExprNode*) calloc(1, sizeof(tExprNode));
  p1->nodeType = TSQL_NODE_EXPR;
  
  p1->_node.optr = TSDB_RELATION_LIKE;
  p1->_node.pLeft = pLeft2;
  p1->_node.pRight = pRight2;
  p1->_node.hasPK = false;
  
  auto *pLeft1 = (tExprNode*) calloc(1, sizeof(tExprNode));
  pLeft1->nodeType = TSQL_NODE_COL;
  pLeft1->pSchema = (SSchema*) calloc(1, sizeof(SSchema));
  
  strcpy(pLeft1->pSchema->name, "col_b");
  pLeft1->pSchema->type = TSDB_DATA_TYPE_DOUBLE;
  pLeft1->pSchema->bytes = 8;
  pLeft1->pSchema->colId = 99;
  
  auto *pRight1 = (tExprNode*) calloc(1, sizeof(tExprNode));
  pRight1->nodeType = TSQL_NODE_VALUE;
  pRight1->pVal = (tVariant*) calloc(1, sizeof(tVariant));
  
  pRight1->pVal->nType = TSDB_DATA_TYPE_DOUBLE;
  pRight1->pVal->dKey = 91.99;
  
  auto *p2 = (tExprNode*) calloc(1, sizeof(tExprNode));
  p2->nodeType = TSQL_NODE_EXPR;
  
  p2->_node.optr = TSDB_RELATION_GREATER_EQUAL;
  p2->_node.pLeft = pLeft1;
  p2->_node.pRight = pRight1;
  p2->_node.hasPK = false;
  
  auto *pRoot = (tExprNode*) calloc(1, sizeof(tExprNode));
  pRoot->nodeType = TSQL_NODE_EXPR;
  
  pRoot->_node.optr = TSDB_RELATION_OR;
  pRoot->_node.pLeft = p1;
  pRoot->_node.pRight = p2;
  pRoot->_node.hasPK = true;
  return pRoot;
}

void exprSerializeTest1() {
  tExprNode* p1 = createExpr1();
  SBufferWriter bw = tbufInitWriter(NULL, false);
  exprTreeToBinary(&bw, p1);
  
  size_t size = tbufTell(&bw);
  ASSERT_TRUE(size > 0);
  char* b = tbufGetData(&bw, false);
  
  tExprNode* p2 = exprTreeFromBinary(b, size);
  ASSERT_EQ(p1->nodeType, p2->nodeType);
  
  ASSERT_EQ(p2->_node.optr, p1->_node.optr);
  ASSERT_EQ(p2->_node.pLeft->nodeType, p1->_node.pLeft->nodeType);
  ASSERT_EQ(p2->_node.pRight->nodeType, p1->_node.pRight->nodeType);
  
  SSchema* s1 = p1->_node.pLeft->pSchema;
  SSchema* s2 = p2->_node.pLeft->pSchema;
  
  ASSERT_EQ(s2->colId, s1->colId);
  ASSERT_EQ(s2->type, s1->type);
  ASSERT_EQ(s2->bytes, s1->bytes);
  ASSERT_STRCASEEQ(s2->name, s1->name);
  
  tVariant* v1 = p1->_node.pRight->pVal;
  tVariant* v2 = p2->_node.pRight->pVal;
  
  ASSERT_EQ(v1->nType, v2->nType);
  ASSERT_EQ(v1->i64, v2->i64);
  ASSERT_EQ(p1->_node.hasPK, p2->_node.hasPK);
  
  tExprTreeDestroy(&p1, nullptr);
  tExprTreeDestroy(&p2, nullptr);
  
  // tbufClose(&bw);
}

void exprSerializeTest2() {
  tExprNode* p1 = createExpr2();
  SBufferWriter bw = tbufInitWriter(NULL, false);
  exprTreeToBinary(&bw, p1);
  
  size_t size = tbufTell(&bw);
  ASSERT_TRUE(size > 0);
  char* b = tbufGetData(&bw, false);
  
  tExprNode* p2 = exprTreeFromBinary(b, size);
  ASSERT_EQ(p1->nodeType, p2->nodeType);
  
  ASSERT_EQ(p2->_node.optr, p1->_node.optr);
  ASSERT_EQ(p2->_node.pLeft->nodeType, p1->_node.pLeft->nodeType);
  ASSERT_EQ(p2->_node.pRight->nodeType, p1->_node.pRight->nodeType);
  
  tExprNode* c1Left = p1->_node.pLeft;
  tExprNode* c2Left = p2->_node.pLeft;
  
  ASSERT_EQ(c1Left->nodeType, c2Left->nodeType);
  
  ASSERT_EQ(c2Left->nodeType, TSQL_NODE_EXPR);
  ASSERT_EQ(c2Left->_node.optr, TSDB_RELATION_LIKE);
  
  ASSERT_STRCASEEQ(c2Left->_node.pLeft->pSchema->name, "col_a");
  ASSERT_EQ(c2Left->_node.pRight->nodeType, TSQL_NODE_VALUE);
  
  ASSERT_STRCASEEQ(c2Left->_node.pRight->pVal->pz, "hello world!");
  
  tExprNode* c1Right = p1->_node.pRight;
  tExprNode* c2Right = p2->_node.pRight;
  
  ASSERT_EQ(c1Right->nodeType, c2Right->nodeType);
  ASSERT_EQ(c2Right->nodeType, TSQL_NODE_EXPR);
  ASSERT_EQ(c2Right->_node.optr, TSDB_RELATION_GREATER_EQUAL);
  ASSERT_EQ(c2Right->_node.pRight->pVal->dKey, 91.99);
  
  ASSERT_EQ(p2->_node.hasPK, true);
  
  tExprTreeDestroy(&p1, nullptr);
  tExprTreeDestroy(&p2, nullptr);

  // tbufClose(&bw);
}
}  // namespace
TEST(testCase, astTest) {
//  exprSerializeTest2();
}
#endif