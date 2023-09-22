#include "parInt.h"
#include "parTranslater.h"

#include "catalog.h"
#include "cmdnodes.h"
#include "filter.h"
#include "functionMgt.h"
#include "parUtil.h"
#include "scalar.h"
#include "systable.h"
#include "tglobal.h"
#include "ttime.h"

static void biMakeAliasNameInMD5(char* pExprStr, int32_t len, char* pAlias) {
  T_MD5_CTX ctx;
  tMD5Init(&ctx);
  tMD5Update(&ctx, pExprStr, len);
  tMD5Final(&ctx);
  char* p = pAlias;
  for (uint8_t i = 0; i < tListLen(ctx.digest); ++i) {
    sprintf(p, "%02x", ctx.digest[i]);
    p += 2;
  }
}

static SNode* biMakeTbnameProjectAstNode(char* funcName, char* tableAlias) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  val->literal = strdup(tableAlias);
  val->node.resType.type = TSDB_DATA_TYPE_BINARY;
  val->node.resType.bytes = strlen(val->literal);
  val->isDuration = false;
  val->translate = false;

  SNodeList* paramList = nodesMakeList();
  nodesListAppend(paramList, (SNode*)val);

  SFunctionNode* tbNameFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  strncpy(tbNameFunc->functionName, "tbname", strlen("tbname"));
  nodesListMakeAppend(&tbNameFunc->pParameterList, (SNode*)val);

  snprintf(tbNameFunc->node.userAlias, sizeof(tbNameFunc->node.userAlias), "%s.tbname", tableAlias);
  biMakeAliasNameInMD5(tbNameFunc->node.userAlias, strlen(tbNameFunc->node.userAlias), tbNameFunc->node.aliasName);
  if (funcName == NULL) {
    return (SNode*)tbNameFunc;
  } else {
    SFunctionNode* multiResFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
    strncpy(multiResFunc->functionName, funcName, strlen(funcName));
    nodesListMakeAppend(&multiResFunc->pParameterList, (SNode*)tbNameFunc);

    snprintf(multiResFunc->node.userAlias, sizeof(multiResFunc->node.userAlias), "%s(%s.tbname)", funcName, tableAlias);
    biMakeAliasNameInMD5(multiResFunc->node.userAlias, strlen(multiResFunc->node.userAlias), multiResFunc->node.aliasName);
    return (SNode*)multiResFunc;
  }
}

static int32_t biRewriteSelectFuncParamStar(STranslateContext* pCxt, SSelectStmt* pSelect, SNode* pNode, SListCell* pSelectListCell) {
  SNodeList* pTbnameNodeList = nodesMakeList();

  SFunctionNode* pFunc = (SFunctionNode*)pNode;
  if (strcasecmp(pFunc->functionName, "last") == 0 || 
      strcasecmp(pFunc->functionName, "last_row") == 0 ||
      strcasecmp(pFunc->functionName, "first") == 0) {
    SNodeList* pParams = pFunc->pParameterList;
    SNode*     pPara = NULL;
    FOREACH(pPara, pParams) {
      if (nodesIsStar(pPara)) {
        SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
        size_t  n = taosArrayGetSize(pTables);
        for (int32_t i = 0; i < n; ++i) {
          STableNode* pTable = taosArrayGetP(pTables, i);
          SNode*      pTbnameNode = biMakeTbnameProjectAstNode(pFunc->functionName, pTable->tableAlias);
          nodesListAppend(pTbnameNodeList, pTbnameNode);
        }
        nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
      } else if (nodesIsTableStar(pPara)) {
        char*  pTableAlias = ((SColumnNode*)pPara)->tableAlias;
        SNode* pTbnameNode = biMakeTbnameProjectAstNode(pFunc->functionName, pTableAlias);
        nodesListAppend(pTbnameNodeList, pTbnameNode);
        nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

// after translate from
// before translate select list
int32_t biRewriteSelectStar(STranslateContext* pCxt, SSelectStmt* pSelect) {
  SNode* pNode = NULL;
  SNodeList* pTbnameNodeList = nodesMakeList();
  WHERE_EACH(pNode, pSelect->pProjectionList) {
    if (nodesIsStar(pNode)) {
      SArray* pTables = taosArrayGetP(pCxt->pNsLevel, pCxt->currLevel);
      size_t n = taosArrayGetSize(pTables);
      for (int32_t i = 0; i < n; ++i) {
        STableNode* pTable = taosArrayGetP(pTables, i);
        SNode* pTbnameNode = biMakeTbnameProjectAstNode(NULL, pTable->tableAlias);
        nodesListAppend(pTbnameNodeList, pTbnameNode);
      }
      nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
    } else if (nodesIsTableStar(pNode)) {
      char* pTableAlias = ((SColumnNode*)pNode)->tableAlias;
      SNode* pTbnameNode = biMakeTbnameProjectAstNode(NULL, pTableAlias);
      nodesListAppend(pTbnameNodeList, pTbnameNode);
      nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
    } else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
      biRewriteSelectFuncParamStar(pCxt, pSelect, pNode, cell);
    }
     WHERE_NEXT;
  }

  return TSDB_CODE_SUCCESS;
}
