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
  SValueNode* valNode = NULL;
  if (tableAlias != NULL) {
    SValueNode* n = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
    n->literal = strdup(tableAlias);
    n->node.resType.type = TSDB_DATA_TYPE_BINARY;
    n->node.resType.bytes = strlen(n->literal);
    n->isDuration = false;
    n->translate = false;
    valNode = n;
  }

  SFunctionNode* tbNameFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  strncpy(tbNameFunc->functionName, "tbname", strlen("tbname"));
  if (valNode != NULL) {
    nodesListMakeAppend(&tbNameFunc->pParameterList, (SNode*)valNode);
  }
  snprintf(tbNameFunc->node.userAlias, sizeof(tbNameFunc->node.userAlias), 
                (tableAlias)? "%s.tbname" : "%stbname", 
                (tableAlias)? tableAlias : "");
  strcpy(tbNameFunc->node.aliasName, tbNameFunc->functionName);

  if (funcName == NULL) {
    return (SNode*)tbNameFunc;
  } else {
    SFunctionNode* multiResFunc = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
    strncpy(multiResFunc->functionName, funcName, strlen(funcName));
    nodesListMakeAppend(&multiResFunc->pParameterList, (SNode*)tbNameFunc);

    if (tsKeepColumnName) {
      snprintf(multiResFunc->node.userAlias, sizeof(tbNameFunc->node.userAlias), 
                (tableAlias)? "%s.tbname" : "%stbname", 
                (tableAlias)? tableAlias : "");
      strcpy(multiResFunc->node.aliasName, tbNameFunc->functionName);
    } else {
      snprintf(multiResFunc->node.userAlias, sizeof(multiResFunc->node.userAlias), 
              tableAlias? "%s(%s.tbname)" : "%s(%stbname)", funcName, 
              tableAlias? tableAlias: "");
      biMakeAliasNameInMD5(multiResFunc->node.userAlias, strlen(multiResFunc->node.userAlias), multiResFunc->node.aliasName);
    }

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
          if (nodeType(pTable) == QUERY_NODE_REAL_TABLE && ((SRealTableNode*)pTable)->pMeta != NULL &&
              ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
            SNode* pTbnameNode = biMakeTbnameProjectAstNode(pFunc->functionName, NULL);
            nodesListAppend(pTbnameNodeList, pTbnameNode);
          }
        }
        if (LIST_LENGTH(pTbnameNodeList) > 0) {
          nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
        }
      } else if (nodesIsTableStar(pPara)) {
        char* pTableAlias = ((SColumnNode*)pPara)->tableAlias;
        STableNode* pTable = NULL;
        int32_t     code = findTable(pCxt, pTableAlias, &pTable);
        if (TSDB_CODE_SUCCESS == code && nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
            ((SRealTableNode*)pTable)->pMeta != NULL &&
            ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
          SNode* pTbnameNode = biMakeTbnameProjectAstNode(pFunc->functionName, pTableAlias);
          nodesListAppend(pTbnameNodeList, pTbnameNode);
        }
        if (LIST_LENGTH(pTbnameNodeList) > 0) {
          nodesListInsertListAfterPos(pSelect->pProjectionList, pSelectListCell, pTbnameNodeList);
        }
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
        if (nodeType(pTable) == QUERY_NODE_REAL_TABLE && 
            ((SRealTableNode*)pTable)->pMeta != NULL && 
            ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
          SNode* pTbnameNode = biMakeTbnameProjectAstNode(NULL, NULL);
          nodesListAppend(pTbnameNodeList, pTbnameNode);
        }
      }
      if (LIST_LENGTH(pTbnameNodeList) > 0) {
        nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
      }
    } else if (nodesIsTableStar(pNode)) {
      char* pTableAlias = ((SColumnNode*)pNode)->tableAlias;
      STableNode* pTable = NULL;
      int32_t     code = findTable(pCxt, pTableAlias, &pTable);
      if (TSDB_CODE_SUCCESS == code && 
          nodeType(pTable) == QUERY_NODE_REAL_TABLE &&
          ((SRealTableNode*)pTable)->pMeta != NULL && 
          ((SRealTableNode*)pTable)->pMeta->tableType == TSDB_SUPER_TABLE) {
        SNode* pTbnameNode = biMakeTbnameProjectAstNode(NULL, pTableAlias);
        nodesListAppend(pTbnameNodeList, pTbnameNode);
      }
      if (LIST_LENGTH(pTbnameNodeList) > 0) {
        nodesListInsertListAfterPos(pSelect->pProjectionList, cell, pTbnameNodeList);
      }
    } else if (nodeType(pNode) == QUERY_NODE_FUNCTION) {
      biRewriteSelectFuncParamStar(pCxt, pSelect, pNode, cell);
    }
     WHERE_NEXT;
  }

  return TSDB_CODE_SUCCESS;
}

EDealRes biRewriteToTbnameFuncAndTranslate(STranslateContext* pCxt, SColumnNode** ppCol) {
  SFunctionNode* tbnameFuncNode = NULL;
  tbnameFuncNode = (SFunctionNode*)biMakeTbnameProjectAstNode(NULL, ((*ppCol)->tableAlias[0]!='\0') ? (*ppCol)->tableAlias : NULL);
  tbnameFuncNode->node.resType = (*ppCol)->node.resType;
  strcpy(tbnameFuncNode->node.aliasName, (*ppCol)->node.aliasName);
  strcpy(tbnameFuncNode->node.userAlias, (*ppCol)->node.userAlias);

  nodesDestroyNode(*(SNode**)ppCol);
  *(SNode**)ppCol = (SNode*)tbnameFuncNode;

  EDealRes res = translateFunction(pCxt, &tbnameFuncNode);
  return res;
}