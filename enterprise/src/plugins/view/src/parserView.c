/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#include "parInt.h"
#include "catalog.h"
#include "cmdnodes.h"

int32_t getViewMetaFromMetaCache(STranslateContext* pCxt, SName* pName, SViewMeta** ppViewMeta) {
  char fullName[TSDB_TABLE_FNAME_LEN];
  (void)tNameExtractFullName(pName, fullName);
  return getMetaDataFromHash(fullName, strlen(fullName), pCxt->pMetaCache->pViews, (void**)ppViewMeta);
}

int32_t getViewQuerySqlUser(STranslateContext* pCxt, SName* pName, char** querySql, char** user) {
  SViewMeta* pViewMeta = NULL;
  int32_t     code = getViewMetaFromMetaCache(pCxt, pName, &pViewMeta);
  if (TSDB_CODE_SUCCESS == code) {
    *querySql = tstrdup(pViewMeta->querySql);
    if (NULL == *querySql) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    *user = tstrdup(pViewMeta->user);
    if (NULL == *user) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}
 
int32_t translateView(STranslateContext* pCxt, SNode** pTable, SName* pName) {
   SRealTableNode* pRealTable = (SRealTableNode*)*pTable;
   SParseContext* pParseCxt = pCxt->pParseCxt;
   char* querySql = NULL;
   char* user = NULL;
   SNode* pQuery = NULL;
   SParseSqlRes res = {.resType = PARSE_SQL_RES_QUERY};

   pParseCxt->isView = true;
   int32_t code = getViewQuerySqlUser(pCxt, pName, &querySql, &user);
   if (TSDB_CODE_SUCCESS != code) {
     (void)generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
     goto _exit;
   }
   parserDebug("translate view %d.%s.%s, querySQL:%s, effectiveUser:%s", pName->acctId, pName->dbname, pName->tname, querySql, user);
   code = (*pCxt->pParseCxt->parseSqlFp)(pCxt->pParseCxt->parseSqlParam, pName->dbname, querySql, true, user, &res);

   TSWAP(pQuery, res.queryRes.pQuery);
   if (NULL == pParseCxt->pSubMetaList) {
     pParseCxt->pSubMetaList = taosArrayInit(4, sizeof(res.queryRes));
     if (NULL == pParseCxt->pSubMetaList) {
       code = TSDB_CODE_OUT_OF_MEMORY;
       tfreeSParseQueryRes(&res.queryRes);
       goto _exit;
     }
   }
   if (NULL == taosArrayPush(pParseCxt->pSubMetaList, &res.queryRes)) {
     tfreeSParseQueryRes(&res.queryRes);
     goto _exit;
   }

   if (TSDB_CODE_SUCCESS != code) {
     goto _exit;
   }
   code = putMetaDataToCache(res.queryRes.pCatalogReq, &res.queryRes.meta, pCxt->pMetaCache);
   if (TSDB_CODE_SUCCESS != code) {
     goto _exit;
   }

   STempTableNode* tempTable = NULL;
   code = nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&tempTable);
   if (NULL == tempTable) {
     goto _exit;
   }
   tstrncpy(tempTable->table.tableAlias, pRealTable->table.tableAlias, sizeof(tempTable->table.tableAlias));
   if (QUERY_NODE_SELECT_STMT == nodeType(pQuery)) {
     TAOS_STRCPY(((SSelectStmt*)pQuery)->stmtName, tempTable->table.tableAlias);
     ((SSelectStmt*)pQuery)->isSubquery = true;
   } else if (QUERY_NODE_SET_OPERATOR == nodeType(pQuery)) {
     TAOS_STRCPY(((SSetOperator*)pQuery)->stmtName, tempTable->table.tableAlias);
   }
   TSWAP(tempTable->pSubquery, pQuery);
   nodesDestroyNode(*pTable);
   *pTable = (SNode*)tempTable;
 
   code = translateTable(pCxt, pTable, NULL);
 
 _exit:
 
   taosMemoryFree(querySql);
   taosMemoryFree(user);
   nodesDestroyNode(pQuery);
   
   return code;
}



