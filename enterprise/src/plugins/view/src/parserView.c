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
  tNameExtractFullName(pName, fullName);
  return getMetaDataFromHash(fullName, strlen(fullName), pCxt->pMetaCache->pViews, (void**)ppViewMeta);
}

int32_t getViewQuerySql(STranslateContext* pCxt, SName* pName, char** querySql) {
  SViewMeta* pViewMeta = NULL;
  int32_t     code = getViewMetaFromMetaCache(pCxt, pName, &pViewMeta);
  if (TSDB_CODE_SUCCESS == code) {
    *querySql = strdup(pViewMeta->querySql);
    if (NULL == *querySql) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return code;
}
 
int32_t translateView(STranslateContext* pCxt, SNode** pTable, SName* pName) {
   SRealTableNode* pRealTable = (SRealTableNode*)*pTable;
   char* querySql = NULL;
   SParseSqlRes res = {.resType = PARSE_SQL_RES_QUERY};
   int32_t code = getViewQuerySql(pCxt, pName, &querySql);
   if (TSDB_CODE_SUCCESS != code) {
     code = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_GET_META_ERROR, tstrerror(code));
     goto _exit;
   }
   code = (*pCxt->pParseCxt->parseSqlFp)(pCxt->pParseCxt->parseSqlParam, querySql, true, &res);
   if (TSDB_CODE_SUCCESS != code) {
     goto _exit;
   }
   code = putMetaDataToCache(res.queryRes.pCatalogReq, &res.queryRes.meta, pCxt->pMetaCache);
   if (TSDB_CODE_SUCCESS != code) {
     goto _exit;
   }
   STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
   if (NULL == tempTable) {
     code = TSDB_CODE_OUT_OF_MEMORY;
     goto _exit;
   }
   tstrncpy(tempTable->table.tableAlias, pRealTable->table.tableAlias, sizeof(tempTable->table.tableAlias));
   if (QUERY_NODE_SELECT_STMT == nodeType(res.queryRes.pQuery)) {
     strcpy(((SSelectStmt*)res.queryRes.pQuery)->stmtName, tempTable->table.tableAlias);
     ((SSelectStmt*)res.queryRes.pQuery)->isSubquery = true;
   } else if (QUERY_NODE_SET_OPERATOR == nodeType(res.queryRes.pQuery)) {
     strcpy(((SSetOperator*)res.queryRes.pQuery)->stmtName, tempTable->table.tableAlias);
   }
   TSWAP(tempTable->pSubquery, res.queryRes.pQuery);
   nodesDestroyNode(*pTable);
   *pTable = (SNode*)tempTable;
 
   code = translateTable(pCxt, pTable);
 
 _exit:
 
   taosMemoryFree(querySql);
   nodesDestroyNode(res.queryRes.pQuery);
   
   return code;
}



