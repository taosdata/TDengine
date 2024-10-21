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

#ifndef _TD_INSERT_NODES_H_
#define _TD_INSERT_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"

typedef struct value_s              value_t;
typedef struct row_s                row_t;
typedef struct using_clause_s       using_clause_t;
typedef struct multiple_targets_s   multiple_targets_t;

typedef struct SInsertMultiStmt {
  ENodeType                    type;  // QUERY_NODE_INSERT_MULTI_STMT
  multiple_targets_t          *multiple_targets;
} SInsertMultiStmt;

typedef struct SInsertQuestionStmt {
  ENodeType                    type;  // QUERY_NODE_INSERT_QUESTION_STMT
  value_t                     *table_value;
  using_clause_t              *using_clause;
  SNodeList                   *fields_clause;
  row_t                       *rows;
} SInsertQuestionStmt;

void InsertMultiStmtRelease(SInsertMultiStmt *pStmt);
void InsertQuestionStmtRelease(SInsertQuestionStmt *pStmt);

#ifdef __cplusplus
}
#endif

#endif /*_TD_INSERT_NODES_H_*/

