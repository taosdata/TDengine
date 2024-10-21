//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y

%name taosParse
%token_prefix TK_
%token_type { SToken }
%default_type { SNode* }
%default_destructor { nodesDestroyNode($$); }

%extra_argument { SAstCreateContext* pCxt }

%include {
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#define ALLOW_FORBID_FUNC

#include "nodes.h"
#include "parAst.h"
#include "parInt.h"
#include "parse_result.h"

#define YYSTACKDEPTH 0

#define NULL_TOKEN       ((SToken*)0)
#define NULL_NODE        ((SNode*)0)
#define NULL_LIST        ((SNodeList*)0)

#define GEN_ERR(...) do {                                                      \
  /* TODO: freemine, this is error prone         */                            \
  /*       if look deep into the implementation  */                            \
  pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, ##__VA_ARGS__);          \
} while (0)

#define TOKEN_TO_INT8(target, token) do {                                      \
  target = 0;                                                                  \
  if (pCxt->errCode == 0) {                                                    \
    target = taosStr2Int8((token).z, NULL, 10);                                \
  }                                                                            \
} while (0)

#define TOKEN_TO_INT32(target, token) do {                                     \
  target = 0;                                                                  \
  if (pCxt->errCode == 0) {                                                    \
    target = taosStr2Int32((token).z, NULL, 10);                               \
  }                                                                            \
} while (0)

#define ROOT_NODE() pCxt->parse_result->pRootNode

static SNodeList* createNodeListSafe(SAstCreateContext* pCxt, SNode* pNode) {
  SNodeList *list = createNodeList(pCxt, pNode);
  if (!list) nodesDestroyNode(pNode);
  return list;
}

static int addNodeToListSafe(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  pCxt->errCode = nodesListAppend(pList, pNode);
  if (pCxt->errCode) {
    nodesDestroyNode(pNode);
    nodesDestroyList(pList);
    return -1;
  }
  return 0;
}

static SNode* createRawExprNodeSafe(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  SNode *p = createRawExprNode(pCxt, pToken, pNode);
  if (!p) nodesDestroyNode(pNode);
  return p;
}

static SNode* createRawExprNodeExtSafe(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  SNode *p = createRawExprNodeExt(pCxt, pStart, pEnd, pNode);
  if (!p) nodesDestroyNode(pNode);
  return p;
}

#define LIST_MK(target, node) do {                                             \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    target = createNodeListSafe(pCxt, node);                                   \
    node = NULL;                                                               \
    if (target) break;                                                         \
  }                                                                            \
  nodesDestroyNode(node); node = NULL;                                         \
} while (0)

#define LIST_ADD_NODE(target, node) do {                                       \
  if (pCxt->errCode == 0) {                                                    \
    addNodeToList(pCxt, target, node);                                         \
    if (!pCxt->errCode) { node = NULL; break; }                                \
  }                                                                            \
  nodesDestroyList(target);                                                    \
  nodesDestroyNode(node);                                                      \
  target = NULL;                                                               \
} while (0)

#define COL_LIST_ADD_NODE(target, node) do {                                   \
  if (pCxt->errCode == 0) {                                                    \
    SColumnNode *me = (SColumnNode*)node;                                      \
    int found = 0;                                                             \
    SNode *p = NULL;                                                           \
    FOREACH(p, target) {                                                       \
      SColumnNode *col = (SColumnNode*)p;                                      \
      if (0==strcmp(col->colName, me->colName)) {                              \
        found = 1;                                                             \
        break;                                                                 \
      }                                                                        \
    }                                                                          \
    if (found) {                                                               \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "duplicated col/tag name:[%s]", me->colName);                        \
    }                                                                          \
    if (pCxt->errCode == 0) {                                                  \
      addNodeToList(pCxt, target, node);                                       \
      if (!pCxt->errCode) { node = NULL; break; }                              \
    }                                                                          \
  }                                                                            \
  nodesDestroyList(target);                                                    \
  nodesDestroyNode(node);                                                      \
  target = NULL;                                                               \
} while (0)

#define IP_RANGE_LIST_MK(target, token) do {                                   \
  target = NULL;                                                               \
  SNode *node = NULL;                                                          \
  if (pCxt->errCode == 0) {                                                    \
    node = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &token);               \
    if (node) {                                                                \
      LIST_MK(target, node);                                                   \
      if (target) break;                                                       \
    }                                                                          \
  }                                                                            \
} while (0)

#define IP_RANGE_LIST_APPEND(target, token) do {                               \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &token);          \
  LIST_ADD_NODE(target, node);                                                 \
} while (0)

#define CREATE_USER(uid, pass, sinfo, is_createdb_opt, is_import_opt, white_list_opt) do {        \
  if (pCxt->errCode == 0) {                                                                       \
    SNode *node = createCreateUserStmt(pCxt, &uid, &pass, sinfo, is_createdb_opt, is_import_opt); \
    if (node) {                                                                                   \
      ROOT_NODE() = addCreateUserStmtWhiteList(pCxt, node, white_list_opt);                       \
      break;                                                                                      \
    }                                                                                             \
  }                                                                                               \
  nodesDestroyList(white_list_opt);                                                               \
} while (0)

#define LITERAL_QUESTION_MK(target, token) do {                                \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  SNode *node = createPlaceholderValueNode(pCxt, &token);                      \
  if (!node) break;                                                            \
  target = createRawExprNodeSafe(pCxt, &token, node);                          \
} while (0)

#define LITERAL_MK(target, token, tsdb_type) do {                              \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  SNode *node = createValueNode(pCxt, tsdb_type, &token);                      \
  if (!node) break;                                                            \
  target = createRawExprNodeSafe(pCxt, &token, node);                          \
} while (0)

#define LITERAL_MK_EXT(target, start, end, tsdb_type) do {                     \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  SNode *node = createValueNode(pCxt, tsdb_type, &end);                        \
  if (!node) break;                                                            \
  target = createRawExprNodeExtSafe(pCxt, &start, &end, node);                 \
} while (0)

#define DURA_LITERAL_MK(target, token) do {                                    \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  SNode *node = createDurationValueNode(pCxt, &token);                         \
  if (!node) break;                                                            \
  target = createRawExprNodeSafe(pCxt, &token, node);                          \
} while (0)

#define SIGNED_MK(target, token, tsdb_type) do {                               \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  target = createValueNode(pCxt, tsdb_type, &token);                           \
} while (0)

#define ALTER_USER(user_name, token, tsdb_type) do {                           \
  ROOT_NODE() = createAlterUserStmt(pCxt, &user_name, tsdb_type, &token);      \
} while (0)

#define ALTER_USER_WLIST(user_name, wlist, tsdb_type) do {                     \
  ROOT_NODE() = createAlterUserStmt(pCxt, &user_name, tsdb_type, wlist);       \
} while (0)

#define DROP_USER(user_name) do {                                              \
  ROOT_NODE() = createDropUserStmt(pCxt, &user_name);                          \
} while (0)

#define GRANT_PRIV(priv, priv_level, user_name, tag_cond) do {                       \
  ROOT_NODE() = createGrantStmt(pCxt, priv, &priv_level, &user_name, tag_cond);      \
  if (!ROOT_NODE()) nodesDestroyNode(tag_cond);                                      \
} while (0)

#define REVOKE_PRIV(priv, priv_level, user_name, tag_cond) do {                       \
  ROOT_NODE() = createRevokeStmt(pCxt, priv, &priv_level, &user_name, tag_cond);      \
  if (!ROOT_NODE()) nodesDestroyNode(tag_cond);                                       \
} while (0)

#define CREATE_ENCRYPT_KEY(token) do {                                         \
  ROOT_NODE() = createEncryptKeyStmt(pCxt, &token);                            \
} while (0)

#define CREATE_DNODE(fqdn, port) do {                                          \
  ROOT_NODE() = createCreateDnodeStmt(pCxt, &fqdn, &port);                     \
} while (0)

#define DROP_DNODE(dnode, force, unsafe) do {                                  \
  ROOT_NODE() = createDropDnodeStmt(pCxt, &dnode, force, unsafe);              \
} while (0)

#define ALTER_DNODE(dnode, config, val) do {                                   \
  ROOT_NODE() = createAlterDnodeStmt(pCxt, &dnode, &config, &val);             \
} while (0)

#define RESTORE_DNODE(dnode) do {                                                                \
  ROOT_NODE() = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_DNODE_STMT, &dnode);     \
} while (0)

#define ALTER_CLUSTER(config, token) do {                                      \
  ROOT_NODE() = createAlterClusterStmt(pCxt, &config, &token);                 \
} while (0)

#define ALTER_LOCAL(config, token) do {                                        \
  ROOT_NODE() = createAlterLocalStmt(pCxt, &config, &token);                   \
} while (0)

#define CREATE_QNODE(token) do {                                                                  \
  ROOT_NODE() = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_QNODE_STMT, &token);        \
} while (0)

#define DROP_QNODE(token) do {                                                                    \
  ROOT_NODE() = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_QNODE_STMT, &token);            \
} while (0)

#define RESTORE_QNODE(token) do {                                                                 \
  ROOT_NODE() = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_QNODE_STMT, &token);      \
} while (0)

#define CREATE_BNODE(token) do {                                                                  \
  ROOT_NODE() = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_BNODE_STMT, &token);        \
} while (0)

#define DROP_BNODE(token) do {                                                                    \
  ROOT_NODE() = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_BNODE_STMT, &token);            \
} while (0)

#define CREATE_SNODE(token) do {                                                                  \
  ROOT_NODE() = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_SNODE_STMT, &token);        \
} while (0)

#define DROP_SNODE(token) do {                                                                    \
  ROOT_NODE() = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_SNODE_STMT, &token);            \
} while (0)

#define CREATE_MNODE(token) do {                                                                  \
  ROOT_NODE() = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_MNODE_STMT, &token);        \
} while (0)

#define DROP_MNODE(token) do {                                                                    \
  ROOT_NODE() = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_MNODE_STMT, &token);            \
} while (0)

#define RESTORE_MNODE(token) do {                                                                 \
  ROOT_NODE() = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_MNODE_STMT, &token);      \
} while (0)

#define RESTORE_VNODE(token) do {                                                                 \
  ROOT_NODE() = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_VNODE_STMT, &token);      \
} while (0)

#define CREATE_DB(ignore_exists, dbname, options) do {                                            \
  ROOT_NODE() = createCreateDatabaseStmt(pCxt, ignore_exists, &dbname, options);                  \
  if (!ROOT_NODE()) nodesDestroyNode(options);                                                    \
} while (0)

#define DROP_DB(ignore_not_exists, dbname) do {                                                   \
  ROOT_NODE() = createDropDatabaseStmt(pCxt, ignore_not_exists, &dbname);                         \
} while (0)

#define USE_DB(token) do {                                                     \
  ROOT_NODE() = createUseDatabaseStmt(pCxt, &token);                           \
} while (0)

#define ALTER_DB(token, node) do {                                             \
  ROOT_NODE() = createAlterDatabaseStmt(pCxt, &token, node);                   \
  if (!ROOT_NODE()) nodesDestroyNode(node);                                    \
} while (0)

#define FLUSH_DB(token) do {                                                   \
  ROOT_NODE() = createFlushDatabaseStmt(pCxt, &token);                         \
} while (0)

#define TRIM_DB(token, max_speed) do {                                         \
  ROOT_NODE() = createTrimDatabaseStmt(pCxt, &token, max_speed);               \
} while (0)

#define S3MIGRATE_DB(token) do {                                               \
  ROOT_NODE() = createS3MigrateDatabaseStmt(pCxt, &token);                     \
} while (0)

#define COMPACT_DB(dbname, start, end) do {                                    \
  ROOT_NODE() = createCompactStmt(pCxt, &dbname, start, end);                  \
  if (!ROOT_NODE()) {                                                          \
    nodesDestroyNode(start);                                                   \
    nodesDestroyNode(end);                                                     \
  }                                                                            \
} while (0)

#define CREATE_DEFAULT_DB_OPT(target) do {                                     \
  target = createDefaultDatabaseOptions(pCxt);                                 \
} while (0)

#define INTEGER_LIST_MK(target, token) do {                                    \
  target = NULL;                                                               \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &token);          \
  if (!node) break;                                                            \
  LIST_MK(target, node);                                                       \
} while (0)

#define INTEGER_LIST_APPEND(target, token) do {                                \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &token);          \
  if (node) {                                                                  \
    LIST_ADD_NODE(target, node);                                               \
    break;                                                                     \
  }                                                                            \
  nodesDestroyList(target); target = NULL;                                     \
} while (0)

#define VAR_LIST_MK(target, token) do {                                        \
  target = NULL;                                                               \
  SNode *node = createDurationValueNode(pCxt, &token);                         \
  if (!node) break;                                                            \
  LIST_MK(target, node);                                                       \
} while (0)

#define VAR_LIST_APPEND(target, token) do {                                    \
  SNode *node = createDurationValueNode(pCxt, &token);                         \
  if (node) {                                                                  \
    LIST_ADD_NODE(target, node);                                               \
    break;                                                                     \
  }                                                                            \
  nodesDestroyList(target); target = NULL;                                     \
} while (0)

#define CREATE_TABLE(ignore_exists, tbl, cols, tags, opts) do {                           \
  ROOT_NODE() = createCreateTableStmt(pCxt, ignore_exists, tbl, cols, tags, opts);        \
  if (ROOT_NODE()) break;                                                                 \
  nodesDestroyNode(tbl);                                                                  \
  nodesDestroyList(cols);                                                                 \
  nodesDestroyList(tags);                                                                 \
  nodesDestroyNode(opts);                                                                 \
} while (0)

#define CREATE_MULTI_TABLE(sub_tbls) do {                                      \
  ROOT_NODE() = createCreateMultiTableStmt(pCxt, sub_tbls);                    \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyList(sub_tbls);                                                  \
} while (0)

#define CREATE_SUBTBL_FROM_FILE(ignore_exists, use_real, tags, file) do {                           \
  ROOT_NODE() = createCreateSubTableFromFileClause(pCxt, ignore_exists, use_real, tags, &file);     \
  if (ROOT_NODE()) break;                                                                           \
  nodesDestroyNode(use_real);                                                                       \
  nodesDestroyList(tags);                                                                           \
} while (0)

#define DROP_TABLE(tables) do {                                                \
  ROOT_NODE() = createDropTableStmt(pCxt, tables);                             \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyList(tables);                                                    \
} while (0)

#define DROP_SUPER_TABLE(ignore_not_exists, real) do {                         \
  ROOT_NODE() = createDropSuperTableStmt(pCxt, ignore_not_exists, real);       \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(real);                                                      \
} while (0)

#define ALTER_SUPER_TABLE(clause) do {                                         \
  ROOT_NODE() = setAlterSuperTableType(clause);                                \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(clause);                                                    \
} while (0)

#define ALTER_TABLE_CLAUSE(target, real, opts) do {                            \
  target = createAlterTableModifyOptions(pCxt, real, opts);                    \
  if (target) break;                                                           \
  nodesDestroyNode(real);                                                      \
  nodesDestroyNode(opts);                                                      \
} while (0)

#define ALTER_TABLE_ADD_COL(target, real, col, dt, opts) do {                                                 \
  target = createAlterTableAddModifyColOptions2(pCxt, real, TSDB_ALTER_TABLE_ADD_COLUMN, &col, dt, opts);     \
  if (target) break;                                                                                          \
  nodesDestroyNode(real);                                                                                     \
  nodesDestroyNode(opts);                                                                                     \
} while (0)

#define ALTER_TABLE_DROP_COL(target, real, col) do {                                   \
  target = createAlterTableDropCol(pCxt, real, TSDB_ALTER_TABLE_DROP_COLUMN, &col);    \
  if (target) break;                                                                   \
  nodesDestroyNode(real);                                                              \
} while (0)

#define ALTER_TABLE_MOD_COL_NAME(target, real, old_name, new_name) do {                                      \
  target = createAlterTableRenameCol(pCxt, real, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &old_name, &new_name); \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
} while (0)

#define ALTER_TABLE_ADD_TAG(target, real, col, dt) do {                                                      \
  target = createAlterTableAddModifyCol(pCxt, real, TSDB_ALTER_TABLE_ADD_TAG, &col, dt);                     \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
} while (0)

#define ALTER_TABLE_DROP_TAG(target, real, col) do {                                                         \
  target = createAlterTableDropCol(pCxt, real, TSDB_ALTER_TABLE_DROP_TAG, &col);                             \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
} while (0)

#define ALTER_TABLE_MOD_TAG_NAME(target, real, old_name, new_name) do {                                      \
  target = createAlterTableRenameCol(pCxt, real, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &old_name, &new_name);    \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
} while (0)

#define ALTER_TABLE_SET_TAG(target, real, tag, val) do {                                                     \
  target = createAlterTableSetTag(pCxt, real, &tag, val);                                                    \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
  nodesDestroyNode(val);                                                                                     \
} while (0)

#define CREATE_SUBTBL_CLAUSE(target, ignore_exists, real, use_tbl, tags, vals, opts) do {                    \
  target = createCreateSubTableClause(pCxt, ignore_exists, real, use_tbl, tags, vals, opts);                 \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
  nodesDestroyNode(use_tbl);                                                                                 \
  nodesDestroyList(tags);                                                                                    \
  nodesDestroyList(vals);                                                                                    \
  nodesDestroyNode(opts);                                                                                    \
} while (0)

#define DROP_TABLE_CLAUSE(target, ignore_not_exists, real) do {                                              \
  target = createDropTableClause(pCxt, ignore_not_exists, real);                                             \
  if (target) break;                                                                                         \
  nodesDestroyNode(real);                                                                                    \
} while (0)

#define CREATE_COL_DEF(target, col, dt, node) do {                                                           \
  target = createColumnDefNode(pCxt, &col, dt, node);                                                        \
  if (target) break;                                                                                         \
  nodesDestroyNode(node);                                                                                    \
} while (0)

#define CREATE_FUNC(target, func, params) do {                                                               \
  target = createFunctionNode(pCxt, &func, params);                                                          \
  if (target) break;                                                                                         \
  nodesDestroyList(params);                                                                                  \
} while (0)

#define SHOW_TABLE(opt, tbname, op) do {                                       \
  ROOT_NODE() = createShowTablesStmt(pCxt, opt, tbname, op);                   \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(tbname);                                                    \
} while (0)

#define SHOW_STABLE(db, tb, op) do {                                                           \
  ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_STABLES_STMT, db, tb, op);        \
  if (ROOT_NODE()) break;                                                                      \
  nodesDestroyNode(db);                                                                        \
  nodesDestroyNode(tb);                                                                        \
} while (0)

#define SHOW_VGROUPS(db, op) do {                                                              \
  ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, db, NULL, op);      \
  if (ROOT_NODE()) break;                                                                      \
  nodesDestroyNode(db);                                                                        \
} while (0)

#define SHOW_INDEXES(tb, db, op) do {                                                          \
  ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, db, tb, op);        \
  if (ROOT_NODE()) break;                                                                      \
  nodesDestroyNode(db);                                                                        \
  nodesDestroyNode(tb);                                                                        \
} while (0)

#define SHOW_INDEXES2(db, tb, op) do {                                                         \
  SNode *a = NULL, *b = NULL;                                                                  \
  a = createIdentifierValueNode(pCxt, &db);                                                    \
  if (a) {                                                                                     \
    b = createIdentifierValueNode(pCxt, &tb);                                                  \
    if (b) {                                                                                   \
      ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, a, b, op);      \
      if (ROOT_NODE()) break;                                                                  \
    }                                                                                          \
  }                                                                                            \
  nodesDestroyNode(a);                                                                         \
  nodesDestroyNode(a);                                                                         \
} while (0)

#define SHOW_CREATE_TABLE(real, op) do {                                       \
  ROOT_NODE() = createShowCreateTableStmt(pCxt, op, real);                     \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(real);                                                      \
} while (0)

#define SHOW_DNODE(id, like) do {                                              \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &id);             \
  if (node) {                                                                  \
    ROOT_NODE() = createShowDnodeVariablesStmt(pCxt, node, like);              \
    if (ROOT_NODE()) break;                                                    \
  }                                                                            \
  nodesDestroyNode(like);                                                      \
  nodesDestroyNode(node);                                                      \
} while (0)

#define SHOW_TABLE_DIST(real) do {                                             \
  ROOT_NODE() = createShowTableDistributedStmt(pCxt, real);                    \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(real);                                                      \
} while (0)

#define SHOW_TAGS(tb, db, op) do {                                                        \
  ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, db, tb, op);      \
  if (ROOT_NODE()) break;                                                                 \
  nodesDestroyNode(db);                                                                   \
  nodesDestroyNode(tb);                                                                   \
} while (0)

#define SHOW_TAGS2(tb, db, op) do {                                                            \
  SNode *a = NULL, *b = NULL;                                                                  \
  a = createIdentifierValueNode(pCxt, &db);                                                    \
  if (a) {                                                                                     \
    b = createIdentifierValueNode(pCxt, &tb);                                                  \
    if (b) {                                                                                   \
      ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, a, b, op);         \
      if (ROOT_NODE()) break;                                                                  \
    }                                                                                          \
  }                                                                                            \
  nodesDestroyNode(a);                                                                         \
  nodesDestroyNode(b);                                                                         \
} while (0)

#define SHOW_TABLE_TAGS(tags, tb, db) do {                                     \
  ROOT_NODE() = createShowTableTagsStmt(pCxt, tb, db, tags);                   \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(db);                                                        \
  nodesDestroyNode(tb);                                                        \
  nodesDestroyList(tags);                                                      \
} while (0)

#define SHOW_TABLE_TAGS2(tags, db, tb) do {                                    \
  SNode *a = NULL, *b = NULL;                                                  \
  a = createIdentifierValueNode(pCxt, &db);                                    \
  if (a) {                                                                     \
    b = createIdentifierValueNode(pCxt, &tb);                                  \
    if (b) {                                                                   \
      ROOT_NODE() = createShowTableTagsStmt(pCxt, b, a, tags);                 \
      if (ROOT_NODE()) break;                                                  \
    }                                                                          \
  }                                                                            \
  nodesDestroyNode(a);                                                         \
  nodesDestroyNode(b);                                                         \
  nodesDestroyList(tags);                                                      \
} while (0)

#define SHOW_VNODES_ON_DNODE(id) do {                                          \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &id);             \
  if (node) {                                                                  \
    ROOT_NODE() = createShowVnodesStmt(pCxt, node, NULL);                      \
    if (ROOT_NODE()) break;                                                    \
  }                                                                            \
  nodesDestroyNode(node);                                                      \
} while (0)

#define SHOW_STMT_ALIVE(node, type) do {                                       \
  ROOT_NODE() = createShowAliveStmt(pCxt, node, type);                         \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(node);                                                      \
} while (0)

#define SHOW_CREATE_VIEW(real) do {                                                           \
  ROOT_NODE() = createShowCreateViewStmt(pCxt, QUERY_NODE_SHOW_CREATE_VIEW_STMT, real);       \
  if (ROOT_NODE()) break;                                                                     \
  nodesDestroyNode(real);                                                                     \
} while (0)

#define SHOW_COMPACT(token) do {                                               \
  SNode *node = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &token);          \
  if (!node) break;                                                            \
  ROOT_NODE() = createShowCompactDetailsStmt(pCxt, node);                      \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(node);                                                      \
} while (0)

#define SET_TBNAME(target, func) do {                                          \
  SNode *node = NULL;                                                          \
  CREATE_FUNC(node, func, NULL_LIST);                                          \
  if (node) {                                                                  \
    target = setProjectionAlias(pCxt, node, &func);                            \
    if (target) break;                                                         \
  }                                                                            \
  nodesDestroyNode(node);                                                      \
} while (0)

#define SET_PROJECTION_ALIAS(target, name, alias) do {                         \
  SNode *node = createColumnNode(pCxt, NULL_TOKEN, &name);                     \
  if (node) {                                                                  \
    target = setProjectionAlias(pCxt, node, &alias);                           \
    if (target) break;                                                         \
  }                                                                            \
  nodesDestroyNode(node);                                                      \
} while (0)

#define CREATE_TSMA(no_exists, name, tbl, funcs, dura) do {                                     \
  SNode *node = releaseRawExprNode(pCxt, dura);                                                 \
  if (node) {                                                                                   \
    dura = NULL;                                                                                \
    ROOT_NODE() = createCreateTSMAStmt(pCxt, no_exists, &name, funcs, tbl, node);               \
    if (ROOT_NODE()) break;                                                                     \
  }                                                                                             \
  nodesDestroyNode(dura);                                                                       \
  nodesDestroyNode(node);                                                                       \
  nodesDestroyNode(funcs);                                                                      \
  nodesDestroyNode(tbl);                                                                        \
} while (0)

#define DROP_TSMA(ignore_not_exists, real) do {                                \
  ROOT_NODE() = createDropTSMAStmt(pCxt, ignore_not_exists, real);             \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(real);                                                      \
} while (0)

#define SHOW_TSMA(node) do {                                                   \
  ROOT_NODE() = createShowTSMASStmt(pCxt, node);                               \
  if (ROOT_NODE()) break;                                                      \
  nodesDestroyNode(node);                                                      \
} while (0)

#define CREATE_TSMA_FUNCS(target, funcs) do {                                  \
  target = createTSMAOptions(pCxt, funcs);                                     \
  if (target) break;                                                           \
  nodesDestroyList(funcs);                                                     \
} while (0)

#define USING_CLAUSE_MK(v_target, v_supertable, v_tags, v_vals) do {           \
  v_target = NULL;                                                             \
  if (pCxt->errCode == 0) {                                                    \
    if ((!v_tags) || (v_tags->length == values_count(v_vals))) {               \
      v_target = parser_create_using_clause(pCxt);                             \
      if (v_target) {                                                          \
        pCxt->in_using_clause = 0;                                             \
        pCxt->nr_cols = 0;                                                     \
        v_target->supertable        = v_supertable;                            \
        v_target->tags              = v_tags;                                  \
        v_target->vals              = v_vals;                                  \
        break;                                                                 \
      }                                                                        \
    } else {                                                                   \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "value count(%zd) does not match tag count(%d)",                     \
          values_count(v_vals), v_tags->length);                               \
    }                                                                          \
  }                                                                            \
  nodesDestroyList(v_tags);                                                    \
  values_destroy(v_vals);                                                      \
} while (0)

#define ROW_MK(target, values) do {                                            \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    if ((!pCxt->nr_cols) || values_count(values) == pCxt->nr_cols) {           \
      target = parser_create_row(pCxt);                                        \
      if (target) {                                                            \
        target->first = values;                                                \
        pCxt->nr_rows_of_single_table += 1;                                    \
        break;                                                                 \
      }                                                                        \
    } else {                                                                   \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "value count(%zd) does not match column count(%d)",                  \
          values_count(values), pCxt->nr_cols);                                \
    }                                                                          \
  }                                                                            \
  values_destroy(values);                                                      \
} while (0)

#define ROWS_APPEND(target, row) do {                                          \
  if (pCxt->errCode == 0) {                                                    \
    if (row_values_count(target) == row_values_count(row)) {                   \
      rows_append(target, row);                                                \
      break;                                                                   \
    }                                                                          \
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                       \
        "value count(%zd) of row %zd does not match previous count(%zd)",      \
        row_values_count(row), rows_count(target) + 1,                         \
        row_values_count(target));                                             \
  }                                                                            \
  rows_destroy(target);       target = NULL;                                   \
  row_destroy(row);           row    = NULL;                                   \
} while (0)

#define CHK_ROW_BEGIN() do {                                                   \
  if (pCxt->errCode == 0) {                                                    \
    if (pCxt->parse_result->placeholderNo && pCxt->nr_rows_of_single_table) {  \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "try to literally insert multiple rows "                             \
          "but statement seems parameterized");                                \
    }                                                                          \
  }                                                                            \
} while (0)

#define VALUES_APPEND(target, value) do {                                      \
  if (pCxt->errCode == 0) {                                                    \
    if ((!pCxt->nr_cols) || values_count(target) < pCxt->nr_cols) {            \
      values_append(target, value);                                            \
      break;                                                                   \
    }                                                                          \
    if (pCxt->in_using_clause) {                                               \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "value count(%zd) does not match tag count(%d)",                     \
          values_count(target)+1, pCxt->nr_cols);                              \
    } else {                                                                   \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "value count(%zd) does not match column count(%d)",                  \
          values_count(target)+1, pCxt->nr_cols);                              \
    }                                                                          \
  }                                                                            \
  values_destroy(target);     target = NULL;                                   \
  value_destroy(value);       value  = NULL;                                   \
} while (0)

#define CREATE_QUESTION(target, token) do {                                    \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    if (pCxt->nr_rows_of_single_table == 0) {                                  \
      if (pCxt->nr_tables == 1) {                                              \
        target = parser_create_question_value(pCxt, &token);                   \
        if (target) break;                                                     \
      } else {                                                                 \
        GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                   \
            "try to use parameter placeholder in multiple table");             \
      }                                                                        \
    } else {                                                                   \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                     \
          "try to use parameter placeholder in multiple rows");                \
    }                                                                          \
  }                                                                            \
} while (0)

#define SET_VALUE_TOKEN(target, t1, t2) do {                                   \
  if (!target) break;                                                          \
  value_set_token(target, &t1, &t2);                                           \
} while (0)

#define CREATE_TABLE_NODE(tbl_node, db_tbl) do {                               \
  tbl_node = NULL;                                                             \
  if (pCxt->errCode == 0) {                                                    \
    if (db_tbl.db.n == 0) {                                                    \
      tbl_node = createRealTableNode(pCxt, NULL, &db_tbl.tbl, NULL);           \
    } else {                                                                   \
      tbl_node = createRealTableNode(pCxt, &db_tbl.db, &db_tbl.tbl, NULL);     \
    }                                                                          \
  }                                                                            \
} while (0)

#define CREATE_INSERT_QUERY(target, target_tbl, fields, query) do {            \
  target = NULL;                                                               \
  SNode *node = NULL;                                                          \
  if (pCxt->errCode == 0) {                                                    \
    CREATE_TABLE_NODE(node, target_tbl);                                       \
    if (node) {                                                                \
      target = createInsertStmt(pCxt, node, fields, query);                    \
      if (target) break;                                                       \
    }                                                                          \
  }                                                                            \
  nodesDestroyNode(node);                                                      \
  nodesDestroyList(fields);                                                    \
  nodesDestroyNode(query);                                                     \
} while (0)

#define INSERT_INTO_MULTIPLE_TARGETS(insert_query, multiple_targets) do {      \
  insert_query = NULL;                                                         \
  if (pCxt->errCode == 0) {                                                    \
    insert_query = createInsertMultiStmt(pCxt, multiple_targets);              \
    if (insert_query) break;                                                   \
  }                                                                            \
  multiple_targets_destroy(multiple_targets);                                  \
} while (0)

#define INSERT_INTO_QUESTION(insert_query, question, using_clause, fields_clause, rows) do {   \
  insert_query = NULL;                                                                         \
  if (pCxt->errCode == 0) {                                                                    \
    insert_query = createInsertQuestionStmt(pCxt, question, using_clause, fields_clause, rows);\
    if (insert_query) break;                                                                   \
  }                                                                                            \
  value_destroy(question);                                                                     \
  using_clause_destroy(using_clause);                                                          \
  nodesDestroyList(fields_clause);                                                             \
  rows_destroy(rows);                                                                          \
} while (0)

#define SIMPLE_TARGET_MK(v_simple_target, v_target, v_using_clause, v_fields_clause, v_rows) do {     \
  v_simple_target = NULL;                                                                             \
  SNode *node = NULL;                                                                                 \
  if (pCxt->errCode == 0) {                                                                           \
    if ((!v_fields_clause) || (v_fields_clause->length == row_values_count(v_rows))) {                \
      v_simple_target = parser_create_simple_target(pCxt);                                            \
      if (v_simple_target) {                                                                          \
        v_simple_target->tbl            = v_target;                                                   \
        v_simple_target->using_clause   = v_using_clause;                                             \
        v_simple_target->fields_clause  = v_fields_clause;                                            \
        v_simple_target->rows           = v_rows;                                                     \
        break;                                                                                        \
      }                                                                                               \
    } else {                                                                                          \
      GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                                            \
          "value count(%zd) does not match column count(%d)",                                         \
          row_values_count(v_rows), v_fields_clause->length);                                         \
    }                                                                                                 \
  }                                                                                                   \
  nodesDestroyNode(node);                                                                             \
  using_clause_destroy(v_using_clause);                                                               \
  nodesDestroyList(v_fields_clause);                                                                  \
  rows_destroy(v_rows);                                                                               \
} while (0)

#define MULTIPLE_TARGETS_MK(multiple_targets, simple_target) do {                            \
  multiple_targets = NULL;                                                                   \
  if (pCxt->errCode == 0) {                                                                  \
    multiple_targets = parser_create_multiple_targets(pCxt);                                 \
    if (multiple_targets) {                                                                  \
      MULTIPLE_TARGETS_APPEND(multiple_targets, simple_target);                              \
      break;                                                                                 \
    }                                                                                        \
  }                                                                                          \
  simple_target_destroy(simple_target);      simple_target = NULL;                           \
} while (0)

#define CREATE_TARGET(target, v_db, v_tbl) do {                                \
  memset(&target, 0, sizeof(target));                                          \
  if (pCxt->errCode) break;                                                    \
  if (pCxt->parse_result->placeholderNo) {                                     \
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                       \
        "try to insert into multiple tables "                                  \
        "but statement seems parameterized");                                  \
    break;                                                                     \
  }                                                                            \
  if (parser_db_table_token_normalize_from(pCxt, &target, &v_db, &v_tbl)) {    \
    break;                                                                     \
  }                                                                            \
  pCxt->nr_cols                     = 0;                                       \
  pCxt->nr_rows_of_single_table     = 0;                                       \
  pCxt->nr_tables                  += 1;                                       \
  pCxt->in_using_clause             = 0;                                       \
} while (0)

#define CREATE_SUPERTABLE(target, v_db, v_tbl) do {                            \
  if (pCxt->errCode) break;                                                    \
  if (parser_db_table_token_normalize_from(pCxt, &target, &v_db, &v_tbl)) {    \
    break;                                                                     \
  }                                                                            \
  pCxt->nr_cols                     = 0;                                       \
  pCxt->nr_rows_of_single_table     = 0;                                       \
  pCxt->in_using_clause             = 1;                                       \
} while (0)

#define CREATE_QUESTION_TABLE(target, db, tbl) do {                            \
  target = NULL;                                                               \
  if (pCxt->errCode) break;                                                    \
  if (pCxt->parse_result->placeholderNo) {                                     \
    GEN_ERRX(TSDB_CODE_PAR_SYNTAX_ERROR,                                       \
        "try to insert into multiple tables "                                  \
        "but statement seems parameterized");                                  \
    break;                                                                     \
  }                                                                            \
  pCxt->nr_cols                     = 0;                                       \
  pCxt->nr_rows_of_single_table     = 0;                                       \
  pCxt->nr_tables                  += 1;                                       \
  pCxt->in_using_clause             = 0;                                       \
  target = parser_create_table_value(pCxt, db, tbl);                           \
} while (0)

#define MULTIPLE_TARGETS_APPEND(v_multiple_targets, v_simple_target) do {                        \
  if (pCxt->errCode == 0) {                                                                      \
    if (0 == parser_multiple_targets_append(pCxt, v_multiple_targets, v_simple_target)) break;   \
  }                                                                                              \
  multiple_targets_destroy(v_multiple_targets);                                                  \
  simple_target_destroy(v_simple_target);                                                        \
} while (0)

#define BIN_OP(target, op, left, right) do {                                   \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    target = op(pCxt, left, right);                                            \
    if (target) break;                                                         \
  }                                                                            \
  value_destroy(left);                                                         \
  value_destroy(right);                                                        \
} while (0)

#define ADD(target, left, right) do {                                          \
  BIN_OP(target, parser_create_add_op, left, right);                           \
} while (0)

#define SUB(target, left, right) do {                                          \
  BIN_OP(target, parser_create_sub_op, left, right);                           \
} while (0)

#define MUL(target, left, right) do {                                          \
  BIN_OP(target, parser_create_mul_op, left, right);                           \
} while (0)

#define DIV(target, left, right) do {                                          \
  BIN_OP(target, parser_create_div_op, left, right);                           \
} while (0)

#define NEG(target, neg, val) do {                                             \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    target = parser_create_neg_op(pCxt, &neg, val);                            \
    if (target) break;                                                         \
  }                                                                            \
  value_destroy(val);                                                          \
} while (0)

#define CAL(target, op, vals) do {                                             \
  target = NULL;                                                               \
  if (pCxt->errCode == 0) {                                                    \
    target = parser_create_cal_op(pCxt, &op, vals);                            \
    if (target) break;                                                         \
  }                                                                            \
  values_destroy(vals);                                                        \
} while (0)

}

%syntax_error {
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    if(TOKEN.z) {
      GEN_ERR(TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      GEN_ERR(TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
  } else if (TSDB_CODE_PAR_DB_NOT_SPECIFIED == pCxt->errCode && TK_NK_FLOAT == TOKEN.type) {
    GEN_ERR(TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
  }
}

%left OR.
%left AND.
%left UNION ALL MINUS EXCEPT INTERSECT.
%left NK_BITAND NK_BITOR NK_LSHIFT NK_RSHIFT.
%left NK_PLUS NK_MINUS.
%left NK_STAR NK_SLASH NK_REM.
%left NK_CONCAT.

/************************************************ create/alter account *****************************************/
cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options.                      { GEN_ERR(TSDB_CODE_PAR_EXPRIE_STATEMENT); }
cmd ::= ALTER ACCOUNT NK_ID alter_account_options.                                { GEN_ERR(TSDB_CODE_PAR_EXPRIE_STATEMENT); }

%type account_options                  { int32_t }
%destructor account_options            { }
account_options ::= .                                                             { }
account_options ::= account_options PPS literal.                                  { }
account_options ::= account_options TSERIES literal.                              { }
account_options ::= account_options STORAGE literal.                              { }
account_options ::= account_options STREAMS literal.                              { }
account_options ::= account_options QTIME literal.                                { }
account_options ::= account_options DBS literal.                                  { }
account_options ::= account_options USERS literal.                                { }
account_options ::= account_options CONNS literal.                                { }
account_options ::= account_options STATE literal.                                { }

%type alter_account_options            { int32_t }
%destructor alter_account_options      { }
alter_account_options ::= alter_account_option.                                   { }
alter_account_options ::= alter_account_options alter_account_option.             { }

%type alter_account_option             { int32_t }
%destructor alter_account_option       { }
alter_account_option ::= PASS literal.                                            { }
alter_account_option ::= PPS literal.                                             { }
alter_account_option ::= TSERIES literal.                                         { }
alter_account_option ::= STORAGE literal.                                         { }
alter_account_option ::= STREAMS literal.                                         { }
alter_account_option ::= QTIME literal.                                           { }
alter_account_option ::= DBS literal.                                             { }
alter_account_option ::= USERS literal.                                           { }
alter_account_option ::= CONNS literal.                                           { }
alter_account_option ::= STATE literal.                                           { }

%type ip_range_list                    { SNodeList* }
%destructor ip_range_list              { nodesDestroyList($$); }
ip_range_list(A) ::= NK_STRING(B).                                                { IP_RANGE_LIST_MK(A, B); }
ip_range_list(A) ::= ip_range_list(B) NK_COMMA NK_STRING(C).                      { A = B; IP_RANGE_LIST_APPEND(A, C); }

%type white_list                                                                  { SNodeList* }
%destructor white_list                                                            { nodesDestroyList($$); }
white_list(A) ::= HOST ip_range_list(B).                                          { A = B; }

%type white_list_opt                                                              { SNodeList* }
%destructor white_list_opt                                                        { nodesDestroyList($$); }
white_list_opt(A) ::= .                                                           { A = NULL; }
white_list_opt(A) ::= white_list(B).                                              { A = B; }

%type is_import_opt                                                                 { int8_t }
%destructor is_import_opt                                                           { }
is_import_opt(A) ::= .                                                              { A = 0; }
is_import_opt(A) ::= IS_IMPORT NK_INTEGER(B).                                       { TOKEN_TO_INT8(A, B); }

%type is_createdb_opt                  { int8_t }
%destructor is_createdb_opt            { }
is_createdb_opt(A) ::= .                                                              { A = 0; }
is_createdb_opt(A) ::= CREATEDB NK_INTEGER(B).                                        { TOKEN_TO_INT8(A, B); }
/************************************************ create/alter/drop user **********************************************/
cmd ::= CREATE USER user_name(A) PASS NK_STRING(B) sysinfo_opt(C) is_createdb_opt(D) is_import_opt(E)
                      white_list_opt(F).                                          { CREATE_USER(A, B, C, D, E, F); }
cmd ::= ALTER USER user_name(A) PASS NK_STRING(B).                                { ALTER_USER(A, B, TSDB_ALTER_USER_PASSWD); }
cmd ::= ALTER USER user_name(A) ENABLE NK_INTEGER(B).                             { ALTER_USER(A, B, TSDB_ALTER_USER_ENABLE); }
cmd ::= ALTER USER user_name(A) SYSINFO NK_INTEGER(B).                            { ALTER_USER(A, B, TSDB_ALTER_USER_SYSINFO); }
cmd ::= ALTER USER user_name(A) CREATEDB NK_INTEGER(B).                           { ALTER_USER(A, B, TSDB_ALTER_USER_CREATEDB); }
cmd ::= ALTER USER user_name(A) ADD white_list(B).                                { ALTER_USER_WLIST(A, B, TSDB_ALTER_USER_ADD_WHITE_LIST); }
cmd ::= ALTER USER user_name(A) DROP white_list(B).                               { ALTER_USER_WLIST(A, B, TSDB_ALTER_USER_DROP_WHITE_LIST); }
cmd ::= DROP USER user_name(A).                                                   { DROP_USER(A); }

%type sysinfo_opt                      { int8_t }
%destructor sysinfo_opt                { }
sysinfo_opt(A) ::= .                                                              { A = 1; }
sysinfo_opt(A) ::= SYSINFO NK_INTEGER(B).                                         { TOKEN_TO_INT8(A, B); }

/************************************************ grant/revoke ********************************************************/
cmd ::= GRANT privileges(A) ON priv_level(B) with_opt(D) TO user_name(C).         { GRANT_PRIV(A, B, C, D); }
cmd ::= REVOKE privileges(A) ON priv_level(B) with_opt(D) FROM user_name(C).      { REVOKE_PRIV(A, B, C, D); }

%type privileges                       { int64_t }
%destructor privileges                 { }
privileges(A) ::= ALL.                                                            { A = PRIVILEGE_TYPE_ALL; }
privileges(A) ::= priv_type_list(B).                                              { A = B; }
privileges(A) ::= SUBSCRIBE.                                                      { A = PRIVILEGE_TYPE_SUBSCRIBE; }

%type priv_type_list                   { int64_t }
%destructor priv_type_list             { }
priv_type_list(A) ::= priv_type(B).                                               { A = B; }
priv_type_list(A) ::= priv_type_list(B) NK_COMMA priv_type(C).                    { A = B | C; }

%type priv_type                        { int64_t }
%destructor priv_type                  { }
priv_type(A) ::= READ.                                                            { A = PRIVILEGE_TYPE_READ; }
priv_type(A) ::= WRITE.                                                           { A = PRIVILEGE_TYPE_WRITE; }
priv_type(A) ::= ALTER.                                                           { A = PRIVILEGE_TYPE_ALTER; }

%type priv_level                       { STokenPair }
%destructor priv_level                 { }
priv_level(A) ::= NK_STAR(B) NK_DOT NK_STAR(C).                                   { A.first = B; A.second = C; }
priv_level(A) ::= db_name(B) NK_DOT NK_STAR(C).                                   { A.first = B; A.second = C; }
priv_level(A) ::= db_name(B) NK_DOT table_name(C).                                { A.first = B; A.second = C; }
priv_level(A) ::= topic_name(B).                                                  { A.first = B; A.second = nil_token; }

with_opt(A) ::= .                                                                 { A = NULL; }
with_opt(A) ::= WITH search_condition(B).                                         { A = B; }

/************************************************ create encrypt_key *********************************************/
cmd ::= CREATE ENCRYPT_KEY NK_STRING(A).                                          { CREATE_ENCRYPT_KEY(A); }

/************************************************ create/drop/alter/restore dnode *********************************************/
cmd ::= CREATE DNODE dnode_endpoint(A).                                           { CREATE_DNODE(A, *NULL_TOKEN); }
cmd ::= CREATE DNODE dnode_endpoint(A) PORT NK_INTEGER(B).                        { CREATE_DNODE(A, B); }
cmd ::= DROP DNODE NK_INTEGER(A) force_opt(B).                                    { DROP_DNODE(A, B, false); }
cmd ::= DROP DNODE dnode_endpoint(A) force_opt(B).                                { DROP_DNODE(A, B, false); }
cmd ::= DROP DNODE NK_INTEGER(A) unsafe_opt(B).                                   { DROP_DNODE(A, false, B); }
cmd ::= DROP DNODE dnode_endpoint(A) unsafe_opt(B).                               { DROP_DNODE(A, false, B); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B).                                   { ALTER_DNODE(A, B, *NULL_TOKEN); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B) NK_STRING(C).                      { ALTER_DNODE(A, B, C); }
cmd ::= ALTER ALL DNODES NK_STRING(A).                                            { ALTER_DNODE(*NULL_TOKEN, A, *NULL_TOKEN); }
cmd ::= ALTER ALL DNODES NK_STRING(A) NK_STRING(B).                               { ALTER_DNODE(*NULL_TOKEN, A, B); }
cmd ::= RESTORE DNODE NK_INTEGER(A).                                              { RESTORE_DNODE(A); }

%type dnode_endpoint                   { SToken }
%destructor dnode_endpoint             { }
dnode_endpoint(A) ::= NK_STRING(B).                                               { A = B; }
dnode_endpoint(A) ::= NK_ID(B).                                                   { A = B; }
dnode_endpoint(A) ::= NK_IPTOKEN(B).                                              { A = B; }

%type force_opt                        { bool }
%destructor force_opt                  { }
force_opt(A) ::= .                                                                { A = false; }
force_opt(A) ::= FORCE.                                                           { A = true; }

%type unsafe_opt                       { bool }
%destructor unsafe_opt                 { }
unsafe_opt(A) ::= UNSAFE.                                                         { A = true; }

/************************************************ alter cluster *********************************************************/
cmd ::= ALTER CLUSTER NK_STRING(A).                                               { ALTER_CLUSTER(A, *NULL_TOKEN); }
cmd ::= ALTER CLUSTER NK_STRING(A) NK_STRING(B).                                  { ALTER_CLUSTER(A, B); }

/************************************************ alter local *********************************************************/
cmd ::= ALTER LOCAL NK_STRING(A).                                                 { ALTER_LOCAL(A, *NULL_TOKEN); }
cmd ::= ALTER LOCAL NK_STRING(A) NK_STRING(B).                                    { ALTER_LOCAL(A, B); }

/************************************************ create/drop/restore qnode ***************************************************/
cmd ::= CREATE QNODE ON DNODE NK_INTEGER(A).                                      { CREATE_QNODE(A); }
cmd ::= DROP QNODE ON DNODE NK_INTEGER(A).                                        { DROP_QNODE(A); }
cmd ::= RESTORE QNODE ON DNODE NK_INTEGER(A).                                     { RESTORE_QNODE(A); }

/************************************************ create/drop bnode ***************************************************/
cmd ::= CREATE BNODE ON DNODE NK_INTEGER(A).                                      { CREATE_BNODE(A); }
cmd ::= DROP BNODE ON DNODE NK_INTEGER(A).                                        { DROP_BNODE(A); }

/************************************************ create/drop snode ***************************************************/
cmd ::= CREATE SNODE ON DNODE NK_INTEGER(A).                                      { CREATE_SNODE(A); }
cmd ::= DROP SNODE ON DNODE NK_INTEGER(A).                                        { DROP_SNODE(A); }

/************************************************ create/drop/restore mnode ***************************************************/
cmd ::= CREATE MNODE ON DNODE NK_INTEGER(A).                                      { CREATE_MNODE(A); }
cmd ::= DROP MNODE ON DNODE NK_INTEGER(A).                                        { DROP_MNODE(A); }
cmd ::= RESTORE MNODE ON DNODE NK_INTEGER(A).                                     { RESTORE_MNODE(A); }

/************************************************ restore vnode ***************************************************/
cmd ::= RESTORE VNODE ON DNODE NK_INTEGER(A).                                     { RESTORE_VNODE(A); }

/************************************************ create/drop/use database ********************************************/
cmd ::= CREATE DATABASE not_exists_opt(A) db_name(B) db_options(C).               { CREATE_DB(A, B, C); }
cmd ::= DROP DATABASE exists_opt(A) db_name(B).                                   { DROP_DB(A, B); }
cmd ::= USE db_name(A).                                                           { USE_DB(A); }
cmd ::= ALTER DATABASE db_name(A) alter_db_options(B).                            { ALTER_DB(A, B); }
cmd ::= FLUSH DATABASE db_name(A).                                                { FLUSH_DB(A); }
cmd ::= TRIM DATABASE db_name(A) speed_opt(B).                                    { TRIM_DB(A, B); }
cmd ::= S3MIGRATE DATABASE db_name(A).                                            { S3MIGRATE_DB(A); }
cmd ::= COMPACT DATABASE db_name(A) start_opt(B) end_opt(C).                      { COMPACT_DB(A, B, C); }

%type not_exists_opt                   { bool }
%destructor not_exists_opt             { }
not_exists_opt(A) ::= IF NOT EXISTS.                                              { A = true; }
not_exists_opt(A) ::= .                                                           { A = false; }

%type exists_opt                       { bool }
%destructor exists_opt                 { }
exists_opt(A) ::= IF EXISTS.                                                      { A = true; }
exists_opt(A) ::= .                                                               { A = false; }

db_options(A) ::= .                                                               { CREATE_DEFAULT_DB_OPT(A); }
db_options(A) ::= db_options(B) BUFFER NK_INTEGER(C).                             { A = setDatabaseOption(pCxt, B, DB_OPTION_BUFFER, &C); }
db_options(A) ::= db_options(B) CACHEMODEL NK_STRING(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_CACHEMODEL, &C); }
db_options(A) ::= db_options(B) CACHESIZE NK_INTEGER(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_CACHESIZE, &C); }
db_options(A) ::= db_options(B) COMP NK_INTEGER(C).                               { A = setDatabaseOption(pCxt, B, DB_OPTION_COMP, &C); }
db_options(A) ::= db_options(B) DURATION NK_INTEGER(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_DAYS, &C); }
db_options(A) ::= db_options(B) DURATION NK_VARIABLE(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_DAYS, &C); }
db_options(A) ::= db_options(B) MAXROWS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_MAXROWS, &C); }
db_options(A) ::= db_options(B) MINROWS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_MINROWS, &C); }
db_options(A) ::= db_options(B) KEEP integer_list(C).                             { A = setDatabaseOption(pCxt, B, DB_OPTION_KEEP, C); }
db_options(A) ::= db_options(B) KEEP variable_list(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_KEEP, C); }
db_options(A) ::= db_options(B) PAGES NK_INTEGER(C).                              { A = setDatabaseOption(pCxt, B, DB_OPTION_PAGES, &C); }
db_options(A) ::= db_options(B) PAGESIZE NK_INTEGER(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_PAGESIZE, &C); }
db_options(A) ::= db_options(B) TSDB_PAGESIZE NK_INTEGER(C).                      { A = setDatabaseOption(pCxt, B, DB_OPTION_TSDB_PAGESIZE, &C); }
db_options(A) ::= db_options(B) PRECISION NK_STRING(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_PRECISION, &C); }
db_options(A) ::= db_options(B) REPLICA NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_REPLICA, &C); }
//db_options(A) ::= db_options(B) STRICT NK_STRING(C).                              { A = setDatabaseOption(pCxt, B, DB_OPTION_STRICT, &C); }
db_options(A) ::= db_options(B) VGROUPS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_VGROUPS, &C); }
db_options(A) ::= db_options(B) SINGLE_STABLE NK_INTEGER(C).                      { A = setDatabaseOption(pCxt, B, DB_OPTION_SINGLE_STABLE, &C); }
db_options(A) ::= db_options(B) RETENTIONS retention_list(C).                     { A = setDatabaseOption(pCxt, B, DB_OPTION_RETENTIONS, C); }
db_options(A) ::= db_options(B) SCHEMALESS NK_INTEGER(C).                         { A = setDatabaseOption(pCxt, B, DB_OPTION_SCHEMALESS, &C); }
db_options(A) ::= db_options(B) WAL_LEVEL NK_INTEGER(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL, &C); }
db_options(A) ::= db_options(B) WAL_FSYNC_PERIOD NK_INTEGER(C).                   { A = setDatabaseOption(pCxt, B, DB_OPTION_FSYNC, &C); }
db_options(A) ::= db_options(B) WAL_RETENTION_PERIOD NK_INTEGER(C).               { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_RETENTION_PERIOD, &C); }
db_options(A) ::= db_options(B) WAL_RETENTION_PERIOD NK_MINUS(D) NK_INTEGER(C).   {
                                                                                    SToken t = D;
                                                                                    t.n = (C.z + C.n) - D.z;
                                                                                    A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_RETENTION_PERIOD, &t);
                                                                                  }
db_options(A) ::= db_options(B) WAL_RETENTION_SIZE NK_INTEGER(C).                 { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_RETENTION_SIZE, &C); }
db_options(A) ::= db_options(B) WAL_RETENTION_SIZE NK_MINUS(D) NK_INTEGER(C).     {
                                                                                    SToken t = D;
                                                                                    t.n = (C.z + C.n) - D.z;
                                                                                    A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_RETENTION_SIZE, &t);
                                                                                  }
db_options(A) ::= db_options(B) WAL_ROLL_PERIOD NK_INTEGER(C).                    { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_ROLL_PERIOD, &C); }
db_options(A) ::= db_options(B) WAL_SEGMENT_SIZE NK_INTEGER(C).                   { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL_SEGMENT_SIZE, &C); }
db_options(A) ::= db_options(B) STT_TRIGGER NK_INTEGER(C).                        { A = setDatabaseOption(pCxt, B, DB_OPTION_STT_TRIGGER, &C); }
db_options(A) ::= db_options(B) TABLE_PREFIX signed(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_TABLE_PREFIX, C); }
db_options(A) ::= db_options(B) TABLE_SUFFIX signed(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_TABLE_SUFFIX, C); }
db_options(A) ::= db_options(B) S3_CHUNKSIZE NK_INTEGER(C).                       { A = setDatabaseOption(pCxt, B, DB_OPTION_S3_CHUNKSIZE, &C); }
db_options(A) ::= db_options(B) S3_KEEPLOCAL NK_INTEGER(C).                       { A = setDatabaseOption(pCxt, B, DB_OPTION_S3_KEEPLOCAL, &C); }
db_options(A) ::= db_options(B) S3_KEEPLOCAL NK_VARIABLE(C).                      { A = setDatabaseOption(pCxt, B, DB_OPTION_S3_KEEPLOCAL, &C); }
db_options(A) ::= db_options(B) S3_COMPACT NK_INTEGER(C).                         { A = setDatabaseOption(pCxt, B, DB_OPTION_S3_COMPACT, &C); }
db_options(A) ::= db_options(B) KEEP_TIME_OFFSET NK_INTEGER(C).                   { A = setDatabaseOption(pCxt, B, DB_OPTION_KEEP_TIME_OFFSET, &C); }
db_options(A) ::= db_options(B) ENCRYPT_ALGORITHM NK_STRING(C).                   { A = setDatabaseOption(pCxt, B, DB_OPTION_ENCRYPT_ALGORITHM, &C); }

alter_db_options(A) ::= alter_db_option(B).                                       { A = createAlterDatabaseOptions(pCxt); A = setAlterDatabaseOption(pCxt, A, &B); }
alter_db_options(A) ::= alter_db_options(B) alter_db_option(C).                   { A = setAlterDatabaseOption(pCxt, B, &C); }

%type alter_db_option                  { SAlterOption }
%destructor alter_db_option            { }
alter_db_option(A) ::= BUFFER NK_INTEGER(B).                                      { A.type = DB_OPTION_BUFFER; A.val = B; }
alter_db_option(A) ::= CACHEMODEL NK_STRING(B).                                   { A.type = DB_OPTION_CACHEMODEL; A.val = B; }
alter_db_option(A) ::= CACHESIZE NK_INTEGER(B).                                   { A.type = DB_OPTION_CACHESIZE; A.val = B; }
alter_db_option(A) ::= WAL_FSYNC_PERIOD NK_INTEGER(B).                            { A.type = DB_OPTION_FSYNC; A.val = B; }
alter_db_option(A) ::= KEEP integer_list(B).                                      { A.type = DB_OPTION_KEEP; A.pList = B; }
alter_db_option(A) ::= KEEP variable_list(B).                                     { A.type = DB_OPTION_KEEP; A.pList = B; }
alter_db_option(A) ::= PAGES NK_INTEGER(B).                                       { A.type = DB_OPTION_PAGES; A.val = B; }
alter_db_option(A) ::= REPLICA NK_INTEGER(B).                                     { A.type = DB_OPTION_REPLICA; A.val = B; }
//alter_db_option(A) ::= STRICT NK_STRING(B).                                       { A.type = DB_OPTION_STRICT; A.val = B; }
alter_db_option(A) ::= WAL_LEVEL NK_INTEGER(B).                                   { A.type = DB_OPTION_WAL; A.val = B; }
alter_db_option(A) ::= STT_TRIGGER NK_INTEGER(B).                                 { A.type = DB_OPTION_STT_TRIGGER; A.val = B; }
alter_db_option(A) ::= MINROWS NK_INTEGER(B).                                     { A.type = DB_OPTION_MINROWS; A.val = B; }
alter_db_option(A) ::= WAL_RETENTION_PERIOD NK_INTEGER(B).                        { A.type = DB_OPTION_WAL_RETENTION_PERIOD; A.val = B; }
alter_db_option(A) ::= WAL_RETENTION_PERIOD NK_MINUS(B) NK_INTEGER(C).            {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A.type = DB_OPTION_WAL_RETENTION_PERIOD; A.val = t;
                                                                                  }
alter_db_option(A) ::= WAL_RETENTION_SIZE NK_INTEGER(B).                          { A.type = DB_OPTION_WAL_RETENTION_SIZE; A.val = B; }
alter_db_option(A) ::= WAL_RETENTION_SIZE NK_MINUS(B) NK_INTEGER(C).              {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A.type = DB_OPTION_WAL_RETENTION_SIZE; A.val = t;
                                                                                  }
alter_db_option(A) ::= S3_KEEPLOCAL NK_INTEGER(B).                                { A.type = DB_OPTION_S3_KEEPLOCAL; A.val = B; }
alter_db_option(A) ::= S3_KEEPLOCAL NK_VARIABLE(B).                               { A.type = DB_OPTION_S3_KEEPLOCAL; A.val = B; }
alter_db_option(A) ::= S3_COMPACT NK_INTEGER(B).                                  { A.type = DB_OPTION_S3_COMPACT, A.val = B; }
alter_db_option(A) ::= KEEP_TIME_OFFSET NK_INTEGER(B).                            { A.type = DB_OPTION_KEEP_TIME_OFFSET; A.val = B; }
alter_db_option(A) ::= ENCRYPT_ALGORITHM NK_STRING(B).                            { A.type = DB_OPTION_ENCRYPT_ALGORITHM; A.val = B; }

%type integer_list                     { SNodeList* }
%destructor integer_list               { nodesDestroyList($$); }
integer_list(A) ::= NK_INTEGER(B).                                                { INTEGER_LIST_MK(A, B); }
integer_list(A) ::= integer_list(B) NK_COMMA NK_INTEGER(C).                       { A = B; INTEGER_LIST_APPEND(A, C); }

%type variable_list                    { SNodeList* }
%destructor variable_list              { nodesDestroyList($$); }
variable_list(A) ::= NK_VARIABLE(B).                                              { VAR_LIST_MK(A, B); }
variable_list(A) ::= variable_list(B) NK_COMMA NK_VARIABLE(C).                    { A = B; VAR_LIST_APPEND(A, C); }

%type retention_list                   { SNodeList* }
%destructor retention_list             { nodesDestroyList($$); }
retention_list(A) ::= retention(B).                                               { LIST_MK(A, B); }
retention_list(A) ::= retention_list(B) NK_COMMA retention(C).                    { A = B; LIST_ADD_NODE(A, C); }

retention(A) ::= NK_VARIABLE(B) NK_COLON NK_VARIABLE(C).                          { A = createNodeListNodeEx(pCxt, createDurationValueNode(pCxt, &B), createDurationValueNode(pCxt, &C)); }
retention(A) ::= NK_MINUS(B) NK_COLON NK_VARIABLE(C).                             { A = createNodeListNodeEx(pCxt, createDurationValueNode(pCxt, &B), createDurationValueNode(pCxt, &C)); }

%type speed_opt                        { int32_t }
%destructor speed_opt                  { }
speed_opt(A) ::= .                                                                { A = 0; }
speed_opt(A) ::= BWLIMIT NK_INTEGER(B).                                           { TOKEN_TO_INT32(A, B); }

start_opt(A) ::= .                                                                { A = NULL; }
start_opt(A) ::= START WITH NK_INTEGER(B).                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B); }
start_opt(A) ::= START WITH NK_STRING(B).                                         { A = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &B); }
start_opt(A) ::= START WITH TIMESTAMP NK_STRING(B).                               { A = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &B); }

end_opt(A) ::= .                                                                  { A = NULL; }
end_opt(A) ::= END WITH NK_INTEGER(B).                                            { A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B); }
end_opt(A) ::= END WITH NK_STRING(B).                                             { A = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &B); }
end_opt(A) ::= END WITH TIMESTAMP NK_STRING(B).                                   { A = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &B); }

/************************************************ create/drop table/stable ********************************************/
cmd ::= CREATE TABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def_opt(D) table_options(E).                { CREATE_TABLE(A, B, C, D, E); }
cmd ::= CREATE TABLE multi_create_clause(A).                                      { CREATE_MULTI_TABLE(A); }
cmd ::= CREATE TABLE not_exists_opt(B) USING full_table_name(C)
  NK_LP tag_list_opt(D) NK_RP FILE NK_STRING(E).                                  { CREATE_SUBTBL_FROM_FILE(B, C, D, E); }
cmd ::= CREATE STABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def(D) table_options(E).                    { CREATE_TABLE(A, B, C, D, E); }
cmd ::= DROP TABLE multi_drop_clause(A).                                          { DROP_TABLE(A); }
cmd ::= DROP STABLE exists_opt(A) full_table_name(B).                             { DROP_SUPER_TABLE(A, B); }

cmd ::= ALTER TABLE alter_table_clause(A).                                        { ROOT_NODE() = A; }
cmd ::= ALTER STABLE alter_table_clause(A).                                       { ALTER_SUPER_TABLE(A); }

alter_table_clause(A) ::= full_table_name(B) alter_table_options(C).              { ALTER_TABLE_CLAUSE(A, B, C); }
alter_table_clause(A) ::=
  full_table_name(B) ADD COLUMN column_name(C) type_name(D) column_options(E).    { ALTER_TABLE_ADD_COL(A, B, C, D, E); }
alter_table_clause(A) ::= full_table_name(B) DROP COLUMN column_name(C).          { ALTER_TABLE_DROP_COL(A, B, C); }
alter_table_clause(A) ::=
  full_table_name(B) MODIFY COLUMN column_name(C) type_name(D).                   { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &C, D); }
alter_table_clause(A) ::=
  full_table_name(B) MODIFY COLUMN column_name(C) column_options(D).              { A = createAlterTableAddModifyColOptions(pCxt, B, TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS, &C, D); }
alter_table_clause(A) ::=
  full_table_name(B) RENAME COLUMN column_name(C) column_name(D).                 { ALTER_TABLE_MOD_COL_NAME(A, B, C, D); }
alter_table_clause(A) ::=
  full_table_name(B) ADD TAG column_name(C) type_name(D).                         { ALTER_TABLE_ADD_TAG(A, B, C, D); }
alter_table_clause(A) ::= full_table_name(B) DROP TAG column_name(C).             { ALTER_TABLE_DROP_TAG(A, B, C); }
alter_table_clause(A) ::=
  full_table_name(B) MODIFY TAG column_name(C) type_name(D).                      { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &C, D); }
alter_table_clause(A) ::=
  full_table_name(B) RENAME TAG column_name(C) column_name(D).                    { ALTER_TABLE_MOD_TAG_NAME(A, B, C, D); }
alter_table_clause(A) ::=
  full_table_name(B) SET TAG column_name(C) NK_EQ tags_literal(D).                { ALTER_TABLE_SET_TAG(A, B, C, D); }

%type multi_create_clause              { SNodeList* }
%destructor multi_create_clause        { nodesDestroyList($$); }
multi_create_clause(A) ::= create_subtable_clause(B).                             { LIST_MK(A, B); }
multi_create_clause(A) ::= multi_create_clause(B) create_subtable_clause(C).      { A = B; LIST_ADD_NODE(A, C); }

create_subtable_clause(A) ::=
  not_exists_opt(B) full_table_name(C) USING full_table_name(D)
  specific_cols_opt(E) TAGS NK_LP tags_literal_list(F) NK_RP table_options(G).    { CREATE_SUBTBL_CLAUSE(A, B, C, D, E, F, G); }

%type multi_drop_clause                { SNodeList* }
%destructor multi_drop_clause          { nodesDestroyList($$); }
multi_drop_clause(A) ::= drop_table_clause(B).                                    { LIST_MK(A, B); }
multi_drop_clause(A) ::= multi_drop_clause(B) NK_COMMA drop_table_clause(C).      { A = B; LIST_ADD_NODE(A, C); }

drop_table_clause(A) ::= exists_opt(B) full_table_name(C).                        { DROP_TABLE_CLAUSE(A, B, C); }

%type specific_cols_opt                { SNodeList* }
%destructor specific_cols_opt          { nodesDestroyList($$); }
specific_cols_opt(A) ::= .                                                        { A = NULL; }
specific_cols_opt(A) ::= NK_LP col_name_list(B) NK_RP.                            { A = B; }

full_table_name(A) ::= table_name(B).                                             { A = createRealTableNode(pCxt, NULL, &B, NULL); }
full_table_name(A) ::= db_name(B) NK_DOT table_name(C).                           { A = createRealTableNode(pCxt, &B, &C, NULL); }

%type tag_def_list                     { SNodeList* }
%destructor tag_def_list               { nodesDestroyList($$); }
tag_def_list(A) ::= tag_def(B).                                                   { LIST_MK(A, B); }
tag_def_list(A) ::= tag_def_list(B) NK_COMMA tag_def(C).                          { A = B; LIST_ADD_NODE(A, C); }
tag_def(A) ::= column_name(B) type_name(C).                                       { CREATE_COL_DEF(A, B, C, NULL_NODE); }

%type column_def_list                  { SNodeList* }
%destructor column_def_list            { nodesDestroyList($$); }
column_def_list(A) ::= column_def(B).                                             { LIST_MK(A, B); }
column_def_list(A) ::= column_def_list(B) NK_COMMA column_def(C).                 { A = B; LIST_ADD_NODE(A, C); }

// column_def(A) ::= column_name(B) type_name(C).                                 { A = createColumnDefNode(pCxt, &B, C, NULL); }
column_def(A) ::= column_name(B) type_name(C) column_options(D).                  { CREATE_COL_DEF(A, B, C, D); }

%type type_name                        { SDataType }
%destructor type_name                  { }
type_name(A) ::= BOOL.                                                            { A = createDataType(TSDB_DATA_TYPE_BOOL); }
type_name(A) ::= TINYINT.                                                         { A = createDataType(TSDB_DATA_TYPE_TINYINT); }
type_name(A) ::= SMALLINT.                                                        { A = createDataType(TSDB_DATA_TYPE_SMALLINT); }
type_name(A) ::= INT.                                                             { A = createDataType(TSDB_DATA_TYPE_INT); }
type_name(A) ::= INTEGER.                                                         { A = createDataType(TSDB_DATA_TYPE_INT); }
type_name(A) ::= BIGINT.                                                          { A = createDataType(TSDB_DATA_TYPE_BIGINT); }
type_name(A) ::= FLOAT.                                                           { A = createDataType(TSDB_DATA_TYPE_FLOAT); }
type_name(A) ::= DOUBLE.                                                          { A = createDataType(TSDB_DATA_TYPE_DOUBLE); }
type_name(A) ::= BINARY NK_LP NK_INTEGER(B) NK_RP.                                { A = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &B); }
type_name(A) ::= TIMESTAMP.                                                       { A = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
type_name(A) ::= NCHAR NK_LP NK_INTEGER(B) NK_RP.                                 { A = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &B); }
type_name(A) ::= TINYINT UNSIGNED.                                                { A = createDataType(TSDB_DATA_TYPE_UTINYINT); }
type_name(A) ::= SMALLINT UNSIGNED.                                               { A = createDataType(TSDB_DATA_TYPE_USMALLINT); }
type_name(A) ::= INT UNSIGNED.                                                    { A = createDataType(TSDB_DATA_TYPE_UINT); }
type_name(A) ::= BIGINT UNSIGNED.                                                 { A = createDataType(TSDB_DATA_TYPE_UBIGINT); }
type_name(A) ::= JSON.                                                            { A = createDataType(TSDB_DATA_TYPE_JSON); }
type_name(A) ::= VARCHAR NK_LP NK_INTEGER(B) NK_RP.                               { A = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &B); }
type_name(A) ::= MEDIUMBLOB.                                                      { A = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
type_name(A) ::= BLOB.                                                            { A = createDataType(TSDB_DATA_TYPE_BLOB); }
type_name(A) ::= VARBINARY NK_LP NK_INTEGER(B) NK_RP.                             { A = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &B); }
type_name(A) ::= GEOMETRY NK_LP NK_INTEGER(B) NK_RP.                              { A = createVarLenDataType(TSDB_DATA_TYPE_GEOMETRY, &B); }
type_name(A) ::= DECIMAL.                                                         { A = createDataType(TSDB_DATA_TYPE_DECIMAL); }
type_name(A) ::= DECIMAL NK_LP NK_INTEGER NK_RP.                                  { A = createDataType(TSDB_DATA_TYPE_DECIMAL); }
type_name(A) ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP.              { A = createDataType(TSDB_DATA_TYPE_DECIMAL); }

%type type_name_default_len            { SDataType }
%destructor type_name_default_len      { }
type_name_default_len(A) ::= BINARY.                                              { A = createVarLenDataType(TSDB_DATA_TYPE_BINARY, NULL); }
type_name_default_len(A) ::= NCHAR.                                               { A = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, NULL); }
type_name_default_len(A) ::= VARCHAR.                                             { A = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, NULL); }
type_name_default_len(A) ::= VARBINARY.                                           { A = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, NULL); }

%type tags_def_opt                     { SNodeList* }
%destructor tags_def_opt               { nodesDestroyList($$); }
tags_def_opt(A) ::= .                                                             { A = NULL; }
tags_def_opt(A) ::= tags_def(B).                                                  { A = B; }

%type tags_def                         { SNodeList* }
%destructor tags_def                   { nodesDestroyList($$); }
tags_def(A) ::= TAGS NK_LP tag_def_list(B) NK_RP.                              { A = B; }

table_options(A) ::= .                                                            { A = createDefaultTableOptions(pCxt); }
table_options(A) ::= table_options(B) COMMENT NK_STRING(C).                       { A = setTableOption(pCxt, B, TABLE_OPTION_COMMENT, &C); }
table_options(A) ::= table_options(B) MAX_DELAY duration_list(C).                 { A = setTableOption(pCxt, B, TABLE_OPTION_MAXDELAY, C); }
table_options(A) ::= table_options(B) WATERMARK duration_list(C).                 { A = setTableOption(pCxt, B, TABLE_OPTION_WATERMARK, C); }
table_options(A) ::= table_options(B) ROLLUP NK_LP rollup_func_list(C) NK_RP.     { A = setTableOption(pCxt, B, TABLE_OPTION_ROLLUP, C); }
table_options(A) ::= table_options(B) TTL NK_INTEGER(C).                          { A = setTableOption(pCxt, B, TABLE_OPTION_TTL, &C); }
table_options(A) ::= table_options(B) SMA NK_LP col_name_list(C) NK_RP.           { A = setTableOption(pCxt, B, TABLE_OPTION_SMA, C); }
table_options(A) ::= table_options(B) DELETE_MARK duration_list(C).               { A = setTableOption(pCxt, B, TABLE_OPTION_DELETE_MARK, C); }

alter_table_options(A) ::= alter_table_option(B).                                 { A = createAlterTableOptions(pCxt); A = setTableOption(pCxt, A, B.type, &B.val); }
alter_table_options(A) ::= alter_table_options(B) alter_table_option(C).          { A = setTableOption(pCxt, B, C.type, &C.val); }

%type alter_table_option               { SAlterOption }
%destructor alter_table_option         { }
alter_table_option(A) ::= COMMENT NK_STRING(B).                                   { A.type = TABLE_OPTION_COMMENT; A.val = B; }
alter_table_option(A) ::= TTL NK_INTEGER(B).                                      { A.type = TABLE_OPTION_TTL; A.val = B; }

%type duration_list                    { SNodeList* }
%destructor duration_list              { nodesDestroyList($$); }
duration_list(A) ::= duration_literal(B).                                         { A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
duration_list(A) ::= duration_list(B) NK_COMMA duration_literal(C).               { A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

%type rollup_func_list                 { SNodeList* }
%destructor rollup_func_list           { nodesDestroyList($$); }
rollup_func_list(A) ::= rollup_func_name(B).                                      { LIST_MK(A, B); }
rollup_func_list(A) ::= rollup_func_list(B) NK_COMMA rollup_func_name(C).         { A = B; LIST_ADD_NODE(A, C); }

rollup_func_name(A) ::= function_name(B).                                         { CREATE_FUNC(A, B, NULL_LIST); }
rollup_func_name(A) ::= FIRST(B).                                                 { CREATE_FUNC(A, B, NULL_LIST); }
rollup_func_name(A) ::= LAST(B).                                                  { CREATE_FUNC(A, B, NULL_LIST); }

%type col_name_list                    { SNodeList* }
%destructor col_name_list              { nodesDestroyList($$); }
col_name_list(A) ::= col_name(B).                                                 { LIST_MK(A, B); }
col_name_list(A) ::= col_name_list(B) NK_COMMA col_name(C).                       { A = B; COL_LIST_ADD_NODE(A, C); }

col_name(A) ::= column_name(B).                                                   { A = createColumnNode(pCxt, NULL, &B); }

/************************************************ show ****************************************************************/
cmd ::= SHOW DNODES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT); }
cmd ::= SHOW USERS.                                                               { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT); }
cmd ::= SHOW USERS FULL.                                                          { ROOT_NODE() = createShowStmtWithFull(pCxt, QUERY_NODE_SHOW_USERS_FULL_STMT); }
cmd ::= SHOW USER PRIVILEGES.                                                     { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_USER_PRIVILEGES_STMT); }
cmd ::= SHOW db_kind_opt(A) DATABASES.                                            {
                                                                                    ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT);
                                                                                    if (!ROOT_NODE()) break;
                                                                                    setShowKind(pCxt, ROOT_NODE(), A);
                                                                                  }
cmd ::= SHOW table_kind_db_name_cond_opt(A) TABLES like_pattern_opt(B).           { SHOW_TABLE(A, B, OP_TYPE_LIKE); }
cmd ::= SHOW db_name_cond_opt(A) STABLES like_pattern_opt(B).                     { SHOW_STABLE(A, B, OP_TYPE_LIKE); }
cmd ::= SHOW db_name_cond_opt(A) VGROUPS.                                         { SHOW_VGROUPS(A, OP_TYPE_LIKE); }
cmd ::= SHOW MNODES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT); }
//cmd ::= SHOW MODULES.                                                             { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_MODULES_STMT); }
cmd ::= SHOW QNODES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT); }
cmd ::= SHOW ARBGROUPS.                                                           { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_ARBGROUPS_STMT); }
cmd ::= SHOW FUNCTIONS.                                                           { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT); }
cmd ::= SHOW INDEXES FROM table_name_cond(A) from_db_opt(B).                      { SHOW_INDEXES(A, B, OP_TYPE_EQUAL); }
cmd ::= SHOW INDEXES FROM db_name(A) NK_DOT table_name(B).                        { SHOW_INDEXES2(A, B, OP_TYPE_EQUAL); }
cmd ::= SHOW STREAMS.                                                             { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT); }
cmd ::= SHOW ACCOUNTS.                                                            { GEN_ERR(TSDB_CODE_PAR_EXPRIE_STATEMENT); }
cmd ::= SHOW APPS.                                                                { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_APPS_STMT); }
cmd ::= SHOW CONNECTIONS.                                                         { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_CONNECTIONS_STMT); }
cmd ::= SHOW LICENCES.                                                            { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_LICENCES_STMT); }
cmd ::= SHOW GRANTS.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_LICENCES_STMT); }
cmd ::= SHOW GRANTS FULL.                                                         { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_FULL_STMT); }
cmd ::= SHOW GRANTS LOGS.                                                         { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_LOGS_STMT); }
cmd ::= SHOW CLUSTER MACHINES.                                                    { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT); }
cmd ::= SHOW CREATE DATABASE db_name(A).                                          { ROOT_NODE() = createShowCreateDatabaseStmt(pCxt, &A); }
cmd ::= SHOW CREATE TABLE full_table_name(A).                                     { SHOW_CREATE_TABLE(A, QUERY_NODE_SHOW_CREATE_TABLE_STMT); }
cmd ::= SHOW CREATE STABLE full_table_name(A).                                    { SHOW_CREATE_TABLE(A, QUERY_NODE_SHOW_CREATE_STABLE_STMT); }
cmd ::= SHOW ENCRYPTIONS.                                                         { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_ENCRYPTIONS_STMT); }
cmd ::= SHOW QUERIES.                                                             { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_QUERIES_STMT); }
cmd ::= SHOW SCORES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_SCORES_STMT); }
cmd ::= SHOW TOPICS.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_TOPICS_STMT); }
cmd ::= SHOW VARIABLES.                                                           { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_VARIABLES_STMT); }
cmd ::= SHOW CLUSTER VARIABLES.                                                   { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_VARIABLES_STMT); }
cmd ::= SHOW LOCAL VARIABLES.                                                     { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT); }
cmd ::= SHOW DNODE NK_INTEGER(A) VARIABLES like_pattern_opt(B).                   { SHOW_DNODE(A, B); }
cmd ::= SHOW BNODES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_BNODES_STMT); }
cmd ::= SHOW SNODES.                                                              { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_SNODES_STMT); }
cmd ::= SHOW CLUSTER.                                                             { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_STMT); }
cmd ::= SHOW TRANSACTIONS.                                                        { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_TRANSACTIONS_STMT); }
cmd ::= SHOW TABLE DISTRIBUTED full_table_name(A).                                { SHOW_TABLE_DIST(A); }
cmd ::= SHOW CONSUMERS.                                                           { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_CONSUMERS_STMT); }
cmd ::= SHOW SUBSCRIPTIONS.                                                       { ROOT_NODE() = createShowStmt(pCxt, QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT); }
cmd ::= SHOW TAGS FROM table_name_cond(A) from_db_opt(B).                         { SHOW_TAGS(A, B, OP_TYPE_EQUAL); }
cmd ::= SHOW TAGS FROM db_name(B) NK_DOT table_name(A).                           { SHOW_TAGS2(A, B, OP_TYPE_EQUAL); }
cmd ::= SHOW TABLE TAGS tag_list_opt(A) FROM table_name_cond(B) from_db_opt(C).   { SHOW_TABLE_TAGS(A, B, C); }
cmd ::= SHOW TABLE TAGS tag_list_opt(A) FROM db_name(B) NK_DOT table_name(C).     { SHOW_TABLE_TAGS2(A, B, C); }
cmd ::= SHOW VNODES ON DNODE NK_INTEGER(A).                                       { SHOW_VNODES_ON_DNODE(A); }
cmd ::= SHOW VNODES.                                                              { ROOT_NODE() = createShowVnodesStmt(pCxt, NULL, NULL); }
// show alive
cmd ::= SHOW db_name_cond_opt(A) ALIVE.                                           { SHOW_STMT_ALIVE(A, QUERY_NODE_SHOW_DB_ALIVE_STMT); }
cmd ::= SHOW CLUSTER ALIVE.                                                       { SHOW_STMT_ALIVE(NULL, QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT); }
cmd ::= SHOW db_name_cond_opt(A) VIEWS like_pattern_opt(B).                       { ROOT_NODE() = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VIEWS_STMT, A, B, OP_TYPE_LIKE); }
cmd ::= SHOW CREATE VIEW full_table_name(A).                                      { SHOW_CREATE_VIEW(A); }
cmd ::= SHOW COMPACTS.                                                            { ROOT_NODE() = createShowCompactsStmt(pCxt, QUERY_NODE_SHOW_COMPACTS_STMT); }
cmd ::= SHOW COMPACT NK_INTEGER(A).                                               { SHOW_COMPACT(A); }

%type table_kind_db_name_cond_opt           { SShowTablesOption }
%destructor table_kind_db_name_cond_opt     { }
table_kind_db_name_cond_opt(A) ::= .                                              { A.kind = SHOW_KIND_ALL; A.dbName = nil_token; }
table_kind_db_name_cond_opt(A) ::= table_kind(B).                                 { A.kind = B; A.dbName = nil_token; }
table_kind_db_name_cond_opt(A) ::= db_name(C) NK_DOT.                             { A.kind = SHOW_KIND_ALL; A.dbName = C; }
table_kind_db_name_cond_opt(A) ::= table_kind(B) db_name(C) NK_DOT.               { A.kind = B; A.dbName = C; }

%type table_kind                       { EShowKind }
%destructor table_kind                 { }
table_kind(A) ::= NORMAL.                                                         { A = SHOW_KIND_TABLES_NORMAL; }
table_kind(A) ::= CHILD.                                                          { A = SHOW_KIND_TABLES_CHILD; }

db_name_cond_opt(A) ::= .                                                         { A = createDefaultDatabaseCondValue(pCxt); }
db_name_cond_opt(A) ::= db_name(B) NK_DOT.                                        { A = createIdentifierValueNode(pCxt, &B); }

like_pattern_opt(A) ::= .                                                         { A = NULL; }
like_pattern_opt(A) ::= LIKE NK_STRING(B).                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

table_name_cond(A) ::= table_name(B).                                             { A = createIdentifierValueNode(pCxt, &B); }

from_db_opt(A) ::= .                                                              { A = createDefaultDatabaseCondValue(pCxt); }
from_db_opt(A) ::= FROM db_name(B).                                               { A = createIdentifierValueNode(pCxt, &B); }

%type tag_list_opt                     { SNodeList* }
%destructor tag_list_opt               { nodesDestroyList($$); }
tag_list_opt(A) ::= .                                                             { A = NULL; }
tag_list_opt(A) ::= tag_item(B).                                                  { LIST_MK(A, B); }
tag_list_opt(A) ::= tag_list_opt(B) NK_COMMA tag_item(C).                         { A = B; LIST_ADD_NODE(A, C); }

tag_item(A) ::= TBNAME(B).                                                        { SET_TBNAME(A, B); }
tag_item(A) ::= QTAGS(B).                                                         { CREATE_FUNC(A, B, NULL_LIST); }
tag_item(A) ::= column_name(B).                                                   { A = createColumnNode(pCxt, NULL, &B); }
tag_item(A) ::= column_name(B) column_alias(C).                                   { SET_PROJECTION_ALIAS(A, B, C); }
tag_item(A) ::= column_name(B) AS column_alias(C).                                { SET_PROJECTION_ALIAS(A, B, C); }

%type db_kind_opt                      { EShowKind }
%destructor db_kind_opt                { }
db_kind_opt(A) ::= .                                                              { A = SHOW_KIND_ALL; }
db_kind_opt(A) ::= USER.                                                          { A = SHOW_KIND_DATABASES_USER; }
db_kind_opt(A) ::= SYSTEM.                                                        { A = SHOW_KIND_DATABASES_SYSTEM; }


/************************************************ tsma ********************************************************/
cmd ::= CREATE TSMA not_exists_opt(B) tsma_name(C)
  ON full_table_name(D) tsma_func_list(E)
  INTERVAL NK_LP duration_literal(F) NK_RP.                                       { CREATE_TSMA(B, C, D, E, F); }
cmd ::= CREATE RECURSIVE TSMA not_exists_opt(B) tsma_name(C)
  ON full_table_name(D) INTERVAL NK_LP duration_literal(E) NK_RP.                 { CREATE_TSMA(B, C, D, NULL_NODE, E); }
cmd ::= DROP TSMA exists_opt(B) full_tsma_name(C).                                { DROP_TSMA(B, C); }
cmd ::= SHOW db_name_cond_opt(B) TSMAS.                                           { SHOW_TSMA(B); }

full_tsma_name(A) ::= tsma_name(B).                                               { A = createRealTableNode(pCxt, NULL, &B, NULL); }
full_tsma_name(A) ::= db_name(B) NK_DOT tsma_name(C).                             { A = createRealTableNode(pCxt, &B, &C, NULL); }

%type tsma_func_list                   { SNode* }
%destructor tsma_func_list             { nodesDestroyNode($$); }
tsma_func_list(A) ::= FUNCTION NK_LP func_list(B) NK_RP.                          { CREATE_TSMA_FUNCS(A, B); }

/************************************************ create index ********************************************************/
cmd ::= CREATE SMA INDEX not_exists_opt(D)
  col_name(A) ON full_table_name(B) index_options(C).                      { ROOT_NODE() = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, D, A, B, NULL, C); }
cmd ::= CREATE INDEX not_exists_opt(D)
  col_name(A) ON full_table_name(B) NK_LP col_name_list(C) NK_RP.          { ROOT_NODE() = createCreateIndexStmt(pCxt, INDEX_TYPE_NORMAL, D, A, B, C, NULL); }
cmd ::= DROP INDEX exists_opt(B) full_index_name(A).                              { ROOT_NODE() = createDropIndexStmt(pCxt, B, A); }

full_index_name(A) ::= index_name(B).                                             { A = createRealTableNodeForIndexName(pCxt, NULL, &B); }
full_index_name(A) ::= db_name(B) NK_DOT index_name(C).                           { A = createRealTableNodeForIndexName(pCxt, &B, &C); }

index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL
  NK_LP duration_literal(C) NK_RP sliding_opt(D) sma_stream_opt(E).               { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), NULL, D, E); }
index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL
  NK_LP duration_literal(C) NK_COMMA duration_literal(D) NK_RP sliding_opt(E)
  sma_stream_opt(F).                                                              { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D), E, F); }

%type func_list                        { SNodeList* }
%destructor func_list                  { nodesDestroyList($$); }
func_list(A) ::= func(B).                                                         { LIST_MK(A, B); }
func_list(A) ::= func_list(B) NK_COMMA func(C).                                   { A = B; LIST_ADD_NODE(A, C); }

func(A) ::= sma_func_name(B) NK_LP expression_list(C) NK_RP.                      { A = createFunctionNode(pCxt, &B, C); }

%type sma_func_name                    { SToken }
%destructor sma_func_name              { }
sma_func_name(A) ::= function_name(B).                                            { A = B; }
sma_func_name(A) ::= COUNT(B).                                                    { A = B; }
sma_func_name(A) ::= FIRST(B).                                                    { A = B; }
sma_func_name(A) ::= LAST(B).                                                     { A = B; }
sma_func_name(A) ::= LAST_ROW(B).                                                 { A = B; }

sma_stream_opt(A) ::= .                                                           { A = createStreamOptions(pCxt); }
sma_stream_opt(A) ::= sma_stream_opt(B) WATERMARK duration_literal(C).            { ((SStreamOptions*)B)->pWatermark = releaseRawExprNode(pCxt, C); A = B; }
sma_stream_opt(A) ::= sma_stream_opt(B) MAX_DELAY duration_literal(C).            { ((SStreamOptions*)B)->pDelay = releaseRawExprNode(pCxt, C); A = B; }
sma_stream_opt(A) ::= sma_stream_opt(B) DELETE_MARK duration_literal(C).          { ((SStreamOptions*)B)->pDeleteMark = releaseRawExprNode(pCxt, C); A = B; }

/************************************************ create/drop topic ***************************************************/
%type with_meta                        { int32_t }
%destructor with_meta                  { }
with_meta(A) ::= AS.                                                              { A = 0; }
with_meta(A) ::= WITH META AS.                                                    { A = 1; }
with_meta(A) ::= ONLY META AS.                                                    { A = 2; }

cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) AS query_or_subquery(C).     { ROOT_NODE() = createCreateTopicStmtUseQuery(pCxt, A, &B, C); }
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) with_meta(D)
  DATABASE db_name(C).                                                            { ROOT_NODE() = createCreateTopicStmtUseDb(pCxt, A, &B, &C, D); }
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) with_meta(E)
  STABLE full_table_name(C) where_clause_opt(D).                                  { ROOT_NODE() = createCreateTopicStmtUseTable(pCxt, A, &B, C, E, D); }

cmd ::= DROP TOPIC exists_opt(A) topic_name(B).                                   { ROOT_NODE() = createDropTopicStmt(pCxt, A, &B); }
cmd ::= DROP CONSUMER GROUP exists_opt(A) cgroup_name(B) ON topic_name(C).        { ROOT_NODE() = createDropCGroupStmt(pCxt, A, &B, &C); }

/************************************************ desc/describe *******************************************************/
cmd ::= DESC full_table_name(A).                                                  { ROOT_NODE() = createDescribeStmt(pCxt, A); }
cmd ::= DESCRIBE full_table_name(A).                                              { ROOT_NODE() = createDescribeStmt(pCxt, A); }

/************************************************ reset query cache ***************************************************/
cmd ::= RESET QUERY CACHE.                                                        { ROOT_NODE() = createResetQueryCacheStmt(pCxt); }

/************************************************ explain *************************************************************/
cmd ::= EXPLAIN analyze_opt(A) explain_options(B) query_or_subquery(C).           { ROOT_NODE() = createExplainStmt(pCxt, A, B, C); }
cmd ::= EXPLAIN analyze_opt(A) explain_options(B) insert_query(C).                { ROOT_NODE() = createExplainStmt(pCxt, A, B, C); }

%type analyze_opt                      { bool }
%destructor analyze_opt                { }
analyze_opt(A) ::= .                                                              { A = false; }
analyze_opt(A) ::= ANALYZE.                                                       { A = true; }

explain_options(A) ::= .                                                          { A = createDefaultExplainOptions(pCxt); }
explain_options(A) ::= explain_options(B) VERBOSE NK_BOOL(C).                     { A = setExplainVerbose(pCxt, B, &C); }
explain_options(A) ::= explain_options(B) RATIO NK_FLOAT(C).                      { A = setExplainRatio(pCxt, B, &C); }

/************************************************ create/drop function ************************************************/
cmd ::= CREATE or_replace_opt(H) agg_func_opt(A) FUNCTION not_exists_opt(F)
  function_name(B) AS NK_STRING(C) OUTPUTTYPE type_name(D) bufsize_opt(E)
  language_opt(G).                                                                { ROOT_NODE() = createCreateFunctionStmt(pCxt, F, A, &B, &C, D, E, &G, H); }
cmd ::= DROP FUNCTION exists_opt(B) function_name(A).                             { ROOT_NODE() = createDropFunctionStmt(pCxt, B, &A); }

%type agg_func_opt                     { bool }
%destructor agg_func_opt               { }
agg_func_opt(A) ::= .                                                             { A = false; }
agg_func_opt(A) ::= AGGREGATE.                                                    { A = true; }

%type bufsize_opt                      { int32_t }
%destructor bufsize_opt                { }
bufsize_opt(A) ::= .                                                              { A = 0; }
bufsize_opt(A) ::= BUFSIZE NK_INTEGER(B).                                         { A = taosStr2Int32(B.z, NULL, 10); }

%type language_opt                     { SToken }
%destructor language_opt               { }
language_opt(A) ::= .                                                              { A = nil_token; }
language_opt(A) ::= LANGUAGE NK_STRING(B).                                         { A = B; }

%type or_replace_opt                   { bool }
%destructor or_replace_opt             { }
or_replace_opt(A) ::= .                                                            { A = false; }
or_replace_opt(A) ::= OR REPLACE.                                                  { A = true; }

/************************************************ create/drop view **************************************************/
cmd ::= CREATE or_replace_opt(A) VIEW full_view_name(B) AS(C) query_or_subquery(D).
                                                                                  { ROOT_NODE() = createCreateViewStmt(pCxt, A, B, &C, D); }
cmd ::= DROP VIEW exists_opt(A) full_view_name(B).                                { ROOT_NODE() = createDropViewStmt(pCxt, A, B); }

full_view_name(A) ::= view_name(B).                                               { A = createViewNode(pCxt, NULL, &B); }
full_view_name(A) ::= db_name(B) NK_DOT view_name(C).                             { A = createViewNode(pCxt, &B, &C); }

/************************************************ create/drop stream **************************************************/
cmd ::= CREATE STREAM not_exists_opt(E) stream_name(A) stream_options(B) INTO
  full_table_name(C) col_list_opt(H) tag_def_or_ref_opt(F) subtable_opt(G)
  AS query_or_subquery(D).                                                        { ROOT_NODE() = createCreateStreamStmt(pCxt, E, &A, C, B, F, G, D, H); }
cmd ::= DROP STREAM exists_opt(A) stream_name(B).                                 { ROOT_NODE() = createDropStreamStmt(pCxt, A, &B); }
cmd ::= PAUSE STREAM exists_opt(A) stream_name(B).                                { ROOT_NODE() = createPauseStreamStmt(pCxt, A, &B); }
cmd ::= RESUME STREAM exists_opt(A) ignore_opt(C) stream_name(B).                 { ROOT_NODE() = createResumeStreamStmt(pCxt, A, C, &B); }

%type col_list_opt                     { SNodeList* }
%destructor col_list_opt               { nodesDestroyList($$); }
col_list_opt(A) ::= .                                                             { A = NULL; }
col_list_opt(A) ::= NK_LP column_stream_def_list(B) NK_RP.                        { A = B; }

%type column_stream_def_list           { SNodeList* }
%destructor column_stream_def_list     { nodesDestroyList($$); }
column_stream_def_list(A) ::= column_stream_def(B).                               { LIST_MK(A, B); }
column_stream_def_list(A) ::= column_stream_def_list(B)
 NK_COMMA column_stream_def(C).                                                   { A = B; LIST_ADD_NODE(A, C); }

column_stream_def(A) ::= column_name(B) stream_col_options(C).                    { A = createColumnDefNode(pCxt, &B, createDataType(TSDB_DATA_TYPE_NULL), C); }
stream_col_options(A) ::= .                                                       { A = createDefaultColumnOptions(pCxt); }
stream_col_options(A) ::= stream_col_options(B) PRIMARY KEY.                      { A = setColumnOptionsPK(pCxt, B); }
//column_stream_def(A) ::= column_def(B).                                         { A = B; }

%type tag_def_or_ref_opt               { SNodeList* }
%destructor tag_def_or_ref_opt         { nodesDestroyList($$); }
tag_def_or_ref_opt(A) ::= .                                                       { A = NULL; }
tag_def_or_ref_opt(A) ::= tags_def(B).                                            { A = B; }
tag_def_or_ref_opt(A) ::= TAGS NK_LP column_stream_def_list(B) NK_RP.             { A = B; }

stream_options(A) ::= .                                                           { A = createStreamOptions(pCxt); }
stream_options(A) ::= stream_options(B) TRIGGER AT_ONCE(C).                       { A = setStreamOptions(pCxt, B, SOPT_TRIGGER_TYPE_SET, &C, NULL); }
stream_options(A) ::= stream_options(B) TRIGGER WINDOW_CLOSE(C).                  { A = setStreamOptions(pCxt, B, SOPT_TRIGGER_TYPE_SET, &C, NULL); }
stream_options(A) ::= stream_options(B) TRIGGER MAX_DELAY(C) duration_literal(D). { A = setStreamOptions(pCxt, B, SOPT_TRIGGER_TYPE_SET, &C, releaseRawExprNode(pCxt, D)); }
stream_options(A) ::= stream_options(B) WATERMARK duration_literal(C).            { A = setStreamOptions(pCxt, B, SOPT_WATERMARK_SET, NULL, releaseRawExprNode(pCxt, C)); }
stream_options(A) ::= stream_options(B) IGNORE EXPIRED NK_INTEGER(C).             { A = setStreamOptions(pCxt, B, SOPT_IGNORE_EXPIRED_SET, &C, NULL); }
stream_options(A) ::= stream_options(B) FILL_HISTORY NK_INTEGER(C).               { A = setStreamOptions(pCxt, B, SOPT_FILL_HISTORY_SET, &C, NULL); }
stream_options(A) ::= stream_options(B) DELETE_MARK duration_literal(C).          { A = setStreamOptions(pCxt, B, SOPT_DELETE_MARK_SET, NULL, releaseRawExprNode(pCxt, C)); }
stream_options(A) ::= stream_options(B) IGNORE UPDATE NK_INTEGER(C).              { A = setStreamOptions(pCxt, B, SOPT_IGNORE_UPDATE_SET, &C, NULL); }

subtable_opt(A) ::= .                                                             { A = NULL; }
subtable_opt(A) ::= SUBTABLE NK_LP expression(B) NK_RP.                           { A = releaseRawExprNode(pCxt, B); }

%type ignore_opt                       { bool }
%destructor ignore_opt                 { }
ignore_opt(A) ::= .                                                               { A = false; }
ignore_opt(A) ::= IGNORE UNTREATED.                                               { A = true; }

/************************************************ kill connection/query ***********************************************/
cmd ::= KILL CONNECTION NK_INTEGER(A).                                            { ROOT_NODE() = createKillStmt(pCxt, QUERY_NODE_KILL_CONNECTION_STMT, &A); }
cmd ::= KILL QUERY NK_STRING(A).                                                  { ROOT_NODE() = createKillQueryStmt(pCxt, &A); }
cmd ::= KILL TRANSACTION NK_INTEGER(A).                                           { ROOT_NODE() = createKillStmt(pCxt, QUERY_NODE_KILL_TRANSACTION_STMT, &A); }
cmd ::= KILL COMPACT NK_INTEGER(A).                                               { ROOT_NODE() = createKillStmt(pCxt, QUERY_NODE_KILL_COMPACT_STMT, &A); }

/************************************************ merge/redistribute/ vgroup ******************************************/
cmd ::= BALANCE VGROUP.                                                           { ROOT_NODE() = createBalanceVgroupStmt(pCxt); }
cmd ::= BALANCE VGROUP LEADER on_vgroup_id(A).                                    { ROOT_NODE() = createBalanceVgroupLeaderStmt(pCxt, &A); }
cmd ::= BALANCE VGROUP LEADER DATABASE db_name(A).                                { ROOT_NODE() = createBalanceVgroupLeaderDBNameStmt(pCxt, &A); }
cmd ::= MERGE VGROUP NK_INTEGER(A) NK_INTEGER(B).                                 { ROOT_NODE() = createMergeVgroupStmt(pCxt, &A, &B); }
cmd ::= REDISTRIBUTE VGROUP NK_INTEGER(A) dnode_list(B).                          { ROOT_NODE() = createRedistributeVgroupStmt(pCxt, &A, B); }
cmd ::= SPLIT VGROUP NK_INTEGER(A).                                               { ROOT_NODE() = createSplitVgroupStmt(pCxt, &A); }

%type on_vgroup_id                     { SToken }
%destructor on_vgroup_id               { }
on_vgroup_id(A) ::= .                                                             { A = nil_token; }
on_vgroup_id(A) ::= ON NK_INTEGER(B).                                             { A = B; }

%type dnode_list                       { SNodeList* }
%destructor dnode_list                 { nodesDestroyList($$); }
dnode_list(A) ::= DNODE NK_INTEGER(B).                                            { A = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B)); }
dnode_list(A) ::= dnode_list(B) DNODE NK_INTEGER(C).                              { A = addNodeToList(pCxt, B, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &C)); }

/************************************************ syncdb **************************************************************/
//cmd ::= SYNCDB db_name(A) REPLICA.                                                { ROOT_NODE() = createSyncdbStmt(pCxt, &A); }

/************************************************ syncdb **************************************************************/
cmd ::= DELETE FROM full_table_name(A) where_clause_opt(B).                       { ROOT_NODE() = createDeleteStmt(pCxt, A, B); }

/************************************************ select **************************************************************/
cmd ::= query_or_subquery(A).                                                     { ROOT_NODE() = A; }

/************************************************ insert **************************************************************/
cmd ::= insert_query(A).                                                          { ROOT_NODE() = A; }

insert_query(A) ::= insert_into target(B)
  NK_LP col_name_list(C) NK_RP query_or_subquery(D).                           { CREATE_INSERT_QUERY(A, B, C, D); }
insert_query(A) ::= insert_into target(B) query_or_subquery(C).                { CREATE_INSERT_QUERY(A, B, NULL, C); }

cmd ::= insert(A).                                                             { ROOT_NODE() = A; }

insert(A) ::= insert_into multiple_targets(B).                                 { INSERT_INTO_MULTIPLE_TARGETS(A, B); }
insert(A) ::= insert_into question(B) fields_clause(C) VALUES row(D).                    { INSERT_INTO_QUESTION(A, B, NULL, C, D); }
insert(A) ::= insert_into question(B) using_clause(C) fields_clause(D) VALUES row(E).    { INSERT_INTO_QUESTION(A, B, C, D, E); }

insert_into(A) ::= INSERT INTO.                                                { A = NULL; }

%type simple_target                 { simple_target_t *}
%destructor simple_target           { simple_target_destroy($$); }
simple_target(A) ::= target(B) fields_clause(C) VALUES rows(D).                { SIMPLE_TARGET_MK(A, B, NULL, C, D); }
simple_target(A) ::= target(B) using_clause(C) fields_clause(D) VALUES rows(E).{ SIMPLE_TARGET_MK(A, B, C, D, E); }

%type multiple_targets              { multiple_targets_t* }
%destructor multiple_targets        { multiple_targets_destroy($$); }
multiple_targets(A) ::= simple_target(B).                                      { MULTIPLE_TARGETS_MK(A, B); }
multiple_targets(A) ::= multiple_targets(B) simple_target(C).                  { A = B; MULTIPLE_TARGETS_APPEND(A, C); }

%type target                        { db_table_token_t }
%destructor target                  { }
target(A) ::= NK_ID(B).                                                        { CREATE_TARGET(A, nil_token, B); }
target(A) ::= NK_ID(B) NK_DOT NK_ID(C).                                        { CREATE_TARGET(A, B, C); }

%type question                      { value_t * }
%destructor question                { value_destroy($$); }
question(A) ::= NK_QUESTION(B).                                                { CREATE_QUESTION_TABLE(A, NULL, &B); }
question(A) ::= NK_ID(B) NK_DOT NK_QUESTION(C).                                { CREATE_QUESTION_TABLE(A, &B, &C); }

%type using_clause                  { using_clause_t* }
%destructor using_clause            { using_clause_destroy($$); }
using_clause(A) ::= USING supertable(B) fields_clause(C) TAGS NK_LP values(D) NK_RP.       { USING_CLAUSE_MK(A, B, C, D); }

%type supertable                    { db_table_token_t }
%destructor supertable              { }
supertable(A) ::= NK_ID(B).                                                    { CREATE_SUPERTABLE(A, nil_token, B); }
supertable(A) ::= NK_ID(B) NK_DOT NK_ID(C).                                    { CREATE_SUPERTABLE(A, B, C); }

%type fields_clause                    { SNodeList* }
%destructor fields_clause              { nodesDestroyList($$); }
fields_clause(A) ::= .                                                         { A = NULL; pCxt->nr_cols = 0; }
fields_clause(A) ::= NK_LP col_name_list(B) NK_RP.                             { A = B; pCxt->nr_cols = A->length; }

%type rows                             { row_t* }
%destructor rows                       { rows_destroy($$); }
rows(A) ::= row(B).                                                            { A = B; }
rows(A) ::= rows(B) row(C).                                                    { A = B; ROWS_APPEND(A, C); }

%type row                              { row_t* }
%destructor row                        { row_destroy($$); }
row(A) ::= row_lp values(B) NK_RP.                                             { ROW_MK(A, B); }

%type row_lp                           { int }
%destructor row_lp                     { }
row_lp ::= NK_LP.                                                              { CHK_ROW_BEGIN(); }

%type values                           { value_t* }
%destructor values                     { values_destroy($$); }
values(A) ::= value(B).                                                        { A = B; }
values(A) ::= values(B) NK_COMMA value(C).                                     { A = B; VALUES_APPEND(A, C); }

%type value                            { value_t* }
%destructor value                      { value_destroy($$); }
value(A) ::= NK_INTEGER(B).                                                    { A = parser_create_integer_value(pCxt, &B); }
value(A) ::= NK_FLOAT(B).                                                      { A = parser_create_flt_value(pCxt, &B); }
value(A) ::= NK_STRING(B).                                                     { A = parser_create_str_value(pCxt, &B); }
value(A) ::= NK_BOOL(B).                                                       { A = parser_create_bool_value(pCxt, &B); }
value(A) ::= TIMESTAMP(B) NK_STRING(C).                                        { A = parser_create_ts_value(pCxt, &C); SET_VALUE_TOKEN(A, B, C); }
value(A) ::= NK_VARIABLE(B).                                                   { A = parser_create_interval_value(pCxt, &B); }
value(A) ::= NULL(B).                                                          { A = parser_create_null_value(pCxt, &B); }
value(A) ::= NK_QUESTION(B).                                                   { CREATE_QUESTION(A, B); }

value(A) ::= value(B) NK_PLUS value(C).                                        { ADD(A, B, C); }
value(A) ::= value(B) NK_MINUS value(C).                                       { SUB(A, B, C); }
value(A) ::= value(B) NK_STAR value(C).                                        { MUL(A, B, C); }
value(A) ::= value(B) NK_SLASH value(C).                                       { DIV(A, B, C); }
value(A) ::= NK_MINUS(B) value(C).                                             { NEG(A, B, C); }
value(A) ::= NK_LP(B) value(C) NK_RP(D).                                       { A = C; SET_VALUE_TOKEN(A, B, D); }
value(A) ::= NK_ID(B) NK_LP values(C) NK_RP(D).                                { CAL(A, B, C); SET_VALUE_TOKEN(A, B, D); }
value(A) ::= NOW(B) NK_LP NK_RP(C).                                            { A = parser_create_now(pCxt, &B); SET_VALUE_TOKEN(A, B, C); }
value(A) ::= NOW(B).                                                           { A = parser_create_now(pCxt, &B); }

/************************************************ tags_literal *************************************************************/
tags_literal(A) ::= NK_INTEGER(B).                                                { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B, NULL); }
tags_literal(A) ::= NK_INTEGER(B) NK_PLUS duration_literal(C).                    {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_INTEGER(B) NK_MINUS duration_literal(C).                   {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_INTEGER(C).                                     {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_INTEGER NK_PLUS duration_literal(C).            {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_INTEGER NK_MINUS duration_literal(C).           {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_INTEGER(C).                                    {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_INTEGER NK_PLUS duration_literal(C).           {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_INTEGER NK_MINUS duration_literal(C).          {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_FLOAT(B).                                                  { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B, NULL); }
tags_literal(A) ::= NK_PLUS(B) NK_FLOAT(C).                                       {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_FLOAT(C).                                      {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t, NULL);
                                                                                  }

tags_literal(A) ::= NK_BIN(B).                                                    { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B, NULL); }
tags_literal(A) ::= NK_BIN(B) NK_PLUS duration_literal(C).                        {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_BIN(B) NK_MINUS duration_literal(C).                       {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_BIN(C).                                         {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_BIN NK_PLUS duration_literal(C).                {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_BIN NK_MINUS duration_literal(C).               {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_BIN(C).                                        {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_BIN NK_PLUS duration_literal(C).               {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_BIN NK_MINUS duration_literal(C).              {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_HEX(B).                                                    { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B, NULL); }
tags_literal(A) ::= NK_HEX(B) NK_PLUS duration_literal(C).                        {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_HEX(B) NK_MINUS duration_literal(C).                       {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_HEX(C).                                         {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_HEX NK_PLUS duration_literal(C).                {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_PLUS(B) NK_HEX NK_MINUS duration_literal(C).               {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_HEX(C).                                        {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_HEX NK_PLUS duration_literal(C).               {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_MINUS(B) NK_HEX NK_MINUS duration_literal(C).              {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }

tags_literal(A) ::= NK_STRING(B).                                                 { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B, NULL); }
tags_literal(A) ::= NK_STRING(B) NK_PLUS duration_literal(C).                     {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_STRING(B) NK_MINUS duration_literal(C).                    {
                                                                                    SToken l = B;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, C);
                                                                                  }
tags_literal(A) ::= NK_BOOL(B).                                                   { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &B, NULL); }
tags_literal(A) ::= NULL(B).                                                      { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_NULL, &B, NULL); }

tags_literal(A) ::= literal_func(B).                                              { A = createRawValueNode(pCxt, TSDB_DATA_TYPE_BINARY, NULL, B); }
tags_literal(A) ::= literal_func(B) NK_PLUS duration_literal(C).                  {
                                                                                    SToken l = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, B, C);
                                                                                  }
tags_literal(A) ::= literal_func(B) NK_MINUS duration_literal(C).                 {
                                                                                    SToken l = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, C);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    A = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, B, C);
                                                                                  }

%type tags_literal_list                { SNodeList* }
%destructor tags_literal_list          { nodesDestroyList($$); }
tags_literal_list(A) ::= tags_literal(B).                                         { LIST_MK(A, B); }
tags_literal_list(A) ::= tags_literal_list(B) NK_COMMA tags_literal(C).           { A = B; LIST_ADD_NODE(A, C); }

/************************************************ literal *************************************************************/
literal(A) ::= NK_INTEGER(B).                                                     { LITERAL_MK(A, B, TSDB_DATA_TYPE_UBIGINT); }
literal(A) ::= NK_FLOAT(B).                                                       { LITERAL_MK(A, B, TSDB_DATA_TYPE_DOUBLE); }
literal(A) ::= NK_STRING(B).                                                      { LITERAL_MK(A, B, TSDB_DATA_TYPE_BINARY); }
literal(A) ::= NK_BOOL(B).                                                        { LITERAL_MK(A, B, TSDB_DATA_TYPE_BOOL); }
literal(A) ::= TIMESTAMP(B) NK_STRING(C).                                         { LITERAL_MK_EXT(A, B, C, TSDB_DATA_TYPE_TIMESTAMP); }
literal(A) ::= duration_literal(B).                                               { A = B; }
literal(A) ::= NULL(B).                                                           { LITERAL_MK(A, B, TSDB_DATA_TYPE_NULL); }
literal(A) ::= NK_QUESTION(B).                                                    { LITERAL_QUESTION_MK(A, B); }

duration_literal(A) ::= NK_VARIABLE(B).                                           { DURA_LITERAL_MK(A, B); }

signed(A) ::= NK_INTEGER(B).                                                      { SIGNED_MK(A, B, TSDB_DATA_TYPE_UBIGINT); }
signed(A) ::= NK_PLUS NK_INTEGER(B).                                              { SIGNED_MK(A, B, TSDB_DATA_TYPE_UBIGINT); }
signed(A) ::= NK_MINUS(B) NK_INTEGER(C).                                          {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    SIGNED_MK(A, t, TSDB_DATA_TYPE_BIGINT);
                                                                                  }
signed(A) ::= NK_FLOAT(B).                                                        { SIGNED_MK(A, B, TSDB_DATA_TYPE_DOUBLE); }
signed(A) ::= NK_PLUS NK_FLOAT(B).                                                { SIGNED_MK(A, B, TSDB_DATA_TYPE_DOUBLE); }
signed(A) ::= NK_MINUS(B) NK_FLOAT(C).                                            {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    SIGNED_MK(A, t, TSDB_DATA_TYPE_DOUBLE);
                                                                                  }

signed_literal(A) ::= signed(B).                                                  { A = B; }
signed_literal(A) ::= NK_STRING(B).                                               { SIGNED_MK(A, B, TSDB_DATA_TYPE_BINARY); }
signed_literal(A) ::= NK_BOOL(B).                                                 { SIGNED_MK(A, B, TSDB_DATA_TYPE_BOOL); }
signed_literal(A) ::= TIMESTAMP NK_STRING(B).                                     { SIGNED_MK(A, B, TSDB_DATA_TYPE_TIMESTAMP); }
signed_literal(A) ::= duration_literal(B).                                        { A = releaseRawExprNode(pCxt, B); }
signed_literal(A) ::= NULL(B).                                                    { SIGNED_MK(A, B, TSDB_DATA_TYPE_NULL); }
signed_literal(A) ::= literal_func(B).                                            { A = releaseRawExprNode(pCxt, B); }
signed_literal(A) ::= NK_QUESTION(B).                                             { A = createPlaceholderValueNode(pCxt, &B); }

%type literal_list                     { SNodeList* }
%destructor literal_list               { nodesDestroyList($$); }
literal_list(A) ::= signed_literal(B).                                            { LIST_MK(A, B); }
literal_list(A) ::= literal_list(B) NK_COMMA signed_literal(C).                   { A = B; LIST_ADD_NODE(A, C); }

/************************************************ names and identifiers ***********************************************/
%type db_name                          { SToken }
%destructor db_name                    { }
db_name(A) ::= NK_ID(B).                                                          { A = B; }

%type table_name                       { SToken }
%destructor table_name                 { }
table_name(A) ::= NK_ID(B).                                                       { A = B; }

%type column_name                      { SToken }
%destructor column_name                { }
column_name(A) ::= NK_ID(B).                                                      { A = B; }

%type function_name                    { SToken }
%destructor function_name              { }
function_name(A) ::= NK_ID(B).                                                    { A = B; }

%type view_name                        { SToken }
%destructor view_name                  { }
view_name(A) ::= NK_ID(B).                                                        { A = B; }

%type table_alias                      { SToken }
%destructor table_alias                { }
table_alias(A) ::= NK_ID(B).                                                      { A = B; }

%type column_alias                     { SToken }
%destructor column_alias               { }
column_alias(A) ::= NK_ID(B).                                                     { A = B; }
column_alias(A) ::= NK_ALIAS(B).                                                  { A = B; }

%type user_name                        { SToken }
%destructor user_name                  { }
user_name(A) ::= NK_ID(B).                                                        { A = B; }

%type topic_name                       { SToken }
%destructor topic_name                 { }
topic_name(A) ::= NK_ID(B).                                                       { A = B; }

%type stream_name                      { SToken }
%destructor stream_name                { }
stream_name(A) ::= NK_ID(B).                                                      { A = B; }

%type cgroup_name                      { SToken }
%destructor cgroup_name                { }
cgroup_name(A) ::= NK_ID(B).                                                      { A = B; }

%type index_name                       { SToken }
%destructor index_name                 { }
index_name(A) ::= NK_ID(B).                                                       { A = B; }

%type tsma_name                        { SToken }
%destructor tsma_name                  { }
tsma_name(A) ::= NK_ID(B).                                                        { A = B; }

/************************************************ expression **********************************************************/
expr_or_subquery(A) ::= expression(B).                                            { A = B; }
//expr_or_subquery(A) ::= subquery(B).                                              { A = createTempTableNode(pCxt, releaseRawExprNode(pCxt, B), NULL); }

expression(A) ::= literal(B).                                                     { A = B; }
expression(A) ::= pseudo_column(B).                                               { A = B; setRawExprNodeIsPseudoColumn(pCxt, A, true); }
expression(A) ::= column_reference(B).                                            { A = B; }
expression(A) ::= function_expression(B).                                         { A = B; }
expression(A) ::= case_when_expression(B).                                        { A = B; }
expression(A) ::= NK_LP(B) expression(C) NK_RP(D).                                { A = createRawExprNodeExt(pCxt, &B, &D, releaseRawExprNode(pCxt, C)); }
expression(A) ::= NK_PLUS(B) expr_or_subquery(C).                                 {
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, releaseRawExprNode(pCxt, C));
                                                                                  }
expression(A) ::= NK_MINUS(B) expr_or_subquery(C).                                {
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, createOperatorNode(pCxt, OP_TYPE_MINUS, releaseRawExprNode(pCxt, C), NULL));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_PLUS expr_or_subquery(C).                {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_MINUS expr_or_subquery(C).               {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_STAR expr_or_subquery(C).                {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_SLASH expr_or_subquery(C).               {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_REM expr_or_subquery(C).                 {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_REM, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= column_reference(B) NK_ARROW NK_STRING(C).                      {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &C, createOperatorNode(pCxt, OP_TYPE_JSON_GET_VALUE, releaseRawExprNode(pCxt, B), createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_BITAND expr_or_subquery(C).              {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_BIT_AND, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
expression(A) ::= expr_or_subquery(B) NK_BITOR expr_or_subquery(C).               {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_BIT_OR, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }

%type expression_list                  { SNodeList* }
%destructor expression_list            { nodesDestroyList($$); }
expression_list(A) ::= expr_or_subquery(B).                                       { A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
expression_list(A) ::= expression_list(B) NK_COMMA expr_or_subquery(C).           { A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

column_reference(A) ::= column_name(B).                                           { A = createRawExprNode(pCxt, &B, createColumnNode(pCxt, NULL, &B)); }
column_reference(A) ::= table_name(B) NK_DOT column_name(C).                      { A = createRawExprNodeExt(pCxt, &B, &C, createColumnNode(pCxt, &B, &C)); }
column_reference(A) ::= NK_ALIAS(B).                                              { A = createRawExprNode(pCxt, &B, createColumnNode(pCxt, NULL, &B)); }
column_reference(A) ::= table_name(B) NK_DOT NK_ALIAS(C).                         { A = createRawExprNodeExt(pCxt, &B, &C, createColumnNode(pCxt, &B, &C)); }

pseudo_column(A) ::= ROWTS(B).                                                    { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= TBNAME(B).                                                   { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= table_name(B) NK_DOT TBNAME(C).                              { A = createRawExprNodeExt(pCxt, &B, &C, createFunctionNode(pCxt, &C, createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B)))); }
pseudo_column(A) ::= QSTART(B).                                                   { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= QEND(B).                                                     { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= QDURATION(B).                                                { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= WSTART(B).                                                   { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= WEND(B).                                                     { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= WDURATION(B).                                                { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= IROWTS(B).                                                   { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= ISFILLED(B).                                                 { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
pseudo_column(A) ::= QTAGS(B).                                                    { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }

function_expression(A) ::= function_name(B) NK_LP expression_list(C) NK_RP(D).    { A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, C)); }
function_expression(A) ::= star_func(B) NK_LP star_func_para_list(C) NK_RP(D).    { A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, C)); }
function_expression(A) ::=
  CAST(B) NK_LP expr_or_subquery(C) AS type_name(D) NK_RP(E).                     { A = createRawExprNodeExt(pCxt, &B, &E, createCastFunctionNode(pCxt, releaseRawExprNode(pCxt, C), D)); }
function_expression(A) ::=
  CAST(B) NK_LP expr_or_subquery(C) AS type_name_default_len(D) NK_RP(E).         { A = createRawExprNodeExt(pCxt, &B, &E, createCastFunctionNode(pCxt, releaseRawExprNode(pCxt, C), D)); }

function_expression(A) ::= literal_func(B).                                       { A = B; }

literal_func(A) ::= noarg_func(B) NK_LP NK_RP(C).                                 { A = createRawExprNodeExt(pCxt, &B, &C, createFunctionNode(pCxt, &B, NULL)); }
literal_func(A) ::= NOW(B).                                                       { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }
literal_func(A) ::= TODAY(B).                                                     { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }

%type noarg_func                       { SToken }
%destructor noarg_func                 { }
noarg_func(A) ::= NOW(B).                                                         { A = B; }
noarg_func(A) ::= TODAY(B).                                                       { A = B; }
noarg_func(A) ::= TIMEZONE(B).                                                    { A = B; }
noarg_func(A) ::= DATABASE(B).                                                    { A = B; }
noarg_func(A) ::= CLIENT_VERSION(B).                                              { A = B; }
noarg_func(A) ::= SERVER_VERSION(B).                                              { A = B; }
noarg_func(A) ::= SERVER_STATUS(B).                                               { A = B; }
noarg_func(A) ::= CURRENT_USER(B).                                                { A = B; }
noarg_func(A) ::= USER(B).                                                        { A = B; }

%type star_func                        { SToken }
%destructor star_func                  { }
star_func(A) ::= COUNT(B).                                                        { A = B; }
star_func(A) ::= FIRST(B).                                                        { A = B; }
star_func(A) ::= LAST(B).                                                         { A = B; }
star_func(A) ::= LAST_ROW(B).                                                     { A = B; }

%type star_func_para_list              { SNodeList* }
%destructor star_func_para_list        { nodesDestroyList($$); }
star_func_para_list(A) ::= NK_STAR(B).                                            { A = createNodeList(pCxt, createColumnNode(pCxt, NULL, &B)); }
star_func_para_list(A) ::= other_para_list(B).                                    { A = B; }

%type other_para_list                  { SNodeList* }
%destructor other_para_list            { nodesDestroyList($$); }
other_para_list(A) ::= star_func_para(B).                                         { LIST_MK(A, B); }
other_para_list(A) ::= other_para_list(B) NK_COMMA star_func_para(C).             { A = B; LIST_ADD_NODE(A, C); }

star_func_para(A) ::= expr_or_subquery(B).                                        { A = releaseRawExprNode(pCxt, B); }
star_func_para(A) ::= table_name(B) NK_DOT NK_STAR(C).                            { A = createColumnNode(pCxt, &B, &C); }

case_when_expression(A) ::=
  CASE(E) when_then_list(C) case_when_else_opt(D) END(F).                         { A = createRawExprNodeExt(pCxt, &E, &F, createCaseWhenNode(pCxt, NULL, C, D)); }
case_when_expression(A) ::=
  CASE(E) common_expression(B) when_then_list(C) case_when_else_opt(D) END(F).    { A = createRawExprNodeExt(pCxt, &E, &F, createCaseWhenNode(pCxt, releaseRawExprNode(pCxt, B), C, D)); }

%type when_then_list                   { SNodeList* }
%destructor when_then_list             { nodesDestroyList($$); }
when_then_list(A) ::= when_then_expr(B).                                          { LIST_MK(A, B); }
when_then_list(A) ::= when_then_list(B) when_then_expr(C).                        { A = B; LIST_ADD_NODE(A, C); }

when_then_expr(A) ::= WHEN common_expression(B) THEN common_expression(C).        { A = createWhenThenNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)); }

case_when_else_opt(A) ::= .                                                       { A = NULL; }
case_when_else_opt(A) ::= ELSE common_expression(B).                              { A = releaseRawExprNode(pCxt, B); }

/************************************************ predicate ***********************************************************/
predicate(A) ::= expr_or_subquery(B) compare_op(C) expr_or_subquery(D).           {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, C, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)));
                                                                                  }
//predicate(A) ::= expression(B) compare_op sub_type expression(B).
predicate(A) ::=
  expr_or_subquery(B) BETWEEN expr_or_subquery(C) AND expr_or_subquery(D).        {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D)));
                                                                                  }
predicate(A) ::=
  expr_or_subquery(B) NOT BETWEEN expr_or_subquery(C) AND expr_or_subquery(D).    {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D)));
                                                                                  }
predicate(A) ::= expr_or_subquery(B) IS NULL(C).                                  {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &C, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, B), NULL));
                                                                                  }
predicate(A) ::= expr_or_subquery(B) IS NOT NULL(C).                              {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &C, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, B), NULL));
                                                                                  }
predicate(A) ::= expr_or_subquery(B) in_op(C) in_predicate_value(D).              {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, C, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)));
                                                                                  }

%type compare_op                       { EOperatorType }
%destructor compare_op                 { }
compare_op(A) ::= NK_LT.                                                          { A = OP_TYPE_LOWER_THAN; }
compare_op(A) ::= NK_GT.                                                          { A = OP_TYPE_GREATER_THAN; }
compare_op(A) ::= NK_LE.                                                          { A = OP_TYPE_LOWER_EQUAL; }
compare_op(A) ::= NK_GE.                                                          { A = OP_TYPE_GREATER_EQUAL; }
compare_op(A) ::= NK_NE.                                                          { A = OP_TYPE_NOT_EQUAL; }
compare_op(A) ::= NK_EQ.                                                          { A = OP_TYPE_EQUAL; }
compare_op(A) ::= LIKE.                                                           { A = OP_TYPE_LIKE; }
compare_op(A) ::= NOT LIKE.                                                       { A = OP_TYPE_NOT_LIKE; }
compare_op(A) ::= MATCH.                                                          { A = OP_TYPE_MATCH; }
compare_op(A) ::= NMATCH.                                                         { A = OP_TYPE_NMATCH; }
compare_op(A) ::= CONTAINS.                                                       { A = OP_TYPE_JSON_CONTAINS; }

%type in_op                            { EOperatorType }
%destructor in_op                      { }
in_op(A) ::= IN.                                                                  { A = OP_TYPE_IN; }
in_op(A) ::= NOT IN.                                                              { A = OP_TYPE_NOT_IN; }

in_predicate_value(A) ::= NK_LP(C) literal_list(B) NK_RP(D).                      { A = createRawExprNodeExt(pCxt, &C, &D, createNodeListNode(pCxt, B)); }

/************************************************ boolean_value_expression ********************************************/
boolean_value_expression(A) ::= boolean_primary(B).                               { A = B; }
boolean_value_expression(A) ::= NOT(C) boolean_primary(B).                        {
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &C, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, B), NULL));
                                                                                  }
boolean_value_expression(A) ::=
  boolean_value_expression(B) OR boolean_value_expression(C).                     {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }
boolean_value_expression(A) ::=
  boolean_value_expression(B) AND boolean_value_expression(C).                    {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)));
                                                                                  }

boolean_primary(A) ::= predicate(B).                                              { A = B; }
boolean_primary(A) ::= NK_LP(C) boolean_value_expression(B) NK_RP(D).             { A = createRawExprNodeExt(pCxt, &C, &D, releaseRawExprNode(pCxt, B)); }

/************************************************ common_expression ********************************************/
common_expression(A) ::= expr_or_subquery(B).                                     { A = B; }
common_expression(A) ::= boolean_value_expression(B).                             { A = B; }

/************************************************ from_clause_opt *********************************************************/
from_clause_opt(A) ::= .                                                          { A = NULL; }
from_clause_opt(A) ::= FROM table_reference_list(B).                              { A = B; }

table_reference_list(A) ::= table_reference(B).                                   { A = B; }
table_reference_list(A) ::= table_reference_list(B) NK_COMMA table_reference(C).  { A = createJoinTableNode(pCxt, JOIN_TYPE_INNER, JOIN_STYPE_NONE, B, C, NULL); }

/************************************************ table_reference *****************************************************/
table_reference(A) ::= table_primary(B).                                          { A = B; }
table_reference(A) ::= joined_table(B).                                           { A = B; }

table_primary(A) ::= table_name(B) alias_opt(C).                                  { A = createRealTableNode(pCxt, NULL, &B, &C); }
table_primary(A) ::= db_name(B) NK_DOT table_name(C) alias_opt(D).                { A = createRealTableNode(pCxt, &B, &C, &D); }
table_primary(A) ::= subquery(B) alias_opt(C).                                    { A = createTempTableNode(pCxt, releaseRawExprNode(pCxt, B), &C); }
table_primary(A) ::= parenthesized_joined_table(B).                               { A = B; }

%type alias_opt                        { SToken }
%destructor alias_opt                  { }
alias_opt(A) ::= .                                                                { A = nil_token;  }
alias_opt(A) ::= table_alias(B).                                                  { A = B; }
alias_opt(A) ::= AS table_alias(B).                                               { A = B; }

parenthesized_joined_table(A) ::= NK_LP joined_table(B) NK_RP.                    { A = B; }
parenthesized_joined_table(A) ::= NK_LP parenthesized_joined_table(B) NK_RP.      { A = B; }

/************************************************ joined_table ********************************************************/
joined_table(A) ::=
  table_reference(B) join_type(C) join_subtype(D) JOIN table_primary(E) join_on_clause_opt(F)
  window_offset_clause_opt(G) jlimit_clause_opt(H).                               {
                                                                                    A = createJoinTableNode(pCxt, C, D, B, E, F);
                                                                                    A = addWindowOffsetClause(pCxt, A, G);
                                                                                    A = addJLimitClause(pCxt, A, H);
                                                                                  }

%type join_type                        { EJoinType }
%destructor join_type                  { }
join_type(A) ::= .                                                                { A = JOIN_TYPE_INNER; }
join_type(A) ::= INNER.                                                           { A = JOIN_TYPE_INNER; }
join_type(A) ::= LEFT.                                                            { A = JOIN_TYPE_LEFT; }
join_type(A) ::= RIGHT.                                                           { A = JOIN_TYPE_RIGHT; }
join_type(A) ::= FULL.                                                            { A = JOIN_TYPE_FULL; }

%type join_subtype                     { EJoinSubType }
%destructor join_subtype               { }
join_subtype(A) ::= .                                                             { A = JOIN_STYPE_NONE; }
join_subtype(A) ::= OUTER.                                                        { A = JOIN_STYPE_OUTER; }
join_subtype(A) ::= SEMI.                                                         { A = JOIN_STYPE_SEMI; }
join_subtype(A) ::= ANTI.                                                         { A = JOIN_STYPE_ANTI; }
join_subtype(A) ::= ASOF.                                                         { A = JOIN_STYPE_ASOF; }
join_subtype(A) ::= WINDOW.                                                       { A = JOIN_STYPE_WIN; }

join_on_clause_opt(A) ::= .                                                       { A = NULL; }
join_on_clause_opt(A) ::= ON search_condition(B).                                 { A = B; }

window_offset_clause_opt(A) ::= .                                                 { A = NULL; }
window_offset_clause_opt(A) ::= WINDOW_OFFSET NK_LP window_offset_literal(B)
  NK_COMMA window_offset_literal(C) NK_RP.                                        { A = createWindowOffsetNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)); }

window_offset_literal(A) ::= NK_VARIABLE(B).                                      { A = createRawExprNode(pCxt, &B, createTimeOffsetValueNode(pCxt, &B)); }
window_offset_literal(A) ::= NK_MINUS(B) NK_VARIABLE(C).                          {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createRawExprNode(pCxt, &t, createTimeOffsetValueNode(pCxt, &t));
                                                                                  }

jlimit_clause_opt(A) ::= .                                                        { A = NULL; }
jlimit_clause_opt(A) ::= JLIMIT NK_INTEGER(B).                                    { A = createLimitNode(pCxt, &B, NULL); }

/************************************************ query_specification *************************************************/
query_specification(A) ::=
  SELECT hint_list(M) set_quantifier_opt(B) tag_mode_opt(N) select_list(C) from_clause_opt(D)
  where_clause_opt(E) partition_by_clause_opt(F) range_opt(J) every_opt(K)
  fill_opt(L) twindow_clause_opt(G) group_by_clause_opt(H) having_clause_opt(I).  {
                                                                                    A = createSelectStmt(pCxt, B, C, D, M);
                                                                                    A = setSelectStmtTagMode(pCxt, A, N);
                                                                                    A = addWhereClause(pCxt, A, E);
                                                                                    A = addPartitionByClause(pCxt, A, F);
                                                                                    A = addWindowClauseClause(pCxt, A, G);
                                                                                    A = addGroupByClause(pCxt, A, H);
                                                                                    A = addHavingClause(pCxt, A, I);
                                                                                    A = addRangeClause(pCxt, A, J);
                                                                                    A = addEveryClause(pCxt, A, K);
                                                                                    A = addFillClause(pCxt, A, L);
                                                                                  }

%type hint_list                        { SNodeList* }
%destructor hint_list                  { nodesDestroyList($$); }
hint_list(A) ::= .                                                                { A = createHintNodeList(pCxt, NULL); }
hint_list(A) ::= NK_HINT(B).                                                      { A = createHintNodeList(pCxt, &B); }

%type tag_mode_opt                     { bool }
%destructor tag_mode_opt               { }
tag_mode_opt(A) ::= .                                                             { A = false; }
tag_mode_opt(A) ::= TAGS.                                                         { A = true; }

%type set_quantifier_opt               { bool }
%destructor set_quantifier_opt         { }
set_quantifier_opt(A) ::= .                                                       { A = false; }
set_quantifier_opt(A) ::= DISTINCT.                                               { A = true; }
set_quantifier_opt(A) ::= ALL.                                                    { A = false; }

%type select_list                      { SNodeList* }
%destructor select_list                { nodesDestroyList($$); }
select_list(A) ::= select_item(B).                                                { LIST_MK(A, B); }
select_list(A) ::= select_list(B) NK_COMMA select_item(C).                        { A = B; LIST_ADD_NODE(A, C); }

select_item(A) ::= NK_STAR(B).                                                    { A = createColumnNode(pCxt, NULL, &B); }
select_item(A) ::= common_expression(B).                                          { A = releaseRawExprNode(pCxt, B); }
select_item(A) ::= common_expression(B) column_alias(C).                          { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= common_expression(B) AS column_alias(C).                       { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                               { A = createColumnNode(pCxt, &B, &C); }

where_clause_opt(A) ::= .                                                         { A = NULL; }
where_clause_opt(A) ::= WHERE search_condition(B).                                { A = B; }

%type partition_by_clause_opt          { SNodeList* }
%destructor partition_by_clause_opt    { nodesDestroyList($$); }
partition_by_clause_opt(A) ::= .                                                  { A = NULL; }
partition_by_clause_opt(A) ::= PARTITION BY partition_list(B).                    { A = B; }

%type partition_list                   { SNodeList* }
%destructor partition_list             { nodesDestroyList($$); }
partition_list(A) ::= partition_item(B).                                          { LIST_MK(A, B); }
partition_list(A) ::= partition_list(B) NK_COMMA partition_item(C).               { A = B; LIST_ADD_NODE(A, C); }

partition_item(A) ::= expr_or_subquery(B).                                        { A = releaseRawExprNode(pCxt, B); }
partition_item(A) ::= expr_or_subquery(B) column_alias(C).                        { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
partition_item(A) ::= expr_or_subquery(B) AS column_alias(C).                     { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }

twindow_clause_opt(A) ::= .                                                       { A = NULL; }
twindow_clause_opt(A) ::= SESSION NK_LP column_reference(B) NK_COMMA
  interval_sliding_duration_literal(C) NK_RP.                                     { A = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)); }
twindow_clause_opt(A) ::= STATE_WINDOW NK_LP expr_or_subquery(B) NK_RP.           { A = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, B)); }
twindow_clause_opt(A) ::= INTERVAL NK_LP interval_sliding_duration_literal(B)
  NK_RP sliding_opt(C) fill_opt(D).                                               { A = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, B), NULL, C, D); }
twindow_clause_opt(A) ::=
  INTERVAL NK_LP interval_sliding_duration_literal(B) NK_COMMA
  interval_sliding_duration_literal(C) NK_RP
  sliding_opt(D) fill_opt(E).                                                     { A = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), D, E); }
twindow_clause_opt(A) ::=
  EVENT_WINDOW START WITH search_condition(B) END WITH search_condition(C).       { A = createEventWindowNode(pCxt, B, C); }
twindow_clause_opt(A) ::=
  COUNT_WINDOW NK_LP NK_INTEGER(B) NK_RP.                                         { A = createCountWindowNode(pCxt, &B, &B); }
twindow_clause_opt(A) ::=
  COUNT_WINDOW NK_LP NK_INTEGER(B) NK_COMMA NK_INTEGER(C) NK_RP.                  { A = createCountWindowNode(pCxt, &B, &C); }

sliding_opt(A) ::= .                                                              { A = NULL; }
sliding_opt(A) ::= SLIDING NK_LP interval_sliding_duration_literal(B) NK_RP.      { A = releaseRawExprNode(pCxt, B); }

interval_sliding_duration_literal(A) ::= NK_VARIABLE(B).                          { A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }
interval_sliding_duration_literal(A) ::= NK_STRING(B).                            { A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }
interval_sliding_duration_literal(A) ::= NK_INTEGER(B).                           { A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }

fill_opt(A) ::= .                                                                 { A = NULL; }
fill_opt(A) ::= FILL NK_LP fill_mode(B) NK_RP.                                    { A = createFillNode(pCxt, B, NULL); }
fill_opt(A) ::= FILL NK_LP VALUE NK_COMMA expression_list(B) NK_RP.               { A = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, B)); }
fill_opt(A) ::= FILL NK_LP VALUE_F NK_COMMA expression_list(B) NK_RP.             { A = createFillNode(pCxt, FILL_MODE_VALUE_F, createNodeListNode(pCxt, B)); }

%type fill_mode                        { EFillMode }
%destructor fill_mode                  { }
fill_mode(A) ::= NONE.                                                            { A = FILL_MODE_NONE; }
fill_mode(A) ::= PREV.                                                            { A = FILL_MODE_PREV; }
fill_mode(A) ::= NULL.                                                            { A = FILL_MODE_NULL; }
fill_mode(A) ::= NULL_F.                                                          { A = FILL_MODE_NULL_F; }
fill_mode(A) ::= LINEAR.                                                          { A = FILL_MODE_LINEAR; }
fill_mode(A) ::= NEXT.                                                            { A = FILL_MODE_NEXT; }

%type group_by_clause_opt              { SNodeList* }
%destructor group_by_clause_opt        { nodesDestroyList($$); }
group_by_clause_opt(A) ::= .                                                      { A = NULL; }
group_by_clause_opt(A) ::= GROUP BY group_by_list(B).                             { A = B; }

%type group_by_list                    { SNodeList* }
%destructor group_by_list              { nodesDestroyList($$); }
group_by_list(A) ::= expr_or_subquery(B).                                         { A = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, B))); }
group_by_list(A) ::= group_by_list(B) NK_COMMA expr_or_subquery(C).               { A = addNodeToList(pCxt, B, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, C))); }

having_clause_opt(A) ::= .                                                        { A = NULL; }
having_clause_opt(A) ::= HAVING search_condition(B).                              { A = B; }

range_opt(A) ::= .                                                                { A = NULL; }
range_opt(A) ::=
  RANGE NK_LP expr_or_subquery(B) NK_COMMA expr_or_subquery(C) NK_RP.             { A = createInterpTimeRange(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)); }
range_opt(A) ::=
  RANGE NK_LP expr_or_subquery(B) NK_RP.                                          { A = createInterpTimePoint(pCxt, releaseRawExprNode(pCxt, B)); }

every_opt(A) ::= .                                                                { A = NULL; }
every_opt(A) ::= EVERY NK_LP duration_literal(B) NK_RP.                           { A = releaseRawExprNode(pCxt, B); }

/************************************************ query_expression ****************************************************/
query_expression(A) ::= query_simple(B)
  order_by_clause_opt(C) slimit_clause_opt(D) limit_clause_opt(E).                {
                                                                                    A = addOrderByClause(pCxt, B, C);
                                                                                    A = addSlimitClause(pCxt, A, D);
                                                                                    A = addLimitClause(pCxt, A, E);
                                                                                  }

query_simple(A) ::= query_specification(B).                                       { A = B; }
query_simple(A) ::= union_query_expression(B).                                    { A = B; }

union_query_expression(A) ::=
  query_simple_or_subquery(B) UNION ALL query_simple_or_subquery(C).              { A = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, B, C); }
union_query_expression(A) ::=
  query_simple_or_subquery(B) UNION query_simple_or_subquery(C).                  { A = createSetOperator(pCxt, SET_OP_TYPE_UNION, B, C); }

query_simple_or_subquery(A) ::= query_simple(B).                                  { A = B; }
query_simple_or_subquery(A) ::= subquery(B).                                      { A = releaseRawExprNode(pCxt, B); }

query_or_subquery(A) ::= query_expression(B).                                     { A = B; }
query_or_subquery(A) ::= subquery(B).                                             { A = releaseRawExprNode(pCxt, B); }

%type order_by_clause_opt              { SNodeList* }
%destructor order_by_clause_opt        { nodesDestroyList($$); }
order_by_clause_opt(A) ::= .                                                      { A = NULL; }
order_by_clause_opt(A) ::= ORDER BY sort_specification_list(B).                   { A = B; }

slimit_clause_opt(A) ::= .                                                        { A = NULL; }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(B).                                    { A = createLimitNode(pCxt, &B, NULL); }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(B) SOFFSET NK_INTEGER(C).              { A = createLimitNode(pCxt, &B, &C); }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).             { A = createLimitNode(pCxt, &B, &C); }

limit_clause_opt(A) ::= .                                                         { A = NULL; }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(B).                                      { A = createLimitNode(pCxt, &B, NULL); }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(B) OFFSET NK_INTEGER(C).                 { A = createLimitNode(pCxt, &B, &C); }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).               { A = createLimitNode(pCxt, &B, &C); }

/************************************************ subquery ************************************************************/
subquery(A) ::= NK_LP(B) query_expression(C) NK_RP(D).                            { A = createRawExprNodeExt(pCxt, &B, &D, C); }
subquery(A) ::= NK_LP(B) subquery(C) NK_RP(D).                                    { A = createRawExprNodeExt(pCxt, &B, &D, releaseRawExprNode(pCxt, C)); }

/************************************************ search_condition ****************************************************/
search_condition(A) ::= common_expression(B).                                     { A = releaseRawExprNode(pCxt, B); }

/************************************************ sort_specification_list *********************************************/
%type sort_specification_list          { SNodeList* }
%destructor sort_specification_list    { nodesDestroyList($$); }
sort_specification_list(A) ::= sort_specification(B).                             { LIST_MK(A, B); }
sort_specification_list(A) ::=
  sort_specification_list(B) NK_COMMA sort_specification(C).                      { A = B; LIST_ADD_NODE(A, C); }

sort_specification(A) ::=
  expr_or_subquery(B) ordering_specification_opt(C) null_ordering_opt(D).         { A = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, B), C, D); }

%type ordering_specification_opt       { EOrder }
%destructor ordering_specification_opt { }
ordering_specification_opt(A) ::= .                                               { A = ORDER_ASC; }
ordering_specification_opt(A) ::= ASC.                                            { A = ORDER_ASC; }
ordering_specification_opt(A) ::= DESC.                                           { A = ORDER_DESC; }

%type null_ordering_opt                { ENullOrder }
%destructor null_ordering_opt          { }
null_ordering_opt(A) ::= .                                                        { A = NULL_ORDER_DEFAULT; }
null_ordering_opt(A) ::= NULLS FIRST.                                             { A = NULL_ORDER_FIRST; }
null_ordering_opt(A) ::= NULLS LAST.                                              { A = NULL_ORDER_LAST; }

%fallback ABORT AFTER ATTACH BEFORE BEGIN BITAND BITNOT BITOR BLOCKS CHANGE COMMA CONCAT CONFLICT COPY DEFERRED DELIMITERS DETACH DIVIDE DOT EACH END FAIL
  FILE FOR GLOB ID IMMEDIATE IMPORT INITIALLY INSTEAD ISNULL KEY MODULES NK_BITNOT NK_SEMI NOTNULL OF PLUS PRIVILEGE RAISE RESTRICT ROW SEMI STAR STATEMENT
  STRICT STRING TIMES VALUES VARIABLE VIEW WAL.

column_options(A) ::= .                                                           { A = createDefaultColumnOptions(pCxt); }
column_options(A) ::= column_options(B) PRIMARY KEY.                              { A = setColumnOptionsPK(pCxt, B); }
column_options(A) ::= column_options(B) NK_ID(C) NK_STRING(D).                    { A = setColumnOptions(pCxt, B, &C, &D); }

%code {

static void* this_malloc(size_t sz) {
  return taosMemoryMalloc(sz);
}

static void this_free(void *p) {
  if (!p) return;
  taosMemoryFree(p);
}


int32_t buildQueryAfterParse(SQuery** pQuery, SNode* pRootNode, int16_t placeholderNo, SArray** pPlaceholderValues) {
  *pQuery = (SQuery*)nodesMakeNode(QUERY_NODE_QUERY);
  if (NULL == *pQuery) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pQuery)->pRoot = pRootNode;
  (*pQuery)->placeholderNum = placeholderNo;
  TSWAP((*pQuery)->pPlaceholderValues, *pPlaceholderValues);
  (*pQuery)->execStage = QUERY_EXEC_STAGE_ANALYSE;

  return TSDB_CODE_SUCCESS;
}

static void do_parse(void *pParser, SAstCreateContext *cxt) {
  int32_t i = 0;
  while (1) {
    SToken t0 = {0};
    if (cxt->pQueryCxt->pSql[i] == 0) {
      taosParse(pParser, 0, t0, cxt);
      break;
    }
    t0.n = tGetToken((char*)&cxt->pQueryCxt->pSql[i], &t0.type);
    t0.z = (char*)(cxt->pQueryCxt->pSql + i);
    i += t0.n;

    switch (t0.type) {
      case TK_NK_SPACE:
      case TK_NK_COMMENT: {
        break;
      }
      case TK_NK_SEMI: {
        taosParse(pParser, 0, t0, cxt);
        return;
      }
      case TK_NK_ILLEGAL: {
        snprintf(cxt->pQueryCxt->pMsg, cxt->pQueryCxt->msgLen, "unrecognized token: \"%s\"", t0.z);
        cxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
        return;
      }
      case TK_NK_OCT: {
        snprintf(cxt->pQueryCxt->pMsg, cxt->pQueryCxt->msgLen, "unsupported token: \"%s\"", t0.z);
        cxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
        return;
      }
      default:
        // taosParseTrace(stderr, "");
        taosParse(pParser, t0.type, t0, cxt);
        if (TSDB_CODE_SUCCESS != cxt->errCode) {
          return;
        }
    }
  }
}

void parse_result_reset(parse_result_t *result) {
  if (!result) return;
  nodesDestroyNode(result->pRootNode);
  result->pRootNode = NULL;
  taosArrayDestroy(result->pPlaceholderValues);
  result->pPlaceholderValues    = NULL;
  result->placeholderNo         = 0;
  memset(&result->meta, 0, sizeof(result->meta));
  result->nr_sql                = 0;
  if (result->sql) result->sql[0] = '\0';
}

void parse_result_release(parse_result_t *result) {
  if (!result) return;
  parse_result_reset(result);
  taosMemoryFreeClear(result->sql);
  result->cap_sql = 0;
}

static int32_t do_parse_pure(SParseContext* pParseCxt, parse_result_t *result) {
  SAstCreateContext cxt = {0};
  initAstCreateContext(pParseCxt, &cxt);
  cxt.parse_result = result;

  void*   pParser = taosParseAlloc(this_malloc);
  // XE("[%.*s]", (int)pParseCxt->sqlLen, pParseCxt->pSql);
  do_parse(pParser, &cxt);
  taosParseFree(pParser, this_free);

  if (!cxt.errCode) {
    parse_result_post_fill_meta(result);
  }

  return cxt.errCode;
}

int32_t parse(SParseContext* pParseCxt, SQuery** pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t code1 = TSDB_CODE_SUCCESS;

  parse_result_t result = {0};
  code = do_parse_pure(pParseCxt, &result);

  if (TSDB_CODE_SUCCESS == code) {
    code1 = buildQueryAfterParse(pQuery, result.pRootNode, result.placeholderNo, &result.pPlaceholderValues);
    if (code1 == TSDB_CODE_SUCCESS) {
      result.pRootNode       = NULL;
      result.placeholderNo   = 0;
    }
  }

  taosArrayDestroy(result.pPlaceholderValues);
  parse_result_release(&result);

  return (TSDB_CODE_SUCCESS != code1) ? code1 : code;
}

int32_t qParsePure(TAOS_STMT *stmt, const char *sql, size_t len, parse_result_t *result) {
  SParseContext ctx = {0};
  ctx.db         = result->current_db;
  ctx.pSql       = sql;
  ctx.sqlLen     = len;
  ctx.pMsg       = result->err_msg;
  ctx.msgLen     = sizeof(result->err_msg);
  ctx.stmt       = stmt;
  ctx.prepare    = 1;

  int32_t code = do_parse_pure(&ctx, result);

  return code;
}

}

