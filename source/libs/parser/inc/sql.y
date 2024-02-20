//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y

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

#include "functionMgt.h"
#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"

#define YYSTACKDEPTH 0
}

%syntax_error {
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    if(TOKEN.z) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
  } else if (TSDB_CODE_PAR_DB_NOT_SPECIFIED == pCxt->errCode && TK_NK_FLOAT == TOKEN.type) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
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
cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options.                      { pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
cmd ::= ALTER ACCOUNT NK_ID alter_account_options.                                { pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }

%type account_options                                                             { int32_t }
%destructor account_options                                                       { }
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

%type alter_account_options                                                       { int32_t }
%destructor alter_account_options                                                 { }
alter_account_options ::= alter_account_option.                                   { }
alter_account_options ::= alter_account_options alter_account_option.             { }

%type alter_account_option                                                        { int32_t }
%destructor alter_account_option                                                  { }
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

%type ip_range_list                                                               { SNodeList* }
%destructor ip_range_list                                                         { nodesDestroyList($$); }
ip_range_list(A) ::= NK_STRING(B).                                                { A = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B)); }
ip_range_list(A) ::= ip_range_list(B) NK_COMMA NK_STRING(C).                      { A = addNodeToList(pCxt, B, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &C)); }

%type white_list                                                                  { SNodeList* }
%destructor white_list                                                            { nodesDestroyList($$); }
white_list(A) ::= HOST ip_range_list(B).                                          { A = B; }

%type white_list_opt                                                              { SNodeList* }
%destructor white_list_opt                                                        { nodesDestroyList($$); }
white_list_opt(A) ::= .                                                           { A = NULL; }
white_list_opt(A) ::= white_list(B).                                              { A = B; }

/************************************************ create/alter/drop user **********************************************/
cmd ::= CREATE USER user_name(A) PASS NK_STRING(B) sysinfo_opt(C)
                      white_list_opt(D).                                          {
                                                                                    pCxt->pRootNode = createCreateUserStmt(pCxt, &A, &B, C);
                                                                                    pCxt->pRootNode = addCreateUserStmtWhiteList(pCxt, pCxt->pRootNode, D);
                                                                                  }
cmd ::= ALTER USER user_name(A) PASS NK_STRING(B).                                { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_PASSWD, &B); }
cmd ::= ALTER USER user_name(A) ENABLE NK_INTEGER(B).                             { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_ENABLE, &B); }
cmd ::= ALTER USER user_name(A) SYSINFO NK_INTEGER(B).                            { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_SYSINFO, &B); }
cmd ::= ALTER USER user_name(A) ADD white_list(B).                                { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_ADD_WHITE_LIST, B); }
cmd ::= ALTER USER user_name(A) DROP white_list(B).                               { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_DROP_WHITE_LIST, B); }
cmd ::= DROP USER user_name(A).                                                   { pCxt->pRootNode = createDropUserStmt(pCxt, &A); }

%type sysinfo_opt                                                                 { int8_t }
%destructor sysinfo_opt                                                           { }
sysinfo_opt(A) ::= .                                                              { A = 1; }
sysinfo_opt(A) ::= SYSINFO NK_INTEGER(B).                                         { A = taosStr2Int8(B.z, NULL, 10); }

/************************************************ grant/revoke ********************************************************/
cmd ::= GRANT privileges(A) ON priv_level(B) with_opt(D) TO user_name(C).         { pCxt->pRootNode = createGrantStmt(pCxt, A, &B, &C, D); }
cmd ::= REVOKE privileges(A) ON priv_level(B) with_opt(D) FROM user_name(C).      { pCxt->pRootNode = createRevokeStmt(pCxt, A, &B, &C, D); }

%type privileges                                                                  { int64_t }
%destructor privileges                                                            { }
privileges(A) ::= ALL.                                                            { A = PRIVILEGE_TYPE_ALL; }
privileges(A) ::= priv_type_list(B).                                              { A = B; }
privileges(A) ::= SUBSCRIBE.                                                      { A = PRIVILEGE_TYPE_SUBSCRIBE; }

%type priv_type_list                                                              { int64_t }
%destructor priv_type_list                                                        { }
priv_type_list(A) ::= priv_type(B).                                               { A = B; }
priv_type_list(A) ::= priv_type_list(B) NK_COMMA priv_type(C).                    { A = B | C; }

%type priv_type                                                                   { int64_t }
%destructor priv_type                                                             { }
priv_type(A) ::= READ.                                                            { A = PRIVILEGE_TYPE_READ; }
priv_type(A) ::= WRITE.                                                           { A = PRIVILEGE_TYPE_WRITE; }
priv_type(A) ::= ALTER.                                                           { A = PRIVILEGE_TYPE_ALTER; }

%type priv_level                                                                  { STokenPair }
%destructor priv_level                                                            { }
priv_level(A) ::= NK_STAR(B) NK_DOT NK_STAR(C).                                   { A.first = B; A.second = C; }
priv_level(A) ::= db_name(B) NK_DOT NK_STAR(C).                                   { A.first = B; A.second = C; }
priv_level(A) ::= db_name(B) NK_DOT table_name(C).                                { A.first = B; A.second = C; }
priv_level(A) ::= topic_name(B).                                                  { A.first = B; A.second = nil_token; }

with_opt(A) ::= .                                                                 { A = NULL; }
with_opt(A) ::= WITH search_condition(B).                                         { A = B; }

/************************************************ create/drop/alter/restore dnode *********************************************/
cmd ::= CREATE DNODE dnode_endpoint(A).                                           { pCxt->pRootNode = createCreateDnodeStmt(pCxt, &A, NULL); }
cmd ::= CREATE DNODE dnode_endpoint(A) PORT NK_INTEGER(B).                        { pCxt->pRootNode = createCreateDnodeStmt(pCxt, &A, &B); }
cmd ::= DROP DNODE NK_INTEGER(A) force_opt(B).                                    { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A, B, false); }
cmd ::= DROP DNODE dnode_endpoint(A) force_opt(B).                                { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A, B, false); }
cmd ::= DROP DNODE NK_INTEGER(A) unsafe_opt(B).                                   { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A, false, B); }
cmd ::= DROP DNODE dnode_endpoint(A) unsafe_opt(B).                               { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A, false, B); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B).                                   { pCxt->pRootNode = createAlterDnodeStmt(pCxt, &A, &B, NULL); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B) NK_STRING(C).                      { pCxt->pRootNode = createAlterDnodeStmt(pCxt, &A, &B, &C); }
cmd ::= ALTER ALL DNODES NK_STRING(A).                                            { pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &A, NULL); }
cmd ::= ALTER ALL DNODES NK_STRING(A) NK_STRING(B).                               { pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &A, &B); }
cmd ::= RESTORE DNODE NK_INTEGER(A).                                              { pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_DNODE_STMT, &A); }

%type dnode_endpoint                                                              { SToken }
%destructor dnode_endpoint                                                        { }
dnode_endpoint(A) ::= NK_STRING(B).                                               { A = B; }
dnode_endpoint(A) ::= NK_ID(B).                                                   { A = B; }
dnode_endpoint(A) ::= NK_IPTOKEN(B).                                              { A = B; }

%type force_opt                                                                   { bool }
%destructor force_opt                                                             { }
force_opt(A) ::= .                                                                { A = false; }
force_opt(A) ::= FORCE.                                                           { A = true; }

%type unsafe_opt                                                                  { bool }
%destructor unsafe_opt                                                            { }
unsafe_opt(A) ::= UNSAFE.                                                         { A = true; }

/************************************************ alter cluster *********************************************************/
cmd ::= ALTER CLUSTER NK_STRING(A).                                               { pCxt->pRootNode = createAlterClusterStmt(pCxt, &A, NULL); }
cmd ::= ALTER CLUSTER NK_STRING(A) NK_STRING(B).                                  { pCxt->pRootNode = createAlterClusterStmt(pCxt, &A, &B); }

/************************************************ alter local *********************************************************/
cmd ::= ALTER LOCAL NK_STRING(A).                                                 { pCxt->pRootNode = createAlterLocalStmt(pCxt, &A, NULL); }
cmd ::= ALTER LOCAL NK_STRING(A) NK_STRING(B).                                    { pCxt->pRootNode = createAlterLocalStmt(pCxt, &A, &B); }

/************************************************ create/drop/restore qnode ***************************************************/
cmd ::= CREATE QNODE ON DNODE NK_INTEGER(A).                                      { pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_QNODE_STMT, &A); }
cmd ::= DROP QNODE ON DNODE NK_INTEGER(A).                                        { pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_QNODE_STMT, &A); }
cmd ::= RESTORE QNODE ON DNODE NK_INTEGER(A).                                     { pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_QNODE_STMT, &A); }

/************************************************ create/drop bnode ***************************************************/
cmd ::= CREATE BNODE ON DNODE NK_INTEGER(A).                                      { pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_BNODE_STMT, &A); }
cmd ::= DROP BNODE ON DNODE NK_INTEGER(A).                                        { pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_BNODE_STMT, &A); }

/************************************************ create/drop snode ***************************************************/
cmd ::= CREATE SNODE ON DNODE NK_INTEGER(A).                                      { pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_SNODE_STMT, &A); }
cmd ::= DROP SNODE ON DNODE NK_INTEGER(A).                                        { pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_SNODE_STMT, &A); }

/************************************************ create/drop/restore mnode ***************************************************/
cmd ::= CREATE MNODE ON DNODE NK_INTEGER(A).                                      { pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_MNODE_STMT, &A); }
cmd ::= DROP MNODE ON DNODE NK_INTEGER(A).                                        { pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_MNODE_STMT, &A); }
cmd ::= RESTORE MNODE ON DNODE NK_INTEGER(A).                                     { pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_MNODE_STMT, &A); }

/************************************************ restore vnode ***************************************************/
cmd ::= RESTORE VNODE ON DNODE NK_INTEGER(A).                                     { pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_VNODE_STMT, &A); }

/************************************************ create/drop/use database ********************************************/
cmd ::= CREATE DATABASE not_exists_opt(A) db_name(B) db_options(C).               { pCxt->pRootNode = createCreateDatabaseStmt(pCxt, A, &B, C); }
cmd ::= DROP DATABASE exists_opt(A) db_name(B).                                   { pCxt->pRootNode = createDropDatabaseStmt(pCxt, A, &B); }
cmd ::= USE db_name(A).                                                           { pCxt->pRootNode = createUseDatabaseStmt(pCxt, &A); }
cmd ::= ALTER DATABASE db_name(A) alter_db_options(B).                            { pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &A, B); }
cmd ::= FLUSH DATABASE db_name(A).                                                { pCxt->pRootNode = createFlushDatabaseStmt(pCxt, &A); }
cmd ::= TRIM DATABASE db_name(A) speed_opt(B).                                    { pCxt->pRootNode = createTrimDatabaseStmt(pCxt, &A, B); }
cmd ::= COMPACT DATABASE db_name(A) start_opt(B) end_opt(C).                      { pCxt->pRootNode = createCompactStmt(pCxt, &A, B, C); }

%type not_exists_opt                                                              { bool }
%destructor not_exists_opt                                                        { }
not_exists_opt(A) ::= IF NOT EXISTS.                                              { A = true; }
not_exists_opt(A) ::= .                                                           { A = false; }

%type exists_opt                                                                  { bool }
%destructor exists_opt                                                            { }
exists_opt(A) ::= IF EXISTS.                                                      { A = true; }
exists_opt(A) ::= .                                                               { A = false; }

db_options(A) ::= .                                                               { A = createDefaultDatabaseOptions(pCxt); }
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
db_options(A) ::= db_options(B) KEEP_TIME_OFFSET NK_INTEGER(C).                   { A = setDatabaseOption(pCxt, B, DB_OPTION_KEEP_TIME_OFFSET, &C); }

alter_db_options(A) ::= alter_db_option(B).                                       { A = createAlterDatabaseOptions(pCxt); A = setAlterDatabaseOption(pCxt, A, &B); }
alter_db_options(A) ::= alter_db_options(B) alter_db_option(C).                   { A = setAlterDatabaseOption(pCxt, B, &C); }

%type alter_db_option                                                             { SAlterOption }
%destructor alter_db_option                                                       { }
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
alter_db_option(A) ::= KEEP_TIME_OFFSET NK_INTEGER(B).                            { A.type = DB_OPTION_KEEP_TIME_OFFSET; A.val = B; }

%type integer_list                                                                { SNodeList* }
%destructor integer_list                                                          { nodesDestroyList($$); }
integer_list(A) ::= NK_INTEGER(B).                                                { A = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B)); }
integer_list(A) ::= integer_list(B) NK_COMMA NK_INTEGER(C).                       { A = addNodeToList(pCxt, B, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &C)); }

%type variable_list                                                               { SNodeList* }
%destructor variable_list                                                         { nodesDestroyList($$); }
variable_list(A) ::= NK_VARIABLE(B).                                              { A = createNodeList(pCxt, createDurationValueNode(pCxt, &B)); }
variable_list(A) ::= variable_list(B) NK_COMMA NK_VARIABLE(C).                    { A = addNodeToList(pCxt, B, createDurationValueNode(pCxt, &C)); }

%type retention_list                                                              { SNodeList* }
%destructor retention_list                                                        { nodesDestroyList($$); }
retention_list(A) ::= retention(B).                                               { A = createNodeList(pCxt, B); }
retention_list(A) ::= retention_list(B) NK_COMMA retention(C).                    { A = addNodeToList(pCxt, B, C); }

retention(A) ::= NK_VARIABLE(B) NK_COLON NK_VARIABLE(C).                          { A = createNodeListNodeEx(pCxt, createDurationValueNode(pCxt, &B), createDurationValueNode(pCxt, &C)); }
retention(A) ::= NK_MINUS(B) NK_COLON NK_VARIABLE(C).                             { A = createNodeListNodeEx(pCxt, createDurationValueNode(pCxt, &B), createDurationValueNode(pCxt, &C)); }

%type speed_opt                                                                   { int32_t }
%destructor speed_opt                                                             { }
speed_opt(A) ::= .                                                                { A = 0; }
speed_opt(A) ::= BWLIMIT NK_INTEGER(B).                                           { A = taosStr2Int32(B.z, NULL, 10); }

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
  NK_LP column_def_list(C) NK_RP tags_def_opt(D) table_options(E).                { pCxt->pRootNode = createCreateTableStmt(pCxt, A, B, C, D, E); }
cmd ::= CREATE TABLE multi_create_clause(A).                                      { pCxt->pRootNode = createCreateMultiTableStmt(pCxt, A); }
cmd ::= CREATE STABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def(D) table_options(E).                    { pCxt->pRootNode = createCreateTableStmt(pCxt, A, B, C, D, E); }
cmd ::= DROP TABLE multi_drop_clause(A).                                          { pCxt->pRootNode = createDropTableStmt(pCxt, A); }
cmd ::= DROP STABLE exists_opt(A) full_table_name(B).                             { pCxt->pRootNode = createDropSuperTableStmt(pCxt, A, B); }

cmd ::= ALTER TABLE alter_table_clause(A).                                        { pCxt->pRootNode = A; }
cmd ::= ALTER STABLE alter_table_clause(A).                                       { pCxt->pRootNode = setAlterSuperTableType(A); }

alter_table_clause(A) ::= full_table_name(B) alter_table_options(C).              { A = createAlterTableModifyOptions(pCxt, B, C); }
alter_table_clause(A) ::=
  full_table_name(B) ADD COLUMN column_name(C) type_name(D).                      { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_ADD_COLUMN, &C, D); }
alter_table_clause(A) ::= full_table_name(B) DROP COLUMN column_name(C).          { A = createAlterTableDropCol(pCxt, B, TSDB_ALTER_TABLE_DROP_COLUMN, &C); }
alter_table_clause(A) ::=
  full_table_name(B) MODIFY COLUMN column_name(C) type_name(D).                   { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &C, D); }
alter_table_clause(A) ::=
  full_table_name(B) RENAME COLUMN column_name(C) column_name(D).                 { A = createAlterTableRenameCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &C, &D); }
alter_table_clause(A) ::=
  full_table_name(B) ADD TAG column_name(C) type_name(D).                         { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_ADD_TAG, &C, D); }
alter_table_clause(A) ::= full_table_name(B) DROP TAG column_name(C).             { A = createAlterTableDropCol(pCxt, B, TSDB_ALTER_TABLE_DROP_TAG, &C); }
alter_table_clause(A) ::=
  full_table_name(B) MODIFY TAG column_name(C) type_name(D).                      { A = createAlterTableAddModifyCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &C, D); }
alter_table_clause(A) ::=
  full_table_name(B) RENAME TAG column_name(C) column_name(D).                    { A = createAlterTableRenameCol(pCxt, B, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &C, &D); }
alter_table_clause(A) ::=
  full_table_name(B) SET TAG column_name(C) NK_EQ signed_literal(D).              { A = createAlterTableSetTag(pCxt, B, &C, D); }

%type multi_create_clause                                                         { SNodeList* }
%destructor multi_create_clause                                                   { nodesDestroyList($$); }
multi_create_clause(A) ::= create_subtable_clause(B).                             { A = createNodeList(pCxt, B); }
multi_create_clause(A) ::= multi_create_clause(B) create_subtable_clause(C).      { A = addNodeToList(pCxt, B, C); }

create_subtable_clause(A) ::=
  not_exists_opt(B) full_table_name(C) USING full_table_name(D)
  specific_cols_opt(E) TAGS NK_LP expression_list(F) NK_RP table_options(G).      { A = createCreateSubTableClause(pCxt, B, C, D, E, F, G); }

%type multi_drop_clause                                                           { SNodeList* }
%destructor multi_drop_clause                                                     { nodesDestroyList($$); }
multi_drop_clause(A) ::= drop_table_clause(B).                                    { A = createNodeList(pCxt, B); }
multi_drop_clause(A) ::= multi_drop_clause(B) NK_COMMA drop_table_clause(C).      { A = addNodeToList(pCxt, B, C); }

drop_table_clause(A) ::= exists_opt(B) full_table_name(C).                        { A = createDropTableClause(pCxt, B, C); }

%type specific_cols_opt                                                           { SNodeList* }
%destructor specific_cols_opt                                                     { nodesDestroyList($$); }
specific_cols_opt(A) ::= .                                                        { A = NULL; }
specific_cols_opt(A) ::= NK_LP col_name_list(B) NK_RP.                            { A = B; }

full_table_name(A) ::= table_name(B).                                             { A = createRealTableNode(pCxt, NULL, &B, NULL); }
full_table_name(A) ::= db_name(B) NK_DOT table_name(C).                           { A = createRealTableNode(pCxt, &B, &C, NULL); }

%type column_def_list                                                             { SNodeList* }
%destructor column_def_list                                                       { nodesDestroyList($$); }
column_def_list(A) ::= column_def(B).                                             { A = createNodeList(pCxt, B); }
column_def_list(A) ::= column_def_list(B) NK_COMMA column_def(C).                 { A = addNodeToList(pCxt, B, C); }

column_def(A) ::= column_name(B) type_name(C).                                    { A = createColumnDefNode(pCxt, &B, C, NULL); }
//column_def(A) ::= column_name(B) type_name(C) COMMENT NK_STRING(D).               { A = createColumnDefNode(pCxt, &B, C, &D); }

%type type_name                                                                   { SDataType }
%destructor type_name                                                             { }
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

%type tags_def_opt                                                                { SNodeList* }
%destructor tags_def_opt                                                          { nodesDestroyList($$); }
tags_def_opt(A) ::= .                                                             { A = NULL; }
tags_def_opt(A) ::= tags_def(B).                                                  { A = B; }

%type tags_def                                                                    { SNodeList* }
%destructor tags_def                                                              { nodesDestroyList($$); }
tags_def(A) ::= TAGS NK_LP column_def_list(B) NK_RP.                              { A = B; }

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

%type alter_table_option                                                          { SAlterOption }
%destructor alter_table_option                                                    { }
alter_table_option(A) ::= COMMENT NK_STRING(B).                                   { A.type = TABLE_OPTION_COMMENT; A.val = B; }
alter_table_option(A) ::= TTL NK_INTEGER(B).                                      { A.type = TABLE_OPTION_TTL; A.val = B; }

%type duration_list                                                               { SNodeList* }
%destructor duration_list                                                         { nodesDestroyList($$); }
duration_list(A) ::= duration_literal(B).                                         { A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
duration_list(A) ::= duration_list(B) NK_COMMA duration_literal(C).               { A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

%type rollup_func_list                                                            { SNodeList* }
%destructor rollup_func_list                                                      { nodesDestroyList($$); }
rollup_func_list(A) ::= rollup_func_name(B).                                      { A = createNodeList(pCxt, B); }
rollup_func_list(A) ::= rollup_func_list(B) NK_COMMA rollup_func_name(C).         { A = addNodeToList(pCxt, B, C); }

rollup_func_name(A) ::= function_name(B).                                         { A = createFunctionNode(pCxt, &B, NULL); }
rollup_func_name(A) ::= FIRST(B).                                                 { A = createFunctionNode(pCxt, &B, NULL); }
rollup_func_name(A) ::= LAST(B).                                                  { A = createFunctionNode(pCxt, &B, NULL); }

%type col_name_list                                                               { SNodeList* }
%destructor col_name_list                                                         { nodesDestroyList($$); }
col_name_list(A) ::= col_name(B).                                                 { A = createNodeList(pCxt, B); }
col_name_list(A) ::= col_name_list(B) NK_COMMA col_name(C).                       { A = addNodeToList(pCxt, B, C); }

col_name(A) ::= column_name(B).                                                   { A = createColumnNode(pCxt, NULL, &B); }

/************************************************ show ****************************************************************/
cmd ::= SHOW DNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT); }
cmd ::= SHOW USERS.                                                               { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT); }
cmd ::= SHOW USER PRIVILEGES.                                                     { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USER_PRIVILEGES_STMT); }
cmd ::= SHOW db_kind_opt(A) DATABASES.                                            {
                                                                                    pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT);
                                                                                    setShowKind(pCxt, pCxt->pRootNode, A);
                                                                                  }
cmd ::= SHOW table_kind_db_name_cond_opt(A) TABLES like_pattern_opt(B).           {
                                                                                    pCxt->pRootNode = createShowTablesStmt(pCxt, A, B, OP_TYPE_LIKE);
                                                                                  }
cmd ::= SHOW db_name_cond_opt(A) STABLES like_pattern_opt(B).                     { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_STABLES_STMT, A, B, OP_TYPE_LIKE); }
cmd ::= SHOW db_name_cond_opt(A) VGROUPS.                                         { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, A, NULL, OP_TYPE_LIKE); }
cmd ::= SHOW MNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT); }
//cmd ::= SHOW MODULES.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MODULES_STMT); }
cmd ::= SHOW QNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT); }
cmd ::= SHOW FUNCTIONS.                                                           { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT); }
cmd ::= SHOW INDEXES FROM table_name_cond(A) from_db_opt(B).                      { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, B, A, OP_TYPE_EQUAL); }
cmd ::= SHOW INDEXES FROM db_name(B) NK_DOT table_name(A).                        { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, createIdentifierValueNode(pCxt, &B), createIdentifierValueNode(pCxt, &A), OP_TYPE_EQUAL); }
cmd ::= SHOW STREAMS.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT); }
cmd ::= SHOW ACCOUNTS.                                                            { pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
cmd ::= SHOW APPS.                                                                { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_APPS_STMT); }
cmd ::= SHOW CONNECTIONS.                                                         { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CONNECTIONS_STMT); }
cmd ::= SHOW LICENCES.                                                            { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_LICENCES_STMT); }
cmd ::= SHOW GRANTS.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_LICENCES_STMT); }
cmd ::= SHOW GRANTS FULL.                                                         { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_FULL_STMT); }
cmd ::= SHOW GRANTS LOGS.                                                         { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_LOGS_STMT); }
cmd ::= SHOW CLUSTER MACHINES.                                                    { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT); }
cmd ::= SHOW CREATE DATABASE db_name(A).                                          { pCxt->pRootNode = createShowCreateDatabaseStmt(pCxt, &A); }
cmd ::= SHOW CREATE TABLE full_table_name(A).                                     { pCxt->pRootNode = createShowCreateTableStmt(pCxt, QUERY_NODE_SHOW_CREATE_TABLE_STMT, A); }
cmd ::= SHOW CREATE STABLE full_table_name(A).                                    { pCxt->pRootNode = createShowCreateTableStmt(pCxt, QUERY_NODE_SHOW_CREATE_STABLE_STMT, A); }
cmd ::= SHOW QUERIES.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QUERIES_STMT); }
cmd ::= SHOW SCORES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SCORES_STMT); }
cmd ::= SHOW TOPICS.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TOPICS_STMT); }
cmd ::= SHOW VARIABLES.                                                           { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VARIABLES_STMT); }
cmd ::= SHOW CLUSTER VARIABLES.                                                   { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VARIABLES_STMT); }
cmd ::= SHOW LOCAL VARIABLES.                                                     { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT); }
cmd ::= SHOW DNODE NK_INTEGER(A) VARIABLES like_pattern_opt(B).                   { pCxt->pRootNode = createShowDnodeVariablesStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &A), B); }
cmd ::= SHOW BNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_BNODES_STMT); }
cmd ::= SHOW SNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SNODES_STMT); }
cmd ::= SHOW CLUSTER.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_STMT); }
cmd ::= SHOW TRANSACTIONS.                                                        { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TRANSACTIONS_STMT); }
cmd ::= SHOW TABLE DISTRIBUTED full_table_name(A).                                { pCxt->pRootNode = createShowTableDistributedStmt(pCxt, A); }
cmd ::= SHOW CONSUMERS.                                                           { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CONSUMERS_STMT); }
cmd ::= SHOW SUBSCRIPTIONS.                                                       { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT); }
cmd ::= SHOW TAGS FROM table_name_cond(A) from_db_opt(B).                         { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, B, A, OP_TYPE_EQUAL); }
cmd ::= SHOW TAGS FROM db_name(B) NK_DOT table_name(A).                           { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, createIdentifierValueNode(pCxt, &B), createIdentifierValueNode(pCxt, &A), OP_TYPE_EQUAL); }
cmd ::= SHOW TABLE TAGS tag_list_opt(C) FROM table_name_cond(A) from_db_opt(B).   { pCxt->pRootNode = createShowTableTagsStmt(pCxt, A, B, C); }
cmd ::= SHOW TABLE TAGS tag_list_opt(C) FROM db_name(B) NK_DOT table_name(A).     { pCxt->pRootNode = createShowTableTagsStmt(pCxt, createIdentifierValueNode(pCxt, &A), createIdentifierValueNode(pCxt, &B), C); }
cmd ::= SHOW VNODES ON DNODE NK_INTEGER(A).                                       { pCxt->pRootNode = createShowVnodesStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &A), NULL); }
cmd ::= SHOW VNODES.                                                              { pCxt->pRootNode = createShowVnodesStmt(pCxt, NULL, NULL); }
// show alive
cmd ::= SHOW db_name_cond_opt(A) ALIVE.                                           { pCxt->pRootNode = createShowAliveStmt(pCxt, A,    QUERY_NODE_SHOW_DB_ALIVE_STMT); }
cmd ::= SHOW CLUSTER ALIVE.                                                       { pCxt->pRootNode = createShowAliveStmt(pCxt, NULL, QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT); }
cmd ::= SHOW db_name_cond_opt(A) VIEWS like_pattern_opt(B).                       { pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VIEWS_STMT, A, B, OP_TYPE_LIKE); }
cmd ::= SHOW CREATE VIEW full_table_name(A).                                      { pCxt->pRootNode = createShowCreateViewStmt(pCxt, QUERY_NODE_SHOW_CREATE_VIEW_STMT, A); }
cmd ::= SHOW COMPACTS.                                                            { pCxt->pRootNode = createShowCompactsStmt(pCxt, QUERY_NODE_SHOW_COMPACTS_STMT); }
cmd ::= SHOW COMPACT NK_INTEGER(A).                                               { pCxt->pRootNode = createShowCompactDetailsStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &A)); }

%type table_kind_db_name_cond_opt                                                 { SShowTablesOption }
%destructor table_kind_db_name_cond_opt                                           { }
table_kind_db_name_cond_opt(A) ::= .                                              { A.kind = SHOW_KIND_ALL; A.dbName = nil_token; }
table_kind_db_name_cond_opt(A) ::= table_kind(B).                                 { A.kind = B; A.dbName = nil_token; }
table_kind_db_name_cond_opt(A) ::= db_name(C) NK_DOT.                             { A.kind = SHOW_KIND_ALL; A.dbName = C; }
table_kind_db_name_cond_opt(A) ::= table_kind(B) db_name(C) NK_DOT.               { A.kind = B; A.dbName = C; }

%type table_kind                                                                  { EShowKind }
%destructor table_kind                                                            { }   
table_kind(A) ::= NORMAL.                                                         { A = SHOW_KIND_TABLES_NORMAL; }
table_kind(A) ::= CHILD.                                                          { A = SHOW_KIND_TABLES_CHILD; }

db_name_cond_opt(A) ::= .                                                         { A = createDefaultDatabaseCondValue(pCxt); }
db_name_cond_opt(A) ::= db_name(B) NK_DOT.                                        { A = createIdentifierValueNode(pCxt, &B); }

like_pattern_opt(A) ::= .                                                         { A = NULL; }
like_pattern_opt(A) ::= LIKE NK_STRING(B).                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

table_name_cond(A) ::= table_name(B).                                             { A = createIdentifierValueNode(pCxt, &B); }

from_db_opt(A) ::= .                                                              { A = createDefaultDatabaseCondValue(pCxt); }
from_db_opt(A) ::= FROM db_name(B).                                               { A = createIdentifierValueNode(pCxt, &B); }

%type tag_list_opt                                                                { SNodeList* }
%destructor tag_list_opt                                                          { nodesDestroyList($$); }
tag_list_opt(A) ::= .                                                             { A = NULL; }
tag_list_opt(A) ::= tag_item(B).                                                  { A = createNodeList(pCxt, B); }
tag_list_opt(A) ::= tag_list_opt(B) NK_COMMA tag_item(C).                         { A = addNodeToList(pCxt, B, C); }

tag_item(A) ::= TBNAME(B).                                                        { A = setProjectionAlias(pCxt, createFunctionNode(pCxt, &B, NULL), &B); }
tag_item(A) ::= QTAGS(B).                                                         { A = createFunctionNode(pCxt, &B, NULL); }
tag_item(A) ::= column_name(B).                                                   { A = createColumnNode(pCxt, NULL, &B); }
tag_item(A) ::= column_name(B) column_alias(C).                                   { A = setProjectionAlias(pCxt, createColumnNode(pCxt, NULL, &B), &C); }
tag_item(A) ::= column_name(B) AS column_alias(C).                                { A = setProjectionAlias(pCxt, createColumnNode(pCxt, NULL, &B), &C); }

%type db_kind_opt                                                                 { EShowKind }
%destructor db_kind_opt                                                           { }
db_kind_opt(A) ::= .                                                              { A = SHOW_KIND_ALL; }
db_kind_opt(A) ::= USER.                                                          { A = SHOW_KIND_DATABASES_USER; }
db_kind_opt(A) ::= SYSTEM.                                                        { A = SHOW_KIND_DATABASES_SYSTEM; }

/************************************************ create index ********************************************************/
cmd ::= CREATE SMA INDEX not_exists_opt(D)
  col_name(A) ON full_table_name(B) index_options(C).                      { pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, D, A, B, NULL, C); }
cmd ::= CREATE INDEX not_exists_opt(D)
  col_name(A) ON full_table_name(B) NK_LP col_name_list(C) NK_RP.          { pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_NORMAL, D, A, B, C, NULL); }
cmd ::= DROP INDEX exists_opt(B) full_index_name(A).                              { pCxt->pRootNode = createDropIndexStmt(pCxt, B, A); }

full_index_name(A) ::= index_name(B).                                             { A = createRealTableNodeForIndexName(pCxt, NULL, &B); }
full_index_name(A) ::= db_name(B) NK_DOT index_name(C).                           { A = createRealTableNodeForIndexName(pCxt, &B, &C); }

index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL
  NK_LP duration_literal(C) NK_RP sliding_opt(D) sma_stream_opt(E).               { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), NULL, D, E); }
index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL
  NK_LP duration_literal(C) NK_COMMA duration_literal(D) NK_RP sliding_opt(E)
  sma_stream_opt(F).                                                              { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D), E, F); }

%type func_list                                                                   { SNodeList* }
%destructor func_list                                                             { nodesDestroyList($$); }
func_list(A) ::= func(B).                                                         { A = createNodeList(pCxt, B); }
func_list(A) ::= func_list(B) NK_COMMA func(C).                                   { A = addNodeToList(pCxt, B, C); }

func(A) ::= sma_func_name(B) NK_LP expression_list(C) NK_RP.                      { A = createFunctionNode(pCxt, &B, C); }

%type sma_func_name                                                               { SToken }
%destructor sma_func_name                                                         { }
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
%type with_meta                                                                   { int32_t }
%destructor with_meta                                                             { }
with_meta(A) ::= AS.                                                              { A = 0; }
with_meta(A) ::= WITH META AS.                                                    { A = 1; }
with_meta(A) ::= ONLY META AS.                                                    { A = 2; }

cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) AS query_or_subquery(C).     { pCxt->pRootNode = createCreateTopicStmtUseQuery(pCxt, A, &B, C); }
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) with_meta(D)
  DATABASE db_name(C).                                                            { pCxt->pRootNode = createCreateTopicStmtUseDb(pCxt, A, &B, &C, D); }
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) with_meta(E)
  STABLE full_table_name(C) where_clause_opt(D).                                  { pCxt->pRootNode = createCreateTopicStmtUseTable(pCxt, A, &B, C, E, D); }

cmd ::= DROP TOPIC exists_opt(A) topic_name(B).                                   { pCxt->pRootNode = createDropTopicStmt(pCxt, A, &B); }
cmd ::= DROP CONSUMER GROUP exists_opt(A) cgroup_name(B) ON topic_name(C).        { pCxt->pRootNode = createDropCGroupStmt(pCxt, A, &B, &C); }

/************************************************ desc/describe *******************************************************/
cmd ::= DESC full_table_name(A).                                                  { pCxt->pRootNode = createDescribeStmt(pCxt, A); }
cmd ::= DESCRIBE full_table_name(A).                                              { pCxt->pRootNode = createDescribeStmt(pCxt, A); }

/************************************************ reset query cache ***************************************************/
cmd ::= RESET QUERY CACHE.                                                        { pCxt->pRootNode = createResetQueryCacheStmt(pCxt); }

/************************************************ explain *************************************************************/
cmd ::= EXPLAIN analyze_opt(A) explain_options(B) query_or_subquery(C).           { pCxt->pRootNode = createExplainStmt(pCxt, A, B, C); }
cmd ::= EXPLAIN analyze_opt(A) explain_options(B) insert_query(C).                { pCxt->pRootNode = createExplainStmt(pCxt, A, B, C); }

%type analyze_opt                                                                 { bool }
%destructor analyze_opt                                                           { }
analyze_opt(A) ::= .                                                              { A = false; }
analyze_opt(A) ::= ANALYZE.                                                       { A = true; }

explain_options(A) ::= .                                                          { A = createDefaultExplainOptions(pCxt); }
explain_options(A) ::= explain_options(B) VERBOSE NK_BOOL(C).                     { A = setExplainVerbose(pCxt, B, &C); }
explain_options(A) ::= explain_options(B) RATIO NK_FLOAT(C).                      { A = setExplainRatio(pCxt, B, &C); }

/************************************************ create/drop function ************************************************/
cmd ::= CREATE or_replace_opt(H) agg_func_opt(A) FUNCTION not_exists_opt(F)
  function_name(B) AS NK_STRING(C) OUTPUTTYPE type_name(D) bufsize_opt(E)
  language_opt(G).                                                                { pCxt->pRootNode = createCreateFunctionStmt(pCxt, F, A, &B, &C, D, E, &G, H); }
cmd ::= DROP FUNCTION exists_opt(B) function_name(A).                             { pCxt->pRootNode = createDropFunctionStmt(pCxt, B, &A); }

%type agg_func_opt                                                                { bool }
%destructor agg_func_opt                                                          { }
agg_func_opt(A) ::= .                                                             { A = false; }
agg_func_opt(A) ::= AGGREGATE.                                                    { A = true; }

%type bufsize_opt                                                                 { int32_t }
%destructor bufsize_opt                                                           { }
bufsize_opt(A) ::= .                                                              { A = 0; }
bufsize_opt(A) ::= BUFSIZE NK_INTEGER(B).                                         { A = taosStr2Int32(B.z, NULL, 10); }

%type language_opt                                                                 { SToken }
%destructor language_opt                                                           { }
language_opt(A) ::= .                                                              { A = nil_token; }
language_opt(A) ::= LANGUAGE NK_STRING(B).                                         { A = B; }

%type or_replace_opt                                                               { bool }
%destructor or_replace_opt                                                         { }
or_replace_opt(A) ::= .                                                            { A = false; }
or_replace_opt(A) ::= OR REPLACE.                                                  { A = true; }

/************************************************ create/drop view **************************************************/
cmd ::= CREATE or_replace_opt(A) VIEW full_view_name(B) AS(C) query_or_subquery(D).
                                                                                  { pCxt->pRootNode = createCreateViewStmt(pCxt, A, B, &C, D); }
cmd ::= DROP VIEW exists_opt(A) full_view_name(B).                                { pCxt->pRootNode = createDropViewStmt(pCxt, A, B); }

full_view_name(A) ::= view_name(B).                                               { A = createViewNode(pCxt, NULL, &B); }
full_view_name(A) ::= db_name(B) NK_DOT view_name(C).                             { A = createViewNode(pCxt, &B, &C); }

/************************************************ create/drop stream **************************************************/
cmd ::= CREATE STREAM not_exists_opt(E) stream_name(A) stream_options(B) INTO
  full_table_name(C) col_list_opt(H) tag_def_or_ref_opt(F) subtable_opt(G)
  AS query_or_subquery(D).                                                        { pCxt->pRootNode = createCreateStreamStmt(pCxt, E, &A, C, B, F, G, D, H); }
cmd ::= DROP STREAM exists_opt(A) stream_name(B).                                 { pCxt->pRootNode = createDropStreamStmt(pCxt, A, &B); }
cmd ::= PAUSE STREAM exists_opt(A) stream_name(B).                                { pCxt->pRootNode = createPauseStreamStmt(pCxt, A, &B); }
cmd ::= RESUME STREAM exists_opt(A) ignore_opt(C) stream_name(B).                 { pCxt->pRootNode = createResumeStreamStmt(pCxt, A, C, &B); }

%type col_list_opt                                                                { SNodeList* }
%destructor col_list_opt                                                          { nodesDestroyList($$); }
col_list_opt(A) ::= .                                                             { A = NULL; }
col_list_opt(A) ::= NK_LP col_name_list(B) NK_RP.                                 { A = B; }

%type tag_def_or_ref_opt                                                          { SNodeList* }
%destructor tag_def_or_ref_opt                                                    { nodesDestroyList($$); }
tag_def_or_ref_opt(A) ::= .                                                       { A = NULL; }
tag_def_or_ref_opt(A) ::= tags_def(B).                                            { A = B; }
tag_def_or_ref_opt(A) ::= TAGS NK_LP col_name_list(B) NK_RP.                      { A = B; }

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

%type ignore_opt                                                                  { bool }
%destructor ignore_opt                                                            { }
ignore_opt(A) ::= .                                                               { A = false; }
ignore_opt(A) ::= IGNORE UNTREATED.                                               { A = true; }

/************************************************ kill connection/query ***********************************************/
cmd ::= KILL CONNECTION NK_INTEGER(A).                                            { pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_CONNECTION_STMT, &A); }
cmd ::= KILL QUERY NK_STRING(A).                                                  { pCxt->pRootNode = createKillQueryStmt(pCxt, &A); }
cmd ::= KILL TRANSACTION NK_INTEGER(A).                                           { pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_TRANSACTION_STMT, &A); }
cmd ::= KILL COMPACT NK_INTEGER(A).                                               { pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_COMPACT_STMT, &A); }

/************************************************ merge/redistribute/ vgroup ******************************************/
cmd ::= BALANCE VGROUP.                                                           { pCxt->pRootNode = createBalanceVgroupStmt(pCxt); }
cmd ::= BALANCE VGROUP LEADER on_vgroup_id(A).                                    { pCxt->pRootNode = createBalanceVgroupLeaderStmt(pCxt, &A); }
cmd ::= MERGE VGROUP NK_INTEGER(A) NK_INTEGER(B).                                 { pCxt->pRootNode = createMergeVgroupStmt(pCxt, &A, &B); }
cmd ::= REDISTRIBUTE VGROUP NK_INTEGER(A) dnode_list(B).                          { pCxt->pRootNode = createRedistributeVgroupStmt(pCxt, &A, B); }
cmd ::= SPLIT VGROUP NK_INTEGER(A).                                               { pCxt->pRootNode = createSplitVgroupStmt(pCxt, &A); }

%type on_vgroup_id                                                                { SToken }
%destructor on_vgroup_id                                                          { }
on_vgroup_id(A) ::= .                                                             { A = nil_token; }
on_vgroup_id(A) ::= ON NK_INTEGER(B).                                             { A = B; }

%type dnode_list                                                                  { SNodeList* }
%destructor dnode_list                                                            { nodesDestroyList($$); }
dnode_list(A) ::= DNODE NK_INTEGER(B).                                            { A = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B)); }
dnode_list(A) ::= dnode_list(B) DNODE NK_INTEGER(C).                              { A = addNodeToList(pCxt, B, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &C)); }

/************************************************ syncdb **************************************************************/
//cmd ::= SYNCDB db_name(A) REPLICA.                                                { pCxt->pRootNode = createSyncdbStmt(pCxt, &A); }

/************************************************ syncdb **************************************************************/
cmd ::= DELETE FROM full_table_name(A) where_clause_opt(B).                       { pCxt->pRootNode = createDeleteStmt(pCxt, A, B); }

/************************************************ select **************************************************************/
cmd ::= query_or_subquery(A).                                                     { pCxt->pRootNode = A; }

/************************************************ insert **************************************************************/
cmd ::= insert_query(A).                                                          { pCxt->pRootNode = A; }

insert_query(A) ::= INSERT INTO full_table_name(D)
  NK_LP col_name_list(B) NK_RP query_or_subquery(C).                              { A = createInsertStmt(pCxt, D, B, C); }
insert_query(A) ::= INSERT INTO full_table_name(C) query_or_subquery(B).          { A = createInsertStmt(pCxt, C, NULL, B); }

/************************************************ literal *************************************************************/
literal(A) ::= NK_INTEGER(B).                                                     { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B)); }
literal(A) ::= NK_FLOAT(B).                                                       { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B)); }
literal(A) ::= NK_STRING(B).                                                      { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B)); }
literal(A) ::= NK_BOOL(B).                                                        { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &B)); }
literal(A) ::= TIMESTAMP(B) NK_STRING(C).                                         { A = createRawExprNodeExt(pCxt, &B, &C, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &C)); }
literal(A) ::= duration_literal(B).                                               { A = B; }
literal(A) ::= NULL(B).                                                           { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_NULL, &B)); }
literal(A) ::= NK_QUESTION(B).                                                    { A = createRawExprNode(pCxt, &B, createPlaceholderValueNode(pCxt, &B)); }

duration_literal(A) ::= NK_VARIABLE(B).                                           { A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }

signed(A) ::= NK_INTEGER(B).                                                      { A = createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B); }
signed(A) ::= NK_PLUS NK_INTEGER(B).                                              { A = createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &B); }
signed(A) ::= NK_MINUS(B) NK_INTEGER(C).                                          {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &t);
                                                                                  }
signed(A) ::= NK_FLOAT(B).                                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B); }
signed(A) ::= NK_PLUS NK_FLOAT(B).                                                { A = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B); }
signed(A) ::= NK_MINUS(B) NK_FLOAT(C).                                            {
                                                                                    SToken t = B;
                                                                                    t.n = (C.z + C.n) - B.z;
                                                                                    A = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t);
                                                                                  }

signed_literal(A) ::= signed(B).                                                  { A = B; }
signed_literal(A) ::= NK_STRING(B).                                               { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }
signed_literal(A) ::= NK_BOOL(B).                                                 { A = createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &B); }
signed_literal(A) ::= TIMESTAMP NK_STRING(B).                                     { A = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &B); }
signed_literal(A) ::= duration_literal(B).                                        { A = releaseRawExprNode(pCxt, B); }
signed_literal(A) ::= NULL(B).                                                    { A = createValueNode(pCxt, TSDB_DATA_TYPE_NULL, &B); }
signed_literal(A) ::= literal_func(B).                                            { A = releaseRawExprNode(pCxt, B); }
signed_literal(A) ::= NK_QUESTION(B).                                             { A = createPlaceholderValueNode(pCxt, &B); }

%type literal_list                                                                { SNodeList* }
%destructor literal_list                                                          { nodesDestroyList($$); }
literal_list(A) ::= signed_literal(B).                                            { A = createNodeList(pCxt, B); }
literal_list(A) ::= literal_list(B) NK_COMMA signed_literal(C).                   { A = addNodeToList(pCxt, B, C); }

/************************************************ names and identifiers ***********************************************/
%type db_name                                                                     { SToken }
%destructor db_name                                                               { }
db_name(A) ::= NK_ID(B).                                                          { A = B; }

%type table_name                                                                  { SToken }
%destructor table_name                                                            { }
table_name(A) ::= NK_ID(B).                                                       { A = B; }

%type column_name                                                                 { SToken }
%destructor column_name                                                           { }
column_name(A) ::= NK_ID(B).                                                      { A = B; }

%type function_name                                                               { SToken }
%destructor function_name                                                         { }
function_name(A) ::= NK_ID(B).                                                    { A = B; }

%type view_name                                                                   { SToken }
%destructor view_name                                                             { }
view_name(A) ::= NK_ID(B).                                                        { A = B; }

%type table_alias                                                                 { SToken }
%destructor table_alias                                                           { }
table_alias(A) ::= NK_ID(B).                                                      { A = B; }

%type column_alias                                                                { SToken }
%destructor column_alias                                                          { }
column_alias(A) ::= NK_ID(B).                                                     { A = B; }
column_alias(A) ::= NK_ALIAS(B).                                                  { A = B; }

%type user_name                                                                   { SToken }
%destructor user_name                                                             { }
user_name(A) ::= NK_ID(B).                                                        { A = B; }

%type topic_name                                                                  { SToken }
%destructor topic_name                                                            { }
topic_name(A) ::= NK_ID(B).                                                       { A = B; }

%type stream_name                                                                 { SToken }
%destructor stream_name                                                           { }
stream_name(A) ::= NK_ID(B).                                                      { A = B; }

%type cgroup_name                                                                 { SToken }
%destructor cgroup_name                                                           { }
cgroup_name(A) ::= NK_ID(B).                                                      { A = B; }

%type index_name                                                                  { SToken }
%destructor index_name                                                            { }
index_name(A) ::= NK_ID(B).                                                       { A = B; }

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

%type expression_list                                                             { SNodeList* }
%destructor expression_list                                                       { nodesDestroyList($$); }
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
function_expression(A) ::= literal_func(B).                                       { A = B; }

literal_func(A) ::= noarg_func(B) NK_LP NK_RP(C).                                 { A = createRawExprNodeExt(pCxt, &B, &C, createFunctionNode(pCxt, &B, NULL)); }
literal_func(A) ::= NOW(B).                                                       { A = createRawExprNode(pCxt, &B, createFunctionNode(pCxt, &B, NULL)); }

%type noarg_func                                                                  { SToken }
%destructor noarg_func                                                            { }
noarg_func(A) ::= NOW(B).                                                         { A = B; }
noarg_func(A) ::= TODAY(B).                                                       { A = B; }
noarg_func(A) ::= TIMEZONE(B).                                                    { A = B; }
noarg_func(A) ::= DATABASE(B).                                                    { A = B; }
noarg_func(A) ::= CLIENT_VERSION(B).                                              { A = B; }
noarg_func(A) ::= SERVER_VERSION(B).                                              { A = B; }
noarg_func(A) ::= SERVER_STATUS(B).                                               { A = B; }
noarg_func(A) ::= CURRENT_USER(B).                                                { A = B; }
noarg_func(A) ::= USER(B).                                                        { A = B; }

%type star_func                                                                   { SToken }
%destructor star_func                                                             { }
star_func(A) ::= COUNT(B).                                                        { A = B; }
star_func(A) ::= FIRST(B).                                                        { A = B; }
star_func(A) ::= LAST(B).                                                         { A = B; }
star_func(A) ::= LAST_ROW(B).                                                     { A = B; }

%type star_func_para_list                                                         { SNodeList* }
%destructor star_func_para_list                                                   { nodesDestroyList($$); }
star_func_para_list(A) ::= NK_STAR(B).                                            { A = createNodeList(pCxt, createColumnNode(pCxt, NULL, &B)); }
star_func_para_list(A) ::= other_para_list(B).                                    { A = B; }

%type other_para_list                                                             { SNodeList* }
%destructor other_para_list                                                       { nodesDestroyList($$); }
other_para_list(A) ::= star_func_para(B).                                         { A = createNodeList(pCxt, B); }
other_para_list(A) ::= other_para_list(B) NK_COMMA star_func_para(C).             { A = addNodeToList(pCxt, B, C); }

star_func_para(A) ::= expr_or_subquery(B).                                        { A = releaseRawExprNode(pCxt, B); }
star_func_para(A) ::= table_name(B) NK_DOT NK_STAR(C).                            { A = createColumnNode(pCxt, &B, &C); }

case_when_expression(A) ::=
  CASE(E) when_then_list(C) case_when_else_opt(D) END(F).                         { A = createRawExprNodeExt(pCxt, &E, &F, createCaseWhenNode(pCxt, NULL, C, D)); }
case_when_expression(A) ::=
  CASE(E) common_expression(B) when_then_list(C) case_when_else_opt(D) END(F).    { A = createRawExprNodeExt(pCxt, &E, &F, createCaseWhenNode(pCxt, releaseRawExprNode(pCxt, B), C, D)); }

%type when_then_list                                                              { SNodeList* }
%destructor when_then_list                                                        { nodesDestroyList($$); }
when_then_list(A) ::= when_then_expr(B).                                          { A = createNodeList(pCxt, B); }
when_then_list(A) ::= when_then_list(B) when_then_expr(C).                        { A = addNodeToList(pCxt, B, C); }

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

%type compare_op                                                                  { EOperatorType }
%destructor compare_op                                                            { }
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

%type in_op                                                                       { EOperatorType }
%destructor in_op                                                                 { }
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
table_reference_list(A) ::= table_reference_list(B) NK_COMMA table_reference(C).  { A = createJoinTableNode(pCxt, JOIN_TYPE_INNER, B, C, NULL); }

/************************************************ table_reference *****************************************************/
table_reference(A) ::= table_primary(B).                                          { A = B; }
table_reference(A) ::= joined_table(B).                                           { A = B; }

table_primary(A) ::= table_name(B) alias_opt(C).                                  { A = createRealTableNode(pCxt, NULL, &B, &C); }
table_primary(A) ::= db_name(B) NK_DOT table_name(C) alias_opt(D).                { A = createRealTableNode(pCxt, &B, &C, &D); }
table_primary(A) ::= subquery(B) alias_opt(C).                                    { A = createTempTableNode(pCxt, releaseRawExprNode(pCxt, B), &C); }
table_primary(A) ::= parenthesized_joined_table(B).                               { A = B; }

%type alias_opt                                                                   { SToken }
%destructor alias_opt                                                             { }
alias_opt(A) ::= .                                                                { A = nil_token;  }
alias_opt(A) ::= table_alias(B).                                                  { A = B; }
alias_opt(A) ::= AS table_alias(B).                                               { A = B; }

parenthesized_joined_table(A) ::= NK_LP joined_table(B) NK_RP.                    { A = B; }
parenthesized_joined_table(A) ::= NK_LP parenthesized_joined_table(B) NK_RP.      { A = B; }

/************************************************ joined_table ********************************************************/
joined_table(A) ::=
  table_reference(B) join_type(C) JOIN table_reference(D) ON search_condition(E). { A = createJoinTableNode(pCxt, C, B, D, E); }

%type join_type                                                                   { EJoinType }
%destructor join_type                                                             { }
join_type(A) ::= .                                                                { A = JOIN_TYPE_INNER; }
join_type(A) ::= INNER.                                                           { A = JOIN_TYPE_INNER; }

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

%type hint_list                                                                   { SNodeList* }
%destructor hint_list                                                             { nodesDestroyList($$); }
hint_list(A) ::= .                                                                { A = createHintNodeList(pCxt, NULL); }
hint_list(A) ::= NK_HINT(B).                                                      { A = createHintNodeList(pCxt, &B); }

%type tag_mode_opt                                                                { bool }
%destructor tag_mode_opt                                                          { }
tag_mode_opt(A) ::= .                                                             { A = false; }
tag_mode_opt(A) ::= TAGS.                                                         { A = true; }

%type set_quantifier_opt                                                          { bool }
%destructor set_quantifier_opt                                                    { }
set_quantifier_opt(A) ::= .                                                       { A = false; }
set_quantifier_opt(A) ::= DISTINCT.                                               { A = true; }
set_quantifier_opt(A) ::= ALL.                                                    { A = false; }

%type select_list                                                                 { SNodeList* }
%destructor select_list                                                           { nodesDestroyList($$); }
select_list(A) ::= select_item(B).                                                { A = createNodeList(pCxt, B); }
select_list(A) ::= select_list(B) NK_COMMA select_item(C).                        { A = addNodeToList(pCxt, B, C); }

select_item(A) ::= NK_STAR(B).                                                    { A = createColumnNode(pCxt, NULL, &B); }
select_item(A) ::= common_expression(B).                                          { A = releaseRawExprNode(pCxt, B); }
select_item(A) ::= common_expression(B) column_alias(C).                          { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= common_expression(B) AS column_alias(C).                       { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                               { A = createColumnNode(pCxt, &B, &C); }

where_clause_opt(A) ::= .                                                         { A = NULL; }
where_clause_opt(A) ::= WHERE search_condition(B).                                { A = B; }

%type partition_by_clause_opt                                                     { SNodeList* }
%destructor partition_by_clause_opt                                               { nodesDestroyList($$); }
partition_by_clause_opt(A) ::= .                                                  { A = NULL; }
partition_by_clause_opt(A) ::= PARTITION BY partition_list(B).                    { A = B; }

%type partition_list                                                              { SNodeList* }
%destructor partition_list                                                        { nodesDestroyList($$); }
partition_list(A) ::= partition_item(B).                                          { A = createNodeList(pCxt, B); }
partition_list(A) ::= partition_list(B) NK_COMMA partition_item(C).               { A = addNodeToList(pCxt, B, C); }

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

%type fill_mode                                                                   { EFillMode }
%destructor fill_mode                                                             { }
fill_mode(A) ::= NONE.                                                            { A = FILL_MODE_NONE; }
fill_mode(A) ::= PREV.                                                            { A = FILL_MODE_PREV; }
fill_mode(A) ::= NULL.                                                            { A = FILL_MODE_NULL; }
fill_mode(A) ::= NULL_F.                                                          { A = FILL_MODE_NULL_F; }
fill_mode(A) ::= LINEAR.                                                          { A = FILL_MODE_LINEAR; }
fill_mode(A) ::= NEXT.                                                            { A = FILL_MODE_NEXT; }

%type group_by_clause_opt                                                         { SNodeList* }
%destructor group_by_clause_opt                                                   { nodesDestroyList($$); }
group_by_clause_opt(A) ::= .                                                      { A = NULL; }
group_by_clause_opt(A) ::= GROUP BY group_by_list(B).                             { A = B; }

%type group_by_list                                                               { SNodeList* }
%destructor group_by_list                                                         { nodesDestroyList($$); }
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

%type order_by_clause_opt                                                         { SNodeList* }
%destructor order_by_clause_opt                                                   { nodesDestroyList($$); }
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
%type sort_specification_list                                                     { SNodeList* }
%destructor sort_specification_list                                               { nodesDestroyList($$); }
sort_specification_list(A) ::= sort_specification(B).                             { A = createNodeList(pCxt, B); }
sort_specification_list(A) ::=
  sort_specification_list(B) NK_COMMA sort_specification(C).                      { A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::=
  expr_or_subquery(B) ordering_specification_opt(C) null_ordering_opt(D).         { A = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, B), C, D); }

%type ordering_specification_opt EOrder
%destructor ordering_specification_opt                                            { }
ordering_specification_opt(A) ::= .                                               { A = ORDER_ASC; }
ordering_specification_opt(A) ::= ASC.                                            { A = ORDER_ASC; }
ordering_specification_opt(A) ::= DESC.                                           { A = ORDER_DESC; }

%type null_ordering_opt ENullOrder
%destructor null_ordering_opt                                                     { }
null_ordering_opt(A) ::= .                                                        { A = NULL_ORDER_DEFAULT; }
null_ordering_opt(A) ::= NULLS FIRST.                                             { A = NULL_ORDER_FIRST; }
null_ordering_opt(A) ::= NULLS LAST.                                              { A = NULL_ORDER_LAST; }

%fallback ABORT AFTER ATTACH BEFORE BEGIN BITAND BITNOT BITOR BLOCKS CHANGE COMMA CONCAT CONFLICT COPY DEFERRED DELIMITERS DETACH DIVIDE DOT EACH END FAIL
  FILE FOR GLOB ID IMMEDIATE IMPORT INITIALLY INSTEAD ISNULL KEY MODULES NK_BITNOT NK_SEMI NOTNULL OF PLUS PRIVILEGE RAISE RESTRICT ROW SEMI STAR STATEMENT
  STRICT STRING TIMES VALUES VARIABLE VIEW WAL.
