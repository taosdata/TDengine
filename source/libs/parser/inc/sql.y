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

#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"
}

%syntax_error {
  if (pCxt->valid) {
    if(TOKEN.z) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
    pCxt->valid = false;
  }
}

%left OR.
%left AND.
//%right NOT.
%left UNION ALL MINUS EXCEPT INTERSECT.
%left NK_BITAND NK_BITOR NK_LSHIFT NK_RSHIFT.
%left NK_PLUS NK_MINUS.
//%left DIVIDE TIMES.
%left NK_STAR NK_SLASH NK_REM.
%left NK_CONCAT.
//%right NK_BITNOT.

/************************************************ create/alter account *****************************************/
cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options.                      { pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
cmd ::= ALTER ACCOUNT NK_ID alter_account_options.                                { pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }

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

/************************************************ create/alter/drop/show user *****************************************/
cmd ::= CREATE USER user_name(A) PASS NK_STRING(B).                               { pCxt->pRootNode = createCreateUserStmt(pCxt, &A, &B); }
cmd ::= ALTER USER user_name(A) PASS NK_STRING(B).                                { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_PASSWD, &B); }
cmd ::= ALTER USER user_name(A) PRIVILEGE NK_STRING(B).                           { pCxt->pRootNode = createAlterUserStmt(pCxt, &A, TSDB_ALTER_USER_PRIVILEGES, &B); }
cmd ::= DROP USER user_name(A).                                                   { pCxt->pRootNode = createDropUserStmt(pCxt, &A); }

/************************************************ create/drop/alter/show dnode ****************************************/
cmd ::= CREATE DNODE dnode_endpoint(A).                                           { pCxt->pRootNode = createCreateDnodeStmt(pCxt, &A, NULL); }
cmd ::= CREATE DNODE dnode_host_name(A) PORT NK_INTEGER(B).                       { pCxt->pRootNode = createCreateDnodeStmt(pCxt, &A, &B); }
cmd ::= DROP DNODE NK_INTEGER(A).                                                 { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A); }
cmd ::= DROP DNODE dnode_endpoint(A).                                             { pCxt->pRootNode = createDropDnodeStmt(pCxt, &A); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B).                                   { pCxt->pRootNode = createAlterDnodeStmt(pCxt, &A, &B, NULL); }
cmd ::= ALTER DNODE NK_INTEGER(A) NK_STRING(B) NK_STRING(C).                      { pCxt->pRootNode = createAlterDnodeStmt(pCxt, &A, &B, &C); }
cmd ::= ALTER ALL DNODES NK_STRING(A).                                            { pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &A, NULL); }
cmd ::= ALTER ALL DNODES NK_STRING(A) NK_STRING(B).                               { pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &A, &B); }

%type dnode_endpoint                                                              { SToken }
%destructor dnode_endpoint                                                        { }
dnode_endpoint(A) ::= NK_STRING(B).                                               { A = B; }

%type dnode_host_name                                                             { SToken }
%destructor dnode_host_name                                                       { }
dnode_host_name(A) ::= NK_ID(B).                                                  { A = B; }
dnode_host_name(A) ::= NK_IPTOKEN(B).                                             { A = B; }

/************************************************ alter local *********************************************************/
cmd ::= ALTER LOCAL NK_STRING(A).                                                 { pCxt->pRootNode = createAlterLocalStmt(pCxt, &A, NULL); }
cmd ::= ALTER LOCAL NK_STRING(A) NK_STRING(B).                                    { pCxt->pRootNode = createAlterLocalStmt(pCxt, &A, &B); }

/************************************************ create/drop qnode ***************************************************/
cmd ::= CREATE QNODE ON DNODE NK_INTEGER(A).                                      { pCxt->pRootNode = createCreateQnodeStmt(pCxt, &A); }
cmd ::= DROP QNODE ON DNODE NK_INTEGER(A).                                        { pCxt->pRootNode = createDropQnodeStmt(pCxt, &A); }

/************************************************ create/drop/show/use database ***************************************/
cmd ::= CREATE DATABASE not_exists_opt(A) db_name(B) db_options(C).               { pCxt->pRootNode = createCreateDatabaseStmt(pCxt, A, &B, C); }
cmd ::= DROP DATABASE exists_opt(A) db_name(B).                                   { pCxt->pRootNode = createDropDatabaseStmt(pCxt, A, &B); }
cmd ::= USE db_name(A).                                                           { pCxt->pRootNode = createUseDatabaseStmt(pCxt, &A); }
cmd ::= ALTER DATABASE db_name(A) alter_db_options(B).                            { pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &A, B); }

%type not_exists_opt                                                              { bool }
%destructor not_exists_opt                                                        { }
not_exists_opt(A) ::= IF NOT EXISTS.                                              { A = true; }
not_exists_opt(A) ::= .                                                           { A = false; }

%type exists_opt                                                                  { bool }
%destructor exists_opt                                                            { }
exists_opt(A) ::= IF EXISTS.                                                      { A = true; }
exists_opt(A) ::= .                                                               { A = false; }

db_options(A) ::= .                                                               { A = createDefaultDatabaseOptions(pCxt); }
db_options(A) ::= db_options(B) BLOCKS NK_INTEGER(C).                             { A = setDatabaseOption(pCxt, B, DB_OPTION_BLOCKS, &C); }
db_options(A) ::= db_options(B) CACHE NK_INTEGER(C).                              { A = setDatabaseOption(pCxt, B, DB_OPTION_CACHE, &C); }
db_options(A) ::= db_options(B) CACHELAST NK_INTEGER(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_CACHELAST, &C); }
db_options(A) ::= db_options(B) COMP NK_INTEGER(C).                               { A = setDatabaseOption(pCxt, B, DB_OPTION_COMP, &C); }
db_options(A) ::= db_options(B) DAYS NK_INTEGER(C).                               { A = setDatabaseOption(pCxt, B, DB_OPTION_DAYS, &C); }
db_options(A) ::= db_options(B) FSYNC NK_INTEGER(C).                              { A = setDatabaseOption(pCxt, B, DB_OPTION_FSYNC, &C); }
db_options(A) ::= db_options(B) MAXROWS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_MAXROWS, &C); }
db_options(A) ::= db_options(B) MINROWS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_MINROWS, &C); }
db_options(A) ::= db_options(B) KEEP NK_INTEGER(C).                               { A = setDatabaseOption(pCxt, B, DB_OPTION_KEEP, &C); }
db_options(A) ::= db_options(B) PRECISION NK_STRING(C).                           { A = setDatabaseOption(pCxt, B, DB_OPTION_PRECISION, &C); }
db_options(A) ::= db_options(B) QUORUM NK_INTEGER(C).                             { A = setDatabaseOption(pCxt, B, DB_OPTION_QUORUM, &C); }
db_options(A) ::= db_options(B) REPLICA NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_REPLICA, &C); }
db_options(A) ::= db_options(B) TTL NK_INTEGER(C).                                { A = setDatabaseOption(pCxt, B, DB_OPTION_TTL, &C); }
db_options(A) ::= db_options(B) WAL NK_INTEGER(C).                                { A = setDatabaseOption(pCxt, B, DB_OPTION_WAL, &C); }
db_options(A) ::= db_options(B) VGROUPS NK_INTEGER(C).                            { A = setDatabaseOption(pCxt, B, DB_OPTION_VGROUPS, &C); }
db_options(A) ::= db_options(B) SINGLE_STABLE NK_INTEGER(C).                      { A = setDatabaseOption(pCxt, B, DB_OPTION_SINGLE_STABLE, &C); }
db_options(A) ::= db_options(B) STREAM_MODE NK_INTEGER(C).                        { A = setDatabaseOption(pCxt, B, DB_OPTION_STREAM_MODE, &C); }
db_options(A) ::= db_options(B) RETENTIONS NK_STRING(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_RETENTIONS, &C); }
db_options(A) ::= db_options(B) FILE_FACTOR NK_FLOAT(C).                          { A = setDatabaseOption(pCxt, B, DB_OPTION_FILE_FACTOR, &C); }

alter_db_options(A) ::= alter_db_option(B).                                       { A = createDefaultAlterDatabaseOptions(pCxt); A = setDatabaseOption(pCxt, A, B.type, &B.val); }
alter_db_options(A) ::= alter_db_options(B) alter_db_option(C).                   { A = setDatabaseOption(pCxt, B, C.type, &C.val); }

%type alter_db_option                                                             { SAlterOption }
%destructor alter_db_option                                                       { }
alter_db_option(A) ::= BLOCKS NK_INTEGER(B).                                      { A.type = DB_OPTION_BLOCKS; A.val = B; }
alter_db_option(A) ::= FSYNC NK_INTEGER(B).                                       { A.type = DB_OPTION_FSYNC; A.val = B; }
alter_db_option(A) ::= KEEP NK_INTEGER(B).                                        { A.type = DB_OPTION_KEEP; A.val = B; }
alter_db_option(A) ::= WAL NK_INTEGER(B).                                         { A.type = DB_OPTION_WAL; A.val = B; }
alter_db_option(A) ::= QUORUM NK_INTEGER(B).                                      { A.type = DB_OPTION_QUORUM; A.val = B; }
alter_db_option(A) ::= CACHELAST NK_INTEGER(B).                                   { A.type = DB_OPTION_CACHELAST; A.val = B; }

/************************************************ create/drop table/stable ********************************************/
cmd ::= CREATE TABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def_opt(D) table_options(E).                { pCxt->pRootNode = createCreateTableStmt(pCxt, A, B, C, D, E); }
cmd ::= CREATE TABLE multi_create_clause(A).                                      { pCxt->pRootNode = createCreateMultiTableStmt(pCxt, A); }
cmd ::= CREATE STABLE not_exists_opt(A) full_table_name(B)
  NK_LP column_def_list(C) NK_RP tags_def(D) table_options(E).                    { pCxt->pRootNode = createCreateTableStmt(pCxt, A, B, C, D, E); }
cmd ::= DROP TABLE multi_drop_clause(A).                                          { pCxt->pRootNode = createDropTableStmt(pCxt, A); }
cmd ::= DROP STABLE exists_opt(A) full_table_name(B).                             { pCxt->pRootNode = createDropSuperTableStmt(pCxt, A, B); }

cmd ::= ALTER TABLE alter_table_clause(A).                                        { pCxt->pRootNode = A; }
cmd ::= ALTER STABLE alter_table_clause(A).                                       { pCxt->pRootNode = A; }

alter_table_clause(A) ::= full_table_name(B) alter_table_options(C).              { A = createAlterTableOption(pCxt, B, C); }
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
  full_table_name(B) SET TAG column_name(C) NK_EQ literal(D).                     { A = createAlterTableSetTag(pCxt, B, &C, D); }

%type multi_create_clause                                                         { SNodeList* }
%destructor multi_create_clause                                                   { nodesDestroyList($$); }
multi_create_clause(A) ::= create_subtable_clause(B).                             { A = createNodeList(pCxt, B); }
multi_create_clause(A) ::= multi_create_clause(B) create_subtable_clause(C).      { A = addNodeToList(pCxt, B, C); }

create_subtable_clause(A) ::=
  not_exists_opt(B) full_table_name(C) USING full_table_name(D)
  specific_tags_opt(E) TAGS NK_LP literal_list(F) NK_RP.                          { A = createCreateSubTableClause(pCxt, B, C, D, E, F); }

%type multi_drop_clause                                                           { SNodeList* }
%destructor multi_drop_clause                                                     { nodesDestroyList($$); }
multi_drop_clause(A) ::= drop_table_clause(B).                                    { A = createNodeList(pCxt, B); }
multi_drop_clause(A) ::= multi_drop_clause(B) drop_table_clause(C).               { A = addNodeToList(pCxt, B, C); }

drop_table_clause(A) ::= exists_opt(B) full_table_name(C).                        { A = createDropTableClause(pCxt, B, C); }

%type specific_tags_opt                                                           { SNodeList* }
%destructor specific_tags_opt                                                     { nodesDestroyList($$); }
specific_tags_opt(A) ::= .                                                        { A = NULL; }
specific_tags_opt(A) ::= NK_LP col_name_list(B) NK_RP.                            { A = B; }

full_table_name(A) ::= table_name(B).                                             { A = createRealTableNode(pCxt, NULL, &B, NULL); }
full_table_name(A) ::= db_name(B) NK_DOT table_name(C).                           { A = createRealTableNode(pCxt, &B, &C, NULL); }

%type column_def_list                                                             { SNodeList* }
%destructor column_def_list                                                       { nodesDestroyList($$); }
column_def_list(A) ::= column_def(B).                                             { A = createNodeList(pCxt, B); }
column_def_list(A) ::= column_def_list(B) NK_COMMA column_def(C).                 { A = addNodeToList(pCxt, B, C); }

column_def(A) ::= column_name(B) type_name(C).                                    { A = createColumnDefNode(pCxt, &B, C, NULL); }
column_def(A) ::= column_name(B) type_name(C) COMMENT NK_STRING(D).               { A = createColumnDefNode(pCxt, &B, C, &D); }

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
table_options(A) ::= table_options(B) KEEP NK_INTEGER(C).                         { A = setTableOption(pCxt, B, TABLE_OPTION_KEEP, &C); }
table_options(A) ::= table_options(B) TTL NK_INTEGER(C).                          { A = setTableOption(pCxt, B, TABLE_OPTION_TTL, &C); }
table_options(A) ::= table_options(B) SMA NK_LP col_name_list(C) NK_RP.           { A = setTableSmaOption(pCxt, B, C); }
table_options(A) ::= table_options(B) ROLLUP NK_LP func_name_list(C) NK_RP.       { A = setTableRollupOption(pCxt, B, C); }

alter_table_options(A) ::= alter_table_option(B).                                 { A = createDefaultAlterTableOptions(pCxt); A = setTableOption(pCxt, A, B.type, &B.val); }
alter_table_options(A) ::= alter_table_options(B) alter_table_option(C).          { A = setTableOption(pCxt, B, C.type, &C.val); }

%type alter_table_option                                                          { SAlterOption }
%destructor alter_table_option                                                    { }
alter_table_option(A) ::= COMMENT NK_STRING(B).                                   { A.type = TABLE_OPTION_COMMENT; A.val = B; }
alter_table_option(A) ::= KEEP NK_INTEGER(B).                                     { A.type = TABLE_OPTION_KEEP; A.val = B; }
alter_table_option(A) ::= TTL NK_INTEGER(B).                                      { A.type = TABLE_OPTION_TTL; A.val = B; }

%type col_name_list                                                               { SNodeList* }
%destructor col_name_list                                                         { nodesDestroyList($$); }
col_name_list(A) ::= col_name(B).                                                 { A = createNodeList(pCxt, B); }
col_name_list(A) ::= col_name_list(B) NK_COMMA col_name(C).                       { A = addNodeToList(pCxt, B, C); }

col_name(A) ::= column_name(B).                                                   { A = createColumnNode(pCxt, NULL, &B); }

/************************************************ show ****************************************************************/
cmd ::= SHOW DNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL, NULL); }
cmd ::= SHOW USERS.                                                               { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL, NULL); }
cmd ::= SHOW DATABASES.                                                           { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL, NULL); }
cmd ::= SHOW db_name_cond_opt(A) TABLES like_pattern_opt(B).                      { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, A, B); }
cmd ::= SHOW db_name_cond_opt(A) STABLES like_pattern_opt(B).                     { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, A, B); }
cmd ::= SHOW db_name_cond_opt(A) VGROUPS.                                         { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, A, NULL); }
cmd ::= SHOW MNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL, NULL); }
cmd ::= SHOW MODULES.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MODULES_STMT, NULL, NULL); }
cmd ::= SHOW QNODES.                                                              { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL, NULL); }
cmd ::= SHOW FUNCTIONS.                                                           { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT, NULL, NULL); }
cmd ::= SHOW INDEXES FROM table_name_cond(A) from_db_opt(B).                      { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, A, B); }
cmd ::= SHOW STREAMS.                                                             { pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT, NULL, NULL); }

db_name_cond_opt(A) ::= .                                                         { A = createDefaultDatabaseCondValue(pCxt); }
db_name_cond_opt(A) ::= db_name(B) NK_DOT.                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

like_pattern_opt(A) ::= .                                                         { A = NULL; }
like_pattern_opt(A) ::= LIKE NK_STRING(B).                                        { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

table_name_cond(A) ::= table_name(B).                                             { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

from_db_opt(A) ::= .                                                              { A = createDefaultDatabaseCondValue(pCxt); }
from_db_opt(A) ::= FROM db_name(B).                                               { A = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B); }

%type func_name_list                                                              { SNodeList* }
%destructor func_name_list                                                        { nodesDestroyList($$); }
func_name_list(A) ::= func_name(B).                                               { A = createNodeList(pCxt, B); }
func_name_list(A) ::= func_name_list(B) NK_COMMA col_name(C).                     { A = addNodeToList(pCxt, B, C); }

func_name(A) ::= function_name(B).                                                { A = createFunctionNode(pCxt, &B, NULL); }

/************************************************ create index ********************************************************/
cmd ::= CREATE SMA INDEX not_exists_opt(D) 
  index_name(A) ON table_name(B) index_options(C).                                { pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, D, &A, &B, NULL, C); }
cmd ::= CREATE FULLTEXT INDEX not_exists_opt(D)
  index_name(A) ON table_name(B) NK_LP col_name_list(C) NK_RP.                    { pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, D, &A, &B, C, NULL); }
cmd ::= DROP INDEX exists_opt(C) index_name(A) ON table_name(B).                  { pCxt->pRootNode = createDropIndexStmt(pCxt, C, &A, &B); }

index_options(A) ::= .                                                            { A = NULL; }
index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL 
  NK_LP duration_literal(C) NK_RP sliding_opt(D).                                 { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), NULL, D); }
index_options(A) ::= FUNCTION NK_LP func_list(B) NK_RP INTERVAL 
  NK_LP duration_literal(C) NK_COMMA duration_literal(D) NK_RP sliding_opt(E).    { A = createIndexOption(pCxt, B, releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D), E); }

%type func_list                                                                   { SNodeList* }
%destructor func_list                                                             { nodesDestroyList($$); }
func_list(A) ::= func(B).                                                         { A = createNodeList(pCxt, B); }
func_list(A) ::= func_list(B) NK_COMMA func(C).                                   { A = addNodeToList(pCxt, B, C); }

func(A) ::= function_name(B) NK_LP expression_list(C) NK_RP.                      { A = createFunctionNode(pCxt, &B, C); }

/************************************************ create/drop topic ***************************************************/
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) AS query_expression(C).      { pCxt->pRootNode = createCreateTopicStmt(pCxt, A, &B, C, NULL); }
cmd ::= CREATE TOPIC not_exists_opt(A) topic_name(B) AS db_name(C).               { pCxt->pRootNode = createCreateTopicStmt(pCxt, A, &B, NULL, &C); }
cmd ::= DROP TOPIC exists_opt(A) topic_name(B).                                   { pCxt->pRootNode = createDropTopicStmt(pCxt, A, &B); }

/************************************************ select **************************************************************/
cmd ::= query_expression(A).                                                      { pCxt->pRootNode = A; }

/************************************************ literal *************************************************************/
literal(A) ::= NK_INTEGER(B).                                                     { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B)); }
literal(A) ::= NK_FLOAT(B).                                                       { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B)); }
literal(A) ::= NK_STRING(B).                                                      { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B)); }
literal(A) ::= NK_BOOL(B).                                                        { A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &B)); }
literal(A) ::= TIMESTAMP(B) NK_STRING(C).                                         { A = createRawExprNodeExt(pCxt, &B, &C, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &C)); }
literal(A) ::= duration_literal(B).                                               { A = B; }

duration_literal(A) ::= NK_VARIABLE(B).                                           { A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }

signed(A) ::= NK_INTEGER(B).                                                      { A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B); }
signed(A) ::= NK_PLUS NK_INTEGER(B).                                              { A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B); }
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

%type table_alias                                                                 { SToken }
%destructor table_alias                                                           { }
table_alias(A) ::= NK_ID(B).                                                      { A = B; }

%type column_alias                                                                { SToken }
%destructor column_alias                                                          { }
column_alias(A) ::= NK_ID(B).                                                     { A = B; }

%type user_name                                                                   { SToken }
%destructor user_name                                                             { }
user_name(A) ::= NK_ID(B).                                                        { A = B; }

%type index_name                                                                  { SToken }
%destructor index_name                                                            { }
index_name(A) ::= NK_ID(B).                                                       { A = B; }

%type topic_name                                                                  { SToken }
%destructor topic_name                                                            { }
topic_name(A) ::= NK_ID(B).                                                       { A = B; }

/************************************************ expression **********************************************************/
expression(A) ::= literal(B).                                                     { A = B; }
//expression(A) ::= NK_QUESTION(B).                                                 { A = B; }
//expression(A) ::= pseudo_column(B).                                               { A = B; }
expression(A) ::= column_reference(B).                                            { A = B; }
expression(A) ::= function_name(B) NK_LP expression_list(C) NK_RP(D).             { A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, C)); }
expression(A) ::= function_name(B) NK_LP NK_STAR(C) NK_RP(D).                     { A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, createNodeList(pCxt, createColumnNode(pCxt, NULL, &C)))); }
//expression(A) ::= cast_expression(B).                                             { A = B; }
//expression(A) ::= case_expression(B).                                             { A = B; }
expression(A) ::= subquery(B).                                                    { A = B; }
expression(A) ::= NK_LP(B) expression(C) NK_RP(D).                                { A = createRawExprNodeExt(pCxt, &B, &D, releaseRawExprNode(pCxt, C)); }
expression(A) ::= NK_PLUS(B) expression(C).                                       {
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, releaseRawExprNode(pCxt, C));
                                                                                  }
expression(A) ::= NK_MINUS(B) expression(C).                                      {
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, C), NULL));
                                                                                  }
expression(A) ::= expression(B) NK_PLUS expression(C).                            {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_MINUS expression(C).                           {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_STAR expression(C).                            {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_SLASH expression(C).                           {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_REM expression(C).                             {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }

%type expression_list                                                             { SNodeList* }
%destructor expression_list                                                       { nodesDestroyList($$); }
expression_list(A) ::= expression(B).                                             { A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
expression_list(A) ::= expression_list(B) NK_COMMA expression(C).                 { A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

column_reference(A) ::= column_name(B).                                           { A = createRawExprNode(pCxt, &B, createColumnNode(pCxt, NULL, &B)); }
column_reference(A) ::= table_name(B) NK_DOT column_name(C).                      { A = createRawExprNodeExt(pCxt, &B, &C, createColumnNode(pCxt, &B, &C)); }

//pseudo_column(A) ::= NK_NOW.                                                      { A = createFunctionNode(pCxt, NULL, NULL); }

/************************************************ predicate ***********************************************************/
predicate(A) ::= expression(B) compare_op(C) expression(D).                       {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, C, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)));
                                                                                  }
//predicate(A) ::= expression(B) compare_op sub_type expression(B).
predicate(A) ::= expression(B) BETWEEN expression(C) AND expression(D).           {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D)));
                                                                                  }
predicate(A) ::= expression(B) NOT BETWEEN expression(C) AND expression(D).       {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, D);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)));
                                                                                  }
predicate(A) ::= expression(B) IS NULL(C).                                        {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &C, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, B), NULL));
                                                                                  }
predicate(A) ::= expression(B) IS NOT NULL(C).                                    {
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &C, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, B), NULL));
                                                                                  }
predicate(A) ::= expression(B) in_op(C) in_predicate_value(D).                    {
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

%type in_op                                                                       { EOperatorType }
%destructor in_op                                                                 { }
in_op(A) ::= IN.                                                                  { A = OP_TYPE_IN; }
in_op(A) ::= NOT IN.                                                              { A = OP_TYPE_NOT_IN; }

in_predicate_value(A) ::= NK_LP(C) expression_list(B) NK_RP(D).                   { A = createRawExprNodeExt(pCxt, &C, &D, createNodeListNode(pCxt, B)); }

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
common_expression(A) ::= expression(B).                                           { A = B; }
common_expression(A) ::= boolean_value_expression(B).                             { A = B; }

/************************************************ from_clause *********************************************************/
from_clause(A) ::= FROM table_reference_list(B).                                  { A = B; }

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
  SELECT set_quantifier_opt(B) select_list(C) from_clause(D) where_clause_opt(E) 
    partition_by_clause_opt(F) twindow_clause_opt(G) 
    group_by_clause_opt(H) having_clause_opt(I).                                  { 
                                                                                    A = createSelectStmt(pCxt, B, C, D);
                                                                                    A = addWhereClause(pCxt, A, E);
                                                                                    A = addPartitionByClause(pCxt, A, F);
                                                                                    A = addWindowClauseClause(pCxt, A, G);
                                                                                    A = addGroupByClause(pCxt, A, H);
                                                                                    A = addHavingClause(pCxt, A, I);
                                                                                  }

%type set_quantifier_opt                                                          { bool }
%destructor set_quantifier_opt                                                    { }
set_quantifier_opt(A) ::= .                                                       { A = false; }
set_quantifier_opt(A) ::= DISTINCT.                                               { A = true; }
set_quantifier_opt(A) ::= ALL.                                                    { A = false; }

%type select_list                                                                 { SNodeList* }
%destructor select_list                                                           { nodesDestroyList($$); }
select_list(A) ::= NK_STAR.                                                       { A = NULL; }
select_list(A) ::= select_sublist(B).                                             { A = B; }

%type select_sublist                                                              { SNodeList* }
%destructor select_sublist                                                        { nodesDestroyList($$); }
select_sublist(A) ::= select_item(B).                                             { A = createNodeList(pCxt, B); }
select_sublist(A) ::= select_sublist(B) NK_COMMA select_item(C).                  { A = addNodeToList(pCxt, B, C); }

select_item(A) ::= common_expression(B).                                          {
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &t);
                                                                                  }
select_item(A) ::= common_expression(B) column_alias(C).                          { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= common_expression(B) AS column_alias(C).                       { A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                               { A = createColumnNode(pCxt, &B, &C); }

where_clause_opt(A) ::= .                                                         { A = NULL; }
where_clause_opt(A) ::= WHERE search_condition(B).                                { A = B; }

%type partition_by_clause_opt                                                     { SNodeList* }
%destructor partition_by_clause_opt                                               { nodesDestroyList($$); }
partition_by_clause_opt(A) ::= .                                                  { A = NULL; }
partition_by_clause_opt(A) ::= PARTITION BY expression_list(B).                   { A = B; }

twindow_clause_opt(A) ::= .                                                       { A = NULL; }
twindow_clause_opt(A) ::=
  SESSION NK_LP column_reference(B) NK_COMMA duration_literal(C) NK_RP.           { A = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C)); }
twindow_clause_opt(A) ::= STATE_WINDOW NK_LP column_reference(B) NK_RP.           { A = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, B)); }
twindow_clause_opt(A) ::=
  INTERVAL NK_LP duration_literal(B) NK_RP sliding_opt(C) fill_opt(D).            { A = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, B), NULL, C, D); }
twindow_clause_opt(A) ::=
  INTERVAL NK_LP duration_literal(B) NK_COMMA duration_literal(C) NK_RP 
  sliding_opt(D) fill_opt(E).                                                     { A = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), D, E); }

sliding_opt(A) ::= .                                                              { A = NULL; }
sliding_opt(A) ::= SLIDING NK_LP duration_literal(B) NK_RP.                       { A = releaseRawExprNode(pCxt, B); }

fill_opt(A) ::= .                                                                 { A = NULL; }
fill_opt(A) ::= FILL NK_LP fill_mode(B) NK_RP.                                    { A = createFillNode(pCxt, B, NULL); }
fill_opt(A) ::= FILL NK_LP VALUE NK_COMMA literal_list(B) NK_RP.                  { A = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, B)); }  

%type fill_mode                                                                   { EFillMode }
%destructor fill_mode                                                             { }
fill_mode(A) ::= NONE.                                                            { A = FILL_MODE_NONE; }
fill_mode(A) ::= PREV.                                                            { A = FILL_MODE_PREV; }
fill_mode(A) ::= NULL.                                                            { A = FILL_MODE_NULL; }
fill_mode(A) ::= LINEAR.                                                          { A = FILL_MODE_LINEAR; }
fill_mode(A) ::= NEXT.                                                            { A = FILL_MODE_NEXT; }

%type group_by_clause_opt                                                         { SNodeList* }
%destructor group_by_clause_opt                                                   { nodesDestroyList($$); }
group_by_clause_opt(A) ::= .                                                      { A = NULL; }
group_by_clause_opt(A) ::= GROUP BY group_by_list(B).                             { A = B; }

%type group_by_list                                                             { SNodeList* }
%destructor group_by_list                                                       { nodesDestroyList($$); }
group_by_list(A) ::= expression(B).                                             { A = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, B))); }
group_by_list(A) ::= group_by_list(B) NK_COMMA expression(C).                   { A = addNodeToList(pCxt, B, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, C))); }

having_clause_opt(A) ::= .                                                        { A = NULL; }
having_clause_opt(A) ::= HAVING search_condition(B).                              { A = B; }

/************************************************ query_expression ****************************************************/
query_expression(A) ::= 
  query_expression_body(B) 
    order_by_clause_opt(C) slimit_clause_opt(D) limit_clause_opt(E).              { 
                                                                                    A = addOrderByClause(pCxt, B, C);
                                                                                    A = addSlimitClause(pCxt, A, D);
                                                                                    A = addLimitClause(pCxt, A, E);
                                                                                  }

query_expression_body(A) ::= query_primary(B).                                    { A = B; }
query_expression_body(A) ::=
  query_expression_body(B) UNION ALL query_expression_body(D).                    { A = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, B, D); }

query_primary(A) ::= query_specification(B).                                      { A = B; }
//query_primary(A) ::=
//  NK_LP query_expression_body(B) 
//    order_by_clause_opt slimit_clause_opt limit_clause_opt NK_RP.                 { A = B; }

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

/************************************************ search_condition ****************************************************/
search_condition(A) ::= common_expression(B).                                     { A = releaseRawExprNode(pCxt, B); }

/************************************************ sort_specification_list *********************************************/
%type sort_specification_list                                                     { SNodeList* }
%destructor sort_specification_list                                               { nodesDestroyList($$); }
sort_specification_list(A) ::= sort_specification(B).                             { A = createNodeList(pCxt, B); }
sort_specification_list(A) ::=
  sort_specification_list(B) NK_COMMA sort_specification(C).                      { A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::= 
  expression(B) ordering_specification_opt(C) null_ordering_opt(D).               { A = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, B), C, D); }

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
