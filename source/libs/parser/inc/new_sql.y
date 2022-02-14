//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y

%name NewParse

%token_prefix NEW_TK_
%token_type { SToken }
%default_type { SNode* }
%default_destructor { PARSER_DESTRUCTOR_TRACE; nodesDestroyNode($$); }

%extra_argument { SAstCreateContext* pCxt }

%include {
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "nodes.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "astCreateFuncs.h"

#if 0
#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
#define PARSER_COMPLETE printf("parsing complete!\n" )
#else
#define PARSER_TRACE
#define PARSER_DESTRUCTOR_TRACE
#define PARSER_COMPLETE
#endif
}

%syntax_error {  
  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > pCxt->pQueryCxt->msgLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        sprintf(pCxt->pQueryCxt->pMsg, msg, tmpstr);
    } else {
        sprintf(pCxt->pQueryCxt->pMsg, msg, &TOKEN.z[0]);
    }
  } else {
    sprintf(pCxt->pQueryCxt->pMsg, "Incomplete SQL statement");
  }
  pCxt->valid = false;
}

%parse_accept       { PARSER_COMPLETE; }

%left OR.
%left AND.
%left UNION ALL MINUS EXCEPT INTERSECT.
//%left BITAND BITOR LSHIFT RSHIFT.
%left NK_PLUS NK_MINUS.
//%left DIVIDE TIMES.
%left NK_STAR NK_SLASH NK_REM.
//%left CONCAT.
//%right UMINUS UPLUS BITNOT.

cmd ::= SHOW DATABASES.                                                           { PARSER_TRACE; createShowStmt(pCxt, SHOW_TYPE_DATABASE); }
cmd ::= query_expression(A).                                                      { PARSER_TRACE; pCxt->pRootNode = A; }

/************************************************ literal *************************************************************/
literal(A) ::= NK_INTEGER(B).                                                     { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B)); }
literal(A) ::= NK_FLOAT(B).                                                       { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B)); }
literal(A) ::= NK_STRING(B).                                                      { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &B)); }
literal(A) ::= NK_BOOL(B).                                                        { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &B)); }
literal(A) ::= TIMESTAMP(B) NK_STRING(C).                                         { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &C, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &C)); }
literal(A) ::= duration_literal(B).                                               { PARSER_TRACE; A = B; }

duration_literal(A) ::= NK_VARIABLE(B).                                           { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createDurationValueNode(pCxt, &B)); }

%type literal_list                                                                { SNodeList* }
%destructor literal_list                                                          { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
literal_list(A) ::= literal(B).                                                   { PARSER_TRACE; A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
literal_list(A) ::= literal_list(B) NK_COMMA literal(C).                          { PARSER_TRACE; A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

/************************************************ names and identifiers ***********************************************/
%type db_name                                                                     { SToken }
%destructor db_name                                                               { PARSER_DESTRUCTOR_TRACE; }
db_name(A) ::= NK_ID(B).                                                          { PARSER_TRACE; A = B; }

%type table_name                                                                  { SToken }
%destructor table_name                                                            { PARSER_DESTRUCTOR_TRACE; }
table_name(A) ::= NK_ID(B).                                                       { PARSER_TRACE; A = B; }

%type column_name                                                                 { SToken }
%destructor column_name                                                           { PARSER_DESTRUCTOR_TRACE; }
column_name(A) ::= NK_ID(B).                                                      { PARSER_TRACE; A = B; }

%type function_name                                                               { SToken }
%destructor function_name                                                         { PARSER_DESTRUCTOR_TRACE; }
function_name(A) ::= NK_ID(B).                                                    { PARSER_TRACE; A = B; }

%type table_alias                                                                 { SToken }
%destructor table_alias                                                           { PARSER_DESTRUCTOR_TRACE; }
table_alias(A) ::= NK_ID(B).                                                      { PARSER_TRACE; A = B; }

%type column_alias                                                                { SToken }
%destructor column_alias                                                          { PARSER_DESTRUCTOR_TRACE; }
column_alias(A) ::= NK_ID(B).                                                     { PARSER_TRACE; A = B; }

/************************************************ expression **********************************************************/
expression(A) ::= literal(B).                                                     { PARSER_TRACE; A = B; }
//expression(A) ::= NK_QUESTION(B).                                                 { PARSER_TRACE; A = B; }
//expression(A) ::= pseudo_column(B).                                               { PARSER_TRACE; A = B; }
expression(A) ::= column_reference(B).                                            { PARSER_TRACE; A = B; }
expression(A) ::= function_name(B) NK_LP expression_list(C) NK_RP(D).             { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, C)); }
expression(A) ::= function_name(B) NK_LP NK_STAR(C) NK_RP(D).                     { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &D, createFunctionNode(pCxt, &B, createNodeList(pCxt, createColumnNode(pCxt, NULL, &C)))); }
//expression(A) ::= cast_expression(B).                                             { PARSER_TRACE; A = B; }
//expression(A) ::= case_expression(B).                                             { PARSER_TRACE; A = B; }
expression(A) ::= subquery(B).                                                    { PARSER_TRACE; A = B; }
expression(A) ::= NK_LP(B) expression(C) NK_RP(D).                                { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &D, releaseRawExprNode(pCxt, C)); }
expression(A) ::= NK_PLUS(B) expression(C).                                       {
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, releaseRawExprNode(pCxt, C));
                                                                                  }
expression(A) ::= NK_MINUS(B) expression(C).                                      {
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &B, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, C), NULL));
                                                                                  }
expression(A) ::= expression(B) NK_PLUS expression(C).                            {
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_MINUS expression(C).                           {
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_STAR expression(C).                            {
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_SLASH expression(C).                           {
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }
expression(A) ::= expression(B) NK_REM expression(C).                             {
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, B);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, C);
                                                                                    A = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C))); 
                                                                                  }

%type expression_list                                                             { SNodeList* }
%destructor expression_list                                                       { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
expression_list(A) ::= expression(B).                                             { PARSER_TRACE; A = createNodeList(pCxt, releaseRawExprNode(pCxt, B)); }
expression_list(A) ::= expression_list(B) NK_COMMA expression(C).                 { PARSER_TRACE; A = addNodeToList(pCxt, B, releaseRawExprNode(pCxt, C)); }

column_reference(A) ::= column_name(B).                                           { PARSER_TRACE; A = createRawExprNode(pCxt, &B, createColumnNode(pCxt, NULL, &B)); }
column_reference(A) ::= table_name(B) NK_DOT column_name(C).                      { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &C, createColumnNode(pCxt, &B, &C)); }

//pseudo_column(A) ::= NK_NOW.                                                      { PARSER_TRACE; A = createFunctionNode(pCxt, NULL, NULL); }

/************************************************ predicate ***********************************************************/
predicate(A) ::= expression(B) compare_op(C) expression(D).                       { PARSER_TRACE; A = createOperatorNode(pCxt, C, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)); }
//predicate(A) ::= expression(B) compare_op sub_type expression(B).
predicate(A) ::= expression(B) BETWEEN expression(C) AND expression(D).           { PARSER_TRACE; A = createBetweenAnd(pCxt, releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, D)); }
predicate(A) ::= expression(B) NOT BETWEEN expression(C) AND expression(D).       { PARSER_TRACE; A = createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, C), releaseRawExprNode(pCxt, B), releaseRawExprNode(pCxt, D)); }
predicate(A) ::= expression(B) IS NULL.                                           { PARSER_TRACE; A = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, B), true); }
predicate(A) ::= expression(B) IS NOT NULL.                                       { PARSER_TRACE; A = createIsNullCondNode(pCxt, releaseRawExprNode(pCxt, B), false); }
predicate(A) ::= expression(B) in_op(C) in_predicate_value(D).                    { PARSER_TRACE; A = createOperatorNode(pCxt, C, releaseRawExprNode(pCxt, B), D); }

%type compare_op                                                                  { EOperatorType }
%destructor compare_op                                                            { PARSER_DESTRUCTOR_TRACE; }
compare_op(A) ::= NK_LT.                                                          { PARSER_TRACE; A = OP_TYPE_LOWER_THAN; }
compare_op(A) ::= NK_GT.                                                          { PARSER_TRACE; A = OP_TYPE_GREATER_THAN; }
compare_op(A) ::= NK_LE.                                                          { PARSER_TRACE; A = OP_TYPE_LOWER_EQUAL; }
compare_op(A) ::= NK_GE.                                                          { PARSER_TRACE; A = OP_TYPE_GREATER_EQUAL; }
compare_op(A) ::= NK_NE.                                                          { PARSER_TRACE; A = OP_TYPE_NOT_EQUAL; }
compare_op(A) ::= NK_EQ.                                                          { PARSER_TRACE; A = OP_TYPE_EQUAL; }
compare_op(A) ::= LIKE.                                                           { PARSER_TRACE; A = OP_TYPE_LIKE; }
compare_op(A) ::= NOT LIKE.                                                       { PARSER_TRACE; A = OP_TYPE_NOT_LIKE; }
compare_op(A) ::= MATCH.                                                          { PARSER_TRACE; A = OP_TYPE_MATCH; }
compare_op(A) ::= NMATCH.                                                         { PARSER_TRACE; A = OP_TYPE_NMATCH; }

%type in_op                                                                       { EOperatorType }
%destructor in_op                                                                 { PARSER_DESTRUCTOR_TRACE; }
in_op(A) ::= IN.                                                                  { PARSER_TRACE; A = OP_TYPE_IN; }
in_op(A) ::= NOT IN.                                                              { PARSER_TRACE; A = OP_TYPE_NOT_IN; }

in_predicate_value(A) ::= NK_LP expression_list(B) NK_RP.                         { PARSER_TRACE; A = createNodeListNode(pCxt, B); }

/************************************************ boolean_value_expression ********************************************/
boolean_value_expression(A) ::= boolean_primary(B).                               { PARSER_TRACE; A = B; }
boolean_value_expression(A) ::= NOT boolean_primary(B).                           { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, B, NULL); }
boolean_value_expression(A) ::=
  boolean_value_expression(B) OR boolean_value_expression(C).                     { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, B, C); }
boolean_value_expression(A) ::=
  boolean_value_expression(B) AND boolean_value_expression(C).                    { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, B, C); }

boolean_primary(A) ::= predicate(B).                                              { PARSER_TRACE; A = B; }
boolean_primary(A) ::= NK_LP boolean_value_expression(B) NK_RP.                   { PARSER_TRACE; A = B; }

/************************************************ from_clause *********************************************************/
from_clause(A) ::= FROM table_reference_list(B).                                  { PARSER_TRACE; A = B; }

table_reference_list(A) ::= table_reference(B).                                   { PARSER_TRACE; A = B; }
table_reference_list(A) ::= table_reference_list(B) NK_COMMA table_reference(C).  { PARSER_TRACE; A = createJoinTableNode(pCxt, JOIN_TYPE_INNER, B, C, NULL); }

/************************************************ table_reference *****************************************************/
table_reference(A) ::= table_primary(B).                                          { PARSER_TRACE; A = B; }
table_reference(A) ::= joined_table(B).                                           { PARSER_TRACE; A = B; }

table_primary(A) ::= table_name(B) alias_opt(C).                                  { PARSER_TRACE; A = createRealTableNode(pCxt, NULL, &B, &C); }
table_primary(A) ::= db_name(B) NK_DOT table_name(C) alias_opt(D).                { PARSER_TRACE; A = createRealTableNode(pCxt, &B, &C, &D); }
table_primary(A) ::= subquery(B) alias_opt(C).                                    { PARSER_TRACE; A = createTempTableNode(pCxt, releaseRawExprNode(pCxt, B), &C); }
table_primary(A) ::= parenthesized_joined_table(B).                               { PARSER_TRACE; A = B; }

%type alias_opt                                                                   { SToken }
%destructor alias_opt                                                             { PARSER_DESTRUCTOR_TRACE; }
alias_opt(A) ::= .                                                                { PARSER_TRACE; A = nil_token;  }
alias_opt(A) ::= table_alias(B).                                                  { PARSER_TRACE; A = B; }
alias_opt(A) ::= AS table_alias(B).                                               { PARSER_TRACE; A = B; }

parenthesized_joined_table(A) ::= NK_LP joined_table(B) NK_RP.                    { PARSER_TRACE; A = B; }
parenthesized_joined_table(A) ::= NK_LP parenthesized_joined_table(B) NK_RP.      { PARSER_TRACE; A = B; }

/************************************************ joined_table ********************************************************/
joined_table(A) ::=
  table_reference(B) join_type(C) JOIN table_reference(D) ON search_condition(E). { PARSER_TRACE; A = createJoinTableNode(pCxt, C, B, D, E); }

%type join_type                                                                   { EJoinType }
%destructor join_type                                                             { PARSER_DESTRUCTOR_TRACE; }
join_type(A) ::= .                                                                { PARSER_TRACE; A = JOIN_TYPE_INNER; }
join_type(A) ::= INNER.                                                           { PARSER_TRACE; A = JOIN_TYPE_INNER; }

/************************************************ query_specification *************************************************/
query_specification(A) ::=
  SELECT set_quantifier_opt(B) select_list(C) from_clause(D) where_clause_opt(E) 
    partition_by_clause_opt(F) twindow_clause_opt(G) 
    group_by_clause_opt(H) having_clause_opt(I).                                  { 
                                                                                    PARSER_TRACE;
                                                                                    A = createSelectStmt(pCxt, B, C, D);
                                                                                    A = addWhereClause(pCxt, A, E);
                                                                                    A = addPartitionByClause(pCxt, A, F);
                                                                                    A = addWindowClauseClause(pCxt, A, G);
                                                                                    A = addGroupByClause(pCxt, A, H);
                                                                                    A = addHavingClause(pCxt, A, I);
                                                                                  }

%type set_quantifier_opt                                                          { bool }
%destructor set_quantifier_opt                                                    { PARSER_DESTRUCTOR_TRACE; }
set_quantifier_opt(A) ::= .                                                       { PARSER_TRACE; A = false; }
set_quantifier_opt(A) ::= DISTINCT.                                               { PARSER_TRACE; A = true; }
set_quantifier_opt(A) ::= ALL.                                                    { PARSER_TRACE; A = false; }

%type select_list                                                                 { SNodeList* }
%destructor select_list                                                           { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
select_list(A) ::= NK_STAR.                                                       { PARSER_TRACE; A = NULL; }
select_list(A) ::= select_sublist(B).                                             { PARSER_TRACE; A = B; }

%type select_sublist                                                              { SNodeList* }
%destructor select_sublist                                                        { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
select_sublist(A) ::= select_item(B).                                             { PARSER_TRACE; A = createNodeList(pCxt, B); }
select_sublist(A) ::= select_sublist(B) NK_COMMA select_item(C).                  { PARSER_TRACE; A = addNodeToList(pCxt, B, C); }

select_item(A) ::= expression(B).                                                 {
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, B);
                                                                                    A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &t);
                                                                                  }
select_item(A) ::= expression(B) column_alias(C).                                 { PARSER_TRACE; A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= expression(B) AS column_alias(C).                              { PARSER_TRACE; A = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, B), &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                               { PARSER_TRACE; A = createColumnNode(pCxt, &B, &C); }

where_clause_opt(A) ::= .                                                         { PARSER_TRACE; A = NULL; }
where_clause_opt(A) ::= WHERE search_condition(B).                                { PARSER_TRACE; A = B; }

%type partition_by_clause_opt                                                     { SNodeList* }
%destructor partition_by_clause_opt                                               { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
partition_by_clause_opt(A) ::= .                                                  { PARSER_TRACE; A = NULL; }
partition_by_clause_opt(A) ::= PARTITION BY expression_list(B).                   { PARSER_TRACE; A = B; }

twindow_clause_opt(A) ::= .                                                       { PARSER_TRACE; A = NULL; }
twindow_clause_opt(A) ::=
  SESSION NK_LP column_reference(B) NK_COMMA NK_INTEGER(C) NK_RP.                 { PARSER_TRACE; A = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, B), &C); }
twindow_clause_opt(A) ::= STATE_WINDOW NK_LP column_reference(B) NK_RP.           { PARSER_TRACE; A = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, B)); }
twindow_clause_opt(A) ::=
  INTERVAL NK_LP duration_literal(B) NK_RP sliding_opt(C) fill_opt(D).            { PARSER_TRACE; A = createIntervalWindowNode(pCxt, B, NULL, C, D); }
twindow_clause_opt(A) ::=
  INTERVAL NK_LP duration_literal(B) NK_COMMA duration_literal(C) NK_RP 
  sliding_opt(D) fill_opt(E).                                                     { PARSER_TRACE; A = createIntervalWindowNode(pCxt, B, C, D, E); }

sliding_opt(A) ::= .                                                              { PARSER_TRACE; A = NULL; }
sliding_opt(A) ::= SLIDING NK_LP duration_literal(B) NK_RP.                       { PARSER_TRACE; A = B; }

fill_opt(A) ::= .                                                                 { PARSER_TRACE; A = NULL; }
fill_opt(A) ::= FILL NK_LP fill_mode(B) NK_RP.                                    { PARSER_TRACE; A = createFillNode(pCxt, B, NULL); }
fill_opt(A) ::= FILL NK_LP VALUE NK_COMMA literal_list(B) NK_RP.                  { PARSER_TRACE; A = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, B)); }  

%type fill_mode                                                                   { EFillMode }
%destructor fill_mode                                                             { PARSER_DESTRUCTOR_TRACE; }
fill_mode(A) ::= NONE.                                                            { PARSER_TRACE; A = FILL_MODE_NONE; }
fill_mode(A) ::= PREV.                                                            { PARSER_TRACE; A = FILL_MODE_PREV; }
fill_mode(A) ::= NULL.                                                            { PARSER_TRACE; A = FILL_MODE_NULL; }
fill_mode(A) ::= LINEAR.                                                          { PARSER_TRACE; A = FILL_MODE_LINEAR; }
fill_mode(A) ::= NEXT.                                                            { PARSER_TRACE; A = FILL_MODE_NEXT; }

%type group_by_clause_opt                                                         { SNodeList* }
%destructor group_by_clause_opt                                                   { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
group_by_clause_opt(A) ::= .                                                      { PARSER_TRACE; A = NULL; }
group_by_clause_opt(A) ::= GROUP BY group_by_list(B).                             { PARSER_TRACE; A = B; }

%type group_by_list                                                             { SNodeList* }
%destructor group_by_list                                                       { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
group_by_list(A) ::= expression(B).                                             { PARSER_TRACE; A = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, B))); }
group_by_list(A) ::= group_by_list(B) NK_COMMA expression(C).                   { PARSER_TRACE; A = addNodeToList(pCxt, B, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, C))); }

having_clause_opt(A) ::= .                                                        { PARSER_TRACE; A = NULL; }
having_clause_opt(A) ::= HAVING search_condition(B).                              { PARSER_TRACE; A = B; }

/************************************************ query_expression ****************************************************/
query_expression(A) ::= 
  query_expression_body(B) 
    order_by_clause_opt(C) slimit_clause_opt(D) limit_clause_opt(E).              { 
                                                                                    PARSER_TRACE;
                                                                                    A = addOrderByClause(pCxt, B, C);
                                                                                    A = addSlimitClause(pCxt, A, D);
                                                                                    A = addLimitClause(pCxt, A, E);
                                                                                  }

query_expression_body(A) ::= query_primary(B).                                    { PARSER_TRACE; A = B; }
query_expression_body(A) ::=
  query_expression_body(B) UNION ALL query_expression_body(D).                    { PARSER_TRACE; A = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, B, D); }

query_primary(A) ::= query_specification(B).                                      { PARSER_TRACE; A = B; }
//query_primary(A) ::=
//  NK_LP query_expression_body(B) 
//    order_by_clause_opt slimit_clause_opt limit_clause_opt NK_RP.                 { PARSER_TRACE; A = B;}

%type order_by_clause_opt                                                         { SNodeList* }
%destructor order_by_clause_opt                                                   { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
order_by_clause_opt(A) ::= .                                                      { PARSER_TRACE; A = NULL; }
order_by_clause_opt(A) ::= ORDER BY sort_specification_list(B).                   { PARSER_TRACE; A = B; }

slimit_clause_opt(A) ::= .                                                        { PARSER_TRACE; A = NULL; }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(B).                                    { PARSER_TRACE; A = createLimitNode(pCxt, &B, NULL); }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(B) SOFFSET NK_INTEGER(C).              { PARSER_TRACE; A = createLimitNode(pCxt, &B, &C); }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).             { PARSER_TRACE; A = createLimitNode(pCxt, &B, &C); }

limit_clause_opt(A) ::= .                                                         { PARSER_TRACE; A = NULL; }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(B).                                      { PARSER_TRACE; A = createLimitNode(pCxt, &B, NULL); }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(B) OFFSET NK_INTEGER(C).                 { PARSER_TRACE; A = createLimitNode(pCxt, &B, &C); }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).               { PARSER_TRACE; A = createLimitNode(pCxt, &B, &C); }

/************************************************ subquery ************************************************************/
subquery(A) ::= NK_LP(B) query_expression(C) NK_RP(D).                                  { PARSER_TRACE; A = createRawExprNodeExt(pCxt, &B, &D, C); }

/************************************************ search_condition ****************************************************/
search_condition(A) ::= boolean_value_expression(B).                              { PARSER_TRACE; A = B; }

/************************************************ sort_specification_list *********************************************/
%type sort_specification_list                                                     { SNodeList* }
%destructor sort_specification_list                                               { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
sort_specification_list(A) ::= sort_specification(B).                             { PARSER_TRACE; A = createNodeList(pCxt, B); }
sort_specification_list(A) ::=
  sort_specification_list(B) NK_COMMA sort_specification(C).                      { PARSER_TRACE; A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::= 
  expression(B) ordering_specification_opt(C) null_ordering_opt(D).               { PARSER_TRACE; A = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, B), C, D); }

%type ordering_specification_opt EOrder
%destructor ordering_specification_opt                                            { PARSER_DESTRUCTOR_TRACE; }
ordering_specification_opt(A) ::= .                                               { PARSER_TRACE; A = ORDER_ASC; }
ordering_specification_opt(A) ::= ASC.                                            { PARSER_TRACE; A = ORDER_ASC; }
ordering_specification_opt(A) ::= DESC.                                           { PARSER_TRACE; A = ORDER_DESC; }

%type null_ordering_opt ENullOrder
%destructor null_ordering_opt                                                     { PARSER_DESTRUCTOR_TRACE; }
null_ordering_opt(A) ::= .                                                        { PARSER_TRACE; A = NULL_ORDER_DEFAULT; }
null_ordering_opt(A) ::= NULLS FIRST.                                             { PARSER_TRACE; A = NULL_ORDER_FIRST; }
null_ordering_opt(A) ::= NULLS LAST.                                              { PARSER_TRACE; A = NULL_ORDER_LAST; }
