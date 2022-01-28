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

#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
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

%parse_accept       { printf("parsing complete!\n" );}

%left OR.
%left AND.
%right NOT.
%left UNION ALL MINUS EXCEPT INTERSECT.
//%left EQ NE ISNULL NOTNULL IS LIKE MATCH NMATCH GLOB BETWEEN IN.
//%left GT GE LT LE.
//%left BITAND BITOR LSHIFT RSHIFT.
%left NK_PLUS NK_MINUS.
//%left DIVIDE TIMES.
%left NK_STAR NK_SLASH. //REM.
//%left CONCAT.
//%right UMINUS UPLUS BITNOT.

cmd ::= SHOW DATABASES.                                                           { PARSER_TRACE; createShowStmt(pCxt, SHOW_TYPE_DATABASE); }
cmd ::= query_expression(A).                                                      { PARSER_TRACE; pCxt->pRootNode = A; }

/*5.4*********************************************** literal *********************************************************/
unsigned_integer(A) ::= NK_INTEGER(B).                                            { A = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &B); }
unsigned_approximate_numeric(A) ::= NK_FLOAT(B).                                  { A = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &B); }

signed_integer(A) ::= unsigned_integer(B).                                        { A = B; }
signed_integer(A) ::= NK_PLUS unsigned_integer(B).                                { A = B; }
signed_integer(A) ::= NK_MINUS unsigned_integer(B).                               { A = addMinusSign(pCxt, B);}

unsigned_literal ::= unsigned_numeric_literal.
unsigned_literal ::= general_literal.

unsigned_numeric_literal ::= unsigned_integer.
unsigned_numeric_literal ::= unsigned_approximate_numeric.

general_literal ::= NK_STRING.
general_literal ::= NK_BOOL.
general_literal ::= NK_NOW.

/*5.4*********************************************** names and identifiers *********************************************************/
%type db_name                                                                     { SToken }
%destructor db_name                                                               { PARSER_DESTRUCTOR_TRACE; }
db_name(A) ::= NK_ID(B).                                                          { PARSER_TRACE; A = B; }

%type table_name                                                                  { SToken }
%destructor table_name                                                            { PARSER_DESTRUCTOR_TRACE; }
table_name(A) ::= NK_ID(B).                                                       { PARSER_TRACE; A = B; }

%type column_name                                                                 { SToken }
%destructor column_name                                                           { PARSER_DESTRUCTOR_TRACE; }
column_name(A) ::= NK_ID(B).                                                      { PARSER_TRACE; A = B; }

%type table_alias                                                                 { SToken }
%destructor table_alias                                                           { PARSER_DESTRUCTOR_TRACE; }
table_alias(A) ::= NK_ID(B).                                                      { PARSER_TRACE; A = B; }

/*6.4*********************************************** value_specification *********************************************************/
unsigned_value_specification ::= unsigned_literal.
unsigned_value_specification ::= NK_QUESTION.

/*6.35 todo *********************************************** value_expression_primary *********************************************************/
value_expression_primary ::= NK_LP value_expression NK_RP.
value_expression_primary ::= nonparenthesized_value_expression_primary.

nonparenthesized_value_expression_primary ::= unsigned_value_specification.
nonparenthesized_value_expression_primary ::= column_reference.
//nonparenthesized_value_expression_primary ::= agg_function.
nonparenthesized_value_expression_primary ::= subquery.
//nonparenthesized_value_expression_primary ::= case_expression. // todo
//nonparenthesized_value_expression_primary ::= cast_specification. // todo

column_reference(A) ::= column_name(B).                                           { PARSER_TRACE; A = createColumnNode(pCxt, NULL, &B); }
column_reference(A) ::= table_name(B) NK_DOT column_name(C).                      { PARSER_TRACE; A = createColumnNode(pCxt, &B, &C); }

/*6.35*********************************************** boolean_value_expression *********************************************************/
boolean_value_expression(A) ::= boolean_primary(B).                               { PARSER_TRACE; A = B; }
boolean_value_expression(A) ::= NOT boolean_primary(B).                           { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, B, NULL); }
boolean_value_expression(A) ::=
  boolean_value_expression(B) OR boolean_value_expression(C).                     { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, B, C); }
boolean_value_expression(A) ::=
  boolean_value_expression(B) AND boolean_value_expression(C).                    { PARSER_TRACE; A = createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, B, C); }

boolean_primary(A) ::= predicate(B).                                              { PARSER_TRACE; A = B; }
boolean_primary(A) ::= NK_LP boolean_value_expression(B) NK_RP.                   { PARSER_TRACE; A = B; }

/*7.5*********************************************** from_clause *********************************************************/
from_clause(A) ::= FROM table_reference_list(B).                                  { PARSER_TRACE; A = B; }

table_reference_list(A) ::= table_reference(B).                                   { PARSER_TRACE; A = B; }
table_reference_list(A) ::= table_reference_list(B) NK_COMMA table_reference(C).  { PARSER_TRACE; A = createJoinTableNode(pCxt, JOIN_TYPE_INNER, B, C, NULL); }

/*7.6*********************************************** table_reference *****************************************************/
table_reference(A) ::= table_primary(B).                                          { PARSER_TRACE; A = B; }
table_reference(A) ::= joined_table(B).                                           { PARSER_TRACE; A = B; }

table_primary(A) ::= table_name(B) correlation_or_recognition_opt(C).             { PARSER_TRACE; A = createRealTableNode(pCxt, NULL, &B, &C); }
table_primary(A) ::=
  db_name(B) NK_DOT table_name(C) correlation_or_recognition_opt(D).              { PARSER_TRACE; A = createRealTableNode(pCxt, &B, &C, &D); }
table_primary(A) ::= subquery(B) correlation_or_recognition_opt(C).               { PARSER_TRACE; A = createTempTableNode(pCxt, B, &C); }
table_primary ::= parenthesized_joined_table.

%type correlation_or_recognition_opt                                              { SToken }
%destructor correlation_or_recognition_opt                                        { PARSER_DESTRUCTOR_TRACE; }
correlation_or_recognition_opt(A) ::= .                                           { PARSER_TRACE; A = nil_token;  }
correlation_or_recognition_opt(A) ::= table_alias(B).                             { PARSER_TRACE; A = B; }
correlation_or_recognition_opt(A) ::= AS table_alias(B).                          { PARSER_TRACE; A = B; }

parenthesized_joined_table(A) ::= NK_LP joined_table(B) NK_RP.                    { PARSER_TRACE; A = B; }
parenthesized_joined_table(A) ::= NK_LP parenthesized_joined_table(B) NK_RP.      { PARSER_TRACE; A = B; }

/*7.10*********************************************** joined_table ************************************************************/
joined_table(A) ::=
  table_reference(B) join_type(C) JOIN table_reference(D) ON search_condition(E). { PARSER_TRACE; A = createJoinTableNode(pCxt, C, B, D, E); }

%type join_type                                                                   { EJoinType }
%destructor join_type                                                             { PARSER_DESTRUCTOR_TRACE; }
join_type(A) ::= INNER.                                                           { PARSER_TRACE; A = JOIN_TYPE_INNER; }

/*7.15*********************************************** query_specification *************************************************/
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

select_item(A) ::= value_expression(B).                                           { PARSER_TRACE; A = B; }
select_item(A) ::= value_expression(B) NK_ID(C).                                  { PARSER_TRACE; A = setProjectionAlias(pCxt, B, &C); }
select_item(A) ::= value_expression(B) AS NK_ID(C).                               { PARSER_TRACE; A = setProjectionAlias(pCxt, B, &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                               { PARSER_TRACE; A = createColumnNode(pCxt, &B, &C); }

/*7.16*********************************************** query_expression ****************************************************/
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
query_primary(A) ::=
  NK_LP query_expression_body(B) 
    order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP.                 { PARSER_TRACE; A = B;}

%type order_by_clause_opt                                                         { SNodeList* }
%destructor order_by_clause_opt                                                   { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
order_by_clause_opt(A) ::= .                                                      { PARSER_TRACE; A = NULL; }
order_by_clause_opt(A) ::= ORDER BY sort_specification_list(B).                   { PARSER_TRACE; A = B; }

slimit_clause_opt(A) ::= .                                                        { PARSER_TRACE; A = NULL; }
slimit_clause_opt(A) ::= SLIMIT signed_integer(B).                                { PARSER_TRACE; A = createLimitNode(pCxt, B, 0); }
slimit_clause_opt(A) ::= SLIMIT signed_integer(B) SOFFSET signed_integer(C).      { PARSER_TRACE; A = createLimitNode(pCxt, B, C); }
slimit_clause_opt(A) ::= SLIMIT signed_integer(C) NK_COMMA signed_integer(B).     { PARSER_TRACE; A = createLimitNode(pCxt, B, C); }

limit_clause_opt(A) ::= .                                                         { PARSER_TRACE; A = NULL; }
limit_clause_opt(A) ::= LIMIT signed_integer(B).                                  { PARSER_TRACE; A = createLimitNode(pCxt, B, 0); }
limit_clause_opt(A) ::= LIMIT signed_integer(B) OFFSET signed_integer(C).         { PARSER_TRACE; A = createLimitNode(pCxt, B, C); }
limit_clause_opt(A) ::= LIMIT signed_integer(C) NK_COMMA signed_integer(B).       { PARSER_TRACE; A = createLimitNode(pCxt, B, C); }

/*7.18*********************************************** subquery ************************************************************/
subquery(A) ::= NK_LR query_expression(B) NK_RP.                                  { PARSER_TRACE; A = B; }

/*8.1*********************************************** predicate ************************************************************/
predicate ::= .

/*8.21 todo *********************************************** search_condition ************************************************************/
search_condition(A) ::= boolean_value_expression(B).                              { PARSER_TRACE; A = B; }

/*10.10*********************************************** sort_specification_list ************************************************************/
%type sort_specification_list                                                     { SNodeList* }
%destructor sort_specification_list                                               { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
sort_specification_list(A) ::= sort_specification(B).                             { PARSER_TRACE; A = createNodeList(pCxt, B); }
sort_specification_list(A) ::=
  sort_specification_list(B) NK_COMMA sort_specification(C).                      { PARSER_TRACE; A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::= 
  value_expression(B) ordering_specification_opt(C) null_ordering_opt(D).         { PARSER_TRACE; A = createOrderByExprNode(pCxt, B, C, D); }

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

/************************************************ todo ************************************************************/

where_clause_opt ::= .

%type partition_by_clause_opt                                                                                                              { SNodeList* }
%destructor partition_by_clause_opt                                                                                                        { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
partition_by_clause_opt ::=.

twindow_clause_opt ::= .

%type group_by_clause_opt                                                                                                              { SNodeList* }
%destructor group_by_clause_opt                                                                                                        { PARSER_DESTRUCTOR_TRACE; nodesDestroyList($$); }
group_by_clause_opt ::= .

having_clause_opt ::= .

//////////////////////// value_function /////////////////////////////////
value_function ::= NK_ID NK_LP value_expression NK_RP.
value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP.

//////////////////////// value_expression /////////////////////////////////
value_expression ::= common_value_expression.

common_value_expression ::= numeric_value_expression.

numeric_value_expression ::= numeric_primary.
numeric_value_expression ::= NK_PLUS numeric_primary.
numeric_value_expression ::= NK_MINUS numeric_primary.
numeric_value_expression ::= numeric_value_expression NK_PLUS numeric_value_expression.
numeric_value_expression ::= numeric_value_expression NK_MINUS numeric_value_expression.
numeric_value_expression ::= numeric_value_expression NK_STAR numeric_value_expression.
numeric_value_expression ::= numeric_value_expression NK_SLASH numeric_value_expression.

numeric_primary ::= value_expression_primary.
numeric_primary ::= value_function.