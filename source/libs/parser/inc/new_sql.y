//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y

%name NewParse

%token_prefix NEW_TK_
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
#include "ttoken.h"
#include "ttokendef.h"
#include "astCreateFuncs.h"

#define PARSER_TRACE printf("rule = %s\n", yyRuleName[yyruleno])
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

//%left OR.
//%left AND.
//%right NOT.
%left UNION ALL MINUS EXCEPT INTERSECT.
//%left EQ NE ISNULL NOTNULL IS LIKE MATCH NMATCH GLOB BETWEEN IN.
//%left GT GE LT LE.
//%left BITAND BITOR LSHIFT RSHIFT.
%left NK_PLUS NK_MINUS.
//%left DIVIDE TIMES.
%left NK_STAR NK_SLASH. //REM.
//%left CONCAT.
//%right UMINUS UPLUS BITNOT.

cmd ::= SHOW DATABASES.  { PARSER_TRACE; createShowStmt(pCxt, SHOW_TYPE_DATABASE); }

cmd ::= query_expression(A). { PARSER_TRACE; pCxt->pRootNode = A; }

//////////////////////// value_function /////////////////////////////////
value_function ::= NK_ID NK_LP value_expression NK_RP.
value_function ::= NK_ID NK_LP value_expression NK_COMMA value_expression NK_RP.

//////////////////////// value_expression_primary /////////////////////////////////
value_expression_primary ::= NK_LP value_expression NK_RP.
value_expression_primary ::= nonparenthesized_value_expression_primary.

nonparenthesized_value_expression_primary ::= literal.
// ?
nonparenthesized_value_expression_primary ::= column_reference.

literal ::= NK_LITERAL.

column_reference(A) ::= NK_ID(B).                      { PARSER_TRACE; A = createColumnNode(pCxt, NULL, &B); }
column_reference(A) ::= table_name(B) NK_DOT NK_ID(C). { PARSER_TRACE; A = createColumnNode(pCxt, &B, &C); }

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

//////////////////////// query_specification /////////////////////////////////
query_specification(A) ::= SELECT set_quantifier_opt(B) select_list(C) from_clause(D). { PARSER_TRACE; A = createSelectStmt(pCxt, B, C, D); }

%type set_quantifier_opt { bool }
%destructor set_quantifier_opt {}
set_quantifier_opt(A) ::= .         { PARSER_TRACE; A = false; }
set_quantifier_opt(A) ::= DISTINCT. { PARSER_TRACE; A = true; }
set_quantifier_opt(A) ::= ALL.      { PARSER_TRACE; A = false; }

%type select_list { SNodeList* }
%destructor select_list { nodesDestroyNodeList($$); }
select_list(A) ::= NK_STAR.           { PARSER_TRACE; A = NULL; }
select_list(A) ::= select_sublist(B). { PARSER_TRACE; A = B; }

%type select_sublist { SNodeList* }
%destructor select_sublist { nodesDestroyNodeList($$); }
select_sublist(A) ::= select_item(B).                            { PARSER_TRACE; A = createNodeList(pCxt, B); }
select_sublist(A) ::= select_sublist(B) NK_COMMA select_item(C). { PARSER_TRACE; A = addNodeToList(pCxt, B, C); }

select_item(A) ::= value_expression(B).                     { PARSER_TRACE; A = B; }
select_item(A) ::= value_expression(B) AS NK_ID(C).         { PARSER_TRACE; A = setProjectionAlias(pCxt, B, &C); }
select_item(A) ::= table_name(B) NK_DOT NK_STAR(C).                 { PARSER_TRACE; A = createColumnNode(pCxt, &B, &C); }

from_clause(A) ::= FROM table_reference_list(B). { PARSER_TRACE; A = B; }

//%type table_reference_list { SNodeList* }
//%destructor table_reference_list { nodesDestroyNodeList($$); }
table_reference_list(A) ::= table_reference(B).                               { PARSER_TRACE; A = B; }
//table_reference_list(A) ::= table_reference_list(B) NK_COMMA table_reference(C). { PARSER_TRACE; A = createJoinTableNode(pCxt, B, C); }

//table_reference(A) ::= NK_ID(B). { PARSER_TRACE; A = createRealTableNode(pCxt, ); }
table_reference(A) ::= table_factor(B). { PARSER_TRACE; A = B; }
//table_reference ::= joined_table.

table_factor(A) ::= table_primary(B). { PARSER_TRACE; A = B; }

table_primary(A) ::= table_name(B).                   { PARSER_TRACE; A = createRealTableNode(pCxt, NULL, &B); }
table_primary(A) ::= db_name(B) NK_DOT table_name(C). { PARSER_TRACE; A = createRealTableNode(pCxt, &B, &C); }
table_primary ::= derived_table.

derived_table ::= table_subquery.

%type db_name { SToken }
db_name(A) ::= NK_ID(B).    { PARSER_TRACE; A = B; }
%type table_name { SToken }
table_name(A) ::= NK_ID(B). { PARSER_TRACE; A = B; }

//////////////////////// subquery /////////////////////////////////
subquery ::= NK_LR query_expression NK_RP.

table_subquery ::= subquery.

// query_expression
query_expression(A) ::= with_clause_opt query_expression_body(B) order_by_clause_opt(C) slimit_clause_opt(D) limit_clause_opt(E). { 
                                                                                                                                    PARSER_TRACE;
                                                                                                                                    addOrderByList(pCxt, B, C);
                                                                                                                                    addSlimit(pCxt, B, D);
                                                                                                                                    addLimit(pCxt, B, E);
                                                                                                                                    A = B;  
                                                                                                                                  }

// WITH AS 
with_clause_opt ::= .                                                                                                             {}

query_expression_body(A) ::= query_primary(B).                                                                                    { PARSER_TRACE; A = B; }
query_expression_body(A) ::= query_expression_body(B) UNION ALL query_expression_body(D).                                         { PARSER_TRACE; A = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, B, D); }

query_primary(A) ::= query_specification(B).                                                                                      { PARSER_TRACE; A = B; }
query_primary(A) ::= NK_LP query_expression_body(B) order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP.                 { PARSER_TRACE; A = B;}

%type order_by_clause_opt { SNodeList* }
%destructor order_by_clause_opt { nodesDestroyNodeList($$); }
order_by_clause_opt(A) ::= .                                                                                                      { PARSER_TRACE; A = NULL; }
order_by_clause_opt(A) ::= ORDER BY sort_specification_list(B).                                                                   { PARSER_TRACE; A = B; }

slimit_clause_opt(A) ::= .                                                                                                        { A = NULL; }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(B) SOFFSET NK_INTEGER(C).                                                              { A = createLimitNode(pCxt, &B, &C); }
slimit_clause_opt(A) ::= SLIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).                                                             { A = createLimitNode(pCxt, &B, &C); }

limit_clause_opt(A) ::= .                                                                                                         { A = NULL; }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(B) OFFSET NK_INTEGER(C).                                                                 { A = createLimitNode(pCxt, &B, &C); }
limit_clause_opt(A) ::= LIMIT NK_INTEGER(C) NK_COMMA NK_INTEGER(B).                                                               { A = createLimitNode(pCxt, &B, &C); }

//////////////////////// sort_specification_list /////////////////////////////////
%type sort_specification_list { SNodeList* }
%destructor sort_specification_list { nodesDestroyNodeList($$); }
sort_specification_list(A) ::= sort_specification(B).                                     { PARSER_TRACE; A = createNodeList(pCxt, B); }
sort_specification_list(A) ::= sort_specification_list(B) NK_COMMA sort_specification(C). { PARSER_TRACE; A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::= value_expression(B) ordering_specification_opt(C) null_ordering_opt(D). { PARSER_TRACE; A = createOrderByExprNode(pCxt, B, C, D); }

%type ordering_specification_opt EOrder
%destructor ordering_specification_opt {}
ordering_specification_opt(A) ::= .     { PARSER_TRACE; A = ORDER_ASC; }
ordering_specification_opt(A) ::= ASC.  { PARSER_TRACE; A = ORDER_ASC; }
ordering_specification_opt(A) ::= DESC. { PARSER_TRACE; A = ORDER_DESC; }

%type null_ordering_opt ENullOrder
%destructor null_ordering_opt {}
null_ordering_opt(A) ::= .            { PARSER_TRACE; A = NULL_ORDER_DEFAULT; }
null_ordering_opt(A) ::= NULLS FIRST. { PARSER_TRACE; A = NULL_ORDER_FIRST; }
null_ordering_opt(A) ::= NULLS LAST.  { PARSER_TRACE; A = NULL_ORDER_LAST; }
