//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y

%name NewParse

%token_prefix NEW_TK_
%token_type { SToken* }
%default_type { SNode* }
%default_destructor { nodesDestroyNode($$); }

%extra_argument { SAstCreaterContext* pCxt }

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
}

%syntax_error {  
  if(TOKEN->z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN->z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > pCxt->pQueryCxt->msgLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN->z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        sprintf(pCxt->pQueryCxt->pMsg, msg, tmpstr);
    } else {
        sprintf(pCxt->pQueryCxt->pMsg, msg, &TOKEN->z[0]);
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

cmd ::= SHOW DATABASES.  { createShowStmt(pCxt, SHOW_TYPE_DATABASE); }

cmd ::= query_expression(A). { pCxt->pRootNode = A; }

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

column_reference(A) ::= NK_ID(B).                                 { A = createColumnNode(pCxt, NULL, NULL, B); }
column_reference(A) ::= NK_ID(B) NK_DOT NK_ID(C).                 { A = createColumnNode(pCxt, NULL, B, C); }
column_reference(A) ::= NK_ID(B) NK_DOT NK_ID(C) NK_DOT NK_ID(D). { A = createColumnNode(pCxt, B, C, D); }

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
query_specification(A) ::= SELECT set_quantifier_opt(B) select_list(C) from_clause(D). { A = createSelectStmt(pCxt, B, C, D); }

%type set_quantifier_opt { bool }
%destructor set_quantifier_opt {}
set_quantifier_opt(A) ::= .         { A = false; }
set_quantifier_opt(A) ::= DISTINCT. { A = true; }
set_quantifier_opt(A) ::= ALL.      { A = false; }

%type select_list { SNodeList* }
%destructor select_list { nodesDestroyNodeList($$); }
select_list(A) ::= NK_STAR.           { A = NULL; }
select_list(A) ::= select_sublist(B). { A = B; }

%type select_sublist { SNodeList* }
%destructor select_sublist { nodesDestroyNodeList($$); }
select_sublist(A) ::= select_item(B).                            { A = createNodeList(pCxt, B); }
select_sublist(A) ::= select_sublist(B) NK_COMMA select_item(C). { A = addNodeToList(pCxt, B, C); }

select_item(A) ::= value_expression(B).                     { A = B; }
select_item(A) ::= value_expression(B) AS NK_ID(C).         { A = setProjectionAlias(pCxt, B, C); }
select_item(A) ::= NK_ID(B) NK_DOT NK_STAR(C).                 { A = createColumnNode(pCxt, NULL, B, C); }
select_item(A) ::= NK_ID(B) NK_DOT NK_ID(C) NK_DOT NK_STAR(D). { A = createColumnNode(pCxt, B, C, D); }

from_clause ::= FROM table_reference_list.

table_reference_list ::= table_reference.
table_reference_list ::= table_reference_list NK_COMMA table_reference.

table_reference ::= NK_ID.
//table_reference ::= joined_table.

//////////////////////// query_expression /////////////////////////////////
query_expression(A) ::= with_clause_opt query_expression_body(B) order_by_clause_opt limit_clause_opt slimit_clause_opt. { A = B; }

with_clause_opt ::= .                         {}
with_clause_opt ::= WITH with_list.           { pCxt->notSupport = true; pCxt->valid = false; }
with_clause_opt ::= WITH RECURSIVE with_list. { pCxt->notSupport = true; pCxt->valid = false; }

with_list ::= with_list_element.                    {}
with_list ::= with_list NK_COMMA with_list_element. {}

with_list_element ::= NK_ID AS table_subquery. {}

table_subquery ::= . {}

query_expression_body(A) ::= query_primary(B).                                            { A = B; }
query_expression_body(A) ::= query_expression_body(B) UNION ALL query_expression_body(C). { A = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, B, C); }

query_primary(A) ::= query_specification(B).                                                                   { A = B; }
query_primary(A) ::= NK_LP query_expression_body(B) order_by_clause_opt limit_clause_opt slimit_clause_opt NK_RP. { A = B;}

%type order_by_clause_opt { SNodeList* }
%destructor order_by_clause_opt { nodesDestroyNodeList($$); }
order_by_clause_opt(A) ::= .                                    { A = NULL; }
order_by_clause_opt(A) ::= ORDER BY sort_specification_list(B). { A = B; }

limit_clause_opt ::= .

slimit_clause_opt ::= .

//////////////////////// sort_specification_list /////////////////////////////////
%type sort_specification_list { SNodeList* }
%destructor sort_specification_list { nodesDestroyNodeList($$); }
sort_specification_list(A) ::= sort_specification(B).                                     { A = createNodeList(pCxt, B); }
sort_specification_list(A) ::= sort_specification_list(B) NK_COMMA sort_specification(C). { A = addNodeToList(pCxt, B, C); }

sort_specification(A) ::= value_expression(B) ordering_specification_opt(C) null_ordering_opt(D). { A = createOrderByExprNode(pCxt, B, C, D); }

%type ordering_specification_opt EOrder
%destructor ordering_specification_opt {}
ordering_specification_opt(A) ::= .     { A = ORDER_ASC; }
ordering_specification_opt(A) ::= ASC.  { A = ORDER_ASC; }
ordering_specification_opt(A) ::= DESC. { A = ORDER_DESC; }

%type null_ordering_opt ENullOrder
%destructor null_ordering_opt {}
null_ordering_opt(A) ::= .            { A = NULL_ORDER_DEFAULT; }
null_ordering_opt(A) ::= NULLS FIRST. { A = NULL_ORDER_FIRST; }
null_ordering_opt(A) ::= NULLS LAST.  { A = NULL_ORDER_LAST; }
