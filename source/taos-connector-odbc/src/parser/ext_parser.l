%{
/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
%}


%{
#include "parser_macros.h"
%}

%option bison-bridge bison-locations reentrant
%option nounistd
%option noyywrap noinput nounput
%option verbose
%option debug
%option stack
%option nodefault
%option warn
%option perf-report
%option 8bit
%option never-interactive
%option nounistd

%x TAOS_ODBC_EXT
%x TAOS_ODBC_EXT_TOPIC
%x TAOS_ODBC_EXT_TOPIC_LP
%x TAOS_ODBC_EXT_TOPIC_EQ
%x TAOS_ODBC_EXT_INSERT_INTO
%x TAOS_ODBC_EXT_INSERT_INTO_LR
%x QSTR SSTR TSTR ESC

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]


LP            [{]
RP            [}]
LR            [(]
RR            [)]
TOPIC         (?i:topic)
INSERT        (?i:insert)
INTO          (?i:into)
USING         (?i:using)
WITH          (?i:with)
TAGS          (?i:tags)
VALUES        (?i:values)
TNAME         [_[:alpha:]][_[:alnum:]]*
TKEY          [_[:alpha:]][_.[:alnum:]]*
TVAL          [-_.\[\](),?*!@[:alnum:]]+
ID            [_[:alpha:]][_[:alnum:]]*
INTEGRAL      [[:digit:]]+
NUMBER        ("0."|[1-9][[:digit:]]*[.])[[:digit:]]*([eE][-+][[:digit:]]+)?

DIGITS        [[:digit:]]+
%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
"!"           { R(); PUSH(TAOS_ODBC_EXT); C(); return (unsigned char)*yytext; }
  /* {DIGITS}      { R(); SET_STR(); C(); return MKT(DIGITS); } */
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */

<TAOS_ODBC_EXT>{
{TOPIC}       { R(); CHG(TAOS_ODBC_EXT_TOPIC); C(); return MKT(TOPIC); }
{INSERT}      { R(); C(); return MKT(INSERT); }
{INTO}        { R(); CHG(TAOS_ODBC_EXT_INSERT_INTO); C(); return MKT(INTO); }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
{ID}          { R(); C(); return MKT(ID); }
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ODBC_EXT_INSERT_INTO>{
"?"           { R(); C(); return *yytext; }
"."           { R(); C(); return *yytext; }
{LR}          { R(); PUSH(TAOS_ODBC_EXT_INSERT_INTO_LR); C(); return (unsigned char)*yytext; }
{USING}       { R(); C(); return MKT(USING); }
{WITH}        { R(); C(); return MKT(WITH); }
{TAGS}        { R(); C(); return MKT(TAGS); }
{VALUES}      { R(); C(); return MKT(VALUES); }
{ID}          { R(); SET_STR(); C(); return MKT(ID); }
["]           { R(); PUSH(QSTR); return *yytext; }
[']           { R(); PUSH(SSTR); return *yytext; }
[`]           { R(); PUSH(TSTR); return *yytext; }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ODBC_EXT_INSERT_INTO_LR>{
{RR}          { R(); POP(); C(); return (unsigned char)*yytext; }
{LR}          { R(); PUSH(TAOS_ODBC_EXT_INSERT_INTO_LR); C(); return (unsigned char)*yytext; }
{ID}          { R(); SET_STR(); C(); return MKT(ID); }
{INTEGRAL}    { R(); SET_STR(); C(); return MKT(INTEGRAL); }
{NUMBER}      { R(); SET_STR(); C(); return MKT(NUMBER); }
["]           { R(); PUSH(QSTR); return *yytext; }
[']           { R(); PUSH(SSTR); return *yytext; }
[`]           { R(); PUSH(TSTR); return *yytext; }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<QSTR>{
["]           { R(); POP(); C(); return *yytext; }
["]/["]       { R(); PUSH(ESC); C(); return *yytext; }
[^"\f\r\n]+   { R(); SET_STR(); C(); return MKT(QSTR); }
"\f"          { R(); SET_STR(); L(); return MKT(QSTR); }
{LN}          { R(); SET_STR(); L(); return MKT(QSTR); }
  /* .             { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<SSTR>{
[']           { R(); POP(); C(); return *yytext; }
[']/[']       { R(); PUSH(ESC); C(); return *yytext; }
[^'\f\r\n]+   { R(); SET_STR(); C(); return MKT(SSTR); }
"\f"          { R(); SET_STR(); L(); return MKT(SSTR); }
{LN}          { R(); SET_STR(); L(); return MKT(SSTR); }
  /* .             { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<TSTR>{
[`]           { R(); POP(); C(); return *yytext; }
[`]/[`]       { R(); PUSH(ESC); C(); return *yytext; }
[^`\f\r\n]+   { R(); SET_STR(); C(); return MKT(TSTR); }
"\f"          { R(); SET_STR(); L(); return MKT(TSTR); }
{LN}          { R(); SET_STR(); L(); return MKT(TSTR); }
  /* .             { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<ESC>{
{LN}          { R(); L(); return *yytext; } /* let bison to handle */
.             { R(); POP(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ODBC_EXT_TOPIC>{
{TNAME}       { R(); SET_STR(); C(); return MKT(TNAME); }
{LP}          { R(); PUSH(TAOS_ODBC_EXT_TOPIC_LP); C(); return (unsigned char)*yytext; }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ODBC_EXT_TOPIC_LP>{
{RP}          { R(); POP(); C(); return (unsigned char)*yytext; }
{TKEY}        { R(); SET_STR(); C(); return MKT(TKEY); }
"="           { R(); PUSH(TAOS_ODBC_EXT_TOPIC_EQ); C(); return (unsigned char)*yytext; }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ODBC_EXT_TOPIC_EQ>{
{TVAL}        { R(); SET_STR(); POP(); C(); return MKT(TVAL); }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

%%

