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

%x EQ
%x CURLY
%x SERVER
%x SERVER_EQ
%x FQDN
%x COLON

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]
DSN           (?i:dsn)
UID           (?i:uid)
PWD           (?i:pwd)
DB            (?i:db)
DATABASE      (?i:database)
DRIVER        (?i:driver)
URL           (?i:url)
SERVER        (?i:server)
CHARSET_FOR_COL_BIND   (?i:charset_encoder_for_col_bind)
CHARSET_FOR_PARAM_BIND (?i:charset_encoder_for_param_bind)
UNSIGNED_PROMOTION          (?i:unsigned_promotion)
TIMESTAMP_AS_IS             (?i:timestamp_as_is)
CONN_MODE                   (?i:conn_mode)
CUSTOMPRODUCT (?i:customproduct)
FQDN          [-[:alnum:]]+((\.[-[:alnum:]]+)+)*(\.)?
ID            [^\[\]{}(),;?*=!@[:space:]]+
VALUE         [^\[\]{}(),;?*=!@[:space:]]+
VALUEX        [^{}[:space:]][^{}]*[^{}[:space:]]

DIGITS        [[:digit:]]+
%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
{DSN}         { R(); C(); return MKT(DSN); }
{UID}         { R(); C(); return MKT(UID); }
{PWD}         { R(); C(); return MKT(PWD); }
{DB}          { R(); C(); return MKT(DB); }
{DATABASE}    { R(); C(); return MKT(DB); }
{DRIVER}      { R(); C(); return MKT(DRIVER); }
{URL}         { R(); C(); return MKT(URL); }
{SERVER}      { R(); PUSH(SERVER); C(); return MKT(SERVER); }
{CHARSET_FOR_COL_BIND}     { R(); C(); return MKT(CHARSET_FOR_COL_BIND); }
{CHARSET_FOR_PARAM_BIND}   { R(); C(); return MKT(CHARSET_FOR_PARAM_BIND); }
{UNSIGNED_PROMOTION}       { R(); C(); return MKT(UNSIGNED_PROMOTION); }
{TIMESTAMP_AS_IS}          { R(); C(); return MKT(TIMESTAMP_AS_IS); }
{CONN_MODE}                { R(); C(); return MKT(CONN_MODE); }
{CUSTOMPRODUCT}            { R(); C(); return MKT(CUSTOMPRODUCT); }
{DIGITS}      { R(); SET_STR(); C(); return MKT(DIGITS); }
{ID}          { R(); SET_STR(); C(); return MKT(ID); }
"="           { R(); PUSH(EQ); C(); return (unsigned char)*yytext; }
"{"           { R(); PUSH(CURLY); C(); return (unsigned char)*yytext; }       /* "}" */
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */

<SERVER>{
"="       { R(); CHG(SERVER_EQ); C(); return (unsigned char)*yytext; }
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
";"       { R(); POP(); C(); return (unsigned char)*yytext; }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<SERVER_EQ>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
{FQDN}    { R(); SET_STR(); CHG(FQDN); C(); return MKT(FQDN); }
";"       { R(); POP(); C(); return (unsigned char)*yytext; }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<FQDN>{
":"       { R(); CHG(COLON); C(); return ':'; }
";"       { R(); POP(); C(); return ';'; }
{SP}      { R(); POP(); C(); }
{LN}      { R(); POP(); L(); }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<COLON>{
{DIGITS}  { R(); POP(); SET_STR(); C(); return MKT(DIGITS); }
";"       { R(); POP(); C(); return ';'; }
{SP}      { R(); POP(); C(); }
{LN}      { R(); POP(); L(); }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<EQ>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
"{"       { R(); CHG(CURLY); C(); return (unsigned char)*yytext; }       /* "}" */
{DIGITS}  { R(); POP(); SET_STR(); C(); return MKT(DIGITS); }
{VALUE}   { R(); POP(); SET_STR(); C(); return MKT(VALUE); }
";"       { R(); POP(); C(); return (unsigned char)*yytext; }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<CURLY>{
{SP}      { R(); C(); } /* eat */
{LN}      { R(); L(); } /* eat */
  /* "{" */
"}"       { R(); POP(); C(); return (unsigned char)*yytext; }
{VALUEX}  { R(); SET_STR(); C(); return MKT(VALUEX); }
.         { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

%%

