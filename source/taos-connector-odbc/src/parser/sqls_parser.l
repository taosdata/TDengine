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
%option extra-type="parser_ctx_t *"
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

%x LP LC LB
%x DQ SQ AA
%x ESC TAOS_ESCAPE
%x COMMENT

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]
PUNC          [-~!@#$%^&*_+=|:,.?/<>]
ESCAPE        [\\][rntfb\\]
LP            "("
RP            ")"
LC            "{"
RC            "}"
LB            "["
RB            "]"
DQ            "\""
SQ            "'"
AA            "`"
QM            "?"

%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
{SP}                  { R(); C(); } /* eat */
{LN}                  { R(); L(); } /* eat */
"/*"                  { R(); C(); PUSH(COMMENT); } /* eat */
[[:alnum:]]+          { R(); C(); return MKT(TOKEN); }
{QM}                  { R(); C(); return (unsigned char)*yytext; }
{PUNC}                { R(); C(); return MKT(TOKEN); }

{LP}                  { R(); C(); PUSH(LP); return (unsigned char)*yytext; }
{LC}                  { R(); C(); PUSH(LC); return (unsigned char)*yytext; }
{LB}                  { R(); C(); PUSH(LB); return (unsigned char)*yytext; }

{DQ}                  { R(); C(); PUSH(DQ); return (unsigned char)*yytext; }
{SQ}                  { R(); C(); PUSH(SQ); return (unsigned char)*yytext; }
{AA}                  { R(); C(); PUSH(AA); return (unsigned char)*yytext; }
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */

<COMMENT>{
"*/"                  { R(); C(); POP(); } /* eat */
[^*\n]+               { R(); C(); } /* eat */
"*"                   { R(); C(); } /* eat */
"\n"                  { R(); L(); } /* eat */
}

<LP>{
{RP}                  { R(); C(); POP(); return (unsigned char)*yytext; }

{SP}                  { R(); C(); } /* eat */
{LN}                  { R(); L(); } /* eat */
[[:alnum:]]+          { R(); C(); return MKT(TOKEN); }
{QM}                  { R(); C(); return (unsigned char)*yytext; }
{PUNC}                { R(); C(); return MKT(TOKEN); }
[;]                   { R(); C(); return MKT(TOKEN); }

{LP}                  { R(); C(); PUSH(LP); return (unsigned char)*yytext; }
{LC}                  { R(); C(); PUSH(LC); return (unsigned char)*yytext; }
{LB}                  { R(); C(); PUSH(LB); return (unsigned char)*yytext; }

{DQ}                  { R(); C(); PUSH(DQ); return (unsigned char)*yytext; }
{SQ}                  { R(); C(); PUSH(SQ); return (unsigned char)*yytext; }
{AA}                  { R(); C(); PUSH(AA); return (unsigned char)*yytext; }
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<LC>{
{RC}                  { R(); C(); POP(); return (unsigned char)*yytext; }

{SP}                  { R(); C(); } /* eat */
{LN}                  { R(); L(); } /* eat */
[[:alnum:]]+          { R(); C(); return MKT(TOKEN); }
{QM}                  { R(); C(); return (unsigned char)*yytext; }
{PUNC}                { R(); C(); return MKT(TOKEN); }
[;]                   { R(); C(); return MKT(TOKEN); }

{LP}                  { R(); C(); PUSH(LP); return (unsigned char)*yytext; }
{LC}                  { R(); C(); PUSH(LC); return (unsigned char)*yytext; }
{LB}                  { R(); C(); PUSH(LB); return (unsigned char)*yytext; }

{DQ}                  { R(); C(); PUSH(DQ); return (unsigned char)*yytext; }
{SQ}                  { R(); C(); PUSH(SQ); return (unsigned char)*yytext; }
{AA}                  { R(); C(); PUSH(AA); return (unsigned char)*yytext; }
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<LB>{
{RB}                  { R(); C(); POP(); return (unsigned char)*yytext; }

{SP}                  { R(); C(); } /* eat */
{LN}                  { R(); L(); } /* eat */
[[:alnum:]]+          { R(); C(); return MKT(TOKEN); }
{QM}                  { R(); C(); return (unsigned char)*yytext; }
{PUNC}                { R(); C(); return MKT(TOKEN); }
[;]                   { R(); C(); return MKT(TOKEN); }

{LP}                  { R(); C(); PUSH(LP); return (unsigned char)*yytext; }
{LC}                  { R(); C(); PUSH(LC); return (unsigned char)*yytext; }
{LB}                  { R(); C(); PUSH(LB); return (unsigned char)*yytext; }

{DQ}                  { R(); C(); PUSH(DQ); return (unsigned char)*yytext; }
{SQ}                  { R(); C(); PUSH(SQ); return (unsigned char)*yytext; }
{AA}                  { R(); C(); PUSH(AA); return (unsigned char)*yytext; }
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<DQ>{
{DQ}                  { R(); C(); POP(); return (unsigned char)*yytext; }
["]/["]               { R(); PUSH(ESC); C(); return MKT(DQ); }
[^"\\\f\r\n]+         { R(); SET_STR(); C(); return MKT(STR); }
"\f"                  { R(); SET_STR(); L(); return MKT(STR); }
{LN}                  { R(); SET_STR(); L(); return MKT(STR); }
[\\]                  { R(); PUSH(TAOS_ESCAPE); C(); return (unsigned char)*yytext; }
  /* .                     { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<SQ>{
{SQ}                  { R(); C(); POP(); return (unsigned char)*yytext; }
[']/[']               { R(); PUSH(ESC); C(); return MKT(SQ); }
[^'\\\f\r\n]+         { R(); SET_STR(); C(); return MKT(STR); }
"\f"                  { R(); SET_STR(); L(); return MKT(STR); }
{LN}                  { R(); SET_STR(); L(); return MKT(STR); }
[\\]                  { R(); PUSH(TAOS_ESCAPE); C(); return (unsigned char)*yytext; }
  /* .                     { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<AA>{
{AA}                  { R(); C(); POP(); return (unsigned char)*yytext; }
[`]/[`]               { R(); PUSH(ESC); C(); return MKT(AA); }
[^`\\\f\r\n]+         { R(); SET_STR(); C(); return MKT(STR); }
"\f"                  { R(); SET_STR(); L(); return MKT(STR); }
{LN}                  { R(); SET_STR(); L(); return MKT(STR); }
[\\]                  { R(); PUSH(TAOS_ESCAPE); C(); return (unsigned char)*yytext; }
  /* .                     { R(); C(); return (unsigned char)*yytext; } */ /* let bison to handle */
}

<ESC>{
["]                   { R(); POP(); C(); return MKT(DQ); } /* let bison to handle */
[']                   { R(); POP(); C(); return MKT(SQ); } /* let bison to handle */
[`]                   { R(); POP(); C(); return MKT(AA); } /* let bison to handle */
{LN}                  { R(); L(); return *yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<TAOS_ESCAPE>{        /* NOTE: taos-specific-escape */
{LN}                  { R(); L(); return *yytext; } /* let bison to handle */
.                     { R(); POP(); C(); return MKT(CH); }
}

%%

