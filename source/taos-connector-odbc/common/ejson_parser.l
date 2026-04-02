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

%x BQ DQ SQ AA ESC

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]


LP            [{]
RP            [}]
LB            "["
RB            "]"
DQ            ["]
SQ            "'"
AA            [`]
ID            ([^[:space:][:digit:][:punct:]]|[_])([_]|[^[:space:][:punct:]])*
NUMBER        [-+]?([0]|[[:digit:]]+)([.][[:digit:]]*)?([eE][-+][[:digit:]]+)?
V_TRUE        (?i:true)
V_FALSE       (?i:false)
V_NULL        (?i:null)

%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
{NUMBER}      { R(); SET_STR(); C(); return MKT(NUMBER); }
{V_TRUE}      { R(); SET_STR(); C(); return MKT(V_TRUE); }
{V_FALSE}     { R(); SET_STR(); C(); return MKT(V_FALSE); }
{V_NULL}      { R(); SET_STR(); C(); return MKT(V_NULL); }
{ID}          { R(); SET_STR(); C(); return MKT(ID); }
{LP}          { R(); C(); return *yytext; }
{RP}          { R(); C(); return *yytext; }
{LB}          { R(); C(); return *yytext; }
{RB}          { R(); C(); return *yytext; }
{DQ}          { R(); PUSH(DQ); C(); return *yytext; }
{SQ}          { R(); PUSH(SQ); C(); return *yytext; }
{AA}          { R(); PUSH(AA); C(); return *yytext; }
"b"["]        { R(); PUSH(BQ); C(); return MKT(BQ); }
{SP}          { R(); C(); } /* eat */
{LN}          { R(); L(); } /* eat */
.             { R(); C(); return *yytext; } /* let bison to handle */

<ESC>{
[b]                 { R(); CHR_BY_STR("\b"); POP(); C(); return MKT(CHR); }
[f]                 { R(); CHR_BY_STR("\f"); POP(); C(); return MKT(CHR); }
[n]                 { R(); CHR_BY_STR("\n"); POP(); C(); return MKT(CHR); }
[r]                 { R(); CHR_BY_STR("\r"); POP(); C(); return MKT(CHR); }
[t]                 { R(); CHR_BY_STR("\t"); POP(); C(); return MKT(CHR); }
["]                 { R(); CHR_BY_STR("\""); POP(); C(); return MKT(CHR); }
[']                 { R(); CHR_BY_STR("'");  POP(); C(); return MKT(CHR); }
[`]                 { R(); CHR_BY_STR("`");  POP(); C(); return MKT(CHR); }
[\\]                { R(); CHR_BY_STR("\\"); POP(); C(); return MKT(CHR); }
{LN}                { R(); L(); return *yytext; } /* let bison to handle */
.                   { R(); C(); return *yytext; } /* let bison to handle */
}

<BQ>{
{DQ}                   { R(); POP();     C(); return MKT(BQ); }
[^"\\[:cntrl:]]+       { R(); SET_STR(); C(); return MKT(STR); }
"\\"/[bfnrt"\\]        { R(); PUSH(ESC); C(); } /* eat */
"\\x"[[:xdigit:]]+     { R(); SET_STR(); C(); return MKT(HEX); }
"\t"                   { R(); CHR_BY_STR("\t"); C(); return MKT(CHR); }
["[:cntrl:]]           { R(); C(); return *yytext; } /* let bison to handle */
.                      { R(); C(); return *yytext; } /* let bison to handle */
}

  /* https://en.wikipedia.org/wiki/UTF-16 */
<DQ>{
{DQ}                   { R(); POP(); C(); return *yytext; }
[^"\\[:cntrl:]]+       { R(); SET_STR(); C(); return MKT(STR); }
"\\"/[bfnrt"\\]        { R(); PUSH(ESC); C(); } /* eat */
"\\u"[dD][8-9a-fA-F][[:xdigit:]]{2}"\\u"[dD][c-fC-F][[:xdigit:]]{2} { R(); SET_STR(); C(); return MKT(EUNI); }
"\\u"[[:xdigit:]]{4}   { R(); SET_STR(); C(); return MKT(EUNI); }
"\t"                   { R(); CHR_BY_STR("\t"); C(); return MKT(CHR); }
["[:cntrl:]]           { R(); C(); return *yytext; } /* let bison to handle */
.                      { R(); C(); return *yytext; } /* let bison to handle */
}

<SQ>{
{SQ}                   { R(); POP(); C(); return *yytext; }
[^'\\[:cntrl:]]+       { R(); SET_STR(); C(); return MKT(STR); }
"\\"/[bfnrt'\\]        { R(); PUSH(ESC); C(); } /* eat */
"\\u"[dD][8-9a-fA-F][[:xdigit:]]{2}"\\u"[dD][c-fC-F][[:xdigit:]]{2} { R(); SET_STR(); C(); return MKT(EUNI); }
"\\u"[[:xdigit:]]{4}   { R(); SET_STR(); C(); return MKT(EUNI); }
"\t"                   { R(); CHR_BY_STR("\t"); C(); return MKT(CHR); }
['[:cntrl:]]           { R(); C(); return *yytext; } /* let bison to handle */
.                      { R(); C(); return *yytext; } /* let bison to handle */
}

<AA>{
{AA}                   { R(); POP(); C(); return *yytext; }
[^`\\[:cntrl:]]+       { R(); SET_STR(); C(); return MKT(STR); }
"\\"/[bfnrt`\\]        { R(); PUSH(ESC); C(); } /* eat */
"\\u"[dD][8-9a-fA-F][[:xdigit:]]{2}"\\u"[dD][c-fC-F][[:xdigit:]]{2} { R(); SET_STR(); C(); return MKT(EUNI); }
"\\u"[[:xdigit:]]{4}   { R(); SET_STR(); C(); return MKT(EUNI); }
"\t"                   { R(); CHR_BY_STR("\t"); C(); return MKT(CHR); }
[`[:cntrl:]]           { R(); C(); return *yytext; } /* let bison to handle */
.                      { R(); C(); return *yytext; } /* let bison to handle */
}

%%

