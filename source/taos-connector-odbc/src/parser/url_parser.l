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

%x SCHEME SCHEME_COLON
%x AUTH
%x USERINFO
%x PATH
%x IPV6
%x HOST
%x SLASH
%x QUERY
%x FRAGMENT

SP            [ \t]
LN            "\r\n"|"\n\r"|[\f\r\n]
SCHEME        [[:alpha:]][-+.[:alnum:]]*
PCT           [%][[:xdigit:]]{2}
UNRESERVED    [-._~[:alnum:]]
GEN_DELIMS    [:/?#\[\]@]
SUB_DELIMS    [!$&'()*+,;=]
DIGIT         [[:digit:]]

PPCHAR        {UNRESERVED}|{SUB_DELIMS}|[:@]
USERINFO      (({UNRESERVED}|{SUB_DELIMS}|[:])+|({PCT}+))+
DEC_OCTET     [0-9]|[1-9][0-9]|[1][0-9][0-9]|[2][0-4][0-9]|[2][5][0-5]
IPV4          {DEC_OCTET}[.]{DEC_OCTET}[.]{DEC_OCTET}[.]{DEC_OCTET}
REG_NAME      (({UNRESERVED}|{SUB_DELIMS})+|({PCT}+))+
IPVFUTURE     [v][:xdigit:][.]({UNRESERVED}|{SUB_DELIMS}|[:])
H16           [[:xdigit:]]{4}
LS32          ({H16}[:]{H16})|{IPV4}
IPV6_1        ({H16}[:]){6}{LS32}
IPV6_2        "::"({H16}[:]){5}{LS32}
IPV6_3        {H16}?"::"({H16}[:]){4}{LS32}
IPV6_4        (({H16}":"){1}{H16})?"::"({H16}[:]){3}{LS32}
IPV6_5        (({H16}":"){2}{H16})?"::"({H16}[:]){2}{LS32}
IPV6_6        (({H16}":"){3}{H16})?"::"({H16}[:]){LS32}
IPV6_7        (({H16}":"){4}{H16})?"::"{LS32}
IPV6_8        (({H16}":"){5}{H16})?"::"{H16}
IPV6_9        (({H16}":"){6}{H16})?"::"
PPCHAR_PCT    ({PPCHAR}+)|({PCT}+)

%%

<<EOF>> { if (0) { int state; TOP_STATE(state); if (state != INITIAL) return -1; }
          yyterminate(); }
{SP}                  { R(); C(); } /* eat */
"file"/[:]            { R(); C(); return MKT(FILE_SCHEME_NOT_SUPPORTED_YET); }
{SCHEME}/[:]          { R(); C(); SET_STR(); PUSH(SCHEME); return MKT(SCHEME); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */

<SCHEME>{
":"                   { R(); C(); CHG(SCHEME_COLON); return (unsigned char)*yytext; }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<SCHEME_COLON>{
[/]/[/]               { R(); C(); CHG(AUTH); return (unsigned char)*yytext; }
"/"                   { R(); C(); CHG(SLASH); return (unsigned char)*yytext; }
{PPCHAR_PCT}+         { R(); C(); SET_STR(); CHG(PATH); return MKT(PCHAR); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<AUTH>{
{USERINFO}/[@]        { R(); C(); SET_STR(); CHG(USERINFO); return MKT(USERINFO); }
"["                   { R(); C(); CHG(IPV6); return (unsigned char)*yytext; }  // "]"
{IPV4}                { R(); C(); SET_STR(); CHG(HOST); return MKT(IPV4); }
{REG_NAME}            { R(); C(); SET_STR(); CHG(HOST); return MKT(REG_NAME); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<USERINFO>{
"@"                   { R(); C(); return (unsigned char)*yytext; }
"["                   { R(); C(); CHG(IPV6); return (unsigned char)*yytext; } // "]"
{IPV4}                { R(); C(); SET_STR(); CHG(HOST); return MKT(IPV4); }
{REG_NAME}            { R(); C(); SET_STR(); CHG(HOST); return MKT(REG_NAME); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<IPV6>{
"]"                   { R(); C(); CHG(HOST); return (unsigned char)*yytext; }
{IPVFUTURE}           { R(); C(); SET_STR(); CHG(HOST); return MKT(IPVFUTURE); }
{IPV6_1}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_2}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_3}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_4}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_5}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_6}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_7}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_8}              { R(); C(); SET_STR(); return MKT(IPV6); }
{IPV6_9}              { R(); C(); SET_STR(); return MKT(IPV6); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<HOST>{
":"                   { R(); C(); return (unsigned char)*yytext; }
"/"                   { R(); C(); CHG(SLASH); return (unsigned char)*yytext; }
"?"                   { R(); C(); CHG(QUERY); return (unsigned char)*yytext; }
"#"                   { R(); C(); CHG(FRAGMENT); return (unsigned char)*yytext; }
{DIGIT}+              { R(); C(); SET_STR(); return MKT(PORT); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<SLASH>{
"?"                   { R(); C(); CHG(QUERY); return (unsigned char)*yytext; }
"#"                   { R(); C(); CHG(FRAGMENT); return (unsigned char)*yytext; }
{PPCHAR_PCT}+         { R(); C(); SET_STR(); CHG(PATH); return MKT(PCHAR); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<PATH>{
"/"                   { R(); C(); CHG(SLASH); return (unsigned char)*yytext; }
"?"                   { R(); C(); CHG(QUERY); return (unsigned char)*yytext; }
"#"                   { R(); C(); CHG(FRAGMENT); return (unsigned char)*yytext; }
{PPCHAR_PCT}+         { R(); C(); SET_STR(); CHG(PATH); return MKT(PCHAR); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<QUERY>{
"#"                   { R(); C(); CHG(FRAGMENT); return (unsigned char)*yytext; }
({PPCHAR_PCT}|[/?]+)+ { R(); C(); SET_STR(); return MKT(PCHAR); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

<FRAGMENT>{
({PPCHAR_PCT}|[/?]+)+ { R(); C(); SET_STR(); return MKT(PCHAR); }
{LN}                  { R(); L(); return (unsigned char)*yytext; } /* let bison to handle */
.                     { R(); C(); return (unsigned char)*yytext; } /* let bison to handle */
}

%%

