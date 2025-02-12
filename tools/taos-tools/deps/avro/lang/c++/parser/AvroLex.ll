%{
/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
 
 https://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

// on some systems, won't find an EOF definition
#ifndef EOF
#define EOF (-1)
#endif

#include "AvroYacc.hh"

// this undef is a hack for my mac implementation
#undef yyFlexLexer
#include "Compiler.hh"

#define YY_STACK_USED 1

using std::cin;
using std::cout;
using std::cerr;

%}

%option c++
%option noyywrap

%{

int yylex(int *val, void *ctx)
{
    avro::CompilerContext *c = static_cast<avro::CompilerContext *>(ctx);
    int ret = c->lexer().yylex();
    if( ret > AVRO_LEX_OUTPUT_TEXT_BEGIN && ret < AVRO_LEX_OUTPUT_TEXT_END ) {
        c->setText( c->lexer().YYText()) ;
    }
    return ret;
}

%}

%x READTYPE
%x STARTTYPE
%x STARTSCHEMA
%x READNAME
%x READFIELD
%x READFIELDS
%x READFIELDNAME
%x READSYMBOLS
%x READSYMBOL
%x READSIZE
%x INUNION
%x INOBJECT
%x READMETADATA
%x SKIPJSONSTRING
%x SKIPJSONARRAY
%x SKIPJSONOBJECT

ws [ \t\r\n]
nonws [^ \t\r\n]
delim {ws}*:{ws}*
avrotext [a-zA-Z_][a-zA-Z0-9_.]*
startunion \[
startobject \{
integer [0-9]+
anytext .*

%%
<READTYPE>int                   return AVRO_LEX_INT;
<READTYPE>long                  return AVRO_LEX_LONG;
<READTYPE>null                  return AVRO_LEX_NULL;
<READTYPE>boolean               return AVRO_LEX_BOOL;
<READTYPE>float                 return AVRO_LEX_FLOAT;
<READTYPE>double                return AVRO_LEX_DOUBLE;
<READTYPE>string                return AVRO_LEX_STRING;
<READTYPE>bytes                 return AVRO_LEX_BYTES;
<READTYPE>record                return AVRO_LEX_RECORD;
<READTYPE>enum                  return AVRO_LEX_ENUM;
<READTYPE>map                   return AVRO_LEX_MAP;
<READTYPE>array                 return AVRO_LEX_ARRAY;
<READTYPE>fixed                 return AVRO_LEX_FIXED;
<READTYPE>{avrotext}            return AVRO_LEX_NAMED_TYPE;
<READTYPE>\"                    yy_pop_state();

<READNAME>{avrotext}            return AVRO_LEX_NAME;
<READNAME>\"                    yy_pop_state();

<READSYMBOL>{avrotext}          return AVRO_LEX_SYMBOL;
<READSYMBOL>\"                  yy_pop_state();

<READFIELDNAME>{avrotext}       return AVRO_LEX_FIELD_NAME;
<READFIELDNAME>\"               yy_pop_state();

<READFIELD>\"type\"{delim}      yy_push_state(STARTSCHEMA);
<READFIELD>\"name\"{delim}\"    yy_push_state(READFIELDNAME);
<READFIELD>\}                   yy_pop_state(); return AVRO_LEX_FIELD_END;
<READFIELD>,                    return yytext[0];
<READFIELD>\"{avrotext}\"+{delim}      yy_push_state(READMETADATA); return AVRO_LEX_METADATA;
<READFIELD>{ws}                 ;

<READFIELDS>\{                  yy_push_state(READFIELD); return AVRO_LEX_FIELD;
<READFIELDS>\]                  yy_pop_state(); return AVRO_LEX_FIELDS_END;
<READFIELDS>,                   return yytext[0];
<READFIELDS>{ws}                ;

<READSYMBOLS>\"                 yy_push_state(READSYMBOL);
<READSYMBOLS>,                  return yytext[0];
<READSYMBOLS>\]                 yy_pop_state(); return AVRO_LEX_SYMBOLS_END;
<READSYMBOLS>{ws}               ;

<READSIZE>{integer}             yy_pop_state(); return AVRO_LEX_SIZE;

<INUNION>\"                     yy_push_state(READTYPE); return AVRO_LEX_SIMPLE_TYPE;
<INUNION>{startobject}          yy_push_state(INOBJECT); return yytext[0];
<INUNION>\]                     yy_pop_state(); return yytext[0];
<INUNION>,                      return yytext[0];
<INUNION>{ws}                   ;

<SKIPJSONSTRING>\"              yy_pop_state();
<SKIPJSONSTRING>\\.             ;
<SKIPJSONSTRING>[^\"\\]+        ;

<SKIPJSONOBJECT>\}              yy_pop_state();
<SKIPJSONOBJECT>\{              yy_push_state(SKIPJSONOBJECT);
<SKIPJSONOBJECT>\"              yy_push_state(SKIPJSONSTRING);
<SKIPJSONOBJECT>[^\{\}\"]+      ;

<SKIPJSONARRAY>\]               yy_pop_state();
<SKIPJSONARRAY>\[               yy_push_state(SKIPJSONARRAY);
<SKIPJSONARRAY>\"               yy_push_state(SKIPJSONSTRING);
<SKIPJSONARRAY>[^\[\]\"]+       ;

<READMETADATA>\"                yy_pop_state(); yy_push_state(SKIPJSONSTRING);
<READMETADATA>\{                yy_pop_state(); yy_push_state(SKIPJSONOBJECT);
<READMETADATA>\[                yy_pop_state(); yy_push_state(SKIPJSONARRAY);
<READMETADATA>[^\"\{\[,\}]+     yy_pop_state();

<INOBJECT>\"type\"{delim}       yy_push_state(STARTTYPE); return AVRO_LEX_TYPE;
<INOBJECT>\"name\"{delim}\"     yy_push_state(READNAME);
<INOBJECT>\"size\"{delim}       yy_push_state(READSIZE);
<INOBJECT>\"items\"{delim}      yy_push_state(STARTSCHEMA); return AVRO_LEX_ITEMS;
<INOBJECT>\"values\"{delim}     yy_push_state(STARTSCHEMA); return AVRO_LEX_VALUES;
<INOBJECT>\"fields\"{delim}\[   yy_push_state(READFIELDS); return AVRO_LEX_FIELDS;
<INOBJECT>\"symbols\"{delim}\[  yy_push_state(READSYMBOLS); return AVRO_LEX_SYMBOLS;
<INOBJECT>,                     return yytext[0];
<INOBJECT>\}                    yy_pop_state(); return yytext[0];
<INOBJECT>\"{avrotext}+\"{delim}       yy_push_state(READMETADATA); return AVRO_LEX_METADATA;
<INOBJECT>{ws}                  ;

<STARTTYPE>\"                   yy_pop_state(); yy_push_state(READTYPE);
<STARTTYPE>{startunion}         yy_pop_state(); yy_push_state(INUNION); return yytext[0];
<STARTTYPE>{startobject}        yy_pop_state(); yy_push_state(INOBJECT); return yytext[0];

<STARTSCHEMA>\"                 yy_pop_state(); yy_push_state(READTYPE); return AVRO_LEX_SIMPLE_TYPE;
<STARTSCHEMA>{startunion}       yy_pop_state(); yy_push_state(INUNION); return yytext[0];
<STARTSCHEMA>{startobject}      yy_pop_state(); yy_push_state(INOBJECT); return yytext[0];

{startobject}                   yy_push_state(INOBJECT); return yytext[0];
{startunion}                    yy_push_state(INUNION); return yytext[0];
\"                              yy_push_state(READTYPE); return AVRO_LEX_SIMPLE_TYPE;
{ws}                            ;
<<EOF>>                         {
#if !YY_FLEX_SUBMINOR_VERSION || YY_FLEX_SUBMINOR_VERSION < 27
// The versions of flex before 3.5.27 do not free their stack when done, so explcitly free it.
// Note that versions before did not actually define a subminor macro.
                                    if (yy_start_stack) {
                                        yy_flex_free(yy_start_stack);
                                        yy_start_stack = 0;
                                    }
#endif
#if YY_FLEX_SUBMINOR_VERSION > 35
// At this time, 3.5.35 is the latest version.
#warning "Warning:  untested version of flex"
#endif
#if YY_FLEX_SUBMINOR_VERSION >= 31 && YY_FLEX_SUBMINOR_VERSION < 34
// The versions of flex starting 3.5.31 do not free yy_buffer_stack, so do so
// explicitly (first yy_delete_buffer must be called to free pointers stored on the stack, then it is
// safe to remove the stack).  This was fixed in 3.4.34.
                                    if(yy_buffer_stack) {
                                        yy_delete_buffer(YY_CURRENT_BUFFER);
                                        yyfree(yy_buffer_stack);
                                        yy_buffer_stack = 0;
                                    }
#endif
                                    yyterminate();
                                }

%%

