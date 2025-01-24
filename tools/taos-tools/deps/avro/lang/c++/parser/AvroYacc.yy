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

#include <boost/format.hpp>
#include "Compiler.hh"
#include "Exception.hh"

#define YYLEX_PARAM ctx
#define YYPARSE_PARAM ctx

void yyerror(const char *str)
{
    throw avro::Exception(boost::format("Parser error: %1%") % str);
}

extern void *lexer;
extern int yylex(int *, void *);

avro::CompilerContext &context(void *ctx) {
    return *static_cast<avro::CompilerContext *>(ctx);
};

%}

%pure-parser
%error-verbose

%token AVRO_LEX_INT AVRO_LEX_LONG
%token AVRO_LEX_FLOAT AVRO_LEX_DOUBLE
%token AVRO_LEX_BOOL AVRO_LEX_NULL
%token AVRO_LEX_BYTES AVRO_LEX_STRING
%token AVRO_LEX_RECORD AVRO_LEX_ENUM AVRO_LEX_ARRAY AVRO_LEX_MAP AVRO_LEX_UNION AVRO_LEX_FIXED

%token AVRO_LEX_METADATA

%token AVRO_LEX_SYMBOLS AVRO_LEX_SYMBOLS_END
%token AVRO_LEX_FIELDS AVRO_LEX_FIELDS_END AVRO_LEX_FIELD AVRO_LEX_FIELD_END

%token AVRO_LEX_TYPE AVRO_LEX_ITEMS AVRO_LEX_VALUES

// Tokens that output text:
%token AVRO_LEX_OUTPUT_TEXT_BEGIN
%token AVRO_LEX_NAME
%token AVRO_LEX_NAMED_TYPE
%token AVRO_LEX_FIELD_NAME
%token AVRO_LEX_SYMBOL
%token AVRO_LEX_SIZE
%token AVRO_LEX_OUTPUT_TEXT_END

%token AVRO_LEX_SIMPLE_TYPE

%%

avroschema:
        simpleprimitive | object | union_t
        ;

primitive:
        AVRO_LEX_INT    { context(ctx).addType(avro::AVRO_INT); }
        |
        AVRO_LEX_LONG   { context(ctx).addType(avro::AVRO_LONG); }
        |
        AVRO_LEX_FLOAT  { context(ctx).addType(avro::AVRO_FLOAT); }
        |
        AVRO_LEX_DOUBLE { context(ctx).addType(avro::AVRO_DOUBLE); }
        |
        AVRO_LEX_BOOL   { context(ctx).addType(avro::AVRO_BOOL); }
        |
        AVRO_LEX_NULL   { context(ctx).addType(avro::AVRO_NULL); }
        |
        AVRO_LEX_BYTES  { context(ctx).addType(avro::AVRO_BYTES); }
        |
        AVRO_LEX_STRING { context(ctx).addType(avro::AVRO_STRING); }
        |
        AVRO_LEX_NAMED_TYPE { context(ctx).addNamedType(); }
        ;

simpleprimitive:
        AVRO_LEX_SIMPLE_TYPE { context(ctx).startType(); } primitive { context(ctx).stopType(); }
        ;

primitive_t:
        AVRO_LEX_TYPE primitive
        ;

array_t:
        AVRO_LEX_TYPE AVRO_LEX_ARRAY { context(ctx).addType(avro::AVRO_ARRAY); }
        ;

enum_t:
        AVRO_LEX_TYPE AVRO_LEX_ENUM { context(ctx).addType(avro::AVRO_ENUM); }
        ;

fixed_t:
        AVRO_LEX_TYPE AVRO_LEX_FIXED { context(ctx).addType(avro::AVRO_FIXED); }
        ;

map_t:
        AVRO_LEX_TYPE AVRO_LEX_MAP { context(ctx).addType(avro::AVRO_MAP); }
        ;

record_t:
        AVRO_LEX_TYPE AVRO_LEX_RECORD { context(ctx).addType(avro::AVRO_RECORD); }
        ;

type_attribute:
        array_t | enum_t | fixed_t | map_t | record_t | primitive_t
        ;

union_t:
        '[' { context(ctx).startType(); context(ctx).addType(avro::AVRO_UNION); context(ctx).setTypesAttribute(); }
        unionlist
        ']' { context(ctx).stopType(); }
        ;

object:
        '{' { context(ctx).startType(); }
         attributelist
        '}' { context(ctx).stopType(); }
        ;

name_attribute:
        AVRO_LEX_NAME { context(ctx).setNameAttribute(); }
        ;

size_attribute:
        AVRO_LEX_SIZE { context(ctx).setSizeAttribute(); }
        ;

values_attribute:
        AVRO_LEX_VALUES { context(ctx).setValuesAttribute(); } avroschema
        ;

fields_attribute:
        AVRO_LEX_FIELDS { context(ctx).setFieldsAttribute(); } fieldslist AVRO_LEX_FIELDS_END
        ;

items_attribute:
        AVRO_LEX_ITEMS { context(ctx).setItemsAttribute(); } avroschema
        ;

symbols_attribute:
        AVRO_LEX_SYMBOLS symbollist AVRO_LEX_SYMBOLS_END
        ;

attribute:
        type_attribute | name_attribute | fields_attribute | items_attribute | size_attribute | values_attribute | symbols_attribute | AVRO_LEX_METADATA
        ;

attributelist:
        attribute | attributelist ',' attribute
        ;

symbol:
        AVRO_LEX_SYMBOL { context(ctx).setSymbolsAttribute(); }
        ;

symbollist:
        symbol | symbollist ',' symbol
        ;

fieldsetting:
        fieldname | avroschema | AVRO_LEX_METADATA
        ;

fieldsettinglist:
        fieldsetting | fieldsettinglist ',' fieldsetting
        ;

fields:
        AVRO_LEX_FIELD fieldsettinglist AVRO_LEX_FIELD_END
        ;

fieldname:
        AVRO_LEX_FIELD_NAME { context(ctx).textContainsFieldName(); }
        ;

fieldslist:
        fields | fieldslist ',' fields
        ;

unionlist:
        avroschema | unionlist ',' avroschema
        ;
