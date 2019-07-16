//lemon parser file to generate sql parse by using finite-state-machine code used to parse sql
//usage: lemon sql.y
%token_prefix TK_

%token_type {SSQLToken}
%default_type {SSQLToken}
%extra_argument {SSqlInfo* pInfo}

%fallback ID BOOL TINYINT SMALLINT INTEGER BIGINT FLOAT DOUBLE STRING TIMESTAMP BINARY NCHAR.

%left OR.
%left AND.
%right NOT.
%left EQ NE ISNULL NOTNULL IS LIKE GLOB BETWEEN IN.
%left GT GE LT LE.
%left BITAND BITOR LSHIFT RSHIFT.
%left PLUS MINUS.
%left DIVIDE TIMES.
%left STAR SLASH REM.
%left CONCAT.
%right UMINUS UPLUS BITNOT.

%include {
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#include "tsql.h"
#include "tutil.h"
}

%syntax_error {
  pInfo->validSql = false;
  int32_t outputBufLen = tListLen(pInfo->pzErrMsg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->pzErrMsg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->pzErrMsg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->pzErrMsg, "Incomplete SQL statement");
  }

  assert(len <= outputBufLen);
}

%parse_accept       {}

program ::= cmd.    {}

//////////////////////////////////THE SHOW STATEMENT///////////////////////////////////////////
cmd ::= SHOW DATABASES.  { setDCLSQLElems(pInfo, SHOW_DATABASES, 0);}
cmd ::= SHOW MNODES.     { setDCLSQLElems(pInfo, SHOW_MNODES, 0);}
cmd ::= SHOW DNODES.     { setDCLSQLElems(pInfo, SHOW_DNODES, 0);}
cmd ::= SHOW USERS.      { setDCLSQLElems(pInfo, SHOW_USERS, 0);}

cmd ::= SHOW MODULES.    { setDCLSQLElems(pInfo, SHOW_MODULES, 0);  }
cmd ::= SHOW QUERIES.    { setDCLSQLElems(pInfo, SHOW_QUERIES, 0);  }
cmd ::= SHOW CONNECTIONS.{ setDCLSQLElems(pInfo, SHOW_CONNECTIONS, 0);}
cmd ::= SHOW STREAMS.    { setDCLSQLElems(pInfo, SHOW_STREAMS, 0);  }
cmd ::= SHOW CONFIGS.    { setDCLSQLElems(pInfo, SHOW_CONFIGS, 0);  }
cmd ::= SHOW SCORES.     { setDCLSQLElems(pInfo, SHOW_SCORES, 0);   }
cmd ::= SHOW GRANTS.     { setDCLSQLElems(pInfo, SHOW_GRANTS, 0);   }

%type dbPrefix {SSQLToken}
dbPrefix(A) ::=.                   {A.n = 0;}
dbPrefix(A) ::= ids(X) DOT.        {A = X;  }

%type cpxName {SSQLToken}
cpxName(A) ::= .             {A.n = 0;  }
cpxName(A) ::= DOT ids(Y).   {A = Y; A.n += 1;    }

cmd ::= SHOW dbPrefix(X) TABLES.         {
    setDCLSQLElems(pInfo, SHOW_TABLES, 1, &X);
}

cmd ::= SHOW dbPrefix(X) TABLES LIKE ids(Y).         {
    setDCLSQLElems(pInfo, SHOW_TABLES, 2, &X, &Y);
}

cmd ::= SHOW dbPrefix(X) STABLES.      {
    setDCLSQLElems(pInfo, SHOW_STABLES, 1, &X);
}

cmd ::= SHOW dbPrefix(X) STABLES LIKE ids(Y).      {
    SSQLToken token;
    setDBName(&token, &X);
    setDCLSQLElems(pInfo, SHOW_STABLES, 2, &token, &Y);
}

cmd ::= SHOW dbPrefix(X) VGROUPS.    {
    SSQLToken token;
    setDBName(&token, &X);
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 1, &token);
}

//drop configure for tables
cmd ::= DROP TABLE ifexists(Y) ids(X) cpxName(Z).   {
    X.n += Z.n;
    setDCLSQLElems(pInfo, DROP_TABLE, 2, &X, &Y);
}

cmd ::= DROP DATABASE ifexists(Y) ids(X).    { setDCLSQLElems(pInfo, DROP_DATABASE, 2, &X, &Y); }
cmd ::= DROP USER ids(X).        { setDCLSQLElems(pInfo, DROP_USER, 1, &X);     }

/////////////////////////////////THE USE STATEMENT//////////////////////////////////////////
cmd ::= USE ids(X).              { setDCLSQLElems(pInfo, USE_DATABASE, 1, &X);}

/////////////////////////////////THE DESCRIBE STATEMENT/////////////////////////////////////
cmd ::= DESCRIBE ids(X) cpxName(Y). {
    X.n += Y.n;
    setDCLSQLElems(pInfo, DESCRIBE_TABLE, 1, &X);
}

/////////////////////////////////THE ALTER STATEMENT////////////////////////////////////////
cmd ::= ALTER USER ids(X) PASS ids(Y).          { setDCLSQLElems(pInfo, ALTER_USER_PASSWD, 2, &X, &Y);    }
cmd ::= ALTER USER ids(X) PRIVILEGE ids(Y).     { setDCLSQLElems(pInfo, ALTER_USER_PRIVILEGES, 2, &X, &Y);}
cmd ::= ALTER DNODE IP(X) ids(Y).               { setDCLSQLElems(pInfo, ALTER_DNODE, 2, &X, &Y);          }
cmd ::= ALTER DNODE IP(X) ids(Y) ids(Z).        { setDCLSQLElems(pInfo, ALTER_DNODE, 3, &X, &Y, &Z);      }
cmd ::= ALTER LOCAL ids(X).                     { setDCLSQLElems(pInfo, ALTER_LOCAL, 1, &X);              }
cmd ::= ALTER DATABASE ids(X) alter_db_optr(Y). { SSQLToken t = {0};  setCreateDBSQL(pInfo, ALTER_DATABASE, &X, &Y, &t);}

// An IDENTIFIER can be a generic identifier, or one of several keywords.
// Any non-standard keyword can also be an identifier.
// And "ids" is an identifer-or-string.
%type ids {SSQLToken}
ids(A) ::= ID(X).        {A = X; }
ids(A) ::= STRING(X).    {A = X; }

%type ifexists {SSQLToken}
ifexists(X) ::= IF EXISTS.          {X.n = 1;}
ifexists(X) ::= .                   {X.n = 0;}

%type ifnotexists {SSQLToken}
ifnotexists(X) ::= IF NOT EXISTS.   {X.n = 1;}
ifnotexists(X) ::= .                {X.n = 0;}

/////////////////////////////////THE CREATE STATEMENT///////////////////////////////////////
cmd ::= CREATE DATABASE ifnotexists(Z) ids(X) db_optr(Y).  { setCreateDBSQL(pInfo, CREATE_DATABASE, &X, &Y, &Z);}
cmd ::= CREATE USER ids(X) PASS ids(Y).     { setDCLSQLElems(pInfo, CREATE_USER, 2, &X, &Y);}

%type keep {tVariantList*}
%destructor keep {tVariantListDestroy($$);}
keep(Y)    ::= KEEP tagitemlist(X).           { Y = X; }

tables(Y)  ::= TABLES INTEGER(X).             { Y = X; }
cache(Y)   ::= CACHE INTEGER(X).              { Y = X; }
replica(Y) ::= REPLICA INTEGER(X).            { Y = X; }
days(Y)    ::= DAYS INTEGER(X).               { Y = X; }
rows(Y)    ::= ROWS INTEGER(X).               { Y = X; }

ablocks(Y) ::= ABLOCKS ID(X).                 { Y = X; }
tblocks(Y) ::= TBLOCKS INTEGER(X).            { Y = X; }
ctime(Y)   ::= CTIME INTEGER(X).              { Y = X; }
clog(Y)    ::= CLOG INTEGER(X).               { Y = X; }
comp(Y)    ::= COMP INTEGER(X).               { Y = X; }
prec(Y)    ::= PRECISION STRING(X).           { Y = X; }

%type db_optr {SCreateDBInfo}
db_optr    ::= . {}
db_optr(Y) ::= db_optr(Z) tables(X).         { Y = Z; Y.tablesPerVnode = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) cache(X).          { Y = Z; Y.cacheBlockSize = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) replica(X).        { Y = Z; Y.replica = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) days(X).           { Y = Z; Y.daysPerFile = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) rows(X).           { Y = Z; Y.rowPerFileBlock = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) ablocks(X).        { Y = Z; Y.numOfAvgCacheBlocks = strtod(X.z, NULL); }
db_optr(Y) ::= db_optr(Z) tblocks(X).        { Y = Z; Y.numOfBlocksPerTable = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) ctime(X).          { Y = Z; Y.commitTime = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) clog(X).           { Y = Z; Y.commitLog = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) comp(X).           { Y = Z; Y.compressionLevel = strtol(X.z, NULL, 10); }
db_optr(Y) ::= db_optr(Z) prec(X).           { Y = Z; Y.precision = X; }
db_optr(Y) ::= db_optr(Z) keep(X).           { Y = Z; Y.keep = X; }

%type alter_db_optr {SCreateDBInfo}
alter_db_optr(Y) ::= REPLICA tagitem(A).     {
    Y.replica = A.i64Key;
}

%type typename {TAOS_FIELD}
typename(A) ::= ids(X).              { tSQLSetColumnType (&A, &X); }

//define binary type, e.g., binary(10), nchar(10)
typename(A) ::= ids(X) LP signed(Y) RP.    {
    X.type = -Y;          // negative value of name length
    tSQLSetColumnType(&A, &X);
}

%type signed {int64_t}
signed(A) ::= INTEGER(X).         { A = strtol(X.z, NULL, 10); }
signed(A) ::= PLUS INTEGER(X).    { A = strtol(X.z, NULL, 10); }
signed(A) ::= MINUS INTEGER(X).   { A = -strtol(X.z, NULL, 10);}

////////////////////////////////// The CREATE TABLE statement ///////////////////////////////
cmd ::= CREATE TABLE ifnotexists(Y) ids(X) cpxName(Z) create_table_args.  {
    X.n += Z.n;
    setCreatedMeterName(pInfo, &X, &Y);
}

%type create_table_args{SCreateTableSQL*}
create_table_args(A) ::= LP columnlist(X) RP. {
    A = tSetCreateSQLElems(X, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, A, NULL, TSQL_CREATE_NORMAL_METER);
}

// create metric
create_table_args(A) ::= LP columnlist(X) RP TAGS LP columnlist(Y) RP. {
    A = tSetCreateSQLElems(X, Y, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, A, NULL, TSQL_CREATE_NORMAL_METRIC);
}

// create meter by using metric
// create meter meter_name using metric_name tags(tag_values1, tag_values2)
create_table_args(A) ::= USING ids(X) cpxName(F) TAGS LP tagitemlist(Y) RP.  {
    X.n += F.n;
    A = tSetCreateSQLElems(NULL, NULL, &X, Y, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, A, NULL, TSQL_CREATE_METER_FROM_METRIC);
}

// create stream
// create table table_name as select count(*) from metric_name interval(time)
create_table_args(A) ::= AS select(S). {
    A = tSetCreateSQLElems(NULL, NULL, NULL, NULL, S, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, A, NULL, TSQL_CREATE_STREAM);
}

%type column{TAOS_FIELD}
%type columnlist{tFieldList*}
columnlist(A) ::= columnlist(X) COMMA column(Y).  {A = tFieldListAppend(X, &Y);   }
columnlist(A) ::= column(X).                      {A = tFieldListAppend(NULL, &X);}

// The information used for a column is the name and type of column:
// tinyint smallint int bigint float double bool timestamp binary(x) nchar(x)
column(A) ::= ids(X) typename(Y).          {
    tSQLSetColumnInfo(&A, &X, &Y);
}

%type tagitemlist {tVariantList*}
%destructor tagitemlist {tVariantListDestroy($$);}

%type tagitem {tVariant}
tagitemlist(A) ::= tagitemlist(X) COMMA tagitem(Y). { A = tVariantListAppend(X, &Y, -1);    }
tagitemlist(A) ::= tagitem(X).                      { A = tVariantListAppend(NULL, &X, -1); }

tagitem(A) ::= INTEGER(X).      {toTSDBType(X.type); tVariantCreate(&A, &X); }
tagitem(A) ::= FLOAT(X).        {toTSDBType(X.type); tVariantCreate(&A, &X); }
tagitem(A) ::= STRING(X).       {toTSDBType(X.type); tVariantCreate(&A, &X); }
tagitem(A) ::= BOOL(X).         {toTSDBType(X.type); tVariantCreate(&A, &X); }
tagitem(A) ::= NULL(X).         { X.type = TK_STRING; toTSDBType(X.type); tVariantCreate(&A, &X); }

tagitem(A) ::= MINUS(X) INTEGER(Y).{
    X.n += Y.n;
    X.type = Y.type;
    toTSDBType(X.type);
    tVariantCreate(&A, &X);
}

tagitem(A) ::= MINUS(X) FLOAT(Y).  {
    X.n += Y.n;
    X.type = Y.type;
    toTSDBType(X.type);
    tVariantCreate(&A, &X);
}


//////////////////////// The SELECT statement /////////////////////////////////
cmd ::= select(X).  {
    setSQLInfo(pInfo, X, NULL, TSQL_QUERY_METER);
}

%type select {SQuerySQL*}
%destructor select {destroyQuerySql($$);}
select(A) ::= SELECT(T) selcollist(W) from(X) where_opt(Y) interval_opt(K) fill_opt(F) sliding_opt(S) groupby_opt(P) orderby_opt(Z) having_opt(N) slimit_opt(G) limit_opt(L). {
  A = tSetQuerySQLElems(&T, W, &X, Y, P, Z, &K, &S, F, &L, &G);
}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an opcode of TK_ALL.
%type selcollist {tSQLExprList*}
%destructor selcollist {tSQLExprListDestroy($$);}

%type sclp {tSQLExprList*}
%destructor sclp {tSQLExprListDestroy($$);}
sclp(A) ::= selcollist(X) COMMA.             {A = X;}
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(P) expr(X) as(Y).     {
   A = tSQLExprListAppend(P, X, Y.n?&Y:0);
}

selcollist(A) ::= sclp(P) STAR. {
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   A = tSQLExprListAppend(P, pNode, 0);
}

// An option "AS <id>" phrase that can follow one of the expressions that
// define the result set, or one of the tables in the FROM clause.
//
%type as {SSQLToken}
as(X) ::= AS ids(Y).    { X = Y;    }
as(X) ::= ids(Y).       { X = Y;    }
as(X) ::= .             { X.n = 0;  }

// A complete FROM clause.
%type from {SSQLToken}
// current not support query from no-table
from(A) ::= FROM ids(X) cpxName(Y).                 {A = X; A.n += Y.n;}

// The value of interval should be the form of "number+[a,s,m,h,d,n,y]" or "now"
%type tmvar {SSQLToken}
tmvar(A) ::= VARIABLE(X).   {A = X;}

%type interval_opt {SSQLToken}
interval_opt(N) ::= INTERVAL LP tmvar(E) RP.    {N = E;     }
interval_opt(N) ::= .                           {N.n = 0;   }

%type fill_opt {tVariantList*}
%destructor fill_opt {tVariantListDestroy($$);}
fill_opt(N) ::= .                               {N = 0;     }
fill_opt(N) ::= FILL LP ID(Y) COMMA tagitemlist(X) RP.      {
    tVariant A = {0};
    toTSDBType(Y.type);
    tVariantCreate(&A, &Y);

    tVariantListInsert(X, &A, -1, 0);
    N = X;
}

fill_opt(N) ::= FILL LP ID(Y) RP.               {
    toTSDBType(Y.type);
    N = tVariantListAppendToken(NULL, &Y, -1);
}

%type sliding_opt {SSQLToken}
sliding_opt(K) ::= SLIDING LP tmvar(E) RP.      {K = E;     }
sliding_opt(K) ::= .                            {K.n = 0;   }

%type orderby_opt {tVariantList*}
%destructor orderby_opt {tVariantListDestroy($$);}

%type sortlist {tVariantList*}
%destructor sortlist {tVariantListDestroy($$);}

%type sortitem {tVariant}
%destructor sortitem {tVariantDestroy(&$$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}

sortlist(A) ::= sortlist(X) COMMA item(Y) sortorder(Z). {
    A = tVariantListAppend(X, &Y, Z);
}

sortlist(A) ::= item(Y) sortorder(Z). {
  A = tVariantListAppend(NULL, &Y, Z);
}

%type item {tVariant}
item(A) ::= ids(X) cpxName(Y).   {
  toTSDBType(X.type);
  X.n += Y.n;

  tVariantCreate(&A, &X);
}

%type sortorder {int}
sortorder(A) ::= ASC.           {A = TSQL_SO_ASC; }
sortorder(A) ::= DESC.          {A = TSQL_SO_DESC;}
sortorder(A) ::= .              {A = TSQL_SO_ASC;}  //default is descend order

//group by clause
%type groupby_opt {tVariantList*}
%destructor groupby_opt {tVariantListDestroy($$);}
%type grouplist {tVariantList*}
%destructor grouplist {tVariantListDestroy($$);}

groupby_opt(A) ::= .                       {A = 0;}
groupby_opt(A) ::= GROUP BY grouplist(X).  {A = X;}

grouplist(A) ::= grouplist(X) COMMA item(Y).    {
  A = tVariantListAppend(X, &Y, -1);
}

grouplist(A) ::= item(X).                       {
  A = tVariantListAppend(NULL, &X, -1);
}

//having clause, ignore the input condition in having
%type having_opt {tSQLExpr*}
%destructor having_opt {tSQLExprDestroy($$);}
having_opt(A) ::=.                  {A = 0;}
having_opt(A) ::= HAVING expr(X).   {A = X;}

//limit-offset subclause
%type limit_opt {SLimitVal}
limit_opt(A) ::= .                     {A.limit = -1; A.offset = 0;}
limit_opt(A) ::= LIMIT signed(X).      {A.limit = X;  A.offset = 0;}
limit_opt(A) ::= LIMIT signed(X) OFFSET signed(Y).
                                       {A.limit = X;  A.offset = Y;}
limit_opt(A) ::= LIMIT signed(X) COMMA signed(Y).
                                       {A.limit = Y;  A.offset = X;}

%type slimit_opt {SLimitVal}
slimit_opt(A) ::= .                    {A.limit = -1; A.offset = 0;}
slimit_opt(A) ::= SLIMIT signed(X).    {A.limit = X;  A.offset = 0;}
slimit_opt(A) ::= SLIMIT signed(X) SOFFSET signed(Y).
                                       {A.limit = X;  A.offset = Y;}
slimit_opt(A) ::= SLIMIT signed(X) COMMA  signed(Y).
                                       {A.limit = Y;  A.offset = X;}

%type where_opt {tSQLExpr*}
%destructor where_opt {tSQLExprDestroy($$);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X;}

/////////////////////////// Expression Processing /////////////////////////////
//
%type expr {tSQLExpr*}
%destructor expr {tSQLExprDestroy($$);}

expr(A) ::= LP expr(X) RP.   {A = X; }

expr(A) ::= ID(X).           {A = tSQLExprIdValueCreate(&X, TK_ID);}
expr(A) ::= ID(X) DOT ID(Y). {X.n += (1+Y.n); A = tSQLExprIdValueCreate(&X, TK_ID);}
expr(A) ::= ID(X) DOT STAR(Y). {X.n += (1+Y.n); A = tSQLExprIdValueCreate(&X, TK_ALL);}

expr(A) ::= INTEGER(X).      {A = tSQLExprIdValueCreate(&X, TK_INTEGER);}
expr(A) ::= MINUS(X) INTEGER(Y). {X.n += Y.n; X.type = TK_INTEGER; A = tSQLExprIdValueCreate(&X, TK_INTEGER);}
expr(A) ::= PLUS(X)  INTEGER(Y). {X.n += Y.n; X.type = TK_INTEGER; A = tSQLExprIdValueCreate(&X, TK_INTEGER);}
expr(A) ::= FLOAT(X).        {A = tSQLExprIdValueCreate(&X, TK_FLOAT);}
expr(A) ::= MINUS(X) FLOAT(Y).       {X.n += Y.n; X.type = TK_FLOAT; A = tSQLExprIdValueCreate(&X, TK_FLOAT);}
expr(A) ::= PLUS(X) FLOAT(Y).        {X.n += Y.n; X.type = TK_FLOAT; A = tSQLExprIdValueCreate(&X, TK_FLOAT);}
expr(A) ::= STRING(X).       {A = tSQLExprIdValueCreate(&X, TK_STRING);}
expr(A) ::= NOW(X).          {A = tSQLExprIdValueCreate(&X, TK_NOW); }
expr(A) ::= VARIABLE(X).     {A = tSQLExprIdValueCreate(&X, TK_VARIABLE);}
expr(A) ::= BOOL(X).         {A = tSQLExprIdValueCreate(&X, TK_BOOL);}
// normal functions: min(x)
expr(A) ::= ID(X) LP exprlist(Y) RP(E). {
  A = tSQLExprCreateFunction(Y, &X, &E, X.type);
}

// this is for: count(*)/first(*)/last(*) operation
expr(A) ::= ID(X) LP STAR RP(Y). {
  A = tSQLExprCreateFunction(NULL, &X, &Y, X.type);
}

//binary expression: a+2, b+3
expr(A) ::= expr(X) AND expr(Y).   {A = tSQLExprCreate(X, Y, TK_AND);}
expr(A) ::= expr(X) OR  expr(Y).   {A = tSQLExprCreate(X, Y, TK_OR); }

//binary relational expression
expr(A) ::= expr(X) LT expr(Y).    {A = tSQLExprCreate(X, Y, TK_LT);}
expr(A) ::= expr(X) GT expr(Y).    {A = tSQLExprCreate(X, Y, TK_GT);}
expr(A) ::= expr(X) LE expr(Y).    {A = tSQLExprCreate(X, Y, TK_LE);}
expr(A) ::= expr(X) GE expr(Y).    {A = tSQLExprCreate(X, Y, TK_GE);}
expr(A) ::= expr(X) NE expr(Y).    {A = tSQLExprCreate(X, Y, TK_NE);}
expr(A) ::= expr(X) EQ expr(Y).    {A = tSQLExprCreate(X, Y, TK_EQ);}

//binary arithmetic expression
expr(A) ::= expr(X) PLUS  expr(Y).   {A = tSQLExprCreate(X, Y, TK_PLUS);  }
expr(A) ::= expr(X) MINUS expr(Y).   {A = tSQLExprCreate(X, Y, TK_MINUS); }
expr(A) ::= expr(X) STAR  expr(Y).   {A = tSQLExprCreate(X, Y, TK_STAR);  }
expr(A) ::= expr(X) SLASH expr(Y).   {A = tSQLExprCreate(X, Y, TK_DIVIDE);}
expr(A) ::= expr(X) REM   expr(Y).   {A = tSQLExprCreate(X, Y, TK_REM);   }

//like expression
expr(A) ::= expr(X) LIKE  expr(Y).   {A = tSQLExprCreate(X, Y, TK_LIKE);  }

//in expression
expr(A) ::= expr(X) IN LP exprlist(Y) RP.   {A = tSQLExprCreate(X, (tSQLExpr*)Y, TK_IN); }

%type exprlist {tSQLExprList*}
%destructor exprlist {tSQLExprListDestroy($$);}

%type expritem {tSQLExpr*}
%destructor expritem {tSQLExprDestroy($$);}

exprlist(A) ::= exprlist(X) COMMA expritem(Y). {A = tSQLExprListAppend(X,Y,0);}
exprlist(A) ::= expritem(X).            {A = tSQLExprListAppend(0,X,0);}
expritem(A) ::= expr(X).                {A = X;}
expritem(A) ::= .                       {A = 0;}

////////////////////////// The INSERT command /////////////////////////////////
// add support "values() values() values() tags()" operation....
cmd ::= INSERT INTO cpxName(X) insert_value_list(K). {
    tSetInsertSQLElems(pInfo, &X, K);
}

%type insert_value_list {tSQLExprListList*}
insert_value_list(X) ::= VALUES LP itemlist(Y) RP. {X = tSQLListListAppend(NULL, Y);}
insert_value_list(X) ::= insert_value_list(K) VALUES LP itemlist(Y) RP.
{X = tSQLListListAppend(K, Y);}

//cmd ::= INSERT INTO cpxName(X) select(S).
//            {sqliteInsert(pParse, sqliteSrcListAppend(0,&X,&D), 0, S, F, R);}

%type itemlist {tSQLExprList*}
%destructor itemlist {tSQLExprListDestroy($$);}

itemlist(A) ::= itemlist(X) COMMA expr(Y).  {A = tSQLExprListAppend(X,Y,0);}
itemlist(A) ::= expr(X).                    {A = tSQLExprListAppend(0,X,0);}

///////////////////////////////////reset query cache//////////////////////////////////////
cmd ::= RESET QUERY CACHE.  { setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}

///////////////////////////////////ALTER TABLE statement//////////////////////////////////
cmd ::= ALTER TABLE ids(X) cpxName(F) ADD COLUMN columnlist(A).     {
    X.n += F.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, A, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}

cmd ::= ALTER TABLE ids(X) cpxName(F) DROP COLUMN ids(A).     {
    X.n += F.n;

    toTSDBType(A.type);
    tVariantList* K = tVariantListAppendToken(NULL, &A, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}

//////////////////////////////////ALTER TAGS statement/////////////////////////////////////
cmd ::= ALTER TABLE ids(X) cpxName(Y) ADD TAG columnlist(A).        {
    X.n += Y.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, A, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
cmd ::= ALTER TABLE ids(X) cpxName(Z) DROP TAG ids(Y).          {
    X.n += Z.n;

    toTSDBType(Y.type);
    tVariantList* A = tVariantListAppendToken(NULL, &Y, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}

cmd ::= ALTER TABLE ids(X) cpxName(F) CHANGE TAG ids(Y) ids(Z). {
    X.n += F.n;

    toTSDBType(Y.type);
    tVariantList* A = tVariantListAppendToken(NULL, &Y, -1);

    toTSDBType(Z.type);
    A = tVariantListAppendToken(A, &Z, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, NULL, A, ALTER_TABLE_TAGS_CHG);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_CHG);
}

cmd ::= ALTER TABLE ids(X) cpxName(F) SET ids(Y) EQ tagitem(Z).     {
    X.n += F.n;

    toTSDBType(Y.type);
    tVariantList* A = tVariantListAppendToken(NULL, &Y, -1);
    A = tVariantListAppend(A, &Z, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&X, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}

////////////////////////////////////////kill statement///////////////////////////////////////
cmd ::= KILL CONNECTION IP(X) COLON(Z) INTEGER(Y).   {X.n += (Z.n + Y.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &X);}
cmd ::= KILL STREAM IP(X) COLON(Z) INTEGER(Y) COLON(K) INTEGER(F).       {X.n += (Z.n + Y.n + K.n + F.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &X);}
cmd ::= KILL QUERY IP(X) COLON(Z) INTEGER(Y) COLON(K) INTEGER(F).        {X.n += (Z.n + Y.n + K.n + F.n); setDCLSQLElems(pInfo, KILL_QUERY, 1, &X);}

%fallback ID ABORT AFTER ASC ATTACH BEFORE BEGIN CASCADE CLUSTER CONFLICT COPY DATABASE DEFERRED
  DELIMITERS DESC DETACH EACH END EXPLAIN FAIL FOR GLOB IGNORE IMMEDIATE INITIALLY INSTEAD
  LIKE MATCH KEY OF OFFSET RAISE REPLACE RESTRICT ROW STATEMENT TRIGGER VIEW ALL
  COUNT SUM AVG MIN MAX FIRST LAST TOP BOTTOM STDDEV PERCENTILE APERCENTILE LEASTSQUARES HISTOGRAM DIFF
  SPREAD WAVG INTERP LAST_ROW NOW IP SEMI NONE PREV LINEAR IMPORT METRIC TBNAME JOIN METRICS STABLE.