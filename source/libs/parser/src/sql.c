/*
** 2000-05-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Driver template for the LEMON parser generator.
**
** The "lemon" program processes an LALR(1) input grammar file, then uses
** this template to construct a parser.  The "lemon" program inserts text
** at each "%%" line.  Also, any "P-a-r-s-e" identifer prefix (without the
** interstitial "-" characters) contained in this template is changed into
** the value of the %name directive from the grammar.  Otherwise, the content
** of this template is copied straight through into the generate parser
** source file.
**
** The following is the concatenation of all %include directives from the
** input grammar file:
*/
#include <stdio.h>
/************ Begin %include sections from the grammar ************************/
#line 11 "sql.y"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

#define ALLOW_FORBID_FUNC

#include "functionMgt.h"
#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"

#define YYSTACKDEPTH 0
#line 45 "sql.c"
/**************** End of %include directives **********************************/
/* These constants specify the various numeric values for terminal symbols
** in a format understandable to "makeheaders".  This section is blank unless
** "lemon" is run with the "-m" command-line option.
***************** Begin makeheaders token definitions *************************/
#if INTERFACE
#define TK_OR                              1
#define TK_AND                             2
#define TK_UNION                           3
#define TK_ALL                             4
#define TK_MINUS                           5
#define TK_EXCEPT                          6
#define TK_INTERSECT                       7
#define TK_NK_BITAND                       8
#define TK_NK_BITOR                        9
#define TK_NK_LSHIFT                      10
#define TK_NK_RSHIFT                      11
#define TK_NK_PLUS                        12
#define TK_NK_MINUS                       13
#define TK_NK_STAR                        14
#define TK_NK_SLASH                       15
#define TK_NK_REM                         16
#define TK_NK_CONCAT                      17
#define TK_CREATE                         18
#define TK_ACCOUNT                        19
#define TK_NK_ID                          20
#define TK_PASS                           21
#define TK_NK_STRING                      22
#define TK_ALTER                          23
#define TK_PPS                            24
#define TK_TSERIES                        25
#define TK_STORAGE                        26
#define TK_STREAMS                        27
#define TK_QTIME                          28
#define TK_DBS                            29
#define TK_USERS                          30
#define TK_CONNS                          31
#define TK_STATE                          32
#define TK_NK_COMMA                       33
#define TK_HOST                           34
#define TK_IS_IMPORT                      35
#define TK_NK_INTEGER                     36
#define TK_CREATEDB                       37
#define TK_USER                           38
#define TK_ENABLE                         39
#define TK_SYSINFO                        40
#define TK_ADD                            41
#define TK_DROP                           42
#define TK_GRANT                          43
#define TK_ON                             44
#define TK_TO                             45
#define TK_REVOKE                         46
#define TK_FROM                           47
#define TK_SUBSCRIBE                      48
#define TK_READ                           49
#define TK_WRITE                          50
#define TK_NK_DOT                         51
#define TK_WITH                           52
#define TK_ENCRYPT_KEY                    53
#define TK_DNODE                          54
#define TK_PORT                           55
#define TK_DNODES                         56
#define TK_RESTORE                        57
#define TK_NK_IPTOKEN                     58
#define TK_FORCE                          59
#define TK_UNSAFE                         60
#define TK_CLUSTER                        61
#define TK_LOCAL                          62
#define TK_QNODE                          63
#define TK_BNODE                          64
#define TK_SNODE                          65
#define TK_MNODE                          66
#define TK_VNODE                          67
#define TK_DATABASE                       68
#define TK_USE                            69
#define TK_FLUSH                          70
#define TK_TRIM                           71
#define TK_S3MIGRATE                      72
#define TK_COMPACT                        73
#define TK_IF                             74
#define TK_NOT                            75
#define TK_EXISTS                         76
#define TK_BUFFER                         77
#define TK_CACHEMODEL                     78
#define TK_CACHESIZE                      79
#define TK_COMP                           80
#define TK_DURATION                       81
#define TK_NK_VARIABLE                    82
#define TK_MAXROWS                        83
#define TK_MINROWS                        84
#define TK_KEEP                           85
#define TK_PAGES                          86
#define TK_PAGESIZE                       87
#define TK_TSDB_PAGESIZE                  88
#define TK_PRECISION                      89
#define TK_REPLICA                        90
#define TK_VGROUPS                        91
#define TK_SINGLE_STABLE                  92
#define TK_RETENTIONS                     93
#define TK_SCHEMALESS                     94
#define TK_WAL_LEVEL                      95
#define TK_WAL_FSYNC_PERIOD               96
#define TK_WAL_RETENTION_PERIOD           97
#define TK_WAL_RETENTION_SIZE             98
#define TK_WAL_ROLL_PERIOD                99
#define TK_WAL_SEGMENT_SIZE               100
#define TK_STT_TRIGGER                    101
#define TK_TABLE_PREFIX                   102
#define TK_TABLE_SUFFIX                   103
#define TK_S3_CHUNKSIZE                   104
#define TK_S3_KEEPLOCAL                   105
#define TK_S3_COMPACT                     106
#define TK_KEEP_TIME_OFFSET               107
#define TK_ENCRYPT_ALGORITHM              108
#define TK_NK_COLON                       109
#define TK_BWLIMIT                        110
#define TK_START                          111
#define TK_TIMESTAMP                      112
#define TK_END                            113
#define TK_TABLE                          114
#define TK_NK_LP                          115
#define TK_NK_RP                          116
#define TK_STABLE                         117
#define TK_COLUMN                         118
#define TK_MODIFY                         119
#define TK_RENAME                         120
#define TK_TAG                            121
#define TK_SET                            122
#define TK_NK_EQ                          123
#define TK_USING                          124
#define TK_TAGS                           125
#define TK_FILE                           126
#define TK_BOOL                           127
#define TK_TINYINT                        128
#define TK_SMALLINT                       129
#define TK_INT                            130
#define TK_INTEGER                        131
#define TK_BIGINT                         132
#define TK_FLOAT                          133
#define TK_DOUBLE                         134
#define TK_BINARY                         135
#define TK_NCHAR                          136
#define TK_UNSIGNED                       137
#define TK_JSON                           138
#define TK_VARCHAR                        139
#define TK_MEDIUMBLOB                     140
#define TK_BLOB                           141
#define TK_VARBINARY                      142
#define TK_GEOMETRY                       143
#define TK_DECIMAL                        144
#define TK_COMMENT                        145
#define TK_MAX_DELAY                      146
#define TK_WATERMARK                      147
#define TK_ROLLUP                         148
#define TK_TTL                            149
#define TK_SMA                            150
#define TK_DELETE_MARK                    151
#define TK_FIRST                          152
#define TK_LAST                           153
#define TK_SHOW                           154
#define TK_FULL                           155
#define TK_PRIVILEGES                     156
#define TK_DATABASES                      157
#define TK_TABLES                         158
#define TK_STABLES                        159
#define TK_MNODES                         160
#define TK_QNODES                         161
#define TK_ARBGROUPS                      162
#define TK_FUNCTIONS                      163
#define TK_INDEXES                        164
#define TK_ACCOUNTS                       165
#define TK_APPS                           166
#define TK_CONNECTIONS                    167
#define TK_LICENCES                       168
#define TK_GRANTS                         169
#define TK_LOGS                           170
#define TK_MACHINES                       171
#define TK_ENCRYPTIONS                    172
#define TK_QUERIES                        173
#define TK_SCORES                         174
#define TK_TOPICS                         175
#define TK_VARIABLES                      176
#define TK_BNODES                         177
#define TK_SNODES                         178
#define TK_TRANSACTIONS                   179
#define TK_DISTRIBUTED                    180
#define TK_CONSUMERS                      181
#define TK_SUBSCRIPTIONS                  182
#define TK_VNODES                         183
#define TK_ALIVE                          184
#define TK_VIEWS                          185
#define TK_VIEW                           186
#define TK_COMPACTS                       187
#define TK_NORMAL                         188
#define TK_CHILD                          189
#define TK_LIKE                           190
#define TK_TBNAME                         191
#define TK_QTAGS                          192
#define TK_AS                             193
#define TK_SYSTEM                         194
#define TK_TSMA                           195
#define TK_INTERVAL                       196
#define TK_RECURSIVE                      197
#define TK_TSMAS                          198
#define TK_FUNCTION                       199
#define TK_INDEX                          200
#define TK_COUNT                          201
#define TK_LAST_ROW                       202
#define TK_META                           203
#define TK_ONLY                           204
#define TK_TOPIC                          205
#define TK_CONSUMER                       206
#define TK_GROUP                          207
#define TK_DESC                           208
#define TK_DESCRIBE                       209
#define TK_RESET                          210
#define TK_QUERY                          211
#define TK_CACHE                          212
#define TK_EXPLAIN                        213
#define TK_ANALYZE                        214
#define TK_VERBOSE                        215
#define TK_NK_BOOL                        216
#define TK_RATIO                          217
#define TK_NK_FLOAT                       218
#define TK_OUTPUTTYPE                     219
#define TK_AGGREGATE                      220
#define TK_BUFSIZE                        221
#define TK_LANGUAGE                       222
#define TK_REPLACE                        223
#define TK_STREAM                         224
#define TK_INTO                           225
#define TK_PAUSE                          226
#define TK_RESUME                         227
#define TK_PRIMARY                        228
#define TK_KEY                            229
#define TK_TRIGGER                        230
#define TK_AT_ONCE                        231
#define TK_WINDOW_CLOSE                   232
#define TK_IGNORE                         233
#define TK_EXPIRED                        234
#define TK_FILL_HISTORY                   235
#define TK_UPDATE                         236
#define TK_SUBTABLE                       237
#define TK_UNTREATED                      238
#define TK_KILL                           239
#define TK_CONNECTION                     240
#define TK_TRANSACTION                    241
#define TK_BALANCE                        242
#define TK_VGROUP                         243
#define TK_LEADER                         244
#define TK_MERGE                          245
#define TK_REDISTRIBUTE                   246
#define TK_SPLIT                          247
#define TK_DELETE                         248
#define TK_INSERT                         249
#define TK_NK_BIN                         250
#define TK_NK_HEX                         251
#define TK_NULL                           252
#define TK_NK_QUESTION                    253
#define TK_NK_ALIAS                       254
#define TK_NK_ARROW                       255
#define TK_ROWTS                          256
#define TK_QSTART                         257
#define TK_QEND                           258
#define TK_QDURATION                      259
#define TK_WSTART                         260
#define TK_WEND                           261
#define TK_WDURATION                      262
#define TK_IROWTS                         263
#define TK_ISFILLED                       264
#define TK_FLOW                           265
#define TK_FHIGH                          266
#define TK_FFULL                          267
#define TK_CAST                           268
#define TK_NOW                            269
#define TK_TODAY                          270
#define TK_TIMEZONE                       271
#define TK_CLIENT_VERSION                 272
#define TK_SERVER_VERSION                 273
#define TK_SERVER_STATUS                  274
#define TK_CURRENT_USER                   275
#define TK_CASE                           276
#define TK_WHEN                           277
#define TK_THEN                           278
#define TK_ELSE                           279
#define TK_BETWEEN                        280
#define TK_IS                             281
#define TK_NK_LT                          282
#define TK_NK_GT                          283
#define TK_NK_LE                          284
#define TK_NK_GE                          285
#define TK_NK_NE                          286
#define TK_MATCH                          287
#define TK_NMATCH                         288
#define TK_CONTAINS                       289
#define TK_IN                             290
#define TK_JOIN                           291
#define TK_INNER                          292
#define TK_LEFT                           293
#define TK_RIGHT                          294
#define TK_OUTER                          295
#define TK_SEMI                           296
#define TK_ANTI                           297
#define TK_ASOF                           298
#define TK_WINDOW                         299
#define TK_WINDOW_OFFSET                  300
#define TK_JLIMIT                         301
#define TK_SELECT                         302
#define TK_NK_HINT                        303
#define TK_DISTINCT                       304
#define TK_WHERE                          305
#define TK_PARTITION                      306
#define TK_BY                             307
#define TK_SESSION                        308
#define TK_STATE_WINDOW                   309
#define TK_EVENT_WINDOW                   310
#define TK_COUNT_WINDOW                   311
#define TK_SLIDING                        312
#define TK_FILL                           313
#define TK_VALUE                          314
#define TK_VALUE_F                        315
#define TK_NONE                           316
#define TK_PREV                           317
#define TK_NULL_F                         318
#define TK_LINEAR                         319
#define TK_NEXT                           320
#define TK_HAVING                         321
#define TK_RANGE                          322
#define TK_EVERY                          323
#define TK_ORDER                          324
#define TK_SLIMIT                         325
#define TK_SOFFSET                        326
#define TK_LIMIT                          327
#define TK_OFFSET                         328
#define TK_ASC                            329
#define TK_NULLS                          330
#define TK_ABORT                          331
#define TK_AFTER                          332
#define TK_ATTACH                         333
#define TK_BEFORE                         334
#define TK_BEGIN                          335
#define TK_BITAND                         336
#define TK_BITNOT                         337
#define TK_BITOR                          338
#define TK_BLOCKS                         339
#define TK_CHANGE                         340
#define TK_COMMA                          341
#define TK_CONCAT                         342
#define TK_CONFLICT                       343
#define TK_COPY                           344
#define TK_DEFERRED                       345
#define TK_DELIMITERS                     346
#define TK_DETACH                         347
#define TK_DIVIDE                         348
#define TK_DOT                            349
#define TK_EACH                           350
#define TK_FAIL                           351
#define TK_FOR                            352
#define TK_GLOB                           353
#define TK_ID                             354
#define TK_IMMEDIATE                      355
#define TK_IMPORT                         356
#define TK_INITIALLY                      357
#define TK_INSTEAD                        358
#define TK_ISNULL                         359
#define TK_MODULES                        360
#define TK_NK_BITNOT                      361
#define TK_NK_SEMI                        362
#define TK_NOTNULL                        363
#define TK_OF                             364
#define TK_PLUS                           365
#define TK_PRIVILEGE                      366
#define TK_RAISE                          367
#define TK_RESTRICT                       368
#define TK_ROW                            369
#define TK_STAR                           370
#define TK_STATEMENT                      371
#define TK_STRICT                         372
#define TK_STRING                         373
#define TK_TIMES                          374
#define TK_VALUES                         375
#define TK_VARIABLE                       376
#define TK_WAL                            377
#define TK_ENCODE                         378
#define TK_COMPRESS                       379
#define TK_LEVEL                          380
#endif
/**************** End makeheaders token definitions ***************************/

/* The next sections is a series of control #defines.
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used to store the integer codes
**                       that represent terminal and non-terminal symbols.
**                       "unsigned char" is used if there are fewer than
**                       256 symbols.  Larger types otherwise.
**    YYNOCODE           is a number of type YYCODETYPE that is not used for
**                       any terminal or nonterminal symbol.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       (also known as: "terminal symbols") have fall-back
**                       values which should be used if the original symbol
**                       would not parse.  This permits keywords to sometimes
**                       be used as identifiers, for example.
**    YYACTIONTYPE       is the data type used for "action codes" - numbers
**                       that indicate what to do in response to the next
**                       token.
**    ParseTOKENTYPE     is the data type used for minor type for terminal
**                       symbols.  Background: A "minor type" is a semantic
**                       value associated with a terminal or non-terminal
**                       symbols.  For example, for an "ID" terminal symbol,
**                       the minor type might be the name of the identifier.
**                       Each non-terminal can have a different minor type.
**                       Terminal symbols all have the same minor type, though.
**                       This macros defines the minor type for terminal 
**                       symbols.
**    YYMINORTYPE        is the data type used for all minor types.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    ParseCTX_*         As ParseARG_ except for %extra_context
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYNTOKEN           Number of terminal symbols
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
**    YY_MIN_REDUCE      Minimum value for reduce actions
**    YY_MAX_REDUCE      Maximum value for reduce actions
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned short int
#define YYNOCODE 562
#define YYACTIONTYPE unsigned short int
#if INTERFACE
#define ParseTOKENTYPE  SToken 
#endif
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SDataType yy96;
  bool yy125;
  SToken yy305;
  int8_t yy339;
  SNodeList* yy436;
  EOrder yy446;
  EShowKind yy477;
  int32_t yy564;
  EOperatorType yy748;
  STokenPair yy797;
  SNode* yy800;
  EJoinType yy844;
  ENullOrder yy889;
  int64_t yy965;
  SAlterOption yy1017;
  EJoinSubType yy1042;
  SShowTablesOption yy1109;
  EFillMode yy1122;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#if INTERFACE
#define ParseARG_SDECL  SAstCreateContext* pCxt ;
#define ParseARG_PDECL , SAstCreateContext* pCxt 
#define ParseARG_PARAM ,pCxt 
#define ParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define ParseARG_STORE yypParser->pCxt =pCxt ;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#endif
#define YYFALLBACK 1
#define YYNSTATE             978
#define YYNRULE              759
#define YYNTOKEN             381
#define YY_MAX_SHIFT         977
#define YY_MIN_SHIFTREDUCE   1452
#define YY_MAX_SHIFTREDUCE   2210
#define YY_ERROR_ACTION      2211
#define YY_ACCEPT_ACTION     2212
#define YY_NO_ACTION         2213
#define YY_MIN_REDUCE        2214
#define YY_MAX_REDUCE        2972
/************* End control #defines *******************************************/

/* Define the yytestcase() macro to be a no-op if is not already defined
** otherwise.
**
** Applications can choose to define yytestcase() in the %include section
** to a macro that can assist in verifying code coverage.  For production
** code the yytestcase() macro should be turned off.  But it is useful
** for testing.
*/
#ifndef yytestcase
# define yytestcase(X)
#endif


/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.  
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N <= YY_MAX_SHIFT             Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   N between YY_MIN_SHIFTREDUCE       Shift to an arbitrary state then
**     and YY_MAX_SHIFTREDUCE           reduce by rule N-YY_MIN_SHIFTREDUCE.
**
**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as either:
**
**    (A)   N = yy_action[ yy_shift_ofst[S] + X ]
**    (B)   N = yy_default[S]
**
** The (A) formula is preferred.  The B formula is used instead if
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X.
**
** The formulas above are for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array.
**
** The following are the tables generated in this section:
**
**  yy_action[]        A single table containing all actions.
**  yy_lookahead[]     A table containing the lookahead for each entry in
**                     yy_action.  Used to detect hash collisions.
**  yy_shift_ofst[]    For each state, the offset into yy_action for
**                     shifting terminals.
**  yy_reduce_ofst[]   For each state, the offset into yy_action for
**                     shifting non-terminals after a reduce.
**  yy_default[]       Default action for each state.
**
*********** Begin parsing tables **********************************************/
#define YY_ACTTAB_COUNT (3125)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   497,   37,  340, 2476, 2478,  868, 2423,  124,  207,  381,
 /*    10 */   212, 2720,   47,   45, 2127,  211,  443,   33,  495, 2425,
 /*    20 */   476, 2215, 1945,   40,   39,  148, 1970,   46,   44,   43,
 /*    30 */    42,   41,  693, 2470,  868, 2423, 1943, 2551, 2036, 2306,
 /*    40 */  1970, 2724,  138, 2745,   66,  137,  136,  135,  134,  133,
 /*    50 */   132,  131,  130,  129,  499,  810,  157,    9,  813,   40,
 /*    60 */    39,  810,  157,   46,   44,   43,   42,   41, 2031,  460,
 /*    70 */  2625,  459, 2625,   40,   39,   19,  182,   46,   44,   43,
 /*    80 */    42,   41, 1951,  192,   29,  317, 2763,  495, 2425,  734,
 /*    90 */   138, 2726, 2729,  137,  136,  135,  134,  133,  132,  131,
 /*   100 */   130,  129,  872, 2237, 2710,  728,  850,  732,  730,  288,
 /*   110 */   287,  867,  974, 2405,  422,   15,  945,  944,  943,  942,
 /*   120 */   505, 1970,  941,  940,  162,  935,  934,  933,  932,  931,
 /*   130 */   930,  929,  161,  923,  922,  921,  504,  503,  918,  917,
 /*   140 */   916,  198,  197,  915,  500,  914,  913,  912, 2520,  868,
 /*   150 */  2423, 2744, 2038, 2039, 2786,   62, 2427,  867,  121, 2746,
 /*   160 */   854, 2748, 2749,  849, 2710,  872, 2412, 2172, 2102,  223,
 /*   170 */   200,  647, 2840, 2578,  648, 2262,  472, 2836,  194, 2848,
 /*   180 */   809, 2171,  149,  808,  127, 2848, 2849,  471,  155, 2853,
 /*   190 */  2938, 2006, 2016, 2575,  855,  655,  219, 2214,  648, 2262,
 /*   200 */   911, 2037, 2040,  185, 2887, 2226,  502,  501,  797,  218,
 /*   210 */   788, 1791, 1792, 2939,  799,  115, 1946, 1678, 1944,  744,
 /*   220 */   606,  147,  146,  145,  144,  143,  142,  141,  140,  139,
 /*   230 */  1952,  482, 1669,  901,  900,  899, 1673,  898, 1675, 1676,
 /*   240 */   897,  894,  872, 1684,  891, 1686, 1687,  888,  885,  882,
 /*   250 */   450,  449, 1949, 1950, 2003,  748, 2005, 2008, 2009, 2010,
 /*   260 */  2011, 2012, 2013, 2014, 2015, 2017, 2018, 2019,  846,  870,
 /*   270 */   869, 2030, 2032, 2033, 2034, 2035,    2,   47,   45, 2483,
 /*   280 */  2745,  527,  419,  841, 1968,  476,  747, 1945,  243,  469,
 /*   290 */   798,  594, 2007,  291,  445,  851,  840,  290, 2938,  867,
 /*   300 */  2481, 1943,  614, 2036, 2860, 2099, 2100, 2101, 2860, 2860,
 /*   310 */  2860, 2860, 2860,  605,  242, 1497,  797,  218,  573,  587,
 /*   320 */   616, 2939,  799, 2763,  586,  420,  575,  603,  448,  447,
 /*   330 */   543,  695,  585, 2031, 1504,  542,  663,  553,  657, 2617,
 /*   340 */    19, 2710,  324,  850,  714,  713,  712, 1951,  783,  868,
 /*   350 */  2423,  704,  154,  708,  697, 2004,  666,  707,  696, 1499,
 /*   360 */  1502, 1503,  706,  711,  453,  452,  868, 2423,  705,   55,
 /*   370 */  2102,  104,  451,  701,  700,  699,  430,  974,  444,  458,
 /*   380 */    15,  736,  479, 1971,  764, 1524,  148, 1523, 2744,  561,
 /*   390 */   181, 2786, 2938,  698,   50,  121, 2746,  854, 2748, 2749,
 /*   400 */   849,  153,  872, 2428,  867,  159,    3,  168, 2811, 2840,
 /*   410 */  2944,  218, 1955,  472, 2836, 2939,  799, 2038, 2039,  531,
 /*   420 */    53, 2558, 2537, 1525,  602,  601,  600,  599,  598,  593,
 /*   430 */   592,  591,  590,  427,  868, 2423,  580,  579,  578,  577,
 /*   440 */   576,  570,  569,  568, 2003,  563,  562,  442,  533,  529,
 /*   450 */   183,  554, 1779, 1780,  547,  394, 2006, 2016, 1798, 2578,
 /*   460 */   789,  784,  777,  773,   40,   39, 2037, 2040,   46,   44,
 /*   470 */    43,   42,   41,  478,  392,   77, 1974,  181,   76, 2575,
 /*   480 */   855, 1946, 1524, 1944, 1523,  488,  670,   40,   39,  421,
 /*   490 */  2429,   46,   44,   43,   42,   41,  667, 1973,  550,   50,
 /*   500 */   381,  252,  629,  627,  624,  622, 2860, 2099, 2100, 2101,
 /*   510 */  2860, 2860, 2860, 2860, 2860, 1818, 1819, 1949, 1950, 2003,
 /*   520 */  1525, 2005, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015,
 /*   530 */  2017, 2018, 2019,  846,  870,  869, 2030, 2032, 2033, 2034,
 /*   540 */  2035,    2,   12,   47,   45, 1945, 2745,   62,  522,  612,
 /*   550 */   610,  476,  415, 1945,  435,  232, 2565,  274,  668, 1943,
 /*   560 */  2212,  851,  254, 2626, 1817, 1820,  650, 1943, 2270, 2036,
 /*   570 */    40,   39,  225,  193,   46,   44,   43,   42,   41, 2477,
 /*   580 */  2478,  324,  687,  683,  679,  675,   63,  273,  238, 2763,
 /*   590 */  2578, 2483,  909,  174,  173,  906,  905,  904,  171, 2031,
 /*   600 */  1678,  441, 2065,  745, 2483, 1951,   19, 2710, 1970,  850,
 /*   610 */  2576,  855, 2481, 1951,  481, 1669,  901,  900,  899, 1673,
 /*   620 */   898, 1675, 1676,  845,  844, 2481, 1684,  843, 1686, 1687,
 /*   630 */   842,  885,  882,  102,   95,  974,  271,   94,  721,  184,
 /*   640 */    89,   88,  546,  974,   12,  231,   15, 2355,  928,  214,
 /*   650 */   508, 2381, 2703,  735, 2744,  507,  493, 2786,  538,  536,
 /*   660 */  1951,  186, 2746,  854, 2748, 2749,  849, 1914,  872, 2066,
 /*   670 */   322,  418,  289,  763,  525,  902, 2131,  521,  517,  513,
 /*   680 */   510,  539, 1970, 2038, 2039,   79,   40,   39,  724,  798,
 /*   690 */    46,   44,   43,   42,   41,  718,  716, 2938, 2102,  487,
 /*   700 */   486, 2855,  286,   62,  764,  259,   93,  652,  765, 2898,
 /*   710 */  1975, 2745, 2938,  649,  270,  797,  218,  482,  261,  268,
 /*   720 */  2939,  799, 2006, 2016,  266,  661,  813, 2852,  872,   85,
 /*   730 */  2944,  218, 2037, 2040,  324, 2939,  799, 1971,  256, 1946,
 /*   740 */    92, 1944,  650,  258, 2270, 1974,  160, 1946,   72, 1944,
 /*   750 */  2397,   71,  810,  157, 2763, 2413, 2415,  559, 2547,   36,
 /*   760 */   474, 2060, 2061, 2062, 2063, 2064, 2068, 2069, 2070, 2071,
 /*   770 */  2202,   12, 2710,   10,  850, 1949, 1950, 2104, 2105, 2106,
 /*   780 */  2107, 2108, 1974, 1949, 1950, 2003,  878, 2005, 2008, 2009,
 /*   790 */  2010, 2011, 2012, 2013, 2014, 2015, 2017, 2018, 2019,  846,
 /*   800 */   870,  869, 2030, 2032, 2033, 2034, 2035,    2,   47,   45,
 /*   810 */  2041,  491,  234,   62,   60, 1506,  476, 2704, 1945, 2744,
 /*   820 */   748, 1969, 2786,  207, 2745,  761,  121, 2746,  854, 2748,
 /*   830 */  2749,  849, 1943,  872, 2036, 2099, 2100, 2101,  200,  851,
 /*   840 */  2840, 2272,   40,   39,  472, 2836,   46,   44,   43,   42,
 /*   850 */    41, 2745, 2552, 2007, 2485,   40,   39, 2943, 2160,   46,
 /*   860 */    44,   43,   42,   41, 2031, 2938,  851, 2763, 2895, 1717,
 /*   870 */  1718,  125, 2888, 2236,  812,  187, 2848, 2849, 1951,  155,
 /*   880 */  2853,  834,  482, 2812, 2942, 2710,  374,  850, 2939, 2941,
 /*   890 */   324, 2201, 2943,  872, 2763,  909,  174,  173,  906,  905,
 /*   900 */   904,  171, 2745,  750, 2617,  787,  565, 2547,  974, 1973,
 /*   910 */  2901,   48, 2710,  541,  850,  540, 2004,  851,  213, 2908,
 /*   920 */   780,  779, 2158, 2159, 2161, 2162, 2163,   46,   44,   43,
 /*   930 */    42,   41, 2744, 2763, 2710, 2786,  877,  876,  875,  121,
 /*   940 */  2746,  854, 2748, 2749,  849, 2763,  872,  539, 2038, 2039,
 /*   950 */  2143, 2958,  207, 2840,  868, 2423, 1504,  472, 2836, 2744,
 /*   960 */   480,  236, 2786, 2710, 2395,  850,  121, 2746,  854, 2748,
 /*   970 */  2749,  849, 1974,  872,  548,  868, 2423,  644, 2958, 1975,
 /*   980 */  2840, 2551, 1502, 1503,  472, 2836,  642, 2006, 2016,  638,
 /*   990 */   634,  868, 2423, 1913, 2410,  567, 2067, 2037, 2040, 1615,
 /*  1000 */   324, 1627, 2046,  868, 2423, 2195,   62,  786, 1970,  615,
 /*  1010 */  2744,  581, 1946, 2786, 1944, 1626, 1975,  121, 2746,  854,
 /*  1020 */  2748, 2749,  849,  370,  872,  490,  489,   35,  664, 2958,
 /*  1030 */  2152, 2840, 2408,   40,   39,  472, 2836,   46,   44,   43,
 /*  1040 */    42,   41,  331,  332, 2153, 1617, 2663,  330, 1949, 1950,
 /*  1050 */  2003,  101, 2005, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
 /*  1060 */  2015, 2017, 2018, 2019,  846,  870,  869, 2030, 2032, 2033,
 /*  1070 */  2034, 2035,    2,   47,   45,  868, 2423,  498, 2503, 2664,
 /*  1080 */  2419,  476,  322, 1945, 2483,  181,   34,  596, 2547,   51,
 /*  1090 */  2151,  617,  292,  665, 2571,  582, 2072, 1943, 2428, 2036,
 /*  1100 */   764,  868, 2423,  868, 2423,  817, 2720, 2745, 2938,  909,
 /*  1110 */   174,  173,  906,  905,  904,  171,  380, 2943,  741, 1527,
 /*  1120 */  1528,  583,  851,  669,  775, 2938, 2944,  218,  836, 2031,
 /*  1130 */  2812, 2939,  799,  764,  101,  299, 2724,  868, 2423, 2745,
 /*  1140 */  2235, 2938,  241, 1951, 2942,  221,  868, 2423, 2939, 2940,
 /*  1150 */  2763,  446,   14,   13,  851, 1631, 2931, 2420,  749, 2944,
 /*  1160 */   218,  868, 2423, 2418, 2939,  799,  294, 2942, 2710, 1630,
 /*  1170 */   850, 2483,  764,  974, 2745,  387,   48, 1872, 1873, 2007,
 /*  1180 */  2938,  302, 2763,   43,   42,   41, 2726, 2728,  473,  851,
 /*  1190 */  2855, 2872,  825,  324, 2427,  502,  501,  872, 2944,  218,
 /*  1200 */  2710, 2710,  850, 2939,  799, 1959, 1975, 2356,  868, 2423,
 /*  1210 */   868, 2423,  764, 2038, 2039, 2744, 2851, 2763, 2786, 1952,
 /*  1220 */  2938, 2036,  121, 2746,  854, 2748, 2749,  849,  816,  872,
 /*  1230 */   335,  810,  157,  589, 2958, 2710, 2840,  850, 2944,  218,
 /*  1240 */   472, 2836, 2004, 2939,  799,  619, 2124, 2744,  588, 2234,
 /*  1250 */  2786, 2031, 2006, 2016,  121, 2746,  854, 2748, 2749,  849,
 /*  1260 */   697,  872, 2037, 2040,  696, 1951, 2958, 2079, 2840,  689,
 /*  1270 */   688, 2855,  472, 2836,  691,  690,  324, 1946, 2483, 1944,
 /*  1280 */   710,  709, 2744,  802, 2233, 2786,  868, 2423,  496,  121,
 /*  1290 */  2746,  854, 2748, 2749,  849,  838,  872, 2850,  814, 2481,
 /*  1300 */  2396, 2958, 1970, 2840,  939,  937,  830,  472, 2836, 2232,
 /*  1310 */  2710, 2231, 2230, 1949, 1950, 2003, 2229, 2005, 2008, 2009,
 /*  1320 */  2010, 2011, 2012, 2013, 2014, 2015, 2017, 2018, 2019,  846,
 /*  1330 */   870,  869, 2030, 2032, 2033, 2034, 2035,    2,   47,   45,
 /*  1340 */   925,  868, 2423,  868, 2423, 2710,  476, 2631, 1945,  868,
 /*  1350 */  2423,  172,  764, 2745,  188, 2848, 2849, 2228,  155, 2853,
 /*  1360 */  2938,  342, 1943,  862, 2036,  868, 2423, 2225,  851,  863,
 /*  1370 */  2710, 2224, 2710, 2710,  911,  792, 2223, 2710, 2944,  218,
 /*  1380 */   114, 2222, 2745, 2939,  799,  866, 2221, 2220, 2219, 2218,
 /*  1390 */  2217, 2650,  158,  182, 2031, 2811, 2763,  851, 2483, 1960,
 /*  1400 */   903, 1955,  805, 2474,  907, 2426, 2414, 2474, 1951,  927,
 /*  1410 */   293, 2654,  908,  150, 2710, 2474,  850,  702, 2710, 2482,
 /*  1420 */   388, 2242,  967, 2460, 2293, 2763,  105,   91, 2710, 2398,
 /*  1430 */  1610, 2227, 2710, 2530,   54, 1963, 1965, 2710,  974,  192,
 /*  1440 */  1608,   15, 2710, 2710, 2113,  850,  715, 2710, 2710, 2710,
 /*  1450 */  2710, 2710,  870,  869, 2030, 2032, 2033, 2034, 2035, 2291,
 /*  1460 */  1846, 2744,  163,  279, 2786,  515,  277,  318,  121, 2746,
 /*  1470 */   854, 2748, 2749,  849, 2282,  872, 1611,  557, 2038, 2039,
 /*  1480 */  2815,  717, 2840, 2280,  425,  424,  472, 2836,  801,  281,
 /*  1490 */  2744,  703,  280, 2786,  483,  746,  719,  121, 2746,  854,
 /*  1500 */  2748, 2749,  849,  283,  872,  722,  282, 2745,  492, 2813,
 /*  1510 */  2036, 2840,  303,  977, 1606,  472, 2836, 2006, 2016,  738,
 /*  1520 */   164,  737,  851,  285,  164,  839,  284, 2037, 2040,  771,
 /*  1530 */    49,   49,  378,  201, 2204, 2205, 2004,  172,  329,  781,
 /*  1540 */  2031, 2731, 1946, 1954, 1944,   78, 2123,  965,  208,   64,
 /*  1550 */  2763, 1953,   14,   13,   49,   49,   78,  961,  957,  953,
 /*  1560 */   949,  106,  373,  169,  150,  172,  349,  348, 2710,  310,
 /*  1570 */   850,  351,  350,  353,  352,   75,  803,  811, 1949, 1950,
 /*  1580 */  2003,  152, 2005, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
 /*  1590 */  2015, 2017, 2018, 2019,  846,  870,  869, 2030, 2032, 2033,
 /*  1600 */  2034, 2035,    2, 1862,  880, 2733, 2348, 1870,   73,  355,
 /*  1610 */   354,  346, 2147, 2157, 2156, 2744,  308,  170, 2786, 2764,
 /*  1620 */   815,  333,  121, 2746,  854, 2748, 2749,  849,  822,  872,
 /*  1630 */  1588,  172, 2073,  151,  835,  209, 2840, 2020, 1815, 1805,
 /*  1640 */   472, 2836, 2745,  169,  826,  919,  345,  865, 1660,  357,
 /*  1650 */   356,  359,  358,  361,  360,  363,  362,  851,  386,  365,
 /*  1660 */   364,  367,  366,  369,  368,  120, 1561, 2347, 1580, 2263,
 /*  1670 */   920, 2891,  778,  465,  117, 2274, 1589, 2745,  785,  461,
 /*  1680 */   819, 2556,  506,  524,  757, 2763, 2269, 1691, 1936,  344,
 /*  1690 */  1912,  832,  851, 1578,  327, 2471, 2892,  806, 2902,  326,
 /*  1700 */  1699,  320, 2057, 2710,  793,  850, 2745,  794,  315,  323,
 /*  1710 */  2557, 2382, 1562,    5, 1706,  509, 1704,  514,  296,  439,
 /*  1720 */  2763,  851,  485,  484, 1937, 1957,  175, 1968,  523, 1978,
 /*  1730 */   963,  535,  534, 1956,  226,  227,  229,  537, 2710, 1839,
 /*  1740 */   850,  870,  869, 2030, 2032, 2033, 2034, 2035,  379, 2763,
 /*  1750 */  2744,  551, 1969, 2786,  558,  240,  560,  122, 2746,  854,
 /*  1760 */  2748, 2749,  849,  564,  872,  566,  608, 2710,  571,  850,
 /*  1770 */   584, 2840,  595, 2549,  597, 2839, 2836, 2309,  620,  621,
 /*  1780 */   618,  246,  604,  607,  609, 2744, 2745,  245, 2786,  625,
 /*  1790 */   623,  249,  122, 2746,  854, 2748, 2749,  849,  626,  872,
 /*  1800 */  1976,  848,    4,  645,  628,  630, 2840,  653,  646,  654,
 /*  1810 */   837, 2836, 1971,  658,  852, 1977,  257, 2786,  656,  659,
 /*  1820 */  1979,  122, 2746,  854, 2748, 2749,  849,   97,  872, 2763,
 /*  1830 */   260,  660,  263,  265,  662, 2840, 1980, 2745, 2572,  434,
 /*  1840 */  2836, 1981,   98,   99, 2566,  100,  671, 2710,  272,  850,
 /*  1850 */   725,  692,  851, 2745,  714,  713,  712,  694, 2411,  276,
 /*  1860 */   126,  704,  154,  708, 2407,  740,  278,  707,  851,  177,
 /*  1870 */   123,  726,  706,  711,  453,  452, 2409,  413,  705, 2404,
 /*  1880 */  2763,  178,  451,  701,  700,  699,  742,  103,  179, 1972,
 /*  1890 */  2640, 2637, 2636,  295, 2744,  165, 2763, 2786, 2710, 2618,
 /*  1900 */   850,  410, 2746,  854, 2748, 2749,  849,  847,  872,  833,
 /*  1910 */  2805,  382,  752,  751, 2710,  756,  850, 2745,  300,  782,
 /*  1920 */   753,  768,  759,  820,    8, 2907,  791,  758, 2906,  305,
 /*  1930 */   298,  307,  851,  766, 2879,  769,  767,  191,  309,  313,
 /*  1940 */   311,  796,  312,  314,  795, 2744, 2859,  466, 2786, 2961,
 /*  1950 */   804,  316,  189, 2746,  854, 2748, 2749,  849,  807,  872,
 /*  1960 */  2763, 2744,  156, 1973, 2786, 2856, 2121, 2119,  122, 2746,
 /*  1970 */   854, 2748, 2749,  849,  204,  872,  319, 2745, 2710,  383,
 /*  1980 */   850,  818, 2840,  325,  166, 2586, 2585, 2837, 2584,  470,
 /*  1990 */   220,  384,  851,    1,  167,  823,  824,  828,   61,  116,
 /*  2000 */   338, 2821,  858,  831,  856,  860,  861, 2745,  385,  343,
 /*  2010 */   113,  800, 2959, 2937, 2424, 2702,  874, 2701, 2697, 1476,
 /*  2020 */  2763,  969,  851,  970,  971, 2744, 2745, 2696, 2786,  372,
 /*  2030 */   966, 2688,  186, 2746,  854, 2748, 2749,  849, 2710,  872,
 /*  2040 */   850,  851,  176,  389, 2687,  375, 2679, 2678,  376,  973,
 /*  2050 */  2763,  744, 2694, 2693, 2685,   52,  431,  432, 2684,  414,
 /*  2060 */   391, 2673,  463, 2672, 2691, 2690, 2682, 2681, 2710, 2763,
 /*  2070 */   850, 2670, 2662,  401, 2669,  423,  393,  426, 2667, 2666,
 /*  2080 */  2899, 2475,  412,  402, 2661, 2744, 2660, 2710, 2786,  850,
 /*  2090 */  2745,   86,  411, 2746,  854, 2748, 2749,  849, 2655,  872,
 /*  2100 */   511,  512, 1896, 1897,  224,  851, 2745,  516, 2653,  518,
 /*  2110 */   519,  464, 1895,  520, 2652, 2744, 2651, 2649, 2786,  440,
 /*  2120 */  2648,  848,  404, 2746,  854, 2748, 2749,  849,  526,  872,
 /*  2130 */   528, 2647,  530, 2763, 2744,  532, 1883, 2786, 2646, 2622,
 /*  2140 */   228,  411, 2746,  854, 2748, 2749,  849, 2621,  872, 2763,
 /*  2150 */   230, 2710,   87,  850, 1842, 1841, 2599, 2598, 2597,  544,
 /*  2160 */   545, 2596, 2595, 2539,  549, 2536, 1778, 2710,  552,  850,
 /*  2170 */  2745, 2535, 2529,  790, 2526,  556,  555,  233, 2525, 2524,
 /*  2180 */    90, 2523, 2528, 2527, 2522,  851, 2521,  235, 2519, 2518,
 /*  2190 */  2517,  237, 2516,  574, 2514,  572, 2745, 2513, 2744, 2512,
 /*  2200 */  2511, 2786, 2510, 2534, 2509,  189, 2746,  854, 2748, 2749,
 /*  2210 */   849,  851,  872, 2763, 2744, 2508, 2507, 2786, 2532, 2515,
 /*  2220 */  2506,  410, 2746,  854, 2748, 2749,  849, 2505,  872, 2504,
 /*  2230 */  2806, 2710, 2745,  850, 2502, 2501, 2500, 2499, 2498, 2763,
 /*  2240 */  2497, 2496,  239,   96, 2495, 2494, 2493,  851, 1784, 2489,
 /*  2250 */  2488, 2487,  613,  244,  611,  475, 2492, 2710, 2745,  850,
 /*  2260 */  2564, 2533, 2531, 2491, 2490, 2960, 2486, 2484, 2313, 1628,
 /*  2270 */  2312, 2311, 2310,  851,  428, 2763, 1632,  429, 2744,  247,
 /*  2280 */   248, 2786,  250, 2745, 2308,  411, 2746,  854, 2748, 2749,
 /*  2290 */   849, 1624,  872, 2710, 2305,  850,  251,  632,  851, 1505,
 /*  2300 */   253, 2763,  633, 2304,  739,  631,  636, 2786, 2297,  635,
 /*  2310 */  2284,  406, 2746,  854, 2748, 2749,  849,  477,  872, 2710,
 /*  2320 */   637,  850,  639,  641,  643, 2258, 2763,   82,  199,  640,
 /*  2330 */  2257, 2730,  210,  651,  255,   83, 2620, 2616, 2606, 2594,
 /*  2340 */  2744,  262, 2593, 2786, 2710, 2745,  850,  411, 2746,  854,
 /*  2350 */  2748, 2749,  849,  264,  872,  267, 2570,  269, 2563, 2399,
 /*  2360 */   851,  673,  672, 1554, 2307, 2303, 2744, 2745,  674, 2786,
 /*  2370 */  2301,  676,  677,  396, 2746,  854, 2748, 2749,  849, 2299,
 /*  2380 */   872,  680,  851,  681,  678,  682, 2296,  684, 2763, 2279,
 /*  2390 */   686, 2744, 2277, 2278, 2786, 2276, 2745,  685,  395, 2746,
 /*  2400 */   854, 2748, 2749,  849, 2254,  872, 2710, 2401,  850, 1711,
 /*  2410 */  2763,  851, 1710,   74,  275, 2400, 1614, 1613, 1612, 2294,
 /*  2420 */  1609, 1607, 2292, 1605, 1604, 1596, 2283, 1603, 2710, 2281,
 /*  2430 */   850,  936, 1602, 1601,  938, 1598, 1597, 2253, 1595, 2763,
 /*  2440 */   454,  723,  455,  456, 2252,  457, 2251,  727, 2250,  729,
 /*  2450 */  2249, 2248,  731, 2744,  733,  128, 2786, 2710, 2619,  850,
 /*  2460 */   397, 2746,  854, 2748, 2749,  849,  720,  872, 1881, 1877,
 /*  2470 */  1879,   56, 1876, 2615, 1848, 2744,  297, 2745, 2786, 1850,
 /*  2480 */  1852,   28,  403, 2746,  854, 2748, 2749,  849, 1867,  872,
 /*  2490 */  2745,   67,  851,  743,   57, 2605,  180,  754,  755,  301,
 /*  2500 */  2592, 2591, 1827,   20, 2744,  851, 2745, 2786, 1826,   21,
 /*  2510 */  2943,  407, 2746,  854, 2748, 2749,  849,  760,  872, 2745,
 /*  2520 */  2763,  851,  762,   30,   17, 2174,  770,  304,  462,    6,
 /*  2530 */  2148,    7,  772, 2763,  851,   22,  203,  774, 2710,  776,
 /*  2540 */   850,   32,  215,  306, 2114, 2731, 2155, 2142,  216, 2763,
 /*  2550 */  2116, 2710,   65,  850,  190,  202,   31,   84,   24, 2112,
 /*  2560 */   217,  321, 2763, 2194, 2189, 2195, 2188, 2710,  467,  850,
 /*  2570 */  2193, 2096, 2192,  468,   59, 2095, 2590,  195, 2569,  107,
 /*  2580 */  2710,  328,  850,   58,  108, 2744, 2568, 2150, 2786,  205,
 /*  2590 */   109, 2562,  398, 2746,  854, 2748, 2749,  849, 2744,  872,
 /*  2600 */   334, 2786,  821,   23,   69,  408, 2746,  854, 2748, 2749,
 /*  2610 */   849,  337,  872,  827, 2744,  110, 2745, 2786,   18,   13,
 /*  2620 */    25,  399, 2746,  854, 2748, 2749,  849, 2744,  872, 2048,
 /*  2630 */  2786,  851, 2745,   11,  409, 2746,  854, 2748, 2749,  849,
 /*  2640 */  2047,  872, 1961,  196,  206, 2023,  887,  851,  890,  829,
 /*  2650 */  1996,  339,  336, 2561, 2022, 2021,  111,  893, 2745, 2763,
 /*  2660 */   896, 1655,   38,   16,  859,   26,  347, 1988,   27,   70,
 /*  2670 */   864,  857,  341,  851,  112, 2763,  117, 2710,   80,  850,
 /*  2680 */  2791, 2790,  871, 2745, 2025,   68,  873, 2210, 2209, 2208,
 /*  2690 */  2207, 1692,  879, 2710,  494,  850, 2745,  881,  851, 1689,
 /*  2700 */   883, 2763,  371,  884,  886, 1688, 2058,  889,  892,  895,
 /*  2710 */  1685,  851, 1679, 1677, 1683,  853, 1682, 1681, 1680, 2710,
 /*  2720 */   118,  850,  119, 1705, 2744,   81, 2763, 2786, 1701, 1592,
 /*  2730 */  1552,  400, 2746,  854, 2748, 2749,  849,  910,  872, 2763,
 /*  2740 */  2744, 1591, 1622, 2786, 2710, 2745,  850,  416, 2746,  854,
 /*  2750 */  2748, 2749,  849, 1590,  872, 1587, 1584, 2710, 1583,  850,
 /*  2760 */   851, 1582, 1581, 1579, 1577, 1576, 2744, 2745, 1575, 2786,
 /*  2770 */   924, 1621,  926,  417, 2746,  854, 2748, 2749,  849,  222,
 /*  2780 */   872, 1573,  851, 1572, 1571, 1570, 1569, 1568, 2763, 1567,
 /*  2790 */  1618, 2744, 1616, 1564, 2786, 1563, 1560, 1559, 2757, 2746,
 /*  2800 */   854, 2748, 2749,  849, 2744,  872, 2710, 2786,  850, 1558,
 /*  2810 */  2763, 2756, 2746,  854, 2748, 2749,  849, 1557,  872, 2302,
 /*  2820 */   946,  947,  948, 2300,  950,  951,  952, 2298, 2710,  954,
 /*  2830 */   850,  955, 2295,  956,  958,  960, 2275,  959,  962, 2273,
 /*  2840 */   964, 1494, 2247, 1477,  968, 1482, 1484,  377, 1947,  972,
 /*  2850 */   976,  390,  975, 2744, 2213, 2213, 2786, 2213, 2213, 2213,
 /*  2860 */  2755, 2746,  854, 2748, 2749,  849, 2213,  872, 2213, 2213,
 /*  2870 */  2213, 2213, 2213, 2213, 2213, 2744, 2213, 2745, 2786, 2213,
 /*  2880 */  2213, 2213,  436, 2746,  854, 2748, 2749,  849, 2213,  872,
 /*  2890 */  2745, 2213,  851, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  2900 */  2213, 2213, 2213, 2745, 2213,  851, 2213, 2213, 2213, 2213,
 /*  2910 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,  851, 2745,
 /*  2920 */  2763, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  2930 */  2213, 2213, 2213, 2763,  851, 2213, 2213, 2213, 2710, 2213,
 /*  2940 */   850, 2213, 2213, 2213, 2213, 2213, 2763, 2213, 2213, 2213,
 /*  2950 */  2213, 2710, 2213,  850, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  2960 */  2213, 2213, 2763, 2213, 2710, 2213,  850, 2213, 2213, 2213,
 /*  2970 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  2980 */  2710, 2213,  850, 2213, 2213, 2744, 2213, 2213, 2786, 2213,
 /*  2990 */  2213, 2213,  437, 2746,  854, 2748, 2749,  849, 2744,  872,
 /*  3000 */  2213, 2786, 2745, 2213, 2213,  433, 2746,  854, 2748, 2749,
 /*  3010 */   849, 2744,  872, 2213, 2786, 2213, 2213,  851,  438, 2746,
 /*  3020 */   854, 2748, 2749,  849, 2213,  872, 2213,  852, 2213, 2213,
 /*  3030 */  2786, 2213, 2213, 2213,  406, 2746,  854, 2748, 2749,  849,
 /*  3040 */  2213,  872, 2213, 2213, 2213, 2763, 2213, 2213, 2213, 2213,
 /*  3050 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  3060 */  2213, 2213, 2213, 2710, 2213,  850, 2213, 2213, 2213, 2213,
 /*  3070 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  3080 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  3090 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  3100 */  2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213, 2213,
 /*  3110 */  2744, 2213, 2213, 2786, 2213, 2213, 2213,  405, 2746,  854,
 /*  3120 */  2748, 2749,  849, 2213,  872,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   443,  514,  515,  446,  447,  398,  399,  427,  427,  427,
 /*    10 */   477,  415,   12,   13,   14,  426,  435,    2,  438,  439,
 /*    20 */    20,    0,   22,    8,    9,  418,   20,   12,   13,   14,
 /*    30 */    15,   16,  425,  444,  398,  399,   36,  456,   38,    0,
 /*    40 */    20,  445,   21,  384,    4,   24,   25,   26,   27,   28,
 /*    50 */    29,   30,   31,   32,  418,  398,  399,   44,  399,    8,
 /*    60 */     9,  398,  399,   12,   13,   14,   15,   16,   68,  489,
 /*    70 */   490,  489,  490,    8,    9,   75,  427,   12,   13,   14,
 /*    80 */    15,   16,   82,  527,   33,  529,  427,  438,  439,   21,
 /*    90 */    21,  495,  496,   24,   25,   26,   27,   28,   29,   30,
 /*   100 */    31,   32,  506,  384,  445,   37,  447,   39,   40,   41,
 /*   110 */    42,   20,  112,  428,  429,  115,   77,   78,   79,   80,
 /*   120 */    81,   20,   83,   84,   85,   86,   87,   88,   89,   90,
 /*   130 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   140 */   101,  102,  103,  104,  105,  106,  107,  108,    0,  398,
 /*   150 */   399,  492,  152,  153,  495,  115,  428,   20,  499,  500,
 /*   160 */   501,  502,  503,  504,  445,  506,  430,  116,  155,  418,
 /*   170 */   511,  393,  513,  447,  396,  397,  517,  518,  521,  522,
 /*   180 */   523,  116,  525,  526,  521,  522,  523,  461,  525,  526,
 /*   190 */   533,  191,  192,  467,  468,  393,  537,    0,  396,  397,
 /*   200 */    74,  201,  202,  383,  545,  385,   12,   13,  551,  552,
 /*   210 */    20,  191,  192,  556,  557,  124,  216,  112,  218,  491,
 /*   220 */    91,   24,   25,   26,   27,   28,   29,   30,   31,   32,
 /*   230 */    36,  495,  127,  128,  129,  130,  131,  132,  133,  134,
 /*   240 */   135,  136,  506,  138,  139,  140,  141,  142,  143,  144,
 /*   250 */    41,   42,  252,  253,  254,  398,  256,  257,  258,  259,
 /*   260 */   260,  261,  262,  263,  264,  265,  266,  267,  268,  269,
 /*   270 */   270,  271,  272,  273,  274,  275,  276,   12,   13,  427,
 /*   280 */   384,   73,   18,  428,   20,   20,   20,   22,  159,  437,
 /*   290 */   525,   27,  191,  147,   30,  399,  441,  151,  533,   20,
 /*   300 */   448,   36,   38,   38,  291,  292,  293,  294,  295,  296,
 /*   310 */   297,  298,  299,  184,  185,    4,  551,  552,   54,  171,
 /*   320 */    56,  556,  557,  427,  176,   61,   62,  198,  119,  120,
 /*   330 */   471,  122,  184,   68,   23,  476,   20,   73,  481,  482,
 /*   340 */    75,  445,  302,  447,   77,   78,   79,   82,  196,  398,
 /*   350 */   399,   84,   85,   86,  145,  254,   20,   90,  149,   48,
 /*   360 */    49,   50,   95,   96,   97,   98,  398,  399,  101,  418,
 /*   370 */   155,  225,  105,  106,  107,  108,  230,  112,  114,  233,
 /*   380 */   115,  235,  419,   20,  525,   20,  418,   22,  492,  125,
 /*   390 */   427,  495,  533,  425,  115,  499,  500,  501,  502,  503,
 /*   400 */   504,   36,  506,  440,   20,  509,   33,  511,  512,  513,
 /*   410 */   551,  552,  218,  517,  518,  556,  557,  152,  153,  211,
 /*   420 */    47,  157,  158,   58,  160,  161,  162,  163,  164,  165,
 /*   430 */   166,  167,  168,  169,  398,  399,  172,  173,  174,  175,
 /*   440 */   176,  177,  178,  179,  254,  181,  182,  183,  240,  241,
 /*   450 */    18,  187,  188,  189,  418,   23,  191,  192,  194,  447,
 /*   460 */   308,  309,  310,  311,    8,    9,  201,  202,   12,   13,
 /*   470 */    14,   15,   16,  461,   42,   43,   20,  427,   46,  467,
 /*   480 */   468,  216,   20,  218,   22,   36,   74,    8,    9,   57,
 /*   490 */   440,   12,   13,   14,   15,   16,  398,   20,  398,  115,
 /*   500 */   427,   69,   70,   71,   72,   73,  291,  292,  293,  294,
 /*   510 */   295,  296,  297,  298,  299,  152,  153,  252,  253,  254,
 /*   520 */    58,  256,  257,  258,  259,  260,  261,  262,  263,  264,
 /*   530 */   265,  266,  267,  268,  269,  270,  271,  272,  273,  274,
 /*   540 */   275,  276,  277,   12,   13,   22,  384,  115,   44,  449,
 /*   550 */   450,   20,  452,   22,   75,  455,  458,   38,  460,   36,
 /*   560 */   381,  399,  394,  490,  201,  202,  398,   36,  400,   38,
 /*   570 */     8,    9,   68,   54,   12,   13,   14,   15,   16,  446,
 /*   580 */   447,  302,   63,   64,   65,   66,  154,   68,   68,  427,
 /*   590 */   447,  427,  145,  146,  147,  148,  149,  150,  151,   68,
 /*   600 */   112,  437,  123,  125,  427,   82,   75,  445,   20,  447,
 /*   610 */   467,  468,  448,   82,  437,  127,  128,  129,  130,  131,
 /*   620 */   132,  133,  134,  135,  136,  448,  138,  139,  140,  141,
 /*   630 */   142,  143,  144,  114,  114,  112,  117,  117,    4,  408,
 /*   640 */   208,  209,  210,  112,  277,  213,  115,  416,  414,  193,
 /*   650 */   471,  417,  430,   19,  492,  476,  434,  495,  226,  227,
 /*   660 */    82,  499,  500,  501,  502,  503,  504,  218,  506,  190,
 /*   670 */   193,  239,   38,   52,  242,  125,   14,  245,  246,  247,
 /*   680 */   248,  249,   20,  152,  153,  125,    8,    9,   54,  525,
 /*   690 */    12,   13,   14,   15,   16,   61,   62,  533,  155,  250,
 /*   700 */   251,  498,   68,  115,  525,  186,  186,   14,  546,  547,
 /*   710 */   254,  384,  533,   20,  195,  551,  552,  495,  199,  200,
 /*   720 */   556,  557,  191,  192,  205,  206,  399,  524,  506,  405,
 /*   730 */   551,  552,  201,  202,  302,  556,  557,   20,  394,  216,
 /*   740 */   180,  218,  398,  224,  400,   20,  422,  216,  114,  218,
 /*   750 */     0,  117,  398,  399,  427,  431,  432,  398,  399,  280,
 /*   760 */   281,  282,  283,  284,  285,  286,  287,  288,  289,  290,
 /*   770 */   208,  277,  445,  279,  447,  252,  253,  295,  296,  297,
 /*   780 */   298,  299,   20,  252,  253,  254,  228,  256,  257,  258,
 /*   790 */   259,  260,  261,  262,  263,  264,  265,  266,  267,  268,
 /*   800 */   269,  270,  271,  272,  273,  274,  275,  276,   12,   13,
 /*   810 */    14,   36,  453,  115,  193,   14,   20,  430,   22,  492,
 /*   820 */   398,   20,  495,  427,  384,  204,  499,  500,  501,  502,
 /*   830 */   503,  504,   36,  506,   38,  292,  293,  294,  511,  399,
 /*   840 */   513,  401,    8,    9,  517,  518,   12,   13,   14,   15,
 /*   850 */    16,  384,  456,  191,    0,    8,    9,  525,  252,   12,
 /*   860 */    13,   14,   15,   16,   68,  533,  399,  427,  401,  152,
 /*   870 */   153,  193,  545,  384,  520,  521,  522,  523,   82,  525,
 /*   880 */   526,  510,  495,  512,  552,  445,   34,  447,  556,  557,
 /*   890 */   302,  329,    3,  506,  427,  145,  146,  147,  148,  149,
 /*   900 */   150,  151,  384,  481,  482,  399,  398,  399,  112,   20,
 /*   910 */   457,  115,  445,  215,  447,  217,  254,  399,  193,  401,
 /*   920 */   314,  315,  316,  317,  318,  319,  320,   12,   13,   14,
 /*   930 */    15,   16,  492,  427,  445,  495,  378,  379,  380,  499,
 /*   940 */   500,  501,  502,  503,  504,  427,  506,  249,  152,  153,
 /*   950 */   116,  511,  427,  513,  398,  399,   23,  517,  518,  492,
 /*   960 */   435,  453,  495,  445,    0,  447,  499,  500,  501,  502,
 /*   970 */   503,  504,   20,  506,  418,  398,  399,   54,  511,  254,
 /*   980 */   513,  456,   49,   50,  517,  518,   63,  191,  192,   66,
 /*   990 */    67,  398,  399,  218,  428,  418,  190,  201,  202,   36,
 /*  1000 */   302,   22,   14,  398,  399,  116,  115,  501,   20,  155,
 /*  1010 */   492,  418,  216,  495,  218,   36,  254,  499,  500,  501,
 /*  1020 */   502,  503,  504,  418,  506,  250,  251,    2,  398,  511,
 /*  1030 */    22,  513,  428,    8,    9,  517,  518,   12,   13,   14,
 /*  1040 */    15,   16,  146,  147,   36,   82,  471,  151,  252,  253,
 /*  1050 */   254,  407,  256,  257,  258,  259,  260,  261,  262,  263,
 /*  1060 */   264,  265,  266,  267,  268,  269,  270,  271,  272,  273,
 /*  1070 */   274,  275,  276,   12,   13,  398,  399,  419,    0,  471,
 /*  1080 */   436,   20,  193,   22,  427,  427,  280,  398,  399,  115,
 /*  1090 */    82,  112,  146,  463,  464,  418,  290,   36,  440,   38,
 /*  1100 */   525,  398,  399,  398,  399,  448,  415,  384,  533,  145,
 /*  1110 */   146,  147,  148,  149,  150,  151,  428,  525,  471,   59,
 /*  1120 */    60,  418,  399,  418,  401,  533,  551,  552,  510,   68,
 /*  1130 */   512,  556,  557,  525,  407,  428,  445,  398,  399,  384,
 /*  1140 */   384,  533,  453,   82,  552,  193,  398,  399,  556,  557,
 /*  1150 */   427,  424,    1,    2,  399,   22,  401,  418,  471,  551,
 /*  1160 */   552,  398,  399,  436,  556,  557,  418,    3,  445,   36,
 /*  1170 */   447,  427,  525,  112,  384,  428,  115,  231,  232,  191,
 /*  1180 */   533,  418,  427,   14,   15,   16,  495,  496,  497,  399,
 /*  1190 */   498,  401,  448,  302,  428,   12,   13,  506,  551,  552,
 /*  1200 */   445,  445,  447,  556,  557,   22,  254,  416,  398,  399,
 /*  1210 */   398,  399,  525,  152,  153,  492,  524,  427,  495,   36,
 /*  1220 */   533,   38,  499,  500,  501,  502,  503,  504,  418,  506,
 /*  1230 */   418,  398,  399,  155,  511,  445,  513,  447,  551,  552,
 /*  1240 */   517,  518,  254,  556,  557,  112,    4,  492,  170,  384,
 /*  1250 */   495,   68,  191,  192,  499,  500,  501,  502,  503,  504,
 /*  1260 */   145,  506,  201,  202,  149,   82,  511,  116,  513,  403,
 /*  1270 */   404,  498,  517,  518,  403,  404,  302,  216,  427,  218,
 /*  1280 */   412,  413,  492,   33,  384,  495,  398,  399,  437,  499,
 /*  1290 */   500,  501,  502,  503,  504,  112,  506,  524,  471,  448,
 /*  1300 */     0,  511,   20,  513,  412,  413,  418,  517,  518,  384,
 /*  1310 */   445,  384,  384,  252,  253,  254,  384,  256,  257,  258,
 /*  1320 */   259,  260,  261,  262,  263,  264,  265,  266,  267,  268,
 /*  1330 */   269,  270,  271,  272,  273,  274,  275,  276,   12,   13,
 /*  1340 */    13,  398,  399,  398,  399,  445,   20,  423,   22,  398,
 /*  1350 */   399,   33,  525,  384,  521,  522,  523,  384,  525,  526,
 /*  1360 */   533,  418,   36,  418,   38,  398,  399,  384,  399,  418,
 /*  1370 */   445,  384,  445,  445,   74,   13,  384,  445,  551,  552,
 /*  1380 */   405,  384,  384,  556,  557,  418,  384,  384,  384,  384,
 /*  1390 */   384,    0,  509,  427,   68,  512,  427,  399,  427,  216,
 /*  1400 */   442,  218,   33,  445,  442,  439,  431,  445,   82,   82,
 /*  1410 */   486,    0,  442,   33,  445,  445,  447,   13,  445,  448,
 /*  1420 */   420,  387,  388,  423,    0,  427,  186,   47,  445,    0,
 /*  1430 */    36,  385,  445,    0,  116,  252,  253,  445,  112,  527,
 /*  1440 */    36,  115,  445,  445,   82,  447,   22,  445,  445,  445,
 /*  1450 */   445,  445,  269,  270,  271,  272,  273,  274,  275,    0,
 /*  1460 */   220,  492,   33,  118,  495,   54,  121,  560,  499,  500,
 /*  1470 */   501,  502,  503,  504,    0,  506,   82,   44,  152,  153,
 /*  1480 */   511,   22,  513,    0,   12,   13,  517,  518,  324,  118,
 /*  1490 */   492,   13,  121,  495,   22,    1,   22,  499,  500,  501,
 /*  1500 */   502,  503,  504,  118,  506,   22,  121,  384,   36,  511,
 /*  1510 */    38,  513,   68,   19,   36,  517,  518,  191,  192,  234,
 /*  1520 */    33,  236,  399,  118,   33,   75,  121,  201,  202,   33,
 /*  1530 */    33,   33,   38,   33,  152,  153,  254,   33,   33,  549,
 /*  1540 */    68,   51,  216,   36,  218,   33,  304,   53,   54,   33,
 /*  1550 */   427,   36,    1,    2,   33,   33,   33,   63,   64,   65,
 /*  1560 */    66,  117,   68,   33,   33,   33,   12,   13,  445,  542,
 /*  1570 */   447,   12,   13,   12,   13,   33,  326,  528,  252,  253,
 /*  1580 */   254,  402,  256,  257,  258,  259,  260,  261,  262,  263,
 /*  1590 */   264,  265,  266,  267,  268,  269,  270,  271,  272,  273,
 /*  1600 */   274,  275,  276,  116,   33,  115,  415,  116,  114,   12,
 /*  1610 */    13,  117,  116,  116,  116,  492,  116,   33,  495,  427,
 /*  1620 */   116,  116,  499,  500,  501,  502,  503,  504,  116,  506,
 /*  1630 */    36,   33,  116,   33,  511,  244,  513,  116,  116,  116,
 /*  1640 */   517,  518,  384,   33,  150,   13,  116,  116,  116,   12,
 /*  1650 */    13,   12,   13,   12,   13,   12,   13,  399,  116,   12,
 /*  1660 */    13,   12,   13,   12,   13,  115,   36,  415,   36,  397,
 /*  1670 */    13,  457,  548,  548,  124,    0,   82,  384,  548,  470,
 /*  1680 */   548,  457,  402,  493,  478,  427,  399,  116,  216,  195,
 /*  1690 */   218,  197,  399,   36,  200,  444,  457,  328,  457,  205,
 /*  1700 */   116,  553,  252,  445,  532,  447,  384,  532,  519,  535,
 /*  1710 */   457,  417,   82,  305,  116,  472,  116,   54,  224,  494,
 /*  1720 */   427,  399,  250,  251,  252,  218,  116,   20,  398,   20,
 /*  1730 */    55,  483,  233,  218,  488,  407,  407,  483,  445,  214,
 /*  1740 */   447,  269,  270,  271,  272,  273,  274,  275,  474,  427,
 /*  1750 */   492,  398,   20,  495,  399,   47,  454,  499,  500,  501,
 /*  1760 */   502,  503,  504,  399,  506,  454,  190,  445,  451,  447,
 /*  1770 */   398,  513,  399,  398,  454,  517,  518,    0,  113,  411,
 /*  1780 */   111,  398,  451,  451,  451,  492,  384,  410,  495,  110,
 /*  1790 */   398,  398,  499,  500,  501,  502,  503,  504,  409,  506,
 /*  1800 */    20,  399,   52,  391,  398,  398,  513,  391,  395,  395,
 /*  1810 */   517,  518,   20,  447,  492,   20,  407,  495,  483,  400,
 /*  1820 */    20,  499,  500,  501,  502,  503,  504,  407,  506,  427,
 /*  1830 */   407,  473,  407,  407,  400,  513,   20,  384,  464,  517,
 /*  1840 */   518,   20,  407,  407,  458,  407,  398,  445,  407,  447,
 /*  1850 */   387,  391,  399,  384,   77,   78,   79,  427,  427,  427,
 /*  1860 */   398,   84,   85,   86,  427,  237,  427,   90,  399,  427,
 /*  1870 */   427,  387,   95,   96,   97,   98,  427,  391,  101,  427,
 /*  1880 */   427,  427,  105,  106,  107,  108,  487,  115,  427,   20,
 /*  1890 */   445,  445,  445,  405,  492,  485,  427,  495,  445,  482,
 /*  1900 */   447,  499,  500,  501,  502,  503,  504,  505,  506,  507,
 /*  1910 */   508,  483,  222,  221,  445,  447,  447,  384,  405,  313,
 /*  1920 */   480,  445,  398,  312,  321,  541,  207,  472,  541,  465,
 /*  1930 */   479,  465,  399,  306,  544,  323,  322,  541,  543,  538,
 /*  1940 */   540,  301,  539,  472,  300,  492,  531,  330,  495,  561,
 /*  1950 */   325,  530,  499,  500,  501,  502,  503,  504,  327,  506,
 /*  1960 */   427,  492,  399,   20,  495,  498,  125,  303,  499,  500,
 /*  1970 */   501,  502,  503,  504,  400,  506,  554,  384,  445,  465,
 /*  1980 */   447,  445,  513,  405,  405,  445,  445,  518,  445,  445,
 /*  1990 */   534,  465,  399,  536,  405,  199,  462,  445,  115,  115,
 /*  2000 */   405,  516,  445,  458,  199,  459,  458,  384,  423,  405,
 /*  2010 */   405,  558,  559,  555,  399,  445,  433,  445,  445,   22,
 /*  2020 */   427,  386,  399,   35,   37,  492,  384,  445,  495,  405,
 /*  2030 */    40,  445,  499,  500,  501,  502,  503,  504,  445,  506,
 /*  2040 */   447,  399,  389,  398,  445,  390,  445,  445,  392,  391,
 /*  2050 */   427,  491,  445,  445,  445,  475,  466,  466,  445,  484,
 /*  2060 */   406,  445,  469,  445,  445,  445,  445,  445,  445,  427,
 /*  2070 */   447,  445,    0,  421,  445,  429,  382,  429,  445,  445,
 /*  2080 */   547,  445,  421,  421,    0,  492,    0,  445,  495,  447,
 /*  2090 */   384,   47,  499,  500,  501,  502,  503,  504,    0,  506,
 /*  2100 */    36,  243,   36,   36,   36,  399,  384,  243,    0,   36,
 /*  2110 */    36,  469,   36,  243,    0,  492,    0,    0,  495,  243,
 /*  2120 */     0,  399,  499,  500,  501,  502,  503,  504,   36,  506,
 /*  2130 */    36,    0,   22,  427,  492,   36,  238,  495,    0,    0,
 /*  2140 */   224,  499,  500,  501,  502,  503,  504,    0,  506,  427,
 /*  2150 */   224,  445,  225,  447,  218,  216,    0,    0,    0,  212,
 /*  2160 */   211,    0,    0,  158,   51,    0,   51,  445,   36,  447,
 /*  2170 */   384,    0,    0,  550,    0,   54,   36,   51,    0,    0,
 /*  2180 */    47,    0,    0,    0,    0,  399,    0,   51,    0,    0,
 /*  2190 */     0,  176,    0,  176,    0,   36,  384,    0,  492,    0,
 /*  2200 */     0,  495,    0,    0,    0,  499,  500,  501,  502,  503,
 /*  2210 */   504,  399,  506,  427,  492,    0,    0,  495,    0,    0,
 /*  2220 */     0,  499,  500,  501,  502,  503,  504,    0,  506,    0,
 /*  2230 */   508,  445,  384,  447,    0,    0,    0,    0,    0,  427,
 /*  2240 */     0,    0,   51,   47,    0,    0,    0,  399,   22,    0,
 /*  2250 */     0,    0,  156,  158,  157,  469,    0,  445,  384,  447,
 /*  2260 */     0,    0,    0,    0,    0,  559,    0,    0,    0,   22,
 /*  2270 */     0,    0,    0,  399,   52,  427,   22,   52,  492,   68,
 /*  2280 */    68,  495,   68,  384,    0,  499,  500,  501,  502,  503,
 /*  2290 */   504,   36,  506,  445,    0,  447,   68,   54,  399,   14,
 /*  2300 */    47,  427,   44,    0,  492,   36,   54,  495,    0,   36,
 /*  2310 */     0,  499,  500,  501,  502,  503,  504,  469,  506,  445,
 /*  2320 */    44,  447,   36,   44,   36,    0,  427,   44,   33,   54,
 /*  2330 */     0,   51,   51,   51,   45,   44,    0,    0,    0,    0,
 /*  2340 */   492,   44,    0,  495,  445,  384,  447,  499,  500,  501,
 /*  2350 */   502,  503,  504,  207,  506,   51,    0,   51,    0,    0,
 /*  2360 */   399,   54,   36,   76,    0,    0,  492,  384,   44,  495,
 /*  2370 */     0,   36,   54,  499,  500,  501,  502,  503,  504,    0,
 /*  2380 */   506,   36,  399,   54,   44,   44,    0,   36,  427,    0,
 /*  2390 */    44,  492,    0,    0,  495,    0,  384,   54,  499,  500,
 /*  2400 */   501,  502,  503,  504,    0,  506,  445,    0,  447,   36,
 /*  2410 */   427,  399,   22,  123,  121,    0,   22,   36,   36,    0,
 /*  2420 */    36,   36,    0,   36,   36,   22,    0,   36,  445,    0,
 /*  2430 */   447,   33,   36,   36,   33,   36,   36,    0,   36,  427,
 /*  2440 */    22,   36,   22,   22,    0,   22,    0,   36,    0,   36,
 /*  2450 */     0,    0,   36,  492,   22,   20,  495,  445,    0,  447,
 /*  2460 */   499,  500,  501,  502,  503,  504,   56,  506,  116,   36,
 /*  2470 */    36,  193,   36,    0,   36,  492,   51,  384,  495,   22,
 /*  2480 */   223,  115,  499,  500,  501,  502,  503,  504,  229,  506,
 /*  2490 */   384,  115,  399,  228,  193,    0,  219,   22,  193,  199,
 /*  2500 */     0,    0,  193,   33,  492,  399,  384,  495,  193,   33,
 /*  2510 */     3,  499,  500,  501,  502,  503,  504,  203,  506,  384,
 /*  2520 */   427,  399,  203,  115,  307,  116,   36,  115,   36,   52,
 /*  2530 */   116,   52,  115,  427,  399,   33,   33,  113,  445,  111,
 /*  2540 */   447,   33,   51,  116,   82,   51,  116,  116,   33,  427,
 /*  2550 */    36,  445,    3,  447,  115,  115,  115,  115,   33,  116,
 /*  2560 */   115,   51,  427,  116,   36,  116,   36,  445,   36,  447,
 /*  2570 */    36,  116,   36,   36,   33,  116,    0,   51,    0,  115,
 /*  2580 */   445,  116,  447,  291,   44,  492,    0,  116,  495,  115,
 /*  2590 */    44,    0,  499,  500,  501,  502,  503,  504,  492,  506,
 /*  2600 */   115,  495,  196,  307,  115,  499,  500,  501,  502,  503,
 /*  2610 */   504,  115,  506,  116,  492,   44,  384,  495,  307,    2,
 /*  2620 */    33,  499,  500,  501,  502,  503,  504,  492,  506,  113,
 /*  2630 */   495,  399,  384,  278,  499,  500,  501,  502,  503,  504,
 /*  2640 */   113,  506,   22,   51,   51,  116,  115,  399,  115,  196,
 /*  2650 */    22,  195,  200,    0,  116,  116,   44,  115,  384,  427,
 /*  2660 */   115,   22,  115,  115,  196,  115,   51,  116,  115,  115,
 /*  2670 */   126,  116,  115,  399,  115,  427,  124,  445,  115,  447,
 /*  2680 */   115,  115,  115,  384,  116,  115,  125,   22,   22,   22,
 /*  2690 */   229,  116,   36,  445,   36,  447,  384,  115,  399,  116,
 /*  2700 */    36,  427,   33,  115,   36,  116,  252,   36,   36,   36,
 /*  2710 */   116,  399,  116,  116,  137,  255,  137,  137,  137,  445,
 /*  2720 */   115,  447,  115,   36,  492,  115,  427,  495,   22,   22,
 /*  2730 */    76,  499,  500,  501,  502,  503,  504,   75,  506,  427,
 /*  2740 */   492,   36,   82,  495,  445,  384,  447,  499,  500,  501,
 /*  2750 */   502,  503,  504,   36,  506,   36,   36,  445,   36,  447,
 /*  2760 */   399,   36,   36,   36,   36,   36,  492,  384,   36,  495,
 /*  2770 */   109,   82,  109,  499,  500,  501,  502,  503,  504,   33,
 /*  2780 */   506,   36,  399,   36,   36,   22,   36,   36,  427,   36,
 /*  2790 */    82,  492,   36,   36,  495,   36,   36,   36,  499,  500,
 /*  2800 */   501,  502,  503,  504,  492,  506,  445,  495,  447,   22,
 /*  2810 */   427,  499,  500,  501,  502,  503,  504,   36,  506,    0,
 /*  2820 */    36,   54,   44,    0,   36,   54,   44,    0,  445,   36,
 /*  2830 */   447,   54,    0,   44,   36,   44,    0,   54,   36,    0,
 /*  2840 */    22,   36,    0,   22,   33,   36,   36,   22,   22,   21,
 /*  2850 */    20,   22,   21,  492,  562,  562,  495,  562,  562,  562,
 /*  2860 */   499,  500,  501,  502,  503,  504,  562,  506,  562,  562,
 /*  2870 */   562,  562,  562,  562,  562,  492,  562,  384,  495,  562,
 /*  2880 */   562,  562,  499,  500,  501,  502,  503,  504,  562,  506,
 /*  2890 */   384,  562,  399,  562,  562,  562,  562,  562,  562,  562,
 /*  2900 */   562,  562,  562,  384,  562,  399,  562,  562,  562,  562,
 /*  2910 */   562,  562,  562,  562,  562,  562,  562,  562,  399,  384,
 /*  2920 */   427,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  2930 */   562,  562,  562,  427,  399,  562,  562,  562,  445,  562,
 /*  2940 */   447,  562,  562,  562,  562,  562,  427,  562,  562,  562,
 /*  2950 */   562,  445,  562,  447,  562,  562,  562,  562,  562,  562,
 /*  2960 */   562,  562,  427,  562,  445,  562,  447,  562,  562,  562,
 /*  2970 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  2980 */   445,  562,  447,  562,  562,  492,  562,  562,  495,  562,
 /*  2990 */   562,  562,  499,  500,  501,  502,  503,  504,  492,  506,
 /*  3000 */   562,  495,  384,  562,  562,  499,  500,  501,  502,  503,
 /*  3010 */   504,  492,  506,  562,  495,  562,  562,  399,  499,  500,
 /*  3020 */   501,  502,  503,  504,  562,  506,  562,  492,  562,  562,
 /*  3030 */   495,  562,  562,  562,  499,  500,  501,  502,  503,  504,
 /*  3040 */   562,  506,  562,  562,  562,  427,  562,  562,  562,  562,
 /*  3050 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3060 */   562,  562,  562,  445,  562,  447,  562,  562,  562,  562,
 /*  3070 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3080 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3090 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3100 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3110 */   492,  562,  562,  495,  562,  562,  562,  499,  500,  501,
 /*  3120 */   502,  503,  504,  562,  506,  562,  562,  562,  562,  562,
 /*  3130 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3140 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3150 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3160 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3170 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3180 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3190 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3200 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3210 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3220 */   562,  562,  562,  562,  562,  562,  562,  562,  562,  562,
 /*  3230 */   562,  562,  562,  562,
};
#define YY_SHIFT_COUNT    (977)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (2842)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   432,    0,  265,    0,  531,  531,  531,  531,  531,  531,
 /*    10 */   531,  531,  531,  531,  531,  531,  796, 1061, 1061, 1326,
 /*    20 */  1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061,
 /*    30 */  1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061,
 /*    40 */  1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061, 1061,
 /*    50 */   279,  588,  698,  384,  891,  974,  891,  891,  384,  384,
 /*    60 */   891, 1183,  891,  264, 1183,   40,  891,    6, 1472,  363,
 /*    70 */   363,  137,  137,  126, 1472, 1472,  311,  311,  363,   20,
 /*    80 */    20,  717,  693,  693,  190,   91,  137,  137,  137,  137,
 /*    90 */   137,  137,  137,  137,  137,  137,  137,  266,  316,  336,
 /*   100 */   137,  137,  412,    6,  137,  266,  137,    6,  137,  137,
 /*   110 */   137,  137,    6,  137,  137,  137,    6,  137,    6,    6,
 /*   120 */     6,  479,  479,  105,  105,  488,  267,   13,   69,  523,
 /*   130 */   523,  523,  523,  523,  523,  523,  523,  523,  523,  523,
 /*   140 */   523,  523,  523,  523,  523,  523,  523,  523,  209,  889,
 /*   150 */    20,  717, 1060, 1060,  963,  477,  477,  477,  494,  494,
 /*   160 */  1300, 1327,  963,  412,    6,  478,    6,    6,  367,    6,
 /*   170 */     6,  578,    6,  578,  578,  550,  852,  105,  105,  105,
 /*   180 */   105,  105,  105, 1494, 1777,   21,  456,  215,  215,  562,
 /*   190 */   606,  152,  482,  365,  543,  662,  988,  194,  194,  933,
 /*   200 */   725, 1008, 1008, 1008,  621, 1008,  101,  952,  462,  504,
 /*   210 */   801, 1115, 1240,  762,  762, 1282, 1362, 1362, 1164,  373,
 /*   220 */  1242,  762, 1327, 1408, 1663, 1707, 1709, 1499,  412, 1709,
 /*   230 */   412, 1525, 1707, 1732, 1708, 1732, 1708, 1576, 1707, 1732,
 /*   240 */  1707, 1708, 1576, 1576, 1576, 1665, 1669, 1707, 1707, 1679,
 /*   250 */  1707, 1707, 1707, 1780, 1750, 1780, 1750, 1709,  412,  412,
 /*   260 */  1792,  412, 1795, 1800,  412, 1795,  412, 1816,  412, 1821,
 /*   270 */   412,  412, 1707,  412, 1780,    6,    6,    6,    6,    6,
 /*   280 */     6,    6,    6,    6,    6,    6, 1707,  852,  852, 1780,
 /*   290 */   578,  578,  578, 1628, 1772, 1709,  126, 1869, 1690, 1692,
 /*   300 */  1792,  126, 1408, 1707,  578, 1606, 1611, 1606, 1611, 1603,
 /*   310 */  1719, 1606, 1612, 1614, 1627, 1408, 1640, 1644, 1617, 1631,
 /*   320 */  1625, 1732, 1943, 1841, 1664, 1795,  126,  126, 1611,  578,
 /*   330 */   578,  578,  578, 1611,  578, 1796,  126,  578, 1821,  126,
 /*   340 */  1883,  578, 1805, 1821,  126,  550,  126, 1732,  578,  578,
 /*   350 */   578,  578,  578,  578,  578,  578,  578,  578,  578,  578,
 /*   360 */   578,  578,  578,  578,  578,  578,  578,  578,  578,  578,
 /*   370 */  1884,  578, 1707,  126, 1997, 1988, 1987, 1990, 1780, 3125,
 /*   380 */  3125, 3125, 3125, 3125, 3125, 3125, 3125, 3125, 3125, 3125,
 /*   390 */  3125,   39,  519,  197,  634,   51,   65,  834,   15, 1025,
 /*   400 */   678,  750,  964,  847,  847,  847,  847,  847,  847,  847,
 /*   410 */   847,  847,  447,   68,  146,  129,  915,  915,  208,  520,
 /*   420 */   148,  923,  558,  558,  449,  775,  558, 1078,  979, 1133,
 /*   430 */   946,  896,  896, 1169, 1151,  806, 1169, 1169, 1169, 1411,
 /*   440 */  1391, 1318, 1433, 1380,  560,  854, 1429, 1345, 1371, 1385,
 /*   450 */  1405, 1394, 1404, 1478, 1424, 1459, 1474, 1483, 1285, 1487,
 /*   460 */  1491, 1444, 1496, 1497, 1498, 1500, 1382, 1250, 1369, 1504,
 /*   470 */  1505, 1512, 1551, 1516, 1450, 1521, 1490, 1522, 1523, 1530,
 /*   480 */  1531, 1532, 1554, 1559, 1561, 1597, 1637, 1639, 1641, 1643,
 /*   490 */  1647, 1649, 1651, 1542, 1571, 1584, 1598, 1600, 1610, 1550,
 /*   500 */  1594, 1507, 1515, 1632, 1657, 1630, 1675, 2072, 2084, 2086,
 /*   510 */  2044, 2098, 2064, 1858, 2066, 2067, 2068, 1864, 2108, 2073,
 /*   520 */  2074, 1870, 2076, 2114, 2116, 1876, 2117, 2092, 2120, 2094,
 /*   530 */  2131, 2110, 2138, 2099, 1898, 2139, 1916, 2147, 1926, 1927,
 /*   540 */  1936, 1939, 2156, 2157, 2158, 1947, 1949, 2161, 2162, 2005,
 /*   550 */  2113, 2115, 2165, 2132, 2171, 2172, 2140, 2121, 2174, 2126,
 /*   560 */  2178, 2133, 2179, 2181, 2182, 2136, 2183, 2184, 2186, 2188,
 /*   570 */  2189, 2190, 2015, 2159, 2192, 2017, 2194, 2197, 2199, 2200,
 /*   580 */  2202, 2203, 2204, 2215, 2216, 2218, 2219, 2220, 2227, 2229,
 /*   590 */  2234, 2235, 2236, 2237, 2238, 2240, 2191, 2241, 2196, 2244,
 /*   600 */  2245, 2246, 2256, 2260, 2261, 2262, 2263, 2264, 2226, 2249,
 /*   610 */  2095, 2250, 2097, 2251, 2096, 2266, 2267, 2247, 2222, 2254,
 /*   620 */  2225, 2268, 2211, 2270, 2212, 2255, 2271, 2214, 2272, 2228,
 /*   630 */  2284, 2294, 2269, 2243, 2258, 2303, 2273, 2252, 2276, 2308,
 /*   640 */  2286, 2275, 2279, 2310, 2288, 2325, 2253, 2283, 2295, 2280,
 /*   650 */  2281, 2285, 2282, 2330, 2289, 2291, 2336, 2337, 2338, 2339,
 /*   660 */  2297, 2146, 2342, 2280, 2304, 2356, 2280, 2306, 2358, 2359,
 /*   670 */  2287, 2364, 2365, 2326, 2307, 2324, 2370, 2335, 2318, 2340,
 /*   680 */  2379, 2345, 2329, 2341, 2386, 2351, 2343, 2346, 2389, 2392,
 /*   690 */  2393, 2395, 2404, 2407, 2290, 2293, 2373, 2390, 2415, 2394,
 /*   700 */  2381, 2382, 2384, 2385, 2387, 2388, 2391, 2396, 2397, 2398,
 /*   710 */  2401, 2399, 2400, 2403, 2402, 2419, 2418, 2422, 2420, 2426,
 /*   720 */  2421, 2410, 2429, 2423, 2405, 2437, 2444, 2446, 2411, 2448,
 /*   730 */  2413, 2450, 2416, 2451, 2432, 2435, 2433, 2434, 2436, 2352,
 /*   740 */  2366, 2458, 2278, 2259, 2265, 2376, 2257, 2280, 2425, 2473,
 /*   750 */  2301, 2438, 2457, 2495, 2277, 2475, 2305, 2300, 2500, 2501,
 /*   760 */  2309, 2314, 2315, 2319, 2507, 2470, 2217, 2408, 2409, 2412,
 /*   770 */  2414, 2490, 2492, 2417, 2477, 2424, 2479, 2428, 2427, 2476,
 /*   780 */  2502, 2430, 2439, 2440, 2441, 2431, 2503, 2491, 2494, 2442,
 /*   790 */  2508, 2296, 2462, 2443, 2515, 2445, 2514, 2447, 2449, 2549,
 /*   800 */  2525, 2311, 2528, 2530, 2532, 2534, 2536, 2537, 2455, 2459,
 /*   810 */  2510, 2292, 2541, 2526, 2576, 2578, 2464, 2540, 2465, 2471,
 /*   820 */  2474, 2485, 2406, 2489, 2586, 2546, 2452, 2591, 2497, 2496,
 /*   830 */  2453, 2571, 2456, 2587, 2516, 2355, 2527, 2617, 2620, 2454,
 /*   840 */  2529, 2538, 2531, 2533, 2542, 2545, 2547, 2539, 2592, 2548,
 /*   850 */  2550, 2593, 2551, 2628, 2460, 2553, 2554, 2653, 2555, 2557,
 /*   860 */  2468, 2612, 2559, 2552, 2639, 2544, 2563, 2280, 2615, 2565,
 /*   870 */  2566, 2568, 2567, 2570, 2561, 2665, 2666, 2667, 2461, 2575,
 /*   880 */  2656, 2658, 2582, 2583, 2664, 2588, 2589, 2668, 2531, 2594,
 /*   890 */  2671, 2533, 2596, 2672, 2542, 2597, 2673, 2545, 2577, 2579,
 /*   900 */  2580, 2581, 2605, 2669, 2607, 2687, 2610, 2669, 2669, 2706,
 /*   910 */  2654, 2662, 2707, 2705, 2717, 2719, 2720, 2722, 2725, 2726,
 /*   920 */  2727, 2728, 2729, 2732, 2660, 2661, 2689, 2663, 2746, 2745,
 /*   930 */  2747, 2748, 2763, 2750, 2751, 2753, 2708, 2398, 2756, 2401,
 /*   940 */  2757, 2759, 2760, 2761, 2787, 2781, 2819, 2784, 2767, 2778,
 /*   950 */  2823, 2788, 2771, 2782, 2827, 2793, 2777, 2789, 2832, 2798,
 /*   960 */  2783, 2791, 2836, 2802, 2839, 2818, 2805, 2842, 2821, 2811,
 /*   970 */  2809, 2810, 2825, 2828, 2826, 2829, 2831, 2830,
};
#define YY_REDUCE_COUNT (390)
#define YY_REDUCE_MIN   (-513)
#define YY_REDUCE_MAX   (2618)
static const short yy_reduce_ofst[] = {
 /*     0 */   179, -341, -104,  327,  440,  467,  518,  723,  755,  790,
 /*    10 */   969,  998, 1123, 1258, 1293, 1322, 1402,  162, 1453, 1469,
 /*    20 */  1533, 1593, 1642, 1623, 1706, 1722, 1786, 1848, 1812, 1874,
 /*    30 */  1899, 1961, 1983, 2012, 2093, 2106, 2122, 2135, 2232, 2248,
 /*    40 */  2274, 2299, 2312, 2361, 2383, 2493, 2506, 2519, 2535, 2618,
 /*    50 */  -343,  164, -141,  354,  575,  608,  647,  687, -337,  833,
 /*    60 */   827,  691, -235,  100, -404,  332,  592, -420,  222, -274,
 /*    70 */    12, -393,  -32,  324, -264,  387, -222, -198,  143, -419,
 /*    80 */   525, -443,  168,  344,  506, -364, -249,  -49,   36,  556,
 /*    90 */   359,  508,  577,  593,  677,  703,  689, -143,  630,   98,
 /*   100 */   705,  739,  727, -418,  748,  422,  763, -148,  810,  812,
 /*   110 */   888,  943,  -37,  945,  951,  967,  177,  605, -351,  851,
 /*   120 */   658, -513, -513, -315, -272, -145,  231, -444, -180, -281,
 /*   130 */   489,  756,  865,  900,  925,  927,  928,  932,  973,  983,
 /*   140 */   987,  992,  997, 1002, 1003, 1004, 1005, 1006, -411,  203,
 /*   150 */   396,  133,  866,  871,  868,  203,  692,  773,  371,  618,
 /*   160 */   975,  234,  892,  644,   73,  924,  657,  744,  883,   50,
 /*   170 */   966,  958,  971,  962,  970, 1000, 1034,  566,  604,  688,
 /*   180 */   707,  747,  766, -467,  791, 1046,  453,  912,  912,  907,
 /*   190 */   990, 1027, 1049, 1179,  912, 1192, 1192, 1191, 1252, 1272,
 /*   200 */  1214, 1124, 1125, 1130, 1209, 1132, 1192, 1224, 1280, 1190,
 /*   210 */  1287, 1251, 1206, 1239, 1241, 1192, 1172, 1175, 1148, 1189,
 /*   220 */  1174, 1253, 1294, 1243, 1225, 1330, 1248, 1246, 1328, 1254,
 /*   230 */  1329, 1274, 1353, 1355, 1302, 1364, 1311, 1317, 1372, 1373,
 /*   240 */  1375, 1320, 1331, 1332, 1333, 1368, 1377, 1383, 1392, 1389,
 /*   250 */  1393, 1406, 1407, 1412, 1413, 1416, 1414, 1335, 1409, 1420,
 /*   260 */  1366, 1423, 1419, 1358, 1425, 1434, 1426, 1374, 1435, 1386,
 /*   270 */  1436, 1438, 1448, 1441, 1460, 1430, 1431, 1432, 1437, 1439,
 /*   280 */  1442, 1443, 1449, 1452, 1454, 1461, 1462, 1463, 1484, 1486,
 /*   290 */  1445, 1446, 1447, 1399, 1410, 1428, 1488, 1417, 1440, 1451,
 /*   300 */  1468, 1513, 1455, 1524, 1476, 1384, 1464, 1387, 1466, 1390,
 /*   310 */  1395, 1396, 1400, 1403, 1401, 1471, 1415, 1421, 1388, 1458,
 /*   320 */  1422, 1563, 1467, 1457, 1456, 1574, 1578, 1579, 1514, 1536,
 /*   330 */  1540, 1541, 1543, 1526, 1544, 1534, 1589, 1552, 1545, 1595,
 /*   340 */  1485, 1557, 1546, 1548, 1604, 1585, 1605, 1615, 1570, 1572,
 /*   350 */  1573, 1582, 1586, 1599, 1601, 1602, 1607, 1608, 1609, 1613,
 /*   360 */  1616, 1618, 1619, 1620, 1621, 1622, 1626, 1629, 1633, 1634,
 /*   370 */  1583, 1636, 1645, 1624, 1635, 1653, 1655, 1656, 1658, 1580,
 /*   380 */  1646, 1560, 1575, 1590, 1591, 1652, 1661, 1648, 1662, 1654,
 /*   390 */  1694,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    10 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    20 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    30 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    40 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    50 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    60 */  2587, 2211, 2211, 2543, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    70 */  2211, 2211, 2211, 2315, 2211, 2211, 2211, 2211, 2211, 2550,
 /*    80 */  2550, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*    90 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   100 */  2211, 2211, 2317, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   110 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   120 */  2211, 2842, 2211, 2968, 2628, 2211, 2211, 2871, 2211, 2211,
 /*   130 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   140 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2854,
 /*   150 */  2211, 2211, 2288, 2288, 2211, 2854, 2854, 2854, 2814, 2814,
 /*   160 */  2315, 2211, 2211, 2317, 2211, 2630, 2211, 2211, 2211, 2211,
 /*   170 */  2211, 2211, 2211, 2211, 2211, 2459, 2241, 2211, 2211, 2211,
 /*   180 */  2211, 2211, 2211, 2613, 2211, 2211, 2900, 2846, 2847, 2962,
 /*   190 */  2211, 2903, 2865, 2211, 2860, 2211, 2211, 2211, 2211, 2211,
 /*   200 */  2890, 2211, 2211, 2211, 2211, 2211, 2211, 2555, 2211, 2656,
 /*   210 */  2211, 2402, 2607, 2211, 2211, 2211, 2211, 2211, 2946, 2844,
 /*   220 */  2884, 2211, 2211, 2894, 2211, 2211, 2211, 2644, 2317, 2211,
 /*   230 */  2317, 2600, 2538, 2211, 2548, 2211, 2548, 2545, 2211, 2211,
 /*   240 */  2211, 2548, 2545, 2545, 2545, 2391, 2387, 2211, 2211, 2385,
 /*   250 */  2211, 2211, 2211, 2211, 2271, 2211, 2271, 2211, 2317, 2317,
 /*   260 */  2211, 2317, 2211, 2211, 2317, 2211, 2317, 2211, 2317, 2211,
 /*   270 */  2317, 2317, 2211, 2317, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   280 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   290 */  2211, 2211, 2211, 2642, 2623, 2211, 2315, 2211, 2611, 2609,
 /*   300 */  2211, 2315, 2894, 2211, 2211, 2916, 2911, 2916, 2911, 2930,
 /*   310 */  2926, 2916, 2935, 2932, 2896, 2894, 2877, 2873, 2965, 2952,
 /*   320 */  2948, 2211, 2211, 2882, 2880, 2211, 2315, 2315, 2911, 2211,
 /*   330 */  2211, 2211, 2211, 2911, 2211, 2211, 2315, 2211, 2211, 2315,
 /*   340 */  2211, 2211, 2211, 2211, 2315, 2211, 2315, 2211, 2211, 2211,
 /*   350 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   360 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   370 */  2421, 2211, 2211, 2315, 2211, 2243, 2245, 2255, 2211, 2602,
 /*   380 */  2968, 2628, 2633, 2583, 2583, 2462, 2462, 2968, 2462, 2318,
 /*   390 */  2216, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   400 */  2211, 2211, 2211, 2929, 2928, 2762, 2211, 2818, 2817, 2816,
 /*   410 */  2807, 2761, 2416, 2211, 2211, 2211, 2760, 2759, 2211, 2211,
 /*   420 */  2211, 2211, 2406, 2403, 2211, 2211, 2430, 2211, 2211, 2211,
 /*   430 */  2211, 2574, 2573, 2753, 2211, 2211, 2754, 2752, 2751, 2211,
 /*   440 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   450 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   460 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2949, 2953, 2211,
 /*   470 */  2211, 2211, 2843, 2211, 2211, 2211, 2732, 2211, 2211, 2211,
 /*   480 */  2211, 2211, 2700, 2695, 2686, 2677, 2692, 2683, 2671, 2689,
 /*   490 */  2680, 2668, 2665, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   500 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   510 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   520 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   530 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   540 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2544,
 /*   550 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   560 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   570 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   580 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   590 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   600 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   610 */  2211, 2211, 2211, 2211, 2559, 2211, 2211, 2211, 2211, 2211,
 /*   620 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   630 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   640 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2260, 2739,
 /*   650 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   660 */  2211, 2211, 2211, 2742, 2211, 2211, 2743, 2211, 2211, 2211,
 /*   670 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   680 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   690 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   700 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2362,
 /*   710 */  2361, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   720 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   730 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2744,
 /*   740 */  2211, 2211, 2211, 2211, 2627, 2211, 2211, 2734, 2211, 2211,
 /*   750 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   760 */  2211, 2211, 2211, 2211, 2945, 2897, 2211, 2211, 2211, 2211,
 /*   770 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   780 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2732, 2211,
 /*   790 */  2927, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2943, 2211,
 /*   800 */  2947, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2853, 2849,
 /*   810 */  2211, 2211, 2845, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   820 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   830 */  2211, 2211, 2211, 2804, 2211, 2211, 2211, 2838, 2211, 2211,
 /*   840 */  2211, 2211, 2458, 2457, 2456, 2455, 2211, 2211, 2211, 2211,
 /*   850 */  2211, 2211, 2744, 2211, 2747, 2211, 2211, 2211, 2211, 2211,
 /*   860 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2731, 2211, 2789,
 /*   870 */  2788, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   880 */  2211, 2211, 2452, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   890 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2436, 2434,
 /*   900 */  2433, 2432, 2211, 2469, 2211, 2211, 2211, 2465, 2464, 2211,
 /*   910 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   920 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2336, 2211,
 /*   930 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2328, 2211, 2327,
 /*   940 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   950 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
 /*   960 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211, 2240,
 /*   970 */  2211, 2211, 2211, 2211, 2211, 2211, 2211, 2211,
};
/********** End of lemon-generated parsing tables *****************************/

/* The next table maps tokens (terminal symbols) into fallback tokens.  
** If a construct like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
**
** This feature can be used, for example, to cause some keywords in a language
** to revert to identifiers if they keyword does not apply in the context where
** it appears.
*/
#ifdef YYFALLBACK
static const YYCODETYPE yyFallback[] = {
    0,  /*          $ => nothing */
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*      UNION => nothing */
    0,  /*        ALL => nothing */
    0,  /*      MINUS => nothing */
    0,  /*     EXCEPT => nothing */
    0,  /*  INTERSECT => nothing */
    0,  /*  NK_BITAND => nothing */
    0,  /*   NK_BITOR => nothing */
    0,  /*  NK_LSHIFT => nothing */
    0,  /*  NK_RSHIFT => nothing */
    0,  /*    NK_PLUS => nothing */
    0,  /*   NK_MINUS => nothing */
    0,  /*    NK_STAR => nothing */
    0,  /*   NK_SLASH => nothing */
    0,  /*     NK_REM => nothing */
    0,  /*  NK_CONCAT => nothing */
    0,  /*     CREATE => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*      NK_ID => nothing */
    0,  /*       PASS => nothing */
    0,  /*  NK_STRING => nothing */
    0,  /*      ALTER => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*      QTIME => nothing */
    0,  /*        DBS => nothing */
    0,  /*      USERS => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*   NK_COMMA => nothing */
    0,  /*       HOST => nothing */
    0,  /*  IS_IMPORT => nothing */
    0,  /* NK_INTEGER => nothing */
    0,  /*   CREATEDB => nothing */
    0,  /*       USER => nothing */
    0,  /*     ENABLE => nothing */
    0,  /*    SYSINFO => nothing */
    0,  /*        ADD => nothing */
    0,  /*       DROP => nothing */
    0,  /*      GRANT => nothing */
    0,  /*         ON => nothing */
    0,  /*         TO => nothing */
    0,  /*     REVOKE => nothing */
    0,  /*       FROM => nothing */
    0,  /*  SUBSCRIBE => nothing */
    0,  /*       READ => nothing */
    0,  /*      WRITE => nothing */
    0,  /*     NK_DOT => nothing */
    0,  /*       WITH => nothing */
    0,  /* ENCRYPT_KEY => nothing */
    0,  /*      DNODE => nothing */
    0,  /*       PORT => nothing */
    0,  /*     DNODES => nothing */
    0,  /*    RESTORE => nothing */
    0,  /* NK_IPTOKEN => nothing */
    0,  /*      FORCE => nothing */
    0,  /*     UNSAFE => nothing */
    0,  /*    CLUSTER => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*      QNODE => nothing */
    0,  /*      BNODE => nothing */
    0,  /*      SNODE => nothing */
    0,  /*      MNODE => nothing */
    0,  /*      VNODE => nothing */
    0,  /*   DATABASE => nothing */
    0,  /*        USE => nothing */
    0,  /*      FLUSH => nothing */
    0,  /*       TRIM => nothing */
    0,  /*  S3MIGRATE => nothing */
    0,  /*    COMPACT => nothing */
    0,  /*         IF => nothing */
    0,  /*        NOT => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*     BUFFER => nothing */
    0,  /* CACHEMODEL => nothing */
    0,  /*  CACHESIZE => nothing */
    0,  /*       COMP => nothing */
    0,  /*   DURATION => nothing */
    0,  /* NK_VARIABLE => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*       KEEP => nothing */
    0,  /*      PAGES => nothing */
    0,  /*   PAGESIZE => nothing */
    0,  /* TSDB_PAGESIZE => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /* SINGLE_STABLE => nothing */
    0,  /* RETENTIONS => nothing */
    0,  /* SCHEMALESS => nothing */
    0,  /*  WAL_LEVEL => nothing */
    0,  /* WAL_FSYNC_PERIOD => nothing */
    0,  /* WAL_RETENTION_PERIOD => nothing */
    0,  /* WAL_RETENTION_SIZE => nothing */
    0,  /* WAL_ROLL_PERIOD => nothing */
    0,  /* WAL_SEGMENT_SIZE => nothing */
    0,  /* STT_TRIGGER => nothing */
    0,  /* TABLE_PREFIX => nothing */
    0,  /* TABLE_SUFFIX => nothing */
    0,  /* S3_CHUNKSIZE => nothing */
    0,  /* S3_KEEPLOCAL => nothing */
    0,  /* S3_COMPACT => nothing */
    0,  /* KEEP_TIME_OFFSET => nothing */
    0,  /* ENCRYPT_ALGORITHM => nothing */
    0,  /*   NK_COLON => nothing */
    0,  /*    BWLIMIT => nothing */
    0,  /*      START => nothing */
    0,  /*  TIMESTAMP => nothing */
  331,  /*        END => ABORT */
    0,  /*      TABLE => nothing */
    0,  /*      NK_LP => nothing */
    0,  /*      NK_RP => nothing */
    0,  /*     STABLE => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*     MODIFY => nothing */
    0,  /*     RENAME => nothing */
    0,  /*        TAG => nothing */
    0,  /*        SET => nothing */
    0,  /*      NK_EQ => nothing */
    0,  /*      USING => nothing */
    0,  /*       TAGS => nothing */
  331,  /*       FILE => ABORT */
    0,  /*       BOOL => nothing */
    0,  /*    TINYINT => nothing */
    0,  /*   SMALLINT => nothing */
    0,  /*        INT => nothing */
    0,  /*    INTEGER => nothing */
    0,  /*     BIGINT => nothing */
    0,  /*      FLOAT => nothing */
    0,  /*     DOUBLE => nothing */
    0,  /*     BINARY => nothing */
    0,  /*      NCHAR => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       JSON => nothing */
    0,  /*    VARCHAR => nothing */
    0,  /* MEDIUMBLOB => nothing */
    0,  /*       BLOB => nothing */
    0,  /*  VARBINARY => nothing */
    0,  /*   GEOMETRY => nothing */
    0,  /*    DECIMAL => nothing */
    0,  /*    COMMENT => nothing */
    0,  /*  MAX_DELAY => nothing */
    0,  /*  WATERMARK => nothing */
    0,  /*     ROLLUP => nothing */
    0,  /*        TTL => nothing */
    0,  /*        SMA => nothing */
    0,  /* DELETE_MARK => nothing */
    0,  /*      FIRST => nothing */
    0,  /*       LAST => nothing */
    0,  /*       SHOW => nothing */
    0,  /*       FULL => nothing */
    0,  /* PRIVILEGES => nothing */
    0,  /*  DATABASES => nothing */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*     MNODES => nothing */
    0,  /*     QNODES => nothing */
    0,  /*  ARBGROUPS => nothing */
    0,  /*  FUNCTIONS => nothing */
    0,  /*    INDEXES => nothing */
    0,  /*   ACCOUNTS => nothing */
    0,  /*       APPS => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*   LICENCES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*       LOGS => nothing */
    0,  /*   MACHINES => nothing */
    0,  /* ENCRYPTIONS => nothing */
    0,  /*    QUERIES => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     TOPICS => nothing */
    0,  /*  VARIABLES => nothing */
    0,  /*     BNODES => nothing */
    0,  /*     SNODES => nothing */
    0,  /* TRANSACTIONS => nothing */
    0,  /* DISTRIBUTED => nothing */
    0,  /*  CONSUMERS => nothing */
    0,  /* SUBSCRIPTIONS => nothing */
    0,  /*     VNODES => nothing */
    0,  /*      ALIVE => nothing */
    0,  /*      VIEWS => nothing */
  331,  /*       VIEW => ABORT */
    0,  /*   COMPACTS => nothing */
    0,  /*     NORMAL => nothing */
    0,  /*      CHILD => nothing */
    0,  /*       LIKE => nothing */
    0,  /*     TBNAME => nothing */
    0,  /*      QTAGS => nothing */
    0,  /*         AS => nothing */
    0,  /*     SYSTEM => nothing */
    0,  /*       TSMA => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*  RECURSIVE => nothing */
    0,  /*      TSMAS => nothing */
    0,  /*   FUNCTION => nothing */
    0,  /*      INDEX => nothing */
    0,  /*      COUNT => nothing */
    0,  /*   LAST_ROW => nothing */
    0,  /*       META => nothing */
    0,  /*       ONLY => nothing */
    0,  /*      TOPIC => nothing */
    0,  /*   CONSUMER => nothing */
    0,  /*      GROUP => nothing */
    0,  /*       DESC => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    EXPLAIN => nothing */
    0,  /*    ANALYZE => nothing */
    0,  /*    VERBOSE => nothing */
    0,  /*    NK_BOOL => nothing */
    0,  /*      RATIO => nothing */
    0,  /*   NK_FLOAT => nothing */
    0,  /* OUTPUTTYPE => nothing */
    0,  /*  AGGREGATE => nothing */
    0,  /*    BUFSIZE => nothing */
    0,  /*   LANGUAGE => nothing */
    0,  /*    REPLACE => nothing */
    0,  /*     STREAM => nothing */
    0,  /*       INTO => nothing */
    0,  /*      PAUSE => nothing */
    0,  /*     RESUME => nothing */
    0,  /*    PRIMARY => nothing */
  331,  /*        KEY => ABORT */
    0,  /*    TRIGGER => nothing */
    0,  /*    AT_ONCE => nothing */
    0,  /* WINDOW_CLOSE => nothing */
    0,  /*     IGNORE => nothing */
    0,  /*    EXPIRED => nothing */
    0,  /* FILL_HISTORY => nothing */
    0,  /*     UPDATE => nothing */
    0,  /*   SUBTABLE => nothing */
    0,  /*  UNTREATED => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /* TRANSACTION => nothing */
    0,  /*    BALANCE => nothing */
    0,  /*     VGROUP => nothing */
    0,  /*     LEADER => nothing */
    0,  /*      MERGE => nothing */
    0,  /* REDISTRIBUTE => nothing */
    0,  /*      SPLIT => nothing */
    0,  /*     DELETE => nothing */
    0,  /*     INSERT => nothing */
    0,  /*     NK_BIN => nothing */
    0,  /*     NK_HEX => nothing */
    0,  /*       NULL => nothing */
    0,  /* NK_QUESTION => nothing */
    0,  /*   NK_ALIAS => nothing */
    0,  /*   NK_ARROW => nothing */
    0,  /*      ROWTS => nothing */
    0,  /*     QSTART => nothing */
    0,  /*       QEND => nothing */
    0,  /*  QDURATION => nothing */
    0,  /*     WSTART => nothing */
    0,  /*       WEND => nothing */
    0,  /*  WDURATION => nothing */
    0,  /*     IROWTS => nothing */
    0,  /*   ISFILLED => nothing */
    0,  /*       FLOW => nothing */
    0,  /*      FHIGH => nothing */
    0,  /*      FEXPR => nothing */
    0,  /*       CAST => nothing */
    0,  /*        NOW => nothing */
    0,  /*      TODAY => nothing */
    0,  /*   TIMEZONE => nothing */
    0,  /* CLIENT_VERSION => nothing */
    0,  /* SERVER_VERSION => nothing */
    0,  /* SERVER_STATUS => nothing */
    0,  /* CURRENT_USER => nothing */
    0,  /*       CASE => nothing */
    0,  /*       WHEN => nothing */
    0,  /*       THEN => nothing */
    0,  /*       ELSE => nothing */
    0,  /*    BETWEEN => nothing */
    0,  /*         IS => nothing */
    0,  /*      NK_LT => nothing */
    0,  /*      NK_GT => nothing */
    0,  /*      NK_LE => nothing */
    0,  /*      NK_GE => nothing */
    0,  /*      NK_NE => nothing */
    0,  /*      MATCH => nothing */
    0,  /*     NMATCH => nothing */
    0,  /*   CONTAINS => nothing */
    0,  /*         IN => nothing */
    0,  /*       JOIN => nothing */
    0,  /*      INNER => nothing */
    0,  /*       LEFT => nothing */
    0,  /*      RIGHT => nothing */
    0,  /*      OUTER => nothing */
  331,  /*       SEMI => ABORT */
    0,  /*       ANTI => nothing */
    0,  /*       ASOF => nothing */
    0,  /*     WINDOW => nothing */
    0,  /* WINDOW_OFFSET => nothing */
    0,  /*     JLIMIT => nothing */
    0,  /*     SELECT => nothing */
    0,  /*    NK_HINT => nothing */
    0,  /*   DISTINCT => nothing */
    0,  /*      WHERE => nothing */
    0,  /*  PARTITION => nothing */
    0,  /*         BY => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /* EVENT_WINDOW => nothing */
    0,  /* COUNT_WINDOW => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*       FILL => nothing */
    0,  /*      VALUE => nothing */
    0,  /*    VALUE_F => nothing */
    0,  /*       NONE => nothing */
    0,  /*       PREV => nothing */
    0,  /*     NULL_F => nothing */
    0,  /*     LINEAR => nothing */
    0,  /*       NEXT => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      RANGE => nothing */
    0,  /*      EVERY => nothing */
    0,  /*      ORDER => nothing */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      LIMIT => nothing */
    0,  /*     OFFSET => nothing */
    0,  /*        ASC => nothing */
    0,  /*      NULLS => nothing */
    0,  /*      ABORT => nothing */
  331,  /*      AFTER => ABORT */
  331,  /*     ATTACH => ABORT */
  331,  /*     BEFORE => ABORT */
  331,  /*      BEGIN => ABORT */
  331,  /*     BITAND => ABORT */
  331,  /*     BITNOT => ABORT */
  331,  /*      BITOR => ABORT */
  331,  /*     BLOCKS => ABORT */
  331,  /*     CHANGE => ABORT */
  331,  /*      COMMA => ABORT */
  331,  /*     CONCAT => ABORT */
  331,  /*   CONFLICT => ABORT */
  331,  /*       COPY => ABORT */
  331,  /*   DEFERRED => ABORT */
  331,  /* DELIMITERS => ABORT */
  331,  /*     DETACH => ABORT */
  331,  /*     DIVIDE => ABORT */
  331,  /*        DOT => ABORT */
  331,  /*       EACH => ABORT */
  331,  /*       FAIL => ABORT */
  331,  /*        FOR => ABORT */
  331,  /*       GLOB => ABORT */
  331,  /*         ID => ABORT */
  331,  /*  IMMEDIATE => ABORT */
  331,  /*     IMPORT => ABORT */
  331,  /*  INITIALLY => ABORT */
  331,  /*    INSTEAD => ABORT */
  331,  /*     ISNULL => ABORT */
  331,  /*    MODULES => ABORT */
  331,  /*  NK_BITNOT => ABORT */
  331,  /*    NK_SEMI => ABORT */
  331,  /*    NOTNULL => ABORT */
  331,  /*         OF => ABORT */
  331,  /*       PLUS => ABORT */
  331,  /*  PRIVILEGE => ABORT */
  331,  /*      RAISE => ABORT */
  331,  /*   RESTRICT => ABORT */
  331,  /*        ROW => ABORT */
  331,  /*       STAR => ABORT */
  331,  /*  STATEMENT => ABORT */
  331,  /*     STRICT => ABORT */
  331,  /*     STRING => ABORT */
  331,  /*      TIMES => ABORT */
  331,  /*     VALUES => ABORT */
  331,  /*   VARIABLE => ABORT */
  331,  /*        WAL => ABORT */
};
#endif /* YYFALLBACK */

/* The following structure represents a single element of the
** parser's stack.  Information stored includes:
**
**   +  The state number for the parser at this level of the stack.
**
**   +  The value of the token stored at this level of the stack.
**      (In other words, the "major" token.)
**
**   +  The semantic value stored at this level of the stack.  This is
**      the information used by the action routines in the grammar.
**      It is sometimes called the "minor" token.
**
** After the "shift" half of a SHIFTREDUCE action, the stateno field
** actually contains the reduce action for the second half of the
** SHIFTREDUCE.
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number, or reduce action in SHIFTREDUCE */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  yyStackEntry *yytos;          /* Pointer to top element of the stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyhwm;                    /* High-water mark of the stack */
#endif
#ifndef YYNOERRORRECOVERY
  int yyerrcnt;                 /* Shifts left before out of the error */
#endif
  ParseARG_SDECL                /* A place to hold %extra_argument */
  ParseCTX_SDECL                /* A place to hold %extra_context */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
  yyStackEntry yystk0;          /* First stack entry */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
  yyStackEntry *yystackEnd;            /* Last entry in the stack */
#endif
};
typedef struct yyParser yyParser;

#ifndef NDEBUG
#include <stdio.h>
static FILE *yyTraceFILE = 0;
static char *yyTracePrompt = 0;
#endif /* NDEBUG */

#ifndef NDEBUG
/* 
** Turn parser tracing on by giving a stream to which to write the trace
** and a prompt to preface each trace message.  Tracing is turned off
** by making either argument NULL 
**
** Inputs:
** <ul>
** <li> A FILE* to which trace output should be written.
**      If NULL, then tracing is turned off.
** <li> A prefix string written at the beginning of every
**      line of trace output.  If NULL, then tracing is
**      turned off.
** </ul>
**
** Outputs:
** None.
*/
void ParseTrace(FILE *TraceFILE, char *zTracePrompt){
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if( yyTraceFILE==0 ) yyTracePrompt = 0;
  else if( yyTracePrompt==0 ) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#if defined(YYCOVERAGE) || !defined(NDEBUG)
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  /*    0 */ "$",
  /*    1 */ "OR",
  /*    2 */ "AND",
  /*    3 */ "UNION",
  /*    4 */ "ALL",
  /*    5 */ "MINUS",
  /*    6 */ "EXCEPT",
  /*    7 */ "INTERSECT",
  /*    8 */ "NK_BITAND",
  /*    9 */ "NK_BITOR",
  /*   10 */ "NK_LSHIFT",
  /*   11 */ "NK_RSHIFT",
  /*   12 */ "NK_PLUS",
  /*   13 */ "NK_MINUS",
  /*   14 */ "NK_STAR",
  /*   15 */ "NK_SLASH",
  /*   16 */ "NK_REM",
  /*   17 */ "NK_CONCAT",
  /*   18 */ "CREATE",
  /*   19 */ "ACCOUNT",
  /*   20 */ "NK_ID",
  /*   21 */ "PASS",
  /*   22 */ "NK_STRING",
  /*   23 */ "ALTER",
  /*   24 */ "PPS",
  /*   25 */ "TSERIES",
  /*   26 */ "STORAGE",
  /*   27 */ "STREAMS",
  /*   28 */ "QTIME",
  /*   29 */ "DBS",
  /*   30 */ "USERS",
  /*   31 */ "CONNS",
  /*   32 */ "STATE",
  /*   33 */ "NK_COMMA",
  /*   34 */ "HOST",
  /*   35 */ "IS_IMPORT",
  /*   36 */ "NK_INTEGER",
  /*   37 */ "CREATEDB",
  /*   38 */ "USER",
  /*   39 */ "ENABLE",
  /*   40 */ "SYSINFO",
  /*   41 */ "ADD",
  /*   42 */ "DROP",
  /*   43 */ "GRANT",
  /*   44 */ "ON",
  /*   45 */ "TO",
  /*   46 */ "REVOKE",
  /*   47 */ "FROM",
  /*   48 */ "SUBSCRIBE",
  /*   49 */ "READ",
  /*   50 */ "WRITE",
  /*   51 */ "NK_DOT",
  /*   52 */ "WITH",
  /*   53 */ "ENCRYPT_KEY",
  /*   54 */ "DNODE",
  /*   55 */ "PORT",
  /*   56 */ "DNODES",
  /*   57 */ "RESTORE",
  /*   58 */ "NK_IPTOKEN",
  /*   59 */ "FORCE",
  /*   60 */ "UNSAFE",
  /*   61 */ "CLUSTER",
  /*   62 */ "LOCAL",
  /*   63 */ "QNODE",
  /*   64 */ "BNODE",
  /*   65 */ "SNODE",
  /*   66 */ "MNODE",
  /*   67 */ "VNODE",
  /*   68 */ "DATABASE",
  /*   69 */ "USE",
  /*   70 */ "FLUSH",
  /*   71 */ "TRIM",
  /*   72 */ "S3MIGRATE",
  /*   73 */ "COMPACT",
  /*   74 */ "IF",
  /*   75 */ "NOT",
  /*   76 */ "EXISTS",
  /*   77 */ "BUFFER",
  /*   78 */ "CACHEMODEL",
  /*   79 */ "CACHESIZE",
  /*   80 */ "COMP",
  /*   81 */ "DURATION",
  /*   82 */ "NK_VARIABLE",
  /*   83 */ "MAXROWS",
  /*   84 */ "MINROWS",
  /*   85 */ "KEEP",
  /*   86 */ "PAGES",
  /*   87 */ "PAGESIZE",
  /*   88 */ "TSDB_PAGESIZE",
  /*   89 */ "PRECISION",
  /*   90 */ "REPLICA",
  /*   91 */ "VGROUPS",
  /*   92 */ "SINGLE_STABLE",
  /*   93 */ "RETENTIONS",
  /*   94 */ "SCHEMALESS",
  /*   95 */ "WAL_LEVEL",
  /*   96 */ "WAL_FSYNC_PERIOD",
  /*   97 */ "WAL_RETENTION_PERIOD",
  /*   98 */ "WAL_RETENTION_SIZE",
  /*   99 */ "WAL_ROLL_PERIOD",
  /*  100 */ "WAL_SEGMENT_SIZE",
  /*  101 */ "STT_TRIGGER",
  /*  102 */ "TABLE_PREFIX",
  /*  103 */ "TABLE_SUFFIX",
  /*  104 */ "S3_CHUNKSIZE",
  /*  105 */ "S3_KEEPLOCAL",
  /*  106 */ "S3_COMPACT",
  /*  107 */ "KEEP_TIME_OFFSET",
  /*  108 */ "ENCRYPT_ALGORITHM",
  /*  109 */ "NK_COLON",
  /*  110 */ "BWLIMIT",
  /*  111 */ "START",
  /*  112 */ "TIMESTAMP",
  /*  113 */ "END",
  /*  114 */ "TABLE",
  /*  115 */ "NK_LP",
  /*  116 */ "NK_RP",
  /*  117 */ "STABLE",
  /*  118 */ "COLUMN",
  /*  119 */ "MODIFY",
  /*  120 */ "RENAME",
  /*  121 */ "TAG",
  /*  122 */ "SET",
  /*  123 */ "NK_EQ",
  /*  124 */ "USING",
  /*  125 */ "TAGS",
  /*  126 */ "FILE",
  /*  127 */ "BOOL",
  /*  128 */ "TINYINT",
  /*  129 */ "SMALLINT",
  /*  130 */ "INT",
  /*  131 */ "INTEGER",
  /*  132 */ "BIGINT",
  /*  133 */ "FLOAT",
  /*  134 */ "DOUBLE",
  /*  135 */ "BINARY",
  /*  136 */ "NCHAR",
  /*  137 */ "UNSIGNED",
  /*  138 */ "JSON",
  /*  139 */ "VARCHAR",
  /*  140 */ "MEDIUMBLOB",
  /*  141 */ "BLOB",
  /*  142 */ "VARBINARY",
  /*  143 */ "GEOMETRY",
  /*  144 */ "DECIMAL",
  /*  145 */ "COMMENT",
  /*  146 */ "MAX_DELAY",
  /*  147 */ "WATERMARK",
  /*  148 */ "ROLLUP",
  /*  149 */ "TTL",
  /*  150 */ "SMA",
  /*  151 */ "DELETE_MARK",
  /*  152 */ "FIRST",
  /*  153 */ "LAST",
  /*  154 */ "SHOW",
  /*  155 */ "FULL",
  /*  156 */ "PRIVILEGES",
  /*  157 */ "DATABASES",
  /*  158 */ "TABLES",
  /*  159 */ "STABLES",
  /*  160 */ "MNODES",
  /*  161 */ "QNODES",
  /*  162 */ "ARBGROUPS",
  /*  163 */ "FUNCTIONS",
  /*  164 */ "INDEXES",
  /*  165 */ "ACCOUNTS",
  /*  166 */ "APPS",
  /*  167 */ "CONNECTIONS",
  /*  168 */ "LICENCES",
  /*  169 */ "GRANTS",
  /*  170 */ "LOGS",
  /*  171 */ "MACHINES",
  /*  172 */ "ENCRYPTIONS",
  /*  173 */ "QUERIES",
  /*  174 */ "SCORES",
  /*  175 */ "TOPICS",
  /*  176 */ "VARIABLES",
  /*  177 */ "BNODES",
  /*  178 */ "SNODES",
  /*  179 */ "TRANSACTIONS",
  /*  180 */ "DISTRIBUTED",
  /*  181 */ "CONSUMERS",
  /*  182 */ "SUBSCRIPTIONS",
  /*  183 */ "VNODES",
  /*  184 */ "ALIVE",
  /*  185 */ "VIEWS",
  /*  186 */ "VIEW",
  /*  187 */ "COMPACTS",
  /*  188 */ "NORMAL",
  /*  189 */ "CHILD",
  /*  190 */ "LIKE",
  /*  191 */ "TBNAME",
  /*  192 */ "QTAGS",
  /*  193 */ "AS",
  /*  194 */ "SYSTEM",
  /*  195 */ "TSMA",
  /*  196 */ "INTERVAL",
  /*  197 */ "RECURSIVE",
  /*  198 */ "TSMAS",
  /*  199 */ "FUNCTION",
  /*  200 */ "INDEX",
  /*  201 */ "COUNT",
  /*  202 */ "LAST_ROW",
  /*  203 */ "META",
  /*  204 */ "ONLY",
  /*  205 */ "TOPIC",
  /*  206 */ "CONSUMER",
  /*  207 */ "GROUP",
  /*  208 */ "DESC",
  /*  209 */ "DESCRIBE",
  /*  210 */ "RESET",
  /*  211 */ "QUERY",
  /*  212 */ "CACHE",
  /*  213 */ "EXPLAIN",
  /*  214 */ "ANALYZE",
  /*  215 */ "VERBOSE",
  /*  216 */ "NK_BOOL",
  /*  217 */ "RATIO",
  /*  218 */ "NK_FLOAT",
  /*  219 */ "OUTPUTTYPE",
  /*  220 */ "AGGREGATE",
  /*  221 */ "BUFSIZE",
  /*  222 */ "LANGUAGE",
  /*  223 */ "REPLACE",
  /*  224 */ "STREAM",
  /*  225 */ "INTO",
  /*  226 */ "PAUSE",
  /*  227 */ "RESUME",
  /*  228 */ "PRIMARY",
  /*  229 */ "KEY",
  /*  230 */ "TRIGGER",
  /*  231 */ "AT_ONCE",
  /*  232 */ "WINDOW_CLOSE",
  /*  233 */ "IGNORE",
  /*  234 */ "EXPIRED",
  /*  235 */ "FILL_HISTORY",
  /*  236 */ "UPDATE",
  /*  237 */ "SUBTABLE",
  /*  238 */ "UNTREATED",
  /*  239 */ "KILL",
  /*  240 */ "CONNECTION",
  /*  241 */ "TRANSACTION",
  /*  242 */ "BALANCE",
  /*  243 */ "VGROUP",
  /*  244 */ "LEADER",
  /*  245 */ "MERGE",
  /*  246 */ "REDISTRIBUTE",
  /*  247 */ "SPLIT",
  /*  248 */ "DELETE",
  /*  249 */ "INSERT",
  /*  250 */ "NK_BIN",
  /*  251 */ "NK_HEX",
  /*  252 */ "NULL",
  /*  253 */ "NK_QUESTION",
  /*  254 */ "NK_ALIAS",
  /*  255 */ "NK_ARROW",
  /*  256 */ "ROWTS",
  /*  257 */ "QSTART",
  /*  258 */ "QEND",
  /*  259 */ "QDURATION",
  /*  260 */ "WSTART",
  /*  261 */ "WEND",
  /*  262 */ "WDURATION",
  /*  263 */ "IROWTS",
  /*  264 */ "ISFILLED",
  /*  265 */ "FLOW",
  /*  266 */ "FHIGH",
  /*  267 */ "FEXPR",
  /*  268 */ "CAST",
  /*  269 */ "NOW",
  /*  270 */ "TODAY",
  /*  271 */ "TIMEZONE",
  /*  272 */ "CLIENT_VERSION",
  /*  273 */ "SERVER_VERSION",
  /*  274 */ "SERVER_STATUS",
  /*  275 */ "CURRENT_USER",
  /*  276 */ "CASE",
  /*  277 */ "WHEN",
  /*  278 */ "THEN",
  /*  279 */ "ELSE",
  /*  280 */ "BETWEEN",
  /*  281 */ "IS",
  /*  282 */ "NK_LT",
  /*  283 */ "NK_GT",
  /*  284 */ "NK_LE",
  /*  285 */ "NK_GE",
  /*  286 */ "NK_NE",
  /*  287 */ "MATCH",
  /*  288 */ "NMATCH",
  /*  289 */ "CONTAINS",
  /*  290 */ "IN",
  /*  291 */ "JOIN",
  /*  292 */ "INNER",
  /*  293 */ "LEFT",
  /*  294 */ "RIGHT",
  /*  295 */ "OUTER",
  /*  296 */ "SEMI",
  /*  297 */ "ANTI",
  /*  298 */ "ASOF",
  /*  299 */ "WINDOW",
  /*  300 */ "WINDOW_OFFSET",
  /*  301 */ "JLIMIT",
  /*  302 */ "SELECT",
  /*  303 */ "NK_HINT",
  /*  304 */ "DISTINCT",
  /*  305 */ "WHERE",
  /*  306 */ "PARTITION",
  /*  307 */ "BY",
  /*  308 */ "SESSION",
  /*  309 */ "STATE_WINDOW",
  /*  310 */ "EVENT_WINDOW",
  /*  311 */ "COUNT_WINDOW",
  /*  312 */ "SLIDING",
  /*  313 */ "FILL",
  /*  314 */ "VALUE",
  /*  315 */ "VALUE_F",
  /*  316 */ "NONE",
  /*  317 */ "PREV",
  /*  318 */ "NULL_F",
  /*  319 */ "LINEAR",
  /*  320 */ "NEXT",
  /*  321 */ "HAVING",
  /*  322 */ "RANGE",
  /*  323 */ "EVERY",
  /*  324 */ "ORDER",
  /*  325 */ "SLIMIT",
  /*  326 */ "SOFFSET",
  /*  327 */ "LIMIT",
  /*  328 */ "OFFSET",
  /*  329 */ "ASC",
  /*  330 */ "NULLS",
  /*  331 */ "ABORT",
  /*  332 */ "AFTER",
  /*  333 */ "ATTACH",
  /*  334 */ "BEFORE",
  /*  335 */ "BEGIN",
  /*  336 */ "BITAND",
  /*  337 */ "BITNOT",
  /*  338 */ "BITOR",
  /*  339 */ "BLOCKS",
  /*  340 */ "CHANGE",
  /*  341 */ "COMMA",
  /*  342 */ "CONCAT",
  /*  343 */ "CONFLICT",
  /*  344 */ "COPY",
  /*  345 */ "DEFERRED",
  /*  346 */ "DELIMITERS",
  /*  347 */ "DETACH",
  /*  348 */ "DIVIDE",
  /*  349 */ "DOT",
  /*  350 */ "EACH",
  /*  351 */ "FAIL",
  /*  352 */ "FOR",
  /*  353 */ "GLOB",
  /*  354 */ "ID",
  /*  355 */ "IMMEDIATE",
  /*  356 */ "IMPORT",
  /*  357 */ "INITIALLY",
  /*  358 */ "INSTEAD",
  /*  359 */ "ISNULL",
  /*  360 */ "MODULES",
  /*  361 */ "NK_BITNOT",
  /*  362 */ "NK_SEMI",
  /*  363 */ "NOTNULL",
  /*  364 */ "OF",
  /*  365 */ "PLUS",
  /*  366 */ "PRIVILEGE",
  /*  367 */ "RAISE",
  /*  368 */ "RESTRICT",
  /*  369 */ "ROW",
  /*  370 */ "STAR",
  /*  371 */ "STATEMENT",
  /*  372 */ "STRICT",
  /*  373 */ "STRING",
  /*  374 */ "TIMES",
  /*  375 */ "VALUES",
  /*  376 */ "VARIABLE",
  /*  377 */ "WAL",
  /*  378 */ "ENCODE",
  /*  379 */ "COMPRESS",
  /*  380 */ "LEVEL",
  /*  381 */ "cmd",
  /*  382 */ "account_options",
  /*  383 */ "alter_account_options",
  /*  384 */ "literal",
  /*  385 */ "alter_account_option",
  /*  386 */ "ip_range_list",
  /*  387 */ "white_list",
  /*  388 */ "white_list_opt",
  /*  389 */ "is_import_opt",
  /*  390 */ "is_createdb_opt",
  /*  391 */ "user_name",
  /*  392 */ "sysinfo_opt",
  /*  393 */ "privileges",
  /*  394 */ "priv_level",
  /*  395 */ "with_opt",
  /*  396 */ "priv_type_list",
  /*  397 */ "priv_type",
  /*  398 */ "db_name",
  /*  399 */ "table_name",
  /*  400 */ "topic_name",
  /*  401 */ "search_condition",
  /*  402 */ "dnode_endpoint",
  /*  403 */ "force_opt",
  /*  404 */ "unsafe_opt",
  /*  405 */ "not_exists_opt",
  /*  406 */ "db_options",
  /*  407 */ "exists_opt",
  /*  408 */ "alter_db_options",
  /*  409 */ "speed_opt",
  /*  410 */ "start_opt",
  /*  411 */ "end_opt",
  /*  412 */ "integer_list",
  /*  413 */ "variable_list",
  /*  414 */ "retention_list",
  /*  415 */ "signed",
  /*  416 */ "alter_db_option",
  /*  417 */ "retention",
  /*  418 */ "full_table_name",
  /*  419 */ "column_def_list",
  /*  420 */ "tags_def_opt",
  /*  421 */ "table_options",
  /*  422 */ "multi_create_clause",
  /*  423 */ "tags_def",
  /*  424 */ "multi_drop_clause",
  /*  425 */ "alter_table_clause",
  /*  426 */ "alter_table_options",
  /*  427 */ "column_name",
  /*  428 */ "type_name",
  /*  429 */ "column_options",
  /*  430 */ "tags_literal",
  /*  431 */ "create_subtable_clause",
  /*  432 */ "create_from_file_clause",
  /*  433 */ "specific_cols_opt",
  /*  434 */ "tags_literal_list",
  /*  435 */ "tag_list_opt",
  /*  436 */ "drop_table_clause",
  /*  437 */ "col_name_list",
  /*  438 */ "tag_def_list",
  /*  439 */ "tag_def",
  /*  440 */ "column_def",
  /*  441 */ "type_name_default_len",
  /*  442 */ "duration_list",
  /*  443 */ "rollup_func_list",
  /*  444 */ "alter_table_option",
  /*  445 */ "duration_literal",
  /*  446 */ "rollup_func_name",
  /*  447 */ "function_name",
  /*  448 */ "col_name",
  /*  449 */ "db_kind_opt",
  /*  450 */ "table_kind_db_name_cond_opt",
  /*  451 */ "like_pattern_opt",
  /*  452 */ "db_name_cond_opt",
  /*  453 */ "table_name_cond",
  /*  454 */ "from_db_opt",
  /*  455 */ "table_kind",
  /*  456 */ "tag_item",
  /*  457 */ "column_alias",
  /*  458 */ "tsma_name",
  /*  459 */ "tsma_func_list",
  /*  460 */ "full_tsma_name",
  /*  461 */ "func_list",
  /*  462 */ "index_options",
  /*  463 */ "full_index_name",
  /*  464 */ "index_name",
  /*  465 */ "sliding_opt",
  /*  466 */ "sma_stream_opt",
  /*  467 */ "func",
  /*  468 */ "sma_func_name",
  /*  469 */ "expression_list",
  /*  470 */ "with_meta",
  /*  471 */ "query_or_subquery",
  /*  472 */ "where_clause_opt",
  /*  473 */ "cgroup_name",
  /*  474 */ "analyze_opt",
  /*  475 */ "explain_options",
  /*  476 */ "insert_query",
  /*  477 */ "or_replace_opt",
  /*  478 */ "agg_func_opt",
  /*  479 */ "bufsize_opt",
  /*  480 */ "language_opt",
  /*  481 */ "full_view_name",
  /*  482 */ "view_name",
  /*  483 */ "stream_name",
  /*  484 */ "stream_options",
  /*  485 */ "col_list_opt",
  /*  486 */ "tag_def_or_ref_opt",
  /*  487 */ "subtable_opt",
  /*  488 */ "ignore_opt",
  /*  489 */ "column_stream_def_list",
  /*  490 */ "column_stream_def",
  /*  491 */ "stream_col_options",
  /*  492 */ "expression",
  /*  493 */ "on_vgroup_id",
  /*  494 */ "dnode_list",
  /*  495 */ "literal_func",
  /*  496 */ "signed_literal",
  /*  497 */ "literal_list",
  /*  498 */ "table_alias",
  /*  499 */ "expr_or_subquery",
  /*  500 */ "pseudo_column",
  /*  501 */ "column_reference",
  /*  502 */ "function_expression",
  /*  503 */ "case_when_expression",
  /*  504 */ "star_func",
  /*  505 */ "star_func_para_list",
  /*  506 */ "noarg_func",
  /*  507 */ "other_para_list",
  /*  508 */ "star_func_para",
  /*  509 */ "when_then_list",
  /*  510 */ "case_when_else_opt",
  /*  511 */ "common_expression",
  /*  512 */ "when_then_expr",
  /*  513 */ "predicate",
  /*  514 */ "compare_op",
  /*  515 */ "in_op",
  /*  516 */ "in_predicate_value",
  /*  517 */ "boolean_value_expression",
  /*  518 */ "boolean_primary",
  /*  519 */ "from_clause_opt",
  /*  520 */ "table_reference_list",
  /*  521 */ "table_reference",
  /*  522 */ "table_primary",
  /*  523 */ "joined_table",
  /*  524 */ "alias_opt",
  /*  525 */ "subquery",
  /*  526 */ "parenthesized_joined_table",
  /*  527 */ "join_type",
  /*  528 */ "join_subtype",
  /*  529 */ "join_on_clause_opt",
  /*  530 */ "window_offset_clause_opt",
  /*  531 */ "jlimit_clause_opt",
  /*  532 */ "window_offset_literal",
  /*  533 */ "query_specification",
  /*  534 */ "hint_list",
  /*  535 */ "set_quantifier_opt",
  /*  536 */ "tag_mode_opt",
  /*  537 */ "select_list",
  /*  538 */ "partition_by_clause_opt",
  /*  539 */ "range_opt",
  /*  540 */ "every_opt",
  /*  541 */ "fill_opt",
  /*  542 */ "twindow_clause_opt",
  /*  543 */ "group_by_clause_opt",
  /*  544 */ "having_clause_opt",
  /*  545 */ "select_item",
  /*  546 */ "partition_list",
  /*  547 */ "partition_item",
  /*  548 */ "interval_sliding_duration_literal",
  /*  549 */ "fill_mode",
  /*  550 */ "group_by_list",
  /*  551 */ "query_expression",
  /*  552 */ "query_simple",
  /*  553 */ "order_by_clause_opt",
  /*  554 */ "slimit_clause_opt",
  /*  555 */ "limit_clause_opt",
  /*  556 */ "union_query_expression",
  /*  557 */ "query_simple_or_subquery",
  /*  558 */ "sort_specification_list",
  /*  559 */ "sort_specification",
  /*  560 */ "ordering_specification_opt",
  /*  561 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options",
 /*   1 */ "cmd ::= ALTER ACCOUNT NK_ID alter_account_options",
 /*   2 */ "account_options ::=",
 /*   3 */ "account_options ::= account_options PPS literal",
 /*   4 */ "account_options ::= account_options TSERIES literal",
 /*   5 */ "account_options ::= account_options STORAGE literal",
 /*   6 */ "account_options ::= account_options STREAMS literal",
 /*   7 */ "account_options ::= account_options QTIME literal",
 /*   8 */ "account_options ::= account_options DBS literal",
 /*   9 */ "account_options ::= account_options USERS literal",
 /*  10 */ "account_options ::= account_options CONNS literal",
 /*  11 */ "account_options ::= account_options STATE literal",
 /*  12 */ "alter_account_options ::= alter_account_option",
 /*  13 */ "alter_account_options ::= alter_account_options alter_account_option",
 /*  14 */ "alter_account_option ::= PASS literal",
 /*  15 */ "alter_account_option ::= PPS literal",
 /*  16 */ "alter_account_option ::= TSERIES literal",
 /*  17 */ "alter_account_option ::= STORAGE literal",
 /*  18 */ "alter_account_option ::= STREAMS literal",
 /*  19 */ "alter_account_option ::= QTIME literal",
 /*  20 */ "alter_account_option ::= DBS literal",
 /*  21 */ "alter_account_option ::= USERS literal",
 /*  22 */ "alter_account_option ::= CONNS literal",
 /*  23 */ "alter_account_option ::= STATE literal",
 /*  24 */ "ip_range_list ::= NK_STRING",
 /*  25 */ "ip_range_list ::= ip_range_list NK_COMMA NK_STRING",
 /*  26 */ "white_list ::= HOST ip_range_list",
 /*  27 */ "white_list_opt ::=",
 /*  28 */ "white_list_opt ::= white_list",
 /*  29 */ "is_import_opt ::=",
 /*  30 */ "is_import_opt ::= IS_IMPORT NK_INTEGER",
 /*  31 */ "is_createdb_opt ::=",
 /*  32 */ "is_createdb_opt ::= CREATEDB NK_INTEGER",
 /*  33 */ "cmd ::= CREATE USER user_name PASS NK_STRING sysinfo_opt is_createdb_opt is_import_opt white_list_opt",
 /*  34 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*  35 */ "cmd ::= ALTER USER user_name ENABLE NK_INTEGER",
 /*  36 */ "cmd ::= ALTER USER user_name SYSINFO NK_INTEGER",
 /*  37 */ "cmd ::= ALTER USER user_name CREATEDB NK_INTEGER",
 /*  38 */ "cmd ::= ALTER USER user_name ADD white_list",
 /*  39 */ "cmd ::= ALTER USER user_name DROP white_list",
 /*  40 */ "cmd ::= DROP USER user_name",
 /*  41 */ "sysinfo_opt ::=",
 /*  42 */ "sysinfo_opt ::= SYSINFO NK_INTEGER",
 /*  43 */ "cmd ::= GRANT privileges ON priv_level with_opt TO user_name",
 /*  44 */ "cmd ::= REVOKE privileges ON priv_level with_opt FROM user_name",
 /*  45 */ "privileges ::= ALL",
 /*  46 */ "privileges ::= priv_type_list",
 /*  47 */ "privileges ::= SUBSCRIBE",
 /*  48 */ "priv_type_list ::= priv_type",
 /*  49 */ "priv_type_list ::= priv_type_list NK_COMMA priv_type",
 /*  50 */ "priv_type ::= READ",
 /*  51 */ "priv_type ::= WRITE",
 /*  52 */ "priv_type ::= ALTER",
 /*  53 */ "priv_level ::= NK_STAR NK_DOT NK_STAR",
 /*  54 */ "priv_level ::= db_name NK_DOT NK_STAR",
 /*  55 */ "priv_level ::= db_name NK_DOT table_name",
 /*  56 */ "priv_level ::= topic_name",
 /*  57 */ "with_opt ::=",
 /*  58 */ "with_opt ::= WITH search_condition",
 /*  59 */ "cmd ::= CREATE ENCRYPT_KEY NK_STRING",
 /*  60 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*  61 */ "cmd ::= CREATE DNODE dnode_endpoint PORT NK_INTEGER",
 /*  62 */ "cmd ::= DROP DNODE NK_INTEGER force_opt",
 /*  63 */ "cmd ::= DROP DNODE dnode_endpoint force_opt",
 /*  64 */ "cmd ::= DROP DNODE NK_INTEGER unsafe_opt",
 /*  65 */ "cmd ::= DROP DNODE dnode_endpoint unsafe_opt",
 /*  66 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING",
 /*  67 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING",
 /*  68 */ "cmd ::= ALTER ALL DNODES NK_STRING",
 /*  69 */ "cmd ::= ALTER ALL DNODES NK_STRING NK_STRING",
 /*  70 */ "cmd ::= RESTORE DNODE NK_INTEGER",
 /*  71 */ "dnode_endpoint ::= NK_STRING",
 /*  72 */ "dnode_endpoint ::= NK_ID",
 /*  73 */ "dnode_endpoint ::= NK_IPTOKEN",
 /*  74 */ "force_opt ::=",
 /*  75 */ "force_opt ::= FORCE",
 /*  76 */ "unsafe_opt ::= UNSAFE",
 /*  77 */ "cmd ::= ALTER CLUSTER NK_STRING",
 /*  78 */ "cmd ::= ALTER CLUSTER NK_STRING NK_STRING",
 /*  79 */ "cmd ::= ALTER LOCAL NK_STRING",
 /*  80 */ "cmd ::= ALTER LOCAL NK_STRING NK_STRING",
 /*  81 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  82 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  83 */ "cmd ::= RESTORE QNODE ON DNODE NK_INTEGER",
 /*  84 */ "cmd ::= CREATE BNODE ON DNODE NK_INTEGER",
 /*  85 */ "cmd ::= DROP BNODE ON DNODE NK_INTEGER",
 /*  86 */ "cmd ::= CREATE SNODE ON DNODE NK_INTEGER",
 /*  87 */ "cmd ::= DROP SNODE ON DNODE NK_INTEGER",
 /*  88 */ "cmd ::= CREATE MNODE ON DNODE NK_INTEGER",
 /*  89 */ "cmd ::= DROP MNODE ON DNODE NK_INTEGER",
 /*  90 */ "cmd ::= RESTORE MNODE ON DNODE NK_INTEGER",
 /*  91 */ "cmd ::= RESTORE VNODE ON DNODE NK_INTEGER",
 /*  92 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  93 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  94 */ "cmd ::= USE db_name",
 /*  95 */ "cmd ::= ALTER DATABASE db_name alter_db_options",
 /*  96 */ "cmd ::= FLUSH DATABASE db_name",
 /*  97 */ "cmd ::= TRIM DATABASE db_name speed_opt",
 /*  98 */ "cmd ::= S3MIGRATE DATABASE db_name",
 /*  99 */ "cmd ::= COMPACT DATABASE db_name start_opt end_opt",
 /* 100 */ "not_exists_opt ::= IF NOT EXISTS",
 /* 101 */ "not_exists_opt ::=",
 /* 102 */ "exists_opt ::= IF EXISTS",
 /* 103 */ "exists_opt ::=",
 /* 104 */ "db_options ::=",
 /* 105 */ "db_options ::= db_options BUFFER NK_INTEGER",
 /* 106 */ "db_options ::= db_options CACHEMODEL NK_STRING",
 /* 107 */ "db_options ::= db_options CACHESIZE NK_INTEGER",
 /* 108 */ "db_options ::= db_options COMP NK_INTEGER",
 /* 109 */ "db_options ::= db_options DURATION NK_INTEGER",
 /* 110 */ "db_options ::= db_options DURATION NK_VARIABLE",
 /* 111 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /* 112 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /* 113 */ "db_options ::= db_options KEEP integer_list",
 /* 114 */ "db_options ::= db_options KEEP variable_list",
 /* 115 */ "db_options ::= db_options PAGES NK_INTEGER",
 /* 116 */ "db_options ::= db_options PAGESIZE NK_INTEGER",
 /* 117 */ "db_options ::= db_options TSDB_PAGESIZE NK_INTEGER",
 /* 118 */ "db_options ::= db_options PRECISION NK_STRING",
 /* 119 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /* 120 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /* 121 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /* 122 */ "db_options ::= db_options RETENTIONS retention_list",
 /* 123 */ "db_options ::= db_options SCHEMALESS NK_INTEGER",
 /* 124 */ "db_options ::= db_options WAL_LEVEL NK_INTEGER",
 /* 125 */ "db_options ::= db_options WAL_FSYNC_PERIOD NK_INTEGER",
 /* 126 */ "db_options ::= db_options WAL_RETENTION_PERIOD NK_INTEGER",
 /* 127 */ "db_options ::= db_options WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER",
 /* 128 */ "db_options ::= db_options WAL_RETENTION_SIZE NK_INTEGER",
 /* 129 */ "db_options ::= db_options WAL_RETENTION_SIZE NK_MINUS NK_INTEGER",
 /* 130 */ "db_options ::= db_options WAL_ROLL_PERIOD NK_INTEGER",
 /* 131 */ "db_options ::= db_options WAL_SEGMENT_SIZE NK_INTEGER",
 /* 132 */ "db_options ::= db_options STT_TRIGGER NK_INTEGER",
 /* 133 */ "db_options ::= db_options TABLE_PREFIX signed",
 /* 134 */ "db_options ::= db_options TABLE_SUFFIX signed",
 /* 135 */ "db_options ::= db_options S3_CHUNKSIZE NK_INTEGER",
 /* 136 */ "db_options ::= db_options S3_KEEPLOCAL NK_INTEGER",
 /* 137 */ "db_options ::= db_options S3_KEEPLOCAL NK_VARIABLE",
 /* 138 */ "db_options ::= db_options S3_COMPACT NK_INTEGER",
 /* 139 */ "db_options ::= db_options KEEP_TIME_OFFSET NK_INTEGER",
 /* 140 */ "db_options ::= db_options ENCRYPT_ALGORITHM NK_STRING",
 /* 141 */ "alter_db_options ::= alter_db_option",
 /* 142 */ "alter_db_options ::= alter_db_options alter_db_option",
 /* 143 */ "alter_db_option ::= BUFFER NK_INTEGER",
 /* 144 */ "alter_db_option ::= CACHEMODEL NK_STRING",
 /* 145 */ "alter_db_option ::= CACHESIZE NK_INTEGER",
 /* 146 */ "alter_db_option ::= WAL_FSYNC_PERIOD NK_INTEGER",
 /* 147 */ "alter_db_option ::= KEEP integer_list",
 /* 148 */ "alter_db_option ::= KEEP variable_list",
 /* 149 */ "alter_db_option ::= PAGES NK_INTEGER",
 /* 150 */ "alter_db_option ::= REPLICA NK_INTEGER",
 /* 151 */ "alter_db_option ::= WAL_LEVEL NK_INTEGER",
 /* 152 */ "alter_db_option ::= STT_TRIGGER NK_INTEGER",
 /* 153 */ "alter_db_option ::= MINROWS NK_INTEGER",
 /* 154 */ "alter_db_option ::= WAL_RETENTION_PERIOD NK_INTEGER",
 /* 155 */ "alter_db_option ::= WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER",
 /* 156 */ "alter_db_option ::= WAL_RETENTION_SIZE NK_INTEGER",
 /* 157 */ "alter_db_option ::= WAL_RETENTION_SIZE NK_MINUS NK_INTEGER",
 /* 158 */ "alter_db_option ::= S3_KEEPLOCAL NK_INTEGER",
 /* 159 */ "alter_db_option ::= S3_KEEPLOCAL NK_VARIABLE",
 /* 160 */ "alter_db_option ::= S3_COMPACT NK_INTEGER",
 /* 161 */ "alter_db_option ::= KEEP_TIME_OFFSET NK_INTEGER",
 /* 162 */ "alter_db_option ::= ENCRYPT_ALGORITHM NK_STRING",
 /* 163 */ "integer_list ::= NK_INTEGER",
 /* 164 */ "integer_list ::= integer_list NK_COMMA NK_INTEGER",
 /* 165 */ "variable_list ::= NK_VARIABLE",
 /* 166 */ "variable_list ::= variable_list NK_COMMA NK_VARIABLE",
 /* 167 */ "retention_list ::= retention",
 /* 168 */ "retention_list ::= retention_list NK_COMMA retention",
 /* 169 */ "retention ::= NK_VARIABLE NK_COLON NK_VARIABLE",
 /* 170 */ "retention ::= NK_MINUS NK_COLON NK_VARIABLE",
 /* 171 */ "speed_opt ::=",
 /* 172 */ "speed_opt ::= BWLIMIT NK_INTEGER",
 /* 173 */ "start_opt ::=",
 /* 174 */ "start_opt ::= START WITH NK_INTEGER",
 /* 175 */ "start_opt ::= START WITH NK_STRING",
 /* 176 */ "start_opt ::= START WITH TIMESTAMP NK_STRING",
 /* 177 */ "end_opt ::=",
 /* 178 */ "end_opt ::= END WITH NK_INTEGER",
 /* 179 */ "end_opt ::= END WITH NK_STRING",
 /* 180 */ "end_opt ::= END WITH TIMESTAMP NK_STRING",
 /* 181 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /* 182 */ "cmd ::= CREATE TABLE multi_create_clause",
 /* 183 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /* 184 */ "cmd ::= DROP TABLE multi_drop_clause",
 /* 185 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /* 186 */ "cmd ::= ALTER TABLE alter_table_clause",
 /* 187 */ "cmd ::= ALTER STABLE alter_table_clause",
 /* 188 */ "alter_table_clause ::= full_table_name alter_table_options",
 /* 189 */ "alter_table_clause ::= full_table_name ADD COLUMN column_name type_name column_options",
 /* 190 */ "alter_table_clause ::= full_table_name DROP COLUMN column_name",
 /* 191 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name",
 /* 192 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name column_options",
 /* 193 */ "alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name",
 /* 194 */ "alter_table_clause ::= full_table_name ADD TAG column_name type_name",
 /* 195 */ "alter_table_clause ::= full_table_name DROP TAG column_name",
 /* 196 */ "alter_table_clause ::= full_table_name MODIFY TAG column_name type_name",
 /* 197 */ "alter_table_clause ::= full_table_name RENAME TAG column_name column_name",
 /* 198 */ "alter_table_clause ::= full_table_name SET TAG column_name NK_EQ tags_literal",
 /* 199 */ "multi_create_clause ::= create_subtable_clause",
 /* 200 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /* 201 */ "multi_create_clause ::= create_from_file_clause",
 /* 202 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP table_options",
 /* 203 */ "create_from_file_clause ::= not_exists_opt USING full_table_name NK_LP tag_list_opt NK_RP FILE NK_STRING",
 /* 204 */ "multi_drop_clause ::= drop_table_clause",
 /* 205 */ "multi_drop_clause ::= multi_drop_clause NK_COMMA drop_table_clause",
 /* 206 */ "drop_table_clause ::= exists_opt full_table_name",
 /* 207 */ "specific_cols_opt ::=",
 /* 208 */ "specific_cols_opt ::= NK_LP col_name_list NK_RP",
 /* 209 */ "full_table_name ::= table_name",
 /* 210 */ "full_table_name ::= db_name NK_DOT table_name",
 /* 211 */ "tag_def_list ::= tag_def",
 /* 212 */ "tag_def_list ::= tag_def_list NK_COMMA tag_def",
 /* 213 */ "tag_def ::= column_name type_name",
 /* 214 */ "column_def_list ::= column_def",
 /* 215 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /* 216 */ "column_def ::= column_name type_name column_options",
 /* 217 */ "type_name ::= BOOL",
 /* 218 */ "type_name ::= TINYINT",
 /* 219 */ "type_name ::= SMALLINT",
 /* 220 */ "type_name ::= INT",
 /* 221 */ "type_name ::= INTEGER",
 /* 222 */ "type_name ::= BIGINT",
 /* 223 */ "type_name ::= FLOAT",
 /* 224 */ "type_name ::= DOUBLE",
 /* 225 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /* 226 */ "type_name ::= TIMESTAMP",
 /* 227 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /* 228 */ "type_name ::= TINYINT UNSIGNED",
 /* 229 */ "type_name ::= SMALLINT UNSIGNED",
 /* 230 */ "type_name ::= INT UNSIGNED",
 /* 231 */ "type_name ::= BIGINT UNSIGNED",
 /* 232 */ "type_name ::= JSON",
 /* 233 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /* 234 */ "type_name ::= MEDIUMBLOB",
 /* 235 */ "type_name ::= BLOB",
 /* 236 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /* 237 */ "type_name ::= GEOMETRY NK_LP NK_INTEGER NK_RP",
 /* 238 */ "type_name ::= DECIMAL",
 /* 239 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /* 240 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 241 */ "type_name_default_len ::= BINARY",
 /* 242 */ "type_name_default_len ::= NCHAR",
 /* 243 */ "type_name_default_len ::= VARCHAR",
 /* 244 */ "type_name_default_len ::= VARBINARY",
 /* 245 */ "tags_def_opt ::=",
 /* 246 */ "tags_def_opt ::= tags_def",
 /* 247 */ "tags_def ::= TAGS NK_LP tag_def_list NK_RP",
 /* 248 */ "table_options ::=",
 /* 249 */ "table_options ::= table_options COMMENT NK_STRING",
 /* 250 */ "table_options ::= table_options MAX_DELAY duration_list",
 /* 251 */ "table_options ::= table_options WATERMARK duration_list",
 /* 252 */ "table_options ::= table_options ROLLUP NK_LP rollup_func_list NK_RP",
 /* 253 */ "table_options ::= table_options TTL NK_INTEGER",
 /* 254 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /* 255 */ "table_options ::= table_options DELETE_MARK duration_list",
 /* 256 */ "alter_table_options ::= alter_table_option",
 /* 257 */ "alter_table_options ::= alter_table_options alter_table_option",
 /* 258 */ "alter_table_option ::= COMMENT NK_STRING",
 /* 259 */ "alter_table_option ::= TTL NK_INTEGER",
 /* 260 */ "duration_list ::= duration_literal",
 /* 261 */ "duration_list ::= duration_list NK_COMMA duration_literal",
 /* 262 */ "rollup_func_list ::= rollup_func_name",
 /* 263 */ "rollup_func_list ::= rollup_func_list NK_COMMA rollup_func_name",
 /* 264 */ "rollup_func_name ::= function_name",
 /* 265 */ "rollup_func_name ::= FIRST",
 /* 266 */ "rollup_func_name ::= LAST",
 /* 267 */ "col_name_list ::= col_name",
 /* 268 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /* 269 */ "col_name ::= column_name",
 /* 270 */ "cmd ::= SHOW DNODES",
 /* 271 */ "cmd ::= SHOW USERS",
 /* 272 */ "cmd ::= SHOW USERS FULL",
 /* 273 */ "cmd ::= SHOW USER PRIVILEGES",
 /* 274 */ "cmd ::= SHOW db_kind_opt DATABASES",
 /* 275 */ "cmd ::= SHOW table_kind_db_name_cond_opt TABLES like_pattern_opt",
 /* 276 */ "cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt",
 /* 277 */ "cmd ::= SHOW db_name_cond_opt VGROUPS",
 /* 278 */ "cmd ::= SHOW MNODES",
 /* 279 */ "cmd ::= SHOW QNODES",
 /* 280 */ "cmd ::= SHOW ARBGROUPS",
 /* 281 */ "cmd ::= SHOW FUNCTIONS",
 /* 282 */ "cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt",
 /* 283 */ "cmd ::= SHOW INDEXES FROM db_name NK_DOT table_name",
 /* 284 */ "cmd ::= SHOW STREAMS",
 /* 285 */ "cmd ::= SHOW ACCOUNTS",
 /* 286 */ "cmd ::= SHOW APPS",
 /* 287 */ "cmd ::= SHOW CONNECTIONS",
 /* 288 */ "cmd ::= SHOW LICENCES",
 /* 289 */ "cmd ::= SHOW GRANTS",
 /* 290 */ "cmd ::= SHOW GRANTS FULL",
 /* 291 */ "cmd ::= SHOW GRANTS LOGS",
 /* 292 */ "cmd ::= SHOW CLUSTER MACHINES",
 /* 293 */ "cmd ::= SHOW CREATE DATABASE db_name",
 /* 294 */ "cmd ::= SHOW CREATE TABLE full_table_name",
 /* 295 */ "cmd ::= SHOW CREATE STABLE full_table_name",
 /* 296 */ "cmd ::= SHOW ENCRYPTIONS",
 /* 297 */ "cmd ::= SHOW QUERIES",
 /* 298 */ "cmd ::= SHOW SCORES",
 /* 299 */ "cmd ::= SHOW TOPICS",
 /* 300 */ "cmd ::= SHOW VARIABLES",
 /* 301 */ "cmd ::= SHOW CLUSTER VARIABLES",
 /* 302 */ "cmd ::= SHOW LOCAL VARIABLES",
 /* 303 */ "cmd ::= SHOW DNODE NK_INTEGER VARIABLES like_pattern_opt",
 /* 304 */ "cmd ::= SHOW BNODES",
 /* 305 */ "cmd ::= SHOW SNODES",
 /* 306 */ "cmd ::= SHOW CLUSTER",
 /* 307 */ "cmd ::= SHOW TRANSACTIONS",
 /* 308 */ "cmd ::= SHOW TABLE DISTRIBUTED full_table_name",
 /* 309 */ "cmd ::= SHOW CONSUMERS",
 /* 310 */ "cmd ::= SHOW SUBSCRIPTIONS",
 /* 311 */ "cmd ::= SHOW TAGS FROM table_name_cond from_db_opt",
 /* 312 */ "cmd ::= SHOW TAGS FROM db_name NK_DOT table_name",
 /* 313 */ "cmd ::= SHOW TABLE TAGS tag_list_opt FROM table_name_cond from_db_opt",
 /* 314 */ "cmd ::= SHOW TABLE TAGS tag_list_opt FROM db_name NK_DOT table_name",
 /* 315 */ "cmd ::= SHOW VNODES ON DNODE NK_INTEGER",
 /* 316 */ "cmd ::= SHOW VNODES",
 /* 317 */ "cmd ::= SHOW db_name_cond_opt ALIVE",
 /* 318 */ "cmd ::= SHOW CLUSTER ALIVE",
 /* 319 */ "cmd ::= SHOW db_name_cond_opt VIEWS like_pattern_opt",
 /* 320 */ "cmd ::= SHOW CREATE VIEW full_table_name",
 /* 321 */ "cmd ::= SHOW COMPACTS",
 /* 322 */ "cmd ::= SHOW COMPACT NK_INTEGER",
 /* 323 */ "table_kind_db_name_cond_opt ::=",
 /* 324 */ "table_kind_db_name_cond_opt ::= table_kind",
 /* 325 */ "table_kind_db_name_cond_opt ::= db_name NK_DOT",
 /* 326 */ "table_kind_db_name_cond_opt ::= table_kind db_name NK_DOT",
 /* 327 */ "table_kind ::= NORMAL",
 /* 328 */ "table_kind ::= CHILD",
 /* 329 */ "db_name_cond_opt ::=",
 /* 330 */ "db_name_cond_opt ::= db_name NK_DOT",
 /* 331 */ "like_pattern_opt ::=",
 /* 332 */ "like_pattern_opt ::= LIKE NK_STRING",
 /* 333 */ "table_name_cond ::= table_name",
 /* 334 */ "from_db_opt ::=",
 /* 335 */ "from_db_opt ::= FROM db_name",
 /* 336 */ "tag_list_opt ::=",
 /* 337 */ "tag_list_opt ::= tag_item",
 /* 338 */ "tag_list_opt ::= tag_list_opt NK_COMMA tag_item",
 /* 339 */ "tag_item ::= TBNAME",
 /* 340 */ "tag_item ::= QTAGS",
 /* 341 */ "tag_item ::= column_name",
 /* 342 */ "tag_item ::= column_name column_alias",
 /* 343 */ "tag_item ::= column_name AS column_alias",
 /* 344 */ "db_kind_opt ::=",
 /* 345 */ "db_kind_opt ::= USER",
 /* 346 */ "db_kind_opt ::= SYSTEM",
 /* 347 */ "cmd ::= CREATE TSMA not_exists_opt tsma_name ON full_table_name tsma_func_list INTERVAL NK_LP duration_literal NK_RP",
 /* 348 */ "cmd ::= CREATE RECURSIVE TSMA not_exists_opt tsma_name ON full_table_name INTERVAL NK_LP duration_literal NK_RP",
 /* 349 */ "cmd ::= DROP TSMA exists_opt full_tsma_name",
 /* 350 */ "cmd ::= SHOW db_name_cond_opt TSMAS",
 /* 351 */ "full_tsma_name ::= tsma_name",
 /* 352 */ "full_tsma_name ::= db_name NK_DOT tsma_name",
 /* 353 */ "tsma_func_list ::= FUNCTION NK_LP func_list NK_RP",
 /* 354 */ "cmd ::= CREATE SMA INDEX not_exists_opt col_name ON full_table_name index_options",
 /* 355 */ "cmd ::= CREATE INDEX not_exists_opt col_name ON full_table_name NK_LP col_name_list NK_RP",
 /* 356 */ "cmd ::= DROP INDEX exists_opt full_index_name",
 /* 357 */ "full_index_name ::= index_name",
 /* 358 */ "full_index_name ::= db_name NK_DOT index_name",
 /* 359 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt sma_stream_opt",
 /* 360 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt sma_stream_opt",
 /* 361 */ "func_list ::= func",
 /* 362 */ "func_list ::= func_list NK_COMMA func",
 /* 363 */ "func ::= sma_func_name NK_LP expression_list NK_RP",
 /* 364 */ "sma_func_name ::= function_name",
 /* 365 */ "sma_func_name ::= COUNT",
 /* 366 */ "sma_func_name ::= FIRST",
 /* 367 */ "sma_func_name ::= LAST",
 /* 368 */ "sma_func_name ::= LAST_ROW",
 /* 369 */ "sma_stream_opt ::=",
 /* 370 */ "sma_stream_opt ::= sma_stream_opt WATERMARK duration_literal",
 /* 371 */ "sma_stream_opt ::= sma_stream_opt MAX_DELAY duration_literal",
 /* 372 */ "sma_stream_opt ::= sma_stream_opt DELETE_MARK duration_literal",
 /* 373 */ "with_meta ::= AS",
 /* 374 */ "with_meta ::= WITH META AS",
 /* 375 */ "with_meta ::= ONLY META AS",
 /* 376 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_or_subquery",
 /* 377 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta DATABASE db_name",
 /* 378 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta STABLE full_table_name where_clause_opt",
 /* 379 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 380 */ "cmd ::= DROP CONSUMER GROUP exists_opt cgroup_name ON topic_name",
 /* 381 */ "cmd ::= DESC full_table_name",
 /* 382 */ "cmd ::= DESCRIBE full_table_name",
 /* 383 */ "cmd ::= RESET QUERY CACHE",
 /* 384 */ "cmd ::= EXPLAIN analyze_opt explain_options query_or_subquery",
 /* 385 */ "cmd ::= EXPLAIN analyze_opt explain_options insert_query",
 /* 386 */ "analyze_opt ::=",
 /* 387 */ "analyze_opt ::= ANALYZE",
 /* 388 */ "explain_options ::=",
 /* 389 */ "explain_options ::= explain_options VERBOSE NK_BOOL",
 /* 390 */ "explain_options ::= explain_options RATIO NK_FLOAT",
 /* 391 */ "cmd ::= CREATE or_replace_opt agg_func_opt FUNCTION not_exists_opt function_name AS NK_STRING OUTPUTTYPE type_name bufsize_opt language_opt",
 /* 392 */ "cmd ::= DROP FUNCTION exists_opt function_name",
 /* 393 */ "agg_func_opt ::=",
 /* 394 */ "agg_func_opt ::= AGGREGATE",
 /* 395 */ "bufsize_opt ::=",
 /* 396 */ "bufsize_opt ::= BUFSIZE NK_INTEGER",
 /* 397 */ "language_opt ::=",
 /* 398 */ "language_opt ::= LANGUAGE NK_STRING",
 /* 399 */ "or_replace_opt ::=",
 /* 400 */ "or_replace_opt ::= OR REPLACE",
 /* 401 */ "cmd ::= CREATE or_replace_opt VIEW full_view_name AS query_or_subquery",
 /* 402 */ "cmd ::= DROP VIEW exists_opt full_view_name",
 /* 403 */ "full_view_name ::= view_name",
 /* 404 */ "full_view_name ::= db_name NK_DOT view_name",
 /* 405 */ "cmd ::= CREATE STREAM not_exists_opt stream_name stream_options INTO full_table_name col_list_opt tag_def_or_ref_opt subtable_opt AS query_or_subquery",
 /* 406 */ "cmd ::= DROP STREAM exists_opt stream_name",
 /* 407 */ "cmd ::= PAUSE STREAM exists_opt stream_name",
 /* 408 */ "cmd ::= RESUME STREAM exists_opt ignore_opt stream_name",
 /* 409 */ "col_list_opt ::=",
 /* 410 */ "col_list_opt ::= NK_LP column_stream_def_list NK_RP",
 /* 411 */ "column_stream_def_list ::= column_stream_def",
 /* 412 */ "column_stream_def_list ::= column_stream_def_list NK_COMMA column_stream_def",
 /* 413 */ "column_stream_def ::= column_name stream_col_options",
 /* 414 */ "stream_col_options ::=",
 /* 415 */ "stream_col_options ::= stream_col_options PRIMARY KEY",
 /* 416 */ "tag_def_or_ref_opt ::=",
 /* 417 */ "tag_def_or_ref_opt ::= tags_def",
 /* 418 */ "tag_def_or_ref_opt ::= TAGS NK_LP column_stream_def_list NK_RP",
 /* 419 */ "stream_options ::=",
 /* 420 */ "stream_options ::= stream_options TRIGGER AT_ONCE",
 /* 421 */ "stream_options ::= stream_options TRIGGER WINDOW_CLOSE",
 /* 422 */ "stream_options ::= stream_options TRIGGER MAX_DELAY duration_literal",
 /* 423 */ "stream_options ::= stream_options WATERMARK duration_literal",
 /* 424 */ "stream_options ::= stream_options IGNORE EXPIRED NK_INTEGER",
 /* 425 */ "stream_options ::= stream_options FILL_HISTORY NK_INTEGER",
 /* 426 */ "stream_options ::= stream_options DELETE_MARK duration_literal",
 /* 427 */ "stream_options ::= stream_options IGNORE UPDATE NK_INTEGER",
 /* 428 */ "subtable_opt ::=",
 /* 429 */ "subtable_opt ::= SUBTABLE NK_LP expression NK_RP",
 /* 430 */ "ignore_opt ::=",
 /* 431 */ "ignore_opt ::= IGNORE UNTREATED",
 /* 432 */ "cmd ::= KILL CONNECTION NK_INTEGER",
 /* 433 */ "cmd ::= KILL QUERY NK_STRING",
 /* 434 */ "cmd ::= KILL TRANSACTION NK_INTEGER",
 /* 435 */ "cmd ::= KILL COMPACT NK_INTEGER",
 /* 436 */ "cmd ::= BALANCE VGROUP",
 /* 437 */ "cmd ::= BALANCE VGROUP LEADER on_vgroup_id",
 /* 438 */ "cmd ::= BALANCE VGROUP LEADER DATABASE db_name",
 /* 439 */ "cmd ::= MERGE VGROUP NK_INTEGER NK_INTEGER",
 /* 440 */ "cmd ::= REDISTRIBUTE VGROUP NK_INTEGER dnode_list",
 /* 441 */ "cmd ::= SPLIT VGROUP NK_INTEGER",
 /* 442 */ "on_vgroup_id ::=",
 /* 443 */ "on_vgroup_id ::= ON NK_INTEGER",
 /* 444 */ "dnode_list ::= DNODE NK_INTEGER",
 /* 445 */ "dnode_list ::= dnode_list DNODE NK_INTEGER",
 /* 446 */ "cmd ::= DELETE FROM full_table_name where_clause_opt",
 /* 447 */ "cmd ::= query_or_subquery",
 /* 448 */ "cmd ::= insert_query",
 /* 449 */ "insert_query ::= INSERT INTO full_table_name NK_LP col_name_list NK_RP query_or_subquery",
 /* 450 */ "insert_query ::= INSERT INTO full_table_name query_or_subquery",
 /* 451 */ "tags_literal ::= NK_INTEGER",
 /* 452 */ "tags_literal ::= NK_INTEGER NK_PLUS duration_literal",
 /* 453 */ "tags_literal ::= NK_INTEGER NK_MINUS duration_literal",
 /* 454 */ "tags_literal ::= NK_PLUS NK_INTEGER",
 /* 455 */ "tags_literal ::= NK_PLUS NK_INTEGER NK_PLUS duration_literal",
 /* 456 */ "tags_literal ::= NK_PLUS NK_INTEGER NK_MINUS duration_literal",
 /* 457 */ "tags_literal ::= NK_MINUS NK_INTEGER",
 /* 458 */ "tags_literal ::= NK_MINUS NK_INTEGER NK_PLUS duration_literal",
 /* 459 */ "tags_literal ::= NK_MINUS NK_INTEGER NK_MINUS duration_literal",
 /* 460 */ "tags_literal ::= NK_FLOAT",
 /* 461 */ "tags_literal ::= NK_PLUS NK_FLOAT",
 /* 462 */ "tags_literal ::= NK_MINUS NK_FLOAT",
 /* 463 */ "tags_literal ::= NK_BIN",
 /* 464 */ "tags_literal ::= NK_BIN NK_PLUS duration_literal",
 /* 465 */ "tags_literal ::= NK_BIN NK_MINUS duration_literal",
 /* 466 */ "tags_literal ::= NK_PLUS NK_BIN",
 /* 467 */ "tags_literal ::= NK_PLUS NK_BIN NK_PLUS duration_literal",
 /* 468 */ "tags_literal ::= NK_PLUS NK_BIN NK_MINUS duration_literal",
 /* 469 */ "tags_literal ::= NK_MINUS NK_BIN",
 /* 470 */ "tags_literal ::= NK_MINUS NK_BIN NK_PLUS duration_literal",
 /* 471 */ "tags_literal ::= NK_MINUS NK_BIN NK_MINUS duration_literal",
 /* 472 */ "tags_literal ::= NK_HEX",
 /* 473 */ "tags_literal ::= NK_HEX NK_PLUS duration_literal",
 /* 474 */ "tags_literal ::= NK_HEX NK_MINUS duration_literal",
 /* 475 */ "tags_literal ::= NK_PLUS NK_HEX",
 /* 476 */ "tags_literal ::= NK_PLUS NK_HEX NK_PLUS duration_literal",
 /* 477 */ "tags_literal ::= NK_PLUS NK_HEX NK_MINUS duration_literal",
 /* 478 */ "tags_literal ::= NK_MINUS NK_HEX",
 /* 479 */ "tags_literal ::= NK_MINUS NK_HEX NK_PLUS duration_literal",
 /* 480 */ "tags_literal ::= NK_MINUS NK_HEX NK_MINUS duration_literal",
 /* 481 */ "tags_literal ::= NK_STRING",
 /* 482 */ "tags_literal ::= NK_STRING NK_PLUS duration_literal",
 /* 483 */ "tags_literal ::= NK_STRING NK_MINUS duration_literal",
 /* 484 */ "tags_literal ::= NK_BOOL",
 /* 485 */ "tags_literal ::= NULL",
 /* 486 */ "tags_literal ::= literal_func",
 /* 487 */ "tags_literal ::= literal_func NK_PLUS duration_literal",
 /* 488 */ "tags_literal ::= literal_func NK_MINUS duration_literal",
 /* 489 */ "tags_literal_list ::= tags_literal",
 /* 490 */ "tags_literal_list ::= tags_literal_list NK_COMMA tags_literal",
 /* 491 */ "literal ::= NK_INTEGER",
 /* 492 */ "literal ::= NK_FLOAT",
 /* 493 */ "literal ::= NK_STRING",
 /* 494 */ "literal ::= NK_BOOL",
 /* 495 */ "literal ::= TIMESTAMP NK_STRING",
 /* 496 */ "literal ::= duration_literal",
 /* 497 */ "literal ::= NULL",
 /* 498 */ "literal ::= NK_QUESTION",
 /* 499 */ "duration_literal ::= NK_VARIABLE",
 /* 500 */ "signed ::= NK_INTEGER",
 /* 501 */ "signed ::= NK_PLUS NK_INTEGER",
 /* 502 */ "signed ::= NK_MINUS NK_INTEGER",
 /* 503 */ "signed ::= NK_FLOAT",
 /* 504 */ "signed ::= NK_PLUS NK_FLOAT",
 /* 505 */ "signed ::= NK_MINUS NK_FLOAT",
 /* 506 */ "signed_literal ::= signed",
 /* 507 */ "signed_literal ::= NK_STRING",
 /* 508 */ "signed_literal ::= NK_BOOL",
 /* 509 */ "signed_literal ::= TIMESTAMP NK_STRING",
 /* 510 */ "signed_literal ::= duration_literal",
 /* 511 */ "signed_literal ::= NULL",
 /* 512 */ "signed_literal ::= literal_func",
 /* 513 */ "signed_literal ::= NK_QUESTION",
 /* 514 */ "literal_list ::= signed_literal",
 /* 515 */ "literal_list ::= literal_list NK_COMMA signed_literal",
 /* 516 */ "db_name ::= NK_ID",
 /* 517 */ "table_name ::= NK_ID",
 /* 518 */ "column_name ::= NK_ID",
 /* 519 */ "function_name ::= NK_ID",
 /* 520 */ "view_name ::= NK_ID",
 /* 521 */ "table_alias ::= NK_ID",
 /* 522 */ "column_alias ::= NK_ID",
 /* 523 */ "column_alias ::= NK_ALIAS",
 /* 524 */ "user_name ::= NK_ID",
 /* 525 */ "topic_name ::= NK_ID",
 /* 526 */ "stream_name ::= NK_ID",
 /* 527 */ "cgroup_name ::= NK_ID",
 /* 528 */ "index_name ::= NK_ID",
 /* 529 */ "tsma_name ::= NK_ID",
 /* 530 */ "expr_or_subquery ::= expression",
 /* 531 */ "expression ::= literal",
 /* 532 */ "expression ::= pseudo_column",
 /* 533 */ "expression ::= column_reference",
 /* 534 */ "expression ::= function_expression",
 /* 535 */ "expression ::= case_when_expression",
 /* 536 */ "expression ::= NK_LP expression NK_RP",
 /* 537 */ "expression ::= NK_PLUS expr_or_subquery",
 /* 538 */ "expression ::= NK_MINUS expr_or_subquery",
 /* 539 */ "expression ::= expr_or_subquery NK_PLUS expr_or_subquery",
 /* 540 */ "expression ::= expr_or_subquery NK_MINUS expr_or_subquery",
 /* 541 */ "expression ::= expr_or_subquery NK_STAR expr_or_subquery",
 /* 542 */ "expression ::= expr_or_subquery NK_SLASH expr_or_subquery",
 /* 543 */ "expression ::= expr_or_subquery NK_REM expr_or_subquery",
 /* 544 */ "expression ::= column_reference NK_ARROW NK_STRING",
 /* 545 */ "expression ::= expr_or_subquery NK_BITAND expr_or_subquery",
 /* 546 */ "expression ::= expr_or_subquery NK_BITOR expr_or_subquery",
 /* 547 */ "expression_list ::= expr_or_subquery",
 /* 548 */ "expression_list ::= expression_list NK_COMMA expr_or_subquery",
 /* 549 */ "column_reference ::= column_name",
 /* 550 */ "column_reference ::= table_name NK_DOT column_name",
 /* 551 */ "column_reference ::= NK_ALIAS",
 /* 552 */ "column_reference ::= table_name NK_DOT NK_ALIAS",
 /* 553 */ "pseudo_column ::= ROWTS",
 /* 554 */ "pseudo_column ::= TBNAME",
 /* 555 */ "pseudo_column ::= table_name NK_DOT TBNAME",
 /* 556 */ "pseudo_column ::= QSTART",
 /* 557 */ "pseudo_column ::= QEND",
 /* 558 */ "pseudo_column ::= QDURATION",
 /* 559 */ "pseudo_column ::= WSTART",
 /* 560 */ "pseudo_column ::= WEND",
 /* 561 */ "pseudo_column ::= WDURATION",
 /* 562 */ "pseudo_column ::= IROWTS",
 /* 563 */ "pseudo_column ::= ISFILLED",
 /* 564 */ "pseudo_column ::= QTAGS",
 /* 565 */ "pseudo_column ::= FLOW",
 /* 566 */ "pseudo_column ::= FHIGH",
 /* 567 */ "pseudo_column ::= FEXPR",
 /* 568 */ "function_expression ::= function_name NK_LP expression_list NK_RP",
 /* 569 */ "function_expression ::= star_func NK_LP star_func_para_list NK_RP",
 /* 570 */ "function_expression ::= CAST NK_LP expr_or_subquery AS type_name NK_RP",
 /* 571 */ "function_expression ::= CAST NK_LP expr_or_subquery AS type_name_default_len NK_RP",
 /* 572 */ "function_expression ::= literal_func",
 /* 573 */ "literal_func ::= noarg_func NK_LP NK_RP",
 /* 574 */ "literal_func ::= NOW",
 /* 575 */ "literal_func ::= TODAY",
 /* 576 */ "noarg_func ::= NOW",
 /* 577 */ "noarg_func ::= TODAY",
 /* 578 */ "noarg_func ::= TIMEZONE",
 /* 579 */ "noarg_func ::= DATABASE",
 /* 580 */ "noarg_func ::= CLIENT_VERSION",
 /* 581 */ "noarg_func ::= SERVER_VERSION",
 /* 582 */ "noarg_func ::= SERVER_STATUS",
 /* 583 */ "noarg_func ::= CURRENT_USER",
 /* 584 */ "noarg_func ::= USER",
 /* 585 */ "star_func ::= COUNT",
 /* 586 */ "star_func ::= FIRST",
 /* 587 */ "star_func ::= LAST",
 /* 588 */ "star_func ::= LAST_ROW",
 /* 589 */ "star_func_para_list ::= NK_STAR",
 /* 590 */ "star_func_para_list ::= other_para_list",
 /* 591 */ "other_para_list ::= star_func_para",
 /* 592 */ "other_para_list ::= other_para_list NK_COMMA star_func_para",
 /* 593 */ "star_func_para ::= expr_or_subquery",
 /* 594 */ "star_func_para ::= table_name NK_DOT NK_STAR",
 /* 595 */ "case_when_expression ::= CASE when_then_list case_when_else_opt END",
 /* 596 */ "case_when_expression ::= CASE common_expression when_then_list case_when_else_opt END",
 /* 597 */ "when_then_list ::= when_then_expr",
 /* 598 */ "when_then_list ::= when_then_list when_then_expr",
 /* 599 */ "when_then_expr ::= WHEN common_expression THEN common_expression",
 /* 600 */ "case_when_else_opt ::=",
 /* 601 */ "case_when_else_opt ::= ELSE common_expression",
 /* 602 */ "predicate ::= expr_or_subquery compare_op expr_or_subquery",
 /* 603 */ "predicate ::= expr_or_subquery BETWEEN expr_or_subquery AND expr_or_subquery",
 /* 604 */ "predicate ::= expr_or_subquery NOT BETWEEN expr_or_subquery AND expr_or_subquery",
 /* 605 */ "predicate ::= expr_or_subquery IS NULL",
 /* 606 */ "predicate ::= expr_or_subquery IS NOT NULL",
 /* 607 */ "predicate ::= expr_or_subquery in_op in_predicate_value",
 /* 608 */ "compare_op ::= NK_LT",
 /* 609 */ "compare_op ::= NK_GT",
 /* 610 */ "compare_op ::= NK_LE",
 /* 611 */ "compare_op ::= NK_GE",
 /* 612 */ "compare_op ::= NK_NE",
 /* 613 */ "compare_op ::= NK_EQ",
 /* 614 */ "compare_op ::= LIKE",
 /* 615 */ "compare_op ::= NOT LIKE",
 /* 616 */ "compare_op ::= MATCH",
 /* 617 */ "compare_op ::= NMATCH",
 /* 618 */ "compare_op ::= CONTAINS",
 /* 619 */ "in_op ::= IN",
 /* 620 */ "in_op ::= NOT IN",
 /* 621 */ "in_predicate_value ::= NK_LP literal_list NK_RP",
 /* 622 */ "boolean_value_expression ::= boolean_primary",
 /* 623 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 624 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 625 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 626 */ "boolean_primary ::= predicate",
 /* 627 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 628 */ "common_expression ::= expr_or_subquery",
 /* 629 */ "common_expression ::= boolean_value_expression",
 /* 630 */ "from_clause_opt ::=",
 /* 631 */ "from_clause_opt ::= FROM table_reference_list",
 /* 632 */ "table_reference_list ::= table_reference",
 /* 633 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 634 */ "table_reference ::= table_primary",
 /* 635 */ "table_reference ::= joined_table",
 /* 636 */ "table_primary ::= table_name alias_opt",
 /* 637 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 638 */ "table_primary ::= subquery alias_opt",
 /* 639 */ "table_primary ::= parenthesized_joined_table",
 /* 640 */ "alias_opt ::=",
 /* 641 */ "alias_opt ::= table_alias",
 /* 642 */ "alias_opt ::= AS table_alias",
 /* 643 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 644 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 645 */ "joined_table ::= table_reference join_type join_subtype JOIN table_reference join_on_clause_opt window_offset_clause_opt jlimit_clause_opt",
 /* 646 */ "join_type ::=",
 /* 647 */ "join_type ::= INNER",
 /* 648 */ "join_type ::= LEFT",
 /* 649 */ "join_type ::= RIGHT",
 /* 650 */ "join_type ::= FULL",
 /* 651 */ "join_subtype ::=",
 /* 652 */ "join_subtype ::= OUTER",
 /* 653 */ "join_subtype ::= SEMI",
 /* 654 */ "join_subtype ::= ANTI",
 /* 655 */ "join_subtype ::= ASOF",
 /* 656 */ "join_subtype ::= WINDOW",
 /* 657 */ "join_on_clause_opt ::=",
 /* 658 */ "join_on_clause_opt ::= ON search_condition",
 /* 659 */ "window_offset_clause_opt ::=",
 /* 660 */ "window_offset_clause_opt ::= WINDOW_OFFSET NK_LP window_offset_literal NK_COMMA window_offset_literal NK_RP",
 /* 661 */ "window_offset_literal ::= NK_VARIABLE",
 /* 662 */ "window_offset_literal ::= NK_MINUS NK_VARIABLE",
 /* 663 */ "jlimit_clause_opt ::=",
 /* 664 */ "jlimit_clause_opt ::= JLIMIT NK_INTEGER",
 /* 665 */ "query_specification ::= SELECT hint_list set_quantifier_opt tag_mode_opt select_list from_clause_opt where_clause_opt partition_by_clause_opt range_opt every_opt fill_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 666 */ "hint_list ::=",
 /* 667 */ "hint_list ::= NK_HINT",
 /* 668 */ "tag_mode_opt ::=",
 /* 669 */ "tag_mode_opt ::= TAGS",
 /* 670 */ "set_quantifier_opt ::=",
 /* 671 */ "set_quantifier_opt ::= DISTINCT",
 /* 672 */ "set_quantifier_opt ::= ALL",
 /* 673 */ "select_list ::= select_item",
 /* 674 */ "select_list ::= select_list NK_COMMA select_item",
 /* 675 */ "select_item ::= NK_STAR",
 /* 676 */ "select_item ::= common_expression",
 /* 677 */ "select_item ::= common_expression column_alias",
 /* 678 */ "select_item ::= common_expression AS column_alias",
 /* 679 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 680 */ "where_clause_opt ::=",
 /* 681 */ "where_clause_opt ::= WHERE search_condition",
 /* 682 */ "partition_by_clause_opt ::=",
 /* 683 */ "partition_by_clause_opt ::= PARTITION BY partition_list",
 /* 684 */ "partition_list ::= partition_item",
 /* 685 */ "partition_list ::= partition_list NK_COMMA partition_item",
 /* 686 */ "partition_item ::= expr_or_subquery",
 /* 687 */ "partition_item ::= expr_or_subquery column_alias",
 /* 688 */ "partition_item ::= expr_or_subquery AS column_alias",
 /* 689 */ "twindow_clause_opt ::=",
 /* 690 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA interval_sliding_duration_literal NK_RP",
 /* 691 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP expr_or_subquery NK_RP",
 /* 692 */ "twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_RP sliding_opt fill_opt",
 /* 693 */ "twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal NK_RP sliding_opt fill_opt",
 /* 694 */ "twindow_clause_opt ::= EVENT_WINDOW START WITH search_condition END WITH search_condition",
 /* 695 */ "twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_RP",
 /* 696 */ "twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 697 */ "sliding_opt ::=",
 /* 698 */ "sliding_opt ::= SLIDING NK_LP interval_sliding_duration_literal NK_RP",
 /* 699 */ "interval_sliding_duration_literal ::= NK_VARIABLE",
 /* 700 */ "interval_sliding_duration_literal ::= NK_STRING",
 /* 701 */ "interval_sliding_duration_literal ::= NK_INTEGER",
 /* 702 */ "fill_opt ::=",
 /* 703 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 704 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA expression_list NK_RP",
 /* 705 */ "fill_opt ::= FILL NK_LP VALUE_F NK_COMMA expression_list NK_RP",
 /* 706 */ "fill_mode ::= NONE",
 /* 707 */ "fill_mode ::= PREV",
 /* 708 */ "fill_mode ::= NULL",
 /* 709 */ "fill_mode ::= NULL_F",
 /* 710 */ "fill_mode ::= LINEAR",
 /* 711 */ "fill_mode ::= NEXT",
 /* 712 */ "group_by_clause_opt ::=",
 /* 713 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 714 */ "group_by_list ::= expr_or_subquery",
 /* 715 */ "group_by_list ::= group_by_list NK_COMMA expr_or_subquery",
 /* 716 */ "having_clause_opt ::=",
 /* 717 */ "having_clause_opt ::= HAVING search_condition",
 /* 718 */ "range_opt ::=",
 /* 719 */ "range_opt ::= RANGE NK_LP expr_or_subquery NK_COMMA expr_or_subquery NK_RP",
 /* 720 */ "range_opt ::= RANGE NK_LP expr_or_subquery NK_RP",
 /* 721 */ "every_opt ::=",
 /* 722 */ "every_opt ::= EVERY NK_LP duration_literal NK_RP",
 /* 723 */ "query_expression ::= query_simple order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 724 */ "query_simple ::= query_specification",
 /* 725 */ "query_simple ::= union_query_expression",
 /* 726 */ "union_query_expression ::= query_simple_or_subquery UNION ALL query_simple_or_subquery",
 /* 727 */ "union_query_expression ::= query_simple_or_subquery UNION query_simple_or_subquery",
 /* 728 */ "query_simple_or_subquery ::= query_simple",
 /* 729 */ "query_simple_or_subquery ::= subquery",
 /* 730 */ "query_or_subquery ::= query_expression",
 /* 731 */ "query_or_subquery ::= subquery",
 /* 732 */ "order_by_clause_opt ::=",
 /* 733 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 734 */ "slimit_clause_opt ::=",
 /* 735 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 736 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 737 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 738 */ "limit_clause_opt ::=",
 /* 739 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 740 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 741 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 742 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 743 */ "subquery ::= NK_LP subquery NK_RP",
 /* 744 */ "search_condition ::= common_expression",
 /* 745 */ "sort_specification_list ::= sort_specification",
 /* 746 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 747 */ "sort_specification ::= expr_or_subquery ordering_specification_opt null_ordering_opt",
 /* 748 */ "ordering_specification_opt ::=",
 /* 749 */ "ordering_specification_opt ::= ASC",
 /* 750 */ "ordering_specification_opt ::= DESC",
 /* 751 */ "null_ordering_opt ::=",
 /* 752 */ "null_ordering_opt ::= NULLS FIRST",
 /* 753 */ "null_ordering_opt ::= NULLS LAST",
 /* 754 */ "column_options ::=",
 /* 755 */ "column_options ::= column_options PRIMARY KEY",
 /* 756 */ "column_options ::= column_options ENCODE NK_STRING",
 /* 757 */ "column_options ::= column_options COMPRESS NK_STRING",
 /* 758 */ "column_options ::= column_options LEVEL NK_STRING",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.  Return the number
** of errors.  Return 0 on success.
*/
static int yyGrowStack(yyParser *p){
  int newSize;
  int idx;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  idx = p->yytos ? (int)(p->yytos - p->yystack) : 0;
  if( p->yystack==&p->yystk0 ){
    pNew = malloc(newSize*sizeof(pNew[0]));
    if( pNew ) pNew[0] = p->yystk0;
  }else{
    pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  }
  if( pNew ){
    p->yystack = pNew;
    p->yytos = &p->yystack[idx];
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows from %d to %d entries.\n",
              yyTracePrompt, p->yystksz, newSize);
    }
#endif
    p->yystksz = newSize;
  }
  return pNew==0; 
}
#endif

/* Datatype of the argument to the memory allocated passed as the
** second argument to ParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void ParseInit(void *yypRawParser ParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  ParseCTX_STORE
#ifdef YYTRACKMAXSTACKDEPTH
  yypParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  yypParser->yytos = NULL;
  yypParser->yystack = NULL;
  yypParser->yystksz = 0;
  if( yyGrowStack(yypParser) ){
    yypParser->yystack = &yypParser->yystk0;
    yypParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  yypParser->yytos = yypParser->yystack;
  yypParser->yystack[0].stateno = 0;
  yypParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  yypParser->yystackEnd = &yypParser->yystack[YYSTACKDEPTH-1];
#endif
}

#ifndef Parse_ENGINEALWAYSONSTACK
/* 
** This function allocates a new parser.
** The only argument is a pointer to a function which works like
** malloc.
**
** Inputs:
** A pointer to the function used to allocate memory.
**
** Outputs:
** A pointer to a parser.  This pointer is used in subsequent calls
** to Parse and ParseFree.
*/
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) ParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    ParseCTX_STORE
    ParseInit(yypParser ParseCTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* Parse_ENGINEALWAYSONSTACK */


/* The following function deletes the "minor type" or semantic value
** associated with a symbol.  The symbol can be either a terminal
** or nonterminal. "yymajor" is the symbol code, and "yypminor" is
** a pointer to the value to be deleted.  The code used to do the 
** deletions is derived from the %destructor and/or %token_destructor
** directives of the input grammar.
*/
static void yy_destructor(
  yyParser *yypParser,    /* The parser */
  YYCODETYPE yymajor,     /* Type code for object to destroy */
  YYMINORTYPE *yypminor   /* The object to be destroyed */
){
  ParseARG_FETCH
  ParseCTX_FETCH
  switch( yymajor ){
    /* Here is inserted the actions which take place when a
    ** terminal or non-terminal is destroyed.  This can happen
    ** when the symbol is popped from the stack during a
    ** reduce or during error processing or when a parser is 
    ** being destroyed before it is finished parsing.
    **
    ** Note: during a reduce, the only symbols destroyed are those
    ** which appear on the RHS of the rule, but which are *not* used
    ** inside the C code.
    */
/********* Begin destructor definitions ***************************************/
      /* Default NON-TERMINAL Destructor */
    case 381: /* cmd */
    case 384: /* literal */
    case 395: /* with_opt */
    case 401: /* search_condition */
    case 406: /* db_options */
    case 408: /* alter_db_options */
    case 410: /* start_opt */
    case 411: /* end_opt */
    case 415: /* signed */
    case 417: /* retention */
    case 418: /* full_table_name */
    case 421: /* table_options */
    case 425: /* alter_table_clause */
    case 426: /* alter_table_options */
    case 429: /* column_options */
    case 430: /* tags_literal */
    case 431: /* create_subtable_clause */
    case 432: /* create_from_file_clause */
    case 436: /* drop_table_clause */
    case 439: /* tag_def */
    case 440: /* column_def */
    case 445: /* duration_literal */
    case 446: /* rollup_func_name */
    case 448: /* col_name */
    case 451: /* like_pattern_opt */
    case 452: /* db_name_cond_opt */
    case 453: /* table_name_cond */
    case 454: /* from_db_opt */
    case 456: /* tag_item */
    case 460: /* full_tsma_name */
    case 462: /* index_options */
    case 463: /* full_index_name */
    case 465: /* sliding_opt */
    case 466: /* sma_stream_opt */
    case 467: /* func */
    case 471: /* query_or_subquery */
    case 472: /* where_clause_opt */
    case 475: /* explain_options */
    case 476: /* insert_query */
    case 481: /* full_view_name */
    case 484: /* stream_options */
    case 487: /* subtable_opt */
    case 490: /* column_stream_def */
    case 491: /* stream_col_options */
    case 492: /* expression */
    case 495: /* literal_func */
    case 496: /* signed_literal */
    case 499: /* expr_or_subquery */
    case 500: /* pseudo_column */
    case 501: /* column_reference */
    case 502: /* function_expression */
    case 503: /* case_when_expression */
    case 508: /* star_func_para */
    case 510: /* case_when_else_opt */
    case 511: /* common_expression */
    case 512: /* when_then_expr */
    case 513: /* predicate */
    case 516: /* in_predicate_value */
    case 517: /* boolean_value_expression */
    case 518: /* boolean_primary */
    case 519: /* from_clause_opt */
    case 520: /* table_reference_list */
    case 521: /* table_reference */
    case 522: /* table_primary */
    case 523: /* joined_table */
    case 525: /* subquery */
    case 526: /* parenthesized_joined_table */
    case 529: /* join_on_clause_opt */
    case 530: /* window_offset_clause_opt */
    case 531: /* jlimit_clause_opt */
    case 532: /* window_offset_literal */
    case 533: /* query_specification */
    case 539: /* range_opt */
    case 540: /* every_opt */
    case 541: /* fill_opt */
    case 542: /* twindow_clause_opt */
    case 544: /* having_clause_opt */
    case 545: /* select_item */
    case 547: /* partition_item */
    case 548: /* interval_sliding_duration_literal */
    case 551: /* query_expression */
    case 552: /* query_simple */
    case 554: /* slimit_clause_opt */
    case 555: /* limit_clause_opt */
    case 556: /* union_query_expression */
    case 557: /* query_simple_or_subquery */
    case 559: /* sort_specification */
{
#line 7 "source/libs/parser/inc/sql.y"
 nodesDestroyNode((yypminor->yy800)); 
#line 3528 "./source/libs/parser/src/sql.c"
}
      break;
    case 382: /* account_options */
    case 383: /* alter_account_options */
    case 385: /* alter_account_option */
    case 409: /* speed_opt */
    case 470: /* with_meta */
    case 479: /* bufsize_opt */
{
#line 54 "source/libs/parser/inc/sql.y"
 
#line 3540 "./source/libs/parser/src/sql.c"
}
      break;
    case 386: /* ip_range_list */
    case 387: /* white_list */
    case 388: /* white_list_opt */
    case 412: /* integer_list */
    case 413: /* variable_list */
    case 414: /* retention_list */
    case 419: /* column_def_list */
    case 420: /* tags_def_opt */
    case 422: /* multi_create_clause */
    case 423: /* tags_def */
    case 424: /* multi_drop_clause */
    case 433: /* specific_cols_opt */
    case 434: /* tags_literal_list */
    case 435: /* tag_list_opt */
    case 437: /* col_name_list */
    case 438: /* tag_def_list */
    case 442: /* duration_list */
    case 443: /* rollup_func_list */
    case 461: /* func_list */
    case 469: /* expression_list */
    case 485: /* col_list_opt */
    case 486: /* tag_def_or_ref_opt */
    case 489: /* column_stream_def_list */
    case 494: /* dnode_list */
    case 497: /* literal_list */
    case 505: /* star_func_para_list */
    case 507: /* other_para_list */
    case 509: /* when_then_list */
    case 534: /* hint_list */
    case 537: /* select_list */
    case 538: /* partition_by_clause_opt */
    case 543: /* group_by_clause_opt */
    case 546: /* partition_list */
    case 550: /* group_by_list */
    case 553: /* order_by_clause_opt */
    case 558: /* sort_specification_list */
{
#line 85 "source/libs/parser/inc/sql.y"
 nodesDestroyList((yypminor->yy436)); 
#line 3582 "./source/libs/parser/src/sql.c"
}
      break;
    case 389: /* is_import_opt */
    case 390: /* is_createdb_opt */
    case 392: /* sysinfo_opt */
{
#line 99 "source/libs/parser/inc/sql.y"
 
#line 3591 "./source/libs/parser/src/sql.c"
}
      break;
    case 391: /* user_name */
    case 398: /* db_name */
    case 399: /* table_name */
    case 400: /* topic_name */
    case 402: /* dnode_endpoint */
    case 427: /* column_name */
    case 447: /* function_name */
    case 457: /* column_alias */
    case 458: /* tsma_name */
    case 464: /* index_name */
    case 468: /* sma_func_name */
    case 473: /* cgroup_name */
    case 480: /* language_opt */
    case 482: /* view_name */
    case 483: /* stream_name */
    case 493: /* on_vgroup_id */
    case 498: /* table_alias */
    case 504: /* star_func */
    case 506: /* noarg_func */
    case 524: /* alias_opt */
{
#line 1084 "source/libs/parser/inc/sql.y"
 
#line 3617 "./source/libs/parser/src/sql.c"
}
      break;
    case 393: /* privileges */
    case 396: /* priv_type_list */
    case 397: /* priv_type */
{
#line 131 "source/libs/parser/inc/sql.y"
 
#line 3626 "./source/libs/parser/src/sql.c"
}
      break;
    case 394: /* priv_level */
{
#line 148 "source/libs/parser/inc/sql.y"
 
#line 3633 "./source/libs/parser/src/sql.c"
}
      break;
    case 403: /* force_opt */
    case 404: /* unsafe_opt */
    case 405: /* not_exists_opt */
    case 407: /* exists_opt */
    case 474: /* analyze_opt */
    case 477: /* or_replace_opt */
    case 478: /* agg_func_opt */
    case 488: /* ignore_opt */
    case 535: /* set_quantifier_opt */
    case 536: /* tag_mode_opt */
{
#line 180 "source/libs/parser/inc/sql.y"
 
#line 3649 "./source/libs/parser/src/sql.c"
}
      break;
    case 416: /* alter_db_option */
    case 444: /* alter_table_option */
{
#line 288 "source/libs/parser/inc/sql.y"
 
#line 3657 "./source/libs/parser/src/sql.c"
}
      break;
    case 428: /* type_name */
    case 441: /* type_name_default_len */
{
#line 427 "source/libs/parser/inc/sql.y"
 
#line 3665 "./source/libs/parser/src/sql.c"
}
      break;
    case 449: /* db_kind_opt */
    case 455: /* table_kind */
{
#line 606 "source/libs/parser/inc/sql.y"
 
#line 3673 "./source/libs/parser/src/sql.c"
}
      break;
    case 450: /* table_kind_db_name_cond_opt */
{
#line 571 "source/libs/parser/inc/sql.y"
 
#line 3680 "./source/libs/parser/src/sql.c"
}
      break;
    case 459: /* tsma_func_list */
{
#line 625 "source/libs/parser/inc/sql.y"
 nodesDestroyNode((yypminor->yy800)); 
#line 3687 "./source/libs/parser/src/sql.c"
}
      break;
    case 514: /* compare_op */
    case 515: /* in_op */
{
#line 1285 "source/libs/parser/inc/sql.y"
 
#line 3695 "./source/libs/parser/src/sql.c"
}
      break;
    case 527: /* join_type */
{
#line 1366 "source/libs/parser/inc/sql.y"
 
#line 3702 "./source/libs/parser/src/sql.c"
}
      break;
    case 528: /* join_subtype */
{
#line 1374 "source/libs/parser/inc/sql.y"
 
#line 3709 "./source/libs/parser/src/sql.c"
}
      break;
    case 549: /* fill_mode */
{
#line 1490 "source/libs/parser/inc/sql.y"
 
#line 3716 "./source/libs/parser/src/sql.c"
}
      break;
    case 560: /* ordering_specification_opt */
{
#line 1575 "source/libs/parser/inc/sql.y"
 
#line 3723 "./source/libs/parser/src/sql.c"
}
      break;
    case 561: /* null_ordering_opt */
{
#line 1581 "source/libs/parser/inc/sql.y"
 
#line 3730 "./source/libs/parser/src/sql.c"
}
      break;
/********* End destructor definitions *****************************************/
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
*/
static void yy_pop_parser_stack(yyParser *pParser){
  yyStackEntry *yytos;
  assert( pParser->yytos!=0 );
  assert( pParser->yytos > pParser->yystack );
  yytos = pParser->yytos--;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yy_destructor(pParser, yytos->major, &yytos->minor);
}

/*
** Clear all secondary memory allocations from the parser
*/
void ParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef Parse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  ParseFinalize(p);
  (*freeProc)(p);
}
#endif /* Parse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyhwm;
}
#endif

/* This array of booleans keeps track of the parser statement
** coverage.  The element yycoverage[X][Y] is set when the parser
** is in state X and has a lookahead token Y.  In a well-tested
** systems, every element of this matrix should end up being set.
*/
#if defined(YYCOVERAGE)
static unsigned char yycoverage[YYNSTATE][YYNTOKEN];
#endif

/*
** Write into out a description of every state/lookahead combination that
**
**   (1)  has not been used by the parser, and
**   (2)  is not a syntax error.
**
** Return the number of missed state/lookahead combinations.
*/
#if defined(YYCOVERAGE)
int ParseCoverage(FILE *out){
  int stateno, iLookAhead, i;
  int nMissed = 0;
  for(stateno=0; stateno<YYNSTATE; stateno++){
    i = yy_shift_ofst[stateno];
    for(iLookAhead=0; iLookAhead<YYNTOKEN; iLookAhead++){
      if( yy_lookahead[i+iLookAhead]!=iLookAhead ) continue;
      if( yycoverage[stateno][iLookAhead]==0 ) nMissed++;
      if( out ){
        fprintf(out,"State %d lookahead %s %s\n", stateno,
                yyTokenName[iLookAhead],
                yycoverage[stateno][iLookAhead] ? "ok" : "missed");
      }
    }
  }
  return nMissed;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_shift_action(
  YYCODETYPE iLookAhead,    /* The look-ahead token */
  YYACTIONTYPE stateno      /* Current state number */
){
  int i;

  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 );
    assert( i+YYNTOKEN<=(int)sizeof(yy_lookahead)/sizeof(yy_lookahead[0]) );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      if( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0])
             && (iFallback = yyFallback[iLookAhead])!=0 ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE, "%sFALLBACK %s => %s\n",
             yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[iFallback]);
        }
#endif
        assert( yyFallback[iFallback]==0 ); /* Fallback loop must terminate */
        iLookAhead = iFallback;
        continue;
      }
#endif
#ifdef YYWILDCARD
      {
        int j = i - iLookAhead + YYWILDCARD;
        if( 
#if YY_SHIFT_MIN+YYWILDCARD<0
          j>=0 &&
#endif
#if YY_SHIFT_MAX+YYWILDCARD>=YY_ACTTAB_COUNT
          j<YY_ACTTAB_COUNT &&
#endif
          yy_lookahead[j]==YYWILDCARD && iLookAhead>0
        ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead],
               yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
      return yy_default[stateno];
    }else{
      return yy_action[i];
    }
  }while(1);
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
static int yy_find_reduce_action(
  YYACTIONTYPE stateno,     /* Current state number */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
#ifdef YYERRORSYMBOL
  if( stateno>YY_REDUCE_COUNT ){
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_COUNT );
#endif
  i = yy_reduce_ofst[stateno];
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    return yy_default[stateno];
  }
#else
  assert( i>=0 && i<YY_ACTTAB_COUNT );
  assert( yy_lookahead[i]==iLookAhead );
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser){
   ParseARG_FETCH
   ParseCTX_FETCH
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
/******** Begin %stack_overflow code ******************************************/
/******** End %stack_overflow code ********************************************/
   ParseARG_STORE /* Suppress warning about unused %extra_argument var */
   ParseCTX_STORE
}

/*
** Print tracing information for a SHIFT action
*/
#ifndef NDEBUG
static void yyTraceShift(yyParser *yypParser, int yyNewState, const char *zTag){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%s%s '%s', go to state %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState);
    }else{
      fprintf(yyTraceFILE,"%s%s '%s', pending reduce %d\n",
         yyTracePrompt, zTag, yyTokenName[yypParser->yytos->major],
         yyNewState - YY_MIN_REDUCE);
    }
  }
}
#else
# define yyTraceShift(X,Y,Z)
#endif

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  YYACTIONTYPE yyNewState,      /* The new state to shift in */
  YYCODETYPE yyMajor,           /* The major token to shift in */
  ParseTOKENTYPE yyMinor        /* The minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yytos++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
    yypParser->yyhwm++;
    assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack) );
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yytos>yypParser->yystackEnd ){
    yypParser->yytos--;
    yyStackOverflow(yypParser);
    return;
  }
#else
  if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz] ){
    if( yyGrowStack(yypParser) ){
      yypParser->yytos--;
      yyStackOverflow(yypParser);
      return;
    }
  }
#endif
  if( yyNewState > YY_MAX_SHIFT ){
    yyNewState += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
  }
  yytos = yypParser->yytos;
  yytos->stateno = yyNewState;
  yytos->major = yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState, "Shift");
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;       /* Symbol on the left-hand side of the rule */
  signed char nrhs;     /* Negative of the number of RHS symbols in the rule */
} yyRuleInfo[] = {
  {  381,   -6 }, /* (0) cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
  {  381,   -4 }, /* (1) cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
  {  382,    0 }, /* (2) account_options ::= */
  {  382,   -3 }, /* (3) account_options ::= account_options PPS literal */
  {  382,   -3 }, /* (4) account_options ::= account_options TSERIES literal */
  {  382,   -3 }, /* (5) account_options ::= account_options STORAGE literal */
  {  382,   -3 }, /* (6) account_options ::= account_options STREAMS literal */
  {  382,   -3 }, /* (7) account_options ::= account_options QTIME literal */
  {  382,   -3 }, /* (8) account_options ::= account_options DBS literal */
  {  382,   -3 }, /* (9) account_options ::= account_options USERS literal */
  {  382,   -3 }, /* (10) account_options ::= account_options CONNS literal */
  {  382,   -3 }, /* (11) account_options ::= account_options STATE literal */
  {  383,   -1 }, /* (12) alter_account_options ::= alter_account_option */
  {  383,   -2 }, /* (13) alter_account_options ::= alter_account_options alter_account_option */
  {  385,   -2 }, /* (14) alter_account_option ::= PASS literal */
  {  385,   -2 }, /* (15) alter_account_option ::= PPS literal */
  {  385,   -2 }, /* (16) alter_account_option ::= TSERIES literal */
  {  385,   -2 }, /* (17) alter_account_option ::= STORAGE literal */
  {  385,   -2 }, /* (18) alter_account_option ::= STREAMS literal */
  {  385,   -2 }, /* (19) alter_account_option ::= QTIME literal */
  {  385,   -2 }, /* (20) alter_account_option ::= DBS literal */
  {  385,   -2 }, /* (21) alter_account_option ::= USERS literal */
  {  385,   -2 }, /* (22) alter_account_option ::= CONNS literal */
  {  385,   -2 }, /* (23) alter_account_option ::= STATE literal */
  {  386,   -1 }, /* (24) ip_range_list ::= NK_STRING */
  {  386,   -3 }, /* (25) ip_range_list ::= ip_range_list NK_COMMA NK_STRING */
  {  387,   -2 }, /* (26) white_list ::= HOST ip_range_list */
  {  388,    0 }, /* (27) white_list_opt ::= */
  {  388,   -1 }, /* (28) white_list_opt ::= white_list */
  {  389,    0 }, /* (29) is_import_opt ::= */
  {  389,   -2 }, /* (30) is_import_opt ::= IS_IMPORT NK_INTEGER */
  {  390,    0 }, /* (31) is_createdb_opt ::= */
  {  390,   -2 }, /* (32) is_createdb_opt ::= CREATEDB NK_INTEGER */
  {  381,   -9 }, /* (33) cmd ::= CREATE USER user_name PASS NK_STRING sysinfo_opt is_createdb_opt is_import_opt white_list_opt */
  {  381,   -5 }, /* (34) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  381,   -5 }, /* (35) cmd ::= ALTER USER user_name ENABLE NK_INTEGER */
  {  381,   -5 }, /* (36) cmd ::= ALTER USER user_name SYSINFO NK_INTEGER */
  {  381,   -5 }, /* (37) cmd ::= ALTER USER user_name CREATEDB NK_INTEGER */
  {  381,   -5 }, /* (38) cmd ::= ALTER USER user_name ADD white_list */
  {  381,   -5 }, /* (39) cmd ::= ALTER USER user_name DROP white_list */
  {  381,   -3 }, /* (40) cmd ::= DROP USER user_name */
  {  392,    0 }, /* (41) sysinfo_opt ::= */
  {  392,   -2 }, /* (42) sysinfo_opt ::= SYSINFO NK_INTEGER */
  {  381,   -7 }, /* (43) cmd ::= GRANT privileges ON priv_level with_opt TO user_name */
  {  381,   -7 }, /* (44) cmd ::= REVOKE privileges ON priv_level with_opt FROM user_name */
  {  393,   -1 }, /* (45) privileges ::= ALL */
  {  393,   -1 }, /* (46) privileges ::= priv_type_list */
  {  393,   -1 }, /* (47) privileges ::= SUBSCRIBE */
  {  396,   -1 }, /* (48) priv_type_list ::= priv_type */
  {  396,   -3 }, /* (49) priv_type_list ::= priv_type_list NK_COMMA priv_type */
  {  397,   -1 }, /* (50) priv_type ::= READ */
  {  397,   -1 }, /* (51) priv_type ::= WRITE */
  {  397,   -1 }, /* (52) priv_type ::= ALTER */
  {  394,   -3 }, /* (53) priv_level ::= NK_STAR NK_DOT NK_STAR */
  {  394,   -3 }, /* (54) priv_level ::= db_name NK_DOT NK_STAR */
  {  394,   -3 }, /* (55) priv_level ::= db_name NK_DOT table_name */
  {  394,   -1 }, /* (56) priv_level ::= topic_name */
  {  395,    0 }, /* (57) with_opt ::= */
  {  395,   -2 }, /* (58) with_opt ::= WITH search_condition */
  {  381,   -3 }, /* (59) cmd ::= CREATE ENCRYPT_KEY NK_STRING */
  {  381,   -3 }, /* (60) cmd ::= CREATE DNODE dnode_endpoint */
  {  381,   -5 }, /* (61) cmd ::= CREATE DNODE dnode_endpoint PORT NK_INTEGER */
  {  381,   -4 }, /* (62) cmd ::= DROP DNODE NK_INTEGER force_opt */
  {  381,   -4 }, /* (63) cmd ::= DROP DNODE dnode_endpoint force_opt */
  {  381,   -4 }, /* (64) cmd ::= DROP DNODE NK_INTEGER unsafe_opt */
  {  381,   -4 }, /* (65) cmd ::= DROP DNODE dnode_endpoint unsafe_opt */
  {  381,   -4 }, /* (66) cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
  {  381,   -5 }, /* (67) cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
  {  381,   -4 }, /* (68) cmd ::= ALTER ALL DNODES NK_STRING */
  {  381,   -5 }, /* (69) cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
  {  381,   -3 }, /* (70) cmd ::= RESTORE DNODE NK_INTEGER */
  {  402,   -1 }, /* (71) dnode_endpoint ::= NK_STRING */
  {  402,   -1 }, /* (72) dnode_endpoint ::= NK_ID */
  {  402,   -1 }, /* (73) dnode_endpoint ::= NK_IPTOKEN */
  {  403,    0 }, /* (74) force_opt ::= */
  {  403,   -1 }, /* (75) force_opt ::= FORCE */
  {  404,   -1 }, /* (76) unsafe_opt ::= UNSAFE */
  {  381,   -3 }, /* (77) cmd ::= ALTER CLUSTER NK_STRING */
  {  381,   -4 }, /* (78) cmd ::= ALTER CLUSTER NK_STRING NK_STRING */
  {  381,   -3 }, /* (79) cmd ::= ALTER LOCAL NK_STRING */
  {  381,   -4 }, /* (80) cmd ::= ALTER LOCAL NK_STRING NK_STRING */
  {  381,   -5 }, /* (81) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (82) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (83) cmd ::= RESTORE QNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (84) cmd ::= CREATE BNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (85) cmd ::= DROP BNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (86) cmd ::= CREATE SNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (87) cmd ::= DROP SNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (88) cmd ::= CREATE MNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (89) cmd ::= DROP MNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (90) cmd ::= RESTORE MNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (91) cmd ::= RESTORE VNODE ON DNODE NK_INTEGER */
  {  381,   -5 }, /* (92) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  381,   -4 }, /* (93) cmd ::= DROP DATABASE exists_opt db_name */
  {  381,   -2 }, /* (94) cmd ::= USE db_name */
  {  381,   -4 }, /* (95) cmd ::= ALTER DATABASE db_name alter_db_options */
  {  381,   -3 }, /* (96) cmd ::= FLUSH DATABASE db_name */
  {  381,   -4 }, /* (97) cmd ::= TRIM DATABASE db_name speed_opt */
  {  381,   -3 }, /* (98) cmd ::= S3MIGRATE DATABASE db_name */
  {  381,   -5 }, /* (99) cmd ::= COMPACT DATABASE db_name start_opt end_opt */
  {  405,   -3 }, /* (100) not_exists_opt ::= IF NOT EXISTS */
  {  405,    0 }, /* (101) not_exists_opt ::= */
  {  407,   -2 }, /* (102) exists_opt ::= IF EXISTS */
  {  407,    0 }, /* (103) exists_opt ::= */
  {  406,    0 }, /* (104) db_options ::= */
  {  406,   -3 }, /* (105) db_options ::= db_options BUFFER NK_INTEGER */
  {  406,   -3 }, /* (106) db_options ::= db_options CACHEMODEL NK_STRING */
  {  406,   -3 }, /* (107) db_options ::= db_options CACHESIZE NK_INTEGER */
  {  406,   -3 }, /* (108) db_options ::= db_options COMP NK_INTEGER */
  {  406,   -3 }, /* (109) db_options ::= db_options DURATION NK_INTEGER */
  {  406,   -3 }, /* (110) db_options ::= db_options DURATION NK_VARIABLE */
  {  406,   -3 }, /* (111) db_options ::= db_options MAXROWS NK_INTEGER */
  {  406,   -3 }, /* (112) db_options ::= db_options MINROWS NK_INTEGER */
  {  406,   -3 }, /* (113) db_options ::= db_options KEEP integer_list */
  {  406,   -3 }, /* (114) db_options ::= db_options KEEP variable_list */
  {  406,   -3 }, /* (115) db_options ::= db_options PAGES NK_INTEGER */
  {  406,   -3 }, /* (116) db_options ::= db_options PAGESIZE NK_INTEGER */
  {  406,   -3 }, /* (117) db_options ::= db_options TSDB_PAGESIZE NK_INTEGER */
  {  406,   -3 }, /* (118) db_options ::= db_options PRECISION NK_STRING */
  {  406,   -3 }, /* (119) db_options ::= db_options REPLICA NK_INTEGER */
  {  406,   -3 }, /* (120) db_options ::= db_options VGROUPS NK_INTEGER */
  {  406,   -3 }, /* (121) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  406,   -3 }, /* (122) db_options ::= db_options RETENTIONS retention_list */
  {  406,   -3 }, /* (123) db_options ::= db_options SCHEMALESS NK_INTEGER */
  {  406,   -3 }, /* (124) db_options ::= db_options WAL_LEVEL NK_INTEGER */
  {  406,   -3 }, /* (125) db_options ::= db_options WAL_FSYNC_PERIOD NK_INTEGER */
  {  406,   -3 }, /* (126) db_options ::= db_options WAL_RETENTION_PERIOD NK_INTEGER */
  {  406,   -4 }, /* (127) db_options ::= db_options WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER */
  {  406,   -3 }, /* (128) db_options ::= db_options WAL_RETENTION_SIZE NK_INTEGER */
  {  406,   -4 }, /* (129) db_options ::= db_options WAL_RETENTION_SIZE NK_MINUS NK_INTEGER */
  {  406,   -3 }, /* (130) db_options ::= db_options WAL_ROLL_PERIOD NK_INTEGER */
  {  406,   -3 }, /* (131) db_options ::= db_options WAL_SEGMENT_SIZE NK_INTEGER */
  {  406,   -3 }, /* (132) db_options ::= db_options STT_TRIGGER NK_INTEGER */
  {  406,   -3 }, /* (133) db_options ::= db_options TABLE_PREFIX signed */
  {  406,   -3 }, /* (134) db_options ::= db_options TABLE_SUFFIX signed */
  {  406,   -3 }, /* (135) db_options ::= db_options S3_CHUNKSIZE NK_INTEGER */
  {  406,   -3 }, /* (136) db_options ::= db_options S3_KEEPLOCAL NK_INTEGER */
  {  406,   -3 }, /* (137) db_options ::= db_options S3_KEEPLOCAL NK_VARIABLE */
  {  406,   -3 }, /* (138) db_options ::= db_options S3_COMPACT NK_INTEGER */
  {  406,   -3 }, /* (139) db_options ::= db_options KEEP_TIME_OFFSET NK_INTEGER */
  {  406,   -3 }, /* (140) db_options ::= db_options ENCRYPT_ALGORITHM NK_STRING */
  {  408,   -1 }, /* (141) alter_db_options ::= alter_db_option */
  {  408,   -2 }, /* (142) alter_db_options ::= alter_db_options alter_db_option */
  {  416,   -2 }, /* (143) alter_db_option ::= BUFFER NK_INTEGER */
  {  416,   -2 }, /* (144) alter_db_option ::= CACHEMODEL NK_STRING */
  {  416,   -2 }, /* (145) alter_db_option ::= CACHESIZE NK_INTEGER */
  {  416,   -2 }, /* (146) alter_db_option ::= WAL_FSYNC_PERIOD NK_INTEGER */
  {  416,   -2 }, /* (147) alter_db_option ::= KEEP integer_list */
  {  416,   -2 }, /* (148) alter_db_option ::= KEEP variable_list */
  {  416,   -2 }, /* (149) alter_db_option ::= PAGES NK_INTEGER */
  {  416,   -2 }, /* (150) alter_db_option ::= REPLICA NK_INTEGER */
  {  416,   -2 }, /* (151) alter_db_option ::= WAL_LEVEL NK_INTEGER */
  {  416,   -2 }, /* (152) alter_db_option ::= STT_TRIGGER NK_INTEGER */
  {  416,   -2 }, /* (153) alter_db_option ::= MINROWS NK_INTEGER */
  {  416,   -2 }, /* (154) alter_db_option ::= WAL_RETENTION_PERIOD NK_INTEGER */
  {  416,   -3 }, /* (155) alter_db_option ::= WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER */
  {  416,   -2 }, /* (156) alter_db_option ::= WAL_RETENTION_SIZE NK_INTEGER */
  {  416,   -3 }, /* (157) alter_db_option ::= WAL_RETENTION_SIZE NK_MINUS NK_INTEGER */
  {  416,   -2 }, /* (158) alter_db_option ::= S3_KEEPLOCAL NK_INTEGER */
  {  416,   -2 }, /* (159) alter_db_option ::= S3_KEEPLOCAL NK_VARIABLE */
  {  416,   -2 }, /* (160) alter_db_option ::= S3_COMPACT NK_INTEGER */
  {  416,   -2 }, /* (161) alter_db_option ::= KEEP_TIME_OFFSET NK_INTEGER */
  {  416,   -2 }, /* (162) alter_db_option ::= ENCRYPT_ALGORITHM NK_STRING */
  {  412,   -1 }, /* (163) integer_list ::= NK_INTEGER */
  {  412,   -3 }, /* (164) integer_list ::= integer_list NK_COMMA NK_INTEGER */
  {  413,   -1 }, /* (165) variable_list ::= NK_VARIABLE */
  {  413,   -3 }, /* (166) variable_list ::= variable_list NK_COMMA NK_VARIABLE */
  {  414,   -1 }, /* (167) retention_list ::= retention */
  {  414,   -3 }, /* (168) retention_list ::= retention_list NK_COMMA retention */
  {  417,   -3 }, /* (169) retention ::= NK_VARIABLE NK_COLON NK_VARIABLE */
  {  417,   -3 }, /* (170) retention ::= NK_MINUS NK_COLON NK_VARIABLE */
  {  409,    0 }, /* (171) speed_opt ::= */
  {  409,   -2 }, /* (172) speed_opt ::= BWLIMIT NK_INTEGER */
  {  410,    0 }, /* (173) start_opt ::= */
  {  410,   -3 }, /* (174) start_opt ::= START WITH NK_INTEGER */
  {  410,   -3 }, /* (175) start_opt ::= START WITH NK_STRING */
  {  410,   -4 }, /* (176) start_opt ::= START WITH TIMESTAMP NK_STRING */
  {  411,    0 }, /* (177) end_opt ::= */
  {  411,   -3 }, /* (178) end_opt ::= END WITH NK_INTEGER */
  {  411,   -3 }, /* (179) end_opt ::= END WITH NK_STRING */
  {  411,   -4 }, /* (180) end_opt ::= END WITH TIMESTAMP NK_STRING */
  {  381,   -9 }, /* (181) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  381,   -3 }, /* (182) cmd ::= CREATE TABLE multi_create_clause */
  {  381,   -9 }, /* (183) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  381,   -3 }, /* (184) cmd ::= DROP TABLE multi_drop_clause */
  {  381,   -4 }, /* (185) cmd ::= DROP STABLE exists_opt full_table_name */
  {  381,   -3 }, /* (186) cmd ::= ALTER TABLE alter_table_clause */
  {  381,   -3 }, /* (187) cmd ::= ALTER STABLE alter_table_clause */
  {  425,   -2 }, /* (188) alter_table_clause ::= full_table_name alter_table_options */
  {  425,   -6 }, /* (189) alter_table_clause ::= full_table_name ADD COLUMN column_name type_name column_options */
  {  425,   -4 }, /* (190) alter_table_clause ::= full_table_name DROP COLUMN column_name */
  {  425,   -5 }, /* (191) alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
  {  425,   -5 }, /* (192) alter_table_clause ::= full_table_name MODIFY COLUMN column_name column_options */
  {  425,   -5 }, /* (193) alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
  {  425,   -5 }, /* (194) alter_table_clause ::= full_table_name ADD TAG column_name type_name */
  {  425,   -4 }, /* (195) alter_table_clause ::= full_table_name DROP TAG column_name */
  {  425,   -5 }, /* (196) alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
  {  425,   -5 }, /* (197) alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
  {  425,   -6 }, /* (198) alter_table_clause ::= full_table_name SET TAG column_name NK_EQ tags_literal */
  {  422,   -1 }, /* (199) multi_create_clause ::= create_subtable_clause */
  {  422,   -2 }, /* (200) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  422,   -1 }, /* (201) multi_create_clause ::= create_from_file_clause */
  {  431,  -10 }, /* (202) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP table_options */
  {  432,   -8 }, /* (203) create_from_file_clause ::= not_exists_opt USING full_table_name NK_LP tag_list_opt NK_RP FILE NK_STRING */
  {  424,   -1 }, /* (204) multi_drop_clause ::= drop_table_clause */
  {  424,   -3 }, /* (205) multi_drop_clause ::= multi_drop_clause NK_COMMA drop_table_clause */
  {  436,   -2 }, /* (206) drop_table_clause ::= exists_opt full_table_name */
  {  433,    0 }, /* (207) specific_cols_opt ::= */
  {  433,   -3 }, /* (208) specific_cols_opt ::= NK_LP col_name_list NK_RP */
  {  418,   -1 }, /* (209) full_table_name ::= table_name */
  {  418,   -3 }, /* (210) full_table_name ::= db_name NK_DOT table_name */
  {  438,   -1 }, /* (211) tag_def_list ::= tag_def */
  {  438,   -3 }, /* (212) tag_def_list ::= tag_def_list NK_COMMA tag_def */
  {  439,   -2 }, /* (213) tag_def ::= column_name type_name */
  {  419,   -1 }, /* (214) column_def_list ::= column_def */
  {  419,   -3 }, /* (215) column_def_list ::= column_def_list NK_COMMA column_def */
  {  440,   -3 }, /* (216) column_def ::= column_name type_name column_options */
  {  428,   -1 }, /* (217) type_name ::= BOOL */
  {  428,   -1 }, /* (218) type_name ::= TINYINT */
  {  428,   -1 }, /* (219) type_name ::= SMALLINT */
  {  428,   -1 }, /* (220) type_name ::= INT */
  {  428,   -1 }, /* (221) type_name ::= INTEGER */
  {  428,   -1 }, /* (222) type_name ::= BIGINT */
  {  428,   -1 }, /* (223) type_name ::= FLOAT */
  {  428,   -1 }, /* (224) type_name ::= DOUBLE */
  {  428,   -4 }, /* (225) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  428,   -1 }, /* (226) type_name ::= TIMESTAMP */
  {  428,   -4 }, /* (227) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  428,   -2 }, /* (228) type_name ::= TINYINT UNSIGNED */
  {  428,   -2 }, /* (229) type_name ::= SMALLINT UNSIGNED */
  {  428,   -2 }, /* (230) type_name ::= INT UNSIGNED */
  {  428,   -2 }, /* (231) type_name ::= BIGINT UNSIGNED */
  {  428,   -1 }, /* (232) type_name ::= JSON */
  {  428,   -4 }, /* (233) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  428,   -1 }, /* (234) type_name ::= MEDIUMBLOB */
  {  428,   -1 }, /* (235) type_name ::= BLOB */
  {  428,   -4 }, /* (236) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  428,   -4 }, /* (237) type_name ::= GEOMETRY NK_LP NK_INTEGER NK_RP */
  {  428,   -1 }, /* (238) type_name ::= DECIMAL */
  {  428,   -4 }, /* (239) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  428,   -6 }, /* (240) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  441,   -1 }, /* (241) type_name_default_len ::= BINARY */
  {  441,   -1 }, /* (242) type_name_default_len ::= NCHAR */
  {  441,   -1 }, /* (243) type_name_default_len ::= VARCHAR */
  {  441,   -1 }, /* (244) type_name_default_len ::= VARBINARY */
  {  420,    0 }, /* (245) tags_def_opt ::= */
  {  420,   -1 }, /* (246) tags_def_opt ::= tags_def */
  {  423,   -4 }, /* (247) tags_def ::= TAGS NK_LP tag_def_list NK_RP */
  {  421,    0 }, /* (248) table_options ::= */
  {  421,   -3 }, /* (249) table_options ::= table_options COMMENT NK_STRING */
  {  421,   -3 }, /* (250) table_options ::= table_options MAX_DELAY duration_list */
  {  421,   -3 }, /* (251) table_options ::= table_options WATERMARK duration_list */
  {  421,   -5 }, /* (252) table_options ::= table_options ROLLUP NK_LP rollup_func_list NK_RP */
  {  421,   -3 }, /* (253) table_options ::= table_options TTL NK_INTEGER */
  {  421,   -5 }, /* (254) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  421,   -3 }, /* (255) table_options ::= table_options DELETE_MARK duration_list */
  {  426,   -1 }, /* (256) alter_table_options ::= alter_table_option */
  {  426,   -2 }, /* (257) alter_table_options ::= alter_table_options alter_table_option */
  {  444,   -2 }, /* (258) alter_table_option ::= COMMENT NK_STRING */
  {  444,   -2 }, /* (259) alter_table_option ::= TTL NK_INTEGER */
  {  442,   -1 }, /* (260) duration_list ::= duration_literal */
  {  442,   -3 }, /* (261) duration_list ::= duration_list NK_COMMA duration_literal */
  {  443,   -1 }, /* (262) rollup_func_list ::= rollup_func_name */
  {  443,   -3 }, /* (263) rollup_func_list ::= rollup_func_list NK_COMMA rollup_func_name */
  {  446,   -1 }, /* (264) rollup_func_name ::= function_name */
  {  446,   -1 }, /* (265) rollup_func_name ::= FIRST */
  {  446,   -1 }, /* (266) rollup_func_name ::= LAST */
  {  437,   -1 }, /* (267) col_name_list ::= col_name */
  {  437,   -3 }, /* (268) col_name_list ::= col_name_list NK_COMMA col_name */
  {  448,   -1 }, /* (269) col_name ::= column_name */
  {  381,   -2 }, /* (270) cmd ::= SHOW DNODES */
  {  381,   -2 }, /* (271) cmd ::= SHOW USERS */
  {  381,   -3 }, /* (272) cmd ::= SHOW USERS FULL */
  {  381,   -3 }, /* (273) cmd ::= SHOW USER PRIVILEGES */
  {  381,   -3 }, /* (274) cmd ::= SHOW db_kind_opt DATABASES */
  {  381,   -4 }, /* (275) cmd ::= SHOW table_kind_db_name_cond_opt TABLES like_pattern_opt */
  {  381,   -4 }, /* (276) cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
  {  381,   -3 }, /* (277) cmd ::= SHOW db_name_cond_opt VGROUPS */
  {  381,   -2 }, /* (278) cmd ::= SHOW MNODES */
  {  381,   -2 }, /* (279) cmd ::= SHOW QNODES */
  {  381,   -2 }, /* (280) cmd ::= SHOW ARBGROUPS */
  {  381,   -2 }, /* (281) cmd ::= SHOW FUNCTIONS */
  {  381,   -5 }, /* (282) cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
  {  381,   -6 }, /* (283) cmd ::= SHOW INDEXES FROM db_name NK_DOT table_name */
  {  381,   -2 }, /* (284) cmd ::= SHOW STREAMS */
  {  381,   -2 }, /* (285) cmd ::= SHOW ACCOUNTS */
  {  381,   -2 }, /* (286) cmd ::= SHOW APPS */
  {  381,   -2 }, /* (287) cmd ::= SHOW CONNECTIONS */
  {  381,   -2 }, /* (288) cmd ::= SHOW LICENCES */
  {  381,   -2 }, /* (289) cmd ::= SHOW GRANTS */
  {  381,   -3 }, /* (290) cmd ::= SHOW GRANTS FULL */
  {  381,   -3 }, /* (291) cmd ::= SHOW GRANTS LOGS */
  {  381,   -3 }, /* (292) cmd ::= SHOW CLUSTER MACHINES */
  {  381,   -4 }, /* (293) cmd ::= SHOW CREATE DATABASE db_name */
  {  381,   -4 }, /* (294) cmd ::= SHOW CREATE TABLE full_table_name */
  {  381,   -4 }, /* (295) cmd ::= SHOW CREATE STABLE full_table_name */
  {  381,   -2 }, /* (296) cmd ::= SHOW ENCRYPTIONS */
  {  381,   -2 }, /* (297) cmd ::= SHOW QUERIES */
  {  381,   -2 }, /* (298) cmd ::= SHOW SCORES */
  {  381,   -2 }, /* (299) cmd ::= SHOW TOPICS */
  {  381,   -2 }, /* (300) cmd ::= SHOW VARIABLES */
  {  381,   -3 }, /* (301) cmd ::= SHOW CLUSTER VARIABLES */
  {  381,   -3 }, /* (302) cmd ::= SHOW LOCAL VARIABLES */
  {  381,   -5 }, /* (303) cmd ::= SHOW DNODE NK_INTEGER VARIABLES like_pattern_opt */
  {  381,   -2 }, /* (304) cmd ::= SHOW BNODES */
  {  381,   -2 }, /* (305) cmd ::= SHOW SNODES */
  {  381,   -2 }, /* (306) cmd ::= SHOW CLUSTER */
  {  381,   -2 }, /* (307) cmd ::= SHOW TRANSACTIONS */
  {  381,   -4 }, /* (308) cmd ::= SHOW TABLE DISTRIBUTED full_table_name */
  {  381,   -2 }, /* (309) cmd ::= SHOW CONSUMERS */
  {  381,   -2 }, /* (310) cmd ::= SHOW SUBSCRIPTIONS */
  {  381,   -5 }, /* (311) cmd ::= SHOW TAGS FROM table_name_cond from_db_opt */
  {  381,   -6 }, /* (312) cmd ::= SHOW TAGS FROM db_name NK_DOT table_name */
  {  381,   -7 }, /* (313) cmd ::= SHOW TABLE TAGS tag_list_opt FROM table_name_cond from_db_opt */
  {  381,   -8 }, /* (314) cmd ::= SHOW TABLE TAGS tag_list_opt FROM db_name NK_DOT table_name */
  {  381,   -5 }, /* (315) cmd ::= SHOW VNODES ON DNODE NK_INTEGER */
  {  381,   -2 }, /* (316) cmd ::= SHOW VNODES */
  {  381,   -3 }, /* (317) cmd ::= SHOW db_name_cond_opt ALIVE */
  {  381,   -3 }, /* (318) cmd ::= SHOW CLUSTER ALIVE */
  {  381,   -4 }, /* (319) cmd ::= SHOW db_name_cond_opt VIEWS like_pattern_opt */
  {  381,   -4 }, /* (320) cmd ::= SHOW CREATE VIEW full_table_name */
  {  381,   -2 }, /* (321) cmd ::= SHOW COMPACTS */
  {  381,   -3 }, /* (322) cmd ::= SHOW COMPACT NK_INTEGER */
  {  450,    0 }, /* (323) table_kind_db_name_cond_opt ::= */
  {  450,   -1 }, /* (324) table_kind_db_name_cond_opt ::= table_kind */
  {  450,   -2 }, /* (325) table_kind_db_name_cond_opt ::= db_name NK_DOT */
  {  450,   -3 }, /* (326) table_kind_db_name_cond_opt ::= table_kind db_name NK_DOT */
  {  455,   -1 }, /* (327) table_kind ::= NORMAL */
  {  455,   -1 }, /* (328) table_kind ::= CHILD */
  {  452,    0 }, /* (329) db_name_cond_opt ::= */
  {  452,   -2 }, /* (330) db_name_cond_opt ::= db_name NK_DOT */
  {  451,    0 }, /* (331) like_pattern_opt ::= */
  {  451,   -2 }, /* (332) like_pattern_opt ::= LIKE NK_STRING */
  {  453,   -1 }, /* (333) table_name_cond ::= table_name */
  {  454,    0 }, /* (334) from_db_opt ::= */
  {  454,   -2 }, /* (335) from_db_opt ::= FROM db_name */
  {  435,    0 }, /* (336) tag_list_opt ::= */
  {  435,   -1 }, /* (337) tag_list_opt ::= tag_item */
  {  435,   -3 }, /* (338) tag_list_opt ::= tag_list_opt NK_COMMA tag_item */
  {  456,   -1 }, /* (339) tag_item ::= TBNAME */
  {  456,   -1 }, /* (340) tag_item ::= QTAGS */
  {  456,   -1 }, /* (341) tag_item ::= column_name */
  {  456,   -2 }, /* (342) tag_item ::= column_name column_alias */
  {  456,   -3 }, /* (343) tag_item ::= column_name AS column_alias */
  {  449,    0 }, /* (344) db_kind_opt ::= */
  {  449,   -1 }, /* (345) db_kind_opt ::= USER */
  {  449,   -1 }, /* (346) db_kind_opt ::= SYSTEM */
  {  381,  -11 }, /* (347) cmd ::= CREATE TSMA not_exists_opt tsma_name ON full_table_name tsma_func_list INTERVAL NK_LP duration_literal NK_RP */
  {  381,  -11 }, /* (348) cmd ::= CREATE RECURSIVE TSMA not_exists_opt tsma_name ON full_table_name INTERVAL NK_LP duration_literal NK_RP */
  {  381,   -4 }, /* (349) cmd ::= DROP TSMA exists_opt full_tsma_name */
  {  381,   -3 }, /* (350) cmd ::= SHOW db_name_cond_opt TSMAS */
  {  460,   -1 }, /* (351) full_tsma_name ::= tsma_name */
  {  460,   -3 }, /* (352) full_tsma_name ::= db_name NK_DOT tsma_name */
  {  459,   -4 }, /* (353) tsma_func_list ::= FUNCTION NK_LP func_list NK_RP */
  {  381,   -8 }, /* (354) cmd ::= CREATE SMA INDEX not_exists_opt col_name ON full_table_name index_options */
  {  381,   -9 }, /* (355) cmd ::= CREATE INDEX not_exists_opt col_name ON full_table_name NK_LP col_name_list NK_RP */
  {  381,   -4 }, /* (356) cmd ::= DROP INDEX exists_opt full_index_name */
  {  463,   -1 }, /* (357) full_index_name ::= index_name */
  {  463,   -3 }, /* (358) full_index_name ::= db_name NK_DOT index_name */
  {  462,  -10 }, /* (359) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt sma_stream_opt */
  {  462,  -12 }, /* (360) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt sma_stream_opt */
  {  461,   -1 }, /* (361) func_list ::= func */
  {  461,   -3 }, /* (362) func_list ::= func_list NK_COMMA func */
  {  467,   -4 }, /* (363) func ::= sma_func_name NK_LP expression_list NK_RP */
  {  468,   -1 }, /* (364) sma_func_name ::= function_name */
  {  468,   -1 }, /* (365) sma_func_name ::= COUNT */
  {  468,   -1 }, /* (366) sma_func_name ::= FIRST */
  {  468,   -1 }, /* (367) sma_func_name ::= LAST */
  {  468,   -1 }, /* (368) sma_func_name ::= LAST_ROW */
  {  466,    0 }, /* (369) sma_stream_opt ::= */
  {  466,   -3 }, /* (370) sma_stream_opt ::= sma_stream_opt WATERMARK duration_literal */
  {  466,   -3 }, /* (371) sma_stream_opt ::= sma_stream_opt MAX_DELAY duration_literal */
  {  466,   -3 }, /* (372) sma_stream_opt ::= sma_stream_opt DELETE_MARK duration_literal */
  {  470,   -1 }, /* (373) with_meta ::= AS */
  {  470,   -3 }, /* (374) with_meta ::= WITH META AS */
  {  470,   -3 }, /* (375) with_meta ::= ONLY META AS */
  {  381,   -6 }, /* (376) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_or_subquery */
  {  381,   -7 }, /* (377) cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta DATABASE db_name */
  {  381,   -8 }, /* (378) cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta STABLE full_table_name where_clause_opt */
  {  381,   -4 }, /* (379) cmd ::= DROP TOPIC exists_opt topic_name */
  {  381,   -7 }, /* (380) cmd ::= DROP CONSUMER GROUP exists_opt cgroup_name ON topic_name */
  {  381,   -2 }, /* (381) cmd ::= DESC full_table_name */
  {  381,   -2 }, /* (382) cmd ::= DESCRIBE full_table_name */
  {  381,   -3 }, /* (383) cmd ::= RESET QUERY CACHE */
  {  381,   -4 }, /* (384) cmd ::= EXPLAIN analyze_opt explain_options query_or_subquery */
  {  381,   -4 }, /* (385) cmd ::= EXPLAIN analyze_opt explain_options insert_query */
  {  474,    0 }, /* (386) analyze_opt ::= */
  {  474,   -1 }, /* (387) analyze_opt ::= ANALYZE */
  {  475,    0 }, /* (388) explain_options ::= */
  {  475,   -3 }, /* (389) explain_options ::= explain_options VERBOSE NK_BOOL */
  {  475,   -3 }, /* (390) explain_options ::= explain_options RATIO NK_FLOAT */
  {  381,  -12 }, /* (391) cmd ::= CREATE or_replace_opt agg_func_opt FUNCTION not_exists_opt function_name AS NK_STRING OUTPUTTYPE type_name bufsize_opt language_opt */
  {  381,   -4 }, /* (392) cmd ::= DROP FUNCTION exists_opt function_name */
  {  478,    0 }, /* (393) agg_func_opt ::= */
  {  478,   -1 }, /* (394) agg_func_opt ::= AGGREGATE */
  {  479,    0 }, /* (395) bufsize_opt ::= */
  {  479,   -2 }, /* (396) bufsize_opt ::= BUFSIZE NK_INTEGER */
  {  480,    0 }, /* (397) language_opt ::= */
  {  480,   -2 }, /* (398) language_opt ::= LANGUAGE NK_STRING */
  {  477,    0 }, /* (399) or_replace_opt ::= */
  {  477,   -2 }, /* (400) or_replace_opt ::= OR REPLACE */
  {  381,   -6 }, /* (401) cmd ::= CREATE or_replace_opt VIEW full_view_name AS query_or_subquery */
  {  381,   -4 }, /* (402) cmd ::= DROP VIEW exists_opt full_view_name */
  {  481,   -1 }, /* (403) full_view_name ::= view_name */
  {  481,   -3 }, /* (404) full_view_name ::= db_name NK_DOT view_name */
  {  381,  -12 }, /* (405) cmd ::= CREATE STREAM not_exists_opt stream_name stream_options INTO full_table_name col_list_opt tag_def_or_ref_opt subtable_opt AS query_or_subquery */
  {  381,   -4 }, /* (406) cmd ::= DROP STREAM exists_opt stream_name */
  {  381,   -4 }, /* (407) cmd ::= PAUSE STREAM exists_opt stream_name */
  {  381,   -5 }, /* (408) cmd ::= RESUME STREAM exists_opt ignore_opt stream_name */
  {  485,    0 }, /* (409) col_list_opt ::= */
  {  485,   -3 }, /* (410) col_list_opt ::= NK_LP column_stream_def_list NK_RP */
  {  489,   -1 }, /* (411) column_stream_def_list ::= column_stream_def */
  {  489,   -3 }, /* (412) column_stream_def_list ::= column_stream_def_list NK_COMMA column_stream_def */
  {  490,   -2 }, /* (413) column_stream_def ::= column_name stream_col_options */
  {  491,    0 }, /* (414) stream_col_options ::= */
  {  491,   -3 }, /* (415) stream_col_options ::= stream_col_options PRIMARY KEY */
  {  486,    0 }, /* (416) tag_def_or_ref_opt ::= */
  {  486,   -1 }, /* (417) tag_def_or_ref_opt ::= tags_def */
  {  486,   -4 }, /* (418) tag_def_or_ref_opt ::= TAGS NK_LP column_stream_def_list NK_RP */
  {  484,    0 }, /* (419) stream_options ::= */
  {  484,   -3 }, /* (420) stream_options ::= stream_options TRIGGER AT_ONCE */
  {  484,   -3 }, /* (421) stream_options ::= stream_options TRIGGER WINDOW_CLOSE */
  {  484,   -4 }, /* (422) stream_options ::= stream_options TRIGGER MAX_DELAY duration_literal */
  {  484,   -3 }, /* (423) stream_options ::= stream_options WATERMARK duration_literal */
  {  484,   -4 }, /* (424) stream_options ::= stream_options IGNORE EXPIRED NK_INTEGER */
  {  484,   -3 }, /* (425) stream_options ::= stream_options FILL_HISTORY NK_INTEGER */
  {  484,   -3 }, /* (426) stream_options ::= stream_options DELETE_MARK duration_literal */
  {  484,   -4 }, /* (427) stream_options ::= stream_options IGNORE UPDATE NK_INTEGER */
  {  487,    0 }, /* (428) subtable_opt ::= */
  {  487,   -4 }, /* (429) subtable_opt ::= SUBTABLE NK_LP expression NK_RP */
  {  488,    0 }, /* (430) ignore_opt ::= */
  {  488,   -2 }, /* (431) ignore_opt ::= IGNORE UNTREATED */
  {  381,   -3 }, /* (432) cmd ::= KILL CONNECTION NK_INTEGER */
  {  381,   -3 }, /* (433) cmd ::= KILL QUERY NK_STRING */
  {  381,   -3 }, /* (434) cmd ::= KILL TRANSACTION NK_INTEGER */
  {  381,   -3 }, /* (435) cmd ::= KILL COMPACT NK_INTEGER */
  {  381,   -2 }, /* (436) cmd ::= BALANCE VGROUP */
  {  381,   -4 }, /* (437) cmd ::= BALANCE VGROUP LEADER on_vgroup_id */
  {  381,   -5 }, /* (438) cmd ::= BALANCE VGROUP LEADER DATABASE db_name */
  {  381,   -4 }, /* (439) cmd ::= MERGE VGROUP NK_INTEGER NK_INTEGER */
  {  381,   -4 }, /* (440) cmd ::= REDISTRIBUTE VGROUP NK_INTEGER dnode_list */
  {  381,   -3 }, /* (441) cmd ::= SPLIT VGROUP NK_INTEGER */
  {  493,    0 }, /* (442) on_vgroup_id ::= */
  {  493,   -2 }, /* (443) on_vgroup_id ::= ON NK_INTEGER */
  {  494,   -2 }, /* (444) dnode_list ::= DNODE NK_INTEGER */
  {  494,   -3 }, /* (445) dnode_list ::= dnode_list DNODE NK_INTEGER */
  {  381,   -4 }, /* (446) cmd ::= DELETE FROM full_table_name where_clause_opt */
  {  381,   -1 }, /* (447) cmd ::= query_or_subquery */
  {  381,   -1 }, /* (448) cmd ::= insert_query */
  {  476,   -7 }, /* (449) insert_query ::= INSERT INTO full_table_name NK_LP col_name_list NK_RP query_or_subquery */
  {  476,   -4 }, /* (450) insert_query ::= INSERT INTO full_table_name query_or_subquery */
  {  430,   -1 }, /* (451) tags_literal ::= NK_INTEGER */
  {  430,   -3 }, /* (452) tags_literal ::= NK_INTEGER NK_PLUS duration_literal */
  {  430,   -3 }, /* (453) tags_literal ::= NK_INTEGER NK_MINUS duration_literal */
  {  430,   -2 }, /* (454) tags_literal ::= NK_PLUS NK_INTEGER */
  {  430,   -4 }, /* (455) tags_literal ::= NK_PLUS NK_INTEGER NK_PLUS duration_literal */
  {  430,   -4 }, /* (456) tags_literal ::= NK_PLUS NK_INTEGER NK_MINUS duration_literal */
  {  430,   -2 }, /* (457) tags_literal ::= NK_MINUS NK_INTEGER */
  {  430,   -4 }, /* (458) tags_literal ::= NK_MINUS NK_INTEGER NK_PLUS duration_literal */
  {  430,   -4 }, /* (459) tags_literal ::= NK_MINUS NK_INTEGER NK_MINUS duration_literal */
  {  430,   -1 }, /* (460) tags_literal ::= NK_FLOAT */
  {  430,   -2 }, /* (461) tags_literal ::= NK_PLUS NK_FLOAT */
  {  430,   -2 }, /* (462) tags_literal ::= NK_MINUS NK_FLOAT */
  {  430,   -1 }, /* (463) tags_literal ::= NK_BIN */
  {  430,   -3 }, /* (464) tags_literal ::= NK_BIN NK_PLUS duration_literal */
  {  430,   -3 }, /* (465) tags_literal ::= NK_BIN NK_MINUS duration_literal */
  {  430,   -2 }, /* (466) tags_literal ::= NK_PLUS NK_BIN */
  {  430,   -4 }, /* (467) tags_literal ::= NK_PLUS NK_BIN NK_PLUS duration_literal */
  {  430,   -4 }, /* (468) tags_literal ::= NK_PLUS NK_BIN NK_MINUS duration_literal */
  {  430,   -2 }, /* (469) tags_literal ::= NK_MINUS NK_BIN */
  {  430,   -4 }, /* (470) tags_literal ::= NK_MINUS NK_BIN NK_PLUS duration_literal */
  {  430,   -4 }, /* (471) tags_literal ::= NK_MINUS NK_BIN NK_MINUS duration_literal */
  {  430,   -1 }, /* (472) tags_literal ::= NK_HEX */
  {  430,   -3 }, /* (473) tags_literal ::= NK_HEX NK_PLUS duration_literal */
  {  430,   -3 }, /* (474) tags_literal ::= NK_HEX NK_MINUS duration_literal */
  {  430,   -2 }, /* (475) tags_literal ::= NK_PLUS NK_HEX */
  {  430,   -4 }, /* (476) tags_literal ::= NK_PLUS NK_HEX NK_PLUS duration_literal */
  {  430,   -4 }, /* (477) tags_literal ::= NK_PLUS NK_HEX NK_MINUS duration_literal */
  {  430,   -2 }, /* (478) tags_literal ::= NK_MINUS NK_HEX */
  {  430,   -4 }, /* (479) tags_literal ::= NK_MINUS NK_HEX NK_PLUS duration_literal */
  {  430,   -4 }, /* (480) tags_literal ::= NK_MINUS NK_HEX NK_MINUS duration_literal */
  {  430,   -1 }, /* (481) tags_literal ::= NK_STRING */
  {  430,   -3 }, /* (482) tags_literal ::= NK_STRING NK_PLUS duration_literal */
  {  430,   -3 }, /* (483) tags_literal ::= NK_STRING NK_MINUS duration_literal */
  {  430,   -1 }, /* (484) tags_literal ::= NK_BOOL */
  {  430,   -1 }, /* (485) tags_literal ::= NULL */
  {  430,   -1 }, /* (486) tags_literal ::= literal_func */
  {  430,   -3 }, /* (487) tags_literal ::= literal_func NK_PLUS duration_literal */
  {  430,   -3 }, /* (488) tags_literal ::= literal_func NK_MINUS duration_literal */
  {  434,   -1 }, /* (489) tags_literal_list ::= tags_literal */
  {  434,   -3 }, /* (490) tags_literal_list ::= tags_literal_list NK_COMMA tags_literal */
  {  384,   -1 }, /* (491) literal ::= NK_INTEGER */
  {  384,   -1 }, /* (492) literal ::= NK_FLOAT */
  {  384,   -1 }, /* (493) literal ::= NK_STRING */
  {  384,   -1 }, /* (494) literal ::= NK_BOOL */
  {  384,   -2 }, /* (495) literal ::= TIMESTAMP NK_STRING */
  {  384,   -1 }, /* (496) literal ::= duration_literal */
  {  384,   -1 }, /* (497) literal ::= NULL */
  {  384,   -1 }, /* (498) literal ::= NK_QUESTION */
  {  445,   -1 }, /* (499) duration_literal ::= NK_VARIABLE */
  {  415,   -1 }, /* (500) signed ::= NK_INTEGER */
  {  415,   -2 }, /* (501) signed ::= NK_PLUS NK_INTEGER */
  {  415,   -2 }, /* (502) signed ::= NK_MINUS NK_INTEGER */
  {  415,   -1 }, /* (503) signed ::= NK_FLOAT */
  {  415,   -2 }, /* (504) signed ::= NK_PLUS NK_FLOAT */
  {  415,   -2 }, /* (505) signed ::= NK_MINUS NK_FLOAT */
  {  496,   -1 }, /* (506) signed_literal ::= signed */
  {  496,   -1 }, /* (507) signed_literal ::= NK_STRING */
  {  496,   -1 }, /* (508) signed_literal ::= NK_BOOL */
  {  496,   -2 }, /* (509) signed_literal ::= TIMESTAMP NK_STRING */
  {  496,   -1 }, /* (510) signed_literal ::= duration_literal */
  {  496,   -1 }, /* (511) signed_literal ::= NULL */
  {  496,   -1 }, /* (512) signed_literal ::= literal_func */
  {  496,   -1 }, /* (513) signed_literal ::= NK_QUESTION */
  {  497,   -1 }, /* (514) literal_list ::= signed_literal */
  {  497,   -3 }, /* (515) literal_list ::= literal_list NK_COMMA signed_literal */
  {  398,   -1 }, /* (516) db_name ::= NK_ID */
  {  399,   -1 }, /* (517) table_name ::= NK_ID */
  {  427,   -1 }, /* (518) column_name ::= NK_ID */
  {  447,   -1 }, /* (519) function_name ::= NK_ID */
  {  482,   -1 }, /* (520) view_name ::= NK_ID */
  {  498,   -1 }, /* (521) table_alias ::= NK_ID */
  {  457,   -1 }, /* (522) column_alias ::= NK_ID */
  {  457,   -1 }, /* (523) column_alias ::= NK_ALIAS */
  {  391,   -1 }, /* (524) user_name ::= NK_ID */
  {  400,   -1 }, /* (525) topic_name ::= NK_ID */
  {  483,   -1 }, /* (526) stream_name ::= NK_ID */
  {  473,   -1 }, /* (527) cgroup_name ::= NK_ID */
  {  464,   -1 }, /* (528) index_name ::= NK_ID */
  {  458,   -1 }, /* (529) tsma_name ::= NK_ID */
  {  499,   -1 }, /* (530) expr_or_subquery ::= expression */
  {  492,   -1 }, /* (531) expression ::= literal */
  {  492,   -1 }, /* (532) expression ::= pseudo_column */
  {  492,   -1 }, /* (533) expression ::= column_reference */
  {  492,   -1 }, /* (534) expression ::= function_expression */
  {  492,   -1 }, /* (535) expression ::= case_when_expression */
  {  492,   -3 }, /* (536) expression ::= NK_LP expression NK_RP */
  {  492,   -2 }, /* (537) expression ::= NK_PLUS expr_or_subquery */
  {  492,   -2 }, /* (538) expression ::= NK_MINUS expr_or_subquery */
  {  492,   -3 }, /* (539) expression ::= expr_or_subquery NK_PLUS expr_or_subquery */
  {  492,   -3 }, /* (540) expression ::= expr_or_subquery NK_MINUS expr_or_subquery */
  {  492,   -3 }, /* (541) expression ::= expr_or_subquery NK_STAR expr_or_subquery */
  {  492,   -3 }, /* (542) expression ::= expr_or_subquery NK_SLASH expr_or_subquery */
  {  492,   -3 }, /* (543) expression ::= expr_or_subquery NK_REM expr_or_subquery */
  {  492,   -3 }, /* (544) expression ::= column_reference NK_ARROW NK_STRING */
  {  492,   -3 }, /* (545) expression ::= expr_or_subquery NK_BITAND expr_or_subquery */
  {  492,   -3 }, /* (546) expression ::= expr_or_subquery NK_BITOR expr_or_subquery */
  {  469,   -1 }, /* (547) expression_list ::= expr_or_subquery */
  {  469,   -3 }, /* (548) expression_list ::= expression_list NK_COMMA expr_or_subquery */
  {  501,   -1 }, /* (549) column_reference ::= column_name */
  {  501,   -3 }, /* (550) column_reference ::= table_name NK_DOT column_name */
  {  501,   -1 }, /* (551) column_reference ::= NK_ALIAS */
  {  501,   -3 }, /* (552) column_reference ::= table_name NK_DOT NK_ALIAS */
  {  500,   -1 }, /* (553) pseudo_column ::= ROWTS */
  {  500,   -1 }, /* (554) pseudo_column ::= TBNAME */
  {  500,   -3 }, /* (555) pseudo_column ::= table_name NK_DOT TBNAME */
  {  500,   -1 }, /* (556) pseudo_column ::= QSTART */
  {  500,   -1 }, /* (557) pseudo_column ::= QEND */
  {  500,   -1 }, /* (558) pseudo_column ::= QDURATION */
  {  500,   -1 }, /* (559) pseudo_column ::= WSTART */
  {  500,   -1 }, /* (560) pseudo_column ::= WEND */
  {  500,   -1 }, /* (561) pseudo_column ::= WDURATION */
  {  500,   -1 }, /* (562) pseudo_column ::= IROWTS */
  {  500,   -1 }, /* (563) pseudo_column ::= ISFILLED */
  {  500,   -1 }, /* (564) pseudo_column ::= QTAGS */
  {  500,   -1 }, /* (565) pseudo_column ::= FLOW */
  {  500,   -1 }, /* (566) pseudo_column ::= FHIGH */
  {  500,   -1 }, /* (567) pseudo_column ::= FEXPR */
  {  502,   -4 }, /* (568) function_expression ::= function_name NK_LP expression_list NK_RP */
  {  502,   -4 }, /* (569) function_expression ::= star_func NK_LP star_func_para_list NK_RP */
  {  502,   -6 }, /* (570) function_expression ::= CAST NK_LP expr_or_subquery AS type_name NK_RP */
  {  502,   -6 }, /* (571) function_expression ::= CAST NK_LP expr_or_subquery AS type_name_default_len NK_RP */
  {  502,   -1 }, /* (572) function_expression ::= literal_func */
  {  495,   -3 }, /* (573) literal_func ::= noarg_func NK_LP NK_RP */
  {  495,   -1 }, /* (574) literal_func ::= NOW */
  {  495,   -1 }, /* (575) literal_func ::= TODAY */
  {  506,   -1 }, /* (576) noarg_func ::= NOW */
  {  506,   -1 }, /* (577) noarg_func ::= TODAY */
  {  506,   -1 }, /* (578) noarg_func ::= TIMEZONE */
  {  506,   -1 }, /* (579) noarg_func ::= DATABASE */
  {  506,   -1 }, /* (580) noarg_func ::= CLIENT_VERSION */
  {  506,   -1 }, /* (581) noarg_func ::= SERVER_VERSION */
  {  506,   -1 }, /* (582) noarg_func ::= SERVER_STATUS */
  {  506,   -1 }, /* (583) noarg_func ::= CURRENT_USER */
  {  506,   -1 }, /* (584) noarg_func ::= USER */
  {  504,   -1 }, /* (585) star_func ::= COUNT */
  {  504,   -1 }, /* (586) star_func ::= FIRST */
  {  504,   -1 }, /* (587) star_func ::= LAST */
  {  504,   -1 }, /* (588) star_func ::= LAST_ROW */
  {  505,   -1 }, /* (589) star_func_para_list ::= NK_STAR */
  {  505,   -1 }, /* (590) star_func_para_list ::= other_para_list */
  {  507,   -1 }, /* (591) other_para_list ::= star_func_para */
  {  507,   -3 }, /* (592) other_para_list ::= other_para_list NK_COMMA star_func_para */
  {  508,   -1 }, /* (593) star_func_para ::= expr_or_subquery */
  {  508,   -3 }, /* (594) star_func_para ::= table_name NK_DOT NK_STAR */
  {  503,   -4 }, /* (595) case_when_expression ::= CASE when_then_list case_when_else_opt END */
  {  503,   -5 }, /* (596) case_when_expression ::= CASE common_expression when_then_list case_when_else_opt END */
  {  509,   -1 }, /* (597) when_then_list ::= when_then_expr */
  {  509,   -2 }, /* (598) when_then_list ::= when_then_list when_then_expr */
  {  512,   -4 }, /* (599) when_then_expr ::= WHEN common_expression THEN common_expression */
  {  510,    0 }, /* (600) case_when_else_opt ::= */
  {  510,   -2 }, /* (601) case_when_else_opt ::= ELSE common_expression */
  {  513,   -3 }, /* (602) predicate ::= expr_or_subquery compare_op expr_or_subquery */
  {  513,   -5 }, /* (603) predicate ::= expr_or_subquery BETWEEN expr_or_subquery AND expr_or_subquery */
  {  513,   -6 }, /* (604) predicate ::= expr_or_subquery NOT BETWEEN expr_or_subquery AND expr_or_subquery */
  {  513,   -3 }, /* (605) predicate ::= expr_or_subquery IS NULL */
  {  513,   -4 }, /* (606) predicate ::= expr_or_subquery IS NOT NULL */
  {  513,   -3 }, /* (607) predicate ::= expr_or_subquery in_op in_predicate_value */
  {  514,   -1 }, /* (608) compare_op ::= NK_LT */
  {  514,   -1 }, /* (609) compare_op ::= NK_GT */
  {  514,   -1 }, /* (610) compare_op ::= NK_LE */
  {  514,   -1 }, /* (611) compare_op ::= NK_GE */
  {  514,   -1 }, /* (612) compare_op ::= NK_NE */
  {  514,   -1 }, /* (613) compare_op ::= NK_EQ */
  {  514,   -1 }, /* (614) compare_op ::= LIKE */
  {  514,   -2 }, /* (615) compare_op ::= NOT LIKE */
  {  514,   -1 }, /* (616) compare_op ::= MATCH */
  {  514,   -1 }, /* (617) compare_op ::= NMATCH */
  {  514,   -1 }, /* (618) compare_op ::= CONTAINS */
  {  515,   -1 }, /* (619) in_op ::= IN */
  {  515,   -2 }, /* (620) in_op ::= NOT IN */
  {  516,   -3 }, /* (621) in_predicate_value ::= NK_LP literal_list NK_RP */
  {  517,   -1 }, /* (622) boolean_value_expression ::= boolean_primary */
  {  517,   -2 }, /* (623) boolean_value_expression ::= NOT boolean_primary */
  {  517,   -3 }, /* (624) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  517,   -3 }, /* (625) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  518,   -1 }, /* (626) boolean_primary ::= predicate */
  {  518,   -3 }, /* (627) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  511,   -1 }, /* (628) common_expression ::= expr_or_subquery */
  {  511,   -1 }, /* (629) common_expression ::= boolean_value_expression */
  {  519,    0 }, /* (630) from_clause_opt ::= */
  {  519,   -2 }, /* (631) from_clause_opt ::= FROM table_reference_list */
  {  520,   -1 }, /* (632) table_reference_list ::= table_reference */
  {  520,   -3 }, /* (633) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  521,   -1 }, /* (634) table_reference ::= table_primary */
  {  521,   -1 }, /* (635) table_reference ::= joined_table */
  {  522,   -2 }, /* (636) table_primary ::= table_name alias_opt */
  {  522,   -4 }, /* (637) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  522,   -2 }, /* (638) table_primary ::= subquery alias_opt */
  {  522,   -1 }, /* (639) table_primary ::= parenthesized_joined_table */
  {  524,    0 }, /* (640) alias_opt ::= */
  {  524,   -1 }, /* (641) alias_opt ::= table_alias */
  {  524,   -2 }, /* (642) alias_opt ::= AS table_alias */
  {  526,   -3 }, /* (643) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  526,   -3 }, /* (644) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  523,   -8 }, /* (645) joined_table ::= table_reference join_type join_subtype JOIN table_reference join_on_clause_opt window_offset_clause_opt jlimit_clause_opt */
  {  527,    0 }, /* (646) join_type ::= */
  {  527,   -1 }, /* (647) join_type ::= INNER */
  {  527,   -1 }, /* (648) join_type ::= LEFT */
  {  527,   -1 }, /* (649) join_type ::= RIGHT */
  {  527,   -1 }, /* (650) join_type ::= FULL */
  {  528,    0 }, /* (651) join_subtype ::= */
  {  528,   -1 }, /* (652) join_subtype ::= OUTER */
  {  528,   -1 }, /* (653) join_subtype ::= SEMI */
  {  528,   -1 }, /* (654) join_subtype ::= ANTI */
  {  528,   -1 }, /* (655) join_subtype ::= ASOF */
  {  528,   -1 }, /* (656) join_subtype ::= WINDOW */
  {  529,    0 }, /* (657) join_on_clause_opt ::= */
  {  529,   -2 }, /* (658) join_on_clause_opt ::= ON search_condition */
  {  530,    0 }, /* (659) window_offset_clause_opt ::= */
  {  530,   -6 }, /* (660) window_offset_clause_opt ::= WINDOW_OFFSET NK_LP window_offset_literal NK_COMMA window_offset_literal NK_RP */
  {  532,   -1 }, /* (661) window_offset_literal ::= NK_VARIABLE */
  {  532,   -2 }, /* (662) window_offset_literal ::= NK_MINUS NK_VARIABLE */
  {  531,    0 }, /* (663) jlimit_clause_opt ::= */
  {  531,   -2 }, /* (664) jlimit_clause_opt ::= JLIMIT NK_INTEGER */
  {  533,  -14 }, /* (665) query_specification ::= SELECT hint_list set_quantifier_opt tag_mode_opt select_list from_clause_opt where_clause_opt partition_by_clause_opt range_opt every_opt fill_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  534,    0 }, /* (666) hint_list ::= */
  {  534,   -1 }, /* (667) hint_list ::= NK_HINT */
  {  536,    0 }, /* (668) tag_mode_opt ::= */
  {  536,   -1 }, /* (669) tag_mode_opt ::= TAGS */
  {  535,    0 }, /* (670) set_quantifier_opt ::= */
  {  535,   -1 }, /* (671) set_quantifier_opt ::= DISTINCT */
  {  535,   -1 }, /* (672) set_quantifier_opt ::= ALL */
  {  537,   -1 }, /* (673) select_list ::= select_item */
  {  537,   -3 }, /* (674) select_list ::= select_list NK_COMMA select_item */
  {  545,   -1 }, /* (675) select_item ::= NK_STAR */
  {  545,   -1 }, /* (676) select_item ::= common_expression */
  {  545,   -2 }, /* (677) select_item ::= common_expression column_alias */
  {  545,   -3 }, /* (678) select_item ::= common_expression AS column_alias */
  {  545,   -3 }, /* (679) select_item ::= table_name NK_DOT NK_STAR */
  {  472,    0 }, /* (680) where_clause_opt ::= */
  {  472,   -2 }, /* (681) where_clause_opt ::= WHERE search_condition */
  {  538,    0 }, /* (682) partition_by_clause_opt ::= */
  {  538,   -3 }, /* (683) partition_by_clause_opt ::= PARTITION BY partition_list */
  {  546,   -1 }, /* (684) partition_list ::= partition_item */
  {  546,   -3 }, /* (685) partition_list ::= partition_list NK_COMMA partition_item */
  {  547,   -1 }, /* (686) partition_item ::= expr_or_subquery */
  {  547,   -2 }, /* (687) partition_item ::= expr_or_subquery column_alias */
  {  547,   -3 }, /* (688) partition_item ::= expr_or_subquery AS column_alias */
  {  542,    0 }, /* (689) twindow_clause_opt ::= */
  {  542,   -6 }, /* (690) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA interval_sliding_duration_literal NK_RP */
  {  542,   -4 }, /* (691) twindow_clause_opt ::= STATE_WINDOW NK_LP expr_or_subquery NK_RP */
  {  542,   -6 }, /* (692) twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_RP sliding_opt fill_opt */
  {  542,   -8 }, /* (693) twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal NK_RP sliding_opt fill_opt */
  {  542,   -7 }, /* (694) twindow_clause_opt ::= EVENT_WINDOW START WITH search_condition END WITH search_condition */
  {  542,   -4 }, /* (695) twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_RP */
  {  542,   -6 }, /* (696) twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  465,    0 }, /* (697) sliding_opt ::= */
  {  465,   -4 }, /* (698) sliding_opt ::= SLIDING NK_LP interval_sliding_duration_literal NK_RP */
  {  548,   -1 }, /* (699) interval_sliding_duration_literal ::= NK_VARIABLE */
  {  548,   -1 }, /* (700) interval_sliding_duration_literal ::= NK_STRING */
  {  548,   -1 }, /* (701) interval_sliding_duration_literal ::= NK_INTEGER */
  {  541,    0 }, /* (702) fill_opt ::= */
  {  541,   -4 }, /* (703) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  541,   -6 }, /* (704) fill_opt ::= FILL NK_LP VALUE NK_COMMA expression_list NK_RP */
  {  541,   -6 }, /* (705) fill_opt ::= FILL NK_LP VALUE_F NK_COMMA expression_list NK_RP */
  {  549,   -1 }, /* (706) fill_mode ::= NONE */
  {  549,   -1 }, /* (707) fill_mode ::= PREV */
  {  549,   -1 }, /* (708) fill_mode ::= NULL */
  {  549,   -1 }, /* (709) fill_mode ::= NULL_F */
  {  549,   -1 }, /* (710) fill_mode ::= LINEAR */
  {  549,   -1 }, /* (711) fill_mode ::= NEXT */
  {  543,    0 }, /* (712) group_by_clause_opt ::= */
  {  543,   -3 }, /* (713) group_by_clause_opt ::= GROUP BY group_by_list */
  {  550,   -1 }, /* (714) group_by_list ::= expr_or_subquery */
  {  550,   -3 }, /* (715) group_by_list ::= group_by_list NK_COMMA expr_or_subquery */
  {  544,    0 }, /* (716) having_clause_opt ::= */
  {  544,   -2 }, /* (717) having_clause_opt ::= HAVING search_condition */
  {  539,    0 }, /* (718) range_opt ::= */
  {  539,   -6 }, /* (719) range_opt ::= RANGE NK_LP expr_or_subquery NK_COMMA expr_or_subquery NK_RP */
  {  539,   -4 }, /* (720) range_opt ::= RANGE NK_LP expr_or_subquery NK_RP */
  {  540,    0 }, /* (721) every_opt ::= */
  {  540,   -4 }, /* (722) every_opt ::= EVERY NK_LP duration_literal NK_RP */
  {  551,   -4 }, /* (723) query_expression ::= query_simple order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  552,   -1 }, /* (724) query_simple ::= query_specification */
  {  552,   -1 }, /* (725) query_simple ::= union_query_expression */
  {  556,   -4 }, /* (726) union_query_expression ::= query_simple_or_subquery UNION ALL query_simple_or_subquery */
  {  556,   -3 }, /* (727) union_query_expression ::= query_simple_or_subquery UNION query_simple_or_subquery */
  {  557,   -1 }, /* (728) query_simple_or_subquery ::= query_simple */
  {  557,   -1 }, /* (729) query_simple_or_subquery ::= subquery */
  {  471,   -1 }, /* (730) query_or_subquery ::= query_expression */
  {  471,   -1 }, /* (731) query_or_subquery ::= subquery */
  {  553,    0 }, /* (732) order_by_clause_opt ::= */
  {  553,   -3 }, /* (733) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  554,    0 }, /* (734) slimit_clause_opt ::= */
  {  554,   -2 }, /* (735) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  554,   -4 }, /* (736) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  554,   -4 }, /* (737) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  555,    0 }, /* (738) limit_clause_opt ::= */
  {  555,   -2 }, /* (739) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  555,   -4 }, /* (740) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  555,   -4 }, /* (741) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  525,   -3 }, /* (742) subquery ::= NK_LP query_expression NK_RP */
  {  525,   -3 }, /* (743) subquery ::= NK_LP subquery NK_RP */
  {  401,   -1 }, /* (744) search_condition ::= common_expression */
  {  558,   -1 }, /* (745) sort_specification_list ::= sort_specification */
  {  558,   -3 }, /* (746) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  559,   -3 }, /* (747) sort_specification ::= expr_or_subquery ordering_specification_opt null_ordering_opt */
  {  560,    0 }, /* (748) ordering_specification_opt ::= */
  {  560,   -1 }, /* (749) ordering_specification_opt ::= ASC */
  {  560,   -1 }, /* (750) ordering_specification_opt ::= DESC */
  {  561,    0 }, /* (751) null_ordering_opt ::= */
  {  561,   -2 }, /* (752) null_ordering_opt ::= NULLS FIRST */
  {  561,   -2 }, /* (753) null_ordering_opt ::= NULLS LAST */
  {  429,    0 }, /* (754) column_options ::= */
  {  429,   -3 }, /* (755) column_options ::= column_options PRIMARY KEY */
  {  429,   -3 }, /* (756) column_options ::= column_options ENCODE NK_STRING */
  {  429,   -3 }, /* (757) column_options ::= column_options COMPRESS NK_STRING */
  {  429,   -3 }, /* (758) column_options ::= column_options LEVEL NK_STRING */
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
**
** The yyLookahead and yyLookaheadToken parameters provide reduce actions
** access to the lookahead token (if any).  The yyLookahead will be YYNOCODE
** if the lookahead token has already been consumed.  As this procedure is
** only called from one place, optimizing compilers will in-line it, which
** means that the extra parameters have no performance impact.
*/
static YYACTIONTYPE yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  ParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  ParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfo[yyruleno].nrhs;
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s], go to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno], yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s].\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno]);
    }
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfo[yyruleno].nrhs==0 ){
#ifdef YYTRACKMAXSTACKDEPTH
    if( (int)(yypParser->yytos - yypParser->yystack)>yypParser->yyhwm ){
      yypParser->yyhwm++;
      assert( yypParser->yyhwm == (int)(yypParser->yytos - yypParser->yystack));
    }
#endif
#if YYSTACKDEPTH>0 
    if( yypParser->yytos>=yypParser->yystackEnd ){
      yyStackOverflow(yypParser);
      /* The call to yyStackOverflow() above pops the stack until it is
      ** empty, causing the main parser loop to exit.  So the return value
      ** is never used and does not matter. */
      return 0;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        /* The call to yyStackOverflow() above pops the stack until it is
        ** empty, causing the main parser loop to exit.  So the return value
        ** is never used and does not matter. */
        return 0;
      }
      yymsp = yypParser->yytos;
    }
#endif
  }

  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
/********** Begin reduce actions **********************************************/
        YYMINORTYPE yylhsminor;
      case 0: /* cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
#line 50 "source/libs/parser/inc/sql.y"
{ pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
#line 4874 "./source/libs/parser/src/sql.c"
  yy_destructor(yypParser,382,&yymsp[0].minor);
        break;
      case 1: /* cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
#line 51 "source/libs/parser/inc/sql.y"
{ pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
#line 4880 "./source/libs/parser/src/sql.c"
  yy_destructor(yypParser,383,&yymsp[0].minor);
        break;
      case 2: /* account_options ::= */
#line 55 "source/libs/parser/inc/sql.y"
{ }
#line 4886 "./source/libs/parser/src/sql.c"
        break;
      case 3: /* account_options ::= account_options PPS literal */
      case 4: /* account_options ::= account_options TSERIES literal */ yytestcase(yyruleno==4);
      case 5: /* account_options ::= account_options STORAGE literal */ yytestcase(yyruleno==5);
      case 6: /* account_options ::= account_options STREAMS literal */ yytestcase(yyruleno==6);
      case 7: /* account_options ::= account_options QTIME literal */ yytestcase(yyruleno==7);
      case 8: /* account_options ::= account_options DBS literal */ yytestcase(yyruleno==8);
      case 9: /* account_options ::= account_options USERS literal */ yytestcase(yyruleno==9);
      case 10: /* account_options ::= account_options CONNS literal */ yytestcase(yyruleno==10);
      case 11: /* account_options ::= account_options STATE literal */ yytestcase(yyruleno==11);
{  yy_destructor(yypParser,382,&yymsp[-2].minor);
#line 56 "source/libs/parser/inc/sql.y"
{ }
#line 4900 "./source/libs/parser/src/sql.c"
  yy_destructor(yypParser,384,&yymsp[0].minor);
}
        break;
      case 12: /* alter_account_options ::= alter_account_option */
{  yy_destructor(yypParser,385,&yymsp[0].minor);
#line 68 "source/libs/parser/inc/sql.y"
{ }
#line 4908 "./source/libs/parser/src/sql.c"
}
        break;
      case 13: /* alter_account_options ::= alter_account_options alter_account_option */
{  yy_destructor(yypParser,383,&yymsp[-1].minor);
#line 69 "source/libs/parser/inc/sql.y"
{ }
#line 4915 "./source/libs/parser/src/sql.c"
  yy_destructor(yypParser,385,&yymsp[0].minor);
}
        break;
      case 14: /* alter_account_option ::= PASS literal */
      case 15: /* alter_account_option ::= PPS literal */ yytestcase(yyruleno==15);
      case 16: /* alter_account_option ::= TSERIES literal */ yytestcase(yyruleno==16);
      case 17: /* alter_account_option ::= STORAGE literal */ yytestcase(yyruleno==17);
      case 18: /* alter_account_option ::= STREAMS literal */ yytestcase(yyruleno==18);
      case 19: /* alter_account_option ::= QTIME literal */ yytestcase(yyruleno==19);
      case 20: /* alter_account_option ::= DBS literal */ yytestcase(yyruleno==20);
      case 21: /* alter_account_option ::= USERS literal */ yytestcase(yyruleno==21);
      case 22: /* alter_account_option ::= CONNS literal */ yytestcase(yyruleno==22);
      case 23: /* alter_account_option ::= STATE literal */ yytestcase(yyruleno==23);
#line 73 "source/libs/parser/inc/sql.y"
{ }
#line 4931 "./source/libs/parser/src/sql.c"
  yy_destructor(yypParser,384,&yymsp[0].minor);
        break;
      case 24: /* ip_range_list ::= NK_STRING */
#line 86 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
#line 4937 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 25: /* ip_range_list ::= ip_range_list NK_COMMA NK_STRING */
#line 87 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
#line 4943 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 26: /* white_list ::= HOST ip_range_list */
#line 91 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy436 = yymsp[0].minor.yy436; }
#line 4949 "./source/libs/parser/src/sql.c"
        break;
      case 27: /* white_list_opt ::= */
      case 207: /* specific_cols_opt ::= */ yytestcase(yyruleno==207);
      case 245: /* tags_def_opt ::= */ yytestcase(yyruleno==245);
      case 336: /* tag_list_opt ::= */ yytestcase(yyruleno==336);
      case 409: /* col_list_opt ::= */ yytestcase(yyruleno==409);
      case 416: /* tag_def_or_ref_opt ::= */ yytestcase(yyruleno==416);
      case 682: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==682);
      case 712: /* group_by_clause_opt ::= */ yytestcase(yyruleno==712);
      case 732: /* order_by_clause_opt ::= */ yytestcase(yyruleno==732);
#line 95 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy436 = NULL; }
#line 4962 "./source/libs/parser/src/sql.c"
        break;
      case 28: /* white_list_opt ::= white_list */
      case 246: /* tags_def_opt ::= tags_def */ yytestcase(yyruleno==246);
      case 417: /* tag_def_or_ref_opt ::= tags_def */ yytestcase(yyruleno==417);
      case 590: /* star_func_para_list ::= other_para_list */ yytestcase(yyruleno==590);
#line 96 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = yymsp[0].minor.yy436; }
#line 4970 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 29: /* is_import_opt ::= */
      case 31: /* is_createdb_opt ::= */ yytestcase(yyruleno==31);
#line 100 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy339 = 0; }
#line 4977 "./source/libs/parser/src/sql.c"
        break;
      case 30: /* is_import_opt ::= IS_IMPORT NK_INTEGER */
      case 32: /* is_createdb_opt ::= CREATEDB NK_INTEGER */ yytestcase(yyruleno==32);
      case 42: /* sysinfo_opt ::= SYSINFO NK_INTEGER */ yytestcase(yyruleno==42);
#line 101 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy339 = taosStr2Int8(yymsp[0].minor.yy0.z, NULL, 10); }
#line 4984 "./source/libs/parser/src/sql.c"
        break;
      case 33: /* cmd ::= CREATE USER user_name PASS NK_STRING sysinfo_opt is_createdb_opt is_import_opt white_list_opt */
#line 109 "source/libs/parser/inc/sql.y"
{
                                                                                    pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-6].minor.yy305, &yymsp[-4].minor.yy0, yymsp[-3].minor.yy339, yymsp[-1].minor.yy339, yymsp[-2].minor.yy339);
                                                                                    pCxt->pRootNode = addCreateUserStmtWhiteList(pCxt, pCxt->pRootNode, yymsp[0].minor.yy436);
                                                                                  }
#line 4992 "./source/libs/parser/src/sql.c"
        break;
      case 34: /* cmd ::= ALTER USER user_name PASS NK_STRING */
#line 113 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
#line 4997 "./source/libs/parser/src/sql.c"
        break;
      case 35: /* cmd ::= ALTER USER user_name ENABLE NK_INTEGER */
#line 114 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_ENABLE, &yymsp[0].minor.yy0); }
#line 5002 "./source/libs/parser/src/sql.c"
        break;
      case 36: /* cmd ::= ALTER USER user_name SYSINFO NK_INTEGER */
#line 115 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_SYSINFO, &yymsp[0].minor.yy0); }
#line 5007 "./source/libs/parser/src/sql.c"
        break;
      case 37: /* cmd ::= ALTER USER user_name CREATEDB NK_INTEGER */
#line 116 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_CREATEDB, &yymsp[0].minor.yy0); }
#line 5012 "./source/libs/parser/src/sql.c"
        break;
      case 38: /* cmd ::= ALTER USER user_name ADD white_list */
#line 117 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_ADD_WHITE_LIST, yymsp[0].minor.yy436); }
#line 5017 "./source/libs/parser/src/sql.c"
        break;
      case 39: /* cmd ::= ALTER USER user_name DROP white_list */
#line 118 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy305, TSDB_ALTER_USER_DROP_WHITE_LIST, yymsp[0].minor.yy436); }
#line 5022 "./source/libs/parser/src/sql.c"
        break;
      case 40: /* cmd ::= DROP USER user_name */
#line 119 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy305); }
#line 5027 "./source/libs/parser/src/sql.c"
        break;
      case 41: /* sysinfo_opt ::= */
#line 123 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy339 = 1; }
#line 5032 "./source/libs/parser/src/sql.c"
        break;
      case 43: /* cmd ::= GRANT privileges ON priv_level with_opt TO user_name */
#line 127 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createGrantStmt(pCxt, yymsp[-5].minor.yy965, &yymsp[-3].minor.yy797, &yymsp[0].minor.yy305, yymsp[-2].minor.yy800); }
#line 5037 "./source/libs/parser/src/sql.c"
        break;
      case 44: /* cmd ::= REVOKE privileges ON priv_level with_opt FROM user_name */
#line 128 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRevokeStmt(pCxt, yymsp[-5].minor.yy965, &yymsp[-3].minor.yy797, &yymsp[0].minor.yy305, yymsp[-2].minor.yy800); }
#line 5042 "./source/libs/parser/src/sql.c"
        break;
      case 45: /* privileges ::= ALL */
#line 132 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy965 = PRIVILEGE_TYPE_ALL; }
#line 5047 "./source/libs/parser/src/sql.c"
        break;
      case 46: /* privileges ::= priv_type_list */
      case 48: /* priv_type_list ::= priv_type */ yytestcase(yyruleno==48);
#line 133 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy965 = yymsp[0].minor.yy965; }
#line 5053 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy965 = yylhsminor.yy965;
        break;
      case 47: /* privileges ::= SUBSCRIBE */
#line 134 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy965 = PRIVILEGE_TYPE_SUBSCRIBE; }
#line 5059 "./source/libs/parser/src/sql.c"
        break;
      case 49: /* priv_type_list ::= priv_type_list NK_COMMA priv_type */
#line 139 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy965 = yymsp[-2].minor.yy965 | yymsp[0].minor.yy965; }
#line 5064 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy965 = yylhsminor.yy965;
        break;
      case 50: /* priv_type ::= READ */
#line 143 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy965 = PRIVILEGE_TYPE_READ; }
#line 5070 "./source/libs/parser/src/sql.c"
        break;
      case 51: /* priv_type ::= WRITE */
#line 144 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy965 = PRIVILEGE_TYPE_WRITE; }
#line 5075 "./source/libs/parser/src/sql.c"
        break;
      case 52: /* priv_type ::= ALTER */
#line 145 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy965 = PRIVILEGE_TYPE_ALTER; }
#line 5080 "./source/libs/parser/src/sql.c"
        break;
      case 53: /* priv_level ::= NK_STAR NK_DOT NK_STAR */
#line 149 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy797.first = yymsp[-2].minor.yy0; yylhsminor.yy797.second = yymsp[0].minor.yy0; }
#line 5085 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy797 = yylhsminor.yy797;
        break;
      case 54: /* priv_level ::= db_name NK_DOT NK_STAR */
#line 150 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy797.first = yymsp[-2].minor.yy305; yylhsminor.yy797.second = yymsp[0].minor.yy0; }
#line 5091 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy797 = yylhsminor.yy797;
        break;
      case 55: /* priv_level ::= db_name NK_DOT table_name */
#line 151 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy797.first = yymsp[-2].minor.yy305; yylhsminor.yy797.second = yymsp[0].minor.yy305; }
#line 5097 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy797 = yylhsminor.yy797;
        break;
      case 56: /* priv_level ::= topic_name */
#line 152 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy797.first = yymsp[0].minor.yy305; yylhsminor.yy797.second = nil_token; }
#line 5103 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy797 = yylhsminor.yy797;
        break;
      case 57: /* with_opt ::= */
      case 173: /* start_opt ::= */ yytestcase(yyruleno==173);
      case 177: /* end_opt ::= */ yytestcase(yyruleno==177);
      case 331: /* like_pattern_opt ::= */ yytestcase(yyruleno==331);
      case 428: /* subtable_opt ::= */ yytestcase(yyruleno==428);
      case 600: /* case_when_else_opt ::= */ yytestcase(yyruleno==600);
      case 630: /* from_clause_opt ::= */ yytestcase(yyruleno==630);
      case 657: /* join_on_clause_opt ::= */ yytestcase(yyruleno==657);
      case 659: /* window_offset_clause_opt ::= */ yytestcase(yyruleno==659);
      case 663: /* jlimit_clause_opt ::= */ yytestcase(yyruleno==663);
      case 680: /* where_clause_opt ::= */ yytestcase(yyruleno==680);
      case 689: /* twindow_clause_opt ::= */ yytestcase(yyruleno==689);
      case 697: /* sliding_opt ::= */ yytestcase(yyruleno==697);
      case 702: /* fill_opt ::= */ yytestcase(yyruleno==702);
      case 716: /* having_clause_opt ::= */ yytestcase(yyruleno==716);
      case 718: /* range_opt ::= */ yytestcase(yyruleno==718);
      case 721: /* every_opt ::= */ yytestcase(yyruleno==721);
      case 734: /* slimit_clause_opt ::= */ yytestcase(yyruleno==734);
      case 738: /* limit_clause_opt ::= */ yytestcase(yyruleno==738);
#line 154 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = NULL; }
#line 5127 "./source/libs/parser/src/sql.c"
        break;
      case 58: /* with_opt ::= WITH search_condition */
      case 631: /* from_clause_opt ::= FROM table_reference_list */ yytestcase(yyruleno==631);
      case 658: /* join_on_clause_opt ::= ON search_condition */ yytestcase(yyruleno==658);
      case 681: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==681);
      case 717: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==717);
#line 155 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = yymsp[0].minor.yy800; }
#line 5136 "./source/libs/parser/src/sql.c"
        break;
      case 59: /* cmd ::= CREATE ENCRYPT_KEY NK_STRING */
#line 158 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createEncryptKeyStmt(pCxt, &yymsp[0].minor.yy0); }
#line 5141 "./source/libs/parser/src/sql.c"
        break;
      case 60: /* cmd ::= CREATE DNODE dnode_endpoint */
#line 161 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy305, NULL); }
#line 5146 "./source/libs/parser/src/sql.c"
        break;
      case 61: /* cmd ::= CREATE DNODE dnode_endpoint PORT NK_INTEGER */
#line 162 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0); }
#line 5151 "./source/libs/parser/src/sql.c"
        break;
      case 62: /* cmd ::= DROP DNODE NK_INTEGER force_opt */
#line 163 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[-1].minor.yy0, yymsp[0].minor.yy125, false); }
#line 5156 "./source/libs/parser/src/sql.c"
        break;
      case 63: /* cmd ::= DROP DNODE dnode_endpoint force_opt */
#line 164 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[-1].minor.yy305, yymsp[0].minor.yy125, false); }
#line 5161 "./source/libs/parser/src/sql.c"
        break;
      case 64: /* cmd ::= DROP DNODE NK_INTEGER unsafe_opt */
#line 165 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[-1].minor.yy0, false, yymsp[0].minor.yy125); }
#line 5166 "./source/libs/parser/src/sql.c"
        break;
      case 65: /* cmd ::= DROP DNODE dnode_endpoint unsafe_opt */
#line 166 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[-1].minor.yy305, false, yymsp[0].minor.yy125); }
#line 5171 "./source/libs/parser/src/sql.c"
        break;
      case 66: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
#line 167 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL); }
#line 5176 "./source/libs/parser/src/sql.c"
        break;
      case 67: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
#line 168 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
#line 5181 "./source/libs/parser/src/sql.c"
        break;
      case 68: /* cmd ::= ALTER ALL DNODES NK_STRING */
#line 169 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[0].minor.yy0, NULL); }
#line 5186 "./source/libs/parser/src/sql.c"
        break;
      case 69: /* cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
#line 170 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
#line 5191 "./source/libs/parser/src/sql.c"
        break;
      case 70: /* cmd ::= RESTORE DNODE NK_INTEGER */
#line 171 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_DNODE_STMT, &yymsp[0].minor.yy0); }
#line 5196 "./source/libs/parser/src/sql.c"
        break;
      case 71: /* dnode_endpoint ::= NK_STRING */
      case 72: /* dnode_endpoint ::= NK_ID */ yytestcase(yyruleno==72);
      case 73: /* dnode_endpoint ::= NK_IPTOKEN */ yytestcase(yyruleno==73);
      case 365: /* sma_func_name ::= COUNT */ yytestcase(yyruleno==365);
      case 366: /* sma_func_name ::= FIRST */ yytestcase(yyruleno==366);
      case 367: /* sma_func_name ::= LAST */ yytestcase(yyruleno==367);
      case 368: /* sma_func_name ::= LAST_ROW */ yytestcase(yyruleno==368);
      case 516: /* db_name ::= NK_ID */ yytestcase(yyruleno==516);
      case 517: /* table_name ::= NK_ID */ yytestcase(yyruleno==517);
      case 518: /* column_name ::= NK_ID */ yytestcase(yyruleno==518);
      case 519: /* function_name ::= NK_ID */ yytestcase(yyruleno==519);
      case 520: /* view_name ::= NK_ID */ yytestcase(yyruleno==520);
      case 521: /* table_alias ::= NK_ID */ yytestcase(yyruleno==521);
      case 522: /* column_alias ::= NK_ID */ yytestcase(yyruleno==522);
      case 523: /* column_alias ::= NK_ALIAS */ yytestcase(yyruleno==523);
      case 524: /* user_name ::= NK_ID */ yytestcase(yyruleno==524);
      case 525: /* topic_name ::= NK_ID */ yytestcase(yyruleno==525);
      case 526: /* stream_name ::= NK_ID */ yytestcase(yyruleno==526);
      case 527: /* cgroup_name ::= NK_ID */ yytestcase(yyruleno==527);
      case 528: /* index_name ::= NK_ID */ yytestcase(yyruleno==528);
      case 529: /* tsma_name ::= NK_ID */ yytestcase(yyruleno==529);
      case 576: /* noarg_func ::= NOW */ yytestcase(yyruleno==576);
      case 577: /* noarg_func ::= TODAY */ yytestcase(yyruleno==577);
      case 578: /* noarg_func ::= TIMEZONE */ yytestcase(yyruleno==578);
      case 579: /* noarg_func ::= DATABASE */ yytestcase(yyruleno==579);
      case 580: /* noarg_func ::= CLIENT_VERSION */ yytestcase(yyruleno==580);
      case 581: /* noarg_func ::= SERVER_VERSION */ yytestcase(yyruleno==581);
      case 582: /* noarg_func ::= SERVER_STATUS */ yytestcase(yyruleno==582);
      case 583: /* noarg_func ::= CURRENT_USER */ yytestcase(yyruleno==583);
      case 584: /* noarg_func ::= USER */ yytestcase(yyruleno==584);
      case 585: /* star_func ::= COUNT */ yytestcase(yyruleno==585);
      case 586: /* star_func ::= FIRST */ yytestcase(yyruleno==586);
      case 587: /* star_func ::= LAST */ yytestcase(yyruleno==587);
      case 588: /* star_func ::= LAST_ROW */ yytestcase(yyruleno==588);
#line 175 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy305 = yymsp[0].minor.yy0; }
#line 5234 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy305 = yylhsminor.yy305;
        break;
      case 74: /* force_opt ::= */
      case 101: /* not_exists_opt ::= */ yytestcase(yyruleno==101);
      case 103: /* exists_opt ::= */ yytestcase(yyruleno==103);
      case 386: /* analyze_opt ::= */ yytestcase(yyruleno==386);
      case 393: /* agg_func_opt ::= */ yytestcase(yyruleno==393);
      case 399: /* or_replace_opt ::= */ yytestcase(yyruleno==399);
      case 430: /* ignore_opt ::= */ yytestcase(yyruleno==430);
      case 668: /* tag_mode_opt ::= */ yytestcase(yyruleno==668);
      case 670: /* set_quantifier_opt ::= */ yytestcase(yyruleno==670);
#line 181 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy125 = false; }
#line 5248 "./source/libs/parser/src/sql.c"
        break;
      case 75: /* force_opt ::= FORCE */
      case 76: /* unsafe_opt ::= UNSAFE */ yytestcase(yyruleno==76);
      case 387: /* analyze_opt ::= ANALYZE */ yytestcase(yyruleno==387);
      case 394: /* agg_func_opt ::= AGGREGATE */ yytestcase(yyruleno==394);
      case 669: /* tag_mode_opt ::= TAGS */ yytestcase(yyruleno==669);
      case 671: /* set_quantifier_opt ::= DISTINCT */ yytestcase(yyruleno==671);
#line 182 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy125 = true; }
#line 5258 "./source/libs/parser/src/sql.c"
        break;
      case 77: /* cmd ::= ALTER CLUSTER NK_STRING */
#line 189 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterClusterStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
#line 5263 "./source/libs/parser/src/sql.c"
        break;
      case 78: /* cmd ::= ALTER CLUSTER NK_STRING NK_STRING */
#line 190 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterClusterStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
#line 5268 "./source/libs/parser/src/sql.c"
        break;
      case 79: /* cmd ::= ALTER LOCAL NK_STRING */
#line 193 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
#line 5273 "./source/libs/parser/src/sql.c"
        break;
      case 80: /* cmd ::= ALTER LOCAL NK_STRING NK_STRING */
#line 194 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
#line 5278 "./source/libs/parser/src/sql.c"
        break;
      case 81: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
#line 197 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_QNODE_STMT, &yymsp[0].minor.yy0); }
#line 5283 "./source/libs/parser/src/sql.c"
        break;
      case 82: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
#line 198 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_QNODE_STMT, &yymsp[0].minor.yy0); }
#line 5288 "./source/libs/parser/src/sql.c"
        break;
      case 83: /* cmd ::= RESTORE QNODE ON DNODE NK_INTEGER */
#line 199 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_QNODE_STMT, &yymsp[0].minor.yy0); }
#line 5293 "./source/libs/parser/src/sql.c"
        break;
      case 84: /* cmd ::= CREATE BNODE ON DNODE NK_INTEGER */
#line 202 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_BNODE_STMT, &yymsp[0].minor.yy0); }
#line 5298 "./source/libs/parser/src/sql.c"
        break;
      case 85: /* cmd ::= DROP BNODE ON DNODE NK_INTEGER */
#line 203 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_BNODE_STMT, &yymsp[0].minor.yy0); }
#line 5303 "./source/libs/parser/src/sql.c"
        break;
      case 86: /* cmd ::= CREATE SNODE ON DNODE NK_INTEGER */
#line 206 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_SNODE_STMT, &yymsp[0].minor.yy0); }
#line 5308 "./source/libs/parser/src/sql.c"
        break;
      case 87: /* cmd ::= DROP SNODE ON DNODE NK_INTEGER */
#line 207 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_SNODE_STMT, &yymsp[0].minor.yy0); }
#line 5313 "./source/libs/parser/src/sql.c"
        break;
      case 88: /* cmd ::= CREATE MNODE ON DNODE NK_INTEGER */
#line 210 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateComponentNodeStmt(pCxt, QUERY_NODE_CREATE_MNODE_STMT, &yymsp[0].minor.yy0); }
#line 5318 "./source/libs/parser/src/sql.c"
        break;
      case 89: /* cmd ::= DROP MNODE ON DNODE NK_INTEGER */
#line 211 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropComponentNodeStmt(pCxt, QUERY_NODE_DROP_MNODE_STMT, &yymsp[0].minor.yy0); }
#line 5323 "./source/libs/parser/src/sql.c"
        break;
      case 90: /* cmd ::= RESTORE MNODE ON DNODE NK_INTEGER */
#line 212 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_MNODE_STMT, &yymsp[0].minor.yy0); }
#line 5328 "./source/libs/parser/src/sql.c"
        break;
      case 91: /* cmd ::= RESTORE VNODE ON DNODE NK_INTEGER */
#line 215 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRestoreComponentNodeStmt(pCxt, QUERY_NODE_RESTORE_VNODE_STMT, &yymsp[0].minor.yy0); }
#line 5333 "./source/libs/parser/src/sql.c"
        break;
      case 92: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
#line 218 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy125, &yymsp[-1].minor.yy305, yymsp[0].minor.yy800); }
#line 5338 "./source/libs/parser/src/sql.c"
        break;
      case 93: /* cmd ::= DROP DATABASE exists_opt db_name */
#line 219 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 5343 "./source/libs/parser/src/sql.c"
        break;
      case 94: /* cmd ::= USE db_name */
#line 220 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy305); }
#line 5348 "./source/libs/parser/src/sql.c"
        break;
      case 95: /* cmd ::= ALTER DATABASE db_name alter_db_options */
#line 221 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy305, yymsp[0].minor.yy800); }
#line 5353 "./source/libs/parser/src/sql.c"
        break;
      case 96: /* cmd ::= FLUSH DATABASE db_name */
#line 222 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createFlushDatabaseStmt(pCxt, &yymsp[0].minor.yy305); }
#line 5358 "./source/libs/parser/src/sql.c"
        break;
      case 97: /* cmd ::= TRIM DATABASE db_name speed_opt */
#line 223 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createTrimDatabaseStmt(pCxt, &yymsp[-1].minor.yy305, yymsp[0].minor.yy564); }
#line 5363 "./source/libs/parser/src/sql.c"
        break;
      case 98: /* cmd ::= S3MIGRATE DATABASE db_name */
#line 224 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createS3MigrateDatabaseStmt(pCxt, &yymsp[0].minor.yy305); }
#line 5368 "./source/libs/parser/src/sql.c"
        break;
      case 99: /* cmd ::= COMPACT DATABASE db_name start_opt end_opt */
#line 225 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCompactStmt(pCxt, &yymsp[-2].minor.yy305, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 5373 "./source/libs/parser/src/sql.c"
        break;
      case 100: /* not_exists_opt ::= IF NOT EXISTS */
#line 229 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy125 = true; }
#line 5378 "./source/libs/parser/src/sql.c"
        break;
      case 102: /* exists_opt ::= IF EXISTS */
      case 400: /* or_replace_opt ::= OR REPLACE */ yytestcase(yyruleno==400);
      case 431: /* ignore_opt ::= IGNORE UNTREATED */ yytestcase(yyruleno==431);
#line 234 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy125 = true; }
#line 5385 "./source/libs/parser/src/sql.c"
        break;
      case 104: /* db_options ::= */
#line 237 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createDefaultDatabaseOptions(pCxt); }
#line 5390 "./source/libs/parser/src/sql.c"
        break;
      case 105: /* db_options ::= db_options BUFFER NK_INTEGER */
#line 238 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_BUFFER, &yymsp[0].minor.yy0); }
#line 5395 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 106: /* db_options ::= db_options CACHEMODEL NK_STRING */
#line 239 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_CACHEMODEL, &yymsp[0].minor.yy0); }
#line 5401 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 107: /* db_options ::= db_options CACHESIZE NK_INTEGER */
#line 240 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_CACHESIZE, &yymsp[0].minor.yy0); }
#line 5407 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 108: /* db_options ::= db_options COMP NK_INTEGER */
#line 241 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
#line 5413 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 109: /* db_options ::= db_options DURATION NK_INTEGER */
      case 110: /* db_options ::= db_options DURATION NK_VARIABLE */ yytestcase(yyruleno==110);
#line 242 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
#line 5420 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 111: /* db_options ::= db_options MAXROWS NK_INTEGER */
#line 244 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
#line 5426 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 112: /* db_options ::= db_options MINROWS NK_INTEGER */
#line 245 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
#line 5432 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 113: /* db_options ::= db_options KEEP integer_list */
      case 114: /* db_options ::= db_options KEEP variable_list */ yytestcase(yyruleno==114);
#line 246 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_KEEP, yymsp[0].minor.yy436); }
#line 5439 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 115: /* db_options ::= db_options PAGES NK_INTEGER */
#line 248 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_PAGES, &yymsp[0].minor.yy0); }
#line 5445 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 116: /* db_options ::= db_options PAGESIZE NK_INTEGER */
#line 249 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_PAGESIZE, &yymsp[0].minor.yy0); }
#line 5451 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 117: /* db_options ::= db_options TSDB_PAGESIZE NK_INTEGER */
#line 250 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_TSDB_PAGESIZE, &yymsp[0].minor.yy0); }
#line 5457 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 118: /* db_options ::= db_options PRECISION NK_STRING */
#line 251 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
#line 5463 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 119: /* db_options ::= db_options REPLICA NK_INTEGER */
#line 252 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
#line 5469 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 120: /* db_options ::= db_options VGROUPS NK_INTEGER */
#line 254 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
#line 5475 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 121: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
#line 255 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_SINGLE_STABLE, &yymsp[0].minor.yy0); }
#line 5481 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 122: /* db_options ::= db_options RETENTIONS retention_list */
#line 256 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_RETENTIONS, yymsp[0].minor.yy436); }
#line 5487 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 123: /* db_options ::= db_options SCHEMALESS NK_INTEGER */
#line 257 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_SCHEMALESS, &yymsp[0].minor.yy0); }
#line 5493 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 124: /* db_options ::= db_options WAL_LEVEL NK_INTEGER */
#line 258 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
#line 5499 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 125: /* db_options ::= db_options WAL_FSYNC_PERIOD NK_INTEGER */
#line 259 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
#line 5505 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 126: /* db_options ::= db_options WAL_RETENTION_PERIOD NK_INTEGER */
#line 260 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_WAL_RETENTION_PERIOD, &yymsp[0].minor.yy0); }
#line 5511 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 127: /* db_options ::= db_options WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER */
#line 261 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-3].minor.yy800, DB_OPTION_WAL_RETENTION_PERIOD, &t);
                                                                                  }
#line 5521 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 128: /* db_options ::= db_options WAL_RETENTION_SIZE NK_INTEGER */
#line 266 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_WAL_RETENTION_SIZE, &yymsp[0].minor.yy0); }
#line 5527 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 129: /* db_options ::= db_options WAL_RETENTION_SIZE NK_MINUS NK_INTEGER */
#line 267 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-3].minor.yy800, DB_OPTION_WAL_RETENTION_SIZE, &t);
                                                                                  }
#line 5537 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 130: /* db_options ::= db_options WAL_ROLL_PERIOD NK_INTEGER */
#line 272 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_WAL_ROLL_PERIOD, &yymsp[0].minor.yy0); }
#line 5543 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 131: /* db_options ::= db_options WAL_SEGMENT_SIZE NK_INTEGER */
#line 273 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_WAL_SEGMENT_SIZE, &yymsp[0].minor.yy0); }
#line 5549 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 132: /* db_options ::= db_options STT_TRIGGER NK_INTEGER */
#line 274 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_STT_TRIGGER, &yymsp[0].minor.yy0); }
#line 5555 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 133: /* db_options ::= db_options TABLE_PREFIX signed */
#line 275 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_TABLE_PREFIX, yymsp[0].minor.yy800); }
#line 5561 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 134: /* db_options ::= db_options TABLE_SUFFIX signed */
#line 276 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_TABLE_SUFFIX, yymsp[0].minor.yy800); }
#line 5567 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 135: /* db_options ::= db_options S3_CHUNKSIZE NK_INTEGER */
#line 277 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_S3_CHUNKSIZE, &yymsp[0].minor.yy0); }
#line 5573 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 136: /* db_options ::= db_options S3_KEEPLOCAL NK_INTEGER */
      case 137: /* db_options ::= db_options S3_KEEPLOCAL NK_VARIABLE */ yytestcase(yyruleno==137);
#line 278 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_S3_KEEPLOCAL, &yymsp[0].minor.yy0); }
#line 5580 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 138: /* db_options ::= db_options S3_COMPACT NK_INTEGER */
#line 280 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_S3_COMPACT, &yymsp[0].minor.yy0); }
#line 5586 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 139: /* db_options ::= db_options KEEP_TIME_OFFSET NK_INTEGER */
#line 281 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_KEEP_TIME_OFFSET, &yymsp[0].minor.yy0); }
#line 5592 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 140: /* db_options ::= db_options ENCRYPT_ALGORITHM NK_STRING */
#line 282 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setDatabaseOption(pCxt, yymsp[-2].minor.yy800, DB_OPTION_ENCRYPT_ALGORITHM, &yymsp[0].minor.yy0); }
#line 5598 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 141: /* alter_db_options ::= alter_db_option */
#line 284 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterDatabaseOptions(pCxt); yylhsminor.yy800 = setAlterDatabaseOption(pCxt, yylhsminor.yy800, &yymsp[0].minor.yy1017); }
#line 5604 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 142: /* alter_db_options ::= alter_db_options alter_db_option */
#line 285 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setAlterDatabaseOption(pCxt, yymsp[-1].minor.yy800, &yymsp[0].minor.yy1017); }
#line 5610 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 143: /* alter_db_option ::= BUFFER NK_INTEGER */
#line 289 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_BUFFER; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5616 "./source/libs/parser/src/sql.c"
        break;
      case 144: /* alter_db_option ::= CACHEMODEL NK_STRING */
#line 290 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_CACHEMODEL; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5621 "./source/libs/parser/src/sql.c"
        break;
      case 145: /* alter_db_option ::= CACHESIZE NK_INTEGER */
#line 291 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_CACHESIZE; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5626 "./source/libs/parser/src/sql.c"
        break;
      case 146: /* alter_db_option ::= WAL_FSYNC_PERIOD NK_INTEGER */
#line 292 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_FSYNC; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5631 "./source/libs/parser/src/sql.c"
        break;
      case 147: /* alter_db_option ::= KEEP integer_list */
      case 148: /* alter_db_option ::= KEEP variable_list */ yytestcase(yyruleno==148);
#line 293 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_KEEP; yymsp[-1].minor.yy1017.pList = yymsp[0].minor.yy436; }
#line 5637 "./source/libs/parser/src/sql.c"
        break;
      case 149: /* alter_db_option ::= PAGES NK_INTEGER */
#line 295 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_PAGES; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5642 "./source/libs/parser/src/sql.c"
        break;
      case 150: /* alter_db_option ::= REPLICA NK_INTEGER */
#line 296 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_REPLICA; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5647 "./source/libs/parser/src/sql.c"
        break;
      case 151: /* alter_db_option ::= WAL_LEVEL NK_INTEGER */
#line 298 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_WAL; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5652 "./source/libs/parser/src/sql.c"
        break;
      case 152: /* alter_db_option ::= STT_TRIGGER NK_INTEGER */
#line 299 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_STT_TRIGGER; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5657 "./source/libs/parser/src/sql.c"
        break;
      case 153: /* alter_db_option ::= MINROWS NK_INTEGER */
#line 300 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_MINROWS; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5662 "./source/libs/parser/src/sql.c"
        break;
      case 154: /* alter_db_option ::= WAL_RETENTION_PERIOD NK_INTEGER */
#line 301 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_WAL_RETENTION_PERIOD; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5667 "./source/libs/parser/src/sql.c"
        break;
      case 155: /* alter_db_option ::= WAL_RETENTION_PERIOD NK_MINUS NK_INTEGER */
#line 302 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yymsp[-2].minor.yy1017.type = DB_OPTION_WAL_RETENTION_PERIOD; yymsp[-2].minor.yy1017.val = t;
                                                                                  }
#line 5676 "./source/libs/parser/src/sql.c"
        break;
      case 156: /* alter_db_option ::= WAL_RETENTION_SIZE NK_INTEGER */
#line 307 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_WAL_RETENTION_SIZE; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5681 "./source/libs/parser/src/sql.c"
        break;
      case 157: /* alter_db_option ::= WAL_RETENTION_SIZE NK_MINUS NK_INTEGER */
#line 308 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yymsp[-2].minor.yy1017.type = DB_OPTION_WAL_RETENTION_SIZE; yymsp[-2].minor.yy1017.val = t;
                                                                                  }
#line 5690 "./source/libs/parser/src/sql.c"
        break;
      case 158: /* alter_db_option ::= S3_KEEPLOCAL NK_INTEGER */
      case 159: /* alter_db_option ::= S3_KEEPLOCAL NK_VARIABLE */ yytestcase(yyruleno==159);
#line 313 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_S3_KEEPLOCAL; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5696 "./source/libs/parser/src/sql.c"
        break;
      case 160: /* alter_db_option ::= S3_COMPACT NK_INTEGER */
#line 315 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_S3_COMPACT, yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5701 "./source/libs/parser/src/sql.c"
        break;
      case 161: /* alter_db_option ::= KEEP_TIME_OFFSET NK_INTEGER */
#line 316 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_KEEP_TIME_OFFSET; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5706 "./source/libs/parser/src/sql.c"
        break;
      case 162: /* alter_db_option ::= ENCRYPT_ALGORITHM NK_STRING */
#line 317 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = DB_OPTION_ENCRYPT_ALGORITHM; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 5711 "./source/libs/parser/src/sql.c"
        break;
      case 163: /* integer_list ::= NK_INTEGER */
#line 321 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
#line 5716 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 164: /* integer_list ::= integer_list NK_COMMA NK_INTEGER */
      case 445: /* dnode_list ::= dnode_list DNODE NK_INTEGER */ yytestcase(yyruleno==445);
#line 322 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
#line 5723 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 165: /* variable_list ::= NK_VARIABLE */
#line 326 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 5729 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 166: /* variable_list ::= variable_list NK_COMMA NK_VARIABLE */
#line 327 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 5735 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 167: /* retention_list ::= retention */
      case 199: /* multi_create_clause ::= create_subtable_clause */ yytestcase(yyruleno==199);
      case 201: /* multi_create_clause ::= create_from_file_clause */ yytestcase(yyruleno==201);
      case 204: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==204);
      case 211: /* tag_def_list ::= tag_def */ yytestcase(yyruleno==211);
      case 214: /* column_def_list ::= column_def */ yytestcase(yyruleno==214);
      case 262: /* rollup_func_list ::= rollup_func_name */ yytestcase(yyruleno==262);
      case 267: /* col_name_list ::= col_name */ yytestcase(yyruleno==267);
      case 337: /* tag_list_opt ::= tag_item */ yytestcase(yyruleno==337);
      case 361: /* func_list ::= func */ yytestcase(yyruleno==361);
      case 411: /* column_stream_def_list ::= column_stream_def */ yytestcase(yyruleno==411);
      case 489: /* tags_literal_list ::= tags_literal */ yytestcase(yyruleno==489);
      case 514: /* literal_list ::= signed_literal */ yytestcase(yyruleno==514);
      case 591: /* other_para_list ::= star_func_para */ yytestcase(yyruleno==591);
      case 597: /* when_then_list ::= when_then_expr */ yytestcase(yyruleno==597);
      case 673: /* select_list ::= select_item */ yytestcase(yyruleno==673);
      case 684: /* partition_list ::= partition_item */ yytestcase(yyruleno==684);
      case 745: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==745);
#line 331 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, yymsp[0].minor.yy800); }
#line 5758 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 168: /* retention_list ::= retention_list NK_COMMA retention */
      case 205: /* multi_drop_clause ::= multi_drop_clause NK_COMMA drop_table_clause */ yytestcase(yyruleno==205);
      case 212: /* tag_def_list ::= tag_def_list NK_COMMA tag_def */ yytestcase(yyruleno==212);
      case 215: /* column_def_list ::= column_def_list NK_COMMA column_def */ yytestcase(yyruleno==215);
      case 263: /* rollup_func_list ::= rollup_func_list NK_COMMA rollup_func_name */ yytestcase(yyruleno==263);
      case 268: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==268);
      case 338: /* tag_list_opt ::= tag_list_opt NK_COMMA tag_item */ yytestcase(yyruleno==338);
      case 362: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==362);
      case 412: /* column_stream_def_list ::= column_stream_def_list NK_COMMA column_stream_def */ yytestcase(yyruleno==412);
      case 490: /* tags_literal_list ::= tags_literal_list NK_COMMA tags_literal */ yytestcase(yyruleno==490);
      case 515: /* literal_list ::= literal_list NK_COMMA signed_literal */ yytestcase(yyruleno==515);
      case 592: /* other_para_list ::= other_para_list NK_COMMA star_func_para */ yytestcase(yyruleno==592);
      case 674: /* select_list ::= select_list NK_COMMA select_item */ yytestcase(yyruleno==674);
      case 685: /* partition_list ::= partition_list NK_COMMA partition_item */ yytestcase(yyruleno==685);
      case 746: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==746);
#line 332 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, yymsp[0].minor.yy800); }
#line 5778 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 169: /* retention ::= NK_VARIABLE NK_COLON NK_VARIABLE */
      case 170: /* retention ::= NK_MINUS NK_COLON NK_VARIABLE */ yytestcase(yyruleno==170);
#line 334 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createNodeListNodeEx(pCxt, createDurationValueNode(pCxt, &yymsp[-2].minor.yy0), createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 5785 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 171: /* speed_opt ::= */
      case 395: /* bufsize_opt ::= */ yytestcase(yyruleno==395);
#line 339 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy564 = 0; }
#line 5792 "./source/libs/parser/src/sql.c"
        break;
      case 172: /* speed_opt ::= BWLIMIT NK_INTEGER */
      case 396: /* bufsize_opt ::= BUFSIZE NK_INTEGER */ yytestcase(yyruleno==396);
#line 340 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy564 = taosStr2Int32(yymsp[0].minor.yy0.z, NULL, 10); }
#line 5798 "./source/libs/parser/src/sql.c"
        break;
      case 174: /* start_opt ::= START WITH NK_INTEGER */
      case 178: /* end_opt ::= END WITH NK_INTEGER */ yytestcase(yyruleno==178);
#line 343 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0); }
#line 5804 "./source/libs/parser/src/sql.c"
        break;
      case 175: /* start_opt ::= START WITH NK_STRING */
      case 179: /* end_opt ::= END WITH NK_STRING */ yytestcase(yyruleno==179);
#line 344 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0); }
#line 5810 "./source/libs/parser/src/sql.c"
        break;
      case 176: /* start_opt ::= START WITH TIMESTAMP NK_STRING */
      case 180: /* end_opt ::= END WITH TIMESTAMP NK_STRING */ yytestcase(yyruleno==180);
#line 345 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0); }
#line 5816 "./source/libs/parser/src/sql.c"
        break;
      case 181: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 183: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==183);
#line 354 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy125, yymsp[-5].minor.yy800, yymsp[-3].minor.yy436, yymsp[-1].minor.yy436, yymsp[0].minor.yy800); }
#line 5822 "./source/libs/parser/src/sql.c"
        break;
      case 182: /* cmd ::= CREATE TABLE multi_create_clause */
#line 355 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy436); }
#line 5827 "./source/libs/parser/src/sql.c"
        break;
      case 184: /* cmd ::= DROP TABLE multi_drop_clause */
#line 358 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy436); }
#line 5832 "./source/libs/parser/src/sql.c"
        break;
      case 185: /* cmd ::= DROP STABLE exists_opt full_table_name */
#line 359 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy125, yymsp[0].minor.yy800); }
#line 5837 "./source/libs/parser/src/sql.c"
        break;
      case 186: /* cmd ::= ALTER TABLE alter_table_clause */
      case 447: /* cmd ::= query_or_subquery */ yytestcase(yyruleno==447);
      case 448: /* cmd ::= insert_query */ yytestcase(yyruleno==448);
#line 361 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = yymsp[0].minor.yy800; }
#line 5844 "./source/libs/parser/src/sql.c"
        break;
      case 187: /* cmd ::= ALTER STABLE alter_table_clause */
#line 362 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = setAlterSuperTableType(yymsp[0].minor.yy800); }
#line 5849 "./source/libs/parser/src/sql.c"
        break;
      case 188: /* alter_table_clause ::= full_table_name alter_table_options */
#line 364 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableModifyOptions(pCxt, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 5854 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 189: /* alter_table_clause ::= full_table_name ADD COLUMN column_name type_name column_options */
#line 366 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableAddModifyColOptions2(pCxt, yymsp[-5].minor.yy800, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-2].minor.yy305, yymsp[-1].minor.yy96, yymsp[0].minor.yy800); }
#line 5860 "./source/libs/parser/src/sql.c"
  yymsp[-5].minor.yy800 = yylhsminor.yy800;
        break;
      case 190: /* alter_table_clause ::= full_table_name DROP COLUMN column_name */
#line 367 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy800, TSDB_ALTER_TABLE_DROP_COLUMN, &yymsp[0].minor.yy305); }
#line 5866 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 191: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
#line 369 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &yymsp[-1].minor.yy305, yymsp[0].minor.yy96); }
#line 5872 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 192: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name column_options */
#line 371 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableAddModifyColOptions(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_UPDATE_COLUMN_COMPRESS, &yymsp[-1].minor.yy305, yymsp[0].minor.yy800); }
#line 5878 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 193: /* alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
#line 373 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &yymsp[-1].minor.yy305, &yymsp[0].minor.yy305); }
#line 5884 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 194: /* alter_table_clause ::= full_table_name ADD TAG column_name type_name */
#line 375 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_ADD_TAG, &yymsp[-1].minor.yy305, yymsp[0].minor.yy96); }
#line 5890 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 195: /* alter_table_clause ::= full_table_name DROP TAG column_name */
#line 376 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy800, TSDB_ALTER_TABLE_DROP_TAG, &yymsp[0].minor.yy305); }
#line 5896 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 196: /* alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
#line 378 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &yymsp[-1].minor.yy305, yymsp[0].minor.yy96); }
#line 5902 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 197: /* alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
#line 380 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy800, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &yymsp[-1].minor.yy305, &yymsp[0].minor.yy305); }
#line 5908 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 198: /* alter_table_clause ::= full_table_name SET TAG column_name NK_EQ tags_literal */
#line 382 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableSetTag(pCxt, yymsp[-5].minor.yy800, &yymsp[-2].minor.yy305, yymsp[0].minor.yy800); }
#line 5914 "./source/libs/parser/src/sql.c"
  yymsp[-5].minor.yy800 = yylhsminor.yy800;
        break;
      case 200: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 598: /* when_then_list ::= when_then_list when_then_expr */ yytestcase(yyruleno==598);
#line 387 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-1].minor.yy436, yymsp[0].minor.yy800); }
#line 5921 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 202: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_cols_opt TAGS NK_LP tags_literal_list NK_RP table_options */
#line 392 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createCreateSubTableClause(pCxt, yymsp[-9].minor.yy125, yymsp[-8].minor.yy800, yymsp[-6].minor.yy800, yymsp[-5].minor.yy436, yymsp[-2].minor.yy436, yymsp[0].minor.yy800); }
#line 5927 "./source/libs/parser/src/sql.c"
  yymsp[-9].minor.yy800 = yylhsminor.yy800;
        break;
      case 203: /* create_from_file_clause ::= not_exists_opt USING full_table_name NK_LP tag_list_opt NK_RP FILE NK_STRING */
#line 395 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createCreateSubTableFromFileClause(pCxt, yymsp[-7].minor.yy125, yymsp[-5].minor.yy800, yymsp[-3].minor.yy436, &yymsp[0].minor.yy0); }
#line 5933 "./source/libs/parser/src/sql.c"
  yymsp[-7].minor.yy800 = yylhsminor.yy800;
        break;
      case 206: /* drop_table_clause ::= exists_opt full_table_name */
#line 402 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createDropTableClause(pCxt, yymsp[-1].minor.yy125, yymsp[0].minor.yy800); }
#line 5939 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 208: /* specific_cols_opt ::= NK_LP col_name_list NK_RP */
      case 410: /* col_list_opt ::= NK_LP column_stream_def_list NK_RP */ yytestcase(yyruleno==410);
#line 407 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy436 = yymsp[-1].minor.yy436; }
#line 5946 "./source/libs/parser/src/sql.c"
        break;
      case 209: /* full_table_name ::= table_name */
      case 351: /* full_tsma_name ::= tsma_name */ yytestcase(yyruleno==351);
#line 409 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy305, NULL); }
#line 5952 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 210: /* full_table_name ::= db_name NK_DOT table_name */
      case 352: /* full_tsma_name ::= db_name NK_DOT tsma_name */ yytestcase(yyruleno==352);
#line 410 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNode(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305, NULL); }
#line 5959 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 213: /* tag_def ::= column_name type_name */
#line 416 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy305, yymsp[0].minor.yy96, NULL); }
#line 5965 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 216: /* column_def ::= column_name type_name column_options */
#line 424 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnDefNode(pCxt, &yymsp[-2].minor.yy305, yymsp[-1].minor.yy96, yymsp[0].minor.yy800); }
#line 5971 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 217: /* type_name ::= BOOL */
#line 428 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_BOOL); }
#line 5977 "./source/libs/parser/src/sql.c"
        break;
      case 218: /* type_name ::= TINYINT */
#line 429 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_TINYINT); }
#line 5982 "./source/libs/parser/src/sql.c"
        break;
      case 219: /* type_name ::= SMALLINT */
#line 430 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
#line 5987 "./source/libs/parser/src/sql.c"
        break;
      case 220: /* type_name ::= INT */
      case 221: /* type_name ::= INTEGER */ yytestcase(yyruleno==221);
#line 431 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_INT); }
#line 5993 "./source/libs/parser/src/sql.c"
        break;
      case 222: /* type_name ::= BIGINT */
#line 433 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_BIGINT); }
#line 5998 "./source/libs/parser/src/sql.c"
        break;
      case 223: /* type_name ::= FLOAT */
#line 434 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_FLOAT); }
#line 6003 "./source/libs/parser/src/sql.c"
        break;
      case 224: /* type_name ::= DOUBLE */
#line 435 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
#line 6008 "./source/libs/parser/src/sql.c"
        break;
      case 225: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
#line 436 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
#line 6013 "./source/libs/parser/src/sql.c"
        break;
      case 226: /* type_name ::= TIMESTAMP */
#line 437 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
#line 6018 "./source/libs/parser/src/sql.c"
        break;
      case 227: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
#line 438 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
#line 6023 "./source/libs/parser/src/sql.c"
        break;
      case 228: /* type_name ::= TINYINT UNSIGNED */
#line 439 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy96 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
#line 6028 "./source/libs/parser/src/sql.c"
        break;
      case 229: /* type_name ::= SMALLINT UNSIGNED */
#line 440 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy96 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
#line 6033 "./source/libs/parser/src/sql.c"
        break;
      case 230: /* type_name ::= INT UNSIGNED */
#line 441 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy96 = createDataType(TSDB_DATA_TYPE_UINT); }
#line 6038 "./source/libs/parser/src/sql.c"
        break;
      case 231: /* type_name ::= BIGINT UNSIGNED */
#line 442 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy96 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
#line 6043 "./source/libs/parser/src/sql.c"
        break;
      case 232: /* type_name ::= JSON */
#line 443 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_JSON); }
#line 6048 "./source/libs/parser/src/sql.c"
        break;
      case 233: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
#line 444 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
#line 6053 "./source/libs/parser/src/sql.c"
        break;
      case 234: /* type_name ::= MEDIUMBLOB */
#line 445 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
#line 6058 "./source/libs/parser/src/sql.c"
        break;
      case 235: /* type_name ::= BLOB */
#line 446 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_BLOB); }
#line 6063 "./source/libs/parser/src/sql.c"
        break;
      case 236: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
#line 447 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
#line 6068 "./source/libs/parser/src/sql.c"
        break;
      case 237: /* type_name ::= GEOMETRY NK_LP NK_INTEGER NK_RP */
#line 448 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_GEOMETRY, &yymsp[-1].minor.yy0); }
#line 6073 "./source/libs/parser/src/sql.c"
        break;
      case 238: /* type_name ::= DECIMAL */
#line 449 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
#line 6078 "./source/libs/parser/src/sql.c"
        break;
      case 239: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
#line 450 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy96 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
#line 6083 "./source/libs/parser/src/sql.c"
        break;
      case 240: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
#line 451 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy96 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
#line 6088 "./source/libs/parser/src/sql.c"
        break;
      case 241: /* type_name_default_len ::= BINARY */
#line 455 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, NULL); }
#line 6093 "./source/libs/parser/src/sql.c"
        break;
      case 242: /* type_name_default_len ::= NCHAR */
#line 456 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, NULL); }
#line 6098 "./source/libs/parser/src/sql.c"
        break;
      case 243: /* type_name_default_len ::= VARCHAR */
#line 457 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, NULL); }
#line 6103 "./source/libs/parser/src/sql.c"
        break;
      case 244: /* type_name_default_len ::= VARBINARY */
#line 458 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy96 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, NULL); }
#line 6108 "./source/libs/parser/src/sql.c"
        break;
      case 247: /* tags_def ::= TAGS NK_LP tag_def_list NK_RP */
      case 418: /* tag_def_or_ref_opt ::= TAGS NK_LP column_stream_def_list NK_RP */ yytestcase(yyruleno==418);
#line 467 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy436 = yymsp[-1].minor.yy436; }
#line 6114 "./source/libs/parser/src/sql.c"
        break;
      case 248: /* table_options ::= */
#line 469 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createDefaultTableOptions(pCxt); }
#line 6119 "./source/libs/parser/src/sql.c"
        break;
      case 249: /* table_options ::= table_options COMMENT NK_STRING */
#line 470 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-2].minor.yy800, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
#line 6124 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 250: /* table_options ::= table_options MAX_DELAY duration_list */
#line 471 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-2].minor.yy800, TABLE_OPTION_MAXDELAY, yymsp[0].minor.yy436); }
#line 6130 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 251: /* table_options ::= table_options WATERMARK duration_list */
#line 472 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-2].minor.yy800, TABLE_OPTION_WATERMARK, yymsp[0].minor.yy436); }
#line 6136 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 252: /* table_options ::= table_options ROLLUP NK_LP rollup_func_list NK_RP */
#line 473 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-4].minor.yy800, TABLE_OPTION_ROLLUP, yymsp[-1].minor.yy436); }
#line 6142 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 253: /* table_options ::= table_options TTL NK_INTEGER */
#line 474 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-2].minor.yy800, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
#line 6148 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 254: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
#line 475 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-4].minor.yy800, TABLE_OPTION_SMA, yymsp[-1].minor.yy436); }
#line 6154 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 255: /* table_options ::= table_options DELETE_MARK duration_list */
#line 476 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-2].minor.yy800, TABLE_OPTION_DELETE_MARK, yymsp[0].minor.yy436); }
#line 6160 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 256: /* alter_table_options ::= alter_table_option */
#line 478 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createAlterTableOptions(pCxt); yylhsminor.yy800 = setTableOption(pCxt, yylhsminor.yy800, yymsp[0].minor.yy1017.type, &yymsp[0].minor.yy1017.val); }
#line 6166 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 257: /* alter_table_options ::= alter_table_options alter_table_option */
#line 479 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setTableOption(pCxt, yymsp[-1].minor.yy800, yymsp[0].minor.yy1017.type, &yymsp[0].minor.yy1017.val); }
#line 6172 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 258: /* alter_table_option ::= COMMENT NK_STRING */
#line 483 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = TABLE_OPTION_COMMENT; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 6178 "./source/libs/parser/src/sql.c"
        break;
      case 259: /* alter_table_option ::= TTL NK_INTEGER */
#line 484 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy1017.type = TABLE_OPTION_TTL; yymsp[-1].minor.yy1017.val = yymsp[0].minor.yy0; }
#line 6183 "./source/libs/parser/src/sql.c"
        break;
      case 260: /* duration_list ::= duration_literal */
      case 547: /* expression_list ::= expr_or_subquery */ yytestcase(yyruleno==547);
#line 488 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 6189 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 261: /* duration_list ::= duration_list NK_COMMA duration_literal */
      case 548: /* expression_list ::= expression_list NK_COMMA expr_or_subquery */ yytestcase(yyruleno==548);
#line 489 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 6196 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 264: /* rollup_func_name ::= function_name */
#line 496 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createFunctionNode(pCxt, &yymsp[0].minor.yy305, NULL); }
#line 6202 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 265: /* rollup_func_name ::= FIRST */
      case 266: /* rollup_func_name ::= LAST */ yytestcase(yyruleno==266);
      case 340: /* tag_item ::= QTAGS */ yytestcase(yyruleno==340);
#line 497 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createFunctionNode(pCxt, &yymsp[0].minor.yy0, NULL); }
#line 6210 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 269: /* col_name ::= column_name */
      case 341: /* tag_item ::= column_name */ yytestcase(yyruleno==341);
#line 505 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy305); }
#line 6217 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 270: /* cmd ::= SHOW DNODES */
#line 508 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT); }
#line 6223 "./source/libs/parser/src/sql.c"
        break;
      case 271: /* cmd ::= SHOW USERS */
#line 509 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT); }
#line 6228 "./source/libs/parser/src/sql.c"
        break;
      case 272: /* cmd ::= SHOW USERS FULL */
#line 510 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithFull(pCxt, QUERY_NODE_SHOW_USERS_FULL_STMT); }
#line 6233 "./source/libs/parser/src/sql.c"
        break;
      case 273: /* cmd ::= SHOW USER PRIVILEGES */
#line 511 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USER_PRIVILEGES_STMT); }
#line 6238 "./source/libs/parser/src/sql.c"
        break;
      case 274: /* cmd ::= SHOW db_kind_opt DATABASES */
#line 512 "source/libs/parser/inc/sql.y"
{
                                                                                    pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT);
                                                                                    setShowKind(pCxt, pCxt->pRootNode, yymsp[-1].minor.yy477);
                                                                                  }
#line 6246 "./source/libs/parser/src/sql.c"
        break;
      case 275: /* cmd ::= SHOW table_kind_db_name_cond_opt TABLES like_pattern_opt */
#line 516 "source/libs/parser/inc/sql.y"
{
                                                                                    pCxt->pRootNode = createShowTablesStmt(pCxt, yymsp[-2].minor.yy1109, yymsp[0].minor.yy800, OP_TYPE_LIKE);
                                                                                  }
#line 6253 "./source/libs/parser/src/sql.c"
        break;
      case 276: /* cmd ::= SHOW db_name_cond_opt STABLES like_pattern_opt */
#line 519 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_STABLES_STMT, yymsp[-2].minor.yy800, yymsp[0].minor.yy800, OP_TYPE_LIKE); }
#line 6258 "./source/libs/parser/src/sql.c"
        break;
      case 277: /* cmd ::= SHOW db_name_cond_opt VGROUPS */
#line 520 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, yymsp[-1].minor.yy800, NULL, OP_TYPE_LIKE); }
#line 6263 "./source/libs/parser/src/sql.c"
        break;
      case 278: /* cmd ::= SHOW MNODES */
#line 521 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT); }
#line 6268 "./source/libs/parser/src/sql.c"
        break;
      case 279: /* cmd ::= SHOW QNODES */
#line 523 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT); }
#line 6273 "./source/libs/parser/src/sql.c"
        break;
      case 280: /* cmd ::= SHOW ARBGROUPS */
#line 524 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_ARBGROUPS_STMT); }
#line 6278 "./source/libs/parser/src/sql.c"
        break;
      case 281: /* cmd ::= SHOW FUNCTIONS */
#line 525 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_FUNCTIONS_STMT); }
#line 6283 "./source/libs/parser/src/sql.c"
        break;
      case 282: /* cmd ::= SHOW INDEXES FROM table_name_cond from_db_opt */
#line 526 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, yymsp[0].minor.yy800, yymsp[-1].minor.yy800, OP_TYPE_EQUAL); }
#line 6288 "./source/libs/parser/src/sql.c"
        break;
      case 283: /* cmd ::= SHOW INDEXES FROM db_name NK_DOT table_name */
#line 527 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_INDEXES_STMT, createIdentifierValueNode(pCxt, &yymsp[-2].minor.yy305), createIdentifierValueNode(pCxt, &yymsp[0].minor.yy305), OP_TYPE_EQUAL); }
#line 6293 "./source/libs/parser/src/sql.c"
        break;
      case 284: /* cmd ::= SHOW STREAMS */
#line 528 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STREAMS_STMT); }
#line 6298 "./source/libs/parser/src/sql.c"
        break;
      case 285: /* cmd ::= SHOW ACCOUNTS */
#line 529 "source/libs/parser/inc/sql.y"
{ pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
#line 6303 "./source/libs/parser/src/sql.c"
        break;
      case 286: /* cmd ::= SHOW APPS */
#line 530 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_APPS_STMT); }
#line 6308 "./source/libs/parser/src/sql.c"
        break;
      case 287: /* cmd ::= SHOW CONNECTIONS */
#line 531 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CONNECTIONS_STMT); }
#line 6313 "./source/libs/parser/src/sql.c"
        break;
      case 288: /* cmd ::= SHOW LICENCES */
      case 289: /* cmd ::= SHOW GRANTS */ yytestcase(yyruleno==289);
#line 532 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_LICENCES_STMT); }
#line 6319 "./source/libs/parser/src/sql.c"
        break;
      case 290: /* cmd ::= SHOW GRANTS FULL */
#line 534 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_FULL_STMT); }
#line 6324 "./source/libs/parser/src/sql.c"
        break;
      case 291: /* cmd ::= SHOW GRANTS LOGS */
#line 535 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_GRANTS_LOGS_STMT); }
#line 6329 "./source/libs/parser/src/sql.c"
        break;
      case 292: /* cmd ::= SHOW CLUSTER MACHINES */
#line 536 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT); }
#line 6334 "./source/libs/parser/src/sql.c"
        break;
      case 293: /* cmd ::= SHOW CREATE DATABASE db_name */
#line 537 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCreateDatabaseStmt(pCxt, &yymsp[0].minor.yy305); }
#line 6339 "./source/libs/parser/src/sql.c"
        break;
      case 294: /* cmd ::= SHOW CREATE TABLE full_table_name */
#line 538 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCreateTableStmt(pCxt, QUERY_NODE_SHOW_CREATE_TABLE_STMT, yymsp[0].minor.yy800); }
#line 6344 "./source/libs/parser/src/sql.c"
        break;
      case 295: /* cmd ::= SHOW CREATE STABLE full_table_name */
#line 539 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCreateTableStmt(pCxt, QUERY_NODE_SHOW_CREATE_STABLE_STMT,
yymsp[0].minor.yy800); }
#line 6350 "./source/libs/parser/src/sql.c"
        break;
      case 296: /* cmd ::= SHOW ENCRYPTIONS */
#line 541 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_ENCRYPTIONS_STMT); }
#line 6355 "./source/libs/parser/src/sql.c"
        break;
      case 297: /* cmd ::= SHOW QUERIES */
#line 542 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QUERIES_STMT); }
#line 6360 "./source/libs/parser/src/sql.c"
        break;
      case 298: /* cmd ::= SHOW SCORES */
#line 543 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SCORES_STMT); }
#line 6365 "./source/libs/parser/src/sql.c"
        break;
      case 299: /* cmd ::= SHOW TOPICS */
#line 544 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TOPICS_STMT); }
#line 6370 "./source/libs/parser/src/sql.c"
        break;
      case 300: /* cmd ::= SHOW VARIABLES */
      case 301: /* cmd ::= SHOW CLUSTER VARIABLES */ yytestcase(yyruleno==301);
#line 545 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VARIABLES_STMT); }
#line 6376 "./source/libs/parser/src/sql.c"
        break;
      case 302: /* cmd ::= SHOW LOCAL VARIABLES */
#line 547 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT); }
#line 6381 "./source/libs/parser/src/sql.c"
        break;
      case 303: /* cmd ::= SHOW DNODE NK_INTEGER VARIABLES like_pattern_opt */
#line 548 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowDnodeVariablesStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[-2].minor.yy0), yymsp[0].minor.yy800); }
#line 6386 "./source/libs/parser/src/sql.c"
        break;
      case 304: /* cmd ::= SHOW BNODES */
#line 549 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_BNODES_STMT); }
#line 6391 "./source/libs/parser/src/sql.c"
        break;
      case 305: /* cmd ::= SHOW SNODES */
#line 550 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SNODES_STMT); }
#line 6396 "./source/libs/parser/src/sql.c"
        break;
      case 306: /* cmd ::= SHOW CLUSTER */
#line 551 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CLUSTER_STMT); }
#line 6401 "./source/libs/parser/src/sql.c"
        break;
      case 307: /* cmd ::= SHOW TRANSACTIONS */
#line 552 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TRANSACTIONS_STMT); }
#line 6406 "./source/libs/parser/src/sql.c"
        break;
      case 308: /* cmd ::= SHOW TABLE DISTRIBUTED full_table_name */
#line 553 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowTableDistributedStmt(pCxt, yymsp[0].minor.yy800); }
#line 6411 "./source/libs/parser/src/sql.c"
        break;
      case 309: /* cmd ::= SHOW CONSUMERS */
#line 554 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_CONSUMERS_STMT); }
#line 6416 "./source/libs/parser/src/sql.c"
        break;
      case 310: /* cmd ::= SHOW SUBSCRIPTIONS */
#line 555 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT); }
#line 6421 "./source/libs/parser/src/sql.c"
        break;
      case 311: /* cmd ::= SHOW TAGS FROM table_name_cond from_db_opt */
#line 556 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, yymsp[0].minor.yy800, yymsp[-1].minor.yy800, OP_TYPE_EQUAL); }
#line 6426 "./source/libs/parser/src/sql.c"
        break;
      case 312: /* cmd ::= SHOW TAGS FROM db_name NK_DOT table_name */
#line 557 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_TAGS_STMT, createIdentifierValueNode(pCxt, &yymsp[-2].minor.yy305), createIdentifierValueNode(pCxt, &yymsp[0].minor.yy305), OP_TYPE_EQUAL); }
#line 6431 "./source/libs/parser/src/sql.c"
        break;
      case 313: /* cmd ::= SHOW TABLE TAGS tag_list_opt FROM table_name_cond from_db_opt */
#line 558 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowTableTagsStmt(pCxt, yymsp[-1].minor.yy800, yymsp[0].minor.yy800, yymsp[-3].minor.yy436); }
#line 6436 "./source/libs/parser/src/sql.c"
        break;
      case 314: /* cmd ::= SHOW TABLE TAGS tag_list_opt FROM db_name NK_DOT table_name */
#line 559 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowTableTagsStmt(pCxt, createIdentifierValueNode(pCxt, &yymsp[0].minor.yy305), createIdentifierValueNode(pCxt, &yymsp[-2].minor.yy305), yymsp[-4].minor.yy436); }
#line 6441 "./source/libs/parser/src/sql.c"
        break;
      case 315: /* cmd ::= SHOW VNODES ON DNODE NK_INTEGER */
#line 560 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowVnodesStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0), NULL); }
#line 6446 "./source/libs/parser/src/sql.c"
        break;
      case 316: /* cmd ::= SHOW VNODES */
#line 561 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowVnodesStmt(pCxt, NULL, NULL); }
#line 6451 "./source/libs/parser/src/sql.c"
        break;
      case 317: /* cmd ::= SHOW db_name_cond_opt ALIVE */
#line 563 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowAliveStmt(pCxt, yymsp[-1].minor.yy800,    QUERY_NODE_SHOW_DB_ALIVE_STMT); }
#line 6456 "./source/libs/parser/src/sql.c"
        break;
      case 318: /* cmd ::= SHOW CLUSTER ALIVE */
#line 564 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowAliveStmt(pCxt, NULL, QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT); }
#line 6461 "./source/libs/parser/src/sql.c"
        break;
      case 319: /* cmd ::= SHOW db_name_cond_opt VIEWS like_pattern_opt */
#line 565 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowStmtWithCond(pCxt, QUERY_NODE_SHOW_VIEWS_STMT, yymsp[-2].minor.yy800, yymsp[0].minor.yy800, OP_TYPE_LIKE); }
#line 6466 "./source/libs/parser/src/sql.c"
        break;
      case 320: /* cmd ::= SHOW CREATE VIEW full_table_name */
#line 566 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCreateViewStmt(pCxt, QUERY_NODE_SHOW_CREATE_VIEW_STMT, yymsp[0].minor.yy800); }
#line 6471 "./source/libs/parser/src/sql.c"
        break;
      case 321: /* cmd ::= SHOW COMPACTS */
#line 567 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCompactsStmt(pCxt, QUERY_NODE_SHOW_COMPACTS_STMT); }
#line 6476 "./source/libs/parser/src/sql.c"
        break;
      case 322: /* cmd ::= SHOW COMPACT NK_INTEGER */
#line 568 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowCompactDetailsStmt(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
#line 6481 "./source/libs/parser/src/sql.c"
        break;
      case 323: /* table_kind_db_name_cond_opt ::= */
#line 572 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy1109.kind = SHOW_KIND_ALL; yymsp[1].minor.yy1109.dbName = nil_token; }
#line 6486 "./source/libs/parser/src/sql.c"
        break;
      case 324: /* table_kind_db_name_cond_opt ::= table_kind */
#line 573 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy1109.kind = yymsp[0].minor.yy477; yylhsminor.yy1109.dbName = nil_token; }
#line 6491 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy1109 = yylhsminor.yy1109;
        break;
      case 325: /* table_kind_db_name_cond_opt ::= db_name NK_DOT */
#line 574 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy1109.kind = SHOW_KIND_ALL; yylhsminor.yy1109.dbName = yymsp[-1].minor.yy305; }
#line 6497 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy1109 = yylhsminor.yy1109;
        break;
      case 326: /* table_kind_db_name_cond_opt ::= table_kind db_name NK_DOT */
#line 575 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy1109.kind = yymsp[-2].minor.yy477; yylhsminor.yy1109.dbName = yymsp[-1].minor.yy305; }
#line 6503 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy1109 = yylhsminor.yy1109;
        break;
      case 327: /* table_kind ::= NORMAL */
#line 579 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy477 = SHOW_KIND_TABLES_NORMAL; }
#line 6509 "./source/libs/parser/src/sql.c"
        break;
      case 328: /* table_kind ::= CHILD */
#line 580 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy477 = SHOW_KIND_TABLES_CHILD; }
#line 6514 "./source/libs/parser/src/sql.c"
        break;
      case 329: /* db_name_cond_opt ::= */
      case 334: /* from_db_opt ::= */ yytestcase(yyruleno==334);
#line 582 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createDefaultDatabaseCondValue(pCxt); }
#line 6520 "./source/libs/parser/src/sql.c"
        break;
      case 330: /* db_name_cond_opt ::= db_name NK_DOT */
#line 583 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createIdentifierValueNode(pCxt, &yymsp[-1].minor.yy305); }
#line 6525 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 332: /* like_pattern_opt ::= LIKE NK_STRING */
#line 586 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
#line 6531 "./source/libs/parser/src/sql.c"
        break;
      case 333: /* table_name_cond ::= table_name */
#line 588 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createIdentifierValueNode(pCxt, &yymsp[0].minor.yy305); }
#line 6536 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 335: /* from_db_opt ::= FROM db_name */
#line 591 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createIdentifierValueNode(pCxt, &yymsp[0].minor.yy305); }
#line 6542 "./source/libs/parser/src/sql.c"
        break;
      case 339: /* tag_item ::= TBNAME */
#line 599 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setProjectionAlias(pCxt, createFunctionNode(pCxt, &yymsp[0].minor.yy0, NULL), &yymsp[0].minor.yy0); }
#line 6547 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 342: /* tag_item ::= column_name column_alias */
#line 602 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setProjectionAlias(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy305), &yymsp[0].minor.yy305); }
#line 6553 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 343: /* tag_item ::= column_name AS column_alias */
#line 603 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setProjectionAlias(pCxt, createColumnNode(pCxt, NULL, &yymsp[-2].minor.yy305), &yymsp[0].minor.yy305); }
#line 6559 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 344: /* db_kind_opt ::= */
#line 607 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy477 = SHOW_KIND_ALL; }
#line 6565 "./source/libs/parser/src/sql.c"
        break;
      case 345: /* db_kind_opt ::= USER */
#line 608 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy477 = SHOW_KIND_DATABASES_USER; }
#line 6570 "./source/libs/parser/src/sql.c"
        break;
      case 346: /* db_kind_opt ::= SYSTEM */
#line 609 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy477 = SHOW_KIND_DATABASES_SYSTEM; }
#line 6575 "./source/libs/parser/src/sql.c"
        break;
      case 347: /* cmd ::= CREATE TSMA not_exists_opt tsma_name ON full_table_name tsma_func_list INTERVAL NK_LP duration_literal NK_RP */
#line 615 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTSMAStmt(pCxt, yymsp[-8].minor.yy125, &yymsp[-7].minor.yy305, yymsp[-4].minor.yy800, yymsp[-5].minor.yy800, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 6580 "./source/libs/parser/src/sql.c"
        break;
      case 348: /* cmd ::= CREATE RECURSIVE TSMA not_exists_opt tsma_name ON full_table_name INTERVAL NK_LP duration_literal NK_RP */
#line 617 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTSMAStmt(pCxt, yymsp[-7].minor.yy125, &yymsp[-6].minor.yy305, NULL, yymsp[-4].minor.yy800, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 6585 "./source/libs/parser/src/sql.c"
        break;
      case 349: /* cmd ::= DROP TSMA exists_opt full_tsma_name */
#line 618 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropTSMAStmt(pCxt, yymsp[-1].minor.yy125, yymsp[0].minor.yy800); }
#line 6590 "./source/libs/parser/src/sql.c"
        break;
      case 350: /* cmd ::= SHOW db_name_cond_opt TSMAS */
#line 619 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createShowTSMASStmt(pCxt, yymsp[-1].minor.yy800); }
#line 6595 "./source/libs/parser/src/sql.c"
        break;
      case 353: /* tsma_func_list ::= FUNCTION NK_LP func_list NK_RP */
#line 626 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createTSMAOptions(pCxt, yymsp[-1].minor.yy436); }
#line 6600 "./source/libs/parser/src/sql.c"
        break;
      case 354: /* cmd ::= CREATE SMA INDEX not_exists_opt col_name ON full_table_name index_options */
#line 630 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, yymsp[-4].minor.yy125, yymsp[-3].minor.yy800, yymsp[-1].minor.yy800, NULL, yymsp[0].minor.yy800); }
#line 6605 "./source/libs/parser/src/sql.c"
        break;
      case 355: /* cmd ::= CREATE INDEX not_exists_opt col_name ON full_table_name NK_LP col_name_list NK_RP */
#line 632 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_NORMAL, yymsp[-6].minor.yy125, yymsp[-5].minor.yy800, yymsp[-3].minor.yy800, yymsp[-1].minor.yy436, NULL); }
#line 6610 "./source/libs/parser/src/sql.c"
        break;
      case 356: /* cmd ::= DROP INDEX exists_opt full_index_name */
#line 633 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropIndexStmt(pCxt, yymsp[-1].minor.yy125, yymsp[0].minor.yy800); }
#line 6615 "./source/libs/parser/src/sql.c"
        break;
      case 357: /* full_index_name ::= index_name */
#line 635 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNodeForIndexName(pCxt, NULL, &yymsp[0].minor.yy305); }
#line 6620 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 358: /* full_index_name ::= db_name NK_DOT index_name */
#line 636 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNodeForIndexName(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305); }
#line 6626 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 359: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt sma_stream_opt */
#line 639 "source/libs/parser/inc/sql.y"
{ yymsp[-9].minor.yy800 = createIndexOption(pCxt, yymsp[-7].minor.yy436, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), NULL, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 6632 "./source/libs/parser/src/sql.c"
        break;
      case 360: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt sma_stream_opt */
#line 642 "source/libs/parser/inc/sql.y"
{ yymsp[-11].minor.yy800 = createIndexOption(pCxt, yymsp[-9].minor.yy436, releaseRawExprNode(pCxt, yymsp[-5].minor.yy800), releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 6637 "./source/libs/parser/src/sql.c"
        break;
      case 363: /* func ::= sma_func_name NK_LP expression_list NK_RP */
#line 649 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createFunctionNode(pCxt, &yymsp[-3].minor.yy305, yymsp[-1].minor.yy436); }
#line 6642 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 364: /* sma_func_name ::= function_name */
      case 641: /* alias_opt ::= table_alias */ yytestcase(yyruleno==641);
#line 653 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy305 = yymsp[0].minor.yy305; }
#line 6649 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy305 = yylhsminor.yy305;
        break;
      case 369: /* sma_stream_opt ::= */
      case 419: /* stream_options ::= */ yytestcase(yyruleno==419);
#line 659 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createStreamOptions(pCxt); }
#line 6656 "./source/libs/parser/src/sql.c"
        break;
      case 370: /* sma_stream_opt ::= sma_stream_opt WATERMARK duration_literal */
#line 660 "source/libs/parser/inc/sql.y"
{ ((SStreamOptions*)yymsp[-2].minor.yy800)->pWatermark = releaseRawExprNode(pCxt, yymsp[0].minor.yy800); yylhsminor.yy800 = yymsp[-2].minor.yy800; }
#line 6661 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 371: /* sma_stream_opt ::= sma_stream_opt MAX_DELAY duration_literal */
#line 661 "source/libs/parser/inc/sql.y"
{ ((SStreamOptions*)yymsp[-2].minor.yy800)->pDelay = releaseRawExprNode(pCxt, yymsp[0].minor.yy800); yylhsminor.yy800 = yymsp[-2].minor.yy800; }
#line 6667 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 372: /* sma_stream_opt ::= sma_stream_opt DELETE_MARK duration_literal */
#line 662 "source/libs/parser/inc/sql.y"
{ ((SStreamOptions*)yymsp[-2].minor.yy800)->pDeleteMark = releaseRawExprNode(pCxt, yymsp[0].minor.yy800); yylhsminor.yy800 = yymsp[-2].minor.yy800; }
#line 6673 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 373: /* with_meta ::= AS */
#line 667 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy564 = 0; }
#line 6679 "./source/libs/parser/src/sql.c"
        break;
      case 374: /* with_meta ::= WITH META AS */
#line 668 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy564 = 1; }
#line 6684 "./source/libs/parser/src/sql.c"
        break;
      case 375: /* with_meta ::= ONLY META AS */
#line 669 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy564 = 2; }
#line 6689 "./source/libs/parser/src/sql.c"
        break;
      case 376: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_or_subquery */
#line 671 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTopicStmtUseQuery(pCxt, yymsp[-3].minor.yy125, &yymsp[-2].minor.yy305, yymsp[0].minor.yy800); }
#line 6694 "./source/libs/parser/src/sql.c"
        break;
      case 377: /* cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta DATABASE db_name */
#line 673 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTopicStmtUseDb(pCxt, yymsp[-4].minor.yy125, &yymsp[-3].minor.yy305, &yymsp[0].minor.yy305, yymsp[-2].minor.yy564); }
#line 6699 "./source/libs/parser/src/sql.c"
        break;
      case 378: /* cmd ::= CREATE TOPIC not_exists_opt topic_name with_meta STABLE full_table_name where_clause_opt */
#line 675 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateTopicStmtUseTable(pCxt, yymsp[-5].minor.yy125, &yymsp[-4].minor.yy305, yymsp[-1].minor.yy800, yymsp[-3].minor.yy564, yymsp[0].minor.yy800); }
#line 6704 "./source/libs/parser/src/sql.c"
        break;
      case 379: /* cmd ::= DROP TOPIC exists_opt topic_name */
#line 677 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 6709 "./source/libs/parser/src/sql.c"
        break;
      case 380: /* cmd ::= DROP CONSUMER GROUP exists_opt cgroup_name ON topic_name */
#line 678 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropCGroupStmt(pCxt, yymsp[-3].minor.yy125, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305); }
#line 6714 "./source/libs/parser/src/sql.c"
        break;
      case 381: /* cmd ::= DESC full_table_name */
      case 382: /* cmd ::= DESCRIBE full_table_name */ yytestcase(yyruleno==382);
#line 681 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDescribeStmt(pCxt, yymsp[0].minor.yy800); }
#line 6720 "./source/libs/parser/src/sql.c"
        break;
      case 383: /* cmd ::= RESET QUERY CACHE */
#line 685 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createResetQueryCacheStmt(pCxt); }
#line 6725 "./source/libs/parser/src/sql.c"
        break;
      case 384: /* cmd ::= EXPLAIN analyze_opt explain_options query_or_subquery */
      case 385: /* cmd ::= EXPLAIN analyze_opt explain_options insert_query */ yytestcase(yyruleno==385);
#line 688 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createExplainStmt(pCxt, yymsp[-2].minor.yy125, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 6731 "./source/libs/parser/src/sql.c"
        break;
      case 388: /* explain_options ::= */
#line 696 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createDefaultExplainOptions(pCxt); }
#line 6736 "./source/libs/parser/src/sql.c"
        break;
      case 389: /* explain_options ::= explain_options VERBOSE NK_BOOL */
#line 697 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setExplainVerbose(pCxt, yymsp[-2].minor.yy800, &yymsp[0].minor.yy0); }
#line 6741 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 390: /* explain_options ::= explain_options RATIO NK_FLOAT */
#line 698 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setExplainRatio(pCxt, yymsp[-2].minor.yy800, &yymsp[0].minor.yy0); }
#line 6747 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 391: /* cmd ::= CREATE or_replace_opt agg_func_opt FUNCTION not_exists_opt function_name AS NK_STRING OUTPUTTYPE type_name bufsize_opt language_opt */
#line 703 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateFunctionStmt(pCxt, yymsp[-7].minor.yy125, yymsp[-9].minor.yy125, &yymsp[-6].minor.yy305, &yymsp[-4].minor.yy0, yymsp[-2].minor.yy96, yymsp[-1].minor.yy564, &yymsp[0].minor.yy305, yymsp[-10].minor.yy125); }
#line 6753 "./source/libs/parser/src/sql.c"
        break;
      case 392: /* cmd ::= DROP FUNCTION exists_opt function_name */
#line 704 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropFunctionStmt(pCxt, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 6758 "./source/libs/parser/src/sql.c"
        break;
      case 397: /* language_opt ::= */
      case 442: /* on_vgroup_id ::= */ yytestcase(yyruleno==442);
#line 718 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy305 = nil_token; }
#line 6764 "./source/libs/parser/src/sql.c"
        break;
      case 398: /* language_opt ::= LANGUAGE NK_STRING */
      case 443: /* on_vgroup_id ::= ON NK_INTEGER */ yytestcase(yyruleno==443);
#line 719 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy305 = yymsp[0].minor.yy0; }
#line 6770 "./source/libs/parser/src/sql.c"
        break;
      case 401: /* cmd ::= CREATE or_replace_opt VIEW full_view_name AS query_or_subquery */
#line 728 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateViewStmt(pCxt, yymsp[-4].minor.yy125, yymsp[-2].minor.yy800, &yymsp[-1].minor.yy0, yymsp[0].minor.yy800); }
#line 6775 "./source/libs/parser/src/sql.c"
        break;
      case 402: /* cmd ::= DROP VIEW exists_opt full_view_name */
#line 729 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropViewStmt(pCxt, yymsp[-1].minor.yy125, yymsp[0].minor.yy800); }
#line 6780 "./source/libs/parser/src/sql.c"
        break;
      case 403: /* full_view_name ::= view_name */
#line 731 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createViewNode(pCxt, NULL, &yymsp[0].minor.yy305); }
#line 6785 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 404: /* full_view_name ::= db_name NK_DOT view_name */
#line 732 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createViewNode(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305); }
#line 6791 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 405: /* cmd ::= CREATE STREAM not_exists_opt stream_name stream_options INTO full_table_name col_list_opt tag_def_or_ref_opt subtable_opt AS query_or_subquery */
#line 737 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createCreateStreamStmt(pCxt, yymsp[-9].minor.yy125, &yymsp[-8].minor.yy305, yymsp[-5].minor.yy800, yymsp[-7].minor.yy800, yymsp[-3].minor.yy436, yymsp[-2].minor.yy800, yymsp[0].minor.yy800, yymsp[-4].minor.yy436); }
#line 6797 "./source/libs/parser/src/sql.c"
        break;
      case 406: /* cmd ::= DROP STREAM exists_opt stream_name */
#line 738 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDropStreamStmt(pCxt, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 6802 "./source/libs/parser/src/sql.c"
        break;
      case 407: /* cmd ::= PAUSE STREAM exists_opt stream_name */
#line 739 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createPauseStreamStmt(pCxt, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 6807 "./source/libs/parser/src/sql.c"
        break;
      case 408: /* cmd ::= RESUME STREAM exists_opt ignore_opt stream_name */
#line 740 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createResumeStreamStmt(pCxt, yymsp[-2].minor.yy125, yymsp[-1].minor.yy125, &yymsp[0].minor.yy305); }
#line 6812 "./source/libs/parser/src/sql.c"
        break;
      case 413: /* column_stream_def ::= column_name stream_col_options */
#line 753 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy305, createDataType(TSDB_DATA_TYPE_NULL), yymsp[0].minor.yy800); }
#line 6817 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 414: /* stream_col_options ::= */
      case 754: /* column_options ::= */ yytestcase(yyruleno==754);
#line 754 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy800 = createDefaultColumnOptions(pCxt); }
#line 6824 "./source/libs/parser/src/sql.c"
        break;
      case 415: /* stream_col_options ::= stream_col_options PRIMARY KEY */
      case 755: /* column_options ::= column_options PRIMARY KEY */ yytestcase(yyruleno==755);
#line 755 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setColumnOptions(pCxt, yymsp[-2].minor.yy800, COLUMN_OPTION_PRIMARYKEY, NULL); }
#line 6830 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 420: /* stream_options ::= stream_options TRIGGER AT_ONCE */
      case 421: /* stream_options ::= stream_options TRIGGER WINDOW_CLOSE */ yytestcase(yyruleno==421);
#line 765 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-2].minor.yy800, SOPT_TRIGGER_TYPE_SET, &yymsp[0].minor.yy0, NULL); }
#line 6837 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 422: /* stream_options ::= stream_options TRIGGER MAX_DELAY duration_literal */
#line 767 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-3].minor.yy800, SOPT_TRIGGER_TYPE_SET, &yymsp[-1].minor.yy0, releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 6843 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 423: /* stream_options ::= stream_options WATERMARK duration_literal */
#line 768 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-2].minor.yy800, SOPT_WATERMARK_SET, NULL, releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 6849 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 424: /* stream_options ::= stream_options IGNORE EXPIRED NK_INTEGER */
#line 769 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-3].minor.yy800, SOPT_IGNORE_EXPIRED_SET, &yymsp[0].minor.yy0, NULL); }
#line 6855 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 425: /* stream_options ::= stream_options FILL_HISTORY NK_INTEGER */
#line 770 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-2].minor.yy800, SOPT_FILL_HISTORY_SET, &yymsp[0].minor.yy0, NULL); }
#line 6861 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 426: /* stream_options ::= stream_options DELETE_MARK duration_literal */
#line 771 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-2].minor.yy800, SOPT_DELETE_MARK_SET, NULL, releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 6867 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 427: /* stream_options ::= stream_options IGNORE UPDATE NK_INTEGER */
#line 772 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setStreamOptions(pCxt, yymsp[-3].minor.yy800, SOPT_IGNORE_UPDATE_SET, &yymsp[0].minor.yy0, NULL); }
#line 6873 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 429: /* subtable_opt ::= SUBTABLE NK_LP expression NK_RP */
      case 698: /* sliding_opt ::= SLIDING NK_LP interval_sliding_duration_literal NK_RP */ yytestcase(yyruleno==698);
      case 722: /* every_opt ::= EVERY NK_LP duration_literal NK_RP */ yytestcase(yyruleno==722);
#line 775 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy800); }
#line 6881 "./source/libs/parser/src/sql.c"
        break;
      case 432: /* cmd ::= KILL CONNECTION NK_INTEGER */
#line 783 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_CONNECTION_STMT, &yymsp[0].minor.yy0); }
#line 6886 "./source/libs/parser/src/sql.c"
        break;
      case 433: /* cmd ::= KILL QUERY NK_STRING */
#line 784 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createKillQueryStmt(pCxt, &yymsp[0].minor.yy0); }
#line 6891 "./source/libs/parser/src/sql.c"
        break;
      case 434: /* cmd ::= KILL TRANSACTION NK_INTEGER */
#line 785 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_TRANSACTION_STMT, &yymsp[0].minor.yy0); }
#line 6896 "./source/libs/parser/src/sql.c"
        break;
      case 435: /* cmd ::= KILL COMPACT NK_INTEGER */
#line 786 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createKillStmt(pCxt, QUERY_NODE_KILL_COMPACT_STMT, &yymsp[0].minor.yy0); }
#line 6901 "./source/libs/parser/src/sql.c"
        break;
      case 436: /* cmd ::= BALANCE VGROUP */
#line 789 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createBalanceVgroupStmt(pCxt); }
#line 6906 "./source/libs/parser/src/sql.c"
        break;
      case 437: /* cmd ::= BALANCE VGROUP LEADER on_vgroup_id */
#line 790 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createBalanceVgroupLeaderStmt(pCxt, &yymsp[0].minor.yy305); }
#line 6911 "./source/libs/parser/src/sql.c"
        break;
      case 438: /* cmd ::= BALANCE VGROUP LEADER DATABASE db_name */
#line 791 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createBalanceVgroupLeaderDBNameStmt(pCxt, &yymsp[0].minor.yy305); }
#line 6916 "./source/libs/parser/src/sql.c"
        break;
      case 439: /* cmd ::= MERGE VGROUP NK_INTEGER NK_INTEGER */
#line 792 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createMergeVgroupStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
#line 6921 "./source/libs/parser/src/sql.c"
        break;
      case 440: /* cmd ::= REDISTRIBUTE VGROUP NK_INTEGER dnode_list */
#line 793 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createRedistributeVgroupStmt(pCxt, &yymsp[-1].minor.yy0, yymsp[0].minor.yy436); }
#line 6926 "./source/libs/parser/src/sql.c"
        break;
      case 441: /* cmd ::= SPLIT VGROUP NK_INTEGER */
#line 794 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createSplitVgroupStmt(pCxt, &yymsp[0].minor.yy0); }
#line 6931 "./source/libs/parser/src/sql.c"
        break;
      case 444: /* dnode_list ::= DNODE NK_INTEGER */
#line 803 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy436 = createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
#line 6936 "./source/libs/parser/src/sql.c"
        break;
      case 446: /* cmd ::= DELETE FROM full_table_name where_clause_opt */
#line 810 "source/libs/parser/inc/sql.y"
{ pCxt->pRootNode = createDeleteStmt(pCxt, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 6941 "./source/libs/parser/src/sql.c"
        break;
      case 449: /* insert_query ::= INSERT INTO full_table_name NK_LP col_name_list NK_RP query_or_subquery */
#line 819 "source/libs/parser/inc/sql.y"
{ yymsp[-6].minor.yy800 = createInsertStmt(pCxt, yymsp[-4].minor.yy800, yymsp[-2].minor.yy436, yymsp[0].minor.yy800); }
#line 6946 "./source/libs/parser/src/sql.c"
        break;
      case 450: /* insert_query ::= INSERT INTO full_table_name query_or_subquery */
#line 820 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createInsertStmt(pCxt, yymsp[-1].minor.yy800, NULL, yymsp[0].minor.yy800); }
#line 6951 "./source/libs/parser/src/sql.c"
        break;
      case 451: /* tags_literal ::= NK_INTEGER */
      case 463: /* tags_literal ::= NK_BIN */ yytestcase(yyruleno==463);
      case 472: /* tags_literal ::= NK_HEX */ yytestcase(yyruleno==472);
#line 823 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &yymsp[0].minor.yy0, NULL); }
#line 6958 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 452: /* tags_literal ::= NK_INTEGER NK_PLUS duration_literal */
      case 453: /* tags_literal ::= NK_INTEGER NK_MINUS duration_literal */ yytestcase(yyruleno==453);
      case 464: /* tags_literal ::= NK_BIN NK_PLUS duration_literal */ yytestcase(yyruleno==464);
      case 465: /* tags_literal ::= NK_BIN NK_MINUS duration_literal */ yytestcase(yyruleno==465);
      case 473: /* tags_literal ::= NK_HEX NK_PLUS duration_literal */ yytestcase(yyruleno==473);
      case 474: /* tags_literal ::= NK_HEX NK_MINUS duration_literal */ yytestcase(yyruleno==474);
      case 482: /* tags_literal ::= NK_STRING NK_PLUS duration_literal */ yytestcase(yyruleno==482);
      case 483: /* tags_literal ::= NK_STRING NK_MINUS duration_literal */ yytestcase(yyruleno==483);
#line 824 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken l = yymsp[-2].minor.yy0;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    yylhsminor.yy800 = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, yymsp[0].minor.yy800);
                                                                                  }
#line 6976 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 454: /* tags_literal ::= NK_PLUS NK_INTEGER */
      case 457: /* tags_literal ::= NK_MINUS NK_INTEGER */ yytestcase(yyruleno==457);
      case 466: /* tags_literal ::= NK_PLUS NK_BIN */ yytestcase(yyruleno==466);
      case 469: /* tags_literal ::= NK_MINUS NK_BIN */ yytestcase(yyruleno==469);
      case 475: /* tags_literal ::= NK_PLUS NK_HEX */ yytestcase(yyruleno==475);
      case 478: /* tags_literal ::= NK_MINUS NK_HEX */ yytestcase(yyruleno==478);
#line 836 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &t, NULL);
                                                                                  }
#line 6991 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 455: /* tags_literal ::= NK_PLUS NK_INTEGER NK_PLUS duration_literal */
      case 456: /* tags_literal ::= NK_PLUS NK_INTEGER NK_MINUS duration_literal */ yytestcase(yyruleno==456);
      case 458: /* tags_literal ::= NK_MINUS NK_INTEGER NK_PLUS duration_literal */ yytestcase(yyruleno==458);
      case 459: /* tags_literal ::= NK_MINUS NK_INTEGER NK_MINUS duration_literal */ yytestcase(yyruleno==459);
      case 467: /* tags_literal ::= NK_PLUS NK_BIN NK_PLUS duration_literal */ yytestcase(yyruleno==467);
      case 468: /* tags_literal ::= NK_PLUS NK_BIN NK_MINUS duration_literal */ yytestcase(yyruleno==468);
      case 470: /* tags_literal ::= NK_MINUS NK_BIN NK_PLUS duration_literal */ yytestcase(yyruleno==470);
      case 471: /* tags_literal ::= NK_MINUS NK_BIN NK_MINUS duration_literal */ yytestcase(yyruleno==471);
      case 476: /* tags_literal ::= NK_PLUS NK_HEX NK_PLUS duration_literal */ yytestcase(yyruleno==476);
      case 477: /* tags_literal ::= NK_PLUS NK_HEX NK_MINUS duration_literal */ yytestcase(yyruleno==477);
      case 479: /* tags_literal ::= NK_MINUS NK_HEX NK_PLUS duration_literal */ yytestcase(yyruleno==479);
      case 480: /* tags_literal ::= NK_MINUS NK_HEX NK_MINUS duration_literal */ yytestcase(yyruleno==480);
#line 841 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken l = yymsp[-3].minor.yy0;
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    yylhsminor.yy800 = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, NULL, yymsp[0].minor.yy800);
                                                                                  }
#line 7013 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 460: /* tags_literal ::= NK_FLOAT */
#line 870 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0, NULL); }
#line 7019 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 461: /* tags_literal ::= NK_PLUS NK_FLOAT */
      case 462: /* tags_literal ::= NK_MINUS NK_FLOAT */ yytestcase(yyruleno==462);
#line 871 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t, NULL);
                                                                                  }
#line 7030 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 481: /* tags_literal ::= NK_STRING */
#line 977 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0, NULL); }
#line 7036 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 484: /* tags_literal ::= NK_BOOL */
#line 990 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0, NULL); }
#line 7042 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 485: /* tags_literal ::= NULL */
#line 991 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_NULL, &yymsp[0].minor.yy0, NULL); }
#line 7048 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 486: /* tags_literal ::= literal_func */
#line 993 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawValueNode(pCxt, TSDB_DATA_TYPE_BINARY, NULL, yymsp[0].minor.yy800); }
#line 7054 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 487: /* tags_literal ::= literal_func NK_PLUS duration_literal */
      case 488: /* tags_literal ::= literal_func NK_MINUS duration_literal */ yytestcase(yyruleno==488);
#line 994 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken l = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken r = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    l.n = (r.z + r.n) - l.z;
                                                                                    yylhsminor.yy800 = createRawValueNodeExt(pCxt, TSDB_DATA_TYPE_BINARY, &l, yymsp[-2].minor.yy800, yymsp[0].minor.yy800);
                                                                                  }
#line 7066 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 491: /* literal ::= NK_INTEGER */
#line 1013 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &yymsp[0].minor.yy0)); }
#line 7072 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 492: /* literal ::= NK_FLOAT */
#line 1014 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
#line 7078 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 493: /* literal ::= NK_STRING */
#line 1015 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
#line 7084 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 494: /* literal ::= NK_BOOL */
#line 1016 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
#line 7090 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 495: /* literal ::= TIMESTAMP NK_STRING */
#line 1017 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
#line 7096 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 496: /* literal ::= duration_literal */
      case 506: /* signed_literal ::= signed */ yytestcase(yyruleno==506);
      case 530: /* expr_or_subquery ::= expression */ yytestcase(yyruleno==530);
      case 531: /* expression ::= literal */ yytestcase(yyruleno==531);
      case 533: /* expression ::= column_reference */ yytestcase(yyruleno==533);
      case 534: /* expression ::= function_expression */ yytestcase(yyruleno==534);
      case 535: /* expression ::= case_when_expression */ yytestcase(yyruleno==535);
      case 572: /* function_expression ::= literal_func */ yytestcase(yyruleno==572);
      case 622: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==622);
      case 626: /* boolean_primary ::= predicate */ yytestcase(yyruleno==626);
      case 628: /* common_expression ::= expr_or_subquery */ yytestcase(yyruleno==628);
      case 629: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==629);
      case 632: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==632);
      case 634: /* table_reference ::= table_primary */ yytestcase(yyruleno==634);
      case 635: /* table_reference ::= joined_table */ yytestcase(yyruleno==635);
      case 639: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==639);
      case 724: /* query_simple ::= query_specification */ yytestcase(yyruleno==724);
      case 725: /* query_simple ::= union_query_expression */ yytestcase(yyruleno==725);
      case 728: /* query_simple_or_subquery ::= query_simple */ yytestcase(yyruleno==728);
      case 730: /* query_or_subquery ::= query_expression */ yytestcase(yyruleno==730);
#line 1018 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = yymsp[0].minor.yy800; }
#line 7121 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 497: /* literal ::= NULL */
#line 1019 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_NULL, &yymsp[0].minor.yy0)); }
#line 7127 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 498: /* literal ::= NK_QUESTION */
#line 1020 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createPlaceholderValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 7133 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 499: /* duration_literal ::= NK_VARIABLE */
      case 699: /* interval_sliding_duration_literal ::= NK_VARIABLE */ yytestcase(yyruleno==699);
      case 700: /* interval_sliding_duration_literal ::= NK_STRING */ yytestcase(yyruleno==700);
      case 701: /* interval_sliding_duration_literal ::= NK_INTEGER */ yytestcase(yyruleno==701);
#line 1022 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 7142 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 500: /* signed ::= NK_INTEGER */
#line 1024 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &yymsp[0].minor.yy0); }
#line 7148 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 501: /* signed ::= NK_PLUS NK_INTEGER */
#line 1025 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_UBIGINT, &yymsp[0].minor.yy0); }
#line 7154 "./source/libs/parser/src/sql.c"
        break;
      case 502: /* signed ::= NK_MINUS NK_INTEGER */
#line 1026 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &t);
                                                                                  }
#line 7163 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 503: /* signed ::= NK_FLOAT */
#line 1031 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
#line 7169 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 504: /* signed ::= NK_PLUS NK_FLOAT */
#line 1032 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0); }
#line 7175 "./source/libs/parser/src/sql.c"
        break;
      case 505: /* signed ::= NK_MINUS NK_FLOAT */
#line 1033 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &t);
                                                                                  }
#line 7184 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 507: /* signed_literal ::= NK_STRING */
#line 1040 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0); }
#line 7190 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 508: /* signed_literal ::= NK_BOOL */
#line 1041 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0); }
#line 7196 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 509: /* signed_literal ::= TIMESTAMP NK_STRING */
#line 1042 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0); }
#line 7202 "./source/libs/parser/src/sql.c"
        break;
      case 510: /* signed_literal ::= duration_literal */
      case 512: /* signed_literal ::= literal_func */ yytestcase(yyruleno==512);
      case 593: /* star_func_para ::= expr_or_subquery */ yytestcase(yyruleno==593);
      case 676: /* select_item ::= common_expression */ yytestcase(yyruleno==676);
      case 686: /* partition_item ::= expr_or_subquery */ yytestcase(yyruleno==686);
      case 729: /* query_simple_or_subquery ::= subquery */ yytestcase(yyruleno==729);
      case 731: /* query_or_subquery ::= subquery */ yytestcase(yyruleno==731);
      case 744: /* search_condition ::= common_expression */ yytestcase(yyruleno==744);
#line 1043 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = releaseRawExprNode(pCxt, yymsp[0].minor.yy800); }
#line 7214 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 511: /* signed_literal ::= NULL */
#line 1044 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createValueNode(pCxt, TSDB_DATA_TYPE_NULL, &yymsp[0].minor.yy0); }
#line 7220 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 513: /* signed_literal ::= NK_QUESTION */
#line 1046 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createPlaceholderValueNode(pCxt, &yymsp[0].minor.yy0); }
#line 7226 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 532: /* expression ::= pseudo_column */
#line 1112 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = yymsp[0].minor.yy800; setRawExprNodeIsPseudoColumn(pCxt, yylhsminor.yy800, true); }
#line 7232 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 536: /* expression ::= NK_LP expression NK_RP */
      case 627: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==627);
      case 743: /* subquery ::= NK_LP subquery NK_RP */ yytestcase(yyruleno==743);
#line 1116 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7240 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 537: /* expression ::= NK_PLUS expr_or_subquery */
#line 1117 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy800));
                                                                                  }
#line 7249 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 538: /* expression ::= NK_MINUS expr_or_subquery */
#line 1121 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_MINUS, releaseRawExprNode(pCxt, yymsp[0].minor.yy800), NULL));
                                                                                  }
#line 7258 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 539: /* expression ::= expr_or_subquery NK_PLUS expr_or_subquery */
#line 1125 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7268 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 540: /* expression ::= expr_or_subquery NK_MINUS expr_or_subquery */
#line 1130 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7278 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 541: /* expression ::= expr_or_subquery NK_STAR expr_or_subquery */
#line 1135 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7288 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 542: /* expression ::= expr_or_subquery NK_SLASH expr_or_subquery */
#line 1140 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7298 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 543: /* expression ::= expr_or_subquery NK_REM expr_or_subquery */
#line 1145 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_REM, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7308 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 544: /* expression ::= column_reference NK_ARROW NK_STRING */
#line 1150 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_JSON_GET_VALUE, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)));
                                                                                  }
#line 7317 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 545: /* expression ::= expr_or_subquery NK_BITAND expr_or_subquery */
#line 1154 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_BIT_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7327 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 546: /* expression ::= expr_or_subquery NK_BITOR expr_or_subquery */
#line 1159 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_BIT_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7337 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 549: /* column_reference ::= column_name */
#line 1170 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy305, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy305)); }
#line 7343 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 550: /* column_reference ::= table_name NK_DOT column_name */
#line 1171 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305, createColumnNode(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy305)); }
#line 7349 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 551: /* column_reference ::= NK_ALIAS */
#line 1172 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy0)); }
#line 7355 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 552: /* column_reference ::= table_name NK_DOT NK_ALIAS */
#line 1173 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0, createColumnNode(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0)); }
#line 7361 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 553: /* pseudo_column ::= ROWTS */
      case 554: /* pseudo_column ::= TBNAME */ yytestcase(yyruleno==554);
      case 556: /* pseudo_column ::= QSTART */ yytestcase(yyruleno==556);
      case 557: /* pseudo_column ::= QEND */ yytestcase(yyruleno==557);
      case 558: /* pseudo_column ::= QDURATION */ yytestcase(yyruleno==558);
      case 559: /* pseudo_column ::= WSTART */ yytestcase(yyruleno==559);
      case 560: /* pseudo_column ::= WEND */ yytestcase(yyruleno==560);
      case 561: /* pseudo_column ::= WDURATION */ yytestcase(yyruleno==561);
      case 562: /* pseudo_column ::= IROWTS */ yytestcase(yyruleno==562);
      case 563: /* pseudo_column ::= ISFILLED */ yytestcase(yyruleno==563);
      case 564: /* pseudo_column ::= QTAGS */ yytestcase(yyruleno==564);
      case 565: /* pseudo_column ::= FLOW */ yytestcase(yyruleno==565);
      case 566: /* pseudo_column ::= FHIGH */ yytestcase(yyruleno==566);
      case 567: /* pseudo_column ::= FEXPR */ yytestcase(yyruleno==567);
      case 574: /* literal_func ::= NOW */ yytestcase(yyruleno==574);
      case 575: /* literal_func ::= TODAY */ yytestcase(yyruleno==575);
#line 1175 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[0].minor.yy0, NULL)); }
#line 7382 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 555: /* pseudo_column ::= table_name NK_DOT TBNAME */
#line 1177 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[0].minor.yy0, createNodeList(pCxt, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[-2].minor.yy305)))); }
#line 7388 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 568: /* function_expression ::= function_name NK_LP expression_list NK_RP */
      case 569: /* function_expression ::= star_func NK_LP star_func_para_list NK_RP */ yytestcase(yyruleno==569);
#line 1191 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy305, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy305, yymsp[-1].minor.yy436)); }
#line 7395 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 570: /* function_expression ::= CAST NK_LP expr_or_subquery AS type_name NK_RP */
      case 571: /* function_expression ::= CAST NK_LP expr_or_subquery AS type_name_default_len NK_RP */ yytestcase(yyruleno==571);
#line 1194 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-5].minor.yy0, &yymsp[0].minor.yy0, createCastFunctionNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), yymsp[-1].minor.yy96)); }
#line 7402 "./source/libs/parser/src/sql.c"
  yymsp[-5].minor.yy800 = yylhsminor.yy800;
        break;
      case 573: /* literal_func ::= noarg_func NK_LP NK_RP */
#line 1200 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-2].minor.yy305, NULL)); }
#line 7408 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 589: /* star_func_para_list ::= NK_STAR */
#line 1225 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy0)); }
#line 7414 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 594: /* star_func_para ::= table_name NK_DOT NK_STAR */
      case 679: /* select_item ::= table_name NK_DOT NK_STAR */ yytestcase(yyruleno==679);
#line 1234 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnNode(pCxt, &yymsp[-2].minor.yy305, &yymsp[0].minor.yy0); }
#line 7421 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 595: /* case_when_expression ::= CASE when_then_list case_when_else_opt END */
#line 1237 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, createCaseWhenNode(pCxt, NULL, yymsp[-2].minor.yy436, yymsp[-1].minor.yy800)); }
#line 7427 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 596: /* case_when_expression ::= CASE common_expression when_then_list case_when_else_opt END */
#line 1239 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-4].minor.yy0, &yymsp[0].minor.yy0, createCaseWhenNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), yymsp[-2].minor.yy436, yymsp[-1].minor.yy800)); }
#line 7433 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 599: /* when_then_expr ::= WHEN common_expression THEN common_expression */
#line 1246 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createWhenThenNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)); }
#line 7439 "./source/libs/parser/src/sql.c"
        break;
      case 601: /* case_when_else_opt ::= ELSE common_expression */
#line 1249 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = releaseRawExprNode(pCxt, yymsp[0].minor.yy800); }
#line 7444 "./source/libs/parser/src/sql.c"
        break;
      case 602: /* predicate ::= expr_or_subquery compare_op expr_or_subquery */
      case 607: /* predicate ::= expr_or_subquery in_op in_predicate_value */ yytestcase(yyruleno==607);
#line 1252 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy748, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7454 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 603: /* predicate ::= expr_or_subquery BETWEEN expr_or_subquery AND expr_or_subquery */
#line 1259 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy800), releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7464 "./source/libs/parser/src/sql.c"
  yymsp[-4].minor.yy800 = yylhsminor.yy800;
        break;
      case 604: /* predicate ::= expr_or_subquery NOT BETWEEN expr_or_subquery AND expr_or_subquery */
#line 1265 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy800), releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7474 "./source/libs/parser/src/sql.c"
  yymsp[-5].minor.yy800 = yylhsminor.yy800;
        break;
      case 605: /* predicate ::= expr_or_subquery IS NULL */
#line 1270 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), NULL));
                                                                                  }
#line 7483 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 606: /* predicate ::= expr_or_subquery IS NOT NULL */
#line 1274 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), NULL));
                                                                                  }
#line 7492 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 608: /* compare_op ::= NK_LT */
#line 1286 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_LOWER_THAN; }
#line 7498 "./source/libs/parser/src/sql.c"
        break;
      case 609: /* compare_op ::= NK_GT */
#line 1287 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_GREATER_THAN; }
#line 7503 "./source/libs/parser/src/sql.c"
        break;
      case 610: /* compare_op ::= NK_LE */
#line 1288 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_LOWER_EQUAL; }
#line 7508 "./source/libs/parser/src/sql.c"
        break;
      case 611: /* compare_op ::= NK_GE */
#line 1289 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_GREATER_EQUAL; }
#line 7513 "./source/libs/parser/src/sql.c"
        break;
      case 612: /* compare_op ::= NK_NE */
#line 1290 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_NOT_EQUAL; }
#line 7518 "./source/libs/parser/src/sql.c"
        break;
      case 613: /* compare_op ::= NK_EQ */
#line 1291 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_EQUAL; }
#line 7523 "./source/libs/parser/src/sql.c"
        break;
      case 614: /* compare_op ::= LIKE */
#line 1292 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_LIKE; }
#line 7528 "./source/libs/parser/src/sql.c"
        break;
      case 615: /* compare_op ::= NOT LIKE */
#line 1293 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy748 = OP_TYPE_NOT_LIKE; }
#line 7533 "./source/libs/parser/src/sql.c"
        break;
      case 616: /* compare_op ::= MATCH */
#line 1294 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_MATCH; }
#line 7538 "./source/libs/parser/src/sql.c"
        break;
      case 617: /* compare_op ::= NMATCH */
#line 1295 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_NMATCH; }
#line 7543 "./source/libs/parser/src/sql.c"
        break;
      case 618: /* compare_op ::= CONTAINS */
#line 1296 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_JSON_CONTAINS; }
#line 7548 "./source/libs/parser/src/sql.c"
        break;
      case 619: /* in_op ::= IN */
#line 1300 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy748 = OP_TYPE_IN; }
#line 7553 "./source/libs/parser/src/sql.c"
        break;
      case 620: /* in_op ::= NOT IN */
#line 1301 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy748 = OP_TYPE_NOT_IN; }
#line 7558 "./source/libs/parser/src/sql.c"
        break;
      case 621: /* in_predicate_value ::= NK_LP literal_list NK_RP */
#line 1303 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy436)); }
#line 7563 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 623: /* boolean_value_expression ::= NOT boolean_primary */
#line 1307 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy800), NULL));
                                                                                  }
#line 7572 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 624: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
#line 1312 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7582 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 625: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
#line 1318 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy800);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy800);
                                                                                    yylhsminor.yy800 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), releaseRawExprNode(pCxt, yymsp[0].minor.yy800)));
                                                                                  }
#line 7592 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 633: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
#line 1336 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, JOIN_STYPE_NONE, yymsp[-2].minor.yy800, yymsp[0].minor.yy800, NULL); }
#line 7598 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 636: /* table_primary ::= table_name alias_opt */
#line 1342 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy305, &yymsp[0].minor.yy305); }
#line 7604 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 637: /* table_primary ::= db_name NK_DOT table_name alias_opt */
#line 1343 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRealTableNode(pCxt, &yymsp[-3].minor.yy305, &yymsp[-1].minor.yy305, &yymsp[0].minor.yy305); }
#line 7610 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 638: /* table_primary ::= subquery alias_opt */
#line 1344 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800), &yymsp[0].minor.yy305); }
#line 7616 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 640: /* alias_opt ::= */
#line 1349 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy305 = nil_token;  }
#line 7622 "./source/libs/parser/src/sql.c"
        break;
      case 642: /* alias_opt ::= AS table_alias */
#line 1351 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy305 = yymsp[0].minor.yy305; }
#line 7627 "./source/libs/parser/src/sql.c"
        break;
      case 643: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 644: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==644);
#line 1353 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy800 = yymsp[-1].minor.yy800; }
#line 7633 "./source/libs/parser/src/sql.c"
        break;
      case 645: /* joined_table ::= table_reference join_type join_subtype JOIN table_reference join_on_clause_opt window_offset_clause_opt jlimit_clause_opt */
#line 1359 "source/libs/parser/inc/sql.y"
{
                                                                                    yylhsminor.yy800 = createJoinTableNode(pCxt, yymsp[-6].minor.yy844, yymsp[-5].minor.yy1042, yymsp[-7].minor.yy800, yymsp[-3].minor.yy800, yymsp[-2].minor.yy800);
                                                                                    yylhsminor.yy800 = addWindowOffsetClause(pCxt, yylhsminor.yy800, yymsp[-1].minor.yy800);
                                                                                    yylhsminor.yy800 = addJLimitClause(pCxt, yylhsminor.yy800, yymsp[0].minor.yy800);
                                                                                  }
#line 7642 "./source/libs/parser/src/sql.c"
  yymsp[-7].minor.yy800 = yylhsminor.yy800;
        break;
      case 646: /* join_type ::= */
#line 1367 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy844 = JOIN_TYPE_INNER; }
#line 7648 "./source/libs/parser/src/sql.c"
        break;
      case 647: /* join_type ::= INNER */
#line 1368 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy844 = JOIN_TYPE_INNER; }
#line 7653 "./source/libs/parser/src/sql.c"
        break;
      case 648: /* join_type ::= LEFT */
#line 1369 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy844 = JOIN_TYPE_LEFT; }
#line 7658 "./source/libs/parser/src/sql.c"
        break;
      case 649: /* join_type ::= RIGHT */
#line 1370 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy844 = JOIN_TYPE_RIGHT; }
#line 7663 "./source/libs/parser/src/sql.c"
        break;
      case 650: /* join_type ::= FULL */
#line 1371 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy844 = JOIN_TYPE_FULL; }
#line 7668 "./source/libs/parser/src/sql.c"
        break;
      case 651: /* join_subtype ::= */
#line 1375 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy1042 = JOIN_STYPE_NONE; }
#line 7673 "./source/libs/parser/src/sql.c"
        break;
      case 652: /* join_subtype ::= OUTER */
#line 1376 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1042 = JOIN_STYPE_OUTER; }
#line 7678 "./source/libs/parser/src/sql.c"
        break;
      case 653: /* join_subtype ::= SEMI */
#line 1377 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1042 = JOIN_STYPE_SEMI; }
#line 7683 "./source/libs/parser/src/sql.c"
        break;
      case 654: /* join_subtype ::= ANTI */
#line 1378 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1042 = JOIN_STYPE_ANTI; }
#line 7688 "./source/libs/parser/src/sql.c"
        break;
      case 655: /* join_subtype ::= ASOF */
#line 1379 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1042 = JOIN_STYPE_ASOF; }
#line 7693 "./source/libs/parser/src/sql.c"
        break;
      case 656: /* join_subtype ::= WINDOW */
#line 1380 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1042 = JOIN_STYPE_WIN; }
#line 7698 "./source/libs/parser/src/sql.c"
        break;
      case 660: /* window_offset_clause_opt ::= WINDOW_OFFSET NK_LP window_offset_literal NK_COMMA window_offset_literal NK_RP */
#line 1387 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createWindowOffsetNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7703 "./source/libs/parser/src/sql.c"
        break;
      case 661: /* window_offset_literal ::= NK_VARIABLE */
#line 1389 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createTimeOffsetValueNode(pCxt, &yymsp[0].minor.yy0)); }
#line 7708 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 662: /* window_offset_literal ::= NK_MINUS NK_VARIABLE */
#line 1390 "source/libs/parser/inc/sql.y"
{
                                                                                    SToken t = yymsp[-1].minor.yy0;
                                                                                    t.n = (yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z;
                                                                                    yylhsminor.yy800 = createRawExprNode(pCxt, &t, createTimeOffsetValueNode(pCxt, &t));
                                                                                  }
#line 7718 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 664: /* jlimit_clause_opt ::= JLIMIT NK_INTEGER */
      case 735: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */ yytestcase(yyruleno==735);
      case 739: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==739);
#line 1397 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy800 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
#line 7726 "./source/libs/parser/src/sql.c"
        break;
      case 665: /* query_specification ::= SELECT hint_list set_quantifier_opt tag_mode_opt select_list from_clause_opt where_clause_opt partition_by_clause_opt range_opt every_opt fill_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
#line 1403 "source/libs/parser/inc/sql.y"
{
                                                                                    yymsp[-13].minor.yy800 = createSelectStmt(pCxt, yymsp[-11].minor.yy125, yymsp[-9].minor.yy436, yymsp[-8].minor.yy800, yymsp[-12].minor.yy436);
                                                                                    yymsp[-13].minor.yy800 = setSelectStmtTagMode(pCxt, yymsp[-13].minor.yy800, yymsp[-10].minor.yy125);
                                                                                    yymsp[-13].minor.yy800 = addWhereClause(pCxt, yymsp[-13].minor.yy800, yymsp[-7].minor.yy800);
                                                                                    yymsp[-13].minor.yy800 = addPartitionByClause(pCxt, yymsp[-13].minor.yy800, yymsp[-6].minor.yy436);
                                                                                    yymsp[-13].minor.yy800 = addWindowClauseClause(pCxt, yymsp[-13].minor.yy800, yymsp[-2].minor.yy800);
                                                                                    yymsp[-13].minor.yy800 = addGroupByClause(pCxt, yymsp[-13].minor.yy800, yymsp[-1].minor.yy436);
                                                                                    yymsp[-13].minor.yy800 = addHavingClause(pCxt, yymsp[-13].minor.yy800, yymsp[0].minor.yy800);
                                                                                    yymsp[-13].minor.yy800 = addRangeClause(pCxt, yymsp[-13].minor.yy800, yymsp[-5].minor.yy800);
                                                                                    yymsp[-13].minor.yy800 = addEveryClause(pCxt, yymsp[-13].minor.yy800, yymsp[-4].minor.yy800);
                                                                                    yymsp[-13].minor.yy800 = addFillClause(pCxt, yymsp[-13].minor.yy800, yymsp[-3].minor.yy800);
                                                                                  }
#line 7742 "./source/libs/parser/src/sql.c"
        break;
      case 666: /* hint_list ::= */
#line 1418 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy436 = createHintNodeList(pCxt, NULL); }
#line 7747 "./source/libs/parser/src/sql.c"
        break;
      case 667: /* hint_list ::= NK_HINT */
#line 1419 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createHintNodeList(pCxt, &yymsp[0].minor.yy0); }
#line 7752 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 672: /* set_quantifier_opt ::= ALL */
#line 1430 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy125 = false; }
#line 7758 "./source/libs/parser/src/sql.c"
        break;
      case 675: /* select_item ::= NK_STAR */
#line 1437 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy0); }
#line 7763 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy800 = yylhsminor.yy800;
        break;
      case 677: /* select_item ::= common_expression column_alias */
      case 687: /* partition_item ::= expr_or_subquery column_alias */ yytestcase(yyruleno==687);
#line 1439 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800), &yymsp[0].minor.yy305); }
#line 7770 "./source/libs/parser/src/sql.c"
  yymsp[-1].minor.yy800 = yylhsminor.yy800;
        break;
      case 678: /* select_item ::= common_expression AS column_alias */
      case 688: /* partition_item ::= expr_or_subquery AS column_alias */ yytestcase(yyruleno==688);
#line 1440 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), &yymsp[0].minor.yy305); }
#line 7777 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 683: /* partition_by_clause_opt ::= PARTITION BY partition_list */
      case 713: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==713);
      case 733: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==733);
#line 1449 "source/libs/parser/inc/sql.y"
{ yymsp[-2].minor.yy436 = yymsp[0].minor.yy436; }
#line 7785 "./source/libs/parser/src/sql.c"
        break;
      case 690: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA interval_sliding_duration_literal NK_RP */
#line 1462 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7790 "./source/libs/parser/src/sql.c"
        break;
      case 691: /* twindow_clause_opt ::= STATE_WINDOW NK_LP expr_or_subquery NK_RP */
#line 1463 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7795 "./source/libs/parser/src/sql.c"
        break;
      case 692: /* twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_RP sliding_opt fill_opt */
#line 1465 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), NULL, yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 7800 "./source/libs/parser/src/sql.c"
        break;
      case 693: /* twindow_clause_opt ::= INTERVAL NK_LP interval_sliding_duration_literal NK_COMMA interval_sliding_duration_literal NK_RP sliding_opt fill_opt */
#line 1469 "source/libs/parser/inc/sql.y"
{ yymsp[-7].minor.yy800 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy800), releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), yymsp[-1].minor.yy800, yymsp[0].minor.yy800); }
#line 7805 "./source/libs/parser/src/sql.c"
        break;
      case 694: /* twindow_clause_opt ::= EVENT_WINDOW START WITH search_condition END WITH search_condition */
#line 1471 "source/libs/parser/inc/sql.y"
{ yymsp[-6].minor.yy800 = createEventWindowNode(pCxt, yymsp[-3].minor.yy800, yymsp[0].minor.yy800); }
#line 7810 "./source/libs/parser/src/sql.c"
        break;
      case 695: /* twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_RP */
#line 1473 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createCountWindowNode(pCxt, &yymsp[-1].minor.yy0, &yymsp[-1].minor.yy0); }
#line 7815 "./source/libs/parser/src/sql.c"
        break;
      case 696: /* twindow_clause_opt ::= COUNT_WINDOW NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
#line 1475 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createCountWindowNode(pCxt, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0); }
#line 7820 "./source/libs/parser/src/sql.c"
        break;
      case 703: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
#line 1485 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createFillNode(pCxt, yymsp[-1].minor.yy1122, NULL); }
#line 7825 "./source/libs/parser/src/sql.c"
        break;
      case 704: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA expression_list NK_RP */
#line 1486 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy436)); }
#line 7830 "./source/libs/parser/src/sql.c"
        break;
      case 705: /* fill_opt ::= FILL NK_LP VALUE_F NK_COMMA expression_list NK_RP */
#line 1487 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createFillNode(pCxt, FILL_MODE_VALUE_F, createNodeListNode(pCxt, yymsp[-1].minor.yy436)); }
#line 7835 "./source/libs/parser/src/sql.c"
        break;
      case 706: /* fill_mode ::= NONE */
#line 1491 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_NONE; }
#line 7840 "./source/libs/parser/src/sql.c"
        break;
      case 707: /* fill_mode ::= PREV */
#line 1492 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_PREV; }
#line 7845 "./source/libs/parser/src/sql.c"
        break;
      case 708: /* fill_mode ::= NULL */
#line 1493 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_NULL; }
#line 7850 "./source/libs/parser/src/sql.c"
        break;
      case 709: /* fill_mode ::= NULL_F */
#line 1494 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_NULL_F; }
#line 7855 "./source/libs/parser/src/sql.c"
        break;
      case 710: /* fill_mode ::= LINEAR */
#line 1495 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_LINEAR; }
#line 7860 "./source/libs/parser/src/sql.c"
        break;
      case 711: /* fill_mode ::= NEXT */
#line 1496 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy1122 = FILL_MODE_NEXT; }
#line 7865 "./source/libs/parser/src/sql.c"
        break;
      case 714: /* group_by_list ::= expr_or_subquery */
#line 1505 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy800))); }
#line 7870 "./source/libs/parser/src/sql.c"
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 715: /* group_by_list ::= group_by_list NK_COMMA expr_or_subquery */
#line 1506 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy436 = addNodeToList(pCxt, yymsp[-2].minor.yy436, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy800))); }
#line 7876 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 719: /* range_opt ::= RANGE NK_LP expr_or_subquery NK_COMMA expr_or_subquery NK_RP */
#line 1513 "source/libs/parser/inc/sql.y"
{ yymsp[-5].minor.yy800 = createInterpTimeRange(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy800), releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7882 "./source/libs/parser/src/sql.c"
        break;
      case 720: /* range_opt ::= RANGE NK_LP expr_or_subquery NK_RP */
#line 1515 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createInterpTimePoint(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy800)); }
#line 7887 "./source/libs/parser/src/sql.c"
        break;
      case 723: /* query_expression ::= query_simple order_by_clause_opt slimit_clause_opt limit_clause_opt */
#line 1522 "source/libs/parser/inc/sql.y"
{
                                                                                    yylhsminor.yy800 = addOrderByClause(pCxt, yymsp[-3].minor.yy800, yymsp[-2].minor.yy436);
                                                                                    yylhsminor.yy800 = addSlimitClause(pCxt, yylhsminor.yy800, yymsp[-1].minor.yy800);
                                                                                    yylhsminor.yy800 = addLimitClause(pCxt, yylhsminor.yy800, yymsp[0].minor.yy800);
                                                                                  }
#line 7896 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 726: /* union_query_expression ::= query_simple_or_subquery UNION ALL query_simple_or_subquery */
#line 1532 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy800, yymsp[0].minor.yy800); }
#line 7902 "./source/libs/parser/src/sql.c"
  yymsp[-3].minor.yy800 = yylhsminor.yy800;
        break;
      case 727: /* union_query_expression ::= query_simple_or_subquery UNION query_simple_or_subquery */
#line 1534 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createSetOperator(pCxt, SET_OP_TYPE_UNION, yymsp[-2].minor.yy800, yymsp[0].minor.yy800); }
#line 7908 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 736: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 740: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==740);
#line 1549 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
#line 7915 "./source/libs/parser/src/sql.c"
        break;
      case 737: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 741: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==741);
#line 1550 "source/libs/parser/inc/sql.y"
{ yymsp[-3].minor.yy800 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
#line 7921 "./source/libs/parser/src/sql.c"
        break;
      case 742: /* subquery ::= NK_LP query_expression NK_RP */
#line 1558 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy800); }
#line 7926 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 747: /* sort_specification ::= expr_or_subquery ordering_specification_opt null_ordering_opt */
#line 1572 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy800), yymsp[-1].minor.yy446, yymsp[0].minor.yy889); }
#line 7932 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 748: /* ordering_specification_opt ::= */
#line 1576 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy446 = ORDER_ASC; }
#line 7938 "./source/libs/parser/src/sql.c"
        break;
      case 749: /* ordering_specification_opt ::= ASC */
#line 1577 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy446 = ORDER_ASC; }
#line 7943 "./source/libs/parser/src/sql.c"
        break;
      case 750: /* ordering_specification_opt ::= DESC */
#line 1578 "source/libs/parser/inc/sql.y"
{ yymsp[0].minor.yy446 = ORDER_DESC; }
#line 7948 "./source/libs/parser/src/sql.c"
        break;
      case 751: /* null_ordering_opt ::= */
#line 1582 "source/libs/parser/inc/sql.y"
{ yymsp[1].minor.yy889 = NULL_ORDER_DEFAULT; }
#line 7953 "./source/libs/parser/src/sql.c"
        break;
      case 752: /* null_ordering_opt ::= NULLS FIRST */
#line 1583 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy889 = NULL_ORDER_FIRST; }
#line 7958 "./source/libs/parser/src/sql.c"
        break;
      case 753: /* null_ordering_opt ::= NULLS LAST */
#line 1584 "source/libs/parser/inc/sql.y"
{ yymsp[-1].minor.yy889 = NULL_ORDER_LAST; }
#line 7963 "./source/libs/parser/src/sql.c"
        break;
      case 756: /* column_options ::= column_options ENCODE NK_STRING */
#line 1592 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setColumnOptions(pCxt, yymsp[-2].minor.yy800, COLUMN_OPTION_ENCODE, &yymsp[0].minor.yy0); }
#line 7968 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 757: /* column_options ::= column_options COMPRESS NK_STRING */
#line 1593 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setColumnOptions(pCxt, yymsp[-2].minor.yy800, COLUMN_OPTION_COMPRESS, &yymsp[0].minor.yy0); }
#line 7974 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      case 758: /* column_options ::= column_options LEVEL NK_STRING */
#line 1594 "source/libs/parser/inc/sql.y"
{ yylhsminor.yy800 = setColumnOptions(pCxt, yymsp[-2].minor.yy800, COLUMN_OPTION_LEVEL, &yymsp[0].minor.yy0); }
#line 7980 "./source/libs/parser/src/sql.c"
  yymsp[-2].minor.yy800 = yylhsminor.yy800;
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yyact = yy_find_reduce_action(yymsp[yysize].stateno,(YYCODETYPE)yygoto);

  /* There are no SHIFTREDUCE actions on nonterminals because the table
  ** generator has simplified them to pure REDUCE actions. */
  assert( !(yyact>YY_MAX_SHIFT && yyact<=YY_MAX_SHIFTREDUCE) );

  /* It is not possible for a REDUCE to be followed by an error */
  assert( yyact!=YY_ERROR_ACTION );

  yymsp += yysize+1;
  yypParser->yytos = yymsp;
  yymsp->stateno = (YYACTIONTYPE)yyact;
  yymsp->major = (YYCODETYPE)yygoto;
  yyTraceShift(yypParser, yyact, "... then shift");
  return yyact;
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH
  ParseCTX_FETCH
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sFail!\n",yyTracePrompt);
  }
#endif
  while( yypParser->yytos>yypParser->yystack ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
/************ Begin %parse_failure code ***************************************/
/************ End %parse_failure code *****************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  ParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  ParseARG_FETCH
  ParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
#line 29 "source/libs/parser/inc/sql.y"

  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    if(TOKEN.z) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
  } else if (TSDB_CODE_PAR_DB_NOT_SPECIFIED == pCxt->errCode && TK_NK_FLOAT == TOKEN.type) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
  }
#line 8054 "./source/libs/parser/src/sql.c"
/************ End %syntax_error code ******************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH
  ParseCTX_FETCH
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sAccept!\n",yyTracePrompt);
  }
#endif
#ifndef YYNOERRORRECOVERY
  yypParser->yyerrcnt = -1;
#endif
  assert( yypParser->yytos==yypParser->yystack );
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */
/*********** Begin %parse_accept code *****************************************/
/*********** End %parse_accept code *******************************************/
  ParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  ParseCTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "ParseAlloc" which describes the current state of the parser.
** The second argument is the major token number.  The third is
** the minor token.  The fourth optional argument is whatever the
** user wants (and specified in the grammar) and is available for
** use by the action routines.
**
** Inputs:
** <ul>
** <li> A pointer to the parser (an opaque structure.)
** <li> The major token number.
** <li> The minor token number.
** <li> An option argument of a grammar-specified type.
** </ul>
**
** Outputs:
** None.
*/
void Parse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  ParseTOKENTYPE yyminor       /* The value for the token */
  ParseARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  YYACTIONTYPE yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser = (yyParser*)yyp;  /* The parser */
  ParseCTX_FETCH
  ParseARG_STORE

  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif

  yyact = yypParser->yytos->stateno;
#ifndef NDEBUG
  if( yyTraceFILE ){
    if( yyact < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],yyact-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    assert( yyact==yypParser->yytos->stateno );
    yyact = yy_find_shift_action(yymajor,yyact);
    if( yyact >= YY_MIN_REDUCE ){
      yyact = yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,
                        yyminor ParseCTX_PARAM);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      break;
    }else if( yyact==YY_ACCEPT_ACTION ){
      yypParser->yytos--;
      yy_accept(yypParser);
      return;
    }else{
      assert( yyact == YY_ERROR_ACTION );
      yyminorunion.yy0 = yyminor;
#ifdef YYERRORSYMBOL
      int yymx;
#endif
#ifndef NDEBUG
      if( yyTraceFILE ){
        fprintf(yyTraceFILE,"%sSyntax Error!\n",yyTracePrompt);
      }
#endif
#ifdef YYERRORSYMBOL
      /* A syntax error has occurred.
      ** The response to an error depends upon whether or not the
      ** grammar defines an error token "ERROR".  
      **
      ** This is what we do if the grammar does define ERROR:
      **
      **  * Call the %syntax_error function.
      **
      **  * Begin popping the stack until we enter a state where
      **    it is legal to shift the error symbol, then shift
      **    the error symbol.
      **
      **  * Set the error count to three.
      **
      **  * Begin accepting and shifting new tokens.  No new error
      **    processing will occur until three tokens have been
      **    shifted successfully.
      **
      */
      if( yypParser->yyerrcnt<0 ){
        yy_syntax_error(yypParser,yymajor,yyminor);
      }
      yymx = yypParser->yytos->major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor, &yyminorunion);
        yymajor = YYNOCODE;
      }else{
        while( yypParser->yytos >= yypParser->yystack
            && yymx != YYERRORSYMBOL
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yytos < yypParser->yystack || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
          yypParser->yyerrcnt = -1;
#endif
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          yy_shift(yypParser,yyact,YYERRORSYMBOL,yyminor);
        }
      }
      yypParser->yyerrcnt = 3;
      yyerrorhit = 1;
      if( yymajor==YYNOCODE ) break;
      yyact = yypParser->yytos->stateno;
#elif defined(YYNOERRORRECOVERY)
      /* If the YYNOERRORRECOVERY macro is defined, then do not attempt to
      ** do any kind of error recovery.  Instead, simply invoke the syntax
      ** error routine and continue going as if nothing had happened.
      **
      ** Applications can set this macro (for example inside %include) if
      ** they intend to abandon the parse upon the first syntax error seen.
      */
      yy_syntax_error(yypParser,yymajor, yyminor);
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      break;
#else  /* YYERRORSYMBOL is not defined */
      /* This is what we do if the grammar does not define ERROR:
      **
      **  * Report an error message, and throw away the input token.
      **
      **  * If the input token is $, then fail the parse.
      **
      ** As before, subsequent error messages are suppressed until
      ** three input tokens have been successfully shifted.
      */
      if( yypParser->yyerrcnt<=0 ){
        yy_syntax_error(yypParser,yymajor, yyminor);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
#ifndef YYNOERRORRECOVERY
        yypParser->yyerrcnt = -1;
#endif
      }
      break;
#endif
    }
  }while( yypParser->yytos>yypParser->yystack );
#ifndef NDEBUG
  if( yyTraceFILE ){
    yyStackEntry *i;
    char cDiv = '[';
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i=&yypParser->yystack[1]; i<=yypParser->yytos; i++){
      fprintf(yyTraceFILE,"%c%s", cDiv, yyTokenName[i->major]);
      cDiv = ' ';
    }
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}
