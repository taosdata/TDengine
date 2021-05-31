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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "qSqlparser.h"
#include "tcmdtype.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "tutil.h"
#include "tvariant.h"
/**************** End of %include directives **********************************/
/* These constants specify the various numeric values for terminal symbols
** in a format understandable to "makeheaders".  This section is blank unless
** "lemon" is run with the "-m" command-line option.
***************** Begin makeheaders token definitions *************************/
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
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
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
#define YYNOCODE 271
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy112;
  SCreateAcctInfo yy151;
  tSqlExpr* yy166;
  SCreateTableSql* yy182;
  SSqlNode* yy236;
  SRelationInfo* yy244;
  SSessionWindowVal yy259;
  SIntervalVal yy340;
  TAOS_FIELD yy343;
  int64_t yy369;
  SCreateDbInfo yy382;
  SLimitVal yy414;
  SArray* yy441;
  SCreatedTableInfo yy456;
  tVariant yy506;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             338
#define YYNRULE              279
#define YYNTOKEN             192
#define YY_MAX_SHIFT         337
#define YY_MIN_SHIFTREDUCE   538
#define YY_MAX_SHIFTREDUCE   816
#define YY_ERROR_ACTION      817
#define YY_ACCEPT_ACTION     818
#define YY_NO_ACTION         819
#define YY_MIN_REDUCE        820
#define YY_MAX_REDUCE        1098
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
#define YY_ACTTAB_COUNT (716)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   994,  588,   18,  588,  218,  335,  144,  224,   87,  589,
 /*    10 */   588,  589,  151,   50,   51,  151,   54,   55,  589,  195,
 /*    20 */   230,   44,  193,   53,  279,   58,   56,   60,   57,  227,
 /*    30 */  1080,  967,  973,   49,   48,  818,  337,   47,   46,   45,
 /*    40 */   956,  957,   30,  960,   47,   46,   45,  539,  540,  541,
 /*    50 */   542,  543,  544,  545,  546,  547,  548,  549,  550,  551,
 /*    60 */   552,  336,  235,  991,  219,   50,   51,  151,   54,   55,
 /*    70 */   264,  263,  230,   44,  236,   53,  279,   58,   56,   60,
 /*    80 */    57, 1028,  985,   73,   84,   49,   48,  973,  868,   47,
 /*    90 */    46,   45,   50,   51,  179,   54,   55,  257,  195,  230,
 /*   100 */    44,   85,   53,  279,   58,   56,   60,   57,  226, 1080,
 /*   110 */   195,  301,   49,   48,   74,  242,   47,   46,   45,   50,
 /*   120 */    52, 1079,   54,   55,  165,  199,  230,   44,   72,   53,
 /*   130 */   279,   58,   56,   60,   57,  766, 1029,  769,  274,   49,
 /*   140 */    48,  321,  588,   47,   46,   45,   51,   79,   54,   55,
 /*   150 */   589, 1076,  230,   44,   39,   53,  279,   58,   56,   60,
 /*   160 */    57,  958,  877,  760,   33,   49,   48, 1075,  179,   47,
 /*   170 */    46,   45,   24,  299,  330,  329,  298,  297,  296,  328,
 /*   180 */   295,  327,  326,  325,  294,  324,  323,  933,  671,  921,
 /*   190 */   922,  923,  924,  925,  926,  927,  928,  929,  930,  931,
 /*   200 */   932,  934,  935,   54,   55,   19,  220,  230,   44,  970,
 /*   210 */    53,  279,   58,   56,   60,   57,  311,  310,   86, 1090,
 /*   220 */    49,   48,  151,  203,   47,   46,   45,  229,  775,  959,
 /*   230 */   205,  764,  985,  767,  249,  770,  127,  126,  204, 1074,
 /*   240 */   229,  775,  253,  252,  764,   13,  767,  221,  770,   89,
 /*   250 */   237,   25,  773,  212,  941,  985,   33,  939,  940,  214,
 /*   260 */   215,  241,  942,  278,  944,  945,  943,  331,  946,  947,
 /*   270 */   222,   79,  214,  215,   24,  973,  330,  329,   39,  213,
 /*   280 */   765,  328,  768,  327,  326,  325,   33,  324,  323,  276,
 /*   290 */   695,   83,  973,  692,  228,  693, 1039,  694,  233,   33,
 /*   300 */   256,  970,   71,   58,   56,   60,   57,   70,  211,  670,
 /*   310 */   301,   49,   48,  710,   33,   47,   46,   45,    5,   36,
 /*   320 */   169,  244,  245,   33,  242,  168,   96,  101,   92,  100,
 /*   330 */    33,  969,  239,  166,  243,  280,   59,  308,  307,   90,
 /*   340 */   290,  234,  776,    6,  970,  242,   49,   48,  772,   59,
 /*   350 */    47,   46,   45,   33,  971,  776,  304,  114,  699,  970,
 /*   360 */   700,  772,    1,  167,  771,  305,  321,  869,  970,  112,
 /*   370 */   106,  117,  309,  179,  961,  970,  116,  771,  122,  125,
 /*   380 */   115,  187,  185,  183,    3,  180,  119,  197,  182,  131,
 /*   390 */   130,  129,  128,  741,  742,  313,  714,  774,  970,  334,
 /*   400 */   333,  136,  258,  762,  696,  240,   34,  707,  303,   76,
 /*   410 */   142,  140,  139,    8,   77,  726,  260,   64,  732,  146,
 /*   420 */   733,  260,   63,  796,   21,  777,   67,   20,  681,   20,
 /*   430 */   282,  198,   34,  683,   34,  200,  779,   63,   65,  763,
 /*   440 */   284,  972,  682,  254,   88,   68,   63,   29,  105,  104,
 /*   450 */   285,   17,   16,  697,  194,  698,  124,  123,   15,   14,
 /*   460 */   201,  202,  208,  209,  111,  110,  207,  192,  206,  196,
 /*   470 */  1038,  231,   42, 1035, 1034,  232,  312,  143,  993, 1004,
 /*   480 */  1001, 1002, 1006,  145,  986,  261,  149, 1021, 1020,  968,
 /*   490 */   161,  162,  966,  163,  141,  164,  882,  287,  265,  291,
 /*   500 */   881,  937,  159,  725,  983,  153,  288,  152,  154,  155,
 /*   510 */   223,  156,  289,  267,  272,  277,  275,   69,   61,  292,
 /*   520 */   293,   40,  190,   37,  302,   66,  876,  273, 1095,  271,
 /*   530 */   102, 1094, 1092,  269,  170,  306, 1089,  108, 1088, 1086,
 /*   540 */   171,  902,   38,  266,   35,   41,  191,  865,  118,   43,
 /*   550 */   863,  120,  121,  861,  860,  246,  181,  858,  857,  856,
 /*   560 */   855,  854,  853,  852,  184,  186,  849,  847,  845,  843,
 /*   570 */   188,  840,  189,  322,  259,  113,   75,   80,  314,  268,
 /*   580 */  1022,  315,  316,  317,  318,  319,  320,  332,  216,  816,
 /*   590 */   238,  286,  247,  248,  815,  217,  210,  250,   97,   98,
 /*   600 */   251,  880,  814,  802,  801,  255,  260,  281,   78,    9,
 /*   610 */   225,  702,  859,  851,  178,  903,  174,  172,  173,  132,
 /*   620 */   175,  176,  133,  134,  850,  177,  135,  842,  841,    2,
 /*   630 */    26,    4,  262,   81,  727,  157,  158,  160,  147,  148,
 /*   640 */    27,   82,  949,  730,  270,   91,  734,  150,   28,   10,
 /*   650 */    11,  778,    7,   12,  780,   22,   23,  283,   31,   93,
 /*   660 */   602,   89,   94,   32,   95,  634,  630,  628,  627,  626,
 /*   670 */   623,   99,  592,  300,   34,   62,  673,  672,  618,  669,
 /*   680 */   103,  616,  608,  107,  614,  109,  610,  612,  606,  604,
 /*   690 */   637,  636,  635,  633,  632,  631,  629,  625,  624,  590,
 /*   700 */    63,  137,  556,  554,  820,  819,  819,  819,  819,  819,
 /*   710 */   819,  819,  819,  819,  819,  138,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   196,    1,  258,    1,  195,  196,  196,  217,  202,    9,
 /*    10 */     1,    9,  196,   13,   14,  196,   16,   17,    9,  258,
 /*    20 */    20,   21,  258,   23,   24,   25,   26,   27,   28,  268,
 /*    30 */   269,  196,  242,   33,   34,  193,  194,   37,   38,   39,
 /*    40 */   234,  235,  236,  237,   37,   38,   39,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  217,  259,   62,   13,   14,  196,   16,   17,
 /*    70 */   260,  261,   20,   21,  239,   23,   24,   25,   26,   27,
 /*    80 */    28,  265,  240,   83,  265,   33,   34,  242,  201,   37,
 /*    90 */    38,   39,   13,   14,  207,   16,   17,  255,  258,   20,
 /*   100 */    21,  243,   23,   24,   25,   26,   27,   28,  268,  269,
 /*   110 */   258,   81,   33,   34,  256,  196,   37,   38,   39,   13,
 /*   120 */    14,  269,   16,   17,  205,  258,   20,   21,  202,   23,
 /*   130 */    24,   25,   26,   27,   28,    5,  265,    7,  267,   33,
 /*   140 */    34,   87,    1,   37,   38,   39,   14,  110,   16,   17,
 /*   150 */     9,  258,   20,   21,  117,   23,   24,   25,   26,   27,
 /*   160 */    28,  235,  201,  111,  196,   33,   34,  258,  207,   37,
 /*   170 */    38,   39,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   180 */   102,  103,  104,  105,  106,  107,  108,  216,    5,  218,
 /*   190 */   219,  220,  221,  222,  223,  224,  225,  226,  227,  228,
 /*   200 */   229,  230,  231,   16,   17,   44,  238,   20,   21,  241,
 /*   210 */    23,   24,   25,   26,   27,   28,   33,   34,   83,  242,
 /*   220 */    33,   34,  196,   62,   37,   38,   39,    1,    2,    0,
 /*   230 */    69,    5,  240,    7,  140,    9,   75,   76,   77,  258,
 /*   240 */     1,    2,  148,  149,    5,  110,    7,  255,    9,  114,
 /*   250 */   217,  110,  122,  258,  216,  240,  196,  219,  220,   33,
 /*   260 */    34,   69,  224,   37,  226,  227,  228,  217,  230,  231,
 /*   270 */   255,  110,   33,   34,   94,  242,   96,   97,  117,  258,
 /*   280 */     5,  101,    7,  103,  104,  105,  196,  107,  108,  263,
 /*   290 */     2,  265,  242,    5,   61,    7,  233,    9,  238,  196,
 /*   300 */   139,  241,  141,   25,   26,   27,   28,  110,  147,  112,
 /*   310 */    81,   33,   34,   37,  196,   37,   38,   39,   63,   64,
 /*   320 */    65,   33,   34,  196,  196,   70,   71,   72,   73,   74,
 /*   330 */   196,  241,   69,  205,  142,   15,  110,  145,  146,  202,
 /*   340 */    85,  238,  116,  110,  241,  196,   33,   34,  122,  110,
 /*   350 */    37,   38,   39,  196,  205,  116,  238,   78,    5,  241,
 /*   360 */     7,  122,  203,  204,  138,  238,   87,  201,  241,   63,
 /*   370 */    64,   65,  238,  207,  237,  241,   70,  138,   72,   73,
 /*   380 */    74,   63,   64,   65,  199,  200,   80,  258,   70,   71,
 /*   390 */    72,   73,   74,  129,  130,  238,  120,  122,  241,   66,
 /*   400 */    67,   68,  111,    1,  116,  142,  115,  115,  145,  111,
 /*   410 */    63,   64,   65,  121,  111,  111,  118,  115,  111,  115,
 /*   420 */   111,  118,  115,  111,  115,  111,  115,  115,  111,  115,
 /*   430 */   111,  258,  115,  111,  115,  258,  116,  115,  136,   37,
 /*   440 */   111,  242,  111,  196,  115,  134,  115,  110,  143,  144,
 /*   450 */   113,  143,  144,    5,  258,    7,   78,   79,  143,  144,
 /*   460 */   258,  258,  258,  258,  143,  144,  258,  258,  258,  258,
 /*   470 */   233,  233,  257,  233,  233,  233,  233,  196,  196,  196,
 /*   480 */   196,  196,  196,  196,  240,  240,  196,  266,  266,  240,
 /*   490 */   244,  196,  196,  196,   61,  196,  196,  196,  262,   86,
 /*   500 */   206,  232,  246,  122,  254,  252,  196,  253,  251,  250,
 /*   510 */   262,  249,  196,  262,  262,  127,  131,  133,  132,  196,
 /*   520 */   196,  196,  196,  196,  196,  135,  196,  126,  196,  125,
 /*   530 */   196,  196,  196,  124,  196,  196,  196,  196,  196,  196,
 /*   540 */   196,  196,  196,  123,  196,  196,  196,  196,  196,  137,
 /*   550 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   560 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   570 */   196,  196,  196,  109,  197,   93,  197,  197,   92,  197,
 /*   580 */   197,   51,   89,   91,   55,   90,   88,   81,  197,    5,
 /*   590 */   197,  197,  150,    5,    5,  197,  197,  150,  202,  202,
 /*   600 */     5,  206,    5,   96,   95,  140,  118,  113,  119,  110,
 /*   610 */     1,  111,  197,  197,  208,  215,  209,  214,  213,  198,
 /*   620 */   212,  210,  198,  198,  197,  211,  198,  197,  197,  203,
 /*   630 */   110,  199,  115,  115,  111,  248,  247,  245,  110,  115,
 /*   640 */   115,  110,  232,  111,  110,   78,  111,  110,  115,  128,
 /*   650 */   128,  111,  110,  110,  116,  110,  110,  113,   84,   83,
 /*   660 */     5,  114,   71,   84,   83,    9,    5,    5,    5,    5,
 /*   670 */     5,   78,   82,   15,  115,   16,    5,    5,    5,  111,
 /*   680 */   144,    5,    5,  144,    5,  144,    5,    5,    5,    5,
 /*   690 */     5,    5,    5,    5,    5,    5,    5,    5,    5,   82,
 /*   700 */   115,   21,   61,   60,    0,  270,  270,  270,  270,  270,
 /*   710 */   270,  270,  270,  270,  270,   21,  270,  270,  270,  270,
 /*   720 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   730 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   740 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   750 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   760 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   770 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   780 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   790 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   800 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   810 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   820 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   830 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   840 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   850 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   860 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   870 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   880 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   890 */   270,  270,  270,  270,  270,  270,  270,  270,  270,  270,
 /*   900 */   270,  270,  270,  270,  270,  270,  270,  270,
};
#define YY_SHIFT_COUNT    (337)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (704)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   78,   78,  180,  180,   30,  226,  239,  141,    9,
 /*    10 */     9,    9,    9,    9,    9,    9,    9,    9,    0,    2,
 /*    20 */   239,  288,  288,  288,  288,   37,   37,    9,    9,    9,
 /*    30 */   229,    9,    9,    9,    9,  279,   30,   54,   54,  716,
 /*    40 */   716,  716,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  288,  288,  183,  183,  183,  183,  183,  183,
 /*    70 */   183,    9,    9,    9,  276,    9,    9,    9,   37,   37,
 /*    80 */     9,    9,    9,  264,  264,  292,   37,    9,    9,    9,
 /*    90 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   100 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   110 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   120 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   130 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   140 */     9,    9,    9,  433,  433,  433,  381,  381,  381,  433,
 /*   150 */   381,  433,  384,  390,  386,  388,  385,  401,  404,  409,
 /*   160 */   420,  412,  433,  433,  433,  413,  413,  464,   30,   30,
 /*   170 */   433,  433,  482,  486,  530,  493,  492,  529,  495,  498,
 /*   180 */   464,  433,  506,  506,  433,  506,  433,  506,  433,  433,
 /*   190 */   716,  716,   52,   79,  106,   79,   79,  132,  187,  278,
 /*   200 */   278,  278,  278,  255,  306,  318,  313,  313,  313,  313,
 /*   210 */   192,   94,    7,    7,  130,  275,  135,  263,  333,  347,
 /*   220 */   291,  298,  303,  304,  307,  309,  312,  314,  402,  233,
 /*   230 */   320,  302,  311,  317,  319,  322,  329,  331,  337,  305,
 /*   240 */   315,  321,  197,  308,  353,  448,  378,  584,  442,  588,
 /*   250 */   589,  447,  595,  597,  507,  509,  465,  488,  494,  499,
 /*   260 */   489,  500,  520,  517,  518,  523,  528,  532,  524,  531,
 /*   270 */   609,  534,  535,  537,  525,  521,  533,  522,  540,  542,
 /*   280 */   538,  543,  494,  545,  544,  546,  547,  567,  574,  576,
 /*   290 */   591,  655,  579,  581,  656,  661,  662,  663,  664,  665,
 /*   300 */   590,  658,  593,  536,  559,  559,  659,  539,  541,  559,
 /*   310 */   671,  672,  568,  559,  673,  676,  677,  679,  681,  682,
 /*   320 */   683,  684,  685,  686,  687,  688,  689,  690,  691,  692,
 /*   330 */   693,  585,  617,  680,  694,  641,  643,  704,
};
#define YY_REDUCE_COUNT (191)
#define YY_REDUCE_MIN   (-256)
#define YY_REDUCE_MAX   (432)
static const short yy_reduce_ofst[] = {
 /*     0 */  -158,  -29,  -29,   38,   38, -194, -239, -160, -190,  -32,
 /*    10 */  -129,   26,   60,  103,  118,  127,  134,  157, -196, -191,
 /*    20 */  -148, -210, -155,   33,   50,   -8,   15, -184, -181, -165,
 /*    30 */   137,  -81,  128,  149,   90, -113,  -74,  -39,  166, -142,
 /*    40 */   159,  185, -256, -236, -133, -107,  -91,  -19,   -5,   21,
 /*    50 */   129,  173,  177,  196,  202,  203,  204,  205,  208,  209,
 /*    60 */   210,  211,  -23,  199,   63,  237,  238,  240,  241,  242,
 /*    70 */   243,  247,  281,  282,  215,  283,  284,  285,  244,  245,
 /*    80 */   286,  287,  290,  221,  222,  246,  249,  295,  296,  297,
 /*    90 */   299,  300,  301,  310,  316,  323,  324,  325,  326,  327,
 /*   100 */   328,  330,  332,  334,  335,  336,  338,  339,  340,  341,
 /*   110 */   342,  343,  344,  345,  346,  348,  349,  350,  351,  352,
 /*   120 */   354,  355,  356,  357,  358,  359,  360,  361,  362,  363,
 /*   130 */   364,  365,  366,  367,  368,  369,  370,  371,  372,  373,
 /*   140 */   374,  375,  376,  377,  379,  380,  236,  248,  251,  382,
 /*   150 */   252,  383,  250,  254,  253,  257,  259,  262,  387,  389,
 /*   160 */   256,  392,  391,  393,  394,  294,  395,  269,  396,  397,
 /*   170 */   398,  399,  400,  403,  405,  407,  408,  411,  414,  406,
 /*   180 */   410,  415,  421,  424,  416,  425,  427,  428,  430,  431,
 /*   190 */   426,  432,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   817,  936,  878,  948,  866,  875, 1082, 1082,  817,  817,
 /*    10 */   817,  817,  817,  817,  817,  817,  817,  817,  995,  837,
 /*    20 */  1082,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*    30 */   875,  817,  817,  817,  817,  885,  875,  885,  885,  990,
 /*    40 */   920,  938,  817,  817,  817,  817,  817,  817,  817,  817,
 /*    50 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*    60 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*    70 */   817,  817,  817,  817,  997, 1003, 1000,  817,  817,  817,
 /*    80 */  1005,  817,  817, 1025, 1025,  988,  817,  817,  817,  817,
 /*    90 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*   100 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*   110 */   817,  817,  817,  817,  817,  817,  817,  817,  864,  817,
 /*   120 */   862,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*   130 */   817,  817,  817,  817,  817,  817,  848,  817,  817,  817,
 /*   140 */   817,  817,  817,  839,  839,  839,  817,  817,  817,  839,
 /*   150 */   817,  839, 1032, 1036, 1030, 1018, 1026, 1017, 1013, 1011,
 /*   160 */  1010, 1040,  839,  839,  839,  883,  883,  879,  875,  875,
 /*   170 */   839,  839,  901,  899,  897,  889,  895,  891,  893,  887,
 /*   180 */   867,  839,  873,  873,  839,  873,  839,  873,  839,  839,
 /*   190 */   920,  938,  817, 1041,  817, 1081, 1031, 1071, 1070, 1077,
 /*   200 */  1069, 1068, 1067,  817,  817,  817, 1063, 1064, 1066, 1065,
 /*   210 */   817,  817, 1073, 1072,  817,  817,  817,  817,  817,  817,
 /*   220 */   817,  817,  817,  817,  817,  817,  817,  817,  817, 1043,
 /*   230 */   817, 1037, 1033,  817,  817,  817,  817,  817,  817,  817,
 /*   240 */   817,  817,  950,  817,  817,  817,  817,  817,  817,  817,
 /*   250 */   817,  817,  817,  817,  817,  817,  817,  987,  817,  817,
 /*   260 */   817,  817,  817,  999,  998,  817,  817,  817,  817,  817,
 /*   270 */   817,  817,  817,  817, 1027,  817, 1019,  817,  817,  817,
 /*   280 */   817,  817,  962,  817,  817,  817,  817,  817,  817,  817,
 /*   290 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*   300 */   817,  817,  817,  817, 1093, 1091,  817,  817,  817, 1087,
 /*   310 */   817,  817,  817, 1085,  817,  817,  817,  817,  817,  817,
 /*   320 */   817,  817,  817,  817,  817,  817,  817,  817,  817,  817,
 /*   330 */   817,  904,  817,  846,  844,  817,  835,  817,
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
    0,  /*         ID => nothing */
    1,  /*       BOOL => ID */
    1,  /*    TINYINT => ID */
    1,  /*   SMALLINT => ID */
    1,  /*    INTEGER => ID */
    1,  /*     BIGINT => ID */
    1,  /*      FLOAT => ID */
    1,  /*     DOUBLE => ID */
    1,  /*     STRING => ID */
    1,  /*  TIMESTAMP => ID */
    1,  /*     BINARY => ID */
    1,  /*      NCHAR => ID */
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*        NOT => nothing */
    0,  /*         EQ => nothing */
    0,  /*         NE => nothing */
    0,  /*     ISNULL => nothing */
    0,  /*    NOTNULL => nothing */
    0,  /*         IS => nothing */
    1,  /*       LIKE => ID */
    1,  /*       GLOB => ID */
    0,  /*    BETWEEN => nothing */
    0,  /*         IN => nothing */
    0,  /*         GT => nothing */
    0,  /*         GE => nothing */
    0,  /*         LT => nothing */
    0,  /*         LE => nothing */
    0,  /*     BITAND => nothing */
    0,  /*      BITOR => nothing */
    0,  /*     LSHIFT => nothing */
    0,  /*     RSHIFT => nothing */
    0,  /*       PLUS => nothing */
    0,  /*      MINUS => nothing */
    0,  /*     DIVIDE => nothing */
    0,  /*      TIMES => nothing */
    0,  /*       STAR => nothing */
    0,  /*      SLASH => nothing */
    0,  /*        REM => nothing */
    0,  /*     CONCAT => nothing */
    0,  /*     UMINUS => nothing */
    0,  /*      UPLUS => nothing */
    0,  /*     BITNOT => nothing */
    0,  /*       SHOW => nothing */
    0,  /*  DATABASES => nothing */
    0,  /*     TOPICS => nothing */
    0,  /*  FUNCTIONS => nothing */
    0,  /*     MNODES => nothing */
    0,  /*     DNODES => nothing */
    0,  /*   ACCOUNTS => nothing */
    0,  /*      USERS => nothing */
    0,  /*    MODULES => nothing */
    0,  /*    QUERIES => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*  VARIABLES => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     CREATE => nothing */
    0,  /*      TABLE => nothing */
    1,  /*     STABLE => ID */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TOPIC => nothing */
    0,  /*   FUNCTION => nothing */
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*         AS => nothing */
    0,  /* OUTPUTTYPE => nothing */
    0,  /*  AGGREGATE => nothing */
    0,  /*    BUFSIZE => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*       KEEP => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*     QUORUM => nothing */
    0,  /*       DAYS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*     BLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*        WAL => nothing */
    0,  /*      FSYNC => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*     UPDATE => nothing */
    0,  /*  CACHELAST => nothing */
    0,  /* PARTITIONS => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*    SESSION => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    1,  /*       DESC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
    1,  /*        NOW => ID */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*     SYNCDB => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*        TAG => nothing */
    0,  /*     CHANGE => nothing */
    0,  /*        SET => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /*     STREAM => nothing */
    0,  /*      COLON => nothing */
    1,  /*      ABORT => ID */
    1,  /*      AFTER => ID */
    1,  /*     ATTACH => ID */
    1,  /*     BEFORE => ID */
    1,  /*      BEGIN => ID */
    1,  /*    CASCADE => ID */
    1,  /*    CLUSTER => ID */
    1,  /*   CONFLICT => ID */
    1,  /*       COPY => ID */
    1,  /*   DEFERRED => ID */
    1,  /* DELIMITERS => ID */
    1,  /*     DETACH => ID */
    1,  /*       EACH => ID */
    1,  /*        END => ID */
    1,  /*    EXPLAIN => ID */
    1,  /*       FAIL => ID */
    1,  /*        FOR => ID */
    1,  /*     IGNORE => ID */
    1,  /*  IMMEDIATE => ID */
    1,  /*  INITIALLY => ID */
    1,  /*    INSTEAD => ID */
    1,  /*      MATCH => ID */
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*     INSERT => ID */
    1,  /*       INTO => ID */
    1,  /*     VALUES => ID */
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
  /*    1 */ "ID",
  /*    2 */ "BOOL",
  /*    3 */ "TINYINT",
  /*    4 */ "SMALLINT",
  /*    5 */ "INTEGER",
  /*    6 */ "BIGINT",
  /*    7 */ "FLOAT",
  /*    8 */ "DOUBLE",
  /*    9 */ "STRING",
  /*   10 */ "TIMESTAMP",
  /*   11 */ "BINARY",
  /*   12 */ "NCHAR",
  /*   13 */ "OR",
  /*   14 */ "AND",
  /*   15 */ "NOT",
  /*   16 */ "EQ",
  /*   17 */ "NE",
  /*   18 */ "ISNULL",
  /*   19 */ "NOTNULL",
  /*   20 */ "IS",
  /*   21 */ "LIKE",
  /*   22 */ "GLOB",
  /*   23 */ "BETWEEN",
  /*   24 */ "IN",
  /*   25 */ "GT",
  /*   26 */ "GE",
  /*   27 */ "LT",
  /*   28 */ "LE",
  /*   29 */ "BITAND",
  /*   30 */ "BITOR",
  /*   31 */ "LSHIFT",
  /*   32 */ "RSHIFT",
  /*   33 */ "PLUS",
  /*   34 */ "MINUS",
  /*   35 */ "DIVIDE",
  /*   36 */ "TIMES",
  /*   37 */ "STAR",
  /*   38 */ "SLASH",
  /*   39 */ "REM",
  /*   40 */ "CONCAT",
  /*   41 */ "UMINUS",
  /*   42 */ "UPLUS",
  /*   43 */ "BITNOT",
  /*   44 */ "SHOW",
  /*   45 */ "DATABASES",
  /*   46 */ "TOPICS",
  /*   47 */ "FUNCTIONS",
  /*   48 */ "MNODES",
  /*   49 */ "DNODES",
  /*   50 */ "ACCOUNTS",
  /*   51 */ "USERS",
  /*   52 */ "MODULES",
  /*   53 */ "QUERIES",
  /*   54 */ "CONNECTIONS",
  /*   55 */ "STREAMS",
  /*   56 */ "VARIABLES",
  /*   57 */ "SCORES",
  /*   58 */ "GRANTS",
  /*   59 */ "VNODES",
  /*   60 */ "IPTOKEN",
  /*   61 */ "DOT",
  /*   62 */ "CREATE",
  /*   63 */ "TABLE",
  /*   64 */ "STABLE",
  /*   65 */ "DATABASE",
  /*   66 */ "TABLES",
  /*   67 */ "STABLES",
  /*   68 */ "VGROUPS",
  /*   69 */ "DROP",
  /*   70 */ "TOPIC",
  /*   71 */ "FUNCTION",
  /*   72 */ "DNODE",
  /*   73 */ "USER",
  /*   74 */ "ACCOUNT",
  /*   75 */ "USE",
  /*   76 */ "DESCRIBE",
  /*   77 */ "ALTER",
  /*   78 */ "PASS",
  /*   79 */ "PRIVILEGE",
  /*   80 */ "LOCAL",
  /*   81 */ "IF",
  /*   82 */ "EXISTS",
  /*   83 */ "AS",
  /*   84 */ "OUTPUTTYPE",
  /*   85 */ "AGGREGATE",
  /*   86 */ "BUFSIZE",
  /*   87 */ "PPS",
  /*   88 */ "TSERIES",
  /*   89 */ "DBS",
  /*   90 */ "STORAGE",
  /*   91 */ "QTIME",
  /*   92 */ "CONNS",
  /*   93 */ "STATE",
  /*   94 */ "KEEP",
  /*   95 */ "CACHE",
  /*   96 */ "REPLICA",
  /*   97 */ "QUORUM",
  /*   98 */ "DAYS",
  /*   99 */ "MINROWS",
  /*  100 */ "MAXROWS",
  /*  101 */ "BLOCKS",
  /*  102 */ "CTIME",
  /*  103 */ "WAL",
  /*  104 */ "FSYNC",
  /*  105 */ "COMP",
  /*  106 */ "PRECISION",
  /*  107 */ "UPDATE",
  /*  108 */ "CACHELAST",
  /*  109 */ "PARTITIONS",
  /*  110 */ "LP",
  /*  111 */ "RP",
  /*  112 */ "UNSIGNED",
  /*  113 */ "TAGS",
  /*  114 */ "USING",
  /*  115 */ "COMMA",
  /*  116 */ "NULL",
  /*  117 */ "SELECT",
  /*  118 */ "UNION",
  /*  119 */ "ALL",
  /*  120 */ "DISTINCT",
  /*  121 */ "FROM",
  /*  122 */ "VARIABLE",
  /*  123 */ "INTERVAL",
  /*  124 */ "SESSION",
  /*  125 */ "FILL",
  /*  126 */ "SLIDING",
  /*  127 */ "ORDER",
  /*  128 */ "BY",
  /*  129 */ "ASC",
  /*  130 */ "DESC",
  /*  131 */ "GROUP",
  /*  132 */ "HAVING",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "SLIMIT",
  /*  136 */ "SOFFSET",
  /*  137 */ "WHERE",
  /*  138 */ "NOW",
  /*  139 */ "RESET",
  /*  140 */ "QUERY",
  /*  141 */ "SYNCDB",
  /*  142 */ "ADD",
  /*  143 */ "COLUMN",
  /*  144 */ "TAG",
  /*  145 */ "CHANGE",
  /*  146 */ "SET",
  /*  147 */ "KILL",
  /*  148 */ "CONNECTION",
  /*  149 */ "STREAM",
  /*  150 */ "COLON",
  /*  151 */ "ABORT",
  /*  152 */ "AFTER",
  /*  153 */ "ATTACH",
  /*  154 */ "BEFORE",
  /*  155 */ "BEGIN",
  /*  156 */ "CASCADE",
  /*  157 */ "CLUSTER",
  /*  158 */ "CONFLICT",
  /*  159 */ "COPY",
  /*  160 */ "DEFERRED",
  /*  161 */ "DELIMITERS",
  /*  162 */ "DETACH",
  /*  163 */ "EACH",
  /*  164 */ "END",
  /*  165 */ "EXPLAIN",
  /*  166 */ "FAIL",
  /*  167 */ "FOR",
  /*  168 */ "IGNORE",
  /*  169 */ "IMMEDIATE",
  /*  170 */ "INITIALLY",
  /*  171 */ "INSTEAD",
  /*  172 */ "MATCH",
  /*  173 */ "KEY",
  /*  174 */ "OF",
  /*  175 */ "RAISE",
  /*  176 */ "REPLACE",
  /*  177 */ "RESTRICT",
  /*  178 */ "ROW",
  /*  179 */ "STATEMENT",
  /*  180 */ "TRIGGER",
  /*  181 */ "VIEW",
  /*  182 */ "SEMI",
  /*  183 */ "NONE",
  /*  184 */ "PREV",
  /*  185 */ "LINEAR",
  /*  186 */ "IMPORT",
  /*  187 */ "TBNAME",
  /*  188 */ "JOIN",
  /*  189 */ "INSERT",
  /*  190 */ "INTO",
  /*  191 */ "VALUES",
  /*  192 */ "error",
  /*  193 */ "program",
  /*  194 */ "cmd",
  /*  195 */ "dbPrefix",
  /*  196 */ "ids",
  /*  197 */ "cpxName",
  /*  198 */ "ifexists",
  /*  199 */ "alter_db_optr",
  /*  200 */ "alter_topic_optr",
  /*  201 */ "acct_optr",
  /*  202 */ "ifnotexists",
  /*  203 */ "db_optr",
  /*  204 */ "topic_optr",
  /*  205 */ "typename",
  /*  206 */ "bufsize",
  /*  207 */ "pps",
  /*  208 */ "tseries",
  /*  209 */ "dbs",
  /*  210 */ "streams",
  /*  211 */ "storage",
  /*  212 */ "qtime",
  /*  213 */ "users",
  /*  214 */ "conns",
  /*  215 */ "state",
  /*  216 */ "keep",
  /*  217 */ "tagitemlist",
  /*  218 */ "cache",
  /*  219 */ "replica",
  /*  220 */ "quorum",
  /*  221 */ "days",
  /*  222 */ "minrows",
  /*  223 */ "maxrows",
  /*  224 */ "blocks",
  /*  225 */ "ctime",
  /*  226 */ "wal",
  /*  227 */ "fsync",
  /*  228 */ "comp",
  /*  229 */ "prec",
  /*  230 */ "update",
  /*  231 */ "cachelast",
  /*  232 */ "partitions",
  /*  233 */ "signed",
  /*  234 */ "create_table_args",
  /*  235 */ "create_stable_args",
  /*  236 */ "create_table_list",
  /*  237 */ "create_from_stable",
  /*  238 */ "columnlist",
  /*  239 */ "tagNamelist",
  /*  240 */ "select",
  /*  241 */ "column",
  /*  242 */ "tagitem",
  /*  243 */ "selcollist",
  /*  244 */ "from",
  /*  245 */ "where_opt",
  /*  246 */ "interval_opt",
  /*  247 */ "session_option",
  /*  248 */ "fill_opt",
  /*  249 */ "sliding_opt",
  /*  250 */ "groupby_opt",
  /*  251 */ "orderby_opt",
  /*  252 */ "having_opt",
  /*  253 */ "slimit_opt",
  /*  254 */ "limit_opt",
  /*  255 */ "union",
  /*  256 */ "sclp",
  /*  257 */ "distinct",
  /*  258 */ "expr",
  /*  259 */ "as",
  /*  260 */ "tablelist",
  /*  261 */ "sub",
  /*  262 */ "tmvar",
  /*  263 */ "sortlist",
  /*  264 */ "sortitem",
  /*  265 */ "item",
  /*  266 */ "sortorder",
  /*  267 */ "grouplist",
  /*  268 */ "exprlist",
  /*  269 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW FUNCTIONS",
 /*   4 */ "cmd ::= SHOW MNODES",
 /*   5 */ "cmd ::= SHOW DNODES",
 /*   6 */ "cmd ::= SHOW ACCOUNTS",
 /*   7 */ "cmd ::= SHOW USERS",
 /*   8 */ "cmd ::= SHOW MODULES",
 /*   9 */ "cmd ::= SHOW QUERIES",
 /*  10 */ "cmd ::= SHOW CONNECTIONS",
 /*  11 */ "cmd ::= SHOW STREAMS",
 /*  12 */ "cmd ::= SHOW VARIABLES",
 /*  13 */ "cmd ::= SHOW SCORES",
 /*  14 */ "cmd ::= SHOW GRANTS",
 /*  15 */ "cmd ::= SHOW VNODES",
 /*  16 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  17 */ "dbPrefix ::=",
 /*  18 */ "dbPrefix ::= ids DOT",
 /*  19 */ "cpxName ::=",
 /*  20 */ "cpxName ::= DOT ids",
 /*  21 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  22 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
 /*  23 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  27 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  29 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  30 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  32 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  33 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  34 */ "cmd ::= DROP FUNCTION ids",
 /*  35 */ "cmd ::= DROP DNODE ids",
 /*  36 */ "cmd ::= DROP USER ids",
 /*  37 */ "cmd ::= DROP ACCOUNT ids",
 /*  38 */ "cmd ::= USE ids",
 /*  39 */ "cmd ::= DESCRIBE ids cpxName",
 /*  40 */ "cmd ::= ALTER USER ids PASS ids",
 /*  41 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids ids",
 /*  46 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  47 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  49 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  50 */ "ids ::= ID",
 /*  51 */ "ids ::= STRING",
 /*  52 */ "ifexists ::= IF EXISTS",
 /*  53 */ "ifexists ::=",
 /*  54 */ "ifnotexists ::= IF NOT EXISTS",
 /*  55 */ "ifnotexists ::=",
 /*  56 */ "cmd ::= CREATE DNODE ids",
 /*  57 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  58 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  59 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  60 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  61 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE USER ids PASS ids",
 /*  63 */ "bufsize ::=",
 /*  64 */ "bufsize ::= BUFSIZE INTEGER",
 /*  65 */ "pps ::=",
 /*  66 */ "pps ::= PPS INTEGER",
 /*  67 */ "tseries ::=",
 /*  68 */ "tseries ::= TSERIES INTEGER",
 /*  69 */ "dbs ::=",
 /*  70 */ "dbs ::= DBS INTEGER",
 /*  71 */ "streams ::=",
 /*  72 */ "streams ::= STREAMS INTEGER",
 /*  73 */ "storage ::=",
 /*  74 */ "storage ::= STORAGE INTEGER",
 /*  75 */ "qtime ::=",
 /*  76 */ "qtime ::= QTIME INTEGER",
 /*  77 */ "users ::=",
 /*  78 */ "users ::= USERS INTEGER",
 /*  79 */ "conns ::=",
 /*  80 */ "conns ::= CONNS INTEGER",
 /*  81 */ "state ::=",
 /*  82 */ "state ::= STATE ids",
 /*  83 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  84 */ "keep ::= KEEP tagitemlist",
 /*  85 */ "cache ::= CACHE INTEGER",
 /*  86 */ "replica ::= REPLICA INTEGER",
 /*  87 */ "quorum ::= QUORUM INTEGER",
 /*  88 */ "days ::= DAYS INTEGER",
 /*  89 */ "minrows ::= MINROWS INTEGER",
 /*  90 */ "maxrows ::= MAXROWS INTEGER",
 /*  91 */ "blocks ::= BLOCKS INTEGER",
 /*  92 */ "ctime ::= CTIME INTEGER",
 /*  93 */ "wal ::= WAL INTEGER",
 /*  94 */ "fsync ::= FSYNC INTEGER",
 /*  95 */ "comp ::= COMP INTEGER",
 /*  96 */ "prec ::= PRECISION STRING",
 /*  97 */ "update ::= UPDATE INTEGER",
 /*  98 */ "cachelast ::= CACHELAST INTEGER",
 /*  99 */ "partitions ::= PARTITIONS INTEGER",
 /* 100 */ "db_optr ::=",
 /* 101 */ "db_optr ::= db_optr cache",
 /* 102 */ "db_optr ::= db_optr replica",
 /* 103 */ "db_optr ::= db_optr quorum",
 /* 104 */ "db_optr ::= db_optr days",
 /* 105 */ "db_optr ::= db_optr minrows",
 /* 106 */ "db_optr ::= db_optr maxrows",
 /* 107 */ "db_optr ::= db_optr blocks",
 /* 108 */ "db_optr ::= db_optr ctime",
 /* 109 */ "db_optr ::= db_optr wal",
 /* 110 */ "db_optr ::= db_optr fsync",
 /* 111 */ "db_optr ::= db_optr comp",
 /* 112 */ "db_optr ::= db_optr prec",
 /* 113 */ "db_optr ::= db_optr keep",
 /* 114 */ "db_optr ::= db_optr update",
 /* 115 */ "db_optr ::= db_optr cachelast",
 /* 116 */ "topic_optr ::= db_optr",
 /* 117 */ "topic_optr ::= topic_optr partitions",
 /* 118 */ "alter_db_optr ::=",
 /* 119 */ "alter_db_optr ::= alter_db_optr replica",
 /* 120 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 121 */ "alter_db_optr ::= alter_db_optr keep",
 /* 122 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 123 */ "alter_db_optr ::= alter_db_optr comp",
 /* 124 */ "alter_db_optr ::= alter_db_optr wal",
 /* 125 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 126 */ "alter_db_optr ::= alter_db_optr update",
 /* 127 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 128 */ "alter_topic_optr ::= alter_db_optr",
 /* 129 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 130 */ "typename ::= ids",
 /* 131 */ "typename ::= ids LP signed RP",
 /* 132 */ "typename ::= ids UNSIGNED",
 /* 133 */ "signed ::= INTEGER",
 /* 134 */ "signed ::= PLUS INTEGER",
 /* 135 */ "signed ::= MINUS INTEGER",
 /* 136 */ "cmd ::= CREATE TABLE create_table_args",
 /* 137 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 138 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 139 */ "cmd ::= CREATE TABLE create_table_list",
 /* 140 */ "create_table_list ::= create_from_stable",
 /* 141 */ "create_table_list ::= create_table_list create_from_stable",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 143 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 144 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 145 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 146 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 147 */ "tagNamelist ::= ids",
 /* 148 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 149 */ "columnlist ::= columnlist COMMA column",
 /* 150 */ "columnlist ::= column",
 /* 151 */ "column ::= ids typename",
 /* 152 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 153 */ "tagitemlist ::= tagitem",
 /* 154 */ "tagitem ::= INTEGER",
 /* 155 */ "tagitem ::= FLOAT",
 /* 156 */ "tagitem ::= STRING",
 /* 157 */ "tagitem ::= BOOL",
 /* 158 */ "tagitem ::= NULL",
 /* 159 */ "tagitem ::= MINUS INTEGER",
 /* 160 */ "tagitem ::= MINUS FLOAT",
 /* 161 */ "tagitem ::= PLUS INTEGER",
 /* 162 */ "tagitem ::= PLUS FLOAT",
 /* 163 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 164 */ "select ::= LP select RP",
 /* 165 */ "union ::= select",
 /* 166 */ "union ::= union UNION ALL select",
 /* 167 */ "cmd ::= union",
 /* 168 */ "select ::= SELECT selcollist",
 /* 169 */ "sclp ::= selcollist COMMA",
 /* 170 */ "sclp ::=",
 /* 171 */ "selcollist ::= sclp distinct expr as",
 /* 172 */ "selcollist ::= sclp STAR",
 /* 173 */ "as ::= AS ids",
 /* 174 */ "as ::= ids",
 /* 175 */ "as ::=",
 /* 176 */ "distinct ::= DISTINCT",
 /* 177 */ "distinct ::=",
 /* 178 */ "from ::= FROM tablelist",
 /* 179 */ "from ::= FROM sub",
 /* 180 */ "sub ::= LP union RP",
 /* 181 */ "sub ::= LP union RP ids",
 /* 182 */ "sub ::= sub COMMA LP union RP ids",
 /* 183 */ "tablelist ::= ids cpxName",
 /* 184 */ "tablelist ::= ids cpxName ids",
 /* 185 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 186 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 187 */ "tmvar ::= VARIABLE",
 /* 188 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 189 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 190 */ "interval_opt ::=",
 /* 191 */ "session_option ::=",
 /* 192 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 193 */ "fill_opt ::=",
 /* 194 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 195 */ "fill_opt ::= FILL LP ID RP",
 /* 196 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 197 */ "sliding_opt ::=",
 /* 198 */ "orderby_opt ::=",
 /* 199 */ "orderby_opt ::= ORDER BY sortlist",
 /* 200 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 201 */ "sortlist ::= item sortorder",
 /* 202 */ "item ::= ids cpxName",
 /* 203 */ "sortorder ::= ASC",
 /* 204 */ "sortorder ::= DESC",
 /* 205 */ "sortorder ::=",
 /* 206 */ "groupby_opt ::=",
 /* 207 */ "groupby_opt ::= GROUP BY grouplist",
 /* 208 */ "grouplist ::= grouplist COMMA item",
 /* 209 */ "grouplist ::= item",
 /* 210 */ "having_opt ::=",
 /* 211 */ "having_opt ::= HAVING expr",
 /* 212 */ "limit_opt ::=",
 /* 213 */ "limit_opt ::= LIMIT signed",
 /* 214 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 215 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 216 */ "slimit_opt ::=",
 /* 217 */ "slimit_opt ::= SLIMIT signed",
 /* 218 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 219 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 220 */ "where_opt ::=",
 /* 221 */ "where_opt ::= WHERE expr",
 /* 222 */ "expr ::= LP expr RP",
 /* 223 */ "expr ::= ID",
 /* 224 */ "expr ::= ID DOT ID",
 /* 225 */ "expr ::= ID DOT STAR",
 /* 226 */ "expr ::= INTEGER",
 /* 227 */ "expr ::= MINUS INTEGER",
 /* 228 */ "expr ::= PLUS INTEGER",
 /* 229 */ "expr ::= FLOAT",
 /* 230 */ "expr ::= MINUS FLOAT",
 /* 231 */ "expr ::= PLUS FLOAT",
 /* 232 */ "expr ::= STRING",
 /* 233 */ "expr ::= NOW",
 /* 234 */ "expr ::= VARIABLE",
 /* 235 */ "expr ::= PLUS VARIABLE",
 /* 236 */ "expr ::= MINUS VARIABLE",
 /* 237 */ "expr ::= BOOL",
 /* 238 */ "expr ::= NULL",
 /* 239 */ "expr ::= ID LP exprlist RP",
 /* 240 */ "expr ::= ID LP STAR RP",
 /* 241 */ "expr ::= expr IS NULL",
 /* 242 */ "expr ::= expr IS NOT NULL",
 /* 243 */ "expr ::= expr LT expr",
 /* 244 */ "expr ::= expr GT expr",
 /* 245 */ "expr ::= expr LE expr",
 /* 246 */ "expr ::= expr GE expr",
 /* 247 */ "expr ::= expr NE expr",
 /* 248 */ "expr ::= expr EQ expr",
 /* 249 */ "expr ::= expr BETWEEN expr AND expr",
 /* 250 */ "expr ::= expr AND expr",
 /* 251 */ "expr ::= expr OR expr",
 /* 252 */ "expr ::= expr PLUS expr",
 /* 253 */ "expr ::= expr MINUS expr",
 /* 254 */ "expr ::= expr STAR expr",
 /* 255 */ "expr ::= expr SLASH expr",
 /* 256 */ "expr ::= expr REM expr",
 /* 257 */ "expr ::= expr LIKE expr",
 /* 258 */ "expr ::= expr IN LP exprlist RP",
 /* 259 */ "exprlist ::= exprlist COMMA expritem",
 /* 260 */ "exprlist ::= expritem",
 /* 261 */ "expritem ::= expr",
 /* 262 */ "expritem ::=",
 /* 263 */ "cmd ::= RESET QUERY CACHE",
 /* 264 */ "cmd ::= SYNCDB ids REPLICA",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 271 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 272 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 276 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 277 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 278 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
void ParseInit(void *yypParser){
  yyParser *pParser = (yyParser*)yypParser;
#ifdef YYTRACKMAXSTACKDEPTH
  pParser->yyhwm = 0;
#endif
#if YYSTACKDEPTH<=0
  pParser->yytos = NULL;
  pParser->yystack = NULL;
  pParser->yystksz = 0;
  if( yyGrowStack(pParser) ){
    pParser->yystack = &pParser->yystk0;
    pParser->yystksz = 1;
  }
#endif
#ifndef YYNOERRORRECOVERY
  pParser->yyerrcnt = -1;
#endif
  pParser->yytos = pParser->yystack;
  pParser->yystack[0].stateno = 0;
  pParser->yystack[0].major = 0;
#if YYSTACKDEPTH>0
  pParser->yystackEnd = &pParser->yystack[YYSTACKDEPTH-1];
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( pParser ) ParseInit(pParser);
  return pParser;
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
  ParseARG_FETCH;
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
    case 216: /* keep */
    case 217: /* tagitemlist */
    case 238: /* columnlist */
    case 239: /* tagNamelist */
    case 248: /* fill_opt */
    case 250: /* groupby_opt */
    case 251: /* orderby_opt */
    case 263: /* sortlist */
    case 267: /* grouplist */
{
taosArrayDestroy((yypminor->yy441));
}
      break;
    case 236: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy182));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy236));
}
      break;
    case 243: /* selcollist */
    case 256: /* sclp */
    case 268: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy441));
}
      break;
    case 244: /* from */
    case 260: /* tablelist */
    case 261: /* sub */
{
destroyRelationInfo((yypminor->yy244));
}
      break;
    case 245: /* where_opt */
    case 252: /* having_opt */
    case 258: /* expr */
    case 269: /* expritem */
{
tSqlExprDestroy((yypminor->yy166));
}
      break;
    case 255: /* union */
{
destroyAllSqlNode((yypminor->yy441));
}
      break;
    case 264: /* sortitem */
{
tVariantDestroy(&(yypminor->yy506));
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
static unsigned int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yytos->stateno;
 
  if( stateno>YY_MAX_SHIFT ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
#if defined(YYCOVERAGE)
  yycoverage[stateno][iLookAhead] = 1;
#endif
  do{
    i = yy_shift_ofst[stateno];
    assert( i>=0 && i+YYNTOKEN<=sizeof(yy_lookahead)/sizeof(yy_lookahead[0]) );
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
  int stateno,              /* Current state number */
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
   ParseARG_FETCH;
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
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
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
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
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
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
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
  {  193,   -1 }, /* (0) program ::= cmd */
  {  194,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  194,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  194,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  194,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  194,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  194,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  194,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  194,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  194,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  194,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  194,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  194,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  194,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  194,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  194,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  194,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  195,    0 }, /* (17) dbPrefix ::= */
  {  195,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  197,    0 }, /* (19) cpxName ::= */
  {  197,   -2 }, /* (20) cpxName ::= DOT ids */
  {  194,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  194,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  194,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  194,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  194,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  194,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  194,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  194,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  194,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  194,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  194,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  194,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  194,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  194,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  194,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  194,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  194,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  194,   -2 }, /* (38) cmd ::= USE ids */
  {  194,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  194,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  194,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  194,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  194,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  194,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  194,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  194,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  194,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  194,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  194,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  196,   -1 }, /* (50) ids ::= ID */
  {  196,   -1 }, /* (51) ids ::= STRING */
  {  198,   -2 }, /* (52) ifexists ::= IF EXISTS */
  {  198,    0 }, /* (53) ifexists ::= */
  {  202,   -3 }, /* (54) ifnotexists ::= IF NOT EXISTS */
  {  202,    0 }, /* (55) ifnotexists ::= */
  {  194,   -3 }, /* (56) cmd ::= CREATE DNODE ids */
  {  194,   -6 }, /* (57) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  194,   -5 }, /* (58) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  194,   -5 }, /* (59) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  194,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  194,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  194,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  206,    0 }, /* (63) bufsize ::= */
  {  206,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  207,    0 }, /* (65) pps ::= */
  {  207,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  208,    0 }, /* (67) tseries ::= */
  {  208,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  209,    0 }, /* (69) dbs ::= */
  {  209,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  210,    0 }, /* (71) streams ::= */
  {  210,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  211,    0 }, /* (73) storage ::= */
  {  211,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  212,    0 }, /* (75) qtime ::= */
  {  212,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  213,    0 }, /* (77) users ::= */
  {  213,   -2 }, /* (78) users ::= USERS INTEGER */
  {  214,    0 }, /* (79) conns ::= */
  {  214,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  215,    0 }, /* (81) state ::= */
  {  215,   -2 }, /* (82) state ::= STATE ids */
  {  201,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  216,   -2 }, /* (84) keep ::= KEEP tagitemlist */
  {  218,   -2 }, /* (85) cache ::= CACHE INTEGER */
  {  219,   -2 }, /* (86) replica ::= REPLICA INTEGER */
  {  220,   -2 }, /* (87) quorum ::= QUORUM INTEGER */
  {  221,   -2 }, /* (88) days ::= DAYS INTEGER */
  {  222,   -2 }, /* (89) minrows ::= MINROWS INTEGER */
  {  223,   -2 }, /* (90) maxrows ::= MAXROWS INTEGER */
  {  224,   -2 }, /* (91) blocks ::= BLOCKS INTEGER */
  {  225,   -2 }, /* (92) ctime ::= CTIME INTEGER */
  {  226,   -2 }, /* (93) wal ::= WAL INTEGER */
  {  227,   -2 }, /* (94) fsync ::= FSYNC INTEGER */
  {  228,   -2 }, /* (95) comp ::= COMP INTEGER */
  {  229,   -2 }, /* (96) prec ::= PRECISION STRING */
  {  230,   -2 }, /* (97) update ::= UPDATE INTEGER */
  {  231,   -2 }, /* (98) cachelast ::= CACHELAST INTEGER */
  {  232,   -2 }, /* (99) partitions ::= PARTITIONS INTEGER */
  {  203,    0 }, /* (100) db_optr ::= */
  {  203,   -2 }, /* (101) db_optr ::= db_optr cache */
  {  203,   -2 }, /* (102) db_optr ::= db_optr replica */
  {  203,   -2 }, /* (103) db_optr ::= db_optr quorum */
  {  203,   -2 }, /* (104) db_optr ::= db_optr days */
  {  203,   -2 }, /* (105) db_optr ::= db_optr minrows */
  {  203,   -2 }, /* (106) db_optr ::= db_optr maxrows */
  {  203,   -2 }, /* (107) db_optr ::= db_optr blocks */
  {  203,   -2 }, /* (108) db_optr ::= db_optr ctime */
  {  203,   -2 }, /* (109) db_optr ::= db_optr wal */
  {  203,   -2 }, /* (110) db_optr ::= db_optr fsync */
  {  203,   -2 }, /* (111) db_optr ::= db_optr comp */
  {  203,   -2 }, /* (112) db_optr ::= db_optr prec */
  {  203,   -2 }, /* (113) db_optr ::= db_optr keep */
  {  203,   -2 }, /* (114) db_optr ::= db_optr update */
  {  203,   -2 }, /* (115) db_optr ::= db_optr cachelast */
  {  204,   -1 }, /* (116) topic_optr ::= db_optr */
  {  204,   -2 }, /* (117) topic_optr ::= topic_optr partitions */
  {  199,    0 }, /* (118) alter_db_optr ::= */
  {  199,   -2 }, /* (119) alter_db_optr ::= alter_db_optr replica */
  {  199,   -2 }, /* (120) alter_db_optr ::= alter_db_optr quorum */
  {  199,   -2 }, /* (121) alter_db_optr ::= alter_db_optr keep */
  {  199,   -2 }, /* (122) alter_db_optr ::= alter_db_optr blocks */
  {  199,   -2 }, /* (123) alter_db_optr ::= alter_db_optr comp */
  {  199,   -2 }, /* (124) alter_db_optr ::= alter_db_optr wal */
  {  199,   -2 }, /* (125) alter_db_optr ::= alter_db_optr fsync */
  {  199,   -2 }, /* (126) alter_db_optr ::= alter_db_optr update */
  {  199,   -2 }, /* (127) alter_db_optr ::= alter_db_optr cachelast */
  {  200,   -1 }, /* (128) alter_topic_optr ::= alter_db_optr */
  {  200,   -2 }, /* (129) alter_topic_optr ::= alter_topic_optr partitions */
  {  205,   -1 }, /* (130) typename ::= ids */
  {  205,   -4 }, /* (131) typename ::= ids LP signed RP */
  {  205,   -2 }, /* (132) typename ::= ids UNSIGNED */
  {  233,   -1 }, /* (133) signed ::= INTEGER */
  {  233,   -2 }, /* (134) signed ::= PLUS INTEGER */
  {  233,   -2 }, /* (135) signed ::= MINUS INTEGER */
  {  194,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_args */
  {  194,   -3 }, /* (137) cmd ::= CREATE TABLE create_stable_args */
  {  194,   -3 }, /* (138) cmd ::= CREATE STABLE create_stable_args */
  {  194,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_list */
  {  236,   -1 }, /* (140) create_table_list ::= create_from_stable */
  {  236,   -2 }, /* (141) create_table_list ::= create_table_list create_from_stable */
  {  234,   -6 }, /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  235,  -10 }, /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  237,  -10 }, /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  237,  -13 }, /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  239,   -3 }, /* (146) tagNamelist ::= tagNamelist COMMA ids */
  {  239,   -1 }, /* (147) tagNamelist ::= ids */
  {  234,   -5 }, /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
  {  238,   -3 }, /* (149) columnlist ::= columnlist COMMA column */
  {  238,   -1 }, /* (150) columnlist ::= column */
  {  241,   -2 }, /* (151) column ::= ids typename */
  {  217,   -3 }, /* (152) tagitemlist ::= tagitemlist COMMA tagitem */
  {  217,   -1 }, /* (153) tagitemlist ::= tagitem */
  {  242,   -1 }, /* (154) tagitem ::= INTEGER */
  {  242,   -1 }, /* (155) tagitem ::= FLOAT */
  {  242,   -1 }, /* (156) tagitem ::= STRING */
  {  242,   -1 }, /* (157) tagitem ::= BOOL */
  {  242,   -1 }, /* (158) tagitem ::= NULL */
  {  242,   -2 }, /* (159) tagitem ::= MINUS INTEGER */
  {  242,   -2 }, /* (160) tagitem ::= MINUS FLOAT */
  {  242,   -2 }, /* (161) tagitem ::= PLUS INTEGER */
  {  242,   -2 }, /* (162) tagitem ::= PLUS FLOAT */
  {  240,  -13 }, /* (163) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  240,   -3 }, /* (164) select ::= LP select RP */
  {  255,   -1 }, /* (165) union ::= select */
  {  255,   -4 }, /* (166) union ::= union UNION ALL select */
  {  194,   -1 }, /* (167) cmd ::= union */
  {  240,   -2 }, /* (168) select ::= SELECT selcollist */
  {  256,   -2 }, /* (169) sclp ::= selcollist COMMA */
  {  256,    0 }, /* (170) sclp ::= */
  {  243,   -4 }, /* (171) selcollist ::= sclp distinct expr as */
  {  243,   -2 }, /* (172) selcollist ::= sclp STAR */
  {  259,   -2 }, /* (173) as ::= AS ids */
  {  259,   -1 }, /* (174) as ::= ids */
  {  259,    0 }, /* (175) as ::= */
  {  257,   -1 }, /* (176) distinct ::= DISTINCT */
  {  257,    0 }, /* (177) distinct ::= */
  {  244,   -2 }, /* (178) from ::= FROM tablelist */
  {  244,   -2 }, /* (179) from ::= FROM sub */
  {  261,   -3 }, /* (180) sub ::= LP union RP */
  {  261,   -4 }, /* (181) sub ::= LP union RP ids */
  {  261,   -6 }, /* (182) sub ::= sub COMMA LP union RP ids */
  {  260,   -2 }, /* (183) tablelist ::= ids cpxName */
  {  260,   -3 }, /* (184) tablelist ::= ids cpxName ids */
  {  260,   -4 }, /* (185) tablelist ::= tablelist COMMA ids cpxName */
  {  260,   -5 }, /* (186) tablelist ::= tablelist COMMA ids cpxName ids */
  {  262,   -1 }, /* (187) tmvar ::= VARIABLE */
  {  246,   -4 }, /* (188) interval_opt ::= INTERVAL LP tmvar RP */
  {  246,   -6 }, /* (189) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  246,    0 }, /* (190) interval_opt ::= */
  {  247,    0 }, /* (191) session_option ::= */
  {  247,   -7 }, /* (192) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  248,    0 }, /* (193) fill_opt ::= */
  {  248,   -6 }, /* (194) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  248,   -4 }, /* (195) fill_opt ::= FILL LP ID RP */
  {  249,   -4 }, /* (196) sliding_opt ::= SLIDING LP tmvar RP */
  {  249,    0 }, /* (197) sliding_opt ::= */
  {  251,    0 }, /* (198) orderby_opt ::= */
  {  251,   -3 }, /* (199) orderby_opt ::= ORDER BY sortlist */
  {  263,   -4 }, /* (200) sortlist ::= sortlist COMMA item sortorder */
  {  263,   -2 }, /* (201) sortlist ::= item sortorder */
  {  265,   -2 }, /* (202) item ::= ids cpxName */
  {  266,   -1 }, /* (203) sortorder ::= ASC */
  {  266,   -1 }, /* (204) sortorder ::= DESC */
  {  266,    0 }, /* (205) sortorder ::= */
  {  250,    0 }, /* (206) groupby_opt ::= */
  {  250,   -3 }, /* (207) groupby_opt ::= GROUP BY grouplist */
  {  267,   -3 }, /* (208) grouplist ::= grouplist COMMA item */
  {  267,   -1 }, /* (209) grouplist ::= item */
  {  252,    0 }, /* (210) having_opt ::= */
  {  252,   -2 }, /* (211) having_opt ::= HAVING expr */
  {  254,    0 }, /* (212) limit_opt ::= */
  {  254,   -2 }, /* (213) limit_opt ::= LIMIT signed */
  {  254,   -4 }, /* (214) limit_opt ::= LIMIT signed OFFSET signed */
  {  254,   -4 }, /* (215) limit_opt ::= LIMIT signed COMMA signed */
  {  253,    0 }, /* (216) slimit_opt ::= */
  {  253,   -2 }, /* (217) slimit_opt ::= SLIMIT signed */
  {  253,   -4 }, /* (218) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  253,   -4 }, /* (219) slimit_opt ::= SLIMIT signed COMMA signed */
  {  245,    0 }, /* (220) where_opt ::= */
  {  245,   -2 }, /* (221) where_opt ::= WHERE expr */
  {  258,   -3 }, /* (222) expr ::= LP expr RP */
  {  258,   -1 }, /* (223) expr ::= ID */
  {  258,   -3 }, /* (224) expr ::= ID DOT ID */
  {  258,   -3 }, /* (225) expr ::= ID DOT STAR */
  {  258,   -1 }, /* (226) expr ::= INTEGER */
  {  258,   -2 }, /* (227) expr ::= MINUS INTEGER */
  {  258,   -2 }, /* (228) expr ::= PLUS INTEGER */
  {  258,   -1 }, /* (229) expr ::= FLOAT */
  {  258,   -2 }, /* (230) expr ::= MINUS FLOAT */
  {  258,   -2 }, /* (231) expr ::= PLUS FLOAT */
  {  258,   -1 }, /* (232) expr ::= STRING */
  {  258,   -1 }, /* (233) expr ::= NOW */
  {  258,   -1 }, /* (234) expr ::= VARIABLE */
  {  258,   -2 }, /* (235) expr ::= PLUS VARIABLE */
  {  258,   -2 }, /* (236) expr ::= MINUS VARIABLE */
  {  258,   -1 }, /* (237) expr ::= BOOL */
  {  258,   -1 }, /* (238) expr ::= NULL */
  {  258,   -4 }, /* (239) expr ::= ID LP exprlist RP */
  {  258,   -4 }, /* (240) expr ::= ID LP STAR RP */
  {  258,   -3 }, /* (241) expr ::= expr IS NULL */
  {  258,   -4 }, /* (242) expr ::= expr IS NOT NULL */
  {  258,   -3 }, /* (243) expr ::= expr LT expr */
  {  258,   -3 }, /* (244) expr ::= expr GT expr */
  {  258,   -3 }, /* (245) expr ::= expr LE expr */
  {  258,   -3 }, /* (246) expr ::= expr GE expr */
  {  258,   -3 }, /* (247) expr ::= expr NE expr */
  {  258,   -3 }, /* (248) expr ::= expr EQ expr */
  {  258,   -5 }, /* (249) expr ::= expr BETWEEN expr AND expr */
  {  258,   -3 }, /* (250) expr ::= expr AND expr */
  {  258,   -3 }, /* (251) expr ::= expr OR expr */
  {  258,   -3 }, /* (252) expr ::= expr PLUS expr */
  {  258,   -3 }, /* (253) expr ::= expr MINUS expr */
  {  258,   -3 }, /* (254) expr ::= expr STAR expr */
  {  258,   -3 }, /* (255) expr ::= expr SLASH expr */
  {  258,   -3 }, /* (256) expr ::= expr REM expr */
  {  258,   -3 }, /* (257) expr ::= expr LIKE expr */
  {  258,   -5 }, /* (258) expr ::= expr IN LP exprlist RP */
  {  268,   -3 }, /* (259) exprlist ::= exprlist COMMA expritem */
  {  268,   -1 }, /* (260) exprlist ::= expritem */
  {  269,   -1 }, /* (261) expritem ::= expr */
  {  269,    0 }, /* (262) expritem ::= */
  {  194,   -3 }, /* (263) cmd ::= RESET QUERY CACHE */
  {  194,   -3 }, /* (264) cmd ::= SYNCDB ids REPLICA */
  {  194,   -7 }, /* (265) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  194,   -7 }, /* (266) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  194,   -7 }, /* (267) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  194,   -7 }, /* (268) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  194,   -8 }, /* (269) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  194,   -9 }, /* (270) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  194,   -7 }, /* (271) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  194,   -7 }, /* (272) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  194,   -7 }, /* (273) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  194,   -7 }, /* (274) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  194,   -8 }, /* (275) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  194,   -3 }, /* (276) cmd ::= KILL CONNECTION INTEGER */
  {  194,   -5 }, /* (277) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  194,   -5 }, /* (278) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno,       /* Number of the rule by which to reduce */
  int yyLookahead,             /* Lookahead token, or YYNOCODE if none */
  ParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
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
      return;
    }
#else
    if( yypParser->yytos>=&yypParser->yystack[yypParser->yystksz-1] ){
      if( yyGrowStack(yypParser) ){
        yyStackOverflow(yypParser);
        return;
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
      case 0: /* program ::= cmd */
      case 136: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==136);
      case 137: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==137);
      case 138: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==138);
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW TOPICS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_TP, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW FUNCTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_FUNCTION, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 8: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 11: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 13: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 15: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 16: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 17: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 18: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 19: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 20: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 21: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 30: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 31: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 32: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 33: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 34: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 35: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 36: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 37: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 38: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 40: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 41: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 46: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 47: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==47);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy151);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 50: /* ids ::= ID */
      case 51: /* ids ::= STRING */ yytestcase(yyruleno==51);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 52: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 53: /* ifexists ::= */
      case 55: /* ifnotexists ::= */ yytestcase(yyruleno==55);
      case 177: /* distinct ::= */ yytestcase(yyruleno==177);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 54: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 56: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 57: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy151);}
        break;
      case 58: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 59: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==59);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy382, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy343, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy343, &yymsp[0].minor.yy0, 2);}
        break;
      case 62: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 63: /* bufsize ::= */
      case 65: /* pps ::= */ yytestcase(yyruleno==65);
      case 67: /* tseries ::= */ yytestcase(yyruleno==67);
      case 69: /* dbs ::= */ yytestcase(yyruleno==69);
      case 71: /* streams ::= */ yytestcase(yyruleno==71);
      case 73: /* storage ::= */ yytestcase(yyruleno==73);
      case 75: /* qtime ::= */ yytestcase(yyruleno==75);
      case 77: /* users ::= */ yytestcase(yyruleno==77);
      case 79: /* conns ::= */ yytestcase(yyruleno==79);
      case 81: /* state ::= */ yytestcase(yyruleno==81);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 64: /* bufsize ::= BUFSIZE INTEGER */
      case 66: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==70);
      case 72: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==76);
      case 78: /* users ::= USERS INTEGER */ yytestcase(yyruleno==78);
      case 80: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==80);
      case 82: /* state ::= STATE ids */ yytestcase(yyruleno==82);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 83: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy151.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy151.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy151.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy151.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy151.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy151.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy151.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy151.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy151.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy151 = yylhsminor.yy151;
        break;
      case 84: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy441 = yymsp[0].minor.yy441; }
        break;
      case 85: /* cache ::= CACHE INTEGER */
      case 86: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==86);
      case 87: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==87);
      case 88: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==89);
      case 90: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==90);
      case 91: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==91);
      case 92: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==92);
      case 93: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==93);
      case 94: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==94);
      case 95: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==95);
      case 96: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==96);
      case 97: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==97);
      case 98: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==98);
      case 99: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==99);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 100: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy382); yymsp[1].minor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 101: /* db_optr ::= db_optr cache */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 102: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 103: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 104: /* db_optr ::= db_optr days */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 105: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 106: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 107: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 108: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 109: /* db_optr ::= db_optr wal */
      case 124: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==124);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 110: /* db_optr ::= db_optr fsync */
      case 125: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==125);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 111: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 112: /* db_optr ::= db_optr prec */
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 113: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.keep = yymsp[0].minor.yy441; }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 114: /* db_optr ::= db_optr update */
      case 126: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==126);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 115: /* db_optr ::= db_optr cachelast */
      case 127: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==127);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 116: /* topic_optr ::= db_optr */
      case 128: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==128);
{ yylhsminor.yy382 = yymsp[0].minor.yy382; yylhsminor.yy382.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy382 = yylhsminor.yy382;
        break;
      case 117: /* topic_optr ::= topic_optr partitions */
      case 129: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==129);
{ yylhsminor.yy382 = yymsp[-1].minor.yy382; yylhsminor.yy382.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy382 = yylhsminor.yy382;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy382); yymsp[1].minor.yy382.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 130: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy343, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy343 = yylhsminor.yy343;
        break;
      case 131: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy369 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy343, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;  // negative value of name length
    tSetColumnType(&yylhsminor.yy343, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy343 = yylhsminor.yy343;
        break;
      case 132: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy343, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy343 = yylhsminor.yy343;
        break;
      case 133: /* signed ::= INTEGER */
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 134: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 135: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 139: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy182;}
        break;
      case 140: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy456);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy182 = pCreateTable;
}
  yymsp[0].minor.yy182 = yylhsminor.yy182;
        break;
      case 141: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy182->childTableInfo, &yymsp[0].minor.yy456);
  yylhsminor.yy182 = yymsp[-1].minor.yy182;
}
  yymsp[-1].minor.yy182 = yylhsminor.yy182;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy182 = tSetCreateTableInfo(yymsp[-1].minor.yy441, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy182 = yylhsminor.yy182;
        break;
      case 143: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy182 = tSetCreateTableInfo(yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy182 = yylhsminor.yy182;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy456 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy441, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy456 = yylhsminor.yy456;
        break;
      case 145: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy456 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy441, yymsp[-1].minor.yy441, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy456 = yylhsminor.yy456;
        break;
      case 146: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy0); yylhsminor.yy441 = yymsp[-2].minor.yy441;  }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 147: /* tagNamelist ::= ids */
{yylhsminor.yy441 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy441, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 148: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy182 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy236, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy182, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy182 = yylhsminor.yy182;
        break;
      case 149: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy441, &yymsp[0].minor.yy343); yylhsminor.yy441 = yymsp[-2].minor.yy441;  }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 150: /* columnlist ::= column */
{yylhsminor.yy441 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy441, &yymsp[0].minor.yy343);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 151: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy343, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy343);
}
  yymsp[-1].minor.yy343 = yylhsminor.yy343;
        break;
      case 152: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);    }
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 153: /* tagitemlist ::= tagitem */
{ yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1); }
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 154: /* tagitem ::= INTEGER */
      case 155: /* tagitem ::= FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= STRING */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= BOOL */ yytestcase(yyruleno==157);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 158: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy506, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy506 = yylhsminor.yy506;
        break;
      case 159: /* tagitem ::= MINUS INTEGER */
      case 160: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==162);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 163: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy236 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy441, yymsp[-10].minor.yy244, yymsp[-9].minor.yy166, yymsp[-4].minor.yy441, yymsp[-3].minor.yy441, &yymsp[-8].minor.yy340, &yymsp[-7].minor.yy259, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy441, &yymsp[0].minor.yy414, &yymsp[-1].minor.yy414, yymsp[-2].minor.yy166);
}
  yymsp[-12].minor.yy236 = yylhsminor.yy236;
        break;
      case 164: /* select ::= LP select RP */
{yymsp[-2].minor.yy236 = yymsp[-1].minor.yy236;}
        break;
      case 165: /* union ::= select */
{ yylhsminor.yy441 = setSubclause(NULL, yymsp[0].minor.yy236); }
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 166: /* union ::= union UNION ALL select */
{ yylhsminor.yy441 = appendSelectClause(yymsp[-3].minor.yy441, yymsp[0].minor.yy236); }
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 167: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy441, NULL, TSDB_SQL_SELECT); }
        break;
      case 168: /* select ::= SELECT selcollist */
{
  yylhsminor.yy236 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy441, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 169: /* sclp ::= selcollist COMMA */
{yylhsminor.yy441 = yymsp[-1].minor.yy441;}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 170: /* sclp ::= */
      case 198: /* orderby_opt ::= */ yytestcase(yyruleno==198);
{yymsp[1].minor.yy441 = 0;}
        break;
      case 171: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy441 = tSqlExprListAppend(yymsp[-3].minor.yy441, yymsp[-1].minor.yy166,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 172: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy441 = tSqlExprListAppend(yymsp[-1].minor.yy441, pNode, 0, 0);
}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 173: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 174: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 175: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 176: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* from ::= FROM tablelist */
      case 179: /* from ::= FROM sub */ yytestcase(yyruleno==179);
{yymsp[-1].minor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 180: /* sub ::= LP union RP */
{yymsp[-2].minor.yy244 = addSubqueryElem(NULL, yymsp[-1].minor.yy441, NULL);}
        break;
      case 181: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy244 = addSubqueryElem(NULL, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
        break;
      case 182: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy244 = addSubqueryElem(yymsp[-5].minor.yy244, yymsp[-2].minor.yy441, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy244 = yylhsminor.yy244;
        break;
      case 183: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 184: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 185: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(yymsp[-3].minor.yy244, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 186: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy244 = setTableNameList(yymsp[-4].minor.yy244, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy244 = yylhsminor.yy244;
        break;
      case 187: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 188: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy340.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy340.offset.n = 0;}
        break;
      case 189: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy340.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy340.offset = yymsp[-1].minor.yy0;}
        break;
      case 190: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy340, 0, sizeof(yymsp[1].minor.yy340));}
        break;
      case 191: /* session_option ::= */
{yymsp[1].minor.yy259.col.n = 0; yymsp[1].minor.yy259.gap.n = 0;}
        break;
      case 192: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy259.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy259.gap = yymsp[-1].minor.yy0;
}
        break;
      case 193: /* fill_opt ::= */
{ yymsp[1].minor.yy441 = 0;     }
        break;
      case 194: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy441, &A, -1, 0);
    yymsp[-5].minor.yy441 = yymsp[-1].minor.yy441;
}
        break;
      case 195: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy441 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 196: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 197: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 199: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 200: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy441 = tVariantListAppend(yymsp[-3].minor.yy441, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
  yymsp[-3].minor.yy441 = yylhsminor.yy441;
        break;
      case 201: /* sortlist ::= item sortorder */
{
  yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[-1].minor.yy506, yymsp[0].minor.yy112);
}
  yymsp[-1].minor.yy441 = yylhsminor.yy441;
        break;
      case 202: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy506, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy506 = yylhsminor.yy506;
        break;
      case 203: /* sortorder ::= ASC */
{ yymsp[0].minor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 204: /* sortorder ::= DESC */
{ yymsp[0].minor.yy112 = TSDB_ORDER_DESC;}
        break;
      case 205: /* sortorder ::= */
{ yymsp[1].minor.yy112 = TSDB_ORDER_ASC; }
        break;
      case 206: /* groupby_opt ::= */
{ yymsp[1].minor.yy441 = 0;}
        break;
      case 207: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy441 = yymsp[0].minor.yy441;}
        break;
      case 208: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy441 = tVariantListAppend(yymsp[-2].minor.yy441, &yymsp[0].minor.yy506, -1);
}
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 209: /* grouplist ::= item */
{
  yylhsminor.yy441 = tVariantListAppend(NULL, &yymsp[0].minor.yy506, -1);
}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 210: /* having_opt ::= */
      case 220: /* where_opt ::= */ yytestcase(yyruleno==220);
      case 262: /* expritem ::= */ yytestcase(yyruleno==262);
{yymsp[1].minor.yy166 = 0;}
        break;
      case 211: /* having_opt ::= HAVING expr */
      case 221: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==221);
{yymsp[-1].minor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 212: /* limit_opt ::= */
      case 216: /* slimit_opt ::= */ yytestcase(yyruleno==216);
{yymsp[1].minor.yy414.limit = -1; yymsp[1].minor.yy414.offset = 0;}
        break;
      case 213: /* limit_opt ::= LIMIT signed */
      case 217: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==217);
{yymsp[-1].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy414.offset = 0;}
        break;
      case 214: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy414.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 215: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 218: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy414.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[0].minor.yy369;}
        break;
      case 219: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy414.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy414.offset = yymsp[-2].minor.yy369;}
        break;
      case 222: /* expr ::= LP expr RP */
{yylhsminor.yy166 = yymsp[-1].minor.yy166; yylhsminor.yy166->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy166->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 223: /* expr ::= ID */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 224: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 225: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 226: /* expr ::= INTEGER */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 227: /* expr ::= MINUS INTEGER */
      case 228: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==228);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 229: /* expr ::= FLOAT */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 230: /* expr ::= MINUS FLOAT */
      case 231: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==231);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 232: /* expr ::= STRING */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 233: /* expr ::= NOW */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 234: /* expr ::= VARIABLE */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 235: /* expr ::= PLUS VARIABLE */
      case 236: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==236);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 237: /* expr ::= BOOL */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 238: /* expr ::= NULL */
{ yylhsminor.yy166 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 239: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy166 = tSqlExprCreateFunction(yymsp[-1].minor.yy441, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 240: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy166 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 241: /* expr ::= expr IS NULL */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 242: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-3].minor.yy166, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 243: /* expr ::= expr LT expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LT);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 244: /* expr ::= expr GT expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GT);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 245: /* expr ::= expr LE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 246: /* expr ::= expr GE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_GE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 247: /* expr ::= expr NE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_NE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 248: /* expr ::= expr EQ expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_EQ);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 249: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy166); yylhsminor.yy166 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy166, yymsp[-2].minor.yy166, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy166, TK_LE), TK_AND);}
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 250: /* expr ::= expr AND expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_AND);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 251: /* expr ::= expr OR expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_OR); }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 252: /* expr ::= expr PLUS expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_PLUS);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 253: /* expr ::= expr MINUS expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_MINUS); }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 254: /* expr ::= expr STAR expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_STAR);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 255: /* expr ::= expr SLASH expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_DIVIDE);}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 256: /* expr ::= expr REM expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_REM);   }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 257: /* expr ::= expr LIKE expr */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-2].minor.yy166, yymsp[0].minor.yy166, TK_LIKE);  }
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 258: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy166 = tSqlExprCreate(yymsp[-4].minor.yy166, (tSqlExpr*)yymsp[-1].minor.yy441, TK_IN); }
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 259: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy441 = tSqlExprListAppend(yymsp[-2].minor.yy441,yymsp[0].minor.yy166,0, 0);}
  yymsp[-2].minor.yy441 = yylhsminor.yy441;
        break;
      case 260: /* exprlist ::= expritem */
{yylhsminor.yy441 = tSqlExprListAppend(0,yymsp[0].minor.yy166,0, 0);}
  yymsp[0].minor.yy441 = yylhsminor.yy441;
        break;
      case 261: /* expritem ::= expr */
{yylhsminor.yy166 = yymsp[0].minor.yy166;}
  yymsp[0].minor.yy166 = yylhsminor.yy166;
        break;
      case 263: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 264: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy506, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy441, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 277: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 278: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
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
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
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
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
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
  ParseARG_FETCH;
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/

  pInfo->valid = false;
  int32_t outputBufLen = tListLen(pInfo->msg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->msg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->msg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->msg, "Incomplete SQL statement");
  }

  assert(len <= outputBufLen);
/************ End %syntax_error code ******************************************/
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
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
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
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
  unsigned int yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  yypParser = (yyParser*)yyp;
  assert( yypParser->yytos!=0 );
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    int stateno = yypParser->yytos->stateno;
    if( stateno < YY_MIN_REDUCE ){
      fprintf(yyTraceFILE,"%sInput '%s' in state %d\n",
              yyTracePrompt,yyTokenName[yymajor],stateno);
    }else{
      fprintf(yyTraceFILE,"%sInput '%s' with pending reduce %d\n",
              yyTracePrompt,yyTokenName[yymajor],stateno-YY_MIN_REDUCE);
    }
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact >= YY_MIN_REDUCE ){
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,yyminor);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      yymajor = YYNOCODE;
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
      yymajor = YYNOCODE;
      
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
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yytos>yypParser->yystack );
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
