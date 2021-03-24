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
#include <assert.h>
/************ Begin %include sections from the grammar ************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "qSqlparser.h"
#include "tcmdtype.h"
#include "tstoken.h"
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
#define YYNOCODE 265
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SLimitVal yy74;
  SQuerySqlNode* yy110;
  SCreateDbInfo yy122;
  SSessionWindowVal yy139;
  TAOS_FIELD yy153;
  SSubclauseInfo* yy154;
  int64_t yy179;
  SCreateAcctInfo yy211;
  tVariant yy216;
  SFromInfo* yy232;
  SArray* yy291;
  int yy382;
  SIntervalVal yy400;
  SCreateTableSql* yy412;
  tSqlExpr* yy436;
  SCreatedTableInfo yy446;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_PARAM ,pInfo
#define ParseARG_FETCH SSqlInfo* pInfo=yypParser->pInfo;
#define ParseARG_STORE yypParser->pInfo=pInfo;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYFALLBACK 1
#define YYNSTATE             325
#define YYNRULE              269
#define YYNRULE_WITH_ACTION  269
#define YYNTOKEN             190
#define YY_MAX_SHIFT         324
#define YY_MIN_SHIFTREDUCE   518
#define YY_MAX_SHIFTREDUCE   786
#define YY_ERROR_ACTION      787
#define YY_ACCEPT_ACTION     788
#define YY_NO_ACTION         789
#define YY_MIN_REDUCE        790
#define YY_MAX_REDUCE        1058
/************* End control #defines *******************************************/
#define YY_NLOOKAHEAD ((int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])))

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
#define YY_ACTTAB_COUNT (693)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   961,  567,  187,  567,  208,  322,  926,   17,   70,  568,
 /*    10 */    83,  568, 1040,   49,   50,  146,   53,   54,  139,  185,
 /*    20 */   220,   43,  187,   52,  267,   57,   55,   59,   56,  191,
 /*    30 */    32,  215, 1041,   48,   47, 1037,  187,   46,   45,   44,
 /*    40 */   925,  923,  924,   29,  927,  214, 1041,  519,  520,  521,
 /*    50 */   522,  523,  524,  525,  526,  527,  528,  529,  530,  531,
 /*    60 */   532,  323,  958,  212,  237,   49,   50,   32,   53,   54,
 /*    70 */   146,  209,  220,   43,  937,   52,  267,   57,   55,   59,
 /*    80 */    56,  252,  993,   71,  262,   48,   47,  288,  940,   46,
 /*    90 */    45,   44,   49,   50,  288,   53,   54,   32,   82,  220,
 /*   100 */    43,  567,   52,  267,   57,   55,   59,   56,  223,  568,
 /*   110 */    32,  937,   48,   47,   75,  308,   46,   45,   44,   49,
 /*   120 */    51,   38,   53,   54,   12,  225,  220,   43,   85,   52,
 /*   130 */   267,   57,   55,   59,   56,  264,  227,   79,  224,   48,
 /*   140 */    47,  937,  318,   46,   45,   44,   50, 1036,   53,   54,
 /*   150 */   940,  291,  220,   43,  937,   52,  267,   57,   55,   59,
 /*   160 */    56,  940,  734,  837,  146,   48,   47,  940,  172,   46,
 /*   170 */    45,   44,   86,   23,  286,  317,  316,  285,  284,  283,
 /*   180 */   315,  282,  314,  313,  312,  281,  311,  310,  900,   32,
 /*   190 */   888,  889,  890,  891,  892,  893,  894,  895,  896,  897,
 /*   200 */   898,  899,  901,  902,   53,   54,  928,  934,  220,   43,
 /*   210 */   146,   52,  267,   57,   55,   59,   56, 1035,   32,   18,
 /*   220 */   952,   48,   47,    1,  160,   46,   45,   44,  219,  747,
 /*   230 */   292,  992,  738,  937,  741,  210,  744,  195,  219,  747,
 /*   240 */   846,  204,  738,  197,  741,  172,  744,    5,  162,  226,
 /*   250 */   123,  122,  196,   35,  161,   92,   97,   88,   96,  296,
 /*   260 */   216,  217,  937,  229,  266,   23,  205,  317,  316,  278,
 /*   270 */   216,  217,  315,  567,  314,  313,  312,   80,  311,  310,
 /*   280 */   240,  568,  908, 1050,   75,  906,  907,  244,  243,   81,
 /*   290 */   909,   38,  911,  912,  910,  232,  913,  914,   57,   55,
 /*   300 */    59,   56,   72,  231,  850,  687,   48,   47,  108,  113,
 /*   310 */    46,   45,   44,  246,  102,  112,   32,  118,  121,  111,
 /*   320 */   203,  180,  176,  788,  324,  115,  268,  178,  175,  127,
 /*   330 */   126,  125,  124,  648,  672,  230,   58,  669,  290,  670,
 /*   340 */   189,  671,  321,  320,  132,   32,   58,  232,  746,   48,
 /*   350 */    47,    3,  173,   46,   45,   44,  849,  300,  746,  232,
 /*   360 */   937,  298,  297,  218,  745,  234,  235,  684,  938,  952,
 /*   370 */    46,   45,   44,   24,  745,  233,  248,  110,  295,  294,
 /*   380 */    33,   25,  736,  838,  247,  308,  694,  691,  172,  936,
 /*   390 */   715,  716,  700,  250,   63,  706,  141,  707,  767,   62,
 /*   400 */   748,   20,   19,  740,   19,  743,  739,  190,  742,   66,
 /*   410 */   658,    6,  192,  270,   33,   64,  660,   33,  737,  272,
 /*   420 */    62,   28,  186,   84,  273,  659,  750,  193,   67,   62,
 /*   430 */   101,  100,   14,   13,  107,  106,   69,  194,  647,   16,
 /*   440 */    15,  676,  674,  677,  675,  120,  119,  673,  137,  135,
 /*   450 */   200,  201,  199,  184,  198,  188,  939, 1003, 1002,  221,
 /*   460 */   999,  998,  222,  299,  138,   41,  985,  960,  968,  984,
 /*   470 */   136,  970,  953,  249,  251,   73,  140,  144,  935,  156,
 /*   480 */    76,  253,  211,  255,  904,  157,  699,  933,  148,  158,
 /*   490 */   159,  950,  147,  149,  851,  265,  150,  275,  276,   60,
 /*   500 */   277,  279,   68,  280,  260,   65,   39,  182,  263,  261,
 /*   510 */    36,  289,  845, 1055,   98, 1054, 1052,  163,  293, 1049,
 /*   520 */   259,  254,  104, 1048, 1046,  257,   42,  164,  869,   37,
 /*   530 */    34,   40,  183,  834,  114,  832,  116,  117,  830,  829,
 /*   540 */   236,  174,  827,  826,  825,  824,  823,  822,  821,  177,
 /*   550 */   179,  818,  816,  814,  812,  810,  181,  309,  256,  109,
 /*   560 */   986,  301,  302,  303,  304,  305,  306,  307,  319,  206,
 /*   570 */   228,  786,  238,  239,  274,  785,  241,  242,  784,  772,
 /*   580 */   207,   93,   94,  202,  245,  250,   77,    8,  269,   74,
 /*   590 */   213,  679,  701,  142,  751,  271,  704,  166,  870,  167,
 /*   600 */   168,  165,  170,  169,  171,  828,    2,  128,  129,  820,
 /*   610 */     4,  130,  819,  143,  131,  811,  153,  151,  152,  154,
 /*   620 */   155,  916,   78,  258,    9,  708,  145,   10,   87,  749,
 /*   630 */    26,    7,   27,   11,   21,   22,   85,   30,   89,   91,
 /*   640 */    90,   31,  611,  607,  605,  604,  603,  600,   99,  571,
 /*   650 */   287,   33,   95,   61,  103,  650,  649,  646,  595,  593,
 /*   660 */   585,  591,  105,  587,  589,  583,  581,  614,  613,  612,
 /*   670 */   610,  609,  608,  606,  602,  601,   62,  569,  133,  536,
 /*   680 */   534,  790,  789,  789,  789,  789,  789,  789,  789,  789,
 /*   690 */   789,  789,  134,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   193,    1,  254,    1,  192,  193,    0,  254,  199,    9,
 /*    10 */   199,    9,  264,   13,   14,  193,   16,   17,  193,  254,
 /*    20 */    20,   21,  254,   23,   24,   25,   26,   27,   28,  254,
 /*    30 */   193,  263,  264,   33,   34,  254,  254,   37,   38,   39,
 /*    40 */   231,  230,  231,  232,  233,  263,  264,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  255,  213,   62,   13,   14,  193,   16,   17,
 /*    70 */   193,  234,   20,   21,  237,   23,   24,   25,   26,   27,
 /*    80 */    28,  256,  260,   83,  262,   33,   34,   81,  238,   37,
 /*    90 */    38,   39,   13,   14,   81,   16,   17,  193,   83,   20,
 /*   100 */    21,    1,   23,   24,   25,   26,   27,   28,  234,    9,
 /*   110 */   193,  237,   33,   34,  109,   86,   37,   38,   39,   13,
 /*   120 */    14,  116,   16,   17,  109,  213,   20,   21,  113,   23,
 /*   130 */    24,   25,   26,   27,   28,  258,  213,  260,  234,   33,
 /*   140 */    34,  237,  213,   37,   38,   39,   14,  254,   16,   17,
 /*   150 */   238,  234,   20,   21,  237,   23,   24,   25,   26,   27,
 /*   160 */    28,  238,  110,  198,  193,   33,   34,  238,  203,   37,
 /*   170 */    38,   39,  199,   93,   94,   95,   96,   97,   98,   99,
 /*   180 */   100,  101,  102,  103,  104,  105,  106,  107,  212,  193,
 /*   190 */   214,  215,  216,  217,  218,  219,  220,  221,  222,  223,
 /*   200 */   224,  225,  226,  227,   16,   17,  233,  193,   20,   21,
 /*   210 */   193,   23,   24,   25,   26,   27,   28,  254,  193,   44,
 /*   220 */   236,   33,   34,  200,  201,   37,   38,   39,    1,    2,
 /*   230 */   234,  260,    5,  237,    7,  251,    9,   62,    1,    2,
 /*   240 */   198,  254,    5,   68,    7,  203,    9,   63,   64,  235,
 /*   250 */    75,   76,   77,   69,   70,   71,   72,   73,   74,  234,
 /*   260 */    33,   34,  237,   68,   37,   93,  254,   95,   96,   85,
 /*   270 */    33,   34,  100,    1,  102,  103,  104,  260,  106,  107,
 /*   280 */   139,    9,  212,  238,  109,  215,  216,  146,  147,  239,
 /*   290 */   220,  116,  222,  223,  224,  193,  226,  227,   25,   26,
 /*   300 */    27,   28,  252,   68,  202,   37,   33,   34,   63,   64,
 /*   310 */    37,   38,   39,  138,   69,   70,  193,   72,   73,   74,
 /*   320 */   145,   63,   64,  190,  191,   80,   15,   69,   70,   71,
 /*   330 */    72,   73,   74,    5,    2,  140,  109,    5,  143,    7,
 /*   340 */   254,    9,   65,   66,   67,  193,  109,  193,  121,   33,
 /*   350 */    34,  196,  197,   37,   38,   39,  202,  234,  121,  193,
 /*   360 */   237,   33,   34,   61,  137,   33,   34,  114,  202,  236,
 /*   370 */    37,   38,   39,  120,  137,  140,  110,   78,  143,  144,
 /*   380 */   114,  109,    1,  198,  251,   86,  110,  119,  203,  237,
 /*   390 */   128,  129,  110,  117,  114,  110,  114,  110,  110,  114,
 /*   400 */   110,  114,  114,    5,  114,    7,    5,  254,    7,  114,
 /*   410 */   110,  109,  254,  110,  114,  135,  110,  114,   37,  110,
 /*   420 */   114,  109,  254,  114,  112,  110,  115,  254,  133,  114,
 /*   430 */   141,  142,  141,  142,  141,  142,  109,  254,  111,  141,
 /*   440 */   142,    5,    5,    7,    7,   78,   79,  115,   63,   64,
 /*   450 */   254,  254,  254,  254,  254,  254,  238,  229,  229,  229,
 /*   460 */   229,  229,  229,  229,  193,  253,  261,  193,  193,  261,
 /*   470 */    61,  193,  236,  194,  236,  194,  193,  193,  236,  240,
 /*   480 */   194,  257,  257,  257,  228,  193,  121,  193,  248,  193,
 /*   490 */   193,  250,  249,  247,  193,  126,  246,  193,  193,  131,
 /*   500 */   193,  193,  132,  193,  257,  134,  193,  193,  130,  125,
 /*   510 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   520 */   124,  122,  193,  193,  193,  123,  136,  193,  193,  193,
 /*   530 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   540 */   193,  193,  193,  193,  193,  193,  193,  193,  193,  193,
 /*   550 */   193,  193,  193,  193,  193,  193,  193,  108,  194,   92,
 /*   560 */   194,   91,   51,   88,   90,   55,   89,   87,   81,  194,
 /*   570 */   194,    5,  148,    5,  194,    5,  148,    5,    5,   94,
 /*   580 */   194,  199,  199,  194,  139,  117,  114,  109,  112,  118,
 /*   590 */     1,  110,  110,  109,  115,  112,  110,  209,  211,  205,
 /*   600 */   208,  210,  207,  206,  204,  194,  200,  195,  195,  194,
 /*   610 */   196,  195,  194,  114,  195,  194,  243,  245,  244,  242,
 /*   620 */   241,  228,  109,  109,  127,  110,  109,  127,   78,  110,
 /*   630 */   114,  109,  114,  109,  109,  109,  113,   84,   83,   83,
 /*   640 */    71,   84,    9,    5,    5,    5,    5,    5,  142,   82,
 /*   650 */    15,  114,   78,   16,  142,    5,    5,  110,    5,    5,
 /*   660 */     5,    5,  142,    5,    5,    5,    5,    5,    5,    5,
 /*   670 */     5,    5,    5,    5,    5,    5,  114,   82,   21,   61,
 /*   680 */    60,    0,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   690 */   265,  265,   21,  265,  265,  265,  265,  265,  265,  265,
 /*   700 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   710 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   720 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   730 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   740 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   750 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   760 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   770 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   780 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   790 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   800 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   810 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   820 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   830 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   840 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   850 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   860 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   870 */   265,  265,  265,  265,  265,  265,  265,  265,  265,  265,
 /*   880 */   265,  265,  265,
};
#define YY_SHIFT_COUNT    (324)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (681)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   175,   80,   80,  172,  172,   13,  227,  237,  100,  100,
 /*    10 */   100,  100,  100,  100,  100,  100,  100,    0,    2,  237,
 /*    20 */   332,  332,  332,  332,  272,    5,  100,  100,  100,    6,
 /*    30 */   100,  100,  100,  100,  299,   13,   29,   29,  693,  693,
 /*    40 */   693,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*    50 */   237,  237,  237,  237,  237,  237,  237,  237,  237,  237,
 /*    60 */   237,  332,  332,  328,  328,  328,  328,  328,  328,  328,
 /*    70 */   100,  100,  268,  100,    5,    5,  100,  100,  100,  262,
 /*    80 */   262,  253,    5,  100,  100,  100,  100,  100,  100,  100,
 /*    90 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   100 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   110 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   120 */   100,  100,  100,  100,  100,  100,  100,  100,  100,  100,
 /*   130 */   100,  100,  100,  100,  100,  100,  100,  100,  409,  409,
 /*   140 */   409,  365,  365,  365,  409,  365,  409,  370,  371,  368,
 /*   150 */   369,  378,  384,  396,  402,  399,  390,  409,  409,  409,
 /*   160 */   449,   13,   13,  409,  409,  467,  470,  511,  475,  474,
 /*   170 */   510,  477,  480,  449,  409,  487,  487,  409,  487,  409,
 /*   180 */   487,  409,  693,  693,   52,   79,  106,   79,   79,  132,
 /*   190 */   188,  273,  273,  273,  273,  184,  245,  258,  316,  316,
 /*   200 */   316,  316,  235,  141,  333,  333,   15,  195,  277,  266,
 /*   210 */   276,  282,  285,  287,  288,  290,  398,  401,  381,  302,
 /*   220 */   311,  280,  295,  300,  303,  306,  309,  315,  312,  289,
 /*   230 */   291,  293,  327,  298,  436,  437,  367,  385,  566,  424,
 /*   240 */   568,  570,  428,  572,  573,  485,  445,  468,  476,  478,
 /*   250 */   471,  481,  472,  482,  484,  486,  499,  513,  589,  514,
 /*   260 */   515,  517,  516,  497,  518,  500,  519,  522,  479,  524,
 /*   270 */   476,  525,  483,  526,  523,  550,  553,  555,  569,  557,
 /*   280 */   556,  633,  638,  639,  640,  641,  642,  567,  635,  574,
 /*   290 */   506,  537,  537,  637,  512,  520,  537,  650,  651,  547,
 /*   300 */   537,  653,  654,  655,  656,  658,  659,  660,  661,  662,
 /*   310 */   663,  664,  665,  666,  667,  668,  669,  670,  562,  595,
 /*   320 */   657,  671,  618,  620,  681,
};
#define YY_REDUCE_COUNT (183)
#define YY_REDUCE_MIN   (-252)
#define YY_REDUCE_MAX   (421)
static const short yy_reduce_ofst[] = {
 /*     0 */   133,  -24,  -24,   70,   70, -189, -232, -218, -163, -178,
 /*    10 */  -123, -126,  -96,  -83,   -4,   25,  123, -193, -188, -252,
 /*    20 */  -150,  -88,  -77,  -71, -175,  -16,  -29,   17,   14,  -27,
 /*    30 */   102,  154,  166,  152,  -35, -191,   42,  185,   50,   23,
 /*    40 */   155, -247, -235, -225, -219, -107,  -37,  -13,   12,   86,
 /*    50 */   153,  158,  168,  173,  183,  196,  197,  198,  199,  200,
 /*    60 */   201,   45,  218,  228,  229,  230,  231,  232,  233,  234,
 /*    70 */   271,  274,  212,  275,  236,  238,  278,  283,  284,  205,
 /*    80 */   208,  239,  242,  292,  294,  296,  297,  301,  304,  305,
 /*    90 */   307,  308,  310,  313,  314,  317,  318,  319,  320,  321,
 /*   100 */   322,  323,  324,  325,  326,  329,  330,  331,  334,  335,
 /*   110 */   336,  337,  338,  339,  340,  341,  342,  343,  344,  345,
 /*   120 */   346,  347,  348,  349,  350,  351,  352,  353,  354,  355,
 /*   130 */   356,  357,  358,  359,  360,  361,  362,  363,  279,  281,
 /*   140 */   286,  224,  225,  226,  364,  247,  366,  241,  243,  240,
 /*   150 */   246,  250,  372,  374,  373,  377,  379,  375,  376,  380,
 /*   160 */   256,  382,  383,  386,  389,  387,  391,  388,  394,  392,
 /*   170 */   397,  395,  400,  393,  411,  412,  413,  415,  416,  418,
 /*   180 */   419,  421,  406,  414,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   787,  903,  847,  915,  835,  844, 1043, 1043,  787,  787,
 /*    10 */   787,  787,  787,  787,  787,  787,  787,  962,  807, 1043,
 /*    20 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  844,
 /*    30 */   787,  787,  787,  787,  852,  844,  852,  852,  957,  887,
 /*    40 */   905,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*    50 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*    60 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*    70 */   787,  787,  964,  967,  787,  787,  969,  787,  787,  989,
 /*    80 */   989,  955,  787,  787,  787,  787,  787,  787,  787,  787,
 /*    90 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   100 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   110 */   787,  787,  787,  787,  833,  787,  831,  787,  787,  787,
 /*   120 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   130 */   787,  787,  817,  787,  787,  787,  787,  787,  809,  809,
 /*   140 */   809,  787,  787,  787,  809,  787,  809,  996, 1000,  994,
 /*   150 */   982,  990,  981,  977,  975,  974, 1004,  809,  809,  809,
 /*   160 */   848,  844,  844,  809,  809,  868,  866,  864,  856,  862,
 /*   170 */   858,  860,  854,  836,  809,  842,  842,  809,  842,  809,
 /*   180 */   842,  809,  887,  905,  787, 1005,  787, 1042,  995, 1032,
 /*   190 */  1031, 1038, 1030, 1029, 1028,  787,  787,  787, 1024, 1025,
 /*   200 */  1027, 1026,  787,  787, 1034, 1033,  787,  787,  787,  787,
 /*   210 */   787,  787,  787,  787,  787,  787,  787,  787,  787, 1007,
 /*   220 */   787, 1001,  997,  787,  787,  787,  787,  787,  787,  787,
 /*   230 */   787,  787,  917,  787,  787,  787,  787,  787,  787,  787,
 /*   240 */   787,  787,  787,  787,  787,  787,  787,  954,  787,  787,
 /*   250 */   787,  787,  965,  787,  787,  787,  787,  787,  787,  787,
 /*   260 */   787,  787,  991,  787,  983,  787,  787,  787,  787,  787,
 /*   270 */   929,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   280 */   787,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   290 */   787, 1053, 1051,  787,  787,  787, 1047,  787,  787,  787,
 /*   300 */  1045,  787,  787,  787,  787,  787,  787,  787,  787,  787,
 /*   310 */   787,  787,  787,  787,  787,  787,  787,  787,  871,  787,
 /*   320 */   815,  813,  787,  805,  787,
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
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
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
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "STABLE",
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
  /*   86 */ "PPS",
  /*   87 */ "TSERIES",
  /*   88 */ "DBS",
  /*   89 */ "STORAGE",
  /*   90 */ "QTIME",
  /*   91 */ "CONNS",
  /*   92 */ "STATE",
  /*   93 */ "KEEP",
  /*   94 */ "CACHE",
  /*   95 */ "REPLICA",
  /*   96 */ "QUORUM",
  /*   97 */ "DAYS",
  /*   98 */ "MINROWS",
  /*   99 */ "MAXROWS",
  /*  100 */ "BLOCKS",
  /*  101 */ "CTIME",
  /*  102 */ "WAL",
  /*  103 */ "FSYNC",
  /*  104 */ "COMP",
  /*  105 */ "PRECISION",
  /*  106 */ "UPDATE",
  /*  107 */ "CACHELAST",
  /*  108 */ "PARTITIONS",
  /*  109 */ "LP",
  /*  110 */ "RP",
  /*  111 */ "UNSIGNED",
  /*  112 */ "TAGS",
  /*  113 */ "USING",
  /*  114 */ "COMMA",
  /*  115 */ "NULL",
  /*  116 */ "SELECT",
  /*  117 */ "UNION",
  /*  118 */ "ALL",
  /*  119 */ "DISTINCT",
  /*  120 */ "FROM",
  /*  121 */ "VARIABLE",
  /*  122 */ "INTERVAL",
  /*  123 */ "SESSION",
  /*  124 */ "FILL",
  /*  125 */ "SLIDING",
  /*  126 */ "ORDER",
  /*  127 */ "BY",
  /*  128 */ "ASC",
  /*  129 */ "DESC",
  /*  130 */ "GROUP",
  /*  131 */ "HAVING",
  /*  132 */ "LIMIT",
  /*  133 */ "OFFSET",
  /*  134 */ "SLIMIT",
  /*  135 */ "SOFFSET",
  /*  136 */ "WHERE",
  /*  137 */ "NOW",
  /*  138 */ "RESET",
  /*  139 */ "QUERY",
  /*  140 */ "ADD",
  /*  141 */ "COLUMN",
  /*  142 */ "TAG",
  /*  143 */ "CHANGE",
  /*  144 */ "SET",
  /*  145 */ "KILL",
  /*  146 */ "CONNECTION",
  /*  147 */ "STREAM",
  /*  148 */ "COLON",
  /*  149 */ "ABORT",
  /*  150 */ "AFTER",
  /*  151 */ "ATTACH",
  /*  152 */ "BEFORE",
  /*  153 */ "BEGIN",
  /*  154 */ "CASCADE",
  /*  155 */ "CLUSTER",
  /*  156 */ "CONFLICT",
  /*  157 */ "COPY",
  /*  158 */ "DEFERRED",
  /*  159 */ "DELIMITERS",
  /*  160 */ "DETACH",
  /*  161 */ "EACH",
  /*  162 */ "END",
  /*  163 */ "EXPLAIN",
  /*  164 */ "FAIL",
  /*  165 */ "FOR",
  /*  166 */ "IGNORE",
  /*  167 */ "IMMEDIATE",
  /*  168 */ "INITIALLY",
  /*  169 */ "INSTEAD",
  /*  170 */ "MATCH",
  /*  171 */ "KEY",
  /*  172 */ "OF",
  /*  173 */ "RAISE",
  /*  174 */ "REPLACE",
  /*  175 */ "RESTRICT",
  /*  176 */ "ROW",
  /*  177 */ "STATEMENT",
  /*  178 */ "TRIGGER",
  /*  179 */ "VIEW",
  /*  180 */ "SEMI",
  /*  181 */ "NONE",
  /*  182 */ "PREV",
  /*  183 */ "LINEAR",
  /*  184 */ "IMPORT",
  /*  185 */ "TBNAME",
  /*  186 */ "JOIN",
  /*  187 */ "INSERT",
  /*  188 */ "INTO",
  /*  189 */ "VALUES",
  /*  190 */ "program",
  /*  191 */ "cmd",
  /*  192 */ "dbPrefix",
  /*  193 */ "ids",
  /*  194 */ "cpxName",
  /*  195 */ "ifexists",
  /*  196 */ "alter_db_optr",
  /*  197 */ "alter_topic_optr",
  /*  198 */ "acct_optr",
  /*  199 */ "ifnotexists",
  /*  200 */ "db_optr",
  /*  201 */ "topic_optr",
  /*  202 */ "typename",
  /*  203 */ "pps",
  /*  204 */ "tseries",
  /*  205 */ "dbs",
  /*  206 */ "streams",
  /*  207 */ "storage",
  /*  208 */ "qtime",
  /*  209 */ "users",
  /*  210 */ "conns",
  /*  211 */ "state",
  /*  212 */ "keep",
  /*  213 */ "tagitemlist",
  /*  214 */ "cache",
  /*  215 */ "replica",
  /*  216 */ "quorum",
  /*  217 */ "days",
  /*  218 */ "minrows",
  /*  219 */ "maxrows",
  /*  220 */ "blocks",
  /*  221 */ "ctime",
  /*  222 */ "wal",
  /*  223 */ "fsync",
  /*  224 */ "comp",
  /*  225 */ "prec",
  /*  226 */ "update",
  /*  227 */ "cachelast",
  /*  228 */ "partitions",
  /*  229 */ "signed",
  /*  230 */ "create_table_args",
  /*  231 */ "create_stable_args",
  /*  232 */ "create_table_list",
  /*  233 */ "create_from_stable",
  /*  234 */ "columnlist",
  /*  235 */ "tagNamelist",
  /*  236 */ "select",
  /*  237 */ "column",
  /*  238 */ "tagitem",
  /*  239 */ "selcollist",
  /*  240 */ "from",
  /*  241 */ "where_opt",
  /*  242 */ "interval_opt",
  /*  243 */ "session_option",
  /*  244 */ "fill_opt",
  /*  245 */ "sliding_opt",
  /*  246 */ "groupby_opt",
  /*  247 */ "orderby_opt",
  /*  248 */ "having_opt",
  /*  249 */ "slimit_opt",
  /*  250 */ "limit_opt",
  /*  251 */ "union",
  /*  252 */ "sclp",
  /*  253 */ "distinct",
  /*  254 */ "expr",
  /*  255 */ "as",
  /*  256 */ "tablelist",
  /*  257 */ "tmvar",
  /*  258 */ "sortlist",
  /*  259 */ "sortitem",
  /*  260 */ "item",
  /*  261 */ "sortorder",
  /*  262 */ "grouplist",
  /*  263 */ "exprlist",
  /*  264 */ "expritem",
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
 /*  22 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  26 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  28 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  29 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  31 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  32 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  33 */ "cmd ::= DROP FUNCTION ids",
 /*  34 */ "cmd ::= DROP DNODE ids",
 /*  35 */ "cmd ::= DROP USER ids",
 /*  36 */ "cmd ::= DROP ACCOUNT ids",
 /*  37 */ "cmd ::= USE ids",
 /*  38 */ "cmd ::= DESCRIBE ids cpxName",
 /*  39 */ "cmd ::= ALTER USER ids PASS ids",
 /*  40 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids",
 /*  42 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids",
 /*  44 */ "cmd ::= ALTER LOCAL ids ids",
 /*  45 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  46 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  48 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  49 */ "ids ::= ID",
 /*  50 */ "ids ::= STRING",
 /*  51 */ "ifexists ::= IF EXISTS",
 /*  52 */ "ifexists ::=",
 /*  53 */ "ifnotexists ::= IF NOT EXISTS",
 /*  54 */ "ifnotexists ::=",
 /*  55 */ "cmd ::= CREATE DNODE ids",
 /*  56 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  57 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  58 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  59 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename",
 /*  60 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename",
 /*  61 */ "cmd ::= CREATE USER ids PASS ids",
 /*  62 */ "pps ::=",
 /*  63 */ "pps ::= PPS INTEGER",
 /*  64 */ "tseries ::=",
 /*  65 */ "tseries ::= TSERIES INTEGER",
 /*  66 */ "dbs ::=",
 /*  67 */ "dbs ::= DBS INTEGER",
 /*  68 */ "streams ::=",
 /*  69 */ "streams ::= STREAMS INTEGER",
 /*  70 */ "storage ::=",
 /*  71 */ "storage ::= STORAGE INTEGER",
 /*  72 */ "qtime ::=",
 /*  73 */ "qtime ::= QTIME INTEGER",
 /*  74 */ "users ::=",
 /*  75 */ "users ::= USERS INTEGER",
 /*  76 */ "conns ::=",
 /*  77 */ "conns ::= CONNS INTEGER",
 /*  78 */ "state ::=",
 /*  79 */ "state ::= STATE ids",
 /*  80 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  81 */ "keep ::= KEEP tagitemlist",
 /*  82 */ "cache ::= CACHE INTEGER",
 /*  83 */ "replica ::= REPLICA INTEGER",
 /*  84 */ "quorum ::= QUORUM INTEGER",
 /*  85 */ "days ::= DAYS INTEGER",
 /*  86 */ "minrows ::= MINROWS INTEGER",
 /*  87 */ "maxrows ::= MAXROWS INTEGER",
 /*  88 */ "blocks ::= BLOCKS INTEGER",
 /*  89 */ "ctime ::= CTIME INTEGER",
 /*  90 */ "wal ::= WAL INTEGER",
 /*  91 */ "fsync ::= FSYNC INTEGER",
 /*  92 */ "comp ::= COMP INTEGER",
 /*  93 */ "prec ::= PRECISION STRING",
 /*  94 */ "update ::= UPDATE INTEGER",
 /*  95 */ "cachelast ::= CACHELAST INTEGER",
 /*  96 */ "partitions ::= PARTITIONS INTEGER",
 /*  97 */ "db_optr ::=",
 /*  98 */ "db_optr ::= db_optr cache",
 /*  99 */ "db_optr ::= db_optr replica",
 /* 100 */ "db_optr ::= db_optr quorum",
 /* 101 */ "db_optr ::= db_optr days",
 /* 102 */ "db_optr ::= db_optr minrows",
 /* 103 */ "db_optr ::= db_optr maxrows",
 /* 104 */ "db_optr ::= db_optr blocks",
 /* 105 */ "db_optr ::= db_optr ctime",
 /* 106 */ "db_optr ::= db_optr wal",
 /* 107 */ "db_optr ::= db_optr fsync",
 /* 108 */ "db_optr ::= db_optr comp",
 /* 109 */ "db_optr ::= db_optr prec",
 /* 110 */ "db_optr ::= db_optr keep",
 /* 111 */ "db_optr ::= db_optr update",
 /* 112 */ "db_optr ::= db_optr cachelast",
 /* 113 */ "topic_optr ::= db_optr",
 /* 114 */ "topic_optr ::= topic_optr partitions",
 /* 115 */ "alter_db_optr ::=",
 /* 116 */ "alter_db_optr ::= alter_db_optr replica",
 /* 117 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 118 */ "alter_db_optr ::= alter_db_optr keep",
 /* 119 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 120 */ "alter_db_optr ::= alter_db_optr comp",
 /* 121 */ "alter_db_optr ::= alter_db_optr wal",
 /* 122 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 123 */ "alter_db_optr ::= alter_db_optr update",
 /* 124 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 125 */ "alter_topic_optr ::= alter_db_optr",
 /* 126 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 127 */ "typename ::= ids",
 /* 128 */ "typename ::= ids LP signed RP",
 /* 129 */ "typename ::= ids UNSIGNED",
 /* 130 */ "signed ::= INTEGER",
 /* 131 */ "signed ::= PLUS INTEGER",
 /* 132 */ "signed ::= MINUS INTEGER",
 /* 133 */ "cmd ::= CREATE TABLE create_table_args",
 /* 134 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 136 */ "cmd ::= CREATE TABLE create_table_list",
 /* 137 */ "create_table_list ::= create_from_stable",
 /* 138 */ "create_table_list ::= create_table_list create_from_stable",
 /* 139 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 140 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 142 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 143 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 144 */ "tagNamelist ::= ids",
 /* 145 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 146 */ "columnlist ::= columnlist COMMA column",
 /* 147 */ "columnlist ::= column",
 /* 148 */ "column ::= ids typename",
 /* 149 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 150 */ "tagitemlist ::= tagitem",
 /* 151 */ "tagitem ::= INTEGER",
 /* 152 */ "tagitem ::= FLOAT",
 /* 153 */ "tagitem ::= STRING",
 /* 154 */ "tagitem ::= BOOL",
 /* 155 */ "tagitem ::= NULL",
 /* 156 */ "tagitem ::= MINUS INTEGER",
 /* 157 */ "tagitem ::= MINUS FLOAT",
 /* 158 */ "tagitem ::= PLUS INTEGER",
 /* 159 */ "tagitem ::= PLUS FLOAT",
 /* 160 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 161 */ "select ::= LP select RP",
 /* 162 */ "union ::= select",
 /* 163 */ "union ::= union UNION ALL select",
 /* 164 */ "cmd ::= union",
 /* 165 */ "select ::= SELECT selcollist",
 /* 166 */ "sclp ::= selcollist COMMA",
 /* 167 */ "sclp ::=",
 /* 168 */ "selcollist ::= sclp distinct expr as",
 /* 169 */ "selcollist ::= sclp STAR",
 /* 170 */ "as ::= AS ids",
 /* 171 */ "as ::= ids",
 /* 172 */ "as ::=",
 /* 173 */ "distinct ::= DISTINCT",
 /* 174 */ "distinct ::=",
 /* 175 */ "from ::= FROM tablelist",
 /* 176 */ "from ::= FROM LP union RP",
 /* 177 */ "tablelist ::= ids cpxName",
 /* 178 */ "tablelist ::= ids cpxName ids",
 /* 179 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 180 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 181 */ "tmvar ::= VARIABLE",
 /* 182 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 183 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 184 */ "interval_opt ::=",
 /* 185 */ "session_option ::=",
 /* 186 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 187 */ "fill_opt ::=",
 /* 188 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 189 */ "fill_opt ::= FILL LP ID RP",
 /* 190 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 191 */ "sliding_opt ::=",
 /* 192 */ "orderby_opt ::=",
 /* 193 */ "orderby_opt ::= ORDER BY sortlist",
 /* 194 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 195 */ "sortlist ::= item sortorder",
 /* 196 */ "item ::= ids cpxName",
 /* 197 */ "sortorder ::= ASC",
 /* 198 */ "sortorder ::= DESC",
 /* 199 */ "sortorder ::=",
 /* 200 */ "groupby_opt ::=",
 /* 201 */ "groupby_opt ::= GROUP BY grouplist",
 /* 202 */ "grouplist ::= grouplist COMMA item",
 /* 203 */ "grouplist ::= item",
 /* 204 */ "having_opt ::=",
 /* 205 */ "having_opt ::= HAVING expr",
 /* 206 */ "limit_opt ::=",
 /* 207 */ "limit_opt ::= LIMIT signed",
 /* 208 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 209 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 210 */ "slimit_opt ::=",
 /* 211 */ "slimit_opt ::= SLIMIT signed",
 /* 212 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 213 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 214 */ "where_opt ::=",
 /* 215 */ "where_opt ::= WHERE expr",
 /* 216 */ "expr ::= LP expr RP",
 /* 217 */ "expr ::= ID",
 /* 218 */ "expr ::= ID DOT ID",
 /* 219 */ "expr ::= ID DOT STAR",
 /* 220 */ "expr ::= INTEGER",
 /* 221 */ "expr ::= MINUS INTEGER",
 /* 222 */ "expr ::= PLUS INTEGER",
 /* 223 */ "expr ::= FLOAT",
 /* 224 */ "expr ::= MINUS FLOAT",
 /* 225 */ "expr ::= PLUS FLOAT",
 /* 226 */ "expr ::= STRING",
 /* 227 */ "expr ::= NOW",
 /* 228 */ "expr ::= VARIABLE",
 /* 229 */ "expr ::= BOOL",
 /* 230 */ "expr ::= ID LP exprlist RP",
 /* 231 */ "expr ::= ID LP STAR RP",
 /* 232 */ "expr ::= expr IS NULL",
 /* 233 */ "expr ::= expr IS NOT NULL",
 /* 234 */ "expr ::= expr LT expr",
 /* 235 */ "expr ::= expr GT expr",
 /* 236 */ "expr ::= expr LE expr",
 /* 237 */ "expr ::= expr GE expr",
 /* 238 */ "expr ::= expr NE expr",
 /* 239 */ "expr ::= expr EQ expr",
 /* 240 */ "expr ::= expr BETWEEN expr AND expr",
 /* 241 */ "expr ::= expr AND expr",
 /* 242 */ "expr ::= expr OR expr",
 /* 243 */ "expr ::= expr PLUS expr",
 /* 244 */ "expr ::= expr MINUS expr",
 /* 245 */ "expr ::= expr STAR expr",
 /* 246 */ "expr ::= expr SLASH expr",
 /* 247 */ "expr ::= expr REM expr",
 /* 248 */ "expr ::= expr LIKE expr",
 /* 249 */ "expr ::= expr IN LP exprlist RP",
 /* 250 */ "exprlist ::= exprlist COMMA expritem",
 /* 251 */ "exprlist ::= expritem",
 /* 252 */ "expritem ::= expr",
 /* 253 */ "expritem ::=",
 /* 254 */ "cmd ::= RESET QUERY CACHE",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 258 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 263 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 264 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 266 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 267 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 268 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 212: /* keep */
    case 213: /* tagitemlist */
    case 234: /* columnlist */
    case 235: /* tagNamelist */
    case 244: /* fill_opt */
    case 246: /* groupby_opt */
    case 247: /* orderby_opt */
    case 258: /* sortlist */
    case 262: /* grouplist */
{
taosArrayDestroy((yypminor->yy291));
}
      break;
    case 232: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy412));
}
      break;
    case 236: /* select */
{
destroyQuerySqlNode((yypminor->yy110));
}
      break;
    case 239: /* selcollist */
    case 252: /* sclp */
    case 263: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy291));
}
      break;
    case 241: /* where_opt */
    case 248: /* having_opt */
    case 254: /* expr */
    case 264: /* expritem */
{
tSqlExprDestroy((yypminor->yy436));
}
      break;
    case 251: /* union */
{
destroyAllSelectClause((yypminor->yy154));
}
      break;
    case 259: /* sortitem */
{
tVariantDestroy(&(yypminor->yy216));
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
    assert( i<=YY_ACTTAB_COUNT );
    assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD );
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    assert( i<(int)YY_NLOOKAHEAD );
    if( yy_lookahead[i]!=iLookAhead ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      assert( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0]) );
      iFallback = yyFallback[iLookAhead];
      if( iFallback!=0 ){
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
        assert( j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) );
        if( yy_lookahead[j]==YYWILDCARD && iLookAhead>0 ){
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
      assert( i>=0 && i<sizeof(yy_action)/sizeof(yy_action[0]) );
      return yy_action[i];
    }
  }while(1);
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
*/
static YYACTIONTYPE yy_find_reduce_action(
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

/* For rule J, yyRuleInfoLhs[J] contains the symbol on the left-hand side
** of that rule */
static const YYCODETYPE yyRuleInfoLhs[] = {
   190,  /* (0) program ::= cmd */
   191,  /* (1) cmd ::= SHOW DATABASES */
   191,  /* (2) cmd ::= SHOW TOPICS */
   191,  /* (3) cmd ::= SHOW FUNCTIONS */
   191,  /* (4) cmd ::= SHOW MNODES */
   191,  /* (5) cmd ::= SHOW DNODES */
   191,  /* (6) cmd ::= SHOW ACCOUNTS */
   191,  /* (7) cmd ::= SHOW USERS */
   191,  /* (8) cmd ::= SHOW MODULES */
   191,  /* (9) cmd ::= SHOW QUERIES */
   191,  /* (10) cmd ::= SHOW CONNECTIONS */
   191,  /* (11) cmd ::= SHOW STREAMS */
   191,  /* (12) cmd ::= SHOW VARIABLES */
   191,  /* (13) cmd ::= SHOW SCORES */
   191,  /* (14) cmd ::= SHOW GRANTS */
   191,  /* (15) cmd ::= SHOW VNODES */
   191,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   192,  /* (17) dbPrefix ::= */
   192,  /* (18) dbPrefix ::= ids DOT */
   194,  /* (19) cpxName ::= */
   194,  /* (20) cpxName ::= DOT ids */
   191,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   191,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   191,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   191,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   191,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   191,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   191,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   191,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   191,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   191,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   191,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   191,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   191,  /* (33) cmd ::= DROP FUNCTION ids */
   191,  /* (34) cmd ::= DROP DNODE ids */
   191,  /* (35) cmd ::= DROP USER ids */
   191,  /* (36) cmd ::= DROP ACCOUNT ids */
   191,  /* (37) cmd ::= USE ids */
   191,  /* (38) cmd ::= DESCRIBE ids cpxName */
   191,  /* (39) cmd ::= ALTER USER ids PASS ids */
   191,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   191,  /* (41) cmd ::= ALTER DNODE ids ids */
   191,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   191,  /* (43) cmd ::= ALTER LOCAL ids */
   191,  /* (44) cmd ::= ALTER LOCAL ids ids */
   191,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   191,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   191,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   191,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   193,  /* (49) ids ::= ID */
   193,  /* (50) ids ::= STRING */
   195,  /* (51) ifexists ::= IF EXISTS */
   195,  /* (52) ifexists ::= */
   199,  /* (53) ifnotexists ::= IF NOT EXISTS */
   199,  /* (54) ifnotexists ::= */
   191,  /* (55) cmd ::= CREATE DNODE ids */
   191,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   191,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   191,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   191,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   191,  /* (60) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename */
   191,  /* (61) cmd ::= CREATE USER ids PASS ids */
   203,  /* (62) pps ::= */
   203,  /* (63) pps ::= PPS INTEGER */
   204,  /* (64) tseries ::= */
   204,  /* (65) tseries ::= TSERIES INTEGER */
   205,  /* (66) dbs ::= */
   205,  /* (67) dbs ::= DBS INTEGER */
   206,  /* (68) streams ::= */
   206,  /* (69) streams ::= STREAMS INTEGER */
   207,  /* (70) storage ::= */
   207,  /* (71) storage ::= STORAGE INTEGER */
   208,  /* (72) qtime ::= */
   208,  /* (73) qtime ::= QTIME INTEGER */
   209,  /* (74) users ::= */
   209,  /* (75) users ::= USERS INTEGER */
   210,  /* (76) conns ::= */
   210,  /* (77) conns ::= CONNS INTEGER */
   211,  /* (78) state ::= */
   211,  /* (79) state ::= STATE ids */
   198,  /* (80) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   212,  /* (81) keep ::= KEEP tagitemlist */
   214,  /* (82) cache ::= CACHE INTEGER */
   215,  /* (83) replica ::= REPLICA INTEGER */
   216,  /* (84) quorum ::= QUORUM INTEGER */
   217,  /* (85) days ::= DAYS INTEGER */
   218,  /* (86) minrows ::= MINROWS INTEGER */
   219,  /* (87) maxrows ::= MAXROWS INTEGER */
   220,  /* (88) blocks ::= BLOCKS INTEGER */
   221,  /* (89) ctime ::= CTIME INTEGER */
   222,  /* (90) wal ::= WAL INTEGER */
   223,  /* (91) fsync ::= FSYNC INTEGER */
   224,  /* (92) comp ::= COMP INTEGER */
   225,  /* (93) prec ::= PRECISION STRING */
   226,  /* (94) update ::= UPDATE INTEGER */
   227,  /* (95) cachelast ::= CACHELAST INTEGER */
   228,  /* (96) partitions ::= PARTITIONS INTEGER */
   200,  /* (97) db_optr ::= */
   200,  /* (98) db_optr ::= db_optr cache */
   200,  /* (99) db_optr ::= db_optr replica */
   200,  /* (100) db_optr ::= db_optr quorum */
   200,  /* (101) db_optr ::= db_optr days */
   200,  /* (102) db_optr ::= db_optr minrows */
   200,  /* (103) db_optr ::= db_optr maxrows */
   200,  /* (104) db_optr ::= db_optr blocks */
   200,  /* (105) db_optr ::= db_optr ctime */
   200,  /* (106) db_optr ::= db_optr wal */
   200,  /* (107) db_optr ::= db_optr fsync */
   200,  /* (108) db_optr ::= db_optr comp */
   200,  /* (109) db_optr ::= db_optr prec */
   200,  /* (110) db_optr ::= db_optr keep */
   200,  /* (111) db_optr ::= db_optr update */
   200,  /* (112) db_optr ::= db_optr cachelast */
   201,  /* (113) topic_optr ::= db_optr */
   201,  /* (114) topic_optr ::= topic_optr partitions */
   196,  /* (115) alter_db_optr ::= */
   196,  /* (116) alter_db_optr ::= alter_db_optr replica */
   196,  /* (117) alter_db_optr ::= alter_db_optr quorum */
   196,  /* (118) alter_db_optr ::= alter_db_optr keep */
   196,  /* (119) alter_db_optr ::= alter_db_optr blocks */
   196,  /* (120) alter_db_optr ::= alter_db_optr comp */
   196,  /* (121) alter_db_optr ::= alter_db_optr wal */
   196,  /* (122) alter_db_optr ::= alter_db_optr fsync */
   196,  /* (123) alter_db_optr ::= alter_db_optr update */
   196,  /* (124) alter_db_optr ::= alter_db_optr cachelast */
   197,  /* (125) alter_topic_optr ::= alter_db_optr */
   197,  /* (126) alter_topic_optr ::= alter_topic_optr partitions */
   202,  /* (127) typename ::= ids */
   202,  /* (128) typename ::= ids LP signed RP */
   202,  /* (129) typename ::= ids UNSIGNED */
   229,  /* (130) signed ::= INTEGER */
   229,  /* (131) signed ::= PLUS INTEGER */
   229,  /* (132) signed ::= MINUS INTEGER */
   191,  /* (133) cmd ::= CREATE TABLE create_table_args */
   191,  /* (134) cmd ::= CREATE TABLE create_stable_args */
   191,  /* (135) cmd ::= CREATE STABLE create_stable_args */
   191,  /* (136) cmd ::= CREATE TABLE create_table_list */
   232,  /* (137) create_table_list ::= create_from_stable */
   232,  /* (138) create_table_list ::= create_table_list create_from_stable */
   230,  /* (139) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   231,  /* (140) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   233,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   233,  /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   235,  /* (143) tagNamelist ::= tagNamelist COMMA ids */
   235,  /* (144) tagNamelist ::= ids */
   230,  /* (145) create_table_args ::= ifnotexists ids cpxName AS select */
   234,  /* (146) columnlist ::= columnlist COMMA column */
   234,  /* (147) columnlist ::= column */
   237,  /* (148) column ::= ids typename */
   213,  /* (149) tagitemlist ::= tagitemlist COMMA tagitem */
   213,  /* (150) tagitemlist ::= tagitem */
   238,  /* (151) tagitem ::= INTEGER */
   238,  /* (152) tagitem ::= FLOAT */
   238,  /* (153) tagitem ::= STRING */
   238,  /* (154) tagitem ::= BOOL */
   238,  /* (155) tagitem ::= NULL */
   238,  /* (156) tagitem ::= MINUS INTEGER */
   238,  /* (157) tagitem ::= MINUS FLOAT */
   238,  /* (158) tagitem ::= PLUS INTEGER */
   238,  /* (159) tagitem ::= PLUS FLOAT */
   236,  /* (160) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   236,  /* (161) select ::= LP select RP */
   251,  /* (162) union ::= select */
   251,  /* (163) union ::= union UNION ALL select */
   191,  /* (164) cmd ::= union */
   236,  /* (165) select ::= SELECT selcollist */
   252,  /* (166) sclp ::= selcollist COMMA */
   252,  /* (167) sclp ::= */
   239,  /* (168) selcollist ::= sclp distinct expr as */
   239,  /* (169) selcollist ::= sclp STAR */
   255,  /* (170) as ::= AS ids */
   255,  /* (171) as ::= ids */
   255,  /* (172) as ::= */
   253,  /* (173) distinct ::= DISTINCT */
   253,  /* (174) distinct ::= */
   240,  /* (175) from ::= FROM tablelist */
   240,  /* (176) from ::= FROM LP union RP */
   256,  /* (177) tablelist ::= ids cpxName */
   256,  /* (178) tablelist ::= ids cpxName ids */
   256,  /* (179) tablelist ::= tablelist COMMA ids cpxName */
   256,  /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
   257,  /* (181) tmvar ::= VARIABLE */
   242,  /* (182) interval_opt ::= INTERVAL LP tmvar RP */
   242,  /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   242,  /* (184) interval_opt ::= */
   243,  /* (185) session_option ::= */
   243,  /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   244,  /* (187) fill_opt ::= */
   244,  /* (188) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   244,  /* (189) fill_opt ::= FILL LP ID RP */
   245,  /* (190) sliding_opt ::= SLIDING LP tmvar RP */
   245,  /* (191) sliding_opt ::= */
   247,  /* (192) orderby_opt ::= */
   247,  /* (193) orderby_opt ::= ORDER BY sortlist */
   258,  /* (194) sortlist ::= sortlist COMMA item sortorder */
   258,  /* (195) sortlist ::= item sortorder */
   260,  /* (196) item ::= ids cpxName */
   261,  /* (197) sortorder ::= ASC */
   261,  /* (198) sortorder ::= DESC */
   261,  /* (199) sortorder ::= */
   246,  /* (200) groupby_opt ::= */
   246,  /* (201) groupby_opt ::= GROUP BY grouplist */
   262,  /* (202) grouplist ::= grouplist COMMA item */
   262,  /* (203) grouplist ::= item */
   248,  /* (204) having_opt ::= */
   248,  /* (205) having_opt ::= HAVING expr */
   250,  /* (206) limit_opt ::= */
   250,  /* (207) limit_opt ::= LIMIT signed */
   250,  /* (208) limit_opt ::= LIMIT signed OFFSET signed */
   250,  /* (209) limit_opt ::= LIMIT signed COMMA signed */
   249,  /* (210) slimit_opt ::= */
   249,  /* (211) slimit_opt ::= SLIMIT signed */
   249,  /* (212) slimit_opt ::= SLIMIT signed SOFFSET signed */
   249,  /* (213) slimit_opt ::= SLIMIT signed COMMA signed */
   241,  /* (214) where_opt ::= */
   241,  /* (215) where_opt ::= WHERE expr */
   254,  /* (216) expr ::= LP expr RP */
   254,  /* (217) expr ::= ID */
   254,  /* (218) expr ::= ID DOT ID */
   254,  /* (219) expr ::= ID DOT STAR */
   254,  /* (220) expr ::= INTEGER */
   254,  /* (221) expr ::= MINUS INTEGER */
   254,  /* (222) expr ::= PLUS INTEGER */
   254,  /* (223) expr ::= FLOAT */
   254,  /* (224) expr ::= MINUS FLOAT */
   254,  /* (225) expr ::= PLUS FLOAT */
   254,  /* (226) expr ::= STRING */
   254,  /* (227) expr ::= NOW */
   254,  /* (228) expr ::= VARIABLE */
   254,  /* (229) expr ::= BOOL */
   254,  /* (230) expr ::= ID LP exprlist RP */
   254,  /* (231) expr ::= ID LP STAR RP */
   254,  /* (232) expr ::= expr IS NULL */
   254,  /* (233) expr ::= expr IS NOT NULL */
   254,  /* (234) expr ::= expr LT expr */
   254,  /* (235) expr ::= expr GT expr */
   254,  /* (236) expr ::= expr LE expr */
   254,  /* (237) expr ::= expr GE expr */
   254,  /* (238) expr ::= expr NE expr */
   254,  /* (239) expr ::= expr EQ expr */
   254,  /* (240) expr ::= expr BETWEEN expr AND expr */
   254,  /* (241) expr ::= expr AND expr */
   254,  /* (242) expr ::= expr OR expr */
   254,  /* (243) expr ::= expr PLUS expr */
   254,  /* (244) expr ::= expr MINUS expr */
   254,  /* (245) expr ::= expr STAR expr */
   254,  /* (246) expr ::= expr SLASH expr */
   254,  /* (247) expr ::= expr REM expr */
   254,  /* (248) expr ::= expr LIKE expr */
   254,  /* (249) expr ::= expr IN LP exprlist RP */
   263,  /* (250) exprlist ::= exprlist COMMA expritem */
   263,  /* (251) exprlist ::= expritem */
   264,  /* (252) expritem ::= expr */
   264,  /* (253) expritem ::= */
   191,  /* (254) cmd ::= RESET QUERY CACHE */
   191,  /* (255) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   191,  /* (256) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   191,  /* (257) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   191,  /* (258) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   191,  /* (259) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   191,  /* (260) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   191,  /* (261) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   191,  /* (262) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   191,  /* (263) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   191,  /* (264) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   191,  /* (265) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   191,  /* (266) cmd ::= KILL CONNECTION INTEGER */
   191,  /* (267) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   191,  /* (268) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW FUNCTIONS */
   -2,  /* (4) cmd ::= SHOW MNODES */
   -2,  /* (5) cmd ::= SHOW DNODES */
   -2,  /* (6) cmd ::= SHOW ACCOUNTS */
   -2,  /* (7) cmd ::= SHOW USERS */
   -2,  /* (8) cmd ::= SHOW MODULES */
   -2,  /* (9) cmd ::= SHOW QUERIES */
   -2,  /* (10) cmd ::= SHOW CONNECTIONS */
   -2,  /* (11) cmd ::= SHOW STREAMS */
   -2,  /* (12) cmd ::= SHOW VARIABLES */
   -2,  /* (13) cmd ::= SHOW SCORES */
   -2,  /* (14) cmd ::= SHOW GRANTS */
   -2,  /* (15) cmd ::= SHOW VNODES */
   -3,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (17) dbPrefix ::= */
   -2,  /* (18) dbPrefix ::= ids DOT */
    0,  /* (19) cpxName ::= */
   -2,  /* (20) cpxName ::= DOT ids */
   -5,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   -4,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (33) cmd ::= DROP FUNCTION ids */
   -3,  /* (34) cmd ::= DROP DNODE ids */
   -3,  /* (35) cmd ::= DROP USER ids */
   -3,  /* (36) cmd ::= DROP ACCOUNT ids */
   -2,  /* (37) cmd ::= USE ids */
   -3,  /* (38) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (39) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (41) cmd ::= ALTER DNODE ids ids */
   -5,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (43) cmd ::= ALTER LOCAL ids */
   -4,  /* (44) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (49) ids ::= ID */
   -1,  /* (50) ids ::= STRING */
   -2,  /* (51) ifexists ::= IF EXISTS */
    0,  /* (52) ifexists ::= */
   -3,  /* (53) ifnotexists ::= IF NOT EXISTS */
    0,  /* (54) ifnotexists ::= */
   -3,  /* (55) cmd ::= CREATE DNODE ids */
   -6,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -7,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
   -8,  /* (60) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename */
   -5,  /* (61) cmd ::= CREATE USER ids PASS ids */
    0,  /* (62) pps ::= */
   -2,  /* (63) pps ::= PPS INTEGER */
    0,  /* (64) tseries ::= */
   -2,  /* (65) tseries ::= TSERIES INTEGER */
    0,  /* (66) dbs ::= */
   -2,  /* (67) dbs ::= DBS INTEGER */
    0,  /* (68) streams ::= */
   -2,  /* (69) streams ::= STREAMS INTEGER */
    0,  /* (70) storage ::= */
   -2,  /* (71) storage ::= STORAGE INTEGER */
    0,  /* (72) qtime ::= */
   -2,  /* (73) qtime ::= QTIME INTEGER */
    0,  /* (74) users ::= */
   -2,  /* (75) users ::= USERS INTEGER */
    0,  /* (76) conns ::= */
   -2,  /* (77) conns ::= CONNS INTEGER */
    0,  /* (78) state ::= */
   -2,  /* (79) state ::= STATE ids */
   -9,  /* (80) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (81) keep ::= KEEP tagitemlist */
   -2,  /* (82) cache ::= CACHE INTEGER */
   -2,  /* (83) replica ::= REPLICA INTEGER */
   -2,  /* (84) quorum ::= QUORUM INTEGER */
   -2,  /* (85) days ::= DAYS INTEGER */
   -2,  /* (86) minrows ::= MINROWS INTEGER */
   -2,  /* (87) maxrows ::= MAXROWS INTEGER */
   -2,  /* (88) blocks ::= BLOCKS INTEGER */
   -2,  /* (89) ctime ::= CTIME INTEGER */
   -2,  /* (90) wal ::= WAL INTEGER */
   -2,  /* (91) fsync ::= FSYNC INTEGER */
   -2,  /* (92) comp ::= COMP INTEGER */
   -2,  /* (93) prec ::= PRECISION STRING */
   -2,  /* (94) update ::= UPDATE INTEGER */
   -2,  /* (95) cachelast ::= CACHELAST INTEGER */
   -2,  /* (96) partitions ::= PARTITIONS INTEGER */
    0,  /* (97) db_optr ::= */
   -2,  /* (98) db_optr ::= db_optr cache */
   -2,  /* (99) db_optr ::= db_optr replica */
   -2,  /* (100) db_optr ::= db_optr quorum */
   -2,  /* (101) db_optr ::= db_optr days */
   -2,  /* (102) db_optr ::= db_optr minrows */
   -2,  /* (103) db_optr ::= db_optr maxrows */
   -2,  /* (104) db_optr ::= db_optr blocks */
   -2,  /* (105) db_optr ::= db_optr ctime */
   -2,  /* (106) db_optr ::= db_optr wal */
   -2,  /* (107) db_optr ::= db_optr fsync */
   -2,  /* (108) db_optr ::= db_optr comp */
   -2,  /* (109) db_optr ::= db_optr prec */
   -2,  /* (110) db_optr ::= db_optr keep */
   -2,  /* (111) db_optr ::= db_optr update */
   -2,  /* (112) db_optr ::= db_optr cachelast */
   -1,  /* (113) topic_optr ::= db_optr */
   -2,  /* (114) topic_optr ::= topic_optr partitions */
    0,  /* (115) alter_db_optr ::= */
   -2,  /* (116) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (117) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (118) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (119) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (120) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (121) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (122) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (123) alter_db_optr ::= alter_db_optr update */
   -2,  /* (124) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (125) alter_topic_optr ::= alter_db_optr */
   -2,  /* (126) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (127) typename ::= ids */
   -4,  /* (128) typename ::= ids LP signed RP */
   -2,  /* (129) typename ::= ids UNSIGNED */
   -1,  /* (130) signed ::= INTEGER */
   -2,  /* (131) signed ::= PLUS INTEGER */
   -2,  /* (132) signed ::= MINUS INTEGER */
   -3,  /* (133) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (134) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (135) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (136) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (137) create_table_list ::= create_from_stable */
   -2,  /* (138) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (139) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (140) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (143) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (144) tagNamelist ::= ids */
   -5,  /* (145) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (146) columnlist ::= columnlist COMMA column */
   -1,  /* (147) columnlist ::= column */
   -2,  /* (148) column ::= ids typename */
   -3,  /* (149) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (150) tagitemlist ::= tagitem */
   -1,  /* (151) tagitem ::= INTEGER */
   -1,  /* (152) tagitem ::= FLOAT */
   -1,  /* (153) tagitem ::= STRING */
   -1,  /* (154) tagitem ::= BOOL */
   -1,  /* (155) tagitem ::= NULL */
   -2,  /* (156) tagitem ::= MINUS INTEGER */
   -2,  /* (157) tagitem ::= MINUS FLOAT */
   -2,  /* (158) tagitem ::= PLUS INTEGER */
   -2,  /* (159) tagitem ::= PLUS FLOAT */
  -13,  /* (160) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (161) select ::= LP select RP */
   -1,  /* (162) union ::= select */
   -4,  /* (163) union ::= union UNION ALL select */
   -1,  /* (164) cmd ::= union */
   -2,  /* (165) select ::= SELECT selcollist */
   -2,  /* (166) sclp ::= selcollist COMMA */
    0,  /* (167) sclp ::= */
   -4,  /* (168) selcollist ::= sclp distinct expr as */
   -2,  /* (169) selcollist ::= sclp STAR */
   -2,  /* (170) as ::= AS ids */
   -1,  /* (171) as ::= ids */
    0,  /* (172) as ::= */
   -1,  /* (173) distinct ::= DISTINCT */
    0,  /* (174) distinct ::= */
   -2,  /* (175) from ::= FROM tablelist */
   -4,  /* (176) from ::= FROM LP union RP */
   -2,  /* (177) tablelist ::= ids cpxName */
   -3,  /* (178) tablelist ::= ids cpxName ids */
   -4,  /* (179) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (181) tmvar ::= VARIABLE */
   -4,  /* (182) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (184) interval_opt ::= */
    0,  /* (185) session_option ::= */
   -7,  /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (187) fill_opt ::= */
   -6,  /* (188) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (189) fill_opt ::= FILL LP ID RP */
   -4,  /* (190) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (191) sliding_opt ::= */
    0,  /* (192) orderby_opt ::= */
   -3,  /* (193) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (194) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (195) sortlist ::= item sortorder */
   -2,  /* (196) item ::= ids cpxName */
   -1,  /* (197) sortorder ::= ASC */
   -1,  /* (198) sortorder ::= DESC */
    0,  /* (199) sortorder ::= */
    0,  /* (200) groupby_opt ::= */
   -3,  /* (201) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (202) grouplist ::= grouplist COMMA item */
   -1,  /* (203) grouplist ::= item */
    0,  /* (204) having_opt ::= */
   -2,  /* (205) having_opt ::= HAVING expr */
    0,  /* (206) limit_opt ::= */
   -2,  /* (207) limit_opt ::= LIMIT signed */
   -4,  /* (208) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (209) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (210) slimit_opt ::= */
   -2,  /* (211) slimit_opt ::= SLIMIT signed */
   -4,  /* (212) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (213) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (214) where_opt ::= */
   -2,  /* (215) where_opt ::= WHERE expr */
   -3,  /* (216) expr ::= LP expr RP */
   -1,  /* (217) expr ::= ID */
   -3,  /* (218) expr ::= ID DOT ID */
   -3,  /* (219) expr ::= ID DOT STAR */
   -1,  /* (220) expr ::= INTEGER */
   -2,  /* (221) expr ::= MINUS INTEGER */
   -2,  /* (222) expr ::= PLUS INTEGER */
   -1,  /* (223) expr ::= FLOAT */
   -2,  /* (224) expr ::= MINUS FLOAT */
   -2,  /* (225) expr ::= PLUS FLOAT */
   -1,  /* (226) expr ::= STRING */
   -1,  /* (227) expr ::= NOW */
   -1,  /* (228) expr ::= VARIABLE */
   -1,  /* (229) expr ::= BOOL */
   -4,  /* (230) expr ::= ID LP exprlist RP */
   -4,  /* (231) expr ::= ID LP STAR RP */
   -3,  /* (232) expr ::= expr IS NULL */
   -4,  /* (233) expr ::= expr IS NOT NULL */
   -3,  /* (234) expr ::= expr LT expr */
   -3,  /* (235) expr ::= expr GT expr */
   -3,  /* (236) expr ::= expr LE expr */
   -3,  /* (237) expr ::= expr GE expr */
   -3,  /* (238) expr ::= expr NE expr */
   -3,  /* (239) expr ::= expr EQ expr */
   -5,  /* (240) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (241) expr ::= expr AND expr */
   -3,  /* (242) expr ::= expr OR expr */
   -3,  /* (243) expr ::= expr PLUS expr */
   -3,  /* (244) expr ::= expr MINUS expr */
   -3,  /* (245) expr ::= expr STAR expr */
   -3,  /* (246) expr ::= expr SLASH expr */
   -3,  /* (247) expr ::= expr REM expr */
   -3,  /* (248) expr ::= expr LIKE expr */
   -5,  /* (249) expr ::= expr IN LP exprlist RP */
   -3,  /* (250) exprlist ::= exprlist COMMA expritem */
   -1,  /* (251) exprlist ::= expritem */
   -1,  /* (252) expritem ::= expr */
    0,  /* (253) expritem ::= */
   -3,  /* (254) cmd ::= RESET QUERY CACHE */
   -7,  /* (255) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (256) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (257) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (258) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (259) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (260) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (261) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (262) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (263) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (264) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (265) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (266) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (267) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (268) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH
  (void)yyLookahead;
  (void)yyLookaheadToken;
  yymsp = yypParser->yytos;
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfoNRhs[yyruleno];
    if( yysize ){
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s, pop back to state %d.\n",
        yyTracePrompt,
        yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action",
        yymsp[yysize].stateno);
    }else{
      fprintf(yyTraceFILE, "%sReduce %d [%s]%s.\n",
        yyTracePrompt, yyruleno, yyRuleName[yyruleno],
        yyruleno<YYNRULE_WITH_ACTION ? "" : " without external action");
    }
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if( yyRuleInfoNRhs[yyruleno]==0 ){
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
      case 0: /* program ::= cmd */
      case 133: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==134);
      case 135: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==135);
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
      case 22: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 29: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 30: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 31: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 32: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 33: /* cmd ::= DROP FUNCTION ids */
{ setDropFuncInfo(pInfo, TSDB_SQL_DROP_FUNCTION, &yymsp[0].minor.yy0); }
        break;
      case 34: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 35: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 36: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 37: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 38: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 39: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 40: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 42: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 44: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 45: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 46: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==46);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy122, &t);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy211);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 49: /* ids ::= ID */
      case 50: /* ids ::= STRING */ yytestcase(yyruleno==50);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 51: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 52: /* ifexists ::= */
      case 54: /* ifnotexists ::= */ yytestcase(yyruleno==54);
      case 174: /* distinct ::= */ yytestcase(yyruleno==174);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy211);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy122, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy153, 1);}
        break;
      case 60: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-4].minor.yy0, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy153, 2);}
        break;
      case 61: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 62: /* pps ::= */
      case 64: /* tseries ::= */ yytestcase(yyruleno==64);
      case 66: /* dbs ::= */ yytestcase(yyruleno==66);
      case 68: /* streams ::= */ yytestcase(yyruleno==68);
      case 70: /* storage ::= */ yytestcase(yyruleno==70);
      case 72: /* qtime ::= */ yytestcase(yyruleno==72);
      case 74: /* users ::= */ yytestcase(yyruleno==74);
      case 76: /* conns ::= */ yytestcase(yyruleno==76);
      case 78: /* state ::= */ yytestcase(yyruleno==78);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 63: /* pps ::= PPS INTEGER */
      case 65: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==65);
      case 67: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==69);
      case 71: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==71);
      case 73: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==73);
      case 75: /* users ::= USERS INTEGER */ yytestcase(yyruleno==75);
      case 77: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==77);
      case 79: /* state ::= STATE ids */ yytestcase(yyruleno==79);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 80: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy211.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy211.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy211.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy211.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy211.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy211.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy211.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy211.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy211 = yylhsminor.yy211;
        break;
      case 81: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy291 = yymsp[0].minor.yy291; }
        break;
      case 82: /* cache ::= CACHE INTEGER */
      case 83: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==83);
      case 84: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==84);
      case 85: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==86);
      case 87: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==89);
      case 90: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==90);
      case 91: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==91);
      case 92: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==92);
      case 93: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==93);
      case 94: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==94);
      case 95: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==95);
      case 96: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==96);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 97: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy122); yymsp[1].minor.yy122.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 98: /* db_optr ::= db_optr cache */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 99: /* db_optr ::= db_optr replica */
      case 116: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==116);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 100: /* db_optr ::= db_optr quorum */
      case 117: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==117);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 101: /* db_optr ::= db_optr days */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 102: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 103: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 104: /* db_optr ::= db_optr blocks */
      case 119: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==119);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 105: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 106: /* db_optr ::= db_optr wal */
      case 121: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==121);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 107: /* db_optr ::= db_optr fsync */
      case 122: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==122);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 108: /* db_optr ::= db_optr comp */
      case 120: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==120);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 109: /* db_optr ::= db_optr prec */
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 110: /* db_optr ::= db_optr keep */
      case 118: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==118);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.keep = yymsp[0].minor.yy291; }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 111: /* db_optr ::= db_optr update */
      case 123: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==123);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 112: /* db_optr ::= db_optr cachelast */
      case 124: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==124);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 113: /* topic_optr ::= db_optr */
      case 125: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==125);
{ yylhsminor.yy122 = yymsp[0].minor.yy122; yylhsminor.yy122.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy122 = yylhsminor.yy122;
        break;
      case 114: /* topic_optr ::= topic_optr partitions */
      case 126: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==126);
{ yylhsminor.yy122 = yymsp[-1].minor.yy122; yylhsminor.yy122.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy122 = yylhsminor.yy122;
        break;
      case 115: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy122); yymsp[1].minor.yy122.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 127: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy153, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy153 = yylhsminor.yy153;
        break;
      case 128: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy179 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy153, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy179;  // negative value of name length
    tSetColumnType(&yylhsminor.yy153, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy153 = yylhsminor.yy153;
        break;
      case 129: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy153, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy153 = yylhsminor.yy153;
        break;
      case 130: /* signed ::= INTEGER */
{ yylhsminor.yy179 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy179 = yylhsminor.yy179;
        break;
      case 131: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy179 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 132: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy179 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 136: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy412;}
        break;
      case 137: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy446);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy412 = pCreateTable;
}
  yymsp[0].minor.yy412 = yylhsminor.yy412;
        break;
      case 138: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy412->childTableInfo, &yymsp[0].minor.yy446);
  yylhsminor.yy412 = yymsp[-1].minor.yy412;
}
  yymsp[-1].minor.yy412 = yylhsminor.yy412;
        break;
      case 139: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy412 = tSetCreateTableInfo(yymsp[-1].minor.yy291, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy412 = yylhsminor.yy412;
        break;
      case 140: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy412 = tSetCreateTableInfo(yymsp[-5].minor.yy291, yymsp[-1].minor.yy291, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy412 = yylhsminor.yy412;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy446 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy291, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy446 = yylhsminor.yy446;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy446 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy291, yymsp[-1].minor.yy291, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy446 = yylhsminor.yy446;
        break;
      case 143: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy291, &yymsp[0].minor.yy0); yylhsminor.yy291 = yymsp[-2].minor.yy291;  }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 144: /* tagNamelist ::= ids */
{yylhsminor.yy291 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy291, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy412 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy110, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy412, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy412 = yylhsminor.yy412;
        break;
      case 146: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy291, &yymsp[0].minor.yy153); yylhsminor.yy291 = yymsp[-2].minor.yy291;  }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 147: /* columnlist ::= column */
{yylhsminor.yy291 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy291, &yymsp[0].minor.yy153);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 148: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy153, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy153);
}
  yymsp[-1].minor.yy153 = yylhsminor.yy153;
        break;
      case 149: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy291 = tVariantListAppend(yymsp[-2].minor.yy291, &yymsp[0].minor.yy216, -1);    }
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 150: /* tagitemlist ::= tagitem */
{ yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[0].minor.yy216, -1); }
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 151: /* tagitem ::= INTEGER */
      case 152: /* tagitem ::= FLOAT */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= STRING */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= BOOL */ yytestcase(yyruleno==154);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy216, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy216 = yylhsminor.yy216;
        break;
      case 155: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy216, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy216 = yylhsminor.yy216;
        break;
      case 156: /* tagitem ::= MINUS INTEGER */
      case 157: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==159);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy216, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy216 = yylhsminor.yy216;
        break;
      case 160: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy110 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy291, yymsp[-10].minor.yy232, yymsp[-9].minor.yy436, yymsp[-4].minor.yy291, yymsp[-3].minor.yy291, &yymsp[-8].minor.yy400, &yymsp[-7].minor.yy139, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy291, &yymsp[0].minor.yy74, &yymsp[-1].minor.yy74);
}
  yymsp[-12].minor.yy110 = yylhsminor.yy110;
        break;
      case 161: /* select ::= LP select RP */
{yymsp[-2].minor.yy110 = yymsp[-1].minor.yy110;}
        break;
      case 162: /* union ::= select */
{ yylhsminor.yy154 = setSubclause(NULL, yymsp[0].minor.yy110); }
  yymsp[0].minor.yy154 = yylhsminor.yy154;
        break;
      case 163: /* union ::= union UNION ALL select */
{ yylhsminor.yy154 = appendSelectClause(yymsp[-3].minor.yy154, yymsp[0].minor.yy110); }
  yymsp[-3].minor.yy154 = yylhsminor.yy154;
        break;
      case 164: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy154, NULL, TSDB_SQL_SELECT); }
        break;
      case 165: /* select ::= SELECT selcollist */
{
  yylhsminor.yy110 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy291, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy110 = yylhsminor.yy110;
        break;
      case 166: /* sclp ::= selcollist COMMA */
{yylhsminor.yy291 = yymsp[-1].minor.yy291;}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 167: /* sclp ::= */
      case 192: /* orderby_opt ::= */ yytestcase(yyruleno==192);
{yymsp[1].minor.yy291 = 0;}
        break;
      case 168: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy291 = tSqlExprListAppend(yymsp[-3].minor.yy291, yymsp[-1].minor.yy436,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 169: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy291 = tSqlExprListAppend(yymsp[-1].minor.yy291, pNode, 0, 0);
}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 170: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 171: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 173: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 175: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy232 = yymsp[0].minor.yy291;}
        break;
      case 176: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy232 = yymsp[-1].minor.yy154;}
        break;
      case 177: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy291 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 178: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy291 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy291 = setTableNameList(yymsp[-3].minor.yy291, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;

  yylhsminor.yy291 = setTableNameList(yymsp[-4].minor.yy291, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy291 = yylhsminor.yy291;
        break;
      case 181: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy400.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy400.offset.n = 0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy400.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy400.offset = yymsp[-1].minor.yy0;}
        break;
      case 184: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy400, 0, sizeof(yymsp[1].minor.yy400));}
        break;
      case 185: /* session_option ::= */
{yymsp[1].minor.yy139.col.n = 0; yymsp[1].minor.yy139.gap.n = 0;}
        break;
      case 186: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy139.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy139.gap = yymsp[-1].minor.yy0;
}
        break;
      case 187: /* fill_opt ::= */
{ yymsp[1].minor.yy291 = 0;     }
        break;
      case 188: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy291, &A, -1, 0);
    yymsp[-5].minor.yy291 = yymsp[-1].minor.yy291;
}
        break;
      case 189: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy291 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 190: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 191: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 193: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy291 = yymsp[0].minor.yy291;}
        break;
      case 194: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy291 = tVariantListAppend(yymsp[-3].minor.yy291, &yymsp[-1].minor.yy216, yymsp[0].minor.yy382);
}
  yymsp[-3].minor.yy291 = yylhsminor.yy291;
        break;
      case 195: /* sortlist ::= item sortorder */
{
  yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[-1].minor.yy216, yymsp[0].minor.yy382);
}
  yymsp[-1].minor.yy291 = yylhsminor.yy291;
        break;
      case 196: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy216, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy216 = yylhsminor.yy216;
        break;
      case 197: /* sortorder ::= ASC */
{ yymsp[0].minor.yy382 = TSDB_ORDER_ASC; }
        break;
      case 198: /* sortorder ::= DESC */
{ yymsp[0].minor.yy382 = TSDB_ORDER_DESC;}
        break;
      case 199: /* sortorder ::= */
{ yymsp[1].minor.yy382 = TSDB_ORDER_ASC; }
        break;
      case 200: /* groupby_opt ::= */
{ yymsp[1].minor.yy291 = 0;}
        break;
      case 201: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy291 = yymsp[0].minor.yy291;}
        break;
      case 202: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy291 = tVariantListAppend(yymsp[-2].minor.yy291, &yymsp[0].minor.yy216, -1);
}
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 203: /* grouplist ::= item */
{
  yylhsminor.yy291 = tVariantListAppend(NULL, &yymsp[0].minor.yy216, -1);
}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 204: /* having_opt ::= */
      case 214: /* where_opt ::= */ yytestcase(yyruleno==214);
      case 253: /* expritem ::= */ yytestcase(yyruleno==253);
{yymsp[1].minor.yy436 = 0;}
        break;
      case 205: /* having_opt ::= HAVING expr */
      case 215: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==215);
{yymsp[-1].minor.yy436 = yymsp[0].minor.yy436;}
        break;
      case 206: /* limit_opt ::= */
      case 210: /* slimit_opt ::= */ yytestcase(yyruleno==210);
{yymsp[1].minor.yy74.limit = -1; yymsp[1].minor.yy74.offset = 0;}
        break;
      case 207: /* limit_opt ::= LIMIT signed */
      case 211: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-1].minor.yy74.offset = 0;}
        break;
      case 208: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy74.limit = yymsp[-2].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[0].minor.yy179;}
        break;
      case 209: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[-2].minor.yy179;}
        break;
      case 212: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy74.limit = yymsp[-2].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[0].minor.yy179;}
        break;
      case 213: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy74.limit = yymsp[0].minor.yy179;  yymsp[-3].minor.yy74.offset = yymsp[-2].minor.yy179;}
        break;
      case 216: /* expr ::= LP expr RP */
{yylhsminor.yy436 = yymsp[-1].minor.yy436; yylhsminor.yy436->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy436->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 217: /* expr ::= ID */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 218: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 219: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 220: /* expr ::= INTEGER */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 221: /* expr ::= MINUS INTEGER */
      case 222: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 223: /* expr ::= FLOAT */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 224: /* expr ::= MINUS FLOAT */
      case 225: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==225);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy436 = yylhsminor.yy436;
        break;
      case 226: /* expr ::= STRING */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 227: /* expr ::= NOW */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 228: /* expr ::= VARIABLE */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 229: /* expr ::= BOOL */
{ yylhsminor.yy436 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 230: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy436 = tSqlExprCreateFunction(yymsp[-1].minor.yy291, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 231: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy436 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 232: /* expr ::= expr IS NULL */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 233: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-3].minor.yy436, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy436 = yylhsminor.yy436;
        break;
      case 234: /* expr ::= expr LT expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LT);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 235: /* expr ::= expr GT expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_GT);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 236: /* expr ::= expr LE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 237: /* expr ::= expr GE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_GE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 238: /* expr ::= expr NE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_NE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 239: /* expr ::= expr EQ expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_EQ);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 240: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy436); yylhsminor.yy436 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy436, yymsp[-2].minor.yy436, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy436, TK_LE), TK_AND);}
  yymsp[-4].minor.yy436 = yylhsminor.yy436;
        break;
      case 241: /* expr ::= expr AND expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_AND);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 242: /* expr ::= expr OR expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_OR); }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 243: /* expr ::= expr PLUS expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_PLUS);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 244: /* expr ::= expr MINUS expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_MINUS); }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 245: /* expr ::= expr STAR expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_STAR);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 246: /* expr ::= expr SLASH expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_DIVIDE);}
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 247: /* expr ::= expr REM expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_REM);   }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 248: /* expr ::= expr LIKE expr */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-2].minor.yy436, yymsp[0].minor.yy436, TK_LIKE);  }
  yymsp[-2].minor.yy436 = yylhsminor.yy436;
        break;
      case 249: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy436 = tSqlExprCreate(yymsp[-4].minor.yy436, (tSqlExpr*)yymsp[-1].minor.yy291, TK_IN); }
  yymsp[-4].minor.yy436 = yylhsminor.yy436;
        break;
      case 250: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy291 = tSqlExprListAppend(yymsp[-2].minor.yy291,yymsp[0].minor.yy436,0, 0);}
  yymsp[-2].minor.yy291 = yylhsminor.yy291;
        break;
      case 251: /* exprlist ::= expritem */
{yylhsminor.yy291 = tSqlExprListAppend(0,yymsp[0].minor.yy436,0, 0);}
  yymsp[0].minor.yy291 = yylhsminor.yy291;
        break;
      case 252: /* expritem ::= expr */
{yylhsminor.yy436 = yymsp[0].minor.yy436;}
  yymsp[0].minor.yy436 = yylhsminor.yy436;
        break;
      case 254: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 257: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 260: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy216, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy291, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 266: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 267: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 268: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfoLhs)/sizeof(yyRuleInfoLhs[0]) );
  yygoto = yyRuleInfoLhs[yyruleno];
  yysize = yyRuleInfoNRhs[yyruleno];
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
    yyact = yy_find_shift_action((YYCODETYPE)yymajor,yyact);
    if( yyact >= YY_MIN_REDUCE ){
      yyact = yy_reduce(yypParser,yyact-YY_MIN_REDUCE,yymajor,
                        yyminor ParseCTX_PARAM);
    }else if( yyact <= YY_MAX_SHIFTREDUCE ){
      yy_shift(yypParser,yyact,(YYCODETYPE)yymajor,yyminor);
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
            && (yyact = yy_find_reduce_action(
                        yypParser->yytos->stateno,
                        YYERRORSYMBOL)) > YY_MAX_SHIFTREDUCE
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

/*
** Return the fallback token corresponding to canonical token iToken, or
** 0 if iToken has no fallback.
*/
int ParseFallback(int iToken){
#ifdef YYFALLBACK
  assert( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) );
  return yyFallback[iToken];
#else
  (void)iToken;
  return 0;
#endif
}
