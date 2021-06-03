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
#define YYNOCODE 271
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  tSqlExpr* yy42;
  SSessionWindowVal yy51;
  SCreateAcctInfo yy77;
  int yy82;
  SRelationInfo* yy120;
  TAOS_FIELD yy181;
  SLimitVal yy188;
  int64_t yy271;
  SCreatedTableInfo yy282;
  tVariant yy312;
  SIntervalVal yy314;
  SArray* yy347;
  SWindowStateVal yy356;
  SCreateTableSql* yy360;
  SCreateDbInfo yy540;
  SSqlNode* yy542;
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
#define YYNSTATE             342
#define YYNRULE              281
#define YYNRULE_WITH_ACTION  281
#define YYNTOKEN             193
#define YY_MAX_SHIFT         341
#define YY_MIN_SHIFTREDUCE   543
#define YY_MAX_SHIFTREDUCE   823
#define YY_ERROR_ACTION      824
#define YY_ACCEPT_ACTION     825
#define YY_NO_ACTION         826
#define YY_MIN_REDUCE        827
#define YY_MAX_REDUCE        1107
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
#define YY_ACTTAB_COUNT (711)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    18,  593,  992,  593,  195,  145,  992,  593,   88,  594,
 /*    10 */  1001,  594,  201,   50,   51,  594,   54,   55,  223,  197,
 /*    20 */   232,   44,  224,   53,  283,   58,   56,   60,   57,  229,
 /*    30 */  1089,  220,  339,   49,   48,  825,  341,   47,   46,   45,
 /*    40 */   963,  964,   30,  967,  773,  152,  776,  544,  545,  546,
 /*    50 */   547,  548,  549,  550,  551,  552,  553,  554,  555,  556,
 /*    60 */   557,  340,  226,  237,  221,   50,   51,  152,   54,   55,
 /*    70 */   266,  265,  232,   44,  998,   53,  283,   58,   56,   60,
 /*    80 */    57, 1085,  992,   73,  305,   49,   48,  980,  980,   47,
 /*    90 */    46,   45,   50,   51,  325,   54,   55,  197,  259,  232,
 /*   100 */    44,  244,   53,  283,   58,   56,   60,   57, 1088,   33,
 /*   110 */   167, 1084,   49,   48,  197, 1037,   47,   46,   45,   50,
 /*   120 */    52,   79,   54,   55,  228, 1089,  232,   44,   39,   53,
 /*   130 */   283,   58,   56,   60,   57,  280,  772,   84,  775,   49,
 /*   140 */    48, 1083,  593,   47,   46,   45,   51,  152,   54,   55,
 /*   150 */   594,  222,  232,   44,  977,   53,  283,   58,   56,   60,
 /*   160 */    57,  780,  152,  767,   33,   49,   48,    1,  169,   47,
 /*   170 */    46,   45,   24,  303,  334,  333,  302,  301,  300,  332,
 /*   180 */   299,  331,  330,  329,  298,  328,  327,  940,  214,  928,
 /*   190 */   929,  930,  931,  932,  933,  934,  935,  936,  937,  938,
 /*   200 */   939,  941,  942,   54,   55,   19,  235,  232,   44,  977,
 /*   210 */    53,  283,   58,   56,   60,   57,  875, 1038,  215,  278,
 /*   220 */    49,   48,  181,  205,   47,   46,   45,  231,  782,  966,
 /*   230 */   207,  771,   85,  774,  251,  777,  128,  127,  206,  199,
 /*   240 */   231,  782,  255,  254,  771,  239,  774,  974,  777,   33,
 /*   250 */   335,   25,  200,  781,  948,   33,   72,  946,  947,  216,
 /*   260 */   217,  243,  949,  282,  951,  952,  950,   91,  953,  954,
 /*   270 */   980,   79,  216,  217,   24,  980,  334,  333,   39,   76,
 /*   280 */   241,  332,   33,  331,  330,  329,  262,  328,  327,  965,
 /*   290 */   238,  236,   49,   48,  977,   33,   47,   46,   45,   33,
 /*   300 */   976,  258,  968,   71,   58,   56,   60,   57,  884,  213,
 /*   310 */   305,   33,   49,   48,  181,  715,   47,   46,   45,  876,
 /*   320 */   712,    5,   36,  171,  308,  181,    8,  977,  170,   97,
 /*   330 */   102,   93,  101,   87,  676,  245,   59,  309,  312,  311,
 /*   340 */   977,  313,  783,  294,  977,   47,   46,   45,  779,   59,
 /*   350 */   338,  337,  137,  317,  242,  783,  977,  307,  284,   86,
 /*   360 */    13,  779,  315,  314,   90,  778,  244,  143,  141,  140,
 /*   370 */   113,  107,  118,   74,  230,  168,  115,  117,  778,  123,
 /*   380 */   126,  116,  189,  187,  185,  325,  244,  120,  769,  184,
 /*   390 */   132,  131,  130,  129,  700,  978,  260,  697,  719,  698,
 /*   400 */    34,  699,    3,  182,  748,  749,   77,  731,  739,   64,
 /*   410 */   740,  147,   63,  262,   21,   67,  803,  784,  202,   29,
 /*   420 */    20,   20,  289,    6,  770,  246,  247, 1099,  686,  286,
 /*   430 */   196,   65,   34,   34,  688,   68,  288,  687,   63,  979,
 /*   440 */    89,   63,   70,  203,  675,  106,  105,   15,   14, 1048,
 /*   450 */   704,  702,  705,  703,  112,  111,   17,   16,  204,  786,
 /*   460 */   125,  124,  256,  210, 1047,  211,  209,  194,  208,  198,
 /*   470 */   233, 1044,  144, 1043,  234,  316, 1000,   42, 1011, 1008,
 /*   480 */  1030, 1029, 1009,  993,  263, 1013,  146,  150,  272,  163,
 /*   490 */   142,  975,  295,  267,  888,  225,  269,  276,  164,  156,
 /*   500 */   154,  990,  730,  153,  155,  157,  158,  973,  701,  273,
 /*   510 */   165,  166,  889,  159,  271,  275,  291,  292,  293,   69,
 /*   520 */   296,   61,   66,  297,   40,  281,  192,   37,  279,  306,
 /*   530 */   883, 1104,  103, 1103, 1101,  172,  310, 1098,  109,  277,
 /*   540 */  1097, 1095,  173,  909,   38,   35,   41,  193,  872,  119,
 /*   550 */   870,  121,  122,  868,  867,  248,  183,  865,  864,  863,
 /*   560 */   862,  861,  860,  859,  186,  188,  856,  854,  852,  850,
 /*   570 */   190,  847,  191,  268,  261,   75,   80,   43,  270,  326,
 /*   580 */  1031,  114,  318,  319,  320,  321,  322,  323,  324,  336,
 /*   590 */   823,  218,  249,  250,  822,  240,  290,  253,  252,  821,
 /*   600 */   809,  219,  212,  257,   98,   99,  887,  808,   78,  262,
 /*   610 */   707,  285,    9,  866,  732,  133,  134,  858,  857,  176,
 /*   620 */   135,  175,  910,  174,  178,  177,  179,  180,  136,  849,
 /*   630 */   848,    2,   26,  944,    4,  264,   81,  148,  160,  161,
 /*   640 */   162,  735,  227,  956,  149,   82,   10,  737,   83,  274,
 /*   650 */   787,  741,  151,   11,   92,  785,   27,    7,   28,   12,
 /*   660 */    22,  287,   23,   31,   94,   90,   95,  607,   32,   96,
 /*   670 */   639,  635,  633,  632,  631,  628,  100,   34,  597,  304,
 /*   680 */   678,   62,  104,  677,  674,  623,  621,  613,  619,  615,
 /*   690 */   108,  110,  617,  611,  609,  642,  641,  640,  638,  637,
 /*   700 */   636,  634,  630,  629,  595,   63,  138,  139,  561,  559,
 /*   710 */   827,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   259,    1,  240,    1,  259,  196,  240,    1,  202,    9,
 /*    10 */   196,    9,  259,   13,   14,    9,   16,   17,  256,  259,
 /*    20 */    20,   21,  256,   23,   24,   25,   26,   27,   28,  269,
 /*    30 */   270,  195,  196,   33,   34,  193,  194,   37,   38,   39,
 /*    40 */   234,  235,  236,  237,    5,  196,    7,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  217,  217,   62,   13,   14,  196,   16,   17,
 /*    70 */   261,  262,   20,   21,  260,   23,   24,   25,   26,   27,
 /*    80 */    28,  259,  240,   83,   81,   33,   34,  242,  242,   37,
 /*    90 */    38,   39,   13,   14,   87,   16,   17,  259,  256,   20,
 /*   100 */    21,  196,   23,   24,   25,   26,   27,   28,  270,  196,
 /*   110 */   205,  259,   33,   34,  259,  266,   37,   38,   39,   13,
 /*   120 */    14,  110,   16,   17,  269,  270,   20,   21,  117,   23,
 /*   130 */    24,   25,   26,   27,   28,  264,    5,  266,    7,   33,
 /*   140 */    34,  259,    1,   37,   38,   39,   14,  196,   16,   17,
 /*   150 */     9,  238,   20,   21,  241,   23,   24,   25,   26,   27,
 /*   160 */    28,  122,  196,  111,  196,   33,   34,  203,  204,   37,
 /*   170 */    38,   39,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   180 */   102,  103,  104,  105,  106,  107,  108,  216,  259,  218,
 /*   190 */   219,  220,  221,  222,  223,  224,  225,  226,  227,  228,
 /*   200 */   229,  230,  231,   16,   17,   44,  238,   20,   21,  241,
 /*   210 */    23,   24,   25,   26,   27,   28,  201,  266,  259,  268,
 /*   220 */    33,   34,  207,   62,   37,   38,   39,    1,    2,    0,
 /*   230 */    69,    5,  266,    7,  141,    9,   75,   76,   77,  259,
 /*   240 */     1,    2,  149,  150,    5,  217,    7,  196,    9,  196,
 /*   250 */   217,  110,  259,  122,  216,  196,  202,  219,  220,   33,
 /*   260 */    34,   69,  224,   37,  226,  227,  228,  202,  230,  231,
 /*   270 */   242,  110,   33,   34,   94,  242,   96,   97,  117,  111,
 /*   280 */    69,  101,  196,  103,  104,  105,  118,  107,  108,  235,
 /*   290 */   239,  238,   33,   34,  241,  196,   37,   38,   39,  196,
 /*   300 */   241,  140,  237,  142,   25,   26,   27,   28,  201,  148,
 /*   310 */    81,  196,   33,   34,  207,   37,   37,   38,   39,  201,
 /*   320 */   115,   63,   64,   65,  238,  207,  121,  241,   70,   71,
 /*   330 */    72,   73,   74,   83,    5,  143,  110,  238,  146,  147,
 /*   340 */   241,  238,  116,   85,  241,   37,   38,   39,  122,  110,
 /*   350 */    66,   67,   68,  238,  143,  116,  241,  146,   15,  243,
 /*   360 */   110,  122,   33,   34,  114,  139,  196,   63,   64,   65,
 /*   370 */    63,   64,   65,  257,   61,  205,   78,   70,  139,   72,
 /*   380 */    73,   74,   63,   64,   65,   87,  196,   80,    1,   70,
 /*   390 */    71,   72,   73,   74,    2,  205,  111,    5,  120,    7,
 /*   400 */   115,    9,  199,  200,  130,  131,  111,  111,  111,  115,
 /*   410 */   111,  115,  115,  118,  115,  115,  111,  111,  259,  110,
 /*   420 */   115,  115,  113,  110,   37,   33,   34,  242,  111,  111,
 /*   430 */   259,  137,  115,  115,  111,  135,  111,  111,  115,  242,
 /*   440 */   115,  115,  110,  259,  112,  144,  145,  144,  145,  233,
 /*   450 */     5,    5,    7,    7,  144,  145,  144,  145,  259,  116,
 /*   460 */    78,   79,  196,  259,  233,  259,  259,  259,  259,  259,
 /*   470 */   233,  233,  196,  233,  233,  233,  196,  258,  196,  196,
 /*   480 */   267,  267,  196,  240,  240,  196,  196,  196,  196,  244,
 /*   490 */    61,  240,   86,  263,  206,  263,  263,  263,  196,  251,
 /*   500 */   253,  255,  122,  254,  252,  250,  249,  196,  116,  125,
 /*   510 */   196,  196,  196,  248,  124,  126,  196,  196,  196,  134,
 /*   520 */   196,  133,  136,  196,  196,  128,  196,  196,  132,  196,
 /*   530 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  127,
 /*   540 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   550 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   560 */   196,  196,  196,  196,  196,  196,  196,  196,  196,  196,
 /*   570 */   196,  196,  196,  123,  197,  197,  197,  138,  197,  109,
 /*   580 */   197,   93,   92,   51,   89,   91,   55,   90,   88,   81,
 /*   590 */     5,  197,  151,    5,    5,  197,  197,    5,  151,    5,
 /*   600 */    96,  197,  197,  141,  202,  202,  206,   95,  119,  118,
 /*   610 */   111,  113,  110,  197,  111,  198,  198,  197,  197,  209,
 /*   620 */   198,  213,  215,  214,  210,  212,  211,  208,  198,  197,
 /*   630 */   197,  203,  110,  232,  199,  115,  115,  110,  247,  246,
 /*   640 */   245,  111,    1,  232,  115,  110,  129,  111,  110,  110,
 /*   650 */   116,  111,  110,  129,   78,  111,  115,  110,  115,  110,
 /*   660 */   110,  113,  110,   84,   83,  114,   71,    5,   84,   83,
 /*   670 */     9,    5,    5,    5,    5,    5,   78,  115,   82,   15,
 /*   680 */     5,   16,  145,    5,  111,    5,    5,    5,    5,    5,
 /*   690 */   145,  145,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   700 */     5,    5,    5,    5,   82,  115,   21,   21,   61,   60,
 /*   710 */     0,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   720 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   730 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   740 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   750 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   760 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   770 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   780 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   790 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   800 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   810 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   820 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   830 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   840 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   850 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   860 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   870 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   880 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   890 */   271,  271,  271,  271,  271,  271,  271,  271,  271,  271,
 /*   900 */   271,  271,  271,  271,
};
#define YY_SHIFT_COUNT    (341)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (710)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   78,   78,  180,  180,    3,  226,  239,  141,    6,
 /*    10 */     6,    6,    6,    6,    6,    6,    6,    6,    0,    2,
 /*    20 */   239,  392,  392,  392,  392,   11,   11,    6,    6,    6,
 /*    30 */   229,    6,    6,    6,    6,  298,    3,    7,    7,  711,
 /*    40 */   711,  711,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  392,  392,  329,  329,  329,  329,  329,  329,
 /*    70 */   329,    6,    6,    6,  278,    6,    6,    6,   11,   11,
 /*    80 */     6,    6,    6,    6,  274,  274,  205,   11,    6,    6,
 /*    90 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   100 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   110 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   120 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   130 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   140 */     6,    6,    6,    6,  429,  429,  429,  380,  380,  380,
 /*   150 */   429,  380,  429,  385,  386,  388,  397,  396,  412,  389,
 /*   160 */   384,  390,  450,  439,  429,  429,  429,  406,  406,  470,
 /*   170 */     3,    3,  429,  429,  488,  490,  532,  495,  494,  531,
 /*   180 */   497,  500,  470,  429,  508,  508,  429,  508,  429,  508,
 /*   190 */   429,  429,  711,  711,   52,   79,  106,   79,   79,  132,
 /*   200 */   187,  279,  279,  279,  279,  258,  307,  319,  259,  259,
 /*   210 */   259,  259,  192,   93,  308,  308,   39,  131,  250,  211,
 /*   220 */   284,  304,  285,  168,  295,  296,  297,  299,  305,  306,
 /*   230 */   387,  313,  343,  294,  300,  317,  318,  323,  325,  326,
 /*   240 */   309,  301,  303,  310,  332,  312,  445,  446,  382,  585,
 /*   250 */   441,  588,  589,  447,  592,  594,  504,  512,  462,  491,
 /*   260 */   498,  502,  489,  499,  522,  520,  521,  503,  527,  530,
 /*   270 */   529,  535,  536,  538,  641,  539,  540,  542,  541,  517,
 /*   280 */   543,  524,  544,  547,  534,  549,  498,  550,  548,  552,
 /*   290 */   551,  576,  579,  581,  595,  662,  584,  586,  661,  666,
 /*   300 */   667,  668,  669,  670,  596,  664,  598,  537,  562,  562,
 /*   310 */   665,  545,  546,  562,  675,  678,  573,  562,  680,  681,
 /*   320 */   682,  683,  684,  687,  688,  689,  690,  691,  692,  693,
 /*   330 */   694,  695,  696,  697,  698,  590,  622,  685,  686,  647,
 /*   340 */   649,  710,
};
#define YY_REDUCE_COUNT (193)
#define YY_REDUCE_MIN   (-259)
#define YY_REDUCE_MAX   (435)
static const short yy_reduce_ofst[] = {
 /*     0 */  -158,  -29,  -29,   38,   38, -194, -240, -145, -191,  -87,
 /*    10 */   -49, -129,  -32,   53,   86,   99,  103,  115, -186, -164,
 /*    20 */  -162, -155, -154,   28,   33, -238, -234, -151,  -34,   51,
 /*    30 */    65,  -95,  170,  190,   59,   15,   54,  107,  118,  116,
 /*    40 */   -36,  203, -259, -255, -247, -178, -148, -118,  -71,  -41,
 /*    50 */   -20,   -7,  159,  171,  184,  199,  204,  206,  207,  208,
 /*    60 */   209,  210,  185,  197,  216,  231,  237,  238,  240,  241,
 /*    70 */   242,  266,  276,  280,  219,  282,  283,  286,  243,  244,
 /*    80 */   289,  290,  291,  292,  213,  214,  245,  251,  302,  311,
 /*    90 */   314,  315,  316,  320,  321,  322,  324,  327,  328,  330,
 /*   100 */   331,  333,  334,  335,  336,  337,  338,  339,  340,  341,
 /*   110 */   342,  344,  345,  346,  347,  348,  349,  350,  351,  352,
 /*   120 */   353,  354,  355,  356,  357,  358,  359,  360,  361,  362,
 /*   130 */   363,  364,  365,  366,  367,  368,  369,  370,  371,  372,
 /*   140 */   373,  374,  375,  376,  377,  378,  379,  230,  232,  233,
 /*   150 */   381,  234,  383,  246,  249,  247,  252,  248,  255,  257,
 /*   160 */   265,  391,  393,  395,  394,  398,  399,  288,  400,  401,
 /*   170 */   402,  403,  404,  405,  407,  409,  408,  410,  413,  414,
 /*   180 */   415,  419,  411,  416,  417,  418,  420,  422,  421,  430,
 /*   190 */   432,  433,  428,  435,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   824,  943,  885,  955,  873,  882, 1091, 1091,  824,  824,
 /*    10 */   824,  824,  824,  824,  824,  824,  824,  824, 1002,  844,
 /*    20 */  1091,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*    30 */   882,  824,  824,  824,  824,  892,  882,  892,  892,  997,
 /*    40 */   927,  945,  824,  824,  824,  824,  824,  824,  824,  824,
 /*    50 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*    60 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*    70 */   824,  824,  824,  824, 1004, 1010, 1007,  824,  824,  824,
 /*    80 */  1012,  824,  824,  824, 1034, 1034,  995,  824,  824,  824,
 /*    90 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   100 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   110 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  871,
 /*   120 */   824,  869,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   130 */   824,  824,  824,  824,  824,  824,  824,  855,  824,  824,
 /*   140 */   824,  824,  824,  824,  846,  846,  846,  824,  824,  824,
 /*   150 */   846,  824,  846, 1041, 1045, 1039, 1027, 1035, 1026, 1022,
 /*   160 */  1020, 1018, 1017, 1049,  846,  846,  846,  890,  890,  886,
 /*   170 */   882,  882,  846,  846,  908,  906,  904,  896,  902,  898,
 /*   180 */   900,  894,  874,  846,  880,  880,  846,  880,  846,  880,
 /*   190 */   846,  846,  927,  945,  824, 1050,  824, 1090, 1040, 1080,
 /*   200 */  1079, 1086, 1078, 1077, 1076,  824,  824,  824, 1072, 1073,
 /*   210 */  1075, 1074,  824,  824, 1082, 1081,  824,  824,  824,  824,
 /*   220 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   230 */   824, 1052,  824, 1046, 1042,  824,  824,  824,  824,  824,
 /*   240 */   824,  824,  824,  824,  957,  824,  824,  824,  824,  824,
 /*   250 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  994,
 /*   260 */   824,  824,  824,  824,  824, 1006, 1005,  824,  824,  824,
 /*   270 */   824,  824,  824,  824,  824,  824,  824,  824, 1036,  824,
 /*   280 */  1028,  824,  824,  824,  824,  824,  969,  824,  824,  824,
 /*   290 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   300 */   824,  824,  824,  824,  824,  824,  824,  824, 1102, 1100,
 /*   310 */   824,  824,  824, 1096,  824,  824,  824, 1094,  824,  824,
 /*   320 */   824,  824,  824,  824,  824,  824,  824,  824,  824,  824,
 /*   330 */   824,  824,  824,  824,  824,  911,  824,  853,  851,  824,
 /*   340 */   842,  824,
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
    0,  /* STATE_WINDOW => nothing */
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
  /*  125 */ "STATE_WINDOW",
  /*  126 */ "FILL",
  /*  127 */ "SLIDING",
  /*  128 */ "ORDER",
  /*  129 */ "BY",
  /*  130 */ "ASC",
  /*  131 */ "DESC",
  /*  132 */ "GROUP",
  /*  133 */ "HAVING",
  /*  134 */ "LIMIT",
  /*  135 */ "OFFSET",
  /*  136 */ "SLIMIT",
  /*  137 */ "SOFFSET",
  /*  138 */ "WHERE",
  /*  139 */ "NOW",
  /*  140 */ "RESET",
  /*  141 */ "QUERY",
  /*  142 */ "SYNCDB",
  /*  143 */ "ADD",
  /*  144 */ "COLUMN",
  /*  145 */ "TAG",
  /*  146 */ "CHANGE",
  /*  147 */ "SET",
  /*  148 */ "KILL",
  /*  149 */ "CONNECTION",
  /*  150 */ "STREAM",
  /*  151 */ "COLON",
  /*  152 */ "ABORT",
  /*  153 */ "AFTER",
  /*  154 */ "ATTACH",
  /*  155 */ "BEFORE",
  /*  156 */ "BEGIN",
  /*  157 */ "CASCADE",
  /*  158 */ "CLUSTER",
  /*  159 */ "CONFLICT",
  /*  160 */ "COPY",
  /*  161 */ "DEFERRED",
  /*  162 */ "DELIMITERS",
  /*  163 */ "DETACH",
  /*  164 */ "EACH",
  /*  165 */ "END",
  /*  166 */ "EXPLAIN",
  /*  167 */ "FAIL",
  /*  168 */ "FOR",
  /*  169 */ "IGNORE",
  /*  170 */ "IMMEDIATE",
  /*  171 */ "INITIALLY",
  /*  172 */ "INSTEAD",
  /*  173 */ "MATCH",
  /*  174 */ "KEY",
  /*  175 */ "OF",
  /*  176 */ "RAISE",
  /*  177 */ "REPLACE",
  /*  178 */ "RESTRICT",
  /*  179 */ "ROW",
  /*  180 */ "STATEMENT",
  /*  181 */ "TRIGGER",
  /*  182 */ "VIEW",
  /*  183 */ "SEMI",
  /*  184 */ "NONE",
  /*  185 */ "PREV",
  /*  186 */ "LINEAR",
  /*  187 */ "IMPORT",
  /*  188 */ "TBNAME",
  /*  189 */ "JOIN",
  /*  190 */ "INSERT",
  /*  191 */ "INTO",
  /*  192 */ "VALUES",
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
  /*  248 */ "windowstate_option",
  /*  249 */ "fill_opt",
  /*  250 */ "sliding_opt",
  /*  251 */ "groupby_opt",
  /*  252 */ "orderby_opt",
  /*  253 */ "having_opt",
  /*  254 */ "slimit_opt",
  /*  255 */ "limit_opt",
  /*  256 */ "union",
  /*  257 */ "sclp",
  /*  258 */ "distinct",
  /*  259 */ "expr",
  /*  260 */ "as",
  /*  261 */ "tablelist",
  /*  262 */ "sub",
  /*  263 */ "tmvar",
  /*  264 */ "sortlist",
  /*  265 */ "sortitem",
  /*  266 */ "item",
  /*  267 */ "sortorder",
  /*  268 */ "grouplist",
  /*  269 */ "exprlist",
  /*  270 */ "expritem",
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
 /* 163 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
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
 /* 193 */ "windowstate_option ::=",
 /* 194 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 195 */ "fill_opt ::=",
 /* 196 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 197 */ "fill_opt ::= FILL LP ID RP",
 /* 198 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 199 */ "sliding_opt ::=",
 /* 200 */ "orderby_opt ::=",
 /* 201 */ "orderby_opt ::= ORDER BY sortlist",
 /* 202 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 203 */ "sortlist ::= item sortorder",
 /* 204 */ "item ::= ids cpxName",
 /* 205 */ "sortorder ::= ASC",
 /* 206 */ "sortorder ::= DESC",
 /* 207 */ "sortorder ::=",
 /* 208 */ "groupby_opt ::=",
 /* 209 */ "groupby_opt ::= GROUP BY grouplist",
 /* 210 */ "grouplist ::= grouplist COMMA item",
 /* 211 */ "grouplist ::= item",
 /* 212 */ "having_opt ::=",
 /* 213 */ "having_opt ::= HAVING expr",
 /* 214 */ "limit_opt ::=",
 /* 215 */ "limit_opt ::= LIMIT signed",
 /* 216 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 217 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 218 */ "slimit_opt ::=",
 /* 219 */ "slimit_opt ::= SLIMIT signed",
 /* 220 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 221 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 222 */ "where_opt ::=",
 /* 223 */ "where_opt ::= WHERE expr",
 /* 224 */ "expr ::= LP expr RP",
 /* 225 */ "expr ::= ID",
 /* 226 */ "expr ::= ID DOT ID",
 /* 227 */ "expr ::= ID DOT STAR",
 /* 228 */ "expr ::= INTEGER",
 /* 229 */ "expr ::= MINUS INTEGER",
 /* 230 */ "expr ::= PLUS INTEGER",
 /* 231 */ "expr ::= FLOAT",
 /* 232 */ "expr ::= MINUS FLOAT",
 /* 233 */ "expr ::= PLUS FLOAT",
 /* 234 */ "expr ::= STRING",
 /* 235 */ "expr ::= NOW",
 /* 236 */ "expr ::= VARIABLE",
 /* 237 */ "expr ::= PLUS VARIABLE",
 /* 238 */ "expr ::= MINUS VARIABLE",
 /* 239 */ "expr ::= BOOL",
 /* 240 */ "expr ::= NULL",
 /* 241 */ "expr ::= ID LP exprlist RP",
 /* 242 */ "expr ::= ID LP STAR RP",
 /* 243 */ "expr ::= expr IS NULL",
 /* 244 */ "expr ::= expr IS NOT NULL",
 /* 245 */ "expr ::= expr LT expr",
 /* 246 */ "expr ::= expr GT expr",
 /* 247 */ "expr ::= expr LE expr",
 /* 248 */ "expr ::= expr GE expr",
 /* 249 */ "expr ::= expr NE expr",
 /* 250 */ "expr ::= expr EQ expr",
 /* 251 */ "expr ::= expr BETWEEN expr AND expr",
 /* 252 */ "expr ::= expr AND expr",
 /* 253 */ "expr ::= expr OR expr",
 /* 254 */ "expr ::= expr PLUS expr",
 /* 255 */ "expr ::= expr MINUS expr",
 /* 256 */ "expr ::= expr STAR expr",
 /* 257 */ "expr ::= expr SLASH expr",
 /* 258 */ "expr ::= expr REM expr",
 /* 259 */ "expr ::= expr LIKE expr",
 /* 260 */ "expr ::= expr IN LP exprlist RP",
 /* 261 */ "exprlist ::= exprlist COMMA expritem",
 /* 262 */ "exprlist ::= expritem",
 /* 263 */ "expritem ::= expr",
 /* 264 */ "expritem ::=",
 /* 265 */ "cmd ::= RESET QUERY CACHE",
 /* 266 */ "cmd ::= SYNCDB ids REPLICA",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 278 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 279 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 280 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 216: /* keep */
    case 217: /* tagitemlist */
    case 238: /* columnlist */
    case 239: /* tagNamelist */
    case 249: /* fill_opt */
    case 251: /* groupby_opt */
    case 252: /* orderby_opt */
    case 264: /* sortlist */
    case 268: /* grouplist */
{
taosArrayDestroy((yypminor->yy347));
}
      break;
    case 236: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy360));
}
      break;
    case 240: /* select */
{
destroySqlNode((yypminor->yy542));
}
      break;
    case 243: /* selcollist */
    case 257: /* sclp */
    case 269: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy347));
}
      break;
    case 244: /* from */
    case 261: /* tablelist */
    case 262: /* sub */
{
destroyRelationInfo((yypminor->yy120));
}
      break;
    case 245: /* where_opt */
    case 253: /* having_opt */
    case 259: /* expr */
    case 270: /* expritem */
{
tSqlExprDestroy((yypminor->yy42));
}
      break;
    case 256: /* union */
{
destroyAllSqlNode((yypminor->yy347));
}
      break;
    case 265: /* sortitem */
{
tVariantDestroy(&(yypminor->yy312));
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
   193,  /* (0) program ::= cmd */
   194,  /* (1) cmd ::= SHOW DATABASES */
   194,  /* (2) cmd ::= SHOW TOPICS */
   194,  /* (3) cmd ::= SHOW FUNCTIONS */
   194,  /* (4) cmd ::= SHOW MNODES */
   194,  /* (5) cmd ::= SHOW DNODES */
   194,  /* (6) cmd ::= SHOW ACCOUNTS */
   194,  /* (7) cmd ::= SHOW USERS */
   194,  /* (8) cmd ::= SHOW MODULES */
   194,  /* (9) cmd ::= SHOW QUERIES */
   194,  /* (10) cmd ::= SHOW CONNECTIONS */
   194,  /* (11) cmd ::= SHOW STREAMS */
   194,  /* (12) cmd ::= SHOW VARIABLES */
   194,  /* (13) cmd ::= SHOW SCORES */
   194,  /* (14) cmd ::= SHOW GRANTS */
   194,  /* (15) cmd ::= SHOW VNODES */
   194,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   195,  /* (17) dbPrefix ::= */
   195,  /* (18) dbPrefix ::= ids DOT */
   197,  /* (19) cpxName ::= */
   197,  /* (20) cpxName ::= DOT ids */
   194,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   194,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   194,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   194,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   194,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   194,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   194,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   194,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   194,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   194,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   194,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   194,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   194,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   194,  /* (34) cmd ::= DROP FUNCTION ids */
   194,  /* (35) cmd ::= DROP DNODE ids */
   194,  /* (36) cmd ::= DROP USER ids */
   194,  /* (37) cmd ::= DROP ACCOUNT ids */
   194,  /* (38) cmd ::= USE ids */
   194,  /* (39) cmd ::= DESCRIBE ids cpxName */
   194,  /* (40) cmd ::= ALTER USER ids PASS ids */
   194,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   194,  /* (42) cmd ::= ALTER DNODE ids ids */
   194,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   194,  /* (44) cmd ::= ALTER LOCAL ids */
   194,  /* (45) cmd ::= ALTER LOCAL ids ids */
   194,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   194,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   194,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   194,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   196,  /* (50) ids ::= ID */
   196,  /* (51) ids ::= STRING */
   198,  /* (52) ifexists ::= IF EXISTS */
   198,  /* (53) ifexists ::= */
   202,  /* (54) ifnotexists ::= IF NOT EXISTS */
   202,  /* (55) ifnotexists ::= */
   194,  /* (56) cmd ::= CREATE DNODE ids */
   194,  /* (57) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   194,  /* (58) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   194,  /* (59) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   194,  /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   194,  /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   194,  /* (62) cmd ::= CREATE USER ids PASS ids */
   206,  /* (63) bufsize ::= */
   206,  /* (64) bufsize ::= BUFSIZE INTEGER */
   207,  /* (65) pps ::= */
   207,  /* (66) pps ::= PPS INTEGER */
   208,  /* (67) tseries ::= */
   208,  /* (68) tseries ::= TSERIES INTEGER */
   209,  /* (69) dbs ::= */
   209,  /* (70) dbs ::= DBS INTEGER */
   210,  /* (71) streams ::= */
   210,  /* (72) streams ::= STREAMS INTEGER */
   211,  /* (73) storage ::= */
   211,  /* (74) storage ::= STORAGE INTEGER */
   212,  /* (75) qtime ::= */
   212,  /* (76) qtime ::= QTIME INTEGER */
   213,  /* (77) users ::= */
   213,  /* (78) users ::= USERS INTEGER */
   214,  /* (79) conns ::= */
   214,  /* (80) conns ::= CONNS INTEGER */
   215,  /* (81) state ::= */
   215,  /* (82) state ::= STATE ids */
   201,  /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   216,  /* (84) keep ::= KEEP tagitemlist */
   218,  /* (85) cache ::= CACHE INTEGER */
   219,  /* (86) replica ::= REPLICA INTEGER */
   220,  /* (87) quorum ::= QUORUM INTEGER */
   221,  /* (88) days ::= DAYS INTEGER */
   222,  /* (89) minrows ::= MINROWS INTEGER */
   223,  /* (90) maxrows ::= MAXROWS INTEGER */
   224,  /* (91) blocks ::= BLOCKS INTEGER */
   225,  /* (92) ctime ::= CTIME INTEGER */
   226,  /* (93) wal ::= WAL INTEGER */
   227,  /* (94) fsync ::= FSYNC INTEGER */
   228,  /* (95) comp ::= COMP INTEGER */
   229,  /* (96) prec ::= PRECISION STRING */
   230,  /* (97) update ::= UPDATE INTEGER */
   231,  /* (98) cachelast ::= CACHELAST INTEGER */
   232,  /* (99) partitions ::= PARTITIONS INTEGER */
   203,  /* (100) db_optr ::= */
   203,  /* (101) db_optr ::= db_optr cache */
   203,  /* (102) db_optr ::= db_optr replica */
   203,  /* (103) db_optr ::= db_optr quorum */
   203,  /* (104) db_optr ::= db_optr days */
   203,  /* (105) db_optr ::= db_optr minrows */
   203,  /* (106) db_optr ::= db_optr maxrows */
   203,  /* (107) db_optr ::= db_optr blocks */
   203,  /* (108) db_optr ::= db_optr ctime */
   203,  /* (109) db_optr ::= db_optr wal */
   203,  /* (110) db_optr ::= db_optr fsync */
   203,  /* (111) db_optr ::= db_optr comp */
   203,  /* (112) db_optr ::= db_optr prec */
   203,  /* (113) db_optr ::= db_optr keep */
   203,  /* (114) db_optr ::= db_optr update */
   203,  /* (115) db_optr ::= db_optr cachelast */
   204,  /* (116) topic_optr ::= db_optr */
   204,  /* (117) topic_optr ::= topic_optr partitions */
   199,  /* (118) alter_db_optr ::= */
   199,  /* (119) alter_db_optr ::= alter_db_optr replica */
   199,  /* (120) alter_db_optr ::= alter_db_optr quorum */
   199,  /* (121) alter_db_optr ::= alter_db_optr keep */
   199,  /* (122) alter_db_optr ::= alter_db_optr blocks */
   199,  /* (123) alter_db_optr ::= alter_db_optr comp */
   199,  /* (124) alter_db_optr ::= alter_db_optr wal */
   199,  /* (125) alter_db_optr ::= alter_db_optr fsync */
   199,  /* (126) alter_db_optr ::= alter_db_optr update */
   199,  /* (127) alter_db_optr ::= alter_db_optr cachelast */
   200,  /* (128) alter_topic_optr ::= alter_db_optr */
   200,  /* (129) alter_topic_optr ::= alter_topic_optr partitions */
   205,  /* (130) typename ::= ids */
   205,  /* (131) typename ::= ids LP signed RP */
   205,  /* (132) typename ::= ids UNSIGNED */
   233,  /* (133) signed ::= INTEGER */
   233,  /* (134) signed ::= PLUS INTEGER */
   233,  /* (135) signed ::= MINUS INTEGER */
   194,  /* (136) cmd ::= CREATE TABLE create_table_args */
   194,  /* (137) cmd ::= CREATE TABLE create_stable_args */
   194,  /* (138) cmd ::= CREATE STABLE create_stable_args */
   194,  /* (139) cmd ::= CREATE TABLE create_table_list */
   236,  /* (140) create_table_list ::= create_from_stable */
   236,  /* (141) create_table_list ::= create_table_list create_from_stable */
   234,  /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   235,  /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   237,  /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   237,  /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   239,  /* (146) tagNamelist ::= tagNamelist COMMA ids */
   239,  /* (147) tagNamelist ::= ids */
   234,  /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
   238,  /* (149) columnlist ::= columnlist COMMA column */
   238,  /* (150) columnlist ::= column */
   241,  /* (151) column ::= ids typename */
   217,  /* (152) tagitemlist ::= tagitemlist COMMA tagitem */
   217,  /* (153) tagitemlist ::= tagitem */
   242,  /* (154) tagitem ::= INTEGER */
   242,  /* (155) tagitem ::= FLOAT */
   242,  /* (156) tagitem ::= STRING */
   242,  /* (157) tagitem ::= BOOL */
   242,  /* (158) tagitem ::= NULL */
   242,  /* (159) tagitem ::= MINUS INTEGER */
   242,  /* (160) tagitem ::= MINUS FLOAT */
   242,  /* (161) tagitem ::= PLUS INTEGER */
   242,  /* (162) tagitem ::= PLUS FLOAT */
   240,  /* (163) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   240,  /* (164) select ::= LP select RP */
   256,  /* (165) union ::= select */
   256,  /* (166) union ::= union UNION ALL select */
   194,  /* (167) cmd ::= union */
   240,  /* (168) select ::= SELECT selcollist */
   257,  /* (169) sclp ::= selcollist COMMA */
   257,  /* (170) sclp ::= */
   243,  /* (171) selcollist ::= sclp distinct expr as */
   243,  /* (172) selcollist ::= sclp STAR */
   260,  /* (173) as ::= AS ids */
   260,  /* (174) as ::= ids */
   260,  /* (175) as ::= */
   258,  /* (176) distinct ::= DISTINCT */
   258,  /* (177) distinct ::= */
   244,  /* (178) from ::= FROM tablelist */
   244,  /* (179) from ::= FROM sub */
   262,  /* (180) sub ::= LP union RP */
   262,  /* (181) sub ::= LP union RP ids */
   262,  /* (182) sub ::= sub COMMA LP union RP ids */
   261,  /* (183) tablelist ::= ids cpxName */
   261,  /* (184) tablelist ::= ids cpxName ids */
   261,  /* (185) tablelist ::= tablelist COMMA ids cpxName */
   261,  /* (186) tablelist ::= tablelist COMMA ids cpxName ids */
   263,  /* (187) tmvar ::= VARIABLE */
   246,  /* (188) interval_opt ::= INTERVAL LP tmvar RP */
   246,  /* (189) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   246,  /* (190) interval_opt ::= */
   247,  /* (191) session_option ::= */
   247,  /* (192) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   248,  /* (193) windowstate_option ::= */
   248,  /* (194) windowstate_option ::= STATE_WINDOW LP ids RP */
   249,  /* (195) fill_opt ::= */
   249,  /* (196) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   249,  /* (197) fill_opt ::= FILL LP ID RP */
   250,  /* (198) sliding_opt ::= SLIDING LP tmvar RP */
   250,  /* (199) sliding_opt ::= */
   252,  /* (200) orderby_opt ::= */
   252,  /* (201) orderby_opt ::= ORDER BY sortlist */
   264,  /* (202) sortlist ::= sortlist COMMA item sortorder */
   264,  /* (203) sortlist ::= item sortorder */
   266,  /* (204) item ::= ids cpxName */
   267,  /* (205) sortorder ::= ASC */
   267,  /* (206) sortorder ::= DESC */
   267,  /* (207) sortorder ::= */
   251,  /* (208) groupby_opt ::= */
   251,  /* (209) groupby_opt ::= GROUP BY grouplist */
   268,  /* (210) grouplist ::= grouplist COMMA item */
   268,  /* (211) grouplist ::= item */
   253,  /* (212) having_opt ::= */
   253,  /* (213) having_opt ::= HAVING expr */
   255,  /* (214) limit_opt ::= */
   255,  /* (215) limit_opt ::= LIMIT signed */
   255,  /* (216) limit_opt ::= LIMIT signed OFFSET signed */
   255,  /* (217) limit_opt ::= LIMIT signed COMMA signed */
   254,  /* (218) slimit_opt ::= */
   254,  /* (219) slimit_opt ::= SLIMIT signed */
   254,  /* (220) slimit_opt ::= SLIMIT signed SOFFSET signed */
   254,  /* (221) slimit_opt ::= SLIMIT signed COMMA signed */
   245,  /* (222) where_opt ::= */
   245,  /* (223) where_opt ::= WHERE expr */
   259,  /* (224) expr ::= LP expr RP */
   259,  /* (225) expr ::= ID */
   259,  /* (226) expr ::= ID DOT ID */
   259,  /* (227) expr ::= ID DOT STAR */
   259,  /* (228) expr ::= INTEGER */
   259,  /* (229) expr ::= MINUS INTEGER */
   259,  /* (230) expr ::= PLUS INTEGER */
   259,  /* (231) expr ::= FLOAT */
   259,  /* (232) expr ::= MINUS FLOAT */
   259,  /* (233) expr ::= PLUS FLOAT */
   259,  /* (234) expr ::= STRING */
   259,  /* (235) expr ::= NOW */
   259,  /* (236) expr ::= VARIABLE */
   259,  /* (237) expr ::= PLUS VARIABLE */
   259,  /* (238) expr ::= MINUS VARIABLE */
   259,  /* (239) expr ::= BOOL */
   259,  /* (240) expr ::= NULL */
   259,  /* (241) expr ::= ID LP exprlist RP */
   259,  /* (242) expr ::= ID LP STAR RP */
   259,  /* (243) expr ::= expr IS NULL */
   259,  /* (244) expr ::= expr IS NOT NULL */
   259,  /* (245) expr ::= expr LT expr */
   259,  /* (246) expr ::= expr GT expr */
   259,  /* (247) expr ::= expr LE expr */
   259,  /* (248) expr ::= expr GE expr */
   259,  /* (249) expr ::= expr NE expr */
   259,  /* (250) expr ::= expr EQ expr */
   259,  /* (251) expr ::= expr BETWEEN expr AND expr */
   259,  /* (252) expr ::= expr AND expr */
   259,  /* (253) expr ::= expr OR expr */
   259,  /* (254) expr ::= expr PLUS expr */
   259,  /* (255) expr ::= expr MINUS expr */
   259,  /* (256) expr ::= expr STAR expr */
   259,  /* (257) expr ::= expr SLASH expr */
   259,  /* (258) expr ::= expr REM expr */
   259,  /* (259) expr ::= expr LIKE expr */
   259,  /* (260) expr ::= expr IN LP exprlist RP */
   269,  /* (261) exprlist ::= exprlist COMMA expritem */
   269,  /* (262) exprlist ::= expritem */
   270,  /* (263) expritem ::= expr */
   270,  /* (264) expritem ::= */
   194,  /* (265) cmd ::= RESET QUERY CACHE */
   194,  /* (266) cmd ::= SYNCDB ids REPLICA */
   194,  /* (267) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   194,  /* (268) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   194,  /* (269) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   194,  /* (270) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   194,  /* (271) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   194,  /* (272) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   194,  /* (273) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   194,  /* (274) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   194,  /* (275) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   194,  /* (276) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   194,  /* (277) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   194,  /* (278) cmd ::= KILL CONNECTION INTEGER */
   194,  /* (279) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   194,  /* (280) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -5,  /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
   -4,  /* (23) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (24) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (28) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (32) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (33) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (34) cmd ::= DROP FUNCTION ids */
   -3,  /* (35) cmd ::= DROP DNODE ids */
   -3,  /* (36) cmd ::= DROP USER ids */
   -3,  /* (37) cmd ::= DROP ACCOUNT ids */
   -2,  /* (38) cmd ::= USE ids */
   -3,  /* (39) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (40) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (42) cmd ::= ALTER DNODE ids ids */
   -5,  /* (43) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (44) cmd ::= ALTER LOCAL ids */
   -4,  /* (45) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (50) ids ::= ID */
   -1,  /* (51) ids ::= STRING */
   -2,  /* (52) ifexists ::= IF EXISTS */
    0,  /* (53) ifexists ::= */
   -3,  /* (54) ifnotexists ::= IF NOT EXISTS */
    0,  /* (55) ifnotexists ::= */
   -3,  /* (56) cmd ::= CREATE DNODE ids */
   -6,  /* (57) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (58) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (59) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -8,  /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (62) cmd ::= CREATE USER ids PASS ids */
    0,  /* (63) bufsize ::= */
   -2,  /* (64) bufsize ::= BUFSIZE INTEGER */
    0,  /* (65) pps ::= */
   -2,  /* (66) pps ::= PPS INTEGER */
    0,  /* (67) tseries ::= */
   -2,  /* (68) tseries ::= TSERIES INTEGER */
    0,  /* (69) dbs ::= */
   -2,  /* (70) dbs ::= DBS INTEGER */
    0,  /* (71) streams ::= */
   -2,  /* (72) streams ::= STREAMS INTEGER */
    0,  /* (73) storage ::= */
   -2,  /* (74) storage ::= STORAGE INTEGER */
    0,  /* (75) qtime ::= */
   -2,  /* (76) qtime ::= QTIME INTEGER */
    0,  /* (77) users ::= */
   -2,  /* (78) users ::= USERS INTEGER */
    0,  /* (79) conns ::= */
   -2,  /* (80) conns ::= CONNS INTEGER */
    0,  /* (81) state ::= */
   -2,  /* (82) state ::= STATE ids */
   -9,  /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (84) keep ::= KEEP tagitemlist */
   -2,  /* (85) cache ::= CACHE INTEGER */
   -2,  /* (86) replica ::= REPLICA INTEGER */
   -2,  /* (87) quorum ::= QUORUM INTEGER */
   -2,  /* (88) days ::= DAYS INTEGER */
   -2,  /* (89) minrows ::= MINROWS INTEGER */
   -2,  /* (90) maxrows ::= MAXROWS INTEGER */
   -2,  /* (91) blocks ::= BLOCKS INTEGER */
   -2,  /* (92) ctime ::= CTIME INTEGER */
   -2,  /* (93) wal ::= WAL INTEGER */
   -2,  /* (94) fsync ::= FSYNC INTEGER */
   -2,  /* (95) comp ::= COMP INTEGER */
   -2,  /* (96) prec ::= PRECISION STRING */
   -2,  /* (97) update ::= UPDATE INTEGER */
   -2,  /* (98) cachelast ::= CACHELAST INTEGER */
   -2,  /* (99) partitions ::= PARTITIONS INTEGER */
    0,  /* (100) db_optr ::= */
   -2,  /* (101) db_optr ::= db_optr cache */
   -2,  /* (102) db_optr ::= db_optr replica */
   -2,  /* (103) db_optr ::= db_optr quorum */
   -2,  /* (104) db_optr ::= db_optr days */
   -2,  /* (105) db_optr ::= db_optr minrows */
   -2,  /* (106) db_optr ::= db_optr maxrows */
   -2,  /* (107) db_optr ::= db_optr blocks */
   -2,  /* (108) db_optr ::= db_optr ctime */
   -2,  /* (109) db_optr ::= db_optr wal */
   -2,  /* (110) db_optr ::= db_optr fsync */
   -2,  /* (111) db_optr ::= db_optr comp */
   -2,  /* (112) db_optr ::= db_optr prec */
   -2,  /* (113) db_optr ::= db_optr keep */
   -2,  /* (114) db_optr ::= db_optr update */
   -2,  /* (115) db_optr ::= db_optr cachelast */
   -1,  /* (116) topic_optr ::= db_optr */
   -2,  /* (117) topic_optr ::= topic_optr partitions */
    0,  /* (118) alter_db_optr ::= */
   -2,  /* (119) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (120) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (121) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (122) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (123) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (124) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (125) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (126) alter_db_optr ::= alter_db_optr update */
   -2,  /* (127) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (128) alter_topic_optr ::= alter_db_optr */
   -2,  /* (129) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (130) typename ::= ids */
   -4,  /* (131) typename ::= ids LP signed RP */
   -2,  /* (132) typename ::= ids UNSIGNED */
   -1,  /* (133) signed ::= INTEGER */
   -2,  /* (134) signed ::= PLUS INTEGER */
   -2,  /* (135) signed ::= MINUS INTEGER */
   -3,  /* (136) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (137) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (138) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (139) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (140) create_table_list ::= create_from_stable */
   -2,  /* (141) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (146) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (147) tagNamelist ::= ids */
   -5,  /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (149) columnlist ::= columnlist COMMA column */
   -1,  /* (150) columnlist ::= column */
   -2,  /* (151) column ::= ids typename */
   -3,  /* (152) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (153) tagitemlist ::= tagitem */
   -1,  /* (154) tagitem ::= INTEGER */
   -1,  /* (155) tagitem ::= FLOAT */
   -1,  /* (156) tagitem ::= STRING */
   -1,  /* (157) tagitem ::= BOOL */
   -1,  /* (158) tagitem ::= NULL */
   -2,  /* (159) tagitem ::= MINUS INTEGER */
   -2,  /* (160) tagitem ::= MINUS FLOAT */
   -2,  /* (161) tagitem ::= PLUS INTEGER */
   -2,  /* (162) tagitem ::= PLUS FLOAT */
  -14,  /* (163) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (164) select ::= LP select RP */
   -1,  /* (165) union ::= select */
   -4,  /* (166) union ::= union UNION ALL select */
   -1,  /* (167) cmd ::= union */
   -2,  /* (168) select ::= SELECT selcollist */
   -2,  /* (169) sclp ::= selcollist COMMA */
    0,  /* (170) sclp ::= */
   -4,  /* (171) selcollist ::= sclp distinct expr as */
   -2,  /* (172) selcollist ::= sclp STAR */
   -2,  /* (173) as ::= AS ids */
   -1,  /* (174) as ::= ids */
    0,  /* (175) as ::= */
   -1,  /* (176) distinct ::= DISTINCT */
    0,  /* (177) distinct ::= */
   -2,  /* (178) from ::= FROM tablelist */
   -2,  /* (179) from ::= FROM sub */
   -3,  /* (180) sub ::= LP union RP */
   -4,  /* (181) sub ::= LP union RP ids */
   -6,  /* (182) sub ::= sub COMMA LP union RP ids */
   -2,  /* (183) tablelist ::= ids cpxName */
   -3,  /* (184) tablelist ::= ids cpxName ids */
   -4,  /* (185) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (186) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (187) tmvar ::= VARIABLE */
   -4,  /* (188) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (189) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (190) interval_opt ::= */
    0,  /* (191) session_option ::= */
   -7,  /* (192) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (193) windowstate_option ::= */
   -4,  /* (194) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (195) fill_opt ::= */
   -6,  /* (196) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (197) fill_opt ::= FILL LP ID RP */
   -4,  /* (198) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (199) sliding_opt ::= */
    0,  /* (200) orderby_opt ::= */
   -3,  /* (201) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (202) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (203) sortlist ::= item sortorder */
   -2,  /* (204) item ::= ids cpxName */
   -1,  /* (205) sortorder ::= ASC */
   -1,  /* (206) sortorder ::= DESC */
    0,  /* (207) sortorder ::= */
    0,  /* (208) groupby_opt ::= */
   -3,  /* (209) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (210) grouplist ::= grouplist COMMA item */
   -1,  /* (211) grouplist ::= item */
    0,  /* (212) having_opt ::= */
   -2,  /* (213) having_opt ::= HAVING expr */
    0,  /* (214) limit_opt ::= */
   -2,  /* (215) limit_opt ::= LIMIT signed */
   -4,  /* (216) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (217) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (218) slimit_opt ::= */
   -2,  /* (219) slimit_opt ::= SLIMIT signed */
   -4,  /* (220) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (221) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (222) where_opt ::= */
   -2,  /* (223) where_opt ::= WHERE expr */
   -3,  /* (224) expr ::= LP expr RP */
   -1,  /* (225) expr ::= ID */
   -3,  /* (226) expr ::= ID DOT ID */
   -3,  /* (227) expr ::= ID DOT STAR */
   -1,  /* (228) expr ::= INTEGER */
   -2,  /* (229) expr ::= MINUS INTEGER */
   -2,  /* (230) expr ::= PLUS INTEGER */
   -1,  /* (231) expr ::= FLOAT */
   -2,  /* (232) expr ::= MINUS FLOAT */
   -2,  /* (233) expr ::= PLUS FLOAT */
   -1,  /* (234) expr ::= STRING */
   -1,  /* (235) expr ::= NOW */
   -1,  /* (236) expr ::= VARIABLE */
   -2,  /* (237) expr ::= PLUS VARIABLE */
   -2,  /* (238) expr ::= MINUS VARIABLE */
   -1,  /* (239) expr ::= BOOL */
   -1,  /* (240) expr ::= NULL */
   -4,  /* (241) expr ::= ID LP exprlist RP */
   -4,  /* (242) expr ::= ID LP STAR RP */
   -3,  /* (243) expr ::= expr IS NULL */
   -4,  /* (244) expr ::= expr IS NOT NULL */
   -3,  /* (245) expr ::= expr LT expr */
   -3,  /* (246) expr ::= expr GT expr */
   -3,  /* (247) expr ::= expr LE expr */
   -3,  /* (248) expr ::= expr GE expr */
   -3,  /* (249) expr ::= expr NE expr */
   -3,  /* (250) expr ::= expr EQ expr */
   -5,  /* (251) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (252) expr ::= expr AND expr */
   -3,  /* (253) expr ::= expr OR expr */
   -3,  /* (254) expr ::= expr PLUS expr */
   -3,  /* (255) expr ::= expr MINUS expr */
   -3,  /* (256) expr ::= expr STAR expr */
   -3,  /* (257) expr ::= expr SLASH expr */
   -3,  /* (258) expr ::= expr REM expr */
   -3,  /* (259) expr ::= expr LIKE expr */
   -5,  /* (260) expr ::= expr IN LP exprlist RP */
   -3,  /* (261) exprlist ::= exprlist COMMA expritem */
   -1,  /* (262) exprlist ::= expritem */
   -1,  /* (263) expritem ::= expr */
    0,  /* (264) expritem ::= */
   -3,  /* (265) cmd ::= RESET QUERY CACHE */
   -3,  /* (266) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (267) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (268) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (269) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (270) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (271) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (272) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (273) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (274) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (275) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (276) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (277) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (278) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (279) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (280) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy540, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy77);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy77);}
        break;
      case 58: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 59: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==59);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy540, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy181, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy181, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy77.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy77.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy77.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy77.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy77.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy77.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy77.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy77.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy77.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy77 = yylhsminor.yy77;
        break;
      case 84: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy347 = yymsp[0].minor.yy347; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy540); yymsp[1].minor.yy540.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 101: /* db_optr ::= db_optr cache */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 102: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 103: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 104: /* db_optr ::= db_optr days */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 105: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 106: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 107: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 108: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 109: /* db_optr ::= db_optr wal */
      case 124: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==124);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 110: /* db_optr ::= db_optr fsync */
      case 125: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==125);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 111: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 112: /* db_optr ::= db_optr prec */
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 113: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.keep = yymsp[0].minor.yy347; }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 114: /* db_optr ::= db_optr update */
      case 126: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==126);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 115: /* db_optr ::= db_optr cachelast */
      case 127: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==127);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 116: /* topic_optr ::= db_optr */
      case 128: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==128);
{ yylhsminor.yy540 = yymsp[0].minor.yy540; yylhsminor.yy540.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy540 = yylhsminor.yy540;
        break;
      case 117: /* topic_optr ::= topic_optr partitions */
      case 129: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==129);
{ yylhsminor.yy540 = yymsp[-1].minor.yy540; yylhsminor.yy540.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy540 = yylhsminor.yy540;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy540); yymsp[1].minor.yy540.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 130: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy181, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy181 = yylhsminor.yy181;
        break;
      case 131: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy271 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy271;  // negative value of name length
    tSetColumnType(&yylhsminor.yy181, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy181 = yylhsminor.yy181;
        break;
      case 132: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy181, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 133: /* signed ::= INTEGER */
{ yylhsminor.yy271 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy271 = yylhsminor.yy271;
        break;
      case 134: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy271 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 135: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy271 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 139: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy360;}
        break;
      case 140: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy282);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy360 = pCreateTable;
}
  yymsp[0].minor.yy360 = yylhsminor.yy360;
        break;
      case 141: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy360->childTableInfo, &yymsp[0].minor.yy282);
  yylhsminor.yy360 = yymsp[-1].minor.yy360;
}
  yymsp[-1].minor.yy360 = yylhsminor.yy360;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy360 = tSetCreateTableInfo(yymsp[-1].minor.yy347, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy360, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy360 = yylhsminor.yy360;
        break;
      case 143: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy360 = tSetCreateTableInfo(yymsp[-5].minor.yy347, yymsp[-1].minor.yy347, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy360, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy360 = yylhsminor.yy360;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy282 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy347, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy282 = yylhsminor.yy282;
        break;
      case 145: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy282 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy347, yymsp[-1].minor.yy347, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy282 = yylhsminor.yy282;
        break;
      case 146: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy347, &yymsp[0].minor.yy0); yylhsminor.yy347 = yymsp[-2].minor.yy347;  }
  yymsp[-2].minor.yy347 = yylhsminor.yy347;
        break;
      case 147: /* tagNamelist ::= ids */
{yylhsminor.yy347 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy347, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 148: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy360 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy542, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy360, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy360 = yylhsminor.yy360;
        break;
      case 149: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy347, &yymsp[0].minor.yy181); yylhsminor.yy347 = yymsp[-2].minor.yy347;  }
  yymsp[-2].minor.yy347 = yylhsminor.yy347;
        break;
      case 150: /* columnlist ::= column */
{yylhsminor.yy347 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy347, &yymsp[0].minor.yy181);}
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 151: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy181, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy181);
}
  yymsp[-1].minor.yy181 = yylhsminor.yy181;
        break;
      case 152: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy347 = tVariantListAppend(yymsp[-2].minor.yy347, &yymsp[0].minor.yy312, -1);    }
  yymsp[-2].minor.yy347 = yylhsminor.yy347;
        break;
      case 153: /* tagitemlist ::= tagitem */
{ yylhsminor.yy347 = tVariantListAppend(NULL, &yymsp[0].minor.yy312, -1); }
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 154: /* tagitem ::= INTEGER */
      case 155: /* tagitem ::= FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= STRING */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= BOOL */ yytestcase(yyruleno==157);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy312, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy312 = yylhsminor.yy312;
        break;
      case 158: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy312, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy312 = yylhsminor.yy312;
        break;
      case 159: /* tagitem ::= MINUS INTEGER */
      case 160: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==162);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy312, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy312 = yylhsminor.yy312;
        break;
      case 163: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy542 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy347, yymsp[-11].minor.yy120, yymsp[-10].minor.yy42, yymsp[-4].minor.yy347, yymsp[-3].minor.yy347, &yymsp[-9].minor.yy314, &yymsp[-8].minor.yy51, &yymsp[-7].minor.yy356, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy347, &yymsp[0].minor.yy188, &yymsp[-1].minor.yy188, yymsp[-2].minor.yy42);
}
  yymsp[-13].minor.yy542 = yylhsminor.yy542;
        break;
      case 164: /* select ::= LP select RP */
{yymsp[-2].minor.yy542 = yymsp[-1].minor.yy542;}
        break;
      case 165: /* union ::= select */
{ yylhsminor.yy347 = setSubclause(NULL, yymsp[0].minor.yy542); }
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 166: /* union ::= union UNION ALL select */
{ yylhsminor.yy347 = appendSelectClause(yymsp[-3].minor.yy347, yymsp[0].minor.yy542); }
  yymsp[-3].minor.yy347 = yylhsminor.yy347;
        break;
      case 167: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy347, NULL, TSDB_SQL_SELECT); }
        break;
      case 168: /* select ::= SELECT selcollist */
{
  yylhsminor.yy542 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy347, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy542 = yylhsminor.yy542;
        break;
      case 169: /* sclp ::= selcollist COMMA */
{yylhsminor.yy347 = yymsp[-1].minor.yy347;}
  yymsp[-1].minor.yy347 = yylhsminor.yy347;
        break;
      case 170: /* sclp ::= */
      case 200: /* orderby_opt ::= */ yytestcase(yyruleno==200);
{yymsp[1].minor.yy347 = 0;}
        break;
      case 171: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy347 = tSqlExprListAppend(yymsp[-3].minor.yy347, yymsp[-1].minor.yy42,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy347 = yylhsminor.yy347;
        break;
      case 172: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy347 = tSqlExprListAppend(yymsp[-1].minor.yy347, pNode, 0, 0);
}
  yymsp[-1].minor.yy347 = yylhsminor.yy347;
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
{yymsp[-1].minor.yy120 = yymsp[0].minor.yy120;}
        break;
      case 180: /* sub ::= LP union RP */
{yymsp[-2].minor.yy120 = addSubqueryElem(NULL, yymsp[-1].minor.yy347, NULL);}
        break;
      case 181: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy120 = addSubqueryElem(NULL, yymsp[-2].minor.yy347, &yymsp[0].minor.yy0);}
        break;
      case 182: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy120 = addSubqueryElem(yymsp[-5].minor.yy120, yymsp[-2].minor.yy347, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy120 = yylhsminor.yy120;
        break;
      case 183: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy120 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy120 = yylhsminor.yy120;
        break;
      case 184: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy120 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy120 = yylhsminor.yy120;
        break;
      case 185: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy120 = setTableNameList(yymsp[-3].minor.yy120, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy120 = yylhsminor.yy120;
        break;
      case 186: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy120 = setTableNameList(yymsp[-4].minor.yy120, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy120 = yylhsminor.yy120;
        break;
      case 187: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 188: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy314.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy314.offset.n = 0;}
        break;
      case 189: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy314.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy314.offset = yymsp[-1].minor.yy0;}
        break;
      case 190: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy314, 0, sizeof(yymsp[1].minor.yy314));}
        break;
      case 191: /* session_option ::= */
{yymsp[1].minor.yy51.col.n = 0; yymsp[1].minor.yy51.gap.n = 0;}
        break;
      case 192: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy51.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy51.gap = yymsp[-1].minor.yy0;
}
        break;
      case 193: /* windowstate_option ::= */
{yymsp[1].minor.yy356.col.n = 0;}
        break;
      case 194: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yymsp[-3].minor.yy356.col = yymsp[-1].minor.yy0;
}
        break;
      case 195: /* fill_opt ::= */
{ yymsp[1].minor.yy347 = 0;     }
        break;
      case 196: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy347, &A, -1, 0);
    yymsp[-5].minor.yy347 = yymsp[-1].minor.yy347;
}
        break;
      case 197: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy347 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 198: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 199: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 201: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy347 = yymsp[0].minor.yy347;}
        break;
      case 202: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy347 = tVariantListAppend(yymsp[-3].minor.yy347, &yymsp[-1].minor.yy312, yymsp[0].minor.yy82);
}
  yymsp[-3].minor.yy347 = yylhsminor.yy347;
        break;
      case 203: /* sortlist ::= item sortorder */
{
  yylhsminor.yy347 = tVariantListAppend(NULL, &yymsp[-1].minor.yy312, yymsp[0].minor.yy82);
}
  yymsp[-1].minor.yy347 = yylhsminor.yy347;
        break;
      case 204: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy312, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy312 = yylhsminor.yy312;
        break;
      case 205: /* sortorder ::= ASC */
{ yymsp[0].minor.yy82 = TSDB_ORDER_ASC; }
        break;
      case 206: /* sortorder ::= DESC */
{ yymsp[0].minor.yy82 = TSDB_ORDER_DESC;}
        break;
      case 207: /* sortorder ::= */
{ yymsp[1].minor.yy82 = TSDB_ORDER_ASC; }
        break;
      case 208: /* groupby_opt ::= */
{ yymsp[1].minor.yy347 = 0;}
        break;
      case 209: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy347 = yymsp[0].minor.yy347;}
        break;
      case 210: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy347 = tVariantListAppend(yymsp[-2].minor.yy347, &yymsp[0].minor.yy312, -1);
}
  yymsp[-2].minor.yy347 = yylhsminor.yy347;
        break;
      case 211: /* grouplist ::= item */
{
  yylhsminor.yy347 = tVariantListAppend(NULL, &yymsp[0].minor.yy312, -1);
}
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 212: /* having_opt ::= */
      case 222: /* where_opt ::= */ yytestcase(yyruleno==222);
      case 264: /* expritem ::= */ yytestcase(yyruleno==264);
{yymsp[1].minor.yy42 = 0;}
        break;
      case 213: /* having_opt ::= HAVING expr */
      case 223: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==223);
{yymsp[-1].minor.yy42 = yymsp[0].minor.yy42;}
        break;
      case 214: /* limit_opt ::= */
      case 218: /* slimit_opt ::= */ yytestcase(yyruleno==218);
{yymsp[1].minor.yy188.limit = -1; yymsp[1].minor.yy188.offset = 0;}
        break;
      case 215: /* limit_opt ::= LIMIT signed */
      case 219: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==219);
{yymsp[-1].minor.yy188.limit = yymsp[0].minor.yy271;  yymsp[-1].minor.yy188.offset = 0;}
        break;
      case 216: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy188.limit = yymsp[-2].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[0].minor.yy271;}
        break;
      case 217: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy188.limit = yymsp[0].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[-2].minor.yy271;}
        break;
      case 220: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy188.limit = yymsp[-2].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[0].minor.yy271;}
        break;
      case 221: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy188.limit = yymsp[0].minor.yy271;  yymsp[-3].minor.yy188.offset = yymsp[-2].minor.yy271;}
        break;
      case 224: /* expr ::= LP expr RP */
{yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy42->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 225: /* expr ::= ID */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 226: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 227: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 228: /* expr ::= INTEGER */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 229: /* expr ::= MINUS INTEGER */
      case 230: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==230);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 231: /* expr ::= FLOAT */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 232: /* expr ::= MINUS FLOAT */
      case 233: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 234: /* expr ::= STRING */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 235: /* expr ::= NOW */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 236: /* expr ::= VARIABLE */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 237: /* expr ::= PLUS VARIABLE */
      case 238: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==238);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 239: /* expr ::= BOOL */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 240: /* expr ::= NULL */
{ yylhsminor.yy42 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 241: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy42 = tSqlExprCreateFunction(yymsp[-1].minor.yy347, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy42 = yylhsminor.yy42;
        break;
      case 242: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy42 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy42 = yylhsminor.yy42;
        break;
      case 243: /* expr ::= expr IS NULL */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 244: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-3].minor.yy42, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy42 = yylhsminor.yy42;
        break;
      case 245: /* expr ::= expr LT expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_LT);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 246: /* expr ::= expr GT expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_GT);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 247: /* expr ::= expr LE expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_LE);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 248: /* expr ::= expr GE expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_GE);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 249: /* expr ::= expr NE expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_NE);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 250: /* expr ::= expr EQ expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_EQ);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 251: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy42); yylhsminor.yy42 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy42, yymsp[-2].minor.yy42, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy42, TK_LE), TK_AND);}
  yymsp[-4].minor.yy42 = yylhsminor.yy42;
        break;
      case 252: /* expr ::= expr AND expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_AND);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 253: /* expr ::= expr OR expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_OR); }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 254: /* expr ::= expr PLUS expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_PLUS);  }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 255: /* expr ::= expr MINUS expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_MINUS); }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 256: /* expr ::= expr STAR expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_STAR);  }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 257: /* expr ::= expr SLASH expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_DIVIDE);}
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 258: /* expr ::= expr REM expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_REM);   }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 259: /* expr ::= expr LIKE expr */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-2].minor.yy42, yymsp[0].minor.yy42, TK_LIKE);  }
  yymsp[-2].minor.yy42 = yylhsminor.yy42;
        break;
      case 260: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy42 = tSqlExprCreate(yymsp[-4].minor.yy42, (tSqlExpr*)yymsp[-1].minor.yy347, TK_IN); }
  yymsp[-4].minor.yy42 = yylhsminor.yy42;
        break;
      case 261: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy347 = tSqlExprListAppend(yymsp[-2].minor.yy347,yymsp[0].minor.yy42,0, 0);}
  yymsp[-2].minor.yy347 = yylhsminor.yy347;
        break;
      case 262: /* exprlist ::= expritem */
{yylhsminor.yy347 = tSqlExprListAppend(0,yymsp[0].minor.yy42,0, 0);}
  yymsp[0].minor.yy347 = yylhsminor.yy347;
        break;
      case 263: /* expritem ::= expr */
{yylhsminor.yy42 = yymsp[0].minor.yy42;}
  yymsp[0].minor.yy42 = yylhsminor.yy42;
        break;
      case 265: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 266: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy347, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy347, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 272: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy312, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy347, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy347, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 278: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 279: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 280: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
