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
#define YYNOCODE 273
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSqlNode* yy24;
  int yy60;
  SIntervalVal yy136;
  int64_t yy157;
  SCreateAcctInfo yy171;
  SSessionWindowVal yy251;
  SCreateDbInfo yy254;
  SWindowStateVal yy256;
  SLimitVal yy262;
  SRelationInfo* yy292;
  tSqlExpr* yy370;
  tVariant yy394;
  SArray* yy413;
  SCreateTableSql* yy438;
  TAOS_FIELD yy471;
  SCreatedTableInfo yy544;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             342
#define YYNRULE              281
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
 /*   100 */    44,   91,   53,  283,   58,   56,   60,   57, 1088,   33,
 /*   110 */  1084,   87,   49,   48,  197, 1037,   47,   46,   45,   50,
 /*   120 */    52,   79,   54,   55,  228, 1089,  232,   44,   39,   53,
 /*   130 */   283,   58,   56,   60,   57,  280,  968,   84,   13,   49,
 /*   140 */    48,  244,   90,   47,   46,   45,   51,  152,   54,   55,
 /*   150 */   167,  222,  232,   44,  977,   53,  283,   58,   56,   60,
 /*   160 */    57,  780,  152,  767,   33,   49,   48,    1,  169,   47,
 /*   170 */    46,   45,   24,  303,  334,  333,  302,  301,  300,  332,
 /*   180 */   299,  331,  330,  329,  298,  328,  327,  940, 1083,  928,
 /*   190 */   929,  930,  931,  932,  933,  934,  935,  936,  937,  938,
 /*   200 */   939,  941,  942,   54,   55,   19,  235,  232,   44,  977,
 /*   210 */    53,  283,   58,   56,   60,   57,  704, 1038,  705,  278,
 /*   220 */    49,   48,  243,  205,   47,   46,   45,  231,  782,  676,
 /*   230 */   207,  771,   85,  774,  214,  777,  128,  127,  206,  948,
 /*   240 */   231,  782,  946,  947,  771,  284,  774,  949,  777,  951,
 /*   250 */   952,  950,  241,  953,  954,  239,   72,  315,  314,  216,
 /*   260 */   217,   49,   48,  282,  215,   47,   46,   45,  772,   33,
 /*   270 */   775,   79,  216,  217,   24,  966,  334,  333,   39,  199,
 /*   280 */   980,  332,  115,  331,  330,  329,  593,  328,  327,  965,
 /*   290 */   700,  325,  200,  697,  594,  698,  245,  699,   33,  312,
 /*   300 */   311,  258,   33,   71,   58,   56,   60,   57,  202,  213,
 /*   310 */   335,  236,   49,   48,  977,   33,   47,   46,   45,  251,
 /*   320 */   715,  246,  247,    5,   36,  171,  242,  255,  254,  307,
 /*   330 */   170,   97,  102,   93,  101,  980,   59,   47,   46,   45,
 /*   340 */   308,  244,  783,  977,  309,  294,  786,  977,  779,   59,
 /*   350 */   168,   76,   33,  974,   33,  783,  305,  313,  262,   86,
 /*   360 */   977,  779,  338,  337,  137,  778,  143,  141,  140,  875,
 /*   370 */   113,  107,  118,   74,   70,  181,  675,  117,  778,  123,
 /*   380 */   126,  116,  189,  187,  185,  781,  230,  120,  196,  184,
 /*   390 */   132,  131,  130,  129,  317,   25,  238,  977,  244,  976,
 /*   400 */   884,  260,  876,  719,  701,   34,  181,  978,  181,    3,
 /*   410 */   182,  748,  749,  712,  769,   77,  731,  739,  203,    8,
 /*   420 */   147,   63,  262,  740,  803,  784,   64,   21,   20,   20,
 /*   430 */   204,   67,  686,  286,  688,    6,   34,   34,   63,  288,
 /*   440 */   687,  210,   29,   89,   63,  289,  106,  105,   65,  211,
 /*   450 */   770,   68,   15,   14,  112,  111,  702,  209,  703,   17,
 /*   460 */    16,  125,  124,  194, 1099,  208,  198, 1048,  979, 1047,
 /*   470 */   233, 1044, 1043,  256,  144,  234,  316, 1000,   42, 1011,
 /*   480 */  1030, 1008, 1029, 1009,  993,  263, 1013,  142,  146,  150,
 /*   490 */   272,  163,  975,  261,  164,  267,  973,  295,  165,  944,
 /*   500 */   114,  730,  160,  158,  154,  990,  166,  153,  155,  225,
 /*   510 */   156,  269,  276,  277,  889,  291,   69,  292,   61,  281,
 /*   520 */    66,  293,  279,  296,  297,  157,  275,   40,  192,  273,
 /*   530 */    37,  306,  883, 1104,  103, 1103, 1101,  172,  310, 1098,
 /*   540 */   109, 1097, 1095,  173,  909,   38,   35,   41,  193,  872,
 /*   550 */   119,  870,  121,  122,  868,  867,  248,  183,  865,  864,
 /*   560 */   863,  862,  861,  860,  859,  186,  188,  856,  854,  852,
 /*   570 */   850,  190,  847,  191,  271,   75,   80,  268,  270, 1031,
 /*   580 */    43,  326,  318,  319,  320,  321,  322,  218,  323,  324,
 /*   590 */   336,  240,  290,  823,  249,  250,  822,  219,  252,  212,
 /*   600 */    98,   99,  888,  887,  253,  821,  809,  808,  257,  262,
 /*   610 */   285,   78,  866,    9,   26,  133,  176,  175,  910,  134,
 /*   620 */   174,  177,  179,  178,  180,  858,    2,  135,  857,    4,
 /*   630 */   136,  849,  848,  707,  264,  161,  159,  732,  162,   81,
 /*   640 */   148,  227,  956,  149,  735,   82,   10,  737,   83,  274,
 /*   650 */    11,  741,  151,  787,   92,  785,   27,    7,   28,   12,
 /*   660 */    22,  287,   23,   31,   94,   90,   95,  607,   32,   96,
 /*   670 */   639,  635,  633,  632,  631,  628,  597,   34,  304,  100,
 /*   680 */    62,  678,  677,  674,  623,  621,  613,  619,  615,  617,
 /*   690 */   104,  108,  611,  609,  642,  641,  640,  638,  110,  637,
 /*   700 */   636,  634,  630,  629,   63,  595,  138,  139,  561,  559,
 /*   710 */   827,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   260,    1,  241,    1,  260,  197,  241,    1,  203,    9,
 /*    10 */   197,    9,  260,   13,   14,    9,   16,   17,  257,  260,
 /*    20 */    20,   21,  257,   23,   24,   25,   26,   27,   28,  270,
 /*    30 */   271,  196,  197,   33,   34,  194,  195,   37,   38,   39,
 /*    40 */   235,  236,  237,  238,    5,  197,    7,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  218,  218,   62,   13,   14,  197,   16,   17,
 /*    70 */   262,  263,   20,   21,  261,   23,   24,   25,   26,   27,
 /*    80 */    28,  260,  241,   83,   81,   33,   34,  243,  243,   37,
 /*    90 */    38,   39,   13,   14,   87,   16,   17,  260,  257,   20,
 /*   100 */    21,  203,   23,   24,   25,   26,   27,   28,  271,  197,
 /*   110 */   260,   83,   33,   34,  260,  267,   37,   38,   39,   13,
 /*   120 */    14,  110,   16,   17,  270,  271,   20,   21,  117,   23,
 /*   130 */    24,   25,   26,   27,   28,  265,  238,  267,  110,   33,
 /*   140 */    34,  197,  114,   37,   38,   39,   14,  197,   16,   17,
 /*   150 */   206,  239,   20,   21,  242,   23,   24,   25,   26,   27,
 /*   160 */    28,  122,  197,  111,  197,   33,   34,  204,  205,   37,
 /*   170 */    38,   39,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   180 */   102,  103,  104,  105,  106,  107,  108,  217,  260,  219,
 /*   190 */   220,  221,  222,  223,  224,  225,  226,  227,  228,  229,
 /*   200 */   230,  231,  232,   16,   17,   44,  239,   20,   21,  242,
 /*   210 */    23,   24,   25,   26,   27,   28,    5,  267,    7,  269,
 /*   220 */    33,   34,   69,   62,   37,   38,   39,    1,    2,    5,
 /*   230 */    69,    5,  267,    7,  260,    9,   75,   76,   77,  217,
 /*   240 */     1,    2,  220,  221,    5,   15,    7,  225,    9,  227,
 /*   250 */   228,  229,   69,  231,  232,  218,  203,   33,   34,   33,
 /*   260 */    34,   33,   34,   37,  260,   37,   38,   39,    5,  197,
 /*   270 */     7,  110,   33,   34,   94,    0,   96,   97,  117,  260,
 /*   280 */   243,  101,   78,  103,  104,  105,    1,  107,  108,  236,
 /*   290 */     2,   87,  260,    5,    9,    7,  143,    9,  197,  146,
 /*   300 */   147,  140,  197,  142,   25,   26,   27,   28,  260,  148,
 /*   310 */   218,  239,   33,   34,  242,  197,   37,   38,   39,  141,
 /*   320 */    37,   33,   34,   63,   64,   65,  143,  149,  150,  146,
 /*   330 */    70,   71,   72,   73,   74,  243,  110,   37,   38,   39,
 /*   340 */   239,  197,  116,  242,  239,   85,  116,  242,  122,  110,
 /*   350 */   206,  111,  197,  197,  197,  116,   81,  239,  118,  244,
 /*   360 */   242,  122,   66,   67,   68,  139,   63,   64,   65,  202,
 /*   370 */    63,   64,   65,  258,  110,  208,  112,   70,  139,   72,
 /*   380 */    73,   74,   63,   64,   65,  122,   61,   80,  260,   70,
 /*   390 */    71,   72,   73,   74,  239,  110,  240,  242,  197,  242,
 /*   400 */   202,  111,  202,  120,  116,  115,  208,  206,  208,  200,
 /*   410 */   201,  130,  131,  115,    1,  111,  111,  111,  260,  121,
 /*   420 */   115,  115,  118,  111,  111,  111,  115,  115,  115,  115,
 /*   430 */   260,  115,  111,  111,  111,  110,  115,  115,  115,  111,
 /*   440 */   111,  260,  110,  115,  115,  113,  144,  145,  137,  260,
 /*   450 */    37,  135,  144,  145,  144,  145,    5,  260,    7,  144,
 /*   460 */   145,   78,   79,  260,  243,  260,  260,  234,  243,  234,
 /*   470 */   234,  234,  234,  197,  197,  234,  234,  197,  259,  197,
 /*   480 */   268,  197,  268,  197,  241,  241,  197,   61,  197,  197,
 /*   490 */   197,  245,  241,  198,  197,  264,  197,   86,  197,  233,
 /*   500 */    93,  122,  248,  250,  254,  256,  197,  255,  253,  264,
 /*   510 */   252,  264,  264,  127,  197,  197,  134,  197,  133,  128,
 /*   520 */   136,  197,  132,  197,  197,  251,  126,  197,  197,  125,
 /*   530 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   540 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   550 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   560 */   197,  197,  197,  197,  197,  197,  197,  197,  197,  197,
 /*   570 */   197,  197,  197,  197,  124,  198,  198,  123,  198,  198,
 /*   580 */   138,  109,   92,   51,   89,   91,   55,  198,   90,   88,
 /*   590 */    81,  198,  198,    5,  151,    5,    5,  198,  151,  198,
 /*   600 */   203,  203,  207,  207,    5,    5,   96,   95,  141,  118,
 /*   610 */   113,  119,  198,  110,  110,  199,  210,  214,  216,  199,
 /*   620 */   215,  213,  212,  211,  209,  198,  204,  199,  198,  200,
 /*   630 */   199,  198,  198,  111,  115,  247,  249,  111,  246,  115,
 /*   640 */   110,    1,  233,  115,  111,  110,  129,  111,  110,  110,
 /*   650 */   129,  111,  110,  116,   78,  111,  115,  110,  115,  110,
 /*   660 */   110,  113,  110,   84,   83,  114,   71,    5,   84,   83,
 /*   670 */     9,    5,    5,    5,    5,    5,   82,  115,   15,   78,
 /*   680 */    16,    5,    5,  111,    5,    5,    5,    5,    5,    5,
 /*   690 */   145,  145,    5,    5,    5,    5,    5,    5,  145,    5,
 /*   700 */     5,    5,    5,    5,  115,   82,   21,   21,   61,   60,
 /*   710 */     0,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   720 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   730 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   740 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   750 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   760 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   770 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   780 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   790 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   800 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   810 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   820 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   830 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   840 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   850 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   860 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   870 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   880 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   890 */   272,  272,  272,  272,  272,  272,  272,  272,  272,  272,
 /*   900 */   272,  272,  272,  272,
};
#define YY_SHIFT_COUNT    (341)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (710)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   78,   78,  180,  180,    3,  226,  239,  285,    6,
 /*    10 */     6,    6,    6,    6,    6,    6,    6,    6,    0,    2,
 /*    20 */   239,  288,  288,  288,  288,   11,   11,    6,    6,    6,
 /*    30 */   275,    6,    6,    6,    6,  204,    3,    7,    7,  711,
 /*    40 */   711,  711,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  288,  288,  224,  224,  224,  224,  224,  224,
 /*    70 */   224,    6,    6,    6,  283,    6,    6,    6,   11,   11,
 /*    80 */     6,    6,    6,    6,  281,  281,  298,   11,    6,    6,
 /*    90 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   100 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   110 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   120 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   130 */     6,    6,    6,    6,    6,    6,    6,    6,    6,    6,
 /*   140 */     6,    6,    6,    6,  426,  426,  426,  379,  379,  379,
 /*   150 */   426,  379,  426,  382,  384,  385,  391,  390,  386,  400,
 /*   160 */   404,  450,  454,  442,  426,  426,  426,  411,  411,  472,
 /*   170 */     3,    3,  426,  426,  407,  490,  532,  495,  494,  531,
 /*   180 */   498,  501,  472,  426,  509,  509,  426,  509,  426,  509,
 /*   190 */   426,  426,  711,  711,   52,   79,  106,   79,   79,  132,
 /*   200 */   187,  279,  279,  279,  279,  260,  307,  319,  228,  228,
 /*   210 */   228,  228,  153,  178,  300,  300,   39,  263,   28,  183,
 /*   220 */   296,  303,  290,  240,  304,  305,  306,  312,  313,  314,
 /*   230 */   413,  325,  230,  311,  316,  321,  322,  323,  328,  329,
 /*   240 */   332,  302,  308,  310,  264,  315,  211,  451,  383,  588,
 /*   250 */   443,  590,  591,  447,  599,  600,  510,  512,  467,  491,
 /*   260 */   497,  503,  492,  522,  504,  519,  524,  526,  530,  533,
 /*   270 */   528,  535,  536,  538,  640,  539,  540,  542,  541,  517,
 /*   280 */   543,  521,  544,  547,  537,  549,  497,  550,  548,  552,
 /*   290 */   551,  576,  579,  581,  595,  662,  584,  586,  661,  666,
 /*   300 */   667,  668,  669,  670,  594,  663,  601,  545,  562,  562,
 /*   310 */   664,  546,  553,  562,  676,  677,  572,  562,  679,  680,
 /*   320 */   681,  682,  683,  684,  687,  688,  689,  690,  691,  692,
 /*   330 */   694,  695,  696,  697,  698,  589,  623,  685,  686,  647,
 /*   340 */   649,  710,
};
#define YY_REDUCE_COUNT (193)
#define YY_REDUCE_MIN   (-260)
#define YY_REDUCE_MAX   (434)
static const short yy_reduce_ofst[] = {
 /*     0 */  -159,  -30,  -30,   22,   22, -195, -241, -146, -192,  -88,
 /*    10 */   -50, -130,  -33,   72,  101,  105,  118,  155, -187, -165,
 /*    20 */  -163, -156, -155,   37,   92, -239, -235, -152,  -35,  156,
 /*    30 */  -102,  -56,  144,  201,  157,  167,   53,  198,  200,  115,
 /*    40 */   -37,  209, -260, -256, -248, -179, -150,  -72,  -26,    4,
 /*    50 */    19,   32,   48,  128,  158,  170,  181,  189,  197,  203,
 /*    60 */   205,  206,  221,  225,  233,  235,  236,  237,  238,  241,
 /*    70 */   242,  276,  277,  280,  219,  282,  284,  286,  243,  244,
 /*    80 */   289,  291,  292,  293,  212,  214,  246,  251,  297,  299,
 /*    90 */   301,  309,  317,  318,  320,  324,  326,  327,  330,  331,
 /*   100 */   333,  334,  335,  336,  337,  338,  339,  340,  341,  342,
 /*   110 */   343,  344,  345,  346,  347,  348,  349,  350,  351,  352,
 /*   120 */   353,  354,  355,  356,  357,  358,  359,  360,  361,  362,
 /*   130 */   363,  364,  365,  366,  367,  368,  369,  370,  371,  372,
 /*   140 */   373,  374,  375,  376,  295,  377,  378,  231,  245,  247,
 /*   150 */   380,  248,  381,  249,  252,  250,  255,  258,  274,  253,
 /*   160 */   387,  254,  388,  392,  389,  393,  394,  395,  396,  266,
 /*   170 */   397,  398,  399,  401,  402,  405,  403,  406,  408,  412,
 /*   180 */   410,  415,  409,  414,  416,  420,  427,  428,  430,  431,
 /*   190 */   433,  434,  422,  429,
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
  /*  193 */ "error",
  /*  194 */ "program",
  /*  195 */ "cmd",
  /*  196 */ "dbPrefix",
  /*  197 */ "ids",
  /*  198 */ "cpxName",
  /*  199 */ "ifexists",
  /*  200 */ "alter_db_optr",
  /*  201 */ "alter_topic_optr",
  /*  202 */ "acct_optr",
  /*  203 */ "ifnotexists",
  /*  204 */ "db_optr",
  /*  205 */ "topic_optr",
  /*  206 */ "typename",
  /*  207 */ "bufsize",
  /*  208 */ "pps",
  /*  209 */ "tseries",
  /*  210 */ "dbs",
  /*  211 */ "streams",
  /*  212 */ "storage",
  /*  213 */ "qtime",
  /*  214 */ "users",
  /*  215 */ "conns",
  /*  216 */ "state",
  /*  217 */ "keep",
  /*  218 */ "tagitemlist",
  /*  219 */ "cache",
  /*  220 */ "replica",
  /*  221 */ "quorum",
  /*  222 */ "days",
  /*  223 */ "minrows",
  /*  224 */ "maxrows",
  /*  225 */ "blocks",
  /*  226 */ "ctime",
  /*  227 */ "wal",
  /*  228 */ "fsync",
  /*  229 */ "comp",
  /*  230 */ "prec",
  /*  231 */ "update",
  /*  232 */ "cachelast",
  /*  233 */ "partitions",
  /*  234 */ "signed",
  /*  235 */ "create_table_args",
  /*  236 */ "create_stable_args",
  /*  237 */ "create_table_list",
  /*  238 */ "create_from_stable",
  /*  239 */ "columnlist",
  /*  240 */ "tagNamelist",
  /*  241 */ "select",
  /*  242 */ "column",
  /*  243 */ "tagitem",
  /*  244 */ "selcollist",
  /*  245 */ "from",
  /*  246 */ "where_opt",
  /*  247 */ "interval_opt",
  /*  248 */ "session_option",
  /*  249 */ "windowstate_option",
  /*  250 */ "fill_opt",
  /*  251 */ "sliding_opt",
  /*  252 */ "groupby_opt",
  /*  253 */ "orderby_opt",
  /*  254 */ "having_opt",
  /*  255 */ "slimit_opt",
  /*  256 */ "limit_opt",
  /*  257 */ "union",
  /*  258 */ "sclp",
  /*  259 */ "distinct",
  /*  260 */ "expr",
  /*  261 */ "as",
  /*  262 */ "tablelist",
  /*  263 */ "sub",
  /*  264 */ "tmvar",
  /*  265 */ "sortlist",
  /*  266 */ "sortitem",
  /*  267 */ "item",
  /*  268 */ "sortorder",
  /*  269 */ "grouplist",
  /*  270 */ "exprlist",
  /*  271 */ "expritem",
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
    case 217: /* keep */
    case 218: /* tagitemlist */
    case 239: /* columnlist */
    case 240: /* tagNamelist */
    case 250: /* fill_opt */
    case 252: /* groupby_opt */
    case 253: /* orderby_opt */
    case 265: /* sortlist */
    case 269: /* grouplist */
{
taosArrayDestroy((yypminor->yy413));
}
      break;
    case 237: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy438));
}
      break;
    case 241: /* select */
{
destroySqlNode((yypminor->yy24));
}
      break;
    case 244: /* selcollist */
    case 258: /* sclp */
    case 270: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy413));
}
      break;
    case 245: /* from */
    case 262: /* tablelist */
    case 263: /* sub */
{
destroyRelationInfo((yypminor->yy292));
}
      break;
    case 246: /* where_opt */
    case 254: /* having_opt */
    case 260: /* expr */
    case 271: /* expritem */
{
tSqlExprDestroy((yypminor->yy370));
}
      break;
    case 257: /* union */
{
destroyAllSqlNode((yypminor->yy413));
}
      break;
    case 266: /* sortitem */
{
tVariantDestroy(&(yypminor->yy394));
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
  {  194,   -1 }, /* (0) program ::= cmd */
  {  195,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  195,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  195,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  195,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  195,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  195,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  195,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  195,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  195,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  195,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  195,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  195,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  195,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  195,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  195,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  195,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  196,    0 }, /* (17) dbPrefix ::= */
  {  196,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  198,    0 }, /* (19) cpxName ::= */
  {  198,   -2 }, /* (20) cpxName ::= DOT ids */
  {  195,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  195,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  195,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  195,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  195,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  195,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  195,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  195,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  195,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  195,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  195,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  195,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  195,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  195,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  195,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  195,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  195,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  195,   -2 }, /* (38) cmd ::= USE ids */
  {  195,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  195,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  195,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  195,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  195,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  195,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  195,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  195,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  195,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  195,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  195,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  197,   -1 }, /* (50) ids ::= ID */
  {  197,   -1 }, /* (51) ids ::= STRING */
  {  199,   -2 }, /* (52) ifexists ::= IF EXISTS */
  {  199,    0 }, /* (53) ifexists ::= */
  {  203,   -3 }, /* (54) ifnotexists ::= IF NOT EXISTS */
  {  203,    0 }, /* (55) ifnotexists ::= */
  {  195,   -3 }, /* (56) cmd ::= CREATE DNODE ids */
  {  195,   -6 }, /* (57) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  195,   -5 }, /* (58) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  195,   -5 }, /* (59) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  195,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  195,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  195,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  207,    0 }, /* (63) bufsize ::= */
  {  207,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  208,    0 }, /* (65) pps ::= */
  {  208,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  209,    0 }, /* (67) tseries ::= */
  {  209,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  210,    0 }, /* (69) dbs ::= */
  {  210,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  211,    0 }, /* (71) streams ::= */
  {  211,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  212,    0 }, /* (73) storage ::= */
  {  212,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  213,    0 }, /* (75) qtime ::= */
  {  213,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  214,    0 }, /* (77) users ::= */
  {  214,   -2 }, /* (78) users ::= USERS INTEGER */
  {  215,    0 }, /* (79) conns ::= */
  {  215,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  216,    0 }, /* (81) state ::= */
  {  216,   -2 }, /* (82) state ::= STATE ids */
  {  202,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  217,   -2 }, /* (84) keep ::= KEEP tagitemlist */
  {  219,   -2 }, /* (85) cache ::= CACHE INTEGER */
  {  220,   -2 }, /* (86) replica ::= REPLICA INTEGER */
  {  221,   -2 }, /* (87) quorum ::= QUORUM INTEGER */
  {  222,   -2 }, /* (88) days ::= DAYS INTEGER */
  {  223,   -2 }, /* (89) minrows ::= MINROWS INTEGER */
  {  224,   -2 }, /* (90) maxrows ::= MAXROWS INTEGER */
  {  225,   -2 }, /* (91) blocks ::= BLOCKS INTEGER */
  {  226,   -2 }, /* (92) ctime ::= CTIME INTEGER */
  {  227,   -2 }, /* (93) wal ::= WAL INTEGER */
  {  228,   -2 }, /* (94) fsync ::= FSYNC INTEGER */
  {  229,   -2 }, /* (95) comp ::= COMP INTEGER */
  {  230,   -2 }, /* (96) prec ::= PRECISION STRING */
  {  231,   -2 }, /* (97) update ::= UPDATE INTEGER */
  {  232,   -2 }, /* (98) cachelast ::= CACHELAST INTEGER */
  {  233,   -2 }, /* (99) partitions ::= PARTITIONS INTEGER */
  {  204,    0 }, /* (100) db_optr ::= */
  {  204,   -2 }, /* (101) db_optr ::= db_optr cache */
  {  204,   -2 }, /* (102) db_optr ::= db_optr replica */
  {  204,   -2 }, /* (103) db_optr ::= db_optr quorum */
  {  204,   -2 }, /* (104) db_optr ::= db_optr days */
  {  204,   -2 }, /* (105) db_optr ::= db_optr minrows */
  {  204,   -2 }, /* (106) db_optr ::= db_optr maxrows */
  {  204,   -2 }, /* (107) db_optr ::= db_optr blocks */
  {  204,   -2 }, /* (108) db_optr ::= db_optr ctime */
  {  204,   -2 }, /* (109) db_optr ::= db_optr wal */
  {  204,   -2 }, /* (110) db_optr ::= db_optr fsync */
  {  204,   -2 }, /* (111) db_optr ::= db_optr comp */
  {  204,   -2 }, /* (112) db_optr ::= db_optr prec */
  {  204,   -2 }, /* (113) db_optr ::= db_optr keep */
  {  204,   -2 }, /* (114) db_optr ::= db_optr update */
  {  204,   -2 }, /* (115) db_optr ::= db_optr cachelast */
  {  205,   -1 }, /* (116) topic_optr ::= db_optr */
  {  205,   -2 }, /* (117) topic_optr ::= topic_optr partitions */
  {  200,    0 }, /* (118) alter_db_optr ::= */
  {  200,   -2 }, /* (119) alter_db_optr ::= alter_db_optr replica */
  {  200,   -2 }, /* (120) alter_db_optr ::= alter_db_optr quorum */
  {  200,   -2 }, /* (121) alter_db_optr ::= alter_db_optr keep */
  {  200,   -2 }, /* (122) alter_db_optr ::= alter_db_optr blocks */
  {  200,   -2 }, /* (123) alter_db_optr ::= alter_db_optr comp */
  {  200,   -2 }, /* (124) alter_db_optr ::= alter_db_optr wal */
  {  200,   -2 }, /* (125) alter_db_optr ::= alter_db_optr fsync */
  {  200,   -2 }, /* (126) alter_db_optr ::= alter_db_optr update */
  {  200,   -2 }, /* (127) alter_db_optr ::= alter_db_optr cachelast */
  {  201,   -1 }, /* (128) alter_topic_optr ::= alter_db_optr */
  {  201,   -2 }, /* (129) alter_topic_optr ::= alter_topic_optr partitions */
  {  206,   -1 }, /* (130) typename ::= ids */
  {  206,   -4 }, /* (131) typename ::= ids LP signed RP */
  {  206,   -2 }, /* (132) typename ::= ids UNSIGNED */
  {  234,   -1 }, /* (133) signed ::= INTEGER */
  {  234,   -2 }, /* (134) signed ::= PLUS INTEGER */
  {  234,   -2 }, /* (135) signed ::= MINUS INTEGER */
  {  195,   -3 }, /* (136) cmd ::= CREATE TABLE create_table_args */
  {  195,   -3 }, /* (137) cmd ::= CREATE TABLE create_stable_args */
  {  195,   -3 }, /* (138) cmd ::= CREATE STABLE create_stable_args */
  {  195,   -3 }, /* (139) cmd ::= CREATE TABLE create_table_list */
  {  237,   -1 }, /* (140) create_table_list ::= create_from_stable */
  {  237,   -2 }, /* (141) create_table_list ::= create_table_list create_from_stable */
  {  235,   -6 }, /* (142) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  236,  -10 }, /* (143) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  238,  -10 }, /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  238,  -13 }, /* (145) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  240,   -3 }, /* (146) tagNamelist ::= tagNamelist COMMA ids */
  {  240,   -1 }, /* (147) tagNamelist ::= ids */
  {  235,   -5 }, /* (148) create_table_args ::= ifnotexists ids cpxName AS select */
  {  239,   -3 }, /* (149) columnlist ::= columnlist COMMA column */
  {  239,   -1 }, /* (150) columnlist ::= column */
  {  242,   -2 }, /* (151) column ::= ids typename */
  {  218,   -3 }, /* (152) tagitemlist ::= tagitemlist COMMA tagitem */
  {  218,   -1 }, /* (153) tagitemlist ::= tagitem */
  {  243,   -1 }, /* (154) tagitem ::= INTEGER */
  {  243,   -1 }, /* (155) tagitem ::= FLOAT */
  {  243,   -1 }, /* (156) tagitem ::= STRING */
  {  243,   -1 }, /* (157) tagitem ::= BOOL */
  {  243,   -1 }, /* (158) tagitem ::= NULL */
  {  243,   -2 }, /* (159) tagitem ::= MINUS INTEGER */
  {  243,   -2 }, /* (160) tagitem ::= MINUS FLOAT */
  {  243,   -2 }, /* (161) tagitem ::= PLUS INTEGER */
  {  243,   -2 }, /* (162) tagitem ::= PLUS FLOAT */
  {  241,  -14 }, /* (163) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
  {  241,   -3 }, /* (164) select ::= LP select RP */
  {  257,   -1 }, /* (165) union ::= select */
  {  257,   -4 }, /* (166) union ::= union UNION ALL select */
  {  195,   -1 }, /* (167) cmd ::= union */
  {  241,   -2 }, /* (168) select ::= SELECT selcollist */
  {  258,   -2 }, /* (169) sclp ::= selcollist COMMA */
  {  258,    0 }, /* (170) sclp ::= */
  {  244,   -4 }, /* (171) selcollist ::= sclp distinct expr as */
  {  244,   -2 }, /* (172) selcollist ::= sclp STAR */
  {  261,   -2 }, /* (173) as ::= AS ids */
  {  261,   -1 }, /* (174) as ::= ids */
  {  261,    0 }, /* (175) as ::= */
  {  259,   -1 }, /* (176) distinct ::= DISTINCT */
  {  259,    0 }, /* (177) distinct ::= */
  {  245,   -2 }, /* (178) from ::= FROM tablelist */
  {  245,   -2 }, /* (179) from ::= FROM sub */
  {  263,   -3 }, /* (180) sub ::= LP union RP */
  {  263,   -4 }, /* (181) sub ::= LP union RP ids */
  {  263,   -6 }, /* (182) sub ::= sub COMMA LP union RP ids */
  {  262,   -2 }, /* (183) tablelist ::= ids cpxName */
  {  262,   -3 }, /* (184) tablelist ::= ids cpxName ids */
  {  262,   -4 }, /* (185) tablelist ::= tablelist COMMA ids cpxName */
  {  262,   -5 }, /* (186) tablelist ::= tablelist COMMA ids cpxName ids */
  {  264,   -1 }, /* (187) tmvar ::= VARIABLE */
  {  247,   -4 }, /* (188) interval_opt ::= INTERVAL LP tmvar RP */
  {  247,   -6 }, /* (189) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
  {  247,    0 }, /* (190) interval_opt ::= */
  {  248,    0 }, /* (191) session_option ::= */
  {  248,   -7 }, /* (192) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  249,    0 }, /* (193) windowstate_option ::= */
  {  249,   -4 }, /* (194) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  250,    0 }, /* (195) fill_opt ::= */
  {  250,   -6 }, /* (196) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  250,   -4 }, /* (197) fill_opt ::= FILL LP ID RP */
  {  251,   -4 }, /* (198) sliding_opt ::= SLIDING LP tmvar RP */
  {  251,    0 }, /* (199) sliding_opt ::= */
  {  253,    0 }, /* (200) orderby_opt ::= */
  {  253,   -3 }, /* (201) orderby_opt ::= ORDER BY sortlist */
  {  265,   -4 }, /* (202) sortlist ::= sortlist COMMA item sortorder */
  {  265,   -2 }, /* (203) sortlist ::= item sortorder */
  {  267,   -2 }, /* (204) item ::= ids cpxName */
  {  268,   -1 }, /* (205) sortorder ::= ASC */
  {  268,   -1 }, /* (206) sortorder ::= DESC */
  {  268,    0 }, /* (207) sortorder ::= */
  {  252,    0 }, /* (208) groupby_opt ::= */
  {  252,   -3 }, /* (209) groupby_opt ::= GROUP BY grouplist */
  {  269,   -3 }, /* (210) grouplist ::= grouplist COMMA item */
  {  269,   -1 }, /* (211) grouplist ::= item */
  {  254,    0 }, /* (212) having_opt ::= */
  {  254,   -2 }, /* (213) having_opt ::= HAVING expr */
  {  256,    0 }, /* (214) limit_opt ::= */
  {  256,   -2 }, /* (215) limit_opt ::= LIMIT signed */
  {  256,   -4 }, /* (216) limit_opt ::= LIMIT signed OFFSET signed */
  {  256,   -4 }, /* (217) limit_opt ::= LIMIT signed COMMA signed */
  {  255,    0 }, /* (218) slimit_opt ::= */
  {  255,   -2 }, /* (219) slimit_opt ::= SLIMIT signed */
  {  255,   -4 }, /* (220) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  255,   -4 }, /* (221) slimit_opt ::= SLIMIT signed COMMA signed */
  {  246,    0 }, /* (222) where_opt ::= */
  {  246,   -2 }, /* (223) where_opt ::= WHERE expr */
  {  260,   -3 }, /* (224) expr ::= LP expr RP */
  {  260,   -1 }, /* (225) expr ::= ID */
  {  260,   -3 }, /* (226) expr ::= ID DOT ID */
  {  260,   -3 }, /* (227) expr ::= ID DOT STAR */
  {  260,   -1 }, /* (228) expr ::= INTEGER */
  {  260,   -2 }, /* (229) expr ::= MINUS INTEGER */
  {  260,   -2 }, /* (230) expr ::= PLUS INTEGER */
  {  260,   -1 }, /* (231) expr ::= FLOAT */
  {  260,   -2 }, /* (232) expr ::= MINUS FLOAT */
  {  260,   -2 }, /* (233) expr ::= PLUS FLOAT */
  {  260,   -1 }, /* (234) expr ::= STRING */
  {  260,   -1 }, /* (235) expr ::= NOW */
  {  260,   -1 }, /* (236) expr ::= VARIABLE */
  {  260,   -2 }, /* (237) expr ::= PLUS VARIABLE */
  {  260,   -2 }, /* (238) expr ::= MINUS VARIABLE */
  {  260,   -1 }, /* (239) expr ::= BOOL */
  {  260,   -1 }, /* (240) expr ::= NULL */
  {  260,   -4 }, /* (241) expr ::= ID LP exprlist RP */
  {  260,   -4 }, /* (242) expr ::= ID LP STAR RP */
  {  260,   -3 }, /* (243) expr ::= expr IS NULL */
  {  260,   -4 }, /* (244) expr ::= expr IS NOT NULL */
  {  260,   -3 }, /* (245) expr ::= expr LT expr */
  {  260,   -3 }, /* (246) expr ::= expr GT expr */
  {  260,   -3 }, /* (247) expr ::= expr LE expr */
  {  260,   -3 }, /* (248) expr ::= expr GE expr */
  {  260,   -3 }, /* (249) expr ::= expr NE expr */
  {  260,   -3 }, /* (250) expr ::= expr EQ expr */
  {  260,   -5 }, /* (251) expr ::= expr BETWEEN expr AND expr */
  {  260,   -3 }, /* (252) expr ::= expr AND expr */
  {  260,   -3 }, /* (253) expr ::= expr OR expr */
  {  260,   -3 }, /* (254) expr ::= expr PLUS expr */
  {  260,   -3 }, /* (255) expr ::= expr MINUS expr */
  {  260,   -3 }, /* (256) expr ::= expr STAR expr */
  {  260,   -3 }, /* (257) expr ::= expr SLASH expr */
  {  260,   -3 }, /* (258) expr ::= expr REM expr */
  {  260,   -3 }, /* (259) expr ::= expr LIKE expr */
  {  260,   -5 }, /* (260) expr ::= expr IN LP exprlist RP */
  {  270,   -3 }, /* (261) exprlist ::= exprlist COMMA expritem */
  {  270,   -1 }, /* (262) exprlist ::= expritem */
  {  271,   -1 }, /* (263) expritem ::= expr */
  {  271,    0 }, /* (264) expritem ::= */
  {  195,   -3 }, /* (265) cmd ::= RESET QUERY CACHE */
  {  195,   -3 }, /* (266) cmd ::= SYNCDB ids REPLICA */
  {  195,   -7 }, /* (267) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  195,   -7 }, /* (268) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  195,   -7 }, /* (269) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  195,   -7 }, /* (270) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  195,   -8 }, /* (271) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  195,   -9 }, /* (272) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  195,   -7 }, /* (273) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  195,   -7 }, /* (274) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  195,   -7 }, /* (275) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  195,   -7 }, /* (276) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  195,   -8 }, /* (277) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  195,   -3 }, /* (278) cmd ::= KILL CONNECTION INTEGER */
  {  195,   -5 }, /* (279) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  195,   -5 }, /* (280) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy254, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy171);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
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
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy171);}
        break;
      case 58: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 59: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==59);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy254, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy471, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy471, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy171.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy171.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy171.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy171.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy171.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy171.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy171.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy171.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy171 = yylhsminor.yy171;
        break;
      case 84: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy413 = yymsp[0].minor.yy413; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy254); yymsp[1].minor.yy254.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 101: /* db_optr ::= db_optr cache */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 102: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 103: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 104: /* db_optr ::= db_optr days */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 105: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 106: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 107: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 108: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 109: /* db_optr ::= db_optr wal */
      case 124: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==124);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 110: /* db_optr ::= db_optr fsync */
      case 125: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==125);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 111: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 112: /* db_optr ::= db_optr prec */
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 113: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.keep = yymsp[0].minor.yy413; }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 114: /* db_optr ::= db_optr update */
      case 126: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==126);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 115: /* db_optr ::= db_optr cachelast */
      case 127: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==127);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 116: /* topic_optr ::= db_optr */
      case 128: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==128);
{ yylhsminor.yy254 = yymsp[0].minor.yy254; yylhsminor.yy254.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy254 = yylhsminor.yy254;
        break;
      case 117: /* topic_optr ::= topic_optr partitions */
      case 129: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==129);
{ yylhsminor.yy254 = yymsp[-1].minor.yy254; yylhsminor.yy254.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy254); yymsp[1].minor.yy254.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 130: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy471, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy471 = yylhsminor.yy471;
        break;
      case 131: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy157 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy471, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy157;  // negative value of name length
    tSetColumnType(&yylhsminor.yy471, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy471 = yylhsminor.yy471;
        break;
      case 132: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy471, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy471 = yylhsminor.yy471;
        break;
      case 133: /* signed ::= INTEGER */
{ yylhsminor.yy157 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy157 = yylhsminor.yy157;
        break;
      case 134: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy157 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 135: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy157 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 139: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy438;}
        break;
      case 140: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy544);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy438 = pCreateTable;
}
  yymsp[0].minor.yy438 = yylhsminor.yy438;
        break;
      case 141: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy438->childTableInfo, &yymsp[0].minor.yy544);
  yylhsminor.yy438 = yymsp[-1].minor.yy438;
}
  yymsp[-1].minor.yy438 = yylhsminor.yy438;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-1].minor.yy413, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy438 = yylhsminor.yy438;
        break;
      case 143: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy438 = tSetCreateTableInfo(yymsp[-5].minor.yy413, yymsp[-1].minor.yy413, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy438 = yylhsminor.yy438;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy544 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy413, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy544 = yylhsminor.yy544;
        break;
      case 145: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy544 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy413, yymsp[-1].minor.yy413, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy544 = yylhsminor.yy544;
        break;
      case 146: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy413, &yymsp[0].minor.yy0); yylhsminor.yy413 = yymsp[-2].minor.yy413;  }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 147: /* tagNamelist ::= ids */
{yylhsminor.yy413 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy413, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 148: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy438 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy24, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy438, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy438 = yylhsminor.yy438;
        break;
      case 149: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy413, &yymsp[0].minor.yy471); yylhsminor.yy413 = yymsp[-2].minor.yy413;  }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 150: /* columnlist ::= column */
{yylhsminor.yy413 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy413, &yymsp[0].minor.yy471);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 151: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy471, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy471);
}
  yymsp[-1].minor.yy471 = yylhsminor.yy471;
        break;
      case 152: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy413 = tVariantListAppend(yymsp[-2].minor.yy413, &yymsp[0].minor.yy394, -1);    }
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 153: /* tagitemlist ::= tagitem */
{ yylhsminor.yy413 = tVariantListAppend(NULL, &yymsp[0].minor.yy394, -1); }
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 154: /* tagitem ::= INTEGER */
      case 155: /* tagitem ::= FLOAT */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= STRING */ yytestcase(yyruleno==156);
      case 157: /* tagitem ::= BOOL */ yytestcase(yyruleno==157);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy394, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy394 = yylhsminor.yy394;
        break;
      case 158: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy394, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy394 = yylhsminor.yy394;
        break;
      case 159: /* tagitem ::= MINUS INTEGER */
      case 160: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==162);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy394, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy394 = yylhsminor.yy394;
        break;
      case 163: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy24 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy413, yymsp[-11].minor.yy292, yymsp[-10].minor.yy370, yymsp[-4].minor.yy413, yymsp[-3].minor.yy413, &yymsp[-9].minor.yy136, &yymsp[-8].minor.yy251, &yymsp[-7].minor.yy256, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy413, &yymsp[0].minor.yy262, &yymsp[-1].minor.yy262, yymsp[-2].minor.yy370);
}
  yymsp[-13].minor.yy24 = yylhsminor.yy24;
        break;
      case 164: /* select ::= LP select RP */
{yymsp[-2].minor.yy24 = yymsp[-1].minor.yy24;}
        break;
      case 165: /* union ::= select */
{ yylhsminor.yy413 = setSubclause(NULL, yymsp[0].minor.yy24); }
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 166: /* union ::= union UNION ALL select */
{ yylhsminor.yy413 = appendSelectClause(yymsp[-3].minor.yy413, yymsp[0].minor.yy24); }
  yymsp[-3].minor.yy413 = yylhsminor.yy413;
        break;
      case 167: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy413, NULL, TSDB_SQL_SELECT); }
        break;
      case 168: /* select ::= SELECT selcollist */
{
  yylhsminor.yy24 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy413, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy24 = yylhsminor.yy24;
        break;
      case 169: /* sclp ::= selcollist COMMA */
{yylhsminor.yy413 = yymsp[-1].minor.yy413;}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
        break;
      case 170: /* sclp ::= */
      case 200: /* orderby_opt ::= */ yytestcase(yyruleno==200);
{yymsp[1].minor.yy413 = 0;}
        break;
      case 171: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy413 = tSqlExprListAppend(yymsp[-3].minor.yy413, yymsp[-1].minor.yy370,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy413 = yylhsminor.yy413;
        break;
      case 172: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy413 = tSqlExprListAppend(yymsp[-1].minor.yy413, pNode, 0, 0);
}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
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
{yymsp[-1].minor.yy292 = yymsp[0].minor.yy292;}
        break;
      case 180: /* sub ::= LP union RP */
{yymsp[-2].minor.yy292 = addSubqueryElem(NULL, yymsp[-1].minor.yy413, NULL);}
        break;
      case 181: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy292 = addSubqueryElem(NULL, yymsp[-2].minor.yy413, &yymsp[0].minor.yy0);}
        break;
      case 182: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy292 = addSubqueryElem(yymsp[-5].minor.yy292, yymsp[-2].minor.yy413, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy292 = yylhsminor.yy292;
        break;
      case 183: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy292 = yylhsminor.yy292;
        break;
      case 184: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy292 = yylhsminor.yy292;
        break;
      case 185: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(yymsp[-3].minor.yy292, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy292 = yylhsminor.yy292;
        break;
      case 186: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy292 = setTableNameList(yymsp[-4].minor.yy292, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy292 = yylhsminor.yy292;
        break;
      case 187: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 188: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy136.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy136.offset.n = 0;}
        break;
      case 189: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy136.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy136.offset = yymsp[-1].minor.yy0;}
        break;
      case 190: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy136, 0, sizeof(yymsp[1].minor.yy136));}
        break;
      case 191: /* session_option ::= */
{yymsp[1].minor.yy251.col.n = 0; yymsp[1].minor.yy251.gap.n = 0;}
        break;
      case 192: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy251.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy251.gap = yymsp[-1].minor.yy0;
}
        break;
      case 193: /* windowstate_option ::= */
{yymsp[1].minor.yy256.col.n = 0;}
        break;
      case 194: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yymsp[-3].minor.yy256.col = yymsp[-1].minor.yy0;
}
        break;
      case 195: /* fill_opt ::= */
{ yymsp[1].minor.yy413 = 0;     }
        break;
      case 196: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy413, &A, -1, 0);
    yymsp[-5].minor.yy413 = yymsp[-1].minor.yy413;
}
        break;
      case 197: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy413 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 198: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 199: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 201: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy413 = yymsp[0].minor.yy413;}
        break;
      case 202: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy413 = tVariantListAppend(yymsp[-3].minor.yy413, &yymsp[-1].minor.yy394, yymsp[0].minor.yy60);
}
  yymsp[-3].minor.yy413 = yylhsminor.yy413;
        break;
      case 203: /* sortlist ::= item sortorder */
{
  yylhsminor.yy413 = tVariantListAppend(NULL, &yymsp[-1].minor.yy394, yymsp[0].minor.yy60);
}
  yymsp[-1].minor.yy413 = yylhsminor.yy413;
        break;
      case 204: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy394, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy394 = yylhsminor.yy394;
        break;
      case 205: /* sortorder ::= ASC */
{ yymsp[0].minor.yy60 = TSDB_ORDER_ASC; }
        break;
      case 206: /* sortorder ::= DESC */
{ yymsp[0].minor.yy60 = TSDB_ORDER_DESC;}
        break;
      case 207: /* sortorder ::= */
{ yymsp[1].minor.yy60 = TSDB_ORDER_ASC; }
        break;
      case 208: /* groupby_opt ::= */
{ yymsp[1].minor.yy413 = 0;}
        break;
      case 209: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy413 = yymsp[0].minor.yy413;}
        break;
      case 210: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy413 = tVariantListAppend(yymsp[-2].minor.yy413, &yymsp[0].minor.yy394, -1);
}
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 211: /* grouplist ::= item */
{
  yylhsminor.yy413 = tVariantListAppend(NULL, &yymsp[0].minor.yy394, -1);
}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 212: /* having_opt ::= */
      case 222: /* where_opt ::= */ yytestcase(yyruleno==222);
      case 264: /* expritem ::= */ yytestcase(yyruleno==264);
{yymsp[1].minor.yy370 = 0;}
        break;
      case 213: /* having_opt ::= HAVING expr */
      case 223: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==223);
{yymsp[-1].minor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 214: /* limit_opt ::= */
      case 218: /* slimit_opt ::= */ yytestcase(yyruleno==218);
{yymsp[1].minor.yy262.limit = -1; yymsp[1].minor.yy262.offset = 0;}
        break;
      case 215: /* limit_opt ::= LIMIT signed */
      case 219: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==219);
{yymsp[-1].minor.yy262.limit = yymsp[0].minor.yy157;  yymsp[-1].minor.yy262.offset = 0;}
        break;
      case 216: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy262.limit = yymsp[-2].minor.yy157;  yymsp[-3].minor.yy262.offset = yymsp[0].minor.yy157;}
        break;
      case 217: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy262.limit = yymsp[0].minor.yy157;  yymsp[-3].minor.yy262.offset = yymsp[-2].minor.yy157;}
        break;
      case 220: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy262.limit = yymsp[-2].minor.yy157;  yymsp[-3].minor.yy262.offset = yymsp[0].minor.yy157;}
        break;
      case 221: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy262.limit = yymsp[0].minor.yy157;  yymsp[-3].minor.yy262.offset = yymsp[-2].minor.yy157;}
        break;
      case 224: /* expr ::= LP expr RP */
{yylhsminor.yy370 = yymsp[-1].minor.yy370; yylhsminor.yy370->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy370->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 225: /* expr ::= ID */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 226: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 227: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 228: /* expr ::= INTEGER */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 229: /* expr ::= MINUS INTEGER */
      case 230: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==230);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 231: /* expr ::= FLOAT */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 232: /* expr ::= MINUS FLOAT */
      case 233: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 234: /* expr ::= STRING */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 235: /* expr ::= NOW */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 236: /* expr ::= VARIABLE */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 237: /* expr ::= PLUS VARIABLE */
      case 238: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==238);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy370 = yylhsminor.yy370;
        break;
      case 239: /* expr ::= BOOL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 240: /* expr ::= NULL */
{ yylhsminor.yy370 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
        break;
      case 241: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy370 = tSqlExprCreateFunction(yymsp[-1].minor.yy413, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 242: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy370 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 243: /* expr ::= expr IS NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 244: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-3].minor.yy370, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy370 = yylhsminor.yy370;
        break;
      case 245: /* expr ::= expr LT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 246: /* expr ::= expr GT expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 247: /* expr ::= expr LE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 248: /* expr ::= expr GE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 249: /* expr ::= expr NE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 250: /* expr ::= expr EQ expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 251: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy370); yylhsminor.yy370 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy370, yymsp[-2].minor.yy370, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy370, TK_LE), TK_AND);}
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 252: /* expr ::= expr AND expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 253: /* expr ::= expr OR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 254: /* expr ::= expr PLUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 255: /* expr ::= expr MINUS expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 256: /* expr ::= expr STAR expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 257: /* expr ::= expr SLASH expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 258: /* expr ::= expr REM expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 259: /* expr ::= expr LIKE expr */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
  yymsp[-2].minor.yy370 = yylhsminor.yy370;
        break;
      case 260: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy370 = tSqlExprCreate(yymsp[-4].minor.yy370, (tSqlExpr*)yymsp[-1].minor.yy413, TK_IN); }
  yymsp[-4].minor.yy370 = yylhsminor.yy370;
        break;
      case 261: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy413 = tSqlExprListAppend(yymsp[-2].minor.yy413,yymsp[0].minor.yy370,0, 0);}
  yymsp[-2].minor.yy413 = yylhsminor.yy413;
        break;
      case 262: /* exprlist ::= expritem */
{yylhsminor.yy413 = tSqlExprListAppend(0,yymsp[0].minor.yy370,0, 0);}
  yymsp[0].minor.yy413 = yylhsminor.yy413;
        break;
      case 263: /* expritem ::= expr */
{yylhsminor.yy370 = yymsp[0].minor.yy370;}
  yymsp[0].minor.yy370 = yylhsminor.yy370;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy394, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy413, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
