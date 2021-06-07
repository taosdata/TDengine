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
#define YYNOCODE 268
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateDbInfo yy22;
  TAOS_FIELD yy47;
  SRelationInfo* yy52;
  SCreateAcctInfo yy83;
  SSessionWindowVal yy84;
  tSqlExpr* yy162;
  SWindowStateVal yy176;
  int yy196;
  SLimitVal yy230;
  SArray* yy325;
  SIntervalVal yy328;
  int64_t yy373;
  SCreateTableSql* yy422;
  tVariant yy442;
  SCreatedTableInfo yy504;
  SSqlNode* yy536;
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
#define YYNRULE              283
#define YYNRULE_WITH_ACTION  283
#define YYNTOKEN             189
#define YY_MAX_SHIFT         341
#define YY_MIN_SHIFTREDUCE   542
#define YY_MAX_SHIFTREDUCE   824
#define YY_ERROR_ACTION      825
#define YY_ACCEPT_ACTION     826
#define YY_NO_ACTION         827
#define YY_MIN_REDUCE        828
#define YY_MAX_REDUCE        1110
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
#define YY_ACTTAB_COUNT (724)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    22,  590,  193,  999,  195,  144,  218,  339,  195,  591,
 /*    10 */   622,  826,  341,   52,   53, 1086,   56,   57,  227, 1087,
 /*    20 */   230,   46,  590,   55,  283,   60,   58,   62,   59,  964,
 /*    30 */   591,  199,  224,   51,   50,  195,  978,   49,   48,   47,
 /*    40 */    52,   53,   34,   56,   57,  226, 1087,  230,   46,  590,
 /*    50 */    55,  283,   60,   58,   62,   59,  151,  591,  235,  990,
 /*    60 */    51,   50,  978,  151,   49,   48,   47,   53,  996,   56,
 /*    70 */    57,  266,  265,  230,   46,  259,   55,  283,   60,   58,
 /*    80 */    62,   59, 1083,   75,  220,  151,   51,   50,  975,  151,
 /*    90 */    49,   48,   47,  543,  544,  545,  546,  547,  548,  549,
 /*   100 */   550,  551,  552,  553,  554,  555,  340,  246,  299,  219,
 /*   110 */    76,   52,   53,  237,   56,   57,  963,  978,  230,   46,
 /*   120 */    94,   55,  283,   60,   58,   62,   59, 1036,  990,  278,
 /*   130 */   299,   51,   50,  763, 1035,   49,   48,   47,   52,   54,
 /*   140 */    82,   56,   57,  976,  221,  230,   46,   41,   55,  283,
 /*   150 */    60,   58,   62,   59,  280,  966,   87,  874,   51,   50,
 /*   160 */    88,  178,   49,   48,   47,  325,   40,  297,  334,  333,
 /*   170 */   296,  295,  294,  332,  293,  331,  330,  329,  292,  328,
 /*   180 */   327,  938,  926,  927,  928,  929,  930,  931,  932,  933,
 /*   190 */   934,  935,  936,  937,  939,  940,   56,   57,   34,  990,
 /*   200 */   230,   46,  972,   55,  283,   60,   58,   62,   59,   49,
 /*   210 */    48,   47,   23,   51,   50,  222,  977,   49,   48,   47,
 /*   220 */   229,  778, 1082,   34,  767,  769,  770,  772,  773,  204,
 /*   230 */    34,  229,  778,  335,  907,  767,  205,  770,  672,  773,
 /*   240 */   233,  128,  127,  203,  975, 1081,  236,   34,   40,  115,
 /*   250 */   334,  333,  215,  216,  325,  332,  282,  331,  330,  329,
 /*   260 */    89,  328,  327,  215,  216,  213,  315,  314,  946,  974,
 /*   270 */   944,  945,  234,   82,   77,  947,  975,  949,  950,  948,
 /*   280 */    41,  951,  952,   60,   58,   62,   59,  711,  768,  304,
 /*   290 */   771,   51,   50,  975,  590,   49,   48,   47,   34,  113,
 /*   300 */   107,  118,  591,  258, 1106,   74,  117,  123,  126,  116,
 /*   310 */   240,  696,  212,  251,  693,  120,  694,  245,  695,    5,
 /*   320 */    37,  168,  255,  254,   61,  214,  167,  101,   96,  100,
 /*   330 */   779,   34,    1,  166,   91,   61,  775,  776,    3,  179,
 /*   340 */   305,  779,  242,  243,  975,  197,  284,  775,  187,  185,
 /*   350 */   183,  744,  745,  774, 1098,  182,  131,  130,  129,   34,
 /*   360 */   708,   34,   34,  765,  774,  715,  961,  962,   33,  965,
 /*   370 */    51,   50,  198,  306,   49,   48,   47,  975,   35,   34,
 /*   380 */   241,   34,  239,  200,  303,  302,   67,  247,    8,  244,
 /*   390 */    13,  310,  309,  883,   93,   90,  260,  178,   28,  766,
 /*   400 */   777,  307,  875,  311,  312,  975,  178,  975,  975,  338,
 /*   410 */   337,  136,  142,  140,  139,  146,   64,   79,   25,   80,
 /*   420 */   697,  313,   24,  317,  262,  975,  262,  975,   24,  228,
 /*   430 */    68,   70,  194,  727,  735,   35,  736,   35,  201,   64,
 /*   440 */   799,   92,  782,   15,   64,   14,  780,   32,  202,  106,
 /*   450 */   289,  105,   44,  682,   17,  286,   16,  684,   19,  288,
 /*   460 */    18,  700,  683,  701,  112,  698,  111,  699,   73,  208,
 /*   470 */   671,  125,  124,   71,    6,   21, 1046,   20,  209,  207,
 /*   480 */   192,  206, 1045,  196,  231, 1042,  256,  143, 1041,  232,
 /*   490 */   316,  998, 1009, 1006, 1007, 1011, 1028,  145,  149,  991,
 /*   500 */   263,  141, 1027,  973,  272,  162,  163,  942,  971,  164,
 /*   510 */   165,  726,  886,  291,   42,  190,   38,  988,  300,  281,
 /*   520 */   153,  882,  152,  156,  301, 1105,   72,  103,  267,  223,
 /*   530 */  1104, 1101,   69,  169,  269,  276,   63,  308, 1097,  279,
 /*   540 */   109,  154,  277,  275, 1096,  155, 1093,  170,  273,  904,
 /*   550 */    39,   36,   43,  191,  157,  271,  871,  119,  869,  121,
 /*   560 */   122,  268,  867,  866,  248,  181,  864,  863,  862,  861,
 /*   570 */   860,  859,  184,  186,  856,  854,  852,  850,  188,  847,
 /*   580 */   189,   45,  261,   78,   83,  270, 1029,  326,  114,  318,
 /*   590 */   319,  320,  321,  322,  323,  217,  324,  238,  290,  336,
 /*   600 */   824,  249,  210,  211,   97,   98,  250,  823,  252,  253,
 /*   610 */   822,  257,  805,  804,  285,  262,    9,  173,  177,  865,
 /*   620 */   905,  171,  172,  174,  176,  175,  132,  133,  858,  906,
 /*   630 */     2,  134,  857,    4,  135,  849,   81,  848,  703,   29,
 /*   640 */   264,   84,  160,  158,  159,  161,  148,  728,  147,   85,
 /*   650 */   954,  731,  733,   86,  225,  274,   30,  737,   31,  150,
 /*   660 */    10,   11,  781,   95,    7,   12,  783,   26,   27,  287,
 /*   670 */   635,  631,  629,   93,  628,  627,  624,  594,  298,   99,
 /*   680 */    65,   35,  674,  102,   66,  673,  670,  616,  104,  614,
 /*   690 */   108,  606,  612,  608,  610,  604,  602,  638,  110,  637,
 /*   700 */   636,  634,  633,  632,  630,  626,  625,  180,  592,  559,
 /*   710 */   557,  828,  827,  827,  137,  827,  827,  827,  827,  827,
 /*   720 */   827,  827,  827,  138,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   256,    1,  256,  192,  256,  192,  191,  192,  256,    9,
 /*    10 */     5,  189,  190,   13,   14,  267,   16,   17,  266,  267,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,    0,
 /*    30 */     9,  256,  235,   33,   34,  256,  239,   37,   38,   39,
 /*    40 */    13,   14,  192,   16,   17,  266,  267,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  192,    9,  235,  237,
 /*    60 */    33,   34,  239,  192,   37,   38,   39,   14,  257,   16,
 /*    70 */    17,  258,  259,   20,   21,  253,   23,   24,   25,   26,
 /*    80 */    27,   28,  256,  198,  234,  192,   33,   34,  238,  192,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  192,   79,   61,
 /*   110 */   110,   13,   14,  235,   16,   17,  231,  239,   20,   21,
 /*   120 */   198,   23,   24,   25,   26,   27,   28,  263,  237,  265,
 /*   130 */    79,   33,   34,  106,  263,   37,   38,   39,   13,   14,
 /*   140 */   105,   16,   17,  228,  253,   20,   21,  112,   23,   24,
 /*   150 */    25,   26,   27,   28,  261,  233,  263,  197,   33,   34,
 /*   160 */   263,  201,   37,   38,   39,   81,   89,   90,   91,   92,
 /*   170 */    93,   94,   95,   96,   97,   98,   99,  100,  101,  102,
 /*   180 */   103,  212,  213,  214,  215,  216,  217,  218,  219,  220,
 /*   190 */   221,  222,  223,  224,  225,  226,   16,   17,  192,  237,
 /*   200 */    20,   21,  192,   23,   24,   25,   26,   27,   28,   37,
 /*   210 */    38,   39,   44,   33,   34,  253,  239,   37,   38,   39,
 /*   220 */     1,    2,  256,  192,    5,    5,    7,    7,    9,   61,
 /*   230 */   192,    1,    2,  210,  211,    5,   68,    7,    5,    9,
 /*   240 */   234,   73,   74,   75,  238,  256,  236,  192,   89,   76,
 /*   250 */    91,   92,   33,   34,   81,   96,   37,   98,   99,  100,
 /*   260 */   240,  102,  103,   33,   34,  256,   33,   34,  212,  238,
 /*   270 */   214,  215,  234,  105,  254,  219,  238,  221,  222,  223,
 /*   280 */   112,  225,  226,   25,   26,   27,   28,   37,    5,  234,
 /*   290 */     7,   33,   34,  238,    1,   37,   38,   39,  192,   62,
 /*   300 */    63,   64,    9,  135,  239,  137,   69,   70,   71,   72,
 /*   310 */    68,    2,  144,  136,    5,   78,    7,   68,    9,   62,
 /*   320 */    63,   64,  145,  146,  105,  256,   69,   70,   71,   72,
 /*   330 */   111,  192,  199,  200,  198,  105,  117,  117,  195,  196,
 /*   340 */   234,  111,   33,   34,  238,  256,   15,  117,   62,   63,
 /*   350 */    64,  125,  126,  134,  239,   69,   70,   71,   72,  192,
 /*   360 */    88,  192,  192,    1,  134,  115,  230,  231,  232,  233,
 /*   370 */    33,   34,  256,  234,   37,   38,   39,  238,   88,  192,
 /*   380 */   138,  192,  140,  256,  142,  143,   88,  138,  116,  140,
 /*   390 */   105,  142,  143,  197,  109,  110,  106,  201,  105,   37,
 /*   400 */   117,  234,  197,  234,  234,  238,  201,  238,  238,   65,
 /*   410 */    66,   67,   62,   63,   64,   88,   88,  106,   88,  106,
 /*   420 */   111,  234,   88,  234,  113,  238,  113,  238,   88,   60,
 /*   430 */   132,   88,  256,  106,  106,   88,  106,   88,  256,   88,
 /*   440 */   106,   88,  111,  139,   88,  141,  106,  105,  256,  139,
 /*   450 */   108,  141,  255,  106,  139,  106,  141,  106,  139,  106,
 /*   460 */   141,    5,  106,    7,  139,    5,  141,    7,  105,  256,
 /*   470 */   107,   76,   77,  130,  105,  139,  229,  141,  256,  256,
 /*   480 */   256,  256,  229,  256,  229,  229,  192,  192,  229,  229,
 /*   490 */   229,  192,  192,  192,  192,  192,  264,  192,  192,  237,
 /*   500 */   237,   60,  264,  237,  192,  241,  192,  227,  192,  192,
 /*   510 */   192,  117,  192,  192,  192,  192,  192,  252,  192,  123,
 /*   520 */   250,  192,  251,  247,  192,  192,  129,  192,  260,  260,
 /*   530 */   192,  192,  131,  192,  260,  260,  128,  192,  192,  127,
 /*   540 */   192,  249,  122,  121,  192,  248,  192,  192,  120,  192,
 /*   550 */   192,  192,  192,  192,  246,  119,  192,  192,  192,  192,
 /*   560 */   192,  118,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   570 */   192,  192,  192,  192,  192,  192,  192,  192,  192,  192,
 /*   580 */   192,  133,  193,  193,  193,  193,  193,  104,   87,   86,
 /*   590 */    50,   83,   85,   54,   84,  193,   82,  193,  193,   79,
 /*   600 */     5,  147,  193,  193,  198,  198,    5,    5,  147,    5,
 /*   610 */     5,  136,   91,   90,  108,  113,  105,  203,  202,  193,
 /*   620 */   209,  208,  207,  206,  205,  204,  194,  194,  193,  211,
 /*   630 */   199,  194,  193,  195,  194,  193,  114,  193,  106,  105,
 /*   640 */    88,   88,  243,  245,  244,  242,   88,  106,  105,  105,
 /*   650 */   227,  106,  106,  105,    1,  105,   88,  106,   88,  105,
 /*   660 */   124,  124,  106,   76,  105,  105,  111,  105,  105,  108,
 /*   670 */     9,    5,    5,  109,    5,    5,    5,   80,   15,   76,
 /*   680 */    16,   88,    5,  141,   16,    5,  106,    5,  141,    5,
 /*   690 */   141,    5,    5,    5,    5,    5,    5,    5,  141,    5,
 /*   700 */     5,    5,    5,    5,    5,    5,    5,   88,   80,   60,
 /*   710 */    59,    0,  268,  268,   21,  268,  268,  268,  268,  268,
 /*   720 */   268,  268,  268,   21,  268,  268,  268,  268,  268,  268,
 /*   730 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   740 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   750 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   760 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   770 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   780 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   790 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   800 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   810 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   820 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   830 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   840 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   850 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   860 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   870 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   880 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   890 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   900 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   910 */   268,  268,  268,
};
#define YY_SHIFT_COUNT    (341)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (711)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   77,   77,  159,  159,   51,  219,  230,  293,   21,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,    0,   48,  230,  309,  309,  309,   35,   35,
 /*    30 */    21,   21,   21,   29,   21,   21,  173,   51,   84,   84,
 /*    40 */     5,  724,  724,  724,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    60 */   230,  230,  230,  230,  309,  309,  309,  233,  233,  233,
 /*    70 */   233,  233,  233,  233,   21,   21,   21,  250,   21,   21,
 /*    80 */    21,   35,   35,   21,   21,   21,   21,  226,  226,  272,
 /*    90 */    35,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,  441,  441,  441,  394,  394,  394,  441,
 /*   150 */   394,  441,  397,  401,  408,  396,  412,  420,  422,  428,
 /*   160 */   436,  443,  448,  441,  441,  441,  483,   51,   51,  441,
 /*   170 */   441,  501,  503,  540,  508,  507,  539,  510,  514,  483,
 /*   180 */     5,  441,  520,  520,  441,  520,  441,  520,  441,  441,
 /*   190 */   724,  724,   27,   98,  125,   98,   98,   53,  180,  258,
 /*   200 */   258,  258,  258,  237,  257,  286,  337,  337,  337,  337,
 /*   210 */   242,  249,  177,  172,  172,  220,  283,  285,  344,  350,
 /*   220 */   290,  311,  313,  327,  328,  330,  334,  340,  362,  369,
 /*   230 */   331,  298,  343,  347,  349,  351,  353,  356,  342,  304,
 /*   240 */   310,  315,  456,  460,  319,  325,  363,  336,  395,  595,
 /*   250 */   454,  601,  602,  461,  604,  605,  521,  523,  475,  502,
 /*   260 */   506,  511,  522,  532,  534,  552,  553,  541,  543,  545,
 /*   270 */   558,  544,  546,  548,  653,  550,  551,  554,  568,  536,
 /*   280 */   570,  537,  556,  559,  555,  560,  506,  562,  561,  563,
 /*   290 */   564,  587,  661,  666,  667,  669,  670,  671,  597,  663,
 /*   300 */   603,  664,  542,  547,  593,  593,  593,  593,  668,  549,
 /*   310 */   557,  593,  593,  593,  677,  680,  580,  593,  682,  684,
 /*   320 */   686,  687,  688,  689,  690,  691,  692,  694,  695,  696,
 /*   330 */   697,  698,  699,  700,  701,  619,  628,  693,  702,  649,
 /*   340 */   651,  711,
};
#define YY_REDUCE_COUNT (191)
#define YY_REDUCE_MIN   (-256)
#define YY_REDUCE_MAX   (444)
static const short yy_reduce_ofst[] = {
 /*     0 */  -178,  -31,  -31,   56,   56,  136, -248, -221, -187, -150,
 /*    10 */  -136, -107,    6,   38,   55,  106,  139,  167,  169,  170,
 /*    20 */   187,  189, -189, -185, -252, -203, -177, -122, -109,  -38,
 /*    30 */  -129, -103,   10,  -78,  -85,   31,  -40, -115,  196,  205,
 /*    40 */    23,   20,  133,  143, -256, -254, -225, -174,  -34,  -11,
 /*    50 */     9,   69,   89,  116,  127,  176,  182,  192,  213,  222,
 /*    60 */   223,  224,  225,  227,  -23,   65,  115,  247,  253,  255,
 /*    70 */   256,  259,  260,  261,  294,  295,  299,  197,  300,  301,
 /*    80 */   302,  262,  263,  303,  305,  306,  312,  232,  238,  264,
 /*    90 */   266,  314,  316,  317,  318,  320,  321,  322,  323,  324,
 /*   100 */   326,  329,  332,  333,  335,  338,  339,  341,  345,  346,
 /*   110 */   348,  352,  354,  355,  357,  358,  359,  360,  361,  364,
 /*   120 */   365,  366,  367,  368,  370,  371,  372,  373,  374,  375,
 /*   130 */   376,  377,  378,  379,  380,  381,  382,  383,  384,  385,
 /*   140 */   386,  387,  388,  389,  390,  391,  268,  269,  274,  392,
 /*   150 */   275,  393,  265,  271,  270,  292,  297,  276,  308,  398,
 /*   160 */   400,  399,  403,  402,  404,  405,  280,  406,  407,  409,
 /*   170 */   410,  411,  413,  415,  414,  417,  421,  419,  416,  423,
 /*   180 */   418,  426,  432,  433,  435,  437,  439,  440,  442,  444,
 /*   190 */   431,  438,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   825,  941,  884,  953,  872,  881, 1089, 1089,  825,  825,
 /*    10 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    20 */   825,  825, 1000,  844, 1089,  825,  825,  825,  825,  825,
 /*    30 */   825,  825,  825,  881,  825,  825,  887,  881,  887,  887,
 /*    40 */   825,  995,  925,  943,  825,  825,  825,  825,  825,  825,
 /*    50 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    60 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*    70 */   825,  825,  825,  825,  825,  825,  825, 1002, 1008, 1005,
 /*    80 */   825,  825,  825, 1010,  825,  825,  825, 1032, 1032,  993,
 /*    90 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   100 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   110 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  870,
 /*   120 */   825,  868,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   130 */   825,  825,  825,  825,  825,  825,  855,  825,  825,  825,
 /*   140 */   825,  825,  825,  846,  846,  846,  825,  825,  825,  846,
 /*   150 */   825,  846, 1039, 1043, 1037, 1025, 1033, 1024, 1020, 1018,
 /*   160 */  1016, 1015, 1047,  846,  846,  846,  885,  881,  881,  846,
 /*   170 */   846,  903,  901,  899,  891,  897,  893,  895,  889,  873,
 /*   180 */   825,  846,  879,  879,  846,  879,  846,  879,  846,  846,
 /*   190 */   925,  943,  825, 1048,  825, 1088, 1038, 1078, 1077, 1084,
 /*   200 */  1076, 1075, 1074,  825,  825,  825, 1070, 1071, 1073, 1072,
 /*   210 */   825,  825,  825, 1080, 1079,  825,  825,  825,  825,  825,
 /*   220 */   825,  825,  825,  825,  825,  825,  825,  825,  825, 1050,
 /*   230 */   825, 1044, 1040,  825,  825,  825,  825,  825,  825,  825,
 /*   240 */   825,  825,  825,  825,  825,  825,  955,  825,  825,  825,
 /*   250 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  992,
 /*   260 */   825,  825,  825,  825,  825, 1004, 1003,  825,  825,  825,
 /*   270 */   825,  825,  825,  825,  825,  825,  825,  825, 1034,  825,
 /*   280 */  1026,  825,  825,  825,  825,  825,  967,  825,  825,  825,
 /*   290 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   300 */   825,  825,  825,  825, 1107, 1102, 1103, 1100,  825,  825,
 /*   310 */   825, 1099, 1094, 1095,  825,  825,  825, 1092,  825,  825,
 /*   320 */   825,  825,  825,  825,  825,  825,  825,  825,  825,  825,
 /*   330 */   825,  825,  825,  825,  825,  909,  825,  853,  851,  825,
 /*   340 */   842,  825,
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
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*      COMMA => nothing */
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
    0,  /*         AS => nothing */
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
    0,  /*     MODIFY => nothing */
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
  /*   47 */ "MNODES",
  /*   48 */ "DNODES",
  /*   49 */ "ACCOUNTS",
  /*   50 */ "USERS",
  /*   51 */ "MODULES",
  /*   52 */ "QUERIES",
  /*   53 */ "CONNECTIONS",
  /*   54 */ "STREAMS",
  /*   55 */ "VARIABLES",
  /*   56 */ "SCORES",
  /*   57 */ "GRANTS",
  /*   58 */ "VNODES",
  /*   59 */ "IPTOKEN",
  /*   60 */ "DOT",
  /*   61 */ "CREATE",
  /*   62 */ "TABLE",
  /*   63 */ "STABLE",
  /*   64 */ "DATABASE",
  /*   65 */ "TABLES",
  /*   66 */ "STABLES",
  /*   67 */ "VGROUPS",
  /*   68 */ "DROP",
  /*   69 */ "TOPIC",
  /*   70 */ "DNODE",
  /*   71 */ "USER",
  /*   72 */ "ACCOUNT",
  /*   73 */ "USE",
  /*   74 */ "DESCRIBE",
  /*   75 */ "ALTER",
  /*   76 */ "PASS",
  /*   77 */ "PRIVILEGE",
  /*   78 */ "LOCAL",
  /*   79 */ "IF",
  /*   80 */ "EXISTS",
  /*   81 */ "PPS",
  /*   82 */ "TSERIES",
  /*   83 */ "DBS",
  /*   84 */ "STORAGE",
  /*   85 */ "QTIME",
  /*   86 */ "CONNS",
  /*   87 */ "STATE",
  /*   88 */ "COMMA",
  /*   89 */ "KEEP",
  /*   90 */ "CACHE",
  /*   91 */ "REPLICA",
  /*   92 */ "QUORUM",
  /*   93 */ "DAYS",
  /*   94 */ "MINROWS",
  /*   95 */ "MAXROWS",
  /*   96 */ "BLOCKS",
  /*   97 */ "CTIME",
  /*   98 */ "WAL",
  /*   99 */ "FSYNC",
  /*  100 */ "COMP",
  /*  101 */ "PRECISION",
  /*  102 */ "UPDATE",
  /*  103 */ "CACHELAST",
  /*  104 */ "PARTITIONS",
  /*  105 */ "LP",
  /*  106 */ "RP",
  /*  107 */ "UNSIGNED",
  /*  108 */ "TAGS",
  /*  109 */ "USING",
  /*  110 */ "AS",
  /*  111 */ "NULL",
  /*  112 */ "SELECT",
  /*  113 */ "UNION",
  /*  114 */ "ALL",
  /*  115 */ "DISTINCT",
  /*  116 */ "FROM",
  /*  117 */ "VARIABLE",
  /*  118 */ "INTERVAL",
  /*  119 */ "SESSION",
  /*  120 */ "STATE_WINDOW",
  /*  121 */ "FILL",
  /*  122 */ "SLIDING",
  /*  123 */ "ORDER",
  /*  124 */ "BY",
  /*  125 */ "ASC",
  /*  126 */ "DESC",
  /*  127 */ "GROUP",
  /*  128 */ "HAVING",
  /*  129 */ "LIMIT",
  /*  130 */ "OFFSET",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "WHERE",
  /*  134 */ "NOW",
  /*  135 */ "RESET",
  /*  136 */ "QUERY",
  /*  137 */ "SYNCDB",
  /*  138 */ "ADD",
  /*  139 */ "COLUMN",
  /*  140 */ "MODIFY",
  /*  141 */ "TAG",
  /*  142 */ "CHANGE",
  /*  143 */ "SET",
  /*  144 */ "KILL",
  /*  145 */ "CONNECTION",
  /*  146 */ "STREAM",
  /*  147 */ "COLON",
  /*  148 */ "ABORT",
  /*  149 */ "AFTER",
  /*  150 */ "ATTACH",
  /*  151 */ "BEFORE",
  /*  152 */ "BEGIN",
  /*  153 */ "CASCADE",
  /*  154 */ "CLUSTER",
  /*  155 */ "CONFLICT",
  /*  156 */ "COPY",
  /*  157 */ "DEFERRED",
  /*  158 */ "DELIMITERS",
  /*  159 */ "DETACH",
  /*  160 */ "EACH",
  /*  161 */ "END",
  /*  162 */ "EXPLAIN",
  /*  163 */ "FAIL",
  /*  164 */ "FOR",
  /*  165 */ "IGNORE",
  /*  166 */ "IMMEDIATE",
  /*  167 */ "INITIALLY",
  /*  168 */ "INSTEAD",
  /*  169 */ "MATCH",
  /*  170 */ "KEY",
  /*  171 */ "OF",
  /*  172 */ "RAISE",
  /*  173 */ "REPLACE",
  /*  174 */ "RESTRICT",
  /*  175 */ "ROW",
  /*  176 */ "STATEMENT",
  /*  177 */ "TRIGGER",
  /*  178 */ "VIEW",
  /*  179 */ "SEMI",
  /*  180 */ "NONE",
  /*  181 */ "PREV",
  /*  182 */ "LINEAR",
  /*  183 */ "IMPORT",
  /*  184 */ "TBNAME",
  /*  185 */ "JOIN",
  /*  186 */ "INSERT",
  /*  187 */ "INTO",
  /*  188 */ "VALUES",
  /*  189 */ "program",
  /*  190 */ "cmd",
  /*  191 */ "dbPrefix",
  /*  192 */ "ids",
  /*  193 */ "cpxName",
  /*  194 */ "ifexists",
  /*  195 */ "alter_db_optr",
  /*  196 */ "alter_topic_optr",
  /*  197 */ "acct_optr",
  /*  198 */ "ifnotexists",
  /*  199 */ "db_optr",
  /*  200 */ "topic_optr",
  /*  201 */ "pps",
  /*  202 */ "tseries",
  /*  203 */ "dbs",
  /*  204 */ "streams",
  /*  205 */ "storage",
  /*  206 */ "qtime",
  /*  207 */ "users",
  /*  208 */ "conns",
  /*  209 */ "state",
  /*  210 */ "intitemlist",
  /*  211 */ "intitem",
  /*  212 */ "keep",
  /*  213 */ "cache",
  /*  214 */ "replica",
  /*  215 */ "quorum",
  /*  216 */ "days",
  /*  217 */ "minrows",
  /*  218 */ "maxrows",
  /*  219 */ "blocks",
  /*  220 */ "ctime",
  /*  221 */ "wal",
  /*  222 */ "fsync",
  /*  223 */ "comp",
  /*  224 */ "prec",
  /*  225 */ "update",
  /*  226 */ "cachelast",
  /*  227 */ "partitions",
  /*  228 */ "typename",
  /*  229 */ "signed",
  /*  230 */ "create_table_args",
  /*  231 */ "create_stable_args",
  /*  232 */ "create_table_list",
  /*  233 */ "create_from_stable",
  /*  234 */ "columnlist",
  /*  235 */ "tagitemlist",
  /*  236 */ "tagNamelist",
  /*  237 */ "select",
  /*  238 */ "column",
  /*  239 */ "tagitem",
  /*  240 */ "selcollist",
  /*  241 */ "from",
  /*  242 */ "where_opt",
  /*  243 */ "interval_opt",
  /*  244 */ "session_option",
  /*  245 */ "windowstate_option",
  /*  246 */ "fill_opt",
  /*  247 */ "sliding_opt",
  /*  248 */ "groupby_opt",
  /*  249 */ "orderby_opt",
  /*  250 */ "having_opt",
  /*  251 */ "slimit_opt",
  /*  252 */ "limit_opt",
  /*  253 */ "union",
  /*  254 */ "sclp",
  /*  255 */ "distinct",
  /*  256 */ "expr",
  /*  257 */ "as",
  /*  258 */ "tablelist",
  /*  259 */ "sub",
  /*  260 */ "tmvar",
  /*  261 */ "sortlist",
  /*  262 */ "sortitem",
  /*  263 */ "item",
  /*  264 */ "sortorder",
  /*  265 */ "grouplist",
  /*  266 */ "exprlist",
  /*  267 */ "expritem",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW TOPICS",
 /*   3 */ "cmd ::= SHOW MNODES",
 /*   4 */ "cmd ::= SHOW DNODES",
 /*   5 */ "cmd ::= SHOW ACCOUNTS",
 /*   6 */ "cmd ::= SHOW USERS",
 /*   7 */ "cmd ::= SHOW MODULES",
 /*   8 */ "cmd ::= SHOW QUERIES",
 /*   9 */ "cmd ::= SHOW CONNECTIONS",
 /*  10 */ "cmd ::= SHOW STREAMS",
 /*  11 */ "cmd ::= SHOW VARIABLES",
 /*  12 */ "cmd ::= SHOW SCORES",
 /*  13 */ "cmd ::= SHOW GRANTS",
 /*  14 */ "cmd ::= SHOW VNODES",
 /*  15 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  16 */ "dbPrefix ::=",
 /*  17 */ "dbPrefix ::= ids DOT",
 /*  18 */ "cpxName ::=",
 /*  19 */ "cpxName ::= DOT ids",
 /*  20 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  21 */ "cmd ::= SHOW CREATE STABLE ids cpxName",
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
 /*  33 */ "cmd ::= DROP DNODE ids",
 /*  34 */ "cmd ::= DROP USER ids",
 /*  35 */ "cmd ::= DROP ACCOUNT ids",
 /*  36 */ "cmd ::= USE ids",
 /*  37 */ "cmd ::= DESCRIBE ids cpxName",
 /*  38 */ "cmd ::= ALTER USER ids PASS ids",
 /*  39 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids",
 /*  41 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids",
 /*  43 */ "cmd ::= ALTER LOCAL ids ids",
 /*  44 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  45 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  47 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "ids ::= ID",
 /*  49 */ "ids ::= STRING",
 /*  50 */ "ifexists ::= IF EXISTS",
 /*  51 */ "ifexists ::=",
 /*  52 */ "ifnotexists ::= IF NOT EXISTS",
 /*  53 */ "ifnotexists ::=",
 /*  54 */ "cmd ::= CREATE DNODE ids",
 /*  55 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  56 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  57 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  58 */ "cmd ::= CREATE USER ids PASS ids",
 /*  59 */ "pps ::=",
 /*  60 */ "pps ::= PPS INTEGER",
 /*  61 */ "tseries ::=",
 /*  62 */ "tseries ::= TSERIES INTEGER",
 /*  63 */ "dbs ::=",
 /*  64 */ "dbs ::= DBS INTEGER",
 /*  65 */ "streams ::=",
 /*  66 */ "streams ::= STREAMS INTEGER",
 /*  67 */ "storage ::=",
 /*  68 */ "storage ::= STORAGE INTEGER",
 /*  69 */ "qtime ::=",
 /*  70 */ "qtime ::= QTIME INTEGER",
 /*  71 */ "users ::=",
 /*  72 */ "users ::= USERS INTEGER",
 /*  73 */ "conns ::=",
 /*  74 */ "conns ::= CONNS INTEGER",
 /*  75 */ "state ::=",
 /*  76 */ "state ::= STATE ids",
 /*  77 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  78 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  79 */ "intitemlist ::= intitem",
 /*  80 */ "intitem ::= INTEGER",
 /*  81 */ "keep ::= KEEP intitemlist",
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
 /* 160 */ "select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
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
 /* 176 */ "from ::= FROM sub",
 /* 177 */ "sub ::= LP union RP",
 /* 178 */ "sub ::= LP union RP ids",
 /* 179 */ "sub ::= sub COMMA LP union RP ids",
 /* 180 */ "tablelist ::= ids cpxName",
 /* 181 */ "tablelist ::= ids cpxName ids",
 /* 182 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 183 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 184 */ "tmvar ::= VARIABLE",
 /* 185 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 186 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 187 */ "interval_opt ::=",
 /* 188 */ "session_option ::=",
 /* 189 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 190 */ "windowstate_option ::=",
 /* 191 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 192 */ "fill_opt ::=",
 /* 193 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 194 */ "fill_opt ::= FILL LP ID RP",
 /* 195 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 196 */ "sliding_opt ::=",
 /* 197 */ "orderby_opt ::=",
 /* 198 */ "orderby_opt ::= ORDER BY sortlist",
 /* 199 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 200 */ "sortlist ::= item sortorder",
 /* 201 */ "item ::= ids cpxName",
 /* 202 */ "sortorder ::= ASC",
 /* 203 */ "sortorder ::= DESC",
 /* 204 */ "sortorder ::=",
 /* 205 */ "groupby_opt ::=",
 /* 206 */ "groupby_opt ::= GROUP BY grouplist",
 /* 207 */ "grouplist ::= grouplist COMMA item",
 /* 208 */ "grouplist ::= item",
 /* 209 */ "having_opt ::=",
 /* 210 */ "having_opt ::= HAVING expr",
 /* 211 */ "limit_opt ::=",
 /* 212 */ "limit_opt ::= LIMIT signed",
 /* 213 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 214 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 215 */ "slimit_opt ::=",
 /* 216 */ "slimit_opt ::= SLIMIT signed",
 /* 217 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 218 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 219 */ "where_opt ::=",
 /* 220 */ "where_opt ::= WHERE expr",
 /* 221 */ "expr ::= LP expr RP",
 /* 222 */ "expr ::= ID",
 /* 223 */ "expr ::= ID DOT ID",
 /* 224 */ "expr ::= ID DOT STAR",
 /* 225 */ "expr ::= INTEGER",
 /* 226 */ "expr ::= MINUS INTEGER",
 /* 227 */ "expr ::= PLUS INTEGER",
 /* 228 */ "expr ::= FLOAT",
 /* 229 */ "expr ::= MINUS FLOAT",
 /* 230 */ "expr ::= PLUS FLOAT",
 /* 231 */ "expr ::= STRING",
 /* 232 */ "expr ::= NOW",
 /* 233 */ "expr ::= VARIABLE",
 /* 234 */ "expr ::= PLUS VARIABLE",
 /* 235 */ "expr ::= MINUS VARIABLE",
 /* 236 */ "expr ::= BOOL",
 /* 237 */ "expr ::= NULL",
 /* 238 */ "expr ::= ID LP exprlist RP",
 /* 239 */ "expr ::= ID LP STAR RP",
 /* 240 */ "expr ::= expr IS NULL",
 /* 241 */ "expr ::= expr IS NOT NULL",
 /* 242 */ "expr ::= expr LT expr",
 /* 243 */ "expr ::= expr GT expr",
 /* 244 */ "expr ::= expr LE expr",
 /* 245 */ "expr ::= expr GE expr",
 /* 246 */ "expr ::= expr NE expr",
 /* 247 */ "expr ::= expr EQ expr",
 /* 248 */ "expr ::= expr BETWEEN expr AND expr",
 /* 249 */ "expr ::= expr AND expr",
 /* 250 */ "expr ::= expr OR expr",
 /* 251 */ "expr ::= expr PLUS expr",
 /* 252 */ "expr ::= expr MINUS expr",
 /* 253 */ "expr ::= expr STAR expr",
 /* 254 */ "expr ::= expr SLASH expr",
 /* 255 */ "expr ::= expr REM expr",
 /* 256 */ "expr ::= expr LIKE expr",
 /* 257 */ "expr ::= expr IN LP exprlist RP",
 /* 258 */ "exprlist ::= exprlist COMMA expritem",
 /* 259 */ "exprlist ::= expritem",
 /* 260 */ "expritem ::= expr",
 /* 261 */ "expritem ::=",
 /* 262 */ "cmd ::= RESET QUERY CACHE",
 /* 263 */ "cmd ::= SYNCDB ids REPLICA",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 266 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 267 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 268 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 272 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 273 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 274 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 275 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 276 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 280 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 281 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 282 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 210: /* intitemlist */
    case 212: /* keep */
    case 234: /* columnlist */
    case 235: /* tagitemlist */
    case 236: /* tagNamelist */
    case 246: /* fill_opt */
    case 248: /* groupby_opt */
    case 249: /* orderby_opt */
    case 261: /* sortlist */
    case 265: /* grouplist */
{
taosArrayDestroy((yypminor->yy325));
}
      break;
    case 232: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy422));
}
      break;
    case 237: /* select */
{
destroySqlNode((yypminor->yy536));
}
      break;
    case 240: /* selcollist */
    case 254: /* sclp */
    case 266: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy325));
}
      break;
    case 241: /* from */
    case 258: /* tablelist */
    case 259: /* sub */
{
destroyRelationInfo((yypminor->yy52));
}
      break;
    case 242: /* where_opt */
    case 250: /* having_opt */
    case 256: /* expr */
    case 267: /* expritem */
{
tSqlExprDestroy((yypminor->yy162));
}
      break;
    case 253: /* union */
{
destroyAllSqlNode((yypminor->yy325));
}
      break;
    case 262: /* sortitem */
{
tVariantDestroy(&(yypminor->yy442));
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
   189,  /* (0) program ::= cmd */
   190,  /* (1) cmd ::= SHOW DATABASES */
   190,  /* (2) cmd ::= SHOW TOPICS */
   190,  /* (3) cmd ::= SHOW MNODES */
   190,  /* (4) cmd ::= SHOW DNODES */
   190,  /* (5) cmd ::= SHOW ACCOUNTS */
   190,  /* (6) cmd ::= SHOW USERS */
   190,  /* (7) cmd ::= SHOW MODULES */
   190,  /* (8) cmd ::= SHOW QUERIES */
   190,  /* (9) cmd ::= SHOW CONNECTIONS */
   190,  /* (10) cmd ::= SHOW STREAMS */
   190,  /* (11) cmd ::= SHOW VARIABLES */
   190,  /* (12) cmd ::= SHOW SCORES */
   190,  /* (13) cmd ::= SHOW GRANTS */
   190,  /* (14) cmd ::= SHOW VNODES */
   190,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
   191,  /* (16) dbPrefix ::= */
   191,  /* (17) dbPrefix ::= ids DOT */
   193,  /* (18) cpxName ::= */
   193,  /* (19) cpxName ::= DOT ids */
   190,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   190,  /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
   190,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   190,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   190,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   190,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   190,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   190,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   190,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   190,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   190,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   190,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   190,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   190,  /* (33) cmd ::= DROP DNODE ids */
   190,  /* (34) cmd ::= DROP USER ids */
   190,  /* (35) cmd ::= DROP ACCOUNT ids */
   190,  /* (36) cmd ::= USE ids */
   190,  /* (37) cmd ::= DESCRIBE ids cpxName */
   190,  /* (38) cmd ::= ALTER USER ids PASS ids */
   190,  /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
   190,  /* (40) cmd ::= ALTER DNODE ids ids */
   190,  /* (41) cmd ::= ALTER DNODE ids ids ids */
   190,  /* (42) cmd ::= ALTER LOCAL ids */
   190,  /* (43) cmd ::= ALTER LOCAL ids ids */
   190,  /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
   190,  /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
   190,  /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
   190,  /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   192,  /* (48) ids ::= ID */
   192,  /* (49) ids ::= STRING */
   194,  /* (50) ifexists ::= IF EXISTS */
   194,  /* (51) ifexists ::= */
   198,  /* (52) ifnotexists ::= IF NOT EXISTS */
   198,  /* (53) ifnotexists ::= */
   190,  /* (54) cmd ::= CREATE DNODE ids */
   190,  /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   190,  /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   190,  /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   190,  /* (58) cmd ::= CREATE USER ids PASS ids */
   201,  /* (59) pps ::= */
   201,  /* (60) pps ::= PPS INTEGER */
   202,  /* (61) tseries ::= */
   202,  /* (62) tseries ::= TSERIES INTEGER */
   203,  /* (63) dbs ::= */
   203,  /* (64) dbs ::= DBS INTEGER */
   204,  /* (65) streams ::= */
   204,  /* (66) streams ::= STREAMS INTEGER */
   205,  /* (67) storage ::= */
   205,  /* (68) storage ::= STORAGE INTEGER */
   206,  /* (69) qtime ::= */
   206,  /* (70) qtime ::= QTIME INTEGER */
   207,  /* (71) users ::= */
   207,  /* (72) users ::= USERS INTEGER */
   208,  /* (73) conns ::= */
   208,  /* (74) conns ::= CONNS INTEGER */
   209,  /* (75) state ::= */
   209,  /* (76) state ::= STATE ids */
   197,  /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   210,  /* (78) intitemlist ::= intitemlist COMMA intitem */
   210,  /* (79) intitemlist ::= intitem */
   211,  /* (80) intitem ::= INTEGER */
   212,  /* (81) keep ::= KEEP intitemlist */
   213,  /* (82) cache ::= CACHE INTEGER */
   214,  /* (83) replica ::= REPLICA INTEGER */
   215,  /* (84) quorum ::= QUORUM INTEGER */
   216,  /* (85) days ::= DAYS INTEGER */
   217,  /* (86) minrows ::= MINROWS INTEGER */
   218,  /* (87) maxrows ::= MAXROWS INTEGER */
   219,  /* (88) blocks ::= BLOCKS INTEGER */
   220,  /* (89) ctime ::= CTIME INTEGER */
   221,  /* (90) wal ::= WAL INTEGER */
   222,  /* (91) fsync ::= FSYNC INTEGER */
   223,  /* (92) comp ::= COMP INTEGER */
   224,  /* (93) prec ::= PRECISION STRING */
   225,  /* (94) update ::= UPDATE INTEGER */
   226,  /* (95) cachelast ::= CACHELAST INTEGER */
   227,  /* (96) partitions ::= PARTITIONS INTEGER */
   199,  /* (97) db_optr ::= */
   199,  /* (98) db_optr ::= db_optr cache */
   199,  /* (99) db_optr ::= db_optr replica */
   199,  /* (100) db_optr ::= db_optr quorum */
   199,  /* (101) db_optr ::= db_optr days */
   199,  /* (102) db_optr ::= db_optr minrows */
   199,  /* (103) db_optr ::= db_optr maxrows */
   199,  /* (104) db_optr ::= db_optr blocks */
   199,  /* (105) db_optr ::= db_optr ctime */
   199,  /* (106) db_optr ::= db_optr wal */
   199,  /* (107) db_optr ::= db_optr fsync */
   199,  /* (108) db_optr ::= db_optr comp */
   199,  /* (109) db_optr ::= db_optr prec */
   199,  /* (110) db_optr ::= db_optr keep */
   199,  /* (111) db_optr ::= db_optr update */
   199,  /* (112) db_optr ::= db_optr cachelast */
   200,  /* (113) topic_optr ::= db_optr */
   200,  /* (114) topic_optr ::= topic_optr partitions */
   195,  /* (115) alter_db_optr ::= */
   195,  /* (116) alter_db_optr ::= alter_db_optr replica */
   195,  /* (117) alter_db_optr ::= alter_db_optr quorum */
   195,  /* (118) alter_db_optr ::= alter_db_optr keep */
   195,  /* (119) alter_db_optr ::= alter_db_optr blocks */
   195,  /* (120) alter_db_optr ::= alter_db_optr comp */
   195,  /* (121) alter_db_optr ::= alter_db_optr wal */
   195,  /* (122) alter_db_optr ::= alter_db_optr fsync */
   195,  /* (123) alter_db_optr ::= alter_db_optr update */
   195,  /* (124) alter_db_optr ::= alter_db_optr cachelast */
   196,  /* (125) alter_topic_optr ::= alter_db_optr */
   196,  /* (126) alter_topic_optr ::= alter_topic_optr partitions */
   228,  /* (127) typename ::= ids */
   228,  /* (128) typename ::= ids LP signed RP */
   228,  /* (129) typename ::= ids UNSIGNED */
   229,  /* (130) signed ::= INTEGER */
   229,  /* (131) signed ::= PLUS INTEGER */
   229,  /* (132) signed ::= MINUS INTEGER */
   190,  /* (133) cmd ::= CREATE TABLE create_table_args */
   190,  /* (134) cmd ::= CREATE TABLE create_stable_args */
   190,  /* (135) cmd ::= CREATE STABLE create_stable_args */
   190,  /* (136) cmd ::= CREATE TABLE create_table_list */
   232,  /* (137) create_table_list ::= create_from_stable */
   232,  /* (138) create_table_list ::= create_table_list create_from_stable */
   230,  /* (139) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   231,  /* (140) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   233,  /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   233,  /* (142) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   236,  /* (143) tagNamelist ::= tagNamelist COMMA ids */
   236,  /* (144) tagNamelist ::= ids */
   230,  /* (145) create_table_args ::= ifnotexists ids cpxName AS select */
   234,  /* (146) columnlist ::= columnlist COMMA column */
   234,  /* (147) columnlist ::= column */
   238,  /* (148) column ::= ids typename */
   235,  /* (149) tagitemlist ::= tagitemlist COMMA tagitem */
   235,  /* (150) tagitemlist ::= tagitem */
   239,  /* (151) tagitem ::= INTEGER */
   239,  /* (152) tagitem ::= FLOAT */
   239,  /* (153) tagitem ::= STRING */
   239,  /* (154) tagitem ::= BOOL */
   239,  /* (155) tagitem ::= NULL */
   239,  /* (156) tagitem ::= MINUS INTEGER */
   239,  /* (157) tagitem ::= MINUS FLOAT */
   239,  /* (158) tagitem ::= PLUS INTEGER */
   239,  /* (159) tagitem ::= PLUS FLOAT */
   237,  /* (160) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   237,  /* (161) select ::= LP select RP */
   253,  /* (162) union ::= select */
   253,  /* (163) union ::= union UNION ALL select */
   190,  /* (164) cmd ::= union */
   237,  /* (165) select ::= SELECT selcollist */
   254,  /* (166) sclp ::= selcollist COMMA */
   254,  /* (167) sclp ::= */
   240,  /* (168) selcollist ::= sclp distinct expr as */
   240,  /* (169) selcollist ::= sclp STAR */
   257,  /* (170) as ::= AS ids */
   257,  /* (171) as ::= ids */
   257,  /* (172) as ::= */
   255,  /* (173) distinct ::= DISTINCT */
   255,  /* (174) distinct ::= */
   241,  /* (175) from ::= FROM tablelist */
   241,  /* (176) from ::= FROM sub */
   259,  /* (177) sub ::= LP union RP */
   259,  /* (178) sub ::= LP union RP ids */
   259,  /* (179) sub ::= sub COMMA LP union RP ids */
   258,  /* (180) tablelist ::= ids cpxName */
   258,  /* (181) tablelist ::= ids cpxName ids */
   258,  /* (182) tablelist ::= tablelist COMMA ids cpxName */
   258,  /* (183) tablelist ::= tablelist COMMA ids cpxName ids */
   260,  /* (184) tmvar ::= VARIABLE */
   243,  /* (185) interval_opt ::= INTERVAL LP tmvar RP */
   243,  /* (186) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   243,  /* (187) interval_opt ::= */
   244,  /* (188) session_option ::= */
   244,  /* (189) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   245,  /* (190) windowstate_option ::= */
   245,  /* (191) windowstate_option ::= STATE_WINDOW LP ids RP */
   246,  /* (192) fill_opt ::= */
   246,  /* (193) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   246,  /* (194) fill_opt ::= FILL LP ID RP */
   247,  /* (195) sliding_opt ::= SLIDING LP tmvar RP */
   247,  /* (196) sliding_opt ::= */
   249,  /* (197) orderby_opt ::= */
   249,  /* (198) orderby_opt ::= ORDER BY sortlist */
   261,  /* (199) sortlist ::= sortlist COMMA item sortorder */
   261,  /* (200) sortlist ::= item sortorder */
   263,  /* (201) item ::= ids cpxName */
   264,  /* (202) sortorder ::= ASC */
   264,  /* (203) sortorder ::= DESC */
   264,  /* (204) sortorder ::= */
   248,  /* (205) groupby_opt ::= */
   248,  /* (206) groupby_opt ::= GROUP BY grouplist */
   265,  /* (207) grouplist ::= grouplist COMMA item */
   265,  /* (208) grouplist ::= item */
   250,  /* (209) having_opt ::= */
   250,  /* (210) having_opt ::= HAVING expr */
   252,  /* (211) limit_opt ::= */
   252,  /* (212) limit_opt ::= LIMIT signed */
   252,  /* (213) limit_opt ::= LIMIT signed OFFSET signed */
   252,  /* (214) limit_opt ::= LIMIT signed COMMA signed */
   251,  /* (215) slimit_opt ::= */
   251,  /* (216) slimit_opt ::= SLIMIT signed */
   251,  /* (217) slimit_opt ::= SLIMIT signed SOFFSET signed */
   251,  /* (218) slimit_opt ::= SLIMIT signed COMMA signed */
   242,  /* (219) where_opt ::= */
   242,  /* (220) where_opt ::= WHERE expr */
   256,  /* (221) expr ::= LP expr RP */
   256,  /* (222) expr ::= ID */
   256,  /* (223) expr ::= ID DOT ID */
   256,  /* (224) expr ::= ID DOT STAR */
   256,  /* (225) expr ::= INTEGER */
   256,  /* (226) expr ::= MINUS INTEGER */
   256,  /* (227) expr ::= PLUS INTEGER */
   256,  /* (228) expr ::= FLOAT */
   256,  /* (229) expr ::= MINUS FLOAT */
   256,  /* (230) expr ::= PLUS FLOAT */
   256,  /* (231) expr ::= STRING */
   256,  /* (232) expr ::= NOW */
   256,  /* (233) expr ::= VARIABLE */
   256,  /* (234) expr ::= PLUS VARIABLE */
   256,  /* (235) expr ::= MINUS VARIABLE */
   256,  /* (236) expr ::= BOOL */
   256,  /* (237) expr ::= NULL */
   256,  /* (238) expr ::= ID LP exprlist RP */
   256,  /* (239) expr ::= ID LP STAR RP */
   256,  /* (240) expr ::= expr IS NULL */
   256,  /* (241) expr ::= expr IS NOT NULL */
   256,  /* (242) expr ::= expr LT expr */
   256,  /* (243) expr ::= expr GT expr */
   256,  /* (244) expr ::= expr LE expr */
   256,  /* (245) expr ::= expr GE expr */
   256,  /* (246) expr ::= expr NE expr */
   256,  /* (247) expr ::= expr EQ expr */
   256,  /* (248) expr ::= expr BETWEEN expr AND expr */
   256,  /* (249) expr ::= expr AND expr */
   256,  /* (250) expr ::= expr OR expr */
   256,  /* (251) expr ::= expr PLUS expr */
   256,  /* (252) expr ::= expr MINUS expr */
   256,  /* (253) expr ::= expr STAR expr */
   256,  /* (254) expr ::= expr SLASH expr */
   256,  /* (255) expr ::= expr REM expr */
   256,  /* (256) expr ::= expr LIKE expr */
   256,  /* (257) expr ::= expr IN LP exprlist RP */
   266,  /* (258) exprlist ::= exprlist COMMA expritem */
   266,  /* (259) exprlist ::= expritem */
   267,  /* (260) expritem ::= expr */
   267,  /* (261) expritem ::= */
   190,  /* (262) cmd ::= RESET QUERY CACHE */
   190,  /* (263) cmd ::= SYNCDB ids REPLICA */
   190,  /* (264) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   190,  /* (265) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   190,  /* (266) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   190,  /* (267) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   190,  /* (268) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   190,  /* (269) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   190,  /* (270) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   190,  /* (271) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   190,  /* (272) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   190,  /* (273) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   190,  /* (274) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   190,  /* (275) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   190,  /* (276) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   190,  /* (277) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   190,  /* (278) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   190,  /* (279) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   190,  /* (280) cmd ::= KILL CONNECTION INTEGER */
   190,  /* (281) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   190,  /* (282) cmd ::= KILL QUERY INTEGER COLON INTEGER */
};

/* For rule J, yyRuleInfoNRhs[J] contains the negative of the number
** of symbols on the right-hand side of that rule. */
static const signed char yyRuleInfoNRhs[] = {
   -1,  /* (0) program ::= cmd */
   -2,  /* (1) cmd ::= SHOW DATABASES */
   -2,  /* (2) cmd ::= SHOW TOPICS */
   -2,  /* (3) cmd ::= SHOW MNODES */
   -2,  /* (4) cmd ::= SHOW DNODES */
   -2,  /* (5) cmd ::= SHOW ACCOUNTS */
   -2,  /* (6) cmd ::= SHOW USERS */
   -2,  /* (7) cmd ::= SHOW MODULES */
   -2,  /* (8) cmd ::= SHOW QUERIES */
   -2,  /* (9) cmd ::= SHOW CONNECTIONS */
   -2,  /* (10) cmd ::= SHOW STREAMS */
   -2,  /* (11) cmd ::= SHOW VARIABLES */
   -2,  /* (12) cmd ::= SHOW SCORES */
   -2,  /* (13) cmd ::= SHOW GRANTS */
   -2,  /* (14) cmd ::= SHOW VNODES */
   -3,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
    0,  /* (16) dbPrefix ::= */
   -2,  /* (17) dbPrefix ::= ids DOT */
    0,  /* (18) cpxName ::= */
   -2,  /* (19) cpxName ::= DOT ids */
   -5,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   -5,  /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
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
   -3,  /* (33) cmd ::= DROP DNODE ids */
   -3,  /* (34) cmd ::= DROP USER ids */
   -3,  /* (35) cmd ::= DROP ACCOUNT ids */
   -2,  /* (36) cmd ::= USE ids */
   -3,  /* (37) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (38) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (40) cmd ::= ALTER DNODE ids ids */
   -5,  /* (41) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (42) cmd ::= ALTER LOCAL ids */
   -4,  /* (43) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (48) ids ::= ID */
   -1,  /* (49) ids ::= STRING */
   -2,  /* (50) ifexists ::= IF EXISTS */
    0,  /* (51) ifexists ::= */
   -3,  /* (52) ifnotexists ::= IF NOT EXISTS */
    0,  /* (53) ifnotexists ::= */
   -3,  /* (54) cmd ::= CREATE DNODE ids */
   -6,  /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -5,  /* (58) cmd ::= CREATE USER ids PASS ids */
    0,  /* (59) pps ::= */
   -2,  /* (60) pps ::= PPS INTEGER */
    0,  /* (61) tseries ::= */
   -2,  /* (62) tseries ::= TSERIES INTEGER */
    0,  /* (63) dbs ::= */
   -2,  /* (64) dbs ::= DBS INTEGER */
    0,  /* (65) streams ::= */
   -2,  /* (66) streams ::= STREAMS INTEGER */
    0,  /* (67) storage ::= */
   -2,  /* (68) storage ::= STORAGE INTEGER */
    0,  /* (69) qtime ::= */
   -2,  /* (70) qtime ::= QTIME INTEGER */
    0,  /* (71) users ::= */
   -2,  /* (72) users ::= USERS INTEGER */
    0,  /* (73) conns ::= */
   -2,  /* (74) conns ::= CONNS INTEGER */
    0,  /* (75) state ::= */
   -2,  /* (76) state ::= STATE ids */
   -9,  /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -3,  /* (78) intitemlist ::= intitemlist COMMA intitem */
   -1,  /* (79) intitemlist ::= intitem */
   -1,  /* (80) intitem ::= INTEGER */
   -2,  /* (81) keep ::= KEEP intitemlist */
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
  -14,  /* (160) select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
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
   -2,  /* (176) from ::= FROM sub */
   -3,  /* (177) sub ::= LP union RP */
   -4,  /* (178) sub ::= LP union RP ids */
   -6,  /* (179) sub ::= sub COMMA LP union RP ids */
   -2,  /* (180) tablelist ::= ids cpxName */
   -3,  /* (181) tablelist ::= ids cpxName ids */
   -4,  /* (182) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (183) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (184) tmvar ::= VARIABLE */
   -4,  /* (185) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (186) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (187) interval_opt ::= */
    0,  /* (188) session_option ::= */
   -7,  /* (189) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (190) windowstate_option ::= */
   -4,  /* (191) windowstate_option ::= STATE_WINDOW LP ids RP */
    0,  /* (192) fill_opt ::= */
   -6,  /* (193) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (194) fill_opt ::= FILL LP ID RP */
   -4,  /* (195) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (196) sliding_opt ::= */
    0,  /* (197) orderby_opt ::= */
   -3,  /* (198) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (199) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (200) sortlist ::= item sortorder */
   -2,  /* (201) item ::= ids cpxName */
   -1,  /* (202) sortorder ::= ASC */
   -1,  /* (203) sortorder ::= DESC */
    0,  /* (204) sortorder ::= */
    0,  /* (205) groupby_opt ::= */
   -3,  /* (206) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (207) grouplist ::= grouplist COMMA item */
   -1,  /* (208) grouplist ::= item */
    0,  /* (209) having_opt ::= */
   -2,  /* (210) having_opt ::= HAVING expr */
    0,  /* (211) limit_opt ::= */
   -2,  /* (212) limit_opt ::= LIMIT signed */
   -4,  /* (213) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (214) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (215) slimit_opt ::= */
   -2,  /* (216) slimit_opt ::= SLIMIT signed */
   -4,  /* (217) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (218) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (219) where_opt ::= */
   -2,  /* (220) where_opt ::= WHERE expr */
   -3,  /* (221) expr ::= LP expr RP */
   -1,  /* (222) expr ::= ID */
   -3,  /* (223) expr ::= ID DOT ID */
   -3,  /* (224) expr ::= ID DOT STAR */
   -1,  /* (225) expr ::= INTEGER */
   -2,  /* (226) expr ::= MINUS INTEGER */
   -2,  /* (227) expr ::= PLUS INTEGER */
   -1,  /* (228) expr ::= FLOAT */
   -2,  /* (229) expr ::= MINUS FLOAT */
   -2,  /* (230) expr ::= PLUS FLOAT */
   -1,  /* (231) expr ::= STRING */
   -1,  /* (232) expr ::= NOW */
   -1,  /* (233) expr ::= VARIABLE */
   -2,  /* (234) expr ::= PLUS VARIABLE */
   -2,  /* (235) expr ::= MINUS VARIABLE */
   -1,  /* (236) expr ::= BOOL */
   -1,  /* (237) expr ::= NULL */
   -4,  /* (238) expr ::= ID LP exprlist RP */
   -4,  /* (239) expr ::= ID LP STAR RP */
   -3,  /* (240) expr ::= expr IS NULL */
   -4,  /* (241) expr ::= expr IS NOT NULL */
   -3,  /* (242) expr ::= expr LT expr */
   -3,  /* (243) expr ::= expr GT expr */
   -3,  /* (244) expr ::= expr LE expr */
   -3,  /* (245) expr ::= expr GE expr */
   -3,  /* (246) expr ::= expr NE expr */
   -3,  /* (247) expr ::= expr EQ expr */
   -5,  /* (248) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (249) expr ::= expr AND expr */
   -3,  /* (250) expr ::= expr OR expr */
   -3,  /* (251) expr ::= expr PLUS expr */
   -3,  /* (252) expr ::= expr MINUS expr */
   -3,  /* (253) expr ::= expr STAR expr */
   -3,  /* (254) expr ::= expr SLASH expr */
   -3,  /* (255) expr ::= expr REM expr */
   -3,  /* (256) expr ::= expr LIKE expr */
   -5,  /* (257) expr ::= expr IN LP exprlist RP */
   -3,  /* (258) exprlist ::= exprlist COMMA expritem */
   -1,  /* (259) exprlist ::= expritem */
   -1,  /* (260) expritem ::= expr */
    0,  /* (261) expritem ::= */
   -3,  /* (262) cmd ::= RESET QUERY CACHE */
   -3,  /* (263) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (264) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (265) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (266) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (267) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (268) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (269) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (270) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (271) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
   -7,  /* (272) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (273) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (274) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
   -7,  /* (275) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (276) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (277) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (278) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (279) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
   -3,  /* (280) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (281) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (282) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 3: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 7: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 10: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 12: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 14: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 15: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 16: /* dbPrefix ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.type = 0;}
        break;
      case 17: /* dbPrefix ::= ids DOT */
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 18: /* cpxName ::= */
{yymsp[1].minor.yy0.n = 0;  }
        break;
      case 19: /* cpxName ::= DOT ids */
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
        break;
      case 20: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW CREATE STABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_STABLE, 1, &yymsp[-1].minor.yy0);
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
      case 33: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 34: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 35: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 36: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 37: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 38: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 39: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 41: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 43: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 45: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==45);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy83);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
        break;
      case 48: /* ids ::= ID */
      case 49: /* ids ::= STRING */ yytestcase(yyruleno==49);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 50: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 51: /* ifexists ::= */
      case 53: /* ifnotexists ::= */ yytestcase(yyruleno==53);
      case 174: /* distinct ::= */ yytestcase(yyruleno==174);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 52: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &yymsp[-2].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 59: /* pps ::= */
      case 61: /* tseries ::= */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= */ yytestcase(yyruleno==63);
      case 65: /* streams ::= */ yytestcase(yyruleno==65);
      case 67: /* storage ::= */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= */ yytestcase(yyruleno==69);
      case 71: /* users ::= */ yytestcase(yyruleno==71);
      case 73: /* conns ::= */ yytestcase(yyruleno==73);
      case 75: /* state ::= */ yytestcase(yyruleno==75);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 60: /* pps ::= PPS INTEGER */
      case 62: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==62);
      case 64: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==64);
      case 66: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==68);
      case 70: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==70);
      case 72: /* users ::= USERS INTEGER */ yytestcase(yyruleno==72);
      case 74: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==74);
      case 76: /* state ::= STATE ids */ yytestcase(yyruleno==76);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 77: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy83.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy83.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy83.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy83.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy83.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy83.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy83.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy83.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy83.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy83 = yylhsminor.yy83;
        break;
      case 78: /* intitemlist ::= intitemlist COMMA intitem */
      case 149: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==149);
{ yylhsminor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);    }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 79: /* intitemlist ::= intitem */
      case 150: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==150);
{ yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 80: /* intitem ::= INTEGER */
      case 151: /* tagitem ::= INTEGER */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= FLOAT */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= STRING */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= BOOL */ yytestcase(yyruleno==154);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 81: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy325 = yymsp[0].minor.yy325; }
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
{setDefaultCreateDbOption(&yymsp[1].minor.yy22); yymsp[1].minor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 98: /* db_optr ::= db_optr cache */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 99: /* db_optr ::= db_optr replica */
      case 116: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==116);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 100: /* db_optr ::= db_optr quorum */
      case 117: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==117);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 101: /* db_optr ::= db_optr days */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 102: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 103: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 104: /* db_optr ::= db_optr blocks */
      case 119: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==119);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 105: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 106: /* db_optr ::= db_optr wal */
      case 121: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==121);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 107: /* db_optr ::= db_optr fsync */
      case 122: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==122);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 108: /* db_optr ::= db_optr comp */
      case 120: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==120);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 109: /* db_optr ::= db_optr prec */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 110: /* db_optr ::= db_optr keep */
      case 118: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==118);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.keep = yymsp[0].minor.yy325; }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 111: /* db_optr ::= db_optr update */
      case 123: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==123);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 112: /* db_optr ::= db_optr cachelast */
      case 124: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==124);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 113: /* topic_optr ::= db_optr */
      case 125: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==125);
{ yylhsminor.yy22 = yymsp[0].minor.yy22; yylhsminor.yy22.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy22 = yylhsminor.yy22;
        break;
      case 114: /* topic_optr ::= topic_optr partitions */
      case 126: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==126);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 115: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy22); yymsp[1].minor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 127: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy47, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy47 = yylhsminor.yy47;
        break;
      case 128: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy373 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy47, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy373;  // negative value of name length
    tSetColumnType(&yylhsminor.yy47, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy47 = yylhsminor.yy47;
        break;
      case 129: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy47, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy47 = yylhsminor.yy47;
        break;
      case 130: /* signed ::= INTEGER */
{ yylhsminor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 131: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 132: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy373 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 136: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy422;}
        break;
      case 137: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy504);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy422 = pCreateTable;
}
  yymsp[0].minor.yy422 = yylhsminor.yy422;
        break;
      case 138: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy422->childTableInfo, &yymsp[0].minor.yy504);
  yylhsminor.yy422 = yymsp[-1].minor.yy422;
}
  yymsp[-1].minor.yy422 = yylhsminor.yy422;
        break;
      case 139: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-1].minor.yy325, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy422 = yylhsminor.yy422;
        break;
      case 140: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy422 = yylhsminor.yy422;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy504 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy325, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy504 = yylhsminor.yy504;
        break;
      case 142: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy504 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy504 = yylhsminor.yy504;
        break;
      case 143: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy0); yylhsminor.yy325 = yymsp[-2].minor.yy325;  }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 144: /* tagNamelist ::= ids */
{yylhsminor.yy325 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy325, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 145: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy422 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy536, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy422 = yylhsminor.yy422;
        break;
      case 146: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy47); yylhsminor.yy325 = yymsp[-2].minor.yy325;  }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 147: /* columnlist ::= column */
{yylhsminor.yy325 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy325, &yymsp[0].minor.yy47);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 148: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy47, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);
}
  yymsp[-1].minor.yy47 = yylhsminor.yy47;
        break;
      case 155: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 156: /* tagitem ::= MINUS INTEGER */
      case 157: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==159);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 160: /* select ::= SELECT selcollist from where_opt interval_opt session_option windowstate_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy536 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy325, yymsp[-11].minor.yy52, yymsp[-10].minor.yy162, yymsp[-4].minor.yy325, yymsp[-3].minor.yy325, &yymsp[-9].minor.yy328, &yymsp[-8].minor.yy84, &yymsp[-7].minor.yy176, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy325, &yymsp[0].minor.yy230, &yymsp[-1].minor.yy230, yymsp[-2].minor.yy162);
}
  yymsp[-13].minor.yy536 = yylhsminor.yy536;
        break;
      case 161: /* select ::= LP select RP */
{yymsp[-2].minor.yy536 = yymsp[-1].minor.yy536;}
        break;
      case 162: /* union ::= select */
{ yylhsminor.yy325 = setSubclause(NULL, yymsp[0].minor.yy536); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 163: /* union ::= union UNION ALL select */
{ yylhsminor.yy325 = appendSelectClause(yymsp[-3].minor.yy325, yymsp[0].minor.yy536); }
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 164: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy325, NULL, TSDB_SQL_SELECT); }
        break;
      case 165: /* select ::= SELECT selcollist */
{
  yylhsminor.yy536 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy325, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy536 = yylhsminor.yy536;
        break;
      case 166: /* sclp ::= selcollist COMMA */
{yylhsminor.yy325 = yymsp[-1].minor.yy325;}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 167: /* sclp ::= */
      case 197: /* orderby_opt ::= */ yytestcase(yyruleno==197);
{yymsp[1].minor.yy325 = 0;}
        break;
      case 168: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy325 = tSqlExprListAppend(yymsp[-3].minor.yy325, yymsp[-1].minor.yy162,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 169: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy325 = tSqlExprListAppend(yymsp[-1].minor.yy325, pNode, 0, 0);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
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
      case 176: /* from ::= FROM sub */ yytestcase(yyruleno==176);
{yymsp[-1].minor.yy52 = yymsp[0].minor.yy52;}
        break;
      case 177: /* sub ::= LP union RP */
{yymsp[-2].minor.yy52 = addSubqueryElem(NULL, yymsp[-1].minor.yy325, NULL);}
        break;
      case 178: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy52 = addSubqueryElem(NULL, yymsp[-2].minor.yy325, &yymsp[0].minor.yy0);}
        break;
      case 179: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy52 = addSubqueryElem(yymsp[-5].minor.yy52, yymsp[-2].minor.yy325, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy52 = yylhsminor.yy52;
        break;
      case 180: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy52 = yylhsminor.yy52;
        break;
      case 181: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy52 = yylhsminor.yy52;
        break;
      case 182: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-3].minor.yy52, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy52 = yylhsminor.yy52;
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy52 = setTableNameList(yymsp[-4].minor.yy52, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy52 = yylhsminor.yy52;
        break;
      case 184: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 185: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy328.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy328.offset.n = 0;}
        break;
      case 186: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy328.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy328.offset = yymsp[-1].minor.yy0;}
        break;
      case 187: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy328, 0, sizeof(yymsp[1].minor.yy328));}
        break;
      case 188: /* session_option ::= */
{yymsp[1].minor.yy84.col.n = 0; yymsp[1].minor.yy84.gap.n = 0;}
        break;
      case 189: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy84.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy84.gap = yymsp[-1].minor.yy0;
}
        break;
      case 190: /* windowstate_option ::= */
{yymsp[1].minor.yy176.col.n = 0;}
        break;
      case 191: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{
   yymsp[-3].minor.yy176.col = yymsp[-1].minor.yy0;
}
        break;
      case 192: /* fill_opt ::= */
{ yymsp[1].minor.yy325 = 0;     }
        break;
      case 193: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy325, &A, -1, 0);
    yymsp[-5].minor.yy325 = yymsp[-1].minor.yy325;
}
        break;
      case 194: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy325 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 195: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 196: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 198: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 199: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy325 = tVariantListAppend(yymsp[-3].minor.yy325, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 200: /* sortlist ::= item sortorder */
{
  yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 201: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 202: /* sortorder ::= ASC */
{ yymsp[0].minor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 203: /* sortorder ::= DESC */
{ yymsp[0].minor.yy196 = TSDB_ORDER_DESC;}
        break;
      case 204: /* sortorder ::= */
{ yymsp[1].minor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 205: /* groupby_opt ::= */
{ yymsp[1].minor.yy325 = 0;}
        break;
      case 206: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 207: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);
}
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 208: /* grouplist ::= item */
{
  yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 209: /* having_opt ::= */
      case 219: /* where_opt ::= */ yytestcase(yyruleno==219);
      case 261: /* expritem ::= */ yytestcase(yyruleno==261);
{yymsp[1].minor.yy162 = 0;}
        break;
      case 210: /* having_opt ::= HAVING expr */
      case 220: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==220);
{yymsp[-1].minor.yy162 = yymsp[0].minor.yy162;}
        break;
      case 211: /* limit_opt ::= */
      case 215: /* slimit_opt ::= */ yytestcase(yyruleno==215);
{yymsp[1].minor.yy230.limit = -1; yymsp[1].minor.yy230.offset = 0;}
        break;
      case 212: /* limit_opt ::= LIMIT signed */
      case 216: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==216);
{yymsp[-1].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-1].minor.yy230.offset = 0;}
        break;
      case 213: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy230.limit = yymsp[-2].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 214: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 217: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy230.limit = yymsp[-2].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 218: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 221: /* expr ::= LP expr RP */
{yylhsminor.yy162 = yymsp[-1].minor.yy162; yylhsminor.yy162->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy162->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 222: /* expr ::= ID */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 223: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 224: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 225: /* expr ::= INTEGER */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 226: /* expr ::= MINUS INTEGER */
      case 227: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy162 = yylhsminor.yy162;
        break;
      case 228: /* expr ::= FLOAT */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 229: /* expr ::= MINUS FLOAT */
      case 230: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==230);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy162 = yylhsminor.yy162;
        break;
      case 231: /* expr ::= STRING */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 232: /* expr ::= NOW */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 233: /* expr ::= VARIABLE */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 234: /* expr ::= PLUS VARIABLE */
      case 235: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==235);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy162 = yylhsminor.yy162;
        break;
      case 236: /* expr ::= BOOL */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 237: /* expr ::= NULL */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 238: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy162 = tSqlExprCreateFunction(yymsp[-1].minor.yy325, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 239: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy162 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 240: /* expr ::= expr IS NULL */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 241: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-3].minor.yy162, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 242: /* expr ::= expr LT expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LT);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 243: /* expr ::= expr GT expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GT);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 244: /* expr ::= expr LE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 245: /* expr ::= expr GE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 246: /* expr ::= expr NE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_NE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 247: /* expr ::= expr EQ expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_EQ);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 248: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy162); yylhsminor.yy162 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy162, yymsp[-2].minor.yy162, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy162, TK_LE), TK_AND);}
  yymsp[-4].minor.yy162 = yylhsminor.yy162;
        break;
      case 249: /* expr ::= expr AND expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_AND);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 250: /* expr ::= expr OR expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_OR); }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 251: /* expr ::= expr PLUS expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_PLUS);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 252: /* expr ::= expr MINUS expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_MINUS); }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 253: /* expr ::= expr STAR expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_STAR);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 254: /* expr ::= expr SLASH expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_DIVIDE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 255: /* expr ::= expr REM expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_REM);   }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 256: /* expr ::= expr LIKE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LIKE);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 257: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-4].minor.yy162, (tSqlExpr*)yymsp[-1].minor.yy325, TK_IN); }
  yymsp[-4].minor.yy162 = yylhsminor.yy162;
        break;
      case 258: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy325 = tSqlExprListAppend(yymsp[-2].minor.yy325,yymsp[0].minor.yy162,0, 0);}
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 259: /* exprlist ::= expritem */
{yylhsminor.yy325 = tSqlExprListAppend(0,yymsp[0].minor.yy162,0, 0);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 260: /* expritem ::= expr */
{yylhsminor.yy162 = yymsp[0].minor.yy162;}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 262: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 263: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
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
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
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
      case 278: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 281: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 282: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
