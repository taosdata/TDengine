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
#define YYNOCODE 264
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreatedTableInfo yy96;
  SRelationInfo* yy148;
  tSqlExpr* yy178;
  SCreateAcctInfo yy187;
  SArray* yy285;
  TAOS_FIELD yy295;
  SSqlNode* yy344;
  tVariant yy362;
  SIntervalVal yy376;
  SLimitVal yy438;
  int yy460;
  SCreateTableSql* yy470;
  SSessionWindowVal yy523;
  int64_t yy525;
  SCreateDbInfo yy526;
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
#define YYNSTATE             327
#define YYNRULE              274
#define YYNRULE_WITH_ACTION  274
#define YYNTOKEN             188
#define YY_MAX_SHIFT         326
#define YY_MIN_SHIFTREDUCE   523
#define YY_MAX_SHIFTREDUCE   796
#define YY_ERROR_ACTION      797
#define YY_ACCEPT_ACTION     798
#define YY_NO_ACTION         799
#define YY_MIN_REDUCE        800
#define YY_MAX_REDUCE        1073
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
#define YY_ACTTAB_COUNT (694)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   968,  571,  210,  324,   70,   18,  216,  959,  187,  572,
 /*    10 */   798,  326,  185,   48,   49,  145,   52,   53,  219, 1054,
 /*    20 */   222,   42,  213,   51,  271,   56,   54,   58,   55,  933,
 /*    30 */   650,  187,  947,   47,   46,  187,  932,   45,   44,   43,
 /*    40 */    48,   49, 1053,   52,   53,  218, 1054,  222,   42,  571,
 /*    50 */    51,  271,   56,   54,   58,   55,  959,  572,  300,  299,
 /*    60 */    47,   46,  965,  145,   45,   44,   43,   49,   31,   52,
 /*    70 */    53,  249,  138,  222,   42,   83,   51,  271,   56,   54,
 /*    80 */    58,   55,  287, 1003,   88,  266,   47,   46,   72,  231,
 /*    90 */    45,   44,   43,  524,  525,  526,  527,  528,  529,  530,
 /*   100 */   531,  532,  533,  534,  535,  536,  325,  234,  287,  211,
 /*   110 */    71,  571,  943,   48,   49,   31,   52,   53,  935,  572,
 /*   120 */   222,   42,  571,   51,  271,   56,   54,   58,   55,  268,
 /*   130 */   572,   81,  739,   47,   46,  256,  255,   45,   44,   43,
 /*   140 */    48,   50,  945,   52,   53,  145,  310,  222,   42,   77,
 /*   150 */    51,  271,   56,   54,   58,   55,  212,   37,  232,  944,
 /*   160 */    47,   46,  289,  191,   45,   44,   43,   24,  285,  319,
 /*   170 */   318,  284,  283,  282,  317,  281,  316,  315,  314,  280,
 /*   180 */   313,  312,  907,   31,  895,  896,  897,  898,  899,  900,
 /*   190 */   901,  902,  903,  904,  905,  906,  908,  909,   52,   53,
 /*   200 */   846, 1050,  222,   42,  171,   51,  271,   56,   54,   58,
 /*   210 */    55,  941,   19, 1002,   25,   47,   46, 1049,  959,   45,
 /*   220 */    44,   43,  221,  754,  225,   31,  743,  944,  746,  196,
 /*   230 */   749,  221,  754,  214,   13,  743,  197,  746,   87,  749,
 /*   240 */    84,  122,  121,  195,   45,   44,   43,  109,   56,   54,
 /*   250 */    58,   55,  310,  228,  206,  207,   47,   46,  270,   74,
 /*   260 */    45,   44,   43,  206,  207,   75,  226,  252,   24,  944,
 /*   270 */   319,  318,   77,  252,  745,  317,  748,  316,  315,  314,
 /*   280 */    37,  313,  312,  915, 1048,  674,  913,  914,  671,  204,
 /*   290 */   672,  916,  673,  918,  919,  917,   85,  920,  921,  107,
 /*   300 */   100,  112,  248,  686,   69,   31,  111,  117,  120,  110,
 /*   310 */     8,  203,    5,   34,  161,  114,  236,  237,  689,  160,
 /*   320 */    95,   90,   94,   31,  233,   57,  272,  930,  931,   30,
 /*   330 */   934,  297,  755,   31,   57,  179,  177,  175,  751,   31,
 /*   340 */   145,  755,  174,  125,  124,  123,  290,  751,  220,  944,
 /*   350 */   241,   47,   46,  205,  750,   45,   44,   43,  855,  245,
 /*   360 */   244,  189,  171,  750,  291,  227,  744,  944,  747,  229,
 /*   370 */   323,  322,  130,  320,  298,  847,   99,  944,   98,  171,
 /*   380 */   302,    1,  159,  944,    3,  172,  752,  136,  134,  133,
 /*   390 */   741,  947,    6,  235,  675,  947,  693,  294,  293,  947,
 /*   400 */   720,  721,  250,  705,   65,  711,   32,  140,   82,   61,
 /*   410 */    62,  712,  775,  756,  660,   21,   20,   20,   32,  274,
 /*   420 */  1065,  662,  758,   32,   66,   61,  742,  276,  678,  661,
 /*   430 */   679,   86,   63,   61,   29,  946,   68,  277,  649,  190,
 /*   440 */    15,  106,   14,  105,  676,  192,  677,  186,   17,  193,
 /*   450 */    16,  119,  118,  194,  200,  201,  199,  184,  198, 1013,
 /*   460 */   188, 1012,  223, 1009,   40,  246, 1008,  224,  301,  137,
 /*   470 */   967,  978,  995,  975,  994,  976,  960,  253,  753,  135,
 /*   480 */   980,  139,  143,  155,  156,  942,  251,  940,  704,  257,
 /*   490 */   157,  311,  911,  154,  146,  158,  957,  149,  147,  269,
 /*   500 */   215,   59,  259,  858,  279,  264,   38,   67,  182,  148,
 /*   510 */    35,  288,  854,   64, 1070,  265,  267,   96, 1069, 1067,
 /*   520 */   162,  263,  292,  261, 1064,  102,  295, 1063, 1060,  163,
 /*   530 */   876,   36,   33,   39,  183,  843,  113,  841,  115,  116,
 /*   540 */   839,  838,  238,  173,  836,  835,  834,  833,  832,  831,
 /*   550 */   176,  178,  258,  828,  826,  824,  822,  180,  819,  181,
 /*   560 */    41,   73,   78,  108,  260,  996,  303,  304,  305,  306,
 /*   570 */   307,  308,  309,  208,  321,  796,  230,  278,  239,  240,
 /*   580 */   795,  242,  243,   91,   92,  209,  794,  202,  781,  780,
 /*   590 */   247,  252,    9,  273,  681,  837,   76,   26,  165,  877,
 /*   600 */   166,  126,  167,  164,  169,  168,  170,  127,  830,    2,
 /*   610 */   128,  129,  829,  821,  820,  254,   79,  706,    4,  150,
 /*   620 */   151,  152,  153,  141,  923,  709,   80,  142,  217,  262,
 /*   630 */    27,  713,  144,   10,   11,  757,   28,    7,   12,   22,
 /*   640 */   759,   23,   89,  275,  613,  609,   87,  607,  606,  605,
 /*   650 */   602,  575,  286,   97,   93,   32,  784,   60,  652,  651,
 /*   660 */   648,  597,  595,  101,  103,  587,  593,  589,  591,  585,
 /*   670 */   104,  583,  616,  615,  614,  612,  611,  296,  610,  608,
 /*   680 */   604,  603,   61,  573,  540,  538,  131,  800,  799,  799,
 /*   690 */   799,  799,  799,  132,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   191,    1,  190,  191,  197,  252,  210,  234,  252,    9,
 /*    10 */   188,  189,  252,   13,   14,  191,   16,   17,  262,  263,
 /*    20 */    20,   21,  249,   23,   24,   25,   26,   27,   28,    0,
 /*    30 */     5,  252,  236,   33,   34,  252,  229,   37,   38,   39,
 /*    40 */    13,   14,  263,   16,   17,  262,  263,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  234,    9,   33,   34,
 /*    60 */    33,   34,  253,  191,   37,   38,   39,   14,  191,   16,
 /*    70 */    17,  249,  191,   20,   21,  237,   23,   24,   25,   26,
 /*    80 */    27,   28,   79,  259,  197,  261,   33,   34,  250,   68,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,  191,   79,   61,
 /*   110 */   110,    1,  235,   13,   14,  191,   16,   17,  231,    9,
 /*   120 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  257,
 /*   130 */     9,  259,  105,   33,   34,  254,  255,   37,   38,   39,
 /*   140 */    13,   14,  226,   16,   17,  191,   81,   20,   21,  104,
 /*   150 */    23,   24,   25,   26,   27,   28,  232,  112,  137,  235,
 /*   160 */    33,   34,  141,  252,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  209,  191,  211,  212,  213,  214,  215,  216,
 /*   190 */   217,  218,  219,  220,  221,  222,  223,  224,   16,   17,
 /*   200 */   196,  252,   20,   21,  200,   23,   24,   25,   26,   27,
 /*   210 */    28,  191,   44,  259,  104,   33,   34,  252,  234,   37,
 /*   220 */    38,   39,    1,    2,  232,  191,    5,  235,    7,   61,
 /*   230 */     9,    1,    2,  249,  104,    5,   68,    7,  108,    9,
 /*   240 */   110,   73,   74,   75,   37,   38,   39,   76,   25,   26,
 /*   250 */    27,   28,   81,  233,   33,   34,   33,   34,   37,  105,
 /*   260 */    37,   38,   39,   33,   34,  105,  232,  113,   88,  235,
 /*   270 */    90,   91,  104,  113,    5,   95,    7,   97,   98,   99,
 /*   280 */   112,  101,  102,  209,  252,    2,  212,  213,    5,  252,
 /*   290 */     7,  217,    9,  219,  220,  221,  197,  223,  224,   62,
 /*   300 */    63,   64,  134,  109,  136,  191,   69,   70,   71,   72,
 /*   310 */   116,  143,   62,   63,   64,   78,   33,   34,   37,   69,
 /*   320 */    70,   71,   72,  191,   68,  104,   15,  228,  229,  230,
 /*   330 */   231,   75,  111,  191,  104,   62,   63,   64,  117,  191,
 /*   340 */   191,  111,   69,   70,   71,   72,  232,  117,   60,  235,
 /*   350 */   135,   33,   34,  252,  133,   37,   38,   39,  196,  144,
 /*   360 */   145,  252,  200,  133,  232,  210,    5,  235,    7,  210,
 /*   370 */    65,   66,   67,  210,  232,  196,  138,  235,  140,  200,
 /*   380 */   232,  198,  199,  235,  194,  195,  117,   62,   63,   64,
 /*   390 */     1,  236,  104,  137,  111,  236,  115,  141,  142,  236,
 /*   400 */   124,  125,  105,  105,  109,  105,  109,  109,  259,  109,
 /*   410 */   109,  105,  105,  105,  105,  109,  109,  109,  109,  105,
 /*   420 */   236,  105,  111,  109,  129,  109,   37,  105,    5,  105,
 /*   430 */     7,  109,  131,  109,  104,  236,  104,  107,  106,  252,
 /*   440 */   138,  138,  140,  140,    5,  252,    7,  252,  138,  252,
 /*   450 */   140,   76,   77,  252,  252,  252,  252,  252,  252,  227,
 /*   460 */   252,  227,  227,  227,  251,  191,  227,  227,  227,  191,
 /*   470 */   191,  191,  260,  191,  260,  191,  234,  234,  117,   60,
 /*   480 */   191,  191,  191,  238,  191,  234,  192,  191,  117,  256,
 /*   490 */   191,  103,  225,  239,  247,  191,  248,  244,  246,  122,
 /*   500 */   256,  127,  256,  191,  191,  256,  191,  128,  191,  245,
 /*   510 */   191,  191,  191,  130,  191,  121,  126,  191,  191,  191,
 /*   520 */   191,  120,  191,  119,  191,  191,  191,  191,  191,  191,
 /*   530 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   540 */   191,  191,  191,  191,  191,  191,  191,  191,  191,  191,
 /*   550 */   191,  191,  118,  191,  191,  191,  191,  191,  191,  191,
 /*   560 */   132,  192,  192,   87,  192,  192,   86,   50,   83,   85,
 /*   570 */    54,   84,   82,  192,   79,    5,  192,  192,  146,    5,
 /*   580 */     5,  146,    5,  197,  197,  192,    5,  192,   90,   89,
 /*   590 */   135,  113,  104,  107,  105,  192,  114,  104,  206,  208,
 /*   600 */   202,  193,  205,  207,  204,  203,  201,  193,  192,  198,
 /*   610 */   193,  193,  192,  192,  192,  109,  109,  105,  194,  243,
 /*   620 */   242,  241,  240,  104,  225,  105,  104,  109,    1,  104,
 /*   630 */   109,  105,  104,  123,  123,  105,  109,  104,  104,  104,
 /*   640 */   111,  104,   76,  107,    9,    5,  108,    5,    5,    5,
 /*   650 */     5,   80,   15,  140,   76,  109,    5,   16,    5,    5,
 /*   660 */   105,    5,    5,  140,  140,    5,    5,    5,    5,    5,
 /*   670 */   139,    5,    5,    5,    5,    5,    5,  138,    5,    5,
 /*   680 */     5,    5,  109,   80,   60,   59,   21,    0,  264,  264,
 /*   690 */   264,  264,  264,   21,  264,  264,  264,  264,  264,  264,
 /*   700 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   710 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   720 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   730 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   740 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   750 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   760 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   770 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   780 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   790 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   800 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   810 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   820 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   830 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   840 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   850 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   860 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   870 */   264,  264,  264,  264,  264,  264,  264,  264,  264,  264,
 /*   880 */   264,  264,
};
#define YY_SHIFT_COUNT    (326)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (687)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   168,   79,   79,  180,  180,    3,  221,  230,  110,  121,
 /*    10 */   121,  121,  121,  121,  121,  121,  121,  121,    0,   48,
 /*    20 */   230,  283,  283,  283,  283,   45,   45,  121,  121,  121,
 /*    30 */    29,  121,  121,  171,    3,   65,   65,  694,  694,  694,
 /*    40 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    50 */   230,  230,  230,  230,  230,  230,  230,  230,  230,  230,
 /*    60 */   283,  283,   25,   25,   25,   25,   25,   25,   25,  121,
 /*    70 */   121,  121,  281,  121,  121,  121,   45,   45,  121,  121,
 /*    80 */   121,  276,  276,  194,   45,  121,  121,  121,  121,  121,
 /*    90 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   100 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   110 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   120 */   121,  121,  121,  121,  121,  121,  121,  121,  121,  121,
 /*   130 */   121,  121,  121,  121,  121,  121,  121,  419,  419,  419,
 /*   140 */   371,  371,  371,  419,  371,  419,  379,  383,  374,  377,
 /*   150 */   390,  394,  401,  404,  434,  428,  419,  419,  419,  388,
 /*   160 */     3,    3,  419,  419,  476,  480,  517,  485,  484,  516,
 /*   170 */   487,  490,  388,  419,  495,  495,  419,  495,  419,  495,
 /*   180 */   419,  419,  694,  694,   27,  100,  127,  100,  100,   53,
 /*   190 */   182,  223,  223,  223,  223,  237,  250,  273,  318,  318,
 /*   200 */   318,  318,  256,  215,  207,  207,  269,  361,  130,   21,
 /*   210 */   305,  325,  297,  154,  160,  298,  300,  306,  307,  308,
 /*   220 */   389,  288,  311,  301,  295,  309,  314,  316,  322,  324,
 /*   230 */   330,  238,  302,  303,  332,  310,  423,  439,  375,  570,
 /*   240 */   432,  574,  575,  435,  577,  581,  498,  500,  455,  478,
 /*   250 */   486,  488,  482,  489,  493,  506,  507,  512,  519,  520,
 /*   260 */   518,  522,  627,  525,  526,  528,  521,  510,  527,  511,
 /*   270 */   530,  533,  529,  534,  486,  535,  536,  537,  538,  566,
 /*   280 */   635,  640,  642,  643,  644,  645,  571,  637,  578,  513,
 /*   290 */   546,  546,  641,  523,  524,  651,  531,  539,  546,  653,
 /*   300 */   654,  555,  546,  656,  657,  660,  661,  662,  663,  664,
 /*   310 */   666,  667,  668,  669,  670,  671,  673,  674,  675,  676,
 /*   320 */   573,  603,  665,  672,  624,  626,  687,
};
#define YY_REDUCE_COUNT (183)
#define YY_REDUCE_MIN   (-247)
#define YY_REDUCE_MAX   (424)
static const short yy_reduce_ofst[] = {
 /*     0 */  -178,  -27,  -27,   74,   74,   99, -244, -217, -119,  -76,
 /*    10 */  -176, -128,   -8,   34,  114,  132,  142,  148, -191, -188,
 /*    20 */  -221, -204,  155,  159,  163, -227,  -16,  -46,  149,   20,
 /*    30 */  -113,  -84, -123,    4, -193,  162,  179, -162,  183,  190,
 /*    40 */  -247, -240,  -89,  -51,  -35,   32,   37,  101,  109,  187,
 /*    50 */   193,  195,  197,  201,  202,  203,  204,  205,  206,  208,
 /*    60 */   184,  199,  232,  234,  235,  236,  239,  240,  241,  274,
 /*    70 */   278,  279,  213,  280,  282,  284,  242,  243,  289,  290,
 /*    80 */   291,  212,  214,  245,  251,  293,  296,  299,  304,  312,
 /*    90 */   313,  315,  317,  319,  320,  321,  323,  326,  327,  328,
 /*   100 */   329,  331,  333,  334,  335,  336,  337,  338,  339,  340,
 /*   110 */   341,  342,  343,  344,  345,  346,  347,  348,  349,  350,
 /*   120 */   351,  352,  353,  354,  355,  356,  357,  358,  359,  360,
 /*   130 */   362,  363,  364,  365,  366,  367,  368,  294,  369,  370,
 /*   140 */   233,  244,  246,  372,  249,  373,  248,  247,  252,  264,
 /*   150 */   253,  376,  378,  380,  382,  254,  381,  384,  385,  267,
 /*   160 */   386,  387,  393,  395,  391,  396,  392,  398,  397,  402,
 /*   170 */   400,  405,  399,  403,  408,  414,  416,  417,  420,  418,
 /*   180 */   421,  422,  411,  424,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   797,  910,  856,  922,  844,  853, 1056, 1056,  797,  797,
 /*    10 */   797,  797,  797,  797,  797,  797,  797,  797,  969,  816,
 /*    20 */  1056,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*    30 */   853,  797,  797,  859,  853,  859,  859,  964,  894,  912,
 /*    40 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*    50 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*    60 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*    70 */   797,  797,  971,  977,  974,  797,  797,  797,  979,  797,
 /*    80 */   797,  999,  999,  962,  797,  797,  797,  797,  797,  797,
 /*    90 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   100 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   110 */   797,  797,  797,  842,  797,  840,  797,  797,  797,  797,
 /*   120 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   130 */   827,  797,  797,  797,  797,  797,  797,  818,  818,  818,
 /*   140 */   797,  797,  797,  818,  797,  818, 1006, 1010, 1004,  992,
 /*   150 */  1000,  991,  987,  985,  984, 1014,  818,  818,  818,  857,
 /*   160 */   853,  853,  818,  818,  875,  873,  871,  863,  869,  865,
 /*   170 */   867,  861,  845,  818,  851,  851,  818,  851,  818,  851,
 /*   180 */   818,  818,  894,  912,  797, 1015,  797, 1055, 1005, 1045,
 /*   190 */  1044, 1051, 1043, 1042, 1041,  797,  797,  797, 1037, 1038,
 /*   200 */  1040, 1039,  797,  797, 1047, 1046,  797,  797,  797,  797,
 /*   210 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   220 */   797, 1017,  797, 1011, 1007,  797,  797,  797,  797,  797,
 /*   230 */   797,  797,  797,  797,  924,  797,  797,  797,  797,  797,
 /*   240 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  961,
 /*   250 */   797,  797,  797,  797,  797,  973,  972,  797,  797,  797,
 /*   260 */   797,  797,  797,  797,  797,  797, 1001,  797,  993,  797,
 /*   270 */   797,  797,  797,  797,  936,  797,  797,  797,  797,  797,
 /*   280 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   290 */  1068, 1066,  797,  797,  797,  797,  797,  797, 1062,  797,
 /*   300 */   797,  797, 1059,  797,  797,  797,  797,  797,  797,  797,
 /*   310 */   797,  797,  797,  797,  797,  797,  797,  797,  797,  797,
 /*   320 */   878,  797,  825,  823,  797,  814,  797,
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
    0,  /*     LENGTH => nothing */
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
  /*   88 */ "KEEP",
  /*   89 */ "CACHE",
  /*   90 */ "REPLICA",
  /*   91 */ "QUORUM",
  /*   92 */ "DAYS",
  /*   93 */ "MINROWS",
  /*   94 */ "MAXROWS",
  /*   95 */ "BLOCKS",
  /*   96 */ "CTIME",
  /*   97 */ "WAL",
  /*   98 */ "FSYNC",
  /*   99 */ "COMP",
  /*  100 */ "PRECISION",
  /*  101 */ "UPDATE",
  /*  102 */ "CACHELAST",
  /*  103 */ "PARTITIONS",
  /*  104 */ "LP",
  /*  105 */ "RP",
  /*  106 */ "UNSIGNED",
  /*  107 */ "TAGS",
  /*  108 */ "USING",
  /*  109 */ "COMMA",
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
  /*  120 */ "FILL",
  /*  121 */ "SLIDING",
  /*  122 */ "ORDER",
  /*  123 */ "BY",
  /*  124 */ "ASC",
  /*  125 */ "DESC",
  /*  126 */ "GROUP",
  /*  127 */ "HAVING",
  /*  128 */ "LIMIT",
  /*  129 */ "OFFSET",
  /*  130 */ "SLIMIT",
  /*  131 */ "SOFFSET",
  /*  132 */ "WHERE",
  /*  133 */ "NOW",
  /*  134 */ "RESET",
  /*  135 */ "QUERY",
  /*  136 */ "SYNCDB",
  /*  137 */ "ADD",
  /*  138 */ "COLUMN",
  /*  139 */ "LENGTH",
  /*  140 */ "TAG",
  /*  141 */ "CHANGE",
  /*  142 */ "SET",
  /*  143 */ "KILL",
  /*  144 */ "CONNECTION",
  /*  145 */ "STREAM",
  /*  146 */ "COLON",
  /*  147 */ "ABORT",
  /*  148 */ "AFTER",
  /*  149 */ "ATTACH",
  /*  150 */ "BEFORE",
  /*  151 */ "BEGIN",
  /*  152 */ "CASCADE",
  /*  153 */ "CLUSTER",
  /*  154 */ "CONFLICT",
  /*  155 */ "COPY",
  /*  156 */ "DEFERRED",
  /*  157 */ "DELIMITERS",
  /*  158 */ "DETACH",
  /*  159 */ "EACH",
  /*  160 */ "END",
  /*  161 */ "EXPLAIN",
  /*  162 */ "FAIL",
  /*  163 */ "FOR",
  /*  164 */ "IGNORE",
  /*  165 */ "IMMEDIATE",
  /*  166 */ "INITIALLY",
  /*  167 */ "INSTEAD",
  /*  168 */ "MATCH",
  /*  169 */ "KEY",
  /*  170 */ "OF",
  /*  171 */ "RAISE",
  /*  172 */ "REPLACE",
  /*  173 */ "RESTRICT",
  /*  174 */ "ROW",
  /*  175 */ "STATEMENT",
  /*  176 */ "TRIGGER",
  /*  177 */ "VIEW",
  /*  178 */ "SEMI",
  /*  179 */ "NONE",
  /*  180 */ "PREV",
  /*  181 */ "LINEAR",
  /*  182 */ "IMPORT",
  /*  183 */ "TBNAME",
  /*  184 */ "JOIN",
  /*  185 */ "INSERT",
  /*  186 */ "INTO",
  /*  187 */ "VALUES",
  /*  188 */ "program",
  /*  189 */ "cmd",
  /*  190 */ "dbPrefix",
  /*  191 */ "ids",
  /*  192 */ "cpxName",
  /*  193 */ "ifexists",
  /*  194 */ "alter_db_optr",
  /*  195 */ "alter_topic_optr",
  /*  196 */ "acct_optr",
  /*  197 */ "ifnotexists",
  /*  198 */ "db_optr",
  /*  199 */ "topic_optr",
  /*  200 */ "pps",
  /*  201 */ "tseries",
  /*  202 */ "dbs",
  /*  203 */ "streams",
  /*  204 */ "storage",
  /*  205 */ "qtime",
  /*  206 */ "users",
  /*  207 */ "conns",
  /*  208 */ "state",
  /*  209 */ "keep",
  /*  210 */ "tagitemlist",
  /*  211 */ "cache",
  /*  212 */ "replica",
  /*  213 */ "quorum",
  /*  214 */ "days",
  /*  215 */ "minrows",
  /*  216 */ "maxrows",
  /*  217 */ "blocks",
  /*  218 */ "ctime",
  /*  219 */ "wal",
  /*  220 */ "fsync",
  /*  221 */ "comp",
  /*  222 */ "prec",
  /*  223 */ "update",
  /*  224 */ "cachelast",
  /*  225 */ "partitions",
  /*  226 */ "typename",
  /*  227 */ "signed",
  /*  228 */ "create_table_args",
  /*  229 */ "create_stable_args",
  /*  230 */ "create_table_list",
  /*  231 */ "create_from_stable",
  /*  232 */ "columnlist",
  /*  233 */ "tagNamelist",
  /*  234 */ "select",
  /*  235 */ "column",
  /*  236 */ "tagitem",
  /*  237 */ "selcollist",
  /*  238 */ "from",
  /*  239 */ "where_opt",
  /*  240 */ "interval_opt",
  /*  241 */ "session_option",
  /*  242 */ "fill_opt",
  /*  243 */ "sliding_opt",
  /*  244 */ "groupby_opt",
  /*  245 */ "orderby_opt",
  /*  246 */ "having_opt",
  /*  247 */ "slimit_opt",
  /*  248 */ "limit_opt",
  /*  249 */ "union",
  /*  250 */ "sclp",
  /*  251 */ "distinct",
  /*  252 */ "expr",
  /*  253 */ "as",
  /*  254 */ "tablelist",
  /*  255 */ "sub",
  /*  256 */ "tmvar",
  /*  257 */ "sortlist",
  /*  258 */ "sortitem",
  /*  259 */ "item",
  /*  260 */ "sortorder",
  /*  261 */ "grouplist",
  /*  262 */ "exprlist",
  /*  263 */ "expritem",
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
 /*  78 */ "keep ::= KEEP tagitemlist",
 /*  79 */ "cache ::= CACHE INTEGER",
 /*  80 */ "replica ::= REPLICA INTEGER",
 /*  81 */ "quorum ::= QUORUM INTEGER",
 /*  82 */ "days ::= DAYS INTEGER",
 /*  83 */ "minrows ::= MINROWS INTEGER",
 /*  84 */ "maxrows ::= MAXROWS INTEGER",
 /*  85 */ "blocks ::= BLOCKS INTEGER",
 /*  86 */ "ctime ::= CTIME INTEGER",
 /*  87 */ "wal ::= WAL INTEGER",
 /*  88 */ "fsync ::= FSYNC INTEGER",
 /*  89 */ "comp ::= COMP INTEGER",
 /*  90 */ "prec ::= PRECISION STRING",
 /*  91 */ "update ::= UPDATE INTEGER",
 /*  92 */ "cachelast ::= CACHELAST INTEGER",
 /*  93 */ "partitions ::= PARTITIONS INTEGER",
 /*  94 */ "db_optr ::=",
 /*  95 */ "db_optr ::= db_optr cache",
 /*  96 */ "db_optr ::= db_optr replica",
 /*  97 */ "db_optr ::= db_optr quorum",
 /*  98 */ "db_optr ::= db_optr days",
 /*  99 */ "db_optr ::= db_optr minrows",
 /* 100 */ "db_optr ::= db_optr maxrows",
 /* 101 */ "db_optr ::= db_optr blocks",
 /* 102 */ "db_optr ::= db_optr ctime",
 /* 103 */ "db_optr ::= db_optr wal",
 /* 104 */ "db_optr ::= db_optr fsync",
 /* 105 */ "db_optr ::= db_optr comp",
 /* 106 */ "db_optr ::= db_optr prec",
 /* 107 */ "db_optr ::= db_optr keep",
 /* 108 */ "db_optr ::= db_optr update",
 /* 109 */ "db_optr ::= db_optr cachelast",
 /* 110 */ "topic_optr ::= db_optr",
 /* 111 */ "topic_optr ::= topic_optr partitions",
 /* 112 */ "alter_db_optr ::=",
 /* 113 */ "alter_db_optr ::= alter_db_optr replica",
 /* 114 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 115 */ "alter_db_optr ::= alter_db_optr keep",
 /* 116 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 117 */ "alter_db_optr ::= alter_db_optr comp",
 /* 118 */ "alter_db_optr ::= alter_db_optr wal",
 /* 119 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 120 */ "alter_db_optr ::= alter_db_optr update",
 /* 121 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 122 */ "alter_topic_optr ::= alter_db_optr",
 /* 123 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 124 */ "typename ::= ids",
 /* 125 */ "typename ::= ids LP signed RP",
 /* 126 */ "typename ::= ids UNSIGNED",
 /* 127 */ "signed ::= INTEGER",
 /* 128 */ "signed ::= PLUS INTEGER",
 /* 129 */ "signed ::= MINUS INTEGER",
 /* 130 */ "cmd ::= CREATE TABLE create_table_args",
 /* 131 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 133 */ "cmd ::= CREATE TABLE create_table_list",
 /* 134 */ "create_table_list ::= create_from_stable",
 /* 135 */ "create_table_list ::= create_table_list create_from_stable",
 /* 136 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 137 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 139 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 140 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 141 */ "tagNamelist ::= ids",
 /* 142 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 143 */ "columnlist ::= columnlist COMMA column",
 /* 144 */ "columnlist ::= column",
 /* 145 */ "column ::= ids typename",
 /* 146 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 147 */ "tagitemlist ::= tagitem",
 /* 148 */ "tagitem ::= INTEGER",
 /* 149 */ "tagitem ::= FLOAT",
 /* 150 */ "tagitem ::= STRING",
 /* 151 */ "tagitem ::= BOOL",
 /* 152 */ "tagitem ::= NULL",
 /* 153 */ "tagitem ::= MINUS INTEGER",
 /* 154 */ "tagitem ::= MINUS FLOAT",
 /* 155 */ "tagitem ::= PLUS INTEGER",
 /* 156 */ "tagitem ::= PLUS FLOAT",
 /* 157 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 158 */ "select ::= LP select RP",
 /* 159 */ "union ::= select",
 /* 160 */ "union ::= union UNION ALL select",
 /* 161 */ "cmd ::= union",
 /* 162 */ "select ::= SELECT selcollist",
 /* 163 */ "sclp ::= selcollist COMMA",
 /* 164 */ "sclp ::=",
 /* 165 */ "selcollist ::= sclp distinct expr as",
 /* 166 */ "selcollist ::= sclp STAR",
 /* 167 */ "as ::= AS ids",
 /* 168 */ "as ::= ids",
 /* 169 */ "as ::=",
 /* 170 */ "distinct ::= DISTINCT",
 /* 171 */ "distinct ::=",
 /* 172 */ "from ::= FROM tablelist",
 /* 173 */ "from ::= FROM sub",
 /* 174 */ "sub ::= LP union RP",
 /* 175 */ "sub ::= LP union RP ids",
 /* 176 */ "sub ::= sub COMMA LP union RP ids",
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
 /* 229 */ "expr ::= PLUS VARIABLE",
 /* 230 */ "expr ::= MINUS VARIABLE",
 /* 231 */ "expr ::= BOOL",
 /* 232 */ "expr ::= NULL",
 /* 233 */ "expr ::= ID LP exprlist RP",
 /* 234 */ "expr ::= ID LP STAR RP",
 /* 235 */ "expr ::= expr IS NULL",
 /* 236 */ "expr ::= expr IS NOT NULL",
 /* 237 */ "expr ::= expr LT expr",
 /* 238 */ "expr ::= expr GT expr",
 /* 239 */ "expr ::= expr LE expr",
 /* 240 */ "expr ::= expr GE expr",
 /* 241 */ "expr ::= expr NE expr",
 /* 242 */ "expr ::= expr EQ expr",
 /* 243 */ "expr ::= expr BETWEEN expr AND expr",
 /* 244 */ "expr ::= expr AND expr",
 /* 245 */ "expr ::= expr OR expr",
 /* 246 */ "expr ::= expr PLUS expr",
 /* 247 */ "expr ::= expr MINUS expr",
 /* 248 */ "expr ::= expr STAR expr",
 /* 249 */ "expr ::= expr SLASH expr",
 /* 250 */ "expr ::= expr REM expr",
 /* 251 */ "expr ::= expr LIKE expr",
 /* 252 */ "expr ::= expr IN LP exprlist RP",
 /* 253 */ "exprlist ::= exprlist COMMA expritem",
 /* 254 */ "exprlist ::= expritem",
 /* 255 */ "expritem ::= expr",
 /* 256 */ "expritem ::=",
 /* 257 */ "cmd ::= RESET QUERY CACHE",
 /* 258 */ "cmd ::= SYNCDB ids REPLICA",
 /* 259 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 260 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName ALTER COLUMN LENGTH ids INTEGER",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 265 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 268 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 269 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 270 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 271 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 272 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 273 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 209: /* keep */
    case 210: /* tagitemlist */
    case 232: /* columnlist */
    case 233: /* tagNamelist */
    case 242: /* fill_opt */
    case 244: /* groupby_opt */
    case 245: /* orderby_opt */
    case 257: /* sortlist */
    case 261: /* grouplist */
{
taosArrayDestroy((yypminor->yy285));
}
      break;
    case 230: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy470));
}
      break;
    case 234: /* select */
{
destroySqlNode((yypminor->yy344));
}
      break;
    case 237: /* selcollist */
    case 250: /* sclp */
    case 262: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy285));
}
      break;
    case 238: /* from */
    case 254: /* tablelist */
    case 255: /* sub */
{
destroyRelationInfo((yypminor->yy148));
}
      break;
    case 239: /* where_opt */
    case 246: /* having_opt */
    case 252: /* expr */
    case 263: /* expritem */
{
tSqlExprDestroy((yypminor->yy178));
}
      break;
    case 249: /* union */
{
destroyAllSqlNode((yypminor->yy285));
}
      break;
    case 258: /* sortitem */
{
tVariantDestroy(&(yypminor->yy362));
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
   188,  /* (0) program ::= cmd */
   189,  /* (1) cmd ::= SHOW DATABASES */
   189,  /* (2) cmd ::= SHOW TOPICS */
   189,  /* (3) cmd ::= SHOW MNODES */
   189,  /* (4) cmd ::= SHOW DNODES */
   189,  /* (5) cmd ::= SHOW ACCOUNTS */
   189,  /* (6) cmd ::= SHOW USERS */
   189,  /* (7) cmd ::= SHOW MODULES */
   189,  /* (8) cmd ::= SHOW QUERIES */
   189,  /* (9) cmd ::= SHOW CONNECTIONS */
   189,  /* (10) cmd ::= SHOW STREAMS */
   189,  /* (11) cmd ::= SHOW VARIABLES */
   189,  /* (12) cmd ::= SHOW SCORES */
   189,  /* (13) cmd ::= SHOW GRANTS */
   189,  /* (14) cmd ::= SHOW VNODES */
   189,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
   190,  /* (16) dbPrefix ::= */
   190,  /* (17) dbPrefix ::= ids DOT */
   192,  /* (18) cpxName ::= */
   192,  /* (19) cpxName ::= DOT ids */
   189,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   189,  /* (21) cmd ::= SHOW CREATE STABLE ids cpxName */
   189,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   189,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   189,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   189,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   189,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   189,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   189,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   189,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   189,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   189,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   189,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   189,  /* (33) cmd ::= DROP DNODE ids */
   189,  /* (34) cmd ::= DROP USER ids */
   189,  /* (35) cmd ::= DROP ACCOUNT ids */
   189,  /* (36) cmd ::= USE ids */
   189,  /* (37) cmd ::= DESCRIBE ids cpxName */
   189,  /* (38) cmd ::= ALTER USER ids PASS ids */
   189,  /* (39) cmd ::= ALTER USER ids PRIVILEGE ids */
   189,  /* (40) cmd ::= ALTER DNODE ids ids */
   189,  /* (41) cmd ::= ALTER DNODE ids ids ids */
   189,  /* (42) cmd ::= ALTER LOCAL ids */
   189,  /* (43) cmd ::= ALTER LOCAL ids ids */
   189,  /* (44) cmd ::= ALTER DATABASE ids alter_db_optr */
   189,  /* (45) cmd ::= ALTER TOPIC ids alter_topic_optr */
   189,  /* (46) cmd ::= ALTER ACCOUNT ids acct_optr */
   189,  /* (47) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   191,  /* (48) ids ::= ID */
   191,  /* (49) ids ::= STRING */
   193,  /* (50) ifexists ::= IF EXISTS */
   193,  /* (51) ifexists ::= */
   197,  /* (52) ifnotexists ::= IF NOT EXISTS */
   197,  /* (53) ifnotexists ::= */
   189,  /* (54) cmd ::= CREATE DNODE ids */
   189,  /* (55) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   189,  /* (56) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   189,  /* (57) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   189,  /* (58) cmd ::= CREATE USER ids PASS ids */
   200,  /* (59) pps ::= */
   200,  /* (60) pps ::= PPS INTEGER */
   201,  /* (61) tseries ::= */
   201,  /* (62) tseries ::= TSERIES INTEGER */
   202,  /* (63) dbs ::= */
   202,  /* (64) dbs ::= DBS INTEGER */
   203,  /* (65) streams ::= */
   203,  /* (66) streams ::= STREAMS INTEGER */
   204,  /* (67) storage ::= */
   204,  /* (68) storage ::= STORAGE INTEGER */
   205,  /* (69) qtime ::= */
   205,  /* (70) qtime ::= QTIME INTEGER */
   206,  /* (71) users ::= */
   206,  /* (72) users ::= USERS INTEGER */
   207,  /* (73) conns ::= */
   207,  /* (74) conns ::= CONNS INTEGER */
   208,  /* (75) state ::= */
   208,  /* (76) state ::= STATE ids */
   196,  /* (77) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   209,  /* (78) keep ::= KEEP tagitemlist */
   211,  /* (79) cache ::= CACHE INTEGER */
   212,  /* (80) replica ::= REPLICA INTEGER */
   213,  /* (81) quorum ::= QUORUM INTEGER */
   214,  /* (82) days ::= DAYS INTEGER */
   215,  /* (83) minrows ::= MINROWS INTEGER */
   216,  /* (84) maxrows ::= MAXROWS INTEGER */
   217,  /* (85) blocks ::= BLOCKS INTEGER */
   218,  /* (86) ctime ::= CTIME INTEGER */
   219,  /* (87) wal ::= WAL INTEGER */
   220,  /* (88) fsync ::= FSYNC INTEGER */
   221,  /* (89) comp ::= COMP INTEGER */
   222,  /* (90) prec ::= PRECISION STRING */
   223,  /* (91) update ::= UPDATE INTEGER */
   224,  /* (92) cachelast ::= CACHELAST INTEGER */
   225,  /* (93) partitions ::= PARTITIONS INTEGER */
   198,  /* (94) db_optr ::= */
   198,  /* (95) db_optr ::= db_optr cache */
   198,  /* (96) db_optr ::= db_optr replica */
   198,  /* (97) db_optr ::= db_optr quorum */
   198,  /* (98) db_optr ::= db_optr days */
   198,  /* (99) db_optr ::= db_optr minrows */
   198,  /* (100) db_optr ::= db_optr maxrows */
   198,  /* (101) db_optr ::= db_optr blocks */
   198,  /* (102) db_optr ::= db_optr ctime */
   198,  /* (103) db_optr ::= db_optr wal */
   198,  /* (104) db_optr ::= db_optr fsync */
   198,  /* (105) db_optr ::= db_optr comp */
   198,  /* (106) db_optr ::= db_optr prec */
   198,  /* (107) db_optr ::= db_optr keep */
   198,  /* (108) db_optr ::= db_optr update */
   198,  /* (109) db_optr ::= db_optr cachelast */
   199,  /* (110) topic_optr ::= db_optr */
   199,  /* (111) topic_optr ::= topic_optr partitions */
   194,  /* (112) alter_db_optr ::= */
   194,  /* (113) alter_db_optr ::= alter_db_optr replica */
   194,  /* (114) alter_db_optr ::= alter_db_optr quorum */
   194,  /* (115) alter_db_optr ::= alter_db_optr keep */
   194,  /* (116) alter_db_optr ::= alter_db_optr blocks */
   194,  /* (117) alter_db_optr ::= alter_db_optr comp */
   194,  /* (118) alter_db_optr ::= alter_db_optr wal */
   194,  /* (119) alter_db_optr ::= alter_db_optr fsync */
   194,  /* (120) alter_db_optr ::= alter_db_optr update */
   194,  /* (121) alter_db_optr ::= alter_db_optr cachelast */
   195,  /* (122) alter_topic_optr ::= alter_db_optr */
   195,  /* (123) alter_topic_optr ::= alter_topic_optr partitions */
   226,  /* (124) typename ::= ids */
   226,  /* (125) typename ::= ids LP signed RP */
   226,  /* (126) typename ::= ids UNSIGNED */
   227,  /* (127) signed ::= INTEGER */
   227,  /* (128) signed ::= PLUS INTEGER */
   227,  /* (129) signed ::= MINUS INTEGER */
   189,  /* (130) cmd ::= CREATE TABLE create_table_args */
   189,  /* (131) cmd ::= CREATE TABLE create_stable_args */
   189,  /* (132) cmd ::= CREATE STABLE create_stable_args */
   189,  /* (133) cmd ::= CREATE TABLE create_table_list */
   230,  /* (134) create_table_list ::= create_from_stable */
   230,  /* (135) create_table_list ::= create_table_list create_from_stable */
   228,  /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   229,  /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   231,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   231,  /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   233,  /* (140) tagNamelist ::= tagNamelist COMMA ids */
   233,  /* (141) tagNamelist ::= ids */
   228,  /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
   232,  /* (143) columnlist ::= columnlist COMMA column */
   232,  /* (144) columnlist ::= column */
   235,  /* (145) column ::= ids typename */
   210,  /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
   210,  /* (147) tagitemlist ::= tagitem */
   236,  /* (148) tagitem ::= INTEGER */
   236,  /* (149) tagitem ::= FLOAT */
   236,  /* (150) tagitem ::= STRING */
   236,  /* (151) tagitem ::= BOOL */
   236,  /* (152) tagitem ::= NULL */
   236,  /* (153) tagitem ::= MINUS INTEGER */
   236,  /* (154) tagitem ::= MINUS FLOAT */
   236,  /* (155) tagitem ::= PLUS INTEGER */
   236,  /* (156) tagitem ::= PLUS FLOAT */
   234,  /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   234,  /* (158) select ::= LP select RP */
   249,  /* (159) union ::= select */
   249,  /* (160) union ::= union UNION ALL select */
   189,  /* (161) cmd ::= union */
   234,  /* (162) select ::= SELECT selcollist */
   250,  /* (163) sclp ::= selcollist COMMA */
   250,  /* (164) sclp ::= */
   237,  /* (165) selcollist ::= sclp distinct expr as */
   237,  /* (166) selcollist ::= sclp STAR */
   253,  /* (167) as ::= AS ids */
   253,  /* (168) as ::= ids */
   253,  /* (169) as ::= */
   251,  /* (170) distinct ::= DISTINCT */
   251,  /* (171) distinct ::= */
   238,  /* (172) from ::= FROM tablelist */
   238,  /* (173) from ::= FROM sub */
   255,  /* (174) sub ::= LP union RP */
   255,  /* (175) sub ::= LP union RP ids */
   255,  /* (176) sub ::= sub COMMA LP union RP ids */
   254,  /* (177) tablelist ::= ids cpxName */
   254,  /* (178) tablelist ::= ids cpxName ids */
   254,  /* (179) tablelist ::= tablelist COMMA ids cpxName */
   254,  /* (180) tablelist ::= tablelist COMMA ids cpxName ids */
   256,  /* (181) tmvar ::= VARIABLE */
   240,  /* (182) interval_opt ::= INTERVAL LP tmvar RP */
   240,  /* (183) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   240,  /* (184) interval_opt ::= */
   241,  /* (185) session_option ::= */
   241,  /* (186) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   242,  /* (187) fill_opt ::= */
   242,  /* (188) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   242,  /* (189) fill_opt ::= FILL LP ID RP */
   243,  /* (190) sliding_opt ::= SLIDING LP tmvar RP */
   243,  /* (191) sliding_opt ::= */
   245,  /* (192) orderby_opt ::= */
   245,  /* (193) orderby_opt ::= ORDER BY sortlist */
   257,  /* (194) sortlist ::= sortlist COMMA item sortorder */
   257,  /* (195) sortlist ::= item sortorder */
   259,  /* (196) item ::= ids cpxName */
   260,  /* (197) sortorder ::= ASC */
   260,  /* (198) sortorder ::= DESC */
   260,  /* (199) sortorder ::= */
   244,  /* (200) groupby_opt ::= */
   244,  /* (201) groupby_opt ::= GROUP BY grouplist */
   261,  /* (202) grouplist ::= grouplist COMMA item */
   261,  /* (203) grouplist ::= item */
   246,  /* (204) having_opt ::= */
   246,  /* (205) having_opt ::= HAVING expr */
   248,  /* (206) limit_opt ::= */
   248,  /* (207) limit_opt ::= LIMIT signed */
   248,  /* (208) limit_opt ::= LIMIT signed OFFSET signed */
   248,  /* (209) limit_opt ::= LIMIT signed COMMA signed */
   247,  /* (210) slimit_opt ::= */
   247,  /* (211) slimit_opt ::= SLIMIT signed */
   247,  /* (212) slimit_opt ::= SLIMIT signed SOFFSET signed */
   247,  /* (213) slimit_opt ::= SLIMIT signed COMMA signed */
   239,  /* (214) where_opt ::= */
   239,  /* (215) where_opt ::= WHERE expr */
   252,  /* (216) expr ::= LP expr RP */
   252,  /* (217) expr ::= ID */
   252,  /* (218) expr ::= ID DOT ID */
   252,  /* (219) expr ::= ID DOT STAR */
   252,  /* (220) expr ::= INTEGER */
   252,  /* (221) expr ::= MINUS INTEGER */
   252,  /* (222) expr ::= PLUS INTEGER */
   252,  /* (223) expr ::= FLOAT */
   252,  /* (224) expr ::= MINUS FLOAT */
   252,  /* (225) expr ::= PLUS FLOAT */
   252,  /* (226) expr ::= STRING */
   252,  /* (227) expr ::= NOW */
   252,  /* (228) expr ::= VARIABLE */
   252,  /* (229) expr ::= PLUS VARIABLE */
   252,  /* (230) expr ::= MINUS VARIABLE */
   252,  /* (231) expr ::= BOOL */
   252,  /* (232) expr ::= NULL */
   252,  /* (233) expr ::= ID LP exprlist RP */
   252,  /* (234) expr ::= ID LP STAR RP */
   252,  /* (235) expr ::= expr IS NULL */
   252,  /* (236) expr ::= expr IS NOT NULL */
   252,  /* (237) expr ::= expr LT expr */
   252,  /* (238) expr ::= expr GT expr */
   252,  /* (239) expr ::= expr LE expr */
   252,  /* (240) expr ::= expr GE expr */
   252,  /* (241) expr ::= expr NE expr */
   252,  /* (242) expr ::= expr EQ expr */
   252,  /* (243) expr ::= expr BETWEEN expr AND expr */
   252,  /* (244) expr ::= expr AND expr */
   252,  /* (245) expr ::= expr OR expr */
   252,  /* (246) expr ::= expr PLUS expr */
   252,  /* (247) expr ::= expr MINUS expr */
   252,  /* (248) expr ::= expr STAR expr */
   252,  /* (249) expr ::= expr SLASH expr */
   252,  /* (250) expr ::= expr REM expr */
   252,  /* (251) expr ::= expr LIKE expr */
   252,  /* (252) expr ::= expr IN LP exprlist RP */
   262,  /* (253) exprlist ::= exprlist COMMA expritem */
   262,  /* (254) exprlist ::= expritem */
   263,  /* (255) expritem ::= expr */
   263,  /* (256) expritem ::= */
   189,  /* (257) cmd ::= RESET QUERY CACHE */
   189,  /* (258) cmd ::= SYNCDB ids REPLICA */
   189,  /* (259) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   189,  /* (260) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   189,  /* (261) cmd ::= ALTER TABLE ids cpxName ALTER COLUMN LENGTH ids INTEGER */
   189,  /* (262) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   189,  /* (263) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   189,  /* (264) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   189,  /* (265) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   189,  /* (266) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   189,  /* (267) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   189,  /* (268) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   189,  /* (269) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   189,  /* (270) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   189,  /* (271) cmd ::= KILL CONNECTION INTEGER */
   189,  /* (272) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   189,  /* (273) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -2,  /* (78) keep ::= KEEP tagitemlist */
   -2,  /* (79) cache ::= CACHE INTEGER */
   -2,  /* (80) replica ::= REPLICA INTEGER */
   -2,  /* (81) quorum ::= QUORUM INTEGER */
   -2,  /* (82) days ::= DAYS INTEGER */
   -2,  /* (83) minrows ::= MINROWS INTEGER */
   -2,  /* (84) maxrows ::= MAXROWS INTEGER */
   -2,  /* (85) blocks ::= BLOCKS INTEGER */
   -2,  /* (86) ctime ::= CTIME INTEGER */
   -2,  /* (87) wal ::= WAL INTEGER */
   -2,  /* (88) fsync ::= FSYNC INTEGER */
   -2,  /* (89) comp ::= COMP INTEGER */
   -2,  /* (90) prec ::= PRECISION STRING */
   -2,  /* (91) update ::= UPDATE INTEGER */
   -2,  /* (92) cachelast ::= CACHELAST INTEGER */
   -2,  /* (93) partitions ::= PARTITIONS INTEGER */
    0,  /* (94) db_optr ::= */
   -2,  /* (95) db_optr ::= db_optr cache */
   -2,  /* (96) db_optr ::= db_optr replica */
   -2,  /* (97) db_optr ::= db_optr quorum */
   -2,  /* (98) db_optr ::= db_optr days */
   -2,  /* (99) db_optr ::= db_optr minrows */
   -2,  /* (100) db_optr ::= db_optr maxrows */
   -2,  /* (101) db_optr ::= db_optr blocks */
   -2,  /* (102) db_optr ::= db_optr ctime */
   -2,  /* (103) db_optr ::= db_optr wal */
   -2,  /* (104) db_optr ::= db_optr fsync */
   -2,  /* (105) db_optr ::= db_optr comp */
   -2,  /* (106) db_optr ::= db_optr prec */
   -2,  /* (107) db_optr ::= db_optr keep */
   -2,  /* (108) db_optr ::= db_optr update */
   -2,  /* (109) db_optr ::= db_optr cachelast */
   -1,  /* (110) topic_optr ::= db_optr */
   -2,  /* (111) topic_optr ::= topic_optr partitions */
    0,  /* (112) alter_db_optr ::= */
   -2,  /* (113) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (114) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (115) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (116) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (117) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (118) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (119) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (120) alter_db_optr ::= alter_db_optr update */
   -2,  /* (121) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (122) alter_topic_optr ::= alter_db_optr */
   -2,  /* (123) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (124) typename ::= ids */
   -4,  /* (125) typename ::= ids LP signed RP */
   -2,  /* (126) typename ::= ids UNSIGNED */
   -1,  /* (127) signed ::= INTEGER */
   -2,  /* (128) signed ::= PLUS INTEGER */
   -2,  /* (129) signed ::= MINUS INTEGER */
   -3,  /* (130) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (131) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (132) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (133) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (134) create_table_list ::= create_from_stable */
   -2,  /* (135) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (136) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (137) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (139) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (140) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (141) tagNamelist ::= ids */
   -5,  /* (142) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (143) columnlist ::= columnlist COMMA column */
   -1,  /* (144) columnlist ::= column */
   -2,  /* (145) column ::= ids typename */
   -3,  /* (146) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (147) tagitemlist ::= tagitem */
   -1,  /* (148) tagitem ::= INTEGER */
   -1,  /* (149) tagitem ::= FLOAT */
   -1,  /* (150) tagitem ::= STRING */
   -1,  /* (151) tagitem ::= BOOL */
   -1,  /* (152) tagitem ::= NULL */
   -2,  /* (153) tagitem ::= MINUS INTEGER */
   -2,  /* (154) tagitem ::= MINUS FLOAT */
   -2,  /* (155) tagitem ::= PLUS INTEGER */
   -2,  /* (156) tagitem ::= PLUS FLOAT */
  -13,  /* (157) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (158) select ::= LP select RP */
   -1,  /* (159) union ::= select */
   -4,  /* (160) union ::= union UNION ALL select */
   -1,  /* (161) cmd ::= union */
   -2,  /* (162) select ::= SELECT selcollist */
   -2,  /* (163) sclp ::= selcollist COMMA */
    0,  /* (164) sclp ::= */
   -4,  /* (165) selcollist ::= sclp distinct expr as */
   -2,  /* (166) selcollist ::= sclp STAR */
   -2,  /* (167) as ::= AS ids */
   -1,  /* (168) as ::= ids */
    0,  /* (169) as ::= */
   -1,  /* (170) distinct ::= DISTINCT */
    0,  /* (171) distinct ::= */
   -2,  /* (172) from ::= FROM tablelist */
   -2,  /* (173) from ::= FROM sub */
   -3,  /* (174) sub ::= LP union RP */
   -4,  /* (175) sub ::= LP union RP ids */
   -6,  /* (176) sub ::= sub COMMA LP union RP ids */
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
   -2,  /* (229) expr ::= PLUS VARIABLE */
   -2,  /* (230) expr ::= MINUS VARIABLE */
   -1,  /* (231) expr ::= BOOL */
   -1,  /* (232) expr ::= NULL */
   -4,  /* (233) expr ::= ID LP exprlist RP */
   -4,  /* (234) expr ::= ID LP STAR RP */
   -3,  /* (235) expr ::= expr IS NULL */
   -4,  /* (236) expr ::= expr IS NOT NULL */
   -3,  /* (237) expr ::= expr LT expr */
   -3,  /* (238) expr ::= expr GT expr */
   -3,  /* (239) expr ::= expr LE expr */
   -3,  /* (240) expr ::= expr GE expr */
   -3,  /* (241) expr ::= expr NE expr */
   -3,  /* (242) expr ::= expr EQ expr */
   -5,  /* (243) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (244) expr ::= expr AND expr */
   -3,  /* (245) expr ::= expr OR expr */
   -3,  /* (246) expr ::= expr PLUS expr */
   -3,  /* (247) expr ::= expr MINUS expr */
   -3,  /* (248) expr ::= expr STAR expr */
   -3,  /* (249) expr ::= expr SLASH expr */
   -3,  /* (250) expr ::= expr REM expr */
   -3,  /* (251) expr ::= expr LIKE expr */
   -5,  /* (252) expr ::= expr IN LP exprlist RP */
   -3,  /* (253) exprlist ::= exprlist COMMA expritem */
   -1,  /* (254) exprlist ::= expritem */
   -1,  /* (255) expritem ::= expr */
    0,  /* (256) expritem ::= */
   -3,  /* (257) cmd ::= RESET QUERY CACHE */
   -3,  /* (258) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (259) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (260) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -9,  /* (261) cmd ::= ALTER TABLE ids cpxName ALTER COLUMN LENGTH ids INTEGER */
   -7,  /* (262) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (263) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (264) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (265) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (266) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (267) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (268) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (269) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (270) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (271) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (272) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (273) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 130: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==131);
      case 132: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==132);
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &t);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy187);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
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
      case 171: /* distinct ::= */ yytestcase(yyruleno==171);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 52: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 54: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 55: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy187);}
        break;
      case 56: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 57: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==57);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy526, &yymsp[-2].minor.yy0);}
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
    yylhsminor.yy187.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy187.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy187.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy187.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy187.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy187.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy187.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy187.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy187 = yylhsminor.yy187;
        break;
      case 78: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy285 = yymsp[0].minor.yy285; }
        break;
      case 79: /* cache ::= CACHE INTEGER */
      case 80: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==80);
      case 81: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==81);
      case 82: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==85);
      case 86: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==86);
      case 87: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==87);
      case 88: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==88);
      case 89: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==89);
      case 90: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==90);
      case 91: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==91);
      case 92: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==92);
      case 93: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==93);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 94: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 95: /* db_optr ::= db_optr cache */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 96: /* db_optr ::= db_optr replica */
      case 113: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==113);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 97: /* db_optr ::= db_optr quorum */
      case 114: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==114);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 98: /* db_optr ::= db_optr days */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 99: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 100: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 101: /* db_optr ::= db_optr blocks */
      case 116: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==116);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 102: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 103: /* db_optr ::= db_optr wal */
      case 118: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==118);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 104: /* db_optr ::= db_optr fsync */
      case 119: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==119);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 105: /* db_optr ::= db_optr comp */
      case 117: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==117);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 106: /* db_optr ::= db_optr prec */
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 107: /* db_optr ::= db_optr keep */
      case 115: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==115);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.keep = yymsp[0].minor.yy285; }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 108: /* db_optr ::= db_optr update */
      case 120: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==120);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 109: /* db_optr ::= db_optr cachelast */
      case 121: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==121);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 110: /* topic_optr ::= db_optr */
      case 122: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==122);
{ yylhsminor.yy526 = yymsp[0].minor.yy526; yylhsminor.yy526.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy526 = yylhsminor.yy526;
        break;
      case 111: /* topic_optr ::= topic_optr partitions */
      case 123: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==123);
{ yylhsminor.yy526 = yymsp[-1].minor.yy526; yylhsminor.yy526.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy526 = yylhsminor.yy526;
        break;
      case 112: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy526); yymsp[1].minor.yy526.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 124: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy295, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy295 = yylhsminor.yy295;
        break;
      case 125: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy525 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy525;  // negative value of name length
    tSetColumnType(&yylhsminor.yy295, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy295 = yylhsminor.yy295;
        break;
      case 126: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy295, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 127: /* signed ::= INTEGER */
{ yylhsminor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy525 = yylhsminor.yy525;
        break;
      case 128: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy525 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 129: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy525 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 133: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy470;}
        break;
      case 134: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy470 = pCreateTable;
}
  yymsp[0].minor.yy470 = yylhsminor.yy470;
        break;
      case 135: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy470->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy470 = yymsp[-1].minor.yy470;
}
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 136: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy470 = tSetCreateTableInfo(yymsp[-1].minor.yy285, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy470 = yylhsminor.yy470;
        break;
      case 137: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy470 = tSetCreateTableInfo(yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy470 = yylhsminor.yy470;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy285, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 139: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy285, yymsp[-1].minor.yy285, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 140: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy0); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 141: /* tagNamelist ::= ids */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 142: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy470 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy344, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy470, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy470 = yylhsminor.yy470;
        break;
      case 143: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy285, &yymsp[0].minor.yy295); yylhsminor.yy285 = yymsp[-2].minor.yy285;  }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 144: /* columnlist ::= column */
{yylhsminor.yy285 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy285, &yymsp[0].minor.yy295);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 145: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy295, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy295);
}
  yymsp[-1].minor.yy295 = yylhsminor.yy295;
        break;
      case 146: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);    }
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 147: /* tagitemlist ::= tagitem */
{ yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1); }
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 148: /* tagitem ::= INTEGER */
      case 149: /* tagitem ::= FLOAT */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= STRING */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= BOOL */ yytestcase(yyruleno==151);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 152: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy362, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy362 = yylhsminor.yy362;
        break;
      case 153: /* tagitem ::= MINUS INTEGER */
      case 154: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==156);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 157: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy344 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy285, yymsp[-10].minor.yy148, yymsp[-9].minor.yy178, yymsp[-4].minor.yy285, yymsp[-3].minor.yy285, &yymsp[-8].minor.yy376, &yymsp[-7].minor.yy523, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy285, &yymsp[0].minor.yy438, &yymsp[-1].minor.yy438, yymsp[-2].minor.yy178);
}
  yymsp[-12].minor.yy344 = yylhsminor.yy344;
        break;
      case 158: /* select ::= LP select RP */
{yymsp[-2].minor.yy344 = yymsp[-1].minor.yy344;}
        break;
      case 159: /* union ::= select */
{ yylhsminor.yy285 = setSubclause(NULL, yymsp[0].minor.yy344); }
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 160: /* union ::= union UNION ALL select */
{ yylhsminor.yy285 = appendSelectClause(yymsp[-3].minor.yy285, yymsp[0].minor.yy344); }
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 161: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy285, NULL, TSDB_SQL_SELECT); }
        break;
      case 162: /* select ::= SELECT selcollist */
{
  yylhsminor.yy344 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy285, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy344 = yylhsminor.yy344;
        break;
      case 163: /* sclp ::= selcollist COMMA */
{yylhsminor.yy285 = yymsp[-1].minor.yy285;}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 164: /* sclp ::= */
      case 192: /* orderby_opt ::= */ yytestcase(yyruleno==192);
{yymsp[1].minor.yy285 = 0;}
        break;
      case 165: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-3].minor.yy285, yymsp[-1].minor.yy178,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 166: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy285 = tSqlExprListAppend(yymsp[-1].minor.yy285, pNode, 0, 0);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 167: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 168: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 169: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 170: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 172: /* from ::= FROM tablelist */
      case 173: /* from ::= FROM sub */ yytestcase(yyruleno==173);
{yymsp[-1].minor.yy148 = yymsp[0].minor.yy148;}
        break;
      case 174: /* sub ::= LP union RP */
{yymsp[-2].minor.yy148 = addSubqueryElem(NULL, yymsp[-1].minor.yy285, NULL);}
        break;
      case 175: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy148 = addSubqueryElem(NULL, yymsp[-2].minor.yy285, &yymsp[0].minor.yy0);}
        break;
      case 176: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy148 = addSubqueryElem(yymsp[-5].minor.yy148, yymsp[-2].minor.yy285, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy148 = yylhsminor.yy148;
        break;
      case 177: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy148 = yylhsminor.yy148;
        break;
      case 178: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy148 = yylhsminor.yy148;
        break;
      case 179: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(yymsp[-3].minor.yy148, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy148 = yylhsminor.yy148;
        break;
      case 180: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy148 = setTableNameList(yymsp[-4].minor.yy148, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy148 = yylhsminor.yy148;
        break;
      case 181: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy376.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy376.offset.n = 0;}
        break;
      case 183: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy376.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy376.offset = yymsp[-1].minor.yy0;}
        break;
      case 184: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy376, 0, sizeof(yymsp[1].minor.yy376));}
        break;
      case 185: /* session_option ::= */
{yymsp[1].minor.yy523.col.n = 0; yymsp[1].minor.yy523.gap.n = 0;}
        break;
      case 186: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy523.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy523.gap = yymsp[-1].minor.yy0;
}
        break;
      case 187: /* fill_opt ::= */
{ yymsp[1].minor.yy285 = 0;     }
        break;
      case 188: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy285, &A, -1, 0);
    yymsp[-5].minor.yy285 = yymsp[-1].minor.yy285;
}
        break;
      case 189: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy285 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 190: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 191: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 193: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 194: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy285 = tVariantListAppend(yymsp[-3].minor.yy285, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-3].minor.yy285 = yylhsminor.yy285;
        break;
      case 195: /* sortlist ::= item sortorder */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[-1].minor.yy362, yymsp[0].minor.yy460);
}
  yymsp[-1].minor.yy285 = yylhsminor.yy285;
        break;
      case 196: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy362, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy362 = yylhsminor.yy362;
        break;
      case 197: /* sortorder ::= ASC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 198: /* sortorder ::= DESC */
{ yymsp[0].minor.yy460 = TSDB_ORDER_DESC;}
        break;
      case 199: /* sortorder ::= */
{ yymsp[1].minor.yy460 = TSDB_ORDER_ASC; }
        break;
      case 200: /* groupby_opt ::= */
{ yymsp[1].minor.yy285 = 0;}
        break;
      case 201: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy285 = yymsp[0].minor.yy285;}
        break;
      case 202: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy285 = tVariantListAppend(yymsp[-2].minor.yy285, &yymsp[0].minor.yy362, -1);
}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 203: /* grouplist ::= item */
{
  yylhsminor.yy285 = tVariantListAppend(NULL, &yymsp[0].minor.yy362, -1);
}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 204: /* having_opt ::= */
      case 214: /* where_opt ::= */ yytestcase(yyruleno==214);
      case 256: /* expritem ::= */ yytestcase(yyruleno==256);
{yymsp[1].minor.yy178 = 0;}
        break;
      case 205: /* having_opt ::= HAVING expr */
      case 215: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==215);
{yymsp[-1].minor.yy178 = yymsp[0].minor.yy178;}
        break;
      case 206: /* limit_opt ::= */
      case 210: /* slimit_opt ::= */ yytestcase(yyruleno==210);
{yymsp[1].minor.yy438.limit = -1; yymsp[1].minor.yy438.offset = 0;}
        break;
      case 207: /* limit_opt ::= LIMIT signed */
      case 211: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-1].minor.yy438.offset = 0;}
        break;
      case 208: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 209: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 212: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy438.limit = yymsp[-2].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[0].minor.yy525;}
        break;
      case 213: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy438.limit = yymsp[0].minor.yy525;  yymsp[-3].minor.yy438.offset = yymsp[-2].minor.yy525;}
        break;
      case 216: /* expr ::= LP expr RP */
{yylhsminor.yy178 = yymsp[-1].minor.yy178; yylhsminor.yy178->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy178->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 217: /* expr ::= ID */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 218: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 219: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 220: /* expr ::= INTEGER */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 221: /* expr ::= MINUS INTEGER */
      case 222: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 223: /* expr ::= FLOAT */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 224: /* expr ::= MINUS FLOAT */
      case 225: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==225);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 226: /* expr ::= STRING */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 227: /* expr ::= NOW */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 228: /* expr ::= VARIABLE */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 229: /* expr ::= PLUS VARIABLE */
      case 230: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==230);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy178 = yylhsminor.yy178;
        break;
      case 231: /* expr ::= BOOL */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 232: /* expr ::= NULL */
{ yylhsminor.yy178 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 233: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(yymsp[-1].minor.yy285, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 234: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy178 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 235: /* expr ::= expr IS NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 236: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-3].minor.yy178, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy178 = yylhsminor.yy178;
        break;
      case 237: /* expr ::= expr LT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 238: /* expr ::= expr GT expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GT);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 239: /* expr ::= expr LE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 240: /* expr ::= expr GE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_GE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 241: /* expr ::= expr NE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_NE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 242: /* expr ::= expr EQ expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_EQ);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 243: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy178); yylhsminor.yy178 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy178, yymsp[-2].minor.yy178, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy178, TK_LE), TK_AND);}
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 244: /* expr ::= expr AND expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_AND);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 245: /* expr ::= expr OR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_OR); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 246: /* expr ::= expr PLUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_PLUS);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 247: /* expr ::= expr MINUS expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_MINUS); }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 248: /* expr ::= expr STAR expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_STAR);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 249: /* expr ::= expr SLASH expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_DIVIDE);}
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 250: /* expr ::= expr REM expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_REM);   }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 251: /* expr ::= expr LIKE expr */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-2].minor.yy178, yymsp[0].minor.yy178, TK_LIKE);  }
  yymsp[-2].minor.yy178 = yylhsminor.yy178;
        break;
      case 252: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy178 = tSqlExprCreate(yymsp[-4].minor.yy178, (tSqlExpr*)yymsp[-1].minor.yy285, TK_IN); }
  yymsp[-4].minor.yy178 = yylhsminor.yy178;
        break;
      case 253: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy285 = tSqlExprListAppend(yymsp[-2].minor.yy285,yymsp[0].minor.yy178,0, 0);}
  yymsp[-2].minor.yy285 = yylhsminor.yy285;
        break;
      case 254: /* exprlist ::= expritem */
{yylhsminor.yy285 = tSqlExprListAppend(0,yymsp[0].minor.yy178,0, 0);}
  yymsp[0].minor.yy285 = yylhsminor.yy285;
        break;
      case 255: /* expritem ::= expr */
{yylhsminor.yy178 = yymsp[0].minor.yy178;}
  yymsp[0].minor.yy178 = yylhsminor.yy178;
        break;
      case 257: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 258: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 259: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER TABLE ids cpxName ALTER COLUMN LENGTH ids INTEGER */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
    toTSDBType(yymsp[0].minor.yy0.type);
    K = tVariantListAppendToken(K, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, K, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 264: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 265: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy362, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy285, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 271: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 272: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 273: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
