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
#define YYNOCODE 268
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SCreateDbInfo yy22;
  TAOS_FIELD yy47;
  SCreateAcctInfo yy83;
  SSessionWindowVal yy84;
  SSubclauseInfo* yy145;
  tSqlExpr* yy162;
  int yy196;
  SLimitVal yy230;
  SArray* yy325;
  SIntervalVal yy328;
  int64_t yy373;
  SCreateTableSql* yy422;
  tVariant yy442;
  SQuerySqlNode* yy494;
  SCreatedTableInfo yy504;
  SFromInfo* yy506;
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
#define YYNSTATE             330
#define YYNRULE              273
#define YYNRULE_WITH_ACTION  273
#define YYNTOKEN             192
#define YY_MAX_SHIFT         329
#define YY_MIN_SHIFTREDUCE   526
#define YY_MAX_SHIFTREDUCE   798
#define YY_ERROR_ACTION      799
#define YY_ACCEPT_ACTION     800
#define YY_NO_ACTION         801
#define YY_MIN_REDUCE        802
#define YY_MAX_REDUCE        1074
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
 /*     0 */   975,  575,  190,  575,  211,  327,  235,   17,   84,  576,
 /*    10 */   575,  576, 1055,   49,   50,  161,   53,   54,  576,  215,
 /*    20 */   223,   43,  190,   52,  271,   57,   55,   59,   56,   32,
 /*    30 */   140,  218, 1056,   48,   47,  800,  329,   46,   45,   44,
 /*    40 */   937,  938,   29,  941,  954,  948,   87,  527,  528,  529,
 /*    50 */   530,  531,  532,  533,  534,  535,  536,  537,  538,  539,
 /*    60 */   540,  328,  228,  972,  240,   49,   50,  147,   53,   54,
 /*    70 */   147,  212,  223,   43,  951,   52,  271,   57,   55,   59,
 /*    80 */    56,  942,  966,   72,  293,   48,   47,  954,  229,   46,
 /*    90 */    45,   44,   49,   50,  256,   53,   54,  251,  313,  223,
 /*   100 */    43,  190,   52,  271,   57,   55,   59,   56,   71,   32,
 /*   110 */   217, 1056,   48,   47,   76,  235,   46,   45,   44,   49,
 /*   120 */    51,   38,   53,   54,  162,  230,  223,   43,  188,   52,
 /*   130 */   271,   57,   55,   59,   56, 1007,  268,  266,   80,   48,
 /*   140 */    47,  939,  232,   46,   45,   44,   50,  966,   53,   54,
 /*   150 */   954,  226,  223,   43,  951,   52,  271,   57,   55,   59,
 /*   160 */    56,  849,  213,  744,   32,   48,   47,  175,  194,   46,
 /*   170 */    45,   44,   23,  291,  322,  321,  290,  289,  288,  320,
 /*   180 */   287,  319,  318,  317,  286,  316,  315,  914, 1052,  902,
 /*   190 */   903,  904,  905,  906,  907,  908,  909,  910,  911,  912,
 /*   200 */   913,  915,  916,   53,   54,   18,  227,  223,   43,  951,
 /*   210 */    52,  271,   57,   55,   59,   56,  233,  234,  940,  295,
 /*   220 */    48,   47,  147,  198,   46,   45,   44,  222,  757,  200,
 /*   230 */    32,  748,   82,  751, 1051,  754,  124,  123,  199,  922,
 /*   240 */   222,  757,  920,  921,  748,   73,  751,  923,  754,  925,
 /*   250 */   926,  924, 1050,  927,  928,   83,   32, 1066,  147,  219,
 /*   260 */   220,   48,   47,  270,  697,   46,   45,   44,  326,  325,
 /*   270 */   133,   76,  219,  220,   23,  950,  322,  321,   38,    1,
 /*   280 */   163,  320,   12,  319,  318,  317,   86,  316,  315,   32,
 /*   290 */  1006,  236,  725,  726,  300,  299,   32,  207,  296,  293,
 /*   300 */   250,  951,   70,   57,   55,   59,   56,  208,  206,   32,
 /*   310 */   575,   48,   47,    5,  165,   46,   45,   44,  576,   35,
 /*   320 */   164,   93,   98,   89,   97,  682,   81,  750,  679,  753,
 /*   330 */   680,  297,  681,  858,  951,  282,   58,  658,  301,  175,
 /*   340 */   850,  951,  758,  323,  243,  272,  175,  701,  756,   58,
 /*   350 */   192,  305,  247,  246,  951,  758,  237,  238,   46,   45,
 /*   360 */    44,  756,  235,  111,  755,  303,  302,  193,  954,  109,
 /*   370 */   114,  952,  313,    3,  176,  103,  113,  755,  119,  122,
 /*   380 */   112,  183,  179,  749,  746,  752,  116,  181,  178,  128,
 /*   390 */   127,  126,  125,  252,  694,  704,  221,   33,   66,  710,
 /*   400 */    24,  716,  254,  142,  717,   62,  778,  759,   20,   63,
 /*   410 */    19,   19,  668,  686,  274,  687,   33,   67,   33,   25,
 /*   420 */   747,  670,  276,  669,  953,   62,   85,   62,  195,   28,
 /*   430 */    64,  684,  277,  685,  102,  101,   69,  248,  657,  683,
 /*   440 */    14,   13,  108,  107, 1017,    6,  761,   16,   15,  121,
 /*   450 */   120,  138,  136,  189,  196,  197,  203,  204,  202,  187,
 /*   460 */   139, 1016,  201,  191,  224, 1013, 1012,  225,  304,  974,
 /*   470 */    41,  982,  984,  999,  998,  141,  967,  255,  137,  257,
 /*   480 */   949,  145,  158,  157,  947,  214,  283,  159,  918,  156,
 /*   490 */   259,  264,  152,  160,  709,  863,  964,  279,  148,  149,
 /*   500 */   280,  150,  281,  284,  269,  151,  263,   60,  285,   39,
 /*   510 */   185,   36,  294,   68,   65,  857,  267,  265,  153, 1071,
 /*   520 */    99,  261, 1070, 1068,  166,  258,  298,  154, 1065,  105,
 /*   530 */    42, 1064, 1062,  167,  883,   37,   34,   40,  186,  846,
 /*   540 */   115,  844,  117,  118,  842,  841,  239,  177,  839,  838,
 /*   550 */   837,  836,  835,  834,  833,  180,  182,  830,  828,  826,
 /*   560 */   824,  822,  184,  314,  253,   74,   77,  110,  306,  260,
 /*   570 */  1000,  307,  308,  309,  310,  311,  312,  324,  209,  798,
 /*   580 */   231,  241,  278,  242,  797,  244,  245,  796,  784,  783,
 /*   590 */   210,   94,  862,   95,  861,  205,  249,  254,  689,  273,
 /*   600 */     8,  216,  711,   75,   78,  840,  714,  129,  169,  884,
 /*   610 */   172,  168,  170,  174,  171,  173,  130,  832,    2,  131,
 /*   620 */   831,  155,  132,  823,  143,  144,    4,   79,  262,    9,
 /*   630 */    26,   27,  718,  146,   10,  760,    7,   11,  930,   21,
 /*   640 */    88,  762,   22,  275,   30,   86,   90,   91,  589,   31,
 /*   650 */    92,  621,  617,  615,  614,  613,  610,  579,   96,  292,
 /*   660 */    33,   61,  660,  659,  656,  605,  100,  603,  595,  601,
 /*   670 */   597,  599,  104,  593,  591,  624,  623,  106,  622,  620,
 /*   680 */   619,  618,  616,  612,  611,   62,  577,  544,  134,  542,
 /*   690 */   802,  801,  801,  135,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   195,    1,  257,    1,  194,  195,  195,  257,  201,    9,
 /*    10 */     1,    9,  267,   13,   14,  204,   16,   17,    9,  216,
 /*    20 */    20,   21,  257,   23,   24,   25,   26,   27,   28,  195,
 /*    30 */   195,  266,  267,   33,   34,  192,  193,   37,   38,   39,
 /*    40 */   233,  234,  235,  236,  241,  195,  201,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  216,  258,   62,   13,   14,  195,   16,   17,
 /*    70 */   195,  237,   20,   21,  240,   23,   24,   25,   26,   27,
 /*    80 */    28,  236,  239,   83,   81,   33,   34,  241,  238,   37,
 /*    90 */    38,   39,   13,   14,  259,   16,   17,  254,   87,   20,
 /*   100 */    21,  257,   23,   24,   25,   26,   27,   28,  201,  195,
 /*   110 */   266,  267,   33,   34,  110,  195,   37,   38,   39,   13,
 /*   120 */    14,  117,   16,   17,  204,  216,   20,   21,  257,   23,
 /*   130 */    24,   25,   26,   27,   28,  263,  261,  265,  263,   33,
 /*   140 */    34,  234,   68,   37,   38,   39,   14,  239,   16,   17,
 /*   150 */   241,  237,   20,   21,  240,   23,   24,   25,   26,   27,
 /*   160 */    28,  200,  254,  111,  195,   33,   34,  206,  257,   37,
 /*   170 */    38,   39,   94,   95,   96,   97,   98,   99,  100,  101,
 /*   180 */   102,  103,  104,  105,  106,  107,  108,  215,  257,  217,
 /*   190 */   218,  219,  220,  221,  222,  223,  224,  225,  226,  227,
 /*   200 */   228,  229,  230,   16,   17,   44,  237,   20,   21,  240,
 /*   210 */    23,   24,   25,   26,   27,   28,  142,   68,    0,  145,
 /*   220 */    33,   34,  195,   62,   37,   38,   39,    1,    2,   68,
 /*   230 */   195,    5,  242,    7,  257,    9,   75,   76,   77,  215,
 /*   240 */     1,    2,  218,  219,    5,  255,    7,  223,    9,  225,
 /*   250 */   226,  227,  257,  229,  230,   83,  195,  241,  195,   33,
 /*   260 */    34,   33,   34,   37,   37,   37,   38,   39,   65,   66,
 /*   270 */    67,  110,   33,   34,   94,  240,   96,   97,  117,  202,
 /*   280 */   203,  101,  110,  103,  104,  105,  114,  107,  108,  195,
 /*   290 */   263,  142,  129,  130,  145,  146,  195,  257,  237,   81,
 /*   300 */   139,  240,  141,   25,   26,   27,   28,  257,  147,  195,
 /*   310 */     1,   33,   34,   63,   64,   37,   38,   39,    9,   69,
 /*   320 */    70,   71,   72,   73,   74,    2,  263,    5,    5,    7,
 /*   330 */     7,  237,    9,  200,  240,   85,  110,    5,  237,  206,
 /*   340 */   200,  240,  116,  216,  140,   15,  206,  120,  122,  110,
 /*   350 */   257,  237,  148,  149,  240,  116,   33,   34,   37,   38,
 /*   360 */    39,  122,  195,   78,  138,   33,   34,  257,  241,   63,
 /*   370 */    64,  204,   87,  198,  199,   69,   70,  138,   72,   73,
 /*   380 */    74,   63,   64,    5,    1,    7,   80,   69,   70,   71,
 /*   390 */    72,   73,   74,  111,  115,  111,   61,  115,  115,  111,
 /*   400 */   121,  111,  118,  115,  111,  115,  111,  111,  115,  115,
 /*   410 */   115,  115,  111,    5,  111,    7,  115,  134,  115,  110,
 /*   420 */    37,  111,  111,  111,  241,  115,  115,  115,  257,  110,
 /*   430 */   136,    5,  113,    7,  143,  144,  110,  195,  112,  116,
 /*   440 */   143,  144,  143,  144,  232,  110,  116,  143,  144,   78,
 /*   450 */    79,   63,   64,  257,  257,  257,  257,  257,  257,  257,
 /*   460 */   195,  232,  257,  257,  232,  232,  232,  232,  232,  195,
 /*   470 */   256,  195,  195,  264,  264,  195,  239,  239,   61,  260,
 /*   480 */   239,  195,  195,  243,  195,  260,   86,  195,  231,  244,
 /*   490 */   260,  260,  248,  195,  122,  195,  253,  195,  252,  251,
 /*   500 */   195,  250,  195,  195,  127,  249,  125,  132,  195,  195,
 /*   510 */   195,  195,  195,  133,  135,  195,  131,  126,  247,  195,
 /*   520 */   195,  124,  195,  195,  195,  123,  195,  246,  195,  195,
 /*   530 */   137,  195,  195,  195,  195,  195,  195,  195,  195,  195,
 /*   540 */   195,  195,  195,  195,  195,  195,  195,  195,  195,  195,
 /*   550 */   195,  195,  195,  195,  195,  195,  195,  195,  195,  195,
 /*   560 */   195,  195,  195,  109,  196,  196,  196,   93,   92,  196,
 /*   570 */   196,   51,   89,   91,   55,   90,   88,   81,  196,    5,
 /*   580 */   196,  150,  196,    5,    5,  150,    5,    5,   96,   95,
 /*   590 */   196,  201,  205,  201,  205,  196,  140,  118,  111,  113,
 /*   600 */   110,    1,  111,  119,  115,  196,  111,  197,  212,  214,
 /*   610 */   209,  213,  208,  207,  211,  210,  197,  196,  202,  197,
 /*   620 */   196,  245,  197,  196,  110,  115,  198,  110,  110,  128,
 /*   630 */   115,  115,  111,  110,  128,  111,  110,  110,  231,  110,
 /*   640 */    78,  116,  110,  113,   84,  114,   83,   71,    5,   84,
 /*   650 */    83,    9,    5,    5,    5,    5,    5,   82,   78,   15,
 /*   660 */   115,   16,    5,    5,  111,    5,  144,    5,    5,    5,
 /*   670 */     5,    5,  144,    5,    5,    5,    5,  144,    5,    5,
 /*   680 */     5,    5,    5,    5,    5,  115,   82,   61,   21,   60,
 /*   690 */     0,  268,  268,   21,  268,  268,  268,  268,  268,  268,
 /*   700 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   710 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
 /*   720 */   268,  268,  268,  268,  268,  268,  268,  268,  268,  268,
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
 /*   880 */   268,  268,  268,  268,  268,  268,
};
#define YY_SHIFT_COUNT    (329)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (690)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   161,   78,   78,  180,  180,    3,  226,  239,    9,    9,
 /*    10 */     9,    9,    9,    9,    9,    9,    9,    0,    2,  239,
 /*    20 */   323,  323,  323,  323,  309,    4,    9,    9,    9,  218,
 /*    30 */     9,    9,    9,    9,  285,    3,   11,   11,  694,  694,
 /*    40 */   694,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  323,  323,  332,  332,  332,  332,  332,  332,  332,
 /*    70 */     9,    9,    9,  227,    9,    4,    4,    9,    9,    9,
 /*    80 */   163,  163,  279,    4,    9,    9,    9,    9,    9,    9,
 /*    90 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   100 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   110 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   120 */     9,    9,    9,    9,    9,    9,    9,    9,    9,    9,
 /*   130 */     9,    9,    9,    9,    9,    9,    9,    9,    9,  417,
 /*   140 */   417,  417,  372,  372,  372,  417,  372,  417,  380,  379,
 /*   150 */   375,  377,  385,  391,  381,  397,  402,  393,  417,  417,
 /*   160 */   417,  400,  400,  454,    3,    3,  417,  417,  474,  476,
 /*   170 */   520,  483,  482,  519,  485,  488,  454,  417,  496,  496,
 /*   180 */   417,  496,  417,  496,  417,  694,  694,   52,   79,  106,
 /*   190 */    79,   79,  132,  187,  278,  278,  278,  278,  250,  306,
 /*   200 */   318,  228,  228,  228,  228,  149,  204,  321,  321,  172,
 /*   210 */    74,  203,  282,  284,  288,  290,  293,  295,  296,  322,
 /*   220 */   378,  383,  335,  330,  294,  283,  301,  303,  310,  311,
 /*   230 */   312,  319,  291,  297,  299,  326,  304,  408,  426,  371,
 /*   240 */   388,  574,  431,  578,  579,  435,  581,  582,  492,  494,
 /*   250 */   456,  479,  486,  490,  484,  487,  489,  491,  514,  495,
 /*   260 */   510,  517,  600,  518,  521,  523,  515,  501,  516,  506,
 /*   270 */   524,  526,  525,  527,  486,  529,  530,  532,  531,  562,
 /*   280 */   560,  563,  576,  643,  565,  567,  642,  647,  648,  649,
 /*   290 */   650,  651,  575,  644,  580,  522,  545,  545,  645,  528,
 /*   300 */   533,  545,  657,  658,  553,  545,  660,  662,  663,  664,
 /*   310 */   665,  666,  668,  669,  670,  671,  673,  674,  675,  676,
 /*   320 */   677,  678,  679,  570,  604,  667,  672,  626,  629,  690,
};
#define YY_REDUCE_COUNT (186)
#define YY_REDUCE_MIN   (-255)
#define YY_REDUCE_MAX   (428)
static const short yy_reduce_ofst[] = {
 /*     0 */  -157,  -28,  -28,   24,   24, -193, -235, -156, -166, -128,
 /*    10 */  -125,  -86,  -31,   61,   94,  101,  114, -195, -190, -255,
 /*    20 */  -197, -154,  -91,  127, -165,  -92,   27,   63, -150, -155,
 /*    30 */  -189,  -80,  167,   35,  -39,  -93,  133,  140,  -10,   77,
 /*    40 */   175, -250, -129,  -89,  -69,  -23,   -5,   40,   50,   93,
 /*    50 */   110,  171,  196,  197,  198,  199,  200,  201,  202,  205,
 /*    60 */   206,   16,  183,  212,  229,  232,  233,  234,  235,  236,
 /*    70 */   242,  265,  274,  214,  276,  237,  238,  277,  280,  286,
 /*    80 */   209,  210,  240,  241,  287,  289,  292,  298,  300,  302,
 /*    90 */   305,  307,  308,  313,  314,  315,  316,  317,  320,  324,
 /*   100 */   325,  327,  328,  329,  331,  333,  334,  336,  337,  338,
 /*   110 */   339,  340,  341,  342,  343,  344,  345,  346,  347,  348,
 /*   120 */   349,  350,  351,  352,  353,  354,  355,  356,  357,  358,
 /*   130 */   359,  360,  361,  362,  363,  364,  365,  366,  367,  368,
 /*   140 */   369,  370,  219,  225,  230,  373,  231,  374,  243,  246,
 /*   150 */   248,  251,  256,  244,  271,  281,  376,  245,  382,  384,
 /*   160 */   386,  387,  389,  257,  390,  392,  394,  399,  395,  398,
 /*   170 */   396,  404,  403,  401,  405,  406,  407,  409,  410,  419,
 /*   180 */   421,  422,  424,  425,  427,  416,  428,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   799,  917,  859,  929,  847,  856, 1058, 1058,  799,  799,
 /*    10 */   799,  799,  799,  799,  799,  799,  799,  976,  819, 1058,
 /*    20 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  856,
 /*    30 */   799,  799,  799,  799,  866,  856,  866,  866,  971,  901,
 /*    40 */   919,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*    50 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*    60 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*    70 */   799,  799,  799,  978,  981,  799,  799,  983,  799,  799,
 /*    80 */  1003, 1003,  969,  799,  799,  799,  799,  799,  799,  799,
 /*    90 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   100 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   110 */   799,  799,  799,  799,  799,  845,  799,  843,  799,  799,
 /*   120 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   130 */   799,  799,  799,  829,  799,  799,  799,  799,  799,  821,
 /*   140 */   821,  821,  799,  799,  799,  821,  799,  821, 1010, 1014,
 /*   150 */  1008,  996, 1004,  995,  991,  989,  988, 1018,  821,  821,
 /*   160 */   821,  864,  864,  860,  856,  856,  821,  821,  882,  880,
 /*   170 */   878,  870,  876,  872,  874,  868,  848,  821,  854,  854,
 /*   180 */   821,  854,  821,  854,  821,  901,  919,  799, 1019,  799,
 /*   190 */  1057, 1009, 1047, 1046, 1053, 1045, 1044, 1043,  799,  799,
 /*   200 */   799, 1039, 1040, 1042, 1041,  799,  799, 1049, 1048,  799,
 /*   210 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   220 */   799,  799, 1021,  799, 1015, 1011,  799,  799,  799,  799,
 /*   230 */   799,  799,  799,  799,  799,  931,  799,  799,  799,  799,
 /*   240 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   250 */   799,  968,  799,  799,  799,  799,  979,  799,  799,  799,
 /*   260 */   799,  799,  799,  799,  799,  799, 1005,  799,  997,  799,
 /*   270 */   799,  799,  799,  799,  943,  799,  799,  799,  799,  799,
 /*   280 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   290 */   799,  799,  799,  799,  799,  799, 1069, 1067,  799,  799,
 /*   300 */   799, 1063,  799,  799,  799, 1061,  799,  799,  799,  799,
 /*   310 */   799,  799,  799,  799,  799,  799,  799,  799,  799,  799,
 /*   320 */   799,  799,  799,  885,  799,  827,  825,  799,  817,  799,
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
  /*  192 */ "program",
  /*  193 */ "cmd",
  /*  194 */ "dbPrefix",
  /*  195 */ "ids",
  /*  196 */ "cpxName",
  /*  197 */ "ifexists",
  /*  198 */ "alter_db_optr",
  /*  199 */ "alter_topic_optr",
  /*  200 */ "acct_optr",
  /*  201 */ "ifnotexists",
  /*  202 */ "db_optr",
  /*  203 */ "topic_optr",
  /*  204 */ "typename",
  /*  205 */ "bufsize",
  /*  206 */ "pps",
  /*  207 */ "tseries",
  /*  208 */ "dbs",
  /*  209 */ "streams",
  /*  210 */ "storage",
  /*  211 */ "qtime",
  /*  212 */ "users",
  /*  213 */ "conns",
  /*  214 */ "state",
  /*  215 */ "keep",
  /*  216 */ "tagitemlist",
  /*  217 */ "cache",
  /*  218 */ "replica",
  /*  219 */ "quorum",
  /*  220 */ "days",
  /*  221 */ "minrows",
  /*  222 */ "maxrows",
  /*  223 */ "blocks",
  /*  224 */ "ctime",
  /*  225 */ "wal",
  /*  226 */ "fsync",
  /*  227 */ "comp",
  /*  228 */ "prec",
  /*  229 */ "update",
  /*  230 */ "cachelast",
  /*  231 */ "partitions",
  /*  232 */ "signed",
  /*  233 */ "create_table_args",
  /*  234 */ "create_stable_args",
  /*  235 */ "create_table_list",
  /*  236 */ "create_from_stable",
  /*  237 */ "columnlist",
  /*  238 */ "tagNamelist",
  /*  239 */ "select",
  /*  240 */ "column",
  /*  241 */ "tagitem",
  /*  242 */ "selcollist",
  /*  243 */ "from",
  /*  244 */ "where_opt",
  /*  245 */ "interval_opt",
  /*  246 */ "session_option",
  /*  247 */ "fill_opt",
  /*  248 */ "sliding_opt",
  /*  249 */ "groupby_opt",
  /*  250 */ "orderby_opt",
  /*  251 */ "having_opt",
  /*  252 */ "slimit_opt",
  /*  253 */ "limit_opt",
  /*  254 */ "union",
  /*  255 */ "sclp",
  /*  256 */ "distinct",
  /*  257 */ "expr",
  /*  258 */ "as",
  /*  259 */ "tablelist",
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
 /*  59 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  60 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  61 */ "cmd ::= CREATE USER ids PASS ids",
 /*  62 */ "bufsize ::=",
 /*  63 */ "bufsize ::= BUFSIZE INTEGER",
 /*  64 */ "pps ::=",
 /*  65 */ "pps ::= PPS INTEGER",
 /*  66 */ "tseries ::=",
 /*  67 */ "tseries ::= TSERIES INTEGER",
 /*  68 */ "dbs ::=",
 /*  69 */ "dbs ::= DBS INTEGER",
 /*  70 */ "streams ::=",
 /*  71 */ "streams ::= STREAMS INTEGER",
 /*  72 */ "storage ::=",
 /*  73 */ "storage ::= STORAGE INTEGER",
 /*  74 */ "qtime ::=",
 /*  75 */ "qtime ::= QTIME INTEGER",
 /*  76 */ "users ::=",
 /*  77 */ "users ::= USERS INTEGER",
 /*  78 */ "conns ::=",
 /*  79 */ "conns ::= CONNS INTEGER",
 /*  80 */ "state ::=",
 /*  81 */ "state ::= STATE ids",
 /*  82 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  83 */ "keep ::= KEEP tagitemlist",
 /*  84 */ "cache ::= CACHE INTEGER",
 /*  85 */ "replica ::= REPLICA INTEGER",
 /*  86 */ "quorum ::= QUORUM INTEGER",
 /*  87 */ "days ::= DAYS INTEGER",
 /*  88 */ "minrows ::= MINROWS INTEGER",
 /*  89 */ "maxrows ::= MAXROWS INTEGER",
 /*  90 */ "blocks ::= BLOCKS INTEGER",
 /*  91 */ "ctime ::= CTIME INTEGER",
 /*  92 */ "wal ::= WAL INTEGER",
 /*  93 */ "fsync ::= FSYNC INTEGER",
 /*  94 */ "comp ::= COMP INTEGER",
 /*  95 */ "prec ::= PRECISION STRING",
 /*  96 */ "update ::= UPDATE INTEGER",
 /*  97 */ "cachelast ::= CACHELAST INTEGER",
 /*  98 */ "partitions ::= PARTITIONS INTEGER",
 /*  99 */ "db_optr ::=",
 /* 100 */ "db_optr ::= db_optr cache",
 /* 101 */ "db_optr ::= db_optr replica",
 /* 102 */ "db_optr ::= db_optr quorum",
 /* 103 */ "db_optr ::= db_optr days",
 /* 104 */ "db_optr ::= db_optr minrows",
 /* 105 */ "db_optr ::= db_optr maxrows",
 /* 106 */ "db_optr ::= db_optr blocks",
 /* 107 */ "db_optr ::= db_optr ctime",
 /* 108 */ "db_optr ::= db_optr wal",
 /* 109 */ "db_optr ::= db_optr fsync",
 /* 110 */ "db_optr ::= db_optr comp",
 /* 111 */ "db_optr ::= db_optr prec",
 /* 112 */ "db_optr ::= db_optr keep",
 /* 113 */ "db_optr ::= db_optr update",
 /* 114 */ "db_optr ::= db_optr cachelast",
 /* 115 */ "topic_optr ::= db_optr",
 /* 116 */ "topic_optr ::= topic_optr partitions",
 /* 117 */ "alter_db_optr ::=",
 /* 118 */ "alter_db_optr ::= alter_db_optr replica",
 /* 119 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 120 */ "alter_db_optr ::= alter_db_optr keep",
 /* 121 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 122 */ "alter_db_optr ::= alter_db_optr comp",
 /* 123 */ "alter_db_optr ::= alter_db_optr wal",
 /* 124 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 125 */ "alter_db_optr ::= alter_db_optr update",
 /* 126 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 127 */ "alter_topic_optr ::= alter_db_optr",
 /* 128 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 129 */ "typename ::= ids",
 /* 130 */ "typename ::= ids LP signed RP",
 /* 131 */ "typename ::= ids UNSIGNED",
 /* 132 */ "signed ::= INTEGER",
 /* 133 */ "signed ::= PLUS INTEGER",
 /* 134 */ "signed ::= MINUS INTEGER",
 /* 135 */ "cmd ::= CREATE TABLE create_table_args",
 /* 136 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 137 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 138 */ "cmd ::= CREATE TABLE create_table_list",
 /* 139 */ "create_table_list ::= create_from_stable",
 /* 140 */ "create_table_list ::= create_table_list create_from_stable",
 /* 141 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 142 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 143 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 144 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 145 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 146 */ "tagNamelist ::= ids",
 /* 147 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 148 */ "columnlist ::= columnlist COMMA column",
 /* 149 */ "columnlist ::= column",
 /* 150 */ "column ::= ids typename",
 /* 151 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 152 */ "tagitemlist ::= tagitem",
 /* 153 */ "tagitem ::= INTEGER",
 /* 154 */ "tagitem ::= FLOAT",
 /* 155 */ "tagitem ::= STRING",
 /* 156 */ "tagitem ::= BOOL",
 /* 157 */ "tagitem ::= NULL",
 /* 158 */ "tagitem ::= MINUS INTEGER",
 /* 159 */ "tagitem ::= MINUS FLOAT",
 /* 160 */ "tagitem ::= PLUS INTEGER",
 /* 161 */ "tagitem ::= PLUS FLOAT",
 /* 162 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 163 */ "select ::= LP select RP",
 /* 164 */ "union ::= select",
 /* 165 */ "union ::= union UNION ALL select",
 /* 166 */ "cmd ::= union",
 /* 167 */ "select ::= SELECT selcollist",
 /* 168 */ "sclp ::= selcollist COMMA",
 /* 169 */ "sclp ::=",
 /* 170 */ "selcollist ::= sclp distinct expr as",
 /* 171 */ "selcollist ::= sclp STAR",
 /* 172 */ "as ::= AS ids",
 /* 173 */ "as ::= ids",
 /* 174 */ "as ::=",
 /* 175 */ "distinct ::= DISTINCT",
 /* 176 */ "distinct ::=",
 /* 177 */ "from ::= FROM tablelist",
 /* 178 */ "from ::= FROM LP union RP",
 /* 179 */ "tablelist ::= ids cpxName",
 /* 180 */ "tablelist ::= ids cpxName ids",
 /* 181 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 182 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 183 */ "tmvar ::= VARIABLE",
 /* 184 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 185 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 186 */ "interval_opt ::=",
 /* 187 */ "session_option ::=",
 /* 188 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 189 */ "fill_opt ::=",
 /* 190 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 191 */ "fill_opt ::= FILL LP ID RP",
 /* 192 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 193 */ "sliding_opt ::=",
 /* 194 */ "orderby_opt ::=",
 /* 195 */ "orderby_opt ::= ORDER BY sortlist",
 /* 196 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 197 */ "sortlist ::= item sortorder",
 /* 198 */ "item ::= ids cpxName",
 /* 199 */ "sortorder ::= ASC",
 /* 200 */ "sortorder ::= DESC",
 /* 201 */ "sortorder ::=",
 /* 202 */ "groupby_opt ::=",
 /* 203 */ "groupby_opt ::= GROUP BY grouplist",
 /* 204 */ "grouplist ::= grouplist COMMA item",
 /* 205 */ "grouplist ::= item",
 /* 206 */ "having_opt ::=",
 /* 207 */ "having_opt ::= HAVING expr",
 /* 208 */ "limit_opt ::=",
 /* 209 */ "limit_opt ::= LIMIT signed",
 /* 210 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 211 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 212 */ "slimit_opt ::=",
 /* 213 */ "slimit_opt ::= SLIMIT signed",
 /* 214 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 215 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 216 */ "where_opt ::=",
 /* 217 */ "where_opt ::= WHERE expr",
 /* 218 */ "expr ::= LP expr RP",
 /* 219 */ "expr ::= ID",
 /* 220 */ "expr ::= ID DOT ID",
 /* 221 */ "expr ::= ID DOT STAR",
 /* 222 */ "expr ::= INTEGER",
 /* 223 */ "expr ::= MINUS INTEGER",
 /* 224 */ "expr ::= PLUS INTEGER",
 /* 225 */ "expr ::= FLOAT",
 /* 226 */ "expr ::= MINUS FLOAT",
 /* 227 */ "expr ::= PLUS FLOAT",
 /* 228 */ "expr ::= STRING",
 /* 229 */ "expr ::= NOW",
 /* 230 */ "expr ::= VARIABLE",
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
 /* 261 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 262 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 263 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 264 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 265 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 266 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 267 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 268 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 269 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 270 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 271 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 272 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 215: /* keep */
    case 216: /* tagitemlist */
    case 237: /* columnlist */
    case 238: /* tagNamelist */
    case 247: /* fill_opt */
    case 249: /* groupby_opt */
    case 250: /* orderby_opt */
    case 261: /* sortlist */
    case 265: /* grouplist */
{
taosArrayDestroy((yypminor->yy325));
}
      break;
    case 235: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy422));
}
      break;
    case 239: /* select */
{
destroyQuerySqlNode((yypminor->yy494));
}
      break;
    case 242: /* selcollist */
    case 255: /* sclp */
    case 266: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy325));
}
      break;
    case 244: /* where_opt */
    case 251: /* having_opt */
    case 257: /* expr */
    case 267: /* expritem */
{
tSqlExprDestroy((yypminor->yy162));
}
      break;
    case 254: /* union */
{
destroyAllSelectClause((yypminor->yy145));
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
   192,  /* (0) program ::= cmd */
   193,  /* (1) cmd ::= SHOW DATABASES */
   193,  /* (2) cmd ::= SHOW TOPICS */
   193,  /* (3) cmd ::= SHOW FUNCTIONS */
   193,  /* (4) cmd ::= SHOW MNODES */
   193,  /* (5) cmd ::= SHOW DNODES */
   193,  /* (6) cmd ::= SHOW ACCOUNTS */
   193,  /* (7) cmd ::= SHOW USERS */
   193,  /* (8) cmd ::= SHOW MODULES */
   193,  /* (9) cmd ::= SHOW QUERIES */
   193,  /* (10) cmd ::= SHOW CONNECTIONS */
   193,  /* (11) cmd ::= SHOW STREAMS */
   193,  /* (12) cmd ::= SHOW VARIABLES */
   193,  /* (13) cmd ::= SHOW SCORES */
   193,  /* (14) cmd ::= SHOW GRANTS */
   193,  /* (15) cmd ::= SHOW VNODES */
   193,  /* (16) cmd ::= SHOW VNODES IPTOKEN */
   194,  /* (17) dbPrefix ::= */
   194,  /* (18) dbPrefix ::= ids DOT */
   196,  /* (19) cpxName ::= */
   196,  /* (20) cpxName ::= DOT ids */
   193,  /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
   193,  /* (22) cmd ::= SHOW CREATE DATABASE ids */
   193,  /* (23) cmd ::= SHOW dbPrefix TABLES */
   193,  /* (24) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   193,  /* (25) cmd ::= SHOW dbPrefix STABLES */
   193,  /* (26) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   193,  /* (27) cmd ::= SHOW dbPrefix VGROUPS */
   193,  /* (28) cmd ::= SHOW dbPrefix VGROUPS ids */
   193,  /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
   193,  /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
   193,  /* (31) cmd ::= DROP DATABASE ifexists ids */
   193,  /* (32) cmd ::= DROP TOPIC ifexists ids */
   193,  /* (33) cmd ::= DROP FUNCTION ids */
   193,  /* (34) cmd ::= DROP DNODE ids */
   193,  /* (35) cmd ::= DROP USER ids */
   193,  /* (36) cmd ::= DROP ACCOUNT ids */
   193,  /* (37) cmd ::= USE ids */
   193,  /* (38) cmd ::= DESCRIBE ids cpxName */
   193,  /* (39) cmd ::= ALTER USER ids PASS ids */
   193,  /* (40) cmd ::= ALTER USER ids PRIVILEGE ids */
   193,  /* (41) cmd ::= ALTER DNODE ids ids */
   193,  /* (42) cmd ::= ALTER DNODE ids ids ids */
   193,  /* (43) cmd ::= ALTER LOCAL ids */
   193,  /* (44) cmd ::= ALTER LOCAL ids ids */
   193,  /* (45) cmd ::= ALTER DATABASE ids alter_db_optr */
   193,  /* (46) cmd ::= ALTER TOPIC ids alter_topic_optr */
   193,  /* (47) cmd ::= ALTER ACCOUNT ids acct_optr */
   193,  /* (48) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   195,  /* (49) ids ::= ID */
   195,  /* (50) ids ::= STRING */
   197,  /* (51) ifexists ::= IF EXISTS */
   197,  /* (52) ifexists ::= */
   201,  /* (53) ifnotexists ::= IF NOT EXISTS */
   201,  /* (54) ifnotexists ::= */
   193,  /* (55) cmd ::= CREATE DNODE ids */
   193,  /* (56) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   193,  /* (57) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   193,  /* (58) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   193,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   193,  /* (60) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   193,  /* (61) cmd ::= CREATE USER ids PASS ids */
   205,  /* (62) bufsize ::= */
   205,  /* (63) bufsize ::= BUFSIZE INTEGER */
   206,  /* (64) pps ::= */
   206,  /* (65) pps ::= PPS INTEGER */
   207,  /* (66) tseries ::= */
   207,  /* (67) tseries ::= TSERIES INTEGER */
   208,  /* (68) dbs ::= */
   208,  /* (69) dbs ::= DBS INTEGER */
   209,  /* (70) streams ::= */
   209,  /* (71) streams ::= STREAMS INTEGER */
   210,  /* (72) storage ::= */
   210,  /* (73) storage ::= STORAGE INTEGER */
   211,  /* (74) qtime ::= */
   211,  /* (75) qtime ::= QTIME INTEGER */
   212,  /* (76) users ::= */
   212,  /* (77) users ::= USERS INTEGER */
   213,  /* (78) conns ::= */
   213,  /* (79) conns ::= CONNS INTEGER */
   214,  /* (80) state ::= */
   214,  /* (81) state ::= STATE ids */
   200,  /* (82) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   215,  /* (83) keep ::= KEEP tagitemlist */
   217,  /* (84) cache ::= CACHE INTEGER */
   218,  /* (85) replica ::= REPLICA INTEGER */
   219,  /* (86) quorum ::= QUORUM INTEGER */
   220,  /* (87) days ::= DAYS INTEGER */
   221,  /* (88) minrows ::= MINROWS INTEGER */
   222,  /* (89) maxrows ::= MAXROWS INTEGER */
   223,  /* (90) blocks ::= BLOCKS INTEGER */
   224,  /* (91) ctime ::= CTIME INTEGER */
   225,  /* (92) wal ::= WAL INTEGER */
   226,  /* (93) fsync ::= FSYNC INTEGER */
   227,  /* (94) comp ::= COMP INTEGER */
   228,  /* (95) prec ::= PRECISION STRING */
   229,  /* (96) update ::= UPDATE INTEGER */
   230,  /* (97) cachelast ::= CACHELAST INTEGER */
   231,  /* (98) partitions ::= PARTITIONS INTEGER */
   202,  /* (99) db_optr ::= */
   202,  /* (100) db_optr ::= db_optr cache */
   202,  /* (101) db_optr ::= db_optr replica */
   202,  /* (102) db_optr ::= db_optr quorum */
   202,  /* (103) db_optr ::= db_optr days */
   202,  /* (104) db_optr ::= db_optr minrows */
   202,  /* (105) db_optr ::= db_optr maxrows */
   202,  /* (106) db_optr ::= db_optr blocks */
   202,  /* (107) db_optr ::= db_optr ctime */
   202,  /* (108) db_optr ::= db_optr wal */
   202,  /* (109) db_optr ::= db_optr fsync */
   202,  /* (110) db_optr ::= db_optr comp */
   202,  /* (111) db_optr ::= db_optr prec */
   202,  /* (112) db_optr ::= db_optr keep */
   202,  /* (113) db_optr ::= db_optr update */
   202,  /* (114) db_optr ::= db_optr cachelast */
   203,  /* (115) topic_optr ::= db_optr */
   203,  /* (116) topic_optr ::= topic_optr partitions */
   198,  /* (117) alter_db_optr ::= */
   198,  /* (118) alter_db_optr ::= alter_db_optr replica */
   198,  /* (119) alter_db_optr ::= alter_db_optr quorum */
   198,  /* (120) alter_db_optr ::= alter_db_optr keep */
   198,  /* (121) alter_db_optr ::= alter_db_optr blocks */
   198,  /* (122) alter_db_optr ::= alter_db_optr comp */
   198,  /* (123) alter_db_optr ::= alter_db_optr wal */
   198,  /* (124) alter_db_optr ::= alter_db_optr fsync */
   198,  /* (125) alter_db_optr ::= alter_db_optr update */
   198,  /* (126) alter_db_optr ::= alter_db_optr cachelast */
   199,  /* (127) alter_topic_optr ::= alter_db_optr */
   199,  /* (128) alter_topic_optr ::= alter_topic_optr partitions */
   204,  /* (129) typename ::= ids */
   204,  /* (130) typename ::= ids LP signed RP */
   204,  /* (131) typename ::= ids UNSIGNED */
   232,  /* (132) signed ::= INTEGER */
   232,  /* (133) signed ::= PLUS INTEGER */
   232,  /* (134) signed ::= MINUS INTEGER */
   193,  /* (135) cmd ::= CREATE TABLE create_table_args */
   193,  /* (136) cmd ::= CREATE TABLE create_stable_args */
   193,  /* (137) cmd ::= CREATE STABLE create_stable_args */
   193,  /* (138) cmd ::= CREATE TABLE create_table_list */
   235,  /* (139) create_table_list ::= create_from_stable */
   235,  /* (140) create_table_list ::= create_table_list create_from_stable */
   233,  /* (141) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   234,  /* (142) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   236,  /* (143) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   236,  /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   238,  /* (145) tagNamelist ::= tagNamelist COMMA ids */
   238,  /* (146) tagNamelist ::= ids */
   233,  /* (147) create_table_args ::= ifnotexists ids cpxName AS select */
   237,  /* (148) columnlist ::= columnlist COMMA column */
   237,  /* (149) columnlist ::= column */
   240,  /* (150) column ::= ids typename */
   216,  /* (151) tagitemlist ::= tagitemlist COMMA tagitem */
   216,  /* (152) tagitemlist ::= tagitem */
   241,  /* (153) tagitem ::= INTEGER */
   241,  /* (154) tagitem ::= FLOAT */
   241,  /* (155) tagitem ::= STRING */
   241,  /* (156) tagitem ::= BOOL */
   241,  /* (157) tagitem ::= NULL */
   241,  /* (158) tagitem ::= MINUS INTEGER */
   241,  /* (159) tagitem ::= MINUS FLOAT */
   241,  /* (160) tagitem ::= PLUS INTEGER */
   241,  /* (161) tagitem ::= PLUS FLOAT */
   239,  /* (162) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   239,  /* (163) select ::= LP select RP */
   254,  /* (164) union ::= select */
   254,  /* (165) union ::= union UNION ALL select */
   193,  /* (166) cmd ::= union */
   239,  /* (167) select ::= SELECT selcollist */
   255,  /* (168) sclp ::= selcollist COMMA */
   255,  /* (169) sclp ::= */
   242,  /* (170) selcollist ::= sclp distinct expr as */
   242,  /* (171) selcollist ::= sclp STAR */
   258,  /* (172) as ::= AS ids */
   258,  /* (173) as ::= ids */
   258,  /* (174) as ::= */
   256,  /* (175) distinct ::= DISTINCT */
   256,  /* (176) distinct ::= */
   243,  /* (177) from ::= FROM tablelist */
   243,  /* (178) from ::= FROM LP union RP */
   259,  /* (179) tablelist ::= ids cpxName */
   259,  /* (180) tablelist ::= ids cpxName ids */
   259,  /* (181) tablelist ::= tablelist COMMA ids cpxName */
   259,  /* (182) tablelist ::= tablelist COMMA ids cpxName ids */
   260,  /* (183) tmvar ::= VARIABLE */
   245,  /* (184) interval_opt ::= INTERVAL LP tmvar RP */
   245,  /* (185) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   245,  /* (186) interval_opt ::= */
   246,  /* (187) session_option ::= */
   246,  /* (188) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   247,  /* (189) fill_opt ::= */
   247,  /* (190) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   247,  /* (191) fill_opt ::= FILL LP ID RP */
   248,  /* (192) sliding_opt ::= SLIDING LP tmvar RP */
   248,  /* (193) sliding_opt ::= */
   250,  /* (194) orderby_opt ::= */
   250,  /* (195) orderby_opt ::= ORDER BY sortlist */
   261,  /* (196) sortlist ::= sortlist COMMA item sortorder */
   261,  /* (197) sortlist ::= item sortorder */
   263,  /* (198) item ::= ids cpxName */
   264,  /* (199) sortorder ::= ASC */
   264,  /* (200) sortorder ::= DESC */
   264,  /* (201) sortorder ::= */
   249,  /* (202) groupby_opt ::= */
   249,  /* (203) groupby_opt ::= GROUP BY grouplist */
   265,  /* (204) grouplist ::= grouplist COMMA item */
   265,  /* (205) grouplist ::= item */
   251,  /* (206) having_opt ::= */
   251,  /* (207) having_opt ::= HAVING expr */
   253,  /* (208) limit_opt ::= */
   253,  /* (209) limit_opt ::= LIMIT signed */
   253,  /* (210) limit_opt ::= LIMIT signed OFFSET signed */
   253,  /* (211) limit_opt ::= LIMIT signed COMMA signed */
   252,  /* (212) slimit_opt ::= */
   252,  /* (213) slimit_opt ::= SLIMIT signed */
   252,  /* (214) slimit_opt ::= SLIMIT signed SOFFSET signed */
   252,  /* (215) slimit_opt ::= SLIMIT signed COMMA signed */
   244,  /* (216) where_opt ::= */
   244,  /* (217) where_opt ::= WHERE expr */
   257,  /* (218) expr ::= LP expr RP */
   257,  /* (219) expr ::= ID */
   257,  /* (220) expr ::= ID DOT ID */
   257,  /* (221) expr ::= ID DOT STAR */
   257,  /* (222) expr ::= INTEGER */
   257,  /* (223) expr ::= MINUS INTEGER */
   257,  /* (224) expr ::= PLUS INTEGER */
   257,  /* (225) expr ::= FLOAT */
   257,  /* (226) expr ::= MINUS FLOAT */
   257,  /* (227) expr ::= PLUS FLOAT */
   257,  /* (228) expr ::= STRING */
   257,  /* (229) expr ::= NOW */
   257,  /* (230) expr ::= VARIABLE */
   257,  /* (231) expr ::= BOOL */
   257,  /* (232) expr ::= NULL */
   257,  /* (233) expr ::= ID LP exprlist RP */
   257,  /* (234) expr ::= ID LP STAR RP */
   257,  /* (235) expr ::= expr IS NULL */
   257,  /* (236) expr ::= expr IS NOT NULL */
   257,  /* (237) expr ::= expr LT expr */
   257,  /* (238) expr ::= expr GT expr */
   257,  /* (239) expr ::= expr LE expr */
   257,  /* (240) expr ::= expr GE expr */
   257,  /* (241) expr ::= expr NE expr */
   257,  /* (242) expr ::= expr EQ expr */
   257,  /* (243) expr ::= expr BETWEEN expr AND expr */
   257,  /* (244) expr ::= expr AND expr */
   257,  /* (245) expr ::= expr OR expr */
   257,  /* (246) expr ::= expr PLUS expr */
   257,  /* (247) expr ::= expr MINUS expr */
   257,  /* (248) expr ::= expr STAR expr */
   257,  /* (249) expr ::= expr SLASH expr */
   257,  /* (250) expr ::= expr REM expr */
   257,  /* (251) expr ::= expr LIKE expr */
   257,  /* (252) expr ::= expr IN LP exprlist RP */
   266,  /* (253) exprlist ::= exprlist COMMA expritem */
   266,  /* (254) exprlist ::= expritem */
   267,  /* (255) expritem ::= expr */
   267,  /* (256) expritem ::= */
   193,  /* (257) cmd ::= RESET QUERY CACHE */
   193,  /* (258) cmd ::= SYNCDB ids REPLICA */
   193,  /* (259) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   193,  /* (260) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   193,  /* (261) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   193,  /* (262) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   193,  /* (263) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   193,  /* (264) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   193,  /* (265) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   193,  /* (266) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   193,  /* (267) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   193,  /* (268) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   193,  /* (269) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   193,  /* (270) cmd ::= KILL CONNECTION INTEGER */
   193,  /* (271) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   193,  /* (272) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -8,  /* (59) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -9,  /* (60) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
   -5,  /* (61) cmd ::= CREATE USER ids PASS ids */
    0,  /* (62) bufsize ::= */
   -2,  /* (63) bufsize ::= BUFSIZE INTEGER */
    0,  /* (64) pps ::= */
   -2,  /* (65) pps ::= PPS INTEGER */
    0,  /* (66) tseries ::= */
   -2,  /* (67) tseries ::= TSERIES INTEGER */
    0,  /* (68) dbs ::= */
   -2,  /* (69) dbs ::= DBS INTEGER */
    0,  /* (70) streams ::= */
   -2,  /* (71) streams ::= STREAMS INTEGER */
    0,  /* (72) storage ::= */
   -2,  /* (73) storage ::= STORAGE INTEGER */
    0,  /* (74) qtime ::= */
   -2,  /* (75) qtime ::= QTIME INTEGER */
    0,  /* (76) users ::= */
   -2,  /* (77) users ::= USERS INTEGER */
    0,  /* (78) conns ::= */
   -2,  /* (79) conns ::= CONNS INTEGER */
    0,  /* (80) state ::= */
   -2,  /* (81) state ::= STATE ids */
   -9,  /* (82) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (83) keep ::= KEEP tagitemlist */
   -2,  /* (84) cache ::= CACHE INTEGER */
   -2,  /* (85) replica ::= REPLICA INTEGER */
   -2,  /* (86) quorum ::= QUORUM INTEGER */
   -2,  /* (87) days ::= DAYS INTEGER */
   -2,  /* (88) minrows ::= MINROWS INTEGER */
   -2,  /* (89) maxrows ::= MAXROWS INTEGER */
   -2,  /* (90) blocks ::= BLOCKS INTEGER */
   -2,  /* (91) ctime ::= CTIME INTEGER */
   -2,  /* (92) wal ::= WAL INTEGER */
   -2,  /* (93) fsync ::= FSYNC INTEGER */
   -2,  /* (94) comp ::= COMP INTEGER */
   -2,  /* (95) prec ::= PRECISION STRING */
   -2,  /* (96) update ::= UPDATE INTEGER */
   -2,  /* (97) cachelast ::= CACHELAST INTEGER */
   -2,  /* (98) partitions ::= PARTITIONS INTEGER */
    0,  /* (99) db_optr ::= */
   -2,  /* (100) db_optr ::= db_optr cache */
   -2,  /* (101) db_optr ::= db_optr replica */
   -2,  /* (102) db_optr ::= db_optr quorum */
   -2,  /* (103) db_optr ::= db_optr days */
   -2,  /* (104) db_optr ::= db_optr minrows */
   -2,  /* (105) db_optr ::= db_optr maxrows */
   -2,  /* (106) db_optr ::= db_optr blocks */
   -2,  /* (107) db_optr ::= db_optr ctime */
   -2,  /* (108) db_optr ::= db_optr wal */
   -2,  /* (109) db_optr ::= db_optr fsync */
   -2,  /* (110) db_optr ::= db_optr comp */
   -2,  /* (111) db_optr ::= db_optr prec */
   -2,  /* (112) db_optr ::= db_optr keep */
   -2,  /* (113) db_optr ::= db_optr update */
   -2,  /* (114) db_optr ::= db_optr cachelast */
   -1,  /* (115) topic_optr ::= db_optr */
   -2,  /* (116) topic_optr ::= topic_optr partitions */
    0,  /* (117) alter_db_optr ::= */
   -2,  /* (118) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (119) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (120) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (121) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (122) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (123) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (124) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (125) alter_db_optr ::= alter_db_optr update */
   -2,  /* (126) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (127) alter_topic_optr ::= alter_db_optr */
   -2,  /* (128) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (129) typename ::= ids */
   -4,  /* (130) typename ::= ids LP signed RP */
   -2,  /* (131) typename ::= ids UNSIGNED */
   -1,  /* (132) signed ::= INTEGER */
   -2,  /* (133) signed ::= PLUS INTEGER */
   -2,  /* (134) signed ::= MINUS INTEGER */
   -3,  /* (135) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (136) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (137) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (138) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (139) create_table_list ::= create_from_stable */
   -2,  /* (140) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (141) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (142) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (143) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (144) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (145) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (146) tagNamelist ::= ids */
   -5,  /* (147) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (148) columnlist ::= columnlist COMMA column */
   -1,  /* (149) columnlist ::= column */
   -2,  /* (150) column ::= ids typename */
   -3,  /* (151) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (152) tagitemlist ::= tagitem */
   -1,  /* (153) tagitem ::= INTEGER */
   -1,  /* (154) tagitem ::= FLOAT */
   -1,  /* (155) tagitem ::= STRING */
   -1,  /* (156) tagitem ::= BOOL */
   -1,  /* (157) tagitem ::= NULL */
   -2,  /* (158) tagitem ::= MINUS INTEGER */
   -2,  /* (159) tagitem ::= MINUS FLOAT */
   -2,  /* (160) tagitem ::= PLUS INTEGER */
   -2,  /* (161) tagitem ::= PLUS FLOAT */
  -13,  /* (162) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (163) select ::= LP select RP */
   -1,  /* (164) union ::= select */
   -4,  /* (165) union ::= union UNION ALL select */
   -1,  /* (166) cmd ::= union */
   -2,  /* (167) select ::= SELECT selcollist */
   -2,  /* (168) sclp ::= selcollist COMMA */
    0,  /* (169) sclp ::= */
   -4,  /* (170) selcollist ::= sclp distinct expr as */
   -2,  /* (171) selcollist ::= sclp STAR */
   -2,  /* (172) as ::= AS ids */
   -1,  /* (173) as ::= ids */
    0,  /* (174) as ::= */
   -1,  /* (175) distinct ::= DISTINCT */
    0,  /* (176) distinct ::= */
   -2,  /* (177) from ::= FROM tablelist */
   -4,  /* (178) from ::= FROM LP union RP */
   -2,  /* (179) tablelist ::= ids cpxName */
   -3,  /* (180) tablelist ::= ids cpxName ids */
   -4,  /* (181) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (182) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (183) tmvar ::= VARIABLE */
   -4,  /* (184) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (185) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (186) interval_opt ::= */
    0,  /* (187) session_option ::= */
   -7,  /* (188) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (189) fill_opt ::= */
   -6,  /* (190) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (191) fill_opt ::= FILL LP ID RP */
   -4,  /* (192) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (193) sliding_opt ::= */
    0,  /* (194) orderby_opt ::= */
   -3,  /* (195) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (196) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (197) sortlist ::= item sortorder */
   -2,  /* (198) item ::= ids cpxName */
   -1,  /* (199) sortorder ::= ASC */
   -1,  /* (200) sortorder ::= DESC */
    0,  /* (201) sortorder ::= */
    0,  /* (202) groupby_opt ::= */
   -3,  /* (203) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (204) grouplist ::= grouplist COMMA item */
   -1,  /* (205) grouplist ::= item */
    0,  /* (206) having_opt ::= */
   -2,  /* (207) having_opt ::= HAVING expr */
    0,  /* (208) limit_opt ::= */
   -2,  /* (209) limit_opt ::= LIMIT signed */
   -4,  /* (210) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (211) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (212) slimit_opt ::= */
   -2,  /* (213) slimit_opt ::= SLIMIT signed */
   -4,  /* (214) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (215) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (216) where_opt ::= */
   -2,  /* (217) where_opt ::= WHERE expr */
   -3,  /* (218) expr ::= LP expr RP */
   -1,  /* (219) expr ::= ID */
   -3,  /* (220) expr ::= ID DOT ID */
   -3,  /* (221) expr ::= ID DOT STAR */
   -1,  /* (222) expr ::= INTEGER */
   -2,  /* (223) expr ::= MINUS INTEGER */
   -2,  /* (224) expr ::= PLUS INTEGER */
   -1,  /* (225) expr ::= FLOAT */
   -2,  /* (226) expr ::= MINUS FLOAT */
   -2,  /* (227) expr ::= PLUS FLOAT */
   -1,  /* (228) expr ::= STRING */
   -1,  /* (229) expr ::= NOW */
   -1,  /* (230) expr ::= VARIABLE */
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
   -7,  /* (261) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (262) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (263) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (264) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (265) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (266) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (267) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (268) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (269) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (270) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (271) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (272) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 135: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==135);
      case 136: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==136);
      case 137: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==137);
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &t);}
        break;
      case 47: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy83);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
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
      case 176: /* distinct ::= */ yytestcase(yyruleno==176);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 53: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 55: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 56: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy83);}
        break;
      case 57: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 58: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==58);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy22, &yymsp[-2].minor.yy0);}
        break;
      case 59: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy47, &yymsp[0].minor.yy0, 1);}
        break;
      case 60: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy47, &yymsp[0].minor.yy0, 2);}
        break;
      case 61: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 62: /* bufsize ::= */
      case 64: /* pps ::= */ yytestcase(yyruleno==64);
      case 66: /* tseries ::= */ yytestcase(yyruleno==66);
      case 68: /* dbs ::= */ yytestcase(yyruleno==68);
      case 70: /* streams ::= */ yytestcase(yyruleno==70);
      case 72: /* storage ::= */ yytestcase(yyruleno==72);
      case 74: /* qtime ::= */ yytestcase(yyruleno==74);
      case 76: /* users ::= */ yytestcase(yyruleno==76);
      case 78: /* conns ::= */ yytestcase(yyruleno==78);
      case 80: /* state ::= */ yytestcase(yyruleno==80);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 63: /* bufsize ::= BUFSIZE INTEGER */
      case 65: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==67);
      case 69: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==69);
      case 71: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==73);
      case 75: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==75);
      case 77: /* users ::= USERS INTEGER */ yytestcase(yyruleno==77);
      case 79: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==79);
      case 81: /* state ::= STATE ids */ yytestcase(yyruleno==81);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 82: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
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
      case 83: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy325 = yymsp[0].minor.yy325; }
        break;
      case 84: /* cache ::= CACHE INTEGER */
      case 85: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==85);
      case 86: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==86);
      case 87: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==87);
      case 88: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==88);
      case 89: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==89);
      case 90: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==90);
      case 91: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==91);
      case 92: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==92);
      case 93: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==93);
      case 94: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==94);
      case 95: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==95);
      case 96: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==96);
      case 97: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==97);
      case 98: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==98);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 99: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy22); yymsp[1].minor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 100: /* db_optr ::= db_optr cache */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 101: /* db_optr ::= db_optr replica */
      case 118: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==118);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 102: /* db_optr ::= db_optr quorum */
      case 119: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==119);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 103: /* db_optr ::= db_optr days */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 104: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 105: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 106: /* db_optr ::= db_optr blocks */
      case 121: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==121);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 107: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 108: /* db_optr ::= db_optr wal */
      case 123: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==123);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 109: /* db_optr ::= db_optr fsync */
      case 124: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==124);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 110: /* db_optr ::= db_optr comp */
      case 122: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==122);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 111: /* db_optr ::= db_optr prec */
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 112: /* db_optr ::= db_optr keep */
      case 120: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==120);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.keep = yymsp[0].minor.yy325; }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 113: /* db_optr ::= db_optr update */
      case 125: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==125);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 114: /* db_optr ::= db_optr cachelast */
      case 126: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==126);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 115: /* topic_optr ::= db_optr */
      case 127: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==127);
{ yylhsminor.yy22 = yymsp[0].minor.yy22; yylhsminor.yy22.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy22 = yylhsminor.yy22;
        break;
      case 116: /* topic_optr ::= topic_optr partitions */
      case 128: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==128);
{ yylhsminor.yy22 = yymsp[-1].minor.yy22; yylhsminor.yy22.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy22 = yylhsminor.yy22;
        break;
      case 117: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy22); yymsp[1].minor.yy22.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 129: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy47, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy47 = yylhsminor.yy47;
        break;
      case 130: /* typename ::= ids LP signed RP */
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
      case 131: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy47, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy47 = yylhsminor.yy47;
        break;
      case 132: /* signed ::= INTEGER */
{ yylhsminor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy373 = yylhsminor.yy373;
        break;
      case 133: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy373 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 134: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy373 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 138: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy422;}
        break;
      case 139: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy504);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy422 = pCreateTable;
}
  yymsp[0].minor.yy422 = yylhsminor.yy422;
        break;
      case 140: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy422->childTableInfo, &yymsp[0].minor.yy504);
  yylhsminor.yy422 = yymsp[-1].minor.yy422;
}
  yymsp[-1].minor.yy422 = yylhsminor.yy422;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-1].minor.yy325, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy422 = yylhsminor.yy422;
        break;
      case 142: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy422 = tSetCreateTableInfo(yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy422 = yylhsminor.yy422;
        break;
      case 143: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy504 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy325, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy504 = yylhsminor.yy504;
        break;
      case 144: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy504 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy325, yymsp[-1].minor.yy325, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy504 = yylhsminor.yy504;
        break;
      case 145: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy0); yylhsminor.yy325 = yymsp[-2].minor.yy325;  }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 146: /* tagNamelist ::= ids */
{yylhsminor.yy325 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy325, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 147: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy422 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy494, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy422, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy422 = yylhsminor.yy422;
        break;
      case 148: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy325, &yymsp[0].minor.yy47); yylhsminor.yy325 = yymsp[-2].minor.yy325;  }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 149: /* columnlist ::= column */
{yylhsminor.yy325 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy325, &yymsp[0].minor.yy47);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 150: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy47, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy47);
}
  yymsp[-1].minor.yy47 = yylhsminor.yy47;
        break;
      case 151: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);    }
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 152: /* tagitemlist ::= tagitem */
{ yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1); }
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 153: /* tagitem ::= INTEGER */
      case 154: /* tagitem ::= FLOAT */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= STRING */ yytestcase(yyruleno==155);
      case 156: /* tagitem ::= BOOL */ yytestcase(yyruleno==156);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 157: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy442, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy442 = yylhsminor.yy442;
        break;
      case 158: /* tagitem ::= MINUS INTEGER */
      case 159: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==159);
      case 160: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==161);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 162: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy494 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy325, yymsp[-10].minor.yy506, yymsp[-9].minor.yy162, yymsp[-4].minor.yy325, yymsp[-3].minor.yy325, &yymsp[-8].minor.yy328, &yymsp[-7].minor.yy84, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy325, &yymsp[0].minor.yy230, &yymsp[-1].minor.yy230, yymsp[-2].minor.yy162);
}
  yymsp[-12].minor.yy494 = yylhsminor.yy494;
        break;
      case 163: /* select ::= LP select RP */
{yymsp[-2].minor.yy494 = yymsp[-1].minor.yy494;}
        break;
      case 164: /* union ::= select */
{ yylhsminor.yy145 = setSubclause(NULL, yymsp[0].minor.yy494); }
  yymsp[0].minor.yy145 = yylhsminor.yy145;
        break;
      case 165: /* union ::= union UNION ALL select */
{ yylhsminor.yy145 = appendSelectClause(yymsp[-3].minor.yy145, yymsp[0].minor.yy494); }
  yymsp[-3].minor.yy145 = yylhsminor.yy145;
        break;
      case 166: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy145, NULL, TSDB_SQL_SELECT); }
        break;
      case 167: /* select ::= SELECT selcollist */
{
  yylhsminor.yy494 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy325, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy494 = yylhsminor.yy494;
        break;
      case 168: /* sclp ::= selcollist COMMA */
{yylhsminor.yy325 = yymsp[-1].minor.yy325;}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 169: /* sclp ::= */
      case 194: /* orderby_opt ::= */ yytestcase(yyruleno==194);
{yymsp[1].minor.yy325 = 0;}
        break;
      case 170: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy325 = tSqlExprListAppend(yymsp[-3].minor.yy325, yymsp[-1].minor.yy162,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 171: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy325 = tSqlExprListAppend(yymsp[-1].minor.yy325, pNode, 0, 0);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 172: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 173: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 174: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 175: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 177: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy506 = yymsp[0].minor.yy325;}
        break;
      case 178: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy506 = yymsp[-1].minor.yy145;}
        break;
      case 179: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy325 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 180: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy325 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 181: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy325 = setTableNameList(yymsp[-3].minor.yy325, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 182: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;

  yylhsminor.yy325 = setTableNameList(yymsp[-4].minor.yy325, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy325 = yylhsminor.yy325;
        break;
      case 183: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 184: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy328.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy328.offset.n = 0;}
        break;
      case 185: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy328.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy328.offset = yymsp[-1].minor.yy0;}
        break;
      case 186: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy328, 0, sizeof(yymsp[1].minor.yy328));}
        break;
      case 187: /* session_option ::= */
{yymsp[1].minor.yy84.col.n = 0; yymsp[1].minor.yy84.gap.n = 0;}
        break;
      case 188: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy84.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy84.gap = yymsp[-1].minor.yy0;
}
        break;
      case 189: /* fill_opt ::= */
{ yymsp[1].minor.yy325 = 0;     }
        break;
      case 190: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy325, &A, -1, 0);
    yymsp[-5].minor.yy325 = yymsp[-1].minor.yy325;
}
        break;
      case 191: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy325 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 192: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 193: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 195: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 196: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy325 = tVariantListAppend(yymsp[-3].minor.yy325, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-3].minor.yy325 = yylhsminor.yy325;
        break;
      case 197: /* sortlist ::= item sortorder */
{
  yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[-1].minor.yy442, yymsp[0].minor.yy196);
}
  yymsp[-1].minor.yy325 = yylhsminor.yy325;
        break;
      case 198: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy442, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy442 = yylhsminor.yy442;
        break;
      case 199: /* sortorder ::= ASC */
{ yymsp[0].minor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 200: /* sortorder ::= DESC */
{ yymsp[0].minor.yy196 = TSDB_ORDER_DESC;}
        break;
      case 201: /* sortorder ::= */
{ yymsp[1].minor.yy196 = TSDB_ORDER_ASC; }
        break;
      case 202: /* groupby_opt ::= */
{ yymsp[1].minor.yy325 = 0;}
        break;
      case 203: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy325 = yymsp[0].minor.yy325;}
        break;
      case 204: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy325 = tVariantListAppend(yymsp[-2].minor.yy325, &yymsp[0].minor.yy442, -1);
}
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 205: /* grouplist ::= item */
{
  yylhsminor.yy325 = tVariantListAppend(NULL, &yymsp[0].minor.yy442, -1);
}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 206: /* having_opt ::= */
      case 216: /* where_opt ::= */ yytestcase(yyruleno==216);
      case 256: /* expritem ::= */ yytestcase(yyruleno==256);
{yymsp[1].minor.yy162 = 0;}
        break;
      case 207: /* having_opt ::= HAVING expr */
      case 217: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==217);
{yymsp[-1].minor.yy162 = yymsp[0].minor.yy162;}
        break;
      case 208: /* limit_opt ::= */
      case 212: /* slimit_opt ::= */ yytestcase(yyruleno==212);
{yymsp[1].minor.yy230.limit = -1; yymsp[1].minor.yy230.offset = 0;}
        break;
      case 209: /* limit_opt ::= LIMIT signed */
      case 213: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==213);
{yymsp[-1].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-1].minor.yy230.offset = 0;}
        break;
      case 210: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy230.limit = yymsp[-2].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 211: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 214: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy230.limit = yymsp[-2].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[0].minor.yy373;}
        break;
      case 215: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy230.limit = yymsp[0].minor.yy373;  yymsp[-3].minor.yy230.offset = yymsp[-2].minor.yy373;}
        break;
      case 218: /* expr ::= LP expr RP */
{yylhsminor.yy162 = yymsp[-1].minor.yy162; yylhsminor.yy162->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy162->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 219: /* expr ::= ID */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 220: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 221: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 222: /* expr ::= INTEGER */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 223: /* expr ::= MINUS INTEGER */
      case 224: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy162 = yylhsminor.yy162;
        break;
      case 225: /* expr ::= FLOAT */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 226: /* expr ::= MINUS FLOAT */
      case 227: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==227);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy162 = yylhsminor.yy162;
        break;
      case 228: /* expr ::= STRING */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 229: /* expr ::= NOW */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 230: /* expr ::= VARIABLE */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 231: /* expr ::= BOOL */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 232: /* expr ::= NULL */
{ yylhsminor.yy162 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
        break;
      case 233: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy162 = tSqlExprCreateFunction(yymsp[-1].minor.yy325, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 234: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy162 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 235: /* expr ::= expr IS NULL */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 236: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-3].minor.yy162, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy162 = yylhsminor.yy162;
        break;
      case 237: /* expr ::= expr LT expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LT);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 238: /* expr ::= expr GT expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GT);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 239: /* expr ::= expr LE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 240: /* expr ::= expr GE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_GE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 241: /* expr ::= expr NE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_NE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 242: /* expr ::= expr EQ expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_EQ);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 243: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy162); yylhsminor.yy162 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy162, yymsp[-2].minor.yy162, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy162, TK_LE), TK_AND);}
  yymsp[-4].minor.yy162 = yylhsminor.yy162;
        break;
      case 244: /* expr ::= expr AND expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_AND);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 245: /* expr ::= expr OR expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_OR); }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 246: /* expr ::= expr PLUS expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_PLUS);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 247: /* expr ::= expr MINUS expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_MINUS); }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 248: /* expr ::= expr STAR expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_STAR);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 249: /* expr ::= expr SLASH expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_DIVIDE);}
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 250: /* expr ::= expr REM expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_REM);   }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 251: /* expr ::= expr LIKE expr */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-2].minor.yy162, yymsp[0].minor.yy162, TK_LIKE);  }
  yymsp[-2].minor.yy162 = yylhsminor.yy162;
        break;
      case 252: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy162 = tSqlExprCreate(yymsp[-4].minor.yy162, (tSqlExpr*)yymsp[-1].minor.yy325, TK_IN); }
  yymsp[-4].minor.yy162 = yylhsminor.yy162;
        break;
      case 253: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy325 = tSqlExprListAppend(yymsp[-2].minor.yy325,yymsp[0].minor.yy162,0, 0);}
  yymsp[-2].minor.yy325 = yylhsminor.yy325;
        break;
      case 254: /* exprlist ::= expritem */
{yylhsminor.yy325 = tSqlExprListAppend(0,yymsp[0].minor.yy162,0, 0);}
  yymsp[0].minor.yy325 = yylhsminor.yy325;
        break;
      case 255: /* expritem ::= expr */
{yylhsminor.yy162 = yymsp[0].minor.yy162;}
  yymsp[0].minor.yy162 = yylhsminor.yy162;
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
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
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
      case 261: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 263: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 264: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy442, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 265: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 266: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 267: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy325, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 268: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 269: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 270: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 271: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 272: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
