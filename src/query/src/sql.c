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
#define YYNOCODE 283
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SIntervalVal yy66;
  SCreateTableSql* yy86;
  SArray* yy89;
  tVariant yy112;
  int32_t yy130;
  int64_t yy145;
  SRelationInfo* yy166;
  SCreateAcctInfo yy307;
  tSqlExpr* yy342;
  int yy346;
  SLimitVal yy372;
  SSqlNode* yy378;
  SWindowStateVal yy392;
  SSessionWindowVal yy431;
  TAOS_FIELD yy465;
  SCreateDbInfo yy470;
  SCreatedTableInfo yy506;
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
#define YYNSTATE             371
#define YYNRULE              297
#define YYNTOKEN             199
#define YY_MAX_SHIFT         370
#define YY_MIN_SHIFTREDUCE   582
#define YY_MAX_SHIFTREDUCE   878
#define YY_ERROR_ACTION      879
#define YY_ACCEPT_ACTION     880
#define YY_NO_ACTION         881
#define YY_MIN_REDUCE        882
#define YY_MAX_REDUCE        1178
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
#define YY_ACTTAB_COUNT (768)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   250,  633,  633,  633,  233,  368,   23,  249,  254,  634,
 /*    10 */   634,  634,  239,   57,   58,  669,   61,   62, 1065, 1041,
 /*    20 */   253,   51,  633,   60,  324,   65,   63,   66,   64, 1007,
 /*    30 */   634, 1005, 1006,   56,   55,  209, 1008,   54,   53,   52,
 /*    40 */  1009,  210, 1010, 1011,  164,  880,  370,  583,  584,  585,
 /*    50 */   586,  587,  588,  589,  590,  591,  592,  593,  594,  595,
 /*    60 */   596,  369,  216,  212,  234,   57,   58, 1151,   61,   62,
 /*    70 */   212,  212,  253,   51, 1155,   60,  324,   65,   63,   66,
 /*    80 */    64, 1155, 1155,  212,   29,   56,   55,   83, 1062,   54,
 /*    90 */    53,   52,   57,   58, 1154,   61,   62, 1056, 1056,  253,
 /*   100 */    51,  101,   60,  324,   65,   63,   66,   64,   54,   53,
 /*   110 */    52,   89,   56,   55,  276,  236,   54,   53,   52,   57,
 /*   120 */    59, 1103,   61,   62,  364,  968,  253,   51,   98,   60,
 /*   130 */   324,   65,   63,   66,   64,  245,  817, 1025,  164,   56,
 /*   140 */    55,  164, 1041,   54,   53,   52,   58,  247,   61,   62,
 /*   150 */  1056,   45,  253,   51, 1041,   60,  324,   65,   63,   66,
 /*   160 */    64, 1020, 1021,   35, 1024,   56,   55,  237,  164,   54,
 /*   170 */    53,   52,   44,  320,  363,  362,  319,  318,  317,  361,
 /*   180 */   316,  315,  314,  360,  313,  359,  358,  999,  987,  988,
 /*   190 */   989,  990,  991,  992,  993,  994,  995,  996,  997,  998,
 /*   200 */  1000, 1001,   61,   62,   24,   38,  253,   51,  322,   60,
 /*   210 */   324,   65,   63,   66,   64, 1104,  297,  295,   94,   56,
 /*   220 */    55,  173,  215,   54,   53,   52,  252,  832,   74,  221,
 /*   230 */   821,  157,  824,   96,  827,  140,  139,  138,  220,  285,
 /*   240 */   252,  832,  329,   89,  821,   95,  824,   84,  827,  235,
 /*   250 */   782,  783,   65,   63,   66,   64, 1038,   14,  231,  232,
 /*   260 */    56,   55,  325,   38,   54,   53,   52,    5,   41,  183,
 /*   270 */    75,  263,  231,  232,  182,  107,  112,  103,  111,  745,
 /*   280 */   799,  179,  742,   45,  743,  263,  744,  124,  118,  129,
 /*   290 */   256,  100,   81,  309,  128,  180,  134,  137,  127,  823,
 /*   300 */   262,  826,  283,  282,  275,  131,   79,  243,   67,  717,
 /*   310 */   258,  259,  251,  228, 1038,  763,  203,  201,  199,  822,
 /*   320 */    38,  825,   67,  198,  144,  143,  142,  141, 1023,   44,
 /*   330 */    38,  363,  362,   38,    6,   38,  361,  346,  345,  798,
 /*   340 */   360,   38,  359,  358,   38,  833,  828,  829, 1031,   38,
 /*   350 */    56,   55,   38,  268,   54,   53,   52,   38,   38,  833,
 /*   360 */   828,  829,  272,  271,  244,  367,  366,  610,  257,  930,
 /*   370 */   255, 1038,  332,  331,  333,  126,  193,  334,  264,  335,
 /*   380 */   261, 1038,  341,  340, 1038,  336, 1038,  356,  342,  154,
 /*   390 */   152,  151, 1038,  343,  246, 1038,  344,  746,  260,   80,
 /*   400 */  1038,  263,  348, 1038,  767,  940,  760,  931, 1037, 1038,
 /*   410 */    71, 1039,  193,  322,  193,  277,  830,    1,  181,    3,
 /*   420 */   194,   86,   87,  779,  789,  790,  727,  301,  729,   39,
 /*   430 */   303,  728,   34, 1022,  853,    9,  831,  159,   68,   26,
 /*   440 */    39,   39,   68,  834,   99,   68,  819,  326,   25,  632,
 /*   450 */    16,  117,   15,  116,   72,  338,  337,   25,   18,   78,
 /*   460 */    17,  279,  279,   25,  752,  304,  753,  750,   20,  751,
 /*   470 */    19,  123,   22,  122,   21,  136,  135,  356, 1114,  153,
 /*   480 */  1150, 1149,  820,  229,  230, 1113,  213,  214,  217,  211,
 /*   490 */   218,  716, 1040,  219,  223, 1174, 1166,  224,  241,  225,
 /*   500 */   222, 1110, 1109,  208,  242,  175,  347,  273,  155,  156,
 /*   510 */    48, 1036, 1054, 1064, 1096, 1075, 1095, 1072, 1073, 1057,
 /*   520 */   280, 1077,  174,  158,  163, 1032,  299,  291,  165,  176,
 /*   530 */   778, 1030,  177,  284,  178,  945,  306,  307,  308,  238,
 /*   540 */   166,  170,  286,  167,  168,  288,  298,  311,  296,   76,
 /*   550 */   312,   50,  836,   46,  206,   42,  323,  939,  330, 1173,
 /*   560 */   114,   73, 1172, 1169,  184,  339, 1165,  294,  120, 1164,
 /*   570 */  1161,  185,  965,   43,   40,   47,  207,  927,  292,  130,
 /*   580 */   925,  132,  133,  923,  922,  265,  196,  197,  290,  919,
 /*   590 */   918,  917,  916,  915,  914,  913,  200,  202,  287,  909,
 /*   600 */   907,  905,  204,  902,  205,   82,  278, 1034,   85,   90,
 /*   610 */   289,   49, 1097,  310,  357,  125,  349,  350,  351,  352,
 /*   620 */    77,  354,  353,  355,  248,  365,  305,  878,  266,  267,
 /*   630 */   877,  226,  227,  269,  270,  876,  108,  944,  943,  109,
 /*   640 */   859,  858,  274,  279,  300,  921,  920,   10,   30,  912,
 /*   650 */   188,   88,  145,  966,  186,  191,  187,  189,  190,  192,
 /*   660 */   146,  147,    2,  911,  967,  148, 1003,  904,    4,  903,
 /*   670 */   755,  281,   91,   33,  780,  171,  169,  160,  172, 1013,
 /*   680 */   791,  161,  162,  785,   92,  240,  787,   93,  293,   31,
 /*   690 */    11,   32,   12,   97,   13,   27,  302,   28,  100,  102,
 /*   700 */   647,   36,  104,  105,   37,  106,  682,  680,  679,  678,
 /*   710 */   676,  675,  674,  671,  637,  321,  110,    7,  327,  835,
 /*   720 */   837,    8,  328,  719,   39,   69,  113,  115,   70,  749,
 /*   730 */   748,  718,  119,  121,  715,  663,  661,  653,  659,  655,
 /*   740 */   657,  651,  649,  685,  684,  683,  681,  677,  673,  672,
 /*   750 */   195,  635,  600,  598,  882,  881,  881,  881,  881,  881,
 /*   760 */   881,  881,  881,  881,  881,  881,  149,  150,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   208,    1,    1,    1,  201,  202,  271,  208,  208,    9,
 /*    10 */     9,    9,  247,   13,   14,    5,   16,   17,  202,  254,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  225,
 /*    30 */     9,  227,  228,   33,   34,  271,  232,   37,   38,   39,
 /*    40 */   236,  271,  238,  239,  202,  199,  200,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,  271,  271,   62,   13,   14,  271,   16,   17,
 /*    70 */   271,  271,   20,   21,  282,   23,   24,   25,   26,   27,
 /*    80 */    28,  282,  282,  271,   83,   33,   34,   87,  272,   37,
 /*    90 */    38,   39,   13,   14,  282,   16,   17,  251,  251,   20,
 /*   100 */    21,  209,   23,   24,   25,   26,   27,   28,   37,   38,
 /*   110 */    39,   83,   33,   34,  268,  268,   37,   38,   39,   13,
 /*   120 */    14,  279,   16,   17,  223,  224,   20,   21,  209,   23,
 /*   130 */    24,   25,   26,   27,   28,  247,   84,  245,  202,   33,
 /*   140 */    34,  202,  254,   37,   38,   39,   14,  247,   16,   17,
 /*   150 */   251,  123,   20,   21,  254,   23,   24,   25,   26,   27,
 /*   160 */    28,  242,  243,  244,  245,   33,   34,  268,  202,   37,
 /*   170 */    38,   39,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   180 */   107,  108,  109,  110,  111,  112,  113,  225,  226,  227,
 /*   190 */   228,  229,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   200 */   238,  239,   16,   17,   44,  202,   20,   21,   85,   23,
 /*   210 */    24,   25,   26,   27,   28,  279,  277,  281,  279,   33,
 /*   220 */    34,  258,   62,   37,   38,   39,    1,    2,   98,   69,
 /*   230 */     5,  202,    7,  255,    9,   75,   76,   77,   78,  276,
 /*   240 */     1,    2,   82,   83,    5,  279,    7,  269,    9,  246,
 /*   250 */   128,  129,   25,   26,   27,   28,  253,   83,   33,   34,
 /*   260 */    33,   34,   37,  202,   37,   38,   39,   63,   64,   65,
 /*   270 */   140,  202,   33,   34,   70,   71,   72,   73,   74,    2,
 /*   280 */    77,  212,    5,  123,    7,  202,    9,   63,   64,   65,
 /*   290 */    69,  117,  118,   89,   70,  212,   72,   73,   74,    5,
 /*   300 */    69,    7,  273,  274,  144,   81,  146,  246,   83,    5,
 /*   310 */    33,   34,   61,  153,  253,   37,   63,   64,   65,    5,
 /*   320 */   202,    7,   83,   70,   71,   72,   73,   74,    0,   99,
 /*   330 */   202,  101,  102,  202,   83,  202,  106,   33,   34,  136,
 /*   340 */   110,  202,  112,  113,  202,  120,  121,  122,  202,  202,
 /*   350 */    33,   34,  202,  145,   37,   38,   39,  202,  202,  120,
 /*   360 */   121,  122,  154,  155,  246,   66,   67,   68,  147,  207,
 /*   370 */   149,  253,  151,  152,  246,   79,  214,  246,  147,  246,
 /*   380 */   149,  253,  151,  152,  253,  246,  253,   91,  246,   63,
 /*   390 */    64,   65,  253,  246,  248,  253,  246,  120,  121,  209,
 /*   400 */   253,  202,  246,  253,  126,  207,   98,  207,  253,  253,
 /*   410 */    98,  212,  214,   85,  214,   84,  122,  210,  211,  205,
 /*   420 */   206,   84,   84,   84,   84,   84,   84,   84,   84,   98,
 /*   430 */    84,   84,   83,  243,   84,  127,  122,   98,   98,   98,
 /*   440 */    98,   98,   98,   84,   98,   98,    1,   15,   98,   84,
 /*   450 */   148,  148,  150,  150,  142,   33,   34,   98,  148,   83,
 /*   460 */   150,  124,  124,   98,    5,  116,    7,    5,  148,    7,
 /*   470 */   150,  148,  148,  150,  150,   79,   80,   91,  241,   61,
 /*   480 */   271,  271,   37,  271,  271,  241,  271,  271,  271,  271,
 /*   490 */   271,  115,  254,  271,  271,  254,  254,  271,  241,  271,
 /*   500 */   271,  241,  241,  271,  241,  249,  241,  202,  202,  202,
 /*   510 */   270,  202,  267,  202,  280,  202,  280,  202,  202,  251,
 /*   520 */   251,  202,  256,  202,  202,  251,  250,  202,  266,  202,
 /*   530 */   122,  202,  202,  275,  202,  202,  202,  202,  202,  275,
 /*   540 */   265,  261,  275,  264,  263,  275,  134,  202,  137,  139,
 /*   550 */   202,  138,  120,  202,  202,  202,  202,  202,  202,  202,
 /*   560 */   202,  141,  202,  202,  202,  202,  202,  132,  202,  202,
 /*   570 */   202,  202,  202,  202,  202,  202,  202,  202,  131,  202,
 /*   580 */   202,  202,  202,  202,  202,  202,  202,  202,  130,  202,
 /*   590 */   202,  202,  202,  202,  202,  202,  202,  202,  133,  202,
 /*   600 */   202,  202,  202,  202,  202,  119,  203,  203,  203,  203,
 /*   610 */   203,  143,  203,   90,  114,   97,   96,   51,   93,   95,
 /*   620 */   203,   94,   55,   92,  203,   85,  203,    5,  156,    5,
 /*   630 */     5,  203,  203,  156,    5,    5,  209,  213,  213,  209,
 /*   640 */   101,  100,  145,  124,  116,  203,  203,   83,   83,  203,
 /*   650 */   216,  125,  204,  222,  221,  218,  220,  219,  217,  215,
 /*   660 */   204,  204,  210,  203,  224,  204,  240,  203,  205,  203,
 /*   670 */    84,   98,   98,  257,   84,  260,  262,   83,  259,  240,
 /*   680 */    84,   83,   98,   84,   83,    1,   84,   83,   83,   98,
 /*   690 */   135,   98,  135,   87,   83,   83,  116,   83,  117,   79,
 /*   700 */     5,   88,   87,   71,   88,   87,    9,    5,    5,    5,
 /*   710 */     5,    5,    5,    5,   86,   15,   79,   83,   24,   84,
 /*   720 */   120,   83,   59,    5,   98,   16,  150,  150,   16,  122,
 /*   730 */   122,    5,  150,  150,   84,    5,    5,    5,    5,    5,
 /*   740 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   750 */    98,   86,   61,   60,    0,  283,  283,  283,  283,  283,
 /*   760 */   283,  283,  283,  283,  283,  283,   21,   21,  283,  283,
 /*   770 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   780 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   790 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   800 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   810 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   820 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   830 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   840 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   850 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   860 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   870 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   880 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   890 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   900 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   910 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   920 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   930 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   940 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   950 */   283,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   960 */   283,  283,  283,  283,  283,  283,  283,
};
#define YY_SHIFT_COUNT    (370)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (754)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   160,   73,   73,  230,  230,  123,  225,  239,  239,    1,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,   21,    0,    2,  239,  277,  277,  277,   28,
 /*    30 */    28,   21,   21,  122,   21,  328,   21,   21,   21,   21,
 /*    40 */   296,  123,  386,  386,   10,  768,  768,  768,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  239,  239,  239,  239,  239,  239,  277,  277,
 /*    70 */   277,  304,  304,  304,  304,  304,  304,  174,  304,   21,
 /*    80 */    21,   21,   21,   21,  278,   21,   21,   21,   28,   28,
 /*    90 */    21,   21,   21,   21,  203,  203,  308,   28,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   150 */    21,   21,   21,   21,   21,  418,  418,  418,  418,  408,
 /*   160 */   408,  408,  408,  418,  418,  410,  420,  412,  413,  411,
 /*   170 */   435,  447,  458,  465,  468,  486,  418,  418,  418,  523,
 /*   180 */   523,  500,  123,  123,  418,  418,  518,  520,  566,  525,
 /*   190 */   524,  567,  527,  531,  500,   10,  418,  418,  540,  540,
 /*   200 */   418,  540,  418,  540,  418,  418,  768,  768,   52,   79,
 /*   210 */    79,  106,   79,  132,  186,  204,  227,  227,  227,  227,
 /*   220 */   224,  253,  317,  317,  317,  317,  221,  231,  208,   71,
 /*   230 */    71,  294,  314,  299,  326,  331,  337,  338,  339,  340,
 /*   240 */   341,  312,  130,  342,  343,  344,  346,  347,  349,  350,
 /*   250 */   359,  445,  251,  432,  365,  302,  303,  310,  459,  462,
 /*   260 */   422,  320,  323,  376,  324,  396,  622,  472,  624,  625,
 /*   270 */   477,  629,  630,  539,  541,  497,  519,  528,  564,  526,
 /*   280 */   586,  565,  573,  574,  590,  594,  596,  598,  599,  584,
 /*   290 */   601,  602,  604,  684,  605,  591,  555,  593,  557,  606,
 /*   300 */   611,  528,  612,  580,  614,  581,  620,  613,  615,  632,
 /*   310 */   695,  616,  618,  697,  702,  703,  704,  705,  706,  707,
 /*   320 */   708,  628,  700,  637,  634,  635,  600,  638,  694,  663,
 /*   330 */   709,  576,  577,  626,  626,  626,  626,  607,  608,  712,
 /*   340 */   582,  583,  626,  626,  626,  718,  726,  650,  626,  730,
 /*   350 */   731,  732,  733,  734,  735,  736,  737,  738,  739,  740,
 /*   360 */   741,  742,  743,  744,  652,  665,  745,  746,  691,  693,
 /*   370 */   754,
};
#define YY_REDUCE_COUNT (207)
#define YY_REDUCE_MIN   (-265)
#define YY_REDUCE_MAX   (466)
static const short yy_reduce_ofst[] = {
 /*     0 */  -154,  -38,  -38, -196, -196,  -81, -208, -201, -200,   29,
 /*    10 */     3,  -64,  -61,   61,  118,  128,  131,  133,  139,  142,
 /*    20 */   147,  150,  156, -184, -197, -188, -235, -112, -100, -153,
 /*    30 */  -101, -158,  -34,  -37,  146, -108,   69,   83,  199,  155,
 /*    40 */   162,  190,  198,  200,  -99,  -22,  207,  214, -265, -236,
 /*    50 */  -230, -209, -204,  209,  210,  212,  213,  215,  216,  217,
 /*    60 */   218,  219,  222,  223,  226,  228,  229,  232,  238,  241,
 /*    70 */   242,  237,  244,  257,  260,  261,  263,  256,  265,  305,
 /*    80 */   306,  307,  309,  311,  240,  313,  315,  316,  268,  269,
 /*    90 */   319,  321,  322,  325,  234,  236,  266,  274,  327,  329,
 /*   100 */   330,  332,  333,  334,  335,  336,  345,  348,  351,  352,
 /*   110 */   353,  354,  355,  356,  357,  358,  360,  361,  362,  363,
 /*   120 */   364,  366,  367,  368,  369,  370,  371,  372,  373,  374,
 /*   130 */   375,  377,  378,  379,  380,  381,  382,  383,  384,  385,
 /*   140 */   387,  388,  389,  390,  391,  392,  393,  394,  395,  397,
 /*   150 */   398,  399,  400,  401,  402,  403,  404,  405,  406,  258,
 /*   160 */   264,  267,  270,  407,  409,  245,  262,  275,  279,  281,
 /*   170 */   414,  280,  415,  419,  416,  276,  417,  421,  423,  424,
 /*   180 */   425,  426,  427,  430,  428,  429,  431,  433,  436,  434,
 /*   190 */   438,  441,  437,  444,  439,  440,  442,  443,  448,  456,
 /*   200 */   446,  457,  460,  461,  464,  466,  452,  463,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   879, 1002,  941, 1012,  928,  938, 1157, 1157, 1157,  879,
 /*    10 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*    20 */   879,  879,  879, 1066,  899, 1157,  879,  879,  879,  879,
 /*    30 */   879,  879,  879, 1081,  879,  938,  879,  879,  879,  879,
 /*    40 */   948,  938,  948,  948,  879, 1061,  986, 1004,  879,  879,
 /*    50 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*    60 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*    70 */   879,  879,  879,  879,  879,  879,  879, 1033,  879,  879,
 /*    80 */   879,  879,  879,  879, 1068, 1074, 1071,  879,  879,  879,
 /*    90 */  1076,  879,  879,  879, 1100, 1100, 1059,  879,  879,  879,
 /*   100 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   110 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   120 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   130 */   926,  879,  924,  879,  879,  879,  879,  879,  879,  879,
 /*   140 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   150 */   879,  879,  879,  879,  879,  901,  901,  901,  901,  879,
 /*   160 */   879,  879,  879,  901,  901, 1107, 1111, 1093, 1105, 1101,
 /*   170 */  1088, 1086, 1084, 1092, 1115, 1035,  901,  901,  901,  946,
 /*   180 */   946,  942,  938,  938,  901,  901,  964,  962,  960,  952,
 /*   190 */   958,  954,  956,  950,  929,  879,  901,  901,  936,  936,
 /*   200 */   901,  936,  901,  936,  901,  901,  986, 1004,  879, 1116,
 /*   210 */  1106,  879, 1156, 1146, 1145,  879, 1152, 1144, 1143, 1142,
 /*   220 */   879,  879, 1138, 1141, 1140, 1139,  879,  879,  879, 1148,
 /*   230 */  1147,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   240 */   879, 1112, 1108,  879,  879,  879,  879,  879,  879,  879,
 /*   250 */   879,  879, 1118,  879,  879,  879,  879,  879,  879,  879,
 /*   260 */  1047,  879,  879, 1014,  879,  879,  879,  879,  879,  879,
 /*   270 */   879,  879,  879,  879,  879,  879, 1058,  879,  879,  879,
 /*   280 */   879,  879, 1070, 1069,  879,  879,  879,  879,  879,  879,
 /*   290 */   879,  879,  879,  879,  879, 1102,  879, 1094,  879,  879,
 /*   300 */   879, 1026,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   310 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   320 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   330 */   879,  879,  879, 1175, 1170, 1171, 1168,  879,  879,  879,
 /*   340 */   879,  879, 1167, 1162, 1163,  879,  879,  879, 1160,  879,
 /*   350 */   879,  879,  879,  879,  879,  879,  879,  879,  879,  879,
 /*   360 */   879,  879,  879,  879,  970,  879,  908,  906,  879,  897,
 /*   370 */   879,
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
    1,  /*       DESC => ID */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*    COMPACT => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
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
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*         TO => nothing */
    0,  /*      SPLIT => nothing */
    1,  /*       NULL => ID */
    1,  /*        NOW => ID */
    0,  /*   VARIABLE => nothing */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*      EVERY => nothing */
    0,  /*    SESSION => nothing */
    0,  /* STATE_WINDOW => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
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
    1,  /*       FILE => ID */
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
  /*   77 */ "DESC",
  /*   78 */ "ALTER",
  /*   79 */ "PASS",
  /*   80 */ "PRIVILEGE",
  /*   81 */ "LOCAL",
  /*   82 */ "COMPACT",
  /*   83 */ "LP",
  /*   84 */ "RP",
  /*   85 */ "IF",
  /*   86 */ "EXISTS",
  /*   87 */ "AS",
  /*   88 */ "OUTPUTTYPE",
  /*   89 */ "AGGREGATE",
  /*   90 */ "BUFSIZE",
  /*   91 */ "PPS",
  /*   92 */ "TSERIES",
  /*   93 */ "DBS",
  /*   94 */ "STORAGE",
  /*   95 */ "QTIME",
  /*   96 */ "CONNS",
  /*   97 */ "STATE",
  /*   98 */ "COMMA",
  /*   99 */ "KEEP",
  /*  100 */ "CACHE",
  /*  101 */ "REPLICA",
  /*  102 */ "QUORUM",
  /*  103 */ "DAYS",
  /*  104 */ "MINROWS",
  /*  105 */ "MAXROWS",
  /*  106 */ "BLOCKS",
  /*  107 */ "CTIME",
  /*  108 */ "WAL",
  /*  109 */ "FSYNC",
  /*  110 */ "COMP",
  /*  111 */ "PRECISION",
  /*  112 */ "UPDATE",
  /*  113 */ "CACHELAST",
  /*  114 */ "PARTITIONS",
  /*  115 */ "UNSIGNED",
  /*  116 */ "TAGS",
  /*  117 */ "USING",
  /*  118 */ "TO",
  /*  119 */ "SPLIT",
  /*  120 */ "NULL",
  /*  121 */ "NOW",
  /*  122 */ "VARIABLE",
  /*  123 */ "SELECT",
  /*  124 */ "UNION",
  /*  125 */ "ALL",
  /*  126 */ "DISTINCT",
  /*  127 */ "FROM",
  /*  128 */ "INTERVAL",
  /*  129 */ "EVERY",
  /*  130 */ "SESSION",
  /*  131 */ "STATE_WINDOW",
  /*  132 */ "FILL",
  /*  133 */ "SLIDING",
  /*  134 */ "ORDER",
  /*  135 */ "BY",
  /*  136 */ "ASC",
  /*  137 */ "GROUP",
  /*  138 */ "HAVING",
  /*  139 */ "LIMIT",
  /*  140 */ "OFFSET",
  /*  141 */ "SLIMIT",
  /*  142 */ "SOFFSET",
  /*  143 */ "WHERE",
  /*  144 */ "RESET",
  /*  145 */ "QUERY",
  /*  146 */ "SYNCDB",
  /*  147 */ "ADD",
  /*  148 */ "COLUMN",
  /*  149 */ "MODIFY",
  /*  150 */ "TAG",
  /*  151 */ "CHANGE",
  /*  152 */ "SET",
  /*  153 */ "KILL",
  /*  154 */ "CONNECTION",
  /*  155 */ "STREAM",
  /*  156 */ "COLON",
  /*  157 */ "ABORT",
  /*  158 */ "AFTER",
  /*  159 */ "ATTACH",
  /*  160 */ "BEFORE",
  /*  161 */ "BEGIN",
  /*  162 */ "CASCADE",
  /*  163 */ "CLUSTER",
  /*  164 */ "CONFLICT",
  /*  165 */ "COPY",
  /*  166 */ "DEFERRED",
  /*  167 */ "DELIMITERS",
  /*  168 */ "DETACH",
  /*  169 */ "EACH",
  /*  170 */ "END",
  /*  171 */ "EXPLAIN",
  /*  172 */ "FAIL",
  /*  173 */ "FOR",
  /*  174 */ "IGNORE",
  /*  175 */ "IMMEDIATE",
  /*  176 */ "INITIALLY",
  /*  177 */ "INSTEAD",
  /*  178 */ "MATCH",
  /*  179 */ "KEY",
  /*  180 */ "OF",
  /*  181 */ "RAISE",
  /*  182 */ "REPLACE",
  /*  183 */ "RESTRICT",
  /*  184 */ "ROW",
  /*  185 */ "STATEMENT",
  /*  186 */ "TRIGGER",
  /*  187 */ "VIEW",
  /*  188 */ "SEMI",
  /*  189 */ "NONE",
  /*  190 */ "PREV",
  /*  191 */ "LINEAR",
  /*  192 */ "IMPORT",
  /*  193 */ "TBNAME",
  /*  194 */ "JOIN",
  /*  195 */ "INSERT",
  /*  196 */ "INTO",
  /*  197 */ "VALUES",
  /*  198 */ "FILE",
  /*  199 */ "program",
  /*  200 */ "cmd",
  /*  201 */ "dbPrefix",
  /*  202 */ "ids",
  /*  203 */ "cpxName",
  /*  204 */ "ifexists",
  /*  205 */ "alter_db_optr",
  /*  206 */ "alter_topic_optr",
  /*  207 */ "acct_optr",
  /*  208 */ "exprlist",
  /*  209 */ "ifnotexists",
  /*  210 */ "db_optr",
  /*  211 */ "topic_optr",
  /*  212 */ "typename",
  /*  213 */ "bufsize",
  /*  214 */ "pps",
  /*  215 */ "tseries",
  /*  216 */ "dbs",
  /*  217 */ "streams",
  /*  218 */ "storage",
  /*  219 */ "qtime",
  /*  220 */ "users",
  /*  221 */ "conns",
  /*  222 */ "state",
  /*  223 */ "intitemlist",
  /*  224 */ "intitem",
  /*  225 */ "keep",
  /*  226 */ "cache",
  /*  227 */ "replica",
  /*  228 */ "quorum",
  /*  229 */ "days",
  /*  230 */ "minrows",
  /*  231 */ "maxrows",
  /*  232 */ "blocks",
  /*  233 */ "ctime",
  /*  234 */ "wal",
  /*  235 */ "fsync",
  /*  236 */ "comp",
  /*  237 */ "prec",
  /*  238 */ "update",
  /*  239 */ "cachelast",
  /*  240 */ "partitions",
  /*  241 */ "signed",
  /*  242 */ "create_table_args",
  /*  243 */ "create_stable_args",
  /*  244 */ "create_table_list",
  /*  245 */ "create_from_stable",
  /*  246 */ "columnlist",
  /*  247 */ "tagitemlist",
  /*  248 */ "tagNamelist",
  /*  249 */ "to_opt",
  /*  250 */ "split_opt",
  /*  251 */ "select",
  /*  252 */ "to_split",
  /*  253 */ "column",
  /*  254 */ "tagitem",
  /*  255 */ "selcollist",
  /*  256 */ "from",
  /*  257 */ "where_opt",
  /*  258 */ "interval_option",
  /*  259 */ "sliding_opt",
  /*  260 */ "session_option",
  /*  261 */ "windowstate_option",
  /*  262 */ "fill_opt",
  /*  263 */ "groupby_opt",
  /*  264 */ "having_opt",
  /*  265 */ "orderby_opt",
  /*  266 */ "slimit_opt",
  /*  267 */ "limit_opt",
  /*  268 */ "union",
  /*  269 */ "sclp",
  /*  270 */ "distinct",
  /*  271 */ "expr",
  /*  272 */ "as",
  /*  273 */ "tablelist",
  /*  274 */ "sub",
  /*  275 */ "tmvar",
  /*  276 */ "intervalKey",
  /*  277 */ "sortlist",
  /*  278 */ "sortitem",
  /*  279 */ "item",
  /*  280 */ "sortorder",
  /*  281 */ "grouplist",
  /*  282 */ "expritem",
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
 /*  39 */ "cmd ::= DESC ids cpxName",
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
 /*  50 */ "cmd ::= COMPACT VNODES IN LP exprlist RP",
 /*  51 */ "ids ::= ID",
 /*  52 */ "ids ::= STRING",
 /*  53 */ "ifexists ::= IF EXISTS",
 /*  54 */ "ifexists ::=",
 /*  55 */ "ifnotexists ::= IF NOT EXISTS",
 /*  56 */ "ifnotexists ::=",
 /*  57 */ "cmd ::= CREATE DNODE ids",
 /*  58 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  59 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  60 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  61 */ "cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  62 */ "cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize",
 /*  63 */ "cmd ::= CREATE USER ids PASS ids",
 /*  64 */ "bufsize ::=",
 /*  65 */ "bufsize ::= BUFSIZE INTEGER",
 /*  66 */ "pps ::=",
 /*  67 */ "pps ::= PPS INTEGER",
 /*  68 */ "tseries ::=",
 /*  69 */ "tseries ::= TSERIES INTEGER",
 /*  70 */ "dbs ::=",
 /*  71 */ "dbs ::= DBS INTEGER",
 /*  72 */ "streams ::=",
 /*  73 */ "streams ::= STREAMS INTEGER",
 /*  74 */ "storage ::=",
 /*  75 */ "storage ::= STORAGE INTEGER",
 /*  76 */ "qtime ::=",
 /*  77 */ "qtime ::= QTIME INTEGER",
 /*  78 */ "users ::=",
 /*  79 */ "users ::= USERS INTEGER",
 /*  80 */ "conns ::=",
 /*  81 */ "conns ::= CONNS INTEGER",
 /*  82 */ "state ::=",
 /*  83 */ "state ::= STATE ids",
 /*  84 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  85 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  86 */ "intitemlist ::= intitem",
 /*  87 */ "intitem ::= INTEGER",
 /*  88 */ "keep ::= KEEP intitemlist",
 /*  89 */ "cache ::= CACHE INTEGER",
 /*  90 */ "replica ::= REPLICA INTEGER",
 /*  91 */ "quorum ::= QUORUM INTEGER",
 /*  92 */ "days ::= DAYS INTEGER",
 /*  93 */ "minrows ::= MINROWS INTEGER",
 /*  94 */ "maxrows ::= MAXROWS INTEGER",
 /*  95 */ "blocks ::= BLOCKS INTEGER",
 /*  96 */ "ctime ::= CTIME INTEGER",
 /*  97 */ "wal ::= WAL INTEGER",
 /*  98 */ "fsync ::= FSYNC INTEGER",
 /*  99 */ "comp ::= COMP INTEGER",
 /* 100 */ "prec ::= PRECISION STRING",
 /* 101 */ "update ::= UPDATE INTEGER",
 /* 102 */ "cachelast ::= CACHELAST INTEGER",
 /* 103 */ "partitions ::= PARTITIONS INTEGER",
 /* 104 */ "db_optr ::=",
 /* 105 */ "db_optr ::= db_optr cache",
 /* 106 */ "db_optr ::= db_optr replica",
 /* 107 */ "db_optr ::= db_optr quorum",
 /* 108 */ "db_optr ::= db_optr days",
 /* 109 */ "db_optr ::= db_optr minrows",
 /* 110 */ "db_optr ::= db_optr maxrows",
 /* 111 */ "db_optr ::= db_optr blocks",
 /* 112 */ "db_optr ::= db_optr ctime",
 /* 113 */ "db_optr ::= db_optr wal",
 /* 114 */ "db_optr ::= db_optr fsync",
 /* 115 */ "db_optr ::= db_optr comp",
 /* 116 */ "db_optr ::= db_optr prec",
 /* 117 */ "db_optr ::= db_optr keep",
 /* 118 */ "db_optr ::= db_optr update",
 /* 119 */ "db_optr ::= db_optr cachelast",
 /* 120 */ "topic_optr ::= db_optr",
 /* 121 */ "topic_optr ::= topic_optr partitions",
 /* 122 */ "alter_db_optr ::=",
 /* 123 */ "alter_db_optr ::= alter_db_optr replica",
 /* 124 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 125 */ "alter_db_optr ::= alter_db_optr keep",
 /* 126 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 127 */ "alter_db_optr ::= alter_db_optr comp",
 /* 128 */ "alter_db_optr ::= alter_db_optr update",
 /* 129 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 130 */ "alter_topic_optr ::= alter_db_optr",
 /* 131 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 132 */ "typename ::= ids",
 /* 133 */ "typename ::= ids LP signed RP",
 /* 134 */ "typename ::= ids UNSIGNED",
 /* 135 */ "signed ::= INTEGER",
 /* 136 */ "signed ::= PLUS INTEGER",
 /* 137 */ "signed ::= MINUS INTEGER",
 /* 138 */ "cmd ::= CREATE TABLE create_table_args",
 /* 139 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 140 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 141 */ "cmd ::= CREATE TABLE create_table_list",
 /* 142 */ "create_table_list ::= create_from_stable",
 /* 143 */ "create_table_list ::= create_table_list create_from_stable",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 145 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 146 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 147 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 148 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 149 */ "tagNamelist ::= ids",
 /* 150 */ "create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select",
 /* 151 */ "to_opt ::=",
 /* 152 */ "to_opt ::= TO ids cpxName",
 /* 153 */ "split_opt ::=",
 /* 154 */ "split_opt ::= SPLIT ids",
 /* 155 */ "columnlist ::= columnlist COMMA column",
 /* 156 */ "columnlist ::= column",
 /* 157 */ "column ::= ids typename",
 /* 158 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 159 */ "tagitemlist ::= tagitem",
 /* 160 */ "tagitem ::= INTEGER",
 /* 161 */ "tagitem ::= FLOAT",
 /* 162 */ "tagitem ::= STRING",
 /* 163 */ "tagitem ::= BOOL",
 /* 164 */ "tagitem ::= NULL",
 /* 165 */ "tagitem ::= NOW",
 /* 166 */ "tagitem ::= NOW PLUS VARIABLE",
 /* 167 */ "tagitem ::= NOW MINUS VARIABLE",
 /* 168 */ "tagitem ::= MINUS INTEGER",
 /* 169 */ "tagitem ::= MINUS FLOAT",
 /* 170 */ "tagitem ::= PLUS INTEGER",
 /* 171 */ "tagitem ::= PLUS FLOAT",
 /* 172 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 173 */ "select ::= LP select RP",
 /* 174 */ "union ::= select",
 /* 175 */ "union ::= union UNION ALL select",
 /* 176 */ "cmd ::= union",
 /* 177 */ "select ::= SELECT selcollist",
 /* 178 */ "sclp ::= selcollist COMMA",
 /* 179 */ "sclp ::=",
 /* 180 */ "selcollist ::= sclp distinct expr as",
 /* 181 */ "selcollist ::= sclp STAR",
 /* 182 */ "as ::= AS ids",
 /* 183 */ "as ::= ids",
 /* 184 */ "as ::=",
 /* 185 */ "distinct ::= DISTINCT",
 /* 186 */ "distinct ::=",
 /* 187 */ "from ::= FROM tablelist",
 /* 188 */ "from ::= FROM sub",
 /* 189 */ "sub ::= LP union RP",
 /* 190 */ "sub ::= LP union RP ids",
 /* 191 */ "sub ::= sub COMMA LP union RP ids",
 /* 192 */ "tablelist ::= ids cpxName",
 /* 193 */ "tablelist ::= ids cpxName ids",
 /* 194 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 195 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 196 */ "tmvar ::= VARIABLE",
 /* 197 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 198 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 199 */ "interval_option ::=",
 /* 200 */ "intervalKey ::= INTERVAL",
 /* 201 */ "intervalKey ::= EVERY",
 /* 202 */ "session_option ::=",
 /* 203 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 204 */ "windowstate_option ::=",
 /* 205 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 206 */ "fill_opt ::=",
 /* 207 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 208 */ "fill_opt ::= FILL LP ID RP",
 /* 209 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 210 */ "sliding_opt ::=",
 /* 211 */ "orderby_opt ::=",
 /* 212 */ "orderby_opt ::= ORDER BY sortlist",
 /* 213 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 214 */ "sortlist ::= item sortorder",
 /* 215 */ "item ::= ids cpxName",
 /* 216 */ "sortorder ::= ASC",
 /* 217 */ "sortorder ::= DESC",
 /* 218 */ "sortorder ::=",
 /* 219 */ "groupby_opt ::=",
 /* 220 */ "groupby_opt ::= GROUP BY grouplist",
 /* 221 */ "grouplist ::= grouplist COMMA item",
 /* 222 */ "grouplist ::= item",
 /* 223 */ "having_opt ::=",
 /* 224 */ "having_opt ::= HAVING expr",
 /* 225 */ "limit_opt ::=",
 /* 226 */ "limit_opt ::= LIMIT signed",
 /* 227 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 228 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 229 */ "slimit_opt ::=",
 /* 230 */ "slimit_opt ::= SLIMIT signed",
 /* 231 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 232 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 233 */ "where_opt ::=",
 /* 234 */ "where_opt ::= WHERE expr",
 /* 235 */ "expr ::= LP expr RP",
 /* 236 */ "expr ::= ID",
 /* 237 */ "expr ::= ID DOT ID",
 /* 238 */ "expr ::= ID DOT STAR",
 /* 239 */ "expr ::= INTEGER",
 /* 240 */ "expr ::= MINUS INTEGER",
 /* 241 */ "expr ::= PLUS INTEGER",
 /* 242 */ "expr ::= FLOAT",
 /* 243 */ "expr ::= MINUS FLOAT",
 /* 244 */ "expr ::= PLUS FLOAT",
 /* 245 */ "expr ::= STRING",
 /* 246 */ "expr ::= NOW",
 /* 247 */ "expr ::= VARIABLE",
 /* 248 */ "expr ::= PLUS VARIABLE",
 /* 249 */ "expr ::= MINUS VARIABLE",
 /* 250 */ "expr ::= BOOL",
 /* 251 */ "expr ::= NULL",
 /* 252 */ "expr ::= ID LP exprlist RP",
 /* 253 */ "expr ::= ID LP STAR RP",
 /* 254 */ "expr ::= expr IS NULL",
 /* 255 */ "expr ::= expr IS NOT NULL",
 /* 256 */ "expr ::= expr LT expr",
 /* 257 */ "expr ::= expr GT expr",
 /* 258 */ "expr ::= expr LE expr",
 /* 259 */ "expr ::= expr GE expr",
 /* 260 */ "expr ::= expr NE expr",
 /* 261 */ "expr ::= expr EQ expr",
 /* 262 */ "expr ::= expr BETWEEN expr AND expr",
 /* 263 */ "expr ::= expr AND expr",
 /* 264 */ "expr ::= expr OR expr",
 /* 265 */ "expr ::= expr PLUS expr",
 /* 266 */ "expr ::= expr MINUS expr",
 /* 267 */ "expr ::= expr STAR expr",
 /* 268 */ "expr ::= expr SLASH expr",
 /* 269 */ "expr ::= expr REM expr",
 /* 270 */ "expr ::= expr LIKE expr",
 /* 271 */ "expr ::= expr IN LP exprlist RP",
 /* 272 */ "exprlist ::= exprlist COMMA expritem",
 /* 273 */ "exprlist ::= expritem",
 /* 274 */ "expritem ::= expr",
 /* 275 */ "expritem ::=",
 /* 276 */ "cmd ::= RESET QUERY CACHE",
 /* 277 */ "cmd ::= SYNCDB ids REPLICA",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 284 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 285 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 292 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 293 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 294 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 295 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 296 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 208: /* exprlist */
    case 255: /* selcollist */
    case 269: /* sclp */
{
tSqlExprListDestroy((yypminor->yy89));
}
      break;
    case 223: /* intitemlist */
    case 225: /* keep */
    case 246: /* columnlist */
    case 247: /* tagitemlist */
    case 248: /* tagNamelist */
    case 262: /* fill_opt */
    case 263: /* groupby_opt */
    case 265: /* orderby_opt */
    case 277: /* sortlist */
    case 281: /* grouplist */
{
taosArrayDestroy((yypminor->yy89));
}
      break;
    case 244: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy86));
}
      break;
    case 251: /* select */
{
destroySqlNode((yypminor->yy378));
}
      break;
    case 256: /* from */
    case 273: /* tablelist */
    case 274: /* sub */
{
destroyRelationInfo((yypminor->yy166));
}
      break;
    case 257: /* where_opt */
    case 264: /* having_opt */
    case 271: /* expr */
    case 282: /* expritem */
{
tSqlExprDestroy((yypminor->yy342));
}
      break;
    case 268: /* union */
{
destroyAllSqlNode((yypminor->yy89));
}
      break;
    case 278: /* sortitem */
{
tVariantDestroy(&(yypminor->yy112));
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
    /* assert( i+YYNTOKEN<=(int)YY_NLOOKAHEAD ); */
    assert( iLookAhead!=YYNOCODE );
    assert( iLookAhead < YYNTOKEN );
    i += iLookAhead;
    if( i>=YY_NLOOKAHEAD || yy_lookahead[i]!=iLookAhead ){
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
          j<(int)(sizeof(yy_lookahead)/sizeof(yy_lookahead[0])) &&
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

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;       /* Symbol on the left-hand side of the rule */
  signed char nrhs;     /* Negative of the number of RHS symbols in the rule */
} yyRuleInfo[] = {
  {  199,   -1 }, /* (0) program ::= cmd */
  {  200,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  200,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  200,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  200,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  200,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  200,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  200,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  200,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  200,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  200,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  200,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  200,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  200,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  200,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  200,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  200,   -3 }, /* (16) cmd ::= SHOW VNODES IPTOKEN */
  {  201,    0 }, /* (17) dbPrefix ::= */
  {  201,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  203,    0 }, /* (19) cpxName ::= */
  {  203,   -2 }, /* (20) cpxName ::= DOT ids */
  {  200,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  200,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  200,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  200,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  200,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  200,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  200,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  200,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  200,   -5 }, /* (29) cmd ::= DROP TABLE ifexists ids cpxName */
  {  200,   -5 }, /* (30) cmd ::= DROP STABLE ifexists ids cpxName */
  {  200,   -4 }, /* (31) cmd ::= DROP DATABASE ifexists ids */
  {  200,   -4 }, /* (32) cmd ::= DROP TOPIC ifexists ids */
  {  200,   -3 }, /* (33) cmd ::= DROP FUNCTION ids */
  {  200,   -3 }, /* (34) cmd ::= DROP DNODE ids */
  {  200,   -3 }, /* (35) cmd ::= DROP USER ids */
  {  200,   -3 }, /* (36) cmd ::= DROP ACCOUNT ids */
  {  200,   -2 }, /* (37) cmd ::= USE ids */
  {  200,   -3 }, /* (38) cmd ::= DESCRIBE ids cpxName */
  {  200,   -3 }, /* (39) cmd ::= DESC ids cpxName */
  {  200,   -5 }, /* (40) cmd ::= ALTER USER ids PASS ids */
  {  200,   -5 }, /* (41) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  200,   -4 }, /* (42) cmd ::= ALTER DNODE ids ids */
  {  200,   -5 }, /* (43) cmd ::= ALTER DNODE ids ids ids */
  {  200,   -3 }, /* (44) cmd ::= ALTER LOCAL ids */
  {  200,   -4 }, /* (45) cmd ::= ALTER LOCAL ids ids */
  {  200,   -4 }, /* (46) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  200,   -4 }, /* (47) cmd ::= ALTER TOPIC ids alter_topic_optr */
  {  200,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  200,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  200,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  202,   -1 }, /* (51) ids ::= ID */
  {  202,   -1 }, /* (52) ids ::= STRING */
  {  204,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  204,    0 }, /* (54) ifexists ::= */
  {  209,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  209,    0 }, /* (56) ifnotexists ::= */
  {  200,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  200,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  200,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  200,   -5 }, /* (60) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
  {  200,   -8 }, /* (61) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  200,   -9 }, /* (62) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  200,   -5 }, /* (63) cmd ::= CREATE USER ids PASS ids */
  {  213,    0 }, /* (64) bufsize ::= */
  {  213,   -2 }, /* (65) bufsize ::= BUFSIZE INTEGER */
  {  214,    0 }, /* (66) pps ::= */
  {  214,   -2 }, /* (67) pps ::= PPS INTEGER */
  {  215,    0 }, /* (68) tseries ::= */
  {  215,   -2 }, /* (69) tseries ::= TSERIES INTEGER */
  {  216,    0 }, /* (70) dbs ::= */
  {  216,   -2 }, /* (71) dbs ::= DBS INTEGER */
  {  217,    0 }, /* (72) streams ::= */
  {  217,   -2 }, /* (73) streams ::= STREAMS INTEGER */
  {  218,    0 }, /* (74) storage ::= */
  {  218,   -2 }, /* (75) storage ::= STORAGE INTEGER */
  {  219,    0 }, /* (76) qtime ::= */
  {  219,   -2 }, /* (77) qtime ::= QTIME INTEGER */
  {  220,    0 }, /* (78) users ::= */
  {  220,   -2 }, /* (79) users ::= USERS INTEGER */
  {  221,    0 }, /* (80) conns ::= */
  {  221,   -2 }, /* (81) conns ::= CONNS INTEGER */
  {  222,    0 }, /* (82) state ::= */
  {  222,   -2 }, /* (83) state ::= STATE ids */
  {  207,   -9 }, /* (84) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  223,   -3 }, /* (85) intitemlist ::= intitemlist COMMA intitem */
  {  223,   -1 }, /* (86) intitemlist ::= intitem */
  {  224,   -1 }, /* (87) intitem ::= INTEGER */
  {  225,   -2 }, /* (88) keep ::= KEEP intitemlist */
  {  226,   -2 }, /* (89) cache ::= CACHE INTEGER */
  {  227,   -2 }, /* (90) replica ::= REPLICA INTEGER */
  {  228,   -2 }, /* (91) quorum ::= QUORUM INTEGER */
  {  229,   -2 }, /* (92) days ::= DAYS INTEGER */
  {  230,   -2 }, /* (93) minrows ::= MINROWS INTEGER */
  {  231,   -2 }, /* (94) maxrows ::= MAXROWS INTEGER */
  {  232,   -2 }, /* (95) blocks ::= BLOCKS INTEGER */
  {  233,   -2 }, /* (96) ctime ::= CTIME INTEGER */
  {  234,   -2 }, /* (97) wal ::= WAL INTEGER */
  {  235,   -2 }, /* (98) fsync ::= FSYNC INTEGER */
  {  236,   -2 }, /* (99) comp ::= COMP INTEGER */
  {  237,   -2 }, /* (100) prec ::= PRECISION STRING */
  {  238,   -2 }, /* (101) update ::= UPDATE INTEGER */
  {  239,   -2 }, /* (102) cachelast ::= CACHELAST INTEGER */
  {  240,   -2 }, /* (103) partitions ::= PARTITIONS INTEGER */
  {  210,    0 }, /* (104) db_optr ::= */
  {  210,   -2 }, /* (105) db_optr ::= db_optr cache */
  {  210,   -2 }, /* (106) db_optr ::= db_optr replica */
  {  210,   -2 }, /* (107) db_optr ::= db_optr quorum */
  {  210,   -2 }, /* (108) db_optr ::= db_optr days */
  {  210,   -2 }, /* (109) db_optr ::= db_optr minrows */
  {  210,   -2 }, /* (110) db_optr ::= db_optr maxrows */
  {  210,   -2 }, /* (111) db_optr ::= db_optr blocks */
  {  210,   -2 }, /* (112) db_optr ::= db_optr ctime */
  {  210,   -2 }, /* (113) db_optr ::= db_optr wal */
  {  210,   -2 }, /* (114) db_optr ::= db_optr fsync */
  {  210,   -2 }, /* (115) db_optr ::= db_optr comp */
  {  210,   -2 }, /* (116) db_optr ::= db_optr prec */
  {  210,   -2 }, /* (117) db_optr ::= db_optr keep */
  {  210,   -2 }, /* (118) db_optr ::= db_optr update */
  {  210,   -2 }, /* (119) db_optr ::= db_optr cachelast */
  {  211,   -1 }, /* (120) topic_optr ::= db_optr */
  {  211,   -2 }, /* (121) topic_optr ::= topic_optr partitions */
  {  205,    0 }, /* (122) alter_db_optr ::= */
  {  205,   -2 }, /* (123) alter_db_optr ::= alter_db_optr replica */
  {  205,   -2 }, /* (124) alter_db_optr ::= alter_db_optr quorum */
  {  205,   -2 }, /* (125) alter_db_optr ::= alter_db_optr keep */
  {  205,   -2 }, /* (126) alter_db_optr ::= alter_db_optr blocks */
  {  205,   -2 }, /* (127) alter_db_optr ::= alter_db_optr comp */
  {  205,   -2 }, /* (128) alter_db_optr ::= alter_db_optr update */
  {  205,   -2 }, /* (129) alter_db_optr ::= alter_db_optr cachelast */
  {  206,   -1 }, /* (130) alter_topic_optr ::= alter_db_optr */
  {  206,   -2 }, /* (131) alter_topic_optr ::= alter_topic_optr partitions */
  {  212,   -1 }, /* (132) typename ::= ids */
  {  212,   -4 }, /* (133) typename ::= ids LP signed RP */
  {  212,   -2 }, /* (134) typename ::= ids UNSIGNED */
  {  241,   -1 }, /* (135) signed ::= INTEGER */
  {  241,   -2 }, /* (136) signed ::= PLUS INTEGER */
  {  241,   -2 }, /* (137) signed ::= MINUS INTEGER */
  {  200,   -3 }, /* (138) cmd ::= CREATE TABLE create_table_args */
  {  200,   -3 }, /* (139) cmd ::= CREATE TABLE create_stable_args */
  {  200,   -3 }, /* (140) cmd ::= CREATE STABLE create_stable_args */
  {  200,   -3 }, /* (141) cmd ::= CREATE TABLE create_table_list */
  {  244,   -1 }, /* (142) create_table_list ::= create_from_stable */
  {  244,   -2 }, /* (143) create_table_list ::= create_table_list create_from_stable */
  {  242,   -6 }, /* (144) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  243,  -10 }, /* (145) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  245,  -10 }, /* (146) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  245,  -13 }, /* (147) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  248,   -3 }, /* (148) tagNamelist ::= tagNamelist COMMA ids */
  {  248,   -1 }, /* (149) tagNamelist ::= ids */
  {  242,   -7 }, /* (150) create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
  {  249,    0 }, /* (151) to_opt ::= */
  {  249,   -3 }, /* (152) to_opt ::= TO ids cpxName */
  {  250,    0 }, /* (153) split_opt ::= */
  {  250,   -2 }, /* (154) split_opt ::= SPLIT ids */
  {  246,   -3 }, /* (155) columnlist ::= columnlist COMMA column */
  {  246,   -1 }, /* (156) columnlist ::= column */
  {  253,   -2 }, /* (157) column ::= ids typename */
  {  247,   -3 }, /* (158) tagitemlist ::= tagitemlist COMMA tagitem */
  {  247,   -1 }, /* (159) tagitemlist ::= tagitem */
  {  254,   -1 }, /* (160) tagitem ::= INTEGER */
  {  254,   -1 }, /* (161) tagitem ::= FLOAT */
  {  254,   -1 }, /* (162) tagitem ::= STRING */
  {  254,   -1 }, /* (163) tagitem ::= BOOL */
  {  254,   -1 }, /* (164) tagitem ::= NULL */
  {  254,   -1 }, /* (165) tagitem ::= NOW */
  {  254,   -3 }, /* (166) tagitem ::= NOW PLUS VARIABLE */
  {  254,   -3 }, /* (167) tagitem ::= NOW MINUS VARIABLE */
  {  254,   -2 }, /* (168) tagitem ::= MINUS INTEGER */
  {  254,   -2 }, /* (169) tagitem ::= MINUS FLOAT */
  {  254,   -2 }, /* (170) tagitem ::= PLUS INTEGER */
  {  254,   -2 }, /* (171) tagitem ::= PLUS FLOAT */
  {  251,  -14 }, /* (172) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  251,   -3 }, /* (173) select ::= LP select RP */
  {  268,   -1 }, /* (174) union ::= select */
  {  268,   -4 }, /* (175) union ::= union UNION ALL select */
  {  200,   -1 }, /* (176) cmd ::= union */
  {  251,   -2 }, /* (177) select ::= SELECT selcollist */
  {  269,   -2 }, /* (178) sclp ::= selcollist COMMA */
  {  269,    0 }, /* (179) sclp ::= */
  {  255,   -4 }, /* (180) selcollist ::= sclp distinct expr as */
  {  255,   -2 }, /* (181) selcollist ::= sclp STAR */
  {  272,   -2 }, /* (182) as ::= AS ids */
  {  272,   -1 }, /* (183) as ::= ids */
  {  272,    0 }, /* (184) as ::= */
  {  270,   -1 }, /* (185) distinct ::= DISTINCT */
  {  270,    0 }, /* (186) distinct ::= */
  {  256,   -2 }, /* (187) from ::= FROM tablelist */
  {  256,   -2 }, /* (188) from ::= FROM sub */
  {  274,   -3 }, /* (189) sub ::= LP union RP */
  {  274,   -4 }, /* (190) sub ::= LP union RP ids */
  {  274,   -6 }, /* (191) sub ::= sub COMMA LP union RP ids */
  {  273,   -2 }, /* (192) tablelist ::= ids cpxName */
  {  273,   -3 }, /* (193) tablelist ::= ids cpxName ids */
  {  273,   -4 }, /* (194) tablelist ::= tablelist COMMA ids cpxName */
  {  273,   -5 }, /* (195) tablelist ::= tablelist COMMA ids cpxName ids */
  {  275,   -1 }, /* (196) tmvar ::= VARIABLE */
  {  258,   -4 }, /* (197) interval_option ::= intervalKey LP tmvar RP */
  {  258,   -6 }, /* (198) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  258,    0 }, /* (199) interval_option ::= */
  {  276,   -1 }, /* (200) intervalKey ::= INTERVAL */
  {  276,   -1 }, /* (201) intervalKey ::= EVERY */
  {  260,    0 }, /* (202) session_option ::= */
  {  260,   -7 }, /* (203) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  261,    0 }, /* (204) windowstate_option ::= */
  {  261,   -4 }, /* (205) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  262,    0 }, /* (206) fill_opt ::= */
  {  262,   -6 }, /* (207) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  262,   -4 }, /* (208) fill_opt ::= FILL LP ID RP */
  {  259,   -4 }, /* (209) sliding_opt ::= SLIDING LP tmvar RP */
  {  259,    0 }, /* (210) sliding_opt ::= */
  {  265,    0 }, /* (211) orderby_opt ::= */
  {  265,   -3 }, /* (212) orderby_opt ::= ORDER BY sortlist */
  {  277,   -4 }, /* (213) sortlist ::= sortlist COMMA item sortorder */
  {  277,   -2 }, /* (214) sortlist ::= item sortorder */
  {  279,   -2 }, /* (215) item ::= ids cpxName */
  {  280,   -1 }, /* (216) sortorder ::= ASC */
  {  280,   -1 }, /* (217) sortorder ::= DESC */
  {  280,    0 }, /* (218) sortorder ::= */
  {  263,    0 }, /* (219) groupby_opt ::= */
  {  263,   -3 }, /* (220) groupby_opt ::= GROUP BY grouplist */
  {  281,   -3 }, /* (221) grouplist ::= grouplist COMMA item */
  {  281,   -1 }, /* (222) grouplist ::= item */
  {  264,    0 }, /* (223) having_opt ::= */
  {  264,   -2 }, /* (224) having_opt ::= HAVING expr */
  {  267,    0 }, /* (225) limit_opt ::= */
  {  267,   -2 }, /* (226) limit_opt ::= LIMIT signed */
  {  267,   -4 }, /* (227) limit_opt ::= LIMIT signed OFFSET signed */
  {  267,   -4 }, /* (228) limit_opt ::= LIMIT signed COMMA signed */
  {  266,    0 }, /* (229) slimit_opt ::= */
  {  266,   -2 }, /* (230) slimit_opt ::= SLIMIT signed */
  {  266,   -4 }, /* (231) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  266,   -4 }, /* (232) slimit_opt ::= SLIMIT signed COMMA signed */
  {  257,    0 }, /* (233) where_opt ::= */
  {  257,   -2 }, /* (234) where_opt ::= WHERE expr */
  {  271,   -3 }, /* (235) expr ::= LP expr RP */
  {  271,   -1 }, /* (236) expr ::= ID */
  {  271,   -3 }, /* (237) expr ::= ID DOT ID */
  {  271,   -3 }, /* (238) expr ::= ID DOT STAR */
  {  271,   -1 }, /* (239) expr ::= INTEGER */
  {  271,   -2 }, /* (240) expr ::= MINUS INTEGER */
  {  271,   -2 }, /* (241) expr ::= PLUS INTEGER */
  {  271,   -1 }, /* (242) expr ::= FLOAT */
  {  271,   -2 }, /* (243) expr ::= MINUS FLOAT */
  {  271,   -2 }, /* (244) expr ::= PLUS FLOAT */
  {  271,   -1 }, /* (245) expr ::= STRING */
  {  271,   -1 }, /* (246) expr ::= NOW */
  {  271,   -1 }, /* (247) expr ::= VARIABLE */
  {  271,   -2 }, /* (248) expr ::= PLUS VARIABLE */
  {  271,   -2 }, /* (249) expr ::= MINUS VARIABLE */
  {  271,   -1 }, /* (250) expr ::= BOOL */
  {  271,   -1 }, /* (251) expr ::= NULL */
  {  271,   -4 }, /* (252) expr ::= ID LP exprlist RP */
  {  271,   -4 }, /* (253) expr ::= ID LP STAR RP */
  {  271,   -3 }, /* (254) expr ::= expr IS NULL */
  {  271,   -4 }, /* (255) expr ::= expr IS NOT NULL */
  {  271,   -3 }, /* (256) expr ::= expr LT expr */
  {  271,   -3 }, /* (257) expr ::= expr GT expr */
  {  271,   -3 }, /* (258) expr ::= expr LE expr */
  {  271,   -3 }, /* (259) expr ::= expr GE expr */
  {  271,   -3 }, /* (260) expr ::= expr NE expr */
  {  271,   -3 }, /* (261) expr ::= expr EQ expr */
  {  271,   -5 }, /* (262) expr ::= expr BETWEEN expr AND expr */
  {  271,   -3 }, /* (263) expr ::= expr AND expr */
  {  271,   -3 }, /* (264) expr ::= expr OR expr */
  {  271,   -3 }, /* (265) expr ::= expr PLUS expr */
  {  271,   -3 }, /* (266) expr ::= expr MINUS expr */
  {  271,   -3 }, /* (267) expr ::= expr STAR expr */
  {  271,   -3 }, /* (268) expr ::= expr SLASH expr */
  {  271,   -3 }, /* (269) expr ::= expr REM expr */
  {  271,   -3 }, /* (270) expr ::= expr LIKE expr */
  {  271,   -5 }, /* (271) expr ::= expr IN LP exprlist RP */
  {  208,   -3 }, /* (272) exprlist ::= exprlist COMMA expritem */
  {  208,   -1 }, /* (273) exprlist ::= expritem */
  {  282,   -1 }, /* (274) expritem ::= expr */
  {  282,    0 }, /* (275) expritem ::= */
  {  200,   -3 }, /* (276) cmd ::= RESET QUERY CACHE */
  {  200,   -3 }, /* (277) cmd ::= SYNCDB ids REPLICA */
  {  200,   -7 }, /* (278) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (279) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (281) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (282) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (283) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (284) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (285) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -7 }, /* (286) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (287) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (289) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (290) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (291) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (292) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (293) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -3 }, /* (294) cmd ::= KILL CONNECTION INTEGER */
  {  200,   -5 }, /* (295) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  200,   -5 }, /* (296) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 0: /* program ::= cmd */
      case 138: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==138);
      case 139: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==139);
      case 140: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==140);
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
      case 39: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==39);
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
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy470, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy307);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy307);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy89);}
        break;
      case 51: /* ids ::= ID */
      case 52: /* ids ::= STRING */ yytestcase(yyruleno==52);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 53: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 54: /* ifexists ::= */
      case 56: /* ifnotexists ::= */ yytestcase(yyruleno==56);
      case 186: /* distinct ::= */ yytestcase(yyruleno==186);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy307);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 60: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==60);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy470, &yymsp[-2].minor.yy0);}
        break;
      case 61: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy465, &yymsp[0].minor.yy0, 1);}
        break;
      case 62: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy465, &yymsp[0].minor.yy0, 2);}
        break;
      case 63: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 64: /* bufsize ::= */
      case 66: /* pps ::= */ yytestcase(yyruleno==66);
      case 68: /* tseries ::= */ yytestcase(yyruleno==68);
      case 70: /* dbs ::= */ yytestcase(yyruleno==70);
      case 72: /* streams ::= */ yytestcase(yyruleno==72);
      case 74: /* storage ::= */ yytestcase(yyruleno==74);
      case 76: /* qtime ::= */ yytestcase(yyruleno==76);
      case 78: /* users ::= */ yytestcase(yyruleno==78);
      case 80: /* conns ::= */ yytestcase(yyruleno==80);
      case 82: /* state ::= */ yytestcase(yyruleno==82);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 65: /* bufsize ::= BUFSIZE INTEGER */
      case 67: /* pps ::= PPS INTEGER */ yytestcase(yyruleno==67);
      case 69: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==69);
      case 71: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==75);
      case 77: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==77);
      case 79: /* users ::= USERS INTEGER */ yytestcase(yyruleno==79);
      case 81: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==81);
      case 83: /* state ::= STATE ids */ yytestcase(yyruleno==83);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 84: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy307.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy307.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy307.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy307.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy307.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy307.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy307.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy307.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy307.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy307 = yylhsminor.yy307;
        break;
      case 85: /* intitemlist ::= intitemlist COMMA intitem */
      case 158: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==158);
{ yylhsminor.yy89 = tVariantListAppend(yymsp[-2].minor.yy89, &yymsp[0].minor.yy112, -1);    }
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 86: /* intitemlist ::= intitem */
      case 159: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==159);
{ yylhsminor.yy89 = tVariantListAppend(NULL, &yymsp[0].minor.yy112, -1); }
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 87: /* intitem ::= INTEGER */
      case 160: /* tagitem ::= INTEGER */ yytestcase(yyruleno==160);
      case 161: /* tagitem ::= FLOAT */ yytestcase(yyruleno==161);
      case 162: /* tagitem ::= STRING */ yytestcase(yyruleno==162);
      case 163: /* tagitem ::= BOOL */ yytestcase(yyruleno==163);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy112, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 88: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy89 = yymsp[0].minor.yy89; }
        break;
      case 89: /* cache ::= CACHE INTEGER */
      case 90: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==90);
      case 91: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==91);
      case 92: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==95);
      case 96: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==96);
      case 97: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==97);
      case 98: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==98);
      case 99: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==99);
      case 100: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==100);
      case 101: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==101);
      case 102: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==102);
      case 103: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==103);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 104: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy470); yymsp[1].minor.yy470.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 105: /* db_optr ::= db_optr cache */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 106: /* db_optr ::= db_optr replica */
      case 123: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==123);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 107: /* db_optr ::= db_optr quorum */
      case 124: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==124);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 108: /* db_optr ::= db_optr days */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 109: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 110: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 111: /* db_optr ::= db_optr blocks */
      case 126: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==126);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 112: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 113: /* db_optr ::= db_optr wal */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 114: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 115: /* db_optr ::= db_optr comp */
      case 127: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==127);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 116: /* db_optr ::= db_optr prec */
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 117: /* db_optr ::= db_optr keep */
      case 125: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==125);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.keep = yymsp[0].minor.yy89; }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 118: /* db_optr ::= db_optr update */
      case 128: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==128);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 119: /* db_optr ::= db_optr cachelast */
      case 129: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==129);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 120: /* topic_optr ::= db_optr */
      case 130: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==130);
{ yylhsminor.yy470 = yymsp[0].minor.yy470; yylhsminor.yy470.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy470 = yylhsminor.yy470;
        break;
      case 121: /* topic_optr ::= topic_optr partitions */
      case 131: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==131);
{ yylhsminor.yy470 = yymsp[-1].minor.yy470; yylhsminor.yy470.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy470 = yylhsminor.yy470;
        break;
      case 122: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy470); yymsp[1].minor.yy470.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 132: /* typename ::= ids */
{
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy465, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy465 = yylhsminor.yy465;
        break;
      case 133: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy145 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy465, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy145;  // negative value of name length
    tSetColumnType(&yylhsminor.yy465, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy465 = yylhsminor.yy465;
        break;
      case 134: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy465, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy465 = yylhsminor.yy465;
        break;
      case 135: /* signed ::= INTEGER */
{ yylhsminor.yy145 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy145 = yylhsminor.yy145;
        break;
      case 136: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy145 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 137: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy145 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 141: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy86;}
        break;
      case 142: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy506);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy86 = pCreateTable;
}
  yymsp[0].minor.yy86 = yylhsminor.yy86;
        break;
      case 143: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy86->childTableInfo, &yymsp[0].minor.yy506);
  yylhsminor.yy86 = yymsp[-1].minor.yy86;
}
  yymsp[-1].minor.yy86 = yylhsminor.yy86;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy86 = tSetCreateTableInfo(yymsp[-1].minor.yy89, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy86, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy86 = yylhsminor.yy86;
        break;
      case 145: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy86 = tSetCreateTableInfo(yymsp[-5].minor.yy89, yymsp[-1].minor.yy89, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy86, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy86 = yylhsminor.yy86;
        break;
      case 146: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy506 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy89, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy506 = yylhsminor.yy506;
        break;
      case 147: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy506 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy89, yymsp[-1].minor.yy89, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy506 = yylhsminor.yy506;
        break;
      case 148: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy89, &yymsp[0].minor.yy0); yylhsminor.yy89 = yymsp[-2].minor.yy89;  }
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 149: /* tagNamelist ::= ids */
{yylhsminor.yy89 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy89, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 150: /* create_table_args ::= ifnotexists ids cpxName to_opt split_opt AS select */
{
  yylhsminor.yy86 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy378, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy86, NULL, TSDB_SQL_CREATE_TABLE);

  setCreatedStreamOpt(pInfo, &yymsp[-3].minor.yy0, &yymsp[-2].minor.yy0);
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-5].minor.yy0, &yymsp[-6].minor.yy0);
}
  yymsp[-6].minor.yy86 = yylhsminor.yy86;
        break;
      case 151: /* to_opt ::= */
      case 153: /* split_opt ::= */ yytestcase(yyruleno==153);
{yymsp[1].minor.yy0.n = 0;}
        break;
      case 152: /* to_opt ::= TO ids cpxName */
{
   yymsp[-2].minor.yy0 = yymsp[-1].minor.yy0;
   yymsp[-2].minor.yy0.n += yymsp[0].minor.yy0.n;
}
        break;
      case 154: /* split_opt ::= SPLIT ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 155: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy89, &yymsp[0].minor.yy465); yylhsminor.yy89 = yymsp[-2].minor.yy89;  }
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 156: /* columnlist ::= column */
{yylhsminor.yy89 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy89, &yymsp[0].minor.yy465);}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 157: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy465, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy465);
}
  yymsp[-1].minor.yy465 = yylhsminor.yy465;
        break;
      case 164: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy112, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 165: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreateExt(&yylhsminor.yy112, &yymsp[0].minor.yy0, TK_NOW);}
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 166: /* tagitem ::= NOW PLUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy112, &yymsp[0].minor.yy0, TK_PLUS);
}
        break;
      case 167: /* tagitem ::= NOW MINUS VARIABLE */
{
    yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP;
    tVariantCreateExt(&yymsp[-2].minor.yy112, &yymsp[0].minor.yy0, TK_MINUS);
}
        break;
      case 168: /* tagitem ::= MINUS INTEGER */
      case 169: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==169);
      case 170: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==170);
      case 171: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==171);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy112, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy112 = yylhsminor.yy112;
        break;
      case 172: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy378 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy89, yymsp[-11].minor.yy166, yymsp[-10].minor.yy342, yymsp[-4].minor.yy89, yymsp[-2].minor.yy89, &yymsp[-9].minor.yy66, &yymsp[-7].minor.yy431, &yymsp[-6].minor.yy392, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy89, &yymsp[0].minor.yy372, &yymsp[-1].minor.yy372, yymsp[-3].minor.yy342);
}
  yymsp[-13].minor.yy378 = yylhsminor.yy378;
        break;
      case 173: /* select ::= LP select RP */
{yymsp[-2].minor.yy378 = yymsp[-1].minor.yy378;}
        break;
      case 174: /* union ::= select */
{ yylhsminor.yy89 = setSubclause(NULL, yymsp[0].minor.yy378); }
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 175: /* union ::= union UNION ALL select */
{ yylhsminor.yy89 = appendSelectClause(yymsp[-3].minor.yy89, yymsp[0].minor.yy378); }
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 176: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy89, NULL, TSDB_SQL_SELECT); }
        break;
      case 177: /* select ::= SELECT selcollist */
{
  yylhsminor.yy378 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy89, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 178: /* sclp ::= selcollist COMMA */
{yylhsminor.yy89 = yymsp[-1].minor.yy89;}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 179: /* sclp ::= */
      case 211: /* orderby_opt ::= */ yytestcase(yyruleno==211);
{yymsp[1].minor.yy89 = 0;}
        break;
      case 180: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy89 = tSqlExprListAppend(yymsp[-3].minor.yy89, yymsp[-1].minor.yy342,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 181: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(pInfo, NULL, TK_ALL);
   yylhsminor.yy89 = tSqlExprListAppend(yymsp[-1].minor.yy89, pNode, 0, 0);
}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 182: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 183: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 184: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 185: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 187: /* from ::= FROM tablelist */
      case 188: /* from ::= FROM sub */ yytestcase(yyruleno==188);
{yymsp[-1].minor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 189: /* sub ::= LP union RP */
{yymsp[-2].minor.yy166 = addSubqueryElem(NULL, yymsp[-1].minor.yy89, NULL);}
        break;
      case 190: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy166 = addSubqueryElem(NULL, yymsp[-2].minor.yy89, &yymsp[0].minor.yy0);}
        break;
      case 191: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy166 = addSubqueryElem(yymsp[-5].minor.yy166, yymsp[-2].minor.yy89, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy166 = yylhsminor.yy166;
        break;
      case 192: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 193: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 194: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(yymsp[-3].minor.yy166, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 195: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(yymsp[-4].minor.yy166, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 196: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 197: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy66.interval = yymsp[-1].minor.yy0; yylhsminor.yy66.offset.n = 0; yylhsminor.yy66.token = yymsp[-3].minor.yy130;}
  yymsp[-3].minor.yy66 = yylhsminor.yy66;
        break;
      case 198: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy66.interval = yymsp[-3].minor.yy0; yylhsminor.yy66.offset = yymsp[-1].minor.yy0;   yylhsminor.yy66.token = yymsp[-5].minor.yy130;}
  yymsp[-5].minor.yy66 = yylhsminor.yy66;
        break;
      case 199: /* interval_option ::= */
{memset(&yymsp[1].minor.yy66, 0, sizeof(yymsp[1].minor.yy66));}
        break;
      case 200: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy130 = TK_INTERVAL;}
        break;
      case 201: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy130 = TK_EVERY;   }
        break;
      case 202: /* session_option ::= */
{yymsp[1].minor.yy431.col.n = 0; yymsp[1].minor.yy431.gap.n = 0;}
        break;
      case 203: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy431.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy431.gap = yymsp[-1].minor.yy0;
}
        break;
      case 204: /* windowstate_option ::= */
{ yymsp[1].minor.yy392.col.n = 0; yymsp[1].minor.yy392.col.z = NULL;}
        break;
      case 205: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy392.col = yymsp[-1].minor.yy0; }
        break;
      case 206: /* fill_opt ::= */
{ yymsp[1].minor.yy89 = 0;     }
        break;
      case 207: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy89, &A, -1, 0);
    yymsp[-5].minor.yy89 = yymsp[-1].minor.yy89;
}
        break;
      case 208: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy89 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 209: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 210: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 212: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy89 = yymsp[0].minor.yy89;}
        break;
      case 213: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy89 = tVariantListAppend(yymsp[-3].minor.yy89, &yymsp[-1].minor.yy112, yymsp[0].minor.yy346);
}
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 214: /* sortlist ::= item sortorder */
{
  yylhsminor.yy89 = tVariantListAppend(NULL, &yymsp[-1].minor.yy112, yymsp[0].minor.yy346);
}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 215: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy112, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy112 = yylhsminor.yy112;
        break;
      case 216: /* sortorder ::= ASC */
{ yymsp[0].minor.yy346 = TSDB_ORDER_ASC; }
        break;
      case 217: /* sortorder ::= DESC */
{ yymsp[0].minor.yy346 = TSDB_ORDER_DESC;}
        break;
      case 218: /* sortorder ::= */
{ yymsp[1].minor.yy346 = TSDB_ORDER_ASC; }
        break;
      case 219: /* groupby_opt ::= */
{ yymsp[1].minor.yy89 = 0;}
        break;
      case 220: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy89 = yymsp[0].minor.yy89;}
        break;
      case 221: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy89 = tVariantListAppend(yymsp[-2].minor.yy89, &yymsp[0].minor.yy112, -1);
}
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 222: /* grouplist ::= item */
{
  yylhsminor.yy89 = tVariantListAppend(NULL, &yymsp[0].minor.yy112, -1);
}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 223: /* having_opt ::= */
      case 233: /* where_opt ::= */ yytestcase(yyruleno==233);
      case 275: /* expritem ::= */ yytestcase(yyruleno==275);
{yymsp[1].minor.yy342 = 0;}
        break;
      case 224: /* having_opt ::= HAVING expr */
      case 234: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==234);
{yymsp[-1].minor.yy342 = yymsp[0].minor.yy342;}
        break;
      case 225: /* limit_opt ::= */
      case 229: /* slimit_opt ::= */ yytestcase(yyruleno==229);
{yymsp[1].minor.yy372.limit = -1; yymsp[1].minor.yy372.offset = 0;}
        break;
      case 226: /* limit_opt ::= LIMIT signed */
      case 230: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==230);
{yymsp[-1].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-1].minor.yy372.offset = 0;}
        break;
      case 227: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy372.limit = yymsp[-2].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[0].minor.yy145;}
        break;
      case 228: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[-2].minor.yy145;}
        break;
      case 231: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy372.limit = yymsp[-2].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[0].minor.yy145;}
        break;
      case 232: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[-2].minor.yy145;}
        break;
      case 235: /* expr ::= LP expr RP */
{yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy342->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 236: /* expr ::= ID */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 237: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 238: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 239: /* expr ::= INTEGER */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 240: /* expr ::= MINUS INTEGER */
      case 241: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==241);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 242: /* expr ::= FLOAT */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 243: /* expr ::= MINUS FLOAT */
      case 244: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==244);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 245: /* expr ::= STRING */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 246: /* expr ::= NOW */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 247: /* expr ::= VARIABLE */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 248: /* expr ::= PLUS VARIABLE */
      case 249: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==249);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 250: /* expr ::= BOOL */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 251: /* expr ::= NULL */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(pInfo, &yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 252: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy342 = tSqlExprCreateFunction(yymsp[-1].minor.yy89, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 253: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy342 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 254: /* expr ::= expr IS NULL */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 255: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-3].minor.yy342, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 256: /* expr ::= expr LT expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LT);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 257: /* expr ::= expr GT expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_GT);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 258: /* expr ::= expr LE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 259: /* expr ::= expr GE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_GE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 260: /* expr ::= expr NE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_NE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 261: /* expr ::= expr EQ expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_EQ);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 262: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy342); yylhsminor.yy342 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy342, yymsp[-2].minor.yy342, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy342, TK_LE), TK_AND);}
  yymsp[-4].minor.yy342 = yylhsminor.yy342;
        break;
      case 263: /* expr ::= expr AND expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_AND);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 264: /* expr ::= expr OR expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_OR); }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 265: /* expr ::= expr PLUS expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_PLUS);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 266: /* expr ::= expr MINUS expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_MINUS); }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 267: /* expr ::= expr STAR expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_STAR);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 268: /* expr ::= expr SLASH expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_DIVIDE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 269: /* expr ::= expr REM expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_REM);   }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 270: /* expr ::= expr LIKE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LIKE);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 271: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-4].minor.yy342, (tSqlExpr*)yymsp[-1].minor.yy89, TK_IN); }
  yymsp[-4].minor.yy342 = yylhsminor.yy342;
        break;
      case 272: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy89 = tSqlExprListAppend(yymsp[-2].minor.yy89,yymsp[0].minor.yy342,0, 0);}
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 273: /* exprlist ::= expritem */
{yylhsminor.yy89 = tSqlExprListAppend(0,yymsp[0].minor.yy342,0, 0);}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 274: /* expritem ::= expr */
{yylhsminor.yy342 = yymsp[0].minor.yy342;}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 276: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 277: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 284: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy112, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 290: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 292: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy112, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 293: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 294: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 295: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 296: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
