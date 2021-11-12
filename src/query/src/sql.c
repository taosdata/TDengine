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
#define YYNSTATE             368
#define YYNRULE              295
#define YYNTOKEN             199
#define YY_MAX_SHIFT         367
#define YY_MIN_SHIFTREDUCE   578
#define YY_MAX_SHIFTREDUCE   872
#define YY_ERROR_ACTION      873
#define YY_ACCEPT_ACTION     874
#define YY_NO_ACTION         875
#define YY_MIN_REDUCE        876
#define YY_MAX_REDUCE        1170
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
#define YY_ACTTAB_COUNT (764)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   250,  629,  629,  629,  233,  365,   23,  249,  254,  630,
 /*    10 */   630,  630,  239,   57,   58,  665,   61,   62, 1057, 1035,
 /*    20 */   253,   51,  629,   60,  323,   65,   63,   66,   64, 1001,
 /*    30 */   630,  999, 1000,   56,   55,  209, 1002,   54,   53,   52,
 /*    40 */  1003,  210, 1004, 1005,  164,  874,  367,  579,  580,  581,
 /*    50 */   582,  583,  584,  585,  586,  587,  588,  589,  590,  591,
 /*    60 */   592,  366,  325,  212,  234,   57,   58,   80,   61,   62,
 /*    70 */   212,  212,  253,   51, 1147,   60,  323,   65,   63,   66,
 /*    80 */    64, 1147, 1147,  212,   29,   56,   55,   83, 1054,   54,
 /*    90 */    53,   52,   57,   58, 1146,   61,   62, 1048, 1048,  253,
 /*   100 */    51, 1016,   60,  323,   65,   63,   66,   64,   54,   53,
 /*   110 */    52,   89,   56,   55,  275,  236,   54,   53,   52,   57,
 /*   120 */    59, 1095,   61,   62,  321, 1048,  253,   51,   98,   60,
 /*   130 */   323,   65,   63,   66,   64,  817,  811,  820,  164,   56,
 /*   140 */    55,  164,  237,   54,   53,   52,   58,  245,   61,   62,
 /*   150 */    45,  757,  253,   51, 1035,   60,  323,   65,   63,   66,
 /*   160 */    64, 1014, 1015,   35, 1018,   56,   55,  830,  164,   54,
 /*   170 */    53,   52,   44,  319,  360,  359,  318,  317,  316,  358,
 /*   180 */   315,  314,  313,  357,  312,  356,  355,  993,  981,  982,
 /*   190 */   983,  984,  985,  986,  987,  988,  989,  990,  991,  992,
 /*   200 */   994,  995,   61,   62,   24,   38,  253,   51,   86,   60,
 /*   210 */   323,   65,   63,   66,   64, 1096,  296,  294,   94,   56,
 /*   220 */    55,  247,  215,   54,   53,   52,  252,  826, 1035,  221,
 /*   230 */   815,  157,  818,  173,  821,  140,  139,  138,  220,  761,
 /*   240 */   252,  826,  328,   89,  815,   95,  818,  278,  821,  235,
 /*   250 */    38,  284,   65,   63,   66,   64, 1032,  824,  231,  232,
 /*   260 */    56,   55,  324,   38,   54,   53,   52,    5,   41,  183,
 /*   270 */  1017,  256,  231,  232,  182,  107,  112,  103,  111,  101,
 /*   280 */   741,  713,   45,  738,   38,  739,  816,  740,  819,  124,
 /*   290 */   118,  129,  261,  308,  243,  262,  128,   34,  134,  137,
 /*   300 */   127, 1032,  282,  281,  274,  179,   79,  131,   67,  343,
 /*   310 */   342,  258,  259,  228, 1031, 1019,  203,  201,  199, 1025,
 /*   320 */    38,  216,   67,  198,  144,  143,  142,  141,  244,   44,
 /*   330 */   303,  360,  359,   56,   55, 1032,  358,   54,   53,   52,
 /*   340 */   357,   38,  356,  355,   38,  827,  822,  262,   14,  257,
 /*   350 */   126,  255,  823,  331,  330,  321,   38,  180,   38,  827,
 /*   360 */   822,   38,  353,  262,  332,  246,  823,   38,  793,   38,
 /*   370 */   263, 1032,  260, 1033,  338,  337,  353,  267,  364,  363,
 /*   380 */   606, 1143,  100,   81, 1142,  333,  271,  270,  334,  154,
 /*   390 */   152,  151, 1032,   71,  924, 1032,  776,  777,  742,  743,
 /*   400 */   335,  193,  339,  934,  754,  340,   74, 1032,  825, 1032,
 /*   410 */   193,  341, 1032,  345,  925,  361,  962,   96, 1032,  276,
 /*   420 */  1032,  193,    1,  181,    3,  194,   87,  792,  773,  783,
 /*   430 */   784,   84,    9,   39,  813,  723,  300,   72,  725,  302,
 /*   440 */   724,  847,  159,   68,   26,  828,  628,  251,   75,   39,
 /*   450 */    39,   78,   68,   99,   68,   25,  746, 1034,  747,   25,
 /*   460 */    25,   16,  117,   15,  116,  278,   18,   48,   17,    6,
 /*   470 */   814,  744,   20,  745,   19,  123,   22,  122,   21,  136,
 /*   480 */   135, 1141,  229,  712,  230, 1106,  213,  214,  217,  211,
 /*   490 */  1166,  218,  219,  223, 1158,  224, 1105,  241, 1102,  225,
 /*   500 */   222, 1101,  208,  242,  344,  272,  155,  175,  174,  156,
 /*   510 */  1049, 1030, 1056, 1067, 1088,  279,  153, 1064, 1065, 1069,
 /*   520 */  1026,  158,  163,  290,  176, 1087,  277, 1024, 1046,  283,
 /*   530 */   772,  177,  178,  939,  305,  306,  307,  168,  310,  311,
 /*   540 */    46,  206,   42,  322,  297,  933,   76,  238,  285,  329,
 /*   550 */  1165,  114, 1164,   73,  287, 1161,  165,  166,   50,  295,
 /*   560 */   184,  336, 1157,  167,  293,  120, 1156, 1153,  185,  959,
 /*   570 */   291,   43,   40,   47,  207,  921,  130,  919,  132,  133,
 /*   580 */   917,  916,  264,  196,  197,  913,  912,  911,  289,  910,
 /*   590 */   909,  908,  907,  200,  202,  903,  901,  899,  204,  896,
 /*   600 */   205,  286, 1028,   49,   85,   90,   82,  288, 1089,  309,
 /*   610 */   354,  125,  346,  347,  348,  349,  350,  351,   77,  248,
 /*   620 */   304,  352,  362,  872,  266,  265,  226,  871,  268,  227,
 /*   630 */   269,  938,  937,  108,  109,  145,  870,  853,  852,  273,
 /*   640 */   278,  299,   10,   30,  915,  914,  188,  146,  187,  960,
 /*   650 */   186,  906,  190,  189,  191,  192,  147,  905,    4,    2,
 /*   660 */   148,  997,  961,   88,  298,  898,   33,  897,  169,  170,
 /*   670 */   749,  171,  172,  280,   91,  774,  160, 1007,  785,  161,
 /*   680 */   162,  779,   92,  240,  781,   93,  292,   31,   11,   12,
 /*   690 */    32,   97,   13,   27,  301,   28,  100,  105,  104,  102,
 /*   700 */   643,   36,   37,  106,  676,  678,  675,  674,  672,  671,
 /*   710 */   670,  667,  320,  633,  110,    7,  326,  829,  831,    8,
 /*   720 */   327,  113,  115,   69,   70,  715,   39,  119,  714,  711,
 /*   730 */   121,  659,  657,  649,  655,  651,  653,  647,  645,  681,
 /*   740 */   680,  679,  677,  673,  669,  668,  195,  596,  631,  594,
 /*   750 */   876,  875,  875,  875,  875,  875,  875,  875,  875,  875,
 /*   760 */   875,  875,  149,  150,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   208,    1,    1,    1,  201,  202,  271,  208,  208,    9,
 /*    10 */     9,    9,  247,   13,   14,    5,   16,   17,  202,  254,
 /*    20 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  225,
 /*    30 */     9,  227,  228,   33,   34,  271,  232,   37,   38,   39,
 /*    40 */   236,  271,  238,  239,  202,  199,  200,   45,   46,   47,
 /*    50 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*    60 */    58,   59,   15,  271,   62,   13,   14,  209,   16,   17,
 /*    70 */   271,  271,   20,   21,  282,   23,   24,   25,   26,   27,
 /*    80 */    28,  282,  282,  271,   83,   33,   34,   87,  272,   37,
 /*    90 */    38,   39,   13,   14,  282,   16,   17,  251,  251,   20,
 /*   100 */    21,  243,   23,   24,   25,   26,   27,   28,   37,   38,
 /*   110 */    39,   83,   33,   34,  268,  268,   37,   38,   39,   13,
 /*   120 */    14,  279,   16,   17,   85,  251,   20,   21,  209,   23,
 /*   130 */    24,   25,   26,   27,   28,    5,   84,    7,  202,   33,
 /*   140 */    34,  202,  268,   37,   38,   39,   14,  247,   16,   17,
 /*   150 */   122,   37,   20,   21,  254,   23,   24,   25,   26,   27,
 /*   160 */    28,  242,  243,  244,  245,   33,   34,  120,  202,   37,
 /*   170 */    38,   39,   99,  100,  101,  102,  103,  104,  105,  106,
 /*   180 */   107,  108,  109,  110,  111,  112,  113,  225,  226,  227,
 /*   190 */   228,  229,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   200 */   238,  239,   16,   17,   44,  202,   20,   21,   84,   23,
 /*   210 */    24,   25,   26,   27,   28,  279,  277,  281,  279,   33,
 /*   220 */    34,  247,   62,   37,   38,   39,    1,    2,  254,   69,
 /*   230 */     5,  202,    7,  258,    9,   75,   76,   77,   78,  125,
 /*   240 */     1,    2,   82,   83,    5,  279,    7,  123,    9,  246,
 /*   250 */   202,  276,   25,   26,   27,   28,  253,  127,   33,   34,
 /*   260 */    33,   34,   37,  202,   37,   38,   39,   63,   64,   65,
 /*   270 */     0,   69,   33,   34,   70,   71,   72,   73,   74,  209,
 /*   280 */     2,    5,  122,    5,  202,    7,    5,    9,    7,   63,
 /*   290 */    64,   65,   69,   89,  246,  202,   70,   83,   72,   73,
 /*   300 */    74,  253,  273,  274,  144,  212,  146,   81,   83,   33,
 /*   310 */    34,   33,   34,  153,  253,  245,   63,   64,   65,  202,
 /*   320 */   202,  271,   83,   70,   71,   72,   73,   74,  246,   99,
 /*   330 */   116,  101,  102,   33,   34,  253,  106,   37,   38,   39,
 /*   340 */   110,  202,  112,  113,  202,  120,  121,  202,   83,  147,
 /*   350 */    79,  149,  127,  151,  152,   85,  202,  212,  202,  120,
 /*   360 */   121,  202,   91,  202,  246,  248,  127,  202,   77,  202,
 /*   370 */   147,  253,  149,  212,  151,  152,   91,  145,   66,   67,
 /*   380 */    68,  271,  117,  118,  271,  246,  154,  155,  246,   63,
 /*   390 */    64,   65,  253,   98,  207,  253,  128,  129,  120,  121,
 /*   400 */   246,  214,  246,  207,   98,  246,   98,  253,  127,  253,
 /*   410 */   214,  246,  253,  246,  207,  223,  224,  255,  253,   84,
 /*   420 */   253,  214,  210,  211,  205,  206,   84,  136,   84,   84,
 /*   430 */    84,  269,  126,   98,    1,   84,   84,  142,   84,   84,
 /*   440 */    84,   84,   98,   98,   98,   84,   84,   61,  140,   98,
 /*   450 */    98,   83,   98,   98,   98,   98,    5,  254,    7,   98,
 /*   460 */    98,  148,  148,  150,  150,  123,  148,  270,  150,   83,
 /*   470 */    37,    5,  148,    7,  150,  148,  148,  150,  150,   79,
 /*   480 */    80,  271,  271,  115,  271,  241,  271,  271,  271,  271,
 /*   490 */   254,  271,  271,  271,  254,  271,  241,  241,  241,  271,
 /*   500 */   271,  241,  271,  241,  241,  202,  202,  249,  256,  202,
 /*   510 */   251,  202,  202,  202,  280,  251,   61,  202,  202,  202,
 /*   520 */   251,  202,  202,  202,  202,  280,  203,  202,  267,  275,
 /*   530 */   127,  202,  202,  202,  202,  202,  202,  263,  202,  202,
 /*   540 */   202,  202,  202,  202,  134,  202,  139,  275,  275,  202,
 /*   550 */   202,  202,  202,  141,  275,  202,  266,  265,  138,  137,
 /*   560 */   202,  202,  202,  264,  132,  202,  202,  202,  202,  202,
 /*   570 */   131,  202,  202,  202,  202,  202,  202,  202,  202,  202,
 /*   580 */   202,  202,  202,  202,  202,  202,  202,  202,  130,  202,
 /*   590 */   202,  202,  202,  202,  202,  202,  202,  202,  202,  202,
 /*   600 */   202,  133,  203,  143,  203,  203,  119,  203,  203,   90,
 /*   610 */   114,   97,   96,   51,   93,   95,   55,   94,  203,  203,
 /*   620 */   203,   92,   85,    5,    5,  156,  203,    5,  156,  203,
 /*   630 */     5,  213,  213,  209,  209,  204,    5,  101,  100,  145,
 /*   640 */   123,  116,   83,   83,  203,  203,  216,  204,  220,  222,
 /*   650 */   221,  203,  217,  219,  218,  215,  204,  203,  205,  210,
 /*   660 */   204,  240,  224,  124,  250,  203,  257,  203,  262,  261,
 /*   670 */    84,  260,  259,   98,   98,   84,   83,  240,   84,   83,
 /*   680 */    98,   84,   83,    1,   84,   83,   83,   98,  135,  135,
 /*   690 */    98,   87,   83,   83,  116,   83,  117,   71,   87,   79,
 /*   700 */     5,   88,   88,   87,    5,    9,    5,    5,    5,    5,
 /*   710 */     5,    5,   15,   86,   79,   83,   24,   84,  120,   83,
 /*   720 */    59,  150,  150,   16,   16,    5,   98,  150,    5,   84,
 /*   730 */   150,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */     5,    5,    5,    5,    5,    5,   98,   61,   86,   60,
 /*   750 */     0,  283,  283,  283,  283,  283,  283,  283,  283,  283,
 /*   760 */   283,  283,   21,   21,  283,  283,  283,  283,  283,  283,
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
 /*   960 */   283,  283,  283,
};
#define YY_SHIFT_COUNT    (367)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (750)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   160,   73,   73,  230,  230,   39,  225,  239,  239,    1,
 /*    10 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*    20 */    21,   21,   21,    0,    2,  239,  278,  278,  278,   28,
 /*    30 */    28,   21,   21,  268,   21,  270,   21,   21,   21,   21,
 /*    40 */   271,   39,  285,  285,   10,  764,  764,  764,  239,  239,
 /*    50 */   239,  239,  239,  239,  239,  239,  239,  239,  239,  239,
 /*    60 */   239,  239,  239,  239,  239,  239,  239,  239,  278,  278,
 /*    70 */   278,  276,  276,  276,  276,  276,  276,  265,  276,   21,
 /*    80 */    21,   21,   21,   21,  114,   21,   21,   21,   28,   28,
 /*    90 */    21,   21,   21,   21,  291,  291,  306,   28,   21,   21,
 /*   100 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   110 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   120 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   130 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   140 */    21,   21,   21,   21,   21,   21,   21,   21,   21,   21,
 /*   150 */    21,   21,   21,   21,   21,  455,  455,  455,  455,  403,
 /*   160 */   403,  403,  403,  455,  455,  407,  412,  410,  420,  422,
 /*   170 */   432,  439,  458,  468,  460,  487,  455,  455,  455,  519,
 /*   180 */   519,  496,   39,   39,  455,  455,  514,  516,  562,  521,
 /*   190 */   520,  561,  523,  529,  496,   10,  455,  455,  537,  537,
 /*   200 */   455,  537,  455,  537,  455,  455,  764,  764,   52,   79,
 /*   210 */    79,  106,   79,  132,  186,  204,  227,  227,  227,  227,
 /*   220 */   226,  253,  300,  300,  300,  300,  202,  223,  232,   71,
 /*   230 */    71,  130,  281,  312,  326,  335,  124,  342,  344,  345,
 /*   240 */   346,  295,  308,  351,  352,  354,  355,  356,  214,  357,
 /*   250 */   361,  433,  386,   47,  362,  313,  314,  318,  451,  466,
 /*   260 */   324,  327,  368,  328,  400,  618,  469,  619,  622,  472,
 /*   270 */   625,  631,  536,  538,  494,  517,  525,  559,  539,  586,
 /*   280 */   560,  575,  576,  591,  593,  594,  596,  597,  582,  599,
 /*   290 */   600,  602,  682,  603,  589,  553,  592,  554,  604,  609,
 /*   300 */   525,  610,  578,  612,  579,  620,  613,  611,  626,  695,
 /*   310 */   614,  616,  696,  699,  701,  702,  703,  704,  705,  706,
 /*   320 */   627,  697,  635,  632,  633,  598,  636,  692,  661,  707,
 /*   330 */   571,  572,  628,  628,  628,  628,  708,  577,  580,  628,
 /*   340 */   628,  628,  720,  723,  645,  628,  726,  727,  728,  729,
 /*   350 */   730,  731,  732,  733,  734,  735,  736,  737,  738,  739,
 /*   360 */   740,  648,  662,  741,  742,  686,  689,  750,
};
#define YY_REDUCE_COUNT (207)
#define YY_REDUCE_MIN   (-265)
#define YY_REDUCE_MAX   (464)
static const short yy_reduce_ofst[] = {
 /*     0 */  -154,  -38,  -38, -196, -196,  -81, -208, -201, -200,   29,
 /*    10 */     3,  -64,  -61,   48,   82,  118,  139,  142,  154,  156,
 /*    20 */   159,  165,  167, -184, -197, -188, -235, -100,  -26, -153,
 /*    30 */  -126, -158,  -34,  -25,  117,   70,   93,  145,  161,   61,
 /*    40 */   187, -142,  196,  207,  192,  162,  212,  219, -265, -236,
 /*    50 */  -230,   50,  110,  113,  210,  211,  213,  215,  216,  217,
 /*    60 */   218,  220,  221,  222,  224,  228,  229,  231,  203,  236,
 /*    70 */   240,  244,  255,  256,  257,  260,  262,  258,  263,  303,
 /*    80 */   304,  307,  309,  310,  197,  311,  315,  316,  259,  264,
 /*    90 */   317,  319,  320,  321,  234,  245,  252,  269,  322,  325,
 /*   100 */   329,  330,  331,  332,  333,  334,  336,  337,  338,  339,
 /*   110 */   340,  341,  343,  347,  348,  349,  350,  353,  358,  359,
 /*   120 */   360,  363,  364,  365,  366,  367,  369,  370,  371,  372,
 /*   130 */   373,  374,  375,  376,  377,  378,  379,  380,  381,  382,
 /*   140 */   383,  384,  385,  387,  388,  389,  390,  391,  392,  393,
 /*   150 */   394,  395,  396,  397,  398,  323,  399,  401,  402,  254,
 /*   160 */   272,  273,  279,  404,  405,  261,  290,  292,  299,  274,
 /*   170 */   406,  408,  411,  413,  409,  414,  415,  416,  417,  418,
 /*   180 */   419,  421,  424,  425,  423,  426,  427,  429,  428,  430,
 /*   190 */   434,  435,  436,  440,  437,  438,  441,  442,  431,  443,
 /*   200 */   448,  452,  454,  456,  462,  464,  449,  453,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   873,  996,  935, 1006,  922,  932, 1149, 1149, 1149,  873,
 /*    10 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*    20 */   873,  873,  873, 1058,  893, 1149,  873,  873,  873,  873,
 /*    30 */   873,  873,  873, 1073,  873,  932,  873,  873,  873,  873,
 /*    40 */   942,  932,  942,  942,  873, 1053,  980,  998,  873,  873,
 /*    50 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*    60 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*    70 */   873,  873,  873,  873,  873,  873,  873, 1027,  873,  873,
 /*    80 */   873,  873,  873,  873, 1060, 1066, 1063,  873,  873,  873,
 /*    90 */  1068,  873,  873,  873, 1092, 1092, 1051,  873,  873,  873,
 /*   100 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   110 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   120 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   130 */   920,  873,  918,  873,  873,  873,  873,  873,  873,  873,
 /*   140 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   150 */   873,  873,  873,  873,  873,  895,  895,  895,  895,  873,
 /*   160 */   873,  873,  873,  895,  895, 1099, 1103, 1085, 1097, 1093,
 /*   170 */  1080, 1078, 1076, 1084, 1107, 1029,  895,  895,  895,  940,
 /*   180 */   940,  936,  932,  932,  895,  895,  958,  956,  954,  946,
 /*   190 */   952,  948,  950,  944,  923,  873,  895,  895,  930,  930,
 /*   200 */   895,  930,  895,  930,  895,  895,  980,  998,  873, 1108,
 /*   210 */  1098,  873, 1148, 1138, 1137,  873, 1144, 1136, 1135, 1134,
 /*   220 */   873,  873, 1130, 1133, 1132, 1131,  873,  873,  873, 1140,
 /*   230 */  1139,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   240 */   873, 1104, 1100,  873,  873,  873,  873,  873,  873,  873,
 /*   250 */   873,  873, 1110,  873,  873,  873,  873,  873,  873,  873,
 /*   260 */   873,  873, 1008,  873,  873,  873,  873,  873,  873,  873,
 /*   270 */   873,  873,  873,  873,  873, 1050,  873,  873,  873,  873,
 /*   280 */   873, 1062, 1061,  873,  873,  873,  873,  873,  873,  873,
 /*   290 */   873,  873,  873,  873, 1094,  873, 1086,  873,  873,  873,
 /*   300 */  1020,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   310 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   320 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   330 */   873,  873, 1167, 1162, 1163, 1160,  873,  873,  873, 1159,
 /*   340 */  1154, 1155,  873,  873,  873, 1152,  873,  873,  873,  873,
 /*   350 */   873,  873,  873,  873,  873,  873,  873,  873,  873,  873,
 /*   360 */   873,  964,  873,  902,  900,  873,  891,  873,
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
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*   DISTINCT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
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
  /*  122 */ "SELECT",
  /*  123 */ "UNION",
  /*  124 */ "ALL",
  /*  125 */ "DISTINCT",
  /*  126 */ "FROM",
  /*  127 */ "VARIABLE",
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
 /* 166 */ "tagitem ::= MINUS INTEGER",
 /* 167 */ "tagitem ::= MINUS FLOAT",
 /* 168 */ "tagitem ::= PLUS INTEGER",
 /* 169 */ "tagitem ::= PLUS FLOAT",
 /* 170 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 171 */ "select ::= LP select RP",
 /* 172 */ "union ::= select",
 /* 173 */ "union ::= union UNION ALL select",
 /* 174 */ "cmd ::= union",
 /* 175 */ "select ::= SELECT selcollist",
 /* 176 */ "sclp ::= selcollist COMMA",
 /* 177 */ "sclp ::=",
 /* 178 */ "selcollist ::= sclp distinct expr as",
 /* 179 */ "selcollist ::= sclp STAR",
 /* 180 */ "as ::= AS ids",
 /* 181 */ "as ::= ids",
 /* 182 */ "as ::=",
 /* 183 */ "distinct ::= DISTINCT",
 /* 184 */ "distinct ::=",
 /* 185 */ "from ::= FROM tablelist",
 /* 186 */ "from ::= FROM sub",
 /* 187 */ "sub ::= LP union RP",
 /* 188 */ "sub ::= LP union RP ids",
 /* 189 */ "sub ::= sub COMMA LP union RP ids",
 /* 190 */ "tablelist ::= ids cpxName",
 /* 191 */ "tablelist ::= ids cpxName ids",
 /* 192 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 193 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 194 */ "tmvar ::= VARIABLE",
 /* 195 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 196 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 197 */ "interval_option ::=",
 /* 198 */ "intervalKey ::= INTERVAL",
 /* 199 */ "intervalKey ::= EVERY",
 /* 200 */ "session_option ::=",
 /* 201 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 202 */ "windowstate_option ::=",
 /* 203 */ "windowstate_option ::= STATE_WINDOW LP ids RP",
 /* 204 */ "fill_opt ::=",
 /* 205 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 206 */ "fill_opt ::= FILL LP ID RP",
 /* 207 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 208 */ "sliding_opt ::=",
 /* 209 */ "orderby_opt ::=",
 /* 210 */ "orderby_opt ::= ORDER BY sortlist",
 /* 211 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 212 */ "sortlist ::= item sortorder",
 /* 213 */ "item ::= ids cpxName",
 /* 214 */ "sortorder ::= ASC",
 /* 215 */ "sortorder ::= DESC",
 /* 216 */ "sortorder ::=",
 /* 217 */ "groupby_opt ::=",
 /* 218 */ "groupby_opt ::= GROUP BY grouplist",
 /* 219 */ "grouplist ::= grouplist COMMA item",
 /* 220 */ "grouplist ::= item",
 /* 221 */ "having_opt ::=",
 /* 222 */ "having_opt ::= HAVING expr",
 /* 223 */ "limit_opt ::=",
 /* 224 */ "limit_opt ::= LIMIT signed",
 /* 225 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 226 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 227 */ "slimit_opt ::=",
 /* 228 */ "slimit_opt ::= SLIMIT signed",
 /* 229 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 230 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 231 */ "where_opt ::=",
 /* 232 */ "where_opt ::= WHERE expr",
 /* 233 */ "expr ::= LP expr RP",
 /* 234 */ "expr ::= ID",
 /* 235 */ "expr ::= ID DOT ID",
 /* 236 */ "expr ::= ID DOT STAR",
 /* 237 */ "expr ::= INTEGER",
 /* 238 */ "expr ::= MINUS INTEGER",
 /* 239 */ "expr ::= PLUS INTEGER",
 /* 240 */ "expr ::= FLOAT",
 /* 241 */ "expr ::= MINUS FLOAT",
 /* 242 */ "expr ::= PLUS FLOAT",
 /* 243 */ "expr ::= STRING",
 /* 244 */ "expr ::= NOW",
 /* 245 */ "expr ::= VARIABLE",
 /* 246 */ "expr ::= PLUS VARIABLE",
 /* 247 */ "expr ::= MINUS VARIABLE",
 /* 248 */ "expr ::= BOOL",
 /* 249 */ "expr ::= NULL",
 /* 250 */ "expr ::= ID LP exprlist RP",
 /* 251 */ "expr ::= ID LP STAR RP",
 /* 252 */ "expr ::= expr IS NULL",
 /* 253 */ "expr ::= expr IS NOT NULL",
 /* 254 */ "expr ::= expr LT expr",
 /* 255 */ "expr ::= expr GT expr",
 /* 256 */ "expr ::= expr LE expr",
 /* 257 */ "expr ::= expr GE expr",
 /* 258 */ "expr ::= expr NE expr",
 /* 259 */ "expr ::= expr EQ expr",
 /* 260 */ "expr ::= expr BETWEEN expr AND expr",
 /* 261 */ "expr ::= expr AND expr",
 /* 262 */ "expr ::= expr OR expr",
 /* 263 */ "expr ::= expr PLUS expr",
 /* 264 */ "expr ::= expr MINUS expr",
 /* 265 */ "expr ::= expr STAR expr",
 /* 266 */ "expr ::= expr SLASH expr",
 /* 267 */ "expr ::= expr REM expr",
 /* 268 */ "expr ::= expr LIKE expr",
 /* 269 */ "expr ::= expr IN LP exprlist RP",
 /* 270 */ "exprlist ::= exprlist COMMA expritem",
 /* 271 */ "exprlist ::= expritem",
 /* 272 */ "expritem ::= expr",
 /* 273 */ "expritem ::=",
 /* 274 */ "cmd ::= RESET QUERY CACHE",
 /* 275 */ "cmd ::= SYNCDB ids REPLICA",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 277 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 278 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 279 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 280 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 281 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 282 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 283 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 285 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 286 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 287 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 288 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 289 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 290 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 291 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 292 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 293 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 294 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
  {  254,   -2 }, /* (166) tagitem ::= MINUS INTEGER */
  {  254,   -2 }, /* (167) tagitem ::= MINUS FLOAT */
  {  254,   -2 }, /* (168) tagitem ::= PLUS INTEGER */
  {  254,   -2 }, /* (169) tagitem ::= PLUS FLOAT */
  {  251,  -14 }, /* (170) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  251,   -3 }, /* (171) select ::= LP select RP */
  {  268,   -1 }, /* (172) union ::= select */
  {  268,   -4 }, /* (173) union ::= union UNION ALL select */
  {  200,   -1 }, /* (174) cmd ::= union */
  {  251,   -2 }, /* (175) select ::= SELECT selcollist */
  {  269,   -2 }, /* (176) sclp ::= selcollist COMMA */
  {  269,    0 }, /* (177) sclp ::= */
  {  255,   -4 }, /* (178) selcollist ::= sclp distinct expr as */
  {  255,   -2 }, /* (179) selcollist ::= sclp STAR */
  {  272,   -2 }, /* (180) as ::= AS ids */
  {  272,   -1 }, /* (181) as ::= ids */
  {  272,    0 }, /* (182) as ::= */
  {  270,   -1 }, /* (183) distinct ::= DISTINCT */
  {  270,    0 }, /* (184) distinct ::= */
  {  256,   -2 }, /* (185) from ::= FROM tablelist */
  {  256,   -2 }, /* (186) from ::= FROM sub */
  {  274,   -3 }, /* (187) sub ::= LP union RP */
  {  274,   -4 }, /* (188) sub ::= LP union RP ids */
  {  274,   -6 }, /* (189) sub ::= sub COMMA LP union RP ids */
  {  273,   -2 }, /* (190) tablelist ::= ids cpxName */
  {  273,   -3 }, /* (191) tablelist ::= ids cpxName ids */
  {  273,   -4 }, /* (192) tablelist ::= tablelist COMMA ids cpxName */
  {  273,   -5 }, /* (193) tablelist ::= tablelist COMMA ids cpxName ids */
  {  275,   -1 }, /* (194) tmvar ::= VARIABLE */
  {  258,   -4 }, /* (195) interval_option ::= intervalKey LP tmvar RP */
  {  258,   -6 }, /* (196) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  258,    0 }, /* (197) interval_option ::= */
  {  276,   -1 }, /* (198) intervalKey ::= INTERVAL */
  {  276,   -1 }, /* (199) intervalKey ::= EVERY */
  {  260,    0 }, /* (200) session_option ::= */
  {  260,   -7 }, /* (201) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  261,    0 }, /* (202) windowstate_option ::= */
  {  261,   -4 }, /* (203) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  262,    0 }, /* (204) fill_opt ::= */
  {  262,   -6 }, /* (205) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  262,   -4 }, /* (206) fill_opt ::= FILL LP ID RP */
  {  259,   -4 }, /* (207) sliding_opt ::= SLIDING LP tmvar RP */
  {  259,    0 }, /* (208) sliding_opt ::= */
  {  265,    0 }, /* (209) orderby_opt ::= */
  {  265,   -3 }, /* (210) orderby_opt ::= ORDER BY sortlist */
  {  277,   -4 }, /* (211) sortlist ::= sortlist COMMA item sortorder */
  {  277,   -2 }, /* (212) sortlist ::= item sortorder */
  {  279,   -2 }, /* (213) item ::= ids cpxName */
  {  280,   -1 }, /* (214) sortorder ::= ASC */
  {  280,   -1 }, /* (215) sortorder ::= DESC */
  {  280,    0 }, /* (216) sortorder ::= */
  {  263,    0 }, /* (217) groupby_opt ::= */
  {  263,   -3 }, /* (218) groupby_opt ::= GROUP BY grouplist */
  {  281,   -3 }, /* (219) grouplist ::= grouplist COMMA item */
  {  281,   -1 }, /* (220) grouplist ::= item */
  {  264,    0 }, /* (221) having_opt ::= */
  {  264,   -2 }, /* (222) having_opt ::= HAVING expr */
  {  267,    0 }, /* (223) limit_opt ::= */
  {  267,   -2 }, /* (224) limit_opt ::= LIMIT signed */
  {  267,   -4 }, /* (225) limit_opt ::= LIMIT signed OFFSET signed */
  {  267,   -4 }, /* (226) limit_opt ::= LIMIT signed COMMA signed */
  {  266,    0 }, /* (227) slimit_opt ::= */
  {  266,   -2 }, /* (228) slimit_opt ::= SLIMIT signed */
  {  266,   -4 }, /* (229) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  266,   -4 }, /* (230) slimit_opt ::= SLIMIT signed COMMA signed */
  {  257,    0 }, /* (231) where_opt ::= */
  {  257,   -2 }, /* (232) where_opt ::= WHERE expr */
  {  271,   -3 }, /* (233) expr ::= LP expr RP */
  {  271,   -1 }, /* (234) expr ::= ID */
  {  271,   -3 }, /* (235) expr ::= ID DOT ID */
  {  271,   -3 }, /* (236) expr ::= ID DOT STAR */
  {  271,   -1 }, /* (237) expr ::= INTEGER */
  {  271,   -2 }, /* (238) expr ::= MINUS INTEGER */
  {  271,   -2 }, /* (239) expr ::= PLUS INTEGER */
  {  271,   -1 }, /* (240) expr ::= FLOAT */
  {  271,   -2 }, /* (241) expr ::= MINUS FLOAT */
  {  271,   -2 }, /* (242) expr ::= PLUS FLOAT */
  {  271,   -1 }, /* (243) expr ::= STRING */
  {  271,   -1 }, /* (244) expr ::= NOW */
  {  271,   -1 }, /* (245) expr ::= VARIABLE */
  {  271,   -2 }, /* (246) expr ::= PLUS VARIABLE */
  {  271,   -2 }, /* (247) expr ::= MINUS VARIABLE */
  {  271,   -1 }, /* (248) expr ::= BOOL */
  {  271,   -1 }, /* (249) expr ::= NULL */
  {  271,   -4 }, /* (250) expr ::= ID LP exprlist RP */
  {  271,   -4 }, /* (251) expr ::= ID LP STAR RP */
  {  271,   -3 }, /* (252) expr ::= expr IS NULL */
  {  271,   -4 }, /* (253) expr ::= expr IS NOT NULL */
  {  271,   -3 }, /* (254) expr ::= expr LT expr */
  {  271,   -3 }, /* (255) expr ::= expr GT expr */
  {  271,   -3 }, /* (256) expr ::= expr LE expr */
  {  271,   -3 }, /* (257) expr ::= expr GE expr */
  {  271,   -3 }, /* (258) expr ::= expr NE expr */
  {  271,   -3 }, /* (259) expr ::= expr EQ expr */
  {  271,   -5 }, /* (260) expr ::= expr BETWEEN expr AND expr */
  {  271,   -3 }, /* (261) expr ::= expr AND expr */
  {  271,   -3 }, /* (262) expr ::= expr OR expr */
  {  271,   -3 }, /* (263) expr ::= expr PLUS expr */
  {  271,   -3 }, /* (264) expr ::= expr MINUS expr */
  {  271,   -3 }, /* (265) expr ::= expr STAR expr */
  {  271,   -3 }, /* (266) expr ::= expr SLASH expr */
  {  271,   -3 }, /* (267) expr ::= expr REM expr */
  {  271,   -3 }, /* (268) expr ::= expr LIKE expr */
  {  271,   -5 }, /* (269) expr ::= expr IN LP exprlist RP */
  {  208,   -3 }, /* (270) exprlist ::= exprlist COMMA expritem */
  {  208,   -1 }, /* (271) exprlist ::= expritem */
  {  282,   -1 }, /* (272) expritem ::= expr */
  {  282,    0 }, /* (273) expritem ::= */
  {  200,   -3 }, /* (274) cmd ::= RESET QUERY CACHE */
  {  200,   -3 }, /* (275) cmd ::= SYNCDB ids REPLICA */
  {  200,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (277) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (278) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (279) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (280) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (281) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (282) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (283) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  200,   -7 }, /* (285) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  200,   -7 }, /* (286) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  200,   -7 }, /* (287) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  200,   -7 }, /* (288) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  200,   -8 }, /* (289) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  200,   -9 }, /* (290) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  200,   -7 }, /* (291) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  200,   -3 }, /* (292) cmd ::= KILL CONNECTION INTEGER */
  {  200,   -5 }, /* (293) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  200,   -5 }, /* (294) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 184: /* distinct ::= */ yytestcase(yyruleno==184);
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
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; tVariantCreate(&yylhsminor.yy112, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy112 = yylhsminor.yy112;
        break;
      case 166: /* tagitem ::= MINUS INTEGER */
      case 167: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==167);
      case 168: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==168);
      case 169: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==169);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy112, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy112 = yylhsminor.yy112;
        break;
      case 170: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy378 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy89, yymsp[-11].minor.yy166, yymsp[-10].minor.yy342, yymsp[-4].minor.yy89, yymsp[-2].minor.yy89, &yymsp[-9].minor.yy66, &yymsp[-7].minor.yy431, &yymsp[-6].minor.yy392, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy89, &yymsp[0].minor.yy372, &yymsp[-1].minor.yy372, yymsp[-3].minor.yy342);
}
  yymsp[-13].minor.yy378 = yylhsminor.yy378;
        break;
      case 171: /* select ::= LP select RP */
{yymsp[-2].minor.yy378 = yymsp[-1].minor.yy378;}
        break;
      case 172: /* union ::= select */
{ yylhsminor.yy89 = setSubclause(NULL, yymsp[0].minor.yy378); }
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 173: /* union ::= union UNION ALL select */
{ yylhsminor.yy89 = appendSelectClause(yymsp[-3].minor.yy89, yymsp[0].minor.yy378); }
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 174: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy89, NULL, TSDB_SQL_SELECT); }
        break;
      case 175: /* select ::= SELECT selcollist */
{
  yylhsminor.yy378 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy89, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy378 = yylhsminor.yy378;
        break;
      case 176: /* sclp ::= selcollist COMMA */
{yylhsminor.yy89 = yymsp[-1].minor.yy89;}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 177: /* sclp ::= */
      case 209: /* orderby_opt ::= */ yytestcase(yyruleno==209);
{yymsp[1].minor.yy89 = 0;}
        break;
      case 178: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy89 = tSqlExprListAppend(yymsp[-3].minor.yy89, yymsp[-1].minor.yy342,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 179: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy89 = tSqlExprListAppend(yymsp[-1].minor.yy89, pNode, 0, 0);
}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 180: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 181: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 182: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 183: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 185: /* from ::= FROM tablelist */
      case 186: /* from ::= FROM sub */ yytestcase(yyruleno==186);
{yymsp[-1].minor.yy166 = yymsp[0].minor.yy166;}
        break;
      case 187: /* sub ::= LP union RP */
{yymsp[-2].minor.yy166 = addSubqueryElem(NULL, yymsp[-1].minor.yy89, NULL);}
        break;
      case 188: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy166 = addSubqueryElem(NULL, yymsp[-2].minor.yy89, &yymsp[0].minor.yy0);}
        break;
      case 189: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy166 = addSubqueryElem(yymsp[-5].minor.yy166, yymsp[-2].minor.yy89, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy166 = yylhsminor.yy166;
        break;
      case 190: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy166 = yylhsminor.yy166;
        break;
      case 191: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy166 = yylhsminor.yy166;
        break;
      case 192: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(yymsp[-3].minor.yy166, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy166 = yylhsminor.yy166;
        break;
      case 193: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy166 = setTableNameList(yymsp[-4].minor.yy166, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy166 = yylhsminor.yy166;
        break;
      case 194: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 195: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy66.interval = yymsp[-1].minor.yy0; yylhsminor.yy66.offset.n = 0; yylhsminor.yy66.token = yymsp[-3].minor.yy130;}
  yymsp[-3].minor.yy66 = yylhsminor.yy66;
        break;
      case 196: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy66.interval = yymsp[-3].minor.yy0; yylhsminor.yy66.offset = yymsp[-1].minor.yy0;   yylhsminor.yy66.token = yymsp[-5].minor.yy130;}
  yymsp[-5].minor.yy66 = yylhsminor.yy66;
        break;
      case 197: /* interval_option ::= */
{memset(&yymsp[1].minor.yy66, 0, sizeof(yymsp[1].minor.yy66));}
        break;
      case 198: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy130 = TK_INTERVAL;}
        break;
      case 199: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy130 = TK_EVERY;   }
        break;
      case 200: /* session_option ::= */
{yymsp[1].minor.yy431.col.n = 0; yymsp[1].minor.yy431.gap.n = 0;}
        break;
      case 201: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy431.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy431.gap = yymsp[-1].minor.yy0;
}
        break;
      case 202: /* windowstate_option ::= */
{ yymsp[1].minor.yy392.col.n = 0; yymsp[1].minor.yy392.col.z = NULL;}
        break;
      case 203: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy392.col = yymsp[-1].minor.yy0; }
        break;
      case 204: /* fill_opt ::= */
{ yymsp[1].minor.yy89 = 0;     }
        break;
      case 205: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy89, &A, -1, 0);
    yymsp[-5].minor.yy89 = yymsp[-1].minor.yy89;
}
        break;
      case 206: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy89 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 207: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 208: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 210: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy89 = yymsp[0].minor.yy89;}
        break;
      case 211: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy89 = tVariantListAppend(yymsp[-3].minor.yy89, &yymsp[-1].minor.yy112, yymsp[0].minor.yy346);
}
  yymsp[-3].minor.yy89 = yylhsminor.yy89;
        break;
      case 212: /* sortlist ::= item sortorder */
{
  yylhsminor.yy89 = tVariantListAppend(NULL, &yymsp[-1].minor.yy112, yymsp[0].minor.yy346);
}
  yymsp[-1].minor.yy89 = yylhsminor.yy89;
        break;
      case 213: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy112, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy112 = yylhsminor.yy112;
        break;
      case 214: /* sortorder ::= ASC */
{ yymsp[0].minor.yy346 = TSDB_ORDER_ASC; }
        break;
      case 215: /* sortorder ::= DESC */
{ yymsp[0].minor.yy346 = TSDB_ORDER_DESC;}
        break;
      case 216: /* sortorder ::= */
{ yymsp[1].minor.yy346 = TSDB_ORDER_ASC; }
        break;
      case 217: /* groupby_opt ::= */
{ yymsp[1].minor.yy89 = 0;}
        break;
      case 218: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy89 = yymsp[0].minor.yy89;}
        break;
      case 219: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy89 = tVariantListAppend(yymsp[-2].minor.yy89, &yymsp[0].minor.yy112, -1);
}
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 220: /* grouplist ::= item */
{
  yylhsminor.yy89 = tVariantListAppend(NULL, &yymsp[0].minor.yy112, -1);
}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 221: /* having_opt ::= */
      case 231: /* where_opt ::= */ yytestcase(yyruleno==231);
      case 273: /* expritem ::= */ yytestcase(yyruleno==273);
{yymsp[1].minor.yy342 = 0;}
        break;
      case 222: /* having_opt ::= HAVING expr */
      case 232: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==232);
{yymsp[-1].minor.yy342 = yymsp[0].minor.yy342;}
        break;
      case 223: /* limit_opt ::= */
      case 227: /* slimit_opt ::= */ yytestcase(yyruleno==227);
{yymsp[1].minor.yy372.limit = -1; yymsp[1].minor.yy372.offset = 0;}
        break;
      case 224: /* limit_opt ::= LIMIT signed */
      case 228: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==228);
{yymsp[-1].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-1].minor.yy372.offset = 0;}
        break;
      case 225: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy372.limit = yymsp[-2].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[0].minor.yy145;}
        break;
      case 226: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[-2].minor.yy145;}
        break;
      case 229: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy372.limit = yymsp[-2].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[0].minor.yy145;}
        break;
      case 230: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy372.limit = yymsp[0].minor.yy145;  yymsp[-3].minor.yy372.offset = yymsp[-2].minor.yy145;}
        break;
      case 233: /* expr ::= LP expr RP */
{yylhsminor.yy342 = yymsp[-1].minor.yy342; yylhsminor.yy342->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy342->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 234: /* expr ::= ID */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 235: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 236: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 237: /* expr ::= INTEGER */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 238: /* expr ::= MINUS INTEGER */
      case 239: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==239);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 240: /* expr ::= FLOAT */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 241: /* expr ::= MINUS FLOAT */
      case 242: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==242);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 243: /* expr ::= STRING */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 244: /* expr ::= NOW */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 245: /* expr ::= VARIABLE */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 246: /* expr ::= PLUS VARIABLE */
      case 247: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==247);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy342 = yylhsminor.yy342;
        break;
      case 248: /* expr ::= BOOL */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 249: /* expr ::= NULL */
{ yylhsminor.yy342 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 250: /* expr ::= ID LP exprlist RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy342 = tSqlExprCreateFunction(yymsp[-1].minor.yy89, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 251: /* expr ::= ID LP STAR RP */
{ tStrTokenAppend(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy342 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 252: /* expr ::= expr IS NULL */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 253: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-3].minor.yy342, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy342 = yylhsminor.yy342;
        break;
      case 254: /* expr ::= expr LT expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LT);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 255: /* expr ::= expr GT expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_GT);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 256: /* expr ::= expr LE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 257: /* expr ::= expr GE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_GE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 258: /* expr ::= expr NE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_NE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 259: /* expr ::= expr EQ expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_EQ);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 260: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy342); yylhsminor.yy342 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy342, yymsp[-2].minor.yy342, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy342, TK_LE), TK_AND);}
  yymsp[-4].minor.yy342 = yylhsminor.yy342;
        break;
      case 261: /* expr ::= expr AND expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_AND);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 262: /* expr ::= expr OR expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_OR); }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 263: /* expr ::= expr PLUS expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_PLUS);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 264: /* expr ::= expr MINUS expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_MINUS); }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 265: /* expr ::= expr STAR expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_STAR);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 266: /* expr ::= expr SLASH expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_DIVIDE);}
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 267: /* expr ::= expr REM expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_REM);   }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 268: /* expr ::= expr LIKE expr */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-2].minor.yy342, yymsp[0].minor.yy342, TK_LIKE);  }
  yymsp[-2].minor.yy342 = yylhsminor.yy342;
        break;
      case 269: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy342 = tSqlExprCreate(yymsp[-4].minor.yy342, (tSqlExpr*)yymsp[-1].minor.yy89, TK_IN); }
  yymsp[-4].minor.yy342 = yylhsminor.yy342;
        break;
      case 270: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy89 = tSqlExprListAppend(yymsp[-2].minor.yy89,yymsp[0].minor.yy342,0, 0);}
  yymsp[-2].minor.yy89 = yylhsminor.yy89;
        break;
      case 271: /* exprlist ::= expritem */
{yylhsminor.yy89 = tSqlExprListAppend(0,yymsp[0].minor.yy342,0, 0);}
  yymsp[0].minor.yy89 = yylhsminor.yy89;
        break;
      case 272: /* expritem ::= expr */
{yylhsminor.yy342 = yymsp[0].minor.yy342;}
  yymsp[0].minor.yy342 = yylhsminor.yy342;
        break;
      case 274: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 275: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 282: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy112, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 286: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 287: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 288: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 289: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 290: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy112, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 291: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy89, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 292: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 293: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 294: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
