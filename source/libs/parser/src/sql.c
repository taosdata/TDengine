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
#include "astGenerator.h"
#include "tmsgtype.h"
#include "ttoken.h"
#include "ttokendef.h"
#include "tvariant.h"
#include "parserInt.h"
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
#define YYNOCODE 276
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SSessionWindowVal yy39;
  SCreateDbInfo yy42;
  SVariant yy43;
  int yy44;
  tSqlExpr* yy46;
  SLimit yy55;
  SCreatedTableInfo yy96;
  SArray* yy131;
  SSqlNode* yy256;
  SCreateTableSql* yy272;
  SField yy290;
  SSubclause* yy303;
  int32_t yy310;
  SCreateAcctInfo yy341;
  int64_t yy459;
  SIntervalVal yy530;
  SWindowStateVal yy538;
  SRelationInfo* yy544;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             358
#define YYNRULE              288
#define YYNTOKEN             196
#define YY_MAX_SHIFT         357
#define YY_MIN_SHIFTREDUCE   564
#define YY_MAX_SHIFTREDUCE   851
#define YY_ERROR_ACTION      852
#define YY_ACCEPT_ACTION     853
#define YY_NO_ACTION         854
#define YY_MIN_REDUCE        855
#define YY_MAX_REDUCE        1142
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
#define YY_ACTTAB_COUNT (762)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */    94,  615,  233,  244,  356,  227, 1004,  239,  243,  616,
 /*    10 */   615, 1004,   21,   55,   56,  241,   59,   60,  616, 1004,
 /*    20 */   247,   49,   48,   47,  200,   58,  315,   63,   61,   64,
 /*    30 */    62,  987,  988,   33,  991,   54,   53,  853,  357,   52,
 /*    40 */    51,   50,   55,   56,   97,   59,   60,  650,  248,  247,
 /*    50 */    49,   48,   47,  201,   58,  315,   63,   61,   64,   62,
 /*    60 */   976,  203,  974,  975,   54,   53,  203,  977,   52,   51,
 /*    70 */    50,  978, 1119,  979,  980,   54,   53, 1119,  992,   52,
 /*    80 */    51,   50,   55,   56, 1017,   59,   60,  159,   79,  247,
 /*    90 */    49,   48,   47,   27,   58,  315,   63,   61,   64,   62,
 /*   100 */   269,  313,  693,  990,   54,   53,  203,  998,   52,   51,
 /*   110 */    50,  345,   55,   57,  788,   59,   60, 1119,   36,  247,
 /*   120 */    49,   48,   47,  615,   58,  315,   63,   61,   64,   62,
 /*   130 */  1017,  616,  335,  334,   54,   53,  152,  615,   52,   51,
 /*   140 */    50,   56,   36,   59,   60,  616,  230,  247,   49,   48,
 /*   150 */    47,  240,   58,  315,   63,   61,   64,   62,  159, 1066,
 /*   160 */   229,  287,   54,   53, 1001,   73,   52,   51,   50,  565,
 /*   170 */   566,  567,  568,  569,  570,  571,  572,  573,  574,  575,
 /*   180 */   576,  577,  578,  150,  237,  228,   59,   60, 1001,  313,
 /*   190 */   247,   49,   48,   47,  250,   58,  315,   63,   61,   64,
 /*   200 */    62,  203,  275,  274,   74,   54,   53,  753,  754,   52,
 /*   210 */    51,   50, 1118,   42,  311,  351,  350,  310,  309,  308,
 /*   220 */   349,  307,  306,  305,  348,  304,  347,  346,  289,   22,
 /*   230 */    90,  970,  958,  959,  960,  961,  962,  963,  964,  965,
 /*   240 */   966,  967,  968,  969,  971,  972,  212, 1017,  246,  803,
 /*   250 */    36,   36,  792,  213,  795,   36,  798,  168,   36,  134,
 /*   260 */   133,  132,  214,  231,  246,  803,  320,   85,  792,  251,
 /*   270 */   795,  249,  798,  323,  322,  277,   63,   61,   64,   62,
 /*   280 */   206,  255,  225,  226,   54,   53,  316,   85,   52,   51,
 /*   290 */    50,   12,  238,  324,   85,   93, 1001, 1001,  225,  226,
 /*   300 */   325, 1000,  717,   43, 1001,  714,  261,  715,  207,  716,
 /*   310 */   194,  192,  190,  159, 1027,  265,  264,  189,  138,  137,
 /*   320 */   136,  135,   36,   43,   96,  268,   42,   77,  351,  350,
 /*   330 */    43,   65,   84,  349,  221,  252,  253,  348,   36,  347,
 /*   340 */   346,    3,   39,  175,  119,  113,  123,   65,   36,  103,
 /*   350 */   107,   99,  106,  128,  131,  122,  257,   36,  254,   36,
 /*   360 */   330,  329,  125,  159,  326,  804,  799,  300, 1001,  734,
 /*   370 */    36,  121,  800,   52,   51,   50,  794,  256,  797, 1024,
 /*   380 */   327,  804,  799,  345, 1001, 1065,  173,  793,  800,  796,
 /*   390 */   331,  149,  147,  146, 1001,  355,  354,  143,  256,  332,
 /*   400 */   256,  333,  903, 1001,  770, 1001,   78,  174,  185, 1002,
 /*   410 */   352,  940,  337,   70,   92,  731, 1001,  913,  718,  719,
 /*   420 */   904,   82,  270,  185,  790,   83,  185,  750,   80,  760,
 /*   430 */   761,  703,  292,  705,  294,   91,   37,  704,  989,   32,
 /*   440 */     7,  154,  826,   66,   24,   37,   37,   66,   95,  805,
 /*   450 */   245,   66,  317,  738,   71,  614,   23,   69,   76,  208,
 /*   460 */   769,   69,  791,   23,   14,  112,   13,  111,   16,   23,
 /*   470 */    15,  295,    4,  722,  720,  723,  721,   18,  118,   17,
 /*   480 */   117,   20, 1113,   19,  130,  129, 1112, 1111,  223,  692,
 /*   490 */   224,  204,  205,  209,  202,  210,  801,  211,  216,  217,
 /*   500 */   218,  215,  199, 1003, 1138, 1130, 1019,  802, 1076, 1075,
 /*   510 */   235, 1072, 1071,   44,  236,  336,  266, 1058,  169,  151,
 /*   520 */  1026,  148, 1037, 1034, 1035, 1018,  272, 1057, 1039,  999,
 /*   530 */  1015,  276,  153,  232,  278,  280,   31,  158,  283,  163,
 /*   540 */   170,  160,  997,  161,  749,  162,  164,  165,  166,  171,
 /*   550 */   172,  301,  917,  297,  290,  807,  284,  298,  299,  302,
 /*   560 */   303,  197,   40,   75,  314,  912,  321, 1137,  109,   72,
 /*   570 */    46, 1136, 1133,  288,  176,  328, 1129,  115, 1128, 1125,
 /*   580 */   177,  286,  937,   41,   38,  198,  901,  279,  124,  899,
 /*   590 */   126,  127,  897,  896,  258,  187,  188,  893,  892,  891,
 /*   600 */   890,  889,  888,  282,  887,  191,  193,  884,  882,  880,
 /*   610 */   878,  195,  875,  196,  871,   45,  120,  271,   81,   86,
 /*   620 */   338,  281, 1059,  339,  340,  341,  222,  343,  342,  242,
 /*   630 */   296,  344,  353,  851,  259,  260,  219,  220,  850,  104,
 /*   640 */   916,  915,  262,  263,  849,  832,  895,  894,  831,  267,
 /*   650 */   291,  139,   69,  886,  180,  140,  179,  938,  178,  181,
 /*   660 */   182,  184,  183,  939,  141,  885,    8,  142,  877,    2,
 /*   670 */     1,  876,  725,   28,  273,  167,   87,  751,  155,  157,
 /*   680 */   762,  156,  234,  756,   88,   29,  758,   89,  285,    9,
 /*   690 */    30,   10,   11,   25,  293,   26,   96,   98,  101,   34,
 /*   700 */   100,  628,   35,  102,  663,  661,  660,  659,  657,  656,
 /*   710 */   655,  652,  619,  312,  105,    5,  318,  806,  319,    6,
 /*   720 */   808,  108,  110,   67,   68,  695,   37,  694,  691,  114,
 /*   730 */   644,  116,  642,  634,  640,  636,  638,  632,  630,  665,
 /*   740 */   664,  662,  658,  654,  653,  186,  617,  144,  582,  855,
 /*   750 */   854,  854,  854,  854,  854,  854,  854,  854,  854,  854,
 /*   760 */   854,  145,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   206,    1,  242,  205,  199,  200,  246,  242,  205,    9,
 /*    10 */     1,  246,  263,   13,   14,  242,   16,   17,    9,  246,
 /*    20 */    20,   21,   22,   23,  263,   25,   26,   27,   28,   29,
 /*    30 */    30,  237,  238,  239,  240,   35,   36,  197,  198,   39,
 /*    40 */    40,   41,   13,   14,  206,   16,   17,    5,  205,   20,
 /*    50 */    21,   22,   23,  263,   25,   26,   27,   28,   29,   30,
 /*    60 */   221,  263,  223,  224,   35,   36,  263,  228,   39,   40,
 /*    70 */    41,  232,  274,  234,  235,   35,   36,  274,  240,   39,
 /*    80 */    40,   41,   13,   14,  244,   16,   17,  199,   88,   20,
 /*    90 */    21,   22,   23,   84,   25,   26,   27,   28,   29,   30,
 /*   100 */   260,   86,    5,    0,   35,   36,  263,  199,   39,   40,
 /*   110 */    41,   92,   13,   14,   85,   16,   17,  274,  199,   20,
 /*   120 */    21,   22,   23,    1,   25,   26,   27,   28,   29,   30,
 /*   130 */   244,    9,   35,   36,   35,   36,  199,    1,   39,   40,
 /*   140 */    41,   14,  199,   16,   17,    9,  260,   20,   21,   22,
 /*   150 */    23,  243,   25,   26,   27,   28,   29,   30,  199,  271,
 /*   160 */   241,  273,   35,   36,  245,   99,   39,   40,   41,   47,
 /*   170 */    48,   49,   50,   51,   52,   53,   54,   55,   56,   57,
 /*   180 */    58,   59,   60,   61,  241,   63,   16,   17,  245,   86,
 /*   190 */    20,   21,   22,   23,   70,   25,   26,   27,   28,   29,
 /*   200 */    30,  263,  265,  266,  138,   35,   36,  126,  127,   39,
 /*   210 */    40,   41,  274,  100,  101,  102,  103,  104,  105,  106,
 /*   220 */   107,  108,  109,  110,  111,  112,  113,  114,  269,   46,
 /*   230 */   271,  221,  222,  223,  224,  225,  226,  227,  228,  229,
 /*   240 */   230,  231,  232,  233,  234,  235,   63,  244,    1,    2,
 /*   250 */   199,  199,    5,   70,    7,  199,    9,  250,  199,   76,
 /*   260 */    77,   78,   79,  260,    1,    2,   83,   84,    5,  145,
 /*   270 */     7,  147,    9,  149,  150,  268,   27,   28,   29,   30,
 /*   280 */   263,   70,   35,   36,   35,   36,   39,   84,   39,   40,
 /*   290 */    41,   84,  241,  241,   84,   88,  245,  245,   35,   36,
 /*   300 */   241,  245,    2,  120,  245,    5,  143,    7,  263,    9,
 /*   310 */    64,   65,   66,  199,  199,  152,  153,   71,   72,   73,
 /*   320 */    74,   75,  199,  120,  117,  142,  100,  144,  102,  103,
 /*   330 */   120,   84,  122,  107,  151,   35,   36,  111,  199,  113,
 /*   340 */   114,   64,   65,   66,   64,   65,   66,   84,  199,   72,
 /*   350 */    73,   74,   75,   73,   74,   75,  145,  199,  147,  199,
 /*   360 */   149,  150,   82,  199,  241,  118,  119,   90,  245,   39,
 /*   370 */   199,   80,  125,   39,   40,   41,    5,  199,    7,  264,
 /*   380 */   241,  118,  119,   92,  245,  271,  208,    5,  125,    7,
 /*   390 */   241,   64,   65,   66,  245,   67,   68,   69,  199,  241,
 /*   400 */   199,  241,  204,  245,   78,  245,  206,  208,  210,  208,
 /*   410 */   219,  220,  241,   99,  247,   99,  245,  204,  118,  119,
 /*   420 */   204,   85,   85,  210,    1,   85,  210,   85,  261,   85,
 /*   430 */    85,   85,   85,   85,   85,  271,   99,   85,  238,   84,
 /*   440 */   124,   99,   85,   99,   99,   99,   99,   99,   99,   85,
 /*   450 */    62,   99,   15,  123,  140,   85,   99,  121,   84,  263,
 /*   460 */   134,  121,   39,   99,  146,  146,  148,  148,  146,   99,
 /*   470 */   148,  116,   84,    5,    5,    7,    7,  146,  146,  148,
 /*   480 */   148,  146,  263,  148,   80,   81,  263,  263,  263,  115,
 /*   490 */   263,  263,  263,  263,  263,  263,  125,  263,  263,  263,
 /*   500 */   263,  263,  263,  246,  246,  246,  244,  125,  236,  236,
 /*   510 */   236,  236,  236,  262,  236,  236,  199,  272,  248,  199,
 /*   520 */   199,   62,  199,  199,  199,  244,  244,  272,  199,  244,
 /*   530 */   259,  267,  199,  267,  267,  267,  249,  199,  199,  255,
 /*   540 */   199,  258,  199,  257,  125,  256,  254,  253,  252,  199,
 /*   550 */   199,   91,  199,  199,  132,  118,  129,  199,  199,  199,
 /*   560 */   199,  199,  199,  137,  199,  199,  199,  199,  199,  139,
 /*   570 */   136,  199,  199,  135,  199,  199,  199,  199,  199,  199,
 /*   580 */   199,  130,  199,  199,  199,  199,  199,  131,  199,  199,
 /*   590 */   199,  199,  199,  199,  199,  199,  199,  199,  199,  199,
 /*   600 */   199,  199,  199,  128,  199,  199,  199,  199,  199,  199,
 /*   610 */   199,  199,  199,  199,  199,  141,   98,  201,  201,  201,
 /*   620 */    97,  201,  201,   53,   94,   96,  201,   95,   57,  201,
 /*   630 */   201,   93,   86,    5,  154,    5,  201,  201,    5,  206,
 /*   640 */   209,  209,  154,    5,    5,  102,  201,  201,  101,  143,
 /*   650 */   116,  202,  121,  201,  212,  202,  216,  218,  217,  215,
 /*   660 */   213,  211,  214,  220,  202,  201,   84,  202,  201,  203,
 /*   670 */   207,  201,   85,   84,   99,  251,   99,   85,   84,   99,
 /*   680 */    85,   84,    1,   85,   84,   99,   85,   84,   84,  133,
 /*   690 */    99,  133,   84,   84,  116,   84,  117,   80,   72,   89,
 /*   700 */    88,    5,   89,   88,    9,    5,    5,    5,    5,    5,
 /*   710 */     5,    5,   87,   15,   80,   84,   26,   85,   61,   84,
 /*   720 */   118,  148,  148,   16,   16,    5,   99,    5,   85,  148,
 /*   730 */     5,  148,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   740 */     5,    5,    5,    5,    5,   99,   87,   21,   62,    0,
 /*   750 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   760 */   275,   21,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   770 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   780 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   790 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   800 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   810 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   820 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   830 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   840 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   850 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   860 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   870 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   880 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   890 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   900 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   910 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   920 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   930 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   940 */   275,  275,  275,  275,  275,  275,  275,  275,  275,  275,
 /*   950 */   275,  275,  275,  275,  275,  275,  275,  275,
};
#define YY_SHIFT_COUNT    (357)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (749)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   183,  113,  226,   15,  247,  263,  263,    9,  136,  136,
 /*    10 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*    20 */   136,    0,  122,  263,  300,  300,  300,  203,  203,  136,
 /*    30 */   136,   81,  136,  103,  136,  136,  136,  136,  291,   15,
 /*    40 */    19,   19,   42,  762,  263,  263,  263,  263,  263,  263,
 /*    50 */   263,  263,  263,  263,  263,  263,  263,  263,  263,  263,
 /*    60 */   263,  263,  263,  263,  263,  263,  300,  300,  300,  210,
 /*    70 */    97,   97,   97,   97,   97,   97,   97,  136,  136,  136,
 /*    80 */   330,  136,  136,  136,  203,  203,  136,  136,  136,  136,
 /*    90 */   326,  326,  316,  203,  136,  136,  136,  136,  136,  136,
 /*   100 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*   110 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*   120 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*   130 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*   140 */   136,  136,  136,  136,  136,  136,  136,  136,  136,  136,
 /*   150 */   136,  459,  459,  459,  419,  419,  419,  419,  459,  459,
 /*   160 */   426,  430,  422,  434,  438,  451,  427,  475,  456,  474,
 /*   170 */   459,  459,  459,  460,  460,   15,  459,  459,  518,  523,
 /*   180 */   570,  530,  529,  571,  532,  538,   42,  459,  459,  546,
 /*   190 */   546,  459,  546,  459,  546,  459,  459,  762,  762,   29,
 /*   200 */    69,   69,   99,   69,  127,  170,  249,  249,  249,  249,
 /*   210 */   249,  249,  277,  246,  280,   40,   40,   40,   40,  124,
 /*   220 */   211,  163,  207,  334,  334,  371,  382,  328,  327,  337,
 /*   230 */   336,  340,  342,  344,  345,  314,   66,  346,  347,  348,
 /*   240 */   349,  352,  355,  357,  364,  423,  388,  437,  370,  318,
 /*   250 */   319,  322,  468,  469,  331,  332,  374,  335,  404,  628,
 /*   260 */   480,  630,  633,  488,  638,  639,  543,  547,  506,  531,
 /*   270 */   534,  582,  587,  589,  575,  577,  592,  594,  595,  597,
 /*   280 */   598,  580,  600,  601,  603,  681,  604,  586,  556,  591,
 /*   290 */   558,  608,  534,  609,  578,  611,  579,  617,  610,  612,
 /*   300 */   626,  696,  613,  615,  695,  700,  701,  702,  703,  704,
 /*   310 */   705,  706,  625,  698,  634,  631,  632,  602,  635,  690,
 /*   320 */   657,  707,  573,  574,  627,  627,  627,  627,  708,  581,
 /*   330 */   583,  627,  627,  627,  720,  722,  643,  627,  725,  727,
 /*   340 */   728,  729,  730,  731,  732,  733,  734,  735,  736,  737,
 /*   350 */   738,  739,  646,  659,  726,  740,  686,  749,
};
#define YY_REDUCE_COUNT (198)
#define YY_REDUCE_MIN   (-251)
#define YY_REDUCE_MAX   (470)
static const short yy_reduce_ofst[] = {
 /*     0 */  -160,   10, -161, -206, -202, -197, -157,  -63,  -81, -112,
 /*    10 */   -41,  -57,   51,   52,   59,  123,  139,  149,  158,  160,
 /*    20 */   171,  115, -195,  -62, -240, -235, -227, -114,    3,  114,
 /*    30 */   164,    7,  -92, -162,  178,  199,  201,   56,  198,  200,
 /*    40 */   213,  216,  191,  167, -251, -239, -210,   17,   45,  196,
 /*    50 */   219,  223,  224,  225,  227,  228,  229,  230,  231,  232,
 /*    60 */   234,  235,  236,  237,  238,  239,  257,  258,  259,  262,
 /*    70 */   272,  273,  274,  275,  276,  278,  279,  317,  320,  321,
 /*    80 */   251,  323,  324,  325,  281,  282,  329,  333,  338,  339,
 /*    90 */   245,  255,  270,  285,  341,  343,  350,  351,  353,  354,
 /*   100 */   358,  359,  360,  361,  362,  363,  365,  366,  367,  368,
 /*   110 */   369,  372,  373,  375,  376,  377,  378,  379,  380,  381,
 /*   120 */   383,  384,  385,  386,  387,  389,  390,  391,  392,  393,
 /*   130 */   394,  395,  396,  397,  398,  399,  400,  401,  402,  403,
 /*   140 */   405,  406,  407,  408,  409,  410,  411,  412,  413,  414,
 /*   150 */   415,  416,  417,  418,  264,  266,  267,  268,  420,  421,
 /*   160 */   271,  283,  286,  289,  284,  292,  294,  296,  424,  287,
 /*   170 */   425,  428,  429,  431,  432,  433,  435,  436,  439,  441,
 /*   180 */   440,  442,  444,  447,  448,  450,  443,  445,  446,  449,
 /*   190 */   453,  452,  462,  464,  465,  467,  470,  463,  466,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   852,  914,  902,  911, 1121, 1121, 1121,  852,  852,  852,
 /*    10 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*    20 */   852, 1028,  872, 1121,  852,  852,  852,  852,  852,  852,
 /*    30 */   852, 1043,  852,  911,  852,  852,  852,  852,  920,  911,
 /*    40 */   920,  920,  852, 1023,  852,  852,  852,  852,  852,  852,
 /*    50 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*    60 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*    70 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*    80 */  1030, 1036, 1033,  852,  852,  852, 1038,  852,  852,  852,
 /*    90 */  1062, 1062, 1021,  852,  852,  852,  852,  852,  852,  852,
 /*   100 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   110 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   120 */   852,  852,  852,  852,  900,  852,  898,  852,  852,  852,
 /*   130 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   140 */   852,  852,  852,  883,  852,  852,  852,  852,  852,  852,
 /*   150 */   870,  874,  874,  874,  852,  852,  852,  852,  874,  874,
 /*   160 */  1069, 1073, 1055, 1067, 1063, 1050, 1048, 1046, 1054, 1077,
 /*   170 */   874,  874,  874,  918,  918,  911,  874,  874,  936,  934,
 /*   180 */   932,  924,  930,  926,  928,  922,  852,  874,  874,  909,
 /*   190 */   909,  874,  909,  874,  909,  874,  874,  957,  973,  852,
 /*   200 */  1078, 1068,  852, 1120, 1108, 1107, 1116, 1115, 1114, 1106,
 /*   210 */  1105, 1104,  852,  852,  852, 1100, 1103, 1102, 1101,  852,
 /*   220 */   852,  852,  852, 1110, 1109,  852,  852,  852,  852,  852,
 /*   230 */   852,  852,  852,  852,  852, 1074, 1070,  852,  852,  852,
 /*   240 */   852,  852,  852,  852,  852,  852, 1080,  852,  852,  852,
 /*   250 */   852,  852,  852,  852,  852,  852,  981,  852,  852,  852,
 /*   260 */   852,  852,  852,  852,  852,  852,  852,  852,  852, 1020,
 /*   270 */   852,  852,  852,  852, 1032, 1031,  852,  852,  852,  852,
 /*   280 */   852,  852,  852,  852,  852,  852,  852, 1064,  852, 1056,
 /*   290 */   852,  852,  993,  852,  852,  852,  852,  852,  852,  852,
 /*   300 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   310 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   320 */   852,  852,  852,  852, 1139, 1134, 1135, 1132,  852,  852,
 /*   330 */   852, 1131, 1126, 1127,  852,  852,  852, 1124,  852,  852,
 /*   340 */   852,  852,  852,  852,  852,  852,  852,  852,  852,  852,
 /*   350 */   852,  852,  942,  852,  881,  879,  852,  852,
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
    1,  /*      MATCH => ID */
    1,  /*     NMATCH => ID */
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
    0,  /*   UNSIGNED => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
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
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*    IPTOKEN => ID */
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
  /*   22 */ "MATCH",
  /*   23 */ "NMATCH",
  /*   24 */ "GLOB",
  /*   25 */ "BETWEEN",
  /*   26 */ "IN",
  /*   27 */ "GT",
  /*   28 */ "GE",
  /*   29 */ "LT",
  /*   30 */ "LE",
  /*   31 */ "BITAND",
  /*   32 */ "BITOR",
  /*   33 */ "LSHIFT",
  /*   34 */ "RSHIFT",
  /*   35 */ "PLUS",
  /*   36 */ "MINUS",
  /*   37 */ "DIVIDE",
  /*   38 */ "TIMES",
  /*   39 */ "STAR",
  /*   40 */ "SLASH",
  /*   41 */ "REM",
  /*   42 */ "CONCAT",
  /*   43 */ "UMINUS",
  /*   44 */ "UPLUS",
  /*   45 */ "BITNOT",
  /*   46 */ "SHOW",
  /*   47 */ "DATABASES",
  /*   48 */ "TOPICS",
  /*   49 */ "FUNCTIONS",
  /*   50 */ "MNODES",
  /*   51 */ "DNODES",
  /*   52 */ "ACCOUNTS",
  /*   53 */ "USERS",
  /*   54 */ "MODULES",
  /*   55 */ "QUERIES",
  /*   56 */ "CONNECTIONS",
  /*   57 */ "STREAMS",
  /*   58 */ "VARIABLES",
  /*   59 */ "SCORES",
  /*   60 */ "GRANTS",
  /*   61 */ "VNODES",
  /*   62 */ "DOT",
  /*   63 */ "CREATE",
  /*   64 */ "TABLE",
  /*   65 */ "STABLE",
  /*   66 */ "DATABASE",
  /*   67 */ "TABLES",
  /*   68 */ "STABLES",
  /*   69 */ "VGROUPS",
  /*   70 */ "DROP",
  /*   71 */ "TOPIC",
  /*   72 */ "FUNCTION",
  /*   73 */ "DNODE",
  /*   74 */ "USER",
  /*   75 */ "ACCOUNT",
  /*   76 */ "USE",
  /*   77 */ "DESCRIBE",
  /*   78 */ "DESC",
  /*   79 */ "ALTER",
  /*   80 */ "PASS",
  /*   81 */ "PRIVILEGE",
  /*   82 */ "LOCAL",
  /*   83 */ "COMPACT",
  /*   84 */ "LP",
  /*   85 */ "RP",
  /*   86 */ "IF",
  /*   87 */ "EXISTS",
  /*   88 */ "AS",
  /*   89 */ "OUTPUTTYPE",
  /*   90 */ "AGGREGATE",
  /*   91 */ "BUFSIZE",
  /*   92 */ "PPS",
  /*   93 */ "TSERIES",
  /*   94 */ "DBS",
  /*   95 */ "STORAGE",
  /*   96 */ "QTIME",
  /*   97 */ "CONNS",
  /*   98 */ "STATE",
  /*   99 */ "COMMA",
  /*  100 */ "KEEP",
  /*  101 */ "CACHE",
  /*  102 */ "REPLICA",
  /*  103 */ "QUORUM",
  /*  104 */ "DAYS",
  /*  105 */ "MINROWS",
  /*  106 */ "MAXROWS",
  /*  107 */ "BLOCKS",
  /*  108 */ "CTIME",
  /*  109 */ "WAL",
  /*  110 */ "FSYNC",
  /*  111 */ "COMP",
  /*  112 */ "PRECISION",
  /*  113 */ "UPDATE",
  /*  114 */ "CACHELAST",
  /*  115 */ "UNSIGNED",
  /*  116 */ "TAGS",
  /*  117 */ "USING",
  /*  118 */ "NULL",
  /*  119 */ "NOW",
  /*  120 */ "SELECT",
  /*  121 */ "UNION",
  /*  122 */ "ALL",
  /*  123 */ "DISTINCT",
  /*  124 */ "FROM",
  /*  125 */ "VARIABLE",
  /*  126 */ "INTERVAL",
  /*  127 */ "EVERY",
  /*  128 */ "SESSION",
  /*  129 */ "STATE_WINDOW",
  /*  130 */ "FILL",
  /*  131 */ "SLIDING",
  /*  132 */ "ORDER",
  /*  133 */ "BY",
  /*  134 */ "ASC",
  /*  135 */ "GROUP",
  /*  136 */ "HAVING",
  /*  137 */ "LIMIT",
  /*  138 */ "OFFSET",
  /*  139 */ "SLIMIT",
  /*  140 */ "SOFFSET",
  /*  141 */ "WHERE",
  /*  142 */ "RESET",
  /*  143 */ "QUERY",
  /*  144 */ "SYNCDB",
  /*  145 */ "ADD",
  /*  146 */ "COLUMN",
  /*  147 */ "MODIFY",
  /*  148 */ "TAG",
  /*  149 */ "CHANGE",
  /*  150 */ "SET",
  /*  151 */ "KILL",
  /*  152 */ "CONNECTION",
  /*  153 */ "STREAM",
  /*  154 */ "COLON",
  /*  155 */ "ABORT",
  /*  156 */ "AFTER",
  /*  157 */ "ATTACH",
  /*  158 */ "BEFORE",
  /*  159 */ "BEGIN",
  /*  160 */ "CASCADE",
  /*  161 */ "CLUSTER",
  /*  162 */ "CONFLICT",
  /*  163 */ "COPY",
  /*  164 */ "DEFERRED",
  /*  165 */ "DELIMITERS",
  /*  166 */ "DETACH",
  /*  167 */ "EACH",
  /*  168 */ "END",
  /*  169 */ "EXPLAIN",
  /*  170 */ "FAIL",
  /*  171 */ "FOR",
  /*  172 */ "IGNORE",
  /*  173 */ "IMMEDIATE",
  /*  174 */ "INITIALLY",
  /*  175 */ "INSTEAD",
  /*  176 */ "KEY",
  /*  177 */ "OF",
  /*  178 */ "RAISE",
  /*  179 */ "REPLACE",
  /*  180 */ "RESTRICT",
  /*  181 */ "ROW",
  /*  182 */ "STATEMENT",
  /*  183 */ "TRIGGER",
  /*  184 */ "VIEW",
  /*  185 */ "IPTOKEN",
  /*  186 */ "SEMI",
  /*  187 */ "NONE",
  /*  188 */ "PREV",
  /*  189 */ "LINEAR",
  /*  190 */ "IMPORT",
  /*  191 */ "TBNAME",
  /*  192 */ "JOIN",
  /*  193 */ "INSERT",
  /*  194 */ "INTO",
  /*  195 */ "VALUES",
  /*  196 */ "error",
  /*  197 */ "program",
  /*  198 */ "cmd",
  /*  199 */ "ids",
  /*  200 */ "dbPrefix",
  /*  201 */ "cpxName",
  /*  202 */ "ifexists",
  /*  203 */ "alter_db_optr",
  /*  204 */ "acct_optr",
  /*  205 */ "exprlist",
  /*  206 */ "ifnotexists",
  /*  207 */ "db_optr",
  /*  208 */ "typename",
  /*  209 */ "bufsize",
  /*  210 */ "pps",
  /*  211 */ "tseries",
  /*  212 */ "dbs",
  /*  213 */ "streams",
  /*  214 */ "storage",
  /*  215 */ "qtime",
  /*  216 */ "users",
  /*  217 */ "conns",
  /*  218 */ "state",
  /*  219 */ "intitemlist",
  /*  220 */ "intitem",
  /*  221 */ "keep",
  /*  222 */ "cache",
  /*  223 */ "replica",
  /*  224 */ "quorum",
  /*  225 */ "days",
  /*  226 */ "minrows",
  /*  227 */ "maxrows",
  /*  228 */ "blocks",
  /*  229 */ "ctime",
  /*  230 */ "wal",
  /*  231 */ "fsync",
  /*  232 */ "comp",
  /*  233 */ "prec",
  /*  234 */ "update",
  /*  235 */ "cachelast",
  /*  236 */ "signed",
  /*  237 */ "create_table_args",
  /*  238 */ "create_stable_args",
  /*  239 */ "create_table_list",
  /*  240 */ "create_from_stable",
  /*  241 */ "columnlist",
  /*  242 */ "tagitemlist",
  /*  243 */ "tagNamelist",
  /*  244 */ "select",
  /*  245 */ "column",
  /*  246 */ "tagitem",
  /*  247 */ "selcollist",
  /*  248 */ "from",
  /*  249 */ "where_opt",
  /*  250 */ "interval_option",
  /*  251 */ "sliding_opt",
  /*  252 */ "session_option",
  /*  253 */ "windowstate_option",
  /*  254 */ "fill_opt",
  /*  255 */ "groupby_opt",
  /*  256 */ "having_opt",
  /*  257 */ "orderby_opt",
  /*  258 */ "slimit_opt",
  /*  259 */ "limit_opt",
  /*  260 */ "union",
  /*  261 */ "sclp",
  /*  262 */ "distinct",
  /*  263 */ "expr",
  /*  264 */ "as",
  /*  265 */ "tablelist",
  /*  266 */ "sub",
  /*  267 */ "tmvar",
  /*  268 */ "intervalKey",
  /*  269 */ "sortlist",
  /*  270 */ "sortitem",
  /*  271 */ "item",
  /*  272 */ "sortorder",
  /*  273 */ "grouplist",
  /*  274 */ "expritem",
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
 /*  16 */ "cmd ::= SHOW VNODES ids",
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
 /*  40 */ "cmd ::= DESC ids cpxName",
 /*  41 */ "cmd ::= ALTER USER ids PASS ids",
 /*  42 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  43 */ "cmd ::= ALTER DNODE ids ids",
 /*  44 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  45 */ "cmd ::= ALTER LOCAL ids",
 /*  46 */ "cmd ::= ALTER LOCAL ids ids",
 /*  47 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
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
 /*  84 */ "intitemlist ::= intitemlist COMMA intitem",
 /*  85 */ "intitemlist ::= intitem",
 /*  86 */ "intitem ::= INTEGER",
 /*  87 */ "keep ::= KEEP intitemlist",
 /*  88 */ "cache ::= CACHE INTEGER",
 /*  89 */ "replica ::= REPLICA INTEGER",
 /*  90 */ "quorum ::= QUORUM INTEGER",
 /*  91 */ "days ::= DAYS INTEGER",
 /*  92 */ "minrows ::= MINROWS INTEGER",
 /*  93 */ "maxrows ::= MAXROWS INTEGER",
 /*  94 */ "blocks ::= BLOCKS INTEGER",
 /*  95 */ "ctime ::= CTIME INTEGER",
 /*  96 */ "wal ::= WAL INTEGER",
 /*  97 */ "fsync ::= FSYNC INTEGER",
 /*  98 */ "comp ::= COMP INTEGER",
 /*  99 */ "prec ::= PRECISION STRING",
 /* 100 */ "update ::= UPDATE INTEGER",
 /* 101 */ "cachelast ::= CACHELAST INTEGER",
 /* 102 */ "db_optr ::=",
 /* 103 */ "db_optr ::= db_optr cache",
 /* 104 */ "db_optr ::= db_optr replica",
 /* 105 */ "db_optr ::= db_optr quorum",
 /* 106 */ "db_optr ::= db_optr days",
 /* 107 */ "db_optr ::= db_optr minrows",
 /* 108 */ "db_optr ::= db_optr maxrows",
 /* 109 */ "db_optr ::= db_optr blocks",
 /* 110 */ "db_optr ::= db_optr ctime",
 /* 111 */ "db_optr ::= db_optr wal",
 /* 112 */ "db_optr ::= db_optr fsync",
 /* 113 */ "db_optr ::= db_optr comp",
 /* 114 */ "db_optr ::= db_optr prec",
 /* 115 */ "db_optr ::= db_optr keep",
 /* 116 */ "db_optr ::= db_optr update",
 /* 117 */ "db_optr ::= db_optr cachelast",
 /* 118 */ "alter_db_optr ::=",
 /* 119 */ "alter_db_optr ::= alter_db_optr replica",
 /* 120 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 121 */ "alter_db_optr ::= alter_db_optr keep",
 /* 122 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 123 */ "alter_db_optr ::= alter_db_optr comp",
 /* 124 */ "alter_db_optr ::= alter_db_optr update",
 /* 125 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 126 */ "typename ::= ids",
 /* 127 */ "typename ::= ids LP signed RP",
 /* 128 */ "typename ::= ids UNSIGNED",
 /* 129 */ "signed ::= INTEGER",
 /* 130 */ "signed ::= PLUS INTEGER",
 /* 131 */ "signed ::= MINUS INTEGER",
 /* 132 */ "cmd ::= CREATE TABLE create_table_args",
 /* 133 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 134 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 135 */ "cmd ::= CREATE TABLE create_table_list",
 /* 136 */ "create_table_list ::= create_from_stable",
 /* 137 */ "create_table_list ::= create_table_list create_from_stable",
 /* 138 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 139 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 140 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 141 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 142 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 143 */ "tagNamelist ::= ids",
 /* 144 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 145 */ "columnlist ::= columnlist COMMA column",
 /* 146 */ "columnlist ::= column",
 /* 147 */ "column ::= ids typename",
 /* 148 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 149 */ "tagitemlist ::= tagitem",
 /* 150 */ "tagitem ::= INTEGER",
 /* 151 */ "tagitem ::= FLOAT",
 /* 152 */ "tagitem ::= STRING",
 /* 153 */ "tagitem ::= BOOL",
 /* 154 */ "tagitem ::= NULL",
 /* 155 */ "tagitem ::= NOW",
 /* 156 */ "tagitem ::= MINUS INTEGER",
 /* 157 */ "tagitem ::= MINUS FLOAT",
 /* 158 */ "tagitem ::= PLUS INTEGER",
 /* 159 */ "tagitem ::= PLUS FLOAT",
 /* 160 */ "select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt",
 /* 161 */ "select ::= LP select RP",
 /* 162 */ "union ::= select",
 /* 163 */ "union ::= union UNION ALL select",
 /* 164 */ "union ::= union UNION select",
 /* 165 */ "cmd ::= union",
 /* 166 */ "select ::= SELECT selcollist",
 /* 167 */ "sclp ::= selcollist COMMA",
 /* 168 */ "sclp ::=",
 /* 169 */ "selcollist ::= sclp distinct expr as",
 /* 170 */ "selcollist ::= sclp STAR",
 /* 171 */ "as ::= AS ids",
 /* 172 */ "as ::= ids",
 /* 173 */ "as ::=",
 /* 174 */ "distinct ::= DISTINCT",
 /* 175 */ "distinct ::=",
 /* 176 */ "from ::= FROM tablelist",
 /* 177 */ "from ::= FROM sub",
 /* 178 */ "sub ::= LP union RP",
 /* 179 */ "sub ::= LP union RP ids",
 /* 180 */ "sub ::= sub COMMA LP union RP ids",
 /* 181 */ "tablelist ::= ids cpxName",
 /* 182 */ "tablelist ::= ids cpxName ids",
 /* 183 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 184 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 185 */ "tmvar ::= VARIABLE",
 /* 186 */ "interval_option ::= intervalKey LP tmvar RP",
 /* 187 */ "interval_option ::= intervalKey LP tmvar COMMA tmvar RP",
 /* 188 */ "interval_option ::=",
 /* 189 */ "intervalKey ::= INTERVAL",
 /* 190 */ "intervalKey ::= EVERY",
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
 /* 260 */ "expr ::= expr MATCH expr",
 /* 261 */ "expr ::= expr NMATCH expr",
 /* 262 */ "expr ::= expr IN LP exprlist RP",
 /* 263 */ "exprlist ::= exprlist COMMA expritem",
 /* 264 */ "exprlist ::= expritem",
 /* 265 */ "expritem ::= expr",
 /* 266 */ "expritem ::=",
 /* 267 */ "cmd ::= RESET QUERY CACHE",
 /* 268 */ "cmd ::= SYNCDB ids REPLICA",
 /* 269 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 270 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 271 */ "cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist",
 /* 272 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 273 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 274 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 275 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 276 */ "cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist",
 /* 277 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 278 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 279 */ "cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist",
 /* 280 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 281 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 282 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 283 */ "cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem",
 /* 284 */ "cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist",
 /* 285 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 286 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 287 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 205: /* exprlist */
    case 247: /* selcollist */
    case 261: /* sclp */
{
tSqlExprListDestroy((yypminor->yy131));
}
      break;
    case 219: /* intitemlist */
    case 221: /* keep */
    case 241: /* columnlist */
    case 242: /* tagitemlist */
    case 243: /* tagNamelist */
    case 254: /* fill_opt */
    case 255: /* groupby_opt */
    case 257: /* orderby_opt */
    case 269: /* sortlist */
    case 273: /* grouplist */
{
taosArrayDestroy((yypminor->yy131));
}
      break;
    case 239: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy272));
}
      break;
    case 244: /* select */
{
destroySqlNode((yypminor->yy256));
}
      break;
    case 248: /* from */
    case 265: /* tablelist */
    case 266: /* sub */
{
destroyRelationInfo((yypminor->yy544));
}
      break;
    case 249: /* where_opt */
    case 256: /* having_opt */
    case 263: /* expr */
    case 274: /* expritem */
{
tSqlExprDestroy((yypminor->yy46));
}
      break;
    case 260: /* union */
{
destroyAllSqlNode((yypminor->yy303));
}
      break;
    case 270: /* sortitem */
{
taosVariantDestroy(&(yypminor->yy43));
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
  {  197,   -1 }, /* (0) program ::= cmd */
  {  198,   -2 }, /* (1) cmd ::= SHOW DATABASES */
  {  198,   -2 }, /* (2) cmd ::= SHOW TOPICS */
  {  198,   -2 }, /* (3) cmd ::= SHOW FUNCTIONS */
  {  198,   -2 }, /* (4) cmd ::= SHOW MNODES */
  {  198,   -2 }, /* (5) cmd ::= SHOW DNODES */
  {  198,   -2 }, /* (6) cmd ::= SHOW ACCOUNTS */
  {  198,   -2 }, /* (7) cmd ::= SHOW USERS */
  {  198,   -2 }, /* (8) cmd ::= SHOW MODULES */
  {  198,   -2 }, /* (9) cmd ::= SHOW QUERIES */
  {  198,   -2 }, /* (10) cmd ::= SHOW CONNECTIONS */
  {  198,   -2 }, /* (11) cmd ::= SHOW STREAMS */
  {  198,   -2 }, /* (12) cmd ::= SHOW VARIABLES */
  {  198,   -2 }, /* (13) cmd ::= SHOW SCORES */
  {  198,   -2 }, /* (14) cmd ::= SHOW GRANTS */
  {  198,   -2 }, /* (15) cmd ::= SHOW VNODES */
  {  198,   -3 }, /* (16) cmd ::= SHOW VNODES ids */
  {  200,    0 }, /* (17) dbPrefix ::= */
  {  200,   -2 }, /* (18) dbPrefix ::= ids DOT */
  {  201,    0 }, /* (19) cpxName ::= */
  {  201,   -2 }, /* (20) cpxName ::= DOT ids */
  {  198,   -5 }, /* (21) cmd ::= SHOW CREATE TABLE ids cpxName */
  {  198,   -5 }, /* (22) cmd ::= SHOW CREATE STABLE ids cpxName */
  {  198,   -4 }, /* (23) cmd ::= SHOW CREATE DATABASE ids */
  {  198,   -3 }, /* (24) cmd ::= SHOW dbPrefix TABLES */
  {  198,   -5 }, /* (25) cmd ::= SHOW dbPrefix TABLES LIKE ids */
  {  198,   -3 }, /* (26) cmd ::= SHOW dbPrefix STABLES */
  {  198,   -5 }, /* (27) cmd ::= SHOW dbPrefix STABLES LIKE ids */
  {  198,   -3 }, /* (28) cmd ::= SHOW dbPrefix VGROUPS */
  {  198,   -4 }, /* (29) cmd ::= SHOW dbPrefix VGROUPS ids */
  {  198,   -5 }, /* (30) cmd ::= DROP TABLE ifexists ids cpxName */
  {  198,   -5 }, /* (31) cmd ::= DROP STABLE ifexists ids cpxName */
  {  198,   -4 }, /* (32) cmd ::= DROP DATABASE ifexists ids */
  {  198,   -4 }, /* (33) cmd ::= DROP TOPIC ifexists ids */
  {  198,   -3 }, /* (34) cmd ::= DROP FUNCTION ids */
  {  198,   -3 }, /* (35) cmd ::= DROP DNODE ids */
  {  198,   -3 }, /* (36) cmd ::= DROP USER ids */
  {  198,   -3 }, /* (37) cmd ::= DROP ACCOUNT ids */
  {  198,   -2 }, /* (38) cmd ::= USE ids */
  {  198,   -3 }, /* (39) cmd ::= DESCRIBE ids cpxName */
  {  198,   -3 }, /* (40) cmd ::= DESC ids cpxName */
  {  198,   -5 }, /* (41) cmd ::= ALTER USER ids PASS ids */
  {  198,   -5 }, /* (42) cmd ::= ALTER USER ids PRIVILEGE ids */
  {  198,   -4 }, /* (43) cmd ::= ALTER DNODE ids ids */
  {  198,   -5 }, /* (44) cmd ::= ALTER DNODE ids ids ids */
  {  198,   -3 }, /* (45) cmd ::= ALTER LOCAL ids */
  {  198,   -4 }, /* (46) cmd ::= ALTER LOCAL ids ids */
  {  198,   -4 }, /* (47) cmd ::= ALTER DATABASE ids alter_db_optr */
  {  198,   -4 }, /* (48) cmd ::= ALTER ACCOUNT ids acct_optr */
  {  198,   -6 }, /* (49) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
  {  198,   -6 }, /* (50) cmd ::= COMPACT VNODES IN LP exprlist RP */
  {  199,   -1 }, /* (51) ids ::= ID */
  {  199,   -1 }, /* (52) ids ::= STRING */
  {  202,   -2 }, /* (53) ifexists ::= IF EXISTS */
  {  202,    0 }, /* (54) ifexists ::= */
  {  206,   -3 }, /* (55) ifnotexists ::= IF NOT EXISTS */
  {  206,    0 }, /* (56) ifnotexists ::= */
  {  198,   -3 }, /* (57) cmd ::= CREATE DNODE ids */
  {  198,   -6 }, /* (58) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
  {  198,   -5 }, /* (59) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
  {  198,   -8 }, /* (60) cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  198,   -9 }, /* (61) cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
  {  198,   -5 }, /* (62) cmd ::= CREATE USER ids PASS ids */
  {  209,    0 }, /* (63) bufsize ::= */
  {  209,   -2 }, /* (64) bufsize ::= BUFSIZE INTEGER */
  {  210,    0 }, /* (65) pps ::= */
  {  210,   -2 }, /* (66) pps ::= PPS INTEGER */
  {  211,    0 }, /* (67) tseries ::= */
  {  211,   -2 }, /* (68) tseries ::= TSERIES INTEGER */
  {  212,    0 }, /* (69) dbs ::= */
  {  212,   -2 }, /* (70) dbs ::= DBS INTEGER */
  {  213,    0 }, /* (71) streams ::= */
  {  213,   -2 }, /* (72) streams ::= STREAMS INTEGER */
  {  214,    0 }, /* (73) storage ::= */
  {  214,   -2 }, /* (74) storage ::= STORAGE INTEGER */
  {  215,    0 }, /* (75) qtime ::= */
  {  215,   -2 }, /* (76) qtime ::= QTIME INTEGER */
  {  216,    0 }, /* (77) users ::= */
  {  216,   -2 }, /* (78) users ::= USERS INTEGER */
  {  217,    0 }, /* (79) conns ::= */
  {  217,   -2 }, /* (80) conns ::= CONNS INTEGER */
  {  218,    0 }, /* (81) state ::= */
  {  218,   -2 }, /* (82) state ::= STATE ids */
  {  204,   -9 }, /* (83) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
  {  219,   -3 }, /* (84) intitemlist ::= intitemlist COMMA intitem */
  {  219,   -1 }, /* (85) intitemlist ::= intitem */
  {  220,   -1 }, /* (86) intitem ::= INTEGER */
  {  221,   -2 }, /* (87) keep ::= KEEP intitemlist */
  {  222,   -2 }, /* (88) cache ::= CACHE INTEGER */
  {  223,   -2 }, /* (89) replica ::= REPLICA INTEGER */
  {  224,   -2 }, /* (90) quorum ::= QUORUM INTEGER */
  {  225,   -2 }, /* (91) days ::= DAYS INTEGER */
  {  226,   -2 }, /* (92) minrows ::= MINROWS INTEGER */
  {  227,   -2 }, /* (93) maxrows ::= MAXROWS INTEGER */
  {  228,   -2 }, /* (94) blocks ::= BLOCKS INTEGER */
  {  229,   -2 }, /* (95) ctime ::= CTIME INTEGER */
  {  230,   -2 }, /* (96) wal ::= WAL INTEGER */
  {  231,   -2 }, /* (97) fsync ::= FSYNC INTEGER */
  {  232,   -2 }, /* (98) comp ::= COMP INTEGER */
  {  233,   -2 }, /* (99) prec ::= PRECISION STRING */
  {  234,   -2 }, /* (100) update ::= UPDATE INTEGER */
  {  235,   -2 }, /* (101) cachelast ::= CACHELAST INTEGER */
  {  207,    0 }, /* (102) db_optr ::= */
  {  207,   -2 }, /* (103) db_optr ::= db_optr cache */
  {  207,   -2 }, /* (104) db_optr ::= db_optr replica */
  {  207,   -2 }, /* (105) db_optr ::= db_optr quorum */
  {  207,   -2 }, /* (106) db_optr ::= db_optr days */
  {  207,   -2 }, /* (107) db_optr ::= db_optr minrows */
  {  207,   -2 }, /* (108) db_optr ::= db_optr maxrows */
  {  207,   -2 }, /* (109) db_optr ::= db_optr blocks */
  {  207,   -2 }, /* (110) db_optr ::= db_optr ctime */
  {  207,   -2 }, /* (111) db_optr ::= db_optr wal */
  {  207,   -2 }, /* (112) db_optr ::= db_optr fsync */
  {  207,   -2 }, /* (113) db_optr ::= db_optr comp */
  {  207,   -2 }, /* (114) db_optr ::= db_optr prec */
  {  207,   -2 }, /* (115) db_optr ::= db_optr keep */
  {  207,   -2 }, /* (116) db_optr ::= db_optr update */
  {  207,   -2 }, /* (117) db_optr ::= db_optr cachelast */
  {  203,    0 }, /* (118) alter_db_optr ::= */
  {  203,   -2 }, /* (119) alter_db_optr ::= alter_db_optr replica */
  {  203,   -2 }, /* (120) alter_db_optr ::= alter_db_optr quorum */
  {  203,   -2 }, /* (121) alter_db_optr ::= alter_db_optr keep */
  {  203,   -2 }, /* (122) alter_db_optr ::= alter_db_optr blocks */
  {  203,   -2 }, /* (123) alter_db_optr ::= alter_db_optr comp */
  {  203,   -2 }, /* (124) alter_db_optr ::= alter_db_optr update */
  {  203,   -2 }, /* (125) alter_db_optr ::= alter_db_optr cachelast */
  {  208,   -1 }, /* (126) typename ::= ids */
  {  208,   -4 }, /* (127) typename ::= ids LP signed RP */
  {  208,   -2 }, /* (128) typename ::= ids UNSIGNED */
  {  236,   -1 }, /* (129) signed ::= INTEGER */
  {  236,   -2 }, /* (130) signed ::= PLUS INTEGER */
  {  236,   -2 }, /* (131) signed ::= MINUS INTEGER */
  {  198,   -3 }, /* (132) cmd ::= CREATE TABLE create_table_args */
  {  198,   -3 }, /* (133) cmd ::= CREATE TABLE create_stable_args */
  {  198,   -3 }, /* (134) cmd ::= CREATE STABLE create_stable_args */
  {  198,   -3 }, /* (135) cmd ::= CREATE TABLE create_table_list */
  {  239,   -1 }, /* (136) create_table_list ::= create_from_stable */
  {  239,   -2 }, /* (137) create_table_list ::= create_table_list create_from_stable */
  {  237,   -6 }, /* (138) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  {  238,  -10 }, /* (139) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  {  240,  -10 }, /* (140) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  {  240,  -13 }, /* (141) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
  {  243,   -3 }, /* (142) tagNamelist ::= tagNamelist COMMA ids */
  {  243,   -1 }, /* (143) tagNamelist ::= ids */
  {  237,   -5 }, /* (144) create_table_args ::= ifnotexists ids cpxName AS select */
  {  241,   -3 }, /* (145) columnlist ::= columnlist COMMA column */
  {  241,   -1 }, /* (146) columnlist ::= column */
  {  245,   -2 }, /* (147) column ::= ids typename */
  {  242,   -3 }, /* (148) tagitemlist ::= tagitemlist COMMA tagitem */
  {  242,   -1 }, /* (149) tagitemlist ::= tagitem */
  {  246,   -1 }, /* (150) tagitem ::= INTEGER */
  {  246,   -1 }, /* (151) tagitem ::= FLOAT */
  {  246,   -1 }, /* (152) tagitem ::= STRING */
  {  246,   -1 }, /* (153) tagitem ::= BOOL */
  {  246,   -1 }, /* (154) tagitem ::= NULL */
  {  246,   -1 }, /* (155) tagitem ::= NOW */
  {  246,   -2 }, /* (156) tagitem ::= MINUS INTEGER */
  {  246,   -2 }, /* (157) tagitem ::= MINUS FLOAT */
  {  246,   -2 }, /* (158) tagitem ::= PLUS INTEGER */
  {  246,   -2 }, /* (159) tagitem ::= PLUS FLOAT */
  {  244,  -14 }, /* (160) select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
  {  244,   -3 }, /* (161) select ::= LP select RP */
  {  260,   -1 }, /* (162) union ::= select */
  {  260,   -4 }, /* (163) union ::= union UNION ALL select */
  {  260,   -3 }, /* (164) union ::= union UNION select */
  {  198,   -1 }, /* (165) cmd ::= union */
  {  244,   -2 }, /* (166) select ::= SELECT selcollist */
  {  261,   -2 }, /* (167) sclp ::= selcollist COMMA */
  {  261,    0 }, /* (168) sclp ::= */
  {  247,   -4 }, /* (169) selcollist ::= sclp distinct expr as */
  {  247,   -2 }, /* (170) selcollist ::= sclp STAR */
  {  264,   -2 }, /* (171) as ::= AS ids */
  {  264,   -1 }, /* (172) as ::= ids */
  {  264,    0 }, /* (173) as ::= */
  {  262,   -1 }, /* (174) distinct ::= DISTINCT */
  {  262,    0 }, /* (175) distinct ::= */
  {  248,   -2 }, /* (176) from ::= FROM tablelist */
  {  248,   -2 }, /* (177) from ::= FROM sub */
  {  266,   -3 }, /* (178) sub ::= LP union RP */
  {  266,   -4 }, /* (179) sub ::= LP union RP ids */
  {  266,   -6 }, /* (180) sub ::= sub COMMA LP union RP ids */
  {  265,   -2 }, /* (181) tablelist ::= ids cpxName */
  {  265,   -3 }, /* (182) tablelist ::= ids cpxName ids */
  {  265,   -4 }, /* (183) tablelist ::= tablelist COMMA ids cpxName */
  {  265,   -5 }, /* (184) tablelist ::= tablelist COMMA ids cpxName ids */
  {  267,   -1 }, /* (185) tmvar ::= VARIABLE */
  {  250,   -4 }, /* (186) interval_option ::= intervalKey LP tmvar RP */
  {  250,   -6 }, /* (187) interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
  {  250,    0 }, /* (188) interval_option ::= */
  {  268,   -1 }, /* (189) intervalKey ::= INTERVAL */
  {  268,   -1 }, /* (190) intervalKey ::= EVERY */
  {  252,    0 }, /* (191) session_option ::= */
  {  252,   -7 }, /* (192) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
  {  253,    0 }, /* (193) windowstate_option ::= */
  {  253,   -4 }, /* (194) windowstate_option ::= STATE_WINDOW LP ids RP */
  {  254,    0 }, /* (195) fill_opt ::= */
  {  254,   -6 }, /* (196) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
  {  254,   -4 }, /* (197) fill_opt ::= FILL LP ID RP */
  {  251,   -4 }, /* (198) sliding_opt ::= SLIDING LP tmvar RP */
  {  251,    0 }, /* (199) sliding_opt ::= */
  {  257,    0 }, /* (200) orderby_opt ::= */
  {  257,   -3 }, /* (201) orderby_opt ::= ORDER BY sortlist */
  {  269,   -4 }, /* (202) sortlist ::= sortlist COMMA item sortorder */
  {  269,   -2 }, /* (203) sortlist ::= item sortorder */
  {  271,   -2 }, /* (204) item ::= ids cpxName */
  {  272,   -1 }, /* (205) sortorder ::= ASC */
  {  272,   -1 }, /* (206) sortorder ::= DESC */
  {  272,    0 }, /* (207) sortorder ::= */
  {  255,    0 }, /* (208) groupby_opt ::= */
  {  255,   -3 }, /* (209) groupby_opt ::= GROUP BY grouplist */
  {  273,   -3 }, /* (210) grouplist ::= grouplist COMMA item */
  {  273,   -1 }, /* (211) grouplist ::= item */
  {  256,    0 }, /* (212) having_opt ::= */
  {  256,   -2 }, /* (213) having_opt ::= HAVING expr */
  {  259,    0 }, /* (214) limit_opt ::= */
  {  259,   -2 }, /* (215) limit_opt ::= LIMIT signed */
  {  259,   -4 }, /* (216) limit_opt ::= LIMIT signed OFFSET signed */
  {  259,   -4 }, /* (217) limit_opt ::= LIMIT signed COMMA signed */
  {  258,    0 }, /* (218) slimit_opt ::= */
  {  258,   -2 }, /* (219) slimit_opt ::= SLIMIT signed */
  {  258,   -4 }, /* (220) slimit_opt ::= SLIMIT signed SOFFSET signed */
  {  258,   -4 }, /* (221) slimit_opt ::= SLIMIT signed COMMA signed */
  {  249,    0 }, /* (222) where_opt ::= */
  {  249,   -2 }, /* (223) where_opt ::= WHERE expr */
  {  263,   -3 }, /* (224) expr ::= LP expr RP */
  {  263,   -1 }, /* (225) expr ::= ID */
  {  263,   -3 }, /* (226) expr ::= ID DOT ID */
  {  263,   -3 }, /* (227) expr ::= ID DOT STAR */
  {  263,   -1 }, /* (228) expr ::= INTEGER */
  {  263,   -2 }, /* (229) expr ::= MINUS INTEGER */
  {  263,   -2 }, /* (230) expr ::= PLUS INTEGER */
  {  263,   -1 }, /* (231) expr ::= FLOAT */
  {  263,   -2 }, /* (232) expr ::= MINUS FLOAT */
  {  263,   -2 }, /* (233) expr ::= PLUS FLOAT */
  {  263,   -1 }, /* (234) expr ::= STRING */
  {  263,   -1 }, /* (235) expr ::= NOW */
  {  263,   -1 }, /* (236) expr ::= VARIABLE */
  {  263,   -2 }, /* (237) expr ::= PLUS VARIABLE */
  {  263,   -2 }, /* (238) expr ::= MINUS VARIABLE */
  {  263,   -1 }, /* (239) expr ::= BOOL */
  {  263,   -1 }, /* (240) expr ::= NULL */
  {  263,   -4 }, /* (241) expr ::= ID LP exprlist RP */
  {  263,   -4 }, /* (242) expr ::= ID LP STAR RP */
  {  263,   -3 }, /* (243) expr ::= expr IS NULL */
  {  263,   -4 }, /* (244) expr ::= expr IS NOT NULL */
  {  263,   -3 }, /* (245) expr ::= expr LT expr */
  {  263,   -3 }, /* (246) expr ::= expr GT expr */
  {  263,   -3 }, /* (247) expr ::= expr LE expr */
  {  263,   -3 }, /* (248) expr ::= expr GE expr */
  {  263,   -3 }, /* (249) expr ::= expr NE expr */
  {  263,   -3 }, /* (250) expr ::= expr EQ expr */
  {  263,   -5 }, /* (251) expr ::= expr BETWEEN expr AND expr */
  {  263,   -3 }, /* (252) expr ::= expr AND expr */
  {  263,   -3 }, /* (253) expr ::= expr OR expr */
  {  263,   -3 }, /* (254) expr ::= expr PLUS expr */
  {  263,   -3 }, /* (255) expr ::= expr MINUS expr */
  {  263,   -3 }, /* (256) expr ::= expr STAR expr */
  {  263,   -3 }, /* (257) expr ::= expr SLASH expr */
  {  263,   -3 }, /* (258) expr ::= expr REM expr */
  {  263,   -3 }, /* (259) expr ::= expr LIKE expr */
  {  263,   -3 }, /* (260) expr ::= expr MATCH expr */
  {  263,   -3 }, /* (261) expr ::= expr NMATCH expr */
  {  263,   -5 }, /* (262) expr ::= expr IN LP exprlist RP */
  {  205,   -3 }, /* (263) exprlist ::= exprlist COMMA expritem */
  {  205,   -1 }, /* (264) exprlist ::= expritem */
  {  274,   -1 }, /* (265) expritem ::= expr */
  {  274,    0 }, /* (266) expritem ::= */
  {  198,   -3 }, /* (267) cmd ::= RESET QUERY CACHE */
  {  198,   -3 }, /* (268) cmd ::= SYNCDB ids REPLICA */
  {  198,   -7 }, /* (269) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
  {  198,   -7 }, /* (270) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
  {  198,   -7 }, /* (271) cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
  {  198,   -7 }, /* (272) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
  {  198,   -7 }, /* (273) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
  {  198,   -8 }, /* (274) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
  {  198,   -9 }, /* (275) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
  {  198,   -7 }, /* (276) cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
  {  198,   -7 }, /* (277) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
  {  198,   -7 }, /* (278) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
  {  198,   -7 }, /* (279) cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
  {  198,   -7 }, /* (280) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
  {  198,   -7 }, /* (281) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
  {  198,   -8 }, /* (282) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
  {  198,   -9 }, /* (283) cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
  {  198,   -7 }, /* (284) cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
  {  198,   -3 }, /* (285) cmd ::= KILL CONNECTION INTEGER */
  {  198,   -5 }, /* (286) cmd ::= KILL STREAM INTEGER COLON INTEGER */
  {  198,   -5 }, /* (287) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 132: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==132);
      case 133: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==133);
      case 134: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==134);
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
      case 16: /* cmd ::= SHOW VNODES ids */
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
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_STB, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 29: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SToken token;
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
      case 40: /* cmd ::= DESC ids cpxName */ yytestcase(yyruleno==40);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 41: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 42: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 43: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 44: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 45: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 46: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 47: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &t);}
        break;
      case 48: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy341);}
        break;
      case 49: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
        break;
      case 50: /* cmd ::= COMPACT VNODES IN LP exprlist RP */
{ setCompactVnodeSql(pInfo, TSDB_SQL_COMPACT_VNODE, yymsp[-1].minor.yy131);}
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
      case 175: /* distinct ::= */ yytestcase(yyruleno==175);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 55: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 57: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 58: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy341);}
        break;
      case 59: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy42, &yymsp[-2].minor.yy0);}
        break;
      case 60: /* cmd ::= CREATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy290, &yymsp[0].minor.yy0, 1);}
        break;
      case 61: /* cmd ::= CREATE AGGREGATE FUNCTION ids AS ids OUTPUTTYPE typename bufsize */
{ setCreateFuncInfo(pInfo, TSDB_SQL_CREATE_FUNCTION, &yymsp[-5].minor.yy0, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy290, &yymsp[0].minor.yy0, 2);}
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
    yylhsminor.yy341.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy341.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy341.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy341.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy341.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy341.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy341.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy341.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy341 = yylhsminor.yy341;
        break;
      case 84: /* intitemlist ::= intitemlist COMMA intitem */
      case 148: /* tagitemlist ::= tagitemlist COMMA tagitem */ yytestcase(yyruleno==148);
{ yylhsminor.yy131 = tListItemAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy43, -1);    }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 85: /* intitemlist ::= intitem */
      case 149: /* tagitemlist ::= tagitem */ yytestcase(yyruleno==149);
{ yylhsminor.yy131 = tListItemAppend(NULL, &yymsp[0].minor.yy43, -1); }
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 86: /* intitem ::= INTEGER */
      case 150: /* tagitem ::= INTEGER */ yytestcase(yyruleno==150);
      case 151: /* tagitem ::= FLOAT */ yytestcase(yyruleno==151);
      case 152: /* tagitem ::= STRING */ yytestcase(yyruleno==152);
      case 153: /* tagitem ::= BOOL */ yytestcase(yyruleno==153);
{ toTSDBType(yymsp[0].minor.yy0.type); taosVariantCreate(&yylhsminor.yy43, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy43 = yylhsminor.yy43;
        break;
      case 87: /* keep ::= KEEP intitemlist */
{ yymsp[-1].minor.yy131 = yymsp[0].minor.yy131; }
        break;
      case 88: /* cache ::= CACHE INTEGER */
      case 89: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==89);
      case 90: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==90);
      case 91: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==91);
      case 92: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==92);
      case 93: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==93);
      case 94: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==94);
      case 95: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==95);
      case 96: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==96);
      case 97: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==97);
      case 98: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==98);
      case 99: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==99);
      case 100: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==100);
      case 101: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==101);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 102: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy42);}
        break;
      case 103: /* db_optr ::= db_optr cache */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 104: /* db_optr ::= db_optr replica */
      case 119: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==119);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 105: /* db_optr ::= db_optr quorum */
      case 120: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==120);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 106: /* db_optr ::= db_optr days */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 107: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 108: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 109: /* db_optr ::= db_optr blocks */
      case 122: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==122);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 110: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 111: /* db_optr ::= db_optr wal */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 112: /* db_optr ::= db_optr fsync */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 113: /* db_optr ::= db_optr comp */
      case 123: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==123);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 114: /* db_optr ::= db_optr prec */
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 115: /* db_optr ::= db_optr keep */
      case 121: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==121);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.keep = yymsp[0].minor.yy131; }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 116: /* db_optr ::= db_optr update */
      case 124: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==124);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 117: /* db_optr ::= db_optr cachelast */
      case 125: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==125);
{ yylhsminor.yy42 = yymsp[-1].minor.yy42; yylhsminor.yy42.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy42 = yylhsminor.yy42;
        break;
      case 118: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy42);}
        break;
      case 126: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy290, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy290 = yylhsminor.yy290;
        break;
      case 127: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy459 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy290, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy459;  // negative value of name length
    tSetColumnType(&yylhsminor.yy290, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy290 = yylhsminor.yy290;
        break;
      case 128: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy290, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 129: /* signed ::= INTEGER */
{ yylhsminor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy459 = yylhsminor.yy459;
        break;
      case 130: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy459 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 131: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy459 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 135: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy272;}
        break;
      case 136: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy96);
  pCreateTable->type = TSQL_CREATE_CTABLE;
  yylhsminor.yy272 = pCreateTable;
}
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 137: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy272->childTableInfo, &yymsp[0].minor.yy96);
  yylhsminor.yy272 = yymsp[-1].minor.yy272;
}
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 138: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-1].minor.yy131, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy272 = yylhsminor.yy272;
        break;
      case 139: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy272 = tSetCreateTableInfo(yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy272 = yylhsminor.yy272;
        break;
      case 140: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy131, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy96 = yylhsminor.yy96;
        break;
      case 141: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy96 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, yymsp[-1].minor.yy131, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy96 = yylhsminor.yy96;
        break;
      case 142: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy0); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 143: /* tagNamelist ::= ids */
{yylhsminor.yy131 = taosArrayInit(4, sizeof(SToken)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 144: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy272 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy256, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy272, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy272 = yylhsminor.yy272;
        break;
      case 145: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy131, &yymsp[0].minor.yy290); yylhsminor.yy131 = yymsp[-2].minor.yy131;  }
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 146: /* columnlist ::= column */
{yylhsminor.yy131 = taosArrayInit(4, sizeof(SField)); taosArrayPush(yylhsminor.yy131, &yymsp[0].minor.yy290);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 147: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy290, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy290);
}
  yymsp[-1].minor.yy290 = yylhsminor.yy290;
        break;
      case 154: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; taosVariantCreate(&yylhsminor.yy43, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type); }
  yymsp[0].minor.yy43 = yylhsminor.yy43;
        break;
      case 155: /* tagitem ::= NOW */
{ yymsp[0].minor.yy0.type = TSDB_DATA_TYPE_TIMESTAMP; taosVariantCreate(&yylhsminor.yy43, yymsp[0].minor.yy0.z, yymsp[0].minor.yy0.n, yymsp[0].minor.yy0.type);}
  yymsp[0].minor.yy43 = yylhsminor.yy43;
        break;
      case 156: /* tagitem ::= MINUS INTEGER */
      case 157: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==157);
      case 158: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==158);
      case 159: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==159);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    taosVariantCreate(&yylhsminor.yy43, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy43 = yylhsminor.yy43;
        break;
      case 160: /* select ::= SELECT selcollist from where_opt interval_option sliding_opt session_option windowstate_option fill_opt groupby_opt having_opt orderby_opt slimit_opt limit_opt */
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-13].minor.yy0, yymsp[-12].minor.yy131, yymsp[-11].minor.yy544, yymsp[-10].minor.yy46, yymsp[-4].minor.yy131, yymsp[-2].minor.yy131, &yymsp[-9].minor.yy530, &yymsp[-7].minor.yy39, &yymsp[-6].minor.yy538, &yymsp[-8].minor.yy0, yymsp[-5].minor.yy131, &yymsp[0].minor.yy55, &yymsp[-1].minor.yy55, yymsp[-3].minor.yy46);
}
  yymsp[-13].minor.yy256 = yylhsminor.yy256;
        break;
      case 161: /* select ::= LP select RP */
{yymsp[-2].minor.yy256 = yymsp[-1].minor.yy256;}
        break;
      case 162: /* union ::= select */
{ yylhsminor.yy303 = setSubclause(NULL, yymsp[0].minor.yy256); }
  yymsp[0].minor.yy303 = yylhsminor.yy303;
        break;
      case 163: /* union ::= union UNION ALL select */
{ yylhsminor.yy303 = appendSelectClause(yymsp[-3].minor.yy303, SQL_TYPE_UNIONALL, yymsp[0].minor.yy256);  }
  yymsp[-3].minor.yy303 = yylhsminor.yy303;
        break;
      case 164: /* union ::= union UNION select */
{ yylhsminor.yy303 = appendSelectClause(yymsp[-2].minor.yy303, SQL_TYPE_UNION, yymsp[0].minor.yy256);  }
  yymsp[-2].minor.yy303 = yylhsminor.yy303;
        break;
      case 165: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy303, NULL, TSDB_SQL_SELECT); }
        break;
      case 166: /* select ::= SELECT selcollist */
{
  yylhsminor.yy256 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy131, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy256 = yylhsminor.yy256;
        break;
      case 167: /* sclp ::= selcollist COMMA */
{yylhsminor.yy131 = yymsp[-1].minor.yy131;}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 168: /* sclp ::= */
      case 200: /* orderby_opt ::= */ yytestcase(yyruleno==200);
{yymsp[1].minor.yy131 = 0;}
        break;
      case 169: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-3].minor.yy131, yymsp[-1].minor.yy46,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 170: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy131 = tSqlExprListAppend(yymsp[-1].minor.yy131, pNode, 0, 0);
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 171: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 172: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 173: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 174: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 176: /* from ::= FROM tablelist */
      case 177: /* from ::= FROM sub */ yytestcase(yyruleno==177);
{yymsp[-1].minor.yy544 = yymsp[0].minor.yy544;}
        break;
      case 178: /* sub ::= LP union RP */
{yymsp[-2].minor.yy544 = addSubquery(NULL, yymsp[-1].minor.yy303, NULL);}
        break;
      case 179: /* sub ::= LP union RP ids */
{yymsp[-3].minor.yy544 = addSubquery(NULL, yymsp[-2].minor.yy303, &yymsp[0].minor.yy0);}
        break;
      case 180: /* sub ::= sub COMMA LP union RP ids */
{yylhsminor.yy544 = addSubquery(yymsp[-5].minor.yy544, yymsp[-2].minor.yy303, &yymsp[0].minor.yy0);}
  yymsp[-5].minor.yy544 = yylhsminor.yy544;
        break;
      case 181: /* tablelist ::= ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy544 = yylhsminor.yy544;
        break;
      case 182: /* tablelist ::= ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy544 = yylhsminor.yy544;
        break;
      case 183: /* tablelist ::= tablelist COMMA ids cpxName */
{
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-3].minor.yy544, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy544 = yylhsminor.yy544;
        break;
      case 184: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy544 = setTableNameList(yymsp[-4].minor.yy544, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy544 = yylhsminor.yy544;
        break;
      case 185: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 186: /* interval_option ::= intervalKey LP tmvar RP */
{yylhsminor.yy530.interval = yymsp[-1].minor.yy0; yylhsminor.yy530.offset.n = 0; yylhsminor.yy530.token = yymsp[-3].minor.yy310;}
  yymsp[-3].minor.yy530 = yylhsminor.yy530;
        break;
      case 187: /* interval_option ::= intervalKey LP tmvar COMMA tmvar RP */
{yylhsminor.yy530.interval = yymsp[-3].minor.yy0; yylhsminor.yy530.offset = yymsp[-1].minor.yy0;   yylhsminor.yy530.token = yymsp[-5].minor.yy310;}
  yymsp[-5].minor.yy530 = yylhsminor.yy530;
        break;
      case 188: /* interval_option ::= */
{memset(&yymsp[1].minor.yy530, 0, sizeof(yymsp[1].minor.yy530));}
        break;
      case 189: /* intervalKey ::= INTERVAL */
{yymsp[0].minor.yy310 = TK_INTERVAL;}
        break;
      case 190: /* intervalKey ::= EVERY */
{yymsp[0].minor.yy310 = TK_EVERY;   }
        break;
      case 191: /* session_option ::= */
{yymsp[1].minor.yy39.col.n = 0; yymsp[1].minor.yy39.gap.n = 0;}
        break;
      case 192: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy39.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy39.gap = yymsp[-1].minor.yy0;
}
        break;
      case 193: /* windowstate_option ::= */
{ yymsp[1].minor.yy538.col.n = 0; yymsp[1].minor.yy538.col.z = NULL;}
        break;
      case 194: /* windowstate_option ::= STATE_WINDOW LP ids RP */
{ yymsp[-3].minor.yy538.col = yymsp[-1].minor.yy0; }
        break;
      case 195: /* fill_opt ::= */
{ yymsp[1].minor.yy131 = 0;     }
        break;
      case 196: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    SVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    taosVariantCreate(&A, yymsp[-3].minor.yy0.z, yymsp[-3].minor.yy0.n, yymsp[-3].minor.yy0.type);

    tListItemInsert(yymsp[-1].minor.yy131, &A, -1, 0);
    yymsp[-5].minor.yy131 = yymsp[-1].minor.yy131;
}
        break;
      case 197: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy131 = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 198: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 199: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 201: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 202: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy131 = tListItemAppend(yymsp[-3].minor.yy131, &yymsp[-1].minor.yy43, yymsp[0].minor.yy44);
}
  yymsp[-3].minor.yy131 = yylhsminor.yy131;
        break;
      case 203: /* sortlist ::= item sortorder */
{
  yylhsminor.yy131 = tListItemAppend(NULL, &yymsp[-1].minor.yy43, yymsp[0].minor.yy44);
}
  yymsp[-1].minor.yy131 = yylhsminor.yy131;
        break;
      case 204: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  taosVariantCreate(&yylhsminor.yy43, yymsp[-1].minor.yy0.z, yymsp[-1].minor.yy0.n, yymsp[-1].minor.yy0.type);
}
  yymsp[-1].minor.yy43 = yylhsminor.yy43;
        break;
      case 205: /* sortorder ::= ASC */
{ yymsp[0].minor.yy44 = TSDB_ORDER_ASC; }
        break;
      case 206: /* sortorder ::= DESC */
{ yymsp[0].minor.yy44 = TSDB_ORDER_DESC;}
        break;
      case 207: /* sortorder ::= */
{ yymsp[1].minor.yy44 = TSDB_ORDER_ASC; }
        break;
      case 208: /* groupby_opt ::= */
{ yymsp[1].minor.yy131 = 0;}
        break;
      case 209: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy131 = yymsp[0].minor.yy131;}
        break;
      case 210: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy131 = tListItemAppend(yymsp[-2].minor.yy131, &yymsp[0].minor.yy43, -1);
}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 211: /* grouplist ::= item */
{
  yylhsminor.yy131 = tListItemAppend(NULL, &yymsp[0].minor.yy43, -1);
}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 212: /* having_opt ::= */
      case 222: /* where_opt ::= */ yytestcase(yyruleno==222);
      case 266: /* expritem ::= */ yytestcase(yyruleno==266);
{yymsp[1].minor.yy46 = 0;}
        break;
      case 213: /* having_opt ::= HAVING expr */
      case 223: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==223);
{yymsp[-1].minor.yy46 = yymsp[0].minor.yy46;}
        break;
      case 214: /* limit_opt ::= */
      case 218: /* slimit_opt ::= */ yytestcase(yyruleno==218);
{yymsp[1].minor.yy55.limit = -1; yymsp[1].minor.yy55.offset = 0;}
        break;
      case 215: /* limit_opt ::= LIMIT signed */
      case 219: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==219);
{yymsp[-1].minor.yy55.limit = yymsp[0].minor.yy459;  yymsp[-1].minor.yy55.offset = 0;}
        break;
      case 216: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy55.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy55.offset = yymsp[0].minor.yy459;}
        break;
      case 217: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy55.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy55.offset = yymsp[-2].minor.yy459;}
        break;
      case 220: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy55.limit = yymsp[-2].minor.yy459;  yymsp[-3].minor.yy55.offset = yymsp[0].minor.yy459;}
        break;
      case 221: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy55.limit = yymsp[0].minor.yy459;  yymsp[-3].minor.yy55.offset = yymsp[-2].minor.yy459;}
        break;
      case 224: /* expr ::= LP expr RP */
{yylhsminor.yy46 = yymsp[-1].minor.yy46; yylhsminor.yy46->exprToken.z = yymsp[-2].minor.yy0.z; yylhsminor.yy46->exprToken.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 225: /* expr ::= ID */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 226: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 227: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 228: /* expr ::= INTEGER */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 229: /* expr ::= MINUS INTEGER */
      case 230: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==230);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 231: /* expr ::= FLOAT */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 232: /* expr ::= MINUS FLOAT */
      case 233: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==233);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 234: /* expr ::= STRING */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 235: /* expr ::= NOW */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 236: /* expr ::= VARIABLE */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 237: /* expr ::= PLUS VARIABLE */
      case 238: /* expr ::= MINUS VARIABLE */ yytestcase(yyruleno==238);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_VARIABLE; yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_VARIABLE);}
  yymsp[-1].minor.yy46 = yylhsminor.yy46;
        break;
      case 239: /* expr ::= BOOL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 240: /* expr ::= NULL */
{ yylhsminor.yy46 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NULL);}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 241: /* expr ::= ID LP exprlist RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(yymsp[-1].minor.yy131, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 242: /* expr ::= ID LP STAR RP */
{ tRecordFuncName(pInfo->funcs, &yymsp[-3].minor.yy0); yylhsminor.yy46 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 243: /* expr ::= expr IS NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 244: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-3].minor.yy46, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy46 = yylhsminor.yy46;
        break;
      case 245: /* expr ::= expr LT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 246: /* expr ::= expr GT expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GT);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 247: /* expr ::= expr LE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 248: /* expr ::= expr GE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_GE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 249: /* expr ::= expr NE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 250: /* expr ::= expr EQ expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_EQ);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 251: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy46); yylhsminor.yy46 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy46, yymsp[-2].minor.yy46, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy46, TK_LE), TK_AND);}
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 252: /* expr ::= expr AND expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_AND);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 253: /* expr ::= expr OR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_OR); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 254: /* expr ::= expr PLUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_PLUS);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 255: /* expr ::= expr MINUS expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MINUS); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 256: /* expr ::= expr STAR expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_STAR);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 257: /* expr ::= expr SLASH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_DIVIDE);}
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 258: /* expr ::= expr REM expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_REM);   }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 259: /* expr ::= expr LIKE expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_LIKE);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 260: /* expr ::= expr MATCH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_MATCH);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 261: /* expr ::= expr NMATCH expr */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-2].minor.yy46, yymsp[0].minor.yy46, TK_NMATCH);  }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 262: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy46 = tSqlExprCreate(yymsp[-4].minor.yy46, (tSqlExpr*)yymsp[-1].minor.yy131, TK_IN); }
  yymsp[-4].minor.yy46 = yylhsminor.yy46;
        break;
      case 263: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy131 = tSqlExprListAppend(yymsp[-2].minor.yy131,yymsp[0].minor.yy46,0, 0);}
  yymsp[-2].minor.yy131 = yylhsminor.yy131;
        break;
      case 264: /* exprlist ::= expritem */
{yylhsminor.yy131 = tSqlExprListAppend(0,yymsp[0].minor.yy46,0, 0);}
  yymsp[0].minor.yy131 = yylhsminor.yy131;
        break;
      case 265: /* expritem ::= expr */
{yylhsminor.yy46 = yymsp[0].minor.yy46;}
  yymsp[0].minor.yy46 = yylhsminor.yy46;
        break;
      case 267: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 268: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 269: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 270: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 271: /* cmd ::= ALTER TABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 272: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 273: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 274: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 275: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy43, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 276: /* cmd ::= ALTER TABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 277: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 278: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 279: /* cmd ::= ALTER STABLE ids cpxName MODIFY COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_CHANGE_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 280: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 281: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 282: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tListItemAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 283: /* cmd ::= ALTER STABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tListItemAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tListItemAppend(A, &yymsp[0].minor.yy43, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 284: /* cmd ::= ALTER STABLE ids cpxName MODIFY TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy131, NULL, TSDB_ALTER_TABLE_MODIFY_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 285: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 286: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 287: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
