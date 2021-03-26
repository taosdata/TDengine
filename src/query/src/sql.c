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
#define YYNOCODE 262
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SLimitVal yy18;
  SFromInfo* yy70;
  SSessionWindowVal yy87;
  SCreateDbInfo yy94;
  int yy116;
  SSubclauseInfo* yy141;
  tSqlExpr* yy170;
  SCreateTableSql* yy194;
  tVariant yy218;
  SIntervalVal yy220;
  SCreatedTableInfo yy252;
  SQuerySqlNode* yy254;
  SCreateAcctInfo yy419;
  SArray* yy429;
  TAOS_FIELD yy451;
  int64_t yy481;
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
#define YYNSTATE             315
#define YYNRULE              266
#define YYNRULE_WITH_ACTION  266
#define YYNTOKEN             187
#define YY_MAX_SHIFT         314
#define YY_MIN_SHIFTREDUCE   505
#define YY_MAX_SHIFTREDUCE   770
#define YY_ERROR_ACTION      771
#define YY_ACCEPT_ACTION     772
#define YY_NO_ACTION         773
#define YY_MIN_REDUCE        774
#define YY_MAX_REDUCE        1039
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
#define YY_ACTTAB_COUNT (679)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   133,  552,  202,  312,  206,  140,  941,  226,  140,  553,
 /*    10 */   772,  314,   17,   47,   48,  140,   51,   52,   30,  181,
 /*    20 */   214,   41,  181,   50,  262,   55,   53,   57,   54, 1020,
 /*    30 */   920,  209, 1021,   46,   45,  179,  181,   44,   43,   42,
 /*    40 */    47,   48,  918,   51,   52,  208, 1021,  214,   41,  552,
 /*    50 */    50,  262,   55,   53,   57,   54,  932,  553,  185,  203,
 /*    60 */    46,   45,  917,  247,   44,   43,   42,   48,  938,   51,
 /*    70 */    52,  242,  972,  214,   41,   79,   50,  262,   55,   53,
 /*    80 */    57,   54,  973,  631,  257,   30,   46,   45,  278,  225,
 /*    90 */    44,   43,   42,  506,  507,  508,  509,  510,  511,  512,
 /*   100 */   513,  514,  515,  516,  517,  518,  313,  552,   85,  231,
 /*   110 */    70,  288,  287,   47,   48,  553,   51,   52,  298,  219,
 /*   120 */   214,   41,  552,   50,  262,   55,   53,   57,   54,  916,
 /*   130 */   553,  105,  717,   46,   45, 1017,  298,   44,   43,   42,
 /*   140 */    47,   49,  908,   51,   52,  920,  140,  214,   41,  234,
 /*   150 */    50,  262,   55,   53,   57,   54, 1016,  238,  237,  227,
 /*   160 */    46,   45,  285,  284,   44,   43,   42,   23,  276,  307,
 /*   170 */   306,  275,  274,  273,  305,  272,  304,  303,  302,  271,
 /*   180 */   301,  300,  880,   30,  868,  869,  870,  871,  872,  873,
 /*   190 */   874,  875,  876,  877,  878,  879,  881,  882,   51,   52,
 /*   200 */    18,  670,  214,   41,  906,   50,  262,   55,   53,   57,
 /*   210 */    54,  259,  221,   78,   30,   46,   45,  190,   30,   44,
 /*   220 */    43,   42,   12,  191,  217,   25,   84,  917,   81,  118,
 /*   230 */   117,  189,   74,  213,  730,  308, 1015,  721,  920,  724,
 /*   240 */    36,  727,  888,  213,  730,  886,  887,  721,  932,  724,
 /*   250 */   889,  727,  891,  892,  890,  218,  893,  894,  917,  281,
 /*   260 */    74,  920,  917,  204,  677,  210,  211,   69,   36,  261,
 /*   270 */    30,   23,  245,  307,  306,  210,  211,  719,  305,  674,
 /*   280 */   304,  303,  302,  278,  301,  300,   30,   44,   43,   42,
 /*   290 */   241,  198,   68,   30,   55,   53,   57,   54,  197,  905,
 /*   300 */   819,  223,   46,   45,  166,  914,   44,   43,   42,  103,
 /*   310 */   108,  282,  263,  720,  917,   97,  107,  113,  116,  106,
 /*   320 */    28,  655,   80,  268,  652,  110,  653,  286,  654,   67,
 /*   330 */   917,  630,    5,  156,  290,   71,   56,  917,   33,  155,
 /*   340 */    92,   87,   91,    1,  154,   82,   56,  220,  199,  729,
 /*   350 */    46,   45,  228,  229,   44,   43,   42, 1031,  828,  729,
 /*   360 */   174,  170,  166,    3,  167,  728,  172,  169,  121,  120,
 /*   370 */   119,  224,  698,  699,  280,  728,  903,  904,   29,  907,
 /*   380 */   311,  310,  126,  820,  667,  243,  683,  166,  689,   31,
 /*   390 */   135,   24,   60,  690,  750,  731,  212,   20,   19,   19,
 /*   400 */   723,  722,  726,  725,   61,   64,  641,  265,  733,  643,
 /*   410 */    31,   31,  267,   60,  642,  659,   83,  660,   60,  183,
 /*   420 */    96,   95,   14,   13,  184,   65,   62,  657,  186,  658,
 /*   430 */   656,  102,  101,  115,  114,  131,  129,   16,   15,  983,
 /*   440 */     6,  180,  187,  188,  194,  195,  193,  178,  192,  182,
 /*   450 */   919,  239,  982,  215,  979,  978,  216,  289,  132,   39,
 /*   460 */   965,  940,  948,  950,  134,  964,  933,  138,  246,  151,
 /*   470 */   915,  130,  682,  150,  248,  913,  152,  153,  205,  149,
 /*   480 */   254,  831,  270,  256,   37,  260,  930,   66,  143,  141,
 /*   490 */   176,   63,  250,   34,  255,  279,  827,   58, 1036,  142,
 /*   500 */    93, 1035, 1033,  157,  283, 1030,  258,  144,   99,  145,
 /*   510 */  1029, 1027,  158,  849,   35,   32,   38,  177,  816,  109,
 /*   520 */   814,  111,  112,  812,  811,  230,  252,  168,  809,  808,
 /*   530 */   807,  806,  805,  804,  171,  173,  801,  799,  797,  795,
 /*   540 */   793,  175,  249,  244,   72,   75,   40,  251,  299,  966,
 /*   550 */   104,  291,  292,  293,  294,  295,  296,  297,  200,  222,
 /*   560 */   269,  309,  770,  232,  233,  769,  201,  196,   88,  236,
 /*   570 */    89,  235,  768,  756,  755,  810,  240,  245,  264,  122,
 /*   580 */     8,  123,  161,  160,  850,  159,  162,  163,  165,  164,
 /*   590 */   803,    2,  124,  802,  794,  884,  125,  662,    4,   76,
 /*   600 */    73,  684,  136,  137,  148,  146,  147,  896,  687,   77,
 /*   610 */   207,  253,    9,  691,  139,   26,   10,  732,   27,    7,
 /*   620 */    11,   21,  734,   22,   86,  266,  594,  590,   84,  588,
 /*   630 */   587,  586,  583,  556,  277,   31,   94,   90,   98,   59,
 /*   640 */   633,  632,  629,  100,  578,  576,  568,  574,  570,  572,
 /*   650 */   566,  564,  597,  596,  595,  593,  592,  591,  589,  585,
 /*   660 */   584,   60,  554,  522,  520,  774,  773,  773,  773,  773,
 /*   670 */   773,  773,  773,  773,  773,  773,  773,  127,  128,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   190,    1,  189,  190,  209,  190,  190,  190,  190,    9,
 /*    10 */   187,  188,  251,   13,   14,  190,   16,   17,  190,  251,
 /*    20 */    20,   21,  251,   23,   24,   25,   26,   27,   28,  261,
 /*    30 */   235,  260,  261,   33,   34,  251,  251,   37,   38,   39,
 /*    40 */    13,   14,  225,   16,   17,  260,  261,   20,   21,    1,
 /*    50 */    23,   24,   25,   26,   27,   28,  233,    9,  251,  231,
 /*    60 */    33,   34,  234,  253,   37,   38,   39,   14,  252,   16,
 /*    70 */    17,  248,  257,   20,   21,  257,   23,   24,   25,   26,
 /*    80 */    27,   28,  257,    5,  259,  190,   33,   34,   79,   67,
 /*    90 */    37,   38,   39,   45,   46,   47,   48,   49,   50,   51,
 /*   100 */    52,   53,   54,   55,   56,   57,   58,    1,  196,   61,
 /*   110 */   110,   33,   34,   13,   14,    9,   16,   17,   81,  209,
 /*   120 */    20,   21,    1,   23,   24,   25,   26,   27,   28,  234,
 /*   130 */     9,   76,  105,   33,   34,  251,   81,   37,   38,   39,
 /*   140 */    13,   14,  230,   16,   17,  235,  190,   20,   21,  135,
 /*   150 */    23,   24,   25,   26,   27,   28,  251,  143,  144,  137,
 /*   160 */    33,   34,  140,  141,   37,   38,   39,   88,   89,   90,
 /*   170 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   180 */   101,  102,  208,  190,  210,  211,  212,  213,  214,  215,
 /*   190 */   216,  217,  218,  219,  220,  221,  222,  223,   16,   17,
 /*   200 */    44,   37,   20,   21,    0,   23,   24,   25,   26,   27,
 /*   210 */    28,  255,  209,  257,  190,   33,   34,   61,  190,   37,
 /*   220 */    38,   39,  104,   67,  231,  104,  108,  234,  110,   73,
 /*   230 */    74,   75,  104,    1,    2,  209,  251,    5,  235,    7,
 /*   240 */   112,    9,  208,    1,    2,  211,  212,    5,  233,    7,
 /*   250 */   216,    9,  218,  219,  220,  231,  222,  223,  234,  231,
 /*   260 */   104,  235,  234,  248,  105,   33,   34,  196,  112,   37,
 /*   270 */   190,   88,  113,   90,   91,   33,   34,    1,   95,  115,
 /*   280 */    97,   98,   99,   79,  101,  102,  190,   37,   38,   39,
 /*   290 */   134,  251,  136,  190,   25,   26,   27,   28,  142,  228,
 /*   300 */   195,   67,   33,   34,  199,  190,   37,   38,   39,   62,
 /*   310 */    63,  231,   15,   37,  234,   68,   69,   70,   71,   72,
 /*   320 */   104,    2,  236,  107,    5,   78,    7,  231,    9,  104,
 /*   330 */   234,  106,   62,   63,  231,  249,  104,  234,   68,   69,
 /*   340 */    70,   71,   72,  197,  198,  196,  104,  232,  251,  117,
 /*   350 */    33,   34,   33,   34,   37,   38,   39,  235,  195,  117,
 /*   360 */    62,   63,  199,  193,  194,  133,   68,   69,   70,   71,
 /*   370 */    72,  137,  124,  125,  140,  133,  227,  228,  229,  230,
 /*   380 */    64,   65,   66,  195,  109,  105,  105,  199,  105,  109,
 /*   390 */   109,  116,  109,  105,  105,  105,   60,  109,  109,  109,
 /*   400 */     5,    5,    7,    7,  109,  109,  105,  105,  111,  105,
 /*   410 */   109,  109,  105,  109,  105,    5,  109,    7,  109,  251,
 /*   420 */   138,  139,  138,  139,  251,  129,  131,    5,  251,    7,
 /*   430 */   111,  138,  139,   76,   77,   62,   63,  138,  139,  226,
 /*   440 */   104,  251,  251,  251,  251,  251,  251,  251,  251,  251,
 /*   450 */   235,  190,  226,  226,  226,  226,  226,  226,  190,  250,
 /*   460 */   258,  190,  190,  190,  190,  258,  233,  190,  233,  190,
 /*   470 */   233,   60,  117,  237,  254,  190,  190,  190,  254,  238,
 /*   480 */   120,  190,  190,  121,  190,  122,  247,  128,  244,  246,
 /*   490 */   190,  130,  254,  190,  254,  190,  190,  127,  190,  245,
 /*   500 */   190,  190,  190,  190,  190,  190,  126,  243,  190,  242,
 /*   510 */   190,  190,  190,  190,  190,  190,  190,  190,  190,  190,
 /*   520 */   190,  190,  190,  190,  190,  190,  119,  190,  190,  190,
 /*   530 */   190,  190,  190,  190,  190,  190,  190,  190,  190,  190,
 /*   540 */   190,  190,  118,  191,  191,  191,  132,  191,  103,  191,
 /*   550 */    87,   86,   50,   83,   85,   54,   84,   82,  191,  191,
 /*   560 */   191,   79,    5,  145,    5,    5,  191,  191,  196,    5,
 /*   570 */   196,  145,    5,   90,   89,  191,  135,  113,  107,  192,
 /*   580 */   104,  192,  201,  205,  207,  206,  204,  202,  200,  203,
 /*   590 */   191,  197,  192,  191,  191,  224,  192,  105,  193,  109,
 /*   600 */   114,  105,  104,  109,  239,  241,  240,  224,  105,  104,
 /*   610 */     1,  104,  123,  105,  104,  109,  123,  105,  109,  104,
 /*   620 */   104,  104,  111,  104,   76,  107,    9,    5,  108,    5,
 /*   630 */     5,    5,    5,   80,   15,  109,  139,   76,  139,   16,
 /*   640 */     5,    5,  105,  139,    5,    5,    5,    5,    5,    5,
 /*   650 */     5,    5,    5,    5,    5,    5,    5,    5,    5,    5,
 /*   660 */     5,  109,   80,   60,   59,    0,  262,  262,  262,  262,
 /*   670 */   262,  262,  262,  262,  262,  262,  262,   21,   21,  262,
 /*   680 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   690 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   700 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   710 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   720 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   730 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   740 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   750 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   760 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   770 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   780 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   790 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   800 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   810 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   820 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   830 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   840 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   850 */   262,  262,  262,  262,  262,  262,  262,  262,  262,  262,
 /*   860 */   262,  262,  262,  262,  262,  262,
};
#define YY_SHIFT_COUNT    (314)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (665)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   156,   79,   79,  183,  183,    9,  232,  242,  106,  106,
 /*    10 */   106,  106,  106,  106,  106,  106,  106,    0,   48,  242,
 /*    20 */   319,  319,  319,  319,  121,  128,  106,  106,  106,  204,
 /*    30 */   106,  106,   55,    9,   37,   37,  679,  679,  679,  242,
 /*    40 */   242,  242,  242,  242,  242,  242,  242,  242,  242,  242,
 /*    50 */   242,  242,  242,  242,  242,  242,  242,  242,  242,  319,
 /*    60 */   319,   78,   78,   78,   78,   78,   78,   78,  106,  106,
 /*    70 */   106,  164,  106,  128,  128,  106,  106,  106,  248,  248,
 /*    80 */   275,  128,  106,  106,  106,  106,  106,  106,  106,  106,
 /*    90 */   106,  106,  106,  106,  106,  106,  106,  106,  106,  106,
 /*   100 */   106,  106,  106,  106,  106,  106,  106,  106,  106,  106,
 /*   110 */   106,  106,  106,  106,  106,  106,  106,  106,  106,  106,
 /*   120 */   106,  106,  106,  106,  106,  106,  106,  106,  106,  106,
 /*   130 */   106,  106,  411,  411,  411,  355,  355,  355,  411,  355,
 /*   140 */   411,  359,  361,  370,  363,  380,  362,  360,  407,  424,
 /*   150 */   414,  411,  411,  411,  445,    9,    9,  411,  411,  463,
 /*   160 */   465,  502,  470,  469,  501,  472,  475,  445,  411,  482,
 /*   170 */   482,  411,  482,  411,  482,  411,  679,  679,   27,  100,
 /*   180 */   127,  100,  100,   53,  182,  269,  269,  269,  269,  247,
 /*   190 */   270,  298,  317,  317,  317,  317,   22,   14,  250,  250,
 /*   200 */   118,  234,  316,  280,  159,  281,  283,  288,  289,  290,
 /*   210 */   395,  396,  276,  336,  297,  295,  296,  301,  302,  304,
 /*   220 */   307,  309,  216,  282,  284,  293,  225,  299,  410,  422,
 /*   230 */   357,  373,  557,  418,  559,  560,  426,  564,  567,  483,
 /*   240 */   485,  441,  464,  471,  476,  486,  492,  490,  496,  498,
 /*   250 */   503,  494,  505,  609,  507,  508,  510,  506,  489,  509,
 /*   260 */   493,  512,  515,  511,  516,  471,  517,  518,  519,  520,
 /*   270 */   548,  617,  622,  624,  625,  626,  627,  553,  619,  561,
 /*   280 */   497,  526,  526,  623,  499,  504,  526,  635,  636,  537,
 /*   290 */   526,  639,  640,  641,  642,  643,  644,  645,  646,  647,
 /*   300 */   648,  649,  650,  651,  652,  653,  654,  655,  552,  582,
 /*   310 */   656,  657,  603,  605,  665,
};
#define YY_REDUCE_COUNT (177)
#define YY_REDUCE_MIN   (-239)
#define YY_REDUCE_MAX   (405)
static const short yy_reduce_ofst[] = {
 /*     0 */  -177,  -26,  -26,   34,   34,  149, -229, -215, -172, -175,
 /*    10 */   -44,   -7,   24,   28,   80,   96,  103, -184, -187, -232,
 /*    20 */  -205,  -90,    3,   26, -190,   15, -185, -182,  115,  -88,
 /*    30 */  -183, -105,  105,   71,  163,  188,   86,  146,  170, -239,
 /*    40 */  -216, -193, -116,  -95,  -15,   40,   97,  168,  173,  177,
 /*    50 */   190,  191,  192,  193,  194,  195,  196,  197,  198,  122,
 /*    60 */   215,  213,  226,  227,  228,  229,  230,  231,  261,  268,
 /*    70 */   271,  209,  272,  233,  235,  273,  274,  277,  202,  207,
 /*    80 */   236,  237,  279,  285,  286,  287,  291,  292,  294,  300,
 /*    90 */   303,  305,  306,  308,  310,  311,  312,  313,  314,  315,
 /*   100 */   318,  320,  321,  322,  323,  324,  325,  326,  327,  328,
 /*   110 */   329,  330,  331,  332,  333,  334,  335,  337,  338,  339,
 /*   120 */   340,  341,  342,  343,  344,  345,  346,  347,  348,  349,
 /*   130 */   350,  351,  352,  353,  354,  220,  224,  238,  356,  240,
 /*   140 */   358,  239,  243,  254,  244,  264,  267,  364,  366,  365,
 /*   150 */   241,  367,  368,  369,  371,  372,  374,  375,  376,  377,
 /*   160 */   379,  378,  381,  382,  385,  386,  388,  383,  384,  387,
 /*   170 */   389,  399,  400,  402,  404,  403,  394,  405,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   771,  883,  829,  895,  817,  826, 1023, 1023,  771,  771,
 /*    10 */   771,  771,  771,  771,  771,  771,  771,  942,  790, 1023,
 /*    20 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  826,
 /*    30 */   771,  771,  832,  826,  832,  832,  937,  867,  885,  771,
 /*    40 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    50 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    60 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    70 */   771,  944,  947,  771,  771,  949,  771,  771,  969,  969,
 /*    80 */   935,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*    90 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   100 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  815,
 /*   110 */   771,  813,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   120 */   771,  771,  771,  771,  771,  771,  800,  771,  771,  771,
 /*   130 */   771,  771,  792,  792,  792,  771,  771,  771,  792,  771,
 /*   140 */   792,  976,  980,  974,  962,  970,  961,  957,  955,  954,
 /*   150 */   984,  792,  792,  792,  830,  826,  826,  792,  792,  848,
 /*   160 */   846,  844,  836,  842,  838,  840,  834,  818,  792,  824,
 /*   170 */   824,  792,  824,  792,  824,  792,  867,  885,  771,  985,
 /*   180 */   771, 1022,  975, 1012, 1011, 1018, 1010, 1009, 1008,  771,
 /*   190 */   771,  771, 1004, 1005, 1007, 1006,  771,  771, 1014, 1013,
 /*   200 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   210 */   771,  771,  771,  987,  771,  981,  977,  771,  771,  771,
 /*   220 */   771,  771,  771,  771,  771,  771,  897,  771,  771,  771,
 /*   230 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   240 */   771,  771,  934,  771,  771,  771,  771,  945,  771,  771,
 /*   250 */   771,  771,  771,  771,  771,  771,  771,  971,  771,  963,
 /*   260 */   771,  771,  771,  771,  771,  909,  771,  771,  771,  771,
 /*   270 */   771,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   280 */   771, 1034, 1032,  771,  771,  771, 1028,  771,  771,  771,
 /*   290 */  1026,  771,  771,  771,  771,  771,  771,  771,  771,  771,
 /*   300 */   771,  771,  771,  771,  771,  771,  771,  771,  851,  771,
 /*   310 */   798,  796,  771,  788,  771,
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
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    1,  /*     STABLE => ID */
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
  /*   63 */ "DATABASE",
  /*   64 */ "TABLES",
  /*   65 */ "STABLES",
  /*   66 */ "VGROUPS",
  /*   67 */ "DROP",
  /*   68 */ "STABLE",
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
  /*  139 */ "TAG",
  /*  140 */ "CHANGE",
  /*  141 */ "SET",
  /*  142 */ "KILL",
  /*  143 */ "CONNECTION",
  /*  144 */ "STREAM",
  /*  145 */ "COLON",
  /*  146 */ "ABORT",
  /*  147 */ "AFTER",
  /*  148 */ "ATTACH",
  /*  149 */ "BEFORE",
  /*  150 */ "BEGIN",
  /*  151 */ "CASCADE",
  /*  152 */ "CLUSTER",
  /*  153 */ "CONFLICT",
  /*  154 */ "COPY",
  /*  155 */ "DEFERRED",
  /*  156 */ "DELIMITERS",
  /*  157 */ "DETACH",
  /*  158 */ "EACH",
  /*  159 */ "END",
  /*  160 */ "EXPLAIN",
  /*  161 */ "FAIL",
  /*  162 */ "FOR",
  /*  163 */ "IGNORE",
  /*  164 */ "IMMEDIATE",
  /*  165 */ "INITIALLY",
  /*  166 */ "INSTEAD",
  /*  167 */ "MATCH",
  /*  168 */ "KEY",
  /*  169 */ "OF",
  /*  170 */ "RAISE",
  /*  171 */ "REPLACE",
  /*  172 */ "RESTRICT",
  /*  173 */ "ROW",
  /*  174 */ "STATEMENT",
  /*  175 */ "TRIGGER",
  /*  176 */ "VIEW",
  /*  177 */ "SEMI",
  /*  178 */ "NONE",
  /*  179 */ "PREV",
  /*  180 */ "LINEAR",
  /*  181 */ "IMPORT",
  /*  182 */ "TBNAME",
  /*  183 */ "JOIN",
  /*  184 */ "INSERT",
  /*  185 */ "INTO",
  /*  186 */ "VALUES",
  /*  187 */ "program",
  /*  188 */ "cmd",
  /*  189 */ "dbPrefix",
  /*  190 */ "ids",
  /*  191 */ "cpxName",
  /*  192 */ "ifexists",
  /*  193 */ "alter_db_optr",
  /*  194 */ "alter_topic_optr",
  /*  195 */ "acct_optr",
  /*  196 */ "ifnotexists",
  /*  197 */ "db_optr",
  /*  198 */ "topic_optr",
  /*  199 */ "pps",
  /*  200 */ "tseries",
  /*  201 */ "dbs",
  /*  202 */ "streams",
  /*  203 */ "storage",
  /*  204 */ "qtime",
  /*  205 */ "users",
  /*  206 */ "conns",
  /*  207 */ "state",
  /*  208 */ "keep",
  /*  209 */ "tagitemlist",
  /*  210 */ "cache",
  /*  211 */ "replica",
  /*  212 */ "quorum",
  /*  213 */ "days",
  /*  214 */ "minrows",
  /*  215 */ "maxrows",
  /*  216 */ "blocks",
  /*  217 */ "ctime",
  /*  218 */ "wal",
  /*  219 */ "fsync",
  /*  220 */ "comp",
  /*  221 */ "prec",
  /*  222 */ "update",
  /*  223 */ "cachelast",
  /*  224 */ "partitions",
  /*  225 */ "typename",
  /*  226 */ "signed",
  /*  227 */ "create_table_args",
  /*  228 */ "create_stable_args",
  /*  229 */ "create_table_list",
  /*  230 */ "create_from_stable",
  /*  231 */ "columnlist",
  /*  232 */ "tagNamelist",
  /*  233 */ "select",
  /*  234 */ "column",
  /*  235 */ "tagitem",
  /*  236 */ "selcollist",
  /*  237 */ "from",
  /*  238 */ "where_opt",
  /*  239 */ "interval_opt",
  /*  240 */ "session_option",
  /*  241 */ "fill_opt",
  /*  242 */ "sliding_opt",
  /*  243 */ "groupby_opt",
  /*  244 */ "orderby_opt",
  /*  245 */ "having_opt",
  /*  246 */ "slimit_opt",
  /*  247 */ "limit_opt",
  /*  248 */ "union",
  /*  249 */ "sclp",
  /*  250 */ "distinct",
  /*  251 */ "expr",
  /*  252 */ "as",
  /*  253 */ "tablelist",
  /*  254 */ "tmvar",
  /*  255 */ "sortlist",
  /*  256 */ "sortitem",
  /*  257 */ "item",
  /*  258 */ "sortorder",
  /*  259 */ "grouplist",
  /*  260 */ "exprlist",
  /*  261 */ "expritem",
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
 /*  21 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  23 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  25 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  27 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  28 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  29 */ "cmd ::= DROP STABLE ifexists ids cpxName",
 /*  30 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  31 */ "cmd ::= DROP TOPIC ifexists ids",
 /*  32 */ "cmd ::= DROP DNODE ids",
 /*  33 */ "cmd ::= DROP USER ids",
 /*  34 */ "cmd ::= DROP ACCOUNT ids",
 /*  35 */ "cmd ::= USE ids",
 /*  36 */ "cmd ::= DESCRIBE ids cpxName",
 /*  37 */ "cmd ::= ALTER USER ids PASS ids",
 /*  38 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  39 */ "cmd ::= ALTER DNODE ids ids",
 /*  40 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  41 */ "cmd ::= ALTER LOCAL ids",
 /*  42 */ "cmd ::= ALTER LOCAL ids ids",
 /*  43 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  44 */ "cmd ::= ALTER TOPIC ids alter_topic_optr",
 /*  45 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  46 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  47 */ "ids ::= ID",
 /*  48 */ "ids ::= STRING",
 /*  49 */ "ifexists ::= IF EXISTS",
 /*  50 */ "ifexists ::=",
 /*  51 */ "ifnotexists ::= IF NOT EXISTS",
 /*  52 */ "ifnotexists ::=",
 /*  53 */ "cmd ::= CREATE DNODE ids",
 /*  54 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  55 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  56 */ "cmd ::= CREATE TOPIC ifnotexists ids topic_optr",
 /*  57 */ "cmd ::= CREATE USER ids PASS ids",
 /*  58 */ "pps ::=",
 /*  59 */ "pps ::= PPS INTEGER",
 /*  60 */ "tseries ::=",
 /*  61 */ "tseries ::= TSERIES INTEGER",
 /*  62 */ "dbs ::=",
 /*  63 */ "dbs ::= DBS INTEGER",
 /*  64 */ "streams ::=",
 /*  65 */ "streams ::= STREAMS INTEGER",
 /*  66 */ "storage ::=",
 /*  67 */ "storage ::= STORAGE INTEGER",
 /*  68 */ "qtime ::=",
 /*  69 */ "qtime ::= QTIME INTEGER",
 /*  70 */ "users ::=",
 /*  71 */ "users ::= USERS INTEGER",
 /*  72 */ "conns ::=",
 /*  73 */ "conns ::= CONNS INTEGER",
 /*  74 */ "state ::=",
 /*  75 */ "state ::= STATE ids",
 /*  76 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  77 */ "keep ::= KEEP tagitemlist",
 /*  78 */ "cache ::= CACHE INTEGER",
 /*  79 */ "replica ::= REPLICA INTEGER",
 /*  80 */ "quorum ::= QUORUM INTEGER",
 /*  81 */ "days ::= DAYS INTEGER",
 /*  82 */ "minrows ::= MINROWS INTEGER",
 /*  83 */ "maxrows ::= MAXROWS INTEGER",
 /*  84 */ "blocks ::= BLOCKS INTEGER",
 /*  85 */ "ctime ::= CTIME INTEGER",
 /*  86 */ "wal ::= WAL INTEGER",
 /*  87 */ "fsync ::= FSYNC INTEGER",
 /*  88 */ "comp ::= COMP INTEGER",
 /*  89 */ "prec ::= PRECISION STRING",
 /*  90 */ "update ::= UPDATE INTEGER",
 /*  91 */ "cachelast ::= CACHELAST INTEGER",
 /*  92 */ "partitions ::= PARTITIONS INTEGER",
 /*  93 */ "db_optr ::=",
 /*  94 */ "db_optr ::= db_optr cache",
 /*  95 */ "db_optr ::= db_optr replica",
 /*  96 */ "db_optr ::= db_optr quorum",
 /*  97 */ "db_optr ::= db_optr days",
 /*  98 */ "db_optr ::= db_optr minrows",
 /*  99 */ "db_optr ::= db_optr maxrows",
 /* 100 */ "db_optr ::= db_optr blocks",
 /* 101 */ "db_optr ::= db_optr ctime",
 /* 102 */ "db_optr ::= db_optr wal",
 /* 103 */ "db_optr ::= db_optr fsync",
 /* 104 */ "db_optr ::= db_optr comp",
 /* 105 */ "db_optr ::= db_optr prec",
 /* 106 */ "db_optr ::= db_optr keep",
 /* 107 */ "db_optr ::= db_optr update",
 /* 108 */ "db_optr ::= db_optr cachelast",
 /* 109 */ "topic_optr ::= db_optr",
 /* 110 */ "topic_optr ::= topic_optr partitions",
 /* 111 */ "alter_db_optr ::=",
 /* 112 */ "alter_db_optr ::= alter_db_optr replica",
 /* 113 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 114 */ "alter_db_optr ::= alter_db_optr keep",
 /* 115 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 116 */ "alter_db_optr ::= alter_db_optr comp",
 /* 117 */ "alter_db_optr ::= alter_db_optr wal",
 /* 118 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 119 */ "alter_db_optr ::= alter_db_optr update",
 /* 120 */ "alter_db_optr ::= alter_db_optr cachelast",
 /* 121 */ "alter_topic_optr ::= alter_db_optr",
 /* 122 */ "alter_topic_optr ::= alter_topic_optr partitions",
 /* 123 */ "typename ::= ids",
 /* 124 */ "typename ::= ids LP signed RP",
 /* 125 */ "typename ::= ids UNSIGNED",
 /* 126 */ "signed ::= INTEGER",
 /* 127 */ "signed ::= PLUS INTEGER",
 /* 128 */ "signed ::= MINUS INTEGER",
 /* 129 */ "cmd ::= CREATE TABLE create_table_args",
 /* 130 */ "cmd ::= CREATE TABLE create_stable_args",
 /* 131 */ "cmd ::= CREATE STABLE create_stable_args",
 /* 132 */ "cmd ::= CREATE TABLE create_table_list",
 /* 133 */ "create_table_list ::= create_from_stable",
 /* 134 */ "create_table_list ::= create_table_list create_from_stable",
 /* 135 */ "create_table_args ::= ifnotexists ids cpxName LP columnlist RP",
 /* 136 */ "create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP",
 /* 137 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP",
 /* 138 */ "create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP",
 /* 139 */ "tagNamelist ::= tagNamelist COMMA ids",
 /* 140 */ "tagNamelist ::= ids",
 /* 141 */ "create_table_args ::= ifnotexists ids cpxName AS select",
 /* 142 */ "columnlist ::= columnlist COMMA column",
 /* 143 */ "columnlist ::= column",
 /* 144 */ "column ::= ids typename",
 /* 145 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 146 */ "tagitemlist ::= tagitem",
 /* 147 */ "tagitem ::= INTEGER",
 /* 148 */ "tagitem ::= FLOAT",
 /* 149 */ "tagitem ::= STRING",
 /* 150 */ "tagitem ::= BOOL",
 /* 151 */ "tagitem ::= NULL",
 /* 152 */ "tagitem ::= MINUS INTEGER",
 /* 153 */ "tagitem ::= MINUS FLOAT",
 /* 154 */ "tagitem ::= PLUS INTEGER",
 /* 155 */ "tagitem ::= PLUS FLOAT",
 /* 156 */ "select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 157 */ "select ::= LP select RP",
 /* 158 */ "union ::= select",
 /* 159 */ "union ::= union UNION ALL select",
 /* 160 */ "cmd ::= union",
 /* 161 */ "select ::= SELECT selcollist",
 /* 162 */ "sclp ::= selcollist COMMA",
 /* 163 */ "sclp ::=",
 /* 164 */ "selcollist ::= sclp distinct expr as",
 /* 165 */ "selcollist ::= sclp STAR",
 /* 166 */ "as ::= AS ids",
 /* 167 */ "as ::= ids",
 /* 168 */ "as ::=",
 /* 169 */ "distinct ::= DISTINCT",
 /* 170 */ "distinct ::=",
 /* 171 */ "from ::= FROM tablelist",
 /* 172 */ "from ::= FROM LP union RP",
 /* 173 */ "tablelist ::= ids cpxName",
 /* 174 */ "tablelist ::= ids cpxName ids",
 /* 175 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 176 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 177 */ "tmvar ::= VARIABLE",
 /* 178 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 179 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 180 */ "interval_opt ::=",
 /* 181 */ "session_option ::=",
 /* 182 */ "session_option ::= SESSION LP ids cpxName COMMA tmvar RP",
 /* 183 */ "fill_opt ::=",
 /* 184 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 185 */ "fill_opt ::= FILL LP ID RP",
 /* 186 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 187 */ "sliding_opt ::=",
 /* 188 */ "orderby_opt ::=",
 /* 189 */ "orderby_opt ::= ORDER BY sortlist",
 /* 190 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 191 */ "sortlist ::= item sortorder",
 /* 192 */ "item ::= ids cpxName",
 /* 193 */ "sortorder ::= ASC",
 /* 194 */ "sortorder ::= DESC",
 /* 195 */ "sortorder ::=",
 /* 196 */ "groupby_opt ::=",
 /* 197 */ "groupby_opt ::= GROUP BY grouplist",
 /* 198 */ "grouplist ::= grouplist COMMA item",
 /* 199 */ "grouplist ::= item",
 /* 200 */ "having_opt ::=",
 /* 201 */ "having_opt ::= HAVING expr",
 /* 202 */ "limit_opt ::=",
 /* 203 */ "limit_opt ::= LIMIT signed",
 /* 204 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 205 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 206 */ "slimit_opt ::=",
 /* 207 */ "slimit_opt ::= SLIMIT signed",
 /* 208 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 209 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 210 */ "where_opt ::=",
 /* 211 */ "where_opt ::= WHERE expr",
 /* 212 */ "expr ::= LP expr RP",
 /* 213 */ "expr ::= ID",
 /* 214 */ "expr ::= ID DOT ID",
 /* 215 */ "expr ::= ID DOT STAR",
 /* 216 */ "expr ::= INTEGER",
 /* 217 */ "expr ::= MINUS INTEGER",
 /* 218 */ "expr ::= PLUS INTEGER",
 /* 219 */ "expr ::= FLOAT",
 /* 220 */ "expr ::= MINUS FLOAT",
 /* 221 */ "expr ::= PLUS FLOAT",
 /* 222 */ "expr ::= STRING",
 /* 223 */ "expr ::= NOW",
 /* 224 */ "expr ::= VARIABLE",
 /* 225 */ "expr ::= BOOL",
 /* 226 */ "expr ::= ID LP exprlist RP",
 /* 227 */ "expr ::= ID LP STAR RP",
 /* 228 */ "expr ::= expr IS NULL",
 /* 229 */ "expr ::= expr IS NOT NULL",
 /* 230 */ "expr ::= expr LT expr",
 /* 231 */ "expr ::= expr GT expr",
 /* 232 */ "expr ::= expr LE expr",
 /* 233 */ "expr ::= expr GE expr",
 /* 234 */ "expr ::= expr NE expr",
 /* 235 */ "expr ::= expr EQ expr",
 /* 236 */ "expr ::= expr BETWEEN expr AND expr",
 /* 237 */ "expr ::= expr AND expr",
 /* 238 */ "expr ::= expr OR expr",
 /* 239 */ "expr ::= expr PLUS expr",
 /* 240 */ "expr ::= expr MINUS expr",
 /* 241 */ "expr ::= expr STAR expr",
 /* 242 */ "expr ::= expr SLASH expr",
 /* 243 */ "expr ::= expr REM expr",
 /* 244 */ "expr ::= expr LIKE expr",
 /* 245 */ "expr ::= expr IN LP exprlist RP",
 /* 246 */ "exprlist ::= exprlist COMMA expritem",
 /* 247 */ "exprlist ::= expritem",
 /* 248 */ "expritem ::= expr",
 /* 249 */ "expritem ::=",
 /* 250 */ "cmd ::= RESET QUERY CACHE",
 /* 251 */ "cmd ::= SYNCDB ids REPLICA",
 /* 252 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 253 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 254 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 255 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 256 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 257 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 258 */ "cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist",
 /* 259 */ "cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids",
 /* 260 */ "cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist",
 /* 261 */ "cmd ::= ALTER STABLE ids cpxName DROP TAG ids",
 /* 262 */ "cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids",
 /* 263 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 264 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 265 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
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
    case 208: /* keep */
    case 209: /* tagitemlist */
    case 231: /* columnlist */
    case 232: /* tagNamelist */
    case 241: /* fill_opt */
    case 243: /* groupby_opt */
    case 244: /* orderby_opt */
    case 255: /* sortlist */
    case 259: /* grouplist */
{
taosArrayDestroy((yypminor->yy429));
}
      break;
    case 229: /* create_table_list */
{
destroyCreateTableSql((yypminor->yy194));
}
      break;
    case 233: /* select */
{
destroyQuerySqlNode((yypminor->yy254));
}
      break;
    case 236: /* selcollist */
    case 249: /* sclp */
    case 260: /* exprlist */
{
tSqlExprListDestroy((yypminor->yy429));
}
      break;
    case 238: /* where_opt */
    case 245: /* having_opt */
    case 251: /* expr */
    case 261: /* expritem */
{
tSqlExprDestroy((yypminor->yy170));
}
      break;
    case 248: /* union */
{
destroyAllSelectClause((yypminor->yy141));
}
      break;
    case 256: /* sortitem */
{
tVariantDestroy(&(yypminor->yy218));
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
   187,  /* (0) program ::= cmd */
   188,  /* (1) cmd ::= SHOW DATABASES */
   188,  /* (2) cmd ::= SHOW TOPICS */
   188,  /* (3) cmd ::= SHOW MNODES */
   188,  /* (4) cmd ::= SHOW DNODES */
   188,  /* (5) cmd ::= SHOW ACCOUNTS */
   188,  /* (6) cmd ::= SHOW USERS */
   188,  /* (7) cmd ::= SHOW MODULES */
   188,  /* (8) cmd ::= SHOW QUERIES */
   188,  /* (9) cmd ::= SHOW CONNECTIONS */
   188,  /* (10) cmd ::= SHOW STREAMS */
   188,  /* (11) cmd ::= SHOW VARIABLES */
   188,  /* (12) cmd ::= SHOW SCORES */
   188,  /* (13) cmd ::= SHOW GRANTS */
   188,  /* (14) cmd ::= SHOW VNODES */
   188,  /* (15) cmd ::= SHOW VNODES IPTOKEN */
   189,  /* (16) dbPrefix ::= */
   189,  /* (17) dbPrefix ::= ids DOT */
   191,  /* (18) cpxName ::= */
   191,  /* (19) cpxName ::= DOT ids */
   188,  /* (20) cmd ::= SHOW CREATE TABLE ids cpxName */
   188,  /* (21) cmd ::= SHOW CREATE DATABASE ids */
   188,  /* (22) cmd ::= SHOW dbPrefix TABLES */
   188,  /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   188,  /* (24) cmd ::= SHOW dbPrefix STABLES */
   188,  /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   188,  /* (26) cmd ::= SHOW dbPrefix VGROUPS */
   188,  /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
   188,  /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
   188,  /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
   188,  /* (30) cmd ::= DROP DATABASE ifexists ids */
   188,  /* (31) cmd ::= DROP TOPIC ifexists ids */
   188,  /* (32) cmd ::= DROP DNODE ids */
   188,  /* (33) cmd ::= DROP USER ids */
   188,  /* (34) cmd ::= DROP ACCOUNT ids */
   188,  /* (35) cmd ::= USE ids */
   188,  /* (36) cmd ::= DESCRIBE ids cpxName */
   188,  /* (37) cmd ::= ALTER USER ids PASS ids */
   188,  /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
   188,  /* (39) cmd ::= ALTER DNODE ids ids */
   188,  /* (40) cmd ::= ALTER DNODE ids ids ids */
   188,  /* (41) cmd ::= ALTER LOCAL ids */
   188,  /* (42) cmd ::= ALTER LOCAL ids ids */
   188,  /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
   188,  /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
   188,  /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
   188,  /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   190,  /* (47) ids ::= ID */
   190,  /* (48) ids ::= STRING */
   192,  /* (49) ifexists ::= IF EXISTS */
   192,  /* (50) ifexists ::= */
   196,  /* (51) ifnotexists ::= IF NOT EXISTS */
   196,  /* (52) ifnotexists ::= */
   188,  /* (53) cmd ::= CREATE DNODE ids */
   188,  /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   188,  /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   188,  /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   188,  /* (57) cmd ::= CREATE USER ids PASS ids */
   199,  /* (58) pps ::= */
   199,  /* (59) pps ::= PPS INTEGER */
   200,  /* (60) tseries ::= */
   200,  /* (61) tseries ::= TSERIES INTEGER */
   201,  /* (62) dbs ::= */
   201,  /* (63) dbs ::= DBS INTEGER */
   202,  /* (64) streams ::= */
   202,  /* (65) streams ::= STREAMS INTEGER */
   203,  /* (66) storage ::= */
   203,  /* (67) storage ::= STORAGE INTEGER */
   204,  /* (68) qtime ::= */
   204,  /* (69) qtime ::= QTIME INTEGER */
   205,  /* (70) users ::= */
   205,  /* (71) users ::= USERS INTEGER */
   206,  /* (72) conns ::= */
   206,  /* (73) conns ::= CONNS INTEGER */
   207,  /* (74) state ::= */
   207,  /* (75) state ::= STATE ids */
   195,  /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   208,  /* (77) keep ::= KEEP tagitemlist */
   210,  /* (78) cache ::= CACHE INTEGER */
   211,  /* (79) replica ::= REPLICA INTEGER */
   212,  /* (80) quorum ::= QUORUM INTEGER */
   213,  /* (81) days ::= DAYS INTEGER */
   214,  /* (82) minrows ::= MINROWS INTEGER */
   215,  /* (83) maxrows ::= MAXROWS INTEGER */
   216,  /* (84) blocks ::= BLOCKS INTEGER */
   217,  /* (85) ctime ::= CTIME INTEGER */
   218,  /* (86) wal ::= WAL INTEGER */
   219,  /* (87) fsync ::= FSYNC INTEGER */
   220,  /* (88) comp ::= COMP INTEGER */
   221,  /* (89) prec ::= PRECISION STRING */
   222,  /* (90) update ::= UPDATE INTEGER */
   223,  /* (91) cachelast ::= CACHELAST INTEGER */
   224,  /* (92) partitions ::= PARTITIONS INTEGER */
   197,  /* (93) db_optr ::= */
   197,  /* (94) db_optr ::= db_optr cache */
   197,  /* (95) db_optr ::= db_optr replica */
   197,  /* (96) db_optr ::= db_optr quorum */
   197,  /* (97) db_optr ::= db_optr days */
   197,  /* (98) db_optr ::= db_optr minrows */
   197,  /* (99) db_optr ::= db_optr maxrows */
   197,  /* (100) db_optr ::= db_optr blocks */
   197,  /* (101) db_optr ::= db_optr ctime */
   197,  /* (102) db_optr ::= db_optr wal */
   197,  /* (103) db_optr ::= db_optr fsync */
   197,  /* (104) db_optr ::= db_optr comp */
   197,  /* (105) db_optr ::= db_optr prec */
   197,  /* (106) db_optr ::= db_optr keep */
   197,  /* (107) db_optr ::= db_optr update */
   197,  /* (108) db_optr ::= db_optr cachelast */
   198,  /* (109) topic_optr ::= db_optr */
   198,  /* (110) topic_optr ::= topic_optr partitions */
   193,  /* (111) alter_db_optr ::= */
   193,  /* (112) alter_db_optr ::= alter_db_optr replica */
   193,  /* (113) alter_db_optr ::= alter_db_optr quorum */
   193,  /* (114) alter_db_optr ::= alter_db_optr keep */
   193,  /* (115) alter_db_optr ::= alter_db_optr blocks */
   193,  /* (116) alter_db_optr ::= alter_db_optr comp */
   193,  /* (117) alter_db_optr ::= alter_db_optr wal */
   193,  /* (118) alter_db_optr ::= alter_db_optr fsync */
   193,  /* (119) alter_db_optr ::= alter_db_optr update */
   193,  /* (120) alter_db_optr ::= alter_db_optr cachelast */
   194,  /* (121) alter_topic_optr ::= alter_db_optr */
   194,  /* (122) alter_topic_optr ::= alter_topic_optr partitions */
   225,  /* (123) typename ::= ids */
   225,  /* (124) typename ::= ids LP signed RP */
   225,  /* (125) typename ::= ids UNSIGNED */
   226,  /* (126) signed ::= INTEGER */
   226,  /* (127) signed ::= PLUS INTEGER */
   226,  /* (128) signed ::= MINUS INTEGER */
   188,  /* (129) cmd ::= CREATE TABLE create_table_args */
   188,  /* (130) cmd ::= CREATE TABLE create_stable_args */
   188,  /* (131) cmd ::= CREATE STABLE create_stable_args */
   188,  /* (132) cmd ::= CREATE TABLE create_table_list */
   229,  /* (133) create_table_list ::= create_from_stable */
   229,  /* (134) create_table_list ::= create_table_list create_from_stable */
   227,  /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
   228,  /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
   230,  /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
   230,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   232,  /* (139) tagNamelist ::= tagNamelist COMMA ids */
   232,  /* (140) tagNamelist ::= ids */
   227,  /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
   231,  /* (142) columnlist ::= columnlist COMMA column */
   231,  /* (143) columnlist ::= column */
   234,  /* (144) column ::= ids typename */
   209,  /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
   209,  /* (146) tagitemlist ::= tagitem */
   235,  /* (147) tagitem ::= INTEGER */
   235,  /* (148) tagitem ::= FLOAT */
   235,  /* (149) tagitem ::= STRING */
   235,  /* (150) tagitem ::= BOOL */
   235,  /* (151) tagitem ::= NULL */
   235,  /* (152) tagitem ::= MINUS INTEGER */
   235,  /* (153) tagitem ::= MINUS FLOAT */
   235,  /* (154) tagitem ::= PLUS INTEGER */
   235,  /* (155) tagitem ::= PLUS FLOAT */
   233,  /* (156) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   233,  /* (157) select ::= LP select RP */
   248,  /* (158) union ::= select */
   248,  /* (159) union ::= union UNION ALL select */
   188,  /* (160) cmd ::= union */
   233,  /* (161) select ::= SELECT selcollist */
   249,  /* (162) sclp ::= selcollist COMMA */
   249,  /* (163) sclp ::= */
   236,  /* (164) selcollist ::= sclp distinct expr as */
   236,  /* (165) selcollist ::= sclp STAR */
   252,  /* (166) as ::= AS ids */
   252,  /* (167) as ::= ids */
   252,  /* (168) as ::= */
   250,  /* (169) distinct ::= DISTINCT */
   250,  /* (170) distinct ::= */
   237,  /* (171) from ::= FROM tablelist */
   237,  /* (172) from ::= FROM LP union RP */
   253,  /* (173) tablelist ::= ids cpxName */
   253,  /* (174) tablelist ::= ids cpxName ids */
   253,  /* (175) tablelist ::= tablelist COMMA ids cpxName */
   253,  /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
   254,  /* (177) tmvar ::= VARIABLE */
   239,  /* (178) interval_opt ::= INTERVAL LP tmvar RP */
   239,  /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
   239,  /* (180) interval_opt ::= */
   240,  /* (181) session_option ::= */
   240,  /* (182) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
   241,  /* (183) fill_opt ::= */
   241,  /* (184) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   241,  /* (185) fill_opt ::= FILL LP ID RP */
   242,  /* (186) sliding_opt ::= SLIDING LP tmvar RP */
   242,  /* (187) sliding_opt ::= */
   244,  /* (188) orderby_opt ::= */
   244,  /* (189) orderby_opt ::= ORDER BY sortlist */
   255,  /* (190) sortlist ::= sortlist COMMA item sortorder */
   255,  /* (191) sortlist ::= item sortorder */
   257,  /* (192) item ::= ids cpxName */
   258,  /* (193) sortorder ::= ASC */
   258,  /* (194) sortorder ::= DESC */
   258,  /* (195) sortorder ::= */
   243,  /* (196) groupby_opt ::= */
   243,  /* (197) groupby_opt ::= GROUP BY grouplist */
   259,  /* (198) grouplist ::= grouplist COMMA item */
   259,  /* (199) grouplist ::= item */
   245,  /* (200) having_opt ::= */
   245,  /* (201) having_opt ::= HAVING expr */
   247,  /* (202) limit_opt ::= */
   247,  /* (203) limit_opt ::= LIMIT signed */
   247,  /* (204) limit_opt ::= LIMIT signed OFFSET signed */
   247,  /* (205) limit_opt ::= LIMIT signed COMMA signed */
   246,  /* (206) slimit_opt ::= */
   246,  /* (207) slimit_opt ::= SLIMIT signed */
   246,  /* (208) slimit_opt ::= SLIMIT signed SOFFSET signed */
   246,  /* (209) slimit_opt ::= SLIMIT signed COMMA signed */
   238,  /* (210) where_opt ::= */
   238,  /* (211) where_opt ::= WHERE expr */
   251,  /* (212) expr ::= LP expr RP */
   251,  /* (213) expr ::= ID */
   251,  /* (214) expr ::= ID DOT ID */
   251,  /* (215) expr ::= ID DOT STAR */
   251,  /* (216) expr ::= INTEGER */
   251,  /* (217) expr ::= MINUS INTEGER */
   251,  /* (218) expr ::= PLUS INTEGER */
   251,  /* (219) expr ::= FLOAT */
   251,  /* (220) expr ::= MINUS FLOAT */
   251,  /* (221) expr ::= PLUS FLOAT */
   251,  /* (222) expr ::= STRING */
   251,  /* (223) expr ::= NOW */
   251,  /* (224) expr ::= VARIABLE */
   251,  /* (225) expr ::= BOOL */
   251,  /* (226) expr ::= ID LP exprlist RP */
   251,  /* (227) expr ::= ID LP STAR RP */
   251,  /* (228) expr ::= expr IS NULL */
   251,  /* (229) expr ::= expr IS NOT NULL */
   251,  /* (230) expr ::= expr LT expr */
   251,  /* (231) expr ::= expr GT expr */
   251,  /* (232) expr ::= expr LE expr */
   251,  /* (233) expr ::= expr GE expr */
   251,  /* (234) expr ::= expr NE expr */
   251,  /* (235) expr ::= expr EQ expr */
   251,  /* (236) expr ::= expr BETWEEN expr AND expr */
   251,  /* (237) expr ::= expr AND expr */
   251,  /* (238) expr ::= expr OR expr */
   251,  /* (239) expr ::= expr PLUS expr */
   251,  /* (240) expr ::= expr MINUS expr */
   251,  /* (241) expr ::= expr STAR expr */
   251,  /* (242) expr ::= expr SLASH expr */
   251,  /* (243) expr ::= expr REM expr */
   251,  /* (244) expr ::= expr LIKE expr */
   251,  /* (245) expr ::= expr IN LP exprlist RP */
   260,  /* (246) exprlist ::= exprlist COMMA expritem */
   260,  /* (247) exprlist ::= expritem */
   261,  /* (248) expritem ::= expr */
   261,  /* (249) expritem ::= */
   188,  /* (250) cmd ::= RESET QUERY CACHE */
   188,  /* (251) cmd ::= SYNCDB ids REPLICA */
   188,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   188,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   188,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   188,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   188,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   188,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   188,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   188,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   188,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   188,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   188,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   188,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   188,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   188,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
   -4,  /* (21) cmd ::= SHOW CREATE DATABASE ids */
   -3,  /* (22) cmd ::= SHOW dbPrefix TABLES */
   -5,  /* (23) cmd ::= SHOW dbPrefix TABLES LIKE ids */
   -3,  /* (24) cmd ::= SHOW dbPrefix STABLES */
   -5,  /* (25) cmd ::= SHOW dbPrefix STABLES LIKE ids */
   -3,  /* (26) cmd ::= SHOW dbPrefix VGROUPS */
   -4,  /* (27) cmd ::= SHOW dbPrefix VGROUPS ids */
   -5,  /* (28) cmd ::= DROP TABLE ifexists ids cpxName */
   -5,  /* (29) cmd ::= DROP STABLE ifexists ids cpxName */
   -4,  /* (30) cmd ::= DROP DATABASE ifexists ids */
   -4,  /* (31) cmd ::= DROP TOPIC ifexists ids */
   -3,  /* (32) cmd ::= DROP DNODE ids */
   -3,  /* (33) cmd ::= DROP USER ids */
   -3,  /* (34) cmd ::= DROP ACCOUNT ids */
   -2,  /* (35) cmd ::= USE ids */
   -3,  /* (36) cmd ::= DESCRIBE ids cpxName */
   -5,  /* (37) cmd ::= ALTER USER ids PASS ids */
   -5,  /* (38) cmd ::= ALTER USER ids PRIVILEGE ids */
   -4,  /* (39) cmd ::= ALTER DNODE ids ids */
   -5,  /* (40) cmd ::= ALTER DNODE ids ids ids */
   -3,  /* (41) cmd ::= ALTER LOCAL ids */
   -4,  /* (42) cmd ::= ALTER LOCAL ids ids */
   -4,  /* (43) cmd ::= ALTER DATABASE ids alter_db_optr */
   -4,  /* (44) cmd ::= ALTER TOPIC ids alter_topic_optr */
   -4,  /* (45) cmd ::= ALTER ACCOUNT ids acct_optr */
   -6,  /* (46) cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
   -1,  /* (47) ids ::= ID */
   -1,  /* (48) ids ::= STRING */
   -2,  /* (49) ifexists ::= IF EXISTS */
    0,  /* (50) ifexists ::= */
   -3,  /* (51) ifnotexists ::= IF NOT EXISTS */
    0,  /* (52) ifnotexists ::= */
   -3,  /* (53) cmd ::= CREATE DNODE ids */
   -6,  /* (54) cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
   -5,  /* (55) cmd ::= CREATE DATABASE ifnotexists ids db_optr */
   -5,  /* (56) cmd ::= CREATE TOPIC ifnotexists ids topic_optr */
   -5,  /* (57) cmd ::= CREATE USER ids PASS ids */
    0,  /* (58) pps ::= */
   -2,  /* (59) pps ::= PPS INTEGER */
    0,  /* (60) tseries ::= */
   -2,  /* (61) tseries ::= TSERIES INTEGER */
    0,  /* (62) dbs ::= */
   -2,  /* (63) dbs ::= DBS INTEGER */
    0,  /* (64) streams ::= */
   -2,  /* (65) streams ::= STREAMS INTEGER */
    0,  /* (66) storage ::= */
   -2,  /* (67) storage ::= STORAGE INTEGER */
    0,  /* (68) qtime ::= */
   -2,  /* (69) qtime ::= QTIME INTEGER */
    0,  /* (70) users ::= */
   -2,  /* (71) users ::= USERS INTEGER */
    0,  /* (72) conns ::= */
   -2,  /* (73) conns ::= CONNS INTEGER */
    0,  /* (74) state ::= */
   -2,  /* (75) state ::= STATE ids */
   -9,  /* (76) acct_optr ::= pps tseries storage streams qtime dbs users conns state */
   -2,  /* (77) keep ::= KEEP tagitemlist */
   -2,  /* (78) cache ::= CACHE INTEGER */
   -2,  /* (79) replica ::= REPLICA INTEGER */
   -2,  /* (80) quorum ::= QUORUM INTEGER */
   -2,  /* (81) days ::= DAYS INTEGER */
   -2,  /* (82) minrows ::= MINROWS INTEGER */
   -2,  /* (83) maxrows ::= MAXROWS INTEGER */
   -2,  /* (84) blocks ::= BLOCKS INTEGER */
   -2,  /* (85) ctime ::= CTIME INTEGER */
   -2,  /* (86) wal ::= WAL INTEGER */
   -2,  /* (87) fsync ::= FSYNC INTEGER */
   -2,  /* (88) comp ::= COMP INTEGER */
   -2,  /* (89) prec ::= PRECISION STRING */
   -2,  /* (90) update ::= UPDATE INTEGER */
   -2,  /* (91) cachelast ::= CACHELAST INTEGER */
   -2,  /* (92) partitions ::= PARTITIONS INTEGER */
    0,  /* (93) db_optr ::= */
   -2,  /* (94) db_optr ::= db_optr cache */
   -2,  /* (95) db_optr ::= db_optr replica */
   -2,  /* (96) db_optr ::= db_optr quorum */
   -2,  /* (97) db_optr ::= db_optr days */
   -2,  /* (98) db_optr ::= db_optr minrows */
   -2,  /* (99) db_optr ::= db_optr maxrows */
   -2,  /* (100) db_optr ::= db_optr blocks */
   -2,  /* (101) db_optr ::= db_optr ctime */
   -2,  /* (102) db_optr ::= db_optr wal */
   -2,  /* (103) db_optr ::= db_optr fsync */
   -2,  /* (104) db_optr ::= db_optr comp */
   -2,  /* (105) db_optr ::= db_optr prec */
   -2,  /* (106) db_optr ::= db_optr keep */
   -2,  /* (107) db_optr ::= db_optr update */
   -2,  /* (108) db_optr ::= db_optr cachelast */
   -1,  /* (109) topic_optr ::= db_optr */
   -2,  /* (110) topic_optr ::= topic_optr partitions */
    0,  /* (111) alter_db_optr ::= */
   -2,  /* (112) alter_db_optr ::= alter_db_optr replica */
   -2,  /* (113) alter_db_optr ::= alter_db_optr quorum */
   -2,  /* (114) alter_db_optr ::= alter_db_optr keep */
   -2,  /* (115) alter_db_optr ::= alter_db_optr blocks */
   -2,  /* (116) alter_db_optr ::= alter_db_optr comp */
   -2,  /* (117) alter_db_optr ::= alter_db_optr wal */
   -2,  /* (118) alter_db_optr ::= alter_db_optr fsync */
   -2,  /* (119) alter_db_optr ::= alter_db_optr update */
   -2,  /* (120) alter_db_optr ::= alter_db_optr cachelast */
   -1,  /* (121) alter_topic_optr ::= alter_db_optr */
   -2,  /* (122) alter_topic_optr ::= alter_topic_optr partitions */
   -1,  /* (123) typename ::= ids */
   -4,  /* (124) typename ::= ids LP signed RP */
   -2,  /* (125) typename ::= ids UNSIGNED */
   -1,  /* (126) signed ::= INTEGER */
   -2,  /* (127) signed ::= PLUS INTEGER */
   -2,  /* (128) signed ::= MINUS INTEGER */
   -3,  /* (129) cmd ::= CREATE TABLE create_table_args */
   -3,  /* (130) cmd ::= CREATE TABLE create_stable_args */
   -3,  /* (131) cmd ::= CREATE STABLE create_stable_args */
   -3,  /* (132) cmd ::= CREATE TABLE create_table_list */
   -1,  /* (133) create_table_list ::= create_from_stable */
   -2,  /* (134) create_table_list ::= create_table_list create_from_stable */
   -6,  /* (135) create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
  -10,  /* (136) create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
  -10,  /* (137) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
  -13,  /* (138) create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
   -3,  /* (139) tagNamelist ::= tagNamelist COMMA ids */
   -1,  /* (140) tagNamelist ::= ids */
   -5,  /* (141) create_table_args ::= ifnotexists ids cpxName AS select */
   -3,  /* (142) columnlist ::= columnlist COMMA column */
   -1,  /* (143) columnlist ::= column */
   -2,  /* (144) column ::= ids typename */
   -3,  /* (145) tagitemlist ::= tagitemlist COMMA tagitem */
   -1,  /* (146) tagitemlist ::= tagitem */
   -1,  /* (147) tagitem ::= INTEGER */
   -1,  /* (148) tagitem ::= FLOAT */
   -1,  /* (149) tagitem ::= STRING */
   -1,  /* (150) tagitem ::= BOOL */
   -1,  /* (151) tagitem ::= NULL */
   -2,  /* (152) tagitem ::= MINUS INTEGER */
   -2,  /* (153) tagitem ::= MINUS FLOAT */
   -2,  /* (154) tagitem ::= PLUS INTEGER */
   -2,  /* (155) tagitem ::= PLUS FLOAT */
  -13,  /* (156) select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
   -3,  /* (157) select ::= LP select RP */
   -1,  /* (158) union ::= select */
   -4,  /* (159) union ::= union UNION ALL select */
   -1,  /* (160) cmd ::= union */
   -2,  /* (161) select ::= SELECT selcollist */
   -2,  /* (162) sclp ::= selcollist COMMA */
    0,  /* (163) sclp ::= */
   -4,  /* (164) selcollist ::= sclp distinct expr as */
   -2,  /* (165) selcollist ::= sclp STAR */
   -2,  /* (166) as ::= AS ids */
   -1,  /* (167) as ::= ids */
    0,  /* (168) as ::= */
   -1,  /* (169) distinct ::= DISTINCT */
    0,  /* (170) distinct ::= */
   -2,  /* (171) from ::= FROM tablelist */
   -4,  /* (172) from ::= FROM LP union RP */
   -2,  /* (173) tablelist ::= ids cpxName */
   -3,  /* (174) tablelist ::= ids cpxName ids */
   -4,  /* (175) tablelist ::= tablelist COMMA ids cpxName */
   -5,  /* (176) tablelist ::= tablelist COMMA ids cpxName ids */
   -1,  /* (177) tmvar ::= VARIABLE */
   -4,  /* (178) interval_opt ::= INTERVAL LP tmvar RP */
   -6,  /* (179) interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
    0,  /* (180) interval_opt ::= */
    0,  /* (181) session_option ::= */
   -7,  /* (182) session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
    0,  /* (183) fill_opt ::= */
   -6,  /* (184) fill_opt ::= FILL LP ID COMMA tagitemlist RP */
   -4,  /* (185) fill_opt ::= FILL LP ID RP */
   -4,  /* (186) sliding_opt ::= SLIDING LP tmvar RP */
    0,  /* (187) sliding_opt ::= */
    0,  /* (188) orderby_opt ::= */
   -3,  /* (189) orderby_opt ::= ORDER BY sortlist */
   -4,  /* (190) sortlist ::= sortlist COMMA item sortorder */
   -2,  /* (191) sortlist ::= item sortorder */
   -2,  /* (192) item ::= ids cpxName */
   -1,  /* (193) sortorder ::= ASC */
   -1,  /* (194) sortorder ::= DESC */
    0,  /* (195) sortorder ::= */
    0,  /* (196) groupby_opt ::= */
   -3,  /* (197) groupby_opt ::= GROUP BY grouplist */
   -3,  /* (198) grouplist ::= grouplist COMMA item */
   -1,  /* (199) grouplist ::= item */
    0,  /* (200) having_opt ::= */
   -2,  /* (201) having_opt ::= HAVING expr */
    0,  /* (202) limit_opt ::= */
   -2,  /* (203) limit_opt ::= LIMIT signed */
   -4,  /* (204) limit_opt ::= LIMIT signed OFFSET signed */
   -4,  /* (205) limit_opt ::= LIMIT signed COMMA signed */
    0,  /* (206) slimit_opt ::= */
   -2,  /* (207) slimit_opt ::= SLIMIT signed */
   -4,  /* (208) slimit_opt ::= SLIMIT signed SOFFSET signed */
   -4,  /* (209) slimit_opt ::= SLIMIT signed COMMA signed */
    0,  /* (210) where_opt ::= */
   -2,  /* (211) where_opt ::= WHERE expr */
   -3,  /* (212) expr ::= LP expr RP */
   -1,  /* (213) expr ::= ID */
   -3,  /* (214) expr ::= ID DOT ID */
   -3,  /* (215) expr ::= ID DOT STAR */
   -1,  /* (216) expr ::= INTEGER */
   -2,  /* (217) expr ::= MINUS INTEGER */
   -2,  /* (218) expr ::= PLUS INTEGER */
   -1,  /* (219) expr ::= FLOAT */
   -2,  /* (220) expr ::= MINUS FLOAT */
   -2,  /* (221) expr ::= PLUS FLOAT */
   -1,  /* (222) expr ::= STRING */
   -1,  /* (223) expr ::= NOW */
   -1,  /* (224) expr ::= VARIABLE */
   -1,  /* (225) expr ::= BOOL */
   -4,  /* (226) expr ::= ID LP exprlist RP */
   -4,  /* (227) expr ::= ID LP STAR RP */
   -3,  /* (228) expr ::= expr IS NULL */
   -4,  /* (229) expr ::= expr IS NOT NULL */
   -3,  /* (230) expr ::= expr LT expr */
   -3,  /* (231) expr ::= expr GT expr */
   -3,  /* (232) expr ::= expr LE expr */
   -3,  /* (233) expr ::= expr GE expr */
   -3,  /* (234) expr ::= expr NE expr */
   -3,  /* (235) expr ::= expr EQ expr */
   -5,  /* (236) expr ::= expr BETWEEN expr AND expr */
   -3,  /* (237) expr ::= expr AND expr */
   -3,  /* (238) expr ::= expr OR expr */
   -3,  /* (239) expr ::= expr PLUS expr */
   -3,  /* (240) expr ::= expr MINUS expr */
   -3,  /* (241) expr ::= expr STAR expr */
   -3,  /* (242) expr ::= expr SLASH expr */
   -3,  /* (243) expr ::= expr REM expr */
   -3,  /* (244) expr ::= expr LIKE expr */
   -5,  /* (245) expr ::= expr IN LP exprlist RP */
   -3,  /* (246) exprlist ::= exprlist COMMA expritem */
   -1,  /* (247) exprlist ::= expritem */
   -1,  /* (248) expritem ::= expr */
    0,  /* (249) expritem ::= */
   -3,  /* (250) cmd ::= RESET QUERY CACHE */
   -3,  /* (251) cmd ::= SYNCDB ids REPLICA */
   -7,  /* (252) cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (253) cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
   -7,  /* (254) cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
   -7,  /* (255) cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
   -8,  /* (256) cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
   -9,  /* (257) cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
   -7,  /* (258) cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
   -7,  /* (259) cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
   -7,  /* (260) cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
   -7,  /* (261) cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
   -8,  /* (262) cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
   -3,  /* (263) cmd ::= KILL CONNECTION INTEGER */
   -5,  /* (264) cmd ::= KILL STREAM INTEGER COLON INTEGER */
   -5,  /* (265) cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
      case 129: /* cmd ::= CREATE TABLE create_table_args */ yytestcase(yyruleno==129);
      case 130: /* cmd ::= CREATE TABLE create_stable_args */ yytestcase(yyruleno==130);
      case 131: /* cmd ::= CREATE STABLE create_stable_args */ yytestcase(yyruleno==131);
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
      case 21: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSqlElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 27: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    tSetDbName(&token, &yymsp[-2].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, -1);
}
        break;
      case 29: /* cmd ::= DROP STABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDbTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0, -1, TSDB_SUPER_TABLE);
}
        break;
      case 30: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_DEFAULT, -1); }
        break;
      case 31: /* cmd ::= DROP TOPIC ifexists ids */
{ setDropDbTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0, TSDB_DB_TYPE_TOPIC, -1); }
        break;
      case 32: /* cmd ::= DROP DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 33: /* cmd ::= DROP USER ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 34: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 35: /* cmd ::= USE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSqlElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 37: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 38: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSql(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 39: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 41: /* cmd ::= ALTER LOCAL ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 42: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 43: /* cmd ::= ALTER DATABASE ids alter_db_optr */
      case 44: /* cmd ::= ALTER TOPIC ids alter_topic_optr */ yytestcase(yyruleno==44);
{ SStrToken t = {0};  setCreateDbInfo(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy94, &t);}
        break;
      case 45: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy419);}
        break;
      case 46: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy419);}
        break;
      case 47: /* ids ::= ID */
      case 48: /* ids ::= STRING */ yytestcase(yyruleno==48);
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 49: /* ifexists ::= IF EXISTS */
{ yymsp[-1].minor.yy0.n = 1;}
        break;
      case 50: /* ifexists ::= */
      case 52: /* ifnotexists ::= */ yytestcase(yyruleno==52);
      case 170: /* distinct ::= */ yytestcase(yyruleno==170);
{ yymsp[1].minor.yy0.n = 0;}
        break;
      case 51: /* ifnotexists ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy0.n = 1;}
        break;
      case 53: /* cmd ::= CREATE DNODE ids */
{ setDCLSqlElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 54: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSql(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy419);}
        break;
      case 55: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
      case 56: /* cmd ::= CREATE TOPIC ifnotexists ids topic_optr */ yytestcase(yyruleno==56);
{ setCreateDbInfo(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy94, &yymsp[-2].minor.yy0);}
        break;
      case 57: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSql(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 58: /* pps ::= */
      case 60: /* tseries ::= */ yytestcase(yyruleno==60);
      case 62: /* dbs ::= */ yytestcase(yyruleno==62);
      case 64: /* streams ::= */ yytestcase(yyruleno==64);
      case 66: /* storage ::= */ yytestcase(yyruleno==66);
      case 68: /* qtime ::= */ yytestcase(yyruleno==68);
      case 70: /* users ::= */ yytestcase(yyruleno==70);
      case 72: /* conns ::= */ yytestcase(yyruleno==72);
      case 74: /* state ::= */ yytestcase(yyruleno==74);
{ yymsp[1].minor.yy0.n = 0;   }
        break;
      case 59: /* pps ::= PPS INTEGER */
      case 61: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==61);
      case 63: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==63);
      case 65: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==67);
      case 69: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==69);
      case 71: /* users ::= USERS INTEGER */ yytestcase(yyruleno==71);
      case 73: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==73);
      case 75: /* state ::= STATE ids */ yytestcase(yyruleno==75);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 76: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yylhsminor.yy419.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy419.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy419.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy419.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy419.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy419.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy419.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy419.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy419.stat    = yymsp[0].minor.yy0;
}
  yymsp[-8].minor.yy419 = yylhsminor.yy419;
        break;
      case 77: /* keep ::= KEEP tagitemlist */
{ yymsp[-1].minor.yy429 = yymsp[0].minor.yy429; }
        break;
      case 78: /* cache ::= CACHE INTEGER */
      case 79: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==79);
      case 80: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==80);
      case 81: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==81);
      case 82: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==82);
      case 83: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==83);
      case 84: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==84);
      case 85: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==85);
      case 86: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==86);
      case 87: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==87);
      case 88: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==88);
      case 89: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==89);
      case 90: /* update ::= UPDATE INTEGER */ yytestcase(yyruleno==90);
      case 91: /* cachelast ::= CACHELAST INTEGER */ yytestcase(yyruleno==91);
      case 92: /* partitions ::= PARTITIONS INTEGER */ yytestcase(yyruleno==92);
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 93: /* db_optr ::= */
{setDefaultCreateDbOption(&yymsp[1].minor.yy94); yymsp[1].minor.yy94.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 94: /* db_optr ::= db_optr cache */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 95: /* db_optr ::= db_optr replica */
      case 112: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==112);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 96: /* db_optr ::= db_optr quorum */
      case 113: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==113);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 97: /* db_optr ::= db_optr days */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 98: /* db_optr ::= db_optr minrows */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 99: /* db_optr ::= db_optr maxrows */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 100: /* db_optr ::= db_optr blocks */
      case 115: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==115);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 101: /* db_optr ::= db_optr ctime */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 102: /* db_optr ::= db_optr wal */
      case 117: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==117);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 103: /* db_optr ::= db_optr fsync */
      case 118: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==118);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 104: /* db_optr ::= db_optr comp */
      case 116: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==116);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 105: /* db_optr ::= db_optr prec */
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.precision = yymsp[0].minor.yy0; }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 106: /* db_optr ::= db_optr keep */
      case 114: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==114);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.keep = yymsp[0].minor.yy429; }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 107: /* db_optr ::= db_optr update */
      case 119: /* alter_db_optr ::= alter_db_optr update */ yytestcase(yyruleno==119);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.update = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 108: /* db_optr ::= db_optr cachelast */
      case 120: /* alter_db_optr ::= alter_db_optr cachelast */ yytestcase(yyruleno==120);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.cachelast = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 109: /* topic_optr ::= db_optr */
      case 121: /* alter_topic_optr ::= alter_db_optr */ yytestcase(yyruleno==121);
{ yylhsminor.yy94 = yymsp[0].minor.yy94; yylhsminor.yy94.dbType = TSDB_DB_TYPE_TOPIC; }
  yymsp[0].minor.yy94 = yylhsminor.yy94;
        break;
      case 110: /* topic_optr ::= topic_optr partitions */
      case 122: /* alter_topic_optr ::= alter_topic_optr partitions */ yytestcase(yyruleno==122);
{ yylhsminor.yy94 = yymsp[-1].minor.yy94; yylhsminor.yy94.partitions = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[-1].minor.yy94 = yylhsminor.yy94;
        break;
      case 111: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yymsp[1].minor.yy94); yymsp[1].minor.yy94.dbType = TSDB_DB_TYPE_DEFAULT;}
        break;
      case 123: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSetColumnType (&yylhsminor.yy451, &yymsp[0].minor.yy0);
}
  yymsp[0].minor.yy451 = yylhsminor.yy451;
        break;
      case 124: /* typename ::= ids LP signed RP */
{
  if (yymsp[-1].minor.yy481 <= 0) {
    yymsp[-3].minor.yy0.type = 0;
    tSetColumnType(&yylhsminor.yy451, &yymsp[-3].minor.yy0);
  } else {
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy481;  // negative value of name length
    tSetColumnType(&yylhsminor.yy451, &yymsp[-3].minor.yy0);
  }
}
  yymsp[-3].minor.yy451 = yylhsminor.yy451;
        break;
      case 125: /* typename ::= ids UNSIGNED */
{
  yymsp[-1].minor.yy0.type = 0;
  yymsp[-1].minor.yy0.n = ((yymsp[0].minor.yy0.z + yymsp[0].minor.yy0.n) - yymsp[-1].minor.yy0.z);
  tSetColumnType (&yylhsminor.yy451, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy451 = yylhsminor.yy451;
        break;
      case 126: /* signed ::= INTEGER */
{ yylhsminor.yy481 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
  yymsp[0].minor.yy481 = yylhsminor.yy481;
        break;
      case 127: /* signed ::= PLUS INTEGER */
{ yymsp[-1].minor.yy481 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 128: /* signed ::= MINUS INTEGER */
{ yymsp[-1].minor.yy481 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 132: /* cmd ::= CREATE TABLE create_table_list */
{ pInfo->type = TSDB_SQL_CREATE_TABLE; pInfo->pCreateTableInfo = yymsp[0].minor.yy194;}
        break;
      case 133: /* create_table_list ::= create_from_stable */
{
  SCreateTableSql* pCreateTable = calloc(1, sizeof(SCreateTableSql));
  pCreateTable->childTableInfo = taosArrayInit(4, sizeof(SCreatedTableInfo));

  taosArrayPush(pCreateTable->childTableInfo, &yymsp[0].minor.yy252);
  pCreateTable->type = TSQL_CREATE_TABLE_FROM_STABLE;
  yylhsminor.yy194 = pCreateTable;
}
  yymsp[0].minor.yy194 = yylhsminor.yy194;
        break;
      case 134: /* create_table_list ::= create_table_list create_from_stable */
{
  taosArrayPush(yymsp[-1].minor.yy194->childTableInfo, &yymsp[0].minor.yy252);
  yylhsminor.yy194 = yymsp[-1].minor.yy194;
}
  yymsp[-1].minor.yy194 = yylhsminor.yy194;
        break;
      case 135: /* create_table_args ::= ifnotexists ids cpxName LP columnlist RP */
{
  yylhsminor.yy194 = tSetCreateTableInfo(yymsp[-1].minor.yy429, NULL, NULL, TSQL_CREATE_TABLE);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-4].minor.yy0, &yymsp[-5].minor.yy0);
}
  yymsp[-5].minor.yy194 = yylhsminor.yy194;
        break;
      case 136: /* create_stable_args ::= ifnotexists ids cpxName LP columnlist RP TAGS LP columnlist RP */
{
  yylhsminor.yy194 = tSetCreateTableInfo(yymsp[-5].minor.yy429, yymsp[-1].minor.yy429, NULL, TSQL_CREATE_STABLE);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy194 = yylhsminor.yy194;
        break;
      case 137: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName TAGS LP tagitemlist RP */
{
  yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yylhsminor.yy252 = createNewChildTableInfo(&yymsp[-5].minor.yy0, NULL, yymsp[-1].minor.yy429, &yymsp[-8].minor.yy0, &yymsp[-9].minor.yy0);
}
  yymsp[-9].minor.yy252 = yylhsminor.yy252;
        break;
      case 138: /* create_from_stable ::= ifnotexists ids cpxName USING ids cpxName LP tagNamelist RP TAGS LP tagitemlist RP */
{
  yymsp[-8].minor.yy0.n += yymsp[-7].minor.yy0.n;
  yymsp[-11].minor.yy0.n += yymsp[-10].minor.yy0.n;
  yylhsminor.yy252 = createNewChildTableInfo(&yymsp[-8].minor.yy0, yymsp[-5].minor.yy429, yymsp[-1].minor.yy429, &yymsp[-11].minor.yy0, &yymsp[-12].minor.yy0);
}
  yymsp[-12].minor.yy252 = yylhsminor.yy252;
        break;
      case 139: /* tagNamelist ::= tagNamelist COMMA ids */
{taosArrayPush(yymsp[-2].minor.yy429, &yymsp[0].minor.yy0); yylhsminor.yy429 = yymsp[-2].minor.yy429;  }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 140: /* tagNamelist ::= ids */
{yylhsminor.yy429 = taosArrayInit(4, sizeof(SStrToken)); taosArrayPush(yylhsminor.yy429, &yymsp[0].minor.yy0);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 141: /* create_table_args ::= ifnotexists ids cpxName AS select */
{
  yylhsminor.yy194 = tSetCreateTableInfo(NULL, NULL, yymsp[0].minor.yy254, TSQL_CREATE_STREAM);
  setSqlInfo(pInfo, yylhsminor.yy194, NULL, TSDB_SQL_CREATE_TABLE);

  yymsp[-3].minor.yy0.n += yymsp[-2].minor.yy0.n;
  setCreatedTableName(pInfo, &yymsp[-3].minor.yy0, &yymsp[-4].minor.yy0);
}
  yymsp[-4].minor.yy194 = yylhsminor.yy194;
        break;
      case 142: /* columnlist ::= columnlist COMMA column */
{taosArrayPush(yymsp[-2].minor.yy429, &yymsp[0].minor.yy451); yylhsminor.yy429 = yymsp[-2].minor.yy429;  }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 143: /* columnlist ::= column */
{yylhsminor.yy429 = taosArrayInit(4, sizeof(TAOS_FIELD)); taosArrayPush(yylhsminor.yy429, &yymsp[0].minor.yy451);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 144: /* column ::= ids typename */
{
  tSetColumnInfo(&yylhsminor.yy451, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy451);
}
  yymsp[-1].minor.yy451 = yylhsminor.yy451;
        break;
      case 145: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yylhsminor.yy429 = tVariantListAppend(yymsp[-2].minor.yy429, &yymsp[0].minor.yy218, -1);    }
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 146: /* tagitemlist ::= tagitem */
{ yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[0].minor.yy218, -1); }
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 147: /* tagitem ::= INTEGER */
      case 148: /* tagitem ::= FLOAT */ yytestcase(yyruleno==148);
      case 149: /* tagitem ::= STRING */ yytestcase(yyruleno==149);
      case 150: /* tagitem ::= BOOL */ yytestcase(yyruleno==150);
{ toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy218, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy218 = yylhsminor.yy218;
        break;
      case 151: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy218, &yymsp[0].minor.yy0); }
  yymsp[0].minor.yy218 = yylhsminor.yy218;
        break;
      case 152: /* tagitem ::= MINUS INTEGER */
      case 153: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==153);
      case 154: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==154);
      case 155: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==155);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy218, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy218 = yylhsminor.yy218;
        break;
      case 156: /* select ::= SELECT selcollist from where_opt interval_opt session_option fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yylhsminor.yy254 = tSetQuerySqlNode(&yymsp[-12].minor.yy0, yymsp[-11].minor.yy429, yymsp[-10].minor.yy70, yymsp[-9].minor.yy170, yymsp[-4].minor.yy429, yymsp[-3].minor.yy429, &yymsp[-8].minor.yy220, &yymsp[-7].minor.yy87, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy429, &yymsp[0].minor.yy18, &yymsp[-1].minor.yy18, yymsp[-2].minor.yy170);
}
  yymsp[-12].minor.yy254 = yylhsminor.yy254;
        break;
      case 157: /* select ::= LP select RP */
{yymsp[-2].minor.yy254 = yymsp[-1].minor.yy254;}
        break;
      case 158: /* union ::= select */
{ yylhsminor.yy141 = setSubclause(NULL, yymsp[0].minor.yy254); }
  yymsp[0].minor.yy141 = yylhsminor.yy141;
        break;
      case 159: /* union ::= union UNION ALL select */
{ yylhsminor.yy141 = appendSelectClause(yymsp[-3].minor.yy141, yymsp[0].minor.yy254); }
  yymsp[-3].minor.yy141 = yylhsminor.yy141;
        break;
      case 160: /* cmd ::= union */
{ setSqlInfo(pInfo, yymsp[0].minor.yy141, NULL, TSDB_SQL_SELECT); }
        break;
      case 161: /* select ::= SELECT selcollist */
{
  yylhsminor.yy254 = tSetQuerySqlNode(&yymsp[-1].minor.yy0, yymsp[0].minor.yy429, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
  yymsp[-1].minor.yy254 = yylhsminor.yy254;
        break;
      case 162: /* sclp ::= selcollist COMMA */
{yylhsminor.yy429 = yymsp[-1].minor.yy429;}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 163: /* sclp ::= */
      case 188: /* orderby_opt ::= */ yytestcase(yyruleno==188);
{yymsp[1].minor.yy429 = 0;}
        break;
      case 164: /* selcollist ::= sclp distinct expr as */
{
   yylhsminor.yy429 = tSqlExprListAppend(yymsp[-3].minor.yy429, yymsp[-1].minor.yy170,  yymsp[-2].minor.yy0.n? &yymsp[-2].minor.yy0:0, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
  yymsp[-3].minor.yy429 = yylhsminor.yy429;
        break;
      case 165: /* selcollist ::= sclp STAR */
{
   tSqlExpr *pNode = tSqlExprCreateIdValue(NULL, TK_ALL);
   yylhsminor.yy429 = tSqlExprListAppend(yymsp[-1].minor.yy429, pNode, 0, 0);
}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 166: /* as ::= AS ids */
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 167: /* as ::= ids */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 168: /* as ::= */
{ yymsp[1].minor.yy0.n = 0;  }
        break;
      case 169: /* distinct ::= DISTINCT */
{ yylhsminor.yy0 = yymsp[0].minor.yy0;  }
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 171: /* from ::= FROM tablelist */
{yymsp[-1].minor.yy70 = yymsp[0].minor.yy429;}
        break;
      case 172: /* from ::= FROM LP union RP */
{yymsp[-3].minor.yy70 = yymsp[-1].minor.yy141;}
        break;
      case 173: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy429 = setTableNameList(NULL, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 174: /* tablelist ::= ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
  yylhsminor.yy429 = setTableNameList(NULL, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 175: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yylhsminor.yy429 = setTableNameList(yymsp[-3].minor.yy429, &yymsp[-1].minor.yy0, NULL);
}
  yymsp[-3].minor.yy429 = yylhsminor.yy429;
        break;
      case 176: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
  toTSDBType(yymsp[-2].minor.yy0.type);
  toTSDBType(yymsp[0].minor.yy0.type);
  yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;

  yylhsminor.yy429 = setTableNameList(yymsp[-4].minor.yy429, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);
}
  yymsp[-4].minor.yy429 = yylhsminor.yy429;
        break;
      case 177: /* tmvar ::= VARIABLE */
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 178: /* interval_opt ::= INTERVAL LP tmvar RP */
{yymsp[-3].minor.yy220.interval = yymsp[-1].minor.yy0; yymsp[-3].minor.yy220.offset.n = 0;}
        break;
      case 179: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yymsp[-5].minor.yy220.interval = yymsp[-3].minor.yy0; yymsp[-5].minor.yy220.offset = yymsp[-1].minor.yy0;}
        break;
      case 180: /* interval_opt ::= */
{memset(&yymsp[1].minor.yy220, 0, sizeof(yymsp[1].minor.yy220));}
        break;
      case 181: /* session_option ::= */
{yymsp[1].minor.yy87.col.n = 0; yymsp[1].minor.yy87.gap.n = 0;}
        break;
      case 182: /* session_option ::= SESSION LP ids cpxName COMMA tmvar RP */
{
   yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
   yymsp[-6].minor.yy87.col = yymsp[-4].minor.yy0;
   yymsp[-6].minor.yy87.gap = yymsp[-1].minor.yy0;
}
        break;
      case 183: /* fill_opt ::= */
{ yymsp[1].minor.yy429 = 0;     }
        break;
      case 184: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy429, &A, -1, 0);
    yymsp[-5].minor.yy429 = yymsp[-1].minor.yy429;
}
        break;
      case 185: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy429 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 186: /* sliding_opt ::= SLIDING LP tmvar RP */
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 187: /* sliding_opt ::= */
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
        break;
      case 189: /* orderby_opt ::= ORDER BY sortlist */
{yymsp[-2].minor.yy429 = yymsp[0].minor.yy429;}
        break;
      case 190: /* sortlist ::= sortlist COMMA item sortorder */
{
    yylhsminor.yy429 = tVariantListAppend(yymsp[-3].minor.yy429, &yymsp[-1].minor.yy218, yymsp[0].minor.yy116);
}
  yymsp[-3].minor.yy429 = yylhsminor.yy429;
        break;
      case 191: /* sortlist ::= item sortorder */
{
  yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[-1].minor.yy218, yymsp[0].minor.yy116);
}
  yymsp[-1].minor.yy429 = yylhsminor.yy429;
        break;
      case 192: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy218, &yymsp[-1].minor.yy0);
}
  yymsp[-1].minor.yy218 = yylhsminor.yy218;
        break;
      case 193: /* sortorder ::= ASC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 194: /* sortorder ::= DESC */
{ yymsp[0].minor.yy116 = TSDB_ORDER_DESC;}
        break;
      case 195: /* sortorder ::= */
{ yymsp[1].minor.yy116 = TSDB_ORDER_ASC; }
        break;
      case 196: /* groupby_opt ::= */
{ yymsp[1].minor.yy429 = 0;}
        break;
      case 197: /* groupby_opt ::= GROUP BY grouplist */
{ yymsp[-2].minor.yy429 = yymsp[0].minor.yy429;}
        break;
      case 198: /* grouplist ::= grouplist COMMA item */
{
  yylhsminor.yy429 = tVariantListAppend(yymsp[-2].minor.yy429, &yymsp[0].minor.yy218, -1);
}
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 199: /* grouplist ::= item */
{
  yylhsminor.yy429 = tVariantListAppend(NULL, &yymsp[0].minor.yy218, -1);
}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 200: /* having_opt ::= */
      case 210: /* where_opt ::= */ yytestcase(yyruleno==210);
      case 249: /* expritem ::= */ yytestcase(yyruleno==249);
{yymsp[1].minor.yy170 = 0;}
        break;
      case 201: /* having_opt ::= HAVING expr */
      case 211: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==211);
{yymsp[-1].minor.yy170 = yymsp[0].minor.yy170;}
        break;
      case 202: /* limit_opt ::= */
      case 206: /* slimit_opt ::= */ yytestcase(yyruleno==206);
{yymsp[1].minor.yy18.limit = -1; yymsp[1].minor.yy18.offset = 0;}
        break;
      case 203: /* limit_opt ::= LIMIT signed */
      case 207: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==207);
{yymsp[-1].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-1].minor.yy18.offset = 0;}
        break;
      case 204: /* limit_opt ::= LIMIT signed OFFSET signed */
{ yymsp[-3].minor.yy18.limit = yymsp[-2].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[0].minor.yy481;}
        break;
      case 205: /* limit_opt ::= LIMIT signed COMMA signed */
{ yymsp[-3].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[-2].minor.yy481;}
        break;
      case 208: /* slimit_opt ::= SLIMIT signed SOFFSET signed */
{yymsp[-3].minor.yy18.limit = yymsp[-2].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[0].minor.yy481;}
        break;
      case 209: /* slimit_opt ::= SLIMIT signed COMMA signed */
{yymsp[-3].minor.yy18.limit = yymsp[0].minor.yy481;  yymsp[-3].minor.yy18.offset = yymsp[-2].minor.yy481;}
        break;
      case 212: /* expr ::= LP expr RP */
{yylhsminor.yy170 = yymsp[-1].minor.yy170; yylhsminor.yy170->token.z = yymsp[-2].minor.yy0.z; yylhsminor.yy170->token.n = (yymsp[0].minor.yy0.z - yymsp[-2].minor.yy0.z + 1);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 213: /* expr ::= ID */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_ID);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 214: /* expr ::= ID DOT ID */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ID);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 215: /* expr ::= ID DOT STAR */
{ yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-2].minor.yy0, TK_ALL);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 216: /* expr ::= INTEGER */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_INTEGER);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 217: /* expr ::= MINUS INTEGER */
      case 218: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==218);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_INTEGER);}
  yymsp[-1].minor.yy170 = yylhsminor.yy170;
        break;
      case 219: /* expr ::= FLOAT */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_FLOAT);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 220: /* expr ::= MINUS FLOAT */
      case 221: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==221);
{ yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[-1].minor.yy0, TK_FLOAT);}
  yymsp[-1].minor.yy170 = yylhsminor.yy170;
        break;
      case 222: /* expr ::= STRING */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_STRING);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 223: /* expr ::= NOW */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_NOW); }
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 224: /* expr ::= VARIABLE */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_VARIABLE);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 225: /* expr ::= BOOL */
{ yylhsminor.yy170 = tSqlExprCreateIdValue(&yymsp[0].minor.yy0, TK_BOOL);}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 226: /* expr ::= ID LP exprlist RP */
{ yylhsminor.yy170 = tSqlExprCreateFunction(yymsp[-1].minor.yy429, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 227: /* expr ::= ID LP STAR RP */
{ yylhsminor.yy170 = tSqlExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 228: /* expr ::= expr IS NULL */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, NULL, TK_ISNULL);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 229: /* expr ::= expr IS NOT NULL */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-3].minor.yy170, NULL, TK_NOTNULL);}
  yymsp[-3].minor.yy170 = yylhsminor.yy170;
        break;
      case 230: /* expr ::= expr LT expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LT);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 231: /* expr ::= expr GT expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_GT);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 232: /* expr ::= expr LE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 233: /* expr ::= expr GE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_GE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 234: /* expr ::= expr NE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_NE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 235: /* expr ::= expr EQ expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_EQ);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 236: /* expr ::= expr BETWEEN expr AND expr */
{ tSqlExpr* X2 = tSqlExprClone(yymsp[-4].minor.yy170); yylhsminor.yy170 = tSqlExprCreate(tSqlExprCreate(yymsp[-4].minor.yy170, yymsp[-2].minor.yy170, TK_GE), tSqlExprCreate(X2, yymsp[0].minor.yy170, TK_LE), TK_AND);}
  yymsp[-4].minor.yy170 = yylhsminor.yy170;
        break;
      case 237: /* expr ::= expr AND expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_AND);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 238: /* expr ::= expr OR expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_OR); }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 239: /* expr ::= expr PLUS expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_PLUS);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 240: /* expr ::= expr MINUS expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_MINUS); }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 241: /* expr ::= expr STAR expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_STAR);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 242: /* expr ::= expr SLASH expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_DIVIDE);}
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 243: /* expr ::= expr REM expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_REM);   }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 244: /* expr ::= expr LIKE expr */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-2].minor.yy170, yymsp[0].minor.yy170, TK_LIKE);  }
  yymsp[-2].minor.yy170 = yylhsminor.yy170;
        break;
      case 245: /* expr ::= expr IN LP exprlist RP */
{yylhsminor.yy170 = tSqlExprCreate(yymsp[-4].minor.yy170, (tSqlExpr*)yymsp[-1].minor.yy429, TK_IN); }
  yymsp[-4].minor.yy170 = yylhsminor.yy170;
        break;
      case 246: /* exprlist ::= exprlist COMMA expritem */
{yylhsminor.yy429 = tSqlExprListAppend(yymsp[-2].minor.yy429,yymsp[0].minor.yy170,0, 0);}
  yymsp[-2].minor.yy429 = yylhsminor.yy429;
        break;
      case 247: /* exprlist ::= expritem */
{yylhsminor.yy429 = tSqlExprListAppend(0,yymsp[0].minor.yy170,0, 0);}
  yymsp[0].minor.yy429 = yylhsminor.yy429;
        break;
      case 248: /* expritem ::= expr */
{yylhsminor.yy170 = yymsp[0].minor.yy170;}
  yymsp[0].minor.yy170 = yylhsminor.yy170;
        break;
      case 250: /* cmd ::= RESET QUERY CACHE */
{ setDCLSqlElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 251: /* cmd ::= SYNCDB ids REPLICA */
{ setDCLSqlElems(pInfo, TSDB_SQL_SYNC_DB_REPLICA, 1, &yymsp[-1].minor.yy0);}
        break;
      case 252: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 253: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 254: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 255: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 256: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 257: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy218, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL, -1);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 258: /* cmd ::= ALTER STABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 259: /* cmd ::= ALTER STABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 260: /* cmd ::= ALTER STABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, yymsp[0].minor.yy429, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 261: /* cmd ::= ALTER STABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    SArray* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableInfo* pAlterTable = tSetAlterTableInfo(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN, TSDB_SUPER_TABLE);
    setSqlInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 262: /* cmd ::= ALTER STABLE ids cpxName CHANGE TAG ids ids */
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
      case 263: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSql(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 264: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSql(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 265: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
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
