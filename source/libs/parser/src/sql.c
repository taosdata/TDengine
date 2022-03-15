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

#include "nodes.h"
#include "parToken.h"
#include "ttokendef.h"
#include "parAst.h"
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
#define YYCODETYPE unsigned char
#define YYNOCODE 218
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SToken yy5;
  bool yy25;
  SNodeList* yy40;
  ENullOrder yy53;
  EOrder yy54;
  SNode* yy68;
  EJoinType yy92;
  EFillMode yy94;
  SDataType yy372;
  EOperatorType yy416;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL  SAstCreateContext* pCxt ;
#define ParseARG_PDECL , SAstCreateContext* pCxt 
#define ParseARG_PARAM ,pCxt 
#define ParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define ParseARG_STORE yypParser->pCxt =pCxt ;
#define ParseCTX_SDECL
#define ParseCTX_PDECL
#define ParseCTX_PARAM
#define ParseCTX_FETCH
#define ParseCTX_STORE
#define YYNSTATE             313
#define YYNRULE              248
#define YYNTOKEN             140
#define YY_MAX_SHIFT         312
#define YY_MIN_SHIFTREDUCE   480
#define YY_MAX_SHIFTREDUCE   727
#define YY_ERROR_ACTION      728
#define YY_ACCEPT_ACTION     729
#define YY_NO_ACTION         730
#define YY_MIN_REDUCE        731
#define YY_MAX_REDUCE        978
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
#define YY_ACTTAB_COUNT (963)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   152,  236,  277,  871,  844,  252,  252,  167,  186,  784,
 /*    10 */   847,  844,   31,   29,   27,   26,   25,  846,  844,  209,
 /*    20 */   787,  787,  309,  308,  846,   31,   29,   27,   26,   25,
 /*    30 */   746,  236,  151,  871,  106,  826,   39,  824,  251,  844,
 /*    40 */   145,  957,  238,   61,  236,  856,  871,  782,   57,  857,
 /*    50 */   860,  896,  910,  145,  956,  154,  892,  969,  955,  187,
 /*    60 */   164,  610,  213,  832,  239,  166,  930,   10,  826,  907,
 /*    70 */   824,  251,  301,  300,  299,  298,  297,  296,  295,  294,
 /*    80 */   293,  292,  291,  290,  289,  288,  287,  286,  285,  599,
 /*    90 */    31,   29,   27,   26,   25,   24,  109,   23,  159,  228,
 /*   100 */   628,  629,  630,  631,  632,  633,  634,  636,  637,  638,
 /*   110 */    23,  159,   39,  628,  629,  630,  631,  632,  633,  634,
 /*   120 */   636,  637,  638,  783,  542,  275,  274,  273,  546,  272,
 /*   130 */   548,  549,  271,  551,  268,  165,  557,  265,  559,  560,
 /*   140 */   262,  259,  236,   54,  871,  694,  251,  789,   70,  252,
 /*   150 */   844,   65,  249,  238,  779,  224,  856,  208,  214,   56,
 /*   160 */   857,  860,  896,  229,  787,  601,  144,  892,  775,  205,
 /*   170 */   692,  693,  695,  696,  220,   10,  910,   82,  957,   31,
 /*   180 */    29,   27,   26,   25,  215,  210,  223,  170,  871,   64,
 /*   190 */   826,   81,  824,  906,  844,  955,  600,  238,  773,  826,
 /*   200 */   856,  825,  224,   57,  857,  860,  896,  187,   62,  910,
 /*   210 */   154,  892,   76,  723,  724,   27,   26,   25,  102,  903,
 /*   220 */   219,  220,  218,  284,  105,  957,  905,  729,  610,   82,
 /*   230 */   201,  923,   87,  223,   21,  871,   64,  284,   81,  227,
 /*   240 */    75,  844,  955,  635,  238,  104,  639,  856,  490,   86,
 /*   250 */    57,  857,  860,  896,  197,   62,  190,  154,  892,   76,
 /*   260 */   173,   68,  236,  278,  871,   78,  903,  904,  127,  908,
 /*   270 */   844,  817,   40,  238,  252,   84,  856,  250,  924,   57,
 /*   280 */   857,  860,  896,  957,    9,    8,  154,  892,  969,  787,
 /*   290 */   236,   48,  871,  178,   30,   28,   81,  953,  844,  206,
 /*   300 */   955,  238,  780,  590,  856,  833,  239,   57,  857,  860,
 /*   310 */   896,  588,    6,  161,  154,  892,  969,   30,   28,  670,
 /*   320 */   169,   99,   12,  790,   70,  914,  590,   31,   29,   27,
 /*   330 */    26,   25,  789,   70,  588,  774,  161,  189,  490,  236,
 /*   340 */   602,  871,    1,  677,  646,   12,   53,  844,  491,  492,
 /*   350 */   238,   50,  224,  856,   30,   28,  133,  857,  860,  252,
 /*   360 */   599,  253,  122,  590,  252,    1,  226,  172,  915,  665,
 /*   370 */   235,  588,  590,  161,  787,  957,  589,  591,  594,  787,
 /*   380 */   588,  171,   12,  926,  253,  281,   30,   28,   81,  280,
 /*   390 */   669,  665,  955,  789,   70,  590,   82,    9,    8,  589,
 /*   400 */   591,  594,    1,  588,  282,  161,  691,   96,  236,  195,
 /*   410 */   871,  130,  193,   59,   94,  196,  844,  134,   71,  238,
 /*   420 */    91,  253,  856,  279,  221,   58,  857,  860,  896,  625,
 /*   430 */   253,   83,  895,  892,    7,  872,  589,  591,  594,  108,
 /*   440 */   236,    2,  871,  597,  174,  589,  591,  594,  844,  772,
 /*   450 */   231,  238,  188,  253,  856,   38,  640,   58,  857,  860,
 /*   460 */   896,  726,  727,   32,  234,  892,   30,   28,  589,  591,
 /*   470 */   594,   85,   30,   28,  237,  590,  236,  603,  871,  607,
 /*   480 */   191,  590,  150,  588,  844,  161,   32,  238,   82,  588,
 /*   490 */   856,  161,  198,   72,  857,  860,   30,   28,   20,  281,
 /*   500 */   598,  668,  604,  280,  602,  590,  207,   82,   31,   29,
 /*   510 */    27,   26,   25,  588,    7,  161,  232,  199,  282,  927,
 /*   520 */     7,  851,  242,   92,  236,  937,  871,  583,  849,  594,
 /*   530 */   225,  970,  844,  253,   32,  238,  160,  279,  856,  253,
 /*   540 */   114,  138,  857,  860,    1,  204,  936,  112,  589,  591,
 /*   550 */   594,  244,   95,  119,  589,  591,  594,  236,   66,  871,
 /*   560 */    67,    5,  153,  253,  917,  844,  217,   98,  238,  203,
 /*   570 */    74,  856,    4,   63,   58,  857,  860,  896,  589,  591,
 /*   580 */   594,  665,  893,  236,  220,  871,  535,  100,  601,  530,
 /*   590 */   911,  844,   33,   68,  238,  202,   59,  856,  101,   64,
 /*   600 */   138,  857,  860,  236,  155,  871,  563,  972,  233,  567,
 /*   610 */   954,  844,  572,  257,  238,  107,   67,  856,   62,   68,
 /*   620 */   137,  857,  860,   17,  230,  236,  831,  871,   79,  903,
 /*   630 */   904,  878,  908,  844,  245,   47,  238,   49,  240,  856,
 /*   640 */   241,   69,   72,  857,  860,  236,  830,  871,   67,  163,
 /*   650 */   126,  216,  246,  844,  116,  788,  238,  158,  236,  856,
 /*   660 */   871,  255,  138,  857,  860,  247,  844,  128,  123,  238,
 /*   670 */   162,  312,  856,  125,  185,  138,  857,  860,  184,  597,
 /*   680 */   971,   60,  129,  183,   22,  182,  142,  305,  143,  838,
 /*   690 */   124,  236,  749,  871,   31,   29,   27,   26,   25,  844,
 /*   700 */   176,  177,  238,  837,  179,  856,  836,  835,  136,  857,
 /*   710 */   860,  181,  180,   55,  778,  236,  120,  871,  777,  748,
 /*   720 */   745,  740,  735,  844,  776,  501,  238,  747,  739,  856,
 /*   730 */   738,  734,  139,  857,  860,  236,  733,  871,  192,  732,
 /*   740 */   828,  175,  248,  844,  200,   41,  238,  194,   88,  856,
 /*   750 */    89,   90,  131,  857,  860,  236,    3,  871,   32,   14,
 /*   760 */    93,   36,  690,  844,   73,   97,  238,  211,   42,  856,
 /*   770 */   212,  684,  140,  857,  860,  236,  683,  871,  849,   15,
 /*   780 */    19,   34,   11,  844,  662,   43,  238,   44,  236,  856,
 /*   790 */   871,  712,  132,  857,  860,  661,  844,  711,  156,  238,
 /*   800 */    35,  716,  856,  717,  103,  141,  857,  860,   80,  715,
 /*   810 */   157,   16,  236,    8,  871,  110,  608,   13,   18,  113,
 /*   820 */   844,  111,  688,  238,  115,  236,  856,  871,  827,  868,
 /*   830 */   857,  860,  117,  844,  626,   45,  238,  243,  236,  856,
 /*   840 */   871,  118,  867,  857,  860,   46,  844,   50,  592,  238,
 /*   850 */    37,  848,  856,  121,  220,  866,  857,  860,  254,  256,
 /*   860 */   236,  564,  871,  168,  258,  260,  561,  261,  844,   64,
 /*   870 */   558,  238,  263,  236,  856,  871,  264,  148,  857,  860,
 /*   880 */   552,  844,  266,  267,  238,  550,  269,  856,   62,  270,
 /*   890 */   147,  857,  860,  236,  556,  871,  555,  222,   77,  903,
 /*   900 */   904,  844,  908,  554,  238,  541,  553,  856,  276,   51,
 /*   910 */   149,  857,  860,  236,   52,  871,  571,  570,  569,  499,
 /*   920 */   283,  844,  520,  519,  238,  518,  744,  856,  517,  516,
 /*   930 */   146,  857,  860,  236,  515,  871,  514,  513,  512,  511,
 /*   940 */   510,  844,  509,  508,  238,  303,  507,  856,  506,  505,
 /*   950 */   135,  857,  860,  504,  302,  304,  737,  306,  307,  736,
 /*   960 */   731,  310,  311,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   157,  160,  163,  162,  168,  145,  145,  157,  148,  148,
 /*    10 */   174,  168,   12,   13,   14,   15,   16,  174,  168,  178,
 /*    20 */   160,  160,  142,  143,  174,   12,   13,   14,   15,   16,
 /*    30 */     0,  160,  159,  162,  216,  162,  147,  164,   31,  168,
 /*    40 */    40,  196,  171,  154,  160,  174,  162,  158,  177,  178,
 /*    50 */   179,  180,  175,   40,  209,  184,  185,  186,  213,   39,
 /*    60 */   167,   61,  178,  170,  171,  159,  195,   60,  162,  192,
 /*    70 */   164,   31,   42,   43,   44,   45,   46,   47,   48,   49,
 /*    80 */    50,   51,   52,   53,   54,   55,   56,   57,   58,   31,
 /*    90 */    12,   13,   14,   15,   16,  181,  182,   97,   98,   68,
 /*   100 */   100,  101,  102,  103,  104,  105,  106,  107,  108,  109,
 /*   110 */    97,   98,  147,  100,  101,  102,  103,  104,  105,  106,
 /*   120 */   107,  108,  109,  158,   70,   71,   72,   73,   74,   75,
 /*   130 */    76,   77,   78,   79,   80,  149,   82,   83,   84,   85,
 /*   140 */    86,   87,  160,  144,  162,   99,   31,  161,  162,  145,
 /*   150 */   168,  152,  148,  171,  155,  173,  174,   92,   31,  177,
 /*   160 */   178,  179,  180,  132,  160,   31,  184,  185,    0,  123,
 /*   170 */   124,  125,  126,  127,  145,   60,  175,  114,  196,   12,
 /*   180 */    13,   14,   15,   16,  119,  120,  160,  159,  162,  160,
 /*   190 */   162,  209,  164,  192,  168,  213,   31,  171,    0,  162,
 /*   200 */   174,  164,  173,  177,  178,  179,  180,   39,  179,  175,
 /*   210 */   184,  185,  186,  135,  136,   14,   15,   16,  189,  190,
 /*   220 */   191,  145,  193,   39,  198,  196,  192,  140,   61,  114,
 /*   230 */   204,  205,   19,  160,   97,  162,  160,   39,  209,    3,
 /*   240 */    27,  168,  213,  106,  171,  111,  109,  174,   21,   36,
 /*   250 */   177,  178,  179,  180,   61,  179,   29,  184,  185,  186,
 /*   260 */   173,   68,  160,   66,  162,  189,  190,  191,  150,  193,
 /*   270 */   168,  153,   59,  171,  145,   62,  174,  148,  205,  177,
 /*   280 */   178,  179,  180,  196,    1,    2,  184,  185,  186,  160,
 /*   290 */   160,  144,  162,  145,   12,   13,  209,  195,  168,  207,
 /*   300 */   213,  171,  155,   21,  174,  170,  171,  177,  178,  179,
 /*   310 */   180,   29,   34,   31,  184,  185,  186,   12,   13,   14,
 /*   320 */   149,  201,   40,  161,  162,  195,   21,   12,   13,   14,
 /*   330 */    15,   16,  161,  162,   29,    0,   31,  142,   21,  160,
 /*   340 */    31,  162,   60,   14,   61,   40,   60,  168,   31,   32,
 /*   350 */   171,   65,  173,  174,   12,   13,  177,  178,  179,  145,
 /*   360 */    31,   79,  148,   21,  145,   60,  130,  148,  112,  113,
 /*   370 */    40,   29,   21,   31,  160,  196,   94,   95,   96,  160,
 /*   380 */    29,  149,   40,  176,   79,   50,   12,   13,  209,   54,
 /*   390 */     4,  113,  213,  161,  162,   21,  114,    1,    2,   94,
 /*   400 */    95,   96,   60,   29,   69,   31,   61,   61,  160,   20,
 /*   410 */   162,   18,   23,   68,   68,   22,  168,   24,   25,  171,
 /*   420 */   111,   79,  174,   88,  194,  177,  178,  179,  180,   99,
 /*   430 */    79,   38,  184,  185,   60,  162,   94,   95,   96,  210,
 /*   440 */   160,  197,  162,   31,  145,   94,   95,   96,  168,    0,
 /*   450 */    68,  171,  145,   79,  174,  147,   61,  177,  178,  179,
 /*   460 */   180,  138,  139,   68,  184,  185,   12,   13,   94,   95,
 /*   470 */    96,  147,   12,   13,   14,   21,  160,   31,  162,   61,
 /*   480 */   141,   21,  141,   29,  168,   31,   68,  171,  114,   29,
 /*   490 */   174,   31,  160,  177,  178,  179,   12,   13,    2,   50,
 /*   500 */    31,  115,   31,   54,   31,   21,  122,  114,   12,   13,
 /*   510 */    14,   15,   16,   29,   60,   31,  134,  165,   69,  176,
 /*   520 */    60,   60,  121,  169,  160,  206,  162,   61,   67,   96,
 /*   530 */   214,  215,  168,   79,   68,  171,  172,   88,  174,   79,
 /*   540 */    61,  177,  178,  179,   60,  168,  206,   68,   94,   95,
 /*   550 */    96,   61,  169,   61,   94,   95,   96,  160,   68,  162,
 /*   560 */    68,  129,  168,   79,  203,  168,  128,  202,  171,  117,
 /*   570 */   200,  174,  116,  160,  177,  178,  179,  180,   94,   95,
 /*   580 */    96,  113,  185,  160,  145,  162,   61,  199,   31,   61,
 /*   590 */   175,  168,  110,   68,  171,  172,   68,  174,  187,  160,
 /*   600 */   177,  178,  179,  160,  137,  162,   61,  217,  133,   61,
 /*   610 */   212,  168,   61,   68,  171,  211,   68,  174,  179,   68,
 /*   620 */   177,  178,  179,   60,  131,  160,  169,  162,  189,  190,
 /*   630 */   191,  183,  193,  168,   91,  144,  171,   60,  168,  174,
 /*   640 */   168,   61,  177,  178,  179,  160,  169,  162,   68,  168,
 /*   650 */   153,  208,  166,  168,  160,  160,  171,  172,  160,  174,
 /*   660 */   162,  156,  177,  178,  179,  165,  168,  145,  144,  171,
 /*   670 */   172,  141,  174,   19,   26,  177,  178,  179,   30,   31,
 /*   680 */   215,   27,  146,   35,    2,   37,  151,   33,  151,    0,
 /*   690 */    36,  160,    0,  162,   12,   13,   14,   15,   16,  168,
 /*   700 */    56,   67,  171,    0,   56,  174,    0,    0,  177,  178,
 /*   710 */   179,   63,   64,   59,    0,  160,   62,  162,    0,    0,
 /*   720 */     0,    0,    0,  168,    0,   41,  171,    0,    0,  174,
 /*   730 */     0,    0,  177,  178,  179,  160,    0,  162,   21,    0,
 /*   740 */     0,   93,   88,  168,   90,   60,  171,   21,   19,  174,
 /*   750 */    34,   89,  177,  178,  179,  160,   68,  162,   68,  118,
 /*   760 */    61,   68,   61,  168,   60,   60,  171,   29,   60,  174,
 /*   770 */    68,   61,  177,  178,  179,  160,   61,  162,   67,  118,
 /*   780 */    68,  112,  118,  168,   61,   60,  171,    4,  160,  174,
 /*   790 */   162,   29,  177,  178,  179,   61,  168,   29,   29,  171,
 /*   800 */    68,   29,  174,   61,   67,  177,  178,  179,   67,   29,
 /*   810 */    29,   68,  160,    2,  162,   67,   61,   60,   60,   60,
 /*   820 */   168,   61,   61,  171,   60,  160,  174,  162,    0,  177,
 /*   830 */   178,  179,   34,  168,   99,   60,  171,   92,  160,  174,
 /*   840 */   162,   89,  177,  178,  179,   60,  168,   65,   21,  171,
 /*   850 */    60,   67,  174,   67,  145,  177,  178,  179,   66,   29,
 /*   860 */   160,   61,  162,   29,   60,   29,   61,   60,  168,  160,
 /*   870 */    61,  171,   29,  160,  174,  162,   60,  177,  178,  179,
 /*   880 */    61,  168,   29,   60,  171,   61,   29,  174,  179,   60,
 /*   890 */   177,  178,  179,  160,   81,  162,   81,  188,  189,  190,
 /*   900 */   191,  168,  193,   81,  171,   21,   81,  174,   69,   60,
 /*   910 */   177,  178,  179,  160,   60,  162,   29,   29,   21,   41,
 /*   920 */    40,  168,   29,   29,  171,   29,    0,  174,   29,   29,
 /*   930 */   177,  178,  179,  160,   29,  162,   29,   21,   29,   29,
 /*   940 */    29,  168,   29,   29,  171,   27,   29,  174,   29,   29,
 /*   950 */   177,  178,  179,   29,   29,   34,    0,   29,   28,    0,
 /*   960 */     0,   21,   20,  218,  218,  218,  218,  218,  218,  218,
 /*   970 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   980 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*   990 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1000 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1010 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1020 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1030 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1040 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1050 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1060 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1070 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1080 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1090 */   218,  218,  218,  218,  218,  218,  218,  218,  218,  218,
 /*  1100 */   218,  218,  218,
};
#define YY_SHIFT_COUNT    (312)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (960)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   393,  282,  305,  342,  342,  342,  342,  374,  342,  342,
 /*    10 */   115,  454,  484,  460,  454,  454,  454,  454,  454,  454,
 /*    20 */   454,  454,  454,  454,  454,  454,  454,  454,  454,  454,
 /*    30 */   454,  454,  454,    7,    7,    7,  351,  351,   40,   40,
 /*    40 */    20,   58,  127,  127,   63,  165,   58,   40,   40,   58,
 /*    50 */    40,   58,   58,   58,   40,  184,    0,   13,   13,  351,
 /*    60 */   317,  168,  134,  134,  134,  198,  165,   58,   58,  197,
 /*    70 */    54,  648,   78,   46,   65,  227,  309,  256,  278,  256,
 /*    80 */   329,  236,  386,  412,   20,  412,   20,  446,  446,  469,
 /*    90 */   471,  473,  384,  401,  433,  384,  401,  433,  432,  438,
 /*   100 */   452,  456,  468,  469,  557,  482,  467,  475,  493,  563,
 /*   110 */    58,  401,  433,  433,  401,  433,  543,  469,  471,  197,
 /*   120 */   184,  469,  577,  412,  184,  446,  963,  963,  963,   30,
 /*   130 */   654,  496,  682,  167,  213,  315,  315,  315,  315,  315,
 /*   140 */   315,  315,  335,  449,  283,  137,  201,  201,  201,  201,
 /*   150 */   389,  193,  345,  346,  396,  323,   31,  382,  395,  330,
 /*   160 */   418,  461,  466,  479,  490,  492,  525,  528,  545,  548,
 /*   170 */   551,  580,  286,  689,  692,  703,  706,  644,  634,  707,
 /*   180 */   714,  718,  719,  720,  721,  722,  724,  684,  727,  728,
 /*   190 */   730,  731,  736,  717,  739,  726,  729,  740,  685,  716,
 /*   200 */   662,  688,  690,  641,  699,  693,  701,  704,  705,  710,
 /*   210 */   708,  715,  738,  702,  711,  725,  712,  661,  723,  734,
 /*   220 */   737,  669,  732,  741,  742,  743,  664,  783,  762,  768,
 /*   230 */   769,  772,  780,  781,  811,  735,  748,  755,  757,  758,
 /*   240 */   760,  761,  759,  764,  745,  775,  828,  798,  752,  785,
 /*   250 */   782,  784,  786,  827,  790,  792,  800,  830,  834,  804,
 /*   260 */   805,  836,  807,  809,  843,  816,  819,  853,  823,  824,
 /*   270 */   857,  829,  813,  815,  822,  825,  884,  839,  849,  854,
 /*   280 */   887,  888,  897,  878,  880,  893,  894,  896,  899,  900,
 /*   290 */   905,  907,  916,  909,  910,  911,  913,  914,  917,  919,
 /*   300 */   920,  924,  926,  925,  918,  921,  956,  928,  930,  959,
 /*   310 */   960,  940,  942,
};
#define YY_REDUCE_COUNT (128)
#define YY_REDUCE_MIN   (-182)
#define YY_REDUCE_MAX   (773)
static const short yy_reduce_ofst[] = {
 /*     0 */    87,  -18,   26,   73, -129,  102,  130,  179,  248,  280,
 /*    10 */    29,  316,  397,  364,  423,  443,  465,  485,  498,  531,
 /*    20 */   555,  575,  595,  615,  628,  652,  665,  678,  700,  713,
 /*    30 */   733,  753,  773,  709,   76,  439, -157, -150, -140, -139,
 /*    40 */  -111, -127, -159, -116, -155, -107,  -14,    4,  129,  -94,
 /*    50 */   214,  171,   28,  232,  219,   -1,  -86,  -86,  -86, -164,
 /*    60 */  -120,  -35, -123,    1,   34,  147,  135,  162,   37,  118,
 /*    70 */  -161,  148, -182,   92,  120,  195,  207,  230,  230,  230,
 /*    80 */   273,  229,  244,  299,  308,  307,  324,  339,  341,  332,
 /*    90 */   352,  343,  319,  354,  377,  340,  383,  394,  361,  365,
 /*   100 */   370,  388,  230,  413,  415,  411,  390,  398,  404,  448,
 /*   110 */   273,  457,  470,  472,  477,  481,  486,  494,  500,  497,
 /*   120 */   491,  495,  505,  522,  524,  530,  535,  537,  536,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*    10 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*    20 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*    30 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*    40 */   753,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*    50 */   728,  728,  728,  728,  728,  751,  728,  898,  728,  728,
 /*    60 */   728,  753,  909,  909,  909,  751,  728,  728,  728,  816,
 /*    70 */   728,  728,  973,  728,  933,  728,  925,  901,  915,  902,
 /*    80 */   728,  958,  918,  728,  753,  728,  753,  728,  728,  728,
 /*    90 */   728,  728,  940,  938,  728,  940,  938,  728,  952,  948,
 /*   100 */   931,  929,  915,  728,  728,  728,  976,  964,  960,  728,
 /*   110 */   728,  938,  728,  728,  938,  728,  829,  728,  728,  728,
 /*   120 */   751,  728,  785,  728,  751,  728,  819,  819,  754,  728,
 /*   130 */   728,  728,  728,  728,  728,  870,  951,  950,  869,  875,
 /*   140 */   874,  873,  728,  728,  728,  728,  864,  865,  863,  862,
 /*   150 */   728,  728,  728,  728,  899,  728,  961,  965,  728,  728,
 /*   160 */   728,  850,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   170 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   180 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   190 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   200 */   728,  922,  932,  728,  728,  728,  728,  728,  728,  728,
 /*   210 */   728,  728,  728,  728,  850,  728,  949,  728,  908,  904,
 /*   220 */   728,  728,  900,  728,  728,  959,  728,  728,  728,  728,
 /*   230 */   728,  728,  728,  728,  894,  728,  728,  728,  728,  728,
 /*   240 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   250 */   728,  849,  728,  728,  728,  728,  728,  728,  728,  813,
 /*   260 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   270 */   728,  728,  798,  796,  795,  794,  728,  791,  728,  728,
 /*   280 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   290 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   300 */   728,  728,  728,  728,  728,  728,  728,  728,  728,  728,
 /*   310 */   728,  728,  728,
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
  /*    1 */ "OR",
  /*    2 */ "AND",
  /*    3 */ "UNION",
  /*    4 */ "ALL",
  /*    5 */ "MINUS",
  /*    6 */ "EXCEPT",
  /*    7 */ "INTERSECT",
  /*    8 */ "NK_BITAND",
  /*    9 */ "NK_BITOR",
  /*   10 */ "NK_LSHIFT",
  /*   11 */ "NK_RSHIFT",
  /*   12 */ "NK_PLUS",
  /*   13 */ "NK_MINUS",
  /*   14 */ "NK_STAR",
  /*   15 */ "NK_SLASH",
  /*   16 */ "NK_REM",
  /*   17 */ "NK_CONCAT",
  /*   18 */ "CREATE",
  /*   19 */ "USER",
  /*   20 */ "PASS",
  /*   21 */ "NK_STRING",
  /*   22 */ "ALTER",
  /*   23 */ "PRIVILEGE",
  /*   24 */ "DROP",
  /*   25 */ "SHOW",
  /*   26 */ "USERS",
  /*   27 */ "DNODE",
  /*   28 */ "PORT",
  /*   29 */ "NK_INTEGER",
  /*   30 */ "DNODES",
  /*   31 */ "NK_ID",
  /*   32 */ "NK_IPTOKEN",
  /*   33 */ "QNODE",
  /*   34 */ "ON",
  /*   35 */ "QNODES",
  /*   36 */ "DATABASE",
  /*   37 */ "DATABASES",
  /*   38 */ "USE",
  /*   39 */ "IF",
  /*   40 */ "NOT",
  /*   41 */ "EXISTS",
  /*   42 */ "BLOCKS",
  /*   43 */ "CACHE",
  /*   44 */ "CACHELAST",
  /*   45 */ "COMP",
  /*   46 */ "DAYS",
  /*   47 */ "FSYNC",
  /*   48 */ "MAXROWS",
  /*   49 */ "MINROWS",
  /*   50 */ "KEEP",
  /*   51 */ "PRECISION",
  /*   52 */ "QUORUM",
  /*   53 */ "REPLICA",
  /*   54 */ "TTL",
  /*   55 */ "WAL",
  /*   56 */ "VGROUPS",
  /*   57 */ "SINGLE_STABLE",
  /*   58 */ "STREAM_MODE",
  /*   59 */ "TABLE",
  /*   60 */ "NK_LP",
  /*   61 */ "NK_RP",
  /*   62 */ "STABLE",
  /*   63 */ "TABLES",
  /*   64 */ "STABLES",
  /*   65 */ "USING",
  /*   66 */ "TAGS",
  /*   67 */ "NK_DOT",
  /*   68 */ "NK_COMMA",
  /*   69 */ "COMMENT",
  /*   70 */ "BOOL",
  /*   71 */ "TINYINT",
  /*   72 */ "SMALLINT",
  /*   73 */ "INT",
  /*   74 */ "INTEGER",
  /*   75 */ "BIGINT",
  /*   76 */ "FLOAT",
  /*   77 */ "DOUBLE",
  /*   78 */ "BINARY",
  /*   79 */ "TIMESTAMP",
  /*   80 */ "NCHAR",
  /*   81 */ "UNSIGNED",
  /*   82 */ "JSON",
  /*   83 */ "VARCHAR",
  /*   84 */ "MEDIUMBLOB",
  /*   85 */ "BLOB",
  /*   86 */ "VARBINARY",
  /*   87 */ "DECIMAL",
  /*   88 */ "SMA",
  /*   89 */ "INDEX",
  /*   90 */ "FULLTEXT",
  /*   91 */ "FUNCTION",
  /*   92 */ "INTERVAL",
  /*   93 */ "MNODES",
  /*   94 */ "NK_FLOAT",
  /*   95 */ "NK_BOOL",
  /*   96 */ "NK_VARIABLE",
  /*   97 */ "BETWEEN",
  /*   98 */ "IS",
  /*   99 */ "NULL",
  /*  100 */ "NK_LT",
  /*  101 */ "NK_GT",
  /*  102 */ "NK_LE",
  /*  103 */ "NK_GE",
  /*  104 */ "NK_NE",
  /*  105 */ "NK_EQ",
  /*  106 */ "LIKE",
  /*  107 */ "MATCH",
  /*  108 */ "NMATCH",
  /*  109 */ "IN",
  /*  110 */ "FROM",
  /*  111 */ "AS",
  /*  112 */ "JOIN",
  /*  113 */ "INNER",
  /*  114 */ "SELECT",
  /*  115 */ "DISTINCT",
  /*  116 */ "WHERE",
  /*  117 */ "PARTITION",
  /*  118 */ "BY",
  /*  119 */ "SESSION",
  /*  120 */ "STATE_WINDOW",
  /*  121 */ "SLIDING",
  /*  122 */ "FILL",
  /*  123 */ "VALUE",
  /*  124 */ "NONE",
  /*  125 */ "PREV",
  /*  126 */ "LINEAR",
  /*  127 */ "NEXT",
  /*  128 */ "GROUP",
  /*  129 */ "HAVING",
  /*  130 */ "ORDER",
  /*  131 */ "SLIMIT",
  /*  132 */ "SOFFSET",
  /*  133 */ "LIMIT",
  /*  134 */ "OFFSET",
  /*  135 */ "ASC",
  /*  136 */ "DESC",
  /*  137 */ "NULLS",
  /*  138 */ "FIRST",
  /*  139 */ "LAST",
  /*  140 */ "cmd",
  /*  141 */ "user_name",
  /*  142 */ "dnode_endpoint",
  /*  143 */ "dnode_host_name",
  /*  144 */ "not_exists_opt",
  /*  145 */ "db_name",
  /*  146 */ "db_options",
  /*  147 */ "exists_opt",
  /*  148 */ "full_table_name",
  /*  149 */ "column_def_list",
  /*  150 */ "tags_def_opt",
  /*  151 */ "table_options",
  /*  152 */ "multi_create_clause",
  /*  153 */ "tags_def",
  /*  154 */ "multi_drop_clause",
  /*  155 */ "create_subtable_clause",
  /*  156 */ "specific_tags_opt",
  /*  157 */ "literal_list",
  /*  158 */ "drop_table_clause",
  /*  159 */ "col_name_list",
  /*  160 */ "table_name",
  /*  161 */ "column_def",
  /*  162 */ "column_name",
  /*  163 */ "type_name",
  /*  164 */ "col_name",
  /*  165 */ "index_name",
  /*  166 */ "index_options",
  /*  167 */ "func_list",
  /*  168 */ "duration_literal",
  /*  169 */ "sliding_opt",
  /*  170 */ "func",
  /*  171 */ "function_name",
  /*  172 */ "expression_list",
  /*  173 */ "query_expression",
  /*  174 */ "literal",
  /*  175 */ "table_alias",
  /*  176 */ "column_alias",
  /*  177 */ "expression",
  /*  178 */ "column_reference",
  /*  179 */ "subquery",
  /*  180 */ "predicate",
  /*  181 */ "compare_op",
  /*  182 */ "in_op",
  /*  183 */ "in_predicate_value",
  /*  184 */ "boolean_value_expression",
  /*  185 */ "boolean_primary",
  /*  186 */ "common_expression",
  /*  187 */ "from_clause",
  /*  188 */ "table_reference_list",
  /*  189 */ "table_reference",
  /*  190 */ "table_primary",
  /*  191 */ "joined_table",
  /*  192 */ "alias_opt",
  /*  193 */ "parenthesized_joined_table",
  /*  194 */ "join_type",
  /*  195 */ "search_condition",
  /*  196 */ "query_specification",
  /*  197 */ "set_quantifier_opt",
  /*  198 */ "select_list",
  /*  199 */ "where_clause_opt",
  /*  200 */ "partition_by_clause_opt",
  /*  201 */ "twindow_clause_opt",
  /*  202 */ "group_by_clause_opt",
  /*  203 */ "having_clause_opt",
  /*  204 */ "select_sublist",
  /*  205 */ "select_item",
  /*  206 */ "fill_opt",
  /*  207 */ "fill_mode",
  /*  208 */ "group_by_list",
  /*  209 */ "query_expression_body",
  /*  210 */ "order_by_clause_opt",
  /*  211 */ "slimit_clause_opt",
  /*  212 */ "limit_clause_opt",
  /*  213 */ "query_primary",
  /*  214 */ "sort_specification_list",
  /*  215 */ "sort_specification",
  /*  216 */ "ordering_specification_opt",
  /*  217 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE USER user_name PASS NK_STRING",
 /*   1 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*   2 */ "cmd ::= ALTER USER user_name PRIVILEGE NK_STRING",
 /*   3 */ "cmd ::= DROP USER user_name",
 /*   4 */ "cmd ::= SHOW USERS",
 /*   5 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*   6 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*   7 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*   8 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*   9 */ "cmd ::= SHOW DNODES",
 /*  10 */ "dnode_endpoint ::= NK_STRING",
 /*  11 */ "dnode_host_name ::= NK_ID",
 /*  12 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  13 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  14 */ "cmd ::= SHOW QNODES",
 /*  15 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  16 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  17 */ "cmd ::= SHOW DATABASES",
 /*  18 */ "cmd ::= USE db_name",
 /*  19 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  20 */ "not_exists_opt ::=",
 /*  21 */ "exists_opt ::= IF EXISTS",
 /*  22 */ "exists_opt ::=",
 /*  23 */ "db_options ::=",
 /*  24 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  25 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  26 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  27 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  28 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  29 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  30 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  31 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  32 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  33 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  34 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  35 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  36 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  37 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  38 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  39 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  40 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  41 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  42 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  43 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  44 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  45 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  46 */ "cmd ::= SHOW TABLES",
 /*  47 */ "cmd ::= SHOW STABLES",
 /*  48 */ "multi_create_clause ::= create_subtable_clause",
 /*  49 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  50 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  51 */ "multi_drop_clause ::= drop_table_clause",
 /*  52 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  53 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  54 */ "specific_tags_opt ::=",
 /*  55 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  56 */ "full_table_name ::= table_name",
 /*  57 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  58 */ "column_def_list ::= column_def",
 /*  59 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  60 */ "column_def ::= column_name type_name",
 /*  61 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  62 */ "type_name ::= BOOL",
 /*  63 */ "type_name ::= TINYINT",
 /*  64 */ "type_name ::= SMALLINT",
 /*  65 */ "type_name ::= INT",
 /*  66 */ "type_name ::= INTEGER",
 /*  67 */ "type_name ::= BIGINT",
 /*  68 */ "type_name ::= FLOAT",
 /*  69 */ "type_name ::= DOUBLE",
 /*  70 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  71 */ "type_name ::= TIMESTAMP",
 /*  72 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  73 */ "type_name ::= TINYINT UNSIGNED",
 /*  74 */ "type_name ::= SMALLINT UNSIGNED",
 /*  75 */ "type_name ::= INT UNSIGNED",
 /*  76 */ "type_name ::= BIGINT UNSIGNED",
 /*  77 */ "type_name ::= JSON",
 /*  78 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  79 */ "type_name ::= MEDIUMBLOB",
 /*  80 */ "type_name ::= BLOB",
 /*  81 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  82 */ "type_name ::= DECIMAL",
 /*  83 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  84 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  85 */ "tags_def_opt ::=",
 /*  86 */ "tags_def_opt ::= tags_def",
 /*  87 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  88 */ "table_options ::=",
 /*  89 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  90 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  91 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  92 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  93 */ "col_name_list ::= col_name",
 /*  94 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  95 */ "col_name ::= column_name",
 /*  96 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /*  97 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /*  98 */ "index_options ::=",
 /*  99 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 100 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 101 */ "func_list ::= func",
 /* 102 */ "func_list ::= func_list NK_COMMA func",
 /* 103 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 104 */ "cmd ::= SHOW VGROUPS",
 /* 105 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 106 */ "cmd ::= SHOW MNODES",
 /* 107 */ "cmd ::= query_expression",
 /* 108 */ "literal ::= NK_INTEGER",
 /* 109 */ "literal ::= NK_FLOAT",
 /* 110 */ "literal ::= NK_STRING",
 /* 111 */ "literal ::= NK_BOOL",
 /* 112 */ "literal ::= TIMESTAMP NK_STRING",
 /* 113 */ "literal ::= duration_literal",
 /* 114 */ "duration_literal ::= NK_VARIABLE",
 /* 115 */ "literal_list ::= literal",
 /* 116 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 117 */ "db_name ::= NK_ID",
 /* 118 */ "table_name ::= NK_ID",
 /* 119 */ "column_name ::= NK_ID",
 /* 120 */ "function_name ::= NK_ID",
 /* 121 */ "table_alias ::= NK_ID",
 /* 122 */ "column_alias ::= NK_ID",
 /* 123 */ "user_name ::= NK_ID",
 /* 124 */ "index_name ::= NK_ID",
 /* 125 */ "expression ::= literal",
 /* 126 */ "expression ::= column_reference",
 /* 127 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 128 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 129 */ "expression ::= subquery",
 /* 130 */ "expression ::= NK_LP expression NK_RP",
 /* 131 */ "expression ::= NK_PLUS expression",
 /* 132 */ "expression ::= NK_MINUS expression",
 /* 133 */ "expression ::= expression NK_PLUS expression",
 /* 134 */ "expression ::= expression NK_MINUS expression",
 /* 135 */ "expression ::= expression NK_STAR expression",
 /* 136 */ "expression ::= expression NK_SLASH expression",
 /* 137 */ "expression ::= expression NK_REM expression",
 /* 138 */ "expression_list ::= expression",
 /* 139 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 140 */ "column_reference ::= column_name",
 /* 141 */ "column_reference ::= table_name NK_DOT column_name",
 /* 142 */ "predicate ::= expression compare_op expression",
 /* 143 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 144 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 145 */ "predicate ::= expression IS NULL",
 /* 146 */ "predicate ::= expression IS NOT NULL",
 /* 147 */ "predicate ::= expression in_op in_predicate_value",
 /* 148 */ "compare_op ::= NK_LT",
 /* 149 */ "compare_op ::= NK_GT",
 /* 150 */ "compare_op ::= NK_LE",
 /* 151 */ "compare_op ::= NK_GE",
 /* 152 */ "compare_op ::= NK_NE",
 /* 153 */ "compare_op ::= NK_EQ",
 /* 154 */ "compare_op ::= LIKE",
 /* 155 */ "compare_op ::= NOT LIKE",
 /* 156 */ "compare_op ::= MATCH",
 /* 157 */ "compare_op ::= NMATCH",
 /* 158 */ "in_op ::= IN",
 /* 159 */ "in_op ::= NOT IN",
 /* 160 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 161 */ "boolean_value_expression ::= boolean_primary",
 /* 162 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 163 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 164 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 165 */ "boolean_primary ::= predicate",
 /* 166 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 167 */ "common_expression ::= expression",
 /* 168 */ "common_expression ::= boolean_value_expression",
 /* 169 */ "from_clause ::= FROM table_reference_list",
 /* 170 */ "table_reference_list ::= table_reference",
 /* 171 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 172 */ "table_reference ::= table_primary",
 /* 173 */ "table_reference ::= joined_table",
 /* 174 */ "table_primary ::= table_name alias_opt",
 /* 175 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 176 */ "table_primary ::= subquery alias_opt",
 /* 177 */ "table_primary ::= parenthesized_joined_table",
 /* 178 */ "alias_opt ::=",
 /* 179 */ "alias_opt ::= table_alias",
 /* 180 */ "alias_opt ::= AS table_alias",
 /* 181 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 182 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 183 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 184 */ "join_type ::=",
 /* 185 */ "join_type ::= INNER",
 /* 186 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 187 */ "set_quantifier_opt ::=",
 /* 188 */ "set_quantifier_opt ::= DISTINCT",
 /* 189 */ "set_quantifier_opt ::= ALL",
 /* 190 */ "select_list ::= NK_STAR",
 /* 191 */ "select_list ::= select_sublist",
 /* 192 */ "select_sublist ::= select_item",
 /* 193 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 194 */ "select_item ::= common_expression",
 /* 195 */ "select_item ::= common_expression column_alias",
 /* 196 */ "select_item ::= common_expression AS column_alias",
 /* 197 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 198 */ "where_clause_opt ::=",
 /* 199 */ "where_clause_opt ::= WHERE search_condition",
 /* 200 */ "partition_by_clause_opt ::=",
 /* 201 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 202 */ "twindow_clause_opt ::=",
 /* 203 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 204 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 205 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 206 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 207 */ "sliding_opt ::=",
 /* 208 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 209 */ "fill_opt ::=",
 /* 210 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 211 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 212 */ "fill_mode ::= NONE",
 /* 213 */ "fill_mode ::= PREV",
 /* 214 */ "fill_mode ::= NULL",
 /* 215 */ "fill_mode ::= LINEAR",
 /* 216 */ "fill_mode ::= NEXT",
 /* 217 */ "group_by_clause_opt ::=",
 /* 218 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 219 */ "group_by_list ::= expression",
 /* 220 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 221 */ "having_clause_opt ::=",
 /* 222 */ "having_clause_opt ::= HAVING search_condition",
 /* 223 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 224 */ "query_expression_body ::= query_primary",
 /* 225 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 226 */ "query_primary ::= query_specification",
 /* 227 */ "order_by_clause_opt ::=",
 /* 228 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 229 */ "slimit_clause_opt ::=",
 /* 230 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 231 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 232 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 233 */ "limit_clause_opt ::=",
 /* 234 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 235 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 236 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 237 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 238 */ "search_condition ::= common_expression",
 /* 239 */ "sort_specification_list ::= sort_specification",
 /* 240 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 241 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 242 */ "ordering_specification_opt ::=",
 /* 243 */ "ordering_specification_opt ::= ASC",
 /* 244 */ "ordering_specification_opt ::= DESC",
 /* 245 */ "null_ordering_opt ::=",
 /* 246 */ "null_ordering_opt ::= NULLS FIRST",
 /* 247 */ "null_ordering_opt ::= NULLS LAST",
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
      /* Default NON-TERMINAL Destructor */
    case 140: /* cmd */
    case 146: /* db_options */
    case 148: /* full_table_name */
    case 151: /* table_options */
    case 155: /* create_subtable_clause */
    case 158: /* drop_table_clause */
    case 161: /* column_def */
    case 164: /* col_name */
    case 166: /* index_options */
    case 168: /* duration_literal */
    case 169: /* sliding_opt */
    case 170: /* func */
    case 173: /* query_expression */
    case 174: /* literal */
    case 177: /* expression */
    case 178: /* column_reference */
    case 179: /* subquery */
    case 180: /* predicate */
    case 183: /* in_predicate_value */
    case 184: /* boolean_value_expression */
    case 185: /* boolean_primary */
    case 186: /* common_expression */
    case 187: /* from_clause */
    case 188: /* table_reference_list */
    case 189: /* table_reference */
    case 190: /* table_primary */
    case 191: /* joined_table */
    case 193: /* parenthesized_joined_table */
    case 195: /* search_condition */
    case 196: /* query_specification */
    case 199: /* where_clause_opt */
    case 201: /* twindow_clause_opt */
    case 203: /* having_clause_opt */
    case 205: /* select_item */
    case 206: /* fill_opt */
    case 209: /* query_expression_body */
    case 211: /* slimit_clause_opt */
    case 212: /* limit_clause_opt */
    case 213: /* query_primary */
    case 215: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy68)); 
}
      break;
    case 141: /* user_name */
    case 142: /* dnode_endpoint */
    case 143: /* dnode_host_name */
    case 145: /* db_name */
    case 160: /* table_name */
    case 162: /* column_name */
    case 165: /* index_name */
    case 171: /* function_name */
    case 175: /* table_alias */
    case 176: /* column_alias */
    case 192: /* alias_opt */
{
 
}
      break;
    case 144: /* not_exists_opt */
    case 147: /* exists_opt */
    case 197: /* set_quantifier_opt */
{
 
}
      break;
    case 149: /* column_def_list */
    case 150: /* tags_def_opt */
    case 152: /* multi_create_clause */
    case 153: /* tags_def */
    case 154: /* multi_drop_clause */
    case 156: /* specific_tags_opt */
    case 157: /* literal_list */
    case 159: /* col_name_list */
    case 167: /* func_list */
    case 172: /* expression_list */
    case 198: /* select_list */
    case 200: /* partition_by_clause_opt */
    case 202: /* group_by_clause_opt */
    case 204: /* select_sublist */
    case 208: /* group_by_list */
    case 210: /* order_by_clause_opt */
    case 214: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy40)); 
}
      break;
    case 163: /* type_name */
{
 
}
      break;
    case 181: /* compare_op */
    case 182: /* in_op */
{
 
}
      break;
    case 194: /* join_type */
{
 
}
      break;
    case 207: /* fill_mode */
{
 
}
      break;
    case 216: /* ordering_specification_opt */
{
 
}
      break;
    case 217: /* null_ordering_opt */
{
 
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
  {  140,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  140,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  140,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  140,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  140,   -2 }, /* (4) cmd ::= SHOW USERS */
  {  140,   -3 }, /* (5) cmd ::= CREATE DNODE dnode_endpoint */
  {  140,   -5 }, /* (6) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  140,   -3 }, /* (7) cmd ::= DROP DNODE NK_INTEGER */
  {  140,   -3 }, /* (8) cmd ::= DROP DNODE dnode_endpoint */
  {  140,   -2 }, /* (9) cmd ::= SHOW DNODES */
  {  142,   -1 }, /* (10) dnode_endpoint ::= NK_STRING */
  {  143,   -1 }, /* (11) dnode_host_name ::= NK_ID */
  {  143,   -1 }, /* (12) dnode_host_name ::= NK_IPTOKEN */
  {  140,   -5 }, /* (13) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  140,   -2 }, /* (14) cmd ::= SHOW QNODES */
  {  140,   -5 }, /* (15) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  140,   -4 }, /* (16) cmd ::= DROP DATABASE exists_opt db_name */
  {  140,   -2 }, /* (17) cmd ::= SHOW DATABASES */
  {  140,   -2 }, /* (18) cmd ::= USE db_name */
  {  144,   -3 }, /* (19) not_exists_opt ::= IF NOT EXISTS */
  {  144,    0 }, /* (20) not_exists_opt ::= */
  {  147,   -2 }, /* (21) exists_opt ::= IF EXISTS */
  {  147,    0 }, /* (22) exists_opt ::= */
  {  146,    0 }, /* (23) db_options ::= */
  {  146,   -3 }, /* (24) db_options ::= db_options BLOCKS NK_INTEGER */
  {  146,   -3 }, /* (25) db_options ::= db_options CACHE NK_INTEGER */
  {  146,   -3 }, /* (26) db_options ::= db_options CACHELAST NK_INTEGER */
  {  146,   -3 }, /* (27) db_options ::= db_options COMP NK_INTEGER */
  {  146,   -3 }, /* (28) db_options ::= db_options DAYS NK_INTEGER */
  {  146,   -3 }, /* (29) db_options ::= db_options FSYNC NK_INTEGER */
  {  146,   -3 }, /* (30) db_options ::= db_options MAXROWS NK_INTEGER */
  {  146,   -3 }, /* (31) db_options ::= db_options MINROWS NK_INTEGER */
  {  146,   -3 }, /* (32) db_options ::= db_options KEEP NK_INTEGER */
  {  146,   -3 }, /* (33) db_options ::= db_options PRECISION NK_STRING */
  {  146,   -3 }, /* (34) db_options ::= db_options QUORUM NK_INTEGER */
  {  146,   -3 }, /* (35) db_options ::= db_options REPLICA NK_INTEGER */
  {  146,   -3 }, /* (36) db_options ::= db_options TTL NK_INTEGER */
  {  146,   -3 }, /* (37) db_options ::= db_options WAL NK_INTEGER */
  {  146,   -3 }, /* (38) db_options ::= db_options VGROUPS NK_INTEGER */
  {  146,   -3 }, /* (39) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  146,   -3 }, /* (40) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  140,   -9 }, /* (41) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  140,   -3 }, /* (42) cmd ::= CREATE TABLE multi_create_clause */
  {  140,   -9 }, /* (43) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  140,   -3 }, /* (44) cmd ::= DROP TABLE multi_drop_clause */
  {  140,   -4 }, /* (45) cmd ::= DROP STABLE exists_opt full_table_name */
  {  140,   -2 }, /* (46) cmd ::= SHOW TABLES */
  {  140,   -2 }, /* (47) cmd ::= SHOW STABLES */
  {  152,   -1 }, /* (48) multi_create_clause ::= create_subtable_clause */
  {  152,   -2 }, /* (49) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  155,   -9 }, /* (50) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  154,   -1 }, /* (51) multi_drop_clause ::= drop_table_clause */
  {  154,   -2 }, /* (52) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  158,   -2 }, /* (53) drop_table_clause ::= exists_opt full_table_name */
  {  156,    0 }, /* (54) specific_tags_opt ::= */
  {  156,   -3 }, /* (55) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  148,   -1 }, /* (56) full_table_name ::= table_name */
  {  148,   -3 }, /* (57) full_table_name ::= db_name NK_DOT table_name */
  {  149,   -1 }, /* (58) column_def_list ::= column_def */
  {  149,   -3 }, /* (59) column_def_list ::= column_def_list NK_COMMA column_def */
  {  161,   -2 }, /* (60) column_def ::= column_name type_name */
  {  161,   -4 }, /* (61) column_def ::= column_name type_name COMMENT NK_STRING */
  {  163,   -1 }, /* (62) type_name ::= BOOL */
  {  163,   -1 }, /* (63) type_name ::= TINYINT */
  {  163,   -1 }, /* (64) type_name ::= SMALLINT */
  {  163,   -1 }, /* (65) type_name ::= INT */
  {  163,   -1 }, /* (66) type_name ::= INTEGER */
  {  163,   -1 }, /* (67) type_name ::= BIGINT */
  {  163,   -1 }, /* (68) type_name ::= FLOAT */
  {  163,   -1 }, /* (69) type_name ::= DOUBLE */
  {  163,   -4 }, /* (70) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (71) type_name ::= TIMESTAMP */
  {  163,   -4 }, /* (72) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  163,   -2 }, /* (73) type_name ::= TINYINT UNSIGNED */
  {  163,   -2 }, /* (74) type_name ::= SMALLINT UNSIGNED */
  {  163,   -2 }, /* (75) type_name ::= INT UNSIGNED */
  {  163,   -2 }, /* (76) type_name ::= BIGINT UNSIGNED */
  {  163,   -1 }, /* (77) type_name ::= JSON */
  {  163,   -4 }, /* (78) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (79) type_name ::= MEDIUMBLOB */
  {  163,   -1 }, /* (80) type_name ::= BLOB */
  {  163,   -4 }, /* (81) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  163,   -1 }, /* (82) type_name ::= DECIMAL */
  {  163,   -4 }, /* (83) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  163,   -6 }, /* (84) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  150,    0 }, /* (85) tags_def_opt ::= */
  {  150,   -1 }, /* (86) tags_def_opt ::= tags_def */
  {  153,   -4 }, /* (87) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  151,    0 }, /* (88) table_options ::= */
  {  151,   -3 }, /* (89) table_options ::= table_options COMMENT NK_STRING */
  {  151,   -3 }, /* (90) table_options ::= table_options KEEP NK_INTEGER */
  {  151,   -3 }, /* (91) table_options ::= table_options TTL NK_INTEGER */
  {  151,   -5 }, /* (92) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  159,   -1 }, /* (93) col_name_list ::= col_name */
  {  159,   -3 }, /* (94) col_name_list ::= col_name_list NK_COMMA col_name */
  {  164,   -1 }, /* (95) col_name ::= column_name */
  {  140,   -7 }, /* (96) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  140,   -9 }, /* (97) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  166,    0 }, /* (98) index_options ::= */
  {  166,   -9 }, /* (99) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  166,  -11 }, /* (100) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  167,   -1 }, /* (101) func_list ::= func */
  {  167,   -3 }, /* (102) func_list ::= func_list NK_COMMA func */
  {  170,   -4 }, /* (103) func ::= function_name NK_LP expression_list NK_RP */
  {  140,   -2 }, /* (104) cmd ::= SHOW VGROUPS */
  {  140,   -4 }, /* (105) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  140,   -2 }, /* (106) cmd ::= SHOW MNODES */
  {  140,   -1 }, /* (107) cmd ::= query_expression */
  {  174,   -1 }, /* (108) literal ::= NK_INTEGER */
  {  174,   -1 }, /* (109) literal ::= NK_FLOAT */
  {  174,   -1 }, /* (110) literal ::= NK_STRING */
  {  174,   -1 }, /* (111) literal ::= NK_BOOL */
  {  174,   -2 }, /* (112) literal ::= TIMESTAMP NK_STRING */
  {  174,   -1 }, /* (113) literal ::= duration_literal */
  {  168,   -1 }, /* (114) duration_literal ::= NK_VARIABLE */
  {  157,   -1 }, /* (115) literal_list ::= literal */
  {  157,   -3 }, /* (116) literal_list ::= literal_list NK_COMMA literal */
  {  145,   -1 }, /* (117) db_name ::= NK_ID */
  {  160,   -1 }, /* (118) table_name ::= NK_ID */
  {  162,   -1 }, /* (119) column_name ::= NK_ID */
  {  171,   -1 }, /* (120) function_name ::= NK_ID */
  {  175,   -1 }, /* (121) table_alias ::= NK_ID */
  {  176,   -1 }, /* (122) column_alias ::= NK_ID */
  {  141,   -1 }, /* (123) user_name ::= NK_ID */
  {  165,   -1 }, /* (124) index_name ::= NK_ID */
  {  177,   -1 }, /* (125) expression ::= literal */
  {  177,   -1 }, /* (126) expression ::= column_reference */
  {  177,   -4 }, /* (127) expression ::= function_name NK_LP expression_list NK_RP */
  {  177,   -4 }, /* (128) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  177,   -1 }, /* (129) expression ::= subquery */
  {  177,   -3 }, /* (130) expression ::= NK_LP expression NK_RP */
  {  177,   -2 }, /* (131) expression ::= NK_PLUS expression */
  {  177,   -2 }, /* (132) expression ::= NK_MINUS expression */
  {  177,   -3 }, /* (133) expression ::= expression NK_PLUS expression */
  {  177,   -3 }, /* (134) expression ::= expression NK_MINUS expression */
  {  177,   -3 }, /* (135) expression ::= expression NK_STAR expression */
  {  177,   -3 }, /* (136) expression ::= expression NK_SLASH expression */
  {  177,   -3 }, /* (137) expression ::= expression NK_REM expression */
  {  172,   -1 }, /* (138) expression_list ::= expression */
  {  172,   -3 }, /* (139) expression_list ::= expression_list NK_COMMA expression */
  {  178,   -1 }, /* (140) column_reference ::= column_name */
  {  178,   -3 }, /* (141) column_reference ::= table_name NK_DOT column_name */
  {  180,   -3 }, /* (142) predicate ::= expression compare_op expression */
  {  180,   -5 }, /* (143) predicate ::= expression BETWEEN expression AND expression */
  {  180,   -6 }, /* (144) predicate ::= expression NOT BETWEEN expression AND expression */
  {  180,   -3 }, /* (145) predicate ::= expression IS NULL */
  {  180,   -4 }, /* (146) predicate ::= expression IS NOT NULL */
  {  180,   -3 }, /* (147) predicate ::= expression in_op in_predicate_value */
  {  181,   -1 }, /* (148) compare_op ::= NK_LT */
  {  181,   -1 }, /* (149) compare_op ::= NK_GT */
  {  181,   -1 }, /* (150) compare_op ::= NK_LE */
  {  181,   -1 }, /* (151) compare_op ::= NK_GE */
  {  181,   -1 }, /* (152) compare_op ::= NK_NE */
  {  181,   -1 }, /* (153) compare_op ::= NK_EQ */
  {  181,   -1 }, /* (154) compare_op ::= LIKE */
  {  181,   -2 }, /* (155) compare_op ::= NOT LIKE */
  {  181,   -1 }, /* (156) compare_op ::= MATCH */
  {  181,   -1 }, /* (157) compare_op ::= NMATCH */
  {  182,   -1 }, /* (158) in_op ::= IN */
  {  182,   -2 }, /* (159) in_op ::= NOT IN */
  {  183,   -3 }, /* (160) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  184,   -1 }, /* (161) boolean_value_expression ::= boolean_primary */
  {  184,   -2 }, /* (162) boolean_value_expression ::= NOT boolean_primary */
  {  184,   -3 }, /* (163) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  184,   -3 }, /* (164) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  185,   -1 }, /* (165) boolean_primary ::= predicate */
  {  185,   -3 }, /* (166) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  186,   -1 }, /* (167) common_expression ::= expression */
  {  186,   -1 }, /* (168) common_expression ::= boolean_value_expression */
  {  187,   -2 }, /* (169) from_clause ::= FROM table_reference_list */
  {  188,   -1 }, /* (170) table_reference_list ::= table_reference */
  {  188,   -3 }, /* (171) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  189,   -1 }, /* (172) table_reference ::= table_primary */
  {  189,   -1 }, /* (173) table_reference ::= joined_table */
  {  190,   -2 }, /* (174) table_primary ::= table_name alias_opt */
  {  190,   -4 }, /* (175) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  190,   -2 }, /* (176) table_primary ::= subquery alias_opt */
  {  190,   -1 }, /* (177) table_primary ::= parenthesized_joined_table */
  {  192,    0 }, /* (178) alias_opt ::= */
  {  192,   -1 }, /* (179) alias_opt ::= table_alias */
  {  192,   -2 }, /* (180) alias_opt ::= AS table_alias */
  {  193,   -3 }, /* (181) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  193,   -3 }, /* (182) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  191,   -6 }, /* (183) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  194,    0 }, /* (184) join_type ::= */
  {  194,   -1 }, /* (185) join_type ::= INNER */
  {  196,   -9 }, /* (186) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  197,    0 }, /* (187) set_quantifier_opt ::= */
  {  197,   -1 }, /* (188) set_quantifier_opt ::= DISTINCT */
  {  197,   -1 }, /* (189) set_quantifier_opt ::= ALL */
  {  198,   -1 }, /* (190) select_list ::= NK_STAR */
  {  198,   -1 }, /* (191) select_list ::= select_sublist */
  {  204,   -1 }, /* (192) select_sublist ::= select_item */
  {  204,   -3 }, /* (193) select_sublist ::= select_sublist NK_COMMA select_item */
  {  205,   -1 }, /* (194) select_item ::= common_expression */
  {  205,   -2 }, /* (195) select_item ::= common_expression column_alias */
  {  205,   -3 }, /* (196) select_item ::= common_expression AS column_alias */
  {  205,   -3 }, /* (197) select_item ::= table_name NK_DOT NK_STAR */
  {  199,    0 }, /* (198) where_clause_opt ::= */
  {  199,   -2 }, /* (199) where_clause_opt ::= WHERE search_condition */
  {  200,    0 }, /* (200) partition_by_clause_opt ::= */
  {  200,   -3 }, /* (201) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  201,    0 }, /* (202) twindow_clause_opt ::= */
  {  201,   -6 }, /* (203) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  201,   -4 }, /* (204) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  201,   -6 }, /* (205) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  201,   -8 }, /* (206) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  169,    0 }, /* (207) sliding_opt ::= */
  {  169,   -4 }, /* (208) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  206,    0 }, /* (209) fill_opt ::= */
  {  206,   -4 }, /* (210) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  206,   -6 }, /* (211) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  207,   -1 }, /* (212) fill_mode ::= NONE */
  {  207,   -1 }, /* (213) fill_mode ::= PREV */
  {  207,   -1 }, /* (214) fill_mode ::= NULL */
  {  207,   -1 }, /* (215) fill_mode ::= LINEAR */
  {  207,   -1 }, /* (216) fill_mode ::= NEXT */
  {  202,    0 }, /* (217) group_by_clause_opt ::= */
  {  202,   -3 }, /* (218) group_by_clause_opt ::= GROUP BY group_by_list */
  {  208,   -1 }, /* (219) group_by_list ::= expression */
  {  208,   -3 }, /* (220) group_by_list ::= group_by_list NK_COMMA expression */
  {  203,    0 }, /* (221) having_clause_opt ::= */
  {  203,   -2 }, /* (222) having_clause_opt ::= HAVING search_condition */
  {  173,   -4 }, /* (223) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  209,   -1 }, /* (224) query_expression_body ::= query_primary */
  {  209,   -4 }, /* (225) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  213,   -1 }, /* (226) query_primary ::= query_specification */
  {  210,    0 }, /* (227) order_by_clause_opt ::= */
  {  210,   -3 }, /* (228) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  211,    0 }, /* (229) slimit_clause_opt ::= */
  {  211,   -2 }, /* (230) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  211,   -4 }, /* (231) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  211,   -4 }, /* (232) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  212,    0 }, /* (233) limit_clause_opt ::= */
  {  212,   -2 }, /* (234) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  212,   -4 }, /* (235) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  212,   -4 }, /* (236) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  179,   -3 }, /* (237) subquery ::= NK_LP query_expression NK_RP */
  {  195,   -1 }, /* (238) search_condition ::= common_expression */
  {  214,   -1 }, /* (239) sort_specification_list ::= sort_specification */
  {  214,   -3 }, /* (240) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  215,   -3 }, /* (241) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  216,    0 }, /* (242) ordering_specification_opt ::= */
  {  216,   -1 }, /* (243) ordering_specification_opt ::= ASC */
  {  216,   -1 }, /* (244) ordering_specification_opt ::= DESC */
  {  217,    0 }, /* (245) null_ordering_opt ::= */
  {  217,   -2 }, /* (246) null_ordering_opt ::= NULLS FIRST */
  {  217,   -2 }, /* (247) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0);}
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy5, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0);}
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy5, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0);}
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy5); }
        break;
      case 4: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy5, NULL);}
        break;
      case 6: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0);}
        break;
      case 7: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0);}
        break;
      case 8: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy5);}
        break;
      case 9: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 117: /* db_name ::= NK_ID */ yytestcase(yyruleno==117);
      case 118: /* table_name ::= NK_ID */ yytestcase(yyruleno==118);
      case 119: /* column_name ::= NK_ID */ yytestcase(yyruleno==119);
      case 120: /* function_name ::= NK_ID */ yytestcase(yyruleno==120);
      case 121: /* table_alias ::= NK_ID */ yytestcase(yyruleno==121);
      case 122: /* column_alias ::= NK_ID */ yytestcase(yyruleno==122);
      case 123: /* user_name ::= NK_ID */ yytestcase(yyruleno==123);
      case 124: /* index_name ::= NK_ID */ yytestcase(yyruleno==124);
{ yylhsminor.yy5 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy5 = yylhsminor.yy5;
        break;
      case 13: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 14: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL); }
        break;
      case 15: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy25, &yymsp[-1].minor.yy5, yymsp[0].minor.yy68);}
        break;
      case 16: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy25, &yymsp[0].minor.yy5); }
        break;
      case 17: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 18: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy5);}
        break;
      case 19: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy25 = true; }
        break;
      case 20: /* not_exists_opt ::= */
      case 22: /* exists_opt ::= */ yytestcase(yyruleno==22);
      case 187: /* set_quantifier_opt ::= */ yytestcase(yyruleno==187);
{ yymsp[1].minor.yy25 = false; }
        break;
      case 21: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy25 = true; }
        break;
      case 23: /* db_options ::= */
{ yymsp[1].minor.yy68 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 24: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 25: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 26: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 27: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 28: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 29: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 30: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 31: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 32: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 33: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 34: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 35: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 36: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 37: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 38: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 39: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 40: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy68 = setDatabaseOption(pCxt, yymsp[-2].minor.yy68, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 41: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 43: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==43);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy25, yymsp[-5].minor.yy68, yymsp[-3].minor.yy40, yymsp[-1].minor.yy40, yymsp[0].minor.yy68);}
        break;
      case 42: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy40);}
        break;
      case 44: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy40); }
        break;
      case 45: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy25, yymsp[0].minor.yy68); }
        break;
      case 46: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 47: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 48: /* multi_create_clause ::= create_subtable_clause */
      case 51: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==51);
      case 58: /* column_def_list ::= column_def */ yytestcase(yyruleno==58);
      case 93: /* col_name_list ::= col_name */ yytestcase(yyruleno==93);
      case 101: /* func_list ::= func */ yytestcase(yyruleno==101);
      case 192: /* select_sublist ::= select_item */ yytestcase(yyruleno==192);
      case 239: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==239);
{ yylhsminor.yy40 = createNodeList(pCxt, yymsp[0].minor.yy68); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 49: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 52: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==52);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-1].minor.yy40, yymsp[0].minor.yy68); }
  yymsp[-1].minor.yy40 = yylhsminor.yy40;
        break;
      case 50: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy68 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy25, yymsp[-7].minor.yy68, yymsp[-5].minor.yy68, yymsp[-4].minor.yy40, yymsp[-1].minor.yy40); }
  yymsp[-8].minor.yy68 = yylhsminor.yy68;
        break;
      case 53: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy68 = createDropTableClause(pCxt, yymsp[-1].minor.yy25, yymsp[0].minor.yy68); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 54: /* specific_tags_opt ::= */
      case 85: /* tags_def_opt ::= */ yytestcase(yyruleno==85);
      case 200: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==200);
      case 217: /* group_by_clause_opt ::= */ yytestcase(yyruleno==217);
      case 227: /* order_by_clause_opt ::= */ yytestcase(yyruleno==227);
{ yymsp[1].minor.yy40 = NULL; }
        break;
      case 55: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy40 = yymsp[-1].minor.yy40; }
        break;
      case 56: /* full_table_name ::= table_name */
{ yylhsminor.yy68 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy5, NULL); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 57: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy68 = createRealTableNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5, NULL); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 59: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 94: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==94);
      case 102: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==102);
      case 193: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==193);
      case 240: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==240);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, yymsp[0].minor.yy68); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 60: /* column_def ::= column_name type_name */
{ yylhsminor.yy68 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy5, yymsp[0].minor.yy372, NULL); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 61: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy68 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy5, yymsp[-2].minor.yy372, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 62: /* type_name ::= BOOL */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 63: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 64: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 65: /* type_name ::= INT */
      case 66: /* type_name ::= INTEGER */ yytestcase(yyruleno==66);
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 67: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 68: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 69: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 70: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 71: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 72: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 73: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 74: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 75: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 76: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy372 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 77: /* type_name ::= JSON */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 78: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 79: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 80: /* type_name ::= BLOB */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 81: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 82: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 83: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 84: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy372 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 86: /* tags_def_opt ::= tags_def */
      case 191: /* select_list ::= select_sublist */ yytestcase(yyruleno==191);
{ yylhsminor.yy40 = yymsp[0].minor.yy40; }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 87: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy40 = yymsp[-1].minor.yy40; }
        break;
      case 88: /* table_options ::= */
{ yymsp[1].minor.yy68 = createDefaultTableOptions(pCxt);}
        break;
      case 89: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy68 = setTableOption(pCxt, yymsp[-2].minor.yy68, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 90: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy68 = setTableOption(pCxt, yymsp[-2].minor.yy68, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 91: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy68 = setTableOption(pCxt, yymsp[-2].minor.yy68, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 92: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy68 = setTableSmaOption(pCxt, yymsp[-4].minor.yy68, yymsp[-1].minor.yy40); }
  yymsp[-4].minor.yy68 = yylhsminor.yy68;
        break;
      case 95: /* col_name ::= column_name */
{ yylhsminor.yy68 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy5); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 96: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy5, &yymsp[-1].minor.yy5, NULL, yymsp[0].minor.yy68); }
        break;
      case 97: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy5, &yymsp[-3].minor.yy5, yymsp[-1].minor.yy40, NULL); }
        break;
      case 98: /* index_options ::= */
      case 198: /* where_clause_opt ::= */ yytestcase(yyruleno==198);
      case 202: /* twindow_clause_opt ::= */ yytestcase(yyruleno==202);
      case 207: /* sliding_opt ::= */ yytestcase(yyruleno==207);
      case 209: /* fill_opt ::= */ yytestcase(yyruleno==209);
      case 221: /* having_clause_opt ::= */ yytestcase(yyruleno==221);
      case 229: /* slimit_clause_opt ::= */ yytestcase(yyruleno==229);
      case 233: /* limit_clause_opt ::= */ yytestcase(yyruleno==233);
{ yymsp[1].minor.yy68 = NULL; }
        break;
      case 99: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy68 = createIndexOption(pCxt, yymsp[-6].minor.yy40, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), NULL, yymsp[0].minor.yy68); }
        break;
      case 100: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy68 = createIndexOption(pCxt, yymsp[-8].minor.yy40, releaseRawExprNode(pCxt, yymsp[-4].minor.yy68), releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), yymsp[0].minor.yy68); }
        break;
      case 103: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy68 = createFunctionNode(pCxt, &yymsp[-3].minor.yy5, yymsp[-1].minor.yy40); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 104: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 105: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy5); }
        break;
      case 106: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 107: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy68; }
        break;
      case 108: /* literal ::= NK_INTEGER */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 109: /* literal ::= NK_FLOAT */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 110: /* literal ::= NK_STRING */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 111: /* literal ::= NK_BOOL */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 112: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 113: /* literal ::= duration_literal */
      case 125: /* expression ::= literal */ yytestcase(yyruleno==125);
      case 126: /* expression ::= column_reference */ yytestcase(yyruleno==126);
      case 129: /* expression ::= subquery */ yytestcase(yyruleno==129);
      case 161: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==161);
      case 165: /* boolean_primary ::= predicate */ yytestcase(yyruleno==165);
      case 167: /* common_expression ::= expression */ yytestcase(yyruleno==167);
      case 168: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==168);
      case 170: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==170);
      case 172: /* table_reference ::= table_primary */ yytestcase(yyruleno==172);
      case 173: /* table_reference ::= joined_table */ yytestcase(yyruleno==173);
      case 177: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==177);
      case 224: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==224);
      case 226: /* query_primary ::= query_specification */ yytestcase(yyruleno==226);
{ yylhsminor.yy68 = yymsp[0].minor.yy68; }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 114: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 115: /* literal_list ::= literal */
      case 138: /* expression_list ::= expression */ yytestcase(yyruleno==138);
{ yylhsminor.yy40 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68)); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 116: /* literal_list ::= literal_list NK_COMMA literal */
      case 139: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==139);
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, releaseRawExprNode(pCxt, yymsp[0].minor.yy68)); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 127: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy5, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy5, yymsp[-1].minor.yy40)); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 128: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy5, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy5, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 130: /* expression ::= NK_LP expression NK_RP */
      case 166: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==166);
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 131: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy68));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 132: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), NULL));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 133: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 134: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 135: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 136: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 137: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); 
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 140: /* column_reference ::= column_name */
{ yylhsminor.yy68 = createRawExprNode(pCxt, &yymsp[0].minor.yy5, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy5)); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 141: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5, createColumnNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy5)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 142: /* predicate ::= expression compare_op expression */
      case 147: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==147);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy416, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 143: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy68), releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-4].minor.yy68 = yylhsminor.yy68;
        break;
      case 144: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[-5].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-5].minor.yy68 = yylhsminor.yy68;
        break;
      case 145: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), NULL));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 146: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), NULL));
                                                                                  }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 148: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy416 = OP_TYPE_LOWER_THAN; }
        break;
      case 149: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy416 = OP_TYPE_GREATER_THAN; }
        break;
      case 150: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy416 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 151: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy416 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 152: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy416 = OP_TYPE_NOT_EQUAL; }
        break;
      case 153: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy416 = OP_TYPE_EQUAL; }
        break;
      case 154: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy416 = OP_TYPE_LIKE; }
        break;
      case 155: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy416 = OP_TYPE_NOT_LIKE; }
        break;
      case 156: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy416 = OP_TYPE_MATCH; }
        break;
      case 157: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy416 = OP_TYPE_NMATCH; }
        break;
      case 158: /* in_op ::= IN */
{ yymsp[0].minor.yy416 = OP_TYPE_IN; }
        break;
      case 159: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy416 = OP_TYPE_NOT_IN; }
        break;
      case 160: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy40)); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 162: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), NULL));
                                                                                  }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 163: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 164: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy68);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), releaseRawExprNode(pCxt, yymsp[0].minor.yy68)));
                                                                                  }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 169: /* from_clause ::= FROM table_reference_list */
      case 199: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==199);
      case 222: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy68 = yymsp[0].minor.yy68; }
        break;
      case 171: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy68 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy68, yymsp[0].minor.yy68, NULL); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 174: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy68 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy5, &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 175: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy68 = createRealTableNode(pCxt, &yymsp[-3].minor.yy5, &yymsp[-1].minor.yy5, &yymsp[0].minor.yy5); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 176: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy68 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 178: /* alias_opt ::= */
{ yymsp[1].minor.yy5 = nil_token;  }
        break;
      case 179: /* alias_opt ::= table_alias */
{ yylhsminor.yy5 = yymsp[0].minor.yy5; }
  yymsp[0].minor.yy5 = yylhsminor.yy5;
        break;
      case 180: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy5 = yymsp[0].minor.yy5; }
        break;
      case 181: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 182: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==182);
{ yymsp[-2].minor.yy68 = yymsp[-1].minor.yy68; }
        break;
      case 183: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy68 = createJoinTableNode(pCxt, yymsp[-4].minor.yy92, yymsp[-5].minor.yy68, yymsp[-2].minor.yy68, yymsp[0].minor.yy68); }
  yymsp[-5].minor.yy68 = yylhsminor.yy68;
        break;
      case 184: /* join_type ::= */
{ yymsp[1].minor.yy92 = JOIN_TYPE_INNER; }
        break;
      case 185: /* join_type ::= INNER */
{ yymsp[0].minor.yy92 = JOIN_TYPE_INNER; }
        break;
      case 186: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy68 = createSelectStmt(pCxt, yymsp[-7].minor.yy25, yymsp[-6].minor.yy40, yymsp[-5].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addWhereClause(pCxt, yymsp[-8].minor.yy68, yymsp[-4].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addPartitionByClause(pCxt, yymsp[-8].minor.yy68, yymsp[-3].minor.yy40);
                                                                                    yymsp[-8].minor.yy68 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy68, yymsp[-2].minor.yy68);
                                                                                    yymsp[-8].minor.yy68 = addGroupByClause(pCxt, yymsp[-8].minor.yy68, yymsp[-1].minor.yy40);
                                                                                    yymsp[-8].minor.yy68 = addHavingClause(pCxt, yymsp[-8].minor.yy68, yymsp[0].minor.yy68);
                                                                                  }
        break;
      case 188: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy25 = true; }
        break;
      case 189: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy25 = false; }
        break;
      case 190: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy40 = NULL; }
        break;
      case 194: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy68);
                                                                                    yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68), &t);
                                                                                  }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 195: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-1].minor.yy68 = yylhsminor.yy68;
        break;
      case 196: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy68 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), &yymsp[0].minor.yy5); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 197: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy68 = createColumnNode(pCxt, &yymsp[-2].minor.yy5, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 201: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 218: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==218);
      case 228: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==228);
{ yymsp[-2].minor.yy40 = yymsp[0].minor.yy40; }
        break;
      case 203: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy68 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), &yymsp[-1].minor.yy0); }
        break;
      case 204: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy68 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy68)); }
        break;
      case 205: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy68 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), NULL, yymsp[-1].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 206: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy68 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy68), releaseRawExprNode(pCxt, yymsp[-3].minor.yy68), yymsp[-1].minor.yy68, yymsp[0].minor.yy68); }
        break;
      case 208: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy68 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy68); }
        break;
      case 210: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy68 = createFillNode(pCxt, yymsp[-1].minor.yy94, NULL); }
        break;
      case 211: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy68 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy40)); }
        break;
      case 212: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy94 = FILL_MODE_NONE; }
        break;
      case 213: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy94 = FILL_MODE_PREV; }
        break;
      case 214: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy94 = FILL_MODE_NULL; }
        break;
      case 215: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy94 = FILL_MODE_LINEAR; }
        break;
      case 216: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy94 = FILL_MODE_NEXT; }
        break;
      case 219: /* group_by_list ::= expression */
{ yylhsminor.yy40 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); }
  yymsp[0].minor.yy40 = yylhsminor.yy40;
        break;
      case 220: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy40 = addNodeToList(pCxt, yymsp[-2].minor.yy40, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy68))); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 223: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy68 = addOrderByClause(pCxt, yymsp[-3].minor.yy68, yymsp[-2].minor.yy40);
                                                                                    yylhsminor.yy68 = addSlimitClause(pCxt, yylhsminor.yy68, yymsp[-1].minor.yy68);
                                                                                    yylhsminor.yy68 = addLimitClause(pCxt, yylhsminor.yy68, yymsp[0].minor.yy68);
                                                                                  }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 225: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy68 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy68, yymsp[0].minor.yy68); }
  yymsp[-3].minor.yy68 = yylhsminor.yy68;
        break;
      case 230: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 234: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==234);
{ yymsp[-1].minor.yy68 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 231: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 235: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==235);
{ yymsp[-3].minor.yy68 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 232: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 236: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==236);
{ yymsp[-3].minor.yy68 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 237: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy68 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy68); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 238: /* search_condition ::= common_expression */
{ yylhsminor.yy68 = releaseRawExprNode(pCxt, yymsp[0].minor.yy68); }
  yymsp[0].minor.yy68 = yylhsminor.yy68;
        break;
      case 241: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy68 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy68), yymsp[-1].minor.yy54, yymsp[0].minor.yy53); }
  yymsp[-2].minor.yy68 = yylhsminor.yy68;
        break;
      case 242: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy54 = ORDER_ASC; }
        break;
      case 243: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy54 = ORDER_ASC; }
        break;
      case 244: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy54 = ORDER_DESC; }
        break;
      case 245: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy53 = NULL_ORDER_DEFAULT; }
        break;
      case 246: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy53 = NULL_ORDER_FIRST; }
        break;
      case 247: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy53 = NULL_ORDER_LAST; }
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
  
  if(TOKEN.z) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
  } else {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
  }
  pCxt->valid = false;
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
