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
#define YYNOCODE 208
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  ENullOrder yy9;
  SDatabaseOptions* yy103;
  SToken yy161;
  EOrder yy162;
  EFillMode yy166;
  SNodeList* yy184;
  EOperatorType yy220;
  SDataType yy240;
  EJoinType yy308;
  STableOptions* yy334;
  bool yy377;
  SNode* yy392;
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
#define YYNSTATE             278
#define YYNRULE              236
#define YYNTOKEN             134
#define YY_MAX_SHIFT         277
#define YY_MIN_SHIFTREDUCE   438
#define YY_MAX_SHIFTREDUCE   673
#define YY_ERROR_ACTION      674
#define YY_ACCEPT_ACTION     675
#define YY_NO_ACTION         676
#define YY_MIN_REDUCE        677
#define YY_MAX_REDUCE        912
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
#define YY_ACTTAB_COUNT (903)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   137,  149,   23,   95,  770,  719,  768,  246,  150,  781,
 /*    10 */   779,  148,   30,   28,   26,   25,   24,  781,  779,  194,
 /*    20 */   177,  805,   96,  733,   66,   30,   28,   26,   25,   24,
 /*    30 */   690,  166,  194,  221,  805,   60,  207,  132,  790,  779,
 /*    40 */   195,  208,  221,   54,  791,  728,  794,  830,  731,   58,
 /*    50 */   132,  139,  826,  903,   19,  782,  779,  731,  556,   73,
 /*    60 */   837,  838,  864,  842,   30,   28,   26,   25,   24,  270,
 /*    70 */   269,  268,  267,  266,  265,  264,  263,  262,  261,  260,
 /*    80 */   259,  258,  257,  256,  255,  254,   26,   25,   24,   22,
 /*    90 */   141,  171,  574,  575,  576,  577,  578,  579,  580,  582,
 /*   100 */   583,  584,   22,  141,  152,  574,  575,  576,  577,  578,
 /*   110 */   579,  580,  582,  583,  584,  220,  733,   66,  498,  244,
 /*   120 */   243,  242,  502,  241,  504,  505,  240,  507,  237,   41,
 /*   130 */   513,  234,  515,  516,  231,  228,  194,  194,  805,  805,
 /*   140 */   727,  184,  790,  779,  195,  220,  220,   53,  791,  170,
 /*   150 */   794,  830,  194,   77,  805,  131,  826,  206,  790,  779,
 /*   160 */   195,  205,  544,   67,  791,  204,  794,  891,  180,  623,
 /*   170 */   805,   10,   10,  548,  790,  779,  195,  274,  273,   54,
 /*   180 */   791,   76,  794,  830,  201,  889,  546,  139,  826,   71,
 /*   190 */   208,  203,  202,   30,   28,   26,   25,   24,  194,  546,
 /*   200 */   805,   94,  185,  904,  790,  779,  195,  156,  857,  125,
 /*   210 */   791,  145,  794,  180,   20,  805,  172,  167,  165,  790,
 /*   220 */   779,  195,   77,  581,   54,  791,  585,  794,  830,  194,
 /*   230 */   253,  805,  139,  826,   71,  790,  779,  195,   29,   27,
 /*   240 */    54,  791,  247,  794,  830,   93,  200,  537,  139,  826,
 /*   250 */   903,  734,   66,  858,  194,  535,  805,  146,  844,  887,
 /*   260 */   790,  779,  195,   11,  891,   54,  791,  163,  794,  830,
 /*   270 */    29,   27,  616,  139,  826,  903,  841,  114,  890,  537,
 /*   280 */   761,  188,  889,    1,  848,   50,  194,  535,  805,  146,
 /*   290 */    47,  184,  790,  779,  195,   11,  153,  119,  791,  770,
 /*   300 */   794,  768,  222,  221,    9,    8,  218,   29,   27,  154,
 /*   310 */    29,   27,  536,  538,  541,    1,  537,  891,  731,  537,
 /*   320 */   844,  733,   66,  717,  535,  221,  146,  535,  219,  146,
 /*   330 */    41,   76,   11,   77,  222,  889,  615,   61,  840,  844,
 /*   340 */   731,  726,  189,  448,  536,  538,  541,   30,   28,   26,
 /*   350 */    25,   24,    1,  449,  450,    7,  194,  839,  805,  253,
 /*   360 */   549,  592,  790,  779,  195,   21,  178,  125,  791,  157,
 /*   370 */   794,  222,    6,  611,  222,   30,   28,   26,   25,   24,
 /*   380 */   187,  536,  538,  541,  536,  538,  541,  194,  221,  805,
 /*   390 */   770,  109,  769,  790,  779,  195,  177,   88,   55,  791,
 /*   400 */   860,  794,  830,  731,  448,   77,  829,  826,  194,  177,
 /*   410 */   805,   60,  211,   98,  790,  779,  195,  191,  221,   55,
 /*   420 */   791,  155,  794,  830,   60,   58,    2,  181,  826,  184,
 /*   430 */    45,  182,   79,  731,  179,   72,  837,  838,   58,  842,
 /*   440 */   614,  724,   29,   27,  183,   51,  806,  637,   91,  837,
 /*   450 */   176,  537,  175,   62,   56,  891,  723,   29,   27,  535,
 /*   460 */    85,  146,   29,   27,  669,  670,  537,   82,  849,   76,
 /*   470 */   611,  537,  210,  889,  535,  586,  146,  672,  673,  535,
 /*   480 */   192,  146,   31,  675,  194,  571,  805,    7,  549,  861,
 /*   490 */   790,  779,  195,    9,    8,   55,  791,  216,  794,  830,
 /*   500 */   214,  186,    1,  786,  827,  130,  222,    7,  196,  217,
 /*   510 */   784,  127,   68,  164,  871,   80,  536,  538,  541,  161,
 /*   520 */   537,  222,  100,  553,  541,  851,  222,  159,  535,  160,
 /*   530 */    31,  536,  538,  541,  891,  870,  536,  538,  541,  194,
 /*   540 */   138,  805,  106,   84,    5,  790,  779,  195,   76,   63,
 /*   550 */   121,  791,  889,  794,  194,  174,  805,  158,   87,   70,
 /*   560 */   790,  779,  195,    4,  491,  125,  791,  140,  794,   89,
 /*   570 */   194,   64,  805,  611,  545,  222,  790,  779,  195,   59,
 /*   580 */   548,   67,  791,  173,  794,  536,  538,  541,   30,   28,
 /*   590 */    26,   25,   24,  194,   77,  805,  486,  845,   32,  790,
 /*   600 */   779,  195,   90,   56,  120,  791,  194,  794,  805,  519,
 /*   610 */    16,  142,  790,  779,  195,  104,  226,  122,  791,  906,
 /*   620 */   794,  905,  193,   78,  194,  718,  805,  812,  190,  103,
 /*   630 */   790,  779,  195,  888,  556,  117,  791,  523,  794,  194,
 /*   640 */   544,  805,  528,   97,   63,  790,  779,  195,  197,   64,
 /*   650 */   123,  791,   42,  794,  194,  101,  805,   40,  209,  102,
 /*   660 */   790,  779,  195,   65,  550,  118,  791,  113,  794,  212,
 /*   670 */    63,  194,  250,  805,  147,   44,  249,  790,  779,  195,
 /*   680 */   732,   46,  124,  791,  115,  794,  194,  277,  805,  128,
 /*   690 */   224,  251,  790,  779,  195,  110,  116,  802,  791,  129,
 /*   700 */   794,  194,   14,  805,    3,   31,   81,  790,  779,  195,
 /*   710 */   248,  634,  801,  791,   83,  794,  194,   35,  805,  716,
 /*   720 */   636,   69,  790,  779,  195,   86,   37,  800,  791,  630,
 /*   730 */   794,  194,  629,  805,  168,  169,  784,  790,  779,  195,
 /*   740 */    38,   18,  135,  791,  608,  794,   15,  607,  194,   92,
 /*   750 */   805,   33,   34,    8,  790,  779,  195,   75,  572,  134,
 /*   760 */   791,  194,  794,  805,  554,  663,  250,  790,  779,  195,
 /*   770 */   249,  177,  136,  791,   17,  794,  194,   12,  805,   39,
 /*   780 */    99,  658,  790,  779,  195,  251,   60,  133,  791,  640,
 /*   790 */   794,  194,  657,  805,  143,  662,  661,  790,  779,  195,
 /*   800 */    58,  144,  126,  791,  248,  794,   13,  773,  693,  772,
 /*   810 */    74,  837,  838,  112,  842,  162,  638,  639,  641,  642,
 /*   820 */   199,   57,  198,  771,  722,  721,  692,  111,  686,  681,
 /*   830 */   720,  457,  691,  685,  684,  680,  679,  213,  678,  215,
 /*   840 */   105,   43,   47,  783,   36,  108,  223,  520,  539,  225,
 /*   850 */    52,  151,  227,  107,  517,  230,  512,  514,  233,  236,
 /*   860 */   229,  508,  506,  239,  511,  497,  232,   48,  235,  238,
 /*   870 */   510,  245,   49,  525,  527,  509,  526,  476,  475,  455,
 /*   880 */   252,  469,  474,  473,  472,  471,  470,  468,  467,  683,
 /*   890 */   466,  465,  464,  463,  462,  461,  460,  271,  272,  682,
 /*   900 */   677,  275,  276,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   151,  153,  170,  171,  156,    0,  158,  157,  151,  160,
 /*    10 */   161,  143,   12,   13,   14,   15,   16,  160,  161,  154,
 /*    20 */   139,  156,  206,  155,  156,   12,   13,   14,   15,   16,
 /*    30 */     0,  166,  154,  139,  156,  154,  142,   37,  160,  161,
 /*    40 */   162,   36,  139,  165,  166,  142,  168,  169,  154,  168,
 /*    50 */    37,  173,  174,  175,    2,  160,  161,  154,   58,  178,
 /*    60 */   179,  180,  184,  182,   12,   13,   14,   15,   16,   39,
 /*    70 */    40,   41,   42,   43,   44,   45,   46,   47,   48,   49,
 /*    80 */    50,   51,   52,   53,   54,   55,   14,   15,   16,   89,
 /*    90 */    90,   31,   92,   93,   94,   95,   96,   97,   98,   99,
 /*   100 */   100,  101,   89,   90,  143,   92,   93,   94,   95,   96,
 /*   110 */    97,   98,   99,  100,  101,   31,  155,  156,   67,   68,
 /*   120 */    69,   70,   71,   72,   73,   74,   75,   76,   77,  141,
 /*   130 */    79,   80,   81,   82,   83,   84,  154,  154,  156,  156,
 /*   140 */   152,  159,  160,  161,  162,   31,   31,  165,  166,  166,
 /*   150 */   168,  169,  154,  107,  156,  173,  174,   26,  160,  161,
 /*   160 */   162,   30,   31,  165,  166,   34,  168,  185,  154,   14,
 /*   170 */   156,   57,   57,   31,  160,  161,  162,  136,  137,  165,
 /*   180 */   166,  199,  168,  169,   53,  203,   31,  173,  174,  175,
 /*   190 */    36,   60,   61,   12,   13,   14,   15,   16,  154,   31,
 /*   200 */   156,  187,  204,  205,  160,  161,  162,  193,  194,  165,
 /*   210 */   166,  167,  168,  154,   89,  156,  112,  113,  114,  160,
 /*   220 */   161,  162,  107,   98,  165,  166,  101,  168,  169,  154,
 /*   230 */    36,  156,  173,  174,  175,  160,  161,  162,   12,   13,
 /*   240 */   165,  166,   63,  168,  169,  103,  139,   21,  173,  174,
 /*   250 */   175,  155,  156,  194,  154,   29,  156,   31,  163,  184,
 /*   260 */   160,  161,  162,   37,  185,  165,  166,  197,  168,  169,
 /*   270 */    12,   13,   14,  173,  174,  175,  181,  144,  199,   21,
 /*   280 */   147,   65,  203,   57,  184,   57,  154,   29,  156,   31,
 /*   290 */    62,  159,  160,  161,  162,   37,  153,  165,  166,  156,
 /*   300 */   168,  158,   76,  139,    1,    2,  142,   12,   13,  143,
 /*   310 */    12,   13,   86,   87,   88,   57,   21,  185,  154,   21,
 /*   320 */   163,  155,  156,    0,   29,  139,   31,   29,  142,   31,
 /*   330 */   141,  199,   37,  107,   76,  203,    4,  148,  181,  163,
 /*   340 */   154,  152,  126,   21,   86,   87,   88,   12,   13,   14,
 /*   350 */    15,   16,   57,   31,   32,   57,  154,  181,  156,   36,
 /*   360 */    31,   58,  160,  161,  162,    2,  183,  165,  166,  167,
 /*   370 */   168,   76,  105,  106,   76,   12,   13,   14,   15,   16,
 /*   380 */     3,   86,   87,   88,   86,   87,   88,  154,  139,  156,
 /*   390 */   156,  142,  158,  160,  161,  162,  139,  190,  165,  166,
 /*   400 */   164,  168,  169,  154,   21,  107,  173,  174,  154,  139,
 /*   410 */   156,  154,   29,  200,  160,  161,  162,   65,  139,  165,
 /*   420 */   166,  142,  168,  169,  154,  168,  186,  173,  174,  159,
 /*   430 */   138,   37,  103,  154,  177,  178,  179,  180,  168,  182,
 /*   440 */   108,  149,   12,   13,   14,  138,  156,   58,  178,  179,
 /*   450 */   180,   21,  182,  146,   65,  185,  149,   12,   13,   29,
 /*   460 */    58,   31,   12,   13,  129,  130,   21,   65,  104,  199,
 /*   470 */   106,   21,  136,  203,   29,   58,   31,  132,  133,   29,
 /*   480 */   128,   31,   65,  134,  154,   91,  156,   57,   31,  164,
 /*   490 */   160,  161,  162,    1,    2,  165,  166,   20,  168,  169,
 /*   500 */    23,  124,   57,   57,  174,   18,   76,   57,  159,   22,
 /*   510 */    64,   24,   25,  116,  196,  195,   86,   87,   88,  115,
 /*   520 */    21,   76,   35,   58,   88,  192,   76,  161,   29,  161,
 /*   530 */    65,   86,   87,   88,  185,  196,   86,   87,   88,  154,
 /*   540 */   161,  156,   58,  195,  123,  160,  161,  162,  199,   65,
 /*   550 */   165,  166,  203,  168,  154,  122,  156,  110,  191,  189,
 /*   560 */   160,  161,  162,  109,   58,  165,  166,  167,  168,  188,
 /*   570 */   154,   65,  156,  106,   31,   76,  160,  161,  162,  154,
 /*   580 */    31,  165,  166,  198,  168,   86,   87,   88,   12,   13,
 /*   590 */    14,   15,   16,  154,  107,  156,   58,  163,  102,  160,
 /*   600 */   161,  162,  176,   65,  165,  166,  154,  168,  156,   58,
 /*   610 */    57,  131,  160,  161,  162,   19,   65,  165,  166,  207,
 /*   620 */   168,  205,  127,   27,  154,    0,  156,  172,  125,   33,
 /*   630 */   160,  161,  162,  202,   58,  165,  166,   58,  168,  154,
 /*   640 */    31,  156,   58,  201,   65,  160,  161,  162,  139,   65,
 /*   650 */   165,  166,   56,  168,  154,   59,  156,  141,  139,  141,
 /*   660 */   160,  161,  162,   58,   31,  165,  166,  147,  168,  135,
 /*   670 */    65,  154,   47,  156,  135,  138,   51,  160,  161,  162,
 /*   680 */   154,   57,  165,  166,  139,  168,  154,  135,  156,  145,
 /*   690 */   150,   66,  160,  161,  162,  138,  140,  165,  166,  145,
 /*   700 */   168,  154,  111,  156,   65,   65,   58,  160,  161,  162,
 /*   710 */    85,   58,  165,  166,   57,  168,  154,   65,  156,    0,
 /*   720 */    58,   57,  160,  161,  162,   57,   57,  165,  166,   58,
 /*   730 */   168,  154,   58,  156,   29,   65,   64,  160,  161,  162,
 /*   740 */    57,   65,  165,  166,   58,  168,  111,   58,  154,   64,
 /*   750 */   156,  104,   65,    2,  160,  161,  162,   64,   91,  165,
 /*   760 */   166,  154,  168,  156,   58,   58,   47,  160,  161,  162,
 /*   770 */    51,  139,  165,  166,   65,  168,  154,  111,  156,    4,
 /*   780 */    64,   29,  160,  161,  162,   66,  154,  165,  166,   91,
 /*   790 */   168,  154,   29,  156,   29,   29,   29,  160,  161,  162,
 /*   800 */   168,   29,  165,  166,   85,  168,   57,    0,    0,    0,
 /*   810 */   178,  179,  180,   19,  182,  117,  118,  119,  120,  121,
 /*   820 */    64,   27,   53,    0,    0,    0,    0,   33,    0,    0,
 /*   830 */     0,   38,    0,    0,    0,    0,    0,   21,    0,   21,
 /*   840 */    19,   57,   62,   64,   57,   64,   63,   58,   21,   29,
 /*   850 */    56,   29,   57,   59,   58,   57,   78,   58,   57,   57,
 /*   860 */    29,   58,   58,   57,   78,   21,   29,   57,   29,   29,
 /*   870 */    78,   66,   57,   21,   29,   78,   29,   29,   29,   38,
 /*   880 */    37,   21,   29,   29,   29,   29,   29,   29,   29,    0,
 /*   890 */    29,   29,   29,   29,   29,   29,   29,   29,   28,    0,
 /*   900 */     0,   21,   20,  208,  208,  208,  208,  208,  208,  208,
 /*   910 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   920 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   930 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   940 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   950 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   960 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   970 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   980 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   990 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*  1000 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*  1010 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*  1020 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
 /*  1030 */   208,  208,  208,  208,  208,  208,  208,
};
#define YY_SHIFT_COUNT    (277)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (900)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   487,  226,  258,  295,  295,  295,  295,  298,  295,  295,
 /*    10 */   115,  445,  450,  430,  450,  450,  450,  450,  450,  450,
 /*    20 */   450,  450,  450,  450,  450,  450,  450,  450,  450,  450,
 /*    30 */   450,  450,  114,  114,  114,  499,  499,   60,   60,   46,
 /*    40 */    84,   84,  154,  168,   84,   84,  168,   84,  168,  168,
 /*    50 */   168,   84,  194,    0,   13,   13,  499,  322,  142,  142,
 /*    60 */   142,    5,  323,  168,  168,  179,   51,  335,  131,  698,
 /*    70 */   104,  329,  364,  267,  364,  155,  377,  332,  383,  457,
 /*    80 */   397,  404,  436,  436,  397,  404,  436,  421,  433,  447,
 /*    90 */   454,  467,  543,  549,  496,  553,  480,  495,  503,  168,
 /*   100 */   609,  154,  609,  154,  633,  633,  179,  194,  543,  624,
 /*   110 */   609,  194,  633,  903,  903,  903,   30,   52,  363,  576,
 /*   120 */   181,  181,  181,  181,  181,  181,  181,  596,  625,  719,
 /*   130 */   794,  303,  125,   72,   72,   72,   72,  389,  402,  492,
 /*   140 */   417,  394,  345,  216,  352,  465,  446,  477,  484,  506,
 /*   150 */   538,  551,  579,  584,  605,  228,  639,  640,  591,  648,
 /*   160 */   653,  657,  652,  662,  664,  668,  671,  669,  674,  705,
 /*   170 */   670,  672,  683,  676,  635,  686,  689,  685,  647,  687,
 /*   180 */   693,  751,  667,  706,  707,  709,  666,  775,  752,  763,
 /*   190 */   765,  766,  767,  772,  716,  749,  807,  808,  809,  769,
 /*   200 */   756,  823,  824,  825,  826,  828,  829,  830,  793,  832,
 /*   210 */   833,  834,  835,  836,  816,  838,  818,  821,  784,  780,
 /*   220 */   779,  781,  827,  787,  783,  789,  820,  822,  795,  796,
 /*   230 */   831,  798,  799,  837,  801,  803,  839,  802,  804,  840,
 /*   240 */   806,  778,  786,  792,  797,  844,  805,  810,  815,  845,
 /*   250 */   847,  852,  841,  843,  848,  849,  853,  854,  855,  856,
 /*   260 */   857,  860,  858,  859,  861,  862,  863,  864,  865,  866,
 /*   270 */   867,  889,  868,  870,  899,  900,  880,  882,
};
#define YY_REDUCE_COUNT (115)
#define YY_REDUCE_MIN   (-184)
#define YY_REDUCE_MAX   (637)
static const short yy_reduce_ofst[] = {
 /*     0 */   349,  -18,   14,   59, -122,   75,  100,  132,  233,  254,
 /*    10 */   270,  330,   -2,   44,  202,  385,  400,  416,  439,  452,
 /*    20 */   470,  485,  500,  517,  532,  547,  562,  577,  594,  607,
 /*    30 */   622,  637,  257, -119,  632, -151, -143, -135,  -17,   79,
 /*    40 */  -106,  -97,  189, -132,  164,  186, -152,  249,  -39,  143,
 /*    50 */   166,  279,  307, -168, -168, -168, -105,   41,   95,  157,
 /*    60 */   176,  -12,  292,   96,  234,  133, -150, -184,  107,   70,
 /*    70 */   207,  236,  183,  183,  183,  290,  213,  240,  336,  325,
 /*    80 */   318,  320,  366,  368,  339,  348,  379,  333,  367,  370,
 /*    90 */   381,  183,  425,  434,  426,  455,  412,  431,  442,  290,
 /*   100 */   509,  516,  519,  518,  534,  539,  520,  537,  526,  540,
 /*   110 */   545,  557,  552,  544,  554,  556,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*    10 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*    20 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*    30 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*    40 */   674,  674,  697,  674,  674,  674,  674,  674,  674,  674,
 /*    50 */   674,  674,  695,  674,  832,  674,  674,  674,  843,  843,
 /*    60 */   843,  697,  695,  674,  674,  760,  674,  907,  674,  674,
 /*    70 */   867,  859,  835,  849,  836,  674,  892,  852,  674,  674,
 /*    80 */   874,  872,  674,  674,  874,  872,  674,  886,  882,  865,
 /*    90 */   863,  849,  674,  674,  674,  674,  910,  898,  894,  674,
 /*   100 */   674,  697,  674,  697,  674,  674,  674,  695,  674,  729,
 /*   110 */   674,  695,  674,  763,  763,  698,  674,  674,  674,  674,
 /*   120 */   885,  884,  809,  808,  807,  803,  804,  674,  674,  674,
 /*   130 */   674,  674,  674,  798,  799,  797,  796,  674,  674,  833,
 /*   140 */   674,  674,  674,  895,  899,  674,  785,  674,  674,  674,
 /*   150 */   674,  674,  674,  674,  674,  674,  856,  866,  674,  674,
 /*   160 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   170 */   674,  785,  674,  883,  674,  842,  838,  674,  674,  834,
 /*   180 */   674,  828,  674,  674,  674,  893,  674,  674,  674,  674,
 /*   190 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   200 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   210 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   220 */   784,  674,  674,  674,  674,  674,  674,  674,  757,  674,
 /*   230 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   240 */   674,  742,  740,  739,  738,  674,  735,  674,  674,  674,
 /*   250 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   260 */   674,  674,  674,  674,  674,  674,  674,  674,  674,  674,
 /*   270 */   674,  674,  674,  674,  674,  674,  674,  674,
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
  /*   33 */ "DATABASE",
  /*   34 */ "DATABASES",
  /*   35 */ "USE",
  /*   36 */ "IF",
  /*   37 */ "NOT",
  /*   38 */ "EXISTS",
  /*   39 */ "BLOCKS",
  /*   40 */ "CACHE",
  /*   41 */ "CACHELAST",
  /*   42 */ "COMP",
  /*   43 */ "DAYS",
  /*   44 */ "FSYNC",
  /*   45 */ "MAXROWS",
  /*   46 */ "MINROWS",
  /*   47 */ "KEEP",
  /*   48 */ "PRECISION",
  /*   49 */ "QUORUM",
  /*   50 */ "REPLICA",
  /*   51 */ "TTL",
  /*   52 */ "WAL",
  /*   53 */ "VGROUPS",
  /*   54 */ "SINGLE_STABLE",
  /*   55 */ "STREAM_MODE",
  /*   56 */ "TABLE",
  /*   57 */ "NK_LP",
  /*   58 */ "NK_RP",
  /*   59 */ "STABLE",
  /*   60 */ "TABLES",
  /*   61 */ "STABLES",
  /*   62 */ "USING",
  /*   63 */ "TAGS",
  /*   64 */ "NK_DOT",
  /*   65 */ "NK_COMMA",
  /*   66 */ "COMMENT",
  /*   67 */ "BOOL",
  /*   68 */ "TINYINT",
  /*   69 */ "SMALLINT",
  /*   70 */ "INT",
  /*   71 */ "INTEGER",
  /*   72 */ "BIGINT",
  /*   73 */ "FLOAT",
  /*   74 */ "DOUBLE",
  /*   75 */ "BINARY",
  /*   76 */ "TIMESTAMP",
  /*   77 */ "NCHAR",
  /*   78 */ "UNSIGNED",
  /*   79 */ "JSON",
  /*   80 */ "VARCHAR",
  /*   81 */ "MEDIUMBLOB",
  /*   82 */ "BLOB",
  /*   83 */ "VARBINARY",
  /*   84 */ "DECIMAL",
  /*   85 */ "SMA",
  /*   86 */ "NK_FLOAT",
  /*   87 */ "NK_BOOL",
  /*   88 */ "NK_VARIABLE",
  /*   89 */ "BETWEEN",
  /*   90 */ "IS",
  /*   91 */ "NULL",
  /*   92 */ "NK_LT",
  /*   93 */ "NK_GT",
  /*   94 */ "NK_LE",
  /*   95 */ "NK_GE",
  /*   96 */ "NK_NE",
  /*   97 */ "NK_EQ",
  /*   98 */ "LIKE",
  /*   99 */ "MATCH",
  /*  100 */ "NMATCH",
  /*  101 */ "IN",
  /*  102 */ "FROM",
  /*  103 */ "AS",
  /*  104 */ "JOIN",
  /*  105 */ "ON",
  /*  106 */ "INNER",
  /*  107 */ "SELECT",
  /*  108 */ "DISTINCT",
  /*  109 */ "WHERE",
  /*  110 */ "PARTITION",
  /*  111 */ "BY",
  /*  112 */ "SESSION",
  /*  113 */ "STATE_WINDOW",
  /*  114 */ "INTERVAL",
  /*  115 */ "SLIDING",
  /*  116 */ "FILL",
  /*  117 */ "VALUE",
  /*  118 */ "NONE",
  /*  119 */ "PREV",
  /*  120 */ "LINEAR",
  /*  121 */ "NEXT",
  /*  122 */ "GROUP",
  /*  123 */ "HAVING",
  /*  124 */ "ORDER",
  /*  125 */ "SLIMIT",
  /*  126 */ "SOFFSET",
  /*  127 */ "LIMIT",
  /*  128 */ "OFFSET",
  /*  129 */ "ASC",
  /*  130 */ "DESC",
  /*  131 */ "NULLS",
  /*  132 */ "FIRST",
  /*  133 */ "LAST",
  /*  134 */ "cmd",
  /*  135 */ "user_name",
  /*  136 */ "dnode_endpoint",
  /*  137 */ "dnode_host_name",
  /*  138 */ "not_exists_opt",
  /*  139 */ "db_name",
  /*  140 */ "db_options",
  /*  141 */ "exists_opt",
  /*  142 */ "full_table_name",
  /*  143 */ "column_def_list",
  /*  144 */ "tags_def_opt",
  /*  145 */ "table_options",
  /*  146 */ "multi_create_clause",
  /*  147 */ "tags_def",
  /*  148 */ "multi_drop_clause",
  /*  149 */ "create_subtable_clause",
  /*  150 */ "specific_tags_opt",
  /*  151 */ "literal_list",
  /*  152 */ "drop_table_clause",
  /*  153 */ "col_name_list",
  /*  154 */ "table_name",
  /*  155 */ "column_def",
  /*  156 */ "column_name",
  /*  157 */ "type_name",
  /*  158 */ "col_name",
  /*  159 */ "query_expression",
  /*  160 */ "literal",
  /*  161 */ "duration_literal",
  /*  162 */ "function_name",
  /*  163 */ "table_alias",
  /*  164 */ "column_alias",
  /*  165 */ "expression",
  /*  166 */ "column_reference",
  /*  167 */ "expression_list",
  /*  168 */ "subquery",
  /*  169 */ "predicate",
  /*  170 */ "compare_op",
  /*  171 */ "in_op",
  /*  172 */ "in_predicate_value",
  /*  173 */ "boolean_value_expression",
  /*  174 */ "boolean_primary",
  /*  175 */ "common_expression",
  /*  176 */ "from_clause",
  /*  177 */ "table_reference_list",
  /*  178 */ "table_reference",
  /*  179 */ "table_primary",
  /*  180 */ "joined_table",
  /*  181 */ "alias_opt",
  /*  182 */ "parenthesized_joined_table",
  /*  183 */ "join_type",
  /*  184 */ "search_condition",
  /*  185 */ "query_specification",
  /*  186 */ "set_quantifier_opt",
  /*  187 */ "select_list",
  /*  188 */ "where_clause_opt",
  /*  189 */ "partition_by_clause_opt",
  /*  190 */ "twindow_clause_opt",
  /*  191 */ "group_by_clause_opt",
  /*  192 */ "having_clause_opt",
  /*  193 */ "select_sublist",
  /*  194 */ "select_item",
  /*  195 */ "sliding_opt",
  /*  196 */ "fill_opt",
  /*  197 */ "fill_mode",
  /*  198 */ "group_by_list",
  /*  199 */ "query_expression_body",
  /*  200 */ "order_by_clause_opt",
  /*  201 */ "slimit_clause_opt",
  /*  202 */ "limit_clause_opt",
  /*  203 */ "query_primary",
  /*  204 */ "sort_specification_list",
  /*  205 */ "sort_specification",
  /*  206 */ "ordering_specification_opt",
  /*  207 */ "null_ordering_opt",
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
 /*  13 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  14 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  15 */ "cmd ::= SHOW DATABASES",
 /*  16 */ "cmd ::= USE db_name",
 /*  17 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  18 */ "not_exists_opt ::=",
 /*  19 */ "exists_opt ::= IF EXISTS",
 /*  20 */ "exists_opt ::=",
 /*  21 */ "db_options ::=",
 /*  22 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  23 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  24 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  25 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  26 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  27 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  28 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  29 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  30 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  31 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  32 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  33 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  34 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  35 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  36 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  37 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  38 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  39 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  40 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  41 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  42 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  43 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  44 */ "cmd ::= SHOW TABLES",
 /*  45 */ "cmd ::= SHOW STABLES",
 /*  46 */ "multi_create_clause ::= create_subtable_clause",
 /*  47 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  48 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  49 */ "multi_drop_clause ::= drop_table_clause",
 /*  50 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  51 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  52 */ "specific_tags_opt ::=",
 /*  53 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  54 */ "full_table_name ::= table_name",
 /*  55 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  56 */ "column_def_list ::= column_def",
 /*  57 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  58 */ "column_def ::= column_name type_name",
 /*  59 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  60 */ "type_name ::= BOOL",
 /*  61 */ "type_name ::= TINYINT",
 /*  62 */ "type_name ::= SMALLINT",
 /*  63 */ "type_name ::= INT",
 /*  64 */ "type_name ::= INTEGER",
 /*  65 */ "type_name ::= BIGINT",
 /*  66 */ "type_name ::= FLOAT",
 /*  67 */ "type_name ::= DOUBLE",
 /*  68 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  69 */ "type_name ::= TIMESTAMP",
 /*  70 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  71 */ "type_name ::= TINYINT UNSIGNED",
 /*  72 */ "type_name ::= SMALLINT UNSIGNED",
 /*  73 */ "type_name ::= INT UNSIGNED",
 /*  74 */ "type_name ::= BIGINT UNSIGNED",
 /*  75 */ "type_name ::= JSON",
 /*  76 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  77 */ "type_name ::= MEDIUMBLOB",
 /*  78 */ "type_name ::= BLOB",
 /*  79 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  80 */ "type_name ::= DECIMAL",
 /*  81 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  82 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  83 */ "tags_def_opt ::=",
 /*  84 */ "tags_def_opt ::= tags_def",
 /*  85 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  86 */ "table_options ::=",
 /*  87 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  88 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  89 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  90 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  91 */ "col_name_list ::= col_name",
 /*  92 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  93 */ "col_name ::= column_name",
 /*  94 */ "cmd ::= SHOW VGROUPS",
 /*  95 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /*  96 */ "cmd ::= query_expression",
 /*  97 */ "literal ::= NK_INTEGER",
 /*  98 */ "literal ::= NK_FLOAT",
 /*  99 */ "literal ::= NK_STRING",
 /* 100 */ "literal ::= NK_BOOL",
 /* 101 */ "literal ::= TIMESTAMP NK_STRING",
 /* 102 */ "literal ::= duration_literal",
 /* 103 */ "duration_literal ::= NK_VARIABLE",
 /* 104 */ "literal_list ::= literal",
 /* 105 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 106 */ "db_name ::= NK_ID",
 /* 107 */ "table_name ::= NK_ID",
 /* 108 */ "column_name ::= NK_ID",
 /* 109 */ "function_name ::= NK_ID",
 /* 110 */ "table_alias ::= NK_ID",
 /* 111 */ "column_alias ::= NK_ID",
 /* 112 */ "user_name ::= NK_ID",
 /* 113 */ "expression ::= literal",
 /* 114 */ "expression ::= column_reference",
 /* 115 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 116 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 117 */ "expression ::= subquery",
 /* 118 */ "expression ::= NK_LP expression NK_RP",
 /* 119 */ "expression ::= NK_PLUS expression",
 /* 120 */ "expression ::= NK_MINUS expression",
 /* 121 */ "expression ::= expression NK_PLUS expression",
 /* 122 */ "expression ::= expression NK_MINUS expression",
 /* 123 */ "expression ::= expression NK_STAR expression",
 /* 124 */ "expression ::= expression NK_SLASH expression",
 /* 125 */ "expression ::= expression NK_REM expression",
 /* 126 */ "expression_list ::= expression",
 /* 127 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 128 */ "column_reference ::= column_name",
 /* 129 */ "column_reference ::= table_name NK_DOT column_name",
 /* 130 */ "predicate ::= expression compare_op expression",
 /* 131 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 132 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 133 */ "predicate ::= expression IS NULL",
 /* 134 */ "predicate ::= expression IS NOT NULL",
 /* 135 */ "predicate ::= expression in_op in_predicate_value",
 /* 136 */ "compare_op ::= NK_LT",
 /* 137 */ "compare_op ::= NK_GT",
 /* 138 */ "compare_op ::= NK_LE",
 /* 139 */ "compare_op ::= NK_GE",
 /* 140 */ "compare_op ::= NK_NE",
 /* 141 */ "compare_op ::= NK_EQ",
 /* 142 */ "compare_op ::= LIKE",
 /* 143 */ "compare_op ::= NOT LIKE",
 /* 144 */ "compare_op ::= MATCH",
 /* 145 */ "compare_op ::= NMATCH",
 /* 146 */ "in_op ::= IN",
 /* 147 */ "in_op ::= NOT IN",
 /* 148 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 149 */ "boolean_value_expression ::= boolean_primary",
 /* 150 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 151 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 152 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 153 */ "boolean_primary ::= predicate",
 /* 154 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 155 */ "common_expression ::= expression",
 /* 156 */ "common_expression ::= boolean_value_expression",
 /* 157 */ "from_clause ::= FROM table_reference_list",
 /* 158 */ "table_reference_list ::= table_reference",
 /* 159 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 160 */ "table_reference ::= table_primary",
 /* 161 */ "table_reference ::= joined_table",
 /* 162 */ "table_primary ::= table_name alias_opt",
 /* 163 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 164 */ "table_primary ::= subquery alias_opt",
 /* 165 */ "table_primary ::= parenthesized_joined_table",
 /* 166 */ "alias_opt ::=",
 /* 167 */ "alias_opt ::= table_alias",
 /* 168 */ "alias_opt ::= AS table_alias",
 /* 169 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 170 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 171 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 172 */ "join_type ::=",
 /* 173 */ "join_type ::= INNER",
 /* 174 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 175 */ "set_quantifier_opt ::=",
 /* 176 */ "set_quantifier_opt ::= DISTINCT",
 /* 177 */ "set_quantifier_opt ::= ALL",
 /* 178 */ "select_list ::= NK_STAR",
 /* 179 */ "select_list ::= select_sublist",
 /* 180 */ "select_sublist ::= select_item",
 /* 181 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 182 */ "select_item ::= common_expression",
 /* 183 */ "select_item ::= common_expression column_alias",
 /* 184 */ "select_item ::= common_expression AS column_alias",
 /* 185 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 186 */ "where_clause_opt ::=",
 /* 187 */ "where_clause_opt ::= WHERE search_condition",
 /* 188 */ "partition_by_clause_opt ::=",
 /* 189 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 190 */ "twindow_clause_opt ::=",
 /* 191 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 192 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 193 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 194 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 195 */ "sliding_opt ::=",
 /* 196 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 197 */ "fill_opt ::=",
 /* 198 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 199 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 200 */ "fill_mode ::= NONE",
 /* 201 */ "fill_mode ::= PREV",
 /* 202 */ "fill_mode ::= NULL",
 /* 203 */ "fill_mode ::= LINEAR",
 /* 204 */ "fill_mode ::= NEXT",
 /* 205 */ "group_by_clause_opt ::=",
 /* 206 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 207 */ "group_by_list ::= expression",
 /* 208 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 209 */ "having_clause_opt ::=",
 /* 210 */ "having_clause_opt ::= HAVING search_condition",
 /* 211 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 212 */ "query_expression_body ::= query_primary",
 /* 213 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 214 */ "query_primary ::= query_specification",
 /* 215 */ "order_by_clause_opt ::=",
 /* 216 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 217 */ "slimit_clause_opt ::=",
 /* 218 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 219 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 220 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 221 */ "limit_clause_opt ::=",
 /* 222 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 223 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 224 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 225 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 226 */ "search_condition ::= common_expression",
 /* 227 */ "sort_specification_list ::= sort_specification",
 /* 228 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 229 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 230 */ "ordering_specification_opt ::=",
 /* 231 */ "ordering_specification_opt ::= ASC",
 /* 232 */ "ordering_specification_opt ::= DESC",
 /* 233 */ "null_ordering_opt ::=",
 /* 234 */ "null_ordering_opt ::= NULLS FIRST",
 /* 235 */ "null_ordering_opt ::= NULLS LAST",
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
    case 134: /* cmd */
    case 142: /* full_table_name */
    case 149: /* create_subtable_clause */
    case 152: /* drop_table_clause */
    case 155: /* column_def */
    case 158: /* col_name */
    case 159: /* query_expression */
    case 160: /* literal */
    case 161: /* duration_literal */
    case 165: /* expression */
    case 166: /* column_reference */
    case 168: /* subquery */
    case 169: /* predicate */
    case 172: /* in_predicate_value */
    case 173: /* boolean_value_expression */
    case 174: /* boolean_primary */
    case 175: /* common_expression */
    case 176: /* from_clause */
    case 177: /* table_reference_list */
    case 178: /* table_reference */
    case 179: /* table_primary */
    case 180: /* joined_table */
    case 182: /* parenthesized_joined_table */
    case 184: /* search_condition */
    case 185: /* query_specification */
    case 188: /* where_clause_opt */
    case 190: /* twindow_clause_opt */
    case 192: /* having_clause_opt */
    case 194: /* select_item */
    case 195: /* sliding_opt */
    case 196: /* fill_opt */
    case 199: /* query_expression_body */
    case 201: /* slimit_clause_opt */
    case 202: /* limit_clause_opt */
    case 203: /* query_primary */
    case 205: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy392)); 
}
      break;
    case 135: /* user_name */
    case 136: /* dnode_endpoint */
    case 137: /* dnode_host_name */
    case 139: /* db_name */
    case 154: /* table_name */
    case 156: /* column_name */
    case 162: /* function_name */
    case 163: /* table_alias */
    case 164: /* column_alias */
    case 181: /* alias_opt */
{
 
}
      break;
    case 138: /* not_exists_opt */
    case 141: /* exists_opt */
    case 186: /* set_quantifier_opt */
{
 
}
      break;
    case 140: /* db_options */
{
 tfree((yypminor->yy103)); 
}
      break;
    case 143: /* column_def_list */
    case 144: /* tags_def_opt */
    case 146: /* multi_create_clause */
    case 147: /* tags_def */
    case 148: /* multi_drop_clause */
    case 150: /* specific_tags_opt */
    case 151: /* literal_list */
    case 153: /* col_name_list */
    case 167: /* expression_list */
    case 187: /* select_list */
    case 189: /* partition_by_clause_opt */
    case 191: /* group_by_clause_opt */
    case 193: /* select_sublist */
    case 198: /* group_by_list */
    case 200: /* order_by_clause_opt */
    case 204: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy184)); 
}
      break;
    case 145: /* table_options */
{
 tfree((yypminor->yy334)); 
}
      break;
    case 157: /* type_name */
{
 
}
      break;
    case 170: /* compare_op */
    case 171: /* in_op */
{
 
}
      break;
    case 183: /* join_type */
{
 
}
      break;
    case 197: /* fill_mode */
{
 
}
      break;
    case 206: /* ordering_specification_opt */
{
 
}
      break;
    case 207: /* null_ordering_opt */
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
  {  134,   -5 }, /* (0) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  134,   -5 }, /* (1) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  134,   -5 }, /* (2) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  134,   -3 }, /* (3) cmd ::= DROP USER user_name */
  {  134,   -2 }, /* (4) cmd ::= SHOW USERS */
  {  134,   -3 }, /* (5) cmd ::= CREATE DNODE dnode_endpoint */
  {  134,   -5 }, /* (6) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  134,   -3 }, /* (7) cmd ::= DROP DNODE NK_INTEGER */
  {  134,   -3 }, /* (8) cmd ::= DROP DNODE dnode_endpoint */
  {  134,   -2 }, /* (9) cmd ::= SHOW DNODES */
  {  136,   -1 }, /* (10) dnode_endpoint ::= NK_STRING */
  {  137,   -1 }, /* (11) dnode_host_name ::= NK_ID */
  {  137,   -1 }, /* (12) dnode_host_name ::= NK_IPTOKEN */
  {  134,   -5 }, /* (13) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  134,   -4 }, /* (14) cmd ::= DROP DATABASE exists_opt db_name */
  {  134,   -2 }, /* (15) cmd ::= SHOW DATABASES */
  {  134,   -2 }, /* (16) cmd ::= USE db_name */
  {  138,   -3 }, /* (17) not_exists_opt ::= IF NOT EXISTS */
  {  138,    0 }, /* (18) not_exists_opt ::= */
  {  141,   -2 }, /* (19) exists_opt ::= IF EXISTS */
  {  141,    0 }, /* (20) exists_opt ::= */
  {  140,    0 }, /* (21) db_options ::= */
  {  140,   -3 }, /* (22) db_options ::= db_options BLOCKS NK_INTEGER */
  {  140,   -3 }, /* (23) db_options ::= db_options CACHE NK_INTEGER */
  {  140,   -3 }, /* (24) db_options ::= db_options CACHELAST NK_INTEGER */
  {  140,   -3 }, /* (25) db_options ::= db_options COMP NK_INTEGER */
  {  140,   -3 }, /* (26) db_options ::= db_options DAYS NK_INTEGER */
  {  140,   -3 }, /* (27) db_options ::= db_options FSYNC NK_INTEGER */
  {  140,   -3 }, /* (28) db_options ::= db_options MAXROWS NK_INTEGER */
  {  140,   -3 }, /* (29) db_options ::= db_options MINROWS NK_INTEGER */
  {  140,   -3 }, /* (30) db_options ::= db_options KEEP NK_INTEGER */
  {  140,   -3 }, /* (31) db_options ::= db_options PRECISION NK_STRING */
  {  140,   -3 }, /* (32) db_options ::= db_options QUORUM NK_INTEGER */
  {  140,   -3 }, /* (33) db_options ::= db_options REPLICA NK_INTEGER */
  {  140,   -3 }, /* (34) db_options ::= db_options TTL NK_INTEGER */
  {  140,   -3 }, /* (35) db_options ::= db_options WAL NK_INTEGER */
  {  140,   -3 }, /* (36) db_options ::= db_options VGROUPS NK_INTEGER */
  {  140,   -3 }, /* (37) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  140,   -3 }, /* (38) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  134,   -9 }, /* (39) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  134,   -3 }, /* (40) cmd ::= CREATE TABLE multi_create_clause */
  {  134,   -9 }, /* (41) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  134,   -3 }, /* (42) cmd ::= DROP TABLE multi_drop_clause */
  {  134,   -4 }, /* (43) cmd ::= DROP STABLE exists_opt full_table_name */
  {  134,   -2 }, /* (44) cmd ::= SHOW TABLES */
  {  134,   -2 }, /* (45) cmd ::= SHOW STABLES */
  {  146,   -1 }, /* (46) multi_create_clause ::= create_subtable_clause */
  {  146,   -2 }, /* (47) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  149,   -9 }, /* (48) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  148,   -1 }, /* (49) multi_drop_clause ::= drop_table_clause */
  {  148,   -2 }, /* (50) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  152,   -2 }, /* (51) drop_table_clause ::= exists_opt full_table_name */
  {  150,    0 }, /* (52) specific_tags_opt ::= */
  {  150,   -3 }, /* (53) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  142,   -1 }, /* (54) full_table_name ::= table_name */
  {  142,   -3 }, /* (55) full_table_name ::= db_name NK_DOT table_name */
  {  143,   -1 }, /* (56) column_def_list ::= column_def */
  {  143,   -3 }, /* (57) column_def_list ::= column_def_list NK_COMMA column_def */
  {  155,   -2 }, /* (58) column_def ::= column_name type_name */
  {  155,   -4 }, /* (59) column_def ::= column_name type_name COMMENT NK_STRING */
  {  157,   -1 }, /* (60) type_name ::= BOOL */
  {  157,   -1 }, /* (61) type_name ::= TINYINT */
  {  157,   -1 }, /* (62) type_name ::= SMALLINT */
  {  157,   -1 }, /* (63) type_name ::= INT */
  {  157,   -1 }, /* (64) type_name ::= INTEGER */
  {  157,   -1 }, /* (65) type_name ::= BIGINT */
  {  157,   -1 }, /* (66) type_name ::= FLOAT */
  {  157,   -1 }, /* (67) type_name ::= DOUBLE */
  {  157,   -4 }, /* (68) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (69) type_name ::= TIMESTAMP */
  {  157,   -4 }, /* (70) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  157,   -2 }, /* (71) type_name ::= TINYINT UNSIGNED */
  {  157,   -2 }, /* (72) type_name ::= SMALLINT UNSIGNED */
  {  157,   -2 }, /* (73) type_name ::= INT UNSIGNED */
  {  157,   -2 }, /* (74) type_name ::= BIGINT UNSIGNED */
  {  157,   -1 }, /* (75) type_name ::= JSON */
  {  157,   -4 }, /* (76) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (77) type_name ::= MEDIUMBLOB */
  {  157,   -1 }, /* (78) type_name ::= BLOB */
  {  157,   -4 }, /* (79) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (80) type_name ::= DECIMAL */
  {  157,   -4 }, /* (81) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  157,   -6 }, /* (82) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  144,    0 }, /* (83) tags_def_opt ::= */
  {  144,   -1 }, /* (84) tags_def_opt ::= tags_def */
  {  147,   -4 }, /* (85) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  145,    0 }, /* (86) table_options ::= */
  {  145,   -3 }, /* (87) table_options ::= table_options COMMENT NK_STRING */
  {  145,   -3 }, /* (88) table_options ::= table_options KEEP NK_INTEGER */
  {  145,   -3 }, /* (89) table_options ::= table_options TTL NK_INTEGER */
  {  145,   -5 }, /* (90) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  153,   -1 }, /* (91) col_name_list ::= col_name */
  {  153,   -3 }, /* (92) col_name_list ::= col_name_list NK_COMMA col_name */
  {  158,   -1 }, /* (93) col_name ::= column_name */
  {  134,   -2 }, /* (94) cmd ::= SHOW VGROUPS */
  {  134,   -4 }, /* (95) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  134,   -1 }, /* (96) cmd ::= query_expression */
  {  160,   -1 }, /* (97) literal ::= NK_INTEGER */
  {  160,   -1 }, /* (98) literal ::= NK_FLOAT */
  {  160,   -1 }, /* (99) literal ::= NK_STRING */
  {  160,   -1 }, /* (100) literal ::= NK_BOOL */
  {  160,   -2 }, /* (101) literal ::= TIMESTAMP NK_STRING */
  {  160,   -1 }, /* (102) literal ::= duration_literal */
  {  161,   -1 }, /* (103) duration_literal ::= NK_VARIABLE */
  {  151,   -1 }, /* (104) literal_list ::= literal */
  {  151,   -3 }, /* (105) literal_list ::= literal_list NK_COMMA literal */
  {  139,   -1 }, /* (106) db_name ::= NK_ID */
  {  154,   -1 }, /* (107) table_name ::= NK_ID */
  {  156,   -1 }, /* (108) column_name ::= NK_ID */
  {  162,   -1 }, /* (109) function_name ::= NK_ID */
  {  163,   -1 }, /* (110) table_alias ::= NK_ID */
  {  164,   -1 }, /* (111) column_alias ::= NK_ID */
  {  135,   -1 }, /* (112) user_name ::= NK_ID */
  {  165,   -1 }, /* (113) expression ::= literal */
  {  165,   -1 }, /* (114) expression ::= column_reference */
  {  165,   -4 }, /* (115) expression ::= function_name NK_LP expression_list NK_RP */
  {  165,   -4 }, /* (116) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  165,   -1 }, /* (117) expression ::= subquery */
  {  165,   -3 }, /* (118) expression ::= NK_LP expression NK_RP */
  {  165,   -2 }, /* (119) expression ::= NK_PLUS expression */
  {  165,   -2 }, /* (120) expression ::= NK_MINUS expression */
  {  165,   -3 }, /* (121) expression ::= expression NK_PLUS expression */
  {  165,   -3 }, /* (122) expression ::= expression NK_MINUS expression */
  {  165,   -3 }, /* (123) expression ::= expression NK_STAR expression */
  {  165,   -3 }, /* (124) expression ::= expression NK_SLASH expression */
  {  165,   -3 }, /* (125) expression ::= expression NK_REM expression */
  {  167,   -1 }, /* (126) expression_list ::= expression */
  {  167,   -3 }, /* (127) expression_list ::= expression_list NK_COMMA expression */
  {  166,   -1 }, /* (128) column_reference ::= column_name */
  {  166,   -3 }, /* (129) column_reference ::= table_name NK_DOT column_name */
  {  169,   -3 }, /* (130) predicate ::= expression compare_op expression */
  {  169,   -5 }, /* (131) predicate ::= expression BETWEEN expression AND expression */
  {  169,   -6 }, /* (132) predicate ::= expression NOT BETWEEN expression AND expression */
  {  169,   -3 }, /* (133) predicate ::= expression IS NULL */
  {  169,   -4 }, /* (134) predicate ::= expression IS NOT NULL */
  {  169,   -3 }, /* (135) predicate ::= expression in_op in_predicate_value */
  {  170,   -1 }, /* (136) compare_op ::= NK_LT */
  {  170,   -1 }, /* (137) compare_op ::= NK_GT */
  {  170,   -1 }, /* (138) compare_op ::= NK_LE */
  {  170,   -1 }, /* (139) compare_op ::= NK_GE */
  {  170,   -1 }, /* (140) compare_op ::= NK_NE */
  {  170,   -1 }, /* (141) compare_op ::= NK_EQ */
  {  170,   -1 }, /* (142) compare_op ::= LIKE */
  {  170,   -2 }, /* (143) compare_op ::= NOT LIKE */
  {  170,   -1 }, /* (144) compare_op ::= MATCH */
  {  170,   -1 }, /* (145) compare_op ::= NMATCH */
  {  171,   -1 }, /* (146) in_op ::= IN */
  {  171,   -2 }, /* (147) in_op ::= NOT IN */
  {  172,   -3 }, /* (148) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  173,   -1 }, /* (149) boolean_value_expression ::= boolean_primary */
  {  173,   -2 }, /* (150) boolean_value_expression ::= NOT boolean_primary */
  {  173,   -3 }, /* (151) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  173,   -3 }, /* (152) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  174,   -1 }, /* (153) boolean_primary ::= predicate */
  {  174,   -3 }, /* (154) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  175,   -1 }, /* (155) common_expression ::= expression */
  {  175,   -1 }, /* (156) common_expression ::= boolean_value_expression */
  {  176,   -2 }, /* (157) from_clause ::= FROM table_reference_list */
  {  177,   -1 }, /* (158) table_reference_list ::= table_reference */
  {  177,   -3 }, /* (159) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  178,   -1 }, /* (160) table_reference ::= table_primary */
  {  178,   -1 }, /* (161) table_reference ::= joined_table */
  {  179,   -2 }, /* (162) table_primary ::= table_name alias_opt */
  {  179,   -4 }, /* (163) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  179,   -2 }, /* (164) table_primary ::= subquery alias_opt */
  {  179,   -1 }, /* (165) table_primary ::= parenthesized_joined_table */
  {  181,    0 }, /* (166) alias_opt ::= */
  {  181,   -1 }, /* (167) alias_opt ::= table_alias */
  {  181,   -2 }, /* (168) alias_opt ::= AS table_alias */
  {  182,   -3 }, /* (169) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  182,   -3 }, /* (170) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  180,   -6 }, /* (171) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  183,    0 }, /* (172) join_type ::= */
  {  183,   -1 }, /* (173) join_type ::= INNER */
  {  185,   -9 }, /* (174) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  186,    0 }, /* (175) set_quantifier_opt ::= */
  {  186,   -1 }, /* (176) set_quantifier_opt ::= DISTINCT */
  {  186,   -1 }, /* (177) set_quantifier_opt ::= ALL */
  {  187,   -1 }, /* (178) select_list ::= NK_STAR */
  {  187,   -1 }, /* (179) select_list ::= select_sublist */
  {  193,   -1 }, /* (180) select_sublist ::= select_item */
  {  193,   -3 }, /* (181) select_sublist ::= select_sublist NK_COMMA select_item */
  {  194,   -1 }, /* (182) select_item ::= common_expression */
  {  194,   -2 }, /* (183) select_item ::= common_expression column_alias */
  {  194,   -3 }, /* (184) select_item ::= common_expression AS column_alias */
  {  194,   -3 }, /* (185) select_item ::= table_name NK_DOT NK_STAR */
  {  188,    0 }, /* (186) where_clause_opt ::= */
  {  188,   -2 }, /* (187) where_clause_opt ::= WHERE search_condition */
  {  189,    0 }, /* (188) partition_by_clause_opt ::= */
  {  189,   -3 }, /* (189) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  190,    0 }, /* (190) twindow_clause_opt ::= */
  {  190,   -6 }, /* (191) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  190,   -4 }, /* (192) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  190,   -6 }, /* (193) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  190,   -8 }, /* (194) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  195,    0 }, /* (195) sliding_opt ::= */
  {  195,   -4 }, /* (196) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  196,    0 }, /* (197) fill_opt ::= */
  {  196,   -4 }, /* (198) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  196,   -6 }, /* (199) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  197,   -1 }, /* (200) fill_mode ::= NONE */
  {  197,   -1 }, /* (201) fill_mode ::= PREV */
  {  197,   -1 }, /* (202) fill_mode ::= NULL */
  {  197,   -1 }, /* (203) fill_mode ::= LINEAR */
  {  197,   -1 }, /* (204) fill_mode ::= NEXT */
  {  191,    0 }, /* (205) group_by_clause_opt ::= */
  {  191,   -3 }, /* (206) group_by_clause_opt ::= GROUP BY group_by_list */
  {  198,   -1 }, /* (207) group_by_list ::= expression */
  {  198,   -3 }, /* (208) group_by_list ::= group_by_list NK_COMMA expression */
  {  192,    0 }, /* (209) having_clause_opt ::= */
  {  192,   -2 }, /* (210) having_clause_opt ::= HAVING search_condition */
  {  159,   -4 }, /* (211) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  199,   -1 }, /* (212) query_expression_body ::= query_primary */
  {  199,   -4 }, /* (213) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  203,   -1 }, /* (214) query_primary ::= query_specification */
  {  200,    0 }, /* (215) order_by_clause_opt ::= */
  {  200,   -3 }, /* (216) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  201,    0 }, /* (217) slimit_clause_opt ::= */
  {  201,   -2 }, /* (218) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  201,   -4 }, /* (219) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  201,   -4 }, /* (220) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  202,    0 }, /* (221) limit_clause_opt ::= */
  {  202,   -2 }, /* (222) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  202,   -4 }, /* (223) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  202,   -4 }, /* (224) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  168,   -3 }, /* (225) subquery ::= NK_LP query_expression NK_RP */
  {  184,   -1 }, /* (226) search_condition ::= common_expression */
  {  204,   -1 }, /* (227) sort_specification_list ::= sort_specification */
  {  204,   -3 }, /* (228) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  205,   -3 }, /* (229) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  206,    0 }, /* (230) ordering_specification_opt ::= */
  {  206,   -1 }, /* (231) ordering_specification_opt ::= ASC */
  {  206,   -1 }, /* (232) ordering_specification_opt ::= DESC */
  {  207,    0 }, /* (233) null_ordering_opt ::= */
  {  207,   -2 }, /* (234) null_ordering_opt ::= NULLS FIRST */
  {  207,   -2 }, /* (235) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy0);}
        break;
      case 1: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy161, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0);}
        break;
      case 2: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy161, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0);}
        break;
      case 3: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy161); }
        break;
      case 4: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 5: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy161, NULL);}
        break;
      case 6: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy0);}
        break;
      case 7: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0);}
        break;
      case 8: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy161);}
        break;
      case 9: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 106: /* db_name ::= NK_ID */ yytestcase(yyruleno==106);
      case 107: /* table_name ::= NK_ID */ yytestcase(yyruleno==107);
      case 108: /* column_name ::= NK_ID */ yytestcase(yyruleno==108);
      case 109: /* function_name ::= NK_ID */ yytestcase(yyruleno==109);
      case 110: /* table_alias ::= NK_ID */ yytestcase(yyruleno==110);
      case 111: /* column_alias ::= NK_ID */ yytestcase(yyruleno==111);
      case 112: /* user_name ::= NK_ID */ yytestcase(yyruleno==112);
{ yylhsminor.yy161 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 13: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy377, &yymsp[-1].minor.yy161, yymsp[0].minor.yy103);}
        break;
      case 14: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy377, &yymsp[0].minor.yy161); }
        break;
      case 15: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 16: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy161);}
        break;
      case 17: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy377 = true; }
        break;
      case 18: /* not_exists_opt ::= */
      case 20: /* exists_opt ::= */ yytestcase(yyruleno==20);
      case 175: /* set_quantifier_opt ::= */ yytestcase(yyruleno==175);
{ yymsp[1].minor.yy377 = false; }
        break;
      case 19: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy377 = true; }
        break;
      case 21: /* db_options ::= */
{ yymsp[1].minor.yy103 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 22: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 23: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 24: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 25: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 26: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 27: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 28: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 29: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 30: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 31: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 32: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 33: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 34: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 35: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 36: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 37: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 38: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy103 = setDatabaseOption(pCxt, yymsp[-2].minor.yy103, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy103 = yylhsminor.yy103;
        break;
      case 39: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 41: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==41);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy377, yymsp[-5].minor.yy392, yymsp[-3].minor.yy184, yymsp[-1].minor.yy184, yymsp[0].minor.yy334);}
        break;
      case 40: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy184);}
        break;
      case 42: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy184); }
        break;
      case 43: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy377, yymsp[0].minor.yy392); }
        break;
      case 44: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 45: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 46: /* multi_create_clause ::= create_subtable_clause */
      case 49: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==49);
      case 56: /* column_def_list ::= column_def */ yytestcase(yyruleno==56);
      case 91: /* col_name_list ::= col_name */ yytestcase(yyruleno==91);
      case 180: /* select_sublist ::= select_item */ yytestcase(yyruleno==180);
      case 227: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==227);
{ yylhsminor.yy184 = createNodeList(pCxt, yymsp[0].minor.yy392); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 47: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 50: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==50);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-1].minor.yy184, yymsp[0].minor.yy392); }
  yymsp[-1].minor.yy184 = yylhsminor.yy184;
        break;
      case 48: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy392 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy377, yymsp[-7].minor.yy392, yymsp[-5].minor.yy392, yymsp[-4].minor.yy184, yymsp[-1].minor.yy184); }
  yymsp[-8].minor.yy392 = yylhsminor.yy392;
        break;
      case 51: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy392 = createDropTableClause(pCxt, yymsp[-1].minor.yy377, yymsp[0].minor.yy392); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 52: /* specific_tags_opt ::= */
      case 83: /* tags_def_opt ::= */ yytestcase(yyruleno==83);
      case 188: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==188);
      case 205: /* group_by_clause_opt ::= */ yytestcase(yyruleno==205);
      case 215: /* order_by_clause_opt ::= */ yytestcase(yyruleno==215);
{ yymsp[1].minor.yy184 = NULL; }
        break;
      case 53: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy184 = yymsp[-1].minor.yy184; }
        break;
      case 54: /* full_table_name ::= table_name */
{ yylhsminor.yy392 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy161, NULL); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 55: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy392 = createRealTableNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161, NULL); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 57: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 92: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==92);
      case 181: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==181);
      case 228: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==228);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, yymsp[0].minor.yy392); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 58: /* column_def ::= column_name type_name */
{ yylhsminor.yy392 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy161, yymsp[0].minor.yy240, NULL); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 59: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy392 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy161, yymsp[-2].minor.yy240, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 60: /* type_name ::= BOOL */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 61: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 62: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 63: /* type_name ::= INT */
      case 64: /* type_name ::= INTEGER */ yytestcase(yyruleno==64);
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 65: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 66: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 67: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 68: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 69: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 70: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 71: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 72: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 73: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 74: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 75: /* type_name ::= JSON */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 76: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 77: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 78: /* type_name ::= BLOB */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 79: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 80: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 81: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 82: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 84: /* tags_def_opt ::= tags_def */
      case 179: /* select_list ::= select_sublist */ yytestcase(yyruleno==179);
{ yylhsminor.yy184 = yymsp[0].minor.yy184; }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 85: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy184 = yymsp[-1].minor.yy184; }
        break;
      case 86: /* table_options ::= */
{ yymsp[1].minor.yy334 = createDefaultTableOptions(pCxt);}
        break;
      case 87: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 88: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 89: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 90: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy334 = setTableSmaOption(pCxt, yymsp[-4].minor.yy334, yymsp[-1].minor.yy184); }
  yymsp[-4].minor.yy334 = yylhsminor.yy334;
        break;
      case 93: /* col_name ::= column_name */
{ yylhsminor.yy392 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy161); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 94: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 95: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy161); }
        break;
      case 96: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy392; }
        break;
      case 97: /* literal ::= NK_INTEGER */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 98: /* literal ::= NK_FLOAT */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 99: /* literal ::= NK_STRING */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 100: /* literal ::= NK_BOOL */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 101: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 102: /* literal ::= duration_literal */
      case 113: /* expression ::= literal */ yytestcase(yyruleno==113);
      case 114: /* expression ::= column_reference */ yytestcase(yyruleno==114);
      case 117: /* expression ::= subquery */ yytestcase(yyruleno==117);
      case 149: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==149);
      case 153: /* boolean_primary ::= predicate */ yytestcase(yyruleno==153);
      case 155: /* common_expression ::= expression */ yytestcase(yyruleno==155);
      case 156: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==156);
      case 158: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==158);
      case 160: /* table_reference ::= table_primary */ yytestcase(yyruleno==160);
      case 161: /* table_reference ::= joined_table */ yytestcase(yyruleno==161);
      case 165: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==165);
      case 212: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==212);
      case 214: /* query_primary ::= query_specification */ yytestcase(yyruleno==214);
{ yylhsminor.yy392 = yymsp[0].minor.yy392; }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 103: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 104: /* literal_list ::= literal */
      case 126: /* expression_list ::= expression */ yytestcase(yyruleno==126);
{ yylhsminor.yy184 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392)); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 105: /* literal_list ::= literal_list NK_COMMA literal */
      case 127: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==127);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, releaseRawExprNode(pCxt, yymsp[0].minor.yy392)); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 115: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy161, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy161, yymsp[-1].minor.yy184)); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 116: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy161, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy161, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 118: /* expression ::= NK_LP expression NK_RP */
      case 154: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==154);
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 119: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy392));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 120: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), NULL));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 121: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 122: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 123: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 124: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 125: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 128: /* column_reference ::= column_name */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy161, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy161)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 129: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161, createColumnNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 130: /* predicate ::= expression compare_op expression */
      case 135: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==135);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy220, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 131: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy392), releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-4].minor.yy392 = yylhsminor.yy392;
        break;
      case 132: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[-5].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-5].minor.yy392 = yylhsminor.yy392;
        break;
      case 133: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), NULL));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 134: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy392), NULL));
                                                                                  }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 136: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy220 = OP_TYPE_LOWER_THAN; }
        break;
      case 137: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy220 = OP_TYPE_GREATER_THAN; }
        break;
      case 138: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy220 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 139: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy220 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 140: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy220 = OP_TYPE_NOT_EQUAL; }
        break;
      case 141: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy220 = OP_TYPE_EQUAL; }
        break;
      case 142: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy220 = OP_TYPE_LIKE; }
        break;
      case 143: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy220 = OP_TYPE_NOT_LIKE; }
        break;
      case 144: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy220 = OP_TYPE_MATCH; }
        break;
      case 145: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy220 = OP_TYPE_NMATCH; }
        break;
      case 146: /* in_op ::= IN */
{ yymsp[0].minor.yy220 = OP_TYPE_IN; }
        break;
      case 147: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy220 = OP_TYPE_NOT_IN; }
        break;
      case 148: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy184)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 150: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), NULL));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 151: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 152: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 157: /* from_clause ::= FROM table_reference_list */
      case 187: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==187);
      case 210: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==210);
{ yymsp[-1].minor.yy392 = yymsp[0].minor.yy392; }
        break;
      case 159: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy392 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy392, yymsp[0].minor.yy392, NULL); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 162: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy392 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy161, &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 163: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy392 = createRealTableNode(pCxt, &yymsp[-3].minor.yy161, &yymsp[-1].minor.yy161, &yymsp[0].minor.yy161); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 164: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy392 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 166: /* alias_opt ::= */
{ yymsp[1].minor.yy161 = nil_token;  }
        break;
      case 167: /* alias_opt ::= table_alias */
{ yylhsminor.yy161 = yymsp[0].minor.yy161; }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 168: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy161 = yymsp[0].minor.yy161; }
        break;
      case 169: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 170: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==170);
{ yymsp[-2].minor.yy392 = yymsp[-1].minor.yy392; }
        break;
      case 171: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy392 = createJoinTableNode(pCxt, yymsp[-4].minor.yy308, yymsp[-5].minor.yy392, yymsp[-2].minor.yy392, yymsp[0].minor.yy392); }
  yymsp[-5].minor.yy392 = yylhsminor.yy392;
        break;
      case 172: /* join_type ::= */
{ yymsp[1].minor.yy308 = JOIN_TYPE_INNER; }
        break;
      case 173: /* join_type ::= INNER */
{ yymsp[0].minor.yy308 = JOIN_TYPE_INNER; }
        break;
      case 174: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy392 = createSelectStmt(pCxt, yymsp[-7].minor.yy377, yymsp[-6].minor.yy184, yymsp[-5].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addWhereClause(pCxt, yymsp[-8].minor.yy392, yymsp[-4].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addPartitionByClause(pCxt, yymsp[-8].minor.yy392, yymsp[-3].minor.yy184);
                                                                                    yymsp[-8].minor.yy392 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy392, yymsp[-2].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addGroupByClause(pCxt, yymsp[-8].minor.yy392, yymsp[-1].minor.yy184);
                                                                                    yymsp[-8].minor.yy392 = addHavingClause(pCxt, yymsp[-8].minor.yy392, yymsp[0].minor.yy392);
                                                                                  }
        break;
      case 176: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy377 = true; }
        break;
      case 177: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy377 = false; }
        break;
      case 178: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy184 = NULL; }
        break;
      case 182: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), &t);
                                                                                  }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 183: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 184: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 185: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy392 = createColumnNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 186: /* where_clause_opt ::= */
      case 190: /* twindow_clause_opt ::= */ yytestcase(yyruleno==190);
      case 195: /* sliding_opt ::= */ yytestcase(yyruleno==195);
      case 197: /* fill_opt ::= */ yytestcase(yyruleno==197);
      case 209: /* having_clause_opt ::= */ yytestcase(yyruleno==209);
      case 217: /* slimit_clause_opt ::= */ yytestcase(yyruleno==217);
      case 221: /* limit_clause_opt ::= */ yytestcase(yyruleno==221);
{ yymsp[1].minor.yy392 = NULL; }
        break;
      case 189: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 206: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==206);
      case 216: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==216);
{ yymsp[-2].minor.yy184 = yymsp[0].minor.yy184; }
        break;
      case 191: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy392 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy392), &yymsp[-1].minor.yy0); }
        break;
      case 192: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy392 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392)); }
        break;
      case 193: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy392 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy392, NULL, yymsp[-1].minor.yy392, yymsp[0].minor.yy392); }
        break;
      case 194: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy392 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy392, yymsp[-3].minor.yy392, yymsp[-1].minor.yy392, yymsp[0].minor.yy392); }
        break;
      case 196: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy392 = yymsp[-1].minor.yy392; }
        break;
      case 198: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy392 = createFillNode(pCxt, yymsp[-1].minor.yy166, NULL); }
        break;
      case 199: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy392 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy184)); }
        break;
      case 200: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy166 = FILL_MODE_NONE; }
        break;
      case 201: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy166 = FILL_MODE_PREV; }
        break;
      case 202: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy166 = FILL_MODE_NULL; }
        break;
      case 203: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy166 = FILL_MODE_LINEAR; }
        break;
      case 204: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy166 = FILL_MODE_NEXT; }
        break;
      case 207: /* group_by_list ::= expression */
{ yylhsminor.yy184 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 208: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 211: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy392 = addOrderByClause(pCxt, yymsp[-3].minor.yy392, yymsp[-2].minor.yy184);
                                                                                    yylhsminor.yy392 = addSlimitClause(pCxt, yylhsminor.yy392, yymsp[-1].minor.yy392);
                                                                                    yylhsminor.yy392 = addLimitClause(pCxt, yylhsminor.yy392, yymsp[0].minor.yy392);
                                                                                  }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 213: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy392 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy392, yymsp[0].minor.yy392); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 218: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 222: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==222);
{ yymsp[-1].minor.yy392 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 219: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 223: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==223);
{ yymsp[-3].minor.yy392 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 220: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 224: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==224);
{ yymsp[-3].minor.yy392 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 225: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy392); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 226: /* search_condition ::= common_expression */
{ yylhsminor.yy392 = releaseRawExprNode(pCxt, yymsp[0].minor.yy392); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 229: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy392 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), yymsp[-1].minor.yy162, yymsp[0].minor.yy9); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 230: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy162 = ORDER_ASC; }
        break;
      case 231: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy162 = ORDER_ASC; }
        break;
      case 232: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy162 = ORDER_DESC; }
        break;
      case 233: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy9 = NULL_ORDER_DEFAULT; }
        break;
      case 234: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy9 = NULL_ORDER_FIRST; }
        break;
      case 235: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy9 = NULL_ORDER_LAST; }
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
