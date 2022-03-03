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
#include "ttoken.h"
#include "ttokendef.h"
#include "astCreateFuncs.h"

#if 0
#define PARSER_TRACE printf("lemon rule = %s\n", yyRuleName[yyruleno])
#define PARSER_DESTRUCTOR_TRACE printf("lemon destroy token = %s\n", yyTokenName[yymajor])
#define PARSER_COMPLETE printf("parsing complete!\n" )
#else
#define PARSER_TRACE
#define PARSER_DESTRUCTOR_TRACE
#define PARSER_COMPLETE
#endif
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
**    NewParseTOKENTYPE     is the data type used for minor type for terminal
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
**                       which is NewParseTOKENTYPE.  The entry in the union
**                       for terminal symbols is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    NewParseARG_SDECL     A static variable declaration for the %extra_argument
**    NewParseARG_PDECL     A parameter declaration for the %extra_argument
**    NewParseARG_PARAM     Code to pass %extra_argument as a subroutine parameter
**    NewParseARG_STORE     Code to store %extra_argument into yypParser
**    NewParseARG_FETCH     Code to extract %extra_argument from yypParser
**    NewParseCTX_*         As NewParseARG_ except for %extra_context
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
#define YYNOCODE 179
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EOrder yy14;
  ENullOrder yy17;
  SDatabaseOptions* yy27;
  STableOptions* yy40;
  SNodeList* yy60;
  SToken yy105;
  STokenPair yy111;
  SNode* yy172;
  EFillMode yy202;
  EOperatorType yy214;
  SDataType yy248;
  bool yy259;
  EJoinType yy278;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define NewParseARG_SDECL  SAstCreateContext* pCxt ;
#define NewParseARG_PDECL , SAstCreateContext* pCxt 
#define NewParseARG_PARAM ,pCxt 
#define NewParseARG_FETCH  SAstCreateContext* pCxt =yypParser->pCxt ;
#define NewParseARG_STORE yypParser->pCxt =pCxt ;
#define NewParseCTX_SDECL
#define NewParseCTX_PDECL
#define NewParseCTX_PARAM
#define NewParseCTX_FETCH
#define NewParseCTX_STORE
#define YYNSTATE             211
#define YYNRULE              196
#define YYNTOKEN             118
#define YY_MAX_SHIFT         210
#define YY_MIN_SHIFTREDUCE   352
#define YY_MAX_SHIFTREDUCE   547
#define YY_ERROR_ACTION      548
#define YY_ACCEPT_ACTION     549
#define YY_NO_ACTION         550
#define YY_MIN_REDUCE        551
#define YY_MAX_REDUCE        746
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
#define YY_ACTTAB_COUNT (890)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   616,  614,  105,  187,  116,  725,  576,   48,   23,   75,
 /*    10 */    76,  639,   30,   28,   26,   25,   24,  157,  125,  724,
 /*    20 */   678,  100,  128,  723,  678,   30,   28,   26,   25,   24,
 /*    30 */   551,  617,  614,  639,  100,   68,  624,  614,  675,  157,
 /*    40 */   158,  694,  674,   42,  625,  430,  628,  664,   26,   25,
 /*    50 */    24,  663,  660,  210,  678,  209,  208,  207,  206,  205,
 /*    60 */   204,  203,  202,  201,  150,  200,  199,  198,  197,  196,
 /*    70 */   195,  194,  673,   22,  109,  151,  448,  449,  450,  451,
 /*    80 */   452,  453,  454,  456,  457,  458,   22,  109,  140,  448,
 /*    90 */   449,  450,  451,  452,  453,  454,  456,  457,  458,   30,
 /*   100 */    28,   26,   25,   24,  381,  185,  184,  183,  385,  182,
 /*   110 */   387,  388,  181,  390,  178,  117,  396,  175,  398,  399,
 /*   120 */   172,  169,  639,  573,  147,  624,  614,  133,  157,  158,
 /*   130 */   577,   48,   40,  625,   84,  628,  664,  152,  423,   80,
 /*   140 */    99,  660,   10,  639,  142,  497,  624,  614,  421,  143,
 /*   150 */   158,  424,  725,   41,  625,  189,  628,  664,   82,   20,
 /*   160 */   188,  107,  660,   52,   58,  162,   57,  639,  455,  149,
 /*   170 */   723,  459,  190,  157,   10,   74,  142,  421,  132,   73,
 /*   180 */   489,  118,  691,  639,    6,  485,  624,  614,   58,  143,
 /*   190 */   158,  640,   59,   41,  625,   78,  628,  664,  139,    9,
 /*   200 */     8,  107,  660,   52,  683,  639,  485,    2,  624,  614,
 /*   210 */    46,  157,  158,  424,  126,   41,  625,   44,  628,  664,
 /*   220 */   695,   58,  692,  107,  660,  737,  141,   53,  671,  672,
 /*   230 */   511,  676,  639,   43,  698,  624,  614,  705,  157,  158,
 /*   240 */     9,    8,   41,  625,  145,  628,  664,  134,  129,  127,
 /*   250 */   107,  660,  737,  639,  154,  549,  624,  614,  123,  157,
 /*   260 */   158,  721,   60,   41,  625,  160,  628,  664,  488,   29,
 /*   270 */    27,  107,  660,  737,   29,   27,  490,  410,   11,  546,
 /*   280 */   547,  410,  682,   11,  466,   65,  410,  412,   62,  460,
 /*   290 */   416,  412,   31,  725,  427,  121,  412,   31,  445,  621,
 /*   300 */   122,    1,  619,  114,  106,  704,    1,   57,  114,   64,
 /*   310 */   402,  723,  159,  167,   85,    5,  159,   47,  155,  685,
 /*   320 */   136,  159,   67,  411,  413,  416,  120,  411,  413,  416,
 /*   330 */     4,   51,  411,  413,  416,  485,  639,   69,  147,  624,
 /*   340 */   614,  420,  157,  158,   45,  139,   90,  625,   58,  628,
 /*   350 */   423,   29,   27,  147,  679,   29,   27,   46,   19,   32,
 /*   360 */    11,   16,   70,  410,   44,  646,  725,  410,   30,   28,
 /*   370 */    26,   25,   24,  412,   71,  671,  138,  412,  137,  740,
 /*   380 */    57,  725,  110,    1,  723,  114,  156,    7,  153,  114,
 /*   390 */   639,  419,  722,  624,  614,   57,  157,  158,  159,  723,
 /*   400 */    92,  625,  159,  628,  163,   77,  165,  191,  193,  411,
 /*   410 */   413,  416,   81,  411,  413,  416,   86,   83,  639,    3,
 /*   420 */    31,  624,  614,   98,  157,  158,   87,   14,   42,  625,
 /*   430 */    61,  628,  664,  135,   58,  508,  144,  660,   29,   27,
 /*   440 */   146,   29,   27,   63,   29,   27,   35,  510,  639,   50,
 /*   450 */   410,  624,  614,  410,  157,  158,  410,   66,   42,  625,
 /*   460 */   412,  628,  664,  412,  504,   36,  412,  661,  503,   21,
 /*   470 */     7,  130,  114,    1,  619,  114,    7,   37,  114,   30,
 /*   480 */    28,   26,   25,   24,  131,  159,   18,   15,  159,  482,
 /*   490 */    72,  159,   33,  481,   34,    8,  411,  413,  416,  411,
 /*   500 */   413,  416,  411,  413,  416,  639,  618,   56,  624,  614,
 /*   510 */   446,  157,  158,  428,  537,   49,  625,   12,  628,   38,
 /*   520 */    17,  532,  639,  531,  111,  624,  614,  536,  157,  158,
 /*   530 */   535,  112,   96,  625,  113,  628,  639,   79,   13,  624,
 /*   540 */   614,  608,  157,  158,  607,  414,   96,  625,  119,  628,
 /*   550 */   161,  572,  164,  403,  148,  738,  639,  166,  376,  624,
 /*   560 */   614,  115,  157,  158,  168,  171,   96,  625,  108,  628,
 /*   570 */   400,  639,  170,  397,  624,  614,  173,  157,  158,  174,
 /*   580 */   176,   49,  625,  639,  628,  179,  624,  614,  391,  157,
 /*   590 */   158,  177,  395,   91,  625,  389,  628,  639,  180,  394,
 /*   600 */   624,  614,  380,  157,  158,  407,  393,   93,  625,  186,
 /*   610 */   628,  406,  639,  405,  392,  624,  614,   39,  157,  158,
 /*   620 */   353,  739,   88,  625,  639,  628,  372,  624,  614,  192,
 /*   630 */   157,  158,  550,  371,   94,  625,  639,  628,  370,  624,
 /*   640 */   614,  365,  157,  158,  369,  368,   89,  625,  367,  628,
 /*   650 */   366,  364,  639,  550,  363,  624,  614,  550,  157,  158,
 /*   660 */   362,  361,   95,  625,  360,  628,  639,  359,  358,  624,
 /*   670 */   614,  357,  157,  158,  356,  550,  636,  625,  550,  628,
 /*   680 */   550,  639,  550,  550,  624,  614,  550,  157,  158,  550,
 /*   690 */   550,  635,  625,  639,  628,  550,  624,  614,  550,  157,
 /*   700 */   158,  550,  550,  634,  625,  639,  628,  550,  624,  614,
 /*   710 */   550,  157,  158,  550,  550,  103,  625,  639,  628,  550,
 /*   720 */   624,  614,  550,  157,  158,  550,  550,  102,  625,  550,
 /*   730 */   628,  639,  550,  550,  624,  614,  550,  157,  158,  550,
 /*   740 */   550,  104,  625,  639,  628,  139,  624,  614,  550,  157,
 /*   750 */   158,  139,  550,  101,  625,  639,  628,   46,  624,  614,
 /*   760 */   550,  157,  158,   46,   44,   97,  625,  514,  628,  550,
 /*   770 */    44,  550,  550,  550,   54,  671,  672,  550,  676,  550,
 /*   780 */    55,  671,  672,  550,  676,  550,  550,   30,   28,   26,
 /*   790 */    25,   24,  550,  124,  512,  513,  515,  516,  550,  550,
 /*   800 */    30,   28,   26,   25,   24,  550,  550,  550,  550,  550,
 /*   810 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   820 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   830 */   550,  550,  550,  430,  550,  550,  550,  550,  550,  550,
 /*   840 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   850 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   860 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   870 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   880 */   550,  550,  550,  550,  550,  550,  550,  550,  543,  544,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   129,  130,  131,  127,  123,  156,  125,  126,  141,  142,
 /*    10 */   177,  126,   12,   13,   14,   15,   16,  132,  168,  170,
 /*    20 */   134,   21,  137,  174,  134,   12,   13,   14,   15,   16,
 /*    30 */     0,  129,  130,  126,   21,  161,  129,  130,  152,  132,
 /*    40 */   133,  135,  152,  136,  137,   45,  139,  140,   14,   15,
 /*    50 */    16,  144,  145,   23,  134,   25,   26,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,    3,   35,   36,   37,   38,   39,
 /*    70 */    40,   41,  152,   73,   74,   48,   76,   77,   78,   79,
 /*    80 */    80,   81,   82,   83,   84,   85,   73,   74,  154,   76,
 /*    90 */    77,   78,   79,   80,   81,   82,   83,   84,   85,   12,
 /*   100 */    13,   14,   15,   16,   50,   51,   52,   53,   54,   55,
 /*   110 */    56,   57,   58,   59,   60,   18,   62,   63,   64,   65,
 /*   120 */    66,   67,  126,    0,  128,  129,  130,   46,  132,  133,
 /*   130 */   125,  126,  136,  137,   19,  139,  140,  110,   46,   42,
 /*   140 */   144,  145,   44,  126,   46,   14,  129,  130,   46,  132,
 /*   150 */   133,   46,  156,  136,  137,   32,  139,  140,   43,   73,
 /*   160 */    37,  144,  145,  146,   91,   68,  170,  126,   82,  108,
 /*   170 */   174,   85,   49,  132,   44,  158,   46,   46,  137,   87,
 /*   180 */     4,  164,  165,  126,   89,   90,  129,  130,   91,  132,
 /*   190 */   133,  126,   87,  136,  137,  171,  139,  140,  120,    1,
 /*   200 */     2,  144,  145,  146,   88,  126,   90,  157,  129,  130,
 /*   210 */   132,  132,  133,   46,  100,  136,  137,  139,  139,  140,
 /*   220 */   135,   91,  165,  144,  145,  146,  148,  149,  150,  151,
 /*   230 */    45,  153,  126,   48,  155,  129,  130,  167,  132,  133,
 /*   240 */     1,    2,  136,  137,   21,  139,  140,   96,   97,   98,
 /*   250 */   144,  145,  146,  126,   48,  118,  129,  130,   99,  132,
 /*   260 */   133,  155,  166,  136,  137,  128,  139,  140,   92,   12,
 /*   270 */    13,  144,  145,  146,   12,   13,   14,   24,   21,  116,
 /*   280 */   117,   24,  155,   21,   45,   45,   24,   34,   48,   45,
 /*   290 */    72,   34,   48,  156,   45,  130,   34,   48,   75,   44,
 /*   300 */   130,   44,   47,   46,  130,  167,   44,  170,   46,  166,
 /*   310 */    45,  174,   59,   48,   45,  107,   59,   48,  112,  163,
 /*   320 */   106,   59,  162,   70,   71,   72,   94,   70,   71,   72,
 /*   330 */    93,  160,   70,   71,   72,   90,  126,  159,  128,  129,
 /*   340 */   130,   46,  132,  133,  132,  120,  136,  137,   91,  139,
 /*   350 */    46,   12,   13,  128,  134,   12,   13,  132,    2,   86,
 /*   360 */    21,   44,  147,   24,  139,  143,  156,   24,   12,   13,
 /*   370 */    14,   15,   16,   34,  149,  150,  151,   34,  153,  178,
 /*   380 */   170,  156,  115,   44,  174,   46,  111,   44,  109,   46,
 /*   390 */   126,   46,  173,  129,  130,  170,  132,  133,   59,  174,
 /*   400 */   136,  137,   59,  139,  120,  172,   46,  122,   20,   70,
 /*   410 */    71,   72,  119,   70,   71,   72,  120,  119,  126,   48,
 /*   420 */    48,  129,  130,  124,  132,  133,  121,   95,  136,  137,
 /*   430 */    45,  139,  140,  169,   91,   45,  144,  145,   12,   13,
 /*   440 */    14,   12,   13,   44,   12,   13,   48,   45,  126,   44,
 /*   450 */    24,  129,  130,   24,  132,  133,   24,   44,  136,  137,
 /*   460 */    34,  139,  140,   34,   45,   44,   34,  145,   45,    2,
 /*   470 */    44,   24,   46,   44,   47,   46,   44,   44,   46,   12,
 /*   480 */    13,   14,   15,   16,   48,   59,   48,   95,   59,   45,
 /*   490 */    47,   59,   88,   45,   48,    2,   70,   71,   72,   70,
 /*   500 */    71,   72,   70,   71,   72,  126,   47,   47,  129,  130,
 /*   510 */    75,  132,  133,   45,   45,  136,  137,   95,  139,    4,
 /*   520 */    48,   24,  126,   24,   24,  129,  130,   24,  132,  133,
 /*   530 */    24,   24,  136,  137,  138,  139,  126,   47,   44,  129,
 /*   540 */   130,    0,  132,  133,    0,   34,  136,  137,  138,  139,
 /*   550 */    69,    0,   47,   45,  175,  176,  126,   24,   46,  129,
 /*   560 */   130,   24,  132,  133,   44,   44,  136,  137,  138,  139,
 /*   570 */    45,  126,   24,   45,  129,  130,   24,  132,  133,   44,
 /*   580 */    24,  136,  137,  126,  139,   24,  129,  130,   45,  132,
 /*   590 */   133,   44,   61,  136,  137,   45,  139,  126,   44,   61,
 /*   600 */   129,  130,   34,  132,  133,   24,   61,  136,  137,   49,
 /*   610 */   139,   24,  126,   24,   61,  129,  130,   44,  132,  133,
 /*   620 */    22,  176,  136,  137,  126,  139,   24,  129,  130,   21,
 /*   630 */   132,  133,  179,   24,  136,  137,  126,  139,   24,  129,
 /*   640 */   130,   34,  132,  133,   24,   24,  136,  137,   24,  139,
 /*   650 */    24,   24,  126,  179,   24,  129,  130,  179,  132,  133,
 /*   660 */    24,   24,  136,  137,   24,  139,  126,   24,   24,  129,
 /*   670 */   130,   24,  132,  133,   24,  179,  136,  137,  179,  139,
 /*   680 */   179,  126,  179,  179,  129,  130,  179,  132,  133,  179,
 /*   690 */   179,  136,  137,  126,  139,  179,  129,  130,  179,  132,
 /*   700 */   133,  179,  179,  136,  137,  126,  139,  179,  129,  130,
 /*   710 */   179,  132,  133,  179,  179,  136,  137,  126,  139,  179,
 /*   720 */   129,  130,  179,  132,  133,  179,  179,  136,  137,  179,
 /*   730 */   139,  126,  179,  179,  129,  130,  179,  132,  133,  179,
 /*   740 */   179,  136,  137,  126,  139,  120,  129,  130,  179,  132,
 /*   750 */   133,  120,  179,  136,  137,  126,  139,  132,  129,  130,
 /*   760 */   179,  132,  133,  132,  139,  136,  137,   75,  139,  179,
 /*   770 */   139,  179,  179,  179,  149,  150,  151,  179,  153,  179,
 /*   780 */   149,  150,  151,  179,  153,  179,  179,   12,   13,   14,
 /*   790 */    15,   16,  179,  101,  102,  103,  104,  105,  179,  179,
 /*   800 */    12,   13,   14,   15,   16,  179,  179,  179,  179,  179,
 /*   810 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   820 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   830 */   179,  179,  179,   45,  179,  179,  179,  179,  179,  179,
 /*   840 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   850 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   860 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   870 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   880 */   179,  179,  179,  179,  179,  179,  179,  179,  113,  114,
 /*   890 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   900 */   179,  179,  179,  179,  179,  179,  179,  179,  179,  179,
 /*   910 */   179,  179,  179,  179,  179,  179,  179,  179,  179,
};
#define YY_SHIFT_COUNT    (210)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (788)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    97,  257,  262,  339,  339,  339,  339,  343,  339,  339,
 /*    10 */   130,  429,  432,  426,  432,  432,  432,  432,  432,  432,
 /*    20 */   432,  432,  432,  432,  432,  432,  432,  432,  432,  432,
 /*    30 */   432,  432,   98,   98,   98,  253,   81,   81,   73,  102,
 /*    40 */     0,   13,   13,  253,   92,   92,   92,  102,   54,  775,
 /*    50 */   692,  151,  105,  116,   95,  116,  131,   61,  176,  167,
 /*    60 */   114,  159,  218,  218,  114,  159,  218,  208,  214,  232,
 /*    70 */   237,  245,  295,  304,  273,  317,  267,  275,  279,  102,
 /*    80 */   345,  360,  388,  345,  388,  890,  890,   30,  356,  467,
 /*    90 */   788,   87,   87,   87,   87,   87,   87,   87,  123,  239,
 /*   100 */    86,   34,   34,   34,   34,  185,  240,  198,  244,  223,
 /*   110 */   163,   27,  206,  249,  255,  265,  269,  115,  371,  372,
 /*   120 */   332,  385,  390,  399,  398,  402,  405,  413,  419,  421,
 /*   130 */   423,  447,  436,  427,  433,  438,  392,  444,  448,  443,
 /*   140 */   404,  446,  459,  460,  493,  435,  468,  469,  472,  422,
 /*   150 */   515,  497,  499,  500,  503,  506,  507,  490,  494,  511,
 /*   160 */   541,  544,  481,  551,  512,  505,  508,  533,  537,  520,
 /*   170 */   525,  548,  521,  528,  552,  535,  543,  556,  547,  550,
 /*   180 */   561,  554,  531,  538,  545,  553,  568,  560,  581,  587,
 /*   190 */   589,  573,  598,  608,  602,  609,  614,  620,  621,  624,
 /*   200 */   626,  607,  627,  630,  636,  637,  640,  643,  644,  647,
 /*   210 */   650,
};
#define YY_REDUCE_COUNT (86)
#define YY_REDUCE_MIN   (-167)
#define YY_REDUCE_MAX   (631)
static const short yy_reduce_ofst[] = {
 /*     0 */   137,   -4,   17,   57,   79,  106,  127,  210,  -93,  292,
 /*    10 */   225,  322,  379,  396,  410,  264,  430,  445,  457,  471,
 /*    20 */   486,  498,  510,  526,  540,  555,  567,  579,  591,  605,
 /*    30 */   617,  629,   78,  625,  631, -129, -115,   41, -151, -119,
 /*    40 */  -133, -133, -133,  -98, -114, -110,  -80,    5, -124, -167,
 /*    50 */  -150, -126,  -94,  -66,  -66,  -66,   65,   24,   50,   85,
 /*    60 */    70,   96,  165,  170,  138,  143,  174,  156,  160,  171,
 /*    70 */   178,  -66,  212,  220,  215,  222,  201,  219,  233,   65,
 /*    80 */   284,  285,  293,  296,  298,  299,  305,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*    10 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*    20 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*    30 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*    40 */   548,  666,  548,  548,  677,  677,  677,  548,  548,  741,
 /*    50 */   548,  701,  693,  669,  683,  670,  548,  726,  686,  548,
 /*    60 */   708,  706,  548,  548,  708,  706,  548,  720,  716,  699,
 /*    70 */   697,  683,  548,  548,  548,  548,  744,  732,  728,  548,
 /*    80 */   548,  548,  553,  548,  553,  603,  554,  548,  548,  548,
 /*    90 */   548,  719,  718,  643,  642,  641,  637,  638,  548,  548,
 /*   100 */   548,  632,  633,  631,  630,  548,  548,  667,  548,  548,
 /*   110 */   548,  729,  733,  548,  620,  548,  548,  548,  690,  700,
 /*   120 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*   130 */   548,  548,  548,  620,  548,  717,  548,  676,  672,  548,
 /*   140 */   548,  668,  619,  548,  662,  548,  548,  548,  727,  548,
 /*   150 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*   160 */   548,  548,  548,  548,  548,  574,  548,  548,  548,  600,
 /*   170 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*   180 */   548,  548,  585,  583,  582,  581,  548,  578,  548,  548,
 /*   190 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*   200 */   548,  548,  548,  548,  548,  548,  548,  548,  548,  548,
 /*   210 */   548,
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
  NewParseARG_SDECL                /* A place to hold %extra_argument */
  NewParseCTX_SDECL                /* A place to hold %extra_context */
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
void NewParseTrace(FILE *TraceFILE, char *zTracePrompt){
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
  /*   19 */ "DATABASE",
  /*   20 */ "IF",
  /*   21 */ "NOT",
  /*   22 */ "EXISTS",
  /*   23 */ "BLOCKS",
  /*   24 */ "NK_INTEGER",
  /*   25 */ "CACHE",
  /*   26 */ "CACHELAST",
  /*   27 */ "COMP",
  /*   28 */ "DAYS",
  /*   29 */ "FSYNC",
  /*   30 */ "MAXROWS",
  /*   31 */ "MINROWS",
  /*   32 */ "KEEP",
  /*   33 */ "PRECISION",
  /*   34 */ "NK_STRING",
  /*   35 */ "QUORUM",
  /*   36 */ "REPLICA",
  /*   37 */ "TTL",
  /*   38 */ "WAL",
  /*   39 */ "VGROUPS",
  /*   40 */ "SINGLESTABLE",
  /*   41 */ "STREAMMODE",
  /*   42 */ "USE",
  /*   43 */ "TABLE",
  /*   44 */ "NK_LP",
  /*   45 */ "NK_RP",
  /*   46 */ "NK_ID",
  /*   47 */ "NK_DOT",
  /*   48 */ "NK_COMMA",
  /*   49 */ "COMMENT",
  /*   50 */ "BOOL",
  /*   51 */ "TINYINT",
  /*   52 */ "SMALLINT",
  /*   53 */ "INT",
  /*   54 */ "INTEGER",
  /*   55 */ "BIGINT",
  /*   56 */ "FLOAT",
  /*   57 */ "DOUBLE",
  /*   58 */ "BINARY",
  /*   59 */ "TIMESTAMP",
  /*   60 */ "NCHAR",
  /*   61 */ "UNSIGNED",
  /*   62 */ "JSON",
  /*   63 */ "VARCHAR",
  /*   64 */ "MEDIUMBLOB",
  /*   65 */ "BLOB",
  /*   66 */ "VARBINARY",
  /*   67 */ "DECIMAL",
  /*   68 */ "SHOW",
  /*   69 */ "DATABASES",
  /*   70 */ "NK_FLOAT",
  /*   71 */ "NK_BOOL",
  /*   72 */ "NK_VARIABLE",
  /*   73 */ "BETWEEN",
  /*   74 */ "IS",
  /*   75 */ "NULL",
  /*   76 */ "NK_LT",
  /*   77 */ "NK_GT",
  /*   78 */ "NK_LE",
  /*   79 */ "NK_GE",
  /*   80 */ "NK_NE",
  /*   81 */ "NK_EQ",
  /*   82 */ "LIKE",
  /*   83 */ "MATCH",
  /*   84 */ "NMATCH",
  /*   85 */ "IN",
  /*   86 */ "FROM",
  /*   87 */ "AS",
  /*   88 */ "JOIN",
  /*   89 */ "ON",
  /*   90 */ "INNER",
  /*   91 */ "SELECT",
  /*   92 */ "DISTINCT",
  /*   93 */ "WHERE",
  /*   94 */ "PARTITION",
  /*   95 */ "BY",
  /*   96 */ "SESSION",
  /*   97 */ "STATE_WINDOW",
  /*   98 */ "INTERVAL",
  /*   99 */ "SLIDING",
  /*  100 */ "FILL",
  /*  101 */ "VALUE",
  /*  102 */ "NONE",
  /*  103 */ "PREV",
  /*  104 */ "LINEAR",
  /*  105 */ "NEXT",
  /*  106 */ "GROUP",
  /*  107 */ "HAVING",
  /*  108 */ "ORDER",
  /*  109 */ "SLIMIT",
  /*  110 */ "SOFFSET",
  /*  111 */ "LIMIT",
  /*  112 */ "OFFSET",
  /*  113 */ "ASC",
  /*  114 */ "DESC",
  /*  115 */ "NULLS",
  /*  116 */ "FIRST",
  /*  117 */ "LAST",
  /*  118 */ "cmd",
  /*  119 */ "exists_opt",
  /*  120 */ "db_name",
  /*  121 */ "db_options",
  /*  122 */ "full_table_name",
  /*  123 */ "column_def_list",
  /*  124 */ "table_options",
  /*  125 */ "column_def",
  /*  126 */ "column_name",
  /*  127 */ "type_name",
  /*  128 */ "query_expression",
  /*  129 */ "literal",
  /*  130 */ "duration_literal",
  /*  131 */ "literal_list",
  /*  132 */ "table_name",
  /*  133 */ "function_name",
  /*  134 */ "table_alias",
  /*  135 */ "column_alias",
  /*  136 */ "expression",
  /*  137 */ "column_reference",
  /*  138 */ "expression_list",
  /*  139 */ "subquery",
  /*  140 */ "predicate",
  /*  141 */ "compare_op",
  /*  142 */ "in_op",
  /*  143 */ "in_predicate_value",
  /*  144 */ "boolean_value_expression",
  /*  145 */ "boolean_primary",
  /*  146 */ "common_expression",
  /*  147 */ "from_clause",
  /*  148 */ "table_reference_list",
  /*  149 */ "table_reference",
  /*  150 */ "table_primary",
  /*  151 */ "joined_table",
  /*  152 */ "alias_opt",
  /*  153 */ "parenthesized_joined_table",
  /*  154 */ "join_type",
  /*  155 */ "search_condition",
  /*  156 */ "query_specification",
  /*  157 */ "set_quantifier_opt",
  /*  158 */ "select_list",
  /*  159 */ "where_clause_opt",
  /*  160 */ "partition_by_clause_opt",
  /*  161 */ "twindow_clause_opt",
  /*  162 */ "group_by_clause_opt",
  /*  163 */ "having_clause_opt",
  /*  164 */ "select_sublist",
  /*  165 */ "select_item",
  /*  166 */ "sliding_opt",
  /*  167 */ "fill_opt",
  /*  168 */ "fill_mode",
  /*  169 */ "group_by_list",
  /*  170 */ "query_expression_body",
  /*  171 */ "order_by_clause_opt",
  /*  172 */ "slimit_clause_opt",
  /*  173 */ "limit_clause_opt",
  /*  174 */ "query_primary",
  /*  175 */ "sort_specification_list",
  /*  176 */ "sort_specification",
  /*  177 */ "ordering_specification_opt",
  /*  178 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE DATABASE exists_opt db_name db_options",
 /*   1 */ "exists_opt ::= IF NOT EXISTS",
 /*   2 */ "exists_opt ::=",
 /*   3 */ "db_options ::=",
 /*   4 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*   5 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*   6 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*   7 */ "db_options ::= db_options COMP NK_INTEGER",
 /*   8 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*   9 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  10 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  11 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  12 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  13 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  14 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  15 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  16 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  17 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  18 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  19 */ "db_options ::= db_options SINGLESTABLE NK_INTEGER",
 /*  20 */ "db_options ::= db_options STREAMMODE NK_INTEGER",
 /*  21 */ "cmd ::= USE db_name",
 /*  22 */ "cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP table_options",
 /*  23 */ "full_table_name ::= NK_ID",
 /*  24 */ "full_table_name ::= NK_ID NK_DOT NK_ID",
 /*  25 */ "column_def_list ::= column_def",
 /*  26 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  27 */ "column_def ::= column_name type_name",
 /*  28 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  29 */ "type_name ::= BOOL",
 /*  30 */ "type_name ::= TINYINT",
 /*  31 */ "type_name ::= SMALLINT",
 /*  32 */ "type_name ::= INT",
 /*  33 */ "type_name ::= INTEGER",
 /*  34 */ "type_name ::= BIGINT",
 /*  35 */ "type_name ::= FLOAT",
 /*  36 */ "type_name ::= DOUBLE",
 /*  37 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  38 */ "type_name ::= TIMESTAMP",
 /*  39 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  40 */ "type_name ::= TINYINT UNSIGNED",
 /*  41 */ "type_name ::= SMALLINT UNSIGNED",
 /*  42 */ "type_name ::= INT UNSIGNED",
 /*  43 */ "type_name ::= BIGINT UNSIGNED",
 /*  44 */ "type_name ::= JSON",
 /*  45 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  46 */ "type_name ::= MEDIUMBLOB",
 /*  47 */ "type_name ::= BLOB",
 /*  48 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  49 */ "type_name ::= DECIMAL",
 /*  50 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  51 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  52 */ "table_options ::=",
 /*  53 */ "table_options ::= table_options COMMENT NK_INTEGER",
 /*  54 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  55 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  56 */ "cmd ::= SHOW DATABASES",
 /*  57 */ "cmd ::= query_expression",
 /*  58 */ "literal ::= NK_INTEGER",
 /*  59 */ "literal ::= NK_FLOAT",
 /*  60 */ "literal ::= NK_STRING",
 /*  61 */ "literal ::= NK_BOOL",
 /*  62 */ "literal ::= TIMESTAMP NK_STRING",
 /*  63 */ "literal ::= duration_literal",
 /*  64 */ "duration_literal ::= NK_VARIABLE",
 /*  65 */ "literal_list ::= literal",
 /*  66 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  67 */ "db_name ::= NK_ID",
 /*  68 */ "table_name ::= NK_ID",
 /*  69 */ "column_name ::= NK_ID",
 /*  70 */ "function_name ::= NK_ID",
 /*  71 */ "table_alias ::= NK_ID",
 /*  72 */ "column_alias ::= NK_ID",
 /*  73 */ "expression ::= literal",
 /*  74 */ "expression ::= column_reference",
 /*  75 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  76 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  77 */ "expression ::= subquery",
 /*  78 */ "expression ::= NK_LP expression NK_RP",
 /*  79 */ "expression ::= NK_PLUS expression",
 /*  80 */ "expression ::= NK_MINUS expression",
 /*  81 */ "expression ::= expression NK_PLUS expression",
 /*  82 */ "expression ::= expression NK_MINUS expression",
 /*  83 */ "expression ::= expression NK_STAR expression",
 /*  84 */ "expression ::= expression NK_SLASH expression",
 /*  85 */ "expression ::= expression NK_REM expression",
 /*  86 */ "expression_list ::= expression",
 /*  87 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  88 */ "column_reference ::= column_name",
 /*  89 */ "column_reference ::= table_name NK_DOT column_name",
 /*  90 */ "predicate ::= expression compare_op expression",
 /*  91 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  92 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  93 */ "predicate ::= expression IS NULL",
 /*  94 */ "predicate ::= expression IS NOT NULL",
 /*  95 */ "predicate ::= expression in_op in_predicate_value",
 /*  96 */ "compare_op ::= NK_LT",
 /*  97 */ "compare_op ::= NK_GT",
 /*  98 */ "compare_op ::= NK_LE",
 /*  99 */ "compare_op ::= NK_GE",
 /* 100 */ "compare_op ::= NK_NE",
 /* 101 */ "compare_op ::= NK_EQ",
 /* 102 */ "compare_op ::= LIKE",
 /* 103 */ "compare_op ::= NOT LIKE",
 /* 104 */ "compare_op ::= MATCH",
 /* 105 */ "compare_op ::= NMATCH",
 /* 106 */ "in_op ::= IN",
 /* 107 */ "in_op ::= NOT IN",
 /* 108 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 109 */ "boolean_value_expression ::= boolean_primary",
 /* 110 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 111 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 112 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 113 */ "boolean_primary ::= predicate",
 /* 114 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 115 */ "common_expression ::= expression",
 /* 116 */ "common_expression ::= boolean_value_expression",
 /* 117 */ "from_clause ::= FROM table_reference_list",
 /* 118 */ "table_reference_list ::= table_reference",
 /* 119 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 120 */ "table_reference ::= table_primary",
 /* 121 */ "table_reference ::= joined_table",
 /* 122 */ "table_primary ::= table_name alias_opt",
 /* 123 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 124 */ "table_primary ::= subquery alias_opt",
 /* 125 */ "table_primary ::= parenthesized_joined_table",
 /* 126 */ "alias_opt ::=",
 /* 127 */ "alias_opt ::= table_alias",
 /* 128 */ "alias_opt ::= AS table_alias",
 /* 129 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 130 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 131 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 132 */ "join_type ::=",
 /* 133 */ "join_type ::= INNER",
 /* 134 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 135 */ "set_quantifier_opt ::=",
 /* 136 */ "set_quantifier_opt ::= DISTINCT",
 /* 137 */ "set_quantifier_opt ::= ALL",
 /* 138 */ "select_list ::= NK_STAR",
 /* 139 */ "select_list ::= select_sublist",
 /* 140 */ "select_sublist ::= select_item",
 /* 141 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 142 */ "select_item ::= common_expression",
 /* 143 */ "select_item ::= common_expression column_alias",
 /* 144 */ "select_item ::= common_expression AS column_alias",
 /* 145 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 146 */ "where_clause_opt ::=",
 /* 147 */ "where_clause_opt ::= WHERE search_condition",
 /* 148 */ "partition_by_clause_opt ::=",
 /* 149 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 150 */ "twindow_clause_opt ::=",
 /* 151 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 152 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 153 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 154 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 155 */ "sliding_opt ::=",
 /* 156 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 157 */ "fill_opt ::=",
 /* 158 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 159 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 160 */ "fill_mode ::= NONE",
 /* 161 */ "fill_mode ::= PREV",
 /* 162 */ "fill_mode ::= NULL",
 /* 163 */ "fill_mode ::= LINEAR",
 /* 164 */ "fill_mode ::= NEXT",
 /* 165 */ "group_by_clause_opt ::=",
 /* 166 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 167 */ "group_by_list ::= expression",
 /* 168 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 169 */ "having_clause_opt ::=",
 /* 170 */ "having_clause_opt ::= HAVING search_condition",
 /* 171 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 172 */ "query_expression_body ::= query_primary",
 /* 173 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 174 */ "query_primary ::= query_specification",
 /* 175 */ "order_by_clause_opt ::=",
 /* 176 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 177 */ "slimit_clause_opt ::=",
 /* 178 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 179 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 180 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 181 */ "limit_clause_opt ::=",
 /* 182 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 183 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 184 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 185 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 186 */ "search_condition ::= common_expression",
 /* 187 */ "sort_specification_list ::= sort_specification",
 /* 188 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 189 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 190 */ "ordering_specification_opt ::=",
 /* 191 */ "ordering_specification_opt ::= ASC",
 /* 192 */ "ordering_specification_opt ::= DESC",
 /* 193 */ "null_ordering_opt ::=",
 /* 194 */ "null_ordering_opt ::= NULLS FIRST",
 /* 195 */ "null_ordering_opt ::= NULLS LAST",
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
** second argument to NewParseAlloc() below.  This can be changed by
** putting an appropriate #define in the %include section of the input
** grammar.
*/
#ifndef YYMALLOCARGTYPE
# define YYMALLOCARGTYPE size_t
#endif

/* Initialize a new parser that has already been allocated.
*/
void NewParseInit(void *yypRawParser NewParseCTX_PDECL){
  yyParser *yypParser = (yyParser*)yypRawParser;
  NewParseCTX_STORE
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

#ifndef NewParse_ENGINEALWAYSONSTACK
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
** to NewParse and NewParseFree.
*/
void *NewParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE) NewParseCTX_PDECL){
  yyParser *yypParser;
  yypParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( yypParser ){
    NewParseCTX_STORE
    NewParseInit(yypParser NewParseCTX_PARAM);
  }
  return (void*)yypParser;
}
#endif /* NewParse_ENGINEALWAYSONSTACK */


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
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
    case 118: /* cmd */
    case 119: /* exists_opt */
    case 125: /* column_def */
    case 128: /* query_expression */
    case 129: /* literal */
    case 130: /* duration_literal */
    case 136: /* expression */
    case 137: /* column_reference */
    case 139: /* subquery */
    case 140: /* predicate */
    case 143: /* in_predicate_value */
    case 144: /* boolean_value_expression */
    case 145: /* boolean_primary */
    case 146: /* common_expression */
    case 147: /* from_clause */
    case 148: /* table_reference_list */
    case 149: /* table_reference */
    case 150: /* table_primary */
    case 151: /* joined_table */
    case 153: /* parenthesized_joined_table */
    case 155: /* search_condition */
    case 156: /* query_specification */
    case 159: /* where_clause_opt */
    case 161: /* twindow_clause_opt */
    case 163: /* having_clause_opt */
    case 165: /* select_item */
    case 166: /* sliding_opt */
    case 167: /* fill_opt */
    case 170: /* query_expression_body */
    case 172: /* slimit_clause_opt */
    case 173: /* limit_clause_opt */
    case 174: /* query_primary */
    case 176: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy172)); 
}
      break;
    case 120: /* db_name */
    case 126: /* column_name */
    case 132: /* table_name */
    case 133: /* function_name */
    case 134: /* table_alias */
    case 135: /* column_alias */
    case 152: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 121: /* db_options */
{
 tfree((yypminor->yy27)); 
}
      break;
    case 122: /* full_table_name */
{
 
}
      break;
    case 123: /* column_def_list */
{
 nodesDestroyList((yypminor->yy60)); 
}
      break;
    case 124: /* table_options */
{
 tfree((yypminor->yy40)); 
}
      break;
    case 127: /* type_name */
{
 
}
      break;
    case 131: /* literal_list */
    case 138: /* expression_list */
    case 158: /* select_list */
    case 160: /* partition_by_clause_opt */
    case 162: /* group_by_clause_opt */
    case 164: /* select_sublist */
    case 169: /* group_by_list */
    case 171: /* order_by_clause_opt */
    case 175: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy60)); 
}
      break;
    case 141: /* compare_op */
    case 142: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 154: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 157: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 168: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 177: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 178: /* null_ordering_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
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
void NewParseFinalize(void *p){
  yyParser *pParser = (yyParser*)p;
  while( pParser->yytos>pParser->yystack ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  if( pParser->yystack!=&pParser->yystk0 ) free(pParser->yystack);
#endif
}

#ifndef NewParse_ENGINEALWAYSONSTACK
/* 
** Deallocate and destroy a parser.  Destructors are called for
** all stack elements before shutting the parser down.
**
** If the YYPARSEFREENEVERNULL macro exists (for example because it
** is defined in a %include section of the input grammar) then it is
** assumed that the input pointer is never NULL.
*/
void NewParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
#ifndef YYPARSEFREENEVERNULL
  if( p==0 ) return;
#endif
  NewParseFinalize(p);
  (*freeProc)(p);
}
#endif /* NewParse_ENGINEALWAYSONSTACK */

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int NewParseStackPeak(void *p){
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
int NewParseCoverage(FILE *out){
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
   NewParseARG_FETCH
   NewParseCTX_FETCH
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
   NewParseARG_STORE /* Suppress warning about unused %extra_argument var */
   NewParseCTX_STORE
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
  NewParseTOKENTYPE yyMinor        /* The minor token to shift in */
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
  {  118,   -5 }, /* (0) cmd ::= CREATE DATABASE exists_opt db_name db_options */
  {  119,   -3 }, /* (1) exists_opt ::= IF NOT EXISTS */
  {  119,    0 }, /* (2) exists_opt ::= */
  {  121,    0 }, /* (3) db_options ::= */
  {  121,   -3 }, /* (4) db_options ::= db_options BLOCKS NK_INTEGER */
  {  121,   -3 }, /* (5) db_options ::= db_options CACHE NK_INTEGER */
  {  121,   -3 }, /* (6) db_options ::= db_options CACHELAST NK_INTEGER */
  {  121,   -3 }, /* (7) db_options ::= db_options COMP NK_INTEGER */
  {  121,   -3 }, /* (8) db_options ::= db_options DAYS NK_INTEGER */
  {  121,   -3 }, /* (9) db_options ::= db_options FSYNC NK_INTEGER */
  {  121,   -3 }, /* (10) db_options ::= db_options MAXROWS NK_INTEGER */
  {  121,   -3 }, /* (11) db_options ::= db_options MINROWS NK_INTEGER */
  {  121,   -3 }, /* (12) db_options ::= db_options KEEP NK_INTEGER */
  {  121,   -3 }, /* (13) db_options ::= db_options PRECISION NK_STRING */
  {  121,   -3 }, /* (14) db_options ::= db_options QUORUM NK_INTEGER */
  {  121,   -3 }, /* (15) db_options ::= db_options REPLICA NK_INTEGER */
  {  121,   -3 }, /* (16) db_options ::= db_options TTL NK_INTEGER */
  {  121,   -3 }, /* (17) db_options ::= db_options WAL NK_INTEGER */
  {  121,   -3 }, /* (18) db_options ::= db_options VGROUPS NK_INTEGER */
  {  121,   -3 }, /* (19) db_options ::= db_options SINGLESTABLE NK_INTEGER */
  {  121,   -3 }, /* (20) db_options ::= db_options STREAMMODE NK_INTEGER */
  {  118,   -2 }, /* (21) cmd ::= USE db_name */
  {  118,   -8 }, /* (22) cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP table_options */
  {  122,   -1 }, /* (23) full_table_name ::= NK_ID */
  {  122,   -3 }, /* (24) full_table_name ::= NK_ID NK_DOT NK_ID */
  {  123,   -1 }, /* (25) column_def_list ::= column_def */
  {  123,   -3 }, /* (26) column_def_list ::= column_def_list NK_COMMA column_def */
  {  125,   -2 }, /* (27) column_def ::= column_name type_name */
  {  125,   -4 }, /* (28) column_def ::= column_name type_name COMMENT NK_STRING */
  {  127,   -1 }, /* (29) type_name ::= BOOL */
  {  127,   -1 }, /* (30) type_name ::= TINYINT */
  {  127,   -1 }, /* (31) type_name ::= SMALLINT */
  {  127,   -1 }, /* (32) type_name ::= INT */
  {  127,   -1 }, /* (33) type_name ::= INTEGER */
  {  127,   -1 }, /* (34) type_name ::= BIGINT */
  {  127,   -1 }, /* (35) type_name ::= FLOAT */
  {  127,   -1 }, /* (36) type_name ::= DOUBLE */
  {  127,   -4 }, /* (37) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  127,   -1 }, /* (38) type_name ::= TIMESTAMP */
  {  127,   -4 }, /* (39) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  127,   -2 }, /* (40) type_name ::= TINYINT UNSIGNED */
  {  127,   -2 }, /* (41) type_name ::= SMALLINT UNSIGNED */
  {  127,   -2 }, /* (42) type_name ::= INT UNSIGNED */
  {  127,   -2 }, /* (43) type_name ::= BIGINT UNSIGNED */
  {  127,   -1 }, /* (44) type_name ::= JSON */
  {  127,   -4 }, /* (45) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  127,   -1 }, /* (46) type_name ::= MEDIUMBLOB */
  {  127,   -1 }, /* (47) type_name ::= BLOB */
  {  127,   -4 }, /* (48) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  127,   -1 }, /* (49) type_name ::= DECIMAL */
  {  127,   -4 }, /* (50) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  127,   -6 }, /* (51) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  124,    0 }, /* (52) table_options ::= */
  {  124,   -3 }, /* (53) table_options ::= table_options COMMENT NK_INTEGER */
  {  124,   -3 }, /* (54) table_options ::= table_options KEEP NK_INTEGER */
  {  124,   -3 }, /* (55) table_options ::= table_options TTL NK_INTEGER */
  {  118,   -2 }, /* (56) cmd ::= SHOW DATABASES */
  {  118,   -1 }, /* (57) cmd ::= query_expression */
  {  129,   -1 }, /* (58) literal ::= NK_INTEGER */
  {  129,   -1 }, /* (59) literal ::= NK_FLOAT */
  {  129,   -1 }, /* (60) literal ::= NK_STRING */
  {  129,   -1 }, /* (61) literal ::= NK_BOOL */
  {  129,   -2 }, /* (62) literal ::= TIMESTAMP NK_STRING */
  {  129,   -1 }, /* (63) literal ::= duration_literal */
  {  130,   -1 }, /* (64) duration_literal ::= NK_VARIABLE */
  {  131,   -1 }, /* (65) literal_list ::= literal */
  {  131,   -3 }, /* (66) literal_list ::= literal_list NK_COMMA literal */
  {  120,   -1 }, /* (67) db_name ::= NK_ID */
  {  132,   -1 }, /* (68) table_name ::= NK_ID */
  {  126,   -1 }, /* (69) column_name ::= NK_ID */
  {  133,   -1 }, /* (70) function_name ::= NK_ID */
  {  134,   -1 }, /* (71) table_alias ::= NK_ID */
  {  135,   -1 }, /* (72) column_alias ::= NK_ID */
  {  136,   -1 }, /* (73) expression ::= literal */
  {  136,   -1 }, /* (74) expression ::= column_reference */
  {  136,   -4 }, /* (75) expression ::= function_name NK_LP expression_list NK_RP */
  {  136,   -4 }, /* (76) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  136,   -1 }, /* (77) expression ::= subquery */
  {  136,   -3 }, /* (78) expression ::= NK_LP expression NK_RP */
  {  136,   -2 }, /* (79) expression ::= NK_PLUS expression */
  {  136,   -2 }, /* (80) expression ::= NK_MINUS expression */
  {  136,   -3 }, /* (81) expression ::= expression NK_PLUS expression */
  {  136,   -3 }, /* (82) expression ::= expression NK_MINUS expression */
  {  136,   -3 }, /* (83) expression ::= expression NK_STAR expression */
  {  136,   -3 }, /* (84) expression ::= expression NK_SLASH expression */
  {  136,   -3 }, /* (85) expression ::= expression NK_REM expression */
  {  138,   -1 }, /* (86) expression_list ::= expression */
  {  138,   -3 }, /* (87) expression_list ::= expression_list NK_COMMA expression */
  {  137,   -1 }, /* (88) column_reference ::= column_name */
  {  137,   -3 }, /* (89) column_reference ::= table_name NK_DOT column_name */
  {  140,   -3 }, /* (90) predicate ::= expression compare_op expression */
  {  140,   -5 }, /* (91) predicate ::= expression BETWEEN expression AND expression */
  {  140,   -6 }, /* (92) predicate ::= expression NOT BETWEEN expression AND expression */
  {  140,   -3 }, /* (93) predicate ::= expression IS NULL */
  {  140,   -4 }, /* (94) predicate ::= expression IS NOT NULL */
  {  140,   -3 }, /* (95) predicate ::= expression in_op in_predicate_value */
  {  141,   -1 }, /* (96) compare_op ::= NK_LT */
  {  141,   -1 }, /* (97) compare_op ::= NK_GT */
  {  141,   -1 }, /* (98) compare_op ::= NK_LE */
  {  141,   -1 }, /* (99) compare_op ::= NK_GE */
  {  141,   -1 }, /* (100) compare_op ::= NK_NE */
  {  141,   -1 }, /* (101) compare_op ::= NK_EQ */
  {  141,   -1 }, /* (102) compare_op ::= LIKE */
  {  141,   -2 }, /* (103) compare_op ::= NOT LIKE */
  {  141,   -1 }, /* (104) compare_op ::= MATCH */
  {  141,   -1 }, /* (105) compare_op ::= NMATCH */
  {  142,   -1 }, /* (106) in_op ::= IN */
  {  142,   -2 }, /* (107) in_op ::= NOT IN */
  {  143,   -3 }, /* (108) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  144,   -1 }, /* (109) boolean_value_expression ::= boolean_primary */
  {  144,   -2 }, /* (110) boolean_value_expression ::= NOT boolean_primary */
  {  144,   -3 }, /* (111) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  144,   -3 }, /* (112) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  145,   -1 }, /* (113) boolean_primary ::= predicate */
  {  145,   -3 }, /* (114) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  146,   -1 }, /* (115) common_expression ::= expression */
  {  146,   -1 }, /* (116) common_expression ::= boolean_value_expression */
  {  147,   -2 }, /* (117) from_clause ::= FROM table_reference_list */
  {  148,   -1 }, /* (118) table_reference_list ::= table_reference */
  {  148,   -3 }, /* (119) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  149,   -1 }, /* (120) table_reference ::= table_primary */
  {  149,   -1 }, /* (121) table_reference ::= joined_table */
  {  150,   -2 }, /* (122) table_primary ::= table_name alias_opt */
  {  150,   -4 }, /* (123) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  150,   -2 }, /* (124) table_primary ::= subquery alias_opt */
  {  150,   -1 }, /* (125) table_primary ::= parenthesized_joined_table */
  {  152,    0 }, /* (126) alias_opt ::= */
  {  152,   -1 }, /* (127) alias_opt ::= table_alias */
  {  152,   -2 }, /* (128) alias_opt ::= AS table_alias */
  {  153,   -3 }, /* (129) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  153,   -3 }, /* (130) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  151,   -6 }, /* (131) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  154,    0 }, /* (132) join_type ::= */
  {  154,   -1 }, /* (133) join_type ::= INNER */
  {  156,   -9 }, /* (134) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  157,    0 }, /* (135) set_quantifier_opt ::= */
  {  157,   -1 }, /* (136) set_quantifier_opt ::= DISTINCT */
  {  157,   -1 }, /* (137) set_quantifier_opt ::= ALL */
  {  158,   -1 }, /* (138) select_list ::= NK_STAR */
  {  158,   -1 }, /* (139) select_list ::= select_sublist */
  {  164,   -1 }, /* (140) select_sublist ::= select_item */
  {  164,   -3 }, /* (141) select_sublist ::= select_sublist NK_COMMA select_item */
  {  165,   -1 }, /* (142) select_item ::= common_expression */
  {  165,   -2 }, /* (143) select_item ::= common_expression column_alias */
  {  165,   -3 }, /* (144) select_item ::= common_expression AS column_alias */
  {  165,   -3 }, /* (145) select_item ::= table_name NK_DOT NK_STAR */
  {  159,    0 }, /* (146) where_clause_opt ::= */
  {  159,   -2 }, /* (147) where_clause_opt ::= WHERE search_condition */
  {  160,    0 }, /* (148) partition_by_clause_opt ::= */
  {  160,   -3 }, /* (149) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  161,    0 }, /* (150) twindow_clause_opt ::= */
  {  161,   -6 }, /* (151) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  161,   -4 }, /* (152) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  161,   -6 }, /* (153) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  161,   -8 }, /* (154) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  166,    0 }, /* (155) sliding_opt ::= */
  {  166,   -4 }, /* (156) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  167,    0 }, /* (157) fill_opt ::= */
  {  167,   -4 }, /* (158) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  167,   -6 }, /* (159) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  168,   -1 }, /* (160) fill_mode ::= NONE */
  {  168,   -1 }, /* (161) fill_mode ::= PREV */
  {  168,   -1 }, /* (162) fill_mode ::= NULL */
  {  168,   -1 }, /* (163) fill_mode ::= LINEAR */
  {  168,   -1 }, /* (164) fill_mode ::= NEXT */
  {  162,    0 }, /* (165) group_by_clause_opt ::= */
  {  162,   -3 }, /* (166) group_by_clause_opt ::= GROUP BY group_by_list */
  {  169,   -1 }, /* (167) group_by_list ::= expression */
  {  169,   -3 }, /* (168) group_by_list ::= group_by_list NK_COMMA expression */
  {  163,    0 }, /* (169) having_clause_opt ::= */
  {  163,   -2 }, /* (170) having_clause_opt ::= HAVING search_condition */
  {  128,   -4 }, /* (171) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  170,   -1 }, /* (172) query_expression_body ::= query_primary */
  {  170,   -4 }, /* (173) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  174,   -1 }, /* (174) query_primary ::= query_specification */
  {  171,    0 }, /* (175) order_by_clause_opt ::= */
  {  171,   -3 }, /* (176) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  172,    0 }, /* (177) slimit_clause_opt ::= */
  {  172,   -2 }, /* (178) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  172,   -4 }, /* (179) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  172,   -4 }, /* (180) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  173,    0 }, /* (181) limit_clause_opt ::= */
  {  173,   -2 }, /* (182) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  173,   -4 }, /* (183) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  173,   -4 }, /* (184) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  139,   -3 }, /* (185) subquery ::= NK_LP query_expression NK_RP */
  {  155,   -1 }, /* (186) search_condition ::= common_expression */
  {  175,   -1 }, /* (187) sort_specification_list ::= sort_specification */
  {  175,   -3 }, /* (188) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  176,   -3 }, /* (189) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  177,    0 }, /* (190) ordering_specification_opt ::= */
  {  177,   -1 }, /* (191) ordering_specification_opt ::= ASC */
  {  177,   -1 }, /* (192) ordering_specification_opt ::= DESC */
  {  178,    0 }, /* (193) null_ordering_opt ::= */
  {  178,   -2 }, /* (194) null_ordering_opt ::= NULLS FIRST */
  {  178,   -2 }, /* (195) null_ordering_opt ::= NULLS LAST */
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
  NewParseTOKENTYPE yyLookaheadToken  /* Value of the lookahead token */
  NewParseCTX_PDECL                   /* %extra_context */
){
  int yygoto;                     /* The next state */
  YYACTIONTYPE yyact;             /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  NewParseARG_FETCH
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
      case 0: /* cmd ::= CREATE DATABASE exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy259, &yymsp[-1].minor.yy105, yymsp[0].minor.yy27);}
        break;
      case 1: /* exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy259 = true; }
        break;
      case 2: /* exists_opt ::= */
{ yymsp[1].minor.yy259 = false; }
        break;
      case 3: /* db_options ::= */
{ yymsp[1].minor.yy27 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 4: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 5: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 6: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 7: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 8: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 9: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 10: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 11: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 12: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 13: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 14: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 15: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 16: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 17: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 18: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 19: /* db_options ::= db_options SINGLESTABLE NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 20: /* db_options ::= db_options STREAMMODE NK_INTEGER */
{ yylhsminor.yy27 = setDatabaseOption(pCxt, yymsp[-2].minor.yy27, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy27 = yylhsminor.yy27;
        break;
      case 21: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy105);}
        break;
      case 22: /* cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP table_options */
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-5].minor.yy259, &yymsp[-4].minor.yy111, yymsp[-2].minor.yy60, yymsp[0].minor.yy40);}
        break;
      case 23: /* full_table_name ::= NK_ID */
{ STokenPair t = { .first = yymsp[0].minor.yy0, .second = nil_token}; yylhsminor.yy111 = t; }
  yymsp[0].minor.yy111 = yylhsminor.yy111;
        break;
      case 24: /* full_table_name ::= NK_ID NK_DOT NK_ID */
{ STokenPair t = { .first = yymsp[-2].minor.yy0, .second = yymsp[0].minor.yy0}; yylhsminor.yy111 = t; }
  yymsp[-2].minor.yy111 = yylhsminor.yy111;
        break;
      case 25: /* column_def_list ::= column_def */
{ yylhsminor.yy60 = createNodeList(pCxt, yymsp[0].minor.yy172); }
  yymsp[0].minor.yy60 = yylhsminor.yy60;
        break;
      case 26: /* column_def_list ::= column_def_list NK_COMMA column_def */
{ yylhsminor.yy60 = addNodeToList(pCxt, yymsp[-2].minor.yy60, yymsp[0].minor.yy172); }
  yymsp[-2].minor.yy60 = yylhsminor.yy60;
        break;
      case 27: /* column_def ::= column_name type_name */
{ yylhsminor.yy172 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy105, yymsp[0].minor.yy248, NULL); }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 28: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy172 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy105, yymsp[-2].minor.yy248, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 29: /* type_name ::= BOOL */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 30: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 31: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 32: /* type_name ::= INT */
      case 33: /* type_name ::= INTEGER */ yytestcase(yyruleno==33);
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 34: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 35: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 36: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 37: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy248 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 38: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 39: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy248 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 40: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy248 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 41: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy248 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 42: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy248 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 43: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy248 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 44: /* type_name ::= JSON */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 45: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy248 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 46: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 47: /* type_name ::= BLOB */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 48: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy248 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 49: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy248 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 50: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy248 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 51: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy248 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 52: /* table_options ::= */
{ yymsp[1].minor.yy40 = createDefaultTableOptions(pCxt);}
        break;
      case 53: /* table_options ::= table_options COMMENT NK_INTEGER */
{ yylhsminor.yy40 = setTableOption(pCxt, yymsp[-2].minor.yy40, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 54: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy40 = setTableOption(pCxt, yymsp[-2].minor.yy40, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 55: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy40 = setTableOption(pCxt, yymsp[-2].minor.yy40, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy40 = yylhsminor.yy40;
        break;
      case 56: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASE_STMT); }
        break;
      case 57: /* cmd ::= query_expression */
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy172; }
        break;
      case 58: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 59: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 60: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 61: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 62: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 63: /* literal ::= duration_literal */
      case 73: /* expression ::= literal */ yytestcase(yyruleno==73);
      case 74: /* expression ::= column_reference */ yytestcase(yyruleno==74);
      case 77: /* expression ::= subquery */ yytestcase(yyruleno==77);
      case 109: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==109);
      case 113: /* boolean_primary ::= predicate */ yytestcase(yyruleno==113);
      case 118: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==118);
      case 120: /* table_reference ::= table_primary */ yytestcase(yyruleno==120);
      case 121: /* table_reference ::= joined_table */ yytestcase(yyruleno==121);
      case 125: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==125);
      case 172: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==172);
      case 174: /* query_primary ::= query_specification */ yytestcase(yyruleno==174);
{ PARSER_TRACE; yylhsminor.yy172 = yymsp[0].minor.yy172; }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 64: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 65: /* literal_list ::= literal */
      case 86: /* expression_list ::= expression */ yytestcase(yyruleno==86);
{ PARSER_TRACE; yylhsminor.yy60 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy172)); }
  yymsp[0].minor.yy60 = yylhsminor.yy60;
        break;
      case 66: /* literal_list ::= literal_list NK_COMMA literal */
      case 87: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==87);
{ PARSER_TRACE; yylhsminor.yy60 = addNodeToList(pCxt, yymsp[-2].minor.yy60, releaseRawExprNode(pCxt, yymsp[0].minor.yy172)); }
  yymsp[-2].minor.yy60 = yylhsminor.yy60;
        break;
      case 67: /* db_name ::= NK_ID */
      case 68: /* table_name ::= NK_ID */ yytestcase(yyruleno==68);
      case 69: /* column_name ::= NK_ID */ yytestcase(yyruleno==69);
      case 70: /* function_name ::= NK_ID */ yytestcase(yyruleno==70);
      case 71: /* table_alias ::= NK_ID */ yytestcase(yyruleno==71);
      case 72: /* column_alias ::= NK_ID */ yytestcase(yyruleno==72);
{ PARSER_TRACE; yylhsminor.yy105 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy105 = yylhsminor.yy105;
        break;
      case 75: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy105, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy105, yymsp[-1].minor.yy60)); }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 76: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy105, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy105, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 78: /* expression ::= NK_LP expression NK_RP */
      case 114: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==114);
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy172)); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 79: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy172));
                                                                                  }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 80: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy172), NULL));
                                                                                  }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 81: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); 
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 82: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); 
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 83: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); 
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 84: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); 
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 85: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); 
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 88: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNode(pCxt, &yymsp[0].minor.yy105, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy105)); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 89: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy105, &yymsp[0].minor.yy105, createColumnNode(pCxt, &yymsp[-2].minor.yy105, &yymsp[0].minor.yy105)); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 90: /* predicate ::= expression compare_op expression */
      case 95: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==95);
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy214, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172)));
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 91: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy172), releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172)));
                                                                                  }
  yymsp[-4].minor.yy172 = yylhsminor.yy172;
        break;
      case 92: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[-5].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172)));
                                                                                  }
  yymsp[-5].minor.yy172 = yylhsminor.yy172;
        break;
      case 93: /* predicate ::= expression IS NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), NULL));
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 94: /* predicate ::= expression IS NOT NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy172), NULL));
                                                                                  }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 96: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_LOWER_THAN; }
        break;
      case 97: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_GREATER_THAN; }
        break;
      case 98: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 99: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 100: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_NOT_EQUAL; }
        break;
      case 101: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_EQUAL; }
        break;
      case 102: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_LIKE; }
        break;
      case 103: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy214 = OP_TYPE_NOT_LIKE; }
        break;
      case 104: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_MATCH; }
        break;
      case 105: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_NMATCH; }
        break;
      case 106: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy214 = OP_TYPE_IN; }
        break;
      case 107: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy214 = OP_TYPE_NOT_IN; }
        break;
      case 108: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy60)); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 110: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy172), NULL));
                                                                                  }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 111: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172)));
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 112: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy172);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), releaseRawExprNode(pCxt, yymsp[0].minor.yy172)));
                                                                                  }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 115: /* common_expression ::= expression */
      case 116: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==116);
{ yylhsminor.yy172 = yymsp[0].minor.yy172; }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 117: /* from_clause ::= FROM table_reference_list */
      case 147: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==147);
      case 170: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==170);
{ PARSER_TRACE; yymsp[-1].minor.yy172 = yymsp[0].minor.yy172; }
        break;
      case 119: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy172 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy172, yymsp[0].minor.yy172, NULL); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 122: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy172 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy105, &yymsp[0].minor.yy105); }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 123: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy172 = createRealTableNode(pCxt, &yymsp[-3].minor.yy105, &yymsp[-1].minor.yy105, &yymsp[0].minor.yy105); }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 124: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy172 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy172), &yymsp[0].minor.yy105); }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 126: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy105 = nil_token;  }
        break;
      case 127: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy105 = yymsp[0].minor.yy105; }
  yymsp[0].minor.yy105 = yylhsminor.yy105;
        break;
      case 128: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy105 = yymsp[0].minor.yy105; }
        break;
      case 129: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 130: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==130);
{ PARSER_TRACE; yymsp[-2].minor.yy172 = yymsp[-1].minor.yy172; }
        break;
      case 131: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy172 = createJoinTableNode(pCxt, yymsp[-4].minor.yy278, yymsp[-5].minor.yy172, yymsp[-2].minor.yy172, yymsp[0].minor.yy172); }
  yymsp[-5].minor.yy172 = yylhsminor.yy172;
        break;
      case 132: /* join_type ::= */
{ PARSER_TRACE; yymsp[1].minor.yy278 = JOIN_TYPE_INNER; }
        break;
      case 133: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy278 = JOIN_TYPE_INNER; }
        break;
      case 134: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy172 = createSelectStmt(pCxt, yymsp[-7].minor.yy259, yymsp[-6].minor.yy60, yymsp[-5].minor.yy172);
                                                                                    yymsp[-8].minor.yy172 = addWhereClause(pCxt, yymsp[-8].minor.yy172, yymsp[-4].minor.yy172);
                                                                                    yymsp[-8].minor.yy172 = addPartitionByClause(pCxt, yymsp[-8].minor.yy172, yymsp[-3].minor.yy60);
                                                                                    yymsp[-8].minor.yy172 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy172, yymsp[-2].minor.yy172);
                                                                                    yymsp[-8].minor.yy172 = addGroupByClause(pCxt, yymsp[-8].minor.yy172, yymsp[-1].minor.yy60);
                                                                                    yymsp[-8].minor.yy172 = addHavingClause(pCxt, yymsp[-8].minor.yy172, yymsp[0].minor.yy172);
                                                                                  }
        break;
      case 135: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy259 = false; }
        break;
      case 136: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy259 = true; }
        break;
      case 137: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy259 = false; }
        break;
      case 138: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy60 = NULL; }
        break;
      case 139: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy60 = yymsp[0].minor.yy60; }
  yymsp[0].minor.yy60 = yylhsminor.yy60;
        break;
      case 140: /* select_sublist ::= select_item */
      case 187: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==187);
{ PARSER_TRACE; yylhsminor.yy60 = createNodeList(pCxt, yymsp[0].minor.yy172); }
  yymsp[0].minor.yy60 = yylhsminor.yy60;
        break;
      case 141: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 188: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==188);
{ PARSER_TRACE; yylhsminor.yy60 = addNodeToList(pCxt, yymsp[-2].minor.yy60, yymsp[0].minor.yy172); }
  yymsp[-2].minor.yy60 = yylhsminor.yy60;
        break;
      case 142: /* select_item ::= common_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy172);
                                                                                    yylhsminor.yy172 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy172), &t);
                                                                                  }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 143: /* select_item ::= common_expression column_alias */
{ PARSER_TRACE; yylhsminor.yy172 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy172), &yymsp[0].minor.yy105); }
  yymsp[-1].minor.yy172 = yylhsminor.yy172;
        break;
      case 144: /* select_item ::= common_expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy172 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), &yymsp[0].minor.yy105); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 145: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy172 = createColumnNode(pCxt, &yymsp[-2].minor.yy105, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 146: /* where_clause_opt ::= */
      case 150: /* twindow_clause_opt ::= */ yytestcase(yyruleno==150);
      case 155: /* sliding_opt ::= */ yytestcase(yyruleno==155);
      case 157: /* fill_opt ::= */ yytestcase(yyruleno==157);
      case 169: /* having_clause_opt ::= */ yytestcase(yyruleno==169);
      case 177: /* slimit_clause_opt ::= */ yytestcase(yyruleno==177);
      case 181: /* limit_clause_opt ::= */ yytestcase(yyruleno==181);
{ PARSER_TRACE; yymsp[1].minor.yy172 = NULL; }
        break;
      case 148: /* partition_by_clause_opt ::= */
      case 165: /* group_by_clause_opt ::= */ yytestcase(yyruleno==165);
      case 175: /* order_by_clause_opt ::= */ yytestcase(yyruleno==175);
{ PARSER_TRACE; yymsp[1].minor.yy60 = NULL; }
        break;
      case 149: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 166: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==166);
      case 176: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==176);
{ PARSER_TRACE; yymsp[-2].minor.yy60 = yymsp[0].minor.yy60; }
        break;
      case 151: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy172 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy172), &yymsp[-1].minor.yy0); }
        break;
      case 152: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy172 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy172)); }
        break;
      case 153: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy172 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy172, NULL, yymsp[-1].minor.yy172, yymsp[0].minor.yy172); }
        break;
      case 154: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy172 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy172, yymsp[-3].minor.yy172, yymsp[-1].minor.yy172, yymsp[0].minor.yy172); }
        break;
      case 156: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy172 = yymsp[-1].minor.yy172; }
        break;
      case 158: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy172 = createFillNode(pCxt, yymsp[-1].minor.yy202, NULL); }
        break;
      case 159: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy172 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy60)); }
        break;
      case 160: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy202 = FILL_MODE_NONE; }
        break;
      case 161: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy202 = FILL_MODE_PREV; }
        break;
      case 162: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy202 = FILL_MODE_NULL; }
        break;
      case 163: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy202 = FILL_MODE_LINEAR; }
        break;
      case 164: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy202 = FILL_MODE_NEXT; }
        break;
      case 167: /* group_by_list ::= expression */
{ PARSER_TRACE; yylhsminor.yy60 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); }
  yymsp[0].minor.yy60 = yylhsminor.yy60;
        break;
      case 168: /* group_by_list ::= group_by_list NK_COMMA expression */
{ PARSER_TRACE; yylhsminor.yy60 = addNodeToList(pCxt, yymsp[-2].minor.yy60, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy172))); }
  yymsp[-2].minor.yy60 = yylhsminor.yy60;
        break;
      case 171: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy172 = addOrderByClause(pCxt, yymsp[-3].minor.yy172, yymsp[-2].minor.yy60);
                                                                                    yylhsminor.yy172 = addSlimitClause(pCxt, yylhsminor.yy172, yymsp[-1].minor.yy172);
                                                                                    yylhsminor.yy172 = addLimitClause(pCxt, yylhsminor.yy172, yymsp[0].minor.yy172);
                                                                                  }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 173: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy172 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy172, yymsp[0].minor.yy172); }
  yymsp[-3].minor.yy172 = yylhsminor.yy172;
        break;
      case 178: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 182: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==182);
{ PARSER_TRACE; yymsp[-1].minor.yy172 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 179: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 183: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==183);
{ PARSER_TRACE; yymsp[-3].minor.yy172 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 180: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 184: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==184);
{ PARSER_TRACE; yymsp[-3].minor.yy172 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 185: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy172 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy172); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 186: /* search_condition ::= common_expression */
{ PARSER_TRACE; yylhsminor.yy172 = releaseRawExprNode(pCxt, yymsp[0].minor.yy172); }
  yymsp[0].minor.yy172 = yylhsminor.yy172;
        break;
      case 189: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy172 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy172), yymsp[-1].minor.yy14, yymsp[0].minor.yy17); }
  yymsp[-2].minor.yy172 = yylhsminor.yy172;
        break;
      case 190: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy14 = ORDER_ASC; }
        break;
      case 191: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy14 = ORDER_ASC; }
        break;
      case 192: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy14 = ORDER_DESC; }
        break;
      case 193: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy17 = NULL_ORDER_DEFAULT; }
        break;
      case 194: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy17 = NULL_ORDER_FIRST; }
        break;
      case 195: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy17 = NULL_ORDER_LAST; }
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
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  NewParseTOKENTYPE yyminor         /* The minor type of the error token */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
  
  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > pCxt->pQueryCxt->msgLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        sprintf(pCxt->pQueryCxt->pMsg, msg, tmpstr);
    } else {
        sprintf(pCxt->pQueryCxt->pMsg, msg, &TOKEN.z[0]);
    }
  } else {
    sprintf(pCxt->pQueryCxt->pMsg, "Incomplete SQL statement");
  }
  pCxt->valid = false;
/************ End %syntax_error code ******************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  NewParseARG_FETCH
  NewParseCTX_FETCH
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
 PARSER_COMPLETE; 
/*********** End %parse_accept code *******************************************/
  NewParseARG_STORE /* Suppress warning about unused %extra_argument variable */
  NewParseCTX_STORE
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "NewParseAlloc" which describes the current state of the parser.
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
void NewParse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  NewParseTOKENTYPE yyminor       /* The value for the token */
  NewParseARG_PDECL               /* Optional %extra_argument parameter */
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
  NewParseCTX_FETCH
  NewParseARG_STORE

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
                        yyminor NewParseCTX_PARAM);
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
int NewParseFallback(int iToken){
#ifdef YYFALLBACK
  if( iToken<(int)(sizeof(yyFallback)/sizeof(yyFallback[0])) ){
    return yyFallback[iToken];
  }
#else
  (void)iToken;
#endif
  return 0;
}
