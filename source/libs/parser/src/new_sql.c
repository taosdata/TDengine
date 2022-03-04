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
#define YYNOCODE 180
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
  EOperatorType yy20;
  STableOptions* yy46;
  EFillMode yy54;
  STokenPair yy57;
  SNodeList* yy64;
  bool yy137;
  SDatabaseOptions* yy199;
  SToken yy209;
  ENullOrder yy217;
  EOrder yy218;
  EJoinType yy252;
  SNode* yy272;
  SDataType yy304;
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
#define YYNSTATE             212
#define YYNRULE              197
#define YYNTOKEN             119
#define YY_MAX_SHIFT         211
#define YY_MIN_SHIFTREDUCE   353
#define YY_MAX_SHIFTREDUCE   549
#define YY_ERROR_ACTION      550
#define YY_ACCEPT_ACTION     551
#define YY_NO_ACTION         552
#define YY_MIN_REDUCE        553
#define YY_MAX_REDUCE        749
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
#define YY_ACTTAB_COUNT (871)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   619,  617,  105,  188,  117,  728,  578,   48,   23,   75,
 /*    10 */    76,  642,   30,   28,   26,   25,   24,  158,  126,  727,
 /*    20 */   681,  100,  129,  726,  681,   30,   28,   26,   25,   24,
 /*    30 */   553,  620,  617,  642,  100,   68,  627,  617,  678,  158,
 /*    40 */   159,  499,  677,   42,  628,  432,  631,  667,   26,   25,
 /*    50 */    24,  666,  663,  211,  681,  210,  209,  208,  207,  206,
 /*    60 */   205,  204,  203,  202,  697,  201,  200,  199,  198,  197,
 /*    70 */   196,  195,  676,  423,   22,  109,  141,  450,  451,  452,
 /*    80 */   453,  454,  455,  456,  458,  459,  460,   22,  109,  134,
 /*    90 */   450,  451,  452,  453,  454,  455,  456,  458,  459,  460,
 /*   100 */    10,   10,  143,  143,  382,  186,  185,  184,  386,  183,
 /*   110 */   388,  389,  182,  391,  179,  118,  397,  176,  399,  400,
 /*   120 */   173,  170,  642,  575,  148,  627,  617,   84,  158,  159,
 /*   130 */     9,    8,   40,  628,   58,  631,  667,    6,  487,   80,
 /*   140 */    99,  663,  423,  642,  579,   48,  627,  617,   58,  144,
 /*   150 */   159,   82,  728,   41,  628,  190,  631,  667,  642,   20,
 /*   160 */   189,  107,  663,   52,  158,  115,   57,  643,  457,  133,
 /*   170 */   726,  461,  191,  642,  468,   74,  627,  617,  151,  144,
 /*   180 */   159,  119,  694,   41,  628,  491,  631,  667,  426,   58,
 /*   190 */   551,  107,  663,   52,   78,  642,  425,  426,  627,  617,
 /*   200 */   161,  158,  159,  155,  513,   41,  628,   43,  631,  667,
 /*   210 */     9,    8,  695,  107,  663,  740,  642,    2,   65,  627,
 /*   220 */   617,   62,  158,  159,  701,  152,   41,  628,  728,  631,
 /*   230 */   667,  135,  130,  128,  107,  663,  740,  642,   73,   59,
 /*   240 */   627,  617,   57,  158,  159,  724,  726,   41,  628,  698,
 /*   250 */   631,  667,  146,   29,   27,  107,  663,  740,   29,   27,
 /*   260 */   492,  686,   11,  487,  127,  412,  685,   11,  156,  462,
 /*   270 */   412,  708,   31,  429,  490,  414,   31,  548,  549,  624,
 /*   280 */   414,   60,  622,  403,  150,    1,  168,  114,  153,  124,
 /*   290 */     1,  642,  114,  148,  627,  617,  122,  158,  159,  418,
 /*   300 */   160,   90,  628,   85,  631,  160,   47,  447,   29,   27,
 /*   310 */   123,  707,  413,  415,  418,   29,   27,  413,  415,  418,
 /*   320 */   412,  728,  163,  162,   11,   64,  106,  412,    5,  137,
 /*   330 */   414,  688,   67,   58,  121,   57,    4,  414,   69,  726,
 /*   340 */     7,   51,  114,  422,   45,  487,  425,    1,  682,  114,
 /*   350 */   642,   32,   70,  627,  617,  160,  158,  159,   16,  649,
 /*   360 */    49,  628,  160,  631,  110,  743,  725,  413,  415,  418,
 /*   370 */   154,  157,  140,   77,  413,  415,  418,  421,  164,  642,
 /*   380 */   148,  166,  627,  617,   46,  158,  159,  194,   58,   42,
 /*   390 */   628,   44,  631,  667,  192,   29,   27,  145,  663,  149,
 /*   400 */   741,   71,  674,  139,   81,  138,   86,  412,  728,   83,
 /*   410 */    87,   29,   27,  147,  412,   98,    3,  414,   31,   14,
 /*   420 */    61,  510,   57,  412,  414,   63,  726,    1,  642,  114,
 /*   430 */    35,  627,  617,  414,  158,  159,  512,   50,   96,  628,
 /*   440 */   113,  631,  160,    7,   66,  114,  506,  505,   36,  160,
 /*   450 */   131,   37,   15,  132,  413,  415,  418,  622,  160,   18,
 /*   460 */   484,  413,  415,  418,  483,   34,   33,   72,   29,   27,
 /*   470 */   413,  415,  418,    8,  621,  642,   56,  534,  627,  617,
 /*   480 */   412,  158,  159,  430,  448,   42,  628,  539,  631,  667,
 /*   490 */   414,   17,   12,   38,  664,   30,   28,   26,   25,   24,
 /*   500 */     7,  533,  114,  111,  538,  537,  112,  642,   79,   13,
 /*   510 */   627,  617,  416,  158,  159,  160,  611,   96,  628,  120,
 /*   520 */   631,  610,  609,  574,  167,  396,  116,  413,  415,  418,
 /*   530 */   642,  377,  171,  627,  617,  165,  158,  159,  404,  169,
 /*   540 */    92,  628,  401,  631,  398,  642,  381,  172,  627,  617,
 /*   550 */   175,  158,  159,  178,  174,   96,  628,  108,  631,  642,
 /*   560 */   177,  180,  627,  617,  392,  158,  159,  408,  181,   49,
 /*   570 */   628,  642,  631,  136,  627,  617,  390,  158,  159,  395,
 /*   580 */   407,   91,  628,  187,  631,  406,  642,  394,  393,  627,
 /*   590 */   617,   39,  158,  159,  354,  193,   93,  628,  366,  631,
 /*   600 */   642,  373,  372,  627,  617,  371,  158,  159,  370,  742,
 /*   610 */    88,  628,  369,  631,  642,  552,  368,  627,  617,  367,
 /*   620 */   158,  159,  365,  364,   94,  628,  363,  631,  642,  552,
 /*   630 */   362,  627,  617,  361,  158,  159,  360,  359,   89,  628,
 /*   640 */   552,  631,  642,  552,  358,  627,  617,  357,  158,  159,
 /*   650 */   552,  552,   95,  628,  552,  631,  642,  552,  552,  627,
 /*   660 */   617,  552,  158,  159,  552,  552,  639,  628,  642,  631,
 /*   670 */   552,  627,  617,  552,  158,  159,  552,  552,  638,  628,
 /*   680 */   642,  631,  552,  627,  617,  552,  158,  159,  552,  552,
 /*   690 */   637,  628,  552,  631,  642,  552,  552,  627,  617,  552,
 /*   700 */   158,  159,  552,  552,  103,  628,  642,  631,  552,  627,
 /*   710 */   617,  552,  158,  159,  552,  552,  102,  628,  642,  631,
 /*   720 */   552,  627,  617,  552,  158,  159,  552,  552,  104,  628,
 /*   730 */   552,  631,  642,  552,  552,  627,  617,  552,  158,  159,
 /*   740 */   552,  552,  101,  628,  552,  631,  642,  552,  516,  627,
 /*   750 */   617,   19,  158,  159,  140,  552,   97,  628,  552,  631,
 /*   760 */   140,   30,   28,   26,   25,   24,   46,   30,   28,   26,
 /*   770 */    25,   24,   46,   44,  125,  514,  515,  517,  518,   44,
 /*   780 */   552,  552,  142,   53,  674,  675,  552,  679,  140,   54,
 /*   790 */   674,  675,   21,  679,  552,   30,   28,   26,   25,   24,
 /*   800 */    46,  552,   30,   28,   26,   25,   24,   44,  552,  552,
 /*   810 */   552,  552,  552,  552,  552,  552,  552,   55,  674,  675,
 /*   820 */   552,  679,  552,  552,  552,  552,  552,  552,  432,  552,
 /*   830 */   552,  552,  552,  552,  552,  552,  552,  552,  552,  552,
 /*   840 */   552,  552,  552,  552,  552,  552,  552,  552,  552,  552,
 /*   850 */   552,  552,  552,  552,  552,  552,  552,  552,  552,  552,
 /*   860 */   552,  552,  552,  552,  552,  552,  552,  552,  552,  545,
 /*   870 */   546,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */   130,  131,  132,  128,  124,  157,  126,  127,  142,  143,
 /*    10 */   178,  127,   12,   13,   14,   15,   16,  133,  169,  171,
 /*    20 */   135,   21,  138,  175,  135,   12,   13,   14,   15,   16,
 /*    30 */     0,  130,  131,  127,   21,  162,  130,  131,  153,  133,
 /*    40 */   134,   14,  153,  137,  138,   45,  140,  141,   14,   15,
 /*    50 */    16,  145,  146,   23,  135,   25,   26,   27,   28,   29,
 /*    60 */    30,   31,   32,   33,  136,   35,   36,   37,   38,   39,
 /*    70 */    40,   41,  153,   46,   74,   75,  155,   77,   78,   79,
 /*    80 */    80,   81,   82,   83,   84,   85,   86,   74,   75,   46,
 /*    90 */    77,   78,   79,   80,   81,   82,   83,   84,   85,   86,
 /*   100 */    44,   44,   46,   46,   50,   51,   52,   53,   54,   55,
 /*   110 */    56,   57,   58,   59,   60,   18,   62,   63,   64,   65,
 /*   120 */    66,   67,  127,    0,  129,  130,  131,   19,  133,  134,
 /*   130 */     1,    2,  137,  138,   92,  140,  141,   90,   91,   42,
 /*   140 */   145,  146,   46,  127,  126,  127,  130,  131,   92,  133,
 /*   150 */   134,   43,  157,  137,  138,   32,  140,  141,  127,   74,
 /*   160 */    37,  145,  146,  147,  133,   68,  171,  127,   83,  138,
 /*   170 */   175,   86,   49,  127,   45,  159,  130,  131,    3,  133,
 /*   180 */   134,  165,  166,  137,  138,    4,  140,  141,   46,   92,
 /*   190 */   119,  145,  146,  147,  172,  127,   46,   46,  130,  131,
 /*   200 */   129,  133,  134,   48,   45,  137,  138,   48,  140,  141,
 /*   210 */     1,    2,  166,  145,  146,  147,  127,  158,   45,  130,
 /*   220 */   131,   48,  133,  134,  156,   48,  137,  138,  157,  140,
 /*   230 */   141,   97,   98,   99,  145,  146,  147,  127,   88,   88,
 /*   240 */   130,  131,  171,  133,  134,  156,  175,  137,  138,  136,
 /*   250 */   140,  141,   21,   12,   13,  145,  146,  147,   12,   13,
 /*   260 */    14,   89,   21,   91,  101,   24,  156,   21,  113,   45,
 /*   270 */    24,  168,   48,   45,   93,   34,   48,  117,  118,   44,
 /*   280 */    34,  167,   47,   45,  109,   44,   48,   46,  111,  100,
 /*   290 */    44,  127,   46,  129,  130,  131,  131,  133,  134,   73,
 /*   300 */    59,  137,  138,   45,  140,   59,   48,   76,   12,   13,
 /*   310 */   131,  168,   71,   72,   73,   12,   13,   71,   72,   73,
 /*   320 */    24,  157,   69,   70,   21,  167,  131,   24,  108,  107,
 /*   330 */    34,  164,  163,   92,   95,  171,   94,   34,  160,  175,
 /*   340 */    44,  161,   46,   46,  133,   91,   46,   44,  135,   46,
 /*   350 */   127,   87,  148,  130,  131,   59,  133,  134,   44,  144,
 /*   360 */   137,  138,   59,  140,  116,  179,  174,   71,   72,   73,
 /*   370 */   110,  112,  121,  173,   71,   72,   73,   46,  121,  127,
 /*   380 */   129,   46,  130,  131,  133,  133,  134,   20,   92,  137,
 /*   390 */   138,  140,  140,  141,  123,   12,   13,  145,  146,  176,
 /*   400 */   177,  150,  151,  152,  120,  154,  121,   24,  157,  120,
 /*   410 */   122,   12,   13,   14,   24,  125,   48,   34,   48,   96,
 /*   420 */    45,   45,  171,   24,   34,   44,  175,   44,  127,   46,
 /*   430 */    48,  130,  131,   34,  133,  134,   45,   44,  137,  138,
 /*   440 */   139,  140,   59,   44,   44,   46,   45,   45,   44,   59,
 /*   450 */    24,   44,   96,   48,   71,   72,   73,   47,   59,   48,
 /*   460 */    45,   71,   72,   73,   45,   48,   89,   47,   12,   13,
 /*   470 */    71,   72,   73,    2,   47,  127,   47,   24,  130,  131,
 /*   480 */    24,  133,  134,   45,   76,  137,  138,   45,  140,  141,
 /*   490 */    34,   48,   96,    4,  146,   12,   13,   14,   15,   16,
 /*   500 */    44,   24,   46,   24,   24,   24,   24,  127,   47,   44,
 /*   510 */   130,  131,   34,  133,  134,   59,    0,  137,  138,  139,
 /*   520 */   140,    0,    0,    0,   24,   61,   24,   71,   72,   73,
 /*   530 */   127,   46,   24,  130,  131,   47,  133,  134,   45,   44,
 /*   540 */   137,  138,   45,  140,   45,  127,   34,   44,  130,  131,
 /*   550 */    44,  133,  134,   44,   24,  137,  138,  139,  140,  127,
 /*   560 */    24,   24,  130,  131,   45,  133,  134,   24,   44,  137,
 /*   570 */   138,  127,  140,  170,  130,  131,   45,  133,  134,   61,
 /*   580 */    24,  137,  138,   49,  140,   24,  127,   61,   61,  130,
 /*   590 */   131,   44,  133,  134,   22,   21,  137,  138,   34,  140,
 /*   600 */   127,   24,   24,  130,  131,   24,  133,  134,   24,  177,
 /*   610 */   137,  138,   24,  140,  127,  180,   24,  130,  131,   24,
 /*   620 */   133,  134,   24,   24,  137,  138,   24,  140,  127,  180,
 /*   630 */    24,  130,  131,   24,  133,  134,   24,   24,  137,  138,
 /*   640 */   180,  140,  127,  180,   24,  130,  131,   24,  133,  134,
 /*   650 */   180,  180,  137,  138,  180,  140,  127,  180,  180,  130,
 /*   660 */   131,  180,  133,  134,  180,  180,  137,  138,  127,  140,
 /*   670 */   180,  130,  131,  180,  133,  134,  180,  180,  137,  138,
 /*   680 */   127,  140,  180,  130,  131,  180,  133,  134,  180,  180,
 /*   690 */   137,  138,  180,  140,  127,  180,  180,  130,  131,  180,
 /*   700 */   133,  134,  180,  180,  137,  138,  127,  140,  180,  130,
 /*   710 */   131,  180,  133,  134,  180,  180,  137,  138,  127,  140,
 /*   720 */   180,  130,  131,  180,  133,  134,  180,  180,  137,  138,
 /*   730 */   180,  140,  127,  180,  180,  130,  131,  180,  133,  134,
 /*   740 */   180,  180,  137,  138,  180,  140,  127,  180,   76,  130,
 /*   750 */   131,    2,  133,  134,  121,  180,  137,  138,  180,  140,
 /*   760 */   121,   12,   13,   14,   15,   16,  133,   12,   13,   14,
 /*   770 */    15,   16,  133,  140,  102,  103,  104,  105,  106,  140,
 /*   780 */   180,  180,  149,  150,  151,  152,  180,  154,  121,  150,
 /*   790 */   151,  152,    2,  154,  180,   12,   13,   14,   15,   16,
 /*   800 */   133,  180,   12,   13,   14,   15,   16,  140,  180,  180,
 /*   810 */   180,  180,  180,  180,  180,  180,  180,  150,  151,  152,
 /*   820 */   180,  154,  180,  180,  180,  180,  180,  180,   45,  180,
 /*   830 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   840 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   850 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   860 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  114,
 /*   870 */   115,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   880 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   890 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   900 */   180,  180,  180,  180,  180,  180,  180,  180,  180,  180,
 /*   910 */   180,  180,  180,  180,  180,
};
#define YY_SHIFT_COUNT    (211)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (790)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */    97,  241,  246,  303,  303,  303,  303,  296,  303,  303,
 /*    10 */    56,  383,  456,  399,  456,  456,  456,  456,  456,  456,
 /*    20 */   456,  456,  456,  456,  456,  456,  456,  456,  456,  456,
 /*    30 */   456,  456,   57,   57,   57,  390,   43,   43,   42,   96,
 /*    40 */     0,   13,   13,  390,  150,  150,  150,   96,   54,  755,
 /*    50 */   672,  134,  151,  172,   47,  172,   27,  175,  181,  142,
 /*    60 */   163,  189,  226,  226,  163,  189,  226,  220,  222,  239,
 /*    70 */   242,  254,  297,  300,  264,  314,  248,  259,  260,   96,
 /*    80 */   331,  335,  367,  331,  367,  871,  871,   30,  749,  790,
 /*    90 */   783,  483,  483,  483,  483,  483,  483,  483,  123,  129,
 /*   100 */    85,   34,   34,   34,   34,  159,  173,  209,  224,  231,
 /*   110 */   160,  177,  155,  228,  235,  253,  238,  258,  108,  368,
 /*   120 */   370,  323,  375,  376,  381,  382,  391,  393,  400,  401,
 /*   130 */   404,  402,  426,  405,  410,  407,  411,  356,  415,  419,
 /*   140 */   420,  377,  417,  427,  429,  471,  408,  438,  442,  443,
 /*   150 */   396,  489,  453,  477,  479,  480,  481,  482,  461,  465,
 /*   160 */   478,  516,  521,  522,  523,  485,  488,  493,  500,  502,
 /*   170 */   495,  497,  508,  503,  499,  530,  506,  519,  536,  509,
 /*   180 */   531,  537,  524,  464,  518,  526,  527,  512,  534,  543,
 /*   190 */   556,  561,  547,  572,  574,  577,  578,  581,  584,  588,
 /*   200 */   592,  595,  564,  598,  599,  602,  606,  609,  612,  613,
 /*   210 */   620,  623,
};
#define YY_REDUCE_COUNT (86)
#define YY_REDUCE_MIN   (-168)
#define YY_REDUCE_MAX   (667)
static const short yy_reduce_ofst[] = {
 /*     0 */    71,   -5,   16,   46,   68,   89,  110,  164,  -94,  252,
 /*    10 */   251,  348,  223,  301,  380,  403,  418,  432,  444,  459,
 /*    20 */   473,  487,  501,  515,  529,  541,  553,  567,  579,  591,
 /*    30 */   605,  619,  633,  639,  667, -130, -116,   31, -152, -120,
 /*    40 */  -134, -134, -134,  -99, -115, -111,  -81,   18, -125, -168,
 /*    50 */  -151, -127,  -72,  -79,  -79,  -79,   40,   22,   59,  113,
 /*    60 */   103,  114,  165,  179,  143,  158,  195,  167,  169,  180,
 /*    70 */   178,  -79,  211,  213,  204,  215,  186,  192,  200,   40,
 /*    80 */   257,  271,  284,  285,  289,  290,  288,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*    10 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*    20 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*    30 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*    40 */   550,  669,  550,  550,  680,  680,  680,  550,  550,  744,
 /*    50 */   550,  704,  696,  672,  686,  673,  550,  729,  689,  550,
 /*    60 */   711,  709,  550,  550,  711,  709,  550,  723,  719,  702,
 /*    70 */   700,  686,  550,  550,  550,  550,  747,  735,  731,  550,
 /*    80 */   550,  550,  555,  550,  555,  605,  556,  550,  550,  550,
 /*    90 */   550,  722,  721,  646,  645,  644,  640,  641,  550,  550,
 /*   100 */   550,  635,  636,  634,  633,  550,  550,  670,  550,  550,
 /*   110 */   550,  732,  736,  550,  623,  550,  550,  550,  550,  693,
 /*   120 */   703,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   130 */   550,  550,  550,  550,  623,  550,  720,  550,  679,  675,
 /*   140 */   550,  550,  671,  622,  550,  665,  550,  550,  550,  730,
 /*   150 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   160 */   550,  550,  550,  550,  550,  550,  576,  550,  550,  550,
 /*   170 */   602,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   180 */   550,  550,  550,  587,  585,  584,  583,  550,  580,  550,
 /*   190 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   200 */   550,  550,  550,  550,  550,  550,  550,  550,  550,  550,
 /*   210 */   550,  550,
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
  /*   70 */ "TABLES",
  /*   71 */ "NK_FLOAT",
  /*   72 */ "NK_BOOL",
  /*   73 */ "NK_VARIABLE",
  /*   74 */ "BETWEEN",
  /*   75 */ "IS",
  /*   76 */ "NULL",
  /*   77 */ "NK_LT",
  /*   78 */ "NK_GT",
  /*   79 */ "NK_LE",
  /*   80 */ "NK_GE",
  /*   81 */ "NK_NE",
  /*   82 */ "NK_EQ",
  /*   83 */ "LIKE",
  /*   84 */ "MATCH",
  /*   85 */ "NMATCH",
  /*   86 */ "IN",
  /*   87 */ "FROM",
  /*   88 */ "AS",
  /*   89 */ "JOIN",
  /*   90 */ "ON",
  /*   91 */ "INNER",
  /*   92 */ "SELECT",
  /*   93 */ "DISTINCT",
  /*   94 */ "WHERE",
  /*   95 */ "PARTITION",
  /*   96 */ "BY",
  /*   97 */ "SESSION",
  /*   98 */ "STATE_WINDOW",
  /*   99 */ "INTERVAL",
  /*  100 */ "SLIDING",
  /*  101 */ "FILL",
  /*  102 */ "VALUE",
  /*  103 */ "NONE",
  /*  104 */ "PREV",
  /*  105 */ "LINEAR",
  /*  106 */ "NEXT",
  /*  107 */ "GROUP",
  /*  108 */ "HAVING",
  /*  109 */ "ORDER",
  /*  110 */ "SLIMIT",
  /*  111 */ "SOFFSET",
  /*  112 */ "LIMIT",
  /*  113 */ "OFFSET",
  /*  114 */ "ASC",
  /*  115 */ "DESC",
  /*  116 */ "NULLS",
  /*  117 */ "FIRST",
  /*  118 */ "LAST",
  /*  119 */ "cmd",
  /*  120 */ "exists_opt",
  /*  121 */ "db_name",
  /*  122 */ "db_options",
  /*  123 */ "full_table_name",
  /*  124 */ "column_def_list",
  /*  125 */ "table_options",
  /*  126 */ "column_def",
  /*  127 */ "column_name",
  /*  128 */ "type_name",
  /*  129 */ "query_expression",
  /*  130 */ "literal",
  /*  131 */ "duration_literal",
  /*  132 */ "literal_list",
  /*  133 */ "table_name",
  /*  134 */ "function_name",
  /*  135 */ "table_alias",
  /*  136 */ "column_alias",
  /*  137 */ "expression",
  /*  138 */ "column_reference",
  /*  139 */ "expression_list",
  /*  140 */ "subquery",
  /*  141 */ "predicate",
  /*  142 */ "compare_op",
  /*  143 */ "in_op",
  /*  144 */ "in_predicate_value",
  /*  145 */ "boolean_value_expression",
  /*  146 */ "boolean_primary",
  /*  147 */ "common_expression",
  /*  148 */ "from_clause",
  /*  149 */ "table_reference_list",
  /*  150 */ "table_reference",
  /*  151 */ "table_primary",
  /*  152 */ "joined_table",
  /*  153 */ "alias_opt",
  /*  154 */ "parenthesized_joined_table",
  /*  155 */ "join_type",
  /*  156 */ "search_condition",
  /*  157 */ "query_specification",
  /*  158 */ "set_quantifier_opt",
  /*  159 */ "select_list",
  /*  160 */ "where_clause_opt",
  /*  161 */ "partition_by_clause_opt",
  /*  162 */ "twindow_clause_opt",
  /*  163 */ "group_by_clause_opt",
  /*  164 */ "having_clause_opt",
  /*  165 */ "select_sublist",
  /*  166 */ "select_item",
  /*  167 */ "sliding_opt",
  /*  168 */ "fill_opt",
  /*  169 */ "fill_mode",
  /*  170 */ "group_by_list",
  /*  171 */ "query_expression_body",
  /*  172 */ "order_by_clause_opt",
  /*  173 */ "slimit_clause_opt",
  /*  174 */ "limit_clause_opt",
  /*  175 */ "query_primary",
  /*  176 */ "sort_specification_list",
  /*  177 */ "sort_specification",
  /*  178 */ "ordering_specification_opt",
  /*  179 */ "null_ordering_opt",
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
 /*  57 */ "cmd ::= SHOW TABLES",
 /*  58 */ "cmd ::= query_expression",
 /*  59 */ "literal ::= NK_INTEGER",
 /*  60 */ "literal ::= NK_FLOAT",
 /*  61 */ "literal ::= NK_STRING",
 /*  62 */ "literal ::= NK_BOOL",
 /*  63 */ "literal ::= TIMESTAMP NK_STRING",
 /*  64 */ "literal ::= duration_literal",
 /*  65 */ "duration_literal ::= NK_VARIABLE",
 /*  66 */ "literal_list ::= literal",
 /*  67 */ "literal_list ::= literal_list NK_COMMA literal",
 /*  68 */ "db_name ::= NK_ID",
 /*  69 */ "table_name ::= NK_ID",
 /*  70 */ "column_name ::= NK_ID",
 /*  71 */ "function_name ::= NK_ID",
 /*  72 */ "table_alias ::= NK_ID",
 /*  73 */ "column_alias ::= NK_ID",
 /*  74 */ "expression ::= literal",
 /*  75 */ "expression ::= column_reference",
 /*  76 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /*  77 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /*  78 */ "expression ::= subquery",
 /*  79 */ "expression ::= NK_LP expression NK_RP",
 /*  80 */ "expression ::= NK_PLUS expression",
 /*  81 */ "expression ::= NK_MINUS expression",
 /*  82 */ "expression ::= expression NK_PLUS expression",
 /*  83 */ "expression ::= expression NK_MINUS expression",
 /*  84 */ "expression ::= expression NK_STAR expression",
 /*  85 */ "expression ::= expression NK_SLASH expression",
 /*  86 */ "expression ::= expression NK_REM expression",
 /*  87 */ "expression_list ::= expression",
 /*  88 */ "expression_list ::= expression_list NK_COMMA expression",
 /*  89 */ "column_reference ::= column_name",
 /*  90 */ "column_reference ::= table_name NK_DOT column_name",
 /*  91 */ "predicate ::= expression compare_op expression",
 /*  92 */ "predicate ::= expression BETWEEN expression AND expression",
 /*  93 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /*  94 */ "predicate ::= expression IS NULL",
 /*  95 */ "predicate ::= expression IS NOT NULL",
 /*  96 */ "predicate ::= expression in_op in_predicate_value",
 /*  97 */ "compare_op ::= NK_LT",
 /*  98 */ "compare_op ::= NK_GT",
 /*  99 */ "compare_op ::= NK_LE",
 /* 100 */ "compare_op ::= NK_GE",
 /* 101 */ "compare_op ::= NK_NE",
 /* 102 */ "compare_op ::= NK_EQ",
 /* 103 */ "compare_op ::= LIKE",
 /* 104 */ "compare_op ::= NOT LIKE",
 /* 105 */ "compare_op ::= MATCH",
 /* 106 */ "compare_op ::= NMATCH",
 /* 107 */ "in_op ::= IN",
 /* 108 */ "in_op ::= NOT IN",
 /* 109 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 110 */ "boolean_value_expression ::= boolean_primary",
 /* 111 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 112 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 113 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 114 */ "boolean_primary ::= predicate",
 /* 115 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 116 */ "common_expression ::= expression",
 /* 117 */ "common_expression ::= boolean_value_expression",
 /* 118 */ "from_clause ::= FROM table_reference_list",
 /* 119 */ "table_reference_list ::= table_reference",
 /* 120 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 121 */ "table_reference ::= table_primary",
 /* 122 */ "table_reference ::= joined_table",
 /* 123 */ "table_primary ::= table_name alias_opt",
 /* 124 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 125 */ "table_primary ::= subquery alias_opt",
 /* 126 */ "table_primary ::= parenthesized_joined_table",
 /* 127 */ "alias_opt ::=",
 /* 128 */ "alias_opt ::= table_alias",
 /* 129 */ "alias_opt ::= AS table_alias",
 /* 130 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 131 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 132 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 133 */ "join_type ::=",
 /* 134 */ "join_type ::= INNER",
 /* 135 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 136 */ "set_quantifier_opt ::=",
 /* 137 */ "set_quantifier_opt ::= DISTINCT",
 /* 138 */ "set_quantifier_opt ::= ALL",
 /* 139 */ "select_list ::= NK_STAR",
 /* 140 */ "select_list ::= select_sublist",
 /* 141 */ "select_sublist ::= select_item",
 /* 142 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 143 */ "select_item ::= common_expression",
 /* 144 */ "select_item ::= common_expression column_alias",
 /* 145 */ "select_item ::= common_expression AS column_alias",
 /* 146 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 147 */ "where_clause_opt ::=",
 /* 148 */ "where_clause_opt ::= WHERE search_condition",
 /* 149 */ "partition_by_clause_opt ::=",
 /* 150 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 151 */ "twindow_clause_opt ::=",
 /* 152 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 153 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 154 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 155 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 156 */ "sliding_opt ::=",
 /* 157 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 158 */ "fill_opt ::=",
 /* 159 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 160 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 161 */ "fill_mode ::= NONE",
 /* 162 */ "fill_mode ::= PREV",
 /* 163 */ "fill_mode ::= NULL",
 /* 164 */ "fill_mode ::= LINEAR",
 /* 165 */ "fill_mode ::= NEXT",
 /* 166 */ "group_by_clause_opt ::=",
 /* 167 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 168 */ "group_by_list ::= expression",
 /* 169 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 170 */ "having_clause_opt ::=",
 /* 171 */ "having_clause_opt ::= HAVING search_condition",
 /* 172 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 173 */ "query_expression_body ::= query_primary",
 /* 174 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 175 */ "query_primary ::= query_specification",
 /* 176 */ "order_by_clause_opt ::=",
 /* 177 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 178 */ "slimit_clause_opt ::=",
 /* 179 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 180 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 181 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 182 */ "limit_clause_opt ::=",
 /* 183 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 184 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 185 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 186 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 187 */ "search_condition ::= common_expression",
 /* 188 */ "sort_specification_list ::= sort_specification",
 /* 189 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 190 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 191 */ "ordering_specification_opt ::=",
 /* 192 */ "ordering_specification_opt ::= ASC",
 /* 193 */ "ordering_specification_opt ::= DESC",
 /* 194 */ "null_ordering_opt ::=",
 /* 195 */ "null_ordering_opt ::= NULLS FIRST",
 /* 196 */ "null_ordering_opt ::= NULLS LAST",
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
    case 119: /* cmd */
    case 120: /* exists_opt */
    case 126: /* column_def */
    case 129: /* query_expression */
    case 130: /* literal */
    case 131: /* duration_literal */
    case 137: /* expression */
    case 138: /* column_reference */
    case 140: /* subquery */
    case 141: /* predicate */
    case 144: /* in_predicate_value */
    case 145: /* boolean_value_expression */
    case 146: /* boolean_primary */
    case 147: /* common_expression */
    case 148: /* from_clause */
    case 149: /* table_reference_list */
    case 150: /* table_reference */
    case 151: /* table_primary */
    case 152: /* joined_table */
    case 154: /* parenthesized_joined_table */
    case 156: /* search_condition */
    case 157: /* query_specification */
    case 160: /* where_clause_opt */
    case 162: /* twindow_clause_opt */
    case 164: /* having_clause_opt */
    case 166: /* select_item */
    case 167: /* sliding_opt */
    case 168: /* fill_opt */
    case 171: /* query_expression_body */
    case 173: /* slimit_clause_opt */
    case 174: /* limit_clause_opt */
    case 175: /* query_primary */
    case 177: /* sort_specification */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyNode((yypminor->yy272)); 
}
      break;
    case 121: /* db_name */
    case 127: /* column_name */
    case 133: /* table_name */
    case 134: /* function_name */
    case 135: /* table_alias */
    case 136: /* column_alias */
    case 153: /* alias_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 122: /* db_options */
{
 tfree((yypminor->yy199)); 
}
      break;
    case 123: /* full_table_name */
{
 
}
      break;
    case 124: /* column_def_list */
{
 nodesDestroyList((yypminor->yy64)); 
}
      break;
    case 125: /* table_options */
{
 tfree((yypminor->yy46)); 
}
      break;
    case 128: /* type_name */
{
 
}
      break;
    case 132: /* literal_list */
    case 139: /* expression_list */
    case 159: /* select_list */
    case 161: /* partition_by_clause_opt */
    case 163: /* group_by_clause_opt */
    case 165: /* select_sublist */
    case 170: /* group_by_list */
    case 172: /* order_by_clause_opt */
    case 176: /* sort_specification_list */
{
 PARSER_DESTRUCTOR_TRACE; nodesDestroyList((yypminor->yy64)); 
}
      break;
    case 142: /* compare_op */
    case 143: /* in_op */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 155: /* join_type */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 158: /* set_quantifier_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 169: /* fill_mode */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 178: /* ordering_specification_opt */
{
 PARSER_DESTRUCTOR_TRACE; 
}
      break;
    case 179: /* null_ordering_opt */
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
  {  119,   -5 }, /* (0) cmd ::= CREATE DATABASE exists_opt db_name db_options */
  {  120,   -3 }, /* (1) exists_opt ::= IF NOT EXISTS */
  {  120,    0 }, /* (2) exists_opt ::= */
  {  122,    0 }, /* (3) db_options ::= */
  {  122,   -3 }, /* (4) db_options ::= db_options BLOCKS NK_INTEGER */
  {  122,   -3 }, /* (5) db_options ::= db_options CACHE NK_INTEGER */
  {  122,   -3 }, /* (6) db_options ::= db_options CACHELAST NK_INTEGER */
  {  122,   -3 }, /* (7) db_options ::= db_options COMP NK_INTEGER */
  {  122,   -3 }, /* (8) db_options ::= db_options DAYS NK_INTEGER */
  {  122,   -3 }, /* (9) db_options ::= db_options FSYNC NK_INTEGER */
  {  122,   -3 }, /* (10) db_options ::= db_options MAXROWS NK_INTEGER */
  {  122,   -3 }, /* (11) db_options ::= db_options MINROWS NK_INTEGER */
  {  122,   -3 }, /* (12) db_options ::= db_options KEEP NK_INTEGER */
  {  122,   -3 }, /* (13) db_options ::= db_options PRECISION NK_STRING */
  {  122,   -3 }, /* (14) db_options ::= db_options QUORUM NK_INTEGER */
  {  122,   -3 }, /* (15) db_options ::= db_options REPLICA NK_INTEGER */
  {  122,   -3 }, /* (16) db_options ::= db_options TTL NK_INTEGER */
  {  122,   -3 }, /* (17) db_options ::= db_options WAL NK_INTEGER */
  {  122,   -3 }, /* (18) db_options ::= db_options VGROUPS NK_INTEGER */
  {  122,   -3 }, /* (19) db_options ::= db_options SINGLESTABLE NK_INTEGER */
  {  122,   -3 }, /* (20) db_options ::= db_options STREAMMODE NK_INTEGER */
  {  119,   -2 }, /* (21) cmd ::= USE db_name */
  {  119,   -8 }, /* (22) cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP table_options */
  {  123,   -1 }, /* (23) full_table_name ::= NK_ID */
  {  123,   -3 }, /* (24) full_table_name ::= NK_ID NK_DOT NK_ID */
  {  124,   -1 }, /* (25) column_def_list ::= column_def */
  {  124,   -3 }, /* (26) column_def_list ::= column_def_list NK_COMMA column_def */
  {  126,   -2 }, /* (27) column_def ::= column_name type_name */
  {  126,   -4 }, /* (28) column_def ::= column_name type_name COMMENT NK_STRING */
  {  128,   -1 }, /* (29) type_name ::= BOOL */
  {  128,   -1 }, /* (30) type_name ::= TINYINT */
  {  128,   -1 }, /* (31) type_name ::= SMALLINT */
  {  128,   -1 }, /* (32) type_name ::= INT */
  {  128,   -1 }, /* (33) type_name ::= INTEGER */
  {  128,   -1 }, /* (34) type_name ::= BIGINT */
  {  128,   -1 }, /* (35) type_name ::= FLOAT */
  {  128,   -1 }, /* (36) type_name ::= DOUBLE */
  {  128,   -4 }, /* (37) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  128,   -1 }, /* (38) type_name ::= TIMESTAMP */
  {  128,   -4 }, /* (39) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  128,   -2 }, /* (40) type_name ::= TINYINT UNSIGNED */
  {  128,   -2 }, /* (41) type_name ::= SMALLINT UNSIGNED */
  {  128,   -2 }, /* (42) type_name ::= INT UNSIGNED */
  {  128,   -2 }, /* (43) type_name ::= BIGINT UNSIGNED */
  {  128,   -1 }, /* (44) type_name ::= JSON */
  {  128,   -4 }, /* (45) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  128,   -1 }, /* (46) type_name ::= MEDIUMBLOB */
  {  128,   -1 }, /* (47) type_name ::= BLOB */
  {  128,   -4 }, /* (48) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  128,   -1 }, /* (49) type_name ::= DECIMAL */
  {  128,   -4 }, /* (50) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  128,   -6 }, /* (51) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  125,    0 }, /* (52) table_options ::= */
  {  125,   -3 }, /* (53) table_options ::= table_options COMMENT NK_INTEGER */
  {  125,   -3 }, /* (54) table_options ::= table_options KEEP NK_INTEGER */
  {  125,   -3 }, /* (55) table_options ::= table_options TTL NK_INTEGER */
  {  119,   -2 }, /* (56) cmd ::= SHOW DATABASES */
  {  119,   -2 }, /* (57) cmd ::= SHOW TABLES */
  {  119,   -1 }, /* (58) cmd ::= query_expression */
  {  130,   -1 }, /* (59) literal ::= NK_INTEGER */
  {  130,   -1 }, /* (60) literal ::= NK_FLOAT */
  {  130,   -1 }, /* (61) literal ::= NK_STRING */
  {  130,   -1 }, /* (62) literal ::= NK_BOOL */
  {  130,   -2 }, /* (63) literal ::= TIMESTAMP NK_STRING */
  {  130,   -1 }, /* (64) literal ::= duration_literal */
  {  131,   -1 }, /* (65) duration_literal ::= NK_VARIABLE */
  {  132,   -1 }, /* (66) literal_list ::= literal */
  {  132,   -3 }, /* (67) literal_list ::= literal_list NK_COMMA literal */
  {  121,   -1 }, /* (68) db_name ::= NK_ID */
  {  133,   -1 }, /* (69) table_name ::= NK_ID */
  {  127,   -1 }, /* (70) column_name ::= NK_ID */
  {  134,   -1 }, /* (71) function_name ::= NK_ID */
  {  135,   -1 }, /* (72) table_alias ::= NK_ID */
  {  136,   -1 }, /* (73) column_alias ::= NK_ID */
  {  137,   -1 }, /* (74) expression ::= literal */
  {  137,   -1 }, /* (75) expression ::= column_reference */
  {  137,   -4 }, /* (76) expression ::= function_name NK_LP expression_list NK_RP */
  {  137,   -4 }, /* (77) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  137,   -1 }, /* (78) expression ::= subquery */
  {  137,   -3 }, /* (79) expression ::= NK_LP expression NK_RP */
  {  137,   -2 }, /* (80) expression ::= NK_PLUS expression */
  {  137,   -2 }, /* (81) expression ::= NK_MINUS expression */
  {  137,   -3 }, /* (82) expression ::= expression NK_PLUS expression */
  {  137,   -3 }, /* (83) expression ::= expression NK_MINUS expression */
  {  137,   -3 }, /* (84) expression ::= expression NK_STAR expression */
  {  137,   -3 }, /* (85) expression ::= expression NK_SLASH expression */
  {  137,   -3 }, /* (86) expression ::= expression NK_REM expression */
  {  139,   -1 }, /* (87) expression_list ::= expression */
  {  139,   -3 }, /* (88) expression_list ::= expression_list NK_COMMA expression */
  {  138,   -1 }, /* (89) column_reference ::= column_name */
  {  138,   -3 }, /* (90) column_reference ::= table_name NK_DOT column_name */
  {  141,   -3 }, /* (91) predicate ::= expression compare_op expression */
  {  141,   -5 }, /* (92) predicate ::= expression BETWEEN expression AND expression */
  {  141,   -6 }, /* (93) predicate ::= expression NOT BETWEEN expression AND expression */
  {  141,   -3 }, /* (94) predicate ::= expression IS NULL */
  {  141,   -4 }, /* (95) predicate ::= expression IS NOT NULL */
  {  141,   -3 }, /* (96) predicate ::= expression in_op in_predicate_value */
  {  142,   -1 }, /* (97) compare_op ::= NK_LT */
  {  142,   -1 }, /* (98) compare_op ::= NK_GT */
  {  142,   -1 }, /* (99) compare_op ::= NK_LE */
  {  142,   -1 }, /* (100) compare_op ::= NK_GE */
  {  142,   -1 }, /* (101) compare_op ::= NK_NE */
  {  142,   -1 }, /* (102) compare_op ::= NK_EQ */
  {  142,   -1 }, /* (103) compare_op ::= LIKE */
  {  142,   -2 }, /* (104) compare_op ::= NOT LIKE */
  {  142,   -1 }, /* (105) compare_op ::= MATCH */
  {  142,   -1 }, /* (106) compare_op ::= NMATCH */
  {  143,   -1 }, /* (107) in_op ::= IN */
  {  143,   -2 }, /* (108) in_op ::= NOT IN */
  {  144,   -3 }, /* (109) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  145,   -1 }, /* (110) boolean_value_expression ::= boolean_primary */
  {  145,   -2 }, /* (111) boolean_value_expression ::= NOT boolean_primary */
  {  145,   -3 }, /* (112) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  145,   -3 }, /* (113) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  146,   -1 }, /* (114) boolean_primary ::= predicate */
  {  146,   -3 }, /* (115) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  147,   -1 }, /* (116) common_expression ::= expression */
  {  147,   -1 }, /* (117) common_expression ::= boolean_value_expression */
  {  148,   -2 }, /* (118) from_clause ::= FROM table_reference_list */
  {  149,   -1 }, /* (119) table_reference_list ::= table_reference */
  {  149,   -3 }, /* (120) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  150,   -1 }, /* (121) table_reference ::= table_primary */
  {  150,   -1 }, /* (122) table_reference ::= joined_table */
  {  151,   -2 }, /* (123) table_primary ::= table_name alias_opt */
  {  151,   -4 }, /* (124) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  151,   -2 }, /* (125) table_primary ::= subquery alias_opt */
  {  151,   -1 }, /* (126) table_primary ::= parenthesized_joined_table */
  {  153,    0 }, /* (127) alias_opt ::= */
  {  153,   -1 }, /* (128) alias_opt ::= table_alias */
  {  153,   -2 }, /* (129) alias_opt ::= AS table_alias */
  {  154,   -3 }, /* (130) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  154,   -3 }, /* (131) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  152,   -6 }, /* (132) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  155,    0 }, /* (133) join_type ::= */
  {  155,   -1 }, /* (134) join_type ::= INNER */
  {  157,   -9 }, /* (135) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  158,    0 }, /* (136) set_quantifier_opt ::= */
  {  158,   -1 }, /* (137) set_quantifier_opt ::= DISTINCT */
  {  158,   -1 }, /* (138) set_quantifier_opt ::= ALL */
  {  159,   -1 }, /* (139) select_list ::= NK_STAR */
  {  159,   -1 }, /* (140) select_list ::= select_sublist */
  {  165,   -1 }, /* (141) select_sublist ::= select_item */
  {  165,   -3 }, /* (142) select_sublist ::= select_sublist NK_COMMA select_item */
  {  166,   -1 }, /* (143) select_item ::= common_expression */
  {  166,   -2 }, /* (144) select_item ::= common_expression column_alias */
  {  166,   -3 }, /* (145) select_item ::= common_expression AS column_alias */
  {  166,   -3 }, /* (146) select_item ::= table_name NK_DOT NK_STAR */
  {  160,    0 }, /* (147) where_clause_opt ::= */
  {  160,   -2 }, /* (148) where_clause_opt ::= WHERE search_condition */
  {  161,    0 }, /* (149) partition_by_clause_opt ::= */
  {  161,   -3 }, /* (150) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  162,    0 }, /* (151) twindow_clause_opt ::= */
  {  162,   -6 }, /* (152) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  162,   -4 }, /* (153) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  162,   -6 }, /* (154) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  162,   -8 }, /* (155) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  167,    0 }, /* (156) sliding_opt ::= */
  {  167,   -4 }, /* (157) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  168,    0 }, /* (158) fill_opt ::= */
  {  168,   -4 }, /* (159) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  168,   -6 }, /* (160) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  169,   -1 }, /* (161) fill_mode ::= NONE */
  {  169,   -1 }, /* (162) fill_mode ::= PREV */
  {  169,   -1 }, /* (163) fill_mode ::= NULL */
  {  169,   -1 }, /* (164) fill_mode ::= LINEAR */
  {  169,   -1 }, /* (165) fill_mode ::= NEXT */
  {  163,    0 }, /* (166) group_by_clause_opt ::= */
  {  163,   -3 }, /* (167) group_by_clause_opt ::= GROUP BY group_by_list */
  {  170,   -1 }, /* (168) group_by_list ::= expression */
  {  170,   -3 }, /* (169) group_by_list ::= group_by_list NK_COMMA expression */
  {  164,    0 }, /* (170) having_clause_opt ::= */
  {  164,   -2 }, /* (171) having_clause_opt ::= HAVING search_condition */
  {  129,   -4 }, /* (172) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  171,   -1 }, /* (173) query_expression_body ::= query_primary */
  {  171,   -4 }, /* (174) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  175,   -1 }, /* (175) query_primary ::= query_specification */
  {  172,    0 }, /* (176) order_by_clause_opt ::= */
  {  172,   -3 }, /* (177) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  173,    0 }, /* (178) slimit_clause_opt ::= */
  {  173,   -2 }, /* (179) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  173,   -4 }, /* (180) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  173,   -4 }, /* (181) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  174,    0 }, /* (182) limit_clause_opt ::= */
  {  174,   -2 }, /* (183) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  174,   -4 }, /* (184) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  174,   -4 }, /* (185) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  140,   -3 }, /* (186) subquery ::= NK_LP query_expression NK_RP */
  {  156,   -1 }, /* (187) search_condition ::= common_expression */
  {  176,   -1 }, /* (188) sort_specification_list ::= sort_specification */
  {  176,   -3 }, /* (189) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  177,   -3 }, /* (190) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  178,    0 }, /* (191) ordering_specification_opt ::= */
  {  178,   -1 }, /* (192) ordering_specification_opt ::= ASC */
  {  178,   -1 }, /* (193) ordering_specification_opt ::= DESC */
  {  179,    0 }, /* (194) null_ordering_opt ::= */
  {  179,   -2 }, /* (195) null_ordering_opt ::= NULLS FIRST */
  {  179,   -2 }, /* (196) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy137, &yymsp[-1].minor.yy209, yymsp[0].minor.yy199);}
        break;
      case 1: /* exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy137 = true; }
        break;
      case 2: /* exists_opt ::= */
{ yymsp[1].minor.yy137 = false; }
        break;
      case 3: /* db_options ::= */
{ yymsp[1].minor.yy199 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 4: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 5: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 6: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 7: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 8: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 9: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 10: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 11: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 12: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 13: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 14: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 15: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 16: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 17: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 18: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 19: /* db_options ::= db_options SINGLESTABLE NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_SINGLESTABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 20: /* db_options ::= db_options STREAMMODE NK_INTEGER */
{ yylhsminor.yy199 = setDatabaseOption(pCxt, yymsp[-2].minor.yy199, DB_OPTION_STREAMMODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy199 = yylhsminor.yy199;
        break;
      case 21: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy209);}
        break;
      case 22: /* cmd ::= CREATE TABLE exists_opt full_table_name NK_LP column_def_list NK_RP table_options */
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-5].minor.yy137, &yymsp[-4].minor.yy57, yymsp[-2].minor.yy64, yymsp[0].minor.yy46);}
        break;
      case 23: /* full_table_name ::= NK_ID */
{ STokenPair t = { .first = nil_token, .second = yymsp[0].minor.yy0 }; yylhsminor.yy57 = t; }
  yymsp[0].minor.yy57 = yylhsminor.yy57;
        break;
      case 24: /* full_table_name ::= NK_ID NK_DOT NK_ID */
{ STokenPair t = { .first = yymsp[-2].minor.yy0, .second = yymsp[0].minor.yy0 }; yylhsminor.yy57 = t; }
  yymsp[-2].minor.yy57 = yylhsminor.yy57;
        break;
      case 25: /* column_def_list ::= column_def */
{ yylhsminor.yy64 = createNodeList(pCxt, yymsp[0].minor.yy272); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 26: /* column_def_list ::= column_def_list NK_COMMA column_def */
{ yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, yymsp[0].minor.yy272); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 27: /* column_def ::= column_name type_name */
{ yylhsminor.yy272 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy209, yymsp[0].minor.yy304, NULL); }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 28: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy272 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy209, yymsp[-2].minor.yy304, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 29: /* type_name ::= BOOL */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 30: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 31: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 32: /* type_name ::= INT */
      case 33: /* type_name ::= INTEGER */ yytestcase(yyruleno==33);
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 34: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 35: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 36: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 37: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 38: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 39: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 40: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 41: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 42: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 43: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy304 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 44: /* type_name ::= JSON */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 45: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 46: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 47: /* type_name ::= BLOB */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 48: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 49: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 50: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 51: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy304 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 52: /* table_options ::= */
{ yymsp[1].minor.yy46 = createDefaultTableOptions(pCxt);}
        break;
      case 53: /* table_options ::= table_options COMMENT NK_INTEGER */
{ yylhsminor.yy46 = setTableOption(pCxt, yymsp[-2].minor.yy46, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 54: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy46 = setTableOption(pCxt, yymsp[-2].minor.yy46, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 55: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy46 = setTableOption(pCxt, yymsp[-2].minor.yy46, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy46 = yylhsminor.yy46;
        break;
      case 56: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT); }
        break;
      case 57: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT); }
        break;
      case 58: /* cmd ::= query_expression */
{ PARSER_TRACE; pCxt->pRootNode = yymsp[0].minor.yy272; }
        break;
      case 59: /* literal ::= NK_INTEGER */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 60: /* literal ::= NK_FLOAT */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 61: /* literal ::= NK_STRING */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 62: /* literal ::= NK_BOOL */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 63: /* literal ::= TIMESTAMP NK_STRING */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 64: /* literal ::= duration_literal */
      case 74: /* expression ::= literal */ yytestcase(yyruleno==74);
      case 75: /* expression ::= column_reference */ yytestcase(yyruleno==75);
      case 78: /* expression ::= subquery */ yytestcase(yyruleno==78);
      case 110: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==110);
      case 114: /* boolean_primary ::= predicate */ yytestcase(yyruleno==114);
      case 119: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==119);
      case 121: /* table_reference ::= table_primary */ yytestcase(yyruleno==121);
      case 122: /* table_reference ::= joined_table */ yytestcase(yyruleno==122);
      case 126: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==126);
      case 173: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==173);
      case 175: /* query_primary ::= query_specification */ yytestcase(yyruleno==175);
{ PARSER_TRACE; yylhsminor.yy272 = yymsp[0].minor.yy272; }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 65: /* duration_literal ::= NK_VARIABLE */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 66: /* literal_list ::= literal */
      case 87: /* expression_list ::= expression */ yytestcase(yyruleno==87);
{ PARSER_TRACE; yylhsminor.yy64 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy272)); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 67: /* literal_list ::= literal_list NK_COMMA literal */
      case 88: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==88);
{ PARSER_TRACE; yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, releaseRawExprNode(pCxt, yymsp[0].minor.yy272)); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 68: /* db_name ::= NK_ID */
      case 69: /* table_name ::= NK_ID */ yytestcase(yyruleno==69);
      case 70: /* column_name ::= NK_ID */ yytestcase(yyruleno==70);
      case 71: /* function_name ::= NK_ID */ yytestcase(yyruleno==71);
      case 72: /* table_alias ::= NK_ID */ yytestcase(yyruleno==72);
      case 73: /* column_alias ::= NK_ID */ yytestcase(yyruleno==73);
{ PARSER_TRACE; yylhsminor.yy209 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 76: /* expression ::= function_name NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy209, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy209, yymsp[-1].minor.yy64)); }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 77: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy209, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy209, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 79: /* expression ::= NK_LP expression NK_RP */
      case 115: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==115);
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy272)); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 80: /* expression ::= NK_PLUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy272));
                                                                                  }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 81: /* expression ::= NK_MINUS expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy272), NULL));
                                                                                  }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 82: /* expression ::= expression NK_PLUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); 
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 83: /* expression ::= expression NK_MINUS expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); 
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 84: /* expression ::= expression NK_STAR expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); 
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 85: /* expression ::= expression NK_SLASH expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); 
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 86: /* expression ::= expression NK_REM expression */
{
                                                                                    PARSER_TRACE; 
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); 
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 89: /* column_reference ::= column_name */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNode(pCxt, &yymsp[0].minor.yy209, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy209)); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 90: /* column_reference ::= table_name NK_DOT column_name */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy209, createColumnNode(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy209)); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 91: /* predicate ::= expression compare_op expression */
      case 96: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==96);
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy20, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272)));
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 92: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy272), releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272)));
                                                                                  }
  yymsp[-4].minor.yy272 = yylhsminor.yy272;
        break;
      case 93: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[-5].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272)));
                                                                                  }
  yymsp[-5].minor.yy272 = yylhsminor.yy272;
        break;
      case 94: /* predicate ::= expression IS NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), NULL));
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 95: /* predicate ::= expression IS NOT NULL */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy272), NULL));
                                                                                  }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 97: /* compare_op ::= NK_LT */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_LOWER_THAN; }
        break;
      case 98: /* compare_op ::= NK_GT */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_GREATER_THAN; }
        break;
      case 99: /* compare_op ::= NK_LE */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 100: /* compare_op ::= NK_GE */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 101: /* compare_op ::= NK_NE */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_NOT_EQUAL; }
        break;
      case 102: /* compare_op ::= NK_EQ */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_EQUAL; }
        break;
      case 103: /* compare_op ::= LIKE */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_LIKE; }
        break;
      case 104: /* compare_op ::= NOT LIKE */
{ PARSER_TRACE; yymsp[-1].minor.yy20 = OP_TYPE_NOT_LIKE; }
        break;
      case 105: /* compare_op ::= MATCH */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_MATCH; }
        break;
      case 106: /* compare_op ::= NMATCH */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_NMATCH; }
        break;
      case 107: /* in_op ::= IN */
{ PARSER_TRACE; yymsp[0].minor.yy20 = OP_TYPE_IN; }
        break;
      case 108: /* in_op ::= NOT IN */
{ PARSER_TRACE; yymsp[-1].minor.yy20 = OP_TYPE_NOT_IN; }
        break;
      case 109: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy64)); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 111: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy272), NULL));
                                                                                  }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 112: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272)));
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 113: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy272);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), releaseRawExprNode(pCxt, yymsp[0].minor.yy272)));
                                                                                  }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 116: /* common_expression ::= expression */
      case 117: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==117);
{ yylhsminor.yy272 = yymsp[0].minor.yy272; }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 118: /* from_clause ::= FROM table_reference_list */
      case 148: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==148);
      case 171: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==171);
{ PARSER_TRACE; yymsp[-1].minor.yy272 = yymsp[0].minor.yy272; }
        break;
      case 120: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ PARSER_TRACE; yylhsminor.yy272 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy272, yymsp[0].minor.yy272, NULL); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 123: /* table_primary ::= table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy272 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy209, &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 124: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ PARSER_TRACE; yylhsminor.yy272 = createRealTableNode(pCxt, &yymsp[-3].minor.yy209, &yymsp[-1].minor.yy209, &yymsp[0].minor.yy209); }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 125: /* table_primary ::= subquery alias_opt */
{ PARSER_TRACE; yylhsminor.yy272 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy272), &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 127: /* alias_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy209 = nil_token;  }
        break;
      case 128: /* alias_opt ::= table_alias */
{ PARSER_TRACE; yylhsminor.yy209 = yymsp[0].minor.yy209; }
  yymsp[0].minor.yy209 = yylhsminor.yy209;
        break;
      case 129: /* alias_opt ::= AS table_alias */
{ PARSER_TRACE; yymsp[-1].minor.yy209 = yymsp[0].minor.yy209; }
        break;
      case 130: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 131: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==131);
{ PARSER_TRACE; yymsp[-2].minor.yy272 = yymsp[-1].minor.yy272; }
        break;
      case 132: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ PARSER_TRACE; yylhsminor.yy272 = createJoinTableNode(pCxt, yymsp[-4].minor.yy252, yymsp[-5].minor.yy272, yymsp[-2].minor.yy272, yymsp[0].minor.yy272); }
  yymsp[-5].minor.yy272 = yylhsminor.yy272;
        break;
      case 133: /* join_type ::= */
{ PARSER_TRACE; yymsp[1].minor.yy252 = JOIN_TYPE_INNER; }
        break;
      case 134: /* join_type ::= INNER */
{ PARSER_TRACE; yymsp[0].minor.yy252 = JOIN_TYPE_INNER; }
        break;
      case 135: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yymsp[-8].minor.yy272 = createSelectStmt(pCxt, yymsp[-7].minor.yy137, yymsp[-6].minor.yy64, yymsp[-5].minor.yy272);
                                                                                    yymsp[-8].minor.yy272 = addWhereClause(pCxt, yymsp[-8].minor.yy272, yymsp[-4].minor.yy272);
                                                                                    yymsp[-8].minor.yy272 = addPartitionByClause(pCxt, yymsp[-8].minor.yy272, yymsp[-3].minor.yy64);
                                                                                    yymsp[-8].minor.yy272 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy272, yymsp[-2].minor.yy272);
                                                                                    yymsp[-8].minor.yy272 = addGroupByClause(pCxt, yymsp[-8].minor.yy272, yymsp[-1].minor.yy64);
                                                                                    yymsp[-8].minor.yy272 = addHavingClause(pCxt, yymsp[-8].minor.yy272, yymsp[0].minor.yy272);
                                                                                  }
        break;
      case 136: /* set_quantifier_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy137 = false; }
        break;
      case 137: /* set_quantifier_opt ::= DISTINCT */
{ PARSER_TRACE; yymsp[0].minor.yy137 = true; }
        break;
      case 138: /* set_quantifier_opt ::= ALL */
{ PARSER_TRACE; yymsp[0].minor.yy137 = false; }
        break;
      case 139: /* select_list ::= NK_STAR */
{ PARSER_TRACE; yymsp[0].minor.yy64 = NULL; }
        break;
      case 140: /* select_list ::= select_sublist */
{ PARSER_TRACE; yylhsminor.yy64 = yymsp[0].minor.yy64; }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 141: /* select_sublist ::= select_item */
      case 188: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==188);
{ PARSER_TRACE; yylhsminor.yy64 = createNodeList(pCxt, yymsp[0].minor.yy272); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 142: /* select_sublist ::= select_sublist NK_COMMA select_item */
      case 189: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==189);
{ PARSER_TRACE; yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, yymsp[0].minor.yy272); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 143: /* select_item ::= common_expression */
{
                                                                                    PARSER_TRACE;
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy272);
                                                                                    yylhsminor.yy272 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy272), &t);
                                                                                  }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 144: /* select_item ::= common_expression column_alias */
{ PARSER_TRACE; yylhsminor.yy272 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy272), &yymsp[0].minor.yy209); }
  yymsp[-1].minor.yy272 = yylhsminor.yy272;
        break;
      case 145: /* select_item ::= common_expression AS column_alias */
{ PARSER_TRACE; yylhsminor.yy272 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), &yymsp[0].minor.yy209); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 146: /* select_item ::= table_name NK_DOT NK_STAR */
{ PARSER_TRACE; yylhsminor.yy272 = createColumnNode(pCxt, &yymsp[-2].minor.yy209, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 147: /* where_clause_opt ::= */
      case 151: /* twindow_clause_opt ::= */ yytestcase(yyruleno==151);
      case 156: /* sliding_opt ::= */ yytestcase(yyruleno==156);
      case 158: /* fill_opt ::= */ yytestcase(yyruleno==158);
      case 170: /* having_clause_opt ::= */ yytestcase(yyruleno==170);
      case 178: /* slimit_clause_opt ::= */ yytestcase(yyruleno==178);
      case 182: /* limit_clause_opt ::= */ yytestcase(yyruleno==182);
{ PARSER_TRACE; yymsp[1].minor.yy272 = NULL; }
        break;
      case 149: /* partition_by_clause_opt ::= */
      case 166: /* group_by_clause_opt ::= */ yytestcase(yyruleno==166);
      case 176: /* order_by_clause_opt ::= */ yytestcase(yyruleno==176);
{ PARSER_TRACE; yymsp[1].minor.yy64 = NULL; }
        break;
      case 150: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 167: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==167);
      case 177: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==177);
{ PARSER_TRACE; yymsp[-2].minor.yy64 = yymsp[0].minor.yy64; }
        break;
      case 152: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy272 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy272), &yymsp[-1].minor.yy0); }
        break;
      case 153: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy272 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy272)); }
        break;
      case 154: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-5].minor.yy272 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy272, NULL, yymsp[-1].minor.yy272, yymsp[0].minor.yy272); }
        break;
      case 155: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ PARSER_TRACE; yymsp[-7].minor.yy272 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy272, yymsp[-3].minor.yy272, yymsp[-1].minor.yy272, yymsp[0].minor.yy272); }
        break;
      case 157: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy272 = yymsp[-1].minor.yy272; }
        break;
      case 159: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ PARSER_TRACE; yymsp[-3].minor.yy272 = createFillNode(pCxt, yymsp[-1].minor.yy54, NULL); }
        break;
      case 160: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ PARSER_TRACE; yymsp[-5].minor.yy272 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy64)); }
        break;
      case 161: /* fill_mode ::= NONE */
{ PARSER_TRACE; yymsp[0].minor.yy54 = FILL_MODE_NONE; }
        break;
      case 162: /* fill_mode ::= PREV */
{ PARSER_TRACE; yymsp[0].minor.yy54 = FILL_MODE_PREV; }
        break;
      case 163: /* fill_mode ::= NULL */
{ PARSER_TRACE; yymsp[0].minor.yy54 = FILL_MODE_NULL; }
        break;
      case 164: /* fill_mode ::= LINEAR */
{ PARSER_TRACE; yymsp[0].minor.yy54 = FILL_MODE_LINEAR; }
        break;
      case 165: /* fill_mode ::= NEXT */
{ PARSER_TRACE; yymsp[0].minor.yy54 = FILL_MODE_NEXT; }
        break;
      case 168: /* group_by_list ::= expression */
{ PARSER_TRACE; yylhsminor.yy64 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 169: /* group_by_list ::= group_by_list NK_COMMA expression */
{ PARSER_TRACE; yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy272))); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 172: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    PARSER_TRACE;
                                                                                    yylhsminor.yy272 = addOrderByClause(pCxt, yymsp[-3].minor.yy272, yymsp[-2].minor.yy64);
                                                                                    yylhsminor.yy272 = addSlimitClause(pCxt, yylhsminor.yy272, yymsp[-1].minor.yy272);
                                                                                    yylhsminor.yy272 = addLimitClause(pCxt, yylhsminor.yy272, yymsp[0].minor.yy272);
                                                                                  }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 174: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ PARSER_TRACE; yylhsminor.yy272 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy272, yymsp[0].minor.yy272); }
  yymsp[-3].minor.yy272 = yylhsminor.yy272;
        break;
      case 179: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 183: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==183);
{ PARSER_TRACE; yymsp[-1].minor.yy272 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 180: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 184: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==184);
{ PARSER_TRACE; yymsp[-3].minor.yy272 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 181: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 185: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==185);
{ PARSER_TRACE; yymsp[-3].minor.yy272 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 186: /* subquery ::= NK_LP query_expression NK_RP */
{ PARSER_TRACE; yylhsminor.yy272 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy272); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 187: /* search_condition ::= common_expression */
{ PARSER_TRACE; yylhsminor.yy272 = releaseRawExprNode(pCxt, yymsp[0].minor.yy272); }
  yymsp[0].minor.yy272 = yylhsminor.yy272;
        break;
      case 190: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ PARSER_TRACE; yylhsminor.yy272 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy272), yymsp[-1].minor.yy218, yymsp[0].minor.yy217); }
  yymsp[-2].minor.yy272 = yylhsminor.yy272;
        break;
      case 191: /* ordering_specification_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy218 = ORDER_ASC; }
        break;
      case 192: /* ordering_specification_opt ::= ASC */
{ PARSER_TRACE; yymsp[0].minor.yy218 = ORDER_ASC; }
        break;
      case 193: /* ordering_specification_opt ::= DESC */
{ PARSER_TRACE; yymsp[0].minor.yy218 = ORDER_DESC; }
        break;
      case 194: /* null_ordering_opt ::= */
{ PARSER_TRACE; yymsp[1].minor.yy217 = NULL_ORDER_DEFAULT; }
        break;
      case 195: /* null_ordering_opt ::= NULLS FIRST */
{ PARSER_TRACE; yymsp[-1].minor.yy217 = NULL_ORDER_FIRST; }
        break;
      case 196: /* null_ordering_opt ::= NULLS LAST */
{ PARSER_TRACE; yymsp[-1].minor.yy217 = NULL_ORDER_LAST; }
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
