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
#define YYNOCODE 208
#define YYACTIONTYPE unsigned short int
#define NewParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  NewParseTOKENTYPE yy0;
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
#define YYNSTATE             272
#define YYNRULE              234
#define YYNTOKEN             134
#define YY_MAX_SHIFT         271
#define YY_MIN_SHIFTREDUCE   432
#define YY_MAX_SHIFTREDUCE   665
#define YY_ERROR_ACTION      666
#define YY_ACCEPT_ACTION     667
#define YY_NO_ACTION         668
#define YY_MIN_REDUCE        669
#define YY_MAX_REDUCE        902
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
#define YY_ACTTAB_COUNT (892)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   135,  147,   23,   93,  761,  711,  759,  240,  148,  771,
 /*    10 */   769,  146,   30,   28,   26,   25,   24,  771,  769,  192,
 /*    20 */   175,  795,   94,  724,   65,   30,   28,   26,   25,   24,
 /*    30 */   682,  164,  192,  215,  795,   59,  719,  130,  780,  769,
 /*    40 */   193,  202,  215,   53,  781,  212,  784,  820,  722,   57,
 /*    50 */   130,  137,  816,  893,   19,  772,  769,  722,  548,   71,
 /*    60 */   827,  828,  854,  832,   30,   28,   26,   25,   24,  264,
 /*    70 */   263,  262,  261,  260,  259,  258,  257,  256,  255,  254,
 /*    80 */   253,  252,  251,  250,  249,  248,   26,   25,   24,   22,
 /*    90 */   139,  202,  566,  567,  568,  569,  570,  571,  572,  574,
 /*   100 */   575,  576,   22,  139,  150,  566,  567,  568,  569,  570,
 /*   110 */   571,  572,  574,  575,  576,  169,  724,   65,  491,  238,
 /*   120 */   237,  236,  495,  235,  497,  498,  234,  500,  231,  529,
 /*   130 */   506,  228,  508,  509,  225,  222,  192,  527,  795,  881,
 /*   140 */    75,  182,  780,  769,  193,  214,  214,   52,  781,  247,
 /*   150 */   784,  820,  192,  880,  795,  129,  816,  879,  780,  769,
 /*   160 */   193,  709,   40,   66,  781,  152,  784,  881,  178,   60,
 /*   170 */   795,   10,   10,  717,  780,  769,  193,  724,   65,   53,
 /*   180 */   781,   74,  784,  820,  216,  879,  540,  137,  816,   69,
 /*   190 */   170,  165,  163,  214,  528,  530,  533,  247,  192,  538,
 /*   200 */   795,   92,  183,  894,  780,  769,  193,  154,  847,  123,
 /*   210 */   781,  143,  784,  178,  192,  795,  795,  268,  267,  780,
 /*   220 */   769,  193,   75,  629,   53,  781,  168,  784,  820,  192,
 /*   230 */    55,  795,  137,  816,   69,  780,  769,  193,   29,   27,
 /*   240 */    53,  781,  111,  784,  820,  752,  834,  529,  137,  816,
 /*   250 */   893,  725,   65,  848,  192,  527,  795,  144,   91,  877,
 /*   260 */   780,  769,  193,   11,  831,   53,  781,  834,  784,  820,
 /*   270 */    29,   27,  608,  137,  816,  893,  761,  151,  760,  529,
 /*   280 */   761,  834,  759,    1,  838,  830,  192,  527,  795,  144,
 /*   290 */    40,  182,  780,  769,  193,   11,  607,  116,  781,  829,
 /*   300 */   784,  718,  216,  215,    6,  603,  213,   29,   27,   44,
 /*   310 */    29,   27,  528,  530,  533,    1,  529,  881,  722,  529,
 /*   320 */   715,  839,  101,  603,  527,  541,  144,  527,  241,  144,
 /*   330 */    76,   74,   11,   75,  216,  879,  100,   30,   28,   26,
 /*   340 */    25,   24,    9,    8,  528,  530,  533,   30,   28,   26,
 /*   350 */    25,   24,    1,  161,  185,    7,  192,   86,  795,   41,
 /*   360 */     9,    8,  780,  769,  193,   21,   83,  123,  781,  155,
 /*   370 */   784,  216,  186,   80,  216,   30,   28,   26,   25,   24,
 /*   380 */   850,  528,  530,  533,  528,  530,  533,  192,  215,  795,
 /*   390 */   176,  106,  180,  780,  769,  193,  175,   77,   54,  781,
 /*   400 */   606,  784,  820,  722,  615,   75,  819,  816,  192,  175,
 /*   410 */   795,   59,  664,  665,  780,  769,  193,  584,  442,   54,
 /*   420 */   781,  538,  784,  820,   59,   57,  205,  179,  816,  182,
 /*   430 */   442,  189,  796,  187,  177,   70,  827,  828,   57,  832,
 /*   440 */   443,  444,   29,   27,  181,   96,  563,  578,   89,  827,
 /*   450 */   174,  529,  173,  215,   31,  881,  153,   29,   27,  527,
 /*   460 */   545,  144,   29,   27,  661,  662,  529,   31,  722,   74,
 /*   470 */     2,  529,  204,  879,  527,  184,  144,   50,  541,  527,
 /*   480 */   851,  144,  776,  667,  192,   61,  795,    7,  714,  774,
 /*   490 */   780,  769,  193,  201,  190,   54,  781,  200,  784,  820,
 /*   500 */   210,  199,    1,  208,  817,  127,  216,    7,  194,  211,
 /*   510 */    49,  128,  117,  861,  162,   46,  528,  530,  533,  159,
 /*   520 */   196,  216,   98,  103,   78,  533,  216,  198,  197,  157,
 /*   530 */    62,  528,  530,  533,  881,  484,  528,  530,  533,  192,
 /*   540 */   158,  795,   63,  860,  136,  780,  769,  193,   74,   82,
 /*   550 */   119,  781,  879,  784,  192,    5,  795,  479,  172,  841,
 /*   560 */   780,  769,  193,   85,   55,  123,  781,  138,  784,  156,
 /*   570 */   192,   68,  795,    4,   87,  603,  780,  769,  193,  512,
 /*   580 */    20,   66,  781,  171,  784,  537,  220,   58,  540,  573,
 /*   590 */   835,  516,  577,  192,   75,  795,  521,   32,   62,  780,
 /*   600 */   769,  193,   88,   63,  118,  781,  192,  784,  795,   64,
 /*   610 */    16,  191,  780,  769,  193,  802,   62,  120,  781,  140,
 /*   620 */   784,  895,  896,  878,  192,  710,  795,  188,  536,   95,
 /*   630 */   780,  769,  193,  195,   99,  114,  781,  203,  784,  192,
 /*   640 */   542,  795,  206,  145,  110,  780,  769,  193,   43,   45,
 /*   650 */   121,  781,  112,  784,  192,  708,  795,  723,  107,  218,
 /*   660 */   780,  769,  193,  271,  125,  115,  781,  126,  784,  113,
 /*   670 */     3,  192,  244,  795,   14,   31,  243,  780,  769,  193,
 /*   680 */    35,   79,  122,  781,  626,  784,  192,   81,  795,  628,
 /*   690 */    67,  245,  780,  769,  193,   84,   37,  792,  781,  622,
 /*   700 */   784,  192,  244,  795,  621,  166,  243,  780,  769,  193,
 /*   710 */   242,  774,  791,  781,  167,  784,  192,   38,  795,   18,
 /*   720 */   600,  245,  780,  769,  193,   15,  599,  790,  781,   90,
 /*   730 */   784,  192,   33,  795,   34,   73,    8,  780,  769,  193,
 /*   740 */   242,  564,  133,  781,  546,  784,  655,   17,  192,   39,
 /*   750 */   795,   12,  650,  649,  780,  769,  193,  141,  654,  132,
 /*   760 */   781,  192,  784,  795,  653,  142,   97,  780,  769,  193,
 /*   770 */    13,  175,  134,  781,  763,  784,  192,  685,  795,  762,
 /*   780 */   713,  712,  780,  769,  193,  684,   59,  131,  781,  632,
 /*   790 */   784,  192,  678,  795,  673,  451,  683,  780,  769,  193,
 /*   800 */    57,  102,  124,  781,  677,  784,  676,  672,  671,  207,
 /*   810 */    72,  827,  828,  670,  832,  160,  630,  631,  633,  634,
 /*   820 */   109,  209,   42,   30,   28,   26,   25,   24,   56,   46,
 /*   830 */   773,  531,  105,   36,  108,  217,  513,  505,  219,  504,
 /*   840 */   221,  149,  510,  223,  224,  226,  229,  507,  227,  232,
 /*   850 */   490,  501,  230,  503,  520,  499,  233,   51,  518,  519,
 /*   860 */   104,  449,   47,  246,  502,  470,  675,  469,  239,  548,
 /*   870 */   468,  467,   48,  466,  465,  464,  463,  462,  461,  460,
 /*   880 */   459,  458,  457,  456,  674,  455,  454,  265,  266,  669,
 /*   890 */   269,  270,
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
 /*    90 */    90,   36,   92,   93,   94,   95,   96,   97,   98,   99,
 /*   100 */   100,  101,   89,   90,  143,   92,   93,   94,   95,   96,
 /*   110 */    97,   98,   99,  100,  101,   31,  155,  156,   67,   68,
 /*   120 */    69,   70,   71,   72,   73,   74,   75,   76,   77,   21,
 /*   130 */    79,   80,   81,   82,   83,   84,  154,   29,  156,  185,
 /*   140 */   107,  159,  160,  161,  162,   31,   31,  165,  166,   36,
 /*   150 */   168,  169,  154,  199,  156,  173,  174,  203,  160,  161,
 /*   160 */   162,    0,  141,  165,  166,  143,  168,  185,  154,  148,
 /*   170 */   156,   57,   57,  152,  160,  161,  162,  155,  156,  165,
 /*   180 */   166,  199,  168,  169,   76,  203,   31,  173,  174,  175,
 /*   190 */   112,  113,  114,   31,   86,   87,   88,   36,  154,   31,
 /*   200 */   156,  187,  204,  205,  160,  161,  162,  193,  194,  165,
 /*   210 */   166,  167,  168,  154,  154,  156,  156,  136,  137,  160,
 /*   220 */   161,  162,  107,   58,  165,  166,  166,  168,  169,  154,
 /*   230 */    65,  156,  173,  174,  175,  160,  161,  162,   12,   13,
 /*   240 */   165,  166,  144,  168,  169,  147,  163,   21,  173,  174,
 /*   250 */   175,  155,  156,  194,  154,   29,  156,   31,  103,  184,
 /*   260 */   160,  161,  162,   37,  181,  165,  166,  163,  168,  169,
 /*   270 */    12,   13,   14,  173,  174,  175,  156,  153,  158,   21,
 /*   280 */   156,  163,  158,   57,  184,  181,  154,   29,  156,   31,
 /*   290 */   141,  159,  160,  161,  162,   37,    4,  165,  166,  181,
 /*   300 */   168,  152,   76,  139,  105,  106,  142,   12,   13,  138,
 /*   310 */    12,   13,   86,   87,   88,   57,   21,  185,  154,   21,
 /*   320 */   149,  104,   19,  106,   29,   31,   31,   29,   63,   31,
 /*   330 */    27,  199,   37,  107,   76,  203,   33,   12,   13,   14,
 /*   340 */    15,   16,    1,    2,   86,   87,   88,   12,   13,   14,
 /*   350 */    15,   16,   57,  197,    3,   57,  154,  190,  156,   56,
 /*   360 */     1,    2,  160,  161,  162,    2,   58,  165,  166,  167,
 /*   370 */   168,   76,   65,   65,   76,   12,   13,   14,   15,   16,
 /*   380 */   164,   86,   87,   88,   86,   87,   88,  154,  139,  156,
 /*   390 */   183,  142,   37,  160,  161,  162,  139,  103,  165,  166,
 /*   400 */   108,  168,  169,  154,   14,  107,  173,  174,  154,  139,
 /*   410 */   156,  154,  132,  133,  160,  161,  162,   58,   21,  165,
 /*   420 */   166,   31,  168,  169,  154,  168,   29,  173,  174,  159,
 /*   430 */    21,   65,  156,  126,  177,  178,  179,  180,  168,  182,
 /*   440 */    31,   32,   12,   13,   14,  200,   91,   58,  178,  179,
 /*   450 */   180,   21,  182,  139,   65,  185,  142,   12,   13,   29,
 /*   460 */    58,   31,   12,   13,  129,  130,   21,   65,  154,  199,
 /*   470 */   186,   21,  136,  203,   29,  124,   31,  138,   31,   29,
 /*   480 */   164,   31,   57,  134,  154,  146,  156,   57,  149,   64,
 /*   490 */   160,  161,  162,   26,  128,  165,  166,   30,  168,  169,
 /*   500 */    20,   34,   57,   23,  174,   18,   76,   57,  159,   22,
 /*   510 */    57,   24,   25,  196,  116,   62,   86,   87,   88,  115,
 /*   520 */    53,   76,   35,   58,  195,   88,   76,   60,   61,  161,
 /*   530 */    65,   86,   87,   88,  185,   58,   86,   87,   88,  154,
 /*   540 */   161,  156,   65,  196,  161,  160,  161,  162,  199,  195,
 /*   550 */   165,  166,  203,  168,  154,  123,  156,   58,  122,  192,
 /*   560 */   160,  161,  162,  191,   65,  165,  166,  167,  168,  110,
 /*   570 */   154,  189,  156,  109,  188,  106,  160,  161,  162,   58,
 /*   580 */    89,  165,  166,  198,  168,   31,   65,  154,   31,   98,
 /*   590 */   163,   58,  101,  154,  107,  156,   58,  102,   65,  160,
 /*   600 */   161,  162,  176,   65,  165,  166,  154,  168,  156,   58,
 /*   610 */    57,  127,  160,  161,  162,  172,   65,  165,  166,  131,
 /*   620 */   168,  205,  207,  202,  154,    0,  156,  125,   31,  201,
 /*   630 */   160,  161,  162,  139,  141,  165,  166,  139,  168,  154,
 /*   640 */    31,  156,  135,  135,  147,  160,  161,  162,  138,   57,
 /*   650 */   165,  166,  139,  168,  154,    0,  156,  154,  138,  150,
 /*   660 */   160,  161,  162,  135,  145,  165,  166,  145,  168,  140,
 /*   670 */    65,  154,   47,  156,  111,   65,   51,  160,  161,  162,
 /*   680 */    65,   58,  165,  166,   58,  168,  154,   57,  156,   58,
 /*   690 */    57,   66,  160,  161,  162,   57,   57,  165,  166,   58,
 /*   700 */   168,  154,   47,  156,   58,   29,   51,  160,  161,  162,
 /*   710 */    85,   64,  165,  166,   65,  168,  154,   57,  156,   65,
 /*   720 */    58,   66,  160,  161,  162,  111,   58,  165,  166,   64,
 /*   730 */   168,  154,  104,  156,   65,   64,    2,  160,  161,  162,
 /*   740 */    85,   91,  165,  166,   58,  168,   58,   65,  154,    4,
 /*   750 */   156,  111,   29,   29,  160,  161,  162,   29,   29,  165,
 /*   760 */   166,  154,  168,  156,   29,   29,   64,  160,  161,  162,
 /*   770 */    57,  139,  165,  166,    0,  168,  154,    0,  156,    0,
 /*   780 */     0,    0,  160,  161,  162,    0,  154,  165,  166,   91,
 /*   790 */   168,  154,    0,  156,    0,   38,    0,  160,  161,  162,
 /*   800 */   168,   19,  165,  166,    0,  168,    0,    0,    0,   21,
 /*   810 */   178,  179,  180,    0,  182,  117,  118,  119,  120,  121,
 /*   820 */    19,   21,   57,   12,   13,   14,   15,   16,   27,   62,
 /*   830 */    64,   21,   64,   57,   33,   63,   58,   78,   29,   78,
 /*   840 */    57,   29,   58,   29,   57,   29,   29,   58,   57,   29,
 /*   850 */    21,   58,   57,   78,   29,   58,   57,   56,   21,   29,
 /*   860 */    59,   38,   57,   37,   78,   29,    0,   29,   66,   58,
 /*   870 */    29,   29,   57,   29,   29,   29,   21,   29,   29,   29,
 /*   880 */    29,   29,   29,   29,    0,   29,   29,   29,   28,    0,
 /*   890 */    21,   20,  208,  208,  208,  208,  208,  208,  208,  208,
 /*   900 */   208,  208,  208,  208,  208,  208,  208,  208,  208,  208,
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
 /*  1020 */   208,  208,  208,  208,  208,  208,
};
#define YY_SHIFT_COUNT    (271)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (889)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   487,  226,  258,  295,  295,  295,  295,  298,  295,  295,
 /*    10 */   115,  445,  450,  430,  450,  450,  450,  450,  450,  450,
 /*    20 */   450,  450,  450,  450,  450,  450,  450,  450,  450,  450,
 /*    30 */   450,  450,  114,  114,  114,  108,  108,   84,   84,   33,
 /*    40 */   162,   55,  168,  162,  162,  168,  162,  168,  168,  168,
 /*    50 */   162,  113,    0,   13,   13,  108,  409,  155,  155,  155,
 /*    60 */     5,  161,  168,  168,  265,   51,  335,  698,   78,  294,
 /*    70 */   217,  199,  217,  390,  351,  292,  397,  447,  398,  404,
 /*    80 */   437,  437,  398,  404,  437,  432,  436,  459,  464,  469,
 /*    90 */   554,  557,  495,  553,  488,  484,  502,  168,  597,  597,
 /*   100 */    55,  609,  609,  265,  113,  554,  592,  597,  113,  609,
 /*   110 */   892,  892,  892,   30,   52,  363,  811,  467,  325,  325,
 /*   120 */   325,  325,  325,  325,  325,  625,  655,  801,  303,  359,
 /*   130 */   491,   72,   72,   72,   72,  165,  308,  341,  389,  355,
 /*   140 */   280,  307,  366,  402,  425,  480,  465,  477,  499,  521,
 /*   150 */   533,  538,  551,  453,  605,  610,  563,  623,  626,  630,
 /*   160 */   615,  631,  633,  638,  641,  639,  646,  676,  649,  647,
 /*   170 */   660,  654,  614,  662,  668,  665,  628,  669,  671,  734,
 /*   180 */   650,  686,  688,  682,  640,  745,  723,  724,  728,  729,
 /*   190 */   735,  736,  702,  713,  774,  777,  779,  780,  781,  785,
 /*   200 */   792,  794,  757,  796,  804,  806,  807,  808,  788,  813,
 /*   210 */   800,  782,  765,  767,  766,  768,  810,  776,  772,  778,
 /*   220 */   809,  812,  783,  784,  814,  787,  789,  816,  791,  793,
 /*   230 */   817,  795,  797,  820,  799,  759,  761,  775,  786,  829,
 /*   240 */   802,  805,  815,  825,  830,  837,  823,  826,  836,  838,
 /*   250 */   841,  842,  844,  845,  846,  855,  848,  849,  850,  851,
 /*   260 */   852,  853,  854,  856,  857,  866,  858,  860,  884,  889,
 /*   270 */   869,  871,
};
#define YY_REDUCE_COUNT (112)
#define YY_REDUCE_MIN   (-184)
#define YY_REDUCE_MAX   (637)
static const short yy_reduce_ofst[] = {
 /*     0 */   349,  -18,   14,   59, -122,   75,  100,  132,  233,  254,
 /*    10 */   270,  330,   -2,   44,  202,  385,  400,  416,  439,  452,
 /*    20 */   470,  485,  500,  517,  532,  547,  562,  577,  594,  607,
 /*    30 */   622,  637,  257, -119,  632, -151, -143, -135,   60,  -46,
 /*    40 */  -106,   21, -132,  -97,  164, -152,  249,  -39,  124,   22,
 /*    50 */   314,  339, -168, -168, -168, -105,   81,   83,  104,  118,
 /*    60 */   149,  171,   96,  120,   98, -150, -184,  156,  167,  216,
 /*    70 */   207,  207,  207,  276,  245,  284,  336,  316,  317,  329,
 /*    80 */   368,  379,  347,  354,  383,  367,  372,  382,  386,  207,
 /*    90 */   433,  427,  426,  443,  415,  421,  428,  276,  494,  498,
 /*   100 */   493,  507,  508,  497,  510,  503,  509,  513,  520,  528,
 /*   110 */   519,  522,  529,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*    10 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*    20 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*    30 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*    40 */   666,  689,  666,  666,  666,  666,  666,  666,  666,  666,
 /*    50 */   666,  687,  666,  822,  666,  666,  666,  833,  833,  833,
 /*    60 */   689,  687,  666,  666,  751,  666,  897,  666,  857,  849,
 /*    70 */   825,  839,  826,  666,  882,  842,  666,  666,  864,  862,
 /*    80 */   666,  666,  864,  862,  666,  876,  872,  855,  853,  839,
 /*    90 */   666,  666,  666,  666,  900,  888,  884,  666,  666,  666,
 /*   100 */   689,  666,  666,  666,  687,  666,  720,  666,  687,  666,
 /*   110 */   754,  754,  690,  666,  666,  666,  666,  666,  875,  874,
 /*   120 */   799,  798,  797,  793,  794,  666,  666,  666,  666,  666,
 /*   130 */   666,  788,  789,  787,  786,  666,  666,  823,  666,  666,
 /*   140 */   666,  885,  889,  666,  775,  666,  666,  666,  666,  666,
 /*   150 */   666,  666,  666,  666,  846,  856,  666,  666,  666,  666,
 /*   160 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  775,
 /*   170 */   666,  873,  666,  832,  828,  666,  666,  824,  666,  818,
 /*   180 */   666,  666,  666,  883,  666,  666,  666,  666,  666,  666,
 /*   190 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*   200 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*   210 */   666,  666,  666,  666,  774,  666,  666,  666,  666,  666,
 /*   220 */   666,  666,  748,  666,  666,  666,  666,  666,  666,  666,
 /*   230 */   666,  666,  666,  666,  666,  733,  731,  730,  729,  666,
 /*   240 */   726,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*   250 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*   260 */   666,  666,  666,  666,  666,  666,  666,  666,  666,  666,
 /*   270 */   666,  666,
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
 /*  43 */ "cmd ::= SHOW TABLES",
 /*  44 */ "cmd ::= SHOW STABLES",
 /*  45 */ "multi_create_clause ::= create_subtable_clause",
 /*  46 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /*  47 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /*  48 */ "multi_drop_clause ::= drop_table_clause",
 /*  49 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /*  50 */ "drop_table_clause ::= exists_opt full_table_name",
 /*  51 */ "specific_tags_opt ::=",
 /*  52 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /*  53 */ "full_table_name ::= table_name",
 /*  54 */ "full_table_name ::= db_name NK_DOT table_name",
 /*  55 */ "column_def_list ::= column_def",
 /*  56 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /*  57 */ "column_def ::= column_name type_name",
 /*  58 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /*  59 */ "type_name ::= BOOL",
 /*  60 */ "type_name ::= TINYINT",
 /*  61 */ "type_name ::= SMALLINT",
 /*  62 */ "type_name ::= INT",
 /*  63 */ "type_name ::= INTEGER",
 /*  64 */ "type_name ::= BIGINT",
 /*  65 */ "type_name ::= FLOAT",
 /*  66 */ "type_name ::= DOUBLE",
 /*  67 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /*  68 */ "type_name ::= TIMESTAMP",
 /*  69 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /*  70 */ "type_name ::= TINYINT UNSIGNED",
 /*  71 */ "type_name ::= SMALLINT UNSIGNED",
 /*  72 */ "type_name ::= INT UNSIGNED",
 /*  73 */ "type_name ::= BIGINT UNSIGNED",
 /*  74 */ "type_name ::= JSON",
 /*  75 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /*  76 */ "type_name ::= MEDIUMBLOB",
 /*  77 */ "type_name ::= BLOB",
 /*  78 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /*  79 */ "type_name ::= DECIMAL",
 /*  80 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /*  81 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /*  82 */ "tags_def_opt ::=",
 /*  83 */ "tags_def_opt ::= tags_def",
 /*  84 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /*  85 */ "table_options ::=",
 /*  86 */ "table_options ::= table_options COMMENT NK_STRING",
 /*  87 */ "table_options ::= table_options KEEP NK_INTEGER",
 /*  88 */ "table_options ::= table_options TTL NK_INTEGER",
 /*  89 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /*  90 */ "col_name_list ::= col_name",
 /*  91 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /*  92 */ "col_name ::= column_name",
 /*  93 */ "cmd ::= SHOW VGROUPS",
 /*  94 */ "cmd ::= query_expression",
 /*  95 */ "literal ::= NK_INTEGER",
 /*  96 */ "literal ::= NK_FLOAT",
 /*  97 */ "literal ::= NK_STRING",
 /*  98 */ "literal ::= NK_BOOL",
 /*  99 */ "literal ::= TIMESTAMP NK_STRING",
 /* 100 */ "literal ::= duration_literal",
 /* 101 */ "duration_literal ::= NK_VARIABLE",
 /* 102 */ "literal_list ::= literal",
 /* 103 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 104 */ "db_name ::= NK_ID",
 /* 105 */ "table_name ::= NK_ID",
 /* 106 */ "column_name ::= NK_ID",
 /* 107 */ "function_name ::= NK_ID",
 /* 108 */ "table_alias ::= NK_ID",
 /* 109 */ "column_alias ::= NK_ID",
 /* 110 */ "user_name ::= NK_ID",
 /* 111 */ "expression ::= literal",
 /* 112 */ "expression ::= column_reference",
 /* 113 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 114 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 115 */ "expression ::= subquery",
 /* 116 */ "expression ::= NK_LP expression NK_RP",
 /* 117 */ "expression ::= NK_PLUS expression",
 /* 118 */ "expression ::= NK_MINUS expression",
 /* 119 */ "expression ::= expression NK_PLUS expression",
 /* 120 */ "expression ::= expression NK_MINUS expression",
 /* 121 */ "expression ::= expression NK_STAR expression",
 /* 122 */ "expression ::= expression NK_SLASH expression",
 /* 123 */ "expression ::= expression NK_REM expression",
 /* 124 */ "expression_list ::= expression",
 /* 125 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 126 */ "column_reference ::= column_name",
 /* 127 */ "column_reference ::= table_name NK_DOT column_name",
 /* 128 */ "predicate ::= expression compare_op expression",
 /* 129 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 130 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 131 */ "predicate ::= expression IS NULL",
 /* 132 */ "predicate ::= expression IS NOT NULL",
 /* 133 */ "predicate ::= expression in_op in_predicate_value",
 /* 134 */ "compare_op ::= NK_LT",
 /* 135 */ "compare_op ::= NK_GT",
 /* 136 */ "compare_op ::= NK_LE",
 /* 137 */ "compare_op ::= NK_GE",
 /* 138 */ "compare_op ::= NK_NE",
 /* 139 */ "compare_op ::= NK_EQ",
 /* 140 */ "compare_op ::= LIKE",
 /* 141 */ "compare_op ::= NOT LIKE",
 /* 142 */ "compare_op ::= MATCH",
 /* 143 */ "compare_op ::= NMATCH",
 /* 144 */ "in_op ::= IN",
 /* 145 */ "in_op ::= NOT IN",
 /* 146 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 147 */ "boolean_value_expression ::= boolean_primary",
 /* 148 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 149 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 150 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 151 */ "boolean_primary ::= predicate",
 /* 152 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 153 */ "common_expression ::= expression",
 /* 154 */ "common_expression ::= boolean_value_expression",
 /* 155 */ "from_clause ::= FROM table_reference_list",
 /* 156 */ "table_reference_list ::= table_reference",
 /* 157 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 158 */ "table_reference ::= table_primary",
 /* 159 */ "table_reference ::= joined_table",
 /* 160 */ "table_primary ::= table_name alias_opt",
 /* 161 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 162 */ "table_primary ::= subquery alias_opt",
 /* 163 */ "table_primary ::= parenthesized_joined_table",
 /* 164 */ "alias_opt ::=",
 /* 165 */ "alias_opt ::= table_alias",
 /* 166 */ "alias_opt ::= AS table_alias",
 /* 167 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 168 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 169 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 170 */ "join_type ::=",
 /* 171 */ "join_type ::= INNER",
 /* 172 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 173 */ "set_quantifier_opt ::=",
 /* 174 */ "set_quantifier_opt ::= DISTINCT",
 /* 175 */ "set_quantifier_opt ::= ALL",
 /* 176 */ "select_list ::= NK_STAR",
 /* 177 */ "select_list ::= select_sublist",
 /* 178 */ "select_sublist ::= select_item",
 /* 179 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 180 */ "select_item ::= common_expression",
 /* 181 */ "select_item ::= common_expression column_alias",
 /* 182 */ "select_item ::= common_expression AS column_alias",
 /* 183 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 184 */ "where_clause_opt ::=",
 /* 185 */ "where_clause_opt ::= WHERE search_condition",
 /* 186 */ "partition_by_clause_opt ::=",
 /* 187 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 188 */ "twindow_clause_opt ::=",
 /* 189 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 190 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 191 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 192 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 193 */ "sliding_opt ::=",
 /* 194 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 195 */ "fill_opt ::=",
 /* 196 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 197 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 198 */ "fill_mode ::= NONE",
 /* 199 */ "fill_mode ::= PREV",
 /* 200 */ "fill_mode ::= NULL",
 /* 201 */ "fill_mode ::= LINEAR",
 /* 202 */ "fill_mode ::= NEXT",
 /* 203 */ "group_by_clause_opt ::=",
 /* 204 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 205 */ "group_by_list ::= expression",
 /* 206 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 207 */ "having_clause_opt ::=",
 /* 208 */ "having_clause_opt ::= HAVING search_condition",
 /* 209 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 210 */ "query_expression_body ::= query_primary",
 /* 211 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 212 */ "query_primary ::= query_specification",
 /* 213 */ "order_by_clause_opt ::=",
 /* 214 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 215 */ "slimit_clause_opt ::=",
 /* 216 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 217 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 218 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 219 */ "limit_clause_opt ::=",
 /* 220 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 221 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 222 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 223 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 224 */ "search_condition ::= common_expression",
 /* 225 */ "sort_specification_list ::= sort_specification",
 /* 226 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 227 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 228 */ "ordering_specification_opt ::=",
 /* 229 */ "ordering_specification_opt ::= ASC",
 /* 230 */ "ordering_specification_opt ::= DESC",
 /* 231 */ "null_ordering_opt ::=",
 /* 232 */ "null_ordering_opt ::= NULLS FIRST",
 /* 233 */ "null_ordering_opt ::= NULLS LAST",
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
  {  134,   -2 }, /* (43) cmd ::= SHOW TABLES */
  {  134,   -2 }, /* (44) cmd ::= SHOW STABLES */
  {  146,   -1 }, /* (45) multi_create_clause ::= create_subtable_clause */
  {  146,   -2 }, /* (46) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  149,   -9 }, /* (47) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  148,   -1 }, /* (48) multi_drop_clause ::= drop_table_clause */
  {  148,   -2 }, /* (49) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  152,   -2 }, /* (50) drop_table_clause ::= exists_opt full_table_name */
  {  150,    0 }, /* (51) specific_tags_opt ::= */
  {  150,   -3 }, /* (52) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  142,   -1 }, /* (53) full_table_name ::= table_name */
  {  142,   -3 }, /* (54) full_table_name ::= db_name NK_DOT table_name */
  {  143,   -1 }, /* (55) column_def_list ::= column_def */
  {  143,   -3 }, /* (56) column_def_list ::= column_def_list NK_COMMA column_def */
  {  155,   -2 }, /* (57) column_def ::= column_name type_name */
  {  155,   -4 }, /* (58) column_def ::= column_name type_name COMMENT NK_STRING */
  {  157,   -1 }, /* (59) type_name ::= BOOL */
  {  157,   -1 }, /* (60) type_name ::= TINYINT */
  {  157,   -1 }, /* (61) type_name ::= SMALLINT */
  {  157,   -1 }, /* (62) type_name ::= INT */
  {  157,   -1 }, /* (63) type_name ::= INTEGER */
  {  157,   -1 }, /* (64) type_name ::= BIGINT */
  {  157,   -1 }, /* (65) type_name ::= FLOAT */
  {  157,   -1 }, /* (66) type_name ::= DOUBLE */
  {  157,   -4 }, /* (67) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (68) type_name ::= TIMESTAMP */
  {  157,   -4 }, /* (69) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  157,   -2 }, /* (70) type_name ::= TINYINT UNSIGNED */
  {  157,   -2 }, /* (71) type_name ::= SMALLINT UNSIGNED */
  {  157,   -2 }, /* (72) type_name ::= INT UNSIGNED */
  {  157,   -2 }, /* (73) type_name ::= BIGINT UNSIGNED */
  {  157,   -1 }, /* (74) type_name ::= JSON */
  {  157,   -4 }, /* (75) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (76) type_name ::= MEDIUMBLOB */
  {  157,   -1 }, /* (77) type_name ::= BLOB */
  {  157,   -4 }, /* (78) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  157,   -1 }, /* (79) type_name ::= DECIMAL */
  {  157,   -4 }, /* (80) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  157,   -6 }, /* (81) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  144,    0 }, /* (82) tags_def_opt ::= */
  {  144,   -1 }, /* (83) tags_def_opt ::= tags_def */
  {  147,   -4 }, /* (84) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  145,    0 }, /* (85) table_options ::= */
  {  145,   -3 }, /* (86) table_options ::= table_options COMMENT NK_STRING */
  {  145,   -3 }, /* (87) table_options ::= table_options KEEP NK_INTEGER */
  {  145,   -3 }, /* (88) table_options ::= table_options TTL NK_INTEGER */
  {  145,   -5 }, /* (89) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  153,   -1 }, /* (90) col_name_list ::= col_name */
  {  153,   -3 }, /* (91) col_name_list ::= col_name_list NK_COMMA col_name */
  {  158,   -1 }, /* (92) col_name ::= column_name */
  {  134,   -2 }, /* (93) cmd ::= SHOW VGROUPS */
  {  134,   -1 }, /* (94) cmd ::= query_expression */
  {  160,   -1 }, /* (95) literal ::= NK_INTEGER */
  {  160,   -1 }, /* (96) literal ::= NK_FLOAT */
  {  160,   -1 }, /* (97) literal ::= NK_STRING */
  {  160,   -1 }, /* (98) literal ::= NK_BOOL */
  {  160,   -2 }, /* (99) literal ::= TIMESTAMP NK_STRING */
  {  160,   -1 }, /* (100) literal ::= duration_literal */
  {  161,   -1 }, /* (101) duration_literal ::= NK_VARIABLE */
  {  151,   -1 }, /* (102) literal_list ::= literal */
  {  151,   -3 }, /* (103) literal_list ::= literal_list NK_COMMA literal */
  {  139,   -1 }, /* (104) db_name ::= NK_ID */
  {  154,   -1 }, /* (105) table_name ::= NK_ID */
  {  156,   -1 }, /* (106) column_name ::= NK_ID */
  {  162,   -1 }, /* (107) function_name ::= NK_ID */
  {  163,   -1 }, /* (108) table_alias ::= NK_ID */
  {  164,   -1 }, /* (109) column_alias ::= NK_ID */
  {  135,   -1 }, /* (110) user_name ::= NK_ID */
  {  165,   -1 }, /* (111) expression ::= literal */
  {  165,   -1 }, /* (112) expression ::= column_reference */
  {  165,   -4 }, /* (113) expression ::= function_name NK_LP expression_list NK_RP */
  {  165,   -4 }, /* (114) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  165,   -1 }, /* (115) expression ::= subquery */
  {  165,   -3 }, /* (116) expression ::= NK_LP expression NK_RP */
  {  165,   -2 }, /* (117) expression ::= NK_PLUS expression */
  {  165,   -2 }, /* (118) expression ::= NK_MINUS expression */
  {  165,   -3 }, /* (119) expression ::= expression NK_PLUS expression */
  {  165,   -3 }, /* (120) expression ::= expression NK_MINUS expression */
  {  165,   -3 }, /* (121) expression ::= expression NK_STAR expression */
  {  165,   -3 }, /* (122) expression ::= expression NK_SLASH expression */
  {  165,   -3 }, /* (123) expression ::= expression NK_REM expression */
  {  167,   -1 }, /* (124) expression_list ::= expression */
  {  167,   -3 }, /* (125) expression_list ::= expression_list NK_COMMA expression */
  {  166,   -1 }, /* (126) column_reference ::= column_name */
  {  166,   -3 }, /* (127) column_reference ::= table_name NK_DOT column_name */
  {  169,   -3 }, /* (128) predicate ::= expression compare_op expression */
  {  169,   -5 }, /* (129) predicate ::= expression BETWEEN expression AND expression */
  {  169,   -6 }, /* (130) predicate ::= expression NOT BETWEEN expression AND expression */
  {  169,   -3 }, /* (131) predicate ::= expression IS NULL */
  {  169,   -4 }, /* (132) predicate ::= expression IS NOT NULL */
  {  169,   -3 }, /* (133) predicate ::= expression in_op in_predicate_value */
  {  170,   -1 }, /* (134) compare_op ::= NK_LT */
  {  170,   -1 }, /* (135) compare_op ::= NK_GT */
  {  170,   -1 }, /* (136) compare_op ::= NK_LE */
  {  170,   -1 }, /* (137) compare_op ::= NK_GE */
  {  170,   -1 }, /* (138) compare_op ::= NK_NE */
  {  170,   -1 }, /* (139) compare_op ::= NK_EQ */
  {  170,   -1 }, /* (140) compare_op ::= LIKE */
  {  170,   -2 }, /* (141) compare_op ::= NOT LIKE */
  {  170,   -1 }, /* (142) compare_op ::= MATCH */
  {  170,   -1 }, /* (143) compare_op ::= NMATCH */
  {  171,   -1 }, /* (144) in_op ::= IN */
  {  171,   -2 }, /* (145) in_op ::= NOT IN */
  {  172,   -3 }, /* (146) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  173,   -1 }, /* (147) boolean_value_expression ::= boolean_primary */
  {  173,   -2 }, /* (148) boolean_value_expression ::= NOT boolean_primary */
  {  173,   -3 }, /* (149) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  173,   -3 }, /* (150) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  174,   -1 }, /* (151) boolean_primary ::= predicate */
  {  174,   -3 }, /* (152) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  175,   -1 }, /* (153) common_expression ::= expression */
  {  175,   -1 }, /* (154) common_expression ::= boolean_value_expression */
  {  176,   -2 }, /* (155) from_clause ::= FROM table_reference_list */
  {  177,   -1 }, /* (156) table_reference_list ::= table_reference */
  {  177,   -3 }, /* (157) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  178,   -1 }, /* (158) table_reference ::= table_primary */
  {  178,   -1 }, /* (159) table_reference ::= joined_table */
  {  179,   -2 }, /* (160) table_primary ::= table_name alias_opt */
  {  179,   -4 }, /* (161) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  179,   -2 }, /* (162) table_primary ::= subquery alias_opt */
  {  179,   -1 }, /* (163) table_primary ::= parenthesized_joined_table */
  {  181,    0 }, /* (164) alias_opt ::= */
  {  181,   -1 }, /* (165) alias_opt ::= table_alias */
  {  181,   -2 }, /* (166) alias_opt ::= AS table_alias */
  {  182,   -3 }, /* (167) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  182,   -3 }, /* (168) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  180,   -6 }, /* (169) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  183,    0 }, /* (170) join_type ::= */
  {  183,   -1 }, /* (171) join_type ::= INNER */
  {  185,   -9 }, /* (172) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  186,    0 }, /* (173) set_quantifier_opt ::= */
  {  186,   -1 }, /* (174) set_quantifier_opt ::= DISTINCT */
  {  186,   -1 }, /* (175) set_quantifier_opt ::= ALL */
  {  187,   -1 }, /* (176) select_list ::= NK_STAR */
  {  187,   -1 }, /* (177) select_list ::= select_sublist */
  {  193,   -1 }, /* (178) select_sublist ::= select_item */
  {  193,   -3 }, /* (179) select_sublist ::= select_sublist NK_COMMA select_item */
  {  194,   -1 }, /* (180) select_item ::= common_expression */
  {  194,   -2 }, /* (181) select_item ::= common_expression column_alias */
  {  194,   -3 }, /* (182) select_item ::= common_expression AS column_alias */
  {  194,   -3 }, /* (183) select_item ::= table_name NK_DOT NK_STAR */
  {  188,    0 }, /* (184) where_clause_opt ::= */
  {  188,   -2 }, /* (185) where_clause_opt ::= WHERE search_condition */
  {  189,    0 }, /* (186) partition_by_clause_opt ::= */
  {  189,   -3 }, /* (187) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  190,    0 }, /* (188) twindow_clause_opt ::= */
  {  190,   -6 }, /* (189) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  190,   -4 }, /* (190) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  190,   -6 }, /* (191) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  190,   -8 }, /* (192) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  195,    0 }, /* (193) sliding_opt ::= */
  {  195,   -4 }, /* (194) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  196,    0 }, /* (195) fill_opt ::= */
  {  196,   -4 }, /* (196) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  196,   -6 }, /* (197) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  197,   -1 }, /* (198) fill_mode ::= NONE */
  {  197,   -1 }, /* (199) fill_mode ::= PREV */
  {  197,   -1 }, /* (200) fill_mode ::= NULL */
  {  197,   -1 }, /* (201) fill_mode ::= LINEAR */
  {  197,   -1 }, /* (202) fill_mode ::= NEXT */
  {  191,    0 }, /* (203) group_by_clause_opt ::= */
  {  191,   -3 }, /* (204) group_by_clause_opt ::= GROUP BY group_by_list */
  {  198,   -1 }, /* (205) group_by_list ::= expression */
  {  198,   -3 }, /* (206) group_by_list ::= group_by_list NK_COMMA expression */
  {  192,    0 }, /* (207) having_clause_opt ::= */
  {  192,   -2 }, /* (208) having_clause_opt ::= HAVING search_condition */
  {  159,   -4 }, /* (209) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  199,   -1 }, /* (210) query_expression_body ::= query_primary */
  {  199,   -4 }, /* (211) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  203,   -1 }, /* (212) query_primary ::= query_specification */
  {  200,    0 }, /* (213) order_by_clause_opt ::= */
  {  200,   -3 }, /* (214) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  201,    0 }, /* (215) slimit_clause_opt ::= */
  {  201,   -2 }, /* (216) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  201,   -4 }, /* (217) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  201,   -4 }, /* (218) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  202,    0 }, /* (219) limit_clause_opt ::= */
  {  202,   -2 }, /* (220) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  202,   -4 }, /* (221) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  202,   -4 }, /* (222) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  168,   -3 }, /* (223) subquery ::= NK_LP query_expression NK_RP */
  {  184,   -1 }, /* (224) search_condition ::= common_expression */
  {  204,   -1 }, /* (225) sort_specification_list ::= sort_specification */
  {  204,   -3 }, /* (226) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  205,   -3 }, /* (227) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  206,    0 }, /* (228) ordering_specification_opt ::= */
  {  206,   -1 }, /* (229) ordering_specification_opt ::= ASC */
  {  206,   -1 }, /* (230) ordering_specification_opt ::= DESC */
  {  207,    0 }, /* (231) null_ordering_opt ::= */
  {  207,   -2 }, /* (232) null_ordering_opt ::= NULLS FIRST */
  {  207,   -2 }, /* (233) null_ordering_opt ::= NULLS LAST */
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
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT); }
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
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT); }
        break;
      case 10: /* dnode_endpoint ::= NK_STRING */
      case 11: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==11);
      case 12: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==12);
      case 104: /* db_name ::= NK_ID */ yytestcase(yyruleno==104);
      case 105: /* table_name ::= NK_ID */ yytestcase(yyruleno==105);
      case 106: /* column_name ::= NK_ID */ yytestcase(yyruleno==106);
      case 107: /* function_name ::= NK_ID */ yytestcase(yyruleno==107);
      case 108: /* table_alias ::= NK_ID */ yytestcase(yyruleno==108);
      case 109: /* column_alias ::= NK_ID */ yytestcase(yyruleno==109);
      case 110: /* user_name ::= NK_ID */ yytestcase(yyruleno==110);
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
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT); }
        break;
      case 16: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy161);}
        break;
      case 17: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy377 = true; }
        break;
      case 18: /* not_exists_opt ::= */
      case 20: /* exists_opt ::= */ yytestcase(yyruleno==20);
      case 173: /* set_quantifier_opt ::= */ yytestcase(yyruleno==173);
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
      case 43: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT); }
        break;
      case 44: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT); }
        break;
      case 45: /* multi_create_clause ::= create_subtable_clause */
      case 48: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==48);
      case 55: /* column_def_list ::= column_def */ yytestcase(yyruleno==55);
      case 90: /* col_name_list ::= col_name */ yytestcase(yyruleno==90);
      case 178: /* select_sublist ::= select_item */ yytestcase(yyruleno==178);
      case 225: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==225);
{ yylhsminor.yy184 = createNodeList(pCxt, yymsp[0].minor.yy392); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 46: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 49: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==49);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-1].minor.yy184, yymsp[0].minor.yy392); }
  yymsp[-1].minor.yy184 = yylhsminor.yy184;
        break;
      case 47: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy392 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy377, yymsp[-7].minor.yy392, yymsp[-5].minor.yy392, yymsp[-4].minor.yy184, yymsp[-1].minor.yy184); }
  yymsp[-8].minor.yy392 = yylhsminor.yy392;
        break;
      case 50: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy392 = createDropTableClause(pCxt, yymsp[-1].minor.yy377, yymsp[0].minor.yy392); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 51: /* specific_tags_opt ::= */
      case 82: /* tags_def_opt ::= */ yytestcase(yyruleno==82);
      case 186: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==186);
      case 203: /* group_by_clause_opt ::= */ yytestcase(yyruleno==203);
      case 213: /* order_by_clause_opt ::= */ yytestcase(yyruleno==213);
{ yymsp[1].minor.yy184 = NULL; }
        break;
      case 52: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy184 = yymsp[-1].minor.yy184; }
        break;
      case 53: /* full_table_name ::= table_name */
{ yylhsminor.yy392 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy161, NULL); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 54: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy392 = createRealTableNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161, NULL); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 56: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 91: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==91);
      case 179: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==179);
      case 226: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==226);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, yymsp[0].minor.yy392); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 57: /* column_def ::= column_name type_name */
{ yylhsminor.yy392 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy161, yymsp[0].minor.yy240, NULL); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 58: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy392 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy161, yymsp[-2].minor.yy240, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 59: /* type_name ::= BOOL */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 60: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 61: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 62: /* type_name ::= INT */
      case 63: /* type_name ::= INTEGER */ yytestcase(yyruleno==63);
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 64: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 65: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 66: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 67: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 68: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 69: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 70: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 71: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 72: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 73: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy240 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 74: /* type_name ::= JSON */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 75: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 76: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 77: /* type_name ::= BLOB */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 78: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 79: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 80: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 81: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy240 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 83: /* tags_def_opt ::= tags_def */
      case 177: /* select_list ::= select_sublist */ yytestcase(yyruleno==177);
{ yylhsminor.yy184 = yymsp[0].minor.yy184; }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 84: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy184 = yymsp[-1].minor.yy184; }
        break;
      case 85: /* table_options ::= */
{ yymsp[1].minor.yy334 = createDefaultTableOptions(pCxt);}
        break;
      case 86: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 87: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 88: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy334 = setTableOption(pCxt, yymsp[-2].minor.yy334, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy334 = yylhsminor.yy334;
        break;
      case 89: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy334 = setTableSmaOption(pCxt, yymsp[-4].minor.yy334, yymsp[-1].minor.yy184); }
  yymsp[-4].minor.yy334 = yylhsminor.yy334;
        break;
      case 92: /* col_name ::= column_name */
{ yylhsminor.yy392 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy161); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 93: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT); }
        break;
      case 94: /* cmd ::= query_expression */
{ pCxt->pRootNode = yymsp[0].minor.yy392; }
        break;
      case 95: /* literal ::= NK_INTEGER */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 96: /* literal ::= NK_FLOAT */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 97: /* literal ::= NK_STRING */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 98: /* literal ::= NK_BOOL */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 99: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 100: /* literal ::= duration_literal */
      case 111: /* expression ::= literal */ yytestcase(yyruleno==111);
      case 112: /* expression ::= column_reference */ yytestcase(yyruleno==112);
      case 115: /* expression ::= subquery */ yytestcase(yyruleno==115);
      case 147: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==147);
      case 151: /* boolean_primary ::= predicate */ yytestcase(yyruleno==151);
      case 153: /* common_expression ::= expression */ yytestcase(yyruleno==153);
      case 154: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==154);
      case 156: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==156);
      case 158: /* table_reference ::= table_primary */ yytestcase(yyruleno==158);
      case 159: /* table_reference ::= joined_table */ yytestcase(yyruleno==159);
      case 163: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==163);
      case 210: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==210);
      case 212: /* query_primary ::= query_specification */ yytestcase(yyruleno==212);
{ yylhsminor.yy392 = yymsp[0].minor.yy392; }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 101: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 102: /* literal_list ::= literal */
      case 124: /* expression_list ::= expression */ yytestcase(yyruleno==124);
{ yylhsminor.yy184 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392)); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 103: /* literal_list ::= literal_list NK_COMMA literal */
      case 125: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==125);
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, releaseRawExprNode(pCxt, yymsp[0].minor.yy392)); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 113: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy161, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy161, yymsp[-1].minor.yy184)); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 114: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy161, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy161, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 116: /* expression ::= NK_LP expression NK_RP */
      case 152: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==152);
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 117: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy392));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 118: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), NULL));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 119: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 120: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 121: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 122: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 123: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); 
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 126: /* column_reference ::= column_name */
{ yylhsminor.yy392 = createRawExprNode(pCxt, &yymsp[0].minor.yy161, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy161)); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 127: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161, createColumnNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy161)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 128: /* predicate ::= expression compare_op expression */
      case 133: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==133);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy220, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 129: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy392), releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-4].minor.yy392 = yylhsminor.yy392;
        break;
      case 130: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[-5].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-5].minor.yy392 = yylhsminor.yy392;
        break;
      case 131: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), NULL));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 132: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy392), NULL));
                                                                                  }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 134: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy220 = OP_TYPE_LOWER_THAN; }
        break;
      case 135: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy220 = OP_TYPE_GREATER_THAN; }
        break;
      case 136: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy220 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 137: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy220 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 138: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy220 = OP_TYPE_NOT_EQUAL; }
        break;
      case 139: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy220 = OP_TYPE_EQUAL; }
        break;
      case 140: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy220 = OP_TYPE_LIKE; }
        break;
      case 141: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy220 = OP_TYPE_NOT_LIKE; }
        break;
      case 142: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy220 = OP_TYPE_MATCH; }
        break;
      case 143: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy220 = OP_TYPE_NMATCH; }
        break;
      case 144: /* in_op ::= IN */
{ yymsp[0].minor.yy220 = OP_TYPE_IN; }
        break;
      case 145: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy220 = OP_TYPE_NOT_IN; }
        break;
      case 146: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy184)); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 148: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), NULL));
                                                                                  }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 149: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 150: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy392);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), releaseRawExprNode(pCxt, yymsp[0].minor.yy392)));
                                                                                  }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 155: /* from_clause ::= FROM table_reference_list */
      case 185: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==185);
      case 208: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==208);
{ yymsp[-1].minor.yy392 = yymsp[0].minor.yy392; }
        break;
      case 157: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy392 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy392, yymsp[0].minor.yy392, NULL); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 160: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy392 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy161, &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 161: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy392 = createRealTableNode(pCxt, &yymsp[-3].minor.yy161, &yymsp[-1].minor.yy161, &yymsp[0].minor.yy161); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 162: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy392 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 164: /* alias_opt ::= */
{ yymsp[1].minor.yy161 = nil_token;  }
        break;
      case 165: /* alias_opt ::= table_alias */
{ yylhsminor.yy161 = yymsp[0].minor.yy161; }
  yymsp[0].minor.yy161 = yylhsminor.yy161;
        break;
      case 166: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy161 = yymsp[0].minor.yy161; }
        break;
      case 167: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 168: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==168);
{ yymsp[-2].minor.yy392 = yymsp[-1].minor.yy392; }
        break;
      case 169: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy392 = createJoinTableNode(pCxt, yymsp[-4].minor.yy308, yymsp[-5].minor.yy392, yymsp[-2].minor.yy392, yymsp[0].minor.yy392); }
  yymsp[-5].minor.yy392 = yylhsminor.yy392;
        break;
      case 170: /* join_type ::= */
{ yymsp[1].minor.yy308 = JOIN_TYPE_INNER; }
        break;
      case 171: /* join_type ::= INNER */
{ yymsp[0].minor.yy308 = JOIN_TYPE_INNER; }
        break;
      case 172: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy392 = createSelectStmt(pCxt, yymsp[-7].minor.yy377, yymsp[-6].minor.yy184, yymsp[-5].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addWhereClause(pCxt, yymsp[-8].minor.yy392, yymsp[-4].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addPartitionByClause(pCxt, yymsp[-8].minor.yy392, yymsp[-3].minor.yy184);
                                                                                    yymsp[-8].minor.yy392 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy392, yymsp[-2].minor.yy392);
                                                                                    yymsp[-8].minor.yy392 = addGroupByClause(pCxt, yymsp[-8].minor.yy392, yymsp[-1].minor.yy184);
                                                                                    yymsp[-8].minor.yy392 = addHavingClause(pCxt, yymsp[-8].minor.yy392, yymsp[0].minor.yy392);
                                                                                  }
        break;
      case 174: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy377 = true; }
        break;
      case 175: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy377 = false; }
        break;
      case 176: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy184 = NULL; }
        break;
      case 180: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy392);
                                                                                    yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392), &t);
                                                                                  }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 181: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-1].minor.yy392 = yylhsminor.yy392;
        break;
      case 182: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy392 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), &yymsp[0].minor.yy161); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 183: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy392 = createColumnNode(pCxt, &yymsp[-2].minor.yy161, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 184: /* where_clause_opt ::= */
      case 188: /* twindow_clause_opt ::= */ yytestcase(yyruleno==188);
      case 193: /* sliding_opt ::= */ yytestcase(yyruleno==193);
      case 195: /* fill_opt ::= */ yytestcase(yyruleno==195);
      case 207: /* having_clause_opt ::= */ yytestcase(yyruleno==207);
      case 215: /* slimit_clause_opt ::= */ yytestcase(yyruleno==215);
      case 219: /* limit_clause_opt ::= */ yytestcase(yyruleno==219);
{ yymsp[1].minor.yy392 = NULL; }
        break;
      case 187: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 204: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==204);
      case 214: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==214);
{ yymsp[-2].minor.yy184 = yymsp[0].minor.yy184; }
        break;
      case 189: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy392 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy392), &yymsp[-1].minor.yy0); }
        break;
      case 190: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy392 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy392)); }
        break;
      case 191: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy392 = createIntervalWindowNode(pCxt, yymsp[-3].minor.yy392, NULL, yymsp[-1].minor.yy392, yymsp[0].minor.yy392); }
        break;
      case 192: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy392 = createIntervalWindowNode(pCxt, yymsp[-5].minor.yy392, yymsp[-3].minor.yy392, yymsp[-1].minor.yy392, yymsp[0].minor.yy392); }
        break;
      case 194: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy392 = yymsp[-1].minor.yy392; }
        break;
      case 196: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy392 = createFillNode(pCxt, yymsp[-1].minor.yy166, NULL); }
        break;
      case 197: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy392 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy184)); }
        break;
      case 198: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy166 = FILL_MODE_NONE; }
        break;
      case 199: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy166 = FILL_MODE_PREV; }
        break;
      case 200: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy166 = FILL_MODE_NULL; }
        break;
      case 201: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy166 = FILL_MODE_LINEAR; }
        break;
      case 202: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy166 = FILL_MODE_NEXT; }
        break;
      case 205: /* group_by_list ::= expression */
{ yylhsminor.yy184 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); }
  yymsp[0].minor.yy184 = yylhsminor.yy184;
        break;
      case 206: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy184 = addNodeToList(pCxt, yymsp[-2].minor.yy184, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy392))); }
  yymsp[-2].minor.yy184 = yylhsminor.yy184;
        break;
      case 209: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy392 = addOrderByClause(pCxt, yymsp[-3].minor.yy392, yymsp[-2].minor.yy184);
                                                                                    yylhsminor.yy392 = addSlimitClause(pCxt, yylhsminor.yy392, yymsp[-1].minor.yy392);
                                                                                    yylhsminor.yy392 = addLimitClause(pCxt, yylhsminor.yy392, yymsp[0].minor.yy392);
                                                                                  }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 211: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy392 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy392, yymsp[0].minor.yy392); }
  yymsp[-3].minor.yy392 = yylhsminor.yy392;
        break;
      case 216: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 220: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==220);
{ yymsp[-1].minor.yy392 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 217: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 221: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==221);
{ yymsp[-3].minor.yy392 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 218: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 222: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==222);
{ yymsp[-3].minor.yy392 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 223: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy392 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy392); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 224: /* search_condition ::= common_expression */
{ yylhsminor.yy392 = releaseRawExprNode(pCxt, yymsp[0].minor.yy392); }
  yymsp[0].minor.yy392 = yylhsminor.yy392;
        break;
      case 227: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy392 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy392), yymsp[-1].minor.yy162, yymsp[0].minor.yy9); }
  yymsp[-2].minor.yy392 = yylhsminor.yy392;
        break;
      case 228: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy162 = ORDER_ASC; }
        break;
      case 229: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy162 = ORDER_ASC; }
        break;
      case 230: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy162 = ORDER_DESC; }
        break;
      case 231: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy9 = NULL_ORDER_DEFAULT; }
        break;
      case 232: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy9 = NULL_ORDER_FIRST; }
        break;
      case 233: /* null_ordering_opt ::= NULLS LAST */
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
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
  } else {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
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
