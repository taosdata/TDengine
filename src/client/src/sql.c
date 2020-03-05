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
#line 23 "sql.y"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "tscSQLParser.h"
#include "tutil.h"
#line 37 "sql.c"
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
**    YY_MAX_SHIFT       Maximum value for shift actions
**    YY_MIN_SHIFTREDUCE Minimum value for shift-reduce actions
**    YY_MAX_SHIFTREDUCE Maximum value for shift-reduce actions
**    YY_MIN_REDUCE      Maximum value for reduce actions
**    YY_ERROR_ACTION    The yy_action[] code for syntax error
**    YY_ACCEPT_ACTION   The yy_action[] code for accept
**    YY_NO_ACTION       The yy_action[] code for no-op
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/************* Begin control #defines *****************************************/
#define YYCODETYPE unsigned short int
#define YYNOCODE 262
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SQuerySQL* yy138;
  SCreateAcctSQL yy155;
  SLimitVal yy162;
  int yy220;
  tVariant yy236;
  tSQLExprListList* yy237;
  tSQLExpr* yy244;
  SCreateDBInfo yy262;
  tSQLExprList* yy284;
  SCreateTableSQL* yy344;
  int64_t yy369;
  TAOS_FIELD yy397;
  tFieldList* yy421;
  tVariantList* yy480;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             253
#define YYNRULE              218
#define YY_MAX_SHIFT         252
#define YY_MIN_SHIFTREDUCE   405
#define YY_MAX_SHIFTREDUCE   622
#define YY_MIN_REDUCE        623
#define YY_MAX_REDUCE        840
#define YY_ERROR_ACTION      841
#define YY_ACCEPT_ACTION     842
#define YY_NO_ACTION         843
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
**   N between YY_MIN_REDUCE            Reduce by rule N-YY_MIN_REDUCE
**     and YY_MAX_REDUCE

**   N == YY_ERROR_ACTION               A syntax error has occurred.
**
**   N == YY_ACCEPT_ACTION              The parser accepts its input.
**
**   N == YY_NO_ACTION                  No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as
**
**      yy_action[ yy_shift_ofst[S] + X ]
**
** If the index value yy_shift_ofst[S]+X is out of range or if the value
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
** is equal to YY_SHIFT_USE_DFLT, it means that the action is not in the table
** and that yy_default[S] should be used instead.  
**
** The formula above is for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and YY_REDUCE_USE_DFLT is used in place of
** YY_SHIFT_USE_DFLT.
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
#define YY_ACTTAB_COUNT (535)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   446,   74,   78,  245,   85,   77,  506,  241,  447,  842,
 /*    10 */   252,   80,   43,   45,    8,   37,   38,   62,  112,  170,
 /*    20 */    31,  446,  446,  206,   41,   39,   42,   40,   11,  447,
 /*    30 */   447,  136,   36,   35,  230,  229,   34,   33,   32,   43,
 /*    40 */    45,  604,   37,   38,  102,  528,  136,   31,  136,  134,
 /*    50 */   206,   41,   39,   42,   40,  160,  605,  159,  605,   36,
 /*    60 */    35,  155,  243,   34,   33,   32,  406,  407,  408,  409,
 /*    70 */   410,  411,  412,  413,  414,  415,  416,  417,  251,  536,
 /*    80 */    43,   45,  171,   37,   38,  227,  226,   18,   31,  242,
 /*    90 */    22,  206,   41,   39,   42,   40,  203,   98,   59,   57,
 /*   100 */    36,   35,  154,  250,   34,   33,   32,   45,  609,   37,
 /*   110 */    38,  503,   29,  502,   31,  501,   22,  206,   41,   39,
 /*   120 */    42,   40,  168,  573,  515,  134,   36,   35,  533,  179,
 /*   130 */    34,   33,   32,  243,   37,   38,  187,  156,  184,   31,
 /*   140 */   133,  102,  206,   41,   39,   42,   40,  194,  169,  610,
 /*   150 */   515,   36,   35,   29,  102,   34,   33,   32,   18,  220,
 /*   160 */   242,  219,  218,  217,  216,  215,  214,  213,  212,  499,
 /*   170 */   232,  488,  489,  490,  491,  492,  493,  494,  495,  496,
 /*   180 */   497,  498,  164,  586,   12,  619,  577,   22,  580,   22,
 /*   190 */   583,  135,  164,  586,  157,  560,  577,  201,  580,  516,
 /*   200 */   583,   36,   35,  149,  517,   34,   33,   32,  559,   87,
 /*   210 */    86,  143,  518,  167,  161,  162,  102,  148,  205,  228,
 /*   220 */    52,  515,   61,  514,  161,  162,  164,  586,  534,  244,
 /*   230 */   577,  518,  580,   28,  583,   53,   41,   39,   42,   40,
 /*   240 */    34,   33,   32,   27,   36,   35,   22,  518,   34,   33,
 /*   250 */    32,  115,  116,  224,   65,   68,  545,   76,  161,  162,
 /*   260 */    48,  193,  522,  241,  189,  519,  579,  520,  582,  521,
 /*   270 */    60,  151,  129,  127,  246,   89,   88,   44,  233,  578,
 /*   280 */   515,  581,  249,  248,   93,  444,  585,   44,  125,  453,
 /*   290 */   554,  555,  125,  172,  173,  445,  585,  531,  125,  546,
 /*   300 */    19,  584,  575,   16,  603,  163,  587,   49,   15,  512,
 /*   310 */    15,  584,  511,   48,  210,  526,   23,  527,   23,   84,
 /*   320 */    83,   44,   73,   72,   50,   10,    9,  524,  140,  525,
 /*   330 */   585,  601,  600,  535,  599,  152,  153,  141,  576,  570,
 /*   340 */   142,  569,    2,  144,  145,  584,  146,  147,  138,  132,
 /*   350 */   139,  137,  165,  566,  565,  166,   99,  509,  231,  552,
 /*   360 */   551,  113,  523,  114,  111,  455,  211,  130,  190,   25,
 /*   370 */   223,  225,  618,   70,  617,  615,  117,  473,   26,  192,
 /*   380 */    24,  541,  131,  442,   79,   91,  440,  195,   81,  438,
 /*   390 */   437,  529,  174,  126,  435,  199,   54,  434,  103,  433,
 /*   400 */   431,  423,   51,   46,  128,  429,  104,  204,  202,  198,
 /*   410 */   427,  200,  196,  425,   27,   96,   30,  539,  222,  540,
 /*   420 */   553,   75,  234,  235,  236,  238,  622,  237,  239,  208,
 /*   430 */   240,   55,  247,  175,  150,  176,   63,   66,  177,  436,
 /*   440 */   178,  621,   90,   92,  181,  430,  120,  180,  119,  474,
 /*   450 */   118,  121,  122,    4,  123,    1,  124,  182,  183,  513,
 /*   460 */   620,  185,  107,  105,  108,  106,  109,  110,  186,  613,
 /*   470 */    97,  188,   13,   14,   58,  191,  542,  100,  158,   20,
 /*   480 */   197,    5,  547,  101,    3,    6,  588,   21,   17,  207,
 /*   490 */     7,  209,   64,  486,  485,  484,  483,  482,  481,  480,
 /*   500 */   479,  477,  450,  221,   67,  452,   23,  508,   69,   47,
 /*   510 */   507,  505,   56,  471,  469,  461,  467,  463,   71,  465,
 /*   520 */   459,  457,  478,  476,   48,   82,  432,  448,   94,  421,
 /*   530 */   419,  623,  625,  625,   95,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   64,   65,   66,   67,   68,    5,   78,    9,  197,
 /*    10 */   198,   74,   13,   14,   96,   16,   17,   99,  100,   63,
 /*    20 */    21,    1,    1,   24,   25,   26,   27,   28,  248,    9,
 /*    30 */     9,  248,   33,   34,   33,   34,   37,   38,   39,   13,
 /*    40 */    14,  258,   16,   17,  200,  233,  248,   21,  248,  248,
 /*    50 */    24,   25,   26,   27,   28,  257,  258,  257,  258,   33,
 /*    60 */    34,  260,   60,   37,   38,   39,   45,   46,   47,   48,
 /*    70 */    49,   50,   51,   52,   53,   54,   55,   56,   57,  200,
 /*    80 */    13,   14,  126,   16,   17,  129,  130,   85,   21,   87,
 /*    90 */   200,   24,   25,   26,   27,   28,  252,  200,  254,  100,
 /*   100 */    33,   34,  199,  200,   37,   38,   39,   14,   97,   16,
 /*   110 */    17,  216,  101,  218,   21,  220,  200,   24,   25,   26,
 /*   120 */    27,   28,  232,   97,  234,  248,   33,   34,  249,  125,
 /*   130 */    37,   38,   39,   60,   16,   17,  132,  260,  134,   21,
 /*   140 */   248,  200,   24,   25,   26,   27,   28,  250,  232,   97,
 /*   150 */   234,   33,   34,  101,  200,   37,   38,   39,   85,   86,
 /*   160 */    87,   88,   89,   90,   91,   92,   93,   94,   95,  216,
 /*   170 */   200,  218,  219,  220,  221,  222,  223,  224,  225,  226,
 /*   180 */   227,  228,    1,    2,   44,  235,    5,  200,    7,  200,
 /*   190 */     9,  248,    1,    2,  217,  254,    5,  256,    7,  229,
 /*   200 */     9,   33,   34,   63,  235,   37,   38,   39,  254,   69,
 /*   210 */    70,   71,  235,  217,   33,   34,  200,   77,   37,  232,
 /*   220 */   101,  234,  236,  234,   33,   34,    1,    2,   37,  217,
 /*   230 */     5,  235,    7,  247,    9,  116,   25,   26,   27,   28,
 /*   240 */    37,   38,   39,  103,   33,   34,  200,  235,   37,   38,
 /*   250 */    39,   64,   65,   66,   67,   68,   97,   72,   33,   34,
 /*   260 */   101,  121,    2,   78,  124,    5,    5,    7,    7,    9,
 /*   270 */   254,  131,   64,   65,   66,   67,   68,   96,  232,    5,
 /*   280 */   234,    7,   60,   61,   62,  204,  105,   96,  207,  204,
 /*   290 */   111,  112,  207,   33,   34,  204,  105,  101,  207,   97,
 /*   300 */   104,  120,    1,  101,   97,   59,   97,  101,  101,   97,
 /*   310 */   101,  120,   97,  101,   97,    5,  101,    7,  101,   72,
 /*   320 */    73,   96,  127,  128,  118,  127,  128,    5,  248,    7,
 /*   330 */   105,  248,  248,  200,  248,  248,  248,  248,   37,  230,
 /*   340 */   248,  230,   96,  248,  248,  120,  248,  248,  248,  248,
 /*   350 */   248,  248,  230,  230,  230,  230,  200,  231,  230,  255,
 /*   360 */   255,  200,  102,  200,  237,  200,  200,  200,  123,  200,
 /*   370 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  259,
 /*   380 */   200,  105,  200,  200,  200,   59,  200,  251,  200,  200,
 /*   390 */   200,  246,  200,  200,  200,  251,  115,  200,  245,  200,
 /*   400 */   200,  200,  117,  114,  200,  200,  244,  109,  113,  107,
 /*   410 */   200,  108,  106,  200,  103,  201,  119,  201,   75,  201,
 /*   420 */   201,   84,   83,   49,   80,   53,    5,   82,   81,  201,
 /*   430 */    79,  201,   75,  133,  201,    5,  205,  205,  133,  201,
 /*   440 */    58,    5,  202,  202,    5,  201,  209,  133,  213,  215,
 /*   450 */   214,  212,  210,  203,  211,  206,  208,  133,   58,  233,
 /*   460 */     5,  133,  241,  243,  240,  242,  239,  238,   58,   86,
 /*   470 */   122,  125,   96,   96,  101,  123,   97,   96,    1,  101,
 /*   480 */    96,  110,   97,   96,   96,  110,   97,  101,   96,   98,
 /*   490 */    96,   98,   72,    9,    5,    5,    5,    5,    1,    5,
 /*   500 */     5,    5,   76,   15,   72,   58,  101,    5,  128,   16,
 /*   510 */     5,   97,   96,    5,    5,    5,    5,    5,  128,    5,
 /*   520 */     5,    5,    5,    5,  101,   58,   58,   76,   21,   59,
 /*   530 */    58,    0,  261,  261,   21,
};
#define YY_SHIFT_USE_DFLT (-83)
#define YY_SHIFT_COUNT (252)
#define YY_SHIFT_MIN   (-82)
#define YY_SHIFT_MAX   (531)
static const short yy_shift_ofst[] = {
 /*     0 */   140,   73,  181,  225,    2,   20,   20,   20,   20,   20,
 /*    10 */    20,   -1,   21,  225,  225,  225,  260,  260,  260,   20,
 /*    20 */    20,   20,   20,   20,  185,  -71,  -71,  -83,  191,  225,
 /*    30 */   225,  225,  225,  225,  225,  225,  225,  225,  225,  225,
 /*    40 */   225,  225,  225,  225,  225,  225,  225,  260,  260,    1,
 /*    50 */     1,    1,    1,    1,    1,  -82,    1,   20,   20,  179,
 /*    60 */   179,  196,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    70 */    20,   20,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    80 */    20,   20,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    90 */    20,   20,   20,   20,   20,   20,  245,  326,  326,  326,
 /*   100 */   276,  276,  326,  281,  285,  289,  298,  295,  303,  302,
 /*   110 */   306,  297,  311,  326,  326,  343,  343,  326,  337,  339,
 /*   120 */   374,  344,  345,  372,  347,  351,  326,  357,  326,  357,
 /*   130 */   -83,  -83,   26,   67,   67,   67,   67,   67,   93,  118,
 /*   140 */   211,  211,  211,  -63,  168,  168,  168,  168,  187,  208,
 /*   150 */   -44,    4,  203,  203,  222,   11,   52,  159,  202,  207,
 /*   160 */   209,  261,  274,  301,  246,  206,  119,  212,  215,  217,
 /*   170 */   195,  198,  310,  322,  247,  421,  300,  430,  305,  382,
 /*   180 */   436,  314,  439,  324,  400,  455,  328,  410,  383,  346,
 /*   190 */   376,  377,  352,  348,  373,  379,  381,  477,  384,  385,
 /*   200 */   387,  378,  371,  386,  375,  389,  388,  392,  391,  394,
 /*   210 */   393,  420,  484,  489,  490,  491,  492,  497,  494,  495,
 /*   220 */   496,  426,  488,  432,  447,  493,  380,  390,  405,  502,
 /*   230 */   505,  414,  416,  405,  508,  509,  510,  511,  512,  514,
 /*   240 */   515,  516,  517,  518,  423,  467,  468,  451,  507,  513,
 /*   250 */   470,  472,  531,
};
#define YY_REDUCE_USE_DFLT (-221)
#define YY_REDUCE_COUNT (131)
#define YY_REDUCE_MIN   (-220)
#define YY_REDUCE_MAX   (250)
static const short yy_reduce_ofst[] = {
 /*     0 */  -188,  -47, -202, -200, -105,  -59, -156, -110,  -84,  -13,
 /*    10 */    46, -121,  -97, -199, -123, -217,  -23,   -4,   12, -103,
 /*    20 */   -46,   16,  -30,  -11,   81,   85,   91,  -14, -220, -108,
 /*    30 */   -57,   80,   83,   84,   86,   87,   88,   89,   92,   95,
 /*    40 */    96,   98,   99,  100,  101,  102,  103,  -50,  -31,  109,
 /*    50 */   111,  122,  123,  124,  125,  126,  128,  133,  156,  104,
 /*    60 */   105,  127,  161,  163,  165,  166,  167,  169,  170,  171,
 /*    70 */   172,  173,  174,  175,  176,  177,  178,  180,  182,  183,
 /*    80 */   184,  186,  188,  189,  190,  192,  193,  194,  197,  199,
 /*    90 */   200,  201,  204,  205,  210,  213,  120,  214,  216,  218,
 /*   100 */   136,  144,  219,  145,  153,  162,  220,  223,  221,  224,
 /*   110 */   227,  229,  226,  228,  230,  231,  232,  233,  234,  236,
 /*   120 */   235,  237,  239,  242,  243,  248,  238,  240,  244,  241,
 /*   130 */   249,  250,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   841,  672,  825,  825,  661,  841,  841,  841,  841,  841,
 /*    10 */   841,  755,  638,  841,  841,  825,  841,  841,  841,  841,
 /*    20 */   841,  841,  841,  841,  674,  674,  674,  750,  841,  841,
 /*    30 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  841,
 /*    40 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  841,
 /*    50 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  774,
 /*    60 */   774,  748,  841,  841,  841,  841,  841,  841,  841,  841,
 /*    70 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  659,
 /*    80 */   841,  657,  841,  841,  841,  841,  841,  841,  841,  841,
 /*    90 */   841,  841,  841,  646,  841,  841,  841,  640,  640,  640,
 /*   100 */   841,  841,  640,  781,  785,  779,  767,  775,  766,  762,
 /*   110 */   761,  789,  841,  640,  640,  669,  669,  640,  690,  688,
 /*   120 */   686,  678,  684,  680,  682,  676,  640,  667,  640,  667,
 /*   130 */   705,  718,  841,  829,  830,  790,  824,  780,  808,  807,
 /*   140 */   820,  814,  813,  841,  812,  811,  810,  809,  841,  841,
 /*   150 */   841,  841,  816,  815,  841,  841,  841,  841,  841,  841,
 /*   160 */   841,  841,  841,  841,  792,  786,  782,  841,  841,  841,
 /*   170 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  841,
 /*   180 */   841,  841,  841,  841,  841,  841,  841,  841,  841,  841,
 /*   190 */   841,  841,  826,  841,  756,  841,  841,  841,  841,  841,
 /*   200 */   841,  776,  841,  768,  841,  841,  841,  841,  841,  841,
 /*   210 */   728,  841,  841,  841,  841,  841,  841,  841,  841,  841,
 /*   220 */   841,  841,  841,  841,  841,  841,  841,  841,  834,  841,
 /*   230 */   841,  841,  722,  832,  841,  841,  841,  841,  841,  841,
 /*   240 */   841,  841,  841,  841,  693,  841,  841,  841,  644,  642,
 /*   250 */   841,  636,  841,
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
    0,  /*     MNODES => nothing */
    0,  /*     DNODES => nothing */
    0,  /*   ACCOUNTS => nothing */
    0,  /*      USERS => nothing */
    0,  /*    MODULES => nothing */
    0,  /*    QUERIES => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*    CONFIGS => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
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
    0,  /*     CREATE => nothing */
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
    0,  /*       DAYS => nothing */
    0,  /*       ROWS => nothing */
    0,  /*    ABLOCKS => nothing */
    0,  /*    TBLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*       CLOG => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*         AS => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     SELECT => nothing */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
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
    0,  /*     INSERT => nothing */
    0,  /*       INTO => nothing */
    0,  /*     VALUES => nothing */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*        TAG => nothing */
    0,  /*     CHANGE => nothing */
    0,  /*        SET => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /*      COLON => nothing */
    0,  /*     STREAM => nothing */
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
    1,  /*        ALL => ID */
    1,  /*      COUNT => ID */
    1,  /*        SUM => ID */
    1,  /*        AVG => ID */
    1,  /*        MIN => ID */
    1,  /*        MAX => ID */
    1,  /*      FIRST => ID */
    1,  /*       LAST => ID */
    1,  /*        TOP => ID */
    1,  /*     BOTTOM => ID */
    1,  /*     STDDEV => ID */
    1,  /* PERCENTILE => ID */
    1,  /* APERCENTILE => ID */
    1,  /* LEASTSQUARES => ID */
    1,  /*  HISTOGRAM => ID */
    1,  /*       DIFF => ID */
    1,  /*     SPREAD => ID */
    1,  /*        TWA => ID */
    1,  /*     INTERP => ID */
    1,  /*   LAST_ROW => ID */
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     METRIC => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*    METRICS => ID */
    1,  /*     STABLE => ID */
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
  int yyidx;                    /* Index of top element in stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyidxMax;                 /* Maximum value of yyidx */
#endif
#ifndef YYNOERRORRECOVERY
  int yyerrcnt;                 /* Shifts left before out of the error */
#endif
  ParseARG_SDECL                /* A place to hold %extra_argument */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
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
** <li> A FILE *to which trace output should be written.
**      If NULL, then tracing is turned off.
** <li> A prefix string written at the beginning of every
**      line of trace output.  If NULL, then tracing is
**      turned off.
** </ul>
**
** Outputs:
** None.
*/
void ParseTrace(FILE *TraceFILE, char *zTracePrompt) {
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if (yyTraceFILE == 0) yyTracePrompt = 0;
  else if (yyTracePrompt == 0) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  "$",             "ID",            "BOOL",          "TINYINT",     
  "SMALLINT",      "INTEGER",       "BIGINT",        "FLOAT",       
  "DOUBLE",        "STRING",        "TIMESTAMP",     "BINARY",      
  "NCHAR",         "OR",            "AND",           "NOT",         
  "EQ",            "NE",            "ISNULL",        "NOTNULL",     
  "IS",            "LIKE",          "GLOB",          "BETWEEN",     
  "IN",            "GT",            "GE",            "LT",          
  "LE",            "BITAND",        "BITOR",         "LSHIFT",      
  "RSHIFT",        "PLUS",          "MINUS",         "DIVIDE",      
  "TIMES",         "STAR",          "SLASH",         "REM",         
  "CONCAT",        "UMINUS",        "UPLUS",         "BITNOT",      
  "SHOW",          "DATABASES",     "MNODES",        "DNODES",      
  "ACCOUNTS",      "USERS",         "MODULES",       "QUERIES",     
  "CONNECTIONS",   "STREAMS",       "CONFIGS",       "SCORES",      
  "GRANTS",        "VNODES",        "IPTOKEN",       "DOT",         
  "TABLES",        "STABLES",       "VGROUPS",       "DROP",        
  "TABLE",         "DATABASE",      "DNODE",         "USER",        
  "ACCOUNT",       "USE",           "DESCRIBE",      "ALTER",       
  "PASS",          "PRIVILEGE",     "LOCAL",         "IF",          
  "EXISTS",        "CREATE",        "PPS",           "TSERIES",     
  "DBS",           "STORAGE",       "QTIME",         "CONNS",       
  "STATE",         "KEEP",          "CACHE",         "REPLICA",     
  "DAYS",          "ROWS",          "ABLOCKS",       "TBLOCKS",     
  "CTIME",         "CLOG",          "COMP",          "PRECISION",   
  "LP",            "RP",            "TAGS",          "USING",       
  "AS",            "COMMA",         "NULL",          "SELECT",      
  "FROM",          "VARIABLE",      "INTERVAL",      "FILL",        
  "SLIDING",       "ORDER",         "BY",            "ASC",         
  "DESC",          "GROUP",         "HAVING",        "LIMIT",       
  "OFFSET",        "SLIMIT",        "SOFFSET",       "WHERE",       
  "NOW",           "INSERT",        "INTO",          "VALUES",      
  "RESET",         "QUERY",         "ADD",           "COLUMN",      
  "TAG",           "CHANGE",        "SET",           "KILL",        
  "CONNECTION",    "COLON",         "STREAM",        "ABORT",       
  "AFTER",         "ATTACH",        "BEFORE",        "BEGIN",       
  "CASCADE",       "CLUSTER",       "CONFLICT",      "COPY",        
  "DEFERRED",      "DELIMITERS",    "DETACH",        "EACH",        
  "END",           "EXPLAIN",       "FAIL",          "FOR",         
  "IGNORE",        "IMMEDIATE",     "INITIALLY",     "INSTEAD",     
  "MATCH",         "KEY",           "OF",            "RAISE",       
  "REPLACE",       "RESTRICT",      "ROW",           "STATEMENT",   
  "TRIGGER",       "VIEW",          "ALL",           "COUNT",       
  "SUM",           "AVG",           "MIN",           "MAX",         
  "FIRST",         "LAST",          "TOP",           "BOTTOM",      
  "STDDEV",        "PERCENTILE",    "APERCENTILE",   "LEASTSQUARES",
  "HISTOGRAM",     "DIFF",          "SPREAD",        "TWA",         
  "INTERP",        "LAST_ROW",      "SEMI",          "NONE",        
  "PREV",          "LINEAR",        "IMPORT",        "METRIC",      
  "TBNAME",        "JOIN",          "METRICS",       "STABLE",      
  "error",         "program",       "cmd",           "dbPrefix",    
  "ids",           "cpxName",       "ifexists",      "alter_db_optr",
  "acct_optr",     "ifnotexists",   "db_optr",       "pps",         
  "tseries",       "dbs",           "streams",       "storage",     
  "qtime",         "users",         "conns",         "state",       
  "keep",          "tagitemlist",   "tables",        "cache",       
  "replica",       "days",          "rows",          "ablocks",     
  "tblocks",       "ctime",         "clog",          "comp",        
  "prec",          "typename",      "signed",        "create_table_args",
  "columnlist",    "select",        "column",        "tagitem",     
  "selcollist",    "from",          "where_opt",     "interval_opt",
  "fill_opt",      "sliding_opt",   "groupby_opt",   "orderby_opt", 
  "having_opt",    "slimit_opt",    "limit_opt",     "sclp",        
  "expr",          "as",            "tablelist",     "tmvar",       
  "sortlist",      "sortitem",      "item",          "sortorder",   
  "grouplist",     "exprlist",      "expritem",      "insert_value_list",
  "itemlist",    
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW MNODES",
 /*   3 */ "cmd ::= SHOW DNODES",
 /*   4 */ "cmd ::= SHOW ACCOUNTS",
 /*   5 */ "cmd ::= SHOW USERS",
 /*   6 */ "cmd ::= SHOW MODULES",
 /*   7 */ "cmd ::= SHOW QUERIES",
 /*   8 */ "cmd ::= SHOW CONNECTIONS",
 /*   9 */ "cmd ::= SHOW STREAMS",
 /*  10 */ "cmd ::= SHOW CONFIGS",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  20 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  24 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  25 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  26 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  27 */ "cmd ::= DROP DNODE IPTOKEN",
 /*  28 */ "cmd ::= DROP USER ids",
 /*  29 */ "cmd ::= DROP ACCOUNT ids",
 /*  30 */ "cmd ::= USE ids",
 /*  31 */ "cmd ::= DESCRIBE ids cpxName",
 /*  32 */ "cmd ::= ALTER USER ids PASS ids",
 /*  33 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  34 */ "cmd ::= ALTER DNODE IPTOKEN ids",
 /*  35 */ "cmd ::= ALTER DNODE IPTOKEN ids ids",
 /*  36 */ "cmd ::= ALTER LOCAL ids",
 /*  37 */ "cmd ::= ALTER LOCAL ids ids",
 /*  38 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  39 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  40 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  41 */ "ids ::= ID",
 /*  42 */ "ids ::= STRING",
 /*  43 */ "ifexists ::= IF EXISTS",
 /*  44 */ "ifexists ::=",
 /*  45 */ "ifnotexists ::= IF NOT EXISTS",
 /*  46 */ "ifnotexists ::=",
 /*  47 */ "cmd ::= CREATE DNODE IPTOKEN",
 /*  48 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  49 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  50 */ "cmd ::= CREATE USER ids PASS ids",
 /*  51 */ "pps ::=",
 /*  52 */ "pps ::= PPS INTEGER",
 /*  53 */ "tseries ::=",
 /*  54 */ "tseries ::= TSERIES INTEGER",
 /*  55 */ "dbs ::=",
 /*  56 */ "dbs ::= DBS INTEGER",
 /*  57 */ "streams ::=",
 /*  58 */ "streams ::= STREAMS INTEGER",
 /*  59 */ "storage ::=",
 /*  60 */ "storage ::= STORAGE INTEGER",
 /*  61 */ "qtime ::=",
 /*  62 */ "qtime ::= QTIME INTEGER",
 /*  63 */ "users ::=",
 /*  64 */ "users ::= USERS INTEGER",
 /*  65 */ "conns ::=",
 /*  66 */ "conns ::= CONNS INTEGER",
 /*  67 */ "state ::=",
 /*  68 */ "state ::= STATE ids",
 /*  69 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  70 */ "keep ::= KEEP tagitemlist",
 /*  71 */ "tables ::= TABLES INTEGER",
 /*  72 */ "cache ::= CACHE INTEGER",
 /*  73 */ "replica ::= REPLICA INTEGER",
 /*  74 */ "days ::= DAYS INTEGER",
 /*  75 */ "rows ::= ROWS INTEGER",
 /*  76 */ "ablocks ::= ABLOCKS ID",
 /*  77 */ "tblocks ::= TBLOCKS INTEGER",
 /*  78 */ "ctime ::= CTIME INTEGER",
 /*  79 */ "clog ::= CLOG INTEGER",
 /*  80 */ "comp ::= COMP INTEGER",
 /*  81 */ "prec ::= PRECISION STRING",
 /*  82 */ "db_optr ::=",
 /*  83 */ "db_optr ::= db_optr tables",
 /*  84 */ "db_optr ::= db_optr cache",
 /*  85 */ "db_optr ::= db_optr replica",
 /*  86 */ "db_optr ::= db_optr days",
 /*  87 */ "db_optr ::= db_optr rows",
 /*  88 */ "db_optr ::= db_optr ablocks",
 /*  89 */ "db_optr ::= db_optr tblocks",
 /*  90 */ "db_optr ::= db_optr ctime",
 /*  91 */ "db_optr ::= db_optr clog",
 /*  92 */ "db_optr ::= db_optr comp",
 /*  93 */ "db_optr ::= db_optr prec",
 /*  94 */ "db_optr ::= db_optr keep",
 /*  95 */ "alter_db_optr ::=",
 /*  96 */ "alter_db_optr ::= alter_db_optr replica",
 /*  97 */ "alter_db_optr ::= alter_db_optr tables",
 /*  98 */ "alter_db_optr ::= alter_db_optr keep",
 /*  99 */ "typename ::= ids",
 /* 100 */ "typename ::= ids LP signed RP",
 /* 101 */ "signed ::= INTEGER",
 /* 102 */ "signed ::= PLUS INTEGER",
 /* 103 */ "signed ::= MINUS INTEGER",
 /* 104 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 105 */ "create_table_args ::= LP columnlist RP",
 /* 106 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 107 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 108 */ "create_table_args ::= AS select",
 /* 109 */ "columnlist ::= columnlist COMMA column",
 /* 110 */ "columnlist ::= column",
 /* 111 */ "column ::= ids typename",
 /* 112 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 113 */ "tagitemlist ::= tagitem",
 /* 114 */ "tagitem ::= INTEGER",
 /* 115 */ "tagitem ::= FLOAT",
 /* 116 */ "tagitem ::= STRING",
 /* 117 */ "tagitem ::= BOOL",
 /* 118 */ "tagitem ::= NULL",
 /* 119 */ "tagitem ::= MINUS INTEGER",
 /* 120 */ "tagitem ::= MINUS FLOAT",
 /* 121 */ "tagitem ::= PLUS INTEGER",
 /* 122 */ "tagitem ::= PLUS FLOAT",
 /* 123 */ "cmd ::= select",
 /* 124 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 125 */ "select ::= SELECT selcollist",
 /* 126 */ "sclp ::= selcollist COMMA",
 /* 127 */ "sclp ::=",
 /* 128 */ "selcollist ::= sclp expr as",
 /* 129 */ "selcollist ::= sclp STAR",
 /* 130 */ "as ::= AS ids",
 /* 131 */ "as ::= ids",
 /* 132 */ "as ::=",
 /* 133 */ "from ::= FROM tablelist",
 /* 134 */ "tablelist ::= ids cpxName",
 /* 135 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 136 */ "tmvar ::= VARIABLE",
 /* 137 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 138 */ "interval_opt ::=",
 /* 139 */ "fill_opt ::=",
 /* 140 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 141 */ "fill_opt ::= FILL LP ID RP",
 /* 142 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 143 */ "sliding_opt ::=",
 /* 144 */ "orderby_opt ::=",
 /* 145 */ "orderby_opt ::= ORDER BY sortlist",
 /* 146 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 147 */ "sortlist ::= item sortorder",
 /* 148 */ "item ::= ids cpxName",
 /* 149 */ "sortorder ::= ASC",
 /* 150 */ "sortorder ::= DESC",
 /* 151 */ "sortorder ::=",
 /* 152 */ "groupby_opt ::=",
 /* 153 */ "groupby_opt ::= GROUP BY grouplist",
 /* 154 */ "grouplist ::= grouplist COMMA item",
 /* 155 */ "grouplist ::= item",
 /* 156 */ "having_opt ::=",
 /* 157 */ "having_opt ::= HAVING expr",
 /* 158 */ "limit_opt ::=",
 /* 159 */ "limit_opt ::= LIMIT signed",
 /* 160 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 161 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 162 */ "slimit_opt ::=",
 /* 163 */ "slimit_opt ::= SLIMIT signed",
 /* 164 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 165 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 166 */ "where_opt ::=",
 /* 167 */ "where_opt ::= WHERE expr",
 /* 168 */ "expr ::= LP expr RP",
 /* 169 */ "expr ::= ID",
 /* 170 */ "expr ::= ID DOT ID",
 /* 171 */ "expr ::= ID DOT STAR",
 /* 172 */ "expr ::= INTEGER",
 /* 173 */ "expr ::= MINUS INTEGER",
 /* 174 */ "expr ::= PLUS INTEGER",
 /* 175 */ "expr ::= FLOAT",
 /* 176 */ "expr ::= MINUS FLOAT",
 /* 177 */ "expr ::= PLUS FLOAT",
 /* 178 */ "expr ::= STRING",
 /* 179 */ "expr ::= NOW",
 /* 180 */ "expr ::= VARIABLE",
 /* 181 */ "expr ::= BOOL",
 /* 182 */ "expr ::= ID LP exprlist RP",
 /* 183 */ "expr ::= ID LP STAR RP",
 /* 184 */ "expr ::= expr AND expr",
 /* 185 */ "expr ::= expr OR expr",
 /* 186 */ "expr ::= expr LT expr",
 /* 187 */ "expr ::= expr GT expr",
 /* 188 */ "expr ::= expr LE expr",
 /* 189 */ "expr ::= expr GE expr",
 /* 190 */ "expr ::= expr NE expr",
 /* 191 */ "expr ::= expr EQ expr",
 /* 192 */ "expr ::= expr PLUS expr",
 /* 193 */ "expr ::= expr MINUS expr",
 /* 194 */ "expr ::= expr STAR expr",
 /* 195 */ "expr ::= expr SLASH expr",
 /* 196 */ "expr ::= expr REM expr",
 /* 197 */ "expr ::= expr LIKE expr",
 /* 198 */ "expr ::= expr IN LP exprlist RP",
 /* 199 */ "exprlist ::= exprlist COMMA expritem",
 /* 200 */ "exprlist ::= expritem",
 /* 201 */ "expritem ::= expr",
 /* 202 */ "expritem ::=",
 /* 203 */ "cmd ::= INSERT INTO cpxName insert_value_list",
 /* 204 */ "insert_value_list ::= VALUES LP itemlist RP",
 /* 205 */ "insert_value_list ::= insert_value_list VALUES LP itemlist RP",
 /* 206 */ "itemlist ::= itemlist COMMA expr",
 /* 207 */ "itemlist ::= expr",
 /* 208 */ "cmd ::= RESET QUERY CACHE",
 /* 209 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 210 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 211 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 212 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 213 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 214 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 215 */ "cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER",
 /* 216 */ "cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER",
 /* 217 */ "cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.
*/
static void yyGrowStack(yyParser *p) {
  int newSize;
  yyStackEntry *pNew;

  newSize = p->yystksz *2 + 100;
  pNew = realloc(p->yystack, newSize *sizeof(pNew[0]));
  if (pNew) {
    p->yystack = pNew;
    p->yystksz = newSize;
#ifndef NDEBUG
    if (yyTraceFILE) {
      fprintf(yyTraceFILE,"%sStack grows to %d entries!\n",
              yyTracePrompt, p->yystksz);
    }
#endif
  }
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)) {
  yyParser *pParser;
  pParser = (yyParser *)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if (pParser) {
    pParser->yyidx = -1;
#ifdef YYTRACKMAXSTACKDEPTH
    pParser->yyidxMax = 0;
#endif
#if YYSTACKDEPTH<=0
    pParser->yystack = NULL;
    pParser->yystksz = 0;
    yyGrowStack(pParser);
#endif
  }
  return pParser;
}

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
) {
  ParseARG_FETCH;
  switch(yymajor) {
    /* Here is inserted the actions which take place when a
    ** terminal or non-terminal is destroyed.  This can happen
    ** when the symbol is popped from the stack during a
    ** reduce or during error processing or when a parser is 
    ** being destroyed before it is finished parsing.
    **
    ** Note: during a reduce, the only symbols destroyed are those
    ** which appear on the RHS of the rule, but which are *not *used
    ** inside the C code.
    */
/********* Begin destructor definitions ***************************************/
    case 216: /* keep */
    case 217: /* tagitemlist */
    case 240: /* fill_opt */
    case 242: /* groupby_opt */
    case 243: /* orderby_opt */
    case 252: /* sortlist */
    case 256: /* grouplist */
{
#line 213 "sql.y"
tVariantListDestroy((yypminor->yy480));
#line 1085 "sql.c"
}
      break;
    case 232: /* columnlist */
{
#line 301 "sql.y"
tFieldListDestroy((yypminor->yy421));
#line 1092 "sql.c"
}
      break;
    case 233: /* select */
{
#line 358 "sql.y"
destroyQuerySql((yypminor->yy138));
#line 1099 "sql.c"
}
      break;
    case 236: /* selcollist */
    case 247: /* sclp */
    case 257: /* exprlist */
    case 260: /* itemlist */
{
#line 375 "sql.y"
tSQLExprListDestroy((yypminor->yy284));
#line 1109 "sql.c"
}
      break;
    case 238: /* where_opt */
    case 244: /* having_opt */
    case 248: /* expr */
    case 258: /* expritem */
{
#line 510 "sql.y"
tSQLExprDestroy((yypminor->yy244));
#line 1119 "sql.c"
}
      break;
    case 253: /* sortitem */
{
#line 443 "sql.y"
tVariantDestroy(&(yypminor->yy236));
#line 1126 "sql.c"
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
static void yy_pop_parser_stack(yyParser *pParser) {
  yyStackEntry *yytos;
  assert( pParser->yyidx>=0);
  yytos = &pParser->yystack[pParser->yyidx--];
#ifndef NDEBUG
  if (yyTraceFILE) {
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yy_destructor(pParser, yytos->major, &yytos->minor);
}

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
  void (*freeProc)(void *)     /* Function used to reclaim memory */
) {
  yyParser *pParser = (yyParser *)p;
#ifndef YYPARSEFREENEVERNULL
  if (pParser == 0) return;
#endif
  while(pParser->yyidx>=0) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  free(pParser->yystack);
#endif
  (*freeProc)((void *)pParser);
}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p) {
  yyParser *pParser = (yyParser *)p;
  return pParser->yyidxMax;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static unsigned int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
) {
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if (stateno>=YY_MIN_REDUCE) return stateno;
  assert( stateno <= YY_SHIFT_COUNT);
  do{
    i = yy_shift_ofst[stateno];
    if (i == YY_SHIFT_USE_DFLT) return yy_default[stateno];
    assert( iLookAhead!=YYNOCODE);
    i += iLookAhead;
    if (i < 0 || i >= YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead) {
      if (iLookAhead>0) {
#ifdef YYFALLBACK
        YYCODETYPE iFallback;            /* Fallback token */
        if (iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0])
               && (iFallback = yyFallback[iLookAhead])!=0) {
#ifndef NDEBUG
          if (yyTraceFILE) {
            fprintf(yyTraceFILE, "%sFALLBACK %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[iFallback]);
          }
#endif
          assert( yyFallback[iFallback] == 0); /* Fallback loop must terminate */
          iLookAhead = iFallback;
          continue;
        }
#endif
#ifdef YYWILDCARD
        {
          int j = i - iLookAhead + YYWILDCARD;
          if (
#if YY_SHIFT_MIN+YYWILDCARD<0
            j>=0 &&
#endif
#if YY_SHIFT_MAX+YYWILDCARD>=YY_ACTTAB_COUNT
            j<YY_ACTTAB_COUNT &&
#endif
            yy_lookahead[j] == YYWILDCARD
          ) {
#ifndef NDEBUG
            if (yyTraceFILE) {
              fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
                 yyTracePrompt, yyTokenName[iLookAhead],
                 yyTokenName[YYWILDCARD]);
            }
#endif /* NDEBUG */
            return yy_action[j];
          }
        }
#endif /* YYWILDCARD */
      }
      return yy_default[stateno];
    } else{
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
) {
  int i;
#ifdef YYERRORSYMBOL
  if (stateno>YY_REDUCE_COUNT) {
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_COUNT);
#endif
  i = yy_reduce_ofst[stateno];
  assert( i!=YY_REDUCE_USE_DFLT);
  assert( iLookAhead!=YYNOCODE);
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if (i < 0 || i >= YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead) {
    return yy_default[stateno];
  }
#else
  assert( i >= 0 && i < YY_ACTTAB_COUNT);
  assert( yy_lookahead[i] == iLookAhead);
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser) {
   ParseARG_FETCH;
   yypParser->yyidx--;
#ifndef NDEBUG
   if (yyTraceFILE) {
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while(yypParser->yyidx>=0) yy_pop_parser_stack(yypParser);
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
static void yyTraceShift(yyParser *yypParser, int yyNewState) {
  if (yyTraceFILE) {
    if (yyNewState<YYNSTATE) {
      fprintf(yyTraceFILE,"%sShift '%s', go to state %d\n",
         yyTracePrompt,yyTokenName[yypParser->yystack[yypParser->yyidx].major],
         yyNewState);
    } else{
      fprintf(yyTraceFILE,"%sShift '%s'\n",
         yyTracePrompt,yyTokenName[yypParser->yystack[yypParser->yyidx].major]);
    }
  }
}
#else
# define yyTraceShift(X,Y)
#endif

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
  ParseTOKENTYPE yyMinor        /* The minor token to shift in */
) {
  yyStackEntry *yytos;
  yypParser->yyidx++;
#ifdef YYTRACKMAXSTACKDEPTH
  if (yypParser->yyidx>yypParser->yyidxMax) {
    yypParser->yyidxMax = yypParser->yyidx;
  }
#endif
#if YYSTACKDEPTH>0 
  if (yypParser->yyidx>=YYSTACKDEPTH) {
    yyStackOverflow(yypParser);
    return;
  }
#else
  if (yypParser->yyidx>=yypParser->yystksz) {
    yyGrowStack(yypParser);
    if (yypParser->yyidx>=yypParser->yystksz) {
      yyStackOverflow(yypParser);
      return;
    }
  }
#endif
  yytos = &yypParser->yystack[yypParser->yyidx];
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor.yy0 = yyMinor;
  yyTraceShift(yypParser, yyNewState);
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 197, 1 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 2 },
  { 198, 3 },
  { 199, 0 },
  { 199, 2 },
  { 201, 0 },
  { 201, 2 },
  { 198, 3 },
  { 198, 5 },
  { 198, 3 },
  { 198, 5 },
  { 198, 3 },
  { 198, 4 },
  { 198, 5 },
  { 198, 4 },
  { 198, 3 },
  { 198, 3 },
  { 198, 3 },
  { 198, 2 },
  { 198, 3 },
  { 198, 5 },
  { 198, 5 },
  { 198, 4 },
  { 198, 5 },
  { 198, 3 },
  { 198, 4 },
  { 198, 4 },
  { 198, 4 },
  { 198, 6 },
  { 200, 1 },
  { 200, 1 },
  { 202, 2 },
  { 202, 0 },
  { 205, 3 },
  { 205, 0 },
  { 198, 3 },
  { 198, 6 },
  { 198, 5 },
  { 198, 5 },
  { 207, 0 },
  { 207, 2 },
  { 208, 0 },
  { 208, 2 },
  { 209, 0 },
  { 209, 2 },
  { 210, 0 },
  { 210, 2 },
  { 211, 0 },
  { 211, 2 },
  { 212, 0 },
  { 212, 2 },
  { 213, 0 },
  { 213, 2 },
  { 214, 0 },
  { 214, 2 },
  { 215, 0 },
  { 215, 2 },
  { 204, 9 },
  { 216, 2 },
  { 218, 2 },
  { 219, 2 },
  { 220, 2 },
  { 221, 2 },
  { 222, 2 },
  { 223, 2 },
  { 224, 2 },
  { 225, 2 },
  { 226, 2 },
  { 227, 2 },
  { 228, 2 },
  { 206, 0 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 206, 2 },
  { 203, 0 },
  { 203, 2 },
  { 203, 2 },
  { 203, 2 },
  { 229, 1 },
  { 229, 4 },
  { 230, 1 },
  { 230, 2 },
  { 230, 2 },
  { 198, 6 },
  { 231, 3 },
  { 231, 7 },
  { 231, 7 },
  { 231, 2 },
  { 232, 3 },
  { 232, 1 },
  { 234, 2 },
  { 217, 3 },
  { 217, 1 },
  { 235, 1 },
  { 235, 1 },
  { 235, 1 },
  { 235, 1 },
  { 235, 1 },
  { 235, 2 },
  { 235, 2 },
  { 235, 2 },
  { 235, 2 },
  { 198, 1 },
  { 233, 12 },
  { 233, 2 },
  { 247, 2 },
  { 247, 0 },
  { 236, 3 },
  { 236, 2 },
  { 249, 2 },
  { 249, 1 },
  { 249, 0 },
  { 237, 2 },
  { 250, 2 },
  { 250, 4 },
  { 251, 1 },
  { 239, 4 },
  { 239, 0 },
  { 240, 0 },
  { 240, 6 },
  { 240, 4 },
  { 241, 4 },
  { 241, 0 },
  { 243, 0 },
  { 243, 3 },
  { 252, 4 },
  { 252, 2 },
  { 254, 2 },
  { 255, 1 },
  { 255, 1 },
  { 255, 0 },
  { 242, 0 },
  { 242, 3 },
  { 256, 3 },
  { 256, 1 },
  { 244, 0 },
  { 244, 2 },
  { 246, 0 },
  { 246, 2 },
  { 246, 4 },
  { 246, 4 },
  { 245, 0 },
  { 245, 2 },
  { 245, 4 },
  { 245, 4 },
  { 238, 0 },
  { 238, 2 },
  { 248, 3 },
  { 248, 1 },
  { 248, 3 },
  { 248, 3 },
  { 248, 1 },
  { 248, 2 },
  { 248, 2 },
  { 248, 1 },
  { 248, 2 },
  { 248, 2 },
  { 248, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 1 },
  { 248, 4 },
  { 248, 4 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 3 },
  { 248, 5 },
  { 257, 3 },
  { 257, 1 },
  { 258, 1 },
  { 258, 0 },
  { 198, 4 },
  { 259, 4 },
  { 259, 5 },
  { 260, 3 },
  { 260, 1 },
  { 198, 3 },
  { 198, 7 },
  { 198, 7 },
  { 198, 7 },
  { 198, 7 },
  { 198, 8 },
  { 198, 9 },
  { 198, 5 },
  { 198, 7 },
  { 198, 7 },
};

static void yy_accept(yyParser *);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  unsigned int yyruleno        /* Number of the rule by which to reduce */
) {
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = &yypParser->yystack[yypParser->yyidx];
#ifndef NDEBUG
  if (yyTraceFILE && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ) {
    yysize = yyRuleInfo[yyruleno].nrhs;
    fprintf(yyTraceFILE, "%sReduce [%s], go to state %d.\n", yyTracePrompt,
      yyRuleName[yyruleno], yymsp[-yysize].stateno);
  }
#endif /* NDEBUG */

  /* Check that the stack is large enough to grow by a single entry
  ** if the RHS of the rule is empty.  This ensures that there is room
  ** enough on the stack to push the LHS value */
  if (yyRuleInfo[yyruleno].nrhs == 0) {
#ifdef YYTRACKMAXSTACKDEPTH
    if (yypParser->yyidx>yypParser->yyidxMax) {
      yypParser->yyidxMax = yypParser->yyidx;
    }
#endif
#if YYSTACKDEPTH>0 
    if (yypParser->yyidx>=YYSTACKDEPTH-1) {
      yyStackOverflow(yypParser);
      return;
    }
#else
    if (yypParser->yyidx>=yypParser->yystksz-1) {
      yyGrowStack(yypParser);
      if (yypParser->yyidx>=yypParser->yystksz-1) {
        yyStackOverflow(yypParser);
        return;
      }
    }
#endif
  }

  switch(yyruleno) {
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
#line 59 "sql.y"
{}
#line 1650 "sql.c"
        break;
      case 1: /* cmd ::= SHOW DATABASES */
#line 62 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_DATABASES, 0);}
#line 1655 "sql.c"
        break;
      case 2: /* cmd ::= SHOW MNODES */
#line 63 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_MNODES, 0);}
#line 1660 "sql.c"
        break;
      case 3: /* cmd ::= SHOW DNODES */
#line 64 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_DNODES, 0);}
#line 1665 "sql.c"
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
#line 65 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_ACCOUNTS, 0);}
#line 1670 "sql.c"
        break;
      case 5: /* cmd ::= SHOW USERS */
#line 66 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_USERS, 0);}
#line 1675 "sql.c"
        break;
      case 6: /* cmd ::= SHOW MODULES */
#line 68 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_MODULES, 0);  }
#line 1680 "sql.c"
        break;
      case 7: /* cmd ::= SHOW QUERIES */
#line 69 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_QUERIES, 0);  }
#line 1685 "sql.c"
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
#line 70 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_CONNECTIONS, 0);}
#line 1690 "sql.c"
        break;
      case 9: /* cmd ::= SHOW STREAMS */
#line 71 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_STREAMS, 0);  }
#line 1695 "sql.c"
        break;
      case 10: /* cmd ::= SHOW CONFIGS */
#line 72 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_CONFIGS, 0);  }
#line 1700 "sql.c"
        break;
      case 11: /* cmd ::= SHOW SCORES */
#line 73 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_SCORES, 0);   }
#line 1705 "sql.c"
        break;
      case 12: /* cmd ::= SHOW GRANTS */
#line 74 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_GRANTS, 0);   }
#line 1710 "sql.c"
        break;
      case 13: /* cmd ::= SHOW VNODES */
#line 76 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_VNODES, 0); }
#line 1715 "sql.c"
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
#line 77 "sql.y"
{ setDCLSQLElems(pInfo, SHOW_VNODES, 1, &yymsp[0].minor.yy0); }
#line 1720 "sql.c"
        break;
      case 15: /* dbPrefix ::= */
      case 44: /* ifexists ::= */ yytestcase(yyruleno == 44);
      case 46: /* ifnotexists ::= */ yytestcase(yyruleno == 46);
#line 80 "sql.y"
{yymsp[1].minor.yy0.n = 0;}
#line 1727 "sql.c"
        break;
      case 16: /* dbPrefix ::= ids DOT */
#line 81 "sql.y"
{yylhsminor.yy0 = yymsp[-1].minor.yy0;  }
#line 1732 "sql.c"
  yymsp[-1].minor.yy0 = yylhsminor.yy0;
        break;
      case 17: /* cpxName ::= */
#line 84 "sql.y"
{yymsp[1].minor.yy0.n = 0;  }
#line 1738 "sql.c"
        break;
      case 18: /* cpxName ::= DOT ids */
#line 85 "sql.y"
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; yymsp[-1].minor.yy0.n += 1;    }
#line 1743 "sql.c"
        break;
      case 19: /* cmd ::= SHOW dbPrefix TABLES */
#line 87 "sql.y"
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 1, &yymsp[-1].minor.yy0);
}
#line 1750 "sql.c"
        break;
      case 20: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
#line 91 "sql.y"
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 2, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
#line 1757 "sql.c"
        break;
      case 21: /* cmd ::= SHOW dbPrefix STABLES */
#line 95 "sql.y"
{
    setDCLSQLElems(pInfo, SHOW_STABLES, 1, &yymsp[-1].minor.yy0);
}
#line 1764 "sql.c"
        break;
      case 22: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
#line 99 "sql.y"
{
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_STABLES, 2, &token, &yymsp[0].minor.yy0);
}
#line 1773 "sql.c"
        break;
      case 23: /* cmd ::= SHOW dbPrefix VGROUPS */
#line 105 "sql.y"
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 1, &token);
}
#line 1782 "sql.c"
        break;
      case 24: /* cmd ::= SHOW dbPrefix VGROUPS ids */
#line 111 "sql.y"
{
    SSQLToken token;
    setDBName(&token, &yymsp[-2].minor.yy0);    
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 2, &token, &yymsp[0].minor.yy0);
}
#line 1791 "sql.c"
        break;
      case 25: /* cmd ::= DROP TABLE ifexists ids cpxName */
#line 118 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DROP_TABLE, 2, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
#line 1799 "sql.c"
        break;
      case 26: /* cmd ::= DROP DATABASE ifexists ids */
#line 123 "sql.y"
{ setDCLSQLElems(pInfo, DROP_DATABASE, 2, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
#line 1804 "sql.c"
        break;
      case 27: /* cmd ::= DROP DNODE IPTOKEN */
#line 124 "sql.y"
{ setDCLSQLElems(pInfo, DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
#line 1809 "sql.c"
        break;
      case 28: /* cmd ::= DROP USER ids */
#line 125 "sql.y"
{ setDCLSQLElems(pInfo, DROP_USER, 1, &yymsp[0].minor.yy0);     }
#line 1814 "sql.c"
        break;
      case 29: /* cmd ::= DROP ACCOUNT ids */
#line 126 "sql.y"
{ setDCLSQLElems(pInfo, DROP_ACCOUNT, 1, &yymsp[0].minor.yy0);  }
#line 1819 "sql.c"
        break;
      case 30: /* cmd ::= USE ids */
#line 129 "sql.y"
{ setDCLSQLElems(pInfo, USE_DATABASE, 1, &yymsp[0].minor.yy0);}
#line 1824 "sql.c"
        break;
      case 31: /* cmd ::= DESCRIBE ids cpxName */
#line 132 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
#line 1832 "sql.c"
        break;
      case 32: /* cmd ::= ALTER USER ids PASS ids */
#line 138 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_USER_PASSWD, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);    }
#line 1837 "sql.c"
        break;
      case 33: /* cmd ::= ALTER USER ids PRIVILEGE ids */
#line 139 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_USER_PRIVILEGES, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 1842 "sql.c"
        break;
      case 34: /* cmd ::= ALTER DNODE IPTOKEN ids */
#line 140 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 1847 "sql.c"
        break;
      case 35: /* cmd ::= ALTER DNODE IPTOKEN ids ids */
#line 141 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
#line 1852 "sql.c"
        break;
      case 36: /* cmd ::= ALTER LOCAL ids */
#line 142 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 1, &yymsp[0].minor.yy0);              }
#line 1857 "sql.c"
        break;
      case 37: /* cmd ::= ALTER LOCAL ids ids */
#line 143 "sql.y"
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
#line 1862 "sql.c"
        break;
      case 38: /* cmd ::= ALTER DATABASE ids alter_db_optr */
#line 144 "sql.y"
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, ALTER_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
#line 1867 "sql.c"
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids acct_optr */
#line 146 "sql.y"
{ SSQLToken t = {0};  setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-1].minor.yy0, &t, &yymsp[0].minor.yy155);}
#line 1872 "sql.c"
        break;
      case 40: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
#line 147 "sql.y"
{ setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
#line 1877 "sql.c"
        break;
      case 41: /* ids ::= ID */
      case 42: /* ids ::= STRING */ yytestcase(yyruleno == 42);
#line 153 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0; }
#line 1883 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 43: /* ifexists ::= IF EXISTS */
#line 157 "sql.y"
{yymsp[-1].minor.yy0.n = 1;}
#line 1889 "sql.c"
        break;
      case 45: /* ifnotexists ::= IF NOT EXISTS */
#line 161 "sql.y"
{yymsp[-2].minor.yy0.n = 1;}
#line 1894 "sql.c"
        break;
      case 47: /* cmd ::= CREATE DNODE IPTOKEN */
#line 166 "sql.y"
{ setDCLSQLElems(pInfo, CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
#line 1899 "sql.c"
        break;
      case 48: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
#line 168 "sql.y"
{ setCreateAcctSQL(pInfo, CREATE_ACCOUNT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
#line 1904 "sql.c"
        break;
      case 49: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
#line 169 "sql.y"
{ setCreateDBSQL(pInfo, CREATE_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
#line 1909 "sql.c"
        break;
      case 50: /* cmd ::= CREATE USER ids PASS ids */
#line 170 "sql.y"
{ setDCLSQLElems(pInfo, CREATE_USER, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
#line 1914 "sql.c"
        break;
      case 51: /* pps ::= */
      case 53: /* tseries ::= */ yytestcase(yyruleno == 53);
      case 55: /* dbs ::= */ yytestcase(yyruleno == 55);
      case 57: /* streams ::= */ yytestcase(yyruleno == 57);
      case 59: /* storage ::= */ yytestcase(yyruleno == 59);
      case 61: /* qtime ::= */ yytestcase(yyruleno == 61);
      case 63: /* users ::= */ yytestcase(yyruleno == 63);
      case 65: /* conns ::= */ yytestcase(yyruleno == 65);
      case 67: /* state ::= */ yytestcase(yyruleno == 67);
#line 172 "sql.y"
{yymsp[1].minor.yy0.n = 0;   }
#line 1927 "sql.c"
        break;
      case 52: /* pps ::= PPS INTEGER */
      case 54: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno == 54);
      case 56: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno == 56);
      case 58: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno == 58);
      case 60: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno == 60);
      case 62: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno == 62);
      case 64: /* users ::= USERS INTEGER */ yytestcase(yyruleno == 64);
      case 66: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno == 66);
      case 68: /* state ::= STATE ids */ yytestcase(yyruleno == 68);
#line 173 "sql.y"
{yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;     }
#line 1940 "sql.c"
        break;
      case 69: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
#line 200 "sql.y"
{
    yylhsminor.yy155.users   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yylhsminor.yy155.dbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yylhsminor.yy155.tseries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yylhsminor.yy155.streams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yylhsminor.yy155.pps     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yylhsminor.yy155.storage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy155.qtime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yylhsminor.yy155.conns   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yylhsminor.yy155.stat    = yymsp[0].minor.yy0;
}
#line 1955 "sql.c"
  yymsp[-8].minor.yy155 = yylhsminor.yy155;
        break;
      case 70: /* keep ::= KEEP tagitemlist */
#line 214 "sql.y"
{ yymsp[-1].minor.yy480 = yymsp[0].minor.yy480; }
#line 1961 "sql.c"
        break;
      case 71: /* tables ::= TABLES INTEGER */
      case 72: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno == 72);
      case 73: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno == 73);
      case 74: /* days ::= DAYS INTEGER */ yytestcase(yyruleno == 74);
      case 75: /* rows ::= ROWS INTEGER */ yytestcase(yyruleno == 75);
      case 76: /* ablocks ::= ABLOCKS ID */ yytestcase(yyruleno == 76);
      case 77: /* tblocks ::= TBLOCKS INTEGER */ yytestcase(yyruleno == 77);
      case 78: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno == 78);
      case 79: /* clog ::= CLOG INTEGER */ yytestcase(yyruleno == 79);
      case 80: /* comp ::= COMP INTEGER */ yytestcase(yyruleno == 80);
      case 81: /* prec ::= PRECISION STRING */ yytestcase(yyruleno == 81);
#line 216 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0; }
#line 1976 "sql.c"
        break;
      case 82: /* db_optr ::= */
#line 230 "sql.y"
{setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
#line 1981 "sql.c"
        break;
      case 83: /* db_optr ::= db_optr tables */
      case 97: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno == 97);
#line 232 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 1987 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 84: /* db_optr ::= db_optr cache */
#line 233 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 1993 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 85: /* db_optr ::= db_optr replica */
      case 96: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno == 96);
#line 234 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2000 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 86: /* db_optr ::= db_optr days */
#line 235 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2006 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 87: /* db_optr ::= db_optr rows */
#line 236 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2012 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 88: /* db_optr ::= db_optr ablocks */
#line 237 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
#line 2018 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 89: /* db_optr ::= db_optr tblocks */
#line 238 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2024 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 90: /* db_optr ::= db_optr ctime */
#line 239 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2030 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 91: /* db_optr ::= db_optr clog */
#line 240 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2036 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 92: /* db_optr ::= db_optr comp */
#line 241 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2042 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 93: /* db_optr ::= db_optr prec */
#line 242 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.precision = yymsp[0].minor.yy0; }
#line 2048 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 94: /* db_optr ::= db_optr keep */
      case 98: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno == 98);
#line 243 "sql.y"
{ yylhsminor.yy262 = yymsp[-1].minor.yy262; yylhsminor.yy262.keep = yymsp[0].minor.yy480; }
#line 2055 "sql.c"
  yymsp[-1].minor.yy262 = yylhsminor.yy262;
        break;
      case 95: /* alter_db_optr ::= */
#line 246 "sql.y"
{ setDefaultCreateDbOption(&yymsp[1].minor.yy262);}
#line 2061 "sql.c"
        break;
      case 99: /* typename ::= ids */
#line 253 "sql.y"
{ tSQLSetColumnType (&yylhsminor.yy397, &yymsp[0].minor.yy0); }
#line 2066 "sql.c"
  yymsp[0].minor.yy397 = yylhsminor.yy397;
        break;
      case 100: /* typename ::= ids LP signed RP */
#line 256 "sql.y"
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;          // negative value of name length
    tSQLSetColumnType(&yylhsminor.yy397, &yymsp[-3].minor.yy0);
}
#line 2075 "sql.c"
  yymsp[-3].minor.yy397 = yylhsminor.yy397;
        break;
      case 101: /* signed ::= INTEGER */
#line 262 "sql.y"
{ yylhsminor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2081 "sql.c"
  yymsp[0].minor.yy369 = yylhsminor.yy369;
        break;
      case 102: /* signed ::= PLUS INTEGER */
#line 263 "sql.y"
{ yymsp[-1].minor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
#line 2087 "sql.c"
        break;
      case 103: /* signed ::= MINUS INTEGER */
#line 264 "sql.y"
{ yymsp[-1].minor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
#line 2092 "sql.c"
        break;
      case 104: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
#line 267 "sql.y"
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
#line 2100 "sql.c"
        break;
      case 105: /* create_table_args ::= LP columnlist RP */
#line 273 "sql.y"
{
    yymsp[-2].minor.yy344 = tSetCreateSQLElems(yymsp[-1].minor.yy421, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, yymsp[-2].minor.yy344, NULL, TSQL_CREATE_NORMAL_METER);
}
#line 2108 "sql.c"
        break;
      case 106: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
#line 279 "sql.y"
{
    yymsp[-6].minor.yy344 = tSetCreateSQLElems(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, yymsp[-6].minor.yy344, NULL, TSQL_CREATE_NORMAL_METRIC);
}
#line 2116 "sql.c"
        break;
      case 107: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
#line 286 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yymsp[-6].minor.yy344 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy480, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, yymsp[-6].minor.yy344, NULL, TSQL_CREATE_METER_FROM_METRIC);
}
#line 2125 "sql.c"
        break;
      case 108: /* create_table_args ::= AS select */
#line 294 "sql.y"
{
    yymsp[-1].minor.yy344 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy138, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yymsp[-1].minor.yy344, NULL, TSQL_CREATE_STREAM);
}
#line 2133 "sql.c"
        break;
      case 109: /* columnlist ::= columnlist COMMA column */
#line 302 "sql.y"
{yylhsminor.yy421 = tFieldListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy397);   }
#line 2138 "sql.c"
  yymsp[-2].minor.yy421 = yylhsminor.yy421;
        break;
      case 110: /* columnlist ::= column */
#line 303 "sql.y"
{yylhsminor.yy421 = tFieldListAppend(NULL, &yymsp[0].minor.yy397);}
#line 2144 "sql.c"
  yymsp[0].minor.yy421 = yylhsminor.yy421;
        break;
      case 111: /* column ::= ids typename */
#line 307 "sql.y"
{
    tSQLSetColumnInfo(&yylhsminor.yy397, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy397);
}
#line 2152 "sql.c"
  yymsp[-1].minor.yy397 = yylhsminor.yy397;
        break;
      case 112: /* tagitemlist ::= tagitemlist COMMA tagitem */
#line 315 "sql.y"
{ yylhsminor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);    }
#line 2158 "sql.c"
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 113: /* tagitemlist ::= tagitem */
#line 316 "sql.y"
{ yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1); }
#line 2164 "sql.c"
  yymsp[0].minor.yy480 = yylhsminor.yy480;
        break;
      case 114: /* tagitem ::= INTEGER */
      case 115: /* tagitem ::= FLOAT */ yytestcase(yyruleno == 115);
      case 116: /* tagitem ::= STRING */ yytestcase(yyruleno == 116);
      case 117: /* tagitem ::= BOOL */ yytestcase(yyruleno == 117);
#line 318 "sql.y"
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yylhsminor.yy236, &yymsp[0].minor.yy0); }
#line 2173 "sql.c"
  yymsp[0].minor.yy236 = yylhsminor.yy236;
        break;
      case 118: /* tagitem ::= NULL */
#line 322 "sql.y"
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yylhsminor.yy236, &yymsp[0].minor.yy0); }
#line 2179 "sql.c"
  yymsp[0].minor.yy236 = yylhsminor.yy236;
        break;
      case 119: /* tagitem ::= MINUS INTEGER */
      case 120: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno == 120);
      case 121: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno == 121);
      case 122: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno == 122);
#line 324 "sql.y"
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yylhsminor.yy236, &yymsp[-1].minor.yy0);
}
#line 2193 "sql.c"
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 123: /* cmd ::= select */
#line 353 "sql.y"
{
    setSQLInfo(pInfo, yymsp[0].minor.yy138, NULL, TSQL_QUERY_METER);
}
#line 2201 "sql.c"
        break;
      case 124: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
#line 359 "sql.y"
{
  yylhsminor.yy138 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy284, yymsp[-9].minor.yy480, yymsp[-8].minor.yy244, yymsp[-4].minor.yy480, yymsp[-3].minor.yy480, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy480, &yymsp[0].minor.yy162, &yymsp[-1].minor.yy162);
}
#line 2208 "sql.c"
  yymsp[-11].minor.yy138 = yylhsminor.yy138;
        break;
      case 125: /* select ::= SELECT selcollist */
#line 367 "sql.y"
{
  yylhsminor.yy138 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy284, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
#line 2216 "sql.c"
  yymsp[-1].minor.yy138 = yylhsminor.yy138;
        break;
      case 126: /* sclp ::= selcollist COMMA */
#line 379 "sql.y"
{yylhsminor.yy284 = yymsp[-1].minor.yy284;}
#line 2222 "sql.c"
  yymsp[-1].minor.yy284 = yylhsminor.yy284;
        break;
      case 127: /* sclp ::= */
#line 380 "sql.y"
{yymsp[1].minor.yy284 = 0;}
#line 2228 "sql.c"
        break;
      case 128: /* selcollist ::= sclp expr as */
#line 381 "sql.y"
{
   yylhsminor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284, yymsp[-1].minor.yy244, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
#line 2235 "sql.c"
  yymsp[-2].minor.yy284 = yylhsminor.yy284;
        break;
      case 129: /* selcollist ::= sclp STAR */
#line 385 "sql.y"
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yylhsminor.yy284 = tSQLExprListAppend(yymsp[-1].minor.yy284, pNode, 0);
}
#line 2244 "sql.c"
  yymsp[-1].minor.yy284 = yylhsminor.yy284;
        break;
      case 130: /* as ::= AS ids */
#line 394 "sql.y"
{ yymsp[-1].minor.yy0 = yymsp[0].minor.yy0;    }
#line 2250 "sql.c"
        break;
      case 131: /* as ::= ids */
#line 395 "sql.y"
{ yylhsminor.yy0 = yymsp[0].minor.yy0;    }
#line 2255 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 132: /* as ::= */
#line 396 "sql.y"
{ yymsp[1].minor.yy0.n = 0;  }
#line 2261 "sql.c"
        break;
      case 133: /* from ::= FROM tablelist */
#line 401 "sql.y"
{yymsp[-1].minor.yy480 = yymsp[0].minor.yy480;}
#line 2266 "sql.c"
        break;
      case 134: /* tablelist ::= ids cpxName */
#line 404 "sql.y"
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
#line 2271 "sql.c"
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 135: /* tablelist ::= tablelist COMMA ids cpxName */
#line 405 "sql.y"
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yylhsminor.yy480 = tVariantListAppendToken(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, -1);   }
#line 2277 "sql.c"
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 136: /* tmvar ::= VARIABLE */
#line 409 "sql.y"
{yylhsminor.yy0 = yymsp[0].minor.yy0;}
#line 2283 "sql.c"
  yymsp[0].minor.yy0 = yylhsminor.yy0;
        break;
      case 137: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 142: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno == 142);
#line 412 "sql.y"
{yymsp[-3].minor.yy0 = yymsp[-1].minor.yy0;     }
#line 2290 "sql.c"
        break;
      case 138: /* interval_opt ::= */
      case 143: /* sliding_opt ::= */ yytestcase(yyruleno == 143);
#line 413 "sql.y"
{yymsp[1].minor.yy0.n = 0; yymsp[1].minor.yy0.z = NULL; yymsp[1].minor.yy0.type = 0;   }
#line 2296 "sql.c"
        break;
      case 139: /* fill_opt ::= */
#line 417 "sql.y"
{yymsp[1].minor.yy480 = 0;     }
#line 2301 "sql.c"
        break;
      case 140: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
#line 418 "sql.y"
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy480, &A, -1, 0);
    yymsp[-5].minor.yy480 = yymsp[-1].minor.yy480;
}
#line 2313 "sql.c"
        break;
      case 141: /* fill_opt ::= FILL LP ID RP */
#line 427 "sql.y"
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yymsp[-3].minor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
#line 2321 "sql.c"
        break;
      case 144: /* orderby_opt ::= */
      case 152: /* groupby_opt ::= */ yytestcase(yyruleno == 152);
#line 445 "sql.y"
{yymsp[1].minor.yy480 = 0;}
#line 2327 "sql.c"
        break;
      case 145: /* orderby_opt ::= ORDER BY sortlist */
      case 153: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno == 153);
#line 446 "sql.y"
{yymsp[-2].minor.yy480 = yymsp[0].minor.yy480;}
#line 2333 "sql.c"
        break;
      case 146: /* sortlist ::= sortlist COMMA item sortorder */
#line 448 "sql.y"
{
    yylhsminor.yy480 = tVariantListAppend(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
#line 2340 "sql.c"
  yymsp[-3].minor.yy480 = yylhsminor.yy480;
        break;
      case 147: /* sortlist ::= item sortorder */
#line 452 "sql.y"
{
  yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
#line 2348 "sql.c"
  yymsp[-1].minor.yy480 = yylhsminor.yy480;
        break;
      case 148: /* item ::= ids cpxName */
#line 457 "sql.y"
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yylhsminor.yy236, &yymsp[-1].minor.yy0);
}
#line 2359 "sql.c"
  yymsp[-1].minor.yy236 = yylhsminor.yy236;
        break;
      case 149: /* sortorder ::= ASC */
#line 465 "sql.y"
{yymsp[0].minor.yy220 = TSQL_SO_ASC; }
#line 2365 "sql.c"
        break;
      case 150: /* sortorder ::= DESC */
#line 466 "sql.y"
{yymsp[0].minor.yy220 = TSQL_SO_DESC;}
#line 2370 "sql.c"
        break;
      case 151: /* sortorder ::= */
#line 467 "sql.y"
{yymsp[1].minor.yy220 = TSQL_SO_ASC;}
#line 2375 "sql.c"
        break;
      case 154: /* grouplist ::= grouplist COMMA item */
#line 478 "sql.y"
{
  yylhsminor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);
}
#line 2382 "sql.c"
  yymsp[-2].minor.yy480 = yylhsminor.yy480;
        break;
      case 155: /* grouplist ::= item */
#line 482 "sql.y"
{
  yylhsminor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1);
}
#line 2390 "sql.c"
  yymsp[0].minor.yy480 = yylhsminor.yy480;
        break;
      case 156: /* having_opt ::= */
      case 166: /* where_opt ::= */ yytestcase(yyruleno == 166);
      case 202: /* expritem ::= */ yytestcase(yyruleno == 202);
#line 489 "sql.y"
{yymsp[1].minor.yy244 = 0;}
#line 2398 "sql.c"
        break;
      case 157: /* having_opt ::= HAVING expr */
      case 167: /* where_opt ::= WHERE expr */ yytestcase(yyruleno == 167);
#line 490 "sql.y"
{yymsp[-1].minor.yy244 = yymsp[0].minor.yy244;}
#line 2404 "sql.c"
        break;
      case 158: /* limit_opt ::= */
      case 162: /* slimit_opt ::= */ yytestcase(yyruleno == 162);
#line 494 "sql.y"
{yymsp[1].minor.yy162.limit = -1; yymsp[1].minor.yy162.offset = 0;}
#line 2410 "sql.c"
        break;
      case 159: /* limit_opt ::= LIMIT signed */
      case 163: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno == 163);
#line 495 "sql.y"
{yymsp[-1].minor.yy162.limit = yymsp[0].minor.yy369;  yymsp[-1].minor.yy162.offset = 0;}
#line 2416 "sql.c"
        break;
      case 160: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 164: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno == 164);
#line 497 "sql.y"
{yymsp[-3].minor.yy162.limit = yymsp[-2].minor.yy369;  yymsp[-3].minor.yy162.offset = yymsp[0].minor.yy369;}
#line 2422 "sql.c"
        break;
      case 161: /* limit_opt ::= LIMIT signed COMMA signed */
      case 165: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno == 165);
#line 499 "sql.y"
{yymsp[-3].minor.yy162.limit = yymsp[0].minor.yy369;  yymsp[-3].minor.yy162.offset = yymsp[-2].minor.yy369;}
#line 2428 "sql.c"
        break;
      case 168: /* expr ::= LP expr RP */
#line 520 "sql.y"
{yymsp[-2].minor.yy244 = yymsp[-1].minor.yy244; }
#line 2433 "sql.c"
        break;
      case 169: /* expr ::= ID */
#line 522 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
#line 2438 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 170: /* expr ::= ID DOT ID */
#line 523 "sql.y"
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
#line 2444 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 171: /* expr ::= ID DOT STAR */
#line 524 "sql.y"
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
#line 2450 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 172: /* expr ::= INTEGER */
#line 526 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
#line 2456 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 173: /* expr ::= MINUS INTEGER */
      case 174: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno == 174);
#line 527 "sql.y"
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
#line 2463 "sql.c"
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 175: /* expr ::= FLOAT */
#line 529 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
#line 2469 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 176: /* expr ::= MINUS FLOAT */
      case 177: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno == 177);
#line 530 "sql.y"
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
#line 2476 "sql.c"
  yymsp[-1].minor.yy244 = yylhsminor.yy244;
        break;
      case 178: /* expr ::= STRING */
#line 532 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
#line 2482 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 179: /* expr ::= NOW */
#line 533 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
#line 2488 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 180: /* expr ::= VARIABLE */
#line 534 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
#line 2494 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 181: /* expr ::= BOOL */
#line 535 "sql.y"
{yylhsminor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
#line 2500 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 182: /* expr ::= ID LP exprlist RP */
#line 537 "sql.y"
{
  yylhsminor.yy244 = tSQLExprCreateFunction(yymsp[-1].minor.yy284, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
#line 2508 "sql.c"
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 183: /* expr ::= ID LP STAR RP */
#line 542 "sql.y"
{
  yylhsminor.yy244 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
#line 2516 "sql.c"
  yymsp[-3].minor.yy244 = yylhsminor.yy244;
        break;
      case 184: /* expr ::= expr AND expr */
#line 547 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_AND);}
#line 2522 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 185: /* expr ::= expr OR expr */
#line 548 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_OR); }
#line 2528 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 186: /* expr ::= expr LT expr */
#line 551 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LT);}
#line 2534 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 187: /* expr ::= expr GT expr */
#line 552 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GT);}
#line 2540 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 188: /* expr ::= expr LE expr */
#line 553 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LE);}
#line 2546 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 189: /* expr ::= expr GE expr */
#line 554 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GE);}
#line 2552 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 190: /* expr ::= expr NE expr */
#line 555 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_NE);}
#line 2558 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 191: /* expr ::= expr EQ expr */
#line 556 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_EQ);}
#line 2564 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 192: /* expr ::= expr PLUS expr */
#line 559 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_PLUS);  }
#line 2570 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 193: /* expr ::= expr MINUS expr */
#line 560 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_MINUS); }
#line 2576 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 194: /* expr ::= expr STAR expr */
#line 561 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_STAR);  }
#line 2582 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 195: /* expr ::= expr SLASH expr */
#line 562 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_DIVIDE);}
#line 2588 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 196: /* expr ::= expr REM expr */
#line 563 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_REM);   }
#line 2594 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 197: /* expr ::= expr LIKE expr */
#line 566 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LIKE);  }
#line 2600 "sql.c"
  yymsp[-2].minor.yy244 = yylhsminor.yy244;
        break;
      case 198: /* expr ::= expr IN LP exprlist RP */
#line 569 "sql.y"
{yylhsminor.yy244 = tSQLExprCreate(yymsp[-4].minor.yy244, (tSQLExpr*)yymsp[-1].minor.yy284, TK_IN); }
#line 2606 "sql.c"
  yymsp[-4].minor.yy244 = yylhsminor.yy244;
        break;
      case 199: /* exprlist ::= exprlist COMMA expritem */
      case 206: /* itemlist ::= itemlist COMMA expr */ yytestcase(yyruleno == 206);
#line 577 "sql.y"
{yylhsminor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284,yymsp[0].minor.yy244,0);}
#line 2613 "sql.c"
  yymsp[-2].minor.yy284 = yylhsminor.yy284;
        break;
      case 200: /* exprlist ::= expritem */
      case 207: /* itemlist ::= expr */ yytestcase(yyruleno == 207);
#line 578 "sql.y"
{yylhsminor.yy284 = tSQLExprListAppend(0,yymsp[0].minor.yy244,0);}
#line 2620 "sql.c"
  yymsp[0].minor.yy284 = yylhsminor.yy284;
        break;
      case 201: /* expritem ::= expr */
#line 579 "sql.y"
{yylhsminor.yy244 = yymsp[0].minor.yy244;}
#line 2626 "sql.c"
  yymsp[0].minor.yy244 = yylhsminor.yy244;
        break;
      case 203: /* cmd ::= INSERT INTO cpxName insert_value_list */
#line 584 "sql.y"
{
    tSetInsertSQLElems(pInfo, &yymsp[-1].minor.yy0, yymsp[0].minor.yy237);
}
#line 2634 "sql.c"
        break;
      case 204: /* insert_value_list ::= VALUES LP itemlist RP */
#line 589 "sql.y"
{yymsp[-3].minor.yy237 = tSQLListListAppend(NULL, yymsp[-1].minor.yy284);}
#line 2639 "sql.c"
        break;
      case 205: /* insert_value_list ::= insert_value_list VALUES LP itemlist RP */
#line 591 "sql.y"
{yylhsminor.yy237 = tSQLListListAppend(yymsp[-4].minor.yy237, yymsp[-1].minor.yy284);}
#line 2644 "sql.c"
  yymsp[-4].minor.yy237 = yylhsminor.yy237;
        break;
      case 208: /* cmd ::= RESET QUERY CACHE */
#line 603 "sql.y"
{ setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}
#line 2650 "sql.c"
        break;
      case 209: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
#line 606 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}
#line 2659 "sql.c"
        break;
      case 210: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
#line 612 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}
#line 2672 "sql.c"
        break;
      case 211: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
#line 623 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
#line 2681 "sql.c"
        break;
      case 212: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
#line 628 "sql.y"
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}
#line 2694 "sql.c"
        break;
      case 213: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
#line 638 "sql.y"
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, ALTER_TABLE_TAGS_CHG);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_CHG);
}
#line 2710 "sql.c"
        break;
      case 214: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
#line 651 "sql.y"
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy236, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}
#line 2724 "sql.c"
        break;
      case 215: /* cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
#line 663 "sql.y"
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &yymsp[-2].minor.yy0);}
#line 2729 "sql.c"
        break;
      case 216: /* cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
#line 664 "sql.y"
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &yymsp[-4].minor.yy0);}
#line 2734 "sql.c"
        break;
      case 217: /* cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
#line 665 "sql.y"
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_QUERY, 1, &yymsp[-4].minor.yy0);}
#line 2739 "sql.c"
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if (yyact <= YY_MAX_SHIFTREDUCE) {
    if (yyact>YY_MAX_SHIFT) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
    yypParser->yyidx -= yysize - 1;
    yymsp -= yysize-1;
    yymsp->stateno = (YYACTIONTYPE)yyact;
    yymsp->major = (YYCODETYPE)yygoto;
    yyTraceShift(yypParser, yyact);
  } else{
    assert( yyact == YY_ACCEPT_ACTION);
    yypParser->yyidx -= yysize;
    yy_accept(yypParser);
  }
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
) {
  ParseARG_FETCH;
#ifndef NDEBUG
  if (yyTraceFILE) {
    fprintf(yyTraceFILE,"%sFail!\n",yyTracePrompt);
  }
#endif
  while(yypParser->yyidx>=0) yy_pop_parser_stack(yypParser);
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
) {
  ParseARG_FETCH;
#define TOKEN yyminor
/************ Begin %syntax_error code ****************************************/
#line 33 "sql.y"

  pInfo->validSql = false;
  int32_t outputBufLen = tListLen(pInfo->pzErrMsg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->pzErrMsg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->pzErrMsg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->pzErrMsg, "Incomplete SQL statement");
  }

  assert(len <= outputBufLen);
#line 2819 "sql.c"
/************ End %syntax_error code ******************************************/
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
) {
  ParseARG_FETCH;
#ifndef NDEBUG
  if (yyTraceFILE) {
    fprintf(yyTraceFILE,"%sAccept!\n",yyTracePrompt);
  }
#endif
  while(yypParser->yyidx>=0) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */
/*********** Begin %parse_accept code *****************************************/
#line 57 "sql.y"
#line 2841 "sql.c"
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
) {
  YYMINORTYPE yyminorunion;
  unsigned int yyact;   /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  /* (re)initialize the parser, if necessary */
  yypParser = (yyParser *)yyp;
  if (yypParser->yyidx<0) {
#if YYSTACKDEPTH<=0
    if (yypParser->yystksz <=0) {
      yyStackOverflow(yypParser);
      return;
    }
#endif
    yypParser->yyidx = 0;
#ifndef YYNOERRORRECOVERY
    yypParser->yyerrcnt = -1;
#endif
    yypParser->yystack[0].stateno = 0;
    yypParser->yystack[0].major = 0;
#ifndef NDEBUG
    if (yyTraceFILE) {
      fprintf(yyTraceFILE,"%sInitialize. Empty stack. State 0\n",
              yyTracePrompt);
    }
#endif
  }
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor == 0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if (yyTraceFILE) {
    fprintf(yyTraceFILE,"%sInput '%s'\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if (yyact <= YY_MAX_SHIFTREDUCE) {
      if (yyact > YY_MAX_SHIFT) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
      yy_shift(yypParser,yyact,yymajor,yyminor);
#ifndef YYNOERRORRECOVERY
      yypParser->yyerrcnt--;
#endif
      yymajor = YYNOCODE;
    } else if (yyact <= YY_MAX_REDUCE) {
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE);
    } else{
      assert( yyact == YY_ERROR_ACTION);
      yyminorunion.yy0 = yyminor;
#ifdef YYERRORSYMBOL
      int yymx;
#endif
#ifndef NDEBUG
      if (yyTraceFILE) {
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
      if (yypParser->yyerrcnt<0) {
        yy_syntax_error(yypParser,yymajor,yyminor);
      }
      yymx = yypParser->yystack[yypParser->yyidx].major;
      if (yymx == YYERRORSYMBOL || yyerrorhit) {
#ifndef NDEBUG
        if (yyTraceFILE) {
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor, &yyminorunion);
        yymajor = YYNOCODE;
      } else{
        while(
          yypParser->yyidx >= 0 &&
          yymx != YYERRORSYMBOL &&
          (yyact = yy_find_reduce_action(
                        yypParser->yystack[yypParser->yyidx].stateno,
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
        ) {
          yy_pop_parser_stack(yypParser);
        }
        if (yypParser->yyidx < 0 || yymajor == 0) {
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
          yymajor = YYNOCODE;
        } else if (yymx!=YYERRORSYMBOL) {
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
      if (yypParser->yyerrcnt<=0) {
        yy_syntax_error(yypParser,yymajor, yyminor);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if (yyendofinput) {
        yy_parse_failed(yypParser);
      }
      yymajor = YYNOCODE;
#endif
    }
  }while(yymajor!=YYNOCODE && yypParser->yyidx>=0);
#ifndef NDEBUG
  if (yyTraceFILE) {
    int i;
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i = 1; i <= yypParser->yyidx; i++)
      fprintf(yyTraceFILE,"%c%s", i == 1 ? '[' : ' ', 
              yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}
