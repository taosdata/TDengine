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
#include "tscSQLParser.h"
#include "tutil.h"
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
#define YYNSTATE             252
#define YYNRULE              216
#define YY_MAX_SHIFT         251
#define YY_MIN_SHIFTREDUCE   403
#define YY_MAX_SHIFTREDUCE   618
#define YY_MIN_REDUCE        619
#define YY_MAX_REDUCE        834
#define YY_ERROR_ACTION      835
#define YY_ACCEPT_ACTION     836
#define YY_NO_ACTION         837
/************* End control #defines *******************************************/

/* The yyzerominor constant is used to initialize instances of
** YYMINORTYPE objects to zero. */
static const YYMINORTYPE yyzerominor = { 0 };

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
#define YY_ACTTAB_COUNT (531)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   443,   74,   78,  244,   85,   77,  153,  249,  444,  836,
 /*    10 */   251,   80,   43,   45,    7,   37,   38,   62,  111,  171,
 /*    20 */    31,  443,  443,  205,   41,   39,   42,   40,  241,  444,
 /*    30 */   444,  135,   36,   35,   10,  101,   34,   33,   32,   43,
 /*    40 */    45,  600,   37,   38,  156,  524,  135,   31,  135,  133,
 /*    50 */   205,   41,   39,   42,   40,  159,  601,  158,  601,   36,
 /*    60 */    35,  154,  514,   34,   33,   32,  404,  405,  406,  407,
 /*    70 */   408,  409,  410,  411,  412,  413,  414,  415,  250,   21,
 /*    80 */    43,   45,  172,   37,   38,  227,  226,  202,   31,   59,
 /*    90 */    21,  205,   41,   39,   42,   40,   34,   33,   32,   57,
 /*   100 */    36,   35,  550,  551,   34,   33,   32,   45,  232,   37,
 /*   110 */    38,  167,  132,  511,   31,   21,   21,  205,   41,   39,
 /*   120 */    42,   40,  168,  569,  511,  502,   36,   35,  134,  178,
 /*   130 */    34,   33,   32,  243,   37,   38,  186,  512,  183,   31,
 /*   140 */   532,  101,  205,   41,   39,   42,   40,  228,  233,  511,
 /*   150 */   511,   36,   35,  230,  229,   34,   33,   32,   17,  219,
 /*   160 */   242,  218,  217,  216,  215,  214,  213,  212,  211,  496,
 /*   170 */   139,  485,  486,  487,  488,  489,  490,  491,  492,  493,
 /*   180 */   494,  495,  163,  582,   11,   97,  573,  133,  576,  529,
 /*   190 */   579,  597,  163,  582,  166,  556,  573,  200,  576,  155,
 /*   200 */   579,   36,   35,  148,  220,   34,   33,   32,   21,   87,
 /*   210 */    86,  142,  514,  243,  160,  161,  101,  147,  204,  248,
 /*   220 */   247,  426,  514,   76,  160,  161,  163,  582,  530,  241,
 /*   230 */   573,  101,  576,  513,  579,  193,   41,   39,   42,   40,
 /*   240 */   242,  596,  510,   27,   36,   35,   49,  571,   34,   33,
 /*   250 */    32,  114,  115,  224,   65,   68,  505,  441,  160,  161,
 /*   260 */   124,  192,  518,   50,  188,  515,  499,  516,  498,  517,
 /*   270 */   555,  150,  128,  126,  245,   89,   88,   44,  450,  442,
 /*   280 */    61,  124,  124,  572,  595,   60,  581,   44,  575,  527,
 /*   290 */   578,   28,   18,  169,  170,  605,  581,  162,  606,   29,
 /*   300 */   541,  580,   29,  542,   47,   52,  599,   15,  151,  583,
 /*   310 */    14,  580,  574,   14,  577,  508,   73,   72,  507,   47,
 /*   320 */    53,   44,   22,  209,  522,  152,  523,   22,  140,  520,
 /*   330 */   581,  521,    9,    8,    2,   84,   83,  141,  143,  144,
 /*   340 */   145,  615,  146,  137,  131,  580,  138,  136,  531,  566,
 /*   350 */    98,  565,  164,  562,  561,  165,  231,  548,  547,  189,
 /*   360 */   112,  113,  519,  452,  110,  210,  129,   25,  191,  223,
 /*   370 */   225,  614,   70,  613,  611,  116,  470,   26,   23,  130,
 /*   380 */   439,   91,   79,  437,   81,  435,  434,  537,  194,  198,
 /*   390 */   173,   54,  125,  432,  431,  430,  428,  421,  525,  127,
 /*   400 */   425,   51,  423,  102,   46,  203,  103,  104,   95,  199,
 /*   410 */   201,  535,  197,   30,  536,  549,  195,   27,  222,   75,
 /*   420 */   234,  235,  236,  237,  207,   55,  238,  239,  240,  246,
 /*   430 */   149,  618,   63,   66,  175,  433,  174,  176,  177,  617,
 /*   440 */   180,  427,  119,   90,  118,  471,  117,  120,  122,  121,
 /*   450 */   123,   92,  509,    1,   24,  182,  107,  105,  106,  108,
 /*   460 */   109,  179,  181,  616,  184,  185,   12,  609,  190,  187,
 /*   470 */    13,  157,   96,  538,   99,  196,   58,    4,   19,  543,
 /*   480 */   100,    5,  584,    3,   20,   16,  206,    6,  208,   64,
 /*   490 */   483,  482,  481,  480,  479,  478,  477,  476,  474,   47,
 /*   500 */   447,  449,   67,   22,  504,  221,  503,  501,   56,  468,
 /*   510 */   466,   48,  458,  464,   69,  460,  462,  456,  454,  475,
 /*   520 */    71,  473,   82,  429,  445,  419,  417,   93,  619,  621,
 /*   530 */    94,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   64,   65,   66,   67,   68,  199,  200,    9,  197,
 /*    10 */   198,   74,   13,   14,   96,   16,   17,   99,  100,   63,
 /*    20 */    21,    1,    1,   24,   25,   26,   27,   28,   78,    9,
 /*    30 */     9,  248,   33,   34,  248,  200,   37,   38,   39,   13,
 /*    40 */    14,  258,   16,   17,  217,  233,  248,   21,  248,  248,
 /*    50 */    24,   25,   26,   27,   28,  257,  258,  257,  258,   33,
 /*    60 */    34,  260,  235,   37,   38,   39,   45,   46,   47,   48,
 /*    70 */    49,   50,   51,   52,   53,   54,   55,   56,   57,  200,
 /*    80 */    13,   14,  126,   16,   17,  129,  130,  252,   21,  254,
 /*    90 */   200,   24,   25,   26,   27,   28,   37,   38,   39,  100,
 /*   100 */    33,   34,  111,  112,   37,   38,   39,   14,  200,   16,
 /*   110 */    17,  232,  248,  234,   21,  200,  200,   24,   25,   26,
 /*   120 */    27,   28,  232,   97,  234,    5,   33,   34,  248,  125,
 /*   130 */    37,   38,   39,   60,   16,   17,  132,  229,  134,   21,
 /*   140 */   200,  200,   24,   25,   26,   27,   28,  232,  232,  234,
 /*   150 */   234,   33,   34,   33,   34,   37,   38,   39,   85,   86,
 /*   160 */    87,   88,   89,   90,   91,   92,   93,   94,   95,  216,
 /*   170 */   248,  218,  219,  220,  221,  222,  223,  224,  225,  226,
 /*   180 */   227,  228,    1,    2,   44,  200,    5,  248,    7,  249,
 /*   190 */     9,  248,    1,    2,  217,  254,    5,  256,    7,  260,
 /*   200 */     9,   33,   34,   63,  217,   37,   38,   39,  200,   69,
 /*   210 */    70,   71,  235,   60,   33,   34,  200,   77,   37,   60,
 /*   220 */    61,   62,  235,   72,   33,   34,    1,    2,   37,   78,
 /*   230 */     5,  200,    7,  235,    9,  250,   25,   26,   27,   28,
 /*   240 */    87,  248,  234,  103,   33,   34,  101,    1,   37,   38,
 /*   250 */    39,   64,   65,   66,   67,   68,  231,  204,   33,   34,
 /*   260 */   207,  121,    2,  118,  124,    5,  218,    7,  220,    9,
 /*   270 */   254,  131,   64,   65,   66,   67,   68,   96,  204,  204,
 /*   280 */   236,  207,  207,   37,  248,  254,  105,   96,    5,  101,
 /*   290 */     7,  247,  104,   33,   34,   97,  105,   59,   97,  101,
 /*   300 */    97,  120,  101,   97,  101,  101,   97,  101,  248,   97,
 /*   310 */   101,  120,    5,  101,    7,   97,  127,  128,   97,  101,
 /*   320 */   116,   96,  101,   97,    5,  248,    7,  101,  248,    5,
 /*   330 */   105,    7,  127,  128,   96,   72,   73,  248,  248,  248,
 /*   340 */   248,  235,  248,  248,  248,  120,  248,  248,  200,  230,
 /*   350 */   200,  230,  230,  230,  230,  230,  230,  255,  255,  123,
 /*   360 */   200,  200,  102,  200,  237,  200,  200,  200,  259,  200,
 /*   370 */   200,  200,  200,  200,  200,  200,  200,  200,  200,  200,
 /*   380 */   200,   59,  200,  200,  200,  200,  200,  105,  251,  251,
 /*   390 */   200,  115,  200,  200,  200,  200,  200,  200,  246,  200,
 /*   400 */   200,  117,  200,  245,  114,  109,  244,  243,  201,  108,
 /*   410 */   113,  201,  107,  119,  201,  201,  106,  103,   75,   84,
 /*   420 */    83,   49,   80,   82,  201,  201,   53,   81,   79,   75,
 /*   430 */   201,    5,  205,  205,    5,  201,  133,  133,   58,    5,
 /*   440 */     5,  201,  209,  202,  213,  215,  214,  212,  211,  210,
 /*   450 */   208,  202,  233,  206,  203,   58,  240,  242,  241,  239,
 /*   460 */   238,  133,  133,    5,  133,   58,   96,   86,  123,  125,
 /*   470 */    96,    1,  122,   97,   96,   96,  101,  110,  101,   97,
 /*   480 */    96,  110,   97,   96,  101,   96,   98,   96,   98,   72,
 /*   490 */     9,    5,    5,    5,    5,    1,    5,    5,    5,  101,
 /*   500 */    76,   58,   72,  101,    5,   15,    5,   97,   96,    5,
 /*   510 */     5,   16,    5,    5,  128,    5,    5,    5,    5,    5,
 /*   520 */   128,    5,   58,   58,   76,   59,   58,   21,    0,  261,
 /*   530 */    21,
};
#define YY_SHIFT_USE_DFLT (-83)
#define YY_SHIFT_COUNT (251)
#define YY_SHIFT_MIN   (-82)
#define YY_SHIFT_MAX   (528)
static const short yy_shift_ofst[] = {
 /*     0 */   140,   73,  181,  225,   20,   20,   20,   20,   20,   20,
 /*    10 */    -1,   21,  225,  225,  225,  260,  260,  260,   20,   20,
 /*    20 */    20,   20,   20,  151,  153,  -50,  -50,  -83,  191,  225,
 /*    30 */   225,  225,  225,  225,  225,  225,  225,  225,  225,  225,
 /*    40 */   225,  225,  225,  225,  225,  225,  225,  260,  260,  120,
 /*    50 */   120,  120,  120,  120,  120,  -82,  120,   20,   20,   -9,
 /*    60 */    -9,  188,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    70 */    20,   20,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    80 */    20,   20,   20,   20,   20,   20,   20,   20,   20,   20,
 /*    90 */    20,   20,   20,   20,   20,  236,  322,  322,  322,  282,
 /*   100 */   282,  322,  276,  284,  290,  296,  297,  301,  305,  310,
 /*   110 */   294,  314,  322,  322,  343,  343,  322,  335,  337,  372,
 /*   120 */   342,  341,  373,  346,  349,  322,  354,  322,  354,  -83,
 /*   130 */   -83,   26,   67,   67,   67,   67,   67,   93,  118,  211,
 /*   140 */   211,  211,  -63,  168,  168,  168,  168,  187,  208,  -44,
 /*   150 */     4,   59,   59,  159,  198,  201,  203,  206,  209,  212,
 /*   160 */   283,  307,  246,  238,  145,  204,  218,  221,  226,  319,
 /*   170 */   324,  189,  205,  263,  426,  303,  429,  304,  380,  434,
 /*   180 */   328,  435,  329,  397,  458,  331,  407,  381,  344,  370,
 /*   190 */   374,  345,  350,  375,  376,  378,  470,  379,  382,  384,
 /*   200 */   377,  367,  383,  371,  385,  387,  389,  388,  391,  390,
 /*   210 */   417,  481,  486,  487,  488,  489,  494,  491,  492,  493,
 /*   220 */   398,  424,  490,  430,  443,  495,  386,  392,  402,  499,
 /*   230 */   501,  410,  412,  402,  504,  505,  507,  508,  510,  511,
 /*   240 */   512,  513,  514,  516,  464,  465,  448,  506,  509,  466,
 /*   250 */   468,  528,
};
#define YY_REDUCE_USE_DFLT (-218)
#define YY_REDUCE_COUNT (130)
#define YY_REDUCE_MIN   (-217)
#define YY_REDUCE_MAX   (251)
static const short yy_reduce_ofst[] = {
 /*     0 */  -188,  -47, -202, -200,  -59, -165, -121, -110,  -85,  -84,
 /*    10 */   -60, -193, -199,  -61, -217, -173,  -23,  -13,  -15,   16,
 /*    20 */    31,  -92,    8,   53,   48,   74,   75,   44, -214, -136,
 /*    30 */  -120,  -78,  -57,   -7,   36,   60,   77,   80,   89,   90,
 /*    40 */    91,   92,   94,   95,   96,   98,   99,   -2,  106,  119,
 /*    50 */   121,  122,  123,  124,  125,   25,  126,  148,  150,  102,
 /*    60 */   103,  127,  160,  161,  163,  165,  166,  167,  169,  170,
 /*    70 */   171,  172,  173,  174,  175,  176,  177,  178,  179,  180,
 /*    80 */   182,  183,  184,  185,  186,  190,  192,  193,  194,  195,
 /*    90 */   196,  197,  199,  200,  202,  109,  207,  210,  213,  137,
 /*   100 */   138,  214,  152,  158,  162,  164,  215,  217,  216,  220,
 /*   110 */   222,  219,  223,  224,  227,  228,  229,  230,  232,  231,
 /*   120 */   233,  235,  239,  237,  242,  234,  241,  240,  249,  247,
 /*   130 */   251,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   835,  667,  819,  819,  835,  835,  835,  835,  835,  835,
 /*    10 */   749,  634,  835,  835,  819,  835,  835,  835,  835,  835,
 /*    20 */   835,  835,  835,  669,  656,  669,  669,  744,  835,  835,
 /*    30 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  835,
 /*    40 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  835,
 /*    50 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  768,
 /*    60 */   768,  742,  835,  835,  835,  835,  835,  835,  835,  835,
 /*    70 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  654,
 /*    80 */   835,  652,  835,  835,  835,  835,  835,  835,  835,  835,
 /*    90 */   835,  835,  835,  835,  835,  835,  636,  636,  636,  835,
 /*   100 */   835,  636,  775,  779,  773,  761,  769,  760,  756,  755,
 /*   110 */   783,  835,  636,  636,  664,  664,  636,  685,  683,  681,
 /*   120 */   673,  679,  675,  677,  671,  636,  662,  636,  662,  700,
 /*   130 */   713,  835,  823,  824,  784,  818,  774,  802,  801,  814,
 /*   140 */   808,  807,  835,  806,  805,  804,  803,  835,  835,  835,
 /*   150 */   835,  810,  809,  835,  835,  835,  835,  835,  835,  835,
 /*   160 */   835,  835,  835,  786,  780,  776,  835,  835,  835,  835,
 /*   170 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  835,
 /*   180 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  835,
 /*   190 */   835,  820,  835,  750,  835,  835,  835,  835,  835,  835,
 /*   200 */   770,  835,  762,  835,  835,  835,  835,  835,  835,  722,
 /*   210 */   835,  835,  835,  835,  835,  835,  835,  835,  835,  835,
 /*   220 */   688,  835,  835,  835,  835,  835,  835,  835,  828,  835,
 /*   230 */   835,  835,  716,  826,  835,  835,  835,  835,  835,  835,
 /*   240 */   835,  835,  835,  835,  835,  835,  835,  640,  638,  835,
 /*   250 */   632,  835,
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
  int yyerrcnt;                 /* Shifts left before out of the error */
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
 /*  24 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  25 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  26 */ "cmd ::= DROP DNODE IPTOKEN",
 /*  27 */ "cmd ::= DROP USER ids",
 /*  28 */ "cmd ::= DROP ACCOUNT ids",
 /*  29 */ "cmd ::= USE ids",
 /*  30 */ "cmd ::= DESCRIBE ids cpxName",
 /*  31 */ "cmd ::= ALTER USER ids PASS ids",
 /*  32 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  33 */ "cmd ::= ALTER DNODE IPTOKEN ids",
 /*  34 */ "cmd ::= ALTER DNODE IPTOKEN ids ids",
 /*  35 */ "cmd ::= ALTER LOCAL ids",
 /*  36 */ "cmd ::= ALTER LOCAL ids ids",
 /*  37 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  38 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  39 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  40 */ "ids ::= ID",
 /*  41 */ "ids ::= STRING",
 /*  42 */ "ifexists ::= IF EXISTS",
 /*  43 */ "ifexists ::=",
 /*  44 */ "ifnotexists ::= IF NOT EXISTS",
 /*  45 */ "ifnotexists ::=",
 /*  46 */ "cmd ::= CREATE DNODE IPTOKEN",
 /*  47 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  48 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  49 */ "cmd ::= CREATE USER ids PASS ids",
 /*  50 */ "pps ::=",
 /*  51 */ "pps ::= PPS INTEGER",
 /*  52 */ "tseries ::=",
 /*  53 */ "tseries ::= TSERIES INTEGER",
 /*  54 */ "dbs ::=",
 /*  55 */ "dbs ::= DBS INTEGER",
 /*  56 */ "streams ::=",
 /*  57 */ "streams ::= STREAMS INTEGER",
 /*  58 */ "storage ::=",
 /*  59 */ "storage ::= STORAGE INTEGER",
 /*  60 */ "qtime ::=",
 /*  61 */ "qtime ::= QTIME INTEGER",
 /*  62 */ "users ::=",
 /*  63 */ "users ::= USERS INTEGER",
 /*  64 */ "conns ::=",
 /*  65 */ "conns ::= CONNS INTEGER",
 /*  66 */ "state ::=",
 /*  67 */ "state ::= STATE ids",
 /*  68 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  69 */ "keep ::= KEEP tagitemlist",
 /*  70 */ "tables ::= TABLES INTEGER",
 /*  71 */ "cache ::= CACHE INTEGER",
 /*  72 */ "replica ::= REPLICA INTEGER",
 /*  73 */ "days ::= DAYS INTEGER",
 /*  74 */ "rows ::= ROWS INTEGER",
 /*  75 */ "ablocks ::= ABLOCKS ID",
 /*  76 */ "tblocks ::= TBLOCKS INTEGER",
 /*  77 */ "ctime ::= CTIME INTEGER",
 /*  78 */ "clog ::= CLOG INTEGER",
 /*  79 */ "comp ::= COMP INTEGER",
 /*  80 */ "prec ::= PRECISION STRING",
 /*  81 */ "db_optr ::=",
 /*  82 */ "db_optr ::= db_optr tables",
 /*  83 */ "db_optr ::= db_optr cache",
 /*  84 */ "db_optr ::= db_optr replica",
 /*  85 */ "db_optr ::= db_optr days",
 /*  86 */ "db_optr ::= db_optr rows",
 /*  87 */ "db_optr ::= db_optr ablocks",
 /*  88 */ "db_optr ::= db_optr tblocks",
 /*  89 */ "db_optr ::= db_optr ctime",
 /*  90 */ "db_optr ::= db_optr clog",
 /*  91 */ "db_optr ::= db_optr comp",
 /*  92 */ "db_optr ::= db_optr prec",
 /*  93 */ "db_optr ::= db_optr keep",
 /*  94 */ "alter_db_optr ::=",
 /*  95 */ "alter_db_optr ::= alter_db_optr replica",
 /*  96 */ "alter_db_optr ::= alter_db_optr tables",
 /*  97 */ "typename ::= ids",
 /*  98 */ "typename ::= ids LP signed RP",
 /*  99 */ "signed ::= INTEGER",
 /* 100 */ "signed ::= PLUS INTEGER",
 /* 101 */ "signed ::= MINUS INTEGER",
 /* 102 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 103 */ "create_table_args ::= LP columnlist RP",
 /* 104 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 105 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 106 */ "create_table_args ::= AS select",
 /* 107 */ "columnlist ::= columnlist COMMA column",
 /* 108 */ "columnlist ::= column",
 /* 109 */ "column ::= ids typename",
 /* 110 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 111 */ "tagitemlist ::= tagitem",
 /* 112 */ "tagitem ::= INTEGER",
 /* 113 */ "tagitem ::= FLOAT",
 /* 114 */ "tagitem ::= STRING",
 /* 115 */ "tagitem ::= BOOL",
 /* 116 */ "tagitem ::= NULL",
 /* 117 */ "tagitem ::= MINUS INTEGER",
 /* 118 */ "tagitem ::= MINUS FLOAT",
 /* 119 */ "tagitem ::= PLUS INTEGER",
 /* 120 */ "tagitem ::= PLUS FLOAT",
 /* 121 */ "cmd ::= select",
 /* 122 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 123 */ "select ::= SELECT selcollist",
 /* 124 */ "sclp ::= selcollist COMMA",
 /* 125 */ "sclp ::=",
 /* 126 */ "selcollist ::= sclp expr as",
 /* 127 */ "selcollist ::= sclp STAR",
 /* 128 */ "as ::= AS ids",
 /* 129 */ "as ::= ids",
 /* 130 */ "as ::=",
 /* 131 */ "from ::= FROM tablelist",
 /* 132 */ "tablelist ::= ids cpxName",
 /* 133 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 134 */ "tmvar ::= VARIABLE",
 /* 135 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 136 */ "interval_opt ::=",
 /* 137 */ "fill_opt ::=",
 /* 138 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 139 */ "fill_opt ::= FILL LP ID RP",
 /* 140 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 141 */ "sliding_opt ::=",
 /* 142 */ "orderby_opt ::=",
 /* 143 */ "orderby_opt ::= ORDER BY sortlist",
 /* 144 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 145 */ "sortlist ::= item sortorder",
 /* 146 */ "item ::= ids cpxName",
 /* 147 */ "sortorder ::= ASC",
 /* 148 */ "sortorder ::= DESC",
 /* 149 */ "sortorder ::=",
 /* 150 */ "groupby_opt ::=",
 /* 151 */ "groupby_opt ::= GROUP BY grouplist",
 /* 152 */ "grouplist ::= grouplist COMMA item",
 /* 153 */ "grouplist ::= item",
 /* 154 */ "having_opt ::=",
 /* 155 */ "having_opt ::= HAVING expr",
 /* 156 */ "limit_opt ::=",
 /* 157 */ "limit_opt ::= LIMIT signed",
 /* 158 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 159 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 160 */ "slimit_opt ::=",
 /* 161 */ "slimit_opt ::= SLIMIT signed",
 /* 162 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 163 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 164 */ "where_opt ::=",
 /* 165 */ "where_opt ::= WHERE expr",
 /* 166 */ "expr ::= LP expr RP",
 /* 167 */ "expr ::= ID",
 /* 168 */ "expr ::= ID DOT ID",
 /* 169 */ "expr ::= ID DOT STAR",
 /* 170 */ "expr ::= INTEGER",
 /* 171 */ "expr ::= MINUS INTEGER",
 /* 172 */ "expr ::= PLUS INTEGER",
 /* 173 */ "expr ::= FLOAT",
 /* 174 */ "expr ::= MINUS FLOAT",
 /* 175 */ "expr ::= PLUS FLOAT",
 /* 176 */ "expr ::= STRING",
 /* 177 */ "expr ::= NOW",
 /* 178 */ "expr ::= VARIABLE",
 /* 179 */ "expr ::= BOOL",
 /* 180 */ "expr ::= ID LP exprlist RP",
 /* 181 */ "expr ::= ID LP STAR RP",
 /* 182 */ "expr ::= expr AND expr",
 /* 183 */ "expr ::= expr OR expr",
 /* 184 */ "expr ::= expr LT expr",
 /* 185 */ "expr ::= expr GT expr",
 /* 186 */ "expr ::= expr LE expr",
 /* 187 */ "expr ::= expr GE expr",
 /* 188 */ "expr ::= expr NE expr",
 /* 189 */ "expr ::= expr EQ expr",
 /* 190 */ "expr ::= expr PLUS expr",
 /* 191 */ "expr ::= expr MINUS expr",
 /* 192 */ "expr ::= expr STAR expr",
 /* 193 */ "expr ::= expr SLASH expr",
 /* 194 */ "expr ::= expr REM expr",
 /* 195 */ "expr ::= expr LIKE expr",
 /* 196 */ "expr ::= expr IN LP exprlist RP",
 /* 197 */ "exprlist ::= exprlist COMMA expritem",
 /* 198 */ "exprlist ::= expritem",
 /* 199 */ "expritem ::= expr",
 /* 200 */ "expritem ::=",
 /* 201 */ "cmd ::= INSERT INTO cpxName insert_value_list",
 /* 202 */ "insert_value_list ::= VALUES LP itemlist RP",
 /* 203 */ "insert_value_list ::= insert_value_list VALUES LP itemlist RP",
 /* 204 */ "itemlist ::= itemlist COMMA expr",
 /* 205 */ "itemlist ::= expr",
 /* 206 */ "cmd ::= RESET QUERY CACHE",
 /* 207 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 208 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 209 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 210 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 211 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 212 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 213 */ "cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER",
 /* 214 */ "cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER",
 /* 215 */ "cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.
*/
static void yyGrowStack(yyParser *p){
  int newSize;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  if( pNew ){
    p->yystack = pNew;
    p->yystksz = newSize;
#ifndef NDEBUG
    if( yyTraceFILE ){
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
void *ParseAlloc(void *(*mallocProc)(YYMALLOCARGTYPE)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (YYMALLOCARGTYPE)sizeof(yyParser) );
  if( pParser ){
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
    case 216: /* keep */
    case 217: /* tagitemlist */
    case 240: /* fill_opt */
    case 242: /* groupby_opt */
    case 243: /* orderby_opt */
    case 252: /* sortlist */
    case 256: /* grouplist */
{
tVariantListDestroy((yypminor->yy480));
}
      break;
    case 232: /* columnlist */
{
tFieldListDestroy((yypminor->yy421));
}
      break;
    case 233: /* select */
{
destroyQuerySql((yypminor->yy138));
}
      break;
    case 236: /* selcollist */
    case 247: /* sclp */
    case 257: /* exprlist */
    case 260: /* itemlist */
{
tSQLExprListDestroy((yypminor->yy284));
}
      break;
    case 238: /* where_opt */
    case 244: /* having_opt */
    case 248: /* expr */
    case 258: /* expritem */
{
tSQLExprDestroy((yypminor->yy244));
}
      break;
    case 253: /* sortitem */
{
tVariantDestroy(&(yypminor->yy236));
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
  assert( pParser->yyidx>=0 );
  yytos = &pParser->yystack[pParser->yyidx--];
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
  yyParser *pParser = (yyParser*)p;
#ifndef YYPARSEFREENEVERNULL
  if( pParser==0 ) return;
#endif
  while( pParser->yyidx>=0 ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  free(pParser->yystack);
#endif
  (*freeProc)((void*)pParser);
}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyidxMax;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
*/
static int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if( stateno>=YY_MIN_REDUCE ) return stateno;
  assert( stateno <= YY_SHIFT_COUNT );
  do{
    i = yy_shift_ofst[stateno];
    if( i==YY_SHIFT_USE_DFLT ) return yy_default[stateno];
    assert( iLookAhead!=YYNOCODE );
    i += iLookAhead;
    if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
      if( iLookAhead>0 ){
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
            yy_lookahead[j]==YYWILDCARD
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
      }
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
  assert( i!=YY_REDUCE_USE_DFLT );
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
static void yyStackOverflow(yyParser *yypParser, YYMINORTYPE *yypMinor){
   ParseARG_FETCH;
   yypParser->yyidx--;
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
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
static void yyTraceShift(yyParser *yypParser, int yyNewState){
  if( yyTraceFILE ){
    if( yyNewState<YYNSTATE ){
      fprintf(yyTraceFILE,"%sShift '%s', go to state %d\n",
         yyTracePrompt,yyTokenName[yypParser->yystack[yypParser->yyidx].major],
         yyNewState);
    }else{
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
  YYMINORTYPE *yypMinor         /* Pointer to the minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yyidx++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( yypParser->yyidx>yypParser->yyidxMax ){
    yypParser->yyidxMax = yypParser->yyidx;
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yyidx>=YYSTACKDEPTH ){
    yyStackOverflow(yypParser, yypMinor);
    return;
  }
#else
  if( yypParser->yyidx>=yypParser->yystksz ){
    yyGrowStack(yypParser);
    if( yypParser->yyidx>=yypParser->yystksz ){
      yyStackOverflow(yypParser, yypMinor);
      return;
    }
  }
#endif
  yytos = &yypParser->yystack[yypParser->yyidx];
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor = *yypMinor;
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

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  int yyruleno                 /* Number of the rule by which to reduce */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  YYMINORTYPE yygotominor;        /* The LHS of the rule reduced */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = &yypParser->yystack[yypParser->yyidx];
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno>=0 
        && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    yysize = yyRuleInfo[yyruleno].nrhs;
    fprintf(yyTraceFILE, "%sReduce [%s], go to state %d.\n", yyTracePrompt,
      yyRuleName[yyruleno], yymsp[-yysize].stateno);
  }
#endif /* NDEBUG */
  yygotominor = yyzerominor;

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
      case 0: /* program ::= cmd */
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setDCLSQLElems(pInfo, SHOW_DATABASES, 0);}
        break;
      case 2: /* cmd ::= SHOW MNODES */
{ setDCLSQLElems(pInfo, SHOW_MNODES, 0);}
        break;
      case 3: /* cmd ::= SHOW DNODES */
{ setDCLSQLElems(pInfo, SHOW_DNODES, 0);}
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
{ setDCLSQLElems(pInfo, SHOW_ACCOUNTS, 0);}
        break;
      case 5: /* cmd ::= SHOW USERS */
{ setDCLSQLElems(pInfo, SHOW_USERS, 0);}
        break;
      case 6: /* cmd ::= SHOW MODULES */
{ setDCLSQLElems(pInfo, SHOW_MODULES, 0);  }
        break;
      case 7: /* cmd ::= SHOW QUERIES */
{ setDCLSQLElems(pInfo, SHOW_QUERIES, 0);  }
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
{ setDCLSQLElems(pInfo, SHOW_CONNECTIONS, 0);}
        break;
      case 9: /* cmd ::= SHOW STREAMS */
{ setDCLSQLElems(pInfo, SHOW_STREAMS, 0);  }
        break;
      case 10: /* cmd ::= SHOW CONFIGS */
{ setDCLSQLElems(pInfo, SHOW_CONFIGS, 0);  }
        break;
      case 11: /* cmd ::= SHOW SCORES */
{ setDCLSQLElems(pInfo, SHOW_SCORES, 0);   }
        break;
      case 12: /* cmd ::= SHOW GRANTS */
{ setDCLSQLElems(pInfo, SHOW_GRANTS, 0);   }
        break;
      case 13: /* cmd ::= SHOW VNODES */
{ setDCLSQLElems(pInfo, SHOW_VNODES, 0); }
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
{ setDCLSQLElems(pInfo, SHOW_VNODES, 1, &yymsp[0].minor.yy0); }
        break;
      case 15: /* dbPrefix ::= */
      case 43: /* ifexists ::= */ yytestcase(yyruleno==43);
      case 45: /* ifnotexists ::= */ yytestcase(yyruleno==45);
{yygotominor.yy0.n = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 17: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
        break;
      case 19: /* cmd ::= SHOW dbPrefix TABLES */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 2, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix STABLES */
{
    setDCLSQLElems(pInfo, SHOW_STABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_STABLES, 2, &token, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 1, &token);
}
        break;
      case 24: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DROP_TABLE, 2, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 25: /* cmd ::= DROP DATABASE ifexists ids */
{ setDCLSQLElems(pInfo, DROP_DATABASE, 2, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 26: /* cmd ::= DROP DNODE IPTOKEN */
{ setDCLSQLElems(pInfo, DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 27: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 28: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, DROP_ACCOUNT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 29: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, USE_DATABASE, 1, &yymsp[0].minor.yy0);}
        break;
      case 30: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 31: /* cmd ::= ALTER USER ids PASS ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PASSWD, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);    }
        break;
      case 32: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PRIVILEGES, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 33: /* cmd ::= ALTER DNODE IPTOKEN ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 34: /* cmd ::= ALTER DNODE IPTOKEN ids ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 35: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 36: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 37: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, ALTER_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &t);}
        break;
      case 38: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ SSQLToken t = {0};  setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-1].minor.yy0, &t, &yymsp[0].minor.yy155);}
        break;
      case 39: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
        break;
      case 40: /* ids ::= ID */
      case 41: /* ids ::= STRING */ yytestcase(yyruleno==41);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 42: /* ifexists ::= IF EXISTS */
      case 44: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==44);
{yygotominor.yy0.n = 1;}
        break;
      case 46: /* cmd ::= CREATE DNODE IPTOKEN */
{ setDCLSQLElems(pInfo, CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 47: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, CREATE_ACCOUNT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy155);}
        break;
      case 48: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, CREATE_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy262, &yymsp[-2].minor.yy0);}
        break;
      case 49: /* cmd ::= CREATE USER ids PASS ids */
{ setDCLSQLElems(pInfo, CREATE_USER, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 50: /* pps ::= */
      case 52: /* tseries ::= */ yytestcase(yyruleno==52);
      case 54: /* dbs ::= */ yytestcase(yyruleno==54);
      case 56: /* streams ::= */ yytestcase(yyruleno==56);
      case 58: /* storage ::= */ yytestcase(yyruleno==58);
      case 60: /* qtime ::= */ yytestcase(yyruleno==60);
      case 62: /* users ::= */ yytestcase(yyruleno==62);
      case 64: /* conns ::= */ yytestcase(yyruleno==64);
      case 66: /* state ::= */ yytestcase(yyruleno==66);
{yygotominor.yy0.n = 0;   }
        break;
      case 51: /* pps ::= PPS INTEGER */
      case 53: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==53);
      case 55: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==55);
      case 57: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==57);
      case 59: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==59);
      case 61: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==61);
      case 63: /* users ::= USERS INTEGER */ yytestcase(yyruleno==63);
      case 65: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==65);
      case 67: /* state ::= STATE ids */ yytestcase(yyruleno==67);
{yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 68: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy155.users   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy155.dbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy155.tseries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy155.streams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy155.pps     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy155.storage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy155.qtime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy155.conns   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy155.stat    = yymsp[0].minor.yy0;
}
        break;
      case 69: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy480 = yymsp[0].minor.yy480; }
        break;
      case 70: /* tables ::= TABLES INTEGER */
      case 71: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==71);
      case 72: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==72);
      case 73: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==73);
      case 74: /* rows ::= ROWS INTEGER */ yytestcase(yyruleno==74);
      case 75: /* ablocks ::= ABLOCKS ID */ yytestcase(yyruleno==75);
      case 76: /* tblocks ::= TBLOCKS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==77);
      case 78: /* clog ::= CLOG INTEGER */ yytestcase(yyruleno==78);
      case 79: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==79);
      case 80: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==80);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 81: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy262);}
        break;
      case 82: /* db_optr ::= db_optr tables */
      case 96: /* alter_db_optr ::= alter_db_optr tables */ yytestcase(yyruleno==96);
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 83: /* db_optr ::= db_optr cache */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 84: /* db_optr ::= db_optr replica */
      case 95: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==95);
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 85: /* db_optr ::= db_optr days */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 86: /* db_optr ::= db_optr rows */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 87: /* db_optr ::= db_optr ablocks */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 88: /* db_optr ::= db_optr tblocks */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 89: /* db_optr ::= db_optr ctime */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 90: /* db_optr ::= db_optr clog */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 91: /* db_optr ::= db_optr comp */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 92: /* db_optr ::= db_optr prec */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.precision = yymsp[0].minor.yy0; }
        break;
      case 93: /* db_optr ::= db_optr keep */
{ yygotominor.yy262 = yymsp[-1].minor.yy262; yygotominor.yy262.keep = yymsp[0].minor.yy480; }
        break;
      case 94: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy262);}
        break;
      case 97: /* typename ::= ids */
{ tSQLSetColumnType (&yygotominor.yy397, &yymsp[0].minor.yy0); }
        break;
      case 98: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy369;          // negative value of name length
    tSQLSetColumnType(&yygotominor.yy397, &yymsp[-3].minor.yy0);
}
        break;
      case 99: /* signed ::= INTEGER */
      case 100: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==100);
{ yygotominor.yy369 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 101: /* signed ::= MINUS INTEGER */
{ yygotominor.yy369 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 102: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 103: /* create_table_args ::= LP columnlist RP */
{
    yygotominor.yy344 = tSetCreateSQLElems(yymsp[-1].minor.yy421, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, yygotominor.yy344, NULL, TSQL_CREATE_NORMAL_METER);
}
        break;
      case 104: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yygotominor.yy344 = tSetCreateSQLElems(yymsp[-5].minor.yy421, yymsp[-1].minor.yy421, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, yygotominor.yy344, NULL, TSQL_CREATE_NORMAL_METRIC);
}
        break;
      case 105: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yygotominor.yy344 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy480, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, yygotominor.yy344, NULL, TSQL_CREATE_METER_FROM_METRIC);
}
        break;
      case 106: /* create_table_args ::= AS select */
{
    yygotominor.yy344 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy138, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yygotominor.yy344, NULL, TSQL_CREATE_STREAM);
}
        break;
      case 107: /* columnlist ::= columnlist COMMA column */
{yygotominor.yy421 = tFieldListAppend(yymsp[-2].minor.yy421, &yymsp[0].minor.yy397);   }
        break;
      case 108: /* columnlist ::= column */
{yygotominor.yy421 = tFieldListAppend(NULL, &yymsp[0].minor.yy397);}
        break;
      case 109: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yygotominor.yy397, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy397);
}
        break;
      case 110: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);    }
        break;
      case 111: /* tagitemlist ::= tagitem */
{ yygotominor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1); }
        break;
      case 112: /* tagitem ::= INTEGER */
      case 113: /* tagitem ::= FLOAT */ yytestcase(yyruleno==113);
      case 114: /* tagitem ::= STRING */ yytestcase(yyruleno==114);
      case 115: /* tagitem ::= BOOL */ yytestcase(yyruleno==115);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy236, &yymsp[0].minor.yy0); }
        break;
      case 116: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy236, &yymsp[0].minor.yy0); }
        break;
      case 117: /* tagitem ::= MINUS INTEGER */
      case 118: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==118);
      case 119: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==119);
      case 120: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==120);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy236, &yymsp[-1].minor.yy0);
}
        break;
      case 121: /* cmd ::= select */
{
    setSQLInfo(pInfo, yymsp[0].minor.yy138, NULL, TSQL_QUERY_METER);
}
        break;
      case 122: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy138 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy284, yymsp[-9].minor.yy480, yymsp[-8].minor.yy244, yymsp[-4].minor.yy480, yymsp[-3].minor.yy480, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy480, &yymsp[0].minor.yy162, &yymsp[-1].minor.yy162);
}
        break;
      case 123: /* select ::= SELECT selcollist */
{
  yygotominor.yy138 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy284, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 124: /* sclp ::= selcollist COMMA */
{yygotominor.yy284 = yymsp[-1].minor.yy284;}
        break;
      case 125: /* sclp ::= */
{yygotominor.yy284 = 0;}
        break;
      case 126: /* selcollist ::= sclp expr as */
{
   yygotominor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284, yymsp[-1].minor.yy244, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 127: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy284 = tSQLExprListAppend(yymsp[-1].minor.yy284, pNode, 0);
}
        break;
      case 128: /* as ::= AS ids */
      case 129: /* as ::= ids */ yytestcase(yyruleno==129);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 130: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 131: /* from ::= FROM tablelist */
      case 143: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==143);
      case 151: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==151);
{yygotominor.yy480 = yymsp[0].minor.yy480;}
        break;
      case 132: /* tablelist ::= ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yygotominor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);}
        break;
      case 133: /* tablelist ::= tablelist COMMA ids cpxName */
{ toTSDBType(yymsp[-1].minor.yy0.type); yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yygotominor.yy480 = tVariantListAppendToken(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy0, -1);   }
        break;
      case 134: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 135: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 140: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==140);
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 136: /* interval_opt ::= */
      case 141: /* sliding_opt ::= */ yytestcase(yyruleno==141);
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 137: /* fill_opt ::= */
{yygotominor.yy480 = 0;     }
        break;
      case 138: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy480, &A, -1, 0);
    yygotominor.yy480 = yymsp[-1].minor.yy480;
}
        break;
      case 139: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy480 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 142: /* orderby_opt ::= */
      case 150: /* groupby_opt ::= */ yytestcase(yyruleno==150);
{yygotominor.yy480 = 0;}
        break;
      case 144: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy480 = tVariantListAppend(yymsp[-3].minor.yy480, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
        break;
      case 145: /* sortlist ::= item sortorder */
{
  yygotominor.yy480 = tVariantListAppend(NULL, &yymsp[-1].minor.yy236, yymsp[0].minor.yy220);
}
        break;
      case 146: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy236, &yymsp[-1].minor.yy0);
}
        break;
      case 147: /* sortorder ::= ASC */
{yygotominor.yy220 = TSQL_SO_ASC; }
        break;
      case 148: /* sortorder ::= DESC */
{yygotominor.yy220 = TSQL_SO_DESC;}
        break;
      case 149: /* sortorder ::= */
{yygotominor.yy220 = TSQL_SO_ASC;}
        break;
      case 152: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy480 = tVariantListAppend(yymsp[-2].minor.yy480, &yymsp[0].minor.yy236, -1);
}
        break;
      case 153: /* grouplist ::= item */
{
  yygotominor.yy480 = tVariantListAppend(NULL, &yymsp[0].minor.yy236, -1);
}
        break;
      case 154: /* having_opt ::= */
      case 164: /* where_opt ::= */ yytestcase(yyruleno==164);
      case 200: /* expritem ::= */ yytestcase(yyruleno==200);
{yygotominor.yy244 = 0;}
        break;
      case 155: /* having_opt ::= HAVING expr */
      case 165: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==165);
      case 199: /* expritem ::= expr */ yytestcase(yyruleno==199);
{yygotominor.yy244 = yymsp[0].minor.yy244;}
        break;
      case 156: /* limit_opt ::= */
      case 160: /* slimit_opt ::= */ yytestcase(yyruleno==160);
{yygotominor.yy162.limit = -1; yygotominor.yy162.offset = 0;}
        break;
      case 157: /* limit_opt ::= LIMIT signed */
      case 161: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==161);
{yygotominor.yy162.limit = yymsp[0].minor.yy369;  yygotominor.yy162.offset = 0;}
        break;
      case 158: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 162: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==162);
{yygotominor.yy162.limit = yymsp[-2].minor.yy369;  yygotominor.yy162.offset = yymsp[0].minor.yy369;}
        break;
      case 159: /* limit_opt ::= LIMIT signed COMMA signed */
      case 163: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==163);
{yygotominor.yy162.limit = yymsp[0].minor.yy369;  yygotominor.yy162.offset = yymsp[-2].minor.yy369;}
        break;
      case 166: /* expr ::= LP expr RP */
{yygotominor.yy244 = yymsp[-1].minor.yy244; }
        break;
      case 167: /* expr ::= ID */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 168: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 169: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 170: /* expr ::= INTEGER */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 171: /* expr ::= MINUS INTEGER */
      case 172: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==172);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 173: /* expr ::= FLOAT */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 174: /* expr ::= MINUS FLOAT */
      case 175: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==175);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 176: /* expr ::= STRING */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 177: /* expr ::= NOW */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 178: /* expr ::= VARIABLE */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 179: /* expr ::= BOOL */
{yygotominor.yy244 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 180: /* expr ::= ID LP exprlist RP */
{
  yygotominor.yy244 = tSQLExprCreateFunction(yymsp[-1].minor.yy284, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 181: /* expr ::= ID LP STAR RP */
{
  yygotominor.yy244 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 182: /* expr ::= expr AND expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_AND);}
        break;
      case 183: /* expr ::= expr OR expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_OR); }
        break;
      case 184: /* expr ::= expr LT expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LT);}
        break;
      case 185: /* expr ::= expr GT expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GT);}
        break;
      case 186: /* expr ::= expr LE expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LE);}
        break;
      case 187: /* expr ::= expr GE expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_GE);}
        break;
      case 188: /* expr ::= expr NE expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_NE);}
        break;
      case 189: /* expr ::= expr EQ expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_EQ);}
        break;
      case 190: /* expr ::= expr PLUS expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_PLUS);  }
        break;
      case 191: /* expr ::= expr MINUS expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_MINUS); }
        break;
      case 192: /* expr ::= expr STAR expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_STAR);  }
        break;
      case 193: /* expr ::= expr SLASH expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_DIVIDE);}
        break;
      case 194: /* expr ::= expr REM expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_REM);   }
        break;
      case 195: /* expr ::= expr LIKE expr */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-2].minor.yy244, yymsp[0].minor.yy244, TK_LIKE);  }
        break;
      case 196: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy244 = tSQLExprCreate(yymsp[-4].minor.yy244, (tSQLExpr*)yymsp[-1].minor.yy284, TK_IN); }
        break;
      case 197: /* exprlist ::= exprlist COMMA expritem */
      case 204: /* itemlist ::= itemlist COMMA expr */ yytestcase(yyruleno==204);
{yygotominor.yy284 = tSQLExprListAppend(yymsp[-2].minor.yy284,yymsp[0].minor.yy244,0);}
        break;
      case 198: /* exprlist ::= expritem */
      case 205: /* itemlist ::= expr */ yytestcase(yyruleno==205);
{yygotominor.yy284 = tSQLExprListAppend(0,yymsp[0].minor.yy244,0);}
        break;
      case 201: /* cmd ::= INSERT INTO cpxName insert_value_list */
{
    tSetInsertSQLElems(pInfo, &yymsp[-1].minor.yy0, yymsp[0].minor.yy237);
}
        break;
      case 202: /* insert_value_list ::= VALUES LP itemlist RP */
{yygotominor.yy237 = tSQLListListAppend(NULL, yymsp[-1].minor.yy284);}
        break;
      case 203: /* insert_value_list ::= insert_value_list VALUES LP itemlist RP */
{yygotominor.yy237 = tSQLListListAppend(yymsp[-4].minor.yy237, yymsp[-1].minor.yy284);}
        break;
      case 206: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}
        break;
      case 207: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}
        break;
      case 208: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}
        break;
      case 209: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy421, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
        break;
      case 210: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}
        break;
      case 211: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, ALTER_TABLE_TAGS_CHG);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_CHG);
}
        break;
      case 212: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy236, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}
        break;
      case 213: /* cmd ::= KILL CONNECTION IPTOKEN COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &yymsp[-2].minor.yy0);}
        break;
      case 214: /* cmd ::= KILL STREAM IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &yymsp[-4].minor.yy0);}
        break;
      case 215: /* cmd ::= KILL QUERY IPTOKEN COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_QUERY, 1, &yymsp[-4].minor.yy0);}
        break;
      default:
        break;
/********** End reduce actions ************************************************/
  };
  assert( yyruleno>=0 && yyruleno<sizeof(yyRuleInfo)/sizeof(yyRuleInfo[0]) );
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yypParser->yyidx -= yysize;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact <= YY_MAX_SHIFTREDUCE ){
    if( yyact>YY_MAX_SHIFT ) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
    /* If the reduce action popped at least
    ** one element off the stack, then we can push the new element back
    ** onto the stack here, and skip the stack overflow test in yy_shift().
    ** That gives a significant speed improvement. */
    if( yysize ){
      yypParser->yyidx++;
      yymsp -= yysize-1;
      yymsp->stateno = (YYACTIONTYPE)yyact;
      yymsp->major = (YYCODETYPE)yygoto;
      yymsp->minor = yygotominor;
      yyTraceShift(yypParser, yyact);
    }else{
      yy_shift(yypParser,yyact,yygoto,&yygotominor);
    }
  }else{
    assert( yyact == YY_ACCEPT_ACTION );
    yy_accept(yypParser);
  }
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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
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
  YYMINORTYPE yyminor            /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN (yyminor.yy0)
/************ Begin %syntax_error code ****************************************/

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
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
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
  int yyact;            /* The parser action. */
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  int yyendofinput;     /* True if we are at the end of input */
#endif
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  /* (re)initialize the parser, if necessary */
  yypParser = (yyParser*)yyp;
  if( yypParser->yyidx<0 ){
#if YYSTACKDEPTH<=0
    if( yypParser->yystksz <=0 ){
      /*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
      yyminorunion = yyzerominor;
      yyStackOverflow(yypParser, &yyminorunion);
      return;
    }
#endif
    yypParser->yyidx = 0;
    yypParser->yyerrcnt = -1;
    yypParser->yystack[0].stateno = 0;
    yypParser->yystack[0].major = 0;
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sInitialize. Empty stack. State 0\n",
              yyTracePrompt);
    }
#endif
  }
  yyminorunion.yy0 = yyminor;
#if !defined(YYERRORSYMBOL) && !defined(YYNOERRORRECOVERY)
  yyendofinput = (yymajor==0);
#endif
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput '%s'\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact <= YY_MAX_SHIFTREDUCE ){
      if( yyact > YY_MAX_SHIFT ) yyact += YY_MIN_REDUCE - YY_MIN_SHIFTREDUCE;
      yy_shift(yypParser,yyact,yymajor,&yyminorunion);
      yypParser->yyerrcnt--;
      yymajor = YYNOCODE;
    }else if( yyact <= YY_MAX_REDUCE ){
      yy_reduce(yypParser,yyact-YY_MIN_REDUCE);
    }else{
      assert( yyact == YY_ERROR_ACTION );
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yymx = yypParser->yystack[yypParser->yyidx].major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor,&yyminorunion);
        yymajor = YYNOCODE;
      }else{
         while(
          yypParser->yyidx >= 0 &&
          yymx != YYERRORSYMBOL &&
          (yyact = yy_find_reduce_action(
                        yypParser->yystack[yypParser->yyidx].stateno,
                        YYERRORSYMBOL)) >= YY_MIN_REDUCE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yyidx < 0 || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          YYMINORTYPE u2;
          u2.YYERRSYMDT = 0;
          yy_shift(yypParser,yyact,YYERRORSYMBOL,&u2);
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
      yy_syntax_error(yypParser,yymajor,yyminorunion);
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
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
      }
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yyidx>=0 );
#ifndef NDEBUG
  if( yyTraceFILE ){
    int i;
    fprintf(yyTraceFILE,"%sReturn. Stack=",yyTracePrompt);
    for(i=1; i<=yypParser->yyidx; i++)
      fprintf(yyTraceFILE,"%c%s", i==1 ? '[' : ' ', 
              yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"]\n");
  }
#endif
  return;
}
