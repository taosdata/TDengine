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

#include "tsql.h"
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
#define YYCODETYPE unsigned char
#define YYNOCODE 241
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SSQLToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SQuerySQL* yy24;
  tSQLExprList* yy98;
  tFieldList* yy151;
  int64_t yy189;
  tVariantList* yy216;
  tVariant yy266;
  SCreateTableSQL* yy278;
  SLimitVal yy294;
  TAOS_FIELD yy343;
  tSQLExpr* yy370;
  int yy412;
  tSQLExprListList* yy434;
  SCreateDBInfo yy478;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYFALLBACK 1
#define YYNSTATE             217
#define YYNRULE              180
#define YY_MAX_SHIFT         216
#define YY_MIN_SHIFTREDUCE   347
#define YY_MAX_SHIFTREDUCE   526
#define YY_MIN_REDUCE        527
#define YY_MAX_REDUCE        706
#define YY_ERROR_ACTION      707
#define YY_ACCEPT_ACTION     708
#define YY_NO_ACTION         709
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
#define YY_ACTTAB_COUNT (474)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   379,   31,   30,  708,  216,   29,   28,   27,  380,   68,
 /*    10 */    69,   75,   38,   40,  379,   32,   33,  211,  437,   70,
 /*    20 */    26,   54,  380,  183,   36,   34,   37,   35,  133,  215,
 /*    30 */   435,    7,   31,   30,   58,   99,   29,   28,   27,   38,
 /*    40 */    40,  431,   32,   33,  428,   10,  429,   26,  430,  115,
 /*    50 */   183,   36,   34,   37,   35,  442,   89,  139,  509,   31,
 /*    60 */    30,  113,  115,   29,   28,   27,   38,   40,  149,   32,
 /*    70 */    33,  508,  134,  151,   26,  115,   45,  183,   36,   34,
 /*    80 */    37,   35,   89,  138,  509,  112,   31,   30,  379,   53,
 /*    90 */    29,   28,   27,   46,  439,   40,  380,   32,   33,  464,
 /*   100 */    89,  178,   26,  114,  136,  183,   36,   34,   37,   35,
 /*   110 */   102,  103,   61,  477,   31,   30,  427,  146,   29,   28,
 /*   120 */    27,   20,   20,  180,  150,   55,   89,  204,   63,  427,
 /*   130 */   119,  432,  348,  349,  350,  351,  352,  353,  354,  355,
 /*   140 */   356,  357,  358,  463,  147,  148,  424,  424,  209,   32,
 /*   150 */    33,   29,   28,   27,   26,  199,  157,  183,   36,   34,
 /*   160 */    37,   35,  483,  165,  486,  162,   31,   30,  425,   56,
 /*   170 */    29,   28,   27,   17,  198,  197,  196,  195,  194,  193,
 /*   180 */   192,  191,  190,  189,  411,  505,  400,  401,  402,  403,
 /*   190 */   404,  405,  406,  407,  408,  409,  410,  143,  490,  113,
 /*   200 */   200,  481,   11,  484,   48,  487,   20,  143,  490,  415,
 /*   210 */   135,  481,  427,  484,   57,  487,  458,  459,  132,   49,
 /*   220 */   504,   20,   77,   76,  126,   23,  426,   20,  503,  140,
 /*   230 */   141,  423,  131,  182,  109,  107,   78,  207,  206,  140,
 /*   240 */   141,  143,  490,  440,  205,  481,  424,  484,  129,  487,
 /*   250 */   210,   22,  424,  513,  214,  213,  367,   24,  514,  449,
 /*   260 */   479,  450,   24,   42,  507,   15,  491,  142,   14,  171,
 /*   270 */    14,  130,  167,  140,  141,   36,   34,   37,   35,  128,
 /*   280 */   120,  421,   39,   31,   30,   42,  420,   29,   28,   27,
 /*   290 */    21,  489,   39,  482,  187,  485,  480,    2,   21,   67,
 /*   300 */    66,  489,    9,    8,  121,  433,  488,  434,   74,   73,
 /*   310 */   122,  123,  124,  125,  117,  523,  488,  111,  118,  116,
 /*   320 */   412,  474,  441,  473,   86,   98,   39,  144,  470,  469,
 /*   330 */   145,  456,  418,  208,  455,  489,  100,  101,  386,  188,
 /*   340 */   110,  203,  522,   64,  521,  519,  104,  168,   80,  105,
 /*   350 */   488,  377,  376,   71,  374,  373,  445,  152,  106,  371,
 /*   360 */   370,  170,   50,   47,  436,  369,  362,  172,  108,  176,
 /*   370 */   366,  364,   41,  181,   84,  444,  179,  457,  177,  175,
 /*   380 */   173,   25,   22,  202,  185,   90,   44,  212,  526,  153,
 /*   390 */   154,   51,  155,  156,  127,   59,   62,  525,  378,  372,
 /*   400 */   158,   79,   81,  368,  159,  422,  160,  161,    1,   96,
 /*   410 */    93,   91,   92,   94,  524,   95,   97,  163,  164,  517,
 /*   420 */   166,   12,   13,  169,   85,  446,   87,  137,  174,    4,
 /*   430 */    18,  451,   88,    5,  492,    3,   19,   16,  184,    6,
 /*   440 */   186,   60,  398,  397,  396,  395,  394,  393,  392,  391,
 /*   450 */   390,   42,  389,  388,  383,   21,  417,  201,  416,   65,
 /*   460 */   414,   52,  381,   72,  360,   43,  527,  529,  529,  529,
 /*   470 */   529,   82,  529,   83,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,  187,  188,   37,   38,   39,    9,   61,
 /*    10 */    62,   63,   13,   14,    1,   16,   17,   69,   91,   71,
 /*    20 */    21,   94,    9,   24,   25,   26,   27,   28,  189,  190,
 /*    30 */   214,   86,   33,   34,   89,   90,   37,   38,   39,   13,
 /*    40 */    14,    2,   16,   17,    5,  228,    7,   21,    9,  228,
 /*    50 */    24,   25,   26,   27,   28,  190,  190,  236,  237,   33,
 /*    60 */    34,  228,  228,   37,   38,   39,   13,   14,   60,   16,
 /*    70 */    17,  237,  239,   34,   21,  228,   91,   24,   25,   26,
 /*    80 */    27,   28,  190,  236,  237,  228,   33,   34,    1,   90,
 /*    90 */    37,   38,   39,  108,  229,   14,    9,   16,   17,  233,
 /*   100 */   190,  235,   21,  228,  197,   24,   25,   26,   27,   28,
 /*   110 */    61,   62,   63,   87,   33,   34,  209,  197,   37,   38,
 /*   120 */    39,  190,  190,  231,  116,  233,  190,  119,  120,  209,
 /*   130 */   228,   92,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   140 */    53,   54,   55,  233,  213,  213,  215,  215,  190,   16,
 /*   150 */    17,   37,   38,   39,   21,   57,  115,   24,   25,   26,
 /*   160 */    27,   28,    5,  122,    7,  124,   33,   34,  210,  233,
 /*   170 */    37,   38,   39,   75,   76,   77,   78,   79,   80,   81,
 /*   180 */    82,   83,   84,   85,  196,  228,  198,  199,  200,  201,
 /*   190 */   202,  203,  204,  205,  206,  207,  208,    1,    2,  228,
 /*   200 */   197,    5,   44,    7,   91,    9,  190,    1,    2,    5,
 /*   210 */   239,    5,  209,    7,  216,    9,  101,  102,   60,  106,
 /*   220 */   228,  190,   64,   65,   66,  227,  209,  190,  228,   33,
 /*   230 */    34,  215,   74,   37,   61,   62,   63,   33,   34,   33,
 /*   240 */    34,    1,    2,   37,  213,    5,  215,    7,  228,    9,
 /*   250 */   213,   93,  215,   87,   57,   58,   59,   91,   87,   87,
 /*   260 */     1,   87,   91,   91,   87,   91,   87,   56,   91,  111,
 /*   270 */    91,  228,  114,   33,   34,   25,   26,   27,   28,  121,
 /*   280 */   228,   87,   86,   33,   34,   91,   87,   37,   38,   39,
 /*   290 */    91,   95,   86,    5,   87,    7,   37,   86,   91,  117,
 /*   300 */   118,   95,  117,  118,  228,    5,  110,    7,   67,   68,
 /*   310 */   228,  228,  228,  228,  228,  209,  110,  228,  228,  228,
 /*   320 */   209,  211,  190,  211,  190,  217,   86,  211,  211,  211,
 /*   330 */   211,  234,  212,  211,  234,   95,  190,  190,  190,  190,
 /*   340 */   190,  190,  190,  190,  190,  190,  190,  113,   56,  190,
 /*   350 */   110,  190,  190,  190,  190,  190,   95,  190,  190,  190,
 /*   360 */   190,  238,  105,  107,  226,  190,  190,  230,  190,  230,
 /*   370 */   190,  190,  104,   99,  191,  191,  103,  191,   98,   97,
 /*   380 */    96,  109,   93,   72,  191,  225,   77,   72,    5,  123,
 /*   390 */     5,  191,  123,   70,  191,  194,  194,    5,  193,  191,
 /*   400 */   123,  192,  192,  191,    5,  214,  123,   70,  195,  219,
 /*   410 */   222,  224,  223,  221,    5,  220,  218,  123,   70,   76,
 /*   420 */   115,   86,   86,  113,  112,   87,   86,    1,   86,  100,
 /*   430 */    91,   87,   86,  100,   87,   86,   91,   86,   88,   86,
 /*   440 */    88,   67,    9,    5,    5,    5,    5,    1,    5,    5,
 /*   450 */     5,   91,    5,    5,   73,   91,    5,   15,    5,  118,
 /*   460 */    87,   86,   73,   70,   56,   16,    0,  240,  240,  240,
 /*   470 */   240,   21,  240,   21,
};
#define YY_SHIFT_USE_DFLT (-74)
#define YY_SHIFT_COUNT (216)
#define YY_SHIFT_MIN   (-73)
#define YY_SHIFT_MAX   (466)
static const short yy_shift_ofst[] = {
 /*     0 */   158,   98,  196,  240,   13,   13,   13,   13,   13,   13,
 /*    10 */    -1,   87,  240,  240,  240,   39,   39,   39,   13,   13,
 /*    20 */    13,   13,  -74,  206,  240,  240,  240,  240,  240,  240,
 /*    30 */   240,  240,  240,  240,  240,  240,  240,  240,  240,  240,
 /*    40 */   240,  240,   39,   39,   39,  204,  204,  204,  204,  204,
 /*    50 */   204,  -55,  204,   13,   13,  115,  115,  -73,   13,   13,
 /*    60 */    13,   13,   13,   13,   13,   13,   13,   13,   13,   13,
 /*    70 */    13,   13,   13,   13,   13,   13,   13,   13,   13,   13,
 /*    80 */    13,   13,   13,   13,  234,  292,  292,  261,  261,  292,
 /*    90 */   257,  256,  268,  274,  273,  280,  282,  284,  272,  289,
 /*   100 */   292,  292,  311,  311,  292,  309,  292,  315,  292,  315,
 /*   110 */   -74,   26,   53,   53,   53,   53,   53,   81,  133,  250,
 /*   120 */   250,  250,  -32,  -32,  -32,  -32,  -52,    8,   41,  114,
 /*   130 */   114,   49,  173,  197,  166,  171,  172,  174,  177,  179,
 /*   140 */   157,  288,  259,  211,  -15,  113,  194,  199,  207,  182,
 /*   150 */   185,  300,  241,  383,  266,  385,  269,  323,  392,  277,
 /*   160 */   399,  283,  337,  409,  294,  348,  343,  305,  335,  336,
 /*   170 */   310,  312,  338,  340,  426,  342,  344,  346,  339,  329,
 /*   180 */   345,  333,  347,  349,  351,  350,  353,  352,  374,  433,
 /*   190 */   438,  439,  440,  441,  446,  443,  444,  445,  447,  448,
 /*   200 */   360,  381,  442,  449,  341,  364,  451,  453,  373,  375,
 /*   210 */   364,  393,  389,  450,  452,  408,  466,
};
#define YY_REDUCE_USE_DFLT (-185)
#define YY_REDUCE_COUNT (110)
#define YY_REDUCE_MIN   (-184)
#define YY_REDUCE_MAX   (213)
static const short yy_reduce_ofst[] = {
 /*     0 */  -184,  -12, -179, -153, -134, -108,  -69,  -68,   31,   37,
 /*    10 */  -135, -161, -167,  -29, -166,  -93,  -80,    3,  -90,  -64,
 /*    20 */   -42,   16,   -2, -183, -143, -125,  -98,  -43,   -8,    0,
 /*    30 */    20,   43,   52,   76,   82,   83,   84,   85,   86,   89,
 /*    40 */    90,   91,   17,  106,  111,  110,  112,  116,  117,  118,
 /*    50 */   119,  120,  122,  132,  134,   97,  100,  108,  146,  147,
 /*    60 */   148,  149,  150,  151,  152,  153,  154,  155,  156,  159,
 /*    70 */   161,  162,  163,  164,  165,  167,  168,  169,  170,  175,
 /*    80 */   176,  178,  180,  181,  123,  183,  184,  137,  139,  186,
 /*    90 */   138,  160,  187,  189,  188,  192,  195,  190,  198,  191,
 /*   100 */   193,  200,  201,  202,  203,  205,  208,  209,  212,  210,
 /*   110 */   213,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   707,  565,  691,  691,  707,  707,  707,  707,  707,  707,
 /*    10 */   623,  539,  707,  707,  691,  707,  707,  707,  707,  707,
 /*    20 */   707,  707,  618,  707,  707,  707,  707,  707,  707,  707,
 /*    30 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*    40 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*    50 */   707,  707,  707,  707,  707,  640,  640,  707,  707,  707,
 /*    60 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*    70 */   707,  555,  707,  707,  707,  707,  707,  707,  707,  707,
 /*    80 */   707,  707,  707,  707,  707,  541,  541,  707,  707,  541,
 /*    90 */   647,  651,  645,  633,  641,  632,  628,  627,  655,  707,
 /*   100 */   541,  541,  564,  564,  541,  707,  541,  562,  541,  562,
 /*   110 */   579,  707,  695,  696,  656,  690,  646,  674,  673,  686,
 /*   120 */   680,  679,  678,  677,  676,  675,  707,  707,  707,  682,
 /*   130 */   681,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*   140 */   707,  707,  707,  658,  652,  648,  707,  707,  707,  707,
 /*   150 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*   160 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*   170 */   692,  707,  707,  707,  707,  707,  707,  707,  642,  707,
 /*   180 */   634,  707,  707,  707,  707,  707,  707,  599,  707,  707,
 /*   190 */   707,  707,  707,  707,  707,  707,  707,  707,  707,  707,
 /*   200 */   567,  707,  707,  707,  707,  700,  707,  707,  707,  593,
 /*   210 */   698,  707,  707,  545,  543,  707,  707,
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
    0,  /*      USERS => nothing */
    0,  /*    MODULES => nothing */
    0,  /*    QUERIES => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*    CONFIGS => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*        DOT => nothing */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
    0,  /*       USER => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      DNODE => nothing */
    1,  /*         IP => ID */
    0,  /*      LOCAL => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*     CREATE => nothing */
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
    0,  /*       NULL => nothing */
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
    1,  /*       WAVG => ID */
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
  "USERS",         "MODULES",       "QUERIES",       "CONNECTIONS", 
  "STREAMS",       "CONFIGS",       "SCORES",        "GRANTS",      
  "DOT",           "TABLES",        "STABLES",       "VGROUPS",     
  "DROP",          "TABLE",         "DATABASE",      "USER",        
  "USE",           "DESCRIBE",      "ALTER",         "PASS",        
  "PRIVILEGE",     "DNODE",         "IP",            "LOCAL",       
  "IF",            "EXISTS",        "CREATE",        "KEEP",        
  "CACHE",         "REPLICA",       "DAYS",          "ROWS",        
  "ABLOCKS",       "TBLOCKS",       "CTIME",         "CLOG",        
  "COMP",          "PRECISION",     "LP",            "RP",          
  "TAGS",          "USING",         "AS",            "COMMA",       
  "NULL",          "SELECT",        "FROM",          "VARIABLE",    
  "INTERVAL",      "FILL",          "SLIDING",       "ORDER",       
  "BY",            "ASC",           "DESC",          "GROUP",       
  "HAVING",        "LIMIT",         "OFFSET",        "SLIMIT",      
  "SOFFSET",       "WHERE",         "NOW",           "INSERT",      
  "INTO",          "VALUES",        "RESET",         "QUERY",       
  "ADD",           "COLUMN",        "TAG",           "CHANGE",      
  "SET",           "KILL",          "CONNECTION",    "COLON",       
  "STREAM",        "ABORT",         "AFTER",         "ATTACH",      
  "BEFORE",        "BEGIN",         "CASCADE",       "CLUSTER",     
  "CONFLICT",      "COPY",          "DEFERRED",      "DELIMITERS",  
  "DETACH",        "EACH",          "END",           "EXPLAIN",     
  "FAIL",          "FOR",           "IGNORE",        "IMMEDIATE",   
  "INITIALLY",     "INSTEAD",       "MATCH",         "KEY",         
  "OF",            "RAISE",         "REPLACE",       "RESTRICT",    
  "ROW",           "STATEMENT",     "TRIGGER",       "VIEW",        
  "ALL",           "COUNT",         "SUM",           "AVG",         
  "MIN",           "MAX",           "FIRST",         "LAST",        
  "TOP",           "BOTTOM",        "STDDEV",        "PERCENTILE",  
  "APERCENTILE",   "LEASTSQUARES",  "HISTOGRAM",     "DIFF",        
  "SPREAD",        "WAVG",          "INTERP",        "LAST_ROW",    
  "SEMI",          "NONE",          "PREV",          "LINEAR",      
  "IMPORT",        "METRIC",        "TBNAME",        "JOIN",        
  "METRICS",       "STABLE",        "error",         "program",     
  "cmd",           "dbPrefix",      "ids",           "cpxName",     
  "ifexists",      "alter_db_optr",  "ifnotexists",   "db_optr",     
  "keep",          "tagitemlist",   "tables",        "cache",       
  "replica",       "days",          "rows",          "ablocks",     
  "tblocks",       "ctime",         "clog",          "comp",        
  "prec",          "tagitem",       "typename",      "signed",      
  "create_table_args",  "columnlist",    "select",        "column",      
  "selcollist",    "from",          "where_opt",     "interval_opt",
  "fill_opt",      "sliding_opt",   "groupby_opt",   "orderby_opt", 
  "having_opt",    "slimit_opt",    "limit_opt",     "sclp",        
  "expr",          "as",            "tmvar",         "sortlist",    
  "sortitem",      "item",          "sortorder",     "grouplist",   
  "exprlist",      "expritem",      "insert_value_list",  "itemlist",    
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
 /*   4 */ "cmd ::= SHOW USERS",
 /*   5 */ "cmd ::= SHOW MODULES",
 /*   6 */ "cmd ::= SHOW QUERIES",
 /*   7 */ "cmd ::= SHOW CONNECTIONS",
 /*   8 */ "cmd ::= SHOW STREAMS",
 /*   9 */ "cmd ::= SHOW CONFIGS",
 /*  10 */ "cmd ::= SHOW SCORES",
 /*  11 */ "cmd ::= SHOW GRANTS",
 /*  12 */ "dbPrefix ::=",
 /*  13 */ "dbPrefix ::= ids DOT",
 /*  14 */ "cpxName ::=",
 /*  15 */ "cpxName ::= DOT ids",
 /*  16 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  17 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  18 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  19 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  20 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  21 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  22 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  23 */ "cmd ::= DROP USER ids",
 /*  24 */ "cmd ::= USE ids",
 /*  25 */ "cmd ::= DESCRIBE ids cpxName",
 /*  26 */ "cmd ::= ALTER USER ids PASS ids",
 /*  27 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  28 */ "cmd ::= ALTER DNODE IP ids",
 /*  29 */ "cmd ::= ALTER DNODE IP ids ids",
 /*  30 */ "cmd ::= ALTER LOCAL ids",
 /*  31 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  32 */ "ids ::= ID",
 /*  33 */ "ids ::= STRING",
 /*  34 */ "ifexists ::= IF EXISTS",
 /*  35 */ "ifexists ::=",
 /*  36 */ "ifnotexists ::= IF NOT EXISTS",
 /*  37 */ "ifnotexists ::=",
 /*  38 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  39 */ "cmd ::= CREATE USER ids PASS ids",
 /*  40 */ "keep ::= KEEP tagitemlist",
 /*  41 */ "tables ::= TABLES INTEGER",
 /*  42 */ "cache ::= CACHE INTEGER",
 /*  43 */ "replica ::= REPLICA INTEGER",
 /*  44 */ "days ::= DAYS INTEGER",
 /*  45 */ "rows ::= ROWS INTEGER",
 /*  46 */ "ablocks ::= ABLOCKS ID",
 /*  47 */ "tblocks ::= TBLOCKS INTEGER",
 /*  48 */ "ctime ::= CTIME INTEGER",
 /*  49 */ "clog ::= CLOG INTEGER",
 /*  50 */ "comp ::= COMP INTEGER",
 /*  51 */ "prec ::= PRECISION STRING",
 /*  52 */ "db_optr ::=",
 /*  53 */ "db_optr ::= db_optr tables",
 /*  54 */ "db_optr ::= db_optr cache",
 /*  55 */ "db_optr ::= db_optr replica",
 /*  56 */ "db_optr ::= db_optr days",
 /*  57 */ "db_optr ::= db_optr rows",
 /*  58 */ "db_optr ::= db_optr ablocks",
 /*  59 */ "db_optr ::= db_optr tblocks",
 /*  60 */ "db_optr ::= db_optr ctime",
 /*  61 */ "db_optr ::= db_optr clog",
 /*  62 */ "db_optr ::= db_optr comp",
 /*  63 */ "db_optr ::= db_optr prec",
 /*  64 */ "db_optr ::= db_optr keep",
 /*  65 */ "alter_db_optr ::= REPLICA tagitem",
 /*  66 */ "typename ::= ids",
 /*  67 */ "typename ::= ids LP signed RP",
 /*  68 */ "signed ::= INTEGER",
 /*  69 */ "signed ::= PLUS INTEGER",
 /*  70 */ "signed ::= MINUS INTEGER",
 /*  71 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /*  72 */ "create_table_args ::= LP columnlist RP",
 /*  73 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /*  74 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /*  75 */ "create_table_args ::= AS select",
 /*  76 */ "columnlist ::= columnlist COMMA column",
 /*  77 */ "columnlist ::= column",
 /*  78 */ "column ::= ids typename",
 /*  79 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /*  80 */ "tagitemlist ::= tagitem",
 /*  81 */ "tagitem ::= INTEGER",
 /*  82 */ "tagitem ::= FLOAT",
 /*  83 */ "tagitem ::= STRING",
 /*  84 */ "tagitem ::= BOOL",
 /*  85 */ "tagitem ::= NULL",
 /*  86 */ "tagitem ::= MINUS INTEGER",
 /*  87 */ "tagitem ::= MINUS FLOAT",
 /*  88 */ "cmd ::= select",
 /*  89 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /*  90 */ "sclp ::= selcollist COMMA",
 /*  91 */ "sclp ::=",
 /*  92 */ "selcollist ::= sclp expr as",
 /*  93 */ "selcollist ::= sclp STAR",
 /*  94 */ "as ::= AS ids",
 /*  95 */ "as ::= ids",
 /*  96 */ "as ::=",
 /*  97 */ "from ::= FROM ids cpxName",
 /*  98 */ "tmvar ::= VARIABLE",
 /*  99 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 100 */ "interval_opt ::=",
 /* 101 */ "fill_opt ::=",
 /* 102 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 103 */ "fill_opt ::= FILL LP ID RP",
 /* 104 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 105 */ "sliding_opt ::=",
 /* 106 */ "orderby_opt ::=",
 /* 107 */ "orderby_opt ::= ORDER BY sortlist",
 /* 108 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 109 */ "sortlist ::= item sortorder",
 /* 110 */ "item ::= ids cpxName",
 /* 111 */ "sortorder ::= ASC",
 /* 112 */ "sortorder ::= DESC",
 /* 113 */ "sortorder ::=",
 /* 114 */ "groupby_opt ::=",
 /* 115 */ "groupby_opt ::= GROUP BY grouplist",
 /* 116 */ "grouplist ::= grouplist COMMA item",
 /* 117 */ "grouplist ::= item",
 /* 118 */ "having_opt ::=",
 /* 119 */ "having_opt ::= HAVING expr",
 /* 120 */ "limit_opt ::=",
 /* 121 */ "limit_opt ::= LIMIT signed",
 /* 122 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 123 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 124 */ "slimit_opt ::=",
 /* 125 */ "slimit_opt ::= SLIMIT signed",
 /* 126 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 127 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 128 */ "where_opt ::=",
 /* 129 */ "where_opt ::= WHERE expr",
 /* 130 */ "expr ::= LP expr RP",
 /* 131 */ "expr ::= ID",
 /* 132 */ "expr ::= ID DOT ID",
 /* 133 */ "expr ::= ID DOT STAR",
 /* 134 */ "expr ::= INTEGER",
 /* 135 */ "expr ::= MINUS INTEGER",
 /* 136 */ "expr ::= PLUS INTEGER",
 /* 137 */ "expr ::= FLOAT",
 /* 138 */ "expr ::= MINUS FLOAT",
 /* 139 */ "expr ::= PLUS FLOAT",
 /* 140 */ "expr ::= STRING",
 /* 141 */ "expr ::= NOW",
 /* 142 */ "expr ::= VARIABLE",
 /* 143 */ "expr ::= BOOL",
 /* 144 */ "expr ::= ID LP exprlist RP",
 /* 145 */ "expr ::= ID LP STAR RP",
 /* 146 */ "expr ::= expr AND expr",
 /* 147 */ "expr ::= expr OR expr",
 /* 148 */ "expr ::= expr LT expr",
 /* 149 */ "expr ::= expr GT expr",
 /* 150 */ "expr ::= expr LE expr",
 /* 151 */ "expr ::= expr GE expr",
 /* 152 */ "expr ::= expr NE expr",
 /* 153 */ "expr ::= expr EQ expr",
 /* 154 */ "expr ::= expr PLUS expr",
 /* 155 */ "expr ::= expr MINUS expr",
 /* 156 */ "expr ::= expr STAR expr",
 /* 157 */ "expr ::= expr SLASH expr",
 /* 158 */ "expr ::= expr REM expr",
 /* 159 */ "expr ::= expr LIKE expr",
 /* 160 */ "expr ::= expr IN LP exprlist RP",
 /* 161 */ "exprlist ::= exprlist COMMA expritem",
 /* 162 */ "exprlist ::= expritem",
 /* 163 */ "expritem ::= expr",
 /* 164 */ "expritem ::=",
 /* 165 */ "cmd ::= INSERT INTO cpxName insert_value_list",
 /* 166 */ "insert_value_list ::= VALUES LP itemlist RP",
 /* 167 */ "insert_value_list ::= insert_value_list VALUES LP itemlist RP",
 /* 168 */ "itemlist ::= itemlist COMMA expr",
 /* 169 */ "itemlist ::= expr",
 /* 170 */ "cmd ::= RESET QUERY CACHE",
 /* 171 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 172 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 173 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 174 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 175 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 176 */ "cmd ::= ALTER TABLE ids cpxName SET ids EQ tagitem",
 /* 177 */ "cmd ::= KILL CONNECTION IP COLON INTEGER",
 /* 178 */ "cmd ::= KILL STREAM IP COLON INTEGER COLON INTEGER",
 /* 179 */ "cmd ::= KILL QUERY IP COLON INTEGER COLON INTEGER",
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
    case 196: /* keep */
    case 197: /* tagitemlist */
    case 220: /* fill_opt */
    case 222: /* groupby_opt */
    case 223: /* orderby_opt */
    case 231: /* sortlist */
    case 235: /* grouplist */
{
tVariantListDestroy((yypminor->yy216));
}
      break;
    case 214: /* select */
{
destroyQuerySql((yypminor->yy24));
}
      break;
    case 216: /* selcollist */
    case 227: /* sclp */
    case 236: /* exprlist */
    case 239: /* itemlist */
{
tSQLExprListDestroy((yypminor->yy98));
}
      break;
    case 218: /* where_opt */
    case 224: /* having_opt */
    case 228: /* expr */
    case 237: /* expritem */
{
tSQLExprDestroy((yypminor->yy370));
}
      break;
    case 232: /* sortitem */
{
tVariantDestroy(&(yypminor->yy266));
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
  { 187, 1 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 188, 2 },
  { 189, 0 },
  { 189, 2 },
  { 191, 0 },
  { 191, 2 },
  { 188, 3 },
  { 188, 5 },
  { 188, 3 },
  { 188, 5 },
  { 188, 3 },
  { 188, 5 },
  { 188, 4 },
  { 188, 3 },
  { 188, 2 },
  { 188, 3 },
  { 188, 5 },
  { 188, 5 },
  { 188, 4 },
  { 188, 5 },
  { 188, 3 },
  { 188, 4 },
  { 190, 1 },
  { 190, 1 },
  { 192, 2 },
  { 192, 0 },
  { 194, 3 },
  { 194, 0 },
  { 188, 5 },
  { 188, 5 },
  { 196, 2 },
  { 198, 2 },
  { 199, 2 },
  { 200, 2 },
  { 201, 2 },
  { 202, 2 },
  { 203, 2 },
  { 204, 2 },
  { 205, 2 },
  { 206, 2 },
  { 207, 2 },
  { 208, 2 },
  { 195, 0 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 195, 2 },
  { 193, 2 },
  { 210, 1 },
  { 210, 4 },
  { 211, 1 },
  { 211, 2 },
  { 211, 2 },
  { 188, 6 },
  { 212, 3 },
  { 212, 7 },
  { 212, 7 },
  { 212, 2 },
  { 213, 3 },
  { 213, 1 },
  { 215, 2 },
  { 197, 3 },
  { 197, 1 },
  { 209, 1 },
  { 209, 1 },
  { 209, 1 },
  { 209, 1 },
  { 209, 1 },
  { 209, 2 },
  { 209, 2 },
  { 188, 1 },
  { 214, 12 },
  { 227, 2 },
  { 227, 0 },
  { 216, 3 },
  { 216, 2 },
  { 229, 2 },
  { 229, 1 },
  { 229, 0 },
  { 217, 3 },
  { 230, 1 },
  { 219, 4 },
  { 219, 0 },
  { 220, 0 },
  { 220, 6 },
  { 220, 4 },
  { 221, 4 },
  { 221, 0 },
  { 223, 0 },
  { 223, 3 },
  { 231, 4 },
  { 231, 2 },
  { 233, 2 },
  { 234, 1 },
  { 234, 1 },
  { 234, 0 },
  { 222, 0 },
  { 222, 3 },
  { 235, 3 },
  { 235, 1 },
  { 224, 0 },
  { 224, 2 },
  { 226, 0 },
  { 226, 2 },
  { 226, 4 },
  { 226, 4 },
  { 225, 0 },
  { 225, 2 },
  { 225, 4 },
  { 225, 4 },
  { 218, 0 },
  { 218, 2 },
  { 228, 3 },
  { 228, 1 },
  { 228, 3 },
  { 228, 3 },
  { 228, 1 },
  { 228, 2 },
  { 228, 2 },
  { 228, 1 },
  { 228, 2 },
  { 228, 2 },
  { 228, 1 },
  { 228, 1 },
  { 228, 1 },
  { 228, 1 },
  { 228, 4 },
  { 228, 4 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 3 },
  { 228, 5 },
  { 236, 3 },
  { 236, 1 },
  { 237, 1 },
  { 237, 0 },
  { 188, 4 },
  { 238, 4 },
  { 238, 5 },
  { 239, 3 },
  { 239, 1 },
  { 188, 3 },
  { 188, 7 },
  { 188, 7 },
  { 188, 7 },
  { 188, 7 },
  { 188, 8 },
  { 188, 8 },
  { 188, 5 },
  { 188, 7 },
  { 188, 7 },
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
      case 4: /* cmd ::= SHOW USERS */
{ setDCLSQLElems(pInfo, SHOW_USERS, 0);}
        break;
      case 5: /* cmd ::= SHOW MODULES */
{ setDCLSQLElems(pInfo, SHOW_MODULES, 0);  }
        break;
      case 6: /* cmd ::= SHOW QUERIES */
{ setDCLSQLElems(pInfo, SHOW_QUERIES, 0);  }
        break;
      case 7: /* cmd ::= SHOW CONNECTIONS */
{ setDCLSQLElems(pInfo, SHOW_CONNECTIONS, 0);}
        break;
      case 8: /* cmd ::= SHOW STREAMS */
{ setDCLSQLElems(pInfo, SHOW_STREAMS, 0);  }
        break;
      case 9: /* cmd ::= SHOW CONFIGS */
{ setDCLSQLElems(pInfo, SHOW_CONFIGS, 0);  }
        break;
      case 10: /* cmd ::= SHOW SCORES */
{ setDCLSQLElems(pInfo, SHOW_SCORES, 0);   }
        break;
      case 11: /* cmd ::= SHOW GRANTS */
{ setDCLSQLElems(pInfo, SHOW_GRANTS, 0);   }
        break;
      case 12: /* dbPrefix ::= */
      case 35: /* ifexists ::= */ yytestcase(yyruleno==35);
      case 37: /* ifnotexists ::= */ yytestcase(yyruleno==37);
{yygotominor.yy0.n = 0;}
        break;
      case 13: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 14: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 15: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
        break;
      case 16: /* cmd ::= SHOW dbPrefix TABLES */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 17: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setDCLSQLElems(pInfo, SHOW_TABLES, 2, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 18: /* cmd ::= SHOW dbPrefix STABLES */
{
    setDCLSQLElems(pInfo, SHOW_STABLES, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 19: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_STABLES, 2, &token, &yymsp[0].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SSQLToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setDCLSQLElems(pInfo, SHOW_VGROUPS, 1, &token);
}
        break;
      case 21: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DROP_TABLE, 2, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 22: /* cmd ::= DROP DATABASE ifexists ids */
{ setDCLSQLElems(pInfo, DROP_DATABASE, 2, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 23: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 24: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, USE_DATABASE, 1, &yymsp[0].minor.yy0);}
        break;
      case 25: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 26: /* cmd ::= ALTER USER ids PASS ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PASSWD, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);    }
        break;
      case 27: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setDCLSQLElems(pInfo, ALTER_USER_PRIVILEGES, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 28: /* cmd ::= ALTER DNODE IP ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 29: /* cmd ::= ALTER DNODE IP ids ids */
{ setDCLSQLElems(pInfo, ALTER_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 30: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, ALTER_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 31: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SSQLToken t = {0};  setCreateDBSQL(pInfo, ALTER_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &t);}
        break;
      case 32: /* ids ::= ID */
      case 33: /* ids ::= STRING */ yytestcase(yyruleno==33);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 34: /* ifexists ::= IF EXISTS */
      case 36: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==36);
{yygotominor.yy0.n = 1;}
        break;
      case 38: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, CREATE_DATABASE, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy478, &yymsp[-2].minor.yy0);}
        break;
      case 39: /* cmd ::= CREATE USER ids PASS ids */
{ setDCLSQLElems(pInfo, CREATE_USER, 2, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 40: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy216 = yymsp[0].minor.yy216; }
        break;
      case 41: /* tables ::= TABLES INTEGER */
      case 42: /* cache ::= CACHE INTEGER */ yytestcase(yyruleno==42);
      case 43: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==43);
      case 44: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==44);
      case 45: /* rows ::= ROWS INTEGER */ yytestcase(yyruleno==45);
      case 46: /* ablocks ::= ABLOCKS ID */ yytestcase(yyruleno==46);
      case 47: /* tblocks ::= TBLOCKS INTEGER */ yytestcase(yyruleno==47);
      case 48: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==48);
      case 49: /* clog ::= CLOG INTEGER */ yytestcase(yyruleno==49);
      case 50: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==50);
      case 51: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==51);
      case 52: /* db_optr ::= */ yytestcase(yyruleno==52);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 53: /* db_optr ::= db_optr tables */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.tablesPerVnode = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 54: /* db_optr ::= db_optr cache */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 55: /* db_optr ::= db_optr replica */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 56: /* db_optr ::= db_optr days */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 57: /* db_optr ::= db_optr rows */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.rowPerFileBlock = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 58: /* db_optr ::= db_optr ablocks */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.numOfAvgCacheBlocks = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 59: /* db_optr ::= db_optr tblocks */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.numOfBlocksPerTable = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 60: /* db_optr ::= db_optr ctime */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 61: /* db_optr ::= db_optr clog */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.commitLog = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 62: /* db_optr ::= db_optr comp */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 63: /* db_optr ::= db_optr prec */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.precision = yymsp[0].minor.yy0; }
        break;
      case 64: /* db_optr ::= db_optr keep */
{ yygotominor.yy478 = yymsp[-1].minor.yy478; yygotominor.yy478.keep = yymsp[0].minor.yy216; }
        break;
      case 65: /* alter_db_optr ::= REPLICA tagitem */
{
    yygotominor.yy478.replica = yymsp[0].minor.yy266.i64Key;
}
        break;
      case 66: /* typename ::= ids */
{ tSQLSetColumnType (&yygotominor.yy343, &yymsp[0].minor.yy0); }
        break;
      case 67: /* typename ::= ids LP signed RP */
{
    yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy189;          // negative value of name length
    tSQLSetColumnType(&yygotominor.yy343, &yymsp[-3].minor.yy0);
}
        break;
      case 68: /* signed ::= INTEGER */
      case 69: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==69);
{ yygotominor.yy189 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 70: /* signed ::= MINUS INTEGER */
{ yygotominor.yy189 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 71: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedMeterName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 72: /* create_table_args ::= LP columnlist RP */
{
    yygotominor.yy278 = tSetCreateSQLElems(yymsp[-1].minor.yy151, NULL, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METER);
    setSQLInfo(pInfo, yygotominor.yy278, NULL, TSQL_CREATE_NORMAL_METER);
}
        break;
      case 73: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yygotominor.yy278 = tSetCreateSQLElems(yymsp[-5].minor.yy151, yymsp[-1].minor.yy151, NULL, NULL, NULL, TSQL_CREATE_NORMAL_METRIC);
    setSQLInfo(pInfo, yygotominor.yy278, NULL, TSQL_CREATE_NORMAL_METRIC);
}
        break;
      case 74: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yygotominor.yy278 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy216, NULL, TSQL_CREATE_METER_FROM_METRIC);
    setSQLInfo(pInfo, yygotominor.yy278, NULL, TSQL_CREATE_METER_FROM_METRIC);
}
        break;
      case 75: /* create_table_args ::= AS select */
{
    yygotominor.yy278 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy24, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yygotominor.yy278, NULL, TSQL_CREATE_STREAM);
}
        break;
      case 76: /* columnlist ::= columnlist COMMA column */
{yygotominor.yy151 = tFieldListAppend(yymsp[-2].minor.yy151, &yymsp[0].minor.yy343);   }
        break;
      case 77: /* columnlist ::= column */
{yygotominor.yy151 = tFieldListAppend(NULL, &yymsp[0].minor.yy343);}
        break;
      case 78: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yygotominor.yy343, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy343);
}
        break;
      case 79: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy216 = tVariantListAppend(yymsp[-2].minor.yy216, &yymsp[0].minor.yy266, -1);    }
        break;
      case 80: /* tagitemlist ::= tagitem */
{ yygotominor.yy216 = tVariantListAppend(NULL, &yymsp[0].minor.yy266, -1); }
        break;
      case 81: /* tagitem ::= INTEGER */
      case 82: /* tagitem ::= FLOAT */ yytestcase(yyruleno==82);
      case 83: /* tagitem ::= STRING */ yytestcase(yyruleno==83);
      case 84: /* tagitem ::= BOOL */ yytestcase(yyruleno==84);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy266, &yymsp[0].minor.yy0); }
        break;
      case 85: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = TK_STRING; toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy266, &yymsp[0].minor.yy0); }
        break;
      case 86: /* tagitem ::= MINUS INTEGER */
      case 87: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==87);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy266, &yymsp[-1].minor.yy0);
}
        break;
      case 88: /* cmd ::= select */
{
    setSQLInfo(pInfo, yymsp[0].minor.yy24, NULL, TSQL_QUERY_METER);
}
        break;
      case 89: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy24 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy98, &yymsp[-9].minor.yy0, yymsp[-8].minor.yy370, yymsp[-4].minor.yy216, yymsp[-3].minor.yy216, &yymsp[-7].minor.yy0, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy216, &yymsp[0].minor.yy294, &yymsp[-1].minor.yy294);
}
        break;
      case 90: /* sclp ::= selcollist COMMA */
{yygotominor.yy98 = yymsp[-1].minor.yy98;}
        break;
      case 91: /* sclp ::= */
{yygotominor.yy98 = 0;}
        break;
      case 92: /* selcollist ::= sclp expr as */
{
   yygotominor.yy98 = tSQLExprListAppend(yymsp[-2].minor.yy98, yymsp[-1].minor.yy370, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 93: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy98 = tSQLExprListAppend(yymsp[-1].minor.yy98, pNode, 0);
}
        break;
      case 94: /* as ::= AS ids */
      case 95: /* as ::= ids */ yytestcase(yyruleno==95);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 96: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 97: /* from ::= FROM ids cpxName */
{yygotominor.yy0 = yymsp[-1].minor.yy0; yygotominor.yy0.n += yymsp[0].minor.yy0.n;}
        break;
      case 98: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 99: /* interval_opt ::= INTERVAL LP tmvar RP */
      case 104: /* sliding_opt ::= SLIDING LP tmvar RP */ yytestcase(yyruleno==104);
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 100: /* interval_opt ::= */
      case 105: /* sliding_opt ::= */ yytestcase(yyruleno==105);
{yygotominor.yy0.n = 0;   }
        break;
      case 101: /* fill_opt ::= */
{yygotominor.yy216 = 0;     }
        break;
      case 102: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy216, &A, -1, 0);
    yygotominor.yy216 = yymsp[-1].minor.yy216;
}
        break;
      case 103: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy216 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 106: /* orderby_opt ::= */
      case 114: /* groupby_opt ::= */ yytestcase(yyruleno==114);
{yygotominor.yy216 = 0;}
        break;
      case 107: /* orderby_opt ::= ORDER BY sortlist */
      case 115: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==115);
{yygotominor.yy216 = yymsp[0].minor.yy216;}
        break;
      case 108: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy216 = tVariantListAppend(yymsp[-3].minor.yy216, &yymsp[-1].minor.yy266, yymsp[0].minor.yy412);
}
        break;
      case 109: /* sortlist ::= item sortorder */
{
  yygotominor.yy216 = tVariantListAppend(NULL, &yymsp[-1].minor.yy266, yymsp[0].minor.yy412);
}
        break;
      case 110: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy266, &yymsp[-1].minor.yy0);
}
        break;
      case 111: /* sortorder ::= ASC */
{yygotominor.yy412 = TSQL_SO_ASC; }
        break;
      case 112: /* sortorder ::= DESC */
{yygotominor.yy412 = TSQL_SO_DESC;}
        break;
      case 113: /* sortorder ::= */
{yygotominor.yy412 = TSQL_SO_ASC;}
        break;
      case 116: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy216 = tVariantListAppend(yymsp[-2].minor.yy216, &yymsp[0].minor.yy266, -1);
}
        break;
      case 117: /* grouplist ::= item */
{
  yygotominor.yy216 = tVariantListAppend(NULL, &yymsp[0].minor.yy266, -1);
}
        break;
      case 118: /* having_opt ::= */
      case 128: /* where_opt ::= */ yytestcase(yyruleno==128);
      case 164: /* expritem ::= */ yytestcase(yyruleno==164);
{yygotominor.yy370 = 0;}
        break;
      case 119: /* having_opt ::= HAVING expr */
      case 129: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==129);
      case 163: /* expritem ::= expr */ yytestcase(yyruleno==163);
{yygotominor.yy370 = yymsp[0].minor.yy370;}
        break;
      case 120: /* limit_opt ::= */
      case 124: /* slimit_opt ::= */ yytestcase(yyruleno==124);
{yygotominor.yy294.limit = -1; yygotominor.yy294.offset = 0;}
        break;
      case 121: /* limit_opt ::= LIMIT signed */
      case 125: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==125);
{yygotominor.yy294.limit = yymsp[0].minor.yy189;  yygotominor.yy294.offset = 0;}
        break;
      case 122: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 126: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==126);
{yygotominor.yy294.limit = yymsp[-2].minor.yy189;  yygotominor.yy294.offset = yymsp[0].minor.yy189;}
        break;
      case 123: /* limit_opt ::= LIMIT signed COMMA signed */
      case 127: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==127);
{yygotominor.yy294.limit = yymsp[0].minor.yy189;  yygotominor.yy294.offset = yymsp[-2].minor.yy189;}
        break;
      case 130: /* expr ::= LP expr RP */
{yygotominor.yy370 = yymsp[-1].minor.yy370; }
        break;
      case 131: /* expr ::= ID */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 132: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 133: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 134: /* expr ::= INTEGER */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 135: /* expr ::= MINUS INTEGER */
      case 136: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==136);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 137: /* expr ::= FLOAT */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 138: /* expr ::= MINUS FLOAT */
      case 139: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==139);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 140: /* expr ::= STRING */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 141: /* expr ::= NOW */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 142: /* expr ::= VARIABLE */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 143: /* expr ::= BOOL */
{yygotominor.yy370 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 144: /* expr ::= ID LP exprlist RP */
{
  yygotominor.yy370 = tSQLExprCreateFunction(yymsp[-1].minor.yy98, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 145: /* expr ::= ID LP STAR RP */
{
  yygotominor.yy370 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type);
}
        break;
      case 146: /* expr ::= expr AND expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_AND);}
        break;
      case 147: /* expr ::= expr OR expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_OR); }
        break;
      case 148: /* expr ::= expr LT expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LT);}
        break;
      case 149: /* expr ::= expr GT expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GT);}
        break;
      case 150: /* expr ::= expr LE expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LE);}
        break;
      case 151: /* expr ::= expr GE expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_GE);}
        break;
      case 152: /* expr ::= expr NE expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_NE);}
        break;
      case 153: /* expr ::= expr EQ expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_EQ);}
        break;
      case 154: /* expr ::= expr PLUS expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_PLUS);  }
        break;
      case 155: /* expr ::= expr MINUS expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_MINUS); }
        break;
      case 156: /* expr ::= expr STAR expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_STAR);  }
        break;
      case 157: /* expr ::= expr SLASH expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_DIVIDE);}
        break;
      case 158: /* expr ::= expr REM expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_REM);   }
        break;
      case 159: /* expr ::= expr LIKE expr */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-2].minor.yy370, yymsp[0].minor.yy370, TK_LIKE);  }
        break;
      case 160: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy370 = tSQLExprCreate(yymsp[-4].minor.yy370, (tSQLExpr*)yymsp[-1].minor.yy98, TK_IN); }
        break;
      case 161: /* exprlist ::= exprlist COMMA expritem */
      case 168: /* itemlist ::= itemlist COMMA expr */ yytestcase(yyruleno==168);
{yygotominor.yy98 = tSQLExprListAppend(yymsp[-2].minor.yy98,yymsp[0].minor.yy370,0);}
        break;
      case 162: /* exprlist ::= expritem */
      case 169: /* itemlist ::= expr */ yytestcase(yyruleno==169);
{yygotominor.yy98 = tSQLExprListAppend(0,yymsp[0].minor.yy370,0);}
        break;
      case 165: /* cmd ::= INSERT INTO cpxName insert_value_list */
{
    tSetInsertSQLElems(pInfo, &yymsp[-1].minor.yy0, yymsp[0].minor.yy434);
}
        break;
      case 166: /* insert_value_list ::= VALUES LP itemlist RP */
{yygotominor.yy434 = tSQLListListAppend(NULL, yymsp[-1].minor.yy98);}
        break;
      case 167: /* insert_value_list ::= insert_value_list VALUES LP itemlist RP */
{yygotominor.yy434 = tSQLListListAppend(yymsp[-4].minor.yy434, yymsp[-1].minor.yy98);}
        break;
      case 170: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, RESET_QUERY_CACHE, 0);}
        break;
      case 171: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy151, NULL, ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_ADD_COLUMN);
}
        break;
      case 172: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_DROP_COLUMN);
}
        break;
      case 173: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy151, NULL, ALTER_TABLE_TAGS_ADD);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_ADD);
}
        break;
      case 174: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, ALTER_TABLE_TAGS_DROP);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_DROP);
}
        break;
      case 175: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
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
      case 176: /* cmd ::= ALTER TABLE ids cpxName SET ids EQ tagitem */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy266, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, ALTER_TABLE_TAGS_SET);
    setSQLInfo(pInfo, pAlterTable, NULL, ALTER_TABLE_TAGS_SET);
}
        break;
      case 177: /* cmd ::= KILL CONNECTION IP COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_CONNECTION, 1, &yymsp[-2].minor.yy0);}
        break;
      case 178: /* cmd ::= KILL STREAM IP COLON INTEGER COLON INTEGER */
{yymsp[-4].minor.yy0.n += (yymsp[-3].minor.yy0.n + yymsp[-2].minor.yy0.n + yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setDCLSQLElems(pInfo, KILL_STREAM, 1, &yymsp[-4].minor.yy0);}
        break;
      case 179: /* cmd ::= KILL QUERY IP COLON INTEGER COLON INTEGER */
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
