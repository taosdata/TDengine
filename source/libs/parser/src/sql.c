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
#define YYNOCODE 249
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE  SToken 
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  SNode* yy26;
  EOrder yy32;
  SNodeList* yy64;
  EOperatorType yy80;
  bool yy107;
  EFillMode yy192;
  SToken yy353;
  SDataType yy370;
  EJoinType yy372;
  ENullOrder yy391;
  SAlterOption yy443;
  int32_t yy448;
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
#define YYNSTATE             414
#define YYNRULE              316
#define YYNTOKEN             160
#define YY_MAX_SHIFT         413
#define YY_MIN_SHIFTREDUCE   631
#define YY_MAX_SHIFTREDUCE   946
#define YY_ERROR_ACTION      947
#define YY_ACCEPT_ACTION     948
#define YY_NO_ACTION         949
#define YY_MIN_REDUCE        950
#define YY_MAX_REDUCE        1265
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
#define YY_ACTTAB_COUNT (1182)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   996,  347,  347,   24,  162,  334,  253, 1057,  227, 1049,
 /*    10 */   108, 1115, 1158,   31,   29,   27,   26,   25,  951,  331,
 /*    20 */   347, 1098, 1060, 1060,  103,   64,  962,   31,   29,   27,
 /*    30 */    26,   25,  263,  233, 1051,  973, 1106, 1108,  304,   76,
 /*    40 */   346, 1060,   75,   74,   73,   72,   71,   70,   69,   68,
 /*    50 */    67,  201,  398,  397,  396,  395,  394,  393,  392,  391,
 /*    60 */   390,  389,  388,  387,  386,  385,  384,  383,  382,  381,
 /*    70 */   380, 1130, 1130,   30,   28,  829,   31,   29,   27,   26,
 /*    80 */    25,  224,  346,  808,   76,  852,   42,   75,   74,   73,
 /*    90 */    72,   71,   70,   69,   68,   67,  275,  102,  270,   42,
 /*   100 */   806,  274, 1244, 1056,  273, 1063,  271, 1105,   88,  272,
 /*   110 */   289,   12,  950,  214,  201, 1243, 1055,  815, 1103, 1242,
 /*   120 */    23,  222,  346,  847,  848,  849,  850,  851,  853,  855,
 /*   130 */   856,  857,  807,  254,    1,   10,   86,   85,   84,   83,
 /*   140 */    82,   81,   80,   79,   78, 1132,  290,  303,  852,  747,
 /*   150 */   369,  368,  367,  751,  366,  753,  754,  365,  756,  362,
 /*   160 */   410,  762,  359,  764,  765,  356,  353,  287, 1244,  215,
 /*   170 */   942,  943,  817,  310,  305,   10,  913, 1143,  809,  812,
 /*   180 */   285,  116, 1130,   23,  222, 1242,  847,  848,  849,  850,
 /*   190 */   851,  853,  855,  856,  857,  117,  117, 1158,  300,  911,
 /*   200 */   912,  914,  915, 1132,  331,   31,   29,   27,   26,   25,
 /*   210 */   333,  347,  347,  888, 1130, 1143,  344,  345,  815,  319,
 /*   220 */   105, 1025,   60, 1144, 1147, 1183,  948,  230,  249,  200,
 /*   230 */  1179,  117, 1060, 1060,  347, 1158,  972,  292,  248,   64,
 /*   240 */  1130, 1244,  318,  247,  315,  246,  269,  309,  333,  379,
 /*   250 */  1158,   96, 1130, 1197,  116, 1060,  180,  331, 1242, 1090,
 /*   260 */    61, 1144, 1147, 1183,  243,   91, 1105,  217, 1179,  111,
 /*   270 */  1194,  237,  229, 1130,  245,  244,  308, 1103, 1143,  406,
 /*   280 */   405,  158,   30,   28,  889,   89,  347,  296, 1210,  819,
 /*   290 */   224,  175,  808, 1244,  317,  112, 1190, 1191, 1158, 1195,
 /*   300 */   228,  315,   27,   26,   25,  318,  116, 1060,  102,  806,
 /*   310 */  1242,  333,   51, 1143,  239, 1130, 1062,   58, 1202,  884,
 /*   320 */    12,  818,   91,   61, 1144, 1147, 1183,   92, 1036, 1053,
 /*   330 */   217, 1179,  111, 1158, 1052,   31,   29,   27,   26,   25,
 /*   340 */   331,  807,   89,    1,  887,  267,  333,  232, 1143,  266,
 /*   350 */  1130, 1211,  113, 1190, 1191,  102, 1195,  910,   61, 1144,
 /*   360 */  1147, 1183,  235, 1062,  117,  217, 1179, 1256, 1158,  410,
 /*   370 */   102,   77,  372,  971,  268,  331, 1217,  254, 1062, 1143,
 /*   380 */  1105,  333,   30,   28,  157, 1130,  234,  809,  812, 1035,
 /*   390 */   224, 1103,  808,   61, 1144, 1147, 1183,  829,  347, 1158,
 /*   400 */   217, 1179, 1256,  236,    9,    8,  331,   30,   28,  806,
 /*   410 */  1130, 1240,  333,   30,   28,  224, 1130,  808, 1143, 1060,
 /*   420 */    12,  224,  334,  808,   61, 1144, 1147, 1183, 1116, 1197,
 /*   430 */   808,  217, 1179, 1256,  806,  669,  970,  670, 1158,  669,
 /*   440 */   806,  807, 1201,    1, 1045,  331, 1193,  806, 1047,  376,
 /*   450 */  1105,  333,  261,  375,  969, 1130, 1143, 1034,  671,   21,
 /*   460 */   319, 1107,  322,  190, 1144, 1147,  807,  854,    7,  410,
 /*   470 */   858,  149,  807, 1130,    7, 1105, 1158,  865,  377,  807,
 /*   480 */    57,    6, 1244,  331,  968,  147, 1104,  809,  812,  333,
 /*   490 */  1143, 1130,   53, 1130,  410,  116,  967,  374,  373, 1242,
 /*   500 */   410,   62, 1144, 1147, 1183,  820,  379,  410, 1182, 1179,
 /*   510 */  1158,  315,  809,  812,   30,   28,  332,  331,  809,  812,
 /*   520 */   966, 1130,  224,  333,  808,  809,  812, 1130,  896,  117,
 /*   530 */   965,  964,   91, 1130,  817,   62, 1144, 1147, 1183, 1133,
 /*   540 */   961,  806,  329, 1179,  960,   30,   28,  319,  945,  946,
 /*   550 */   959, 1143,   89,  224,  958,  808,  957, 1130,  131,    9,
 /*   560 */     8,  129,  155, 1190,  314,  413,  313, 1130, 1130, 1244,
 /*   570 */   884, 1158,  806,  807,  956,    7, 1130, 1130,  331,  178,
 /*   580 */  1043, 1130,  116,   87,  333,  955, 1242, 1130, 1130,  402,
 /*   590 */  1143, 1130,  177, 1130,  954,  323,  106, 1144, 1147,  330,
 /*   600 */   144,  410,  953,  126,  807,  326,    1,  110, 1197,  321,
 /*   610 */  1158, 1130,  371,  259,  963,  242,  125,  331,   59,  809,
 /*   620 */   812,  173, 1130,  333, 1026, 1192,  133, 1130, 1143,  132,
 /*   630 */   159, 1130,  410,  320, 1257,   62, 1144, 1147, 1183, 1130,
 /*   640 */   991,  135,   43, 1180,  134,  123,  137,  301, 1158,  136,
 /*   650 */   809,  812, 1143,  859,  343,  331,  986,  295,  152,  324,
 /*   660 */   141,  333,  276, 1143,  185, 1130,  984,   32,  223,  187,
 /*   670 */   844,  327, 1158,  196, 1144, 1147, 1099,  826,  278,  331,
 /*   680 */   122,  186,  104, 1158,  120,  333, 1143,  260,  281, 1130,
 /*   690 */   331,   32,  297, 1137,  118, 1213,  333,  196, 1144, 1147,
 /*   700 */  1130, 1143,  316,  798,  167, 1159, 1158, 1135,  195, 1144,
 /*   710 */  1147, 1033,  339,  331,  161,  172, 1143,   32,  165,  333,
 /*   720 */     2, 1158,  250, 1130,  238,  815,   93,  823,  331,   94,
 /*   730 */   119,  106, 1144, 1147,  333,  816, 1158, 1143, 1130,  311,
 /*   740 */   251,  221,  252,  331,  740,  735,  196, 1144, 1147,  333,
 /*   750 */   822,   41, 1143, 1130,  255,  124,  225, 1158,   96,   77,
 /*   760 */   821,  196, 1144, 1147,  331,  262,  264,  768, 1143, 1258,
 /*   770 */   333,  376, 1158,  772, 1130,  375,  778, 1050,  777,  331,
 /*   780 */   117,  351,  194, 1144, 1147,  333,   66,   94, 1158, 1130,
 /*   790 */    95, 1143,   96,  128,   97,  331, 1143,  197, 1144, 1147,
 /*   800 */   377,  333, 1046,  291,  130, 1130,   98, 1143,   94,   99,
 /*   810 */  1048, 1158, 1143,  188, 1144, 1147, 1158, 1044,  331,  374,
 /*   820 */   373,  140,  100,  331,  333,  101,  213, 1158, 1130,  333,
 /*   830 */   820, 1214, 1158, 1130,  331,  293,  198, 1144, 1147,  331,
 /*   840 */   333,  189, 1144, 1147, 1130,  333, 1224, 1143,  294, 1130,
 /*   850 */  1143,  302,  199, 1144, 1147,  337,  145, 1155, 1144, 1147,
 /*   860 */   812,  148,  299,  216,  298,    5, 1204, 1158, 1223,  312,
 /*   870 */  1158,    4,  884,   90,  331,  819,  151,  331,  109, 1198,
 /*   880 */   333,   33,  153,  333, 1130, 1143,  154, 1130,  218,  328,
 /*   890 */    17, 1241, 1154, 1144, 1147, 1153, 1144, 1147, 1259,  325,
 /*   900 */  1143,  160, 1165,  340, 1114, 1158,  335,  169,  336,   50,
 /*   910 */   341, 1113,  331,  226, 1061,  342, 1143,  179,  333,   52,
 /*   920 */  1158, 1143, 1130,  315,  181,  176,  409,  331,  191,  349,
 /*   930 */   204, 1144, 1147,  333,  183,  184, 1158, 1130, 1124,  192,
 /*   940 */   999, 1158, 1123,  331,   91,  203, 1144, 1147,  331,  333,
 /*   950 */  1122,  240, 1143, 1130,  333,  241, 1121,  208, 1130, 1039,
 /*   960 */  1038,  205, 1144, 1147,   89,  998,  202, 1144, 1147, 1000,
 /*   970 */   995,  983, 1158,  121,  114, 1190, 1191,  280, 1195,  331,
 /*   980 */   978, 1120,  267, 1111, 1037,  333,  266,  684,  997, 1130,
 /*   990 */   994,  256,  288,  258,  257,  982,  981,  193, 1144, 1147,
 /*  1000 */   209,  977,  207,  206, 1041,  265,  139,   65,  127,  781,
 /*  1010 */   283,  268, 1040,  783,  782,  277,  713,  712,  711,  138,
 /*  1020 */   992,  275,  210,  270,  710,  709,  274,  987,  708,  273,
 /*  1030 */   211,  271,  279,  985,  272,  212,  282,  976,  284,  975,
 /*  1040 */   286,   63, 1119, 1118, 1110,   38,   36,  142,   37,   44,
 /*  1050 */     3,   20,   32,  143,   14,   39,  146,   15,  306,   34,
 /*  1060 */   307,   31,   29,   27,   26,   25,   22,  909,  903,   47,
 /*  1070 */    11,  107,  150,  931,  930,   45,   31,   29,   27,   26,
 /*  1080 */    25,  902,   46,    8,  881,  880,  219,  935, 1109,  934,
 /*  1090 */   220,  993, 1135,  980,   19,  949,  936,  170,  827,  164,
 /*  1100 */   350,  156,   13,   35,  115,   18,  231,  354,  357,  360,
 /*  1110 */   907,   16,  845,  163,  166,  363,   53,  746,  168,   48,
 /*  1120 */   774,  704,  776,  775,  348,   49,  400, 1134,  769,   40,
 /*  1130 */   352,  399,  766,  355,  401,  378,  763,  757,  682,  358,
 /*  1140 */   171,  338,  174,  703,  361,  702,  755,  364,  701,  700,
 /*  1150 */   699,  698,   54,   55,  697,   56,  696,  705,  695,  694,
 /*  1160 */   761,  693,  692,  691,  690,  689,  403,  370,  688,  687,
 /*  1170 */   404,  979,  974,  407,  760,  408,  759,  810,  758,  182,
 /*  1180 */   411,  412,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     0,  169,  169,  212,  213,  196,  174,  174,  199,  184,
 /*    10 */   182,  202,  183,   12,   13,   14,   15,   16,    0,  190,
 /*    20 */   169,  193,  190,  190,  162,  174,  164,   12,   13,   14,
 /*    30 */    15,   16,  181,  192,  163,  163,  195,  196,  209,   21,
 /*    40 */    20,  190,   24,   25,   26,   27,   28,   29,   30,   31,
 /*    50 */    32,   50,   52,   53,   54,   55,   56,   57,   58,   59,
 /*    60 */    60,   61,   62,   63,   64,   65,   66,   67,   68,   69,
 /*    70 */    70,  200,  200,   12,   13,   74,   12,   13,   14,   15,
 /*    80 */    16,   20,   20,   22,   21,   84,  171,   24,   25,   26,
 /*    90 */    27,   28,   29,   30,   31,   32,   52,  183,   54,  171,
 /*   100 */    39,   57,  227,  188,   60,  191,   62,  183,  180,   65,
 /*   110 */   169,   50,    0,  189,   50,  240,  188,   20,  194,  244,
 /*   120 */   119,  120,   20,  122,  123,  124,  125,  126,  127,  128,
 /*   130 */   129,  130,   71,   49,   73,   73,   24,   25,   26,   27,
 /*   140 */    28,   29,   30,   31,   32,  163,  205,  113,   84,   90,
 /*   150 */    91,   92,   93,   94,   95,   96,   97,   98,   99,  100,
 /*   160 */    99,  102,  103,  104,  105,  106,  107,   21,  227,  187,
 /*   170 */   155,  156,   20,  139,  140,   73,  121,  163,  117,  118,
 /*   180 */    34,  240,  200,  119,  120,  244,  122,  123,  124,  125,
 /*   190 */   126,  127,  128,  129,  130,  134,  134,  183,  143,  144,
 /*   200 */   145,  146,  147,  163,  190,   12,   13,   14,   15,   16,
 /*   210 */   196,  169,  169,    4,  200,  163,  174,  174,   20,  205,
 /*   220 */   172,  173,  208,  209,  210,  211,  160,  187,   30,  215,
 /*   230 */   216,  134,  190,  190,  169,  183,  163,   74,   40,  174,
 /*   240 */   200,  227,  190,   45,  169,   47,  181,   20,  196,   49,
 /*   250 */   183,   88,  200,  206,  240,  190,  176,  190,  244,  179,
 /*   260 */   208,  209,  210,  211,   66,  190,  183,  215,  216,  217,
 /*   270 */   223,  205,  189,  200,   76,   77,  209,  194,  163,  166,
 /*   280 */   167,  229,   12,   13,   14,  210,  169,  235,  236,   20,
 /*   290 */    20,  174,   22,  227,  219,  220,  221,  222,  183,  224,
 /*   300 */   175,  169,   14,   15,   16,  190,  240,  190,  183,   39,
 /*   310 */   244,  196,  168,  163,  116,  200,  191,  168,  132,  133,
 /*   320 */    50,   20,  190,  208,  209,  210,  211,  178,    0,  185,
 /*   330 */   215,  216,  217,  183,  185,   12,   13,   14,   15,   16,
 /*   340 */   190,   71,  210,   73,  135,   60,  196,  175,  163,   64,
 /*   350 */   200,  236,  220,  221,  222,  183,  224,   74,  208,  209,
 /*   360 */   210,  211,  175,  191,  134,  215,  216,  217,  183,   99,
 /*   370 */   183,   88,   86,  163,   89,  190,  226,   49,  191,  163,
 /*   380 */   183,  196,   12,   13,  115,  200,  189,  117,  118,    0,
 /*   390 */    20,  194,   22,  208,  209,  210,  211,   74,  169,  183,
 /*   400 */   215,  216,  217,  174,    1,    2,  190,   12,   13,   39,
 /*   410 */   200,  226,  196,   12,   13,   20,  200,   22,  163,  190,
 /*   420 */    50,   20,  196,   22,  208,  209,  210,  211,  202,  206,
 /*   430 */    22,  215,  216,  217,   39,   22,  163,   20,  183,   22,
 /*   440 */    39,   71,  226,   73,  184,  190,  223,   39,  184,   60,
 /*   450 */   183,  196,   39,   64,  163,  200,  163,    0,   41,  119,
 /*   460 */   205,  194,    3,  208,  209,  210,   71,  127,   73,   99,
 /*   470 */   130,   74,   71,  200,   73,  183,  183,   74,   89,   71,
 /*   480 */    73,   44,  227,  190,  163,   88,  194,  117,  118,  196,
 /*   490 */   163,  200,   85,  200,   99,  240,  163,  108,  109,  244,
 /*   500 */    99,  208,  209,  210,  211,   20,   49,   99,  215,  216,
 /*   510 */   183,  169,  117,  118,   12,   13,   14,  190,  117,  118,
 /*   520 */   163,  200,   20,  196,   22,  117,  118,  200,   14,  134,
 /*   530 */   163,  163,  190,  200,   20,  208,  209,  210,  211,  163,
 /*   540 */   163,   39,  215,  216,  163,   12,   13,  205,  158,  159,
 /*   550 */   163,  163,  210,   20,  163,   22,  163,  200,   79,    1,
 /*   560 */     2,   82,  220,  221,  222,   19,  224,  200,  200,  227,
 /*   570 */   133,  183,   39,   71,  163,   73,  200,  200,  190,   33,
 /*   580 */   184,  200,  240,   37,  196,  163,  244,  200,  200,   43,
 /*   590 */   163,  200,   46,  200,  163,   88,  208,  209,  210,   50,
 /*   600 */   115,   99,  163,   33,   71,   88,   73,   37,  206,  150,
 /*   610 */   183,  200,  184,   43,  164,  169,   46,  190,   72,  117,
 /*   620 */   118,   75,  200,  196,  173,  223,   79,  200,  163,   82,
 /*   630 */   247,  200,   99,  245,  246,  208,  209,  210,  211,  200,
 /*   640 */     0,   79,   72,  216,   82,   75,   79,  238,  183,   82,
 /*   650 */   117,  118,  163,   74,  108,  190,    0,  111,  232,  152,
 /*   660 */   114,  196,   22,  163,   18,  200,    0,   88,  203,   23,
 /*   670 */   121,  154,  183,  208,  209,  210,  193,   74,   22,  190,
 /*   680 */   110,   35,   36,  183,  114,  196,  163,  166,   22,  200,
 /*   690 */   190,   88,  203,   73,   48,  207,  196,  208,  209,  210,
 /*   700 */   200,  163,  225,   74,   74,  183,  183,   87,  208,  209,
 /*   710 */   210,    0,   74,  190,  241,   74,  163,   88,   88,  196,
 /*   720 */   228,  183,  204,  200,  169,   20,   88,   20,  190,   88,
 /*   730 */   171,  208,  209,  210,  196,   20,  183,  163,  200,  239,
 /*   740 */   190,  203,  197,  190,   74,   74,  208,  209,  210,  196,
 /*   750 */    20,  171,  163,  200,  169,  171,  203,  183,   88,   88,
 /*   760 */    20,  208,  209,  210,  190,  165,  183,   74,  163,  246,
 /*   770 */   196,   60,  183,   74,  200,   64,   74,  183,   74,  190,
 /*   780 */   134,   88,  208,  209,  210,  196,  169,   88,  183,  200,
 /*   790 */    88,  163,   88,  183,   74,  190,  163,  208,  209,  210,
 /*   800 */    89,  196,  183,  204,  183,  200,  183,  163,   88,  183,
 /*   810 */   183,  183,  163,  208,  209,  210,  183,  183,  190,  108,
 /*   820 */   109,  168,  183,  190,  196,  183,  165,  183,  200,  196,
 /*   830 */    20,  207,  183,  200,  190,  190,  208,  209,  210,  190,
 /*   840 */   196,  208,  209,  210,  200,  196,  237,  163,  197,  200,
 /*   850 */   163,  142,  208,  209,  210,  141,  201,  208,  209,  210,
 /*   860 */   118,  201,  200,  200,  137,  149,  234,  183,  237,  148,
 /*   870 */   183,  136,  133,  190,  190,   20,  233,  190,  231,  206,
 /*   880 */   196,  131,  230,  196,  200,  163,  218,  200,  157,  153,
 /*   890 */    73,  243,  208,  209,  210,  208,  209,  210,  248,  151,
 /*   900 */   163,  242,  214,  112,  201,  183,  200,  190,  200,  168,
 /*   910 */   198,  201,  190,  200,  190,  197,  163,  179,  196,   73,
 /*   920 */   183,  163,  200,  169,  169,  168,  165,  190,  177,  186,
 /*   930 */   208,  209,  210,  196,  170,  161,  183,  200,    0,  177,
 /*   940 */     0,  183,    0,  190,  190,  208,  209,  210,  190,  196,
 /*   950 */     0,   66,  163,  200,  196,   87,    0,   35,  200,    0,
 /*   960 */     0,  208,  209,  210,  210,    0,  208,  209,  210,    0,
 /*   970 */     0,    0,  183,   44,  220,  221,  222,    4,  224,  190,
 /*   980 */     0,    0,   60,    0,    0,  196,   64,   51,    0,  200,
 /*   990 */     0,   39,   19,   44,   37,    0,    0,  208,  209,  210,
 /*  1000 */    78,    0,   80,   81,    0,   83,   33,   84,   82,   22,
 /*  1010 */    37,   89,    0,   39,   39,   42,   39,   39,   39,   46,
 /*  1020 */     0,   52,   22,   54,   39,   39,   57,    0,   39,   60,
 /*  1030 */    22,   62,   40,    0,   65,   22,   39,    0,   22,    0,
 /*  1040 */    22,   20,    0,    0,    0,   72,  115,   44,   75,   73,
 /*  1050 */    88,    2,   88,  110,  138,   88,   74,  138,   39,  132,
 /*  1060 */    88,   12,   13,   14,   15,   16,    2,   74,   74,    4,
 /*  1070 */   138,   73,   73,   39,   39,   73,   12,   13,   14,   15,
 /*  1080 */    16,   74,   73,    2,   74,   74,   39,   39,    0,   39,
 /*  1090 */    39,    0,   87,    0,   88,  249,   74,   44,   74,   74,
 /*  1100 */    39,   87,   73,   88,   87,   73,   39,   39,   39,   39,
 /*  1110 */    74,   88,  121,   87,   73,   39,   85,   22,   73,   73,
 /*  1120 */    22,   22,   39,   39,   86,   73,   37,   87,   74,   73,
 /*  1130 */    73,   39,   74,   73,   44,   50,   74,   74,   51,   73,
 /*  1140 */   110,  113,   87,   39,   73,   39,   74,   73,   39,   39,
 /*  1150 */    39,   39,   73,   73,   39,   73,   22,   71,   39,   39,
 /*  1160 */   101,   39,   39,   39,   39,   39,   39,   89,   39,   39,
 /*  1170 */    38,    0,    0,   22,  101,   21,  101,   22,  101,   22,
 /*  1180 */    21,   20,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1190 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1200 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1210 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1220 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1230 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1240 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1250 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1260 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1270 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1280 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1290 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1300 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1310 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1320 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1330 */   249,  249,  249,  249,  249,  249,  249,  249,  249,  249,
 /*  1340 */   249,  249,
};
#define YY_SHIFT_COUNT    (413)
#define YY_SHIFT_MIN      (0)
#define YY_SHIFT_MAX      (1172)
static const unsigned short int yy_shift_ofst[] = {
 /*     0 */   646,   61,  270,  370,  370,  370,  370,  395,  370,  370,
 /*    10 */    62,  401,  533,  502,  401,  401,  401,  401,  401,  401,
 /*    20 */   401,  401,  401,  401,  401,  401,  401,  401,  401,  401,
 /*    30 */   401,  401,  401,  102,  102,  102,   97,   20,   20,  408,
 /*    40 */   408,   20,   20,   84,  152,  227,  227,  230,  301,  152,
 /*    50 */    20,   20,  152,   20,  152,  301,  152,  152,   20,  200,
 /*    60 */     1,   64,   64,   63,  922,  408,   44,  408,  408,  408,
 /*    70 */   408,  408,  408,  408,  408,  408,  408,  408,  408,  408,
 /*    80 */   408,  408,  408,  408,  408,  408,  408,  417,  328,  269,
 /*    90 */   269,  269,  457,  301,  152,  152,  152,  286,   59,   59,
 /*   100 */    59,   59,   59,   18,  198,  969,   15,   55,  285,   34,
 /*   110 */   413,  485,  186,  437,  186,  514,  459,  209,  705,  707,
 /*   120 */    84,  715,  730,   84,  705,   84,  740,  152,  152,  152,
 /*   130 */   152,  152,  152,  152,  152,  152,  152,  152,  705,  740,
 /*   140 */   707,  200,  715,  730,  810,  709,  714,  742,  709,  714,
 /*   150 */   742,  716,  721,  727,  735,  739,  715,  855,  750,  731,
 /*   160 */   736,  748,  817,  152,  714,  742,  742,  714,  742,  791,
 /*   170 */   715,  730,  286,  200,  715,  846,  705,  200,  740, 1182,
 /*   180 */  1182, 1182, 1182,    0,  112,  546,  570,  973, 1049, 1064,
 /*   190 */   323,  389,  711,  193,  193,  193,  193,  193,  193,  193,
 /*   200 */   403,  340,  288,  288,  288,  288,  479,  547,  562,  567,
 /*   210 */   640,  656,  666,  146,  163,  283,  397,  558,  390,  507,
 /*   220 */   517,  579,  549,  603,  620,  629,  630,  638,  641,  670,
 /*   230 */   671,  693,  699,  702,  704,  720,  407,  938,  940,  942,
 /*   240 */   950,  885,  868,  956,  959,  960,  965,  970,  971,  980,
 /*   250 */   981,  983,  929,  984,  936,  988,  990,  952,  957,  949,
 /*   260 */   995,  996, 1001, 1004,  923,  926,  974,  975,  987, 1012,
 /*   270 */   977,  978,  979,  985,  986,  989, 1020, 1000, 1027, 1008,
 /*   280 */   992, 1033, 1013,  997, 1037, 1016, 1039, 1018, 1021, 1042,
 /*   290 */  1043,  931, 1044,  976, 1003,  943,  962,  964,  916,  982,
 /*   300 */   967,  993,  998,  999,  994, 1002, 1007, 1019,  972, 1005,
 /*   310 */  1009, 1006,  919, 1010, 1011, 1014,  927, 1015, 1017, 1022,
 /*   320 */  1023,  932, 1065, 1034, 1035, 1047, 1048, 1050, 1051, 1081,
 /*   330 */   991, 1026, 1024, 1029, 1032, 1025, 1036, 1041, 1045, 1028,
 /*   340 */  1046, 1088, 1053, 1030, 1052, 1031, 1040, 1055, 1056, 1038,
 /*   350 */  1054, 1061, 1067, 1057, 1058, 1068, 1060, 1062, 1069, 1066,
 /*   360 */  1063, 1070, 1071, 1072, 1076, 1074, 1059, 1073, 1075, 1077,
 /*   370 */  1095, 1078, 1079, 1080, 1082, 1083, 1084, 1098, 1087, 1085,
 /*   380 */  1086, 1099, 1104, 1106, 1109, 1110, 1111, 1112, 1115, 1134,
 /*   390 */  1119, 1120, 1122, 1123, 1124, 1125, 1126, 1129, 1130, 1091,
 /*   400 */  1092, 1089, 1090, 1093, 1127, 1132, 1171, 1172, 1151, 1154,
 /*   410 */  1155, 1157, 1159, 1161,
};
#define YY_REDUCE_COUNT (182)
#define YY_REDUCE_MIN   (-209)
#define YY_REDUCE_MAX   (789)
static const short yy_reduce_ofst[] = {
 /*     0 */    66,   14,   52,  115,  150,  185,  216,  255,  293,  327,
 /*    10 */   342,  388,  427,  465,  489,  500,  523,  538,  553,  574,
 /*    20 */   589,  605,  628,  633,  644,  649,  684,  687,  722,  737,
 /*    30 */   753,  758,  789,   75,  132,  754,  -59, -149,   65,  -18,
 /*    40 */    40, -168, -167,  -72,  -76, -171,   67, -125, -191,  125,
 /*    50 */    42,   43,   83,  117,  172, -159,  197,  187,  229,  149,
 /*    60 */  -209, -209, -209, -138, -172, -129,   48, -128,   73,  210,
 /*    70 */   273,  291,  321,  333,  357,  367,  368,  376,  377,  381,
 /*    80 */   387,  391,  393,  411,  422,  431,  439,  113,  -85,   47,
 /*    90 */   223,  402,  144,  226,  -86,  267,  292,   80, -175,  260,
 /*   100 */   264,  396,  428,  450,  446,  451,  383,  409,  483,  426,
 /*   110 */   521,  488,  477,  477,  477,  522,  473,  492,  555,  518,
 /*   120 */   559,  550,  545,  580,  585,  584,  600,  583,  594,  610,
 /*   130 */   619,  621,  623,  626,  627,  634,  639,  642,  617,  661,
 /*   140 */   599,  653,  645,  651,  624,  609,  655,  662,  631,  660,
 /*   150 */   663,  632,  643,  647,  652,  477,  683,  673,  668,  650,
 /*   160 */   648,  659,  688,  522,  703,  706,  708,  710,  713,  712,
 /*   170 */   717,  718,  738,  741,  724,  743,  755,  757,  761,  751,
 /*   180 */   762,  764,  774,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    10 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    20 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    30 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    40 */   947,  947,  947, 1004,  947,  947,  947,  947,  947,  947,
 /*    50 */   947,  947,  947,  947,  947,  947,  947,  947,  947, 1002,
 /*    60 */   947, 1185,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    70 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*    80 */   947,  947,  947,  947,  947,  947,  947,  947, 1004, 1196,
 /*    90 */  1196, 1196, 1002,  947,  947,  947,  947, 1089,  947,  947,
 /*   100 */   947,  947,  947,  947,  947,  947, 1260,  947, 1042, 1220,
 /*   110 */   947, 1212, 1188, 1202, 1189,  947, 1245, 1205,  947,  947,
 /*   120 */  1004,  947,  947, 1004,  947, 1004,  947,  947,  947,  947,
 /*   130 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   140 */   947, 1002,  947,  947,  947, 1227, 1225,  947, 1227, 1225,
 /*   150 */   947, 1239, 1235, 1218, 1216, 1202,  947,  947,  947, 1263,
 /*   160 */  1251, 1247,  947,  947, 1225,  947,  947, 1225,  947, 1112,
 /*   170 */   947,  947,  947, 1002,  947, 1058,  947, 1002,  947, 1092,
 /*   180 */  1092, 1005,  952,  947,  947,  947,  947,  947,  947,  947,
 /*   190 */   947,  947,  947, 1157, 1238, 1237, 1156, 1162, 1161, 1160,
 /*   200 */   947,  947, 1151, 1152, 1150, 1149,  947,  947,  947,  947,
 /*   210 */   947,  947,  947,  947,  947,  947,  947, 1186,  947, 1248,
 /*   220 */  1252,  947,  947,  947, 1136,  947,  947,  947,  947,  947,
 /*   230 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   240 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   250 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   260 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   270 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   280 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   290 */   947,  947,  947,  947,  947,  947, 1209, 1219,  947,  947,
 /*   300 */   947,  947,  947,  947,  947,  947,  947,  947,  947, 1136,
 /*   310 */   947, 1236,  947, 1195, 1191,  947,  947, 1187,  947,  947,
 /*   320 */  1246,  947,  947,  947,  947,  947,  947,  947,  947, 1181,
 /*   330 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   340 */   947,  947,  947,  947,  947,  947, 1135,  947,  947,  947,
 /*   350 */   947,  947,  947, 1086,  947,  947,  947,  947,  947,  947,
 /*   360 */   947,  947,  947,  947,  947,  947, 1071, 1069, 1068, 1067,
 /*   370 */   947, 1064,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   380 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   390 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   400 */   947,  947,  947,  947,  947,  947,  947,  947,  947,  947,
 /*   410 */   947,  947,  947,  947,
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
  /*   19 */ "ACCOUNT",
  /*   20 */ "NK_ID",
  /*   21 */ "PASS",
  /*   22 */ "NK_STRING",
  /*   23 */ "ALTER",
  /*   24 */ "PPS",
  /*   25 */ "TSERIES",
  /*   26 */ "STORAGE",
  /*   27 */ "STREAMS",
  /*   28 */ "QTIME",
  /*   29 */ "DBS",
  /*   30 */ "USERS",
  /*   31 */ "CONNS",
  /*   32 */ "STATE",
  /*   33 */ "USER",
  /*   34 */ "PRIVILEGE",
  /*   35 */ "DROP",
  /*   36 */ "SHOW",
  /*   37 */ "DNODE",
  /*   38 */ "PORT",
  /*   39 */ "NK_INTEGER",
  /*   40 */ "DNODES",
  /*   41 */ "NK_IPTOKEN",
  /*   42 */ "LOCAL",
  /*   43 */ "QNODE",
  /*   44 */ "ON",
  /*   45 */ "QNODES",
  /*   46 */ "DATABASE",
  /*   47 */ "DATABASES",
  /*   48 */ "USE",
  /*   49 */ "IF",
  /*   50 */ "NOT",
  /*   51 */ "EXISTS",
  /*   52 */ "BLOCKS",
  /*   53 */ "CACHE",
  /*   54 */ "CACHELAST",
  /*   55 */ "COMP",
  /*   56 */ "DAYS",
  /*   57 */ "FSYNC",
  /*   58 */ "MAXROWS",
  /*   59 */ "MINROWS",
  /*   60 */ "KEEP",
  /*   61 */ "PRECISION",
  /*   62 */ "QUORUM",
  /*   63 */ "REPLICA",
  /*   64 */ "TTL",
  /*   65 */ "WAL",
  /*   66 */ "VGROUPS",
  /*   67 */ "SINGLE_STABLE",
  /*   68 */ "STREAM_MODE",
  /*   69 */ "RETENTIONS",
  /*   70 */ "FILE_FACTOR",
  /*   71 */ "NK_FLOAT",
  /*   72 */ "TABLE",
  /*   73 */ "NK_LP",
  /*   74 */ "NK_RP",
  /*   75 */ "STABLE",
  /*   76 */ "TABLES",
  /*   77 */ "STABLES",
  /*   78 */ "ADD",
  /*   79 */ "COLUMN",
  /*   80 */ "MODIFY",
  /*   81 */ "RENAME",
  /*   82 */ "TAG",
  /*   83 */ "SET",
  /*   84 */ "NK_EQ",
  /*   85 */ "USING",
  /*   86 */ "TAGS",
  /*   87 */ "NK_DOT",
  /*   88 */ "NK_COMMA",
  /*   89 */ "COMMENT",
  /*   90 */ "BOOL",
  /*   91 */ "TINYINT",
  /*   92 */ "SMALLINT",
  /*   93 */ "INT",
  /*   94 */ "INTEGER",
  /*   95 */ "BIGINT",
  /*   96 */ "FLOAT",
  /*   97 */ "DOUBLE",
  /*   98 */ "BINARY",
  /*   99 */ "TIMESTAMP",
  /*  100 */ "NCHAR",
  /*  101 */ "UNSIGNED",
  /*  102 */ "JSON",
  /*  103 */ "VARCHAR",
  /*  104 */ "MEDIUMBLOB",
  /*  105 */ "BLOB",
  /*  106 */ "VARBINARY",
  /*  107 */ "DECIMAL",
  /*  108 */ "SMA",
  /*  109 */ "ROLLUP",
  /*  110 */ "INDEX",
  /*  111 */ "FULLTEXT",
  /*  112 */ "FUNCTION",
  /*  113 */ "INTERVAL",
  /*  114 */ "TOPIC",
  /*  115 */ "AS",
  /*  116 */ "MNODES",
  /*  117 */ "NK_BOOL",
  /*  118 */ "NK_VARIABLE",
  /*  119 */ "BETWEEN",
  /*  120 */ "IS",
  /*  121 */ "NULL",
  /*  122 */ "NK_LT",
  /*  123 */ "NK_GT",
  /*  124 */ "NK_LE",
  /*  125 */ "NK_GE",
  /*  126 */ "NK_NE",
  /*  127 */ "LIKE",
  /*  128 */ "MATCH",
  /*  129 */ "NMATCH",
  /*  130 */ "IN",
  /*  131 */ "FROM",
  /*  132 */ "JOIN",
  /*  133 */ "INNER",
  /*  134 */ "SELECT",
  /*  135 */ "DISTINCT",
  /*  136 */ "WHERE",
  /*  137 */ "PARTITION",
  /*  138 */ "BY",
  /*  139 */ "SESSION",
  /*  140 */ "STATE_WINDOW",
  /*  141 */ "SLIDING",
  /*  142 */ "FILL",
  /*  143 */ "VALUE",
  /*  144 */ "NONE",
  /*  145 */ "PREV",
  /*  146 */ "LINEAR",
  /*  147 */ "NEXT",
  /*  148 */ "GROUP",
  /*  149 */ "HAVING",
  /*  150 */ "ORDER",
  /*  151 */ "SLIMIT",
  /*  152 */ "SOFFSET",
  /*  153 */ "LIMIT",
  /*  154 */ "OFFSET",
  /*  155 */ "ASC",
  /*  156 */ "DESC",
  /*  157 */ "NULLS",
  /*  158 */ "FIRST",
  /*  159 */ "LAST",
  /*  160 */ "cmd",
  /*  161 */ "account_options",
  /*  162 */ "alter_account_options",
  /*  163 */ "literal",
  /*  164 */ "alter_account_option",
  /*  165 */ "user_name",
  /*  166 */ "dnode_endpoint",
  /*  167 */ "dnode_host_name",
  /*  168 */ "not_exists_opt",
  /*  169 */ "db_name",
  /*  170 */ "db_options",
  /*  171 */ "exists_opt",
  /*  172 */ "alter_db_options",
  /*  173 */ "alter_db_option",
  /*  174 */ "full_table_name",
  /*  175 */ "column_def_list",
  /*  176 */ "tags_def_opt",
  /*  177 */ "table_options",
  /*  178 */ "multi_create_clause",
  /*  179 */ "tags_def",
  /*  180 */ "multi_drop_clause",
  /*  181 */ "alter_table_clause",
  /*  182 */ "alter_table_options",
  /*  183 */ "column_name",
  /*  184 */ "type_name",
  /*  185 */ "create_subtable_clause",
  /*  186 */ "specific_tags_opt",
  /*  187 */ "literal_list",
  /*  188 */ "drop_table_clause",
  /*  189 */ "col_name_list",
  /*  190 */ "table_name",
  /*  191 */ "column_def",
  /*  192 */ "func_name_list",
  /*  193 */ "alter_table_option",
  /*  194 */ "col_name",
  /*  195 */ "func_name",
  /*  196 */ "function_name",
  /*  197 */ "index_name",
  /*  198 */ "index_options",
  /*  199 */ "func_list",
  /*  200 */ "duration_literal",
  /*  201 */ "sliding_opt",
  /*  202 */ "func",
  /*  203 */ "expression_list",
  /*  204 */ "topic_name",
  /*  205 */ "query_expression",
  /*  206 */ "table_alias",
  /*  207 */ "column_alias",
  /*  208 */ "expression",
  /*  209 */ "column_reference",
  /*  210 */ "subquery",
  /*  211 */ "predicate",
  /*  212 */ "compare_op",
  /*  213 */ "in_op",
  /*  214 */ "in_predicate_value",
  /*  215 */ "boolean_value_expression",
  /*  216 */ "boolean_primary",
  /*  217 */ "common_expression",
  /*  218 */ "from_clause",
  /*  219 */ "table_reference_list",
  /*  220 */ "table_reference",
  /*  221 */ "table_primary",
  /*  222 */ "joined_table",
  /*  223 */ "alias_opt",
  /*  224 */ "parenthesized_joined_table",
  /*  225 */ "join_type",
  /*  226 */ "search_condition",
  /*  227 */ "query_specification",
  /*  228 */ "set_quantifier_opt",
  /*  229 */ "select_list",
  /*  230 */ "where_clause_opt",
  /*  231 */ "partition_by_clause_opt",
  /*  232 */ "twindow_clause_opt",
  /*  233 */ "group_by_clause_opt",
  /*  234 */ "having_clause_opt",
  /*  235 */ "select_sublist",
  /*  236 */ "select_item",
  /*  237 */ "fill_opt",
  /*  238 */ "fill_mode",
  /*  239 */ "group_by_list",
  /*  240 */ "query_expression_body",
  /*  241 */ "order_by_clause_opt",
  /*  242 */ "slimit_clause_opt",
  /*  243 */ "limit_clause_opt",
  /*  244 */ "query_primary",
  /*  245 */ "sort_specification_list",
  /*  246 */ "sort_specification",
  /*  247 */ "ordering_specification_opt",
  /*  248 */ "null_ordering_opt",
};
#endif /* defined(YYCOVERAGE) || !defined(NDEBUG) */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options",
 /*   1 */ "cmd ::= ALTER ACCOUNT NK_ID alter_account_options",
 /*   2 */ "account_options ::=",
 /*   3 */ "account_options ::= account_options PPS literal",
 /*   4 */ "account_options ::= account_options TSERIES literal",
 /*   5 */ "account_options ::= account_options STORAGE literal",
 /*   6 */ "account_options ::= account_options STREAMS literal",
 /*   7 */ "account_options ::= account_options QTIME literal",
 /*   8 */ "account_options ::= account_options DBS literal",
 /*   9 */ "account_options ::= account_options USERS literal",
 /*  10 */ "account_options ::= account_options CONNS literal",
 /*  11 */ "account_options ::= account_options STATE literal",
 /*  12 */ "alter_account_options ::= alter_account_option",
 /*  13 */ "alter_account_options ::= alter_account_options alter_account_option",
 /*  14 */ "alter_account_option ::= PASS literal",
 /*  15 */ "alter_account_option ::= PPS literal",
 /*  16 */ "alter_account_option ::= TSERIES literal",
 /*  17 */ "alter_account_option ::= STORAGE literal",
 /*  18 */ "alter_account_option ::= STREAMS literal",
 /*  19 */ "alter_account_option ::= QTIME literal",
 /*  20 */ "alter_account_option ::= DBS literal",
 /*  21 */ "alter_account_option ::= USERS literal",
 /*  22 */ "alter_account_option ::= CONNS literal",
 /*  23 */ "alter_account_option ::= STATE literal",
 /*  24 */ "cmd ::= CREATE USER user_name PASS NK_STRING",
 /*  25 */ "cmd ::= ALTER USER user_name PASS NK_STRING",
 /*  26 */ "cmd ::= ALTER USER user_name PRIVILEGE NK_STRING",
 /*  27 */ "cmd ::= DROP USER user_name",
 /*  28 */ "cmd ::= SHOW USERS",
 /*  29 */ "cmd ::= CREATE DNODE dnode_endpoint",
 /*  30 */ "cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER",
 /*  31 */ "cmd ::= DROP DNODE NK_INTEGER",
 /*  32 */ "cmd ::= DROP DNODE dnode_endpoint",
 /*  33 */ "cmd ::= SHOW DNODES",
 /*  34 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING",
 /*  35 */ "cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING",
 /*  36 */ "cmd ::= ALTER ALL DNODES NK_STRING",
 /*  37 */ "cmd ::= ALTER ALL DNODES NK_STRING NK_STRING",
 /*  38 */ "dnode_endpoint ::= NK_STRING",
 /*  39 */ "dnode_host_name ::= NK_ID",
 /*  40 */ "dnode_host_name ::= NK_IPTOKEN",
 /*  41 */ "cmd ::= ALTER LOCAL NK_STRING",
 /*  42 */ "cmd ::= ALTER LOCAL NK_STRING NK_STRING",
 /*  43 */ "cmd ::= CREATE QNODE ON DNODE NK_INTEGER",
 /*  44 */ "cmd ::= DROP QNODE ON DNODE NK_INTEGER",
 /*  45 */ "cmd ::= SHOW QNODES",
 /*  46 */ "cmd ::= CREATE DATABASE not_exists_opt db_name db_options",
 /*  47 */ "cmd ::= DROP DATABASE exists_opt db_name",
 /*  48 */ "cmd ::= SHOW DATABASES",
 /*  49 */ "cmd ::= USE db_name",
 /*  50 */ "cmd ::= ALTER DATABASE db_name alter_db_options",
 /*  51 */ "not_exists_opt ::= IF NOT EXISTS",
 /*  52 */ "not_exists_opt ::=",
 /*  53 */ "exists_opt ::= IF EXISTS",
 /*  54 */ "exists_opt ::=",
 /*  55 */ "db_options ::=",
 /*  56 */ "db_options ::= db_options BLOCKS NK_INTEGER",
 /*  57 */ "db_options ::= db_options CACHE NK_INTEGER",
 /*  58 */ "db_options ::= db_options CACHELAST NK_INTEGER",
 /*  59 */ "db_options ::= db_options COMP NK_INTEGER",
 /*  60 */ "db_options ::= db_options DAYS NK_INTEGER",
 /*  61 */ "db_options ::= db_options FSYNC NK_INTEGER",
 /*  62 */ "db_options ::= db_options MAXROWS NK_INTEGER",
 /*  63 */ "db_options ::= db_options MINROWS NK_INTEGER",
 /*  64 */ "db_options ::= db_options KEEP NK_INTEGER",
 /*  65 */ "db_options ::= db_options PRECISION NK_STRING",
 /*  66 */ "db_options ::= db_options QUORUM NK_INTEGER",
 /*  67 */ "db_options ::= db_options REPLICA NK_INTEGER",
 /*  68 */ "db_options ::= db_options TTL NK_INTEGER",
 /*  69 */ "db_options ::= db_options WAL NK_INTEGER",
 /*  70 */ "db_options ::= db_options VGROUPS NK_INTEGER",
 /*  71 */ "db_options ::= db_options SINGLE_STABLE NK_INTEGER",
 /*  72 */ "db_options ::= db_options STREAM_MODE NK_INTEGER",
 /*  73 */ "db_options ::= db_options RETENTIONS NK_STRING",
 /*  74 */ "db_options ::= db_options FILE_FACTOR NK_FLOAT",
 /*  75 */ "alter_db_options ::= alter_db_option",
 /*  76 */ "alter_db_options ::= alter_db_options alter_db_option",
 /*  77 */ "alter_db_option ::= BLOCKS NK_INTEGER",
 /*  78 */ "alter_db_option ::= FSYNC NK_INTEGER",
 /*  79 */ "alter_db_option ::= KEEP NK_INTEGER",
 /*  80 */ "alter_db_option ::= WAL NK_INTEGER",
 /*  81 */ "alter_db_option ::= QUORUM NK_INTEGER",
 /*  82 */ "alter_db_option ::= CACHELAST NK_INTEGER",
 /*  83 */ "cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options",
 /*  84 */ "cmd ::= CREATE TABLE multi_create_clause",
 /*  85 */ "cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options",
 /*  86 */ "cmd ::= DROP TABLE multi_drop_clause",
 /*  87 */ "cmd ::= DROP STABLE exists_opt full_table_name",
 /*  88 */ "cmd ::= SHOW TABLES",
 /*  89 */ "cmd ::= SHOW STABLES",
 /*  90 */ "cmd ::= ALTER TABLE alter_table_clause",
 /*  91 */ "cmd ::= ALTER STABLE alter_table_clause",
 /*  92 */ "alter_table_clause ::= full_table_name alter_table_options",
 /*  93 */ "alter_table_clause ::= full_table_name ADD COLUMN column_name type_name",
 /*  94 */ "alter_table_clause ::= full_table_name DROP COLUMN column_name",
 /*  95 */ "alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name",
 /*  96 */ "alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name",
 /*  97 */ "alter_table_clause ::= full_table_name ADD TAG column_name type_name",
 /*  98 */ "alter_table_clause ::= full_table_name DROP TAG column_name",
 /*  99 */ "alter_table_clause ::= full_table_name MODIFY TAG column_name type_name",
 /* 100 */ "alter_table_clause ::= full_table_name RENAME TAG column_name column_name",
 /* 101 */ "alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal",
 /* 102 */ "multi_create_clause ::= create_subtable_clause",
 /* 103 */ "multi_create_clause ::= multi_create_clause create_subtable_clause",
 /* 104 */ "create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP",
 /* 105 */ "multi_drop_clause ::= drop_table_clause",
 /* 106 */ "multi_drop_clause ::= multi_drop_clause drop_table_clause",
 /* 107 */ "drop_table_clause ::= exists_opt full_table_name",
 /* 108 */ "specific_tags_opt ::=",
 /* 109 */ "specific_tags_opt ::= NK_LP col_name_list NK_RP",
 /* 110 */ "full_table_name ::= table_name",
 /* 111 */ "full_table_name ::= db_name NK_DOT table_name",
 /* 112 */ "column_def_list ::= column_def",
 /* 113 */ "column_def_list ::= column_def_list NK_COMMA column_def",
 /* 114 */ "column_def ::= column_name type_name",
 /* 115 */ "column_def ::= column_name type_name COMMENT NK_STRING",
 /* 116 */ "type_name ::= BOOL",
 /* 117 */ "type_name ::= TINYINT",
 /* 118 */ "type_name ::= SMALLINT",
 /* 119 */ "type_name ::= INT",
 /* 120 */ "type_name ::= INTEGER",
 /* 121 */ "type_name ::= BIGINT",
 /* 122 */ "type_name ::= FLOAT",
 /* 123 */ "type_name ::= DOUBLE",
 /* 124 */ "type_name ::= BINARY NK_LP NK_INTEGER NK_RP",
 /* 125 */ "type_name ::= TIMESTAMP",
 /* 126 */ "type_name ::= NCHAR NK_LP NK_INTEGER NK_RP",
 /* 127 */ "type_name ::= TINYINT UNSIGNED",
 /* 128 */ "type_name ::= SMALLINT UNSIGNED",
 /* 129 */ "type_name ::= INT UNSIGNED",
 /* 130 */ "type_name ::= BIGINT UNSIGNED",
 /* 131 */ "type_name ::= JSON",
 /* 132 */ "type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP",
 /* 133 */ "type_name ::= MEDIUMBLOB",
 /* 134 */ "type_name ::= BLOB",
 /* 135 */ "type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP",
 /* 136 */ "type_name ::= DECIMAL",
 /* 137 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP",
 /* 138 */ "type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP",
 /* 139 */ "tags_def_opt ::=",
 /* 140 */ "tags_def_opt ::= tags_def",
 /* 141 */ "tags_def ::= TAGS NK_LP column_def_list NK_RP",
 /* 142 */ "table_options ::=",
 /* 143 */ "table_options ::= table_options COMMENT NK_STRING",
 /* 144 */ "table_options ::= table_options KEEP NK_INTEGER",
 /* 145 */ "table_options ::= table_options TTL NK_INTEGER",
 /* 146 */ "table_options ::= table_options SMA NK_LP col_name_list NK_RP",
 /* 147 */ "table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP",
 /* 148 */ "alter_table_options ::= alter_table_option",
 /* 149 */ "alter_table_options ::= alter_table_options alter_table_option",
 /* 150 */ "alter_table_option ::= COMMENT NK_STRING",
 /* 151 */ "alter_table_option ::= KEEP NK_INTEGER",
 /* 152 */ "alter_table_option ::= TTL NK_INTEGER",
 /* 153 */ "col_name_list ::= col_name",
 /* 154 */ "col_name_list ::= col_name_list NK_COMMA col_name",
 /* 155 */ "col_name ::= column_name",
 /* 156 */ "func_name_list ::= func_name",
 /* 157 */ "func_name_list ::= func_name_list NK_COMMA col_name",
 /* 158 */ "func_name ::= function_name",
 /* 159 */ "cmd ::= CREATE SMA INDEX index_name ON table_name index_options",
 /* 160 */ "cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP",
 /* 161 */ "cmd ::= DROP INDEX index_name ON table_name",
 /* 162 */ "index_options ::=",
 /* 163 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt",
 /* 164 */ "index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt",
 /* 165 */ "func_list ::= func",
 /* 166 */ "func_list ::= func_list NK_COMMA func",
 /* 167 */ "func ::= function_name NK_LP expression_list NK_RP",
 /* 168 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression",
 /* 169 */ "cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name",
 /* 170 */ "cmd ::= DROP TOPIC exists_opt topic_name",
 /* 171 */ "cmd ::= SHOW VGROUPS",
 /* 172 */ "cmd ::= SHOW db_name NK_DOT VGROUPS",
 /* 173 */ "cmd ::= SHOW MNODES",
 /* 174 */ "cmd ::= query_expression",
 /* 175 */ "literal ::= NK_INTEGER",
 /* 176 */ "literal ::= NK_FLOAT",
 /* 177 */ "literal ::= NK_STRING",
 /* 178 */ "literal ::= NK_BOOL",
 /* 179 */ "literal ::= TIMESTAMP NK_STRING",
 /* 180 */ "literal ::= duration_literal",
 /* 181 */ "duration_literal ::= NK_VARIABLE",
 /* 182 */ "literal_list ::= literal",
 /* 183 */ "literal_list ::= literal_list NK_COMMA literal",
 /* 184 */ "db_name ::= NK_ID",
 /* 185 */ "table_name ::= NK_ID",
 /* 186 */ "column_name ::= NK_ID",
 /* 187 */ "function_name ::= NK_ID",
 /* 188 */ "table_alias ::= NK_ID",
 /* 189 */ "column_alias ::= NK_ID",
 /* 190 */ "user_name ::= NK_ID",
 /* 191 */ "index_name ::= NK_ID",
 /* 192 */ "topic_name ::= NK_ID",
 /* 193 */ "expression ::= literal",
 /* 194 */ "expression ::= column_reference",
 /* 195 */ "expression ::= function_name NK_LP expression_list NK_RP",
 /* 196 */ "expression ::= function_name NK_LP NK_STAR NK_RP",
 /* 197 */ "expression ::= subquery",
 /* 198 */ "expression ::= NK_LP expression NK_RP",
 /* 199 */ "expression ::= NK_PLUS expression",
 /* 200 */ "expression ::= NK_MINUS expression",
 /* 201 */ "expression ::= expression NK_PLUS expression",
 /* 202 */ "expression ::= expression NK_MINUS expression",
 /* 203 */ "expression ::= expression NK_STAR expression",
 /* 204 */ "expression ::= expression NK_SLASH expression",
 /* 205 */ "expression ::= expression NK_REM expression",
 /* 206 */ "expression_list ::= expression",
 /* 207 */ "expression_list ::= expression_list NK_COMMA expression",
 /* 208 */ "column_reference ::= column_name",
 /* 209 */ "column_reference ::= table_name NK_DOT column_name",
 /* 210 */ "predicate ::= expression compare_op expression",
 /* 211 */ "predicate ::= expression BETWEEN expression AND expression",
 /* 212 */ "predicate ::= expression NOT BETWEEN expression AND expression",
 /* 213 */ "predicate ::= expression IS NULL",
 /* 214 */ "predicate ::= expression IS NOT NULL",
 /* 215 */ "predicate ::= expression in_op in_predicate_value",
 /* 216 */ "compare_op ::= NK_LT",
 /* 217 */ "compare_op ::= NK_GT",
 /* 218 */ "compare_op ::= NK_LE",
 /* 219 */ "compare_op ::= NK_GE",
 /* 220 */ "compare_op ::= NK_NE",
 /* 221 */ "compare_op ::= NK_EQ",
 /* 222 */ "compare_op ::= LIKE",
 /* 223 */ "compare_op ::= NOT LIKE",
 /* 224 */ "compare_op ::= MATCH",
 /* 225 */ "compare_op ::= NMATCH",
 /* 226 */ "in_op ::= IN",
 /* 227 */ "in_op ::= NOT IN",
 /* 228 */ "in_predicate_value ::= NK_LP expression_list NK_RP",
 /* 229 */ "boolean_value_expression ::= boolean_primary",
 /* 230 */ "boolean_value_expression ::= NOT boolean_primary",
 /* 231 */ "boolean_value_expression ::= boolean_value_expression OR boolean_value_expression",
 /* 232 */ "boolean_value_expression ::= boolean_value_expression AND boolean_value_expression",
 /* 233 */ "boolean_primary ::= predicate",
 /* 234 */ "boolean_primary ::= NK_LP boolean_value_expression NK_RP",
 /* 235 */ "common_expression ::= expression",
 /* 236 */ "common_expression ::= boolean_value_expression",
 /* 237 */ "from_clause ::= FROM table_reference_list",
 /* 238 */ "table_reference_list ::= table_reference",
 /* 239 */ "table_reference_list ::= table_reference_list NK_COMMA table_reference",
 /* 240 */ "table_reference ::= table_primary",
 /* 241 */ "table_reference ::= joined_table",
 /* 242 */ "table_primary ::= table_name alias_opt",
 /* 243 */ "table_primary ::= db_name NK_DOT table_name alias_opt",
 /* 244 */ "table_primary ::= subquery alias_opt",
 /* 245 */ "table_primary ::= parenthesized_joined_table",
 /* 246 */ "alias_opt ::=",
 /* 247 */ "alias_opt ::= table_alias",
 /* 248 */ "alias_opt ::= AS table_alias",
 /* 249 */ "parenthesized_joined_table ::= NK_LP joined_table NK_RP",
 /* 250 */ "parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP",
 /* 251 */ "joined_table ::= table_reference join_type JOIN table_reference ON search_condition",
 /* 252 */ "join_type ::=",
 /* 253 */ "join_type ::= INNER",
 /* 254 */ "query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt",
 /* 255 */ "set_quantifier_opt ::=",
 /* 256 */ "set_quantifier_opt ::= DISTINCT",
 /* 257 */ "set_quantifier_opt ::= ALL",
 /* 258 */ "select_list ::= NK_STAR",
 /* 259 */ "select_list ::= select_sublist",
 /* 260 */ "select_sublist ::= select_item",
 /* 261 */ "select_sublist ::= select_sublist NK_COMMA select_item",
 /* 262 */ "select_item ::= common_expression",
 /* 263 */ "select_item ::= common_expression column_alias",
 /* 264 */ "select_item ::= common_expression AS column_alias",
 /* 265 */ "select_item ::= table_name NK_DOT NK_STAR",
 /* 266 */ "where_clause_opt ::=",
 /* 267 */ "where_clause_opt ::= WHERE search_condition",
 /* 268 */ "partition_by_clause_opt ::=",
 /* 269 */ "partition_by_clause_opt ::= PARTITION BY expression_list",
 /* 270 */ "twindow_clause_opt ::=",
 /* 271 */ "twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP",
 /* 272 */ "twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP",
 /* 273 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt",
 /* 274 */ "twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt",
 /* 275 */ "sliding_opt ::=",
 /* 276 */ "sliding_opt ::= SLIDING NK_LP duration_literal NK_RP",
 /* 277 */ "fill_opt ::=",
 /* 278 */ "fill_opt ::= FILL NK_LP fill_mode NK_RP",
 /* 279 */ "fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP",
 /* 280 */ "fill_mode ::= NONE",
 /* 281 */ "fill_mode ::= PREV",
 /* 282 */ "fill_mode ::= NULL",
 /* 283 */ "fill_mode ::= LINEAR",
 /* 284 */ "fill_mode ::= NEXT",
 /* 285 */ "group_by_clause_opt ::=",
 /* 286 */ "group_by_clause_opt ::= GROUP BY group_by_list",
 /* 287 */ "group_by_list ::= expression",
 /* 288 */ "group_by_list ::= group_by_list NK_COMMA expression",
 /* 289 */ "having_clause_opt ::=",
 /* 290 */ "having_clause_opt ::= HAVING search_condition",
 /* 291 */ "query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt",
 /* 292 */ "query_expression_body ::= query_primary",
 /* 293 */ "query_expression_body ::= query_expression_body UNION ALL query_expression_body",
 /* 294 */ "query_primary ::= query_specification",
 /* 295 */ "order_by_clause_opt ::=",
 /* 296 */ "order_by_clause_opt ::= ORDER BY sort_specification_list",
 /* 297 */ "slimit_clause_opt ::=",
 /* 298 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER",
 /* 299 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER",
 /* 300 */ "slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 301 */ "limit_clause_opt ::=",
 /* 302 */ "limit_clause_opt ::= LIMIT NK_INTEGER",
 /* 303 */ "limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER",
 /* 304 */ "limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER",
 /* 305 */ "subquery ::= NK_LP query_expression NK_RP",
 /* 306 */ "search_condition ::= common_expression",
 /* 307 */ "sort_specification_list ::= sort_specification",
 /* 308 */ "sort_specification_list ::= sort_specification_list NK_COMMA sort_specification",
 /* 309 */ "sort_specification ::= expression ordering_specification_opt null_ordering_opt",
 /* 310 */ "ordering_specification_opt ::=",
 /* 311 */ "ordering_specification_opt ::= ASC",
 /* 312 */ "ordering_specification_opt ::= DESC",
 /* 313 */ "null_ordering_opt ::=",
 /* 314 */ "null_ordering_opt ::= NULLS FIRST",
 /* 315 */ "null_ordering_opt ::= NULLS LAST",
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
    case 160: /* cmd */
    case 163: /* literal */
    case 170: /* db_options */
    case 172: /* alter_db_options */
    case 174: /* full_table_name */
    case 177: /* table_options */
    case 181: /* alter_table_clause */
    case 182: /* alter_table_options */
    case 185: /* create_subtable_clause */
    case 188: /* drop_table_clause */
    case 191: /* column_def */
    case 194: /* col_name */
    case 195: /* func_name */
    case 198: /* index_options */
    case 200: /* duration_literal */
    case 201: /* sliding_opt */
    case 202: /* func */
    case 205: /* query_expression */
    case 208: /* expression */
    case 209: /* column_reference */
    case 210: /* subquery */
    case 211: /* predicate */
    case 214: /* in_predicate_value */
    case 215: /* boolean_value_expression */
    case 216: /* boolean_primary */
    case 217: /* common_expression */
    case 218: /* from_clause */
    case 219: /* table_reference_list */
    case 220: /* table_reference */
    case 221: /* table_primary */
    case 222: /* joined_table */
    case 224: /* parenthesized_joined_table */
    case 226: /* search_condition */
    case 227: /* query_specification */
    case 230: /* where_clause_opt */
    case 232: /* twindow_clause_opt */
    case 234: /* having_clause_opt */
    case 236: /* select_item */
    case 237: /* fill_opt */
    case 240: /* query_expression_body */
    case 242: /* slimit_clause_opt */
    case 243: /* limit_clause_opt */
    case 244: /* query_primary */
    case 246: /* sort_specification */
{
 nodesDestroyNode((yypminor->yy26)); 
}
      break;
    case 161: /* account_options */
    case 162: /* alter_account_options */
    case 164: /* alter_account_option */
{
 
}
      break;
    case 165: /* user_name */
    case 166: /* dnode_endpoint */
    case 167: /* dnode_host_name */
    case 169: /* db_name */
    case 183: /* column_name */
    case 190: /* table_name */
    case 196: /* function_name */
    case 197: /* index_name */
    case 204: /* topic_name */
    case 206: /* table_alias */
    case 207: /* column_alias */
    case 223: /* alias_opt */
{
 
}
      break;
    case 168: /* not_exists_opt */
    case 171: /* exists_opt */
    case 228: /* set_quantifier_opt */
{
 
}
      break;
    case 173: /* alter_db_option */
    case 193: /* alter_table_option */
{
 
}
      break;
    case 175: /* column_def_list */
    case 176: /* tags_def_opt */
    case 178: /* multi_create_clause */
    case 179: /* tags_def */
    case 180: /* multi_drop_clause */
    case 186: /* specific_tags_opt */
    case 187: /* literal_list */
    case 189: /* col_name_list */
    case 192: /* func_name_list */
    case 199: /* func_list */
    case 203: /* expression_list */
    case 229: /* select_list */
    case 231: /* partition_by_clause_opt */
    case 233: /* group_by_clause_opt */
    case 235: /* select_sublist */
    case 239: /* group_by_list */
    case 241: /* order_by_clause_opt */
    case 245: /* sort_specification_list */
{
 nodesDestroyList((yypminor->yy64)); 
}
      break;
    case 184: /* type_name */
{
 
}
      break;
    case 212: /* compare_op */
    case 213: /* in_op */
{
 
}
      break;
    case 225: /* join_type */
{
 
}
      break;
    case 238: /* fill_mode */
{
 
}
      break;
    case 247: /* ordering_specification_opt */
{
 
}
      break;
    case 248: /* null_ordering_opt */
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
  {  160,   -6 }, /* (0) cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
  {  160,   -4 }, /* (1) cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
  {  161,    0 }, /* (2) account_options ::= */
  {  161,   -3 }, /* (3) account_options ::= account_options PPS literal */
  {  161,   -3 }, /* (4) account_options ::= account_options TSERIES literal */
  {  161,   -3 }, /* (5) account_options ::= account_options STORAGE literal */
  {  161,   -3 }, /* (6) account_options ::= account_options STREAMS literal */
  {  161,   -3 }, /* (7) account_options ::= account_options QTIME literal */
  {  161,   -3 }, /* (8) account_options ::= account_options DBS literal */
  {  161,   -3 }, /* (9) account_options ::= account_options USERS literal */
  {  161,   -3 }, /* (10) account_options ::= account_options CONNS literal */
  {  161,   -3 }, /* (11) account_options ::= account_options STATE literal */
  {  162,   -1 }, /* (12) alter_account_options ::= alter_account_option */
  {  162,   -2 }, /* (13) alter_account_options ::= alter_account_options alter_account_option */
  {  164,   -2 }, /* (14) alter_account_option ::= PASS literal */
  {  164,   -2 }, /* (15) alter_account_option ::= PPS literal */
  {  164,   -2 }, /* (16) alter_account_option ::= TSERIES literal */
  {  164,   -2 }, /* (17) alter_account_option ::= STORAGE literal */
  {  164,   -2 }, /* (18) alter_account_option ::= STREAMS literal */
  {  164,   -2 }, /* (19) alter_account_option ::= QTIME literal */
  {  164,   -2 }, /* (20) alter_account_option ::= DBS literal */
  {  164,   -2 }, /* (21) alter_account_option ::= USERS literal */
  {  164,   -2 }, /* (22) alter_account_option ::= CONNS literal */
  {  164,   -2 }, /* (23) alter_account_option ::= STATE literal */
  {  160,   -5 }, /* (24) cmd ::= CREATE USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (25) cmd ::= ALTER USER user_name PASS NK_STRING */
  {  160,   -5 }, /* (26) cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
  {  160,   -3 }, /* (27) cmd ::= DROP USER user_name */
  {  160,   -2 }, /* (28) cmd ::= SHOW USERS */
  {  160,   -3 }, /* (29) cmd ::= CREATE DNODE dnode_endpoint */
  {  160,   -5 }, /* (30) cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
  {  160,   -3 }, /* (31) cmd ::= DROP DNODE NK_INTEGER */
  {  160,   -3 }, /* (32) cmd ::= DROP DNODE dnode_endpoint */
  {  160,   -2 }, /* (33) cmd ::= SHOW DNODES */
  {  160,   -4 }, /* (34) cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
  {  160,   -5 }, /* (35) cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
  {  160,   -4 }, /* (36) cmd ::= ALTER ALL DNODES NK_STRING */
  {  160,   -5 }, /* (37) cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
  {  166,   -1 }, /* (38) dnode_endpoint ::= NK_STRING */
  {  167,   -1 }, /* (39) dnode_host_name ::= NK_ID */
  {  167,   -1 }, /* (40) dnode_host_name ::= NK_IPTOKEN */
  {  160,   -3 }, /* (41) cmd ::= ALTER LOCAL NK_STRING */
  {  160,   -4 }, /* (42) cmd ::= ALTER LOCAL NK_STRING NK_STRING */
  {  160,   -5 }, /* (43) cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
  {  160,   -5 }, /* (44) cmd ::= DROP QNODE ON DNODE NK_INTEGER */
  {  160,   -2 }, /* (45) cmd ::= SHOW QNODES */
  {  160,   -5 }, /* (46) cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
  {  160,   -4 }, /* (47) cmd ::= DROP DATABASE exists_opt db_name */
  {  160,   -2 }, /* (48) cmd ::= SHOW DATABASES */
  {  160,   -2 }, /* (49) cmd ::= USE db_name */
  {  160,   -4 }, /* (50) cmd ::= ALTER DATABASE db_name alter_db_options */
  {  168,   -3 }, /* (51) not_exists_opt ::= IF NOT EXISTS */
  {  168,    0 }, /* (52) not_exists_opt ::= */
  {  171,   -2 }, /* (53) exists_opt ::= IF EXISTS */
  {  171,    0 }, /* (54) exists_opt ::= */
  {  170,    0 }, /* (55) db_options ::= */
  {  170,   -3 }, /* (56) db_options ::= db_options BLOCKS NK_INTEGER */
  {  170,   -3 }, /* (57) db_options ::= db_options CACHE NK_INTEGER */
  {  170,   -3 }, /* (58) db_options ::= db_options CACHELAST NK_INTEGER */
  {  170,   -3 }, /* (59) db_options ::= db_options COMP NK_INTEGER */
  {  170,   -3 }, /* (60) db_options ::= db_options DAYS NK_INTEGER */
  {  170,   -3 }, /* (61) db_options ::= db_options FSYNC NK_INTEGER */
  {  170,   -3 }, /* (62) db_options ::= db_options MAXROWS NK_INTEGER */
  {  170,   -3 }, /* (63) db_options ::= db_options MINROWS NK_INTEGER */
  {  170,   -3 }, /* (64) db_options ::= db_options KEEP NK_INTEGER */
  {  170,   -3 }, /* (65) db_options ::= db_options PRECISION NK_STRING */
  {  170,   -3 }, /* (66) db_options ::= db_options QUORUM NK_INTEGER */
  {  170,   -3 }, /* (67) db_options ::= db_options REPLICA NK_INTEGER */
  {  170,   -3 }, /* (68) db_options ::= db_options TTL NK_INTEGER */
  {  170,   -3 }, /* (69) db_options ::= db_options WAL NK_INTEGER */
  {  170,   -3 }, /* (70) db_options ::= db_options VGROUPS NK_INTEGER */
  {  170,   -3 }, /* (71) db_options ::= db_options SINGLE_STABLE NK_INTEGER */
  {  170,   -3 }, /* (72) db_options ::= db_options STREAM_MODE NK_INTEGER */
  {  170,   -3 }, /* (73) db_options ::= db_options RETENTIONS NK_STRING */
  {  170,   -3 }, /* (74) db_options ::= db_options FILE_FACTOR NK_FLOAT */
  {  172,   -1 }, /* (75) alter_db_options ::= alter_db_option */
  {  172,   -2 }, /* (76) alter_db_options ::= alter_db_options alter_db_option */
  {  173,   -2 }, /* (77) alter_db_option ::= BLOCKS NK_INTEGER */
  {  173,   -2 }, /* (78) alter_db_option ::= FSYNC NK_INTEGER */
  {  173,   -2 }, /* (79) alter_db_option ::= KEEP NK_INTEGER */
  {  173,   -2 }, /* (80) alter_db_option ::= WAL NK_INTEGER */
  {  173,   -2 }, /* (81) alter_db_option ::= QUORUM NK_INTEGER */
  {  173,   -2 }, /* (82) alter_db_option ::= CACHELAST NK_INTEGER */
  {  160,   -9 }, /* (83) cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
  {  160,   -3 }, /* (84) cmd ::= CREATE TABLE multi_create_clause */
  {  160,   -9 }, /* (85) cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */
  {  160,   -3 }, /* (86) cmd ::= DROP TABLE multi_drop_clause */
  {  160,   -4 }, /* (87) cmd ::= DROP STABLE exists_opt full_table_name */
  {  160,   -2 }, /* (88) cmd ::= SHOW TABLES */
  {  160,   -2 }, /* (89) cmd ::= SHOW STABLES */
  {  160,   -3 }, /* (90) cmd ::= ALTER TABLE alter_table_clause */
  {  160,   -3 }, /* (91) cmd ::= ALTER STABLE alter_table_clause */
  {  181,   -2 }, /* (92) alter_table_clause ::= full_table_name alter_table_options */
  {  181,   -5 }, /* (93) alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
  {  181,   -4 }, /* (94) alter_table_clause ::= full_table_name DROP COLUMN column_name */
  {  181,   -5 }, /* (95) alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
  {  181,   -5 }, /* (96) alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
  {  181,   -5 }, /* (97) alter_table_clause ::= full_table_name ADD TAG column_name type_name */
  {  181,   -4 }, /* (98) alter_table_clause ::= full_table_name DROP TAG column_name */
  {  181,   -5 }, /* (99) alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
  {  181,   -5 }, /* (100) alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
  {  181,   -6 }, /* (101) alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
  {  178,   -1 }, /* (102) multi_create_clause ::= create_subtable_clause */
  {  178,   -2 }, /* (103) multi_create_clause ::= multi_create_clause create_subtable_clause */
  {  185,   -9 }, /* (104) create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
  {  180,   -1 }, /* (105) multi_drop_clause ::= drop_table_clause */
  {  180,   -2 }, /* (106) multi_drop_clause ::= multi_drop_clause drop_table_clause */
  {  188,   -2 }, /* (107) drop_table_clause ::= exists_opt full_table_name */
  {  186,    0 }, /* (108) specific_tags_opt ::= */
  {  186,   -3 }, /* (109) specific_tags_opt ::= NK_LP col_name_list NK_RP */
  {  174,   -1 }, /* (110) full_table_name ::= table_name */
  {  174,   -3 }, /* (111) full_table_name ::= db_name NK_DOT table_name */
  {  175,   -1 }, /* (112) column_def_list ::= column_def */
  {  175,   -3 }, /* (113) column_def_list ::= column_def_list NK_COMMA column_def */
  {  191,   -2 }, /* (114) column_def ::= column_name type_name */
  {  191,   -4 }, /* (115) column_def ::= column_name type_name COMMENT NK_STRING */
  {  184,   -1 }, /* (116) type_name ::= BOOL */
  {  184,   -1 }, /* (117) type_name ::= TINYINT */
  {  184,   -1 }, /* (118) type_name ::= SMALLINT */
  {  184,   -1 }, /* (119) type_name ::= INT */
  {  184,   -1 }, /* (120) type_name ::= INTEGER */
  {  184,   -1 }, /* (121) type_name ::= BIGINT */
  {  184,   -1 }, /* (122) type_name ::= FLOAT */
  {  184,   -1 }, /* (123) type_name ::= DOUBLE */
  {  184,   -4 }, /* (124) type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (125) type_name ::= TIMESTAMP */
  {  184,   -4 }, /* (126) type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
  {  184,   -2 }, /* (127) type_name ::= TINYINT UNSIGNED */
  {  184,   -2 }, /* (128) type_name ::= SMALLINT UNSIGNED */
  {  184,   -2 }, /* (129) type_name ::= INT UNSIGNED */
  {  184,   -2 }, /* (130) type_name ::= BIGINT UNSIGNED */
  {  184,   -1 }, /* (131) type_name ::= JSON */
  {  184,   -4 }, /* (132) type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (133) type_name ::= MEDIUMBLOB */
  {  184,   -1 }, /* (134) type_name ::= BLOB */
  {  184,   -4 }, /* (135) type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
  {  184,   -1 }, /* (136) type_name ::= DECIMAL */
  {  184,   -4 }, /* (137) type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
  {  184,   -6 }, /* (138) type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
  {  176,    0 }, /* (139) tags_def_opt ::= */
  {  176,   -1 }, /* (140) tags_def_opt ::= tags_def */
  {  179,   -4 }, /* (141) tags_def ::= TAGS NK_LP column_def_list NK_RP */
  {  177,    0 }, /* (142) table_options ::= */
  {  177,   -3 }, /* (143) table_options ::= table_options COMMENT NK_STRING */
  {  177,   -3 }, /* (144) table_options ::= table_options KEEP NK_INTEGER */
  {  177,   -3 }, /* (145) table_options ::= table_options TTL NK_INTEGER */
  {  177,   -5 }, /* (146) table_options ::= table_options SMA NK_LP col_name_list NK_RP */
  {  177,   -5 }, /* (147) table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
  {  182,   -1 }, /* (148) alter_table_options ::= alter_table_option */
  {  182,   -2 }, /* (149) alter_table_options ::= alter_table_options alter_table_option */
  {  193,   -2 }, /* (150) alter_table_option ::= COMMENT NK_STRING */
  {  193,   -2 }, /* (151) alter_table_option ::= KEEP NK_INTEGER */
  {  193,   -2 }, /* (152) alter_table_option ::= TTL NK_INTEGER */
  {  189,   -1 }, /* (153) col_name_list ::= col_name */
  {  189,   -3 }, /* (154) col_name_list ::= col_name_list NK_COMMA col_name */
  {  194,   -1 }, /* (155) col_name ::= column_name */
  {  192,   -1 }, /* (156) func_name_list ::= func_name */
  {  192,   -3 }, /* (157) func_name_list ::= func_name_list NK_COMMA col_name */
  {  195,   -1 }, /* (158) func_name ::= function_name */
  {  160,   -7 }, /* (159) cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
  {  160,   -9 }, /* (160) cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
  {  160,   -5 }, /* (161) cmd ::= DROP INDEX index_name ON table_name */
  {  198,    0 }, /* (162) index_options ::= */
  {  198,   -9 }, /* (163) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
  {  198,  -11 }, /* (164) index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
  {  199,   -1 }, /* (165) func_list ::= func */
  {  199,   -3 }, /* (166) func_list ::= func_list NK_COMMA func */
  {  202,   -4 }, /* (167) func ::= function_name NK_LP expression_list NK_RP */
  {  160,   -6 }, /* (168) cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
  {  160,   -6 }, /* (169) cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
  {  160,   -4 }, /* (170) cmd ::= DROP TOPIC exists_opt topic_name */
  {  160,   -2 }, /* (171) cmd ::= SHOW VGROUPS */
  {  160,   -4 }, /* (172) cmd ::= SHOW db_name NK_DOT VGROUPS */
  {  160,   -2 }, /* (173) cmd ::= SHOW MNODES */
  {  160,   -1 }, /* (174) cmd ::= query_expression */
  {  163,   -1 }, /* (175) literal ::= NK_INTEGER */
  {  163,   -1 }, /* (176) literal ::= NK_FLOAT */
  {  163,   -1 }, /* (177) literal ::= NK_STRING */
  {  163,   -1 }, /* (178) literal ::= NK_BOOL */
  {  163,   -2 }, /* (179) literal ::= TIMESTAMP NK_STRING */
  {  163,   -1 }, /* (180) literal ::= duration_literal */
  {  200,   -1 }, /* (181) duration_literal ::= NK_VARIABLE */
  {  187,   -1 }, /* (182) literal_list ::= literal */
  {  187,   -3 }, /* (183) literal_list ::= literal_list NK_COMMA literal */
  {  169,   -1 }, /* (184) db_name ::= NK_ID */
  {  190,   -1 }, /* (185) table_name ::= NK_ID */
  {  183,   -1 }, /* (186) column_name ::= NK_ID */
  {  196,   -1 }, /* (187) function_name ::= NK_ID */
  {  206,   -1 }, /* (188) table_alias ::= NK_ID */
  {  207,   -1 }, /* (189) column_alias ::= NK_ID */
  {  165,   -1 }, /* (190) user_name ::= NK_ID */
  {  197,   -1 }, /* (191) index_name ::= NK_ID */
  {  204,   -1 }, /* (192) topic_name ::= NK_ID */
  {  208,   -1 }, /* (193) expression ::= literal */
  {  208,   -1 }, /* (194) expression ::= column_reference */
  {  208,   -4 }, /* (195) expression ::= function_name NK_LP expression_list NK_RP */
  {  208,   -4 }, /* (196) expression ::= function_name NK_LP NK_STAR NK_RP */
  {  208,   -1 }, /* (197) expression ::= subquery */
  {  208,   -3 }, /* (198) expression ::= NK_LP expression NK_RP */
  {  208,   -2 }, /* (199) expression ::= NK_PLUS expression */
  {  208,   -2 }, /* (200) expression ::= NK_MINUS expression */
  {  208,   -3 }, /* (201) expression ::= expression NK_PLUS expression */
  {  208,   -3 }, /* (202) expression ::= expression NK_MINUS expression */
  {  208,   -3 }, /* (203) expression ::= expression NK_STAR expression */
  {  208,   -3 }, /* (204) expression ::= expression NK_SLASH expression */
  {  208,   -3 }, /* (205) expression ::= expression NK_REM expression */
  {  203,   -1 }, /* (206) expression_list ::= expression */
  {  203,   -3 }, /* (207) expression_list ::= expression_list NK_COMMA expression */
  {  209,   -1 }, /* (208) column_reference ::= column_name */
  {  209,   -3 }, /* (209) column_reference ::= table_name NK_DOT column_name */
  {  211,   -3 }, /* (210) predicate ::= expression compare_op expression */
  {  211,   -5 }, /* (211) predicate ::= expression BETWEEN expression AND expression */
  {  211,   -6 }, /* (212) predicate ::= expression NOT BETWEEN expression AND expression */
  {  211,   -3 }, /* (213) predicate ::= expression IS NULL */
  {  211,   -4 }, /* (214) predicate ::= expression IS NOT NULL */
  {  211,   -3 }, /* (215) predicate ::= expression in_op in_predicate_value */
  {  212,   -1 }, /* (216) compare_op ::= NK_LT */
  {  212,   -1 }, /* (217) compare_op ::= NK_GT */
  {  212,   -1 }, /* (218) compare_op ::= NK_LE */
  {  212,   -1 }, /* (219) compare_op ::= NK_GE */
  {  212,   -1 }, /* (220) compare_op ::= NK_NE */
  {  212,   -1 }, /* (221) compare_op ::= NK_EQ */
  {  212,   -1 }, /* (222) compare_op ::= LIKE */
  {  212,   -2 }, /* (223) compare_op ::= NOT LIKE */
  {  212,   -1 }, /* (224) compare_op ::= MATCH */
  {  212,   -1 }, /* (225) compare_op ::= NMATCH */
  {  213,   -1 }, /* (226) in_op ::= IN */
  {  213,   -2 }, /* (227) in_op ::= NOT IN */
  {  214,   -3 }, /* (228) in_predicate_value ::= NK_LP expression_list NK_RP */
  {  215,   -1 }, /* (229) boolean_value_expression ::= boolean_primary */
  {  215,   -2 }, /* (230) boolean_value_expression ::= NOT boolean_primary */
  {  215,   -3 }, /* (231) boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
  {  215,   -3 }, /* (232) boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
  {  216,   -1 }, /* (233) boolean_primary ::= predicate */
  {  216,   -3 }, /* (234) boolean_primary ::= NK_LP boolean_value_expression NK_RP */
  {  217,   -1 }, /* (235) common_expression ::= expression */
  {  217,   -1 }, /* (236) common_expression ::= boolean_value_expression */
  {  218,   -2 }, /* (237) from_clause ::= FROM table_reference_list */
  {  219,   -1 }, /* (238) table_reference_list ::= table_reference */
  {  219,   -3 }, /* (239) table_reference_list ::= table_reference_list NK_COMMA table_reference */
  {  220,   -1 }, /* (240) table_reference ::= table_primary */
  {  220,   -1 }, /* (241) table_reference ::= joined_table */
  {  221,   -2 }, /* (242) table_primary ::= table_name alias_opt */
  {  221,   -4 }, /* (243) table_primary ::= db_name NK_DOT table_name alias_opt */
  {  221,   -2 }, /* (244) table_primary ::= subquery alias_opt */
  {  221,   -1 }, /* (245) table_primary ::= parenthesized_joined_table */
  {  223,    0 }, /* (246) alias_opt ::= */
  {  223,   -1 }, /* (247) alias_opt ::= table_alias */
  {  223,   -2 }, /* (248) alias_opt ::= AS table_alias */
  {  224,   -3 }, /* (249) parenthesized_joined_table ::= NK_LP joined_table NK_RP */
  {  224,   -3 }, /* (250) parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */
  {  222,   -6 }, /* (251) joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
  {  225,    0 }, /* (252) join_type ::= */
  {  225,   -1 }, /* (253) join_type ::= INNER */
  {  227,   -9 }, /* (254) query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
  {  228,    0 }, /* (255) set_quantifier_opt ::= */
  {  228,   -1 }, /* (256) set_quantifier_opt ::= DISTINCT */
  {  228,   -1 }, /* (257) set_quantifier_opt ::= ALL */
  {  229,   -1 }, /* (258) select_list ::= NK_STAR */
  {  229,   -1 }, /* (259) select_list ::= select_sublist */
  {  235,   -1 }, /* (260) select_sublist ::= select_item */
  {  235,   -3 }, /* (261) select_sublist ::= select_sublist NK_COMMA select_item */
  {  236,   -1 }, /* (262) select_item ::= common_expression */
  {  236,   -2 }, /* (263) select_item ::= common_expression column_alias */
  {  236,   -3 }, /* (264) select_item ::= common_expression AS column_alias */
  {  236,   -3 }, /* (265) select_item ::= table_name NK_DOT NK_STAR */
  {  230,    0 }, /* (266) where_clause_opt ::= */
  {  230,   -2 }, /* (267) where_clause_opt ::= WHERE search_condition */
  {  231,    0 }, /* (268) partition_by_clause_opt ::= */
  {  231,   -3 }, /* (269) partition_by_clause_opt ::= PARTITION BY expression_list */
  {  232,    0 }, /* (270) twindow_clause_opt ::= */
  {  232,   -6 }, /* (271) twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
  {  232,   -4 }, /* (272) twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
  {  232,   -6 }, /* (273) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
  {  232,   -8 }, /* (274) twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
  {  201,    0 }, /* (275) sliding_opt ::= */
  {  201,   -4 }, /* (276) sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
  {  237,    0 }, /* (277) fill_opt ::= */
  {  237,   -4 }, /* (278) fill_opt ::= FILL NK_LP fill_mode NK_RP */
  {  237,   -6 }, /* (279) fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
  {  238,   -1 }, /* (280) fill_mode ::= NONE */
  {  238,   -1 }, /* (281) fill_mode ::= PREV */
  {  238,   -1 }, /* (282) fill_mode ::= NULL */
  {  238,   -1 }, /* (283) fill_mode ::= LINEAR */
  {  238,   -1 }, /* (284) fill_mode ::= NEXT */
  {  233,    0 }, /* (285) group_by_clause_opt ::= */
  {  233,   -3 }, /* (286) group_by_clause_opt ::= GROUP BY group_by_list */
  {  239,   -1 }, /* (287) group_by_list ::= expression */
  {  239,   -3 }, /* (288) group_by_list ::= group_by_list NK_COMMA expression */
  {  234,    0 }, /* (289) having_clause_opt ::= */
  {  234,   -2 }, /* (290) having_clause_opt ::= HAVING search_condition */
  {  205,   -4 }, /* (291) query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
  {  240,   -1 }, /* (292) query_expression_body ::= query_primary */
  {  240,   -4 }, /* (293) query_expression_body ::= query_expression_body UNION ALL query_expression_body */
  {  244,   -1 }, /* (294) query_primary ::= query_specification */
  {  241,    0 }, /* (295) order_by_clause_opt ::= */
  {  241,   -3 }, /* (296) order_by_clause_opt ::= ORDER BY sort_specification_list */
  {  242,    0 }, /* (297) slimit_clause_opt ::= */
  {  242,   -2 }, /* (298) slimit_clause_opt ::= SLIMIT NK_INTEGER */
  {  242,   -4 }, /* (299) slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
  {  242,   -4 }, /* (300) slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  243,    0 }, /* (301) limit_clause_opt ::= */
  {  243,   -2 }, /* (302) limit_clause_opt ::= LIMIT NK_INTEGER */
  {  243,   -4 }, /* (303) limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */
  {  243,   -4 }, /* (304) limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */
  {  210,   -3 }, /* (305) subquery ::= NK_LP query_expression NK_RP */
  {  226,   -1 }, /* (306) search_condition ::= common_expression */
  {  245,   -1 }, /* (307) sort_specification_list ::= sort_specification */
  {  245,   -3 }, /* (308) sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */
  {  246,   -3 }, /* (309) sort_specification ::= expression ordering_specification_opt null_ordering_opt */
  {  247,    0 }, /* (310) ordering_specification_opt ::= */
  {  247,   -1 }, /* (311) ordering_specification_opt ::= ASC */
  {  247,   -1 }, /* (312) ordering_specification_opt ::= DESC */
  {  248,    0 }, /* (313) null_ordering_opt ::= */
  {  248,   -2 }, /* (314) null_ordering_opt ::= NULLS FIRST */
  {  248,   -2 }, /* (315) null_ordering_opt ::= NULLS LAST */
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
      case 0: /* cmd ::= CREATE ACCOUNT NK_ID PASS NK_STRING account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,161,&yymsp[0].minor);
        break;
      case 1: /* cmd ::= ALTER ACCOUNT NK_ID alter_account_options */
{ pCxt->valid = false; generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_EXPRIE_STATEMENT); }
  yy_destructor(yypParser,162,&yymsp[0].minor);
        break;
      case 2: /* account_options ::= */
{ }
        break;
      case 3: /* account_options ::= account_options PPS literal */
      case 4: /* account_options ::= account_options TSERIES literal */ yytestcase(yyruleno==4);
      case 5: /* account_options ::= account_options STORAGE literal */ yytestcase(yyruleno==5);
      case 6: /* account_options ::= account_options STREAMS literal */ yytestcase(yyruleno==6);
      case 7: /* account_options ::= account_options QTIME literal */ yytestcase(yyruleno==7);
      case 8: /* account_options ::= account_options DBS literal */ yytestcase(yyruleno==8);
      case 9: /* account_options ::= account_options USERS literal */ yytestcase(yyruleno==9);
      case 10: /* account_options ::= account_options CONNS literal */ yytestcase(yyruleno==10);
      case 11: /* account_options ::= account_options STATE literal */ yytestcase(yyruleno==11);
{  yy_destructor(yypParser,161,&yymsp[-2].minor);
{ }
  yy_destructor(yypParser,163,&yymsp[0].minor);
}
        break;
      case 12: /* alter_account_options ::= alter_account_option */
{  yy_destructor(yypParser,164,&yymsp[0].minor);
{ }
}
        break;
      case 13: /* alter_account_options ::= alter_account_options alter_account_option */
{  yy_destructor(yypParser,162,&yymsp[-1].minor);
{ }
  yy_destructor(yypParser,164,&yymsp[0].minor);
}
        break;
      case 14: /* alter_account_option ::= PASS literal */
      case 15: /* alter_account_option ::= PPS literal */ yytestcase(yyruleno==15);
      case 16: /* alter_account_option ::= TSERIES literal */ yytestcase(yyruleno==16);
      case 17: /* alter_account_option ::= STORAGE literal */ yytestcase(yyruleno==17);
      case 18: /* alter_account_option ::= STREAMS literal */ yytestcase(yyruleno==18);
      case 19: /* alter_account_option ::= QTIME literal */ yytestcase(yyruleno==19);
      case 20: /* alter_account_option ::= DBS literal */ yytestcase(yyruleno==20);
      case 21: /* alter_account_option ::= USERS literal */ yytestcase(yyruleno==21);
      case 22: /* alter_account_option ::= CONNS literal */ yytestcase(yyruleno==22);
      case 23: /* alter_account_option ::= STATE literal */ yytestcase(yyruleno==23);
{ }
  yy_destructor(yypParser,163,&yymsp[0].minor);
        break;
      case 24: /* cmd ::= CREATE USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createCreateUserStmt(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy0); }
        break;
      case 25: /* cmd ::= ALTER USER user_name PASS NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy353, TSDB_ALTER_USER_PASSWD, &yymsp[0].minor.yy0); }
        break;
      case 26: /* cmd ::= ALTER USER user_name PRIVILEGE NK_STRING */
{ pCxt->pRootNode = createAlterUserStmt(pCxt, &yymsp[-2].minor.yy353, TSDB_ALTER_USER_PRIVILEGES, &yymsp[0].minor.yy0); }
        break;
      case 27: /* cmd ::= DROP USER user_name */
{ pCxt->pRootNode = createDropUserStmt(pCxt, &yymsp[0].minor.yy353); }
        break;
      case 28: /* cmd ::= SHOW USERS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_USERS_STMT, NULL); }
        break;
      case 29: /* cmd ::= CREATE DNODE dnode_endpoint */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[0].minor.yy353, NULL); }
        break;
      case 30: /* cmd ::= CREATE DNODE dnode_host_name PORT NK_INTEGER */
{ pCxt->pRootNode = createCreateDnodeStmt(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy0); }
        break;
      case 31: /* cmd ::= DROP DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 32: /* cmd ::= DROP DNODE dnode_endpoint */
{ pCxt->pRootNode = createDropDnodeStmt(pCxt, &yymsp[0].minor.yy353); }
        break;
      case 33: /* cmd ::= SHOW DNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DNODES_STMT, NULL); }
        break;
      case 34: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, NULL); }
        break;
      case 35: /* cmd ::= ALTER DNODE NK_INTEGER NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 36: /* cmd ::= ALTER ALL DNODES NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[0].minor.yy0, NULL); }
        break;
      case 37: /* cmd ::= ALTER ALL DNODES NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterDnodeStmt(pCxt, NULL, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 38: /* dnode_endpoint ::= NK_STRING */
      case 39: /* dnode_host_name ::= NK_ID */ yytestcase(yyruleno==39);
      case 40: /* dnode_host_name ::= NK_IPTOKEN */ yytestcase(yyruleno==40);
      case 184: /* db_name ::= NK_ID */ yytestcase(yyruleno==184);
      case 185: /* table_name ::= NK_ID */ yytestcase(yyruleno==185);
      case 186: /* column_name ::= NK_ID */ yytestcase(yyruleno==186);
      case 187: /* function_name ::= NK_ID */ yytestcase(yyruleno==187);
      case 188: /* table_alias ::= NK_ID */ yytestcase(yyruleno==188);
      case 189: /* column_alias ::= NK_ID */ yytestcase(yyruleno==189);
      case 190: /* user_name ::= NK_ID */ yytestcase(yyruleno==190);
      case 191: /* index_name ::= NK_ID */ yytestcase(yyruleno==191);
      case 192: /* topic_name ::= NK_ID */ yytestcase(yyruleno==192);
{ yylhsminor.yy353 = yymsp[0].minor.yy0; }
  yymsp[0].minor.yy353 = yylhsminor.yy353;
        break;
      case 41: /* cmd ::= ALTER LOCAL NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 42: /* cmd ::= ALTER LOCAL NK_STRING NK_STRING */
{ pCxt->pRootNode = createAlterLocalStmt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 43: /* cmd ::= CREATE QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createCreateQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 44: /* cmd ::= DROP QNODE ON DNODE NK_INTEGER */
{ pCxt->pRootNode = createDropQnodeStmt(pCxt, &yymsp[0].minor.yy0); }
        break;
      case 45: /* cmd ::= SHOW QNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_QNODES_STMT, NULL); }
        break;
      case 46: /* cmd ::= CREATE DATABASE not_exists_opt db_name db_options */
{ pCxt->pRootNode = createCreateDatabaseStmt(pCxt, yymsp[-2].minor.yy107, &yymsp[-1].minor.yy353, yymsp[0].minor.yy26); }
        break;
      case 47: /* cmd ::= DROP DATABASE exists_opt db_name */
{ pCxt->pRootNode = createDropDatabaseStmt(pCxt, yymsp[-1].minor.yy107, &yymsp[0].minor.yy353); }
        break;
      case 48: /* cmd ::= SHOW DATABASES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_DATABASES_STMT, NULL); }
        break;
      case 49: /* cmd ::= USE db_name */
{ pCxt->pRootNode = createUseDatabaseStmt(pCxt, &yymsp[0].minor.yy353); }
        break;
      case 50: /* cmd ::= ALTER DATABASE db_name alter_db_options */
{ pCxt->pRootNode = createAlterDatabaseStmt(pCxt, &yymsp[-1].minor.yy353, yymsp[0].minor.yy26); }
        break;
      case 51: /* not_exists_opt ::= IF NOT EXISTS */
{ yymsp[-2].minor.yy107 = true; }
        break;
      case 52: /* not_exists_opt ::= */
      case 54: /* exists_opt ::= */ yytestcase(yyruleno==54);
      case 255: /* set_quantifier_opt ::= */ yytestcase(yyruleno==255);
{ yymsp[1].minor.yy107 = false; }
        break;
      case 53: /* exists_opt ::= IF EXISTS */
{ yymsp[-1].minor.yy107 = true; }
        break;
      case 55: /* db_options ::= */
{ yymsp[1].minor.yy26 = createDefaultDatabaseOptions(pCxt); }
        break;
      case 56: /* db_options ::= db_options BLOCKS NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_BLOCKS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 57: /* db_options ::= db_options CACHE NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_CACHE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 58: /* db_options ::= db_options CACHELAST NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_CACHELAST, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 59: /* db_options ::= db_options COMP NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_COMP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 60: /* db_options ::= db_options DAYS NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_DAYS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 61: /* db_options ::= db_options FSYNC NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_FSYNC, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 62: /* db_options ::= db_options MAXROWS NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_MAXROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 63: /* db_options ::= db_options MINROWS NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_MINROWS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 64: /* db_options ::= db_options KEEP NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 65: /* db_options ::= db_options PRECISION NK_STRING */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_PRECISION, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 66: /* db_options ::= db_options QUORUM NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_QUORUM, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 67: /* db_options ::= db_options REPLICA NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_REPLICA, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 68: /* db_options ::= db_options TTL NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 69: /* db_options ::= db_options WAL NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_WAL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 70: /* db_options ::= db_options VGROUPS NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_VGROUPS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 71: /* db_options ::= db_options SINGLE_STABLE NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_SINGLE_STABLE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 72: /* db_options ::= db_options STREAM_MODE NK_INTEGER */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_STREAM_MODE, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 73: /* db_options ::= db_options RETENTIONS NK_STRING */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_RETENTIONS, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 74: /* db_options ::= db_options FILE_FACTOR NK_FLOAT */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-2].minor.yy26, DB_OPTION_FILE_FACTOR, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 75: /* alter_db_options ::= alter_db_option */
{ yylhsminor.yy26 = createDefaultAlterDatabaseOptions(pCxt); yylhsminor.yy26 = setDatabaseOption(pCxt, yylhsminor.yy26, yymsp[0].minor.yy443.type, &yymsp[0].minor.yy443.val); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 76: /* alter_db_options ::= alter_db_options alter_db_option */
{ yylhsminor.yy26 = setDatabaseOption(pCxt, yymsp[-1].minor.yy26, yymsp[0].minor.yy443.type, &yymsp[0].minor.yy443.val); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 77: /* alter_db_option ::= BLOCKS NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_BLOCKS; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 78: /* alter_db_option ::= FSYNC NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_FSYNC; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 79: /* alter_db_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_KEEP; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 80: /* alter_db_option ::= WAL NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_WAL; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 81: /* alter_db_option ::= QUORUM NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_QUORUM; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 82: /* alter_db_option ::= CACHELAST NK_INTEGER */
{ yymsp[-1].minor.yy443.type = DB_OPTION_CACHELAST; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 83: /* cmd ::= CREATE TABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def_opt table_options */
      case 85: /* cmd ::= CREATE STABLE not_exists_opt full_table_name NK_LP column_def_list NK_RP tags_def table_options */ yytestcase(yyruleno==85);
{ pCxt->pRootNode = createCreateTableStmt(pCxt, yymsp[-6].minor.yy107, yymsp[-5].minor.yy26, yymsp[-3].minor.yy64, yymsp[-1].minor.yy64, yymsp[0].minor.yy26); }
        break;
      case 84: /* cmd ::= CREATE TABLE multi_create_clause */
{ pCxt->pRootNode = createCreateMultiTableStmt(pCxt, yymsp[0].minor.yy64); }
        break;
      case 86: /* cmd ::= DROP TABLE multi_drop_clause */
{ pCxt->pRootNode = createDropTableStmt(pCxt, yymsp[0].minor.yy64); }
        break;
      case 87: /* cmd ::= DROP STABLE exists_opt full_table_name */
{ pCxt->pRootNode = createDropSuperTableStmt(pCxt, yymsp[-1].minor.yy107, yymsp[0].minor.yy26); }
        break;
      case 88: /* cmd ::= SHOW TABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_TABLES_STMT, NULL); }
        break;
      case 89: /* cmd ::= SHOW STABLES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_STABLES_STMT, NULL); }
        break;
      case 90: /* cmd ::= ALTER TABLE alter_table_clause */
      case 91: /* cmd ::= ALTER STABLE alter_table_clause */ yytestcase(yyruleno==91);
      case 174: /* cmd ::= query_expression */ yytestcase(yyruleno==174);
{ pCxt->pRootNode = yymsp[0].minor.yy26; }
        break;
      case 92: /* alter_table_clause ::= full_table_name alter_table_options */
{ yylhsminor.yy26 = createAlterTableOption(pCxt, yymsp[-1].minor.yy26, yymsp[0].minor.yy26); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 93: /* alter_table_clause ::= full_table_name ADD COLUMN column_name type_name */
{ yylhsminor.yy26 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_ADD_COLUMN, &yymsp[-1].minor.yy353, yymsp[0].minor.yy370); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 94: /* alter_table_clause ::= full_table_name DROP COLUMN column_name */
{ yylhsminor.yy26 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy26, TSDB_ALTER_TABLE_DROP_COLUMN, &yymsp[0].minor.yy353); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 95: /* alter_table_clause ::= full_table_name MODIFY COLUMN column_name type_name */
{ yylhsminor.yy26 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES, &yymsp[-1].minor.yy353, yymsp[0].minor.yy370); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 96: /* alter_table_clause ::= full_table_name RENAME COLUMN column_name column_name */
{ yylhsminor.yy26 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_UPDATE_COLUMN_NAME, &yymsp[-1].minor.yy353, &yymsp[0].minor.yy353); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 97: /* alter_table_clause ::= full_table_name ADD TAG column_name type_name */
{ yylhsminor.yy26 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_ADD_TAG, &yymsp[-1].minor.yy353, yymsp[0].minor.yy370); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 98: /* alter_table_clause ::= full_table_name DROP TAG column_name */
{ yylhsminor.yy26 = createAlterTableDropCol(pCxt, yymsp[-3].minor.yy26, TSDB_ALTER_TABLE_DROP_TAG, &yymsp[0].minor.yy353); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 99: /* alter_table_clause ::= full_table_name MODIFY TAG column_name type_name */
{ yylhsminor.yy26 = createAlterTableAddModifyCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_UPDATE_TAG_BYTES, &yymsp[-1].minor.yy353, yymsp[0].minor.yy370); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 100: /* alter_table_clause ::= full_table_name RENAME TAG column_name column_name */
{ yylhsminor.yy26 = createAlterTableRenameCol(pCxt, yymsp[-4].minor.yy26, TSDB_ALTER_TABLE_UPDATE_TAG_NAME, &yymsp[-1].minor.yy353, &yymsp[0].minor.yy353); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 101: /* alter_table_clause ::= full_table_name SET TAG column_name NK_EQ literal */
{ yylhsminor.yy26 = createAlterTableSetTag(pCxt, yymsp[-5].minor.yy26, &yymsp[-2].minor.yy353, yymsp[0].minor.yy26); }
  yymsp[-5].minor.yy26 = yylhsminor.yy26;
        break;
      case 102: /* multi_create_clause ::= create_subtable_clause */
      case 105: /* multi_drop_clause ::= drop_table_clause */ yytestcase(yyruleno==105);
      case 112: /* column_def_list ::= column_def */ yytestcase(yyruleno==112);
      case 153: /* col_name_list ::= col_name */ yytestcase(yyruleno==153);
      case 156: /* func_name_list ::= func_name */ yytestcase(yyruleno==156);
      case 165: /* func_list ::= func */ yytestcase(yyruleno==165);
      case 260: /* select_sublist ::= select_item */ yytestcase(yyruleno==260);
      case 307: /* sort_specification_list ::= sort_specification */ yytestcase(yyruleno==307);
{ yylhsminor.yy64 = createNodeList(pCxt, yymsp[0].minor.yy26); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 103: /* multi_create_clause ::= multi_create_clause create_subtable_clause */
      case 106: /* multi_drop_clause ::= multi_drop_clause drop_table_clause */ yytestcase(yyruleno==106);
{ yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-1].minor.yy64, yymsp[0].minor.yy26); }
  yymsp[-1].minor.yy64 = yylhsminor.yy64;
        break;
      case 104: /* create_subtable_clause ::= not_exists_opt full_table_name USING full_table_name specific_tags_opt TAGS NK_LP literal_list NK_RP */
{ yylhsminor.yy26 = createCreateSubTableClause(pCxt, yymsp[-8].minor.yy107, yymsp[-7].minor.yy26, yymsp[-5].minor.yy26, yymsp[-4].minor.yy64, yymsp[-1].minor.yy64); }
  yymsp[-8].minor.yy26 = yylhsminor.yy26;
        break;
      case 107: /* drop_table_clause ::= exists_opt full_table_name */
{ yylhsminor.yy26 = createDropTableClause(pCxt, yymsp[-1].minor.yy107, yymsp[0].minor.yy26); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 108: /* specific_tags_opt ::= */
      case 139: /* tags_def_opt ::= */ yytestcase(yyruleno==139);
      case 268: /* partition_by_clause_opt ::= */ yytestcase(yyruleno==268);
      case 285: /* group_by_clause_opt ::= */ yytestcase(yyruleno==285);
      case 295: /* order_by_clause_opt ::= */ yytestcase(yyruleno==295);
{ yymsp[1].minor.yy64 = NULL; }
        break;
      case 109: /* specific_tags_opt ::= NK_LP col_name_list NK_RP */
{ yymsp[-2].minor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 110: /* full_table_name ::= table_name */
{ yylhsminor.yy26 = createRealTableNode(pCxt, NULL, &yymsp[0].minor.yy353, NULL); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 111: /* full_table_name ::= db_name NK_DOT table_name */
{ yylhsminor.yy26 = createRealTableNode(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy353, NULL); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 113: /* column_def_list ::= column_def_list NK_COMMA column_def */
      case 154: /* col_name_list ::= col_name_list NK_COMMA col_name */ yytestcase(yyruleno==154);
      case 157: /* func_name_list ::= func_name_list NK_COMMA col_name */ yytestcase(yyruleno==157);
      case 166: /* func_list ::= func_list NK_COMMA func */ yytestcase(yyruleno==166);
      case 261: /* select_sublist ::= select_sublist NK_COMMA select_item */ yytestcase(yyruleno==261);
      case 308: /* sort_specification_list ::= sort_specification_list NK_COMMA sort_specification */ yytestcase(yyruleno==308);
{ yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, yymsp[0].minor.yy26); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 114: /* column_def ::= column_name type_name */
{ yylhsminor.yy26 = createColumnDefNode(pCxt, &yymsp[-1].minor.yy353, yymsp[0].minor.yy370, NULL); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 115: /* column_def ::= column_name type_name COMMENT NK_STRING */
{ yylhsminor.yy26 = createColumnDefNode(pCxt, &yymsp[-3].minor.yy353, yymsp[-2].minor.yy370, &yymsp[0].minor.yy0); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 116: /* type_name ::= BOOL */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_BOOL); }
        break;
      case 117: /* type_name ::= TINYINT */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_TINYINT); }
        break;
      case 118: /* type_name ::= SMALLINT */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_SMALLINT); }
        break;
      case 119: /* type_name ::= INT */
      case 120: /* type_name ::= INTEGER */ yytestcase(yyruleno==120);
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_INT); }
        break;
      case 121: /* type_name ::= BIGINT */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_BIGINT); }
        break;
      case 122: /* type_name ::= FLOAT */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_FLOAT); }
        break;
      case 123: /* type_name ::= DOUBLE */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_DOUBLE); }
        break;
      case 124: /* type_name ::= BINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy370 = createVarLenDataType(TSDB_DATA_TYPE_BINARY, &yymsp[-1].minor.yy0); }
        break;
      case 125: /* type_name ::= TIMESTAMP */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_TIMESTAMP); }
        break;
      case 126: /* type_name ::= NCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy370 = createVarLenDataType(TSDB_DATA_TYPE_NCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 127: /* type_name ::= TINYINT UNSIGNED */
{ yymsp[-1].minor.yy370 = createDataType(TSDB_DATA_TYPE_UTINYINT); }
        break;
      case 128: /* type_name ::= SMALLINT UNSIGNED */
{ yymsp[-1].minor.yy370 = createDataType(TSDB_DATA_TYPE_USMALLINT); }
        break;
      case 129: /* type_name ::= INT UNSIGNED */
{ yymsp[-1].minor.yy370 = createDataType(TSDB_DATA_TYPE_UINT); }
        break;
      case 130: /* type_name ::= BIGINT UNSIGNED */
{ yymsp[-1].minor.yy370 = createDataType(TSDB_DATA_TYPE_UBIGINT); }
        break;
      case 131: /* type_name ::= JSON */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_JSON); }
        break;
      case 132: /* type_name ::= VARCHAR NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy370 = createVarLenDataType(TSDB_DATA_TYPE_VARCHAR, &yymsp[-1].minor.yy0); }
        break;
      case 133: /* type_name ::= MEDIUMBLOB */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_MEDIUMBLOB); }
        break;
      case 134: /* type_name ::= BLOB */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_BLOB); }
        break;
      case 135: /* type_name ::= VARBINARY NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy370 = createVarLenDataType(TSDB_DATA_TYPE_VARBINARY, &yymsp[-1].minor.yy0); }
        break;
      case 136: /* type_name ::= DECIMAL */
{ yymsp[0].minor.yy370 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 137: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_RP */
{ yymsp[-3].minor.yy370 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 138: /* type_name ::= DECIMAL NK_LP NK_INTEGER NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy370 = createDataType(TSDB_DATA_TYPE_DECIMAL); }
        break;
      case 140: /* tags_def_opt ::= tags_def */
      case 259: /* select_list ::= select_sublist */ yytestcase(yyruleno==259);
{ yylhsminor.yy64 = yymsp[0].minor.yy64; }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 141: /* tags_def ::= TAGS NK_LP column_def_list NK_RP */
{ yymsp[-3].minor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 142: /* table_options ::= */
{ yymsp[1].minor.yy26 = createDefaultTableOptions(pCxt); }
        break;
      case 143: /* table_options ::= table_options COMMENT NK_STRING */
{ yylhsminor.yy26 = setTableOption(pCxt, yymsp[-2].minor.yy26, TABLE_OPTION_COMMENT, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 144: /* table_options ::= table_options KEEP NK_INTEGER */
{ yylhsminor.yy26 = setTableOption(pCxt, yymsp[-2].minor.yy26, TABLE_OPTION_KEEP, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 145: /* table_options ::= table_options TTL NK_INTEGER */
{ yylhsminor.yy26 = setTableOption(pCxt, yymsp[-2].minor.yy26, TABLE_OPTION_TTL, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 146: /* table_options ::= table_options SMA NK_LP col_name_list NK_RP */
{ yylhsminor.yy26 = setTableSmaOption(pCxt, yymsp[-4].minor.yy26, yymsp[-1].minor.yy64); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 147: /* table_options ::= table_options ROLLUP NK_LP func_name_list NK_RP */
{ yylhsminor.yy26 = setTableRollupOption(pCxt, yymsp[-4].minor.yy26, yymsp[-1].minor.yy64); }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 148: /* alter_table_options ::= alter_table_option */
{ yylhsminor.yy26 = createDefaultAlterTableOptions(pCxt); yylhsminor.yy26 = setTableOption(pCxt, yylhsminor.yy26, yymsp[0].minor.yy443.type, &yymsp[0].minor.yy443.val); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 149: /* alter_table_options ::= alter_table_options alter_table_option */
{ yylhsminor.yy26 = setTableOption(pCxt, yymsp[-1].minor.yy26, yymsp[0].minor.yy443.type, &yymsp[0].minor.yy443.val); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 150: /* alter_table_option ::= COMMENT NK_STRING */
{ yymsp[-1].minor.yy443.type = TABLE_OPTION_COMMENT; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 151: /* alter_table_option ::= KEEP NK_INTEGER */
{ yymsp[-1].minor.yy443.type = TABLE_OPTION_KEEP; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 152: /* alter_table_option ::= TTL NK_INTEGER */
{ yymsp[-1].minor.yy443.type = TABLE_OPTION_TTL; yymsp[-1].minor.yy443.val = yymsp[0].minor.yy0; }
        break;
      case 155: /* col_name ::= column_name */
{ yylhsminor.yy26 = createColumnNode(pCxt, NULL, &yymsp[0].minor.yy353); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 158: /* func_name ::= function_name */
{ yylhsminor.yy26 = createFunctionNode(pCxt, &yymsp[0].minor.yy353, NULL); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 159: /* cmd ::= CREATE SMA INDEX index_name ON table_name index_options */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_SMA, &yymsp[-3].minor.yy353, &yymsp[-1].minor.yy353, NULL, yymsp[0].minor.yy26); }
        break;
      case 160: /* cmd ::= CREATE FULLTEXT INDEX index_name ON table_name NK_LP col_name_list NK_RP */
{ pCxt->pRootNode = createCreateIndexStmt(pCxt, INDEX_TYPE_FULLTEXT, &yymsp[-5].minor.yy353, &yymsp[-3].minor.yy353, yymsp[-1].minor.yy64, NULL); }
        break;
      case 161: /* cmd ::= DROP INDEX index_name ON table_name */
{ pCxt->pRootNode = createDropIndexStmt(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy353); }
        break;
      case 162: /* index_options ::= */
      case 266: /* where_clause_opt ::= */ yytestcase(yyruleno==266);
      case 270: /* twindow_clause_opt ::= */ yytestcase(yyruleno==270);
      case 275: /* sliding_opt ::= */ yytestcase(yyruleno==275);
      case 277: /* fill_opt ::= */ yytestcase(yyruleno==277);
      case 289: /* having_clause_opt ::= */ yytestcase(yyruleno==289);
      case 297: /* slimit_clause_opt ::= */ yytestcase(yyruleno==297);
      case 301: /* limit_clause_opt ::= */ yytestcase(yyruleno==301);
{ yymsp[1].minor.yy26 = NULL; }
        break;
      case 163: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_RP sliding_opt */
{ yymsp[-8].minor.yy26 = createIndexOption(pCxt, yymsp[-6].minor.yy64, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), NULL, yymsp[0].minor.yy26); }
        break;
      case 164: /* index_options ::= FUNCTION NK_LP func_list NK_RP INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt */
{ yymsp[-10].minor.yy26 = createIndexOption(pCxt, yymsp[-8].minor.yy64, releaseRawExprNode(pCxt, yymsp[-4].minor.yy26), releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), yymsp[0].minor.yy26); }
        break;
      case 167: /* func ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy26 = createFunctionNode(pCxt, &yymsp[-3].minor.yy353, yymsp[-1].minor.yy64); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 168: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS query_expression */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy107, &yymsp[-2].minor.yy353, yymsp[0].minor.yy26, NULL); }
        break;
      case 169: /* cmd ::= CREATE TOPIC not_exists_opt topic_name AS db_name */
{ pCxt->pRootNode = createCreateTopicStmt(pCxt, yymsp[-3].minor.yy107, &yymsp[-2].minor.yy353, NULL, &yymsp[0].minor.yy353); }
        break;
      case 170: /* cmd ::= DROP TOPIC exists_opt topic_name */
{ pCxt->pRootNode = createDropTopicStmt(pCxt, yymsp[-1].minor.yy107, &yymsp[0].minor.yy353); }
        break;
      case 171: /* cmd ::= SHOW VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, NULL); }
        break;
      case 172: /* cmd ::= SHOW db_name NK_DOT VGROUPS */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_VGROUPS_STMT, &yymsp[-2].minor.yy353); }
        break;
      case 173: /* cmd ::= SHOW MNODES */
{ pCxt->pRootNode = createShowStmt(pCxt, QUERY_NODE_SHOW_MNODES_STMT, NULL); }
        break;
      case 175: /* literal ::= NK_INTEGER */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BIGINT, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 176: /* literal ::= NK_FLOAT */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_DOUBLE, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 177: /* literal ::= NK_STRING */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BINARY, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 178: /* literal ::= NK_BOOL */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_BOOL, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 179: /* literal ::= TIMESTAMP NK_STRING */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0, createValueNode(pCxt, TSDB_DATA_TYPE_TIMESTAMP, &yymsp[0].minor.yy0)); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 180: /* literal ::= duration_literal */
      case 193: /* expression ::= literal */ yytestcase(yyruleno==193);
      case 194: /* expression ::= column_reference */ yytestcase(yyruleno==194);
      case 197: /* expression ::= subquery */ yytestcase(yyruleno==197);
      case 229: /* boolean_value_expression ::= boolean_primary */ yytestcase(yyruleno==229);
      case 233: /* boolean_primary ::= predicate */ yytestcase(yyruleno==233);
      case 235: /* common_expression ::= expression */ yytestcase(yyruleno==235);
      case 236: /* common_expression ::= boolean_value_expression */ yytestcase(yyruleno==236);
      case 238: /* table_reference_list ::= table_reference */ yytestcase(yyruleno==238);
      case 240: /* table_reference ::= table_primary */ yytestcase(yyruleno==240);
      case 241: /* table_reference ::= joined_table */ yytestcase(yyruleno==241);
      case 245: /* table_primary ::= parenthesized_joined_table */ yytestcase(yyruleno==245);
      case 292: /* query_expression_body ::= query_primary */ yytestcase(yyruleno==292);
      case 294: /* query_primary ::= query_specification */ yytestcase(yyruleno==294);
{ yylhsminor.yy26 = yymsp[0].minor.yy26; }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 181: /* duration_literal ::= NK_VARIABLE */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy0, createDurationValueNode(pCxt, &yymsp[0].minor.yy0)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 182: /* literal_list ::= literal */
      case 206: /* expression_list ::= expression */ yytestcase(yyruleno==206);
{ yylhsminor.yy64 = createNodeList(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy26)); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 183: /* literal_list ::= literal_list NK_COMMA literal */
      case 207: /* expression_list ::= expression_list NK_COMMA expression */ yytestcase(yyruleno==207);
{ yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, releaseRawExprNode(pCxt, yymsp[0].minor.yy26)); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 195: /* expression ::= function_name NK_LP expression_list NK_RP */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy353, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy353, yymsp[-1].minor.yy64)); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 196: /* expression ::= function_name NK_LP NK_STAR NK_RP */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-3].minor.yy353, &yymsp[0].minor.yy0, createFunctionNode(pCxt, &yymsp[-3].minor.yy353, createNodeList(pCxt, createColumnNode(pCxt, NULL, &yymsp[-1].minor.yy0)))); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 198: /* expression ::= NK_LP expression NK_RP */
      case 234: /* boolean_primary ::= NK_LP boolean_value_expression NK_RP */ yytestcase(yyruleno==234);
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, releaseRawExprNode(pCxt, yymsp[-1].minor.yy26)); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 199: /* expression ::= NK_PLUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, releaseRawExprNode(pCxt, yymsp[0].minor.yy26));
                                                                                  }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 200: /* expression ::= NK_MINUS expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &t, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[0].minor.yy26), NULL));
                                                                                  }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 201: /* expression ::= expression NK_PLUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_ADD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); 
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 202: /* expression ::= expression NK_MINUS expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_SUB, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); 
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 203: /* expression ::= expression NK_STAR expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MULTI, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); 
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 204: /* expression ::= expression NK_SLASH expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_DIV, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); 
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 205: /* expression ::= expression NK_REM expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, OP_TYPE_MOD, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); 
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 208: /* column_reference ::= column_name */
{ yylhsminor.yy26 = createRawExprNode(pCxt, &yymsp[0].minor.yy353, createColumnNode(pCxt, NULL, &yymsp[0].minor.yy353)); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 209: /* column_reference ::= table_name NK_DOT column_name */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy353, createColumnNode(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy353)); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 210: /* predicate ::= expression compare_op expression */
      case 215: /* predicate ::= expression in_op in_predicate_value */ yytestcase(yyruleno==215);
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createOperatorNode(pCxt, yymsp[-1].minor.yy80, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26)));
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 211: /* predicate ::= expression BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-4].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-4].minor.yy26), releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26)));
                                                                                  }
  yymsp[-4].minor.yy26 = yylhsminor.yy26;
        break;
      case 212: /* predicate ::= expression NOT BETWEEN expression AND expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-5].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createNotBetweenAnd(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[-5].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26)));
                                                                                  }
  yymsp[-5].minor.yy26 = yylhsminor.yy26;
        break;
      case 213: /* predicate ::= expression IS NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NULL, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), NULL));
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 214: /* predicate ::= expression IS NOT NULL */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-3].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &yymsp[0].minor.yy0, createOperatorNode(pCxt, OP_TYPE_IS_NOT_NULL, releaseRawExprNode(pCxt, yymsp[-3].minor.yy26), NULL));
                                                                                  }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 216: /* compare_op ::= NK_LT */
{ yymsp[0].minor.yy80 = OP_TYPE_LOWER_THAN; }
        break;
      case 217: /* compare_op ::= NK_GT */
{ yymsp[0].minor.yy80 = OP_TYPE_GREATER_THAN; }
        break;
      case 218: /* compare_op ::= NK_LE */
{ yymsp[0].minor.yy80 = OP_TYPE_LOWER_EQUAL; }
        break;
      case 219: /* compare_op ::= NK_GE */
{ yymsp[0].minor.yy80 = OP_TYPE_GREATER_EQUAL; }
        break;
      case 220: /* compare_op ::= NK_NE */
{ yymsp[0].minor.yy80 = OP_TYPE_NOT_EQUAL; }
        break;
      case 221: /* compare_op ::= NK_EQ */
{ yymsp[0].minor.yy80 = OP_TYPE_EQUAL; }
        break;
      case 222: /* compare_op ::= LIKE */
{ yymsp[0].minor.yy80 = OP_TYPE_LIKE; }
        break;
      case 223: /* compare_op ::= NOT LIKE */
{ yymsp[-1].minor.yy80 = OP_TYPE_NOT_LIKE; }
        break;
      case 224: /* compare_op ::= MATCH */
{ yymsp[0].minor.yy80 = OP_TYPE_MATCH; }
        break;
      case 225: /* compare_op ::= NMATCH */
{ yymsp[0].minor.yy80 = OP_TYPE_NMATCH; }
        break;
      case 226: /* in_op ::= IN */
{ yymsp[0].minor.yy80 = OP_TYPE_IN; }
        break;
      case 227: /* in_op ::= NOT IN */
{ yymsp[-1].minor.yy80 = OP_TYPE_NOT_IN; }
        break;
      case 228: /* in_predicate_value ::= NK_LP expression_list NK_RP */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, createNodeListNode(pCxt, yymsp[-1].minor.yy64)); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 230: /* boolean_value_expression ::= NOT boolean_primary */
{
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-1].minor.yy0, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_NOT, releaseRawExprNode(pCxt, yymsp[0].minor.yy26), NULL));
                                                                                  }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 231: /* boolean_value_expression ::= boolean_value_expression OR boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26)));
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 232: /* boolean_value_expression ::= boolean_value_expression AND boolean_value_expression */
{
                                                                                    SToken s = getTokenFromRawExprNode(pCxt, yymsp[-2].minor.yy26);
                                                                                    SToken e = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = createRawExprNodeExt(pCxt, &s, &e, createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), releaseRawExprNode(pCxt, yymsp[0].minor.yy26)));
                                                                                  }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 237: /* from_clause ::= FROM table_reference_list */
      case 267: /* where_clause_opt ::= WHERE search_condition */ yytestcase(yyruleno==267);
      case 290: /* having_clause_opt ::= HAVING search_condition */ yytestcase(yyruleno==290);
{ yymsp[-1].minor.yy26 = yymsp[0].minor.yy26; }
        break;
      case 239: /* table_reference_list ::= table_reference_list NK_COMMA table_reference */
{ yylhsminor.yy26 = createJoinTableNode(pCxt, JOIN_TYPE_INNER, yymsp[-2].minor.yy26, yymsp[0].minor.yy26, NULL); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 242: /* table_primary ::= table_name alias_opt */
{ yylhsminor.yy26 = createRealTableNode(pCxt, NULL, &yymsp[-1].minor.yy353, &yymsp[0].minor.yy353); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 243: /* table_primary ::= db_name NK_DOT table_name alias_opt */
{ yylhsminor.yy26 = createRealTableNode(pCxt, &yymsp[-3].minor.yy353, &yymsp[-1].minor.yy353, &yymsp[0].minor.yy353); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 244: /* table_primary ::= subquery alias_opt */
{ yylhsminor.yy26 = createTempTableNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy26), &yymsp[0].minor.yy353); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 246: /* alias_opt ::= */
{ yymsp[1].minor.yy353 = nil_token;  }
        break;
      case 247: /* alias_opt ::= table_alias */
{ yylhsminor.yy353 = yymsp[0].minor.yy353; }
  yymsp[0].minor.yy353 = yylhsminor.yy353;
        break;
      case 248: /* alias_opt ::= AS table_alias */
{ yymsp[-1].minor.yy353 = yymsp[0].minor.yy353; }
        break;
      case 249: /* parenthesized_joined_table ::= NK_LP joined_table NK_RP */
      case 250: /* parenthesized_joined_table ::= NK_LP parenthesized_joined_table NK_RP */ yytestcase(yyruleno==250);
{ yymsp[-2].minor.yy26 = yymsp[-1].minor.yy26; }
        break;
      case 251: /* joined_table ::= table_reference join_type JOIN table_reference ON search_condition */
{ yylhsminor.yy26 = createJoinTableNode(pCxt, yymsp[-4].minor.yy372, yymsp[-5].minor.yy26, yymsp[-2].minor.yy26, yymsp[0].minor.yy26); }
  yymsp[-5].minor.yy26 = yylhsminor.yy26;
        break;
      case 252: /* join_type ::= */
{ yymsp[1].minor.yy372 = JOIN_TYPE_INNER; }
        break;
      case 253: /* join_type ::= INNER */
{ yymsp[0].minor.yy372 = JOIN_TYPE_INNER; }
        break;
      case 254: /* query_specification ::= SELECT set_quantifier_opt select_list from_clause where_clause_opt partition_by_clause_opt twindow_clause_opt group_by_clause_opt having_clause_opt */
{ 
                                                                                    yymsp[-8].minor.yy26 = createSelectStmt(pCxt, yymsp[-7].minor.yy107, yymsp[-6].minor.yy64, yymsp[-5].minor.yy26);
                                                                                    yymsp[-8].minor.yy26 = addWhereClause(pCxt, yymsp[-8].minor.yy26, yymsp[-4].minor.yy26);
                                                                                    yymsp[-8].minor.yy26 = addPartitionByClause(pCxt, yymsp[-8].minor.yy26, yymsp[-3].minor.yy64);
                                                                                    yymsp[-8].minor.yy26 = addWindowClauseClause(pCxt, yymsp[-8].minor.yy26, yymsp[-2].minor.yy26);
                                                                                    yymsp[-8].minor.yy26 = addGroupByClause(pCxt, yymsp[-8].minor.yy26, yymsp[-1].minor.yy64);
                                                                                    yymsp[-8].minor.yy26 = addHavingClause(pCxt, yymsp[-8].minor.yy26, yymsp[0].minor.yy26);
                                                                                  }
        break;
      case 256: /* set_quantifier_opt ::= DISTINCT */
{ yymsp[0].minor.yy107 = true; }
        break;
      case 257: /* set_quantifier_opt ::= ALL */
{ yymsp[0].minor.yy107 = false; }
        break;
      case 258: /* select_list ::= NK_STAR */
{ yymsp[0].minor.yy64 = NULL; }
        break;
      case 262: /* select_item ::= common_expression */
{
                                                                                    SToken t = getTokenFromRawExprNode(pCxt, yymsp[0].minor.yy26);
                                                                                    yylhsminor.yy26 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy26), &t);
                                                                                  }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 263: /* select_item ::= common_expression column_alias */
{ yylhsminor.yy26 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy26), &yymsp[0].minor.yy353); }
  yymsp[-1].minor.yy26 = yylhsminor.yy26;
        break;
      case 264: /* select_item ::= common_expression AS column_alias */
{ yylhsminor.yy26 = setProjectionAlias(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), &yymsp[0].minor.yy353); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 265: /* select_item ::= table_name NK_DOT NK_STAR */
{ yylhsminor.yy26 = createColumnNode(pCxt, &yymsp[-2].minor.yy353, &yymsp[0].minor.yy0); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 269: /* partition_by_clause_opt ::= PARTITION BY expression_list */
      case 286: /* group_by_clause_opt ::= GROUP BY group_by_list */ yytestcase(yyruleno==286);
      case 296: /* order_by_clause_opt ::= ORDER BY sort_specification_list */ yytestcase(yyruleno==296);
{ yymsp[-2].minor.yy64 = yymsp[0].minor.yy64; }
        break;
      case 271: /* twindow_clause_opt ::= SESSION NK_LP column_reference NK_COMMA NK_INTEGER NK_RP */
{ yymsp[-5].minor.yy26 = createSessionWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy26), &yymsp[-1].minor.yy0); }
        break;
      case 272: /* twindow_clause_opt ::= STATE_WINDOW NK_LP column_reference NK_RP */
{ yymsp[-3].minor.yy26 = createStateWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-1].minor.yy26)); }
        break;
      case 273: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-5].minor.yy26 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-3].minor.yy26), NULL, yymsp[-1].minor.yy26, yymsp[0].minor.yy26); }
        break;
      case 274: /* twindow_clause_opt ::= INTERVAL NK_LP duration_literal NK_COMMA duration_literal NK_RP sliding_opt fill_opt */
{ yymsp[-7].minor.yy26 = createIntervalWindowNode(pCxt, releaseRawExprNode(pCxt, yymsp[-5].minor.yy26), releaseRawExprNode(pCxt, yymsp[-3].minor.yy26), yymsp[-1].minor.yy26, yymsp[0].minor.yy26); }
        break;
      case 276: /* sliding_opt ::= SLIDING NK_LP duration_literal NK_RP */
{ yymsp[-3].minor.yy26 = releaseRawExprNode(pCxt, yymsp[-1].minor.yy26); }
        break;
      case 278: /* fill_opt ::= FILL NK_LP fill_mode NK_RP */
{ yymsp[-3].minor.yy26 = createFillNode(pCxt, yymsp[-1].minor.yy192, NULL); }
        break;
      case 279: /* fill_opt ::= FILL NK_LP VALUE NK_COMMA literal_list NK_RP */
{ yymsp[-5].minor.yy26 = createFillNode(pCxt, FILL_MODE_VALUE, createNodeListNode(pCxt, yymsp[-1].minor.yy64)); }
        break;
      case 280: /* fill_mode ::= NONE */
{ yymsp[0].minor.yy192 = FILL_MODE_NONE; }
        break;
      case 281: /* fill_mode ::= PREV */
{ yymsp[0].minor.yy192 = FILL_MODE_PREV; }
        break;
      case 282: /* fill_mode ::= NULL */
{ yymsp[0].minor.yy192 = FILL_MODE_NULL; }
        break;
      case 283: /* fill_mode ::= LINEAR */
{ yymsp[0].minor.yy192 = FILL_MODE_LINEAR; }
        break;
      case 284: /* fill_mode ::= NEXT */
{ yymsp[0].minor.yy192 = FILL_MODE_NEXT; }
        break;
      case 287: /* group_by_list ::= expression */
{ yylhsminor.yy64 = createNodeList(pCxt, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); }
  yymsp[0].minor.yy64 = yylhsminor.yy64;
        break;
      case 288: /* group_by_list ::= group_by_list NK_COMMA expression */
{ yylhsminor.yy64 = addNodeToList(pCxt, yymsp[-2].minor.yy64, createGroupingSetNode(pCxt, releaseRawExprNode(pCxt, yymsp[0].minor.yy26))); }
  yymsp[-2].minor.yy64 = yylhsminor.yy64;
        break;
      case 291: /* query_expression ::= query_expression_body order_by_clause_opt slimit_clause_opt limit_clause_opt */
{ 
                                                                                    yylhsminor.yy26 = addOrderByClause(pCxt, yymsp[-3].minor.yy26, yymsp[-2].minor.yy64);
                                                                                    yylhsminor.yy26 = addSlimitClause(pCxt, yylhsminor.yy26, yymsp[-1].minor.yy26);
                                                                                    yylhsminor.yy26 = addLimitClause(pCxt, yylhsminor.yy26, yymsp[0].minor.yy26);
                                                                                  }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 293: /* query_expression_body ::= query_expression_body UNION ALL query_expression_body */
{ yylhsminor.yy26 = createSetOperator(pCxt, SET_OP_TYPE_UNION_ALL, yymsp[-3].minor.yy26, yymsp[0].minor.yy26); }
  yymsp[-3].minor.yy26 = yylhsminor.yy26;
        break;
      case 298: /* slimit_clause_opt ::= SLIMIT NK_INTEGER */
      case 302: /* limit_clause_opt ::= LIMIT NK_INTEGER */ yytestcase(yyruleno==302);
{ yymsp[-1].minor.yy26 = createLimitNode(pCxt, &yymsp[0].minor.yy0, NULL); }
        break;
      case 299: /* slimit_clause_opt ::= SLIMIT NK_INTEGER SOFFSET NK_INTEGER */
      case 303: /* limit_clause_opt ::= LIMIT NK_INTEGER OFFSET NK_INTEGER */ yytestcase(yyruleno==303);
{ yymsp[-3].minor.yy26 = createLimitNode(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0); }
        break;
      case 300: /* slimit_clause_opt ::= SLIMIT NK_INTEGER NK_COMMA NK_INTEGER */
      case 304: /* limit_clause_opt ::= LIMIT NK_INTEGER NK_COMMA NK_INTEGER */ yytestcase(yyruleno==304);
{ yymsp[-3].minor.yy26 = createLimitNode(pCxt, &yymsp[0].minor.yy0, &yymsp[-2].minor.yy0); }
        break;
      case 305: /* subquery ::= NK_LP query_expression NK_RP */
{ yylhsminor.yy26 = createRawExprNodeExt(pCxt, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, yymsp[-1].minor.yy26); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 306: /* search_condition ::= common_expression */
{ yylhsminor.yy26 = releaseRawExprNode(pCxt, yymsp[0].minor.yy26); }
  yymsp[0].minor.yy26 = yylhsminor.yy26;
        break;
      case 309: /* sort_specification ::= expression ordering_specification_opt null_ordering_opt */
{ yylhsminor.yy26 = createOrderByExprNode(pCxt, releaseRawExprNode(pCxt, yymsp[-2].minor.yy26), yymsp[-1].minor.yy32, yymsp[0].minor.yy391); }
  yymsp[-2].minor.yy26 = yylhsminor.yy26;
        break;
      case 310: /* ordering_specification_opt ::= */
{ yymsp[1].minor.yy32 = ORDER_ASC; }
        break;
      case 311: /* ordering_specification_opt ::= ASC */
{ yymsp[0].minor.yy32 = ORDER_ASC; }
        break;
      case 312: /* ordering_specification_opt ::= DESC */
{ yymsp[0].minor.yy32 = ORDER_DESC; }
        break;
      case 313: /* null_ordering_opt ::= */
{ yymsp[1].minor.yy391 = NULL_ORDER_DEFAULT; }
        break;
      case 314: /* null_ordering_opt ::= NULLS FIRST */
{ yymsp[-1].minor.yy391 = NULL_ORDER_FIRST; }
        break;
      case 315: /* null_ordering_opt ::= NULLS LAST */
{ yymsp[-1].minor.yy391 = NULL_ORDER_LAST; }
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

  if (pCxt->valid) {
    if(TOKEN.z) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_SYNTAX_ERROR, TOKEN.z);
    } else {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INCOMPLETE_SQL);
    }
    pCxt->valid = false;
  }
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
