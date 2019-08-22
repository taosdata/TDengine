/* ANSI-C code produced by gperf version 3.0.4 */
/* Command-line: gperf -m 10 lib/aliases_syssolaris.gperf  */
/* Computed positions: -k'1,3-11,$' */

#if !((' ' == 32) && ('!' == 33) && ('"' == 34) && ('#' == 35) \
      && ('%' == 37) && ('&' == 38) && ('\'' == 39) && ('(' == 40) \
      && (')' == 41) && ('*' == 42) && ('+' == 43) && (',' == 44) \
      && ('-' == 45) && ('.' == 46) && ('/' == 47) && ('0' == 48) \
      && ('1' == 49) && ('2' == 50) && ('3' == 51) && ('4' == 52) \
      && ('5' == 53) && ('6' == 54) && ('7' == 55) && ('8' == 56) \
      && ('9' == 57) && (':' == 58) && (';' == 59) && ('<' == 60) \
      && ('=' == 61) && ('>' == 62) && ('?' == 63) && ('A' == 65) \
      && ('B' == 66) && ('C' == 67) && ('D' == 68) && ('E' == 69) \
      && ('F' == 70) && ('G' == 71) && ('H' == 72) && ('I' == 73) \
      && ('J' == 74) && ('K' == 75) && ('L' == 76) && ('M' == 77) \
      && ('N' == 78) && ('O' == 79) && ('P' == 80) && ('Q' == 81) \
      && ('R' == 82) && ('S' == 83) && ('T' == 84) && ('U' == 85) \
      && ('V' == 86) && ('W' == 87) && ('X' == 88) && ('Y' == 89) \
      && ('Z' == 90) && ('[' == 91) && ('\\' == 92) && (']' == 93) \
      && ('^' == 94) && ('_' == 95) && ('a' == 97) && ('b' == 98) \
      && ('c' == 99) && ('d' == 100) && ('e' == 101) && ('f' == 102) \
      && ('g' == 103) && ('h' == 104) && ('i' == 105) && ('j' == 106) \
      && ('k' == 107) && ('l' == 108) && ('m' == 109) && ('n' == 110) \
      && ('o' == 111) && ('p' == 112) && ('q' == 113) && ('r' == 114) \
      && ('s' == 115) && ('t' == 116) && ('u' == 117) && ('v' == 118) \
      && ('w' == 119) && ('x' == 120) && ('y' == 121) && ('z' == 122) \
      && ('{' == 123) && ('|' == 124) && ('}' == 125) && ('~' == 126))
/* The character set is not based on ISO-646.  */
#error "gperf generated tables don't work with this execution character set. Please report a bug to <bug-gnu-gperf@gnu.org>."
#endif

#line 1 "lib/aliases_syssolaris.gperf"
struct alias { int name; unsigned int encoding_index; };

#define TOTAL_KEYWORDS 353
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 45
#define MIN_HASH_VALUE 8
#define MAX_HASH_VALUE 1003
/* maximum key range = 996, duplicates = 0 */

#ifdef __GNUC__
__inline
#else
#ifdef __cplusplus
inline
#endif
#endif
static unsigned int
aliases_hash (register const char *str, register unsigned int len)
{
  static const unsigned short asso_values[] =
    {
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004,    2,  112, 1004,   27,    4,
        34,   57,   16,   22,   11,  168,    3,   10,  254, 1004,
      1004, 1004, 1004, 1004, 1004,   21,  126,    7,   10,   37,
        40,  119,   81,   62,  332,  197,    9,  169,    4,    2,
         8, 1004,    3,   34,  104,  205,  191,  192,  195,   36,
        16, 1004, 1004, 1004, 1004,    3, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004,
      1004, 1004, 1004, 1004, 1004, 1004, 1004, 1004
    };
  register int hval = len;

  switch (hval)
    {
      default:
        hval += asso_values[(unsigned char)str[10]];
      /*FALLTHROUGH*/
      case 10:
        hval += asso_values[(unsigned char)str[9]];
      /*FALLTHROUGH*/
      case 9:
        hval += asso_values[(unsigned char)str[8]];
      /*FALLTHROUGH*/
      case 8:
        hval += asso_values[(unsigned char)str[7]];
      /*FALLTHROUGH*/
      case 7:
        hval += asso_values[(unsigned char)str[6]];
      /*FALLTHROUGH*/
      case 6:
        hval += asso_values[(unsigned char)str[5]];
      /*FALLTHROUGH*/
      case 5:
        hval += asso_values[(unsigned char)str[4]];
      /*FALLTHROUGH*/
      case 4:
        hval += asso_values[(unsigned char)str[3]];
      /*FALLTHROUGH*/
      case 3:
        hval += asso_values[(unsigned char)str[2]];
      /*FALLTHROUGH*/
      case 2:
      case 1:
        hval += asso_values[(unsigned char)str[0]];
        break;
    }
  return hval + asso_values[(unsigned char)str[len - 1]];
}

struct stringpool_t
  {
    char stringpool_str8[sizeof("R8")];
    char stringpool_str13[sizeof("CN")];
    char stringpool_str14[sizeof("L8")];
    char stringpool_str15[sizeof("L1")];
    char stringpool_str22[sizeof("L6")];
    char stringpool_str27[sizeof("L4")];
    char stringpool_str28[sizeof("866")];
    char stringpool_str30[sizeof("C99")];
    char stringpool_str33[sizeof("L5")];
    char stringpool_str36[sizeof("646")];
    char stringpool_str38[sizeof("CHAR")];
    char stringpool_str39[sizeof("CP819")];
    char stringpool_str45[sizeof("L2")];
    char stringpool_str48[sizeof("CP866")];
    char stringpool_str58[sizeof("CP949")];
    char stringpool_str60[sizeof("850")];
    char stringpool_str61[sizeof("5601")];
    char stringpool_str62[sizeof("RK1048")];
    char stringpool_str64[sizeof("EUCCN")];
    char stringpool_str66[sizeof("L10")];
    char stringpool_str67[sizeof("EUC-CN")];
    char stringpool_str68[sizeof("L3")];
    char stringpool_str70[sizeof("CP154")];
    char stringpool_str71[sizeof("PT154")];
    char stringpool_str74[sizeof("862")];
    char stringpool_str79[sizeof("CP1258")];
    char stringpool_str81[sizeof("CP1251")];
    char stringpool_str86[sizeof("CP1131")];
    char stringpool_str88[sizeof("PTCP154")];
    char stringpool_str91[sizeof("CP850")];
    char stringpool_str93[sizeof("CP1361")];
    char stringpool_str94[sizeof("CP862")];
    char stringpool_str95[sizeof("CP1256")];
    char stringpool_str98[sizeof("CP950")];
    char stringpool_str99[sizeof("HZ")];
    char stringpool_str101[sizeof("CP936")];
    char stringpool_str105[sizeof("CP1254")];
    char stringpool_str117[sizeof("CP1255")];
    char stringpool_str119[sizeof("ISO8859-8")];
    char stringpool_str121[sizeof("ISO8859-1")];
    char stringpool_str122[sizeof("ISO-8859-8")];
    char stringpool_str123[sizeof("ISO_8859-8")];
    char stringpool_str124[sizeof("ISO-8859-1")];
    char stringpool_str125[sizeof("ISO_8859-1")];
    char stringpool_str126[sizeof("ISO8859-11")];
    char stringpool_str127[sizeof("CP1250")];
    char stringpool_str128[sizeof("ISO646-CN")];
    char stringpool_str129[sizeof("ISO-8859-11")];
    char stringpool_str130[sizeof("ISO_8859-11")];
    char stringpool_str133[sizeof("ISO8859-9")];
    char stringpool_str135[sizeof("ISO8859-6")];
    char stringpool_str136[sizeof("ISO-8859-9")];
    char stringpool_str137[sizeof("ISO_8859-9")];
    char stringpool_str138[sizeof("ISO-8859-6")];
    char stringpool_str139[sizeof("ISO_8859-6")];
    char stringpool_str140[sizeof("ISO8859-16")];
    char stringpool_str141[sizeof("CP1252")];
    char stringpool_str142[sizeof("ISO_8859-16:2001")];
    char stringpool_str143[sizeof("ISO-8859-16")];
    char stringpool_str144[sizeof("ISO_8859-16")];
    char stringpool_str145[sizeof("ISO8859-4")];
    char stringpool_str146[sizeof("ISO_8859-14:1998")];
    char stringpool_str147[sizeof("CP932")];
    char stringpool_str148[sizeof("ISO-8859-4")];
    char stringpool_str149[sizeof("ISO_8859-4")];
    char stringpool_str150[sizeof("ISO8859-14")];
    char stringpool_str152[sizeof("ISO_8859-15:1998")];
    char stringpool_str153[sizeof("ISO-8859-14")];
    char stringpool_str154[sizeof("ISO_8859-14")];
    char stringpool_str157[sizeof("ISO8859-5")];
    char stringpool_str160[sizeof("ISO-8859-5")];
    char stringpool_str161[sizeof("ISO_8859-5")];
    char stringpool_str162[sizeof("ISO8859-15")];
    char stringpool_str163[sizeof("ISO-IR-6")];
    char stringpool_str165[sizeof("ISO-8859-15")];
    char stringpool_str166[sizeof("ISO_8859-15")];
    char stringpool_str168[sizeof("SJIS")];
    char stringpool_str169[sizeof("ISO-IR-148")];
    char stringpool_str170[sizeof("ISO-IR-58")];
    char stringpool_str172[sizeof("ISO8859-10")];
    char stringpool_str174[sizeof("CYRILLIC")];
    char stringpool_str175[sizeof("ISO-8859-10")];
    char stringpool_str176[sizeof("ISO_8859-10")];
    char stringpool_str177[sizeof("ISO-IR-199")];
    char stringpool_str178[sizeof("ISO-IR-14")];
    char stringpool_str179[sizeof("L7")];
    char stringpool_str180[sizeof("ISO-IR-166")];
    char stringpool_str181[sizeof("ISO8859-2")];
    char stringpool_str182[sizeof("ISO-IR-101")];
    char stringpool_str183[sizeof("ISO-IR-149")];
    char stringpool_str184[sizeof("ISO-8859-2")];
    char stringpool_str185[sizeof("ISO_8859-2")];
    char stringpool_str186[sizeof("MAC")];
    char stringpool_str187[sizeof("CP1253")];
    char stringpool_str188[sizeof("ISO_8859-10:1992")];
    char stringpool_str189[sizeof("ISO-IR-159")];
    char stringpool_str191[sizeof("LATIN8")];
    char stringpool_str192[sizeof("CP1133")];
    char stringpool_str193[sizeof("LATIN1")];
    char stringpool_str194[sizeof("ISO-IR-109")];
    char stringpool_str195[sizeof("ISO-IR-144")];
    char stringpool_str196[sizeof("ANSI-1251")];
    char stringpool_str198[sizeof("CNS11643")];
    char stringpool_str201[sizeof("CSPTCP154")];
    char stringpool_str202[sizeof("ISO-IR-165")];
    char stringpool_str203[sizeof("ISO-IR-126")];
    char stringpool_str204[sizeof("ELOT_928")];
    char stringpool_str205[sizeof("ISO-IR-110")];
    char stringpool_str207[sizeof("LATIN6")];
    char stringpool_str208[sizeof("LATIN-9")];
    char stringpool_str209[sizeof("ROMAN8")];
    char stringpool_str210[sizeof("ISO-IR-138")];
    char stringpool_str211[sizeof("GB_1988-80")];
    char stringpool_str215[sizeof("CP874")];
    char stringpool_str217[sizeof("LATIN4")];
    char stringpool_str219[sizeof("ASCII")];
    char stringpool_str222[sizeof("UHC")];
    char stringpool_str223[sizeof("ISO-2022-CN")];
    char stringpool_str225[sizeof("CHINESE")];
    char stringpool_str227[sizeof("ISO8859-3")];
    char stringpool_str228[sizeof("ISO-IR-100")];
    char stringpool_str229[sizeof("LATIN5")];
    char stringpool_str230[sizeof("ISO-8859-3")];
    char stringpool_str231[sizeof("ISO_8859-3")];
    char stringpool_str232[sizeof("ISO8859-13")];
    char stringpool_str233[sizeof("ISO-IR-226")];
    char stringpool_str234[sizeof("CYRILLIC-ASIAN")];
    char stringpool_str235[sizeof("ISO-8859-13")];
    char stringpool_str236[sizeof("ISO_8859-13")];
    char stringpool_str241[sizeof("US")];
    char stringpool_str242[sizeof("MS-CYRL")];
    char stringpool_str243[sizeof("TIS620")];
    char stringpool_str244[sizeof("LATIN10")];
    char stringpool_str246[sizeof("TIS-620")];
    char stringpool_str250[sizeof("ARABIC")];
    char stringpool_str251[sizeof("ECMA-118")];
    char stringpool_str252[sizeof("EUCKR")];
    char stringpool_str253[sizeof("LATIN2")];
    char stringpool_str255[sizeof("EUC-KR")];
    char stringpool_str258[sizeof("UTF-8")];
    char stringpool_str259[sizeof("KZ-1048")];
    char stringpool_str260[sizeof("CSISO2022CN")];
    char stringpool_str262[sizeof("CSASCII")];
    char stringpool_str263[sizeof("MS936")];
    char stringpool_str264[sizeof("IBM819")];
    char stringpool_str266[sizeof("MULELAO-1")];
    char stringpool_str267[sizeof("X0208")];
    char stringpool_str269[sizeof("X0201")];
    char stringpool_str271[sizeof("GB18030")];
    char stringpool_str272[sizeof("KOREAN")];
    char stringpool_str273[sizeof("IBM866")];
    char stringpool_str274[sizeof("TIS620-0")];
    char stringpool_str276[sizeof("KOI8-R")];
    char stringpool_str277[sizeof("ECMA-114")];
    char stringpool_str278[sizeof("UCS-4")];
    char stringpool_str279[sizeof("UTF-16")];
    char stringpool_str281[sizeof("CSKZ1048")];
    char stringpool_str283[sizeof("KSC_5601")];
    char stringpool_str284[sizeof("CSKOI8R")];
    char stringpool_str287[sizeof("MS-EE")];
    char stringpool_str288[sizeof("GB2312")];
    char stringpool_str291[sizeof("CSUCS4")];
    char stringpool_str293[sizeof("BIG5")];
    char stringpool_str296[sizeof("BIG-5")];
    char stringpool_str297[sizeof("HP-ROMAN8")];
    char stringpool_str299[sizeof("LATIN3")];
    char stringpool_str304[sizeof("KS_C_5601-1989")];
    char stringpool_str306[sizeof("X0212")];
    char stringpool_str307[sizeof("TCVN")];
    char stringpool_str309[sizeof("ISO-CELTIC")];
    char stringpool_str311[sizeof("CSHPROMAN8")];
    char stringpool_str314[sizeof("UCS-2")];
    char stringpool_str316[sizeof("IBM850")];
    char stringpool_str318[sizeof("ISO-IR-203")];
    char stringpool_str319[sizeof("IBM862")];
    char stringpool_str320[sizeof("GB_2312-80")];
    char stringpool_str324[sizeof("CSISOLATIN1")];
    char stringpool_str327[sizeof("ISO-2022-CN-EXT")];
    char stringpool_str335[sizeof("ISO-IR-179")];
    char stringpool_str337[sizeof("CSISOLATINCYRILLIC")];
    char stringpool_str338[sizeof("CSISOLATIN6")];
    char stringpool_str342[sizeof("JP")];
    char stringpool_str346[sizeof("MACICELAND")];
    char stringpool_str347[sizeof("UCS-4LE")];
    char stringpool_str348[sizeof("CSISOLATIN4")];
    char stringpool_str349[sizeof("CSISOLATINARABIC")];
    char stringpool_str350[sizeof("UNICODE-1-1")];
    char stringpool_str353[sizeof("UTF-16LE")];
    char stringpool_str357[sizeof("CSUNICODE11")];
    char stringpool_str360[sizeof("CSISOLATIN5")];
    char stringpool_str361[sizeof("MS-ANSI")];
    char stringpool_str364[sizeof("CSBIG5")];
    char stringpool_str365[sizeof("UCS-2LE")];
    char stringpool_str367[sizeof("CN-BIG5")];
    char stringpool_str372[sizeof("ARMSCII-8")];
    char stringpool_str373[sizeof("ISO-10646-UCS-4")];
    char stringpool_str378[sizeof("UTF-32")];
    char stringpool_str380[sizeof("CSUNICODE")];
    char stringpool_str382[sizeof("ISO_8859-8:1988")];
    char stringpool_str384[sizeof("CSISOLATIN2")];
    char stringpool_str385[sizeof("CN-GB")];
    char stringpool_str386[sizeof("ISO646-US")];
    char stringpool_str387[sizeof("MACROMAN")];
    char stringpool_str389[sizeof("MACCYRILLIC")];
    char stringpool_str391[sizeof("ISO-10646-UCS-2")];
    char stringpool_str394[sizeof("STRK1048-2002")];
    char stringpool_str395[sizeof("ISO_8859-4:1988")];
    char stringpool_str396[sizeof("ISO_8859-9:1989")];
    char stringpool_str397[sizeof("EUCJP")];
    char stringpool_str400[sizeof("EUC-JP")];
    char stringpool_str401[sizeof("ISO_8859-5:1988")];
    char stringpool_str402[sizeof("GREEK8")];
    char stringpool_str403[sizeof("ASMO-708")];
    char stringpool_str405[sizeof("PCK")];
    char stringpool_str408[sizeof("CSIBM866")];
    char stringpool_str409[sizeof("CP1257")];
    char stringpool_str411[sizeof("ISO-2022-KR")];
    char stringpool_str412[sizeof("GEORGIAN-ACADEMY")];
    char stringpool_str415[sizeof("MACCROATIAN")];
    char stringpool_str416[sizeof("CP367")];
    char stringpool_str419[sizeof("GEORGIAN-PS")];
    char stringpool_str423[sizeof("CSGB2312")];
    char stringpool_str424[sizeof("VISCII")];
    char stringpool_str428[sizeof("MS-HEBR")];
    char stringpool_str429[sizeof("UTF-32LE")];
    char stringpool_str430[sizeof("CSISOLATIN3")];
    char stringpool_str432[sizeof("MACARABIC")];
    char stringpool_str436[sizeof("ISO_8859-3:1988")];
    char stringpool_str437[sizeof("IBM-CP1133")];
    char stringpool_str439[sizeof("TIS620.2529-1")];
    char stringpool_str448[sizeof("CSISO2022KR")];
    char stringpool_str449[sizeof("ISO8859-7")];
    char stringpool_str451[sizeof("MACCENTRALEUROPE")];
    char stringpool_str452[sizeof("ISO-8859-7")];
    char stringpool_str453[sizeof("ISO_8859-7")];
    char stringpool_str455[sizeof("CN-GB-ISOIR165")];
    char stringpool_str461[sizeof("ISO646-JP")];
    char stringpool_str462[sizeof("KS_C_5601-1987")];
    char stringpool_str463[sizeof("US-ASCII")];
    char stringpool_str464[sizeof("UCS-4BE")];
    char stringpool_str466[sizeof("CSEUCKR")];
    char stringpool_str467[sizeof("JIS0208")];
    char stringpool_str470[sizeof("UTF-16BE")];
    char stringpool_str475[sizeof("MS-ARAB")];
    char stringpool_str476[sizeof("CSPC862LATINHEBREW")];
    char stringpool_str478[sizeof("KOI8-T")];
    char stringpool_str481[sizeof("ISO-IR-87")];
    char stringpool_str482[sizeof("UCS-2BE")];
    char stringpool_str489[sizeof("MACROMANIA")];
    char stringpool_str492[sizeof("UCS-4-INTERNAL")];
    char stringpool_str493[sizeof("ISO_646.IRV:1991")];
    char stringpool_str495[sizeof("CSVISCII")];
    char stringpool_str497[sizeof("VISCII1.1-1")];
    char stringpool_str500[sizeof("ISO-IR-57")];
    char stringpool_str502[sizeof("NEXTSTEP")];
    char stringpool_str503[sizeof("HZ-GB-2312")];
    char stringpool_str504[sizeof("CSKSC56011987")];
    char stringpool_str505[sizeof("ISO-IR-157")];
    char stringpool_str507[sizeof("JIS_C6220-1969-RO")];
    char stringpool_str508[sizeof("CSISO58GB231280")];
    char stringpool_str509[sizeof("TIS620.2533-1")];
    char stringpool_str510[sizeof("UCS-2-INTERNAL")];
    char stringpool_str511[sizeof("WINDOWS-1258")];
    char stringpool_str512[sizeof("WINDOWS-1251")];
    char stringpool_str513[sizeof("MACTHAI")];
    char stringpool_str515[sizeof("WCHAR_T")];
    char stringpool_str516[sizeof("GBK")];
    char stringpool_str517[sizeof("ISO-IR-127")];
    char stringpool_str519[sizeof("WINDOWS-1256")];
    char stringpool_str520[sizeof("UNICODE-1-1-UTF-7")];
    char stringpool_str521[sizeof("LATIN7")];
    char stringpool_str523[sizeof("ANSI_X3.4-1968")];
    char stringpool_str524[sizeof("WINDOWS-1254")];
    char stringpool_str525[sizeof("CSUNICODE11UTF7")];
    char stringpool_str530[sizeof("WINDOWS-1255")];
    char stringpool_str531[sizeof("ANSI_X3.4-1986")];
    char stringpool_str532[sizeof("TIS620.2533-0")];
    char stringpool_str533[sizeof("EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE")];
    char stringpool_str535[sizeof("WINDOWS-1250")];
    char stringpool_str536[sizeof("WINDOWS-936")];
    char stringpool_str537[sizeof("EUCTW")];
    char stringpool_str540[sizeof("EUC-TW")];
    char stringpool_str542[sizeof("WINDOWS-1252")];
    char stringpool_str543[sizeof("JIS_C6226-1983")];
    char stringpool_str545[sizeof("UCS-4-SWAPPED")];
    char stringpool_str546[sizeof("UTF-32BE")];
    char stringpool_str547[sizeof("TCVN5712-1")];
    char stringpool_str548[sizeof("ISO_8859-1:1987")];
    char stringpool_str553[sizeof("MACINTOSH")];
    char stringpool_str554[sizeof("ISO-2022-JP-1")];
    char stringpool_str555[sizeof("ISO_8859-6:1987")];
    char stringpool_str556[sizeof("ISO-2022-JP")];
    char stringpool_str560[sizeof("TIS620.2533")];
    char stringpool_str563[sizeof("UCS-2-SWAPPED")];
    char stringpool_str565[sizeof("WINDOWS-1253")];
    char stringpool_str569[sizeof("JAVA")];
    char stringpool_str570[sizeof("CSISO57GB1988")];
    char stringpool_str572[sizeof("TCVN-5712")];
    char stringpool_str578[sizeof("ISO_8859-2:1987")];
    char stringpool_str579[sizeof("CSISO14JISC6220RO")];
    char stringpool_str583[sizeof("CSMACINTOSH")];
    char stringpool_str584[sizeof("ISO-2022-JP-2")];
    char stringpool_str588[sizeof("UTF-7")];
    char stringpool_str589[sizeof("CSPC850MULTILINGUAL")];
    char stringpool_str592[sizeof("GREEK")];
    char stringpool_str593[sizeof("CSISO2022JP")];
    char stringpool_str594[sizeof("CSISOLATINHEBREW")];
    char stringpool_str601[sizeof("ISO_8859-7:2003")];
    char stringpool_str616[sizeof("CSISO159JISX02121990")];
    char stringpool_str619[sizeof("BIGFIVE")];
    char stringpool_str620[sizeof("CSISO2022JP2")];
    char stringpool_str622[sizeof("BIG-FIVE")];
    char stringpool_str636[sizeof("CSISOLATINGREEK")];
    char stringpool_str637[sizeof("HEBREW")];
    char stringpool_str641[sizeof("IBM367")];
    char stringpool_str647[sizeof("CSHALFWIDTHKATAKANA")];
    char stringpool_str650[sizeof("WINDOWS-874")];
    char stringpool_str652[sizeof("UNICODELITTLE")];
    char stringpool_str663[sizeof("BIG5HKSCS")];
    char stringpool_str666[sizeof("BIG5-HKSCS")];
    char stringpool_str667[sizeof("JIS_X0208")];
    char stringpool_str669[sizeof("JIS_X0201")];
    char stringpool_str676[sizeof("WINDOWS-1257")];
    char stringpool_str680[sizeof("KOI8-U")];
    char stringpool_str684[sizeof("KOI8-RU")];
    char stringpool_str691[sizeof("JOHAB")];
    char stringpool_str693[sizeof("JISX0201-1976")];
    char stringpool_str702[sizeof("JIS_X0208-1990")];
    char stringpool_str706[sizeof("JIS_X0212")];
    char stringpool_str710[sizeof("JIS_X0212-1990")];
    char stringpool_str712[sizeof("ISO_8859-7:1987")];
    char stringpool_str713[sizeof("SHIFT-JIS")];
    char stringpool_str714[sizeof("SHIFT_JIS")];
    char stringpool_str732[sizeof("JIS_X0208-1983")];
    char stringpool_str751[sizeof("CSEUCTW")];
    char stringpool_str752[sizeof("MACUKRAINE")];
    char stringpool_str759[sizeof("UNICODEBIG")];
    char stringpool_str769[sizeof("MS-GREEK")];
    char stringpool_str774[sizeof("MACGREEK")];
    char stringpool_str800[sizeof("CSSHIFTJIS")];
    char stringpool_str822[sizeof("JIS_X0212.1990-0")];
    char stringpool_str840[sizeof("CSEUCPKDFMTJAPANESE")];
    char stringpool_str853[sizeof("MACHEBREW")];
    char stringpool_str858[sizeof("MS_KANJI")];
    char stringpool_str859[sizeof("TCVN5712-1:1993")];
    char stringpool_str869[sizeof("WINBALTRIM")];
    char stringpool_str884[sizeof("MS-TURK")];
    char stringpool_str895[sizeof("BIG5-HKSCS:2001")];
    char stringpool_str901[sizeof("BIG5-HKSCS:1999")];
    char stringpool_str907[sizeof("BIG5-HKSCS:2004")];
    char stringpool_str917[sizeof("CSISO87JISX0208")];
    char stringpool_str953[sizeof("MACTURKISH")];
    char stringpool_str1003[sizeof("KO_KR.JOHAP92")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "R8",
    "CN",
    "L8",
    "L1",
    "L6",
    "L4",
    "866",
    "C99",
    "L5",
    "646",
    "CHAR",
    "CP819",
    "L2",
    "CP866",
    "CP949",
    "850",
    "5601",
    "RK1048",
    "EUCCN",
    "L10",
    "EUC-CN",
    "L3",
    "CP154",
    "PT154",
    "862",
    "CP1258",
    "CP1251",
    "CP1131",
    "PTCP154",
    "CP850",
    "CP1361",
    "CP862",
    "CP1256",
    "CP950",
    "HZ",
    "CP936",
    "CP1254",
    "CP1255",
    "ISO8859-8",
    "ISO8859-1",
    "ISO-8859-8",
    "ISO_8859-8",
    "ISO-8859-1",
    "ISO_8859-1",
    "ISO8859-11",
    "CP1250",
    "ISO646-CN",
    "ISO-8859-11",
    "ISO_8859-11",
    "ISO8859-9",
    "ISO8859-6",
    "ISO-8859-9",
    "ISO_8859-9",
    "ISO-8859-6",
    "ISO_8859-6",
    "ISO8859-16",
    "CP1252",
    "ISO_8859-16:2001",
    "ISO-8859-16",
    "ISO_8859-16",
    "ISO8859-4",
    "ISO_8859-14:1998",
    "CP932",
    "ISO-8859-4",
    "ISO_8859-4",
    "ISO8859-14",
    "ISO_8859-15:1998",
    "ISO-8859-14",
    "ISO_8859-14",
    "ISO8859-5",
    "ISO-8859-5",
    "ISO_8859-5",
    "ISO8859-15",
    "ISO-IR-6",
    "ISO-8859-15",
    "ISO_8859-15",
    "SJIS",
    "ISO-IR-148",
    "ISO-IR-58",
    "ISO8859-10",
    "CYRILLIC",
    "ISO-8859-10",
    "ISO_8859-10",
    "ISO-IR-199",
    "ISO-IR-14",
    "L7",
    "ISO-IR-166",
    "ISO8859-2",
    "ISO-IR-101",
    "ISO-IR-149",
    "ISO-8859-2",
    "ISO_8859-2",
    "MAC",
    "CP1253",
    "ISO_8859-10:1992",
    "ISO-IR-159",
    "LATIN8",
    "CP1133",
    "LATIN1",
    "ISO-IR-109",
    "ISO-IR-144",
    "ANSI-1251",
    "CNS11643",
    "CSPTCP154",
    "ISO-IR-165",
    "ISO-IR-126",
    "ELOT_928",
    "ISO-IR-110",
    "LATIN6",
    "LATIN-9",
    "ROMAN8",
    "ISO-IR-138",
    "GB_1988-80",
    "CP874",
    "LATIN4",
    "ASCII",
    "UHC",
    "ISO-2022-CN",
    "CHINESE",
    "ISO8859-3",
    "ISO-IR-100",
    "LATIN5",
    "ISO-8859-3",
    "ISO_8859-3",
    "ISO8859-13",
    "ISO-IR-226",
    "CYRILLIC-ASIAN",
    "ISO-8859-13",
    "ISO_8859-13",
    "US",
    "MS-CYRL",
    "TIS620",
    "LATIN10",
    "TIS-620",
    "ARABIC",
    "ECMA-118",
    "EUCKR",
    "LATIN2",
    "EUC-KR",
    "UTF-8",
    "KZ-1048",
    "CSISO2022CN",
    "CSASCII",
    "MS936",
    "IBM819",
    "MULELAO-1",
    "X0208",
    "X0201",
    "GB18030",
    "KOREAN",
    "IBM866",
    "TIS620-0",
    "KOI8-R",
    "ECMA-114",
    "UCS-4",
    "UTF-16",
    "CSKZ1048",
    "KSC_5601",
    "CSKOI8R",
    "MS-EE",
    "GB2312",
    "CSUCS4",
    "BIG5",
    "BIG-5",
    "HP-ROMAN8",
    "LATIN3",
    "KS_C_5601-1989",
    "X0212",
    "TCVN",
    "ISO-CELTIC",
    "CSHPROMAN8",
    "UCS-2",
    "IBM850",
    "ISO-IR-203",
    "IBM862",
    "GB_2312-80",
    "CSISOLATIN1",
    "ISO-2022-CN-EXT",
    "ISO-IR-179",
    "CSISOLATINCYRILLIC",
    "CSISOLATIN6",
    "JP",
    "MACICELAND",
    "UCS-4LE",
    "CSISOLATIN4",
    "CSISOLATINARABIC",
    "UNICODE-1-1",
    "UTF-16LE",
    "CSUNICODE11",
    "CSISOLATIN5",
    "MS-ANSI",
    "CSBIG5",
    "UCS-2LE",
    "CN-BIG5",
    "ARMSCII-8",
    "ISO-10646-UCS-4",
    "UTF-32",
    "CSUNICODE",
    "ISO_8859-8:1988",
    "CSISOLATIN2",
    "CN-GB",
    "ISO646-US",
    "MACROMAN",
    "MACCYRILLIC",
    "ISO-10646-UCS-2",
    "STRK1048-2002",
    "ISO_8859-4:1988",
    "ISO_8859-9:1989",
    "EUCJP",
    "EUC-JP",
    "ISO_8859-5:1988",
    "GREEK8",
    "ASMO-708",
    "PCK",
    "CSIBM866",
    "CP1257",
    "ISO-2022-KR",
    "GEORGIAN-ACADEMY",
    "MACCROATIAN",
    "CP367",
    "GEORGIAN-PS",
    "CSGB2312",
    "VISCII",
    "MS-HEBR",
    "UTF-32LE",
    "CSISOLATIN3",
    "MACARABIC",
    "ISO_8859-3:1988",
    "IBM-CP1133",
    "TIS620.2529-1",
    "CSISO2022KR",
    "ISO8859-7",
    "MACCENTRALEUROPE",
    "ISO-8859-7",
    "ISO_8859-7",
    "CN-GB-ISOIR165",
    "ISO646-JP",
    "KS_C_5601-1987",
    "US-ASCII",
    "UCS-4BE",
    "CSEUCKR",
    "JIS0208",
    "UTF-16BE",
    "MS-ARAB",
    "CSPC862LATINHEBREW",
    "KOI8-T",
    "ISO-IR-87",
    "UCS-2BE",
    "MACROMANIA",
    "UCS-4-INTERNAL",
    "ISO_646.IRV:1991",
    "CSVISCII",
    "VISCII1.1-1",
    "ISO-IR-57",
    "NEXTSTEP",
    "HZ-GB-2312",
    "CSKSC56011987",
    "ISO-IR-157",
    "JIS_C6220-1969-RO",
    "CSISO58GB231280",
    "TIS620.2533-1",
    "UCS-2-INTERNAL",
    "WINDOWS-1258",
    "WINDOWS-1251",
    "MACTHAI",
    "WCHAR_T",
    "GBK",
    "ISO-IR-127",
    "WINDOWS-1256",
    "UNICODE-1-1-UTF-7",
    "LATIN7",
    "ANSI_X3.4-1968",
    "WINDOWS-1254",
    "CSUNICODE11UTF7",
    "WINDOWS-1255",
    "ANSI_X3.4-1986",
    "TIS620.2533-0",
    "EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE",
    "WINDOWS-1250",
    "WINDOWS-936",
    "EUCTW",
    "EUC-TW",
    "WINDOWS-1252",
    "JIS_C6226-1983",
    "UCS-4-SWAPPED",
    "UTF-32BE",
    "TCVN5712-1",
    "ISO_8859-1:1987",
    "MACINTOSH",
    "ISO-2022-JP-1",
    "ISO_8859-6:1987",
    "ISO-2022-JP",
    "TIS620.2533",
    "UCS-2-SWAPPED",
    "WINDOWS-1253",
    "JAVA",
    "CSISO57GB1988",
    "TCVN-5712",
    "ISO_8859-2:1987",
    "CSISO14JISC6220RO",
    "CSMACINTOSH",
    "ISO-2022-JP-2",
    "UTF-7",
    "CSPC850MULTILINGUAL",
    "GREEK",
    "CSISO2022JP",
    "CSISOLATINHEBREW",
    "ISO_8859-7:2003",
    "CSISO159JISX02121990",
    "BIGFIVE",
    "CSISO2022JP2",
    "BIG-FIVE",
    "CSISOLATINGREEK",
    "HEBREW",
    "IBM367",
    "CSHALFWIDTHKATAKANA",
    "WINDOWS-874",
    "UNICODELITTLE",
    "BIG5HKSCS",
    "BIG5-HKSCS",
    "JIS_X0208",
    "JIS_X0201",
    "WINDOWS-1257",
    "KOI8-U",
    "KOI8-RU",
    "JOHAB",
    "JISX0201-1976",
    "JIS_X0208-1990",
    "JIS_X0212",
    "JIS_X0212-1990",
    "ISO_8859-7:1987",
    "SHIFT-JIS",
    "SHIFT_JIS",
    "JIS_X0208-1983",
    "CSEUCTW",
    "MACUKRAINE",
    "UNICODEBIG",
    "MS-GREEK",
    "MACGREEK",
    "CSSHIFTJIS",
    "JIS_X0212.1990-0",
    "CSEUCPKDFMTJAPANESE",
    "MACHEBREW",
    "MS_KANJI",
    "TCVN5712-1:1993",
    "WINBALTRIM",
    "MS-TURK",
    "BIG5-HKSCS:2001",
    "BIG5-HKSCS:1999",
    "BIG5-HKSCS:2004",
    "CSISO87JISX0208",
    "MACTURKISH",
    "KO_KR.JOHAP92"
  };
#define stringpool ((const char *) &stringpool_contents)

static const struct alias aliases[] =
  {
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 229 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str8, ei_hp_roman8},
    {-1}, {-1}, {-1}, {-1},
#line 291 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str13, ei_iso646_cn},
#line 152 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str14, ei_iso8859_14},
#line 61 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str15, ei_iso8859_1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 135 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str22, ei_iso8859_10},
    {-1}, {-1}, {-1}, {-1},
#line 85 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, ei_iso8859_4},
#line 209 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str28, ei_cp866},
    {-1},
#line 52 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str30, ei_c99},
    {-1}, {-1},
#line 127 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str33, ei_iso8859_9},
    {-1}, {-1},
#line 23 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str36, ei_ascii},
    {-1},
#line 363 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str38, ei_local_char},
#line 58 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str39, ei_iso8859_1},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 69 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str45, ei_iso8859_2},
    {-1}, {-1},
#line 207 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str48, ei_cp866},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 356 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str58, ei_cp949},
    {-1},
#line 201 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str60, ei_cp850},
#line 355 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str61, ei_euc_kr},
#line 241 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str62, ei_rk1048},
    {-1},
#line 322 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str64, ei_euc_cn},
    {-1},
#line 166 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str66, ei_iso8859_16},
#line 321 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str67, ei_euc_cn},
#line 77 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str68, ei_iso8859_3},
    {-1},
#line 238 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str70, ei_pt154},
#line 236 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str71, ei_pt154},
    {-1}, {-1},
#line 205 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, ei_cp862},
    {-1}, {-1}, {-1}, {-1},
#line 197 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str79, ei_cp1258},
    {-1},
#line 175 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str81, ei_cp1251},
    {-1}, {-1}, {-1}, {-1},
#line 211 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str86, ei_cp1131},
    {-1},
#line 237 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str88, ei_pt154},
    {-1}, {-1},
#line 199 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str91, ei_cp850},
    {-1},
#line 359 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str93, ei_johab},
#line 203 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str94, ei_cp862},
#line 191 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str95, ei_cp1256},
    {-1}, {-1},
#line 346 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str98, ei_cp950},
#line 334 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str99, ei_hz},
    {-1},
#line 327 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str101, ei_cp936},
    {-1}, {-1}, {-1},
#line 185 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str105, ei_cp1254},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 188 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str117, ei_cp1255},
    {-1},
#line 121 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str119, ei_iso8859_8},
    {-1},
#line 63 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str121, ei_iso8859_1},
#line 115 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str122, ei_iso8859_8},
#line 116 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str123, ei_iso8859_8},
#line 54 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str124, ei_iso8859_1},
#line 55 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str125, ei_iso8859_1},
#line 140 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str126, ei_iso8859_11},
#line 172 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str127, ei_cp1250},
#line 289 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str128, ei_iso646_cn},
#line 138 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str129, ei_iso8859_11},
#line 139 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str130, ei_iso8859_11},
    {-1}, {-1},
#line 129 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str133, ei_iso8859_9},
    {-1},
#line 103 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str135, ei_iso8859_6},
#line 122 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str136, ei_iso8859_9},
#line 123 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str137, ei_iso8859_9},
#line 95 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str138, ei_iso8859_6},
#line 96 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str139, ei_iso8859_6},
#line 167 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str140, ei_iso8859_16},
#line 179 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str141, ei_cp1252},
#line 163 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str142, ei_iso8859_16},
#line 161 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str143, ei_iso8859_16},
#line 162 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str144, ei_iso8859_16},
#line 87 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str145, ei_iso8859_4},
#line 149 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str146, ei_iso8859_14},
#line 315 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str147, ei_cp932},
#line 80 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str148, ei_iso8859_4},
#line 81 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str149, ei_iso8859_4},
#line 154 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str150, ei_iso8859_14},
    {-1},
#line 157 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str152, ei_iso8859_15},
#line 147 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str153, ei_iso8859_14},
#line 148 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str154, ei_iso8859_14},
    {-1}, {-1},
#line 94 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str157, ei_iso8859_5},
    {-1}, {-1},
#line 88 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str160, ei_iso8859_5},
#line 89 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str161, ei_iso8859_5},
#line 160 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str162, ei_iso8859_15},
#line 16 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str163, ei_ascii},
    {-1},
#line 155 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str165, ei_iso8859_15},
#line 156 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str166, ei_iso8859_15},
    {-1},
#line 311 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str168, ei_sjis},
#line 125 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str169, ei_iso8859_9},
#line 294 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str170, ei_gb2312},
    {-1},
#line 137 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str172, ei_iso8859_10},
    {-1},
#line 92 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str174, ei_iso8859_5},
#line 130 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str175, ei_iso8859_10},
#line 131 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str176, ei_iso8859_10},
#line 150 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str177, ei_iso8859_14},
#line 267 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str178, ei_iso646_jp},
#line 145 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str179, ei_iso8859_13},
#line 254 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str180, ei_tis620},
#line 71 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str181, ei_iso8859_2},
#line 67 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str182, ei_iso8859_2},
#line 302 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str183, ei_ksc5601},
#line 64 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str184, ei_iso8859_2},
#line 65 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str185, ei_iso8859_2},
#line 214 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str186, ei_mac_roman},
#line 182 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str187, ei_cp1253},
#line 132 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str188, ei_iso8859_10},
#line 286 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str189, ei_jisx0212},
    {-1},
#line 151 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str191, ei_iso8859_14},
#line 246 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str192, ei_cp1133},
#line 60 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str193, ei_iso8859_1},
#line 75 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str194, ei_iso8859_3},
#line 91 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str195, ei_iso8859_5},
#line 178 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str196, ei_cp1251},
    {-1},
#line 339 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str198, ei_euc_tw},
    {-1}, {-1},
#line 240 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str201, ei_pt154},
#line 297 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str202, ei_isoir165},
#line 108 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str203, ei_iso8859_7},
#line 110 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str204, ei_iso8859_7},
#line 83 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str205, ei_iso8859_4},
    {-1},
#line 134 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str207, ei_iso8859_10},
#line 159 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str208, ei_iso8859_15},
#line 228 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str209, ei_hp_roman8},
#line 118 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str210, ei_iso8859_8},
#line 288 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str211, ei_iso646_cn},
    {-1}, {-1}, {-1},
#line 256 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str215, ei_cp874},
    {-1},
#line 84 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str217, ei_iso8859_4},
    {-1},
#line 13 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str219, ei_ascii},
    {-1}, {-1},
#line 357 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str222, ei_cp949},
#line 331 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str223, ei_iso2022_cn},
    {-1},
#line 296 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str225, ei_gb2312},
    {-1},
#line 79 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str227, ei_iso8859_3},
#line 57 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str228, ei_iso8859_1},
#line 126 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str229, ei_iso8859_9},
#line 72 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str230, ei_iso8859_3},
#line 73 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str231, ei_iso8859_3},
#line 146 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str232, ei_iso8859_13},
#line 164 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str233, ei_iso8859_16},
#line 239 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str234, ei_pt154},
#line 141 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str235, ei_iso8859_13},
#line 142 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str236, ei_iso8859_13},
    {-1}, {-1}, {-1}, {-1},
#line 21 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str241, ei_ascii},
#line 177 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str242, ei_cp1251},
#line 249 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str243, ei_tis620},
#line 165 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str244, ei_iso8859_16},
    {-1},
#line 248 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str246, ei_tis620},
    {-1}, {-1}, {-1},
#line 101 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str250, ei_iso8859_6},
#line 109 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str251, ei_iso8859_7},
#line 353 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str252, ei_euc_kr},
#line 68 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str253, ei_iso8859_2},
    {-1},
#line 352 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str255, ei_euc_kr},
    {-1}, {-1},
#line 24 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str258, ei_utf8},
#line 243 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str259, ei_rk1048},
#line 332 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str260, ei_iso2022_cn},
    {-1},
#line 22 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str262, ei_ascii},
#line 328 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str263, ei_cp936},
#line 59 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str264, ei_iso8859_1},
    {-1},
#line 245 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str266, ei_mulelao},
#line 278 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str267, ei_jisx0208},
    {-1},
#line 272 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str269, ei_jisx0201},
    {-1},
#line 330 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str271, ei_gb18030},
#line 304 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str272, ei_ksc5601},
#line 208 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str273, ei_cp866},
#line 250 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str274, ei_tis620},
    {-1},
#line 168 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str276, ei_koi8_r},
#line 99 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str277, ei_iso8859_6},
#line 34 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str278, ei_ucs4},
#line 39 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str279, ei_utf16},
    {-1},
#line 244 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str281, ei_rk1048},
    {-1},
#line 299 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str283, ei_ksc5601},
#line 169 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str284, ei_koi8_r},
    {-1}, {-1},
#line 174 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str287, ei_cp1250},
#line 323 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str288, ei_euc_cn},
    {-1}, {-1},
#line 36 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str291, ei_ucs4},
    {-1},
#line 340 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str293, ei_ces_big5},
    {-1}, {-1},
#line 341 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str296, ei_ces_big5},
#line 227 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str297, ei_hp_roman8},
    {-1},
#line 76 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str299, ei_iso8859_3},
    {-1}, {-1}, {-1}, {-1},
#line 301 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str304, ei_ksc5601},
    {-1},
#line 285 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str306, ei_jisx0212},
#line 261 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str307, ei_tcvn},
    {-1},
#line 153 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str309, ei_iso8859_14},
    {-1},
#line 230 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str311, ei_hp_roman8},
    {-1}, {-1},
#line 25 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str314, ei_ucs2},
    {-1},
#line 200 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str316, ei_cp850},
    {-1},
#line 158 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str318, ei_iso8859_15},
#line 204 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str319, ei_cp862},
#line 293 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str320, ei_gb2312},
    {-1}, {-1}, {-1},
#line 62 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str324, ei_iso8859_1},
    {-1}, {-1},
#line 333 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str327, ei_iso2022_cn_ext},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 143 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str335, ei_iso8859_13},
    {-1},
#line 93 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str337, ei_iso8859_5},
#line 136 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str338, ei_iso8859_10},
    {-1}, {-1}, {-1},
#line 268 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str342, ei_iso646_jp},
    {-1}, {-1}, {-1},
#line 217 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str346, ei_mac_iceland},
#line 38 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str347, ei_ucs4le},
#line 86 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str348, ei_iso8859_4},
#line 102 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str349, ei_iso8859_6},
#line 30 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str350, ei_ucs2be},
    {-1}, {-1},
#line 41 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str353, ei_utf16le},
    {-1}, {-1}, {-1},
#line 31 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str357, ei_ucs2be},
    {-1}, {-1},
#line 128 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str360, ei_iso8859_9},
#line 181 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str361, ei_cp1252},
    {-1}, {-1},
#line 345 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str364, ei_ces_big5},
#line 32 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str365, ei_ucs2le},
    {-1},
#line 344 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str367, ei_ces_big5},
    {-1}, {-1}, {-1}, {-1},
#line 232 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str372, ei_armscii_8},
#line 35 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str373, ei_ucs4},
    {-1}, {-1}, {-1}, {-1},
#line 42 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str378, ei_utf32},
    {-1},
#line 27 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str380, ei_ucs2},
    {-1},
#line 117 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str382, ei_iso8859_8},
    {-1},
#line 70 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str384, ei_iso8859_2},
#line 324 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str385, ei_euc_cn},
#line 14 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str386, ei_ascii},
#line 212 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str387, ei_mac_roman},
    {-1},
#line 220 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str389, ei_mac_cyrillic},
    {-1},
#line 26 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str391, ei_ucs2},
    {-1}, {-1},
#line 242 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str394, ei_rk1048},
#line 82 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str395, ei_iso8859_4},
#line 124 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str396, ei_iso8859_9},
#line 306 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str397, ei_euc_jp},
    {-1}, {-1},
#line 305 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str400, ei_euc_jp},
#line 90 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str401, ei_iso8859_5},
#line 111 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str402, ei_iso8859_7},
#line 100 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str403, ei_iso8859_6},
    {-1},
#line 314 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str405, ei_sjis},
    {-1}, {-1},
#line 210 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str408, ei_cp866},
#line 194 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str409, ei_cp1257},
    {-1},
#line 361 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str411, ei_iso2022_kr},
#line 233 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str412, ei_georgian_academy},
    {-1}, {-1},
#line 218 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str415, ei_mac_croatian},
#line 19 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str416, ei_ascii},
    {-1}, {-1},
#line 234 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str419, ei_georgian_ps},
    {-1}, {-1}, {-1},
#line 325 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str423, ei_euc_cn},
#line 258 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str424, ei_viscii},
    {-1}, {-1}, {-1},
#line 190 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str428, ei_cp1255},
#line 44 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str429, ei_utf32le},
#line 78 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str430, ei_iso8859_3},
    {-1},
#line 225 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str432, ei_mac_arabic},
    {-1}, {-1}, {-1},
#line 74 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str436, ei_iso8859_3},
#line 247 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str437, ei_cp1133},
    {-1},
#line 251 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str439, ei_tis620},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 362 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str448, ei_iso2022_kr},
#line 114 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str449, ei_iso8859_7},
    {-1},
#line 216 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str451, ei_mac_centraleurope},
#line 104 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str452, ei_iso8859_7},
#line 105 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str453, ei_iso8859_7},
    {-1},
#line 298 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str455, ei_isoir165},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 266 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str461, ei_iso646_jp},
#line 300 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str462, ei_ksc5601},
#line 12 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str463, ei_ascii},
#line 37 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str464, ei_ucs4be},
    {-1},
#line 354 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str466, ei_euc_kr},
#line 277 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str467, ei_jisx0208},
    {-1}, {-1},
#line 40 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str470, ei_utf16be},
    {-1}, {-1}, {-1}, {-1},
#line 193 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str475, ei_cp1256},
#line 206 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str476, ei_cp862},
    {-1},
#line 235 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str478, ei_koi8_t},
    {-1}, {-1},
#line 279 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str481, ei_jisx0208},
#line 28 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str482, ei_ucs2be},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 219 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str489, ei_mac_romania},
    {-1}, {-1},
#line 50 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str492, ei_ucs4internal},
#line 15 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str493, ei_ascii},
    {-1},
#line 260 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str495, ei_viscii},
    {-1},
#line 259 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str497, ei_viscii},
    {-1}, {-1},
#line 290 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str500, ei_iso646_cn},
    {-1},
#line 231 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str502, ei_nextstep},
#line 335 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str503, ei_hz},
#line 303 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str504, ei_ksc5601},
#line 133 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str505, ei_iso8859_10},
    {-1},
#line 265 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str507, ei_iso646_jp},
#line 295 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str508, ei_gb2312},
#line 253 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str509, ei_tis620},
#line 48 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str510, ei_ucs2internal},
#line 198 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str511, ei_cp1258},
#line 176 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str512, ei_cp1251},
#line 226 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str513, ei_mac_thai},
    {-1},
#line 364 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str515, ei_local_wchar_t},
#line 326 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str516, ei_ces_gbk},
#line 98 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str517, ei_iso8859_6},
    {-1},
#line 192 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str519, ei_cp1256},
#line 46 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str520, ei_utf7},
#line 144 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str521, ei_iso8859_13},
    {-1},
#line 17 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str523, ei_ascii},
#line 186 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str524, ei_cp1254},
#line 47 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str525, ei_utf7},
    {-1}, {-1}, {-1}, {-1},
#line 189 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str530, ei_cp1255},
#line 18 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str531, ei_ascii},
#line 252 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str532, ei_tis620},
#line 307 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str533, ei_euc_jp},
    {-1},
#line 173 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str535, ei_cp1250},
#line 329 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str536, ei_cp936},
#line 337 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str537, ei_euc_tw},
    {-1}, {-1},
#line 336 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str540, ei_euc_tw},
    {-1},
#line 180 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str542, ei_cp1252},
#line 280 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str543, ei_jisx0208},
    {-1},
#line 51 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str545, ei_ucs4swapped},
#line 43 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str546, ei_utf32be},
#line 263 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str547, ei_tcvn},
#line 56 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str548, ei_iso8859_1},
    {-1}, {-1}, {-1}, {-1},
#line 213 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str553, ei_mac_roman},
#line 318 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str554, ei_iso2022_jp1},
#line 97 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str555, ei_iso8859_6},
#line 316 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str556, ei_iso2022_jp},
    {-1}, {-1}, {-1},
#line 255 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str560, ei_tis620},
    {-1}, {-1},
#line 49 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str563, ei_ucs2swapped},
    {-1},
#line 183 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str565, ei_cp1253},
    {-1}, {-1}, {-1},
#line 53 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str569, ei_java},
#line 292 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str570, ei_iso646_cn},
    {-1},
#line 262 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str572, ei_tcvn},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 66 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str578, ei_iso8859_2},
#line 269 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str579, ei_iso646_jp},
    {-1}, {-1}, {-1},
#line 215 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str583, ei_mac_roman},
#line 319 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str584, ei_iso2022_jp2},
    {-1}, {-1}, {-1},
#line 45 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str588, ei_utf7},
#line 202 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str589, ei_cp850},
    {-1}, {-1},
#line 112 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str592, ei_iso8859_7},
#line 317 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str593, ei_iso2022_jp},
#line 120 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str594, ei_iso8859_8},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 107 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str601, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 287 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str616, ei_jisx0212},
    {-1}, {-1},
#line 343 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str619, ei_ces_big5},
#line 320 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str620, ei_iso2022_jp2},
    {-1},
#line 342 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str622, ei_ces_big5},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 113 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str636, ei_iso8859_7},
#line 119 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str637, ei_iso8859_8},
    {-1}, {-1}, {-1},
#line 20 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str641, ei_ascii},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 273 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str647, ei_jisx0201},
    {-1}, {-1},
#line 257 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str650, ei_cp874},
    {-1},
#line 33 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str652, ei_ucs2le},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 350 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str663, ei_big5hkscs2004},
    {-1}, {-1},
#line 349 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str666, ei_big5hkscs2004},
#line 274 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str667, ei_jisx0208},
    {-1},
#line 270 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str669, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 195 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str676, ei_cp1257},
    {-1}, {-1}, {-1},
#line 170 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str680, ei_koi8_u},
    {-1}, {-1}, {-1},
#line 171 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str684, ei_koi8_ru},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 358 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str691, ei_johab},
    {-1},
#line 271 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str693, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 276 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str702, ei_jisx0208},
    {-1}, {-1}, {-1},
#line 282 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str706, ei_jisx0212},
    {-1}, {-1}, {-1},
#line 284 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str710, ei_jisx0212},
    {-1},
#line 106 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str712, ei_iso8859_7},
#line 310 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str713, ei_sjis},
#line 309 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str714, ei_sjis},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 275 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str732, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 338 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str751, ei_euc_tw},
#line 221 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str752, ei_mac_ukraine},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 29 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str759, ei_ucs2be},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 184 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str769, ei_cp1253},
    {-1}, {-1}, {-1}, {-1},
#line 222 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str774, ei_mac_greek},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 313 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str800, ei_sjis},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 283 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str822, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 308 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str840, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 224 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str853, ei_mac_hebrew},
    {-1}, {-1}, {-1}, {-1},
#line 312 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str858, ei_sjis},
#line 264 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str859, ei_tcvn},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 196 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str869, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 187 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str884, ei_cp1254},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 348 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str895, ei_big5hkscs2001},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 347 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str901, ei_big5hkscs1999},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 351 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str907, ei_big5hkscs2004},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 281 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str917, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 223 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str953, ei_mac_turkish},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 360 "lib/aliases_syssolaris.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str1003, ei_johab}
  };

#ifdef __GNUC__
__inline
#if defined __GNUC_STDC_INLINE__ || defined __GNUC_GNU_INLINE__
__attribute__ ((__gnu_inline__))
#endif
#endif
const struct alias *
aliases_lookup (register const char *str, register unsigned int len)
{
  if (len <= MAX_WORD_LENGTH && len >= MIN_WORD_LENGTH)
    {
      register int key = aliases_hash (str, len);

      if (key <= MAX_HASH_VALUE && key >= 0)
        {
          register int o = aliases[key].name;
          if (o >= 0)
            {
              register const char *s = o + stringpool;

              if (*str == *s && !strcmp (str + 1, s + 1))
                return &aliases[key];
            }
        }
    }
  return 0;
}
