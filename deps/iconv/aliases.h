/* ANSI-C code produced by gperf version 3.0.4 */
/* Command-line: gperf -m 10 lib/aliases.gperf  */
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

#line 1 "lib/aliases.gperf"
struct alias { int name; unsigned int encoding_index; };

#define TOTAL_KEYWORDS 346
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 45
#define MIN_HASH_VALUE 7
#define MAX_HASH_VALUE 935
/* maximum key range = 929, duplicates = 0 */

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
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936,  16,  62, 936,  73,   0,
        5,   2,  47,   4,   1, 168,   8,  12, 357, 936,
      936, 936, 936, 936, 936, 112, 123,   3,  14,  34,
       71, 142, 147,   0, 258,  79,  39, 122,   4,   0,
      109, 936,  76,   1,  54, 147, 114, 180, 102,   3,
       10, 936, 936, 936, 936,  34, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936, 936, 936,
      936, 936, 936, 936, 936, 936, 936, 936
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
    char stringpool_str7[sizeof("SJIS")];
    char stringpool_str9[sizeof("CN")];
    char stringpool_str11[sizeof("CP1131")];
    char stringpool_str12[sizeof("CP1361")];
    char stringpool_str13[sizeof("866")];
    char stringpool_str15[sizeof("CP1133")];
    char stringpool_str18[sizeof("CP1251")];
    char stringpool_str19[sizeof("CP866")];
    char stringpool_str20[sizeof("CP1256")];
    char stringpool_str21[sizeof("862")];
    char stringpool_str22[sizeof("CP1253")];
    char stringpool_str24[sizeof("CP936")];
    char stringpool_str26[sizeof("CP1255")];
    char stringpool_str27[sizeof("CP862")];
    char stringpool_str28[sizeof("CP1252")];
    char stringpool_str30[sizeof("C99")];
    char stringpool_str32[sizeof("CP932")];
    char stringpool_str34[sizeof("CP1258")];
    char stringpool_str40[sizeof("CP819")];
    char stringpool_str41[sizeof("L1")];
    char stringpool_str42[sizeof("L6")];
    char stringpool_str43[sizeof("L3")];
    char stringpool_str45[sizeof("L5")];
    char stringpool_str46[sizeof("L2")];
    char stringpool_str49[sizeof("L8")];
    char stringpool_str53[sizeof("EUCCN")];
    char stringpool_str57[sizeof("ISO8859-1")];
    char stringpool_str58[sizeof("ISO8859-11")];
    char stringpool_str59[sizeof("ISO8859-6")];
    char stringpool_str60[sizeof("ISO8859-16")];
    char stringpool_str61[sizeof("ISO8859-3")];
    char stringpool_str62[sizeof("ISO8859-13")];
    char stringpool_str65[sizeof("ISO8859-5")];
    char stringpool_str66[sizeof("ISO8859-15")];
    char stringpool_str67[sizeof("ISO8859-2")];
    char stringpool_str70[sizeof("EUC-CN")];
    char stringpool_str73[sizeof("ISO8859-8")];
    char stringpool_str74[sizeof("ISO-8859-1")];
    char stringpool_str75[sizeof("ISO-8859-11")];
    char stringpool_str76[sizeof("ISO-8859-6")];
    char stringpool_str77[sizeof("ISO-8859-16")];
    char stringpool_str78[sizeof("ISO-8859-3")];
    char stringpool_str79[sizeof("ISO-8859-13")];
    char stringpool_str81[sizeof("ISO8859-9")];
    char stringpool_str82[sizeof("ISO-8859-5")];
    char stringpool_str83[sizeof("ISO-8859-15")];
    char stringpool_str84[sizeof("ISO-8859-2")];
    char stringpool_str85[sizeof("ISO646-CN")];
    char stringpool_str86[sizeof("R8")];
    char stringpool_str88[sizeof("L4")];
    char stringpool_str90[sizeof("ISO-8859-8")];
    char stringpool_str91[sizeof("CP949")];
    char stringpool_str92[sizeof("ISO_8859-1")];
    char stringpool_str93[sizeof("ISO_8859-11")];
    char stringpool_str94[sizeof("ISO_8859-6")];
    char stringpool_str95[sizeof("ISO_8859-16")];
    char stringpool_str96[sizeof("ISO_8859-3")];
    char stringpool_str97[sizeof("ISO_8859-13")];
    char stringpool_str98[sizeof("ISO-8859-9")];
    char stringpool_str99[sizeof("ISO_8859-16:2001")];
    char stringpool_str100[sizeof("ISO_8859-5")];
    char stringpool_str101[sizeof("ISO_8859-15")];
    char stringpool_str102[sizeof("ISO_8859-2")];
    char stringpool_str103[sizeof("LATIN1")];
    char stringpool_str105[sizeof("LATIN6")];
    char stringpool_str106[sizeof("CP154")];
    char stringpool_str107[sizeof("LATIN3")];
    char stringpool_str108[sizeof("ISO_8859-8")];
    char stringpool_str110[sizeof("ISO_8859-15:1998")];
    char stringpool_str111[sizeof("LATIN5")];
    char stringpool_str112[sizeof("CP1254")];
    char stringpool_str113[sizeof("LATIN2")];
    char stringpool_str114[sizeof("CSISO2022CN")];
    char stringpool_str116[sizeof("ISO_8859-9")];
    char stringpool_str117[sizeof("CHINESE")];
    char stringpool_str118[sizeof("ISO-IR-6")];
    char stringpool_str119[sizeof("LATIN8")];
    char stringpool_str120[sizeof("ASCII")];
    char stringpool_str121[sizeof("ISO-IR-166")];
    char stringpool_str122[sizeof("X0212")];
    char stringpool_str124[sizeof("VISCII")];
    char stringpool_str125[sizeof("ISO-IR-126")];
    char stringpool_str126[sizeof("CSASCII")];
    char stringpool_str127[sizeof("ISO-IR-165")];
    char stringpool_str129[sizeof("CSVISCII")];
    char stringpool_str130[sizeof("ISO-IR-226")];
    char stringpool_str131[sizeof("MAC")];
    char stringpool_str136[sizeof("ISO-IR-138")];
    char stringpool_str137[sizeof("ISO-IR-58")];
    char stringpool_str139[sizeof("IBM866")];
    char stringpool_str142[sizeof("ISO-2022-CN")];
    char stringpool_str143[sizeof("MS936")];
    char stringpool_str144[sizeof("LATIN-9")];
    char stringpool_str146[sizeof("ISO-IR-159")];
    char stringpool_str147[sizeof("IBM862")];
    char stringpool_str150[sizeof("US")];
    char stringpool_str151[sizeof("ISO8859-4")];
    char stringpool_str152[sizeof("ISO8859-14")];
    char stringpool_str153[sizeof("ISO_8859-14:1998")];
    char stringpool_str154[sizeof("ISO-IR-199")];
    char stringpool_str156[sizeof("UHC")];
    char stringpool_str157[sizeof("850")];
    char stringpool_str159[sizeof("HZ")];
    char stringpool_str160[sizeof("IBM819")];
    char stringpool_str162[sizeof("ISO-CELTIC")];
    char stringpool_str163[sizeof("ELOT_928")];
    char stringpool_str164[sizeof("CP1250")];
    char stringpool_str165[sizeof("GB2312")];
    char stringpool_str166[sizeof("CP850")];
    char stringpool_str168[sizeof("ISO-8859-4")];
    char stringpool_str169[sizeof("ISO-8859-14")];
    char stringpool_str170[sizeof("CP950")];
    char stringpool_str171[sizeof("CYRILLIC")];
    char stringpool_str176[sizeof("ISO_8859-10:1992")];
    char stringpool_str179[sizeof("UCS-2")];
    char stringpool_str180[sizeof("TCVN")];
    char stringpool_str181[sizeof("ISO-IR-148")];
    char stringpool_str185[sizeof("X0201")];
    char stringpool_str186[sizeof("ISO_8859-4")];
    char stringpool_str187[sizeof("ISO_8859-14")];
    char stringpool_str188[sizeof("L10")];
    char stringpool_str189[sizeof("ISO-IR-149")];
    char stringpool_str191[sizeof("ISO-IR-101")];
    char stringpool_str196[sizeof("ISO-2022-CN-EXT")];
    char stringpool_str197[sizeof("LATIN4")];
    char stringpool_str200[sizeof("ISO-IR-203")];
    char stringpool_str201[sizeof("X0208")];
    char stringpool_str202[sizeof("KSC_5601")];
    char stringpool_str204[sizeof("ISO8859-10")];
    char stringpool_str207[sizeof("VISCII1.1-1")];
    char stringpool_str209[sizeof("L7")];
    char stringpool_str211[sizeof("ISO-IR-14")];
    char stringpool_str212[sizeof("PT154")];
    char stringpool_str213[sizeof("TIS620")];
    char stringpool_str215[sizeof("ISO-IR-109")];
    char stringpool_str216[sizeof("CSUNICODE11")];
    char stringpool_str217[sizeof("KOI8-T")];
    char stringpool_str218[sizeof("RK1048")];
    char stringpool_str221[sizeof("ISO-8859-10")];
    char stringpool_str222[sizeof("TIS620.2533-1")];
    char stringpool_str223[sizeof("ISO646-US")];
    char stringpool_str224[sizeof("CSISOLATIN1")];
    char stringpool_str226[sizeof("CSISOLATIN6")];
    char stringpool_str228[sizeof("CSISOLATIN3")];
    char stringpool_str230[sizeof("TIS-620")];
    char stringpool_str232[sizeof("CSISOLATIN5")];
    char stringpool_str234[sizeof("CSISOLATIN2")];
    char stringpool_str235[sizeof("TIS620.2529-1")];
    char stringpool_str236[sizeof("CSKZ1048")];
    char stringpool_str237[sizeof("CSISOLATINCYRILLIC")];
    char stringpool_str238[sizeof("KZ-1048")];
    char stringpool_str239[sizeof("ISO_8859-10")];
    char stringpool_str241[sizeof("UNICODE-1-1")];
    char stringpool_str242[sizeof("UTF-16")];
    char stringpool_str245[sizeof("MS-EE")];
    char stringpool_str248[sizeof("CSUNICODE")];
    char stringpool_str249[sizeof("CSKOI8R")];
    char stringpool_str250[sizeof("LATIN10")];
    char stringpool_str252[sizeof("UTF-32")];
    char stringpool_str254[sizeof("CSUCS4")];
    char stringpool_str255[sizeof("UTF-8")];
    char stringpool_str259[sizeof("ISO-IR-144")];
    char stringpool_str261[sizeof("KOI8-R")];
    char stringpool_str262[sizeof("MS-ANSI")];
    char stringpool_str263[sizeof("UCS-4")];
    char stringpool_str264[sizeof("ISO-IR-110")];
    char stringpool_str266[sizeof("IBM-CP1133")];
    char stringpool_str267[sizeof("CSIBM866")];
    char stringpool_str270[sizeof("KS_C_5601-1989")];
    char stringpool_str271[sizeof("CHAR")];
    char stringpool_str273[sizeof("EUCKR")];
    char stringpool_str277[sizeof("BIG5")];
    char stringpool_str278[sizeof("CP874")];
    char stringpool_str279[sizeof("ARMSCII-8")];
    char stringpool_str282[sizeof("CSBIG5")];
    char stringpool_str283[sizeof("UCS-2LE")];
    char stringpool_str286[sizeof("IBM850")];
    char stringpool_str287[sizeof("US-ASCII")];
    char stringpool_str290[sizeof("EUC-KR")];
    char stringpool_str293[sizeof("CSGB2312")];
    char stringpool_str294[sizeof("BIG-5")];
    char stringpool_str295[sizeof("TIS620.2533-0")];
    char stringpool_str299[sizeof("CN-BIG5")];
    char stringpool_str302[sizeof("MACCYRILLIC")];
    char stringpool_str303[sizeof("GBK")];
    char stringpool_str304[sizeof("TIS620-0")];
    char stringpool_str305[sizeof("MS-CYRL")];
    char stringpool_str307[sizeof("CYRILLIC-ASIAN")];
    char stringpool_str308[sizeof("ECMA-118")];
    char stringpool_str310[sizeof("ISO-IR-179")];
    char stringpool_str311[sizeof("GREEK8")];
    char stringpool_str315[sizeof("KOREAN")];
    char stringpool_str318[sizeof("CSISOLATIN4")];
    char stringpool_str321[sizeof("ISO-10646-UCS-2")];
    char stringpool_str325[sizeof("UCS-4LE")];
    char stringpool_str326[sizeof("PTCP154")];
    char stringpool_str330[sizeof("CSISO14JISC6220RO")];
    char stringpool_str334[sizeof("CSISO2022KR")];
    char stringpool_str336[sizeof("ROMAN8")];
    char stringpool_str337[sizeof("ISO-IR-100")];
    char stringpool_str340[sizeof("JIS_C6226-1983")];
    char stringpool_str344[sizeof("CSISOLATINARABIC")];
    char stringpool_str347[sizeof("CP367")];
    char stringpool_str350[sizeof("UTF-16LE")];
    char stringpool_str351[sizeof("ISO_646.IRV:1991")];
    char stringpool_str354[sizeof("CP1257")];
    char stringpool_str355[sizeof("MACICELAND")];
    char stringpool_str356[sizeof("UTF-32LE")];
    char stringpool_str357[sizeof("CSKSC56011987")];
    char stringpool_str359[sizeof("ARABIC")];
    char stringpool_str362[sizeof("ISO-2022-KR")];
    char stringpool_str363[sizeof("ISO-10646-UCS-4")];
    char stringpool_str367[sizeof("UCS-2BE")];
    char stringpool_str368[sizeof("GB_2312-80")];
    char stringpool_str369[sizeof("JP")];
    char stringpool_str371[sizeof("MULELAO-1")];
    char stringpool_str372[sizeof("CSISO159JISX02121990")];
    char stringpool_str373[sizeof("GREEK")];
    char stringpool_str375[sizeof("TCVN5712-1")];
    char stringpool_str376[sizeof("CSISO58GB231280")];
    char stringpool_str378[sizeof("GB18030")];
    char stringpool_str379[sizeof("TCVN-5712")];
    char stringpool_str384[sizeof("GB_1988-80")];
    char stringpool_str385[sizeof("CSPTCP154")];
    char stringpool_str386[sizeof("ECMA-114")];
    char stringpool_str388[sizeof("CSUNICODE11UTF7")];
    char stringpool_str391[sizeof("ANSI_X3.4-1986")];
    char stringpool_str392[sizeof("UNICODELITTLE")];
    char stringpool_str393[sizeof("ISO8859-7")];
    char stringpool_str395[sizeof("CN-GB-ISOIR165")];
    char stringpool_str396[sizeof("STRK1048-2002")];
    char stringpool_str398[sizeof("ANSI_X3.4-1968")];
    char stringpool_str403[sizeof("KOI8-U")];
    char stringpool_str406[sizeof("UCS-2-INTERNAL")];
    char stringpool_str409[sizeof("UCS-4BE")];
    char stringpool_str410[sizeof("ISO-8859-7")];
    char stringpool_str411[sizeof("SHIFT-JIS")];
    char stringpool_str412[sizeof("CN-GB")];
    char stringpool_str413[sizeof("JIS_C6220-1969-RO")];
    char stringpool_str415[sizeof("UNICODE-1-1-UTF-7")];
    char stringpool_str416[sizeof("WINDOWS-1251")];
    char stringpool_str417[sizeof("WINDOWS-1256")];
    char stringpool_str418[sizeof("WINDOWS-1253")];
    char stringpool_str420[sizeof("WINDOWS-1255")];
    char stringpool_str421[sizeof("WINDOWS-1252")];
    char stringpool_str422[sizeof("WINDOWS-936")];
    char stringpool_str424[sizeof("WINDOWS-1258")];
    char stringpool_str425[sizeof("CSEUCKR")];
    char stringpool_str426[sizeof("KS_C_5601-1987")];
    char stringpool_str428[sizeof("ISO_8859-7")];
    char stringpool_str429[sizeof("SHIFT_JIS")];
    char stringpool_str433[sizeof("JIS0208")];
    char stringpool_str434[sizeof("UTF-16BE")];
    char stringpool_str439[sizeof("LATIN7")];
    char stringpool_str440[sizeof("UTF-32BE")];
    char stringpool_str445[sizeof("MACTHAI")];
    char stringpool_str448[sizeof("UCS-4-INTERNAL")];
    char stringpool_str449[sizeof("CSISOLATINGREEK")];
    char stringpool_str451[sizeof("MACROMAN")];
    char stringpool_str452[sizeof("EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE")];
    char stringpool_str456[sizeof("EUCTW")];
    char stringpool_str457[sizeof("ISO-IR-57")];
    char stringpool_str458[sizeof("ISO-IR-157")];
    char stringpool_str459[sizeof("ISO-IR-127")];
    char stringpool_str461[sizeof("ISO-IR-87")];
    char stringpool_str463[sizeof("WINDOWS-1254")];
    char stringpool_str464[sizeof("ISO_8859-3:1988")];
    char stringpool_str466[sizeof("ISO_8859-5:1988")];
    char stringpool_str467[sizeof("IBM367")];
    char stringpool_str470[sizeof("ISO_8859-8:1988")];
    char stringpool_str471[sizeof("HZ-GB-2312")];
    char stringpool_str473[sizeof("EUC-TW")];
    char stringpool_str474[sizeof("CSISO57GB1988")];
    char stringpool_str475[sizeof("NEXTSTEP")];
    char stringpool_str476[sizeof("CSISO2022JP2")];
    char stringpool_str478[sizeof("ISO_8859-9:1989")];
    char stringpool_str480[sizeof("KOI8-RU")];
    char stringpool_str487[sizeof("MACINTOSH")];
    char stringpool_str489[sizeof("WINDOWS-1250")];
    char stringpool_str492[sizeof("JIS_X0212")];
    char stringpool_str500[sizeof("ISO-2022-JP-1")];
    char stringpool_str501[sizeof("MACCROATIAN")];
    char stringpool_str502[sizeof("HP-ROMAN8")];
    char stringpool_str505[sizeof("ISO-2022-JP-2")];
    char stringpool_str509[sizeof("ISO_8859-4:1988")];
    char stringpool_str510[sizeof("BIG5HKSCS")];
    char stringpool_str515[sizeof("ASMO-708")];
    char stringpool_str518[sizeof("EUCJP")];
    char stringpool_str525[sizeof("BIGFIVE")];
    char stringpool_str527[sizeof("BIG5-HKSCS")];
    char stringpool_str531[sizeof("MACCENTRALEUROPE")];
    char stringpool_str532[sizeof("CSPC862LATINHEBREW")];
    char stringpool_str535[sizeof("EUC-JP")];
    char stringpool_str542[sizeof("BIG-FIVE")];
    char stringpool_str546[sizeof("CSSHIFTJIS")];
    char stringpool_str550[sizeof("ISO646-JP")];
    char stringpool_str554[sizeof("JISX0201-1976")];
    char stringpool_str555[sizeof("JIS_X0201")];
    char stringpool_str556[sizeof("CSISOLATINHEBREW")];
    char stringpool_str563[sizeof("MACARABIC")];
    char stringpool_str564[sizeof("CSISO87JISX0208")];
    char stringpool_str571[sizeof("JIS_X0208")];
    char stringpool_str575[sizeof("UTF-7")];
    char stringpool_str577[sizeof("MACGREEK")];
    char stringpool_str579[sizeof("CSISO2022JP")];
    char stringpool_str580[sizeof("MS-TURK")];
    char stringpool_str581[sizeof("JIS_X0212-1990")];
    char stringpool_str584[sizeof("WINDOWS-1257")];
    char stringpool_str586[sizeof("JIS_X0208-1983")];
    char stringpool_str590[sizeof("MS-GREEK")];
    char stringpool_str599[sizeof("CSHPROMAN8")];
    char stringpool_str600[sizeof("JAVA")];
    char stringpool_str601[sizeof("MS-HEBR")];
    char stringpool_str604[sizeof("CSMACINTOSH")];
    char stringpool_str607[sizeof("ISO-2022-JP")];
    char stringpool_str608[sizeof("CSEUCTW")];
    char stringpool_str614[sizeof("GEORGIAN-PS")];
    char stringpool_str615[sizeof("UNICODEBIG")];
    char stringpool_str617[sizeof("MS_KANJI")];
    char stringpool_str620[sizeof("CSPC850MULTILINGUAL")];
    char stringpool_str621[sizeof("MACUKRAINE")];
    char stringpool_str622[sizeof("ISO_8859-1:1987")];
    char stringpool_str623[sizeof("ISO_8859-6:1987")];
    char stringpool_str624[sizeof("ISO_8859-7:2003")];
    char stringpool_str626[sizeof("GEORGIAN-ACADEMY")];
    char stringpool_str627[sizeof("ISO_8859-2:1987")];
    char stringpool_str629[sizeof("JIS_X0212.1990-0")];
    char stringpool_str657[sizeof("JIS_X0208-1990")];
    char stringpool_str664[sizeof("WCHAR_T")];
    char stringpool_str673[sizeof("MACROMANIA")];
    char stringpool_str676[sizeof("WINDOWS-874")];
    char stringpool_str689[sizeof("CSEUCPKDFMTJAPANESE")];
    char stringpool_str691[sizeof("MS-ARAB")];
    char stringpool_str723[sizeof("UCS-2-SWAPPED")];
    char stringpool_str739[sizeof("TCVN5712-1:1993")];
    char stringpool_str746[sizeof("HEBREW")];
    char stringpool_str765[sizeof("UCS-4-SWAPPED")];
    char stringpool_str768[sizeof("JOHAB")];
    char stringpool_str786[sizeof("MACTURKISH")];
    char stringpool_str790[sizeof("ISO_8859-7:1987")];
    char stringpool_str842[sizeof("WINBALTRIM")];
    char stringpool_str888[sizeof("BIG5-HKSCS:2001")];
    char stringpool_str898[sizeof("CSHALFWIDTHKATAKANA")];
    char stringpool_str900[sizeof("BIG5-HKSCS:1999")];
    char stringpool_str908[sizeof("MACHEBREW")];
    char stringpool_str935[sizeof("BIG5-HKSCS:2004")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "SJIS",
    "CN",
    "CP1131",
    "CP1361",
    "866",
    "CP1133",
    "CP1251",
    "CP866",
    "CP1256",
    "862",
    "CP1253",
    "CP936",
    "CP1255",
    "CP862",
    "CP1252",
    "C99",
    "CP932",
    "CP1258",
    "CP819",
    "L1",
    "L6",
    "L3",
    "L5",
    "L2",
    "L8",
    "EUCCN",
    "ISO8859-1",
    "ISO8859-11",
    "ISO8859-6",
    "ISO8859-16",
    "ISO8859-3",
    "ISO8859-13",
    "ISO8859-5",
    "ISO8859-15",
    "ISO8859-2",
    "EUC-CN",
    "ISO8859-8",
    "ISO-8859-1",
    "ISO-8859-11",
    "ISO-8859-6",
    "ISO-8859-16",
    "ISO-8859-3",
    "ISO-8859-13",
    "ISO8859-9",
    "ISO-8859-5",
    "ISO-8859-15",
    "ISO-8859-2",
    "ISO646-CN",
    "R8",
    "L4",
    "ISO-8859-8",
    "CP949",
    "ISO_8859-1",
    "ISO_8859-11",
    "ISO_8859-6",
    "ISO_8859-16",
    "ISO_8859-3",
    "ISO_8859-13",
    "ISO-8859-9",
    "ISO_8859-16:2001",
    "ISO_8859-5",
    "ISO_8859-15",
    "ISO_8859-2",
    "LATIN1",
    "LATIN6",
    "CP154",
    "LATIN3",
    "ISO_8859-8",
    "ISO_8859-15:1998",
    "LATIN5",
    "CP1254",
    "LATIN2",
    "CSISO2022CN",
    "ISO_8859-9",
    "CHINESE",
    "ISO-IR-6",
    "LATIN8",
    "ASCII",
    "ISO-IR-166",
    "X0212",
    "VISCII",
    "ISO-IR-126",
    "CSASCII",
    "ISO-IR-165",
    "CSVISCII",
    "ISO-IR-226",
    "MAC",
    "ISO-IR-138",
    "ISO-IR-58",
    "IBM866",
    "ISO-2022-CN",
    "MS936",
    "LATIN-9",
    "ISO-IR-159",
    "IBM862",
    "US",
    "ISO8859-4",
    "ISO8859-14",
    "ISO_8859-14:1998",
    "ISO-IR-199",
    "UHC",
    "850",
    "HZ",
    "IBM819",
    "ISO-CELTIC",
    "ELOT_928",
    "CP1250",
    "GB2312",
    "CP850",
    "ISO-8859-4",
    "ISO-8859-14",
    "CP950",
    "CYRILLIC",
    "ISO_8859-10:1992",
    "UCS-2",
    "TCVN",
    "ISO-IR-148",
    "X0201",
    "ISO_8859-4",
    "ISO_8859-14",
    "L10",
    "ISO-IR-149",
    "ISO-IR-101",
    "ISO-2022-CN-EXT",
    "LATIN4",
    "ISO-IR-203",
    "X0208",
    "KSC_5601",
    "ISO8859-10",
    "VISCII1.1-1",
    "L7",
    "ISO-IR-14",
    "PT154",
    "TIS620",
    "ISO-IR-109",
    "CSUNICODE11",
    "KOI8-T",
    "RK1048",
    "ISO-8859-10",
    "TIS620.2533-1",
    "ISO646-US",
    "CSISOLATIN1",
    "CSISOLATIN6",
    "CSISOLATIN3",
    "TIS-620",
    "CSISOLATIN5",
    "CSISOLATIN2",
    "TIS620.2529-1",
    "CSKZ1048",
    "CSISOLATINCYRILLIC",
    "KZ-1048",
    "ISO_8859-10",
    "UNICODE-1-1",
    "UTF-16",
    "MS-EE",
    "CSUNICODE",
    "CSKOI8R",
    "LATIN10",
    "UTF-32",
    "CSUCS4",
    "UTF-8",
    "ISO-IR-144",
    "KOI8-R",
    "MS-ANSI",
    "UCS-4",
    "ISO-IR-110",
    "IBM-CP1133",
    "CSIBM866",
    "KS_C_5601-1989",
    "CHAR",
    "EUCKR",
    "BIG5",
    "CP874",
    "ARMSCII-8",
    "CSBIG5",
    "UCS-2LE",
    "IBM850",
    "US-ASCII",
    "EUC-KR",
    "CSGB2312",
    "BIG-5",
    "TIS620.2533-0",
    "CN-BIG5",
    "MACCYRILLIC",
    "GBK",
    "TIS620-0",
    "MS-CYRL",
    "CYRILLIC-ASIAN",
    "ECMA-118",
    "ISO-IR-179",
    "GREEK8",
    "KOREAN",
    "CSISOLATIN4",
    "ISO-10646-UCS-2",
    "UCS-4LE",
    "PTCP154",
    "CSISO14JISC6220RO",
    "CSISO2022KR",
    "ROMAN8",
    "ISO-IR-100",
    "JIS_C6226-1983",
    "CSISOLATINARABIC",
    "CP367",
    "UTF-16LE",
    "ISO_646.IRV:1991",
    "CP1257",
    "MACICELAND",
    "UTF-32LE",
    "CSKSC56011987",
    "ARABIC",
    "ISO-2022-KR",
    "ISO-10646-UCS-4",
    "UCS-2BE",
    "GB_2312-80",
    "JP",
    "MULELAO-1",
    "CSISO159JISX02121990",
    "GREEK",
    "TCVN5712-1",
    "CSISO58GB231280",
    "GB18030",
    "TCVN-5712",
    "GB_1988-80",
    "CSPTCP154",
    "ECMA-114",
    "CSUNICODE11UTF7",
    "ANSI_X3.4-1986",
    "UNICODELITTLE",
    "ISO8859-7",
    "CN-GB-ISOIR165",
    "STRK1048-2002",
    "ANSI_X3.4-1968",
    "KOI8-U",
    "UCS-2-INTERNAL",
    "UCS-4BE",
    "ISO-8859-7",
    "SHIFT-JIS",
    "CN-GB",
    "JIS_C6220-1969-RO",
    "UNICODE-1-1-UTF-7",
    "WINDOWS-1251",
    "WINDOWS-1256",
    "WINDOWS-1253",
    "WINDOWS-1255",
    "WINDOWS-1252",
    "WINDOWS-936",
    "WINDOWS-1258",
    "CSEUCKR",
    "KS_C_5601-1987",
    "ISO_8859-7",
    "SHIFT_JIS",
    "JIS0208",
    "UTF-16BE",
    "LATIN7",
    "UTF-32BE",
    "MACTHAI",
    "UCS-4-INTERNAL",
    "CSISOLATINGREEK",
    "MACROMAN",
    "EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE",
    "EUCTW",
    "ISO-IR-57",
    "ISO-IR-157",
    "ISO-IR-127",
    "ISO-IR-87",
    "WINDOWS-1254",
    "ISO_8859-3:1988",
    "ISO_8859-5:1988",
    "IBM367",
    "ISO_8859-8:1988",
    "HZ-GB-2312",
    "EUC-TW",
    "CSISO57GB1988",
    "NEXTSTEP",
    "CSISO2022JP2",
    "ISO_8859-9:1989",
    "KOI8-RU",
    "MACINTOSH",
    "WINDOWS-1250",
    "JIS_X0212",
    "ISO-2022-JP-1",
    "MACCROATIAN",
    "HP-ROMAN8",
    "ISO-2022-JP-2",
    "ISO_8859-4:1988",
    "BIG5HKSCS",
    "ASMO-708",
    "EUCJP",
    "BIGFIVE",
    "BIG5-HKSCS",
    "MACCENTRALEUROPE",
    "CSPC862LATINHEBREW",
    "EUC-JP",
    "BIG-FIVE",
    "CSSHIFTJIS",
    "ISO646-JP",
    "JISX0201-1976",
    "JIS_X0201",
    "CSISOLATINHEBREW",
    "MACARABIC",
    "CSISO87JISX0208",
    "JIS_X0208",
    "UTF-7",
    "MACGREEK",
    "CSISO2022JP",
    "MS-TURK",
    "JIS_X0212-1990",
    "WINDOWS-1257",
    "JIS_X0208-1983",
    "MS-GREEK",
    "CSHPROMAN8",
    "JAVA",
    "MS-HEBR",
    "CSMACINTOSH",
    "ISO-2022-JP",
    "CSEUCTW",
    "GEORGIAN-PS",
    "UNICODEBIG",
    "MS_KANJI",
    "CSPC850MULTILINGUAL",
    "MACUKRAINE",
    "ISO_8859-1:1987",
    "ISO_8859-6:1987",
    "ISO_8859-7:2003",
    "GEORGIAN-ACADEMY",
    "ISO_8859-2:1987",
    "JIS_X0212.1990-0",
    "JIS_X0208-1990",
    "WCHAR_T",
    "MACROMANIA",
    "WINDOWS-874",
    "CSEUCPKDFMTJAPANESE",
    "MS-ARAB",
    "UCS-2-SWAPPED",
    "TCVN5712-1:1993",
    "HEBREW",
    "UCS-4-SWAPPED",
    "JOHAB",
    "MACTURKISH",
    "ISO_8859-7:1987",
    "WINBALTRIM",
    "BIG5-HKSCS:2001",
    "CSHALFWIDTHKATAKANA",
    "BIG5-HKSCS:1999",
    "MACHEBREW",
    "BIG5-HKSCS:2004"
  };
#define stringpool ((const char *) &stringpool_contents)

static const struct alias aliases[] =
  {
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 308 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str7, ei_sjis},
    {-1},
#line 288 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str9, ei_iso646_cn},
    {-1},
#line 209 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str11, ei_cp1131},
#line 353 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str12, ei_johab},
#line 207 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str13, ei_cp866},
    {-1},
#line 244 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str15, ei_cp1133},
    {-1}, {-1},
#line 174 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str18, ei_cp1251},
#line 205 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str19, ei_cp866},
#line 189 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str20, ei_cp1256},
#line 203 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str21, ei_cp862},
#line 180 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str22, ei_cp1253},
    {-1},
#line 323 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str24, ei_cp936},
    {-1},
#line 186 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str26, ei_cp1255},
#line 201 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, ei_cp862},
#line 177 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str28, ei_cp1252},
    {-1},
#line 51 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str30, ei_c99},
    {-1},
#line 311 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str32, ei_cp932},
    {-1},
#line 195 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str34, ei_cp1258},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 57 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str40, ei_iso8859_1},
#line 60 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str41, ei_iso8859_1},
#line 134 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str42, ei_iso8859_10},
#line 76 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str43, ei_iso8859_3},
    {-1},
#line 126 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str45, ei_iso8859_9},
#line 68 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str46, ei_iso8859_2},
    {-1}, {-1},
#line 151 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str49, ei_iso8859_14},
    {-1}, {-1}, {-1},
#line 318 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str53, ei_euc_cn},
    {-1}, {-1}, {-1},
#line 62 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str57, ei_iso8859_1},
#line 139 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str58, ei_iso8859_11},
#line 102 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str59, ei_iso8859_6},
#line 166 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str60, ei_iso8859_16},
#line 78 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str61, ei_iso8859_3},
#line 145 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str62, ei_iso8859_13},
    {-1}, {-1},
#line 93 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str65, ei_iso8859_5},
#line 159 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str66, ei_iso8859_15},
#line 70 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str67, ei_iso8859_2},
    {-1}, {-1},
#line 317 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str70, ei_euc_cn},
    {-1}, {-1},
#line 120 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str73, ei_iso8859_8},
#line 53 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, ei_iso8859_1},
#line 137 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str75, ei_iso8859_11},
#line 94 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str76, ei_iso8859_6},
#line 160 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str77, ei_iso8859_16},
#line 71 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str78, ei_iso8859_3},
#line 140 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str79, ei_iso8859_13},
    {-1},
#line 128 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str81, ei_iso8859_9},
#line 87 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str82, ei_iso8859_5},
#line 154 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str83, ei_iso8859_15},
#line 63 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str84, ei_iso8859_2},
#line 286 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str85, ei_iso646_cn},
#line 227 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str86, ei_hp_roman8},
    {-1},
#line 84 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str88, ei_iso8859_4},
    {-1},
#line 114 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str90, ei_iso8859_8},
#line 350 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str91, ei_cp949},
#line 54 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str92, ei_iso8859_1},
#line 138 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str93, ei_iso8859_11},
#line 95 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str94, ei_iso8859_6},
#line 161 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str95, ei_iso8859_16},
#line 72 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str96, ei_iso8859_3},
#line 141 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str97, ei_iso8859_13},
#line 121 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str98, ei_iso8859_9},
#line 162 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str99, ei_iso8859_16},
#line 88 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str100, ei_iso8859_5},
#line 155 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str101, ei_iso8859_15},
#line 64 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str102, ei_iso8859_2},
#line 59 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str103, ei_iso8859_1},
    {-1},
#line 133 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str105, ei_iso8859_10},
#line 236 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str106, ei_pt154},
#line 75 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str107, ei_iso8859_3},
#line 115 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str108, ei_iso8859_8},
    {-1},
#line 156 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str110, ei_iso8859_15},
#line 125 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str111, ei_iso8859_9},
#line 183 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str112, ei_cp1254},
#line 67 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str113, ei_iso8859_2},
#line 328 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str114, ei_iso2022_cn},
    {-1},
#line 122 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str116, ei_iso8859_9},
#line 293 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str117, ei_gb2312},
#line 16 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str118, ei_ascii},
#line 150 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str119, ei_iso8859_14},
#line 13 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str120, ei_ascii},
#line 252 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str121, ei_tis620},
#line 282 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str122, ei_jisx0212},
    {-1},
#line 255 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str124, ei_viscii},
#line 107 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str125, ei_iso8859_7},
#line 22 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str126, ei_ascii},
#line 294 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str127, ei_isoir165},
    {-1},
#line 257 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str129, ei_viscii},
#line 163 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str130, ei_iso8859_16},
#line 212 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str131, ei_mac_roman},
    {-1}, {-1}, {-1}, {-1},
#line 117 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str136, ei_iso8859_8},
#line 291 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str137, ei_gb2312},
    {-1},
#line 206 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str139, ei_cp866},
    {-1}, {-1},
#line 327 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str142, ei_iso2022_cn},
#line 324 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str143, ei_cp936},
#line 158 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str144, ei_iso8859_15},
    {-1},
#line 283 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str146, ei_jisx0212},
#line 202 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str147, ei_cp862},
    {-1}, {-1},
#line 21 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str150, ei_ascii},
#line 86 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str151, ei_iso8859_4},
#line 153 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str152, ei_iso8859_14},
#line 148 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str153, ei_iso8859_14},
#line 149 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str154, ei_iso8859_14},
    {-1},
#line 351 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str156, ei_cp949},
#line 199 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str157, ei_cp850},
    {-1},
#line 330 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str159, ei_hz},
#line 58 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str160, ei_iso8859_1},
    {-1},
#line 152 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str162, ei_iso8859_14},
#line 109 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str163, ei_iso8859_7},
#line 171 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str164, ei_cp1250},
#line 319 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str165, ei_euc_cn},
#line 197 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str166, ei_cp850},
    {-1},
#line 79 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str168, ei_iso8859_4},
#line 146 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str169, ei_iso8859_14},
#line 341 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str170, ei_cp950},
#line 91 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str171, ei_iso8859_5},
    {-1}, {-1}, {-1}, {-1},
#line 131 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str176, ei_iso8859_10},
    {-1}, {-1},
#line 24 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str179, ei_ucs2},
#line 258 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str180, ei_tcvn},
#line 124 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str181, ei_iso8859_9},
    {-1}, {-1}, {-1},
#line 269 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str185, ei_jisx0201},
#line 80 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str186, ei_iso8859_4},
#line 147 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str187, ei_iso8859_14},
#line 165 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str188, ei_iso8859_16},
#line 299 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str189, ei_ksc5601},
    {-1},
#line 66 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str191, ei_iso8859_2},
    {-1}, {-1}, {-1}, {-1},
#line 329 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str196, ei_iso2022_cn_ext},
#line 83 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str197, ei_iso8859_4},
    {-1}, {-1},
#line 157 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str200, ei_iso8859_15},
#line 275 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str201, ei_jisx0208},
#line 296 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str202, ei_ksc5601},
    {-1},
#line 136 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str204, ei_iso8859_10},
    {-1}, {-1},
#line 256 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str207, ei_viscii},
    {-1},
#line 144 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str209, ei_iso8859_13},
    {-1},
#line 264 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str211, ei_iso646_jp},
#line 234 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str212, ei_pt154},
#line 247 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str213, ei_tis620},
    {-1},
#line 74 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str215, ei_iso8859_3},
#line 30 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str216, ei_ucs2be},
#line 233 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str217, ei_koi8_t},
#line 239 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str218, ei_rk1048},
    {-1}, {-1},
#line 129 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str221, ei_iso8859_10},
#line 251 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str222, ei_tis620},
#line 14 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str223, ei_ascii},
#line 61 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str224, ei_iso8859_1},
    {-1},
#line 135 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str226, ei_iso8859_10},
    {-1},
#line 77 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str228, ei_iso8859_3},
    {-1},
#line 246 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str230, ei_tis620},
    {-1},
#line 127 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str232, ei_iso8859_9},
    {-1},
#line 69 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str234, ei_iso8859_2},
#line 249 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str235, ei_tis620},
#line 242 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str236, ei_rk1048},
#line 92 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str237, ei_iso8859_5},
#line 241 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str238, ei_rk1048},
#line 130 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str239, ei_iso8859_10},
    {-1},
#line 29 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str241, ei_ucs2be},
#line 38 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str242, ei_utf16},
    {-1}, {-1},
#line 173 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str245, ei_cp1250},
    {-1}, {-1},
#line 26 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str248, ei_ucs2},
#line 168 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str249, ei_koi8_r},
#line 164 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str250, ei_iso8859_16},
    {-1},
#line 41 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str252, ei_utf32},
    {-1},
#line 35 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str254, ei_ucs4},
#line 23 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str255, ei_utf8},
    {-1}, {-1}, {-1},
#line 90 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str259, ei_iso8859_5},
    {-1},
#line 167 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str261, ei_koi8_r},
#line 179 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str262, ei_cp1252},
#line 33 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str263, ei_ucs4},
#line 82 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str264, ei_iso8859_4},
    {-1},
#line 245 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str266, ei_cp1133},
#line 208 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str267, ei_cp866},
    {-1}, {-1},
#line 298 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str270, ei_ksc5601},
#line 356 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str271, ei_local_char},
    {-1},
#line 348 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str273, ei_euc_kr},
    {-1}, {-1}, {-1},
#line 335 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str277, ei_ces_big5},
#line 253 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str278, ei_cp874},
#line 230 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str279, ei_armscii_8},
    {-1}, {-1},
#line 340 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str282, ei_ces_big5},
#line 31 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str283, ei_ucs2le},
    {-1}, {-1},
#line 198 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str286, ei_cp850},
#line 12 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str287, ei_ascii},
    {-1}, {-1},
#line 347 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str290, ei_euc_kr},
    {-1}, {-1},
#line 321 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str293, ei_euc_cn},
#line 336 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str294, ei_ces_big5},
#line 250 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str295, ei_tis620},
    {-1}, {-1}, {-1},
#line 339 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str299, ei_ces_big5},
    {-1}, {-1},
#line 218 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str302, ei_mac_cyrillic},
#line 322 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str303, ei_ces_gbk},
#line 248 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str304, ei_tis620},
#line 176 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str305, ei_cp1251},
    {-1},
#line 237 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str307, ei_pt154},
#line 108 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str308, ei_iso8859_7},
    {-1},
#line 142 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str310, ei_iso8859_13},
#line 110 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str311, ei_iso8859_7},
    {-1}, {-1}, {-1},
#line 301 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str315, ei_ksc5601},
    {-1}, {-1},
#line 85 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str318, ei_iso8859_4},
    {-1}, {-1},
#line 25 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str321, ei_ucs2},
    {-1}, {-1}, {-1},
#line 37 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str325, ei_ucs4le},
#line 235 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str326, ei_pt154},
    {-1}, {-1}, {-1},
#line 266 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str330, ei_iso646_jp},
    {-1}, {-1}, {-1},
#line 355 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str334, ei_iso2022_kr},
    {-1},
#line 226 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str336, ei_hp_roman8},
#line 56 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str337, ei_iso8859_1},
    {-1}, {-1},
#line 277 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str340, ei_jisx0208},
    {-1}, {-1}, {-1},
#line 101 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str344, ei_iso8859_6},
    {-1}, {-1},
#line 19 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str347, ei_ascii},
    {-1}, {-1},
#line 40 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str350, ei_utf16le},
#line 15 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str351, ei_ascii},
    {-1}, {-1},
#line 192 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str354, ei_cp1257},
#line 215 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str355, ei_mac_iceland},
#line 43 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str356, ei_utf32le},
#line 300 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str357, ei_ksc5601},
    {-1},
#line 100 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str359, ei_iso8859_6},
    {-1}, {-1},
#line 354 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str362, ei_iso2022_kr},
#line 34 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str363, ei_ucs4},
    {-1}, {-1}, {-1},
#line 27 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str367, ei_ucs2be},
#line 290 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str368, ei_gb2312},
#line 265 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str369, ei_iso646_jp},
    {-1},
#line 243 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str371, ei_mulelao},
#line 284 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str372, ei_jisx0212},
#line 111 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str373, ei_iso8859_7},
    {-1},
#line 260 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str375, ei_tcvn},
#line 292 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str376, ei_gb2312},
    {-1},
#line 326 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str378, ei_gb18030},
#line 259 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str379, ei_tcvn},
    {-1}, {-1}, {-1}, {-1},
#line 285 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str384, ei_iso646_cn},
#line 238 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str385, ei_pt154},
#line 98 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str386, ei_iso8859_6},
    {-1},
#line 46 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str388, ei_utf7},
    {-1}, {-1},
#line 18 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str391, ei_ascii},
#line 32 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str392, ei_ucs2le},
#line 113 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str393, ei_iso8859_7},
    {-1},
#line 295 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str395, ei_isoir165},
#line 240 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str396, ei_rk1048},
    {-1},
#line 17 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str398, ei_ascii},
    {-1}, {-1}, {-1}, {-1},
#line 169 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str403, ei_koi8_u},
    {-1}, {-1},
#line 47 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str406, ei_ucs2internal},
    {-1}, {-1},
#line 36 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str409, ei_ucs4be},
#line 103 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str410, ei_iso8859_7},
#line 307 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str411, ei_sjis},
#line 320 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str412, ei_euc_cn},
#line 262 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str413, ei_iso646_jp},
    {-1},
#line 45 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str415, ei_utf7},
#line 175 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str416, ei_cp1251},
#line 190 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str417, ei_cp1256},
#line 181 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str418, ei_cp1253},
    {-1},
#line 187 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str420, ei_cp1255},
#line 178 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str421, ei_cp1252},
#line 325 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str422, ei_cp936},
    {-1},
#line 196 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str424, ei_cp1258},
#line 349 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str425, ei_euc_kr},
#line 297 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str426, ei_ksc5601},
    {-1},
#line 104 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str428, ei_iso8859_7},
#line 306 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str429, ei_sjis},
    {-1}, {-1}, {-1},
#line 274 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str433, ei_jisx0208},
#line 39 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str434, ei_utf16be},
    {-1}, {-1}, {-1}, {-1},
#line 143 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str439, ei_iso8859_13},
#line 42 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str440, ei_utf32be},
    {-1}, {-1}, {-1}, {-1},
#line 224 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str445, ei_mac_thai},
    {-1}, {-1},
#line 49 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str448, ei_ucs4internal},
#line 112 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str449, ei_iso8859_7},
    {-1},
#line 210 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str451, ei_mac_roman},
#line 304 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str452, ei_euc_jp},
    {-1}, {-1}, {-1},
#line 333 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str456, ei_euc_tw},
#line 287 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str457, ei_iso646_cn},
#line 132 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str458, ei_iso8859_10},
#line 97 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str459, ei_iso8859_6},
    {-1},
#line 276 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str461, ei_jisx0208},
    {-1},
#line 184 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str463, ei_cp1254},
#line 73 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str464, ei_iso8859_3},
    {-1},
#line 89 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str466, ei_iso8859_5},
#line 20 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str467, ei_ascii},
    {-1}, {-1},
#line 116 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str470, ei_iso8859_8},
#line 331 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str471, ei_hz},
    {-1},
#line 332 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str473, ei_euc_tw},
#line 289 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str474, ei_iso646_cn},
#line 229 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str475, ei_nextstep},
#line 316 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str476, ei_iso2022_jp2},
    {-1},
#line 123 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str478, ei_iso8859_9},
    {-1},
#line 170 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str480, ei_koi8_ru},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 211 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str487, ei_mac_roman},
    {-1},
#line 172 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str489, ei_cp1250},
    {-1}, {-1},
#line 279 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str492, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 314 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str500, ei_iso2022_jp1},
#line 216 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str501, ei_mac_croatian},
#line 225 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str502, ei_hp_roman8},
    {-1}, {-1},
#line 315 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str505, ei_iso2022_jp2},
    {-1}, {-1}, {-1},
#line 81 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str509, ei_iso8859_4},
#line 345 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str510, ei_big5hkscs2004},
    {-1}, {-1}, {-1}, {-1},
#line 99 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str515, ei_iso8859_6},
    {-1}, {-1},
#line 303 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str518, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 338 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str525, ei_ces_big5},
    {-1},
#line 344 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str527, ei_big5hkscs2004},
    {-1}, {-1}, {-1},
#line 214 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str531, ei_mac_centraleurope},
#line 204 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str532, ei_cp862},
    {-1}, {-1},
#line 302 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str535, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 337 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str542, ei_ces_big5},
    {-1}, {-1}, {-1},
#line 310 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str546, ei_sjis},
    {-1}, {-1}, {-1},
#line 263 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str550, ei_iso646_jp},
    {-1}, {-1}, {-1},
#line 268 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str554, ei_jisx0201},
#line 267 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str555, ei_jisx0201},
#line 119 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str556, ei_iso8859_8},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 223 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str563, ei_mac_arabic},
#line 278 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str564, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 271 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str571, ei_jisx0208},
    {-1}, {-1}, {-1},
#line 44 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str575, ei_utf7},
    {-1},
#line 220 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str577, ei_mac_greek},
    {-1},
#line 313 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str579, ei_iso2022_jp},
#line 185 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str580, ei_cp1254},
#line 281 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str581, ei_jisx0212},
    {-1}, {-1},
#line 193 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str584, ei_cp1257},
    {-1},
#line 272 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str586, ei_jisx0208},
    {-1}, {-1}, {-1},
#line 182 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str590, ei_cp1253},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 228 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str599, ei_hp_roman8},
#line 52 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str600, ei_java},
#line 188 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str601, ei_cp1255},
    {-1}, {-1},
#line 213 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str604, ei_mac_roman},
    {-1}, {-1},
#line 312 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str607, ei_iso2022_jp},
#line 334 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str608, ei_euc_tw},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 232 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str614, ei_georgian_ps},
#line 28 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str615, ei_ucs2be},
    {-1},
#line 309 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str617, ei_sjis},
    {-1}, {-1},
#line 200 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str620, ei_cp850},
#line 219 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str621, ei_mac_ukraine},
#line 55 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str622, ei_iso8859_1},
#line 96 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str623, ei_iso8859_6},
#line 106 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str624, ei_iso8859_7},
    {-1},
#line 231 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str626, ei_georgian_academy},
#line 65 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str627, ei_iso8859_2},
    {-1},
#line 280 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str629, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 273 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str657, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 357 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str664, ei_local_wchar_t},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 217 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str673, ei_mac_romania},
    {-1}, {-1},
#line 254 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str676, ei_cp874},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 305 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str689, ei_euc_jp},
    {-1},
#line 191 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str691, ei_cp1256},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 48 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str723, ei_ucs2swapped},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 261 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str739, ei_tcvn},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 118 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str746, ei_iso8859_8},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 50 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str765, ei_ucs4swapped},
    {-1}, {-1},
#line 352 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str768, ei_johab},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 221 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str786, ei_mac_turkish},
    {-1}, {-1}, {-1},
#line 105 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str790, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 194 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str842, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 343 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str888, ei_big5hkscs2001},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 270 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str898, ei_jisx0201},
    {-1},
#line 342 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str900, ei_big5hkscs1999},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 222 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str908, ei_mac_hebrew},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 346 "lib/aliases.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str935, ei_big5hkscs2004}
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
