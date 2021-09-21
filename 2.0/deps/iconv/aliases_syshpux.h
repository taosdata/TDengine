/* ANSI-C code produced by gperf version 3.0.4 */
/* Command-line: gperf -m 10 lib/aliases_syshpux.gperf  */
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

#line 1 "lib/aliases_syshpux.gperf"
struct alias { int name; unsigned int encoding_index; };

#define TOTAL_KEYWORDS 356
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 45
#define MIN_HASH_VALUE 9
#define MAX_HASH_VALUE 1038
/* maximum key range = 1030, duplicates = 0 */

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
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039,    0,  112, 1039,   57,    2,
         0,   20,   51,    8,    5,   49,   13,   16,  335, 1039,
      1039, 1039, 1039, 1039, 1039,   13,  149,    1,    6,   10,
        55,  139,   10,    0,  328,   86,  210,  147,    6,    0,
        73, 1039,  120,    6,   17,  282,  238,  172,  274,    2,
         0, 1039, 1039, 1039, 1039,   34, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039,
      1039, 1039, 1039, 1039, 1039, 1039, 1039, 1039
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
    char stringpool_str9[sizeof("CN")];
    char stringpool_str12[sizeof("HZ")];
    char stringpool_str16[sizeof("862")];
    char stringpool_str17[sizeof("CP1252")];
    char stringpool_str19[sizeof("ASCII")];
    char stringpool_str21[sizeof("CP1251")];
    char stringpool_str22[sizeof("SJIS")];
    char stringpool_str24[sizeof("CP862")];
    char stringpool_str26[sizeof("866")];
    char stringpool_str27[sizeof("CP1256")];
    char stringpool_str28[sizeof("CSASCII")];
    char stringpool_str29[sizeof("EUCCN")];
    char stringpool_str30[sizeof("EUC-CN")];
    char stringpool_str33[sizeof("CP1255")];
    char stringpool_str34[sizeof("CP866")];
    char stringpool_str35[sizeof("CP1131")];
    char stringpool_str36[sizeof("C99")];
    char stringpool_str38[sizeof("CP1361")];
    char stringpool_str39[sizeof("HP15CN")];
    char stringpool_str42[sizeof("CP932")];
    char stringpool_str43[sizeof("CP1258")];
    char stringpool_str50[sizeof("CHINESE")];
    char stringpool_str52[sizeof("CP936")];
    char stringpool_str53[sizeof("CP819")];
    char stringpool_str57[sizeof("CP1253")];
    char stringpool_str58[sizeof("ISO88592")];
    char stringpool_str59[sizeof("ISO8859-2")];
    char stringpool_str60[sizeof("ISO-8859-2")];
    char stringpool_str62[sizeof("ISO88591")];
    char stringpool_str63[sizeof("ISO8859-1")];
    char stringpool_str64[sizeof("ISO-8859-1")];
    char stringpool_str66[sizeof("ISO8859-11")];
    char stringpool_str67[sizeof("ISO-8859-11")];
    char stringpool_str68[sizeof("ISO88596")];
    char stringpool_str69[sizeof("ISO8859-6")];
    char stringpool_str70[sizeof("ISO-8859-6")];
    char stringpool_str71[sizeof("CP1133")];
    char stringpool_str72[sizeof("ISO8859-16")];
    char stringpool_str73[sizeof("ISO-8859-16")];
    char stringpool_str74[sizeof("ISO88595")];
    char stringpool_str75[sizeof("ISO8859-5")];
    char stringpool_str76[sizeof("ISO-8859-5")];
    char stringpool_str77[sizeof("ISO885915")];
    char stringpool_str78[sizeof("ISO8859-15")];
    char stringpool_str79[sizeof("ISO-8859-15")];
    char stringpool_str81[sizeof("ISO-2022-CN")];
    char stringpool_str83[sizeof("ISO646-CN")];
    char stringpool_str84[sizeof("ISO88598")];
    char stringpool_str85[sizeof("ISO8859-8")];
    char stringpool_str86[sizeof("ISO-8859-8")];
    char stringpool_str88[sizeof("CSISO2022CN")];
    char stringpool_str90[sizeof("ISO88599")];
    char stringpool_str91[sizeof("ISO8859-9")];
    char stringpool_str92[sizeof("ISO-8859-9")];
    char stringpool_str94[sizeof("ISO_8859-2")];
    char stringpool_str96[sizeof("ISO-2022-CN-EXT")];
    char stringpool_str98[sizeof("ISO_8859-1")];
    char stringpool_str99[sizeof("ISO8859-3")];
    char stringpool_str100[sizeof("ISO-8859-3")];
    char stringpool_str101[sizeof("ISO_8859-11")];
    char stringpool_str102[sizeof("ISO8859-13")];
    char stringpool_str103[sizeof("ISO-8859-13")];
    char stringpool_str104[sizeof("ISO_8859-6")];
    char stringpool_str105[sizeof("CP949")];
    char stringpool_str107[sizeof("ISO_8859-16")];
    char stringpool_str109[sizeof("ISO_8859-16:2001")];
    char stringpool_str110[sizeof("ISO_8859-5")];
    char stringpool_str111[sizeof("ELOT_928")];
    char stringpool_str113[sizeof("ISO_8859-15")];
    char stringpool_str115[sizeof("CP1257")];
    char stringpool_str118[sizeof("CP154")];
    char stringpool_str119[sizeof("CP1254")];
    char stringpool_str120[sizeof("ISO_8859-8")];
    char stringpool_str123[sizeof("ISO_8859-15:1998")];
    char stringpool_str126[sizeof("ISO_8859-9")];
    char stringpool_str129[sizeof("CP367")];
    char stringpool_str130[sizeof("850")];
    char stringpool_str131[sizeof("CP1250")];
    char stringpool_str134[sizeof("ISO_8859-3")];
    char stringpool_str135[sizeof("R8")];
    char stringpool_str137[sizeof("ISO_8859-13")];
    char stringpool_str138[sizeof("ISO-IR-6")];
    char stringpool_str139[sizeof("KOI8-T")];
    char stringpool_str140[sizeof("ISO-IR-226")];
    char stringpool_str141[sizeof("CP850")];
    char stringpool_str142[sizeof("ISO-IR-126")];
    char stringpool_str144[sizeof("CP950")];
    char stringpool_str147[sizeof("ISO-IR-166")];
    char stringpool_str148[sizeof("TIS620")];
    char stringpool_str149[sizeof("TIS-620")];
    char stringpool_str152[sizeof("MAC")];
    char stringpool_str153[sizeof("ISO-IR-165")];
    char stringpool_str156[sizeof("ISO88597")];
    char stringpool_str157[sizeof("ISO8859-7")];
    char stringpool_str158[sizeof("ISO-8859-7")];
    char stringpool_str159[sizeof("ISO_8859-10:1992")];
    char stringpool_str161[sizeof("ISO8859-4")];
    char stringpool_str162[sizeof("ISO-8859-4")];
    char stringpool_str163[sizeof("ISO-IR-58")];
    char stringpool_str164[sizeof("ISO8859-14")];
    char stringpool_str165[sizeof("ISO-8859-14")];
    char stringpool_str166[sizeof("ISO_8859-14:1998")];
    char stringpool_str167[sizeof("GB2312")];
    char stringpool_str170[sizeof("CP874")];
    char stringpool_str171[sizeof("IBM862")];
    char stringpool_str172[sizeof("ISO-IR-159")];
    char stringpool_str176[sizeof("ISO8859-10")];
    char stringpool_str177[sizeof("ISO-8859-10")];
    char stringpool_str178[sizeof("ISO-IR-138")];
    char stringpool_str179[sizeof("MS-ANSI")];
    char stringpool_str180[sizeof("ISO-IR-199")];
    char stringpool_str181[sizeof("IBM866")];
    char stringpool_str182[sizeof("MS-EE")];
    char stringpool_str183[sizeof("ARABIC")];
    char stringpool_str190[sizeof("PT154")];
    char stringpool_str192[sizeof("ISO_8859-7")];
    char stringpool_str193[sizeof("ISO-IR-101")];
    char stringpool_str195[sizeof("MACTHAI")];
    char stringpool_str196[sizeof("ISO_8859-4")];
    char stringpool_str198[sizeof("MS936")];
    char stringpool_str199[sizeof("ISO_8859-14")];
    char stringpool_str200[sizeof("IBM819")];
    char stringpool_str202[sizeof("ARMSCII-8")];
    char stringpool_str203[sizeof("KSC_5601")];
    char stringpool_str206[sizeof("MACINTOSH")];
    char stringpool_str207[sizeof("TIS620-0")];
    char stringpool_str208[sizeof("ECMA-118")];
    char stringpool_str209[sizeof("ISO-IR-148")];
    char stringpool_str211[sizeof("ISO_8859-10")];
    char stringpool_str212[sizeof("L2")];
    char stringpool_str213[sizeof("ISO-IR-179")];
    char stringpool_str214[sizeof("L1")];
    char stringpool_str215[sizeof("ISO-IR-149")];
    char stringpool_str217[sizeof("L6")];
    char stringpool_str220[sizeof("L5")];
    char stringpool_str221[sizeof("ISO-IR-109")];
    char stringpool_str222[sizeof("CSMACINTOSH")];
    char stringpool_str225[sizeof("L8")];
    char stringpool_str227[sizeof("ISO-IR-203")];
    char stringpool_str229[sizeof("KZ-1048")];
    char stringpool_str230[sizeof("ISO-IR-127")];
    char stringpool_str231[sizeof("CSKZ1048")];
    char stringpool_str232[sizeof("L3")];
    char stringpool_str233[sizeof("ISO-IR-14")];
    char stringpool_str235[sizeof("ISO-IR-57")];
    char stringpool_str236[sizeof("TIS620.2529-1")];
    char stringpool_str238[sizeof("ISO-IR-157")];
    char stringpool_str239[sizeof("LATIN2")];
    char stringpool_str240[sizeof("ISO-IR-87")];
    char stringpool_str243[sizeof("LATIN1")];
    char stringpool_str246[sizeof("CSKSC56011987")];
    char stringpool_str247[sizeof("KOREAN")];
    char stringpool_str248[sizeof("ISO-IR-110")];
    char stringpool_str249[sizeof("LATIN6")];
    char stringpool_str250[sizeof("ISO-CELTIC")];
    char stringpool_str251[sizeof("VISCII")];
    char stringpool_str254[sizeof("CSVISCII")];
    char stringpool_str255[sizeof("LATIN5")];
    char stringpool_str258[sizeof("CHAR")];
    char stringpool_str259[sizeof("KS_C_5601-1989")];
    char stringpool_str260[sizeof("TIS620.2533-1")];
    char stringpool_str261[sizeof("L7")];
    char stringpool_str262[sizeof("RK1048")];
    char stringpool_str263[sizeof("L4")];
    char stringpool_str264[sizeof("CSISOLATIN2")];
    char stringpool_str265[sizeof("LATIN8")];
    char stringpool_str266[sizeof("PTCP154")];
    char stringpool_str268[sizeof("CSISOLATIN1")];
    char stringpool_str271[sizeof("TCVN")];
    char stringpool_str272[sizeof("LATIN-9")];
    char stringpool_str273[sizeof("CSISOLATINCYRILLIC")];
    char stringpool_str274[sizeof("CSISOLATIN6")];
    char stringpool_str276[sizeof("IBM367")];
    char stringpool_str277[sizeof("GREEK8")];
    char stringpool_str279[sizeof("LATIN3")];
    char stringpool_str280[sizeof("CSISOLATIN5")];
    char stringpool_str281[sizeof("X0212")];
    char stringpool_str283[sizeof("CSISOLATINARABIC")];
    char stringpool_str284[sizeof("ECMA-114")];
    char stringpool_str285[sizeof("ISO-IR-144")];
    char stringpool_str286[sizeof("CSPTCP154")];
    char stringpool_str287[sizeof("UHC")];
    char stringpool_str288[sizeof("IBM850")];
    char stringpool_str290[sizeof("US")];
    char stringpool_str292[sizeof("KS_C_5601-1987")];
    char stringpool_str293[sizeof("UCS-2")];
    char stringpool_str295[sizeof("IBM-CP1133")];
    char stringpool_str300[sizeof("ASMO-708")];
    char stringpool_str303[sizeof("ISO-IR-100")];
    char stringpool_str304[sizeof("CSISOLATIN3")];
    char stringpool_str308[sizeof("BIG5")];
    char stringpool_str309[sizeof("BIG-5")];
    char stringpool_str310[sizeof("US-ASCII")];
    char stringpool_str311[sizeof("CSBIG5")];
    char stringpool_str312[sizeof("CN-BIG5")];
    char stringpool_str314[sizeof("GBK")];
    char stringpool_str315[sizeof("TIS620.2533-0")];
    char stringpool_str316[sizeof("UNICODE-1-1")];
    char stringpool_str318[sizeof("ROMAN8")];
    char stringpool_str319[sizeof("CSGB2312")];
    char stringpool_str323[sizeof("CSUNICODE11")];
    char stringpool_str325[sizeof("CSUNICODE")];
    char stringpool_str327[sizeof("L10")];
    char stringpool_str329[sizeof("TCVN-5712")];
    char stringpool_str330[sizeof("HZ-GB-2312")];
    char stringpool_str331[sizeof("HP-ROMAN8")];
    char stringpool_str332[sizeof("GB_2312-80")];
    char stringpool_str333[sizeof("CSIBM866")];
    char stringpool_str334[sizeof("TCVN5712-1")];
    char stringpool_str335[sizeof("MACCROATIAN")];
    char stringpool_str336[sizeof("GREEK")];
    char stringpool_str337[sizeof("LATIN7")];
    char stringpool_str340[sizeof("X0201")];
    char stringpool_str341[sizeof("LATIN4")];
    char stringpool_str342[sizeof("EUCKR")];
    char stringpool_str343[sizeof("EUC-KR")];
    char stringpool_str345[sizeof("KOI8-R")];
    char stringpool_str347[sizeof("CSKOI8R")];
    char stringpool_str352[sizeof("GB18030")];
    char stringpool_str354[sizeof("GB_1988-80")];
    char stringpool_str355[sizeof("UTF-16")];
    char stringpool_str356[sizeof("LATIN10")];
    char stringpool_str362[sizeof("X0208")];
    char stringpool_str363[sizeof("UTF-32")];
    char stringpool_str364[sizeof("ISO646-US")];
    char stringpool_str366[sizeof("CSISOLATIN4")];
    char stringpool_str367[sizeof("UTF8")];
    char stringpool_str368[sizeof("UTF-8")];
    char stringpool_str369[sizeof("UNICODE-1-1-UTF-7")];
    char stringpool_str374[sizeof("CSUNICODE11UTF7")];
    char stringpool_str376[sizeof("VISCII1.1-1")];
    char stringpool_str377[sizeof("EUCTW")];
    char stringpool_str378[sizeof("EUC-TW")];
    char stringpool_str384[sizeof("WINDOWS-1252")];
    char stringpool_str386[sizeof("WINDOWS-1251")];
    char stringpool_str389[sizeof("WINDOWS-1256")];
    char stringpool_str390[sizeof("WCHAR_T")];
    char stringpool_str392[sizeof("WINDOWS-1255")];
    char stringpool_str394[sizeof("ISO-2022-KR")];
    char stringpool_str395[sizeof("UCS-4")];
    char stringpool_str396[sizeof("CSISO57GB1988")];
    char stringpool_str397[sizeof("WINDOWS-1258")];
    char stringpool_str398[sizeof("CSUCS4")];
    char stringpool_str401[sizeof("CSISO2022KR")];
    char stringpool_str403[sizeof("JP")];
    char stringpool_str404[sizeof("WINDOWS-1253")];
    char stringpool_str405[sizeof("STRK1048-2002")];
    char stringpool_str406[sizeof("CSHPROMAN8")];
    char stringpool_str408[sizeof("CSISO58GB231280")];
    char stringpool_str410[sizeof("MACICELAND")];
    char stringpool_str412[sizeof("CSISO14JISC6220RO")];
    char stringpool_str415[sizeof("JIS_C6226-1983")];
    char stringpool_str417[sizeof("ISO-10646-UCS-2")];
    char stringpool_str419[sizeof("WINDOWS-936")];
    char stringpool_str420[sizeof("BIG5HKSCS")];
    char stringpool_str421[sizeof("BIG5-HKSCS")];
    char stringpool_str427[sizeof("SHIFT-JIS")];
    char stringpool_str433[sizeof("WINDOWS-1257")];
    char stringpool_str435[sizeof("WINDOWS-1254")];
    char stringpool_str437[sizeof("CN-GB-ISOIR165")];
    char stringpool_str439[sizeof("CSSHIFTJIS")];
    char stringpool_str440[sizeof("UTF-7")];
    char stringpool_str441[sizeof("WINDOWS-1250")];
    char stringpool_str442[sizeof("EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE")];
    char stringpool_str443[sizeof("CN-GB")];
    char stringpool_str444[sizeof("CSISO159JISX02121990")];
    char stringpool_str448[sizeof("MACROMAN")];
    char stringpool_str449[sizeof("GEORGIAN-ACADEMY")];
    char stringpool_str450[sizeof("JIS_C6220-1969-RO")];
    char stringpool_str451[sizeof("CSISOLATINHEBREW")];
    char stringpool_str454[sizeof("MACARABIC")];
    char stringpool_str455[sizeof("ISO_8859-5:1988")];
    char stringpool_str460[sizeof("ISO_8859-8:1988")];
    char stringpool_str461[sizeof("SHIFT_JIS")];
    char stringpool_str464[sizeof("UCS-2BE")];
    char stringpool_str466[sizeof("ISO_8859-9:1989")];
    char stringpool_str467[sizeof("ISO_8859-3:1988")];
    char stringpool_str468[sizeof("ISO-10646-UCS-4")];
    char stringpool_str470[sizeof("MACROMANIA")];
    char stringpool_str471[sizeof("ISO-2022-JP-2")];
    char stringpool_str473[sizeof("ISO-2022-JP-1")];
    char stringpool_str477[sizeof("CSISO2022JP2")];
    char stringpool_str481[sizeof("JIS0208")];
    char stringpool_str483[sizeof("ISO_8859-2:1987")];
    char stringpool_str484[sizeof("NEXTSTEP")];
    char stringpool_str485[sizeof("ISO_8859-1:1987")];
    char stringpool_str488[sizeof("ISO_8859-6:1987")];
    char stringpool_str490[sizeof("EUCJP")];
    char stringpool_str491[sizeof("EUC-JP")];
    char stringpool_str493[sizeof("CSISOLATINGREEK")];
    char stringpool_str498[sizeof("ISO_8859-4:1988")];
    char stringpool_str503[sizeof("ISO_8859-7:2003")];
    char stringpool_str513[sizeof("GEORGIAN-PS")];
    char stringpool_str515[sizeof("UCS-4BE")];
    char stringpool_str521[sizeof("UTF-16BE")];
    char stringpool_str523[sizeof("CSPC862LATINHEBREW")];
    char stringpool_str525[sizeof("UCS-2LE")];
    char stringpool_str526[sizeof("CSHALFWIDTHKATAKANA")];
    char stringpool_str531[sizeof("ANSI_X3.4-1986")];
    char stringpool_str532[sizeof("ISO_8859-7:1987")];
    char stringpool_str534[sizeof("UTF-32BE")];
    char stringpool_str537[sizeof("WINDOWS-874")];
    char stringpool_str539[sizeof("ANSI_X3.4-1968")];
    char stringpool_str542[sizeof("ISO-2022-JP")];
    char stringpool_str544[sizeof("ISO646-JP")];
    char stringpool_str549[sizeof("CSISO2022JP")];
    char stringpool_str551[sizeof("CYRILLIC")];
    char stringpool_str561[sizeof("MACCENTRALEUROPE")];
    char stringpool_str563[sizeof("MS-HEBR")];
    char stringpool_str566[sizeof("UNICODELITTLE")];
    char stringpool_str576[sizeof("UCS-4LE")];
    char stringpool_str581[sizeof("CYRILLIC-ASIAN")];
    char stringpool_str582[sizeof("UTF-16LE")];
    char stringpool_str583[sizeof("ISO_646.IRV:1991")];
    char stringpool_str595[sizeof("UTF-32LE")];
    char stringpool_str596[sizeof("JAVA")];
    char stringpool_str598[sizeof("MS-ARAB")];
    char stringpool_str603[sizeof("MULELAO-1")];
    char stringpool_str606[sizeof("MS-GREEK")];
    char stringpool_str607[sizeof("MACGREEK")];
    char stringpool_str608[sizeof("BIGFIVE")];
    char stringpool_str609[sizeof("BIG-FIVE")];
    char stringpool_str622[sizeof("MS_KANJI")];
    char stringpool_str627[sizeof("CSEUCKR")];
    char stringpool_str639[sizeof("HEBREW")];
    char stringpool_str644[sizeof("UCS-2-SWAPPED")];
    char stringpool_str654[sizeof("JOHAB")];
    char stringpool_str662[sizeof("CSEUCTW")];
    char stringpool_str665[sizeof("UCS-2-INTERNAL")];
    char stringpool_str669[sizeof("KOI8-U")];
    char stringpool_str685[sizeof("MACUKRAINE")];
    char stringpool_str689[sizeof("MACTURKISH")];
    char stringpool_str692[sizeof("TCVN5712-1:1993")];
    char stringpool_str695[sizeof("UCS-4-SWAPPED")];
    char stringpool_str697[sizeof("MS-CYRL")];
    char stringpool_str704[sizeof("MACCYRILLIC")];
    char stringpool_str705[sizeof("CSISO87JISX0208")];
    char stringpool_str707[sizeof("CSEUCPKDFMTJAPANESE")];
    char stringpool_str710[sizeof("JIS_X0212")];
    char stringpool_str716[sizeof("UCS-4-INTERNAL")];
    char stringpool_str736[sizeof("UNICODEBIG")];
    char stringpool_str745[sizeof("MS-TURK")];
    char stringpool_str757[sizeof("BIG5-HKSCS:2001")];
    char stringpool_str760[sizeof("JISX0201-1976")];
    char stringpool_str769[sizeof("JIS_X0201")];
    char stringpool_str771[sizeof("BIG5-HKSCS:1999")];
    char stringpool_str774[sizeof("JIS_X0212-1990")];
    char stringpool_str790[sizeof("KOI8-RU")];
    char stringpool_str791[sizeof("JIS_X0208")];
    char stringpool_str800[sizeof("MACHEBREW")];
    char stringpool_str805[sizeof("JIS_X0208-1983")];
    char stringpool_str806[sizeof("BIG5-HKSCS:2004")];
    char stringpool_str842[sizeof("JIS_X0208-1990")];
    char stringpool_str888[sizeof("JIS_X0212.1990-0")];
    char stringpool_str991[sizeof("WINBALTRIM")];
    char stringpool_str1038[sizeof("CSPC850MULTILINGUAL")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "CN",
    "HZ",
    "862",
    "CP1252",
    "ASCII",
    "CP1251",
    "SJIS",
    "CP862",
    "866",
    "CP1256",
    "CSASCII",
    "EUCCN",
    "EUC-CN",
    "CP1255",
    "CP866",
    "CP1131",
    "C99",
    "CP1361",
    "HP15CN",
    "CP932",
    "CP1258",
    "CHINESE",
    "CP936",
    "CP819",
    "CP1253",
    "ISO88592",
    "ISO8859-2",
    "ISO-8859-2",
    "ISO88591",
    "ISO8859-1",
    "ISO-8859-1",
    "ISO8859-11",
    "ISO-8859-11",
    "ISO88596",
    "ISO8859-6",
    "ISO-8859-6",
    "CP1133",
    "ISO8859-16",
    "ISO-8859-16",
    "ISO88595",
    "ISO8859-5",
    "ISO-8859-5",
    "ISO885915",
    "ISO8859-15",
    "ISO-8859-15",
    "ISO-2022-CN",
    "ISO646-CN",
    "ISO88598",
    "ISO8859-8",
    "ISO-8859-8",
    "CSISO2022CN",
    "ISO88599",
    "ISO8859-9",
    "ISO-8859-9",
    "ISO_8859-2",
    "ISO-2022-CN-EXT",
    "ISO_8859-1",
    "ISO8859-3",
    "ISO-8859-3",
    "ISO_8859-11",
    "ISO8859-13",
    "ISO-8859-13",
    "ISO_8859-6",
    "CP949",
    "ISO_8859-16",
    "ISO_8859-16:2001",
    "ISO_8859-5",
    "ELOT_928",
    "ISO_8859-15",
    "CP1257",
    "CP154",
    "CP1254",
    "ISO_8859-8",
    "ISO_8859-15:1998",
    "ISO_8859-9",
    "CP367",
    "850",
    "CP1250",
    "ISO_8859-3",
    "R8",
    "ISO_8859-13",
    "ISO-IR-6",
    "KOI8-T",
    "ISO-IR-226",
    "CP850",
    "ISO-IR-126",
    "CP950",
    "ISO-IR-166",
    "TIS620",
    "TIS-620",
    "MAC",
    "ISO-IR-165",
    "ISO88597",
    "ISO8859-7",
    "ISO-8859-7",
    "ISO_8859-10:1992",
    "ISO8859-4",
    "ISO-8859-4",
    "ISO-IR-58",
    "ISO8859-14",
    "ISO-8859-14",
    "ISO_8859-14:1998",
    "GB2312",
    "CP874",
    "IBM862",
    "ISO-IR-159",
    "ISO8859-10",
    "ISO-8859-10",
    "ISO-IR-138",
    "MS-ANSI",
    "ISO-IR-199",
    "IBM866",
    "MS-EE",
    "ARABIC",
    "PT154",
    "ISO_8859-7",
    "ISO-IR-101",
    "MACTHAI",
    "ISO_8859-4",
    "MS936",
    "ISO_8859-14",
    "IBM819",
    "ARMSCII-8",
    "KSC_5601",
    "MACINTOSH",
    "TIS620-0",
    "ECMA-118",
    "ISO-IR-148",
    "ISO_8859-10",
    "L2",
    "ISO-IR-179",
    "L1",
    "ISO-IR-149",
    "L6",
    "L5",
    "ISO-IR-109",
    "CSMACINTOSH",
    "L8",
    "ISO-IR-203",
    "KZ-1048",
    "ISO-IR-127",
    "CSKZ1048",
    "L3",
    "ISO-IR-14",
    "ISO-IR-57",
    "TIS620.2529-1",
    "ISO-IR-157",
    "LATIN2",
    "ISO-IR-87",
    "LATIN1",
    "CSKSC56011987",
    "KOREAN",
    "ISO-IR-110",
    "LATIN6",
    "ISO-CELTIC",
    "VISCII",
    "CSVISCII",
    "LATIN5",
    "CHAR",
    "KS_C_5601-1989",
    "TIS620.2533-1",
    "L7",
    "RK1048",
    "L4",
    "CSISOLATIN2",
    "LATIN8",
    "PTCP154",
    "CSISOLATIN1",
    "TCVN",
    "LATIN-9",
    "CSISOLATINCYRILLIC",
    "CSISOLATIN6",
    "IBM367",
    "GREEK8",
    "LATIN3",
    "CSISOLATIN5",
    "X0212",
    "CSISOLATINARABIC",
    "ECMA-114",
    "ISO-IR-144",
    "CSPTCP154",
    "UHC",
    "IBM850",
    "US",
    "KS_C_5601-1987",
    "UCS-2",
    "IBM-CP1133",
    "ASMO-708",
    "ISO-IR-100",
    "CSISOLATIN3",
    "BIG5",
    "BIG-5",
    "US-ASCII",
    "CSBIG5",
    "CN-BIG5",
    "GBK",
    "TIS620.2533-0",
    "UNICODE-1-1",
    "ROMAN8",
    "CSGB2312",
    "CSUNICODE11",
    "CSUNICODE",
    "L10",
    "TCVN-5712",
    "HZ-GB-2312",
    "HP-ROMAN8",
    "GB_2312-80",
    "CSIBM866",
    "TCVN5712-1",
    "MACCROATIAN",
    "GREEK",
    "LATIN7",
    "X0201",
    "LATIN4",
    "EUCKR",
    "EUC-KR",
    "KOI8-R",
    "CSKOI8R",
    "GB18030",
    "GB_1988-80",
    "UTF-16",
    "LATIN10",
    "X0208",
    "UTF-32",
    "ISO646-US",
    "CSISOLATIN4",
    "UTF8",
    "UTF-8",
    "UNICODE-1-1-UTF-7",
    "CSUNICODE11UTF7",
    "VISCII1.1-1",
    "EUCTW",
    "EUC-TW",
    "WINDOWS-1252",
    "WINDOWS-1251",
    "WINDOWS-1256",
    "WCHAR_T",
    "WINDOWS-1255",
    "ISO-2022-KR",
    "UCS-4",
    "CSISO57GB1988",
    "WINDOWS-1258",
    "CSUCS4",
    "CSISO2022KR",
    "JP",
    "WINDOWS-1253",
    "STRK1048-2002",
    "CSHPROMAN8",
    "CSISO58GB231280",
    "MACICELAND",
    "CSISO14JISC6220RO",
    "JIS_C6226-1983",
    "ISO-10646-UCS-2",
    "WINDOWS-936",
    "BIG5HKSCS",
    "BIG5-HKSCS",
    "SHIFT-JIS",
    "WINDOWS-1257",
    "WINDOWS-1254",
    "CN-GB-ISOIR165",
    "CSSHIFTJIS",
    "UTF-7",
    "WINDOWS-1250",
    "EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE",
    "CN-GB",
    "CSISO159JISX02121990",
    "MACROMAN",
    "GEORGIAN-ACADEMY",
    "JIS_C6220-1969-RO",
    "CSISOLATINHEBREW",
    "MACARABIC",
    "ISO_8859-5:1988",
    "ISO_8859-8:1988",
    "SHIFT_JIS",
    "UCS-2BE",
    "ISO_8859-9:1989",
    "ISO_8859-3:1988",
    "ISO-10646-UCS-4",
    "MACROMANIA",
    "ISO-2022-JP-2",
    "ISO-2022-JP-1",
    "CSISO2022JP2",
    "JIS0208",
    "ISO_8859-2:1987",
    "NEXTSTEP",
    "ISO_8859-1:1987",
    "ISO_8859-6:1987",
    "EUCJP",
    "EUC-JP",
    "CSISOLATINGREEK",
    "ISO_8859-4:1988",
    "ISO_8859-7:2003",
    "GEORGIAN-PS",
    "UCS-4BE",
    "UTF-16BE",
    "CSPC862LATINHEBREW",
    "UCS-2LE",
    "CSHALFWIDTHKATAKANA",
    "ANSI_X3.4-1986",
    "ISO_8859-7:1987",
    "UTF-32BE",
    "WINDOWS-874",
    "ANSI_X3.4-1968",
    "ISO-2022-JP",
    "ISO646-JP",
    "CSISO2022JP",
    "CYRILLIC",
    "MACCENTRALEUROPE",
    "MS-HEBR",
    "UNICODELITTLE",
    "UCS-4LE",
    "CYRILLIC-ASIAN",
    "UTF-16LE",
    "ISO_646.IRV:1991",
    "UTF-32LE",
    "JAVA",
    "MS-ARAB",
    "MULELAO-1",
    "MS-GREEK",
    "MACGREEK",
    "BIGFIVE",
    "BIG-FIVE",
    "MS_KANJI",
    "CSEUCKR",
    "HEBREW",
    "UCS-2-SWAPPED",
    "JOHAB",
    "CSEUCTW",
    "UCS-2-INTERNAL",
    "KOI8-U",
    "MACUKRAINE",
    "MACTURKISH",
    "TCVN5712-1:1993",
    "UCS-4-SWAPPED",
    "MS-CYRL",
    "MACCYRILLIC",
    "CSISO87JISX0208",
    "CSEUCPKDFMTJAPANESE",
    "JIS_X0212",
    "UCS-4-INTERNAL",
    "UNICODEBIG",
    "MS-TURK",
    "BIG5-HKSCS:2001",
    "JISX0201-1976",
    "JIS_X0201",
    "BIG5-HKSCS:1999",
    "JIS_X0212-1990",
    "KOI8-RU",
    "JIS_X0208",
    "MACHEBREW",
    "JIS_X0208-1983",
    "BIG5-HKSCS:2004",
    "JIS_X0208-1990",
    "JIS_X0212.1990-0",
    "WINBALTRIM",
    "CSPC850MULTILINGUAL"
  };
#define stringpool ((const char *) &stringpool_contents)

static const struct alias aliases[] =
  {
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 297 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str9, ei_iso646_cn},
    {-1}, {-1},
#line 340 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str12, ei_hz},
    {-1}, {-1}, {-1},
#line 212 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str16, ei_cp862},
#line 186 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str17, ei_cp1252},
    {-1},
#line 13 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str19, ei_ascii},
    {-1},
#line 183 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str21, ei_cp1251},
#line 317 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str22, ei_sjis},
    {-1},
#line 210 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str24, ei_cp862},
    {-1},
#line 216 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str26, ei_cp866},
#line 198 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, ei_cp1256},
#line 22 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str28, ei_ascii},
#line 327 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str29, ei_euc_cn},
#line 326 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str30, ei_euc_cn},
    {-1}, {-1},
#line 195 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str33, ei_cp1255},
#line 214 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str34, ei_cp866},
#line 218 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str35, ei_cp1131},
#line 52 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str36, ei_c99},
    {-1},
#line 363 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str38, ei_johab},
#line 331 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str39, ei_euc_cn},
    {-1}, {-1},
#line 320 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str42, ei_cp932},
#line 204 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str43, ei_cp1258},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 302 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str50, ei_gb2312},
    {-1},
#line 333 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str52, ei_cp936},
#line 58 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str53, ei_iso8859_1},
    {-1}, {-1}, {-1},
#line 189 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str57, ei_cp1253},
#line 73 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str58, ei_iso8859_2},
#line 72 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str59, ei_iso8859_2},
#line 65 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str60, ei_iso8859_2},
    {-1},
#line 64 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str62, ei_iso8859_1},
#line 63 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str63, ei_iso8859_1},
#line 54 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str64, ei_iso8859_1},
    {-1},
#line 147 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str66, ei_iso8859_11},
#line 145 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str67, ei_iso8859_11},
#line 107 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str68, ei_iso8859_6},
#line 106 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str69, ei_iso8859_6},
#line 98 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str70, ei_iso8859_6},
#line 253 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str71, ei_cp1133},
#line 175 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str72, ei_iso8859_16},
#line 169 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str73, ei_iso8859_16},
#line 97 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, ei_iso8859_5},
#line 96 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str75, ei_iso8859_5},
#line 90 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str76, ei_iso8859_5},
#line 168 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str77, ei_iso8859_15},
#line 167 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str78, ei_iso8859_15},
#line 162 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str79, ei_iso8859_15},
    {-1},
#line 337 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str81, ei_iso2022_cn},
    {-1},
#line 295 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str83, ei_iso646_cn},
#line 127 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str84, ei_iso8859_8},
#line 126 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str85, ei_iso8859_8},
#line 120 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str86, ei_iso8859_8},
    {-1},
#line 338 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str88, ei_iso2022_cn},
    {-1},
#line 136 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str90, ei_iso8859_9},
#line 135 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str91, ei_iso8859_9},
#line 128 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str92, ei_iso8859_9},
    {-1},
#line 66 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str94, ei_iso8859_2},
    {-1},
#line 339 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str96, ei_iso2022_cn_ext},
    {-1},
#line 55 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str98, ei_iso8859_1},
#line 81 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str99, ei_iso8859_3},
#line 74 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str100, ei_iso8859_3},
#line 146 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str101, ei_iso8859_11},
#line 153 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str102, ei_iso8859_13},
#line 148 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str103, ei_iso8859_13},
#line 99 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str104, ei_iso8859_6},
#line 360 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str105, ei_cp949},
    {-1},
#line 170 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str107, ei_iso8859_16},
    {-1},
#line 171 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str109, ei_iso8859_16},
#line 91 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str110, ei_iso8859_5},
#line 114 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str111, ei_iso8859_7},
    {-1},
#line 163 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str113, ei_iso8859_15},
    {-1},
#line 201 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str115, ei_cp1257},
    {-1}, {-1},
#line 245 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str118, ei_pt154},
#line 192 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str119, ei_cp1254},
#line 121 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str120, ei_iso8859_8},
    {-1}, {-1},
#line 164 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str123, ei_iso8859_15},
    {-1}, {-1},
#line 129 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str126, ei_iso8859_9},
    {-1}, {-1},
#line 19 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str129, ei_ascii},
#line 208 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str130, ei_cp850},
#line 180 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str131, ei_cp1250},
    {-1}, {-1},
#line 75 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str134, ei_iso8859_3},
#line 236 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str135, ei_hp_roman8},
    {-1},
#line 149 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str137, ei_iso8859_13},
#line 16 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str138, ei_ascii},
#line 242 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str139, ei_koi8_t},
#line 172 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str140, ei_iso8859_16},
#line 206 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str141, ei_cp850},
#line 112 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str142, ei_iso8859_7},
    {-1},
#line 351 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str144, ei_cp950},
    {-1}, {-1},
#line 261 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str147, ei_tis620},
#line 256 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str148, ei_tis620},
#line 255 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str149, ei_tis620},
    {-1}, {-1},
#line 221 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str152, ei_mac_roman},
#line 303 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str153, ei_isoir165},
    {-1}, {-1},
#line 119 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str156, ei_iso8859_7},
#line 118 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str157, ei_iso8859_7},
#line 108 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str158, ei_iso8859_7},
#line 139 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str159, ei_iso8859_10},
    {-1},
#line 89 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str161, ei_iso8859_4},
#line 82 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str162, ei_iso8859_4},
#line 300 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str163, ei_gb2312},
#line 161 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str164, ei_iso8859_14},
#line 154 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str165, ei_iso8859_14},
#line 156 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str166, ei_iso8859_14},
#line 328 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str167, ei_euc_cn},
    {-1}, {-1},
#line 262 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str170, ei_cp874},
#line 211 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str171, ei_cp862},
#line 292 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str172, ei_jisx0212},
    {-1}, {-1}, {-1},
#line 144 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str176, ei_iso8859_10},
#line 137 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str177, ei_iso8859_10},
#line 123 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str178, ei_iso8859_8},
#line 188 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str179, ei_cp1252},
#line 157 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str180, ei_iso8859_14},
#line 215 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str181, ei_cp866},
#line 182 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str182, ei_cp1250},
#line 104 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str183, ei_iso8859_6},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 243 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str190, ei_pt154},
    {-1},
#line 109 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str192, ei_iso8859_7},
#line 68 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str193, ei_iso8859_2},
    {-1},
#line 233 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str195, ei_mac_thai},
#line 83 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str196, ei_iso8859_4},
    {-1},
#line 334 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str198, ei_cp936},
#line 155 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str199, ei_iso8859_14},
#line 59 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str200, ei_iso8859_1},
    {-1},
#line 239 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str202, ei_armscii_8},
#line 305 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str203, ei_ksc5601},
    {-1}, {-1},
#line 220 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str206, ei_mac_roman},
#line 257 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str207, ei_tis620},
#line 113 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str208, ei_iso8859_7},
#line 131 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str209, ei_iso8859_9},
    {-1},
#line 138 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str211, ei_iso8859_10},
#line 70 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str212, ei_iso8859_2},
#line 150 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str213, ei_iso8859_13},
#line 61 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str214, ei_iso8859_1},
#line 308 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str215, ei_ksc5601},
    {-1},
#line 142 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str217, ei_iso8859_10},
    {-1}, {-1},
#line 133 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str220, ei_iso8859_9},
#line 77 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str221, ei_iso8859_3},
#line 222 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str222, ei_mac_roman},
    {-1}, {-1},
#line 159 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str225, ei_iso8859_14},
    {-1},
#line 165 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str227, ei_iso8859_15},
    {-1},
#line 250 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str229, ei_rk1048},
#line 101 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str230, ei_iso8859_6},
#line 251 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str231, ei_rk1048},
#line 79 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str232, ei_iso8859_3},
#line 273 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str233, ei_iso646_jp},
    {-1},
#line 296 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str235, ei_iso646_cn},
#line 258 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str236, ei_tis620},
    {-1},
#line 140 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str238, ei_iso8859_10},
#line 69 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str239, ei_iso8859_2},
#line 285 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str240, ei_jisx0208},
    {-1}, {-1},
#line 60 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str243, ei_iso8859_1},
    {-1}, {-1},
#line 309 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str246, ei_ksc5601},
#line 310 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str247, ei_ksc5601},
#line 85 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str248, ei_iso8859_4},
#line 141 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str249, ei_iso8859_10},
#line 160 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str250, ei_iso8859_14},
#line 264 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str251, ei_viscii},
    {-1}, {-1},
#line 266 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str254, ei_viscii},
#line 132 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str255, ei_iso8859_9},
    {-1}, {-1},
#line 366 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str258, ei_local_char},
#line 307 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str259, ei_ksc5601},
#line 260 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str260, ei_tis620},
#line 152 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str261, ei_iso8859_13},
#line 248 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str262, ei_rk1048},
#line 87 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str263, ei_iso8859_4},
#line 71 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str264, ei_iso8859_2},
#line 158 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str265, ei_iso8859_14},
#line 244 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str266, ei_pt154},
    {-1},
#line 62 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str268, ei_iso8859_1},
    {-1}, {-1},
#line 267 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str271, ei_tcvn},
#line 166 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str272, ei_iso8859_15},
#line 95 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str273, ei_iso8859_5},
#line 143 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str274, ei_iso8859_10},
    {-1},
#line 20 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str276, ei_ascii},
#line 115 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str277, ei_iso8859_7},
    {-1},
#line 78 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str279, ei_iso8859_3},
#line 134 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str280, ei_iso8859_9},
#line 291 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str281, ei_jisx0212},
    {-1},
#line 105 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str283, ei_iso8859_6},
#line 102 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str284, ei_iso8859_6},
#line 93 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str285, ei_iso8859_5},
#line 247 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str286, ei_pt154},
#line 361 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str287, ei_cp949},
#line 207 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str288, ei_cp850},
    {-1},
#line 21 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str290, ei_ascii},
    {-1},
#line 306 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str292, ei_ksc5601},
#line 25 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str293, ei_ucs2},
    {-1},
#line 254 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str295, ei_cp1133},
    {-1}, {-1}, {-1}, {-1},
#line 103 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str300, ei_iso8859_6},
    {-1}, {-1},
#line 57 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str303, ei_iso8859_1},
#line 80 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str304, ei_iso8859_3},
    {-1}, {-1}, {-1},
#line 345 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str308, ei_ces_big5},
#line 346 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str309, ei_ces_big5},
#line 12 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str310, ei_ascii},
#line 350 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str311, ei_ces_big5},
#line 349 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str312, ei_ces_big5},
    {-1},
#line 332 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str314, ei_ces_gbk},
#line 259 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str315, ei_tis620},
#line 30 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str316, ei_ucs2be},
    {-1},
#line 235 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str318, ei_hp_roman8},
#line 330 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str319, ei_euc_cn},
    {-1}, {-1}, {-1},
#line 31 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str323, ei_ucs2be},
    {-1},
#line 27 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str325, ei_ucs2},
    {-1},
#line 174 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str327, ei_iso8859_16},
    {-1},
#line 268 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str329, ei_tcvn},
#line 341 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str330, ei_hz},
#line 234 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str331, ei_hp_roman8},
#line 299 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str332, ei_gb2312},
#line 217 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str333, ei_cp866},
#line 269 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str334, ei_tcvn},
#line 225 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str335, ei_mac_croatian},
#line 116 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str336, ei_iso8859_7},
#line 151 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str337, ei_iso8859_13},
    {-1}, {-1},
#line 278 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str340, ei_jisx0201},
#line 86 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str341, ei_iso8859_4},
#line 358 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str342, ei_euc_kr},
#line 357 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str343, ei_euc_kr},
    {-1},
#line 176 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str345, ei_koi8_r},
    {-1},
#line 177 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str347, ei_koi8_r},
    {-1}, {-1}, {-1}, {-1},
#line 336 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str352, ei_gb18030},
    {-1},
#line 294 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str354, ei_iso646_cn},
#line 39 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str355, ei_utf16},
#line 173 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str356, ei_iso8859_16},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 284 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str362, ei_jisx0208},
#line 42 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str363, ei_utf32},
#line 14 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str364, ei_ascii},
    {-1},
#line 88 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str366, ei_iso8859_4},
#line 24 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str367, ei_utf8},
#line 23 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str368, ei_utf8},
#line 46 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str369, ei_utf7},
    {-1}, {-1}, {-1}, {-1},
#line 47 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str374, ei_utf7},
    {-1},
#line 265 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str376, ei_viscii},
#line 343 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str377, ei_euc_tw},
#line 342 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str378, ei_euc_tw},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 187 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str384, ei_cp1252},
    {-1},
#line 184 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str386, ei_cp1251},
    {-1}, {-1},
#line 199 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str389, ei_cp1256},
#line 367 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str390, ei_local_wchar_t},
    {-1},
#line 196 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str392, ei_cp1255},
    {-1},
#line 364 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str394, ei_iso2022_kr},
#line 34 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str395, ei_ucs4},
#line 298 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str396, ei_iso646_cn},
#line 205 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str397, ei_cp1258},
#line 36 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str398, ei_ucs4},
    {-1}, {-1},
#line 365 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str401, ei_iso2022_kr},
    {-1},
#line 274 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str403, ei_iso646_jp},
#line 190 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str404, ei_cp1253},
#line 249 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str405, ei_rk1048},
#line 237 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str406, ei_hp_roman8},
    {-1},
#line 301 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str408, ei_gb2312},
    {-1},
#line 224 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str410, ei_mac_iceland},
    {-1},
#line 275 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str412, ei_iso646_jp},
    {-1}, {-1},
#line 286 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str415, ei_jisx0208},
    {-1},
#line 26 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str417, ei_ucs2},
    {-1},
#line 335 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str419, ei_cp936},
#line 355 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str420, ei_big5hkscs2004},
#line 354 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str421, ei_big5hkscs2004},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 316 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str427, ei_sjis},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 202 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str433, ei_cp1257},
    {-1},
#line 193 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str435, ei_cp1254},
    {-1},
#line 304 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str437, ei_isoir165},
    {-1},
#line 319 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str439, ei_sjis},
#line 45 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str440, ei_utf7},
#line 181 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str441, ei_cp1250},
#line 313 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str442, ei_euc_jp},
#line 329 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str443, ei_euc_cn},
#line 293 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str444, ei_jisx0212},
    {-1}, {-1}, {-1},
#line 219 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str448, ei_mac_roman},
#line 240 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str449, ei_georgian_academy},
#line 271 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str450, ei_iso646_jp},
#line 125 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str451, ei_iso8859_8},
    {-1}, {-1},
#line 232 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str454, ei_mac_arabic},
#line 92 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str455, ei_iso8859_5},
    {-1}, {-1}, {-1}, {-1},
#line 122 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str460, ei_iso8859_8},
#line 315 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str461, ei_sjis},
    {-1}, {-1},
#line 28 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str464, ei_ucs2be},
    {-1},
#line 130 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str466, ei_iso8859_9},
#line 76 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str467, ei_iso8859_3},
#line 35 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str468, ei_ucs4},
    {-1},
#line 226 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str470, ei_mac_romania},
#line 324 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str471, ei_iso2022_jp2},
    {-1},
#line 323 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str473, ei_iso2022_jp1},
    {-1}, {-1}, {-1},
#line 325 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str477, ei_iso2022_jp2},
    {-1}, {-1}, {-1},
#line 283 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str481, ei_jisx0208},
    {-1},
#line 67 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str483, ei_iso8859_2},
#line 238 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str484, ei_nextstep},
#line 56 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str485, ei_iso8859_1},
    {-1}, {-1},
#line 100 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str488, ei_iso8859_6},
    {-1},
#line 312 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str490, ei_euc_jp},
#line 311 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str491, ei_euc_jp},
    {-1},
#line 117 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str493, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1},
#line 84 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str498, ei_iso8859_4},
    {-1}, {-1}, {-1}, {-1},
#line 111 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str503, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 241 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str513, ei_georgian_ps},
    {-1},
#line 37 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str515, ei_ucs4be},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 40 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str521, ei_utf16be},
    {-1},
#line 213 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str523, ei_cp862},
    {-1},
#line 32 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str525, ei_ucs2le},
#line 279 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str526, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1},
#line 18 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str531, ei_ascii},
#line 110 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str532, ei_iso8859_7},
    {-1},
#line 43 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str534, ei_utf32be},
    {-1}, {-1},
#line 263 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str537, ei_cp874},
    {-1},
#line 17 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str539, ei_ascii},
    {-1}, {-1},
#line 321 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str542, ei_iso2022_jp},
    {-1},
#line 272 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str544, ei_iso646_jp},
    {-1}, {-1}, {-1}, {-1},
#line 322 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str549, ei_iso2022_jp},
    {-1},
#line 94 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str551, ei_iso8859_5},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 223 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str561, ei_mac_centraleurope},
    {-1},
#line 197 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str563, ei_cp1255},
    {-1}, {-1},
#line 33 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str566, ei_ucs2le},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 38 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str576, ei_ucs4le},
    {-1}, {-1}, {-1}, {-1},
#line 246 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str581, ei_pt154},
#line 41 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str582, ei_utf16le},
#line 15 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str583, ei_ascii},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 44 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str595, ei_utf32le},
#line 53 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str596, ei_java},
    {-1},
#line 200 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str598, ei_cp1256},
    {-1}, {-1}, {-1}, {-1},
#line 252 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str603, ei_mulelao},
    {-1}, {-1},
#line 191 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str606, ei_cp1253},
#line 229 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str607, ei_mac_greek},
#line 348 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str608, ei_ces_big5},
#line 347 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str609, ei_ces_big5},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 318 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str622, ei_sjis},
    {-1}, {-1}, {-1}, {-1},
#line 359 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str627, ei_euc_kr},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 124 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str639, ei_iso8859_8},
    {-1}, {-1}, {-1}, {-1},
#line 49 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str644, ei_ucs2swapped},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 362 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str654, ei_johab},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 344 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str662, ei_euc_tw},
    {-1}, {-1},
#line 48 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str665, ei_ucs2internal},
    {-1}, {-1}, {-1},
#line 178 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str669, ei_koi8_u},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 228 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str685, ei_mac_ukraine},
    {-1}, {-1}, {-1},
#line 230 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str689, ei_mac_turkish},
    {-1}, {-1},
#line 270 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str692, ei_tcvn},
    {-1}, {-1},
#line 51 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str695, ei_ucs4swapped},
    {-1},
#line 185 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str697, ei_cp1251},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 227 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str704, ei_mac_cyrillic},
#line 287 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str705, ei_jisx0208},
    {-1},
#line 314 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str707, ei_euc_jp},
    {-1}, {-1},
#line 288 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str710, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 50 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str716, ei_ucs4internal},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 29 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str736, ei_ucs2be},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 194 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str745, ei_cp1254},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 353 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str757, ei_big5hkscs2001},
    {-1}, {-1},
#line 277 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str760, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 276 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str769, ei_jisx0201},
    {-1},
#line 352 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str771, ei_big5hkscs1999},
    {-1}, {-1},
#line 290 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str774, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 179 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str790, ei_koi8_ru},
#line 280 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str791, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 231 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str800, ei_mac_hebrew},
    {-1}, {-1}, {-1}, {-1},
#line 281 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str805, ei_jisx0208},
#line 356 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str806, ei_big5hkscs2004},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 282 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str842, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 289 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str888, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 203 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str991, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 209 "lib/aliases_syshpux.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str1038, ei_cp850}
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
