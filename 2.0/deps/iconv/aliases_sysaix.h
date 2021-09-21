/* ANSI-C code produced by gperf version 3.0.4 */
/* Command-line: gperf -m 10 lib/aliases_sysaix.gperf  */
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

#line 1 "lib/aliases_sysaix.gperf"
struct alias { int name; unsigned int encoding_index; };

#define TOTAL_KEYWORDS 355
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 45
#define MIN_HASH_VALUE 13
#define MAX_HASH_VALUE 989
/* maximum key range = 977, duplicates = 0 */

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
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990,  13, 112, 990,  73,   4,
        7,   6,  55,   8,   5, 171,  10,  23, 255, 990,
      990, 990, 990, 990, 990, 147, 128,   4,   9, 125,
      130,   5,  75,   4, 402,  69,   7, 125,  18,   4,
       44, 990,  76,   4,  25, 195, 191, 161, 120,  22,
       15, 990, 990, 990, 990,  27, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990, 990, 990,
      990, 990, 990, 990, 990, 990, 990, 990
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
    char stringpool_str13[sizeof("L1")];
    char stringpool_str14[sizeof("L6")];
    char stringpool_str15[sizeof("L3")];
    char stringpool_str16[sizeof("L2")];
    char stringpool_str17[sizeof("L5")];
    char stringpool_str19[sizeof("L8")];
    char stringpool_str20[sizeof("SJIS")];
    char stringpool_str23[sizeof("866")];
    char stringpool_str24[sizeof("CN")];
    char stringpool_str27[sizeof("862")];
    char stringpool_str32[sizeof("CP1131")];
    char stringpool_str33[sizeof("CP1361")];
    char stringpool_str34[sizeof("CP866")];
    char stringpool_str36[sizeof("CP1133")];
    char stringpool_str37[sizeof("CP1251")];
    char stringpool_str38[sizeof("CP862")];
    char stringpool_str39[sizeof("CP1256")];
    char stringpool_str41[sizeof("CP1253")];
    char stringpool_str42[sizeof("GB2312")];
    char stringpool_str43[sizeof("CP1252")];
    char stringpool_str45[sizeof("CP1255")];
    char stringpool_str48[sizeof("CP936")];
    char stringpool_str49[sizeof("CP1258")];
    char stringpool_str52[sizeof("CP932")];
    char stringpool_str53[sizeof("C99")];
    char stringpool_str64[sizeof("L4")];
    char stringpool_str68[sizeof("LATIN1")];
    char stringpool_str69[sizeof("CP819")];
    char stringpool_str70[sizeof("LATIN6")];
    char stringpool_str72[sizeof("LATIN3")];
    char stringpool_str74[sizeof("LATIN2")];
    char stringpool_str76[sizeof("LATIN5")];
    char stringpool_str80[sizeof("LATIN8")];
    char stringpool_str88[sizeof("R8")];
    char stringpool_str89[sizeof("ISO8859-1")];
    char stringpool_str91[sizeof("ISO8859-6")];
    char stringpool_str92[sizeof("HZ")];
    char stringpool_str93[sizeof("ISO8859-3")];
    char stringpool_str94[sizeof("ISO8859-11")];
    char stringpool_str95[sizeof("ISO8859-2")];
    char stringpool_str96[sizeof("ISO8859-16")];
    char stringpool_str97[sizeof("ISO8859-5")];
    char stringpool_str98[sizeof("ISO8859-13")];
    char stringpool_str101[sizeof("ISO8859-8")];
    char stringpool_str102[sizeof("ISO8859-15")];
    char stringpool_str103[sizeof("ISO-8859-1")];
    char stringpool_str105[sizeof("ISO-8859-6")];
    char stringpool_str107[sizeof("ISO-8859-3")];
    char stringpool_str108[sizeof("ISO-8859-11")];
    char stringpool_str109[sizeof("ISO-8859-2")];
    char stringpool_str110[sizeof("ISO-8859-16")];
    char stringpool_str111[sizeof("ISO-8859-5")];
    char stringpool_str112[sizeof("ISO-8859-13")];
    char stringpool_str115[sizeof("ISO-8859-8")];
    char stringpool_str116[sizeof("ISO-8859-15")];
    char stringpool_str117[sizeof("ISO_8859-1")];
    char stringpool_str118[sizeof("CYRILLIC")];
    char stringpool_str119[sizeof("ISO_8859-6")];
    char stringpool_str120[sizeof("LATIN-9")];
    char stringpool_str121[sizeof("ISO_8859-3")];
    char stringpool_str122[sizeof("ISO_8859-11")];
    char stringpool_str123[sizeof("ISO_8859-2")];
    char stringpool_str124[sizeof("ISO_8859-16")];
    char stringpool_str125[sizeof("ISO_8859-5")];
    char stringpool_str126[sizeof("ISO_8859-13")];
    char stringpool_str127[sizeof("ISO8859-9")];
    char stringpool_str128[sizeof("ISO_8859-16:2001")];
    char stringpool_str129[sizeof("ISO_8859-8")];
    char stringpool_str130[sizeof("ISO_8859-15")];
    char stringpool_str131[sizeof("CP154")];
    char stringpool_str132[sizeof("ISO-IR-6")];
    char stringpool_str133[sizeof("CP949")];
    char stringpool_str135[sizeof("ISO646-CN")];
    char stringpool_str136[sizeof("MAC")];
    char stringpool_str137[sizeof("ISO_8859-15:1998")];
    char stringpool_str139[sizeof("CP1254")];
    char stringpool_str141[sizeof("ISO-8859-9")];
    char stringpool_str143[sizeof("ISO-IR-166")];
    char stringpool_str145[sizeof("ISO-IR-126")];
    char stringpool_str146[sizeof("GBK")];
    char stringpool_str148[sizeof("ISO-IR-226")];
    char stringpool_str149[sizeof("ISO-IR-165")];
    char stringpool_str150[sizeof("X0212")];
    char stringpool_str151[sizeof("ISO-IR-58")];
    char stringpool_str152[sizeof("KOI8-T")];
    char stringpool_str153[sizeof("BIG5")];
    char stringpool_str154[sizeof("ISO-IR-138")];
    char stringpool_str155[sizeof("ISO_8859-9")];
    char stringpool_str156[sizeof("L10")];
    char stringpool_str159[sizeof("850")];
    char stringpool_str160[sizeof("IBM866")];
    char stringpool_str161[sizeof("CSISO2022CN")];
    char stringpool_str163[sizeof("CSBIG5")];
    char stringpool_str164[sizeof("IBM862")];
    char stringpool_str167[sizeof("BIG-5")];
    char stringpool_str168[sizeof("ASCII")];
    char stringpool_str169[sizeof("MS936")];
    char stringpool_str170[sizeof("LATIN4")];
    char stringpool_str171[sizeof("PT154")];
    char stringpool_str172[sizeof("IBM-1131")];
    char stringpool_str173[sizeof("CP850")];
    char stringpool_str174[sizeof("EUCCN")];
    char stringpool_str175[sizeof("CP1250")];
    char stringpool_str176[sizeof("CSGB2312")];
    char stringpool_str177[sizeof("CN-BIG5")];
    char stringpool_str178[sizeof("CSASCII")];
    char stringpool_str179[sizeof("ISO-2022-CN")];
    char stringpool_str180[sizeof("L7")];
    char stringpool_str182[sizeof("ISO-IR-159")];
    char stringpool_str183[sizeof("IBM-1252")];
    char stringpool_str184[sizeof("ISO_8859-14:1998")];
    char stringpool_str186[sizeof("CP950")];
    char stringpool_str187[sizeof("IBM-921")];
    char stringpool_str188[sizeof("EUC-CN")];
    char stringpool_str190[sizeof("ISO-2022-CN-EXT")];
    char stringpool_str191[sizeof("ISO8859-4")];
    char stringpool_str192[sizeof("IBM-932")];
    char stringpool_str193[sizeof("TIS620")];
    char stringpool_str195[sizeof("IBM819")];
    char stringpool_str196[sizeof("ISO8859-14")];
    char stringpool_str197[sizeof("ISO-IR-199")];
    char stringpool_str199[sizeof("ISO_8859-10:1992")];
    char stringpool_str201[sizeof("US")];
    char stringpool_str202[sizeof("KSC_5601")];
    char stringpool_str203[sizeof("ISO-IR-148")];
    char stringpool_str204[sizeof("ISO-CELTIC")];
    char stringpool_str205[sizeof("ISO-8859-4")];
    char stringpool_str206[sizeof("UHC")];
    char stringpool_str207[sizeof("TIS-620")];
    char stringpool_str209[sizeof("ISO-IR-101")];
    char stringpool_str210[sizeof("ISO-8859-14")];
    char stringpool_str211[sizeof("LATIN10")];
    char stringpool_str213[sizeof("X0201")];
    char stringpool_str216[sizeof("ISO-IR-203")];
    char stringpool_str217[sizeof("VISCII")];
    char stringpool_str219[sizeof("ISO_8859-4")];
    char stringpool_str221[sizeof("PTCP154")];
    char stringpool_str224[sizeof("ISO_8859-14")];
    char stringpool_str225[sizeof("X0208")];
    char stringpool_str226[sizeof("IBM-CP1133")];
    char stringpool_str227[sizeof("CSVISCII")];
    char stringpool_str229[sizeof("ISO-IR-149")];
    char stringpool_str231[sizeof("UCS-2")];
    char stringpool_str232[sizeof("ISO8859-10")];
    char stringpool_str234[sizeof("RK1048")];
    char stringpool_str235[sizeof("GB_2312-80")];
    char stringpool_str236[sizeof("CSISOLATIN1")];
    char stringpool_str237[sizeof("ISO-IR-14")];
    char stringpool_str238[sizeof("CSISOLATIN6")];
    char stringpool_str239[sizeof("ELOT_928")];
    char stringpool_str240[sizeof("CSISOLATIN3")];
    char stringpool_str241[sizeof("KZ-1048")];
    char stringpool_str242[sizeof("CSISOLATIN2")];
    char stringpool_str243[sizeof("CSISOLATINCYRILLIC")];
    char stringpool_str244[sizeof("CSISOLATIN5")];
    char stringpool_str246[sizeof("ISO-8859-10")];
    char stringpool_str247[sizeof("ISO-IR-109")];
    char stringpool_str248[sizeof("CSKZ1048")];
    char stringpool_str250[sizeof("CSKOI8R")];
    char stringpool_str251[sizeof("GB18030")];
    char stringpool_str252[sizeof("CSPTCP154")];
    char stringpool_str254[sizeof("KOI8-R")];
    char stringpool_str256[sizeof("TCVN")];
    char stringpool_str258[sizeof("GB_1988-80")];
    char stringpool_str260[sizeof("ISO_8859-10")];
    char stringpool_str261[sizeof("MS-CYRL")];
    char stringpool_str268[sizeof("CSISO58GB231280")];
    char stringpool_str270[sizeof("TIS620.2533-1")];
    char stringpool_str271[sizeof("KS_C_5601-1989")];
    char stringpool_str272[sizeof("MACCYRILLIC")];
    char stringpool_str275[sizeof("HZ-GB-2312")];
    char stringpool_str277[sizeof("CN-GB-ISOIR165")];
    char stringpool_str278[sizeof("ISO-IR-110")];
    char stringpool_str281[sizeof("TIS620-0")];
    char stringpool_str283[sizeof("CN-GB")];
    char stringpool_str288[sizeof("TIS620.2529-1")];
    char stringpool_str293[sizeof("ISO-IR-144")];
    char stringpool_str294[sizeof("CSIBM866")];
    char stringpool_str298[sizeof("ISO646-US")];
    char stringpool_str299[sizeof("IBM850")];
    char stringpool_str300[sizeof("CP874")];
    char stringpool_str302[sizeof("CYRILLIC-ASIAN")];
    char stringpool_str306[sizeof("CSISOLATINGREEK")];
    char stringpool_str307[sizeof("CHAR")];
    char stringpool_str310[sizeof("BIG5HKSCS")];
    char stringpool_str313[sizeof("IBM-850")];
    char stringpool_str322[sizeof("MS-ANSI")];
    char stringpool_str323[sizeof("CSUCS4")];
    char stringpool_str324[sizeof("BIG5-HKSCS")];
    char stringpool_str327[sizeof("UCS-4")];
    char stringpool_str330[sizeof("ARMSCII-8")];
    char stringpool_str335[sizeof("GEORGIAN-PS")];
    char stringpool_str338[sizeof("CSISOLATIN4")];
    char stringpool_str339[sizeof("TIS620.2533-0")];
    char stringpool_str342[sizeof("CSISO2022KR")];
    char stringpool_str343[sizeof("MACINTOSH")];
    char stringpool_str345[sizeof("ISO-IR-179")];
    char stringpool_str347[sizeof("ISO-IR-100")];
    char stringpool_str350[sizeof("GREEK8")];
    char stringpool_str355[sizeof("EUCKR")];
    char stringpool_str358[sizeof("UTF-16")];
    char stringpool_str359[sizeof("VISCII1.1-1")];
    char stringpool_str360[sizeof("ISO-2022-KR")];
    char stringpool_str362[sizeof("CP367")];
    char stringpool_str363[sizeof("UTF-8")];
    char stringpool_str364[sizeof("UTF-32")];
    char stringpool_str369[sizeof("EUC-KR")];
    char stringpool_str371[sizeof("CP1257")];
    char stringpool_str378[sizeof("CSISO57GB1988")];
    char stringpool_str382[sizeof("CSKSC56011987")];
    char stringpool_str383[sizeof("US-ASCII")];
    char stringpool_str384[sizeof("CSISOLATINARABIC")];
    char stringpool_str385[sizeof("ISO_8859-3:1988")];
    char stringpool_str386[sizeof("CSUNICODE11")];
    char stringpool_str387[sizeof("ISO_8859-5:1988")];
    char stringpool_str389[sizeof("ISO_8859-8:1988")];
    char stringpool_str390[sizeof("UNICODE-1-1")];
    char stringpool_str391[sizeof("MACTHAI")];
    char stringpool_str392[sizeof("ROMAN8")];
    char stringpool_str393[sizeof("ISO-10646-UCS-2")];
    char stringpool_str398[sizeof("GREEK")];
    char stringpool_str402[sizeof("LATIN7")];
    char stringpool_str404[sizeof("STRK1048-2002")];
    char stringpool_str405[sizeof("WINDOWS-1251")];
    char stringpool_str406[sizeof("WINDOWS-1256")];
    char stringpool_str407[sizeof("WINDOWS-1253")];
    char stringpool_str408[sizeof("WINDOWS-1252")];
    char stringpool_str409[sizeof("WINDOWS-1255")];
    char stringpool_str411[sizeof("WINDOWS-1258")];
    char stringpool_str412[sizeof("CHINESE")];
    char stringpool_str413[sizeof("NEXTSTEP")];
    char stringpool_str415[sizeof("ISO_8859-9:1989")];
    char stringpool_str419[sizeof("KS_C_5601-1987")];
    char stringpool_str420[sizeof("WINDOWS-936")];
    char stringpool_str423[sizeof("ISO8859-7")];
    char stringpool_str434[sizeof("ISO_8859-4:1988")];
    char stringpool_str436[sizeof("CSPC862LATINHEBREW")];
    char stringpool_str437[sizeof("ISO-8859-7")];
    char stringpool_str440[sizeof("ARABIC")];
    char stringpool_str441[sizeof("ISO-10646-UCS-4")];
    char stringpool_str445[sizeof("MULELAO-1")];
    char stringpool_str446[sizeof("ECMA-118")];
    char stringpool_str448[sizeof("JP")];
    char stringpool_str451[sizeof("ISO_8859-7")];
    char stringpool_str453[sizeof("TCVN-5712")];
    char stringpool_str455[sizeof("TCVN5712-1")];
    char stringpool_str456[sizeof("WINDOWS-1254")];
    char stringpool_str459[sizeof("KOREAN")];
    char stringpool_str461[sizeof("GEORGIAN-ACADEMY")];
    char stringpool_str462[sizeof("MACICELAND")];
    char stringpool_str469[sizeof("CSISOLATINHEBREW")];
    char stringpool_str473[sizeof("ISO-IR-57")];
    char stringpool_str474[sizeof("WINDOWS-1250")];
    char stringpool_str475[sizeof("ISO-IR-87")];
    char stringpool_str477[sizeof("ISO-IR-127")];
    char stringpool_str478[sizeof("ISO-IR-157")];
    char stringpool_str481[sizeof("EUCTW")];
    char stringpool_str483[sizeof("UCS-2LE")];
    char stringpool_str487[sizeof("HP-ROMAN8")];
    char stringpool_str488[sizeof("IBM367")];
    char stringpool_str492[sizeof("KOI8-U")];
    char stringpool_str493[sizeof("UNICODEBIG")];
    char stringpool_str495[sizeof("EUC-TW")];
    char stringpool_str496[sizeof("CSMACINTOSH")];
    char stringpool_str497[sizeof("CSUNICODE")];
    char stringpool_str498[sizeof("JIS_C6226-1983")];
    char stringpool_str501[sizeof("UCS-2-INTERNAL")];
    char stringpool_str503[sizeof("ISO_646.IRV:1991")];
    char stringpool_str510[sizeof("CSISO14JISC6220RO")];
    char stringpool_str511[sizeof("ANSI_X3.4-1986")];
    char stringpool_str515[sizeof("IBM-EUCCN")];
    char stringpool_str516[sizeof("ANSI_X3.4-1968")];
    char stringpool_str518[sizeof("MS-EE")];
    char stringpool_str521[sizeof("CSPC850MULTILINGUAL")];
    char stringpool_str523[sizeof("CSHPROMAN8")];
    char stringpool_str525[sizeof("MACROMAN")];
    char stringpool_str531[sizeof("UCS-4LE")];
    char stringpool_str536[sizeof("ECMA-114")];
    char stringpool_str540[sizeof("UNICODELITTLE")];
    char stringpool_str543[sizeof("WCHAR_T")];
    char stringpool_str544[sizeof("ISO_8859-1:1987")];
    char stringpool_str545[sizeof("ISO_8859-6:1987")];
    char stringpool_str546[sizeof("ISO_8859-7:2003")];
    char stringpool_str547[sizeof("ISO_8859-2:1987")];
    char stringpool_str549[sizeof("UCS-4-INTERNAL")];
    char stringpool_str554[sizeof("CSISO159JISX02121990")];
    char stringpool_str556[sizeof("CSEUCKR")];
    char stringpool_str557[sizeof("CSUNICODE11UTF7")];
    char stringpool_str561[sizeof("ASMO-708")];
    char stringpool_str563[sizeof("UNICODE-1-1-UTF-7")];
    char stringpool_str567[sizeof("JIS_C6220-1969-RO")];
    char stringpool_str569[sizeof("KOI8-RU")];
    char stringpool_str572[sizeof("WINDOWS-1257")];
    char stringpool_str575[sizeof("CSISO2022JP2")];
    char stringpool_str579[sizeof("MS-TURK")];
    char stringpool_str583[sizeof("MACCROATIAN")];
    char stringpool_str584[sizeof("BIG5-HKSCS:2001")];
    char stringpool_str585[sizeof("ISO646-JP")];
    char stringpool_str586[sizeof("JIS0208")];
    char stringpool_str591[sizeof("ISO-2022-JP-1")];
    char stringpool_str594[sizeof("ISO-2022-JP-2")];
    char stringpool_str599[sizeof("SHIFT-JIS")];
    char stringpool_str603[sizeof("BIG5-HKSCS:1999")];
    char stringpool_str604[sizeof("UCS-2BE")];
    char stringpool_str606[sizeof("MACGREEK")];
    char stringpool_str611[sizeof("CSISO2022JP")];
    char stringpool_str612[sizeof("UTF-16LE")];
    char stringpool_str613[sizeof("SHIFT_JIS")];
    char stringpool_str615[sizeof("MS-GREEK")];
    char stringpool_str616[sizeof("UTF-32LE")];
    char stringpool_str624[sizeof("EUCJP")];
    char stringpool_str625[sizeof("MS-HEBR")];
    char stringpool_str629[sizeof("ISO-2022-JP")];
    char stringpool_str635[sizeof("BIG5-HKSCS:2004")];
    char stringpool_str638[sizeof("EUC-JP")];
    char stringpool_str648[sizeof("MACARABIC")];
    char stringpool_str652[sizeof("UCS-4BE")];
    char stringpool_str654[sizeof("UCS-2-SWAPPED")];
    char stringpool_str660[sizeof("JIS_X0212")];
    char stringpool_str662[sizeof("MACTURKISH")];
    char stringpool_str666[sizeof("CSSHIFTJIS")];
    char stringpool_str672[sizeof("WINDOWS-874")];
    char stringpool_str682[sizeof("CSEUCTW")];
    char stringpool_str685[sizeof("UTF-7")];
    char stringpool_str696[sizeof("IBM-EUCKR")];
    char stringpool_str702[sizeof("UCS-4-SWAPPED")];
    char stringpool_str711[sizeof("ISO_8859-7:1987")];
    char stringpool_str715[sizeof("BIGFIVE")];
    char stringpool_str717[sizeof("TCVN5712-1:1993")];
    char stringpool_str723[sizeof("JIS_X0201")];
    char stringpool_str729[sizeof("BIG-FIVE")];
    char stringpool_str732[sizeof("HEBREW")];
    char stringpool_str733[sizeof("UTF-16BE")];
    char stringpool_str735[sizeof("JIS_X0208")];
    char stringpool_str737[sizeof("UTF-32BE")];
    char stringpool_str741[sizeof("JISX0201-1976")];
    char stringpool_str748[sizeof("JIS_X0212-1990")];
    char stringpool_str752[sizeof("CSISO87JISX0208")];
    char stringpool_str753[sizeof("JIS_X0208-1983")];
    char stringpool_str771[sizeof("MS-ARAB")];
    char stringpool_str797[sizeof("MACCENTRALEUROPE")];
    char stringpool_str803[sizeof("CSHALFWIDTHKATAKANA")];
    char stringpool_str804[sizeof("MS_KANJI")];
    char stringpool_str807[sizeof("MACROMANIA")];
    char stringpool_str820[sizeof("JIS_X0208-1990")];
    char stringpool_str822[sizeof("IBM-EUCTW")];
    char stringpool_str826[sizeof("WINBALTRIM")];
    char stringpool_str846[sizeof("EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE")];
    char stringpool_str849[sizeof("JIS_X0212.1990-0")];
    char stringpool_str874[sizeof("CSEUCPKDFMTJAPANESE")];
    char stringpool_str885[sizeof("JOHAB")];
    char stringpool_str891[sizeof("JAVA")];
    char stringpool_str898[sizeof("MACUKRAINE")];
    char stringpool_str965[sizeof("IBM-EUCJP")];
    char stringpool_str989[sizeof("MACHEBREW")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "L1",
    "L6",
    "L3",
    "L2",
    "L5",
    "L8",
    "SJIS",
    "866",
    "CN",
    "862",
    "CP1131",
    "CP1361",
    "CP866",
    "CP1133",
    "CP1251",
    "CP862",
    "CP1256",
    "CP1253",
    "GB2312",
    "CP1252",
    "CP1255",
    "CP936",
    "CP1258",
    "CP932",
    "C99",
    "L4",
    "LATIN1",
    "CP819",
    "LATIN6",
    "LATIN3",
    "LATIN2",
    "LATIN5",
    "LATIN8",
    "R8",
    "ISO8859-1",
    "ISO8859-6",
    "HZ",
    "ISO8859-3",
    "ISO8859-11",
    "ISO8859-2",
    "ISO8859-16",
    "ISO8859-5",
    "ISO8859-13",
    "ISO8859-8",
    "ISO8859-15",
    "ISO-8859-1",
    "ISO-8859-6",
    "ISO-8859-3",
    "ISO-8859-11",
    "ISO-8859-2",
    "ISO-8859-16",
    "ISO-8859-5",
    "ISO-8859-13",
    "ISO-8859-8",
    "ISO-8859-15",
    "ISO_8859-1",
    "CYRILLIC",
    "ISO_8859-6",
    "LATIN-9",
    "ISO_8859-3",
    "ISO_8859-11",
    "ISO_8859-2",
    "ISO_8859-16",
    "ISO_8859-5",
    "ISO_8859-13",
    "ISO8859-9",
    "ISO_8859-16:2001",
    "ISO_8859-8",
    "ISO_8859-15",
    "CP154",
    "ISO-IR-6",
    "CP949",
    "ISO646-CN",
    "MAC",
    "ISO_8859-15:1998",
    "CP1254",
    "ISO-8859-9",
    "ISO-IR-166",
    "ISO-IR-126",
    "GBK",
    "ISO-IR-226",
    "ISO-IR-165",
    "X0212",
    "ISO-IR-58",
    "KOI8-T",
    "BIG5",
    "ISO-IR-138",
    "ISO_8859-9",
    "L10",
    "850",
    "IBM866",
    "CSISO2022CN",
    "CSBIG5",
    "IBM862",
    "BIG-5",
    "ASCII",
    "MS936",
    "LATIN4",
    "PT154",
    "IBM-1131",
    "CP850",
    "EUCCN",
    "CP1250",
    "CSGB2312",
    "CN-BIG5",
    "CSASCII",
    "ISO-2022-CN",
    "L7",
    "ISO-IR-159",
    "IBM-1252",
    "ISO_8859-14:1998",
    "CP950",
    "IBM-921",
    "EUC-CN",
    "ISO-2022-CN-EXT",
    "ISO8859-4",
    "IBM-932",
    "TIS620",
    "IBM819",
    "ISO8859-14",
    "ISO-IR-199",
    "ISO_8859-10:1992",
    "US",
    "KSC_5601",
    "ISO-IR-148",
    "ISO-CELTIC",
    "ISO-8859-4",
    "UHC",
    "TIS-620",
    "ISO-IR-101",
    "ISO-8859-14",
    "LATIN10",
    "X0201",
    "ISO-IR-203",
    "VISCII",
    "ISO_8859-4",
    "PTCP154",
    "ISO_8859-14",
    "X0208",
    "IBM-CP1133",
    "CSVISCII",
    "ISO-IR-149",
    "UCS-2",
    "ISO8859-10",
    "RK1048",
    "GB_2312-80",
    "CSISOLATIN1",
    "ISO-IR-14",
    "CSISOLATIN6",
    "ELOT_928",
    "CSISOLATIN3",
    "KZ-1048",
    "CSISOLATIN2",
    "CSISOLATINCYRILLIC",
    "CSISOLATIN5",
    "ISO-8859-10",
    "ISO-IR-109",
    "CSKZ1048",
    "CSKOI8R",
    "GB18030",
    "CSPTCP154",
    "KOI8-R",
    "TCVN",
    "GB_1988-80",
    "ISO_8859-10",
    "MS-CYRL",
    "CSISO58GB231280",
    "TIS620.2533-1",
    "KS_C_5601-1989",
    "MACCYRILLIC",
    "HZ-GB-2312",
    "CN-GB-ISOIR165",
    "ISO-IR-110",
    "TIS620-0",
    "CN-GB",
    "TIS620.2529-1",
    "ISO-IR-144",
    "CSIBM866",
    "ISO646-US",
    "IBM850",
    "CP874",
    "CYRILLIC-ASIAN",
    "CSISOLATINGREEK",
    "CHAR",
    "BIG5HKSCS",
    "IBM-850",
    "MS-ANSI",
    "CSUCS4",
    "BIG5-HKSCS",
    "UCS-4",
    "ARMSCII-8",
    "GEORGIAN-PS",
    "CSISOLATIN4",
    "TIS620.2533-0",
    "CSISO2022KR",
    "MACINTOSH",
    "ISO-IR-179",
    "ISO-IR-100",
    "GREEK8",
    "EUCKR",
    "UTF-16",
    "VISCII1.1-1",
    "ISO-2022-KR",
    "CP367",
    "UTF-8",
    "UTF-32",
    "EUC-KR",
    "CP1257",
    "CSISO57GB1988",
    "CSKSC56011987",
    "US-ASCII",
    "CSISOLATINARABIC",
    "ISO_8859-3:1988",
    "CSUNICODE11",
    "ISO_8859-5:1988",
    "ISO_8859-8:1988",
    "UNICODE-1-1",
    "MACTHAI",
    "ROMAN8",
    "ISO-10646-UCS-2",
    "GREEK",
    "LATIN7",
    "STRK1048-2002",
    "WINDOWS-1251",
    "WINDOWS-1256",
    "WINDOWS-1253",
    "WINDOWS-1252",
    "WINDOWS-1255",
    "WINDOWS-1258",
    "CHINESE",
    "NEXTSTEP",
    "ISO_8859-9:1989",
    "KS_C_5601-1987",
    "WINDOWS-936",
    "ISO8859-7",
    "ISO_8859-4:1988",
    "CSPC862LATINHEBREW",
    "ISO-8859-7",
    "ARABIC",
    "ISO-10646-UCS-4",
    "MULELAO-1",
    "ECMA-118",
    "JP",
    "ISO_8859-7",
    "TCVN-5712",
    "TCVN5712-1",
    "WINDOWS-1254",
    "KOREAN",
    "GEORGIAN-ACADEMY",
    "MACICELAND",
    "CSISOLATINHEBREW",
    "ISO-IR-57",
    "WINDOWS-1250",
    "ISO-IR-87",
    "ISO-IR-127",
    "ISO-IR-157",
    "EUCTW",
    "UCS-2LE",
    "HP-ROMAN8",
    "IBM367",
    "KOI8-U",
    "UNICODEBIG",
    "EUC-TW",
    "CSMACINTOSH",
    "CSUNICODE",
    "JIS_C6226-1983",
    "UCS-2-INTERNAL",
    "ISO_646.IRV:1991",
    "CSISO14JISC6220RO",
    "ANSI_X3.4-1986",
    "IBM-EUCCN",
    "ANSI_X3.4-1968",
    "MS-EE",
    "CSPC850MULTILINGUAL",
    "CSHPROMAN8",
    "MACROMAN",
    "UCS-4LE",
    "ECMA-114",
    "UNICODELITTLE",
    "WCHAR_T",
    "ISO_8859-1:1987",
    "ISO_8859-6:1987",
    "ISO_8859-7:2003",
    "ISO_8859-2:1987",
    "UCS-4-INTERNAL",
    "CSISO159JISX02121990",
    "CSEUCKR",
    "CSUNICODE11UTF7",
    "ASMO-708",
    "UNICODE-1-1-UTF-7",
    "JIS_C6220-1969-RO",
    "KOI8-RU",
    "WINDOWS-1257",
    "CSISO2022JP2",
    "MS-TURK",
    "MACCROATIAN",
    "BIG5-HKSCS:2001",
    "ISO646-JP",
    "JIS0208",
    "ISO-2022-JP-1",
    "ISO-2022-JP-2",
    "SHIFT-JIS",
    "BIG5-HKSCS:1999",
    "UCS-2BE",
    "MACGREEK",
    "CSISO2022JP",
    "UTF-16LE",
    "SHIFT_JIS",
    "MS-GREEK",
    "UTF-32LE",
    "EUCJP",
    "MS-HEBR",
    "ISO-2022-JP",
    "BIG5-HKSCS:2004",
    "EUC-JP",
    "MACARABIC",
    "UCS-4BE",
    "UCS-2-SWAPPED",
    "JIS_X0212",
    "MACTURKISH",
    "CSSHIFTJIS",
    "WINDOWS-874",
    "CSEUCTW",
    "UTF-7",
    "IBM-EUCKR",
    "UCS-4-SWAPPED",
    "ISO_8859-7:1987",
    "BIGFIVE",
    "TCVN5712-1:1993",
    "JIS_X0201",
    "BIG-FIVE",
    "HEBREW",
    "UTF-16BE",
    "JIS_X0208",
    "UTF-32BE",
    "JISX0201-1976",
    "JIS_X0212-1990",
    "CSISO87JISX0208",
    "JIS_X0208-1983",
    "MS-ARAB",
    "MACCENTRALEUROPE",
    "CSHALFWIDTHKATAKANA",
    "MS_KANJI",
    "MACROMANIA",
    "JIS_X0208-1990",
    "IBM-EUCTW",
    "WINBALTRIM",
    "EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE",
    "JIS_X0212.1990-0",
    "CSEUCPKDFMTJAPANESE",
    "JOHAB",
    "JAVA",
    "MACUKRAINE",
    "IBM-EUCJP",
    "MACHEBREW"
  };
#define stringpool ((const char *) &stringpool_contents)

static const struct alias aliases[] =
  {
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 60 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str13, ei_iso8859_1},
#line 134 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str14, ei_iso8859_10},
#line 76 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str15, ei_iso8859_3},
#line 68 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str16, ei_iso8859_2},
#line 126 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str17, ei_iso8859_9},
    {-1},
#line 152 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str19, ei_iso8859_14},
#line 313 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str20, ei_sjis},
    {-1}, {-1},
#line 210 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str23, ei_cp866},
#line 292 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str24, ei_iso646_cn},
    {-1}, {-1},
#line 206 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, ei_cp862},
    {-1}, {-1}, {-1}, {-1},
#line 212 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str32, ei_cp1131},
#line 362 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str33, ei_johab},
#line 208 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str34, ei_cp866},
    {-1},
#line 248 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str36, ei_cp1133},
#line 175 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str37, ei_cp1251},
#line 204 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str38, ei_cp862},
#line 191 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str39, ei_cp1256},
    {-1},
#line 182 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str41, ei_cp1253},
#line 325 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str42, ei_euc_cn},
#line 178 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str43, ei_cp1252},
    {-1},
#line 188 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str45, ei_cp1255},
    {-1}, {-1},
#line 330 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str48, ei_cp936},
#line 197 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str49, ei_cp1258},
    {-1}, {-1},
#line 316 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str52, ei_cp932},
#line 51 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str53, ei_c99},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 84 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str64, ei_iso8859_4},
    {-1}, {-1}, {-1},
#line 59 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str68, ei_iso8859_1},
#line 57 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str69, ei_iso8859_1},
#line 133 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str70, ei_iso8859_10},
    {-1},
#line 75 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str72, ei_iso8859_3},
    {-1},
#line 67 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, ei_iso8859_2},
    {-1},
#line 125 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str76, ei_iso8859_9},
    {-1}, {-1}, {-1},
#line 151 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str80, ei_iso8859_14},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 231 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str88, ei_hp_roman8},
#line 62 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str89, ei_iso8859_1},
    {-1},
#line 102 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str91, ei_iso8859_6},
#line 337 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str92, ei_hz},
#line 78 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str93, ei_iso8859_3},
#line 139 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str94, ei_iso8859_11},
#line 70 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str95, ei_iso8859_2},
#line 167 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str96, ei_iso8859_16},
#line 93 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str97, ei_iso8859_5},
#line 145 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str98, ei_iso8859_13},
    {-1}, {-1},
#line 120 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str101, ei_iso8859_8},
#line 160 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str102, ei_iso8859_15},
#line 53 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str103, ei_iso8859_1},
    {-1},
#line 94 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str105, ei_iso8859_6},
    {-1},
#line 71 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str107, ei_iso8859_3},
#line 137 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str108, ei_iso8859_11},
#line 63 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str109, ei_iso8859_2},
#line 161 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str110, ei_iso8859_16},
#line 87 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str111, ei_iso8859_5},
#line 140 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str112, ei_iso8859_13},
    {-1}, {-1},
#line 114 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str115, ei_iso8859_8},
#line 155 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str116, ei_iso8859_15},
#line 54 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str117, ei_iso8859_1},
#line 91 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str118, ei_iso8859_5},
#line 95 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str119, ei_iso8859_6},
#line 159 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str120, ei_iso8859_15},
#line 72 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str121, ei_iso8859_3},
#line 138 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str122, ei_iso8859_11},
#line 64 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str123, ei_iso8859_2},
#line 162 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str124, ei_iso8859_16},
#line 88 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str125, ei_iso8859_5},
#line 141 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str126, ei_iso8859_13},
#line 128 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str127, ei_iso8859_9},
#line 163 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str128, ei_iso8859_16},
#line 115 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str129, ei_iso8859_8},
#line 156 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str130, ei_iso8859_15},
#line 240 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str131, ei_pt154},
#line 16 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str132, ei_ascii},
#line 359 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str133, ei_cp949},
    {-1},
#line 290 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str135, ei_iso646_cn},
#line 216 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str136, ei_mac_roman},
#line 157 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str137, ei_iso8859_15},
    {-1},
#line 185 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str139, ei_cp1254},
    {-1},
#line 121 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str141, ei_iso8859_9},
    {-1},
#line 256 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str143, ei_tis620},
    {-1},
#line 107 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str145, ei_iso8859_7},
#line 329 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str146, ei_ces_gbk},
    {-1},
#line 164 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str148, ei_iso8859_16},
#line 298 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str149, ei_isoir165},
#line 286 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str150, ei_jisx0212},
#line 295 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str151, ei_gb2312},
#line 237 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str152, ei_koi8_t},
#line 343 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str153, ei_ces_big5},
#line 117 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str154, ei_iso8859_8},
#line 122 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str155, ei_iso8859_9},
#line 166 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str156, ei_iso8859_16},
    {-1}, {-1},
#line 201 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str159, ei_cp850},
#line 209 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str160, ei_cp866},
#line 335 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str161, ei_iso2022_cn},
    {-1},
#line 348 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str163, ei_ces_big5},
#line 205 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str164, ei_cp862},
    {-1}, {-1},
#line 344 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str167, ei_ces_big5},
#line 13 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str168, ei_ascii},
#line 331 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str169, ei_cp936},
#line 83 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str170, ei_iso8859_4},
#line 238 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str171, ei_pt154},
#line 213 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str172, ei_cp1131},
#line 199 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str173, ei_cp850},
#line 324 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str174, ei_euc_cn},
#line 172 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str175, ei_cp1250},
#line 327 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str176, ei_euc_cn},
#line 347 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str177, ei_ces_big5},
#line 22 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str178, ei_ascii},
#line 334 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str179, ei_iso2022_cn},
#line 144 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str180, ei_iso8859_13},
    {-1},
#line 287 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str182, ei_jisx0212},
#line 181 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str183, ei_cp1252},
#line 149 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str184, ei_iso8859_14},
    {-1},
#line 349 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str186, ei_cp950},
#line 146 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str187, ei_iso8859_13},
#line 323 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str188, ei_euc_cn},
    {-1},
#line 336 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str190, ei_iso2022_cn_ext},
#line 86 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str191, ei_iso8859_4},
#line 317 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str192, ei_cp932},
#line 251 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str193, ei_tis620},
    {-1},
#line 58 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str195, ei_iso8859_1},
#line 154 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str196, ei_iso8859_14},
#line 150 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str197, ei_iso8859_14},
    {-1},
#line 131 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str199, ei_iso8859_10},
    {-1},
#line 21 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str201, ei_ascii},
#line 300 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str202, ei_ksc5601},
#line 124 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str203, ei_iso8859_9},
#line 153 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str204, ei_iso8859_14},
#line 79 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str205, ei_iso8859_4},
#line 360 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str206, ei_cp949},
#line 250 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str207, ei_tis620},
    {-1},
#line 66 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str209, ei_iso8859_2},
#line 147 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str210, ei_iso8859_14},
#line 165 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str211, ei_iso8859_16},
    {-1},
#line 273 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str213, ei_jisx0201},
    {-1}, {-1},
#line 158 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str216, ei_iso8859_15},
#line 259 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str217, ei_viscii},
    {-1},
#line 80 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str219, ei_iso8859_4},
    {-1},
#line 239 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str221, ei_pt154},
    {-1}, {-1},
#line 148 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str224, ei_iso8859_14},
#line 279 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str225, ei_jisx0208},
#line 249 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str226, ei_cp1133},
#line 261 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str227, ei_viscii},
    {-1},
#line 303 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str229, ei_ksc5601},
    {-1},
#line 24 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str231, ei_ucs2},
#line 136 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str232, ei_iso8859_10},
    {-1},
#line 243 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str234, ei_rk1048},
#line 294 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str235, ei_gb2312},
#line 61 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str236, ei_iso8859_1},
#line 268 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str237, ei_iso646_jp},
#line 135 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str238, ei_iso8859_10},
#line 109 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str239, ei_iso8859_7},
#line 77 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str240, ei_iso8859_3},
#line 245 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str241, ei_rk1048},
#line 69 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str242, ei_iso8859_2},
#line 92 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str243, ei_iso8859_5},
#line 127 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str244, ei_iso8859_9},
    {-1},
#line 129 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str246, ei_iso8859_10},
#line 74 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str247, ei_iso8859_3},
#line 246 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str248, ei_rk1048},
    {-1},
#line 169 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str250, ei_koi8_r},
#line 333 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str251, ei_gb18030},
#line 242 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str252, ei_pt154},
    {-1},
#line 168 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str254, ei_koi8_r},
    {-1},
#line 262 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str256, ei_tcvn},
    {-1},
#line 289 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str258, ei_iso646_cn},
    {-1},
#line 130 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str260, ei_iso8859_10},
#line 177 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str261, ei_cp1251},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 296 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str268, ei_gb2312},
    {-1},
#line 255 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str270, ei_tis620},
#line 302 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str271, ei_ksc5601},
#line 222 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str272, ei_mac_cyrillic},
    {-1}, {-1},
#line 338 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str275, ei_hz},
    {-1},
#line 299 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str277, ei_isoir165},
#line 82 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str278, ei_iso8859_4},
    {-1}, {-1},
#line 252 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str281, ei_tis620},
    {-1},
#line 326 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str283, ei_euc_cn},
    {-1}, {-1}, {-1}, {-1},
#line 253 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str288, ei_tis620},
    {-1}, {-1}, {-1}, {-1},
#line 90 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str293, ei_iso8859_5},
#line 211 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str294, ei_cp866},
    {-1}, {-1}, {-1},
#line 14 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str298, ei_ascii},
#line 200 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str299, ei_cp850},
#line 257 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str300, ei_cp874},
    {-1},
#line 241 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str302, ei_pt154},
    {-1}, {-1}, {-1},
#line 112 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str306, ei_iso8859_7},
#line 365 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str307, ei_local_char},
    {-1}, {-1},
#line 353 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str310, ei_big5hkscs2004},
    {-1}, {-1},
#line 203 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str313, ei_cp850},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 180 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str322, ei_cp1252},
#line 35 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str323, ei_ucs4},
#line 352 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str324, ei_big5hkscs2004},
    {-1}, {-1},
#line 33 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str327, ei_ucs4},
    {-1}, {-1},
#line 234 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str330, ei_armscii_8},
    {-1}, {-1}, {-1}, {-1},
#line 236 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str335, ei_georgian_ps},
    {-1}, {-1},
#line 85 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str338, ei_iso8859_4},
#line 254 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str339, ei_tis620},
    {-1}, {-1},
#line 364 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str342, ei_iso2022_kr},
#line 215 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str343, ei_mac_roman},
    {-1},
#line 142 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str345, ei_iso8859_13},
    {-1},
#line 56 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str347, ei_iso8859_1},
    {-1}, {-1},
#line 110 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str350, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1},
#line 356 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str355, ei_euc_kr},
    {-1}, {-1},
#line 38 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str358, ei_utf16},
#line 260 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str359, ei_viscii},
#line 363 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str360, ei_iso2022_kr},
    {-1},
#line 19 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str362, ei_ascii},
#line 23 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str363, ei_utf8},
#line 41 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str364, ei_utf32},
    {-1}, {-1}, {-1}, {-1},
#line 355 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str369, ei_euc_kr},
    {-1},
#line 194 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str371, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 293 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str378, ei_iso646_cn},
    {-1}, {-1}, {-1},
#line 304 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str382, ei_ksc5601},
#line 12 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str383, ei_ascii},
#line 101 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str384, ei_iso8859_6},
#line 73 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str385, ei_iso8859_3},
#line 30 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str386, ei_ucs2be},
#line 89 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str387, ei_iso8859_5},
    {-1},
#line 116 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str389, ei_iso8859_8},
#line 29 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str390, ei_ucs2be},
#line 228 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str391, ei_mac_thai},
#line 230 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str392, ei_hp_roman8},
#line 25 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str393, ei_ucs2},
    {-1}, {-1}, {-1}, {-1},
#line 111 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str398, ei_iso8859_7},
    {-1}, {-1}, {-1},
#line 143 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str402, ei_iso8859_13},
    {-1},
#line 244 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str404, ei_rk1048},
#line 176 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str405, ei_cp1251},
#line 192 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str406, ei_cp1256},
#line 183 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str407, ei_cp1253},
#line 179 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str408, ei_cp1252},
#line 189 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str409, ei_cp1255},
    {-1},
#line 198 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str411, ei_cp1258},
#line 297 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str412, ei_gb2312},
#line 233 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str413, ei_nextstep},
    {-1},
#line 123 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str415, ei_iso8859_9},
    {-1}, {-1}, {-1},
#line 301 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str419, ei_ksc5601},
#line 332 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str420, ei_cp936},
    {-1}, {-1},
#line 113 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str423, ei_iso8859_7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 81 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str434, ei_iso8859_4},
    {-1},
#line 207 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str436, ei_cp862},
#line 103 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str437, ei_iso8859_7},
    {-1}, {-1},
#line 100 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str440, ei_iso8859_6},
#line 34 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str441, ei_ucs4},
    {-1}, {-1}, {-1},
#line 247 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str445, ei_mulelao},
#line 108 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str446, ei_iso8859_7},
    {-1},
#line 269 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str448, ei_iso646_jp},
    {-1}, {-1},
#line 104 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str451, ei_iso8859_7},
    {-1},
#line 263 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str453, ei_tcvn},
    {-1},
#line 264 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str455, ei_tcvn},
#line 186 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str456, ei_cp1254},
    {-1}, {-1},
#line 305 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str459, ei_ksc5601},
    {-1},
#line 235 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str461, ei_georgian_academy},
#line 219 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str462, ei_mac_iceland},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 119 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str469, ei_iso8859_8},
    {-1}, {-1}, {-1},
#line 291 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str473, ei_iso646_cn},
#line 173 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str474, ei_cp1250},
#line 280 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str475, ei_jisx0208},
    {-1},
#line 97 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str477, ei_iso8859_6},
#line 132 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str478, ei_iso8859_10},
    {-1}, {-1},
#line 340 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str481, ei_euc_tw},
    {-1},
#line 31 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str483, ei_ucs2le},
    {-1}, {-1}, {-1},
#line 229 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str487, ei_hp_roman8},
#line 20 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str488, ei_ascii},
    {-1}, {-1}, {-1},
#line 170 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str492, ei_koi8_u},
#line 28 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str493, ei_ucs2be},
    {-1},
#line 339 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str495, ei_euc_tw},
#line 217 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str496, ei_mac_roman},
#line 26 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str497, ei_ucs2},
#line 281 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str498, ei_jisx0208},
    {-1}, {-1},
#line 47 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str501, ei_ucs2internal},
    {-1},
#line 15 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str503, ei_ascii},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 270 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str510, ei_iso646_jp},
#line 18 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str511, ei_ascii},
    {-1}, {-1}, {-1},
#line 328 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str515, ei_euc_cn},
#line 17 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str516, ei_ascii},
    {-1},
#line 174 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str518, ei_cp1250},
    {-1}, {-1},
#line 202 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str521, ei_cp850},
    {-1},
#line 232 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str523, ei_hp_roman8},
    {-1},
#line 214 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str525, ei_mac_roman},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 37 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str531, ei_ucs4le},
    {-1}, {-1}, {-1}, {-1},
#line 98 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str536, ei_iso8859_6},
    {-1}, {-1}, {-1},
#line 32 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str540, ei_ucs2le},
    {-1}, {-1},
#line 366 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str543, ei_local_wchar_t},
#line 55 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str544, ei_iso8859_1},
#line 96 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str545, ei_iso8859_6},
#line 106 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str546, ei_iso8859_7},
#line 65 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str547, ei_iso8859_2},
    {-1},
#line 49 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str549, ei_ucs4internal},
    {-1}, {-1}, {-1}, {-1},
#line 288 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str554, ei_jisx0212},
    {-1},
#line 357 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str556, ei_euc_kr},
#line 46 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str557, ei_utf7},
    {-1}, {-1}, {-1},
#line 99 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str561, ei_iso8859_6},
    {-1},
#line 45 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str563, ei_utf7},
    {-1}, {-1}, {-1},
#line 266 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str567, ei_iso646_jp},
    {-1},
#line 171 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str569, ei_koi8_ru},
    {-1}, {-1},
#line 195 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str572, ei_cp1257},
    {-1}, {-1},
#line 322 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str575, ei_iso2022_jp2},
    {-1}, {-1}, {-1},
#line 187 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str579, ei_cp1254},
    {-1}, {-1}, {-1},
#line 220 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str583, ei_mac_croatian},
#line 351 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str584, ei_big5hkscs2001},
#line 267 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str585, ei_iso646_jp},
#line 278 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str586, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1},
#line 320 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str591, ei_iso2022_jp1},
    {-1}, {-1},
#line 321 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str594, ei_iso2022_jp2},
    {-1}, {-1}, {-1}, {-1},
#line 312 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str599, ei_sjis},
    {-1}, {-1}, {-1},
#line 350 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str603, ei_big5hkscs1999},
#line 27 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str604, ei_ucs2be},
    {-1},
#line 224 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str606, ei_mac_greek},
    {-1}, {-1}, {-1}, {-1},
#line 319 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str611, ei_iso2022_jp},
#line 40 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str612, ei_utf16le},
#line 311 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str613, ei_sjis},
    {-1},
#line 184 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str615, ei_cp1253},
#line 43 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str616, ei_utf32le},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 307 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str624, ei_euc_jp},
#line 190 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str625, ei_cp1255},
    {-1}, {-1}, {-1},
#line 318 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str629, ei_iso2022_jp},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 354 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str635, ei_big5hkscs2004},
    {-1}, {-1},
#line 306 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str638, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 227 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str648, ei_mac_arabic},
    {-1}, {-1}, {-1},
#line 36 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str652, ei_ucs4be},
    {-1},
#line 48 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str654, ei_ucs2swapped},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 283 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str660, ei_jisx0212},
    {-1},
#line 225 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str662, ei_mac_turkish},
    {-1}, {-1}, {-1},
#line 315 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str666, ei_sjis},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 258 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str672, ei_cp874},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 341 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str682, ei_euc_tw},
    {-1}, {-1},
#line 44 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str685, ei_utf7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 358 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str696, ei_euc_kr},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 50 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str702, ei_ucs4swapped},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 105 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str711, ei_iso8859_7},
    {-1}, {-1}, {-1},
#line 346 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str715, ei_ces_big5},
    {-1},
#line 265 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str717, ei_tcvn},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 271 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str723, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 345 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str729, ei_ces_big5},
    {-1}, {-1},
#line 118 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str732, ei_iso8859_8},
#line 39 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str733, ei_utf16be},
    {-1},
#line 275 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str735, ei_jisx0208},
    {-1},
#line 42 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str737, ei_utf32be},
    {-1}, {-1}, {-1},
#line 272 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str741, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 285 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str748, ei_jisx0212},
    {-1}, {-1}, {-1},
#line 282 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str752, ei_jisx0208},
#line 276 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str753, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 193 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str771, ei_cp1256},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 218 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str797, ei_mac_centraleurope},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 274 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str803, ei_jisx0201},
#line 314 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str804, ei_sjis},
    {-1}, {-1},
#line 221 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str807, ei_mac_romania},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 277 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str820, ei_jisx0208},
    {-1},
#line 342 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str822, ei_euc_tw},
    {-1}, {-1}, {-1},
#line 196 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str826, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 308 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str846, ei_euc_jp},
    {-1}, {-1},
#line 284 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str849, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 309 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str874, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1},
#line 361 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str885, ei_johab},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 52 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str891, ei_java},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 223 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str898, ei_mac_ukraine},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1},
#line 310 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str965, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 226 "lib/aliases_sysaix.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str989, ei_mac_hebrew}
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
