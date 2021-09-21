/* ANSI-C code produced by gperf version 3.0.4 */
/* Command-line: gperf -m 10 lib/aliases_sysosf1.gperf  */
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

#line 1 "lib/aliases_sysosf1.gperf"
struct alias { int name; unsigned int encoding_index; };

#define TOTAL_KEYWORDS 351
#define MIN_WORD_LENGTH 2
#define MAX_WORD_LENGTH 45
#define MIN_HASH_VALUE 13
#define MAX_HASH_VALUE 939
/* maximum key range = 927, duplicates = 0 */

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
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940,  13,  80, 940,  73,   4,
        7,   6,  55,   8,   5, 156,  10,  23, 354, 940,
      940, 940, 940, 940, 940, 115, 165,   4,   6, 104,
       89,  13,  53,   4, 304,  95,   7, 150,  18,   4,
       75, 940,  76,  50,  25, 141, 173, 137, 120,   6,
        5, 940, 940, 940, 940,  27, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940, 940, 940,
      940, 940, 940, 940, 940, 940, 940, 940
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
    char stringpool_str43[sizeof("CP1252")];
    char stringpool_str45[sizeof("CP1255")];
    char stringpool_str48[sizeof("CP936")];
    char stringpool_str49[sizeof("CP1258")];
    char stringpool_str50[sizeof("GB2312")];
    char stringpool_str52[sizeof("CP932")];
    char stringpool_str53[sizeof("C99")];
    char stringpool_str60[sizeof("HZ")];
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
    char stringpool_str136[sizeof("ASCII")];
    char stringpool_str137[sizeof("ISO_8859-15:1998")];
    char stringpool_str139[sizeof("CP1254")];
    char stringpool_str141[sizeof("ISO-8859-9")];
    char stringpool_str143[sizeof("ISO-IR-166")];
    char stringpool_str145[sizeof("ISO-IR-126")];
    char stringpool_str148[sizeof("ISO-IR-226")];
    char stringpool_str149[sizeof("ISO-IR-165")];
    char stringpool_str150[sizeof("X0212")];
    char stringpool_str151[sizeof("ISO-IR-58")];
    char stringpool_str152[sizeof("UHC")];
    char stringpool_str153[sizeof("EUCCN")];
    char stringpool_str154[sizeof("ISO-IR-138")];
    char stringpool_str155[sizeof("ISO_8859-9")];
    char stringpool_str156[sizeof("L10")];
    char stringpool_str158[sizeof("SJIS")];
    char stringpool_str159[sizeof("850")];
    char stringpool_str161[sizeof("MAC")];
    char stringpool_str164[sizeof("TACTIS")];
    char stringpool_str165[sizeof("L7")];
    char stringpool_str167[sizeof("EUC-CN")];
    char stringpool_str170[sizeof("LATIN4")];
    char stringpool_str173[sizeof("CP850")];
    char stringpool_str175[sizeof("CP1250")];
    char stringpool_str178[sizeof("KOI8-T")];
    char stringpool_str179[sizeof("ISO-2022-CN")];
    char stringpool_str182[sizeof("ISO-IR-159")];
    char stringpool_str183[sizeof("ISO-CELTIC")];
    char stringpool_str184[sizeof("ISO_8859-14:1998")];
    char stringpool_str185[sizeof("IBM866")];
    char stringpool_str186[sizeof("CP950")];
    char stringpool_str189[sizeof("IBM862")];
    char stringpool_str190[sizeof("ISO-2022-CN-EXT")];
    char stringpool_str191[sizeof("ISO8859-4")];
    char stringpool_str192[sizeof("CSASCII")];
    char stringpool_str193[sizeof("US")];
    char stringpool_str194[sizeof("MS936")];
    char stringpool_str196[sizeof("ISO8859-14")];
    char stringpool_str197[sizeof("ISO-IR-199")];
    char stringpool_str198[sizeof("BIG5")];
    char stringpool_str199[sizeof("ISO_8859-10:1992")];
    char stringpool_str200[sizeof("KSC5601")];
    char stringpool_str202[sizeof("PT154")];
    char stringpool_str203[sizeof("ISO-IR-148")];
    char stringpool_str205[sizeof("ISO-8859-4")];
    char stringpool_str206[sizeof("GBK")];
    char stringpool_str207[sizeof("CSISO2022CN")];
    char stringpool_str208[sizeof("CSBIG5")];
    char stringpool_str209[sizeof("ISO-IR-101")];
    char stringpool_str210[sizeof("ISO-8859-14")];
    char stringpool_str211[sizeof("LATIN10")];
    char stringpool_str212[sizeof("BIG-5")];
    char stringpool_str213[sizeof("X0201")];
    char stringpool_str216[sizeof("ISO-IR-203")];
    char stringpool_str217[sizeof("DECHANZI")];
    char stringpool_str218[sizeof("ELOT_928")];
    char stringpool_str219[sizeof("ISO_8859-4")];
    char stringpool_str220[sizeof("IBM819")];
    char stringpool_str221[sizeof("CSGB2312")];
    char stringpool_str222[sizeof("CN-BIG5")];
    char stringpool_str223[sizeof("UCS-2")];
    char stringpool_str224[sizeof("ISO_8859-14")];
    char stringpool_str225[sizeof("X0208")];
    char stringpool_str228[sizeof("KSC_5601")];
    char stringpool_str229[sizeof("ISO-IR-149")];
    char stringpool_str232[sizeof("ISO8859-10")];
    char stringpool_str234[sizeof("RK1048")];
    char stringpool_str237[sizeof("ISO-IR-14")];
    char stringpool_str238[sizeof("TCVN")];
    char stringpool_str239[sizeof("TIS620")];
    char stringpool_str243[sizeof("GB_2312-80")];
    char stringpool_str245[sizeof("VISCII")];
    char stringpool_str246[sizeof("ISO-8859-10")];
    char stringpool_str247[sizeof("ISO-IR-109")];
    char stringpool_str250[sizeof("CSISOLATIN1")];
    char stringpool_str252[sizeof("CSISOLATIN6")];
    char stringpool_str253[sizeof("TIS-620")];
    char stringpool_str254[sizeof("CSISOLATIN3")];
    char stringpool_str255[sizeof("CSVISCII")];
    char stringpool_str256[sizeof("CSISOLATIN2")];
    char stringpool_str257[sizeof("CSISOLATINCYRILLIC")];
    char stringpool_str258[sizeof("CSISOLATIN5")];
    char stringpool_str259[sizeof("GB18030")];
    char stringpool_str260[sizeof("ISO_8859-10")];
    char stringpool_str263[sizeof("UTF-16")];
    char stringpool_str264[sizeof("CSKZ1048")];
    char stringpool_str266[sizeof("GB_1988-80")];
    char stringpool_str267[sizeof("KZ-1048")];
    char stringpool_str268[sizeof("UTF-8")];
    char stringpool_str269[sizeof("UTF-32")];
    char stringpool_str270[sizeof("MS-CYRL")];
    char stringpool_str275[sizeof("CHAR")];
    char stringpool_str276[sizeof("CSKOI8R")];
    char stringpool_str278[sizeof("ISO-IR-110")];
    char stringpool_str280[sizeof("KOI8-R")];
    char stringpool_str281[sizeof("MACCYRILLIC")];
    char stringpool_str282[sizeof("IBM-CP1133")];
    char stringpool_str283[sizeof("PTCP154")];
    char stringpool_str284[sizeof("TIS620.2533-1")];
    char stringpool_str285[sizeof("CP874")];
    char stringpool_str293[sizeof("ISO-IR-144")];
    char stringpool_str297[sizeof("KS_C_5601-1989")];
    char stringpool_str298[sizeof("HZ-GB-2312")];
    char stringpool_str302[sizeof("TIS620.2529-1")];
    char stringpool_str308[sizeof("CSUNICODE11")];
    char stringpool_str312[sizeof("UNICODE-1-1")];
    char stringpool_str314[sizeof("CSPTCP154")];
    char stringpool_str315[sizeof("CSUCS4")];
    char stringpool_str316[sizeof("CYRILLIC-ASIAN")];
    char stringpool_str319[sizeof("UCS-4")];
    char stringpool_str324[sizeof("IBM850")];
    char stringpool_str327[sizeof("TIS620-0")];
    char stringpool_str330[sizeof("ISO-IR-179")];
    char stringpool_str332[sizeof("CP367")];
    char stringpool_str336[sizeof("ISO646-US")];
    char stringpool_str339[sizeof("ISO-10646-UCS-2")];
    char stringpool_str341[sizeof("CP1257")];
    char stringpool_str342[sizeof("GREEK8")];
    char stringpool_str343[sizeof("US-ASCII")];
    char stringpool_str347[sizeof("ISO-IR-100")];
    char stringpool_str352[sizeof("CSISOLATIN4")];
    char stringpool_str353[sizeof("TIS620.2533-0")];
    char stringpool_str354[sizeof("CSISOLATINGREEK")];
    char stringpool_str355[sizeof("VISCII1.1-1")];
    char stringpool_str356[sizeof("CSIBM866")];
    char stringpool_str359[sizeof("CSISO58GB231280")];
    char stringpool_str360[sizeof("EUCKR")];
    char stringpool_str361[sizeof("MS-ANSI")];
    char stringpool_str362[sizeof("MACTHAI")];
    char stringpool_str365[sizeof("CN-GB")];
    char stringpool_str366[sizeof("CSISOLATINARABIC")];
    char stringpool_str368[sizeof("CN-GB-ISOIR165")];
    char stringpool_str369[sizeof("ARMSCII-8")];
    char stringpool_str370[sizeof("MACINTOSH")];
    char stringpool_str372[sizeof("LATIN7")];
    char stringpool_str374[sizeof("EUC-KR")];
    char stringpool_str381[sizeof("JP")];
    char stringpool_str385[sizeof("ROMAN8")];
    char stringpool_str386[sizeof("ISO-2022-KR")];
    char stringpool_str387[sizeof("ISO-10646-UCS-4")];
    char stringpool_str393[sizeof("ISO8859-7")];
    char stringpool_str395[sizeof("CHINESE")];
    char stringpool_str397[sizeof("GEORGIAN-ACADEMY")];
    char stringpool_str398[sizeof("CSUNICODE")];
    char stringpool_str400[sizeof("WINDOWS-1251")];
    char stringpool_str401[sizeof("WINDOWS-1256")];
    char stringpool_str402[sizeof("WINDOWS-1253")];
    char stringpool_str403[sizeof("WINDOWS-1252")];
    char stringpool_str404[sizeof("WINDOWS-1255")];
    char stringpool_str406[sizeof("WINDOWS-1258")];
    char stringpool_str407[sizeof("ISO-8859-7")];
    char stringpool_str410[sizeof("KOI8-U")];
    char stringpool_str411[sizeof("CSPC862LATINHEBREW")];
    char stringpool_str412[sizeof("EUCTW")];
    char stringpool_str413[sizeof("ARABIC")];
    char stringpool_str414[sizeof("CSISO2022KR")];
    char stringpool_str415[sizeof("WINDOWS-936")];
    char stringpool_str416[sizeof("GREEK")];
    char stringpool_str417[sizeof("MULELAO-1")];
    char stringpool_str418[sizeof("ECMA-118")];
    char stringpool_str420[sizeof("TCVN-5712")];
    char stringpool_str421[sizeof("ISO_8859-7")];
    char stringpool_str422[sizeof("TCVN5712-1")];
    char stringpool_str426[sizeof("EUC-TW")];
    char stringpool_str428[sizeof("MACICELAND")];
    char stringpool_str430[sizeof("KS_C_5601-1987")];
    char stringpool_str432[sizeof("KOREAN")];
    char stringpool_str433[sizeof("UCS-2LE")];
    char stringpool_str437[sizeof("CSISOLATINHEBREW")];
    char stringpool_str439[sizeof("CSKSC56011987")];
    char stringpool_str441[sizeof("UNICODELITTLE")];
    char stringpool_str442[sizeof("GEORGIAN-PS")];
    char stringpool_str443[sizeof("ISO-IR-57")];
    char stringpool_str445[sizeof("ISO-IR-87")];
    char stringpool_str446[sizeof("JIS_C6226-1983")];
    char stringpool_str447[sizeof("ISO-IR-127")];
    char stringpool_str448[sizeof("ISO-IR-157")];
    char stringpool_str449[sizeof("DECKOREAN")];
    char stringpool_str451[sizeof("WINDOWS-1254")];
    char stringpool_str453[sizeof("ISO_646.IRV:1991")];
    char stringpool_str454[sizeof("CSISO57GB1988")];
    char stringpool_str458[sizeof("HP-ROMAN8")];
    char stringpool_str464[sizeof("CSUNICODE11UTF7")];
    char stringpool_str465[sizeof("WCHAR_T")];
    char stringpool_str468[sizeof("UNICODEBIG")];
    char stringpool_str469[sizeof("WINDOWS-1250")];
    char stringpool_str470[sizeof("UNICODE-1-1-UTF-7")];
    char stringpool_str472[sizeof("UCS-2-INTERNAL")];
    char stringpool_str475[sizeof("UTF-16LE")];
    char stringpool_str476[sizeof("STRK1048-2002")];
    char stringpool_str479[sizeof("UTF-32LE")];
    char stringpool_str480[sizeof("MS-EE")];
    char stringpool_str481[sizeof("UCS-4LE")];
    char stringpool_str483[sizeof("IBM367")];
    char stringpool_str484[sizeof("ISO_8859-3:1988")];
    char stringpool_str486[sizeof("ISO_8859-5:1988")];
    char stringpool_str487[sizeof("KOI8-RU")];
    char stringpool_str488[sizeof("ISO_8859-8:1988")];
    char stringpool_str491[sizeof("CSMACINTOSH")];
    char stringpool_str493[sizeof("ANSI_X3.4-1986")];
    char stringpool_str497[sizeof("BIG5HKSCS")];
    char stringpool_str498[sizeof("ANSI_X3.4-1968")];
    char stringpool_str500[sizeof("NEXTSTEP")];
    char stringpool_str504[sizeof("CSISO14JISC6220RO")];
    char stringpool_str507[sizeof("CSEUCKR")];
    char stringpool_str508[sizeof("ECMA-114")];
    char stringpool_str511[sizeof("BIG5-HKSCS")];
    char stringpool_str514[sizeof("ISO_8859-9:1989")];
    char stringpool_str515[sizeof("JIS_C6220-1969-RO")];
    char stringpool_str520[sizeof("UCS-4-INTERNAL")];
    char stringpool_str523[sizeof("CSPC850MULTILINGUAL")];
    char stringpool_str524[sizeof("ISO-2022-JP-1")];
    char stringpool_str525[sizeof("CSHPROMAN8")];
    char stringpool_str527[sizeof("ISO-2022-JP-2")];
    char stringpool_str533[sizeof("ISO_8859-4:1988")];
    char stringpool_str534[sizeof("JIS0208")];
    char stringpool_str539[sizeof("ASMO-708")];
    char stringpool_str543[sizeof("MACROMAN")];
    char stringpool_str544[sizeof("MACCROATIAN")];
    char stringpool_str548[sizeof("CSISO159JISX02121990")];
    char stringpool_str549[sizeof("ISO646-JP")];
    char stringpool_str552[sizeof("WINDOWS-1257")];
    char stringpool_str554[sizeof("CSISO2022JP2")];
    char stringpool_str559[sizeof("CSEUCTW")];
    char stringpool_str560[sizeof("UTF-7")];
    char stringpool_str567[sizeof("EUCJP")];
    char stringpool_str581[sizeof("EUC-JP")];
    char stringpool_str591[sizeof("UCS-2BE")];
    char stringpool_str593[sizeof("ISO-2022-JP")];
    char stringpool_str598[sizeof("SHIFT-JIS")];
    char stringpool_str602[sizeof("MS-TURK")];
    char stringpool_str608[sizeof("JIS_X0212")];
    char stringpool_str612[sizeof("SHIFT_JIS")];
    char stringpool_str621[sizeof("CSISO2022JP")];
    char stringpool_str627[sizeof("CSHALFWIDTHKATAKANA")];
    char stringpool_str628[sizeof("ISO_8859-1:1987")];
    char stringpool_str629[sizeof("ISO_8859-6:1987")];
    char stringpool_str630[sizeof("ISO_8859-7:2003")];
    char stringpool_str631[sizeof("ISO_8859-2:1987")];
    char stringpool_str633[sizeof("UTF-16BE")];
    char stringpool_str637[sizeof("UTF-32BE")];
    char stringpool_str639[sizeof("UCS-4BE")];
    char stringpool_str643[sizeof("CSSHIFTJIS")];
    char stringpool_str644[sizeof("MS-HEBR")];
    char stringpool_str646[sizeof("MACARABIC")];
    char stringpool_str649[sizeof("MACGREEK")];
    char stringpool_str652[sizeof("WINDOWS-874")];
    char stringpool_str658[sizeof("MS-GREEK")];
    char stringpool_str659[sizeof("BIGFIVE")];
    char stringpool_str661[sizeof("MACTURKISH")];
    char stringpool_str671[sizeof("JIS_X0201")];
    char stringpool_str673[sizeof("BIG-FIVE")];
    char stringpool_str678[sizeof("HEBREW")];
    char stringpool_str683[sizeof("JIS_X0208")];
    char stringpool_str689[sizeof("JISX0201-1976")];
    char stringpool_str695[sizeof("UCS-2-SWAPPED")];
    char stringpool_str696[sizeof("JIS_X0212-1990")];
    char stringpool_str701[sizeof("JIS_X0208-1983")];
    char stringpool_str702[sizeof("EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE")];
    char stringpool_str707[sizeof("SDECKANJI")];
    char stringpool_str711[sizeof("JAVA")];
    char stringpool_str725[sizeof("MS_KANJI")];
    char stringpool_str727[sizeof("MACCENTRALEUROPE")];
    char stringpool_str731[sizeof("CSISO87JISX0208")];
    char stringpool_str743[sizeof("UCS-4-SWAPPED")];
    char stringpool_str761[sizeof("MACROMANIA")];
    char stringpool_str765[sizeof("JIS_X0212.1990-0")];
    char stringpool_str768[sizeof("JIS_X0208-1990")];
    char stringpool_str780[sizeof("ISO_8859-7:1987")];
    char stringpool_str783[sizeof("TCVN5712-1:1993")];
    char stringpool_str806[sizeof("MS-ARAB")];
    char stringpool_str807[sizeof("JOHAB")];
    char stringpool_str816[sizeof("CSEUCPKDFMTJAPANESE")];
    char stringpool_str821[sizeof("MACUKRAINE")];
    char stringpool_str824[sizeof("BIG5-HKSCS:2001")];
    char stringpool_str843[sizeof("BIG5-HKSCS:1999")];
    char stringpool_str857[sizeof("WINBALTRIM")];
    char stringpool_str875[sizeof("BIG5-HKSCS:2004")];
    char stringpool_str939[sizeof("MACHEBREW")];
  };
static const struct stringpool_t stringpool_contents =
  {
    "L1",
    "L6",
    "L3",
    "L2",
    "L5",
    "L8",
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
    "CP1252",
    "CP1255",
    "CP936",
    "CP1258",
    "GB2312",
    "CP932",
    "C99",
    "HZ",
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
    "ASCII",
    "ISO_8859-15:1998",
    "CP1254",
    "ISO-8859-9",
    "ISO-IR-166",
    "ISO-IR-126",
    "ISO-IR-226",
    "ISO-IR-165",
    "X0212",
    "ISO-IR-58",
    "UHC",
    "EUCCN",
    "ISO-IR-138",
    "ISO_8859-9",
    "L10",
    "SJIS",
    "850",
    "MAC",
    "TACTIS",
    "L7",
    "EUC-CN",
    "LATIN4",
    "CP850",
    "CP1250",
    "KOI8-T",
    "ISO-2022-CN",
    "ISO-IR-159",
    "ISO-CELTIC",
    "ISO_8859-14:1998",
    "IBM866",
    "CP950",
    "IBM862",
    "ISO-2022-CN-EXT",
    "ISO8859-4",
    "CSASCII",
    "US",
    "MS936",
    "ISO8859-14",
    "ISO-IR-199",
    "BIG5",
    "ISO_8859-10:1992",
    "KSC5601",
    "PT154",
    "ISO-IR-148",
    "ISO-8859-4",
    "GBK",
    "CSISO2022CN",
    "CSBIG5",
    "ISO-IR-101",
    "ISO-8859-14",
    "LATIN10",
    "BIG-5",
    "X0201",
    "ISO-IR-203",
    "DECHANZI",
    "ELOT_928",
    "ISO_8859-4",
    "IBM819",
    "CSGB2312",
    "CN-BIG5",
    "UCS-2",
    "ISO_8859-14",
    "X0208",
    "KSC_5601",
    "ISO-IR-149",
    "ISO8859-10",
    "RK1048",
    "ISO-IR-14",
    "TCVN",
    "TIS620",
    "GB_2312-80",
    "VISCII",
    "ISO-8859-10",
    "ISO-IR-109",
    "CSISOLATIN1",
    "CSISOLATIN6",
    "TIS-620",
    "CSISOLATIN3",
    "CSVISCII",
    "CSISOLATIN2",
    "CSISOLATINCYRILLIC",
    "CSISOLATIN5",
    "GB18030",
    "ISO_8859-10",
    "UTF-16",
    "CSKZ1048",
    "GB_1988-80",
    "KZ-1048",
    "UTF-8",
    "UTF-32",
    "MS-CYRL",
    "CHAR",
    "CSKOI8R",
    "ISO-IR-110",
    "KOI8-R",
    "MACCYRILLIC",
    "IBM-CP1133",
    "PTCP154",
    "TIS620.2533-1",
    "CP874",
    "ISO-IR-144",
    "KS_C_5601-1989",
    "HZ-GB-2312",
    "TIS620.2529-1",
    "CSUNICODE11",
    "UNICODE-1-1",
    "CSPTCP154",
    "CSUCS4",
    "CYRILLIC-ASIAN",
    "UCS-4",
    "IBM850",
    "TIS620-0",
    "ISO-IR-179",
    "CP367",
    "ISO646-US",
    "ISO-10646-UCS-2",
    "CP1257",
    "GREEK8",
    "US-ASCII",
    "ISO-IR-100",
    "CSISOLATIN4",
    "TIS620.2533-0",
    "CSISOLATINGREEK",
    "VISCII1.1-1",
    "CSIBM866",
    "CSISO58GB231280",
    "EUCKR",
    "MS-ANSI",
    "MACTHAI",
    "CN-GB",
    "CSISOLATINARABIC",
    "CN-GB-ISOIR165",
    "ARMSCII-8",
    "MACINTOSH",
    "LATIN7",
    "EUC-KR",
    "JP",
    "ROMAN8",
    "ISO-2022-KR",
    "ISO-10646-UCS-4",
    "ISO8859-7",
    "CHINESE",
    "GEORGIAN-ACADEMY",
    "CSUNICODE",
    "WINDOWS-1251",
    "WINDOWS-1256",
    "WINDOWS-1253",
    "WINDOWS-1252",
    "WINDOWS-1255",
    "WINDOWS-1258",
    "ISO-8859-7",
    "KOI8-U",
    "CSPC862LATINHEBREW",
    "EUCTW",
    "ARABIC",
    "CSISO2022KR",
    "WINDOWS-936",
    "GREEK",
    "MULELAO-1",
    "ECMA-118",
    "TCVN-5712",
    "ISO_8859-7",
    "TCVN5712-1",
    "EUC-TW",
    "MACICELAND",
    "KS_C_5601-1987",
    "KOREAN",
    "UCS-2LE",
    "CSISOLATINHEBREW",
    "CSKSC56011987",
    "UNICODELITTLE",
    "GEORGIAN-PS",
    "ISO-IR-57",
    "ISO-IR-87",
    "JIS_C6226-1983",
    "ISO-IR-127",
    "ISO-IR-157",
    "DECKOREAN",
    "WINDOWS-1254",
    "ISO_646.IRV:1991",
    "CSISO57GB1988",
    "HP-ROMAN8",
    "CSUNICODE11UTF7",
    "WCHAR_T",
    "UNICODEBIG",
    "WINDOWS-1250",
    "UNICODE-1-1-UTF-7",
    "UCS-2-INTERNAL",
    "UTF-16LE",
    "STRK1048-2002",
    "UTF-32LE",
    "MS-EE",
    "UCS-4LE",
    "IBM367",
    "ISO_8859-3:1988",
    "ISO_8859-5:1988",
    "KOI8-RU",
    "ISO_8859-8:1988",
    "CSMACINTOSH",
    "ANSI_X3.4-1986",
    "BIG5HKSCS",
    "ANSI_X3.4-1968",
    "NEXTSTEP",
    "CSISO14JISC6220RO",
    "CSEUCKR",
    "ECMA-114",
    "BIG5-HKSCS",
    "ISO_8859-9:1989",
    "JIS_C6220-1969-RO",
    "UCS-4-INTERNAL",
    "CSPC850MULTILINGUAL",
    "ISO-2022-JP-1",
    "CSHPROMAN8",
    "ISO-2022-JP-2",
    "ISO_8859-4:1988",
    "JIS0208",
    "ASMO-708",
    "MACROMAN",
    "MACCROATIAN",
    "CSISO159JISX02121990",
    "ISO646-JP",
    "WINDOWS-1257",
    "CSISO2022JP2",
    "CSEUCTW",
    "UTF-7",
    "EUCJP",
    "EUC-JP",
    "UCS-2BE",
    "ISO-2022-JP",
    "SHIFT-JIS",
    "MS-TURK",
    "JIS_X0212",
    "SHIFT_JIS",
    "CSISO2022JP",
    "CSHALFWIDTHKATAKANA",
    "ISO_8859-1:1987",
    "ISO_8859-6:1987",
    "ISO_8859-7:2003",
    "ISO_8859-2:1987",
    "UTF-16BE",
    "UTF-32BE",
    "UCS-4BE",
    "CSSHIFTJIS",
    "MS-HEBR",
    "MACARABIC",
    "MACGREEK",
    "WINDOWS-874",
    "MS-GREEK",
    "BIGFIVE",
    "MACTURKISH",
    "JIS_X0201",
    "BIG-FIVE",
    "HEBREW",
    "JIS_X0208",
    "JISX0201-1976",
    "UCS-2-SWAPPED",
    "JIS_X0212-1990",
    "JIS_X0208-1983",
    "EXTENDED_UNIX_CODE_PACKED_FORMAT_FOR_JAPANESE",
    "SDECKANJI",
    "JAVA",
    "MS_KANJI",
    "MACCENTRALEUROPE",
    "CSISO87JISX0208",
    "UCS-4-SWAPPED",
    "MACROMANIA",
    "JIS_X0212.1990-0",
    "JIS_X0208-1990",
    "ISO_8859-7:1987",
    "TCVN5712-1:1993",
    "MS-ARAB",
    "JOHAB",
    "CSEUCPKDFMTJAPANESE",
    "MACUKRAINE",
    "BIG5-HKSCS:2001",
    "BIG5-HKSCS:1999",
    "WINBALTRIM",
    "BIG5-HKSCS:2004",
    "MACHEBREW"
  };
#define stringpool ((const char *) &stringpool_contents)

static const struct alias aliases[] =
  {
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 60 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str13, ei_iso8859_1},
#line 134 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str14, ei_iso8859_10},
#line 76 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str15, ei_iso8859_3},
#line 68 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str16, ei_iso8859_2},
#line 126 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str17, ei_iso8859_9},
    {-1},
#line 151 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str19, ei_iso8859_14},
    {-1}, {-1}, {-1},
#line 207 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str23, ei_cp866},
#line 289 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str24, ei_iso646_cn},
    {-1}, {-1},
#line 203 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str27, ei_cp862},
    {-1}, {-1}, {-1}, {-1},
#line 209 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str32, ei_cp1131},
#line 358 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str33, ei_johab},
#line 205 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str34, ei_cp866},
    {-1},
#line 244 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str36, ei_cp1133},
#line 174 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str37, ei_cp1251},
#line 201 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str38, ei_cp862},
#line 189 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str39, ei_cp1256},
    {-1},
#line 180 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str41, ei_cp1253},
    {-1},
#line 177 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str43, ei_cp1252},
    {-1},
#line 186 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str45, ei_cp1255},
    {-1}, {-1},
#line 326 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str48, ei_cp936},
#line 195 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str49, ei_cp1258},
#line 321 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str50, ei_euc_cn},
    {-1},
#line 313 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str52, ei_cp932},
#line 51 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str53, ei_c99},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 333 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str60, ei_hz},
    {-1}, {-1}, {-1},
#line 84 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str64, ei_iso8859_4},
    {-1}, {-1}, {-1},
#line 59 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str68, ei_iso8859_1},
#line 57 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str69, ei_iso8859_1},
#line 133 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str70, ei_iso8859_10},
    {-1},
#line 75 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str72, ei_iso8859_3},
    {-1},
#line 67 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str74, ei_iso8859_2},
    {-1},
#line 125 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str76, ei_iso8859_9},
    {-1}, {-1}, {-1},
#line 150 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str80, ei_iso8859_14},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 227 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str88, ei_hp_roman8},
#line 62 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str89, ei_iso8859_1},
    {-1},
#line 102 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str91, ei_iso8859_6},
    {-1},
#line 78 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str93, ei_iso8859_3},
#line 139 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str94, ei_iso8859_11},
#line 70 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str95, ei_iso8859_2},
#line 166 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str96, ei_iso8859_16},
#line 93 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str97, ei_iso8859_5},
#line 145 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str98, ei_iso8859_13},
    {-1}, {-1},
#line 120 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str101, ei_iso8859_8},
#line 159 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str102, ei_iso8859_15},
#line 53 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str103, ei_iso8859_1},
    {-1},
#line 94 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str105, ei_iso8859_6},
    {-1},
#line 71 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str107, ei_iso8859_3},
#line 137 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str108, ei_iso8859_11},
#line 63 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str109, ei_iso8859_2},
#line 160 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str110, ei_iso8859_16},
#line 87 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str111, ei_iso8859_5},
#line 140 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str112, ei_iso8859_13},
    {-1}, {-1},
#line 114 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str115, ei_iso8859_8},
#line 154 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str116, ei_iso8859_15},
#line 54 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str117, ei_iso8859_1},
#line 91 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str118, ei_iso8859_5},
#line 95 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str119, ei_iso8859_6},
#line 158 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str120, ei_iso8859_15},
#line 72 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str121, ei_iso8859_3},
#line 138 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str122, ei_iso8859_11},
#line 64 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str123, ei_iso8859_2},
#line 161 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str124, ei_iso8859_16},
#line 88 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str125, ei_iso8859_5},
#line 141 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str126, ei_iso8859_13},
#line 128 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str127, ei_iso8859_9},
#line 162 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str128, ei_iso8859_16},
#line 115 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str129, ei_iso8859_8},
#line 155 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str130, ei_iso8859_15},
#line 236 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str131, ei_pt154},
#line 16 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str132, ei_ascii},
#line 354 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str133, ei_cp949},
    {-1},
#line 287 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str135, ei_iso646_cn},
#line 13 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str136, ei_ascii},
#line 156 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str137, ei_iso8859_15},
    {-1},
#line 183 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str139, ei_cp1254},
    {-1},
#line 121 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str141, ei_iso8859_9},
    {-1},
#line 252 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str143, ei_tis620},
    {-1},
#line 107 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str145, ei_iso8859_7},
    {-1}, {-1},
#line 163 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str148, ei_iso8859_16},
#line 295 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str149, ei_isoir165},
#line 283 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str150, ei_jisx0212},
#line 292 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str151, ei_gb2312},
#line 355 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str152, ei_cp949},
#line 320 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str153, ei_euc_cn},
#line 117 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str154, ei_iso8859_8},
#line 122 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str155, ei_iso8859_9},
#line 165 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str156, ei_iso8859_16},
    {-1},
#line 310 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str158, ei_sjis},
#line 199 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str159, ei_cp850},
    {-1},
#line 212 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str161, ei_mac_roman},
    {-1}, {-1},
#line 253 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str164, ei_tis620},
#line 144 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str165, ei_iso8859_13},
    {-1},
#line 319 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str167, ei_euc_cn},
    {-1}, {-1},
#line 83 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str170, ei_iso8859_4},
    {-1}, {-1},
#line 197 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str173, ei_cp850},
    {-1},
#line 171 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str175, ei_cp1250},
    {-1}, {-1},
#line 233 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str178, ei_koi8_t},
#line 330 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str179, ei_iso2022_cn},
    {-1}, {-1},
#line 284 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str182, ei_jisx0212},
#line 152 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str183, ei_iso8859_14},
#line 148 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str184, ei_iso8859_14},
#line 206 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str185, ei_cp866},
#line 344 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str186, ei_cp950},
    {-1}, {-1},
#line 202 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str189, ei_cp862},
#line 332 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str190, ei_iso2022_cn_ext},
#line 86 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str191, ei_iso8859_4},
#line 22 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str192, ei_ascii},
#line 21 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str193, ei_ascii},
#line 327 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str194, ei_cp936},
    {-1},
#line 153 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str196, ei_iso8859_14},
#line 149 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str197, ei_iso8859_14},
#line 338 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str198, ei_ces_big5},
#line 131 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str199, ei_iso8859_10},
#line 356 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str200, ei_cp949},
    {-1},
#line 234 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str202, ei_pt154},
#line 124 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str203, ei_iso8859_9},
    {-1},
#line 79 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str205, ei_iso8859_4},
#line 325 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str206, ei_ces_gbk},
#line 331 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str207, ei_iso2022_cn},
#line 343 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str208, ei_ces_big5},
#line 66 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str209, ei_iso8859_2},
#line 146 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str210, ei_iso8859_14},
#line 164 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str211, ei_iso8859_16},
#line 339 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str212, ei_ces_big5},
#line 270 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str213, ei_jisx0201},
    {-1}, {-1},
#line 157 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str216, ei_iso8859_15},
#line 324 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str217, ei_euc_cn},
#line 109 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str218, ei_iso8859_7},
#line 80 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str219, ei_iso8859_4},
#line 58 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str220, ei_iso8859_1},
#line 323 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str221, ei_euc_cn},
#line 342 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str222, ei_ces_big5},
#line 24 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str223, ei_ucs2},
#line 147 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str224, ei_iso8859_14},
#line 276 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str225, ei_jisx0208},
    {-1}, {-1},
#line 297 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str228, ei_ksc5601},
#line 300 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str229, ei_ksc5601},
    {-1}, {-1},
#line 136 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str232, ei_iso8859_10},
    {-1},
#line 239 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str234, ei_rk1048},
    {-1}, {-1},
#line 265 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str237, ei_iso646_jp},
#line 259 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str238, ei_tcvn},
#line 247 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str239, ei_tis620},
    {-1}, {-1}, {-1},
#line 291 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str243, ei_gb2312},
    {-1},
#line 256 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str245, ei_viscii},
#line 129 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str246, ei_iso8859_10},
#line 74 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str247, ei_iso8859_3},
    {-1}, {-1},
#line 61 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str250, ei_iso8859_1},
    {-1},
#line 135 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str252, ei_iso8859_10},
#line 246 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str253, ei_tis620},
#line 77 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str254, ei_iso8859_3},
#line 258 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str255, ei_viscii},
#line 69 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str256, ei_iso8859_2},
#line 92 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str257, ei_iso8859_5},
#line 127 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str258, ei_iso8859_9},
#line 329 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str259, ei_gb18030},
#line 130 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str260, ei_iso8859_10},
    {-1}, {-1},
#line 38 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str263, ei_utf16},
#line 242 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str264, ei_rk1048},
    {-1},
#line 286 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str266, ei_iso646_cn},
#line 241 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str267, ei_rk1048},
#line 23 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str268, ei_utf8},
#line 41 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str269, ei_utf32},
#line 176 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str270, ei_cp1251},
    {-1}, {-1}, {-1}, {-1},
#line 361 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str275, ei_local_char},
#line 168 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str276, ei_koi8_r},
    {-1},
#line 82 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str278, ei_iso8859_4},
    {-1},
#line 167 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str280, ei_koi8_r},
#line 218 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str281, ei_mac_cyrillic},
#line 245 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str282, ei_cp1133},
#line 235 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str283, ei_pt154},
#line 251 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str284, ei_tis620},
#line 254 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str285, ei_cp874},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 90 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str293, ei_iso8859_5},
    {-1}, {-1}, {-1},
#line 299 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str297, ei_ksc5601},
#line 334 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str298, ei_hz},
    {-1}, {-1}, {-1},
#line 249 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str302, ei_tis620},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 30 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str308, ei_ucs2be},
    {-1}, {-1}, {-1},
#line 29 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str312, ei_ucs2be},
    {-1},
#line 238 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str314, ei_pt154},
#line 35 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str315, ei_ucs4},
#line 237 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str316, ei_pt154},
    {-1}, {-1},
#line 33 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str319, ei_ucs4},
    {-1}, {-1}, {-1}, {-1},
#line 198 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str324, ei_cp850},
    {-1}, {-1},
#line 248 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str327, ei_tis620},
    {-1}, {-1},
#line 142 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str330, ei_iso8859_13},
    {-1},
#line 19 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str332, ei_ascii},
    {-1}, {-1}, {-1},
#line 14 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str336, ei_ascii},
    {-1}, {-1},
#line 25 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str339, ei_ucs2},
    {-1},
#line 192 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str341, ei_cp1257},
#line 110 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str342, ei_iso8859_7},
#line 12 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str343, ei_ascii},
    {-1}, {-1}, {-1},
#line 56 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str347, ei_iso8859_1},
    {-1}, {-1}, {-1}, {-1},
#line 85 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str352, ei_iso8859_4},
#line 250 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str353, ei_tis620},
#line 112 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str354, ei_iso8859_7},
#line 257 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str355, ei_viscii},
#line 208 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str356, ei_cp866},
    {-1}, {-1},
#line 293 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str359, ei_gb2312},
#line 351 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str360, ei_euc_kr},
#line 179 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str361, ei_cp1252},
#line 224 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str362, ei_mac_thai},
    {-1}, {-1},
#line 322 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str365, ei_euc_cn},
#line 101 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str366, ei_iso8859_6},
    {-1},
#line 296 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str368, ei_isoir165},
#line 230 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str369, ei_armscii_8},
#line 211 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str370, ei_mac_roman},
    {-1},
#line 143 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str372, ei_iso8859_13},
    {-1},
#line 350 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str374, ei_euc_kr},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 266 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str381, ei_iso646_jp},
    {-1}, {-1}, {-1},
#line 226 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str385, ei_hp_roman8},
#line 359 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str386, ei_iso2022_kr},
#line 34 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str387, ei_ucs4},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 113 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str393, ei_iso8859_7},
    {-1},
#line 294 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str395, ei_gb2312},
    {-1},
#line 231 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str397, ei_georgian_academy},
#line 26 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str398, ei_ucs2},
    {-1},
#line 175 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str400, ei_cp1251},
#line 190 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str401, ei_cp1256},
#line 181 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str402, ei_cp1253},
#line 178 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str403, ei_cp1252},
#line 187 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str404, ei_cp1255},
    {-1},
#line 196 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str406, ei_cp1258},
#line 103 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str407, ei_iso8859_7},
    {-1}, {-1},
#line 169 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str410, ei_koi8_u},
#line 204 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str411, ei_cp862},
#line 336 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str412, ei_euc_tw},
#line 100 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str413, ei_iso8859_6},
#line 360 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str414, ei_iso2022_kr},
#line 328 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str415, ei_cp936},
#line 111 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str416, ei_iso8859_7},
#line 243 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str417, ei_mulelao},
#line 108 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str418, ei_iso8859_7},
    {-1},
#line 260 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str420, ei_tcvn},
#line 104 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str421, ei_iso8859_7},
#line 261 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str422, ei_tcvn},
    {-1}, {-1}, {-1},
#line 335 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str426, ei_euc_tw},
    {-1},
#line 215 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str428, ei_mac_iceland},
    {-1},
#line 298 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str430, ei_ksc5601},
    {-1},
#line 302 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str432, ei_ksc5601},
#line 31 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str433, ei_ucs2le},
    {-1}, {-1}, {-1},
#line 119 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str437, ei_iso8859_8},
    {-1},
#line 301 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str439, ei_ksc5601},
    {-1},
#line 32 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str441, ei_ucs2le},
#line 232 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str442, ei_georgian_ps},
#line 288 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str443, ei_iso646_cn},
    {-1},
#line 277 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str445, ei_jisx0208},
#line 278 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str446, ei_jisx0208},
#line 97 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str447, ei_iso8859_6},
#line 132 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str448, ei_iso8859_10},
#line 353 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str449, ei_euc_kr},
    {-1},
#line 184 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str451, ei_cp1254},
    {-1},
#line 15 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str453, ei_ascii},
#line 290 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str454, ei_iso646_cn},
    {-1}, {-1}, {-1},
#line 225 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str458, ei_hp_roman8},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 46 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str464, ei_utf7},
#line 362 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str465, ei_local_wchar_t},
    {-1}, {-1},
#line 28 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str468, ei_ucs2be},
#line 172 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str469, ei_cp1250},
#line 45 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str470, ei_utf7},
    {-1},
#line 47 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str472, ei_ucs2internal},
    {-1}, {-1},
#line 40 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str475, ei_utf16le},
#line 240 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str476, ei_rk1048},
    {-1}, {-1},
#line 43 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str479, ei_utf32le},
#line 173 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str480, ei_cp1250},
#line 37 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str481, ei_ucs4le},
    {-1},
#line 20 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str483, ei_ascii},
#line 73 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str484, ei_iso8859_3},
    {-1},
#line 89 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str486, ei_iso8859_5},
#line 170 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str487, ei_koi8_ru},
#line 116 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str488, ei_iso8859_8},
    {-1}, {-1},
#line 213 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str491, ei_mac_roman},
    {-1},
#line 18 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str493, ei_ascii},
    {-1}, {-1}, {-1},
#line 348 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str497, ei_big5hkscs2004},
#line 17 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str498, ei_ascii},
    {-1},
#line 229 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str500, ei_nextstep},
    {-1}, {-1}, {-1},
#line 267 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str504, ei_iso646_jp},
    {-1}, {-1},
#line 352 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str507, ei_euc_kr},
#line 98 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str508, ei_iso8859_6},
    {-1}, {-1},
#line 347 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str511, ei_big5hkscs2004},
    {-1}, {-1},
#line 123 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str514, ei_iso8859_9},
#line 263 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str515, ei_iso646_jp},
    {-1}, {-1}, {-1}, {-1},
#line 49 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str520, ei_ucs4internal},
    {-1}, {-1},
#line 200 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str523, ei_cp850},
#line 316 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str524, ei_iso2022_jp1},
#line 228 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str525, ei_hp_roman8},
    {-1},
#line 317 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str527, ei_iso2022_jp2},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 81 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str533, ei_iso8859_4},
#line 275 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str534, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1},
#line 99 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str539, ei_iso8859_6},
    {-1}, {-1}, {-1},
#line 210 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str543, ei_mac_roman},
#line 216 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str544, ei_mac_croatian},
    {-1}, {-1}, {-1},
#line 285 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str548, ei_jisx0212},
#line 264 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str549, ei_iso646_jp},
    {-1}, {-1},
#line 193 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str552, ei_cp1257},
    {-1},
#line 318 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str554, ei_iso2022_jp2},
    {-1}, {-1}, {-1}, {-1},
#line 337 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str559, ei_euc_tw},
#line 44 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str560, ei_utf7},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 304 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str567, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 303 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str581, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 27 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str591, ei_ucs2be},
    {-1},
#line 314 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str593, ei_iso2022_jp},
    {-1}, {-1}, {-1}, {-1},
#line 309 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str598, ei_sjis},
    {-1}, {-1}, {-1},
#line 185 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str602, ei_cp1254},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 280 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str608, ei_jisx0212},
    {-1}, {-1}, {-1},
#line 308 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str612, ei_sjis},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 315 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str621, ei_iso2022_jp},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 271 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str627, ei_jisx0201},
#line 55 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str628, ei_iso8859_1},
#line 96 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str629, ei_iso8859_6},
#line 106 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str630, ei_iso8859_7},
#line 65 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str631, ei_iso8859_2},
    {-1},
#line 39 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str633, ei_utf16be},
    {-1}, {-1}, {-1},
#line 42 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str637, ei_utf32be},
    {-1},
#line 36 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str639, ei_ucs4be},
    {-1}, {-1}, {-1},
#line 312 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str643, ei_sjis},
#line 188 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str644, ei_cp1255},
    {-1},
#line 223 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str646, ei_mac_arabic},
    {-1}, {-1},
#line 220 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str649, ei_mac_greek},
    {-1}, {-1},
#line 255 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str652, ei_cp874},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 182 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str658, ei_cp1253},
#line 341 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str659, ei_ces_big5},
    {-1},
#line 221 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str661, ei_mac_turkish},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 268 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str671, ei_jisx0201},
    {-1},
#line 340 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str673, ei_ces_big5},
    {-1}, {-1}, {-1}, {-1},
#line 118 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str678, ei_iso8859_8},
    {-1}, {-1}, {-1}, {-1},
#line 272 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str683, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 269 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str689, ei_jisx0201},
    {-1}, {-1}, {-1}, {-1}, {-1},
#line 48 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str695, ei_ucs2swapped},
#line 282 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str696, ei_jisx0212},
    {-1}, {-1}, {-1}, {-1},
#line 273 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str701, ei_jisx0208},
#line 305 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str702, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1},
#line 307 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str707, ei_euc_jp},
    {-1}, {-1}, {-1},
#line 52 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str711, ei_java},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 311 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str725, ei_sjis},
    {-1},
#line 214 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str727, ei_mac_centraleurope},
    {-1}, {-1}, {-1},
#line 279 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str731, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 50 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str743, ei_ucs4swapped},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 217 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str761, ei_mac_romania},
    {-1}, {-1}, {-1},
#line 281 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str765, ei_jisx0212},
    {-1}, {-1},
#line 274 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str768, ei_jisx0208},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1},
#line 105 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str780, ei_iso8859_7},
    {-1}, {-1},
#line 262 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str783, ei_tcvn},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 191 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str806, ei_cp1256},
#line 357 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str807, ei_johab},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 306 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str816, ei_euc_jp},
    {-1}, {-1}, {-1}, {-1},
#line 219 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str821, ei_mac_ukraine},
    {-1}, {-1},
#line 346 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str824, ei_big5hkscs2001},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 345 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str843, ei_big5hkscs1999},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1},
#line 194 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str857, ei_cp1257},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 349 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str875, ei_big5hkscs2004},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
    {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1}, {-1},
#line 222 "lib/aliases_sysosf1.gperf"
    {(int)(long)&((struct stringpool_t *)0)->stringpool_str939, ei_mac_hebrew}
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
