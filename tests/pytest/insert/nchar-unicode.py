###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()

        tdSql.execute('create table tb (ts timestamp, col nchar(1021))')
        tdSql.execute("insert into tb values (now, 'taosdata')")
        tdSql.query("select * from tb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 'taosdata')

        with open("../../README.md", "r") as inputFile:
            data = inputFile.read(1021).replace(
                "\n",
                " ").replace(
                "\\",
                " ").replace(
                "\'",
                " ").replace(
                "\"",
                " ").replace(
                    "[",
                    " ").replace(
                        "]",
                        " ").replace(
                            "!",
                " ")

        tdLog.info("insert %d length data: %s" % (len(data), data))

        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(2)
        tdSql.checkData(1, 1, data)

        # https://www.ltg.ed.ac.uk/~richard/unicode-sample.html
        # Basic Latin
        data = r'! # $ % & ( ) * + , - . / 0 1 2 3 4 5 6 7 8 9 : ; < = > ? @ A B C D E F G H I J K L M N O P Q R S T U V W X Y Z [ \\ ] ^ _ ` a b c d e f g h i j k l m n o p q r s t u v w x y z { | } ~'
        tdLog.info("insert Basic Latin %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(3)

        data = data.replace('\\\\', '\\')
        tdSql.checkData(2, 1, data)
        # tdSql.execute("insert into tb values(now, 'abc')")

        # Latin-1 Supplement
        data = ' ¡ ¢ £ ¤ ¥ ¦ § ¨ © ª « ¬ ­ ® ¯ ° ± ² ³ ´ µ ¶ · ¸ ¹ º » ¼ ½ ¾ ¿ À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ð Ñ Ò Ó Ô Õ Ö × Ø Ù Ú Û Ü Ý Þ ß à á â ã ä å æ ç è é ê ë ì í î ï ð ñ ò ó ô õ ö ÷ ø ù ú û ü ý þ ÿ'
        tdLog.info(
            "insert Latin-1 Supplement %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(4)
        tdSql.checkData(3, 1, data)

        # Latin Extended-A
        data = 'Ā ā Ă ă Ą ą Ć ć Ĉ ĉ Ċ ċ Č č Ď ď Đ đ Ē ē Ĕ ĕ Ė ė Ę ę Ě ě Ĝ ĝ Ğ ğ Ġ ġ Ģ ģ Ĥ ĥ Ħ ħ Ĩ ĩ Ī ī Ĭ ĭ Į į İ ı Ĳ ĳ Ĵ ĵ Ķ ķ ĸ Ĺ ĺ Ļ ļ Ľ ľ Ŀ ŀ Ł ł Ń ń Ņ ņ Ň ň ŉ Ŋ ŋ Ō ō Ŏ ŏ Ő ő Œ œ Ŕ ŕ Ŗ ŗ Ř ř Ś ś Ŝ ŝ Ş ş Š š Ţ ţ Ť ť Ŧ ŧ Ũ ũ Ū ū Ŭ ŭ Ů ů Ű ű Ų ų Ŵ ŵ Ŷ ŷ Ÿ Ź ź Ż ż Ž ž ſ'
        tdLog.info(
            "insert Latin Extended-A %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(5)
        tdSql.checkData(4, 1, data)

        # Latin Extended-B
        data = 'ƀ Ɓ Ƃ ƃ Ƅ ƅ Ɔ Ƈ ƈ Ɖ Ɗ Ƌ ƌ ƍ Ǝ Ə Ɛ Ƒ ƒ Ɠ Ɣ ƕ Ɩ Ɨ Ƙ ƙ ƚ ƛ Ɯ Ɲ ƞ Ɵ Ơ ơ Ƣ ƣ Ƥ ƥ Ʀ Ƨ ƨ Ʃ ƪ ƫ Ƭ ƭ Ʈ Ư ư Ʊ Ʋ Ƴ ƴ Ƶ ƶ Ʒ Ƹ ƹ ƺ ƻ Ƽ ƽ ƾ ƿ ǀ ǁ ǂ ǃ Ǆ ǅ ǆ Ǉ ǈ ǉ Ǌ ǋ ǌ Ǎ ǎ Ǐ ǐ Ǒ ǒ Ǔ ǔ Ǖ ǖ Ǘ ǘ Ǚ ǚ Ǜ ǜ ǝ Ǟ ǟ Ǡ ǡ Ǣ ǣ Ǥ ǥ Ǧ ǧ Ǩ ǩ Ǫ ǫ Ǭ ǭ Ǯ ǯ ǰ Ǳ ǲ ǳ Ǵ ǵ Ǻ ǻ Ǽ ǽ Ǿ ǿ Ȁ ȁ Ȃ ȃ ...'
        tdLog.info(
            "insert Latin Extended-B %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(6)
        tdSql.checkData(5, 1, data)

        # IPA Extensions
        data = 'ɐ ɑ ɒ ɓ ɔ ɕ ɖ ɗ ɘ ə ɚ ɛ ɜ ɝ ɞ ɟ ɠ ɡ ɢ ɣ ɤ ɥ ɦ ɧ ɨ ɩ ɪ ɫ ɬ ɭ ɮ ɯ ɰ ɱ ɲ ɳ ɴ ɵ ɶ ɷ ɸ ɹ ɺ ɻ ɼ ɽ ɾ ɿ ʀ ʁ ʂ ʃ ʄ ʅ ʆ ʇ ʈ ʉ ʊ ʋ ʌ ʍ ʎ ʏ ʐ ʑ ʒ ʓ ʔ ʕ ʖ ʗ ʘ ʙ ʚ ʛ ʜ ʝ ʞ ʟ ʠ ʡ ʢ ʣ ʤ ʥ ʦ ʧ ʨ'
        tdLog.info(
            "insert IPA Extensions %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(7)
        tdSql.checkData(6, 1, data)

        # Spacing Modifier Letters
        data = 'ʰ ʱ ʲ ʳ ʴ ʵ ʶ ʷ ʸ ʹ ʺ ʻ ʼ ʽ ʾ ʿ ˀ ˁ ˂ ˃ ˄ ˅ ˆ ˇ ˈ ˉ ˊ ˋ ˌ ˍ ˎ ˏ ː ˑ ˒ ˓ ˔ ˕ ˖ ˗ ˘ ˙ ˚ ˛ ˜ ˝ ˞ ˠ ˡ ˢ ˣ ˤ ˥ ˦ ˧ ˨ ˩'
        tdLog.info(
            "insert Spacing Modifier Letters %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(8)
        tdSql.checkData(7, 1, data)

        # Combining Diacritical Marks
        data = '̀ ́ ̂ ̃ ̄ ̅ ̆ ̇ ̈ ̉ ̊ ̋ ̌ ̍ ̎ ̏ ̐ ̑ ̒ ̓ ̔ ̕ ̖ ̗ ̘ ̙ ̚ ̛ ̜ ̝ ̞ ̟ ̠ ̡ ̢ ̣ ̤ ̥ ̦ ̧ ̨ ̩ ̪ ̫ ̬ ̭ ̮ ̯ ̰ ̱ ̲ ̳ ̴ ̵ ̶ ̷ ̸ ̹ ̺ ̻ ̼ ̽ ̾ ̿ ̀ ́ ͂ ̓ ̈́ ͅ ͠ ͡'
        tdLog.info(
            "insert Combining Diacritical Marks %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(9)
        tdSql.checkData(8, 1, data)

        # Greek
        data = 'ʹ ͵ ͺ ; ΄ ΅ Ά · Έ Ή Ί Ό Ύ Ώ ΐ Α Β Γ Δ Ε Ζ Η Θ Ι Κ Λ Μ Ν Ξ Ο Π Ρ Σ Τ Υ Φ Χ Ψ Ω Ϊ Ϋ ά έ ή ί ΰ α β γ δ ε ζ η θ ι κ λ μ ν ξ ο π ρ ς σ τ υ φ χ ψ ω ϊ ϋ ό ύ ώ ϐ ϑ ϒ ϓ ϔ ϕ ϖ Ϛ Ϝ Ϟ Ϡ Ϣ ϣ Ϥ ϥ Ϧ ϧ Ϩ ϩ Ϫ ϫ Ϭ ϭ Ϯ ϯ ϰ ϱ ϲ ϳ'
        tdLog.info("insert Greek %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(10)
        tdSql.checkData(9, 1, data)

        # Cyrillic
        data = 'Ё Ђ Ѓ Є Ѕ І Ї Ј Љ Њ Ћ Ќ Ў Џ А Б В Г Д Е Ж З И Й К Л М Н О П Р С Т У Ф Х Ц Ч Ш Щ Ъ Ы Ь Э Ю Я а б в г д е ж з и й к л м н о п р с т у ф х ц ч ш щ ъ ы ь э ю я ё ђ ѓ є ѕ і ї ј љ њ ћ ќ ў џ Ѡ ѡ Ѣ ѣ Ѥ ѥ Ѧ ѧ Ѩ ѩ Ѫ ѫ Ѭ ѭ Ѯ ѯ Ѱ ѱ Ѳ ѳ Ѵ ѵ Ѷ ѷ Ѹ ѹ Ѻ ѻ Ѽ ѽ Ѿ ѿ Ҁ ҁ ҂ ҃ ...'
        tdLog.info("insert Cyrillic %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(11)
        tdSql.checkData(10, 1, data)

        # Armenian
        data = 'Ա Բ Գ Դ Ե Զ Է Ը Թ Ժ Ի Լ Խ Ծ Կ Հ Ձ Ղ Ճ Մ Յ Ն Շ Ո Չ Պ Ջ Ռ Ս Վ Տ Ր Ց Ւ Փ Ք Օ Ֆ ՙ ՚ ՛ ՜ ՝ ՞ ՟ ա բ գ դ ե զ է ը թ ժ ի լ խ ծ կ հ ձ ղ ճ մ յ ն շ ո չ պ ջ ռ ս վ տ ր ց ւ փ ք օ ֆ և ։'
        tdLog.info("insert Armenian %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(12)
        tdSql.checkData(11, 1, data)

        # Hebrew
        data = ' ֒ ֓ ֔ ֕ ֖ ֗ ֘ ֙ ֚ ֛ ֜ ֝ ֞ ֟ ֠ ֡ ֣ ֤ ֥ ֦ ֧ ֨ ֩ ֪ ֫ ֬ ֭ ֮ ֯ ְ ֱ ֲ ֳ ִ ֵ ֶ ַ ָ ֹ ֻ ּ ֽ ־ ֿ ׀ ׁ ׂ ׃ ׄ א ב ג ד ה ו ז ח ט י ך כ ל ם מ ן נ ס ע ף פ ץ צ ק ר ש ת װ ױ ײ ׳ ״'
        tdLog.info("insert Hebrew %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(13)
        tdSql.checkData(12, 1, data)

        # Arabic
        data = '، ؛ ؟ ء آ أ ؤ إ ئ ا ب ة ت ث ج ح خ د ذ ر ز س ش ص ض ط ظ ع غ ـ ف ق ك ل م ن ه و ى ي ً ٌ ٍ َ ُ ِ ّ ْ ٠ ١ ٢ ٣ ٤ ٥ ٦ ٧ ٨ ٩ ٪ ٫ ٬ ٭ ٰ ٱ ٲ ٳ ٴ ٵ ٶ ٷ ٸ ٹ ٺ ٻ ټ ٽ پ ٿ ڀ ځ ڂ ڃ ڄ څ چ ڇ ڈ ډ ڊ ڋ ڌ ڍ ڎ ڏ ڐ ڑ ڒ ړ ڔ ڕ ږ ڗ ژ ڙ ښ ڛ ڜ ڝ ڞ ڟ ڠ ڡ ڢ ڣ ڤ ڥ ڦ ڧ ڨ ک ڪ ګ ڬ ڭ ڮ گ ڰ ڱ ...'
        tdLog.info(
            "FAILED: insert Arabic %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(14)
        tdSql.checkData(13, 1, data)

        # Devanagari
        data = 'ँ ं ः अ आ इ ई उ ऊ ऋ ऌ ऍ ऎ ए ऐ ऑ ऒ ओ औ क ख ग घ ङ च छ ज झ ञ ट ठ ड ढ ण त थ द ध न ऩ प फ ब भ म य र ऱ ल ळ ऴ व श ष स ह ़ ऽ ा ि ी ु ू ृ ॄ ॅ ॆ े ै ॉ ॊ ो ौ ् ॐ ॑ ॒ ॓ ॔ क़ ख़ ग़ ज़ ड़ ढ़ फ़ य़ ॠ ॡ ॢ ॣ । ॥ ० १ २ ३ ४ ५ ६ ७ ८ ९ ॰'
        tdLog.info("insert Devanagari %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(15)
        tdSql.checkData(14, 1, data)

        # Bengali
        data = 'ঁ ং ঃ অ আ ই ঈ উ ঊ ঋ ঌ এ ঐ ও ঔ ক খ গ ঘ ঙ চ ছ জ ঝ ঞ ট ঠ ড ঢ ণ ত থ দ ধ ন প ফ ব ভ ম য র ল শ ষ স হ ় া ি ী ু ূ ৃ ৄ ে ৈ ো ৌ ্ ৗ ড় ঢ় য় ৠ ৡ ৢ ৣ ০ ১ ২ ৩ ৪ ৫ ৬ ৭ ৮ ৯ ৰ ৱ ৲ ৳ ৴ ৵ ৶ ৷ ৸ ৹ ৺'
        tdLog.info("insert Bengali %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(16)
        tdSql.checkData(15, 1, data)

        # Gurmukhi
        data = 'ਂ ਅ ਆ ਇ ਈ ਉ ਊ ਏ ਐ ਓ ਔ ਕ ਖ ਗ ਘ ਙ ਚ ਛ ਜ ਝ ਞ ਟ ਠ ਡ ਢ ਣ ਤ ਥ ਦ ਧ ਨ ਪ ਫ ਬ ਭ ਮ ਯ ਰ ਲ ਲ਼ ਵ ਸ਼ ਸ ਹ ਼ ਾ ਿ ੀ ੁ ੂ ੇ ੈ ੋ ੌ ੍ ਖ਼ ਗ਼ ਜ਼ ੜ ਫ਼ ੦ ੧ ੨ ੩ ੪ ੫ ੬ ੭ ੮ ੯ ੰ ੱ ੲ ੳ ੴ'
        tdLog.info("insert Gurmukhi %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(17)
        tdSql.checkData(16, 1, data)

        # Gujarati
        data = 'ઁ ં ઃ અ આ ઇ ઈ ઉ ઊ ઋ ઍ એ ઐ ઑ ઓ ઔ ક ખ ગ ઘ ઙ ચ છ જ ઝ ઞ ટ ઠ ડ ઢ ણ ત થ દ ધ ન પ ફ બ ભ મ ય ર લ ળ વ શ ષ સ હ ઼ ઽ ા િ ી ુ ૂ ૃ ૄ ૅ ે ૈ ૉ ો ૌ ્ ૐ ૠ ૦ ૧ ૨ ૩ ૪ ૫ ૬ ૭ ૮ ૯'
        tdLog.info("insert Gujarati %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(18)
        tdSql.checkData(17, 1, data)

        # Oriya
        data = 'ଁ ଂ ଃ ଅ ଆ ଇ ଈ ଉ ଊ ଋ ଌ ଏ ଐ ଓ ଔ କ ଖ ଗ ଘ ଙ ଚ ଛ ଜ ଝ ଞ ଟ ଠ ଡ ଢ ଣ ତ ଥ ଦ ଧ ନ ପ ଫ ବ ଭ ମ ଯ ର ଲ ଳ ଶ ଷ ସ ହ ଼ ଽ ା ି ୀ ୁ ୂ ୃ େ ୈ ୋ ୌ ୍ ୖ ୗ ଡ଼ ଢ଼ ୟ ୠ ୡ ୦ ୧ ୨ ୩ ୪ ୫ ୬ ୭ ୮ ୯ ୰'
        tdLog.info("insert Oriya %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(19)
        tdSql.checkData(18, 1, data)

        # Tamil
        data = 'ஂ ஃ அ ஆ இ ஈ உ ஊ எ ஏ ஐ ஒ ஓ ஔ க ங ச ஜ ஞ ட ண த ந ன ப ம ய ர ற ல ள ழ வ ஷ ஸ ஹ ா ி ீ ு ூ ெ ே ை ொ ோ ௌ ் ௗ ௧ ௨ ௩ ௪ ௫ ௬ ௭ ௮ ௯ ௰ ௱ ௲'
        tdLog.info("insert Tamil %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(20)
        tdSql.checkData(19, 1, data)

        # Telugu
        data = 'ఁ ం ః అ ఆ ఇ ఈ ఉ ఊ ఋ ఌ ఎ ఏ ఐ ఒ ఓ ఔ క ఖ గ ఘ ఙ చ ఛ జ ఝ ఞ ట ఠ డ ఢ ణ త థ ద ధ న ప ఫ బ భ మ య ర ఱ ల ళ వ శ ష స హ ా ి ీ ు ూ ృ ౄ ె ే ై ొ ో ౌ ్ ౕ ౖ ౠ ౡ ౦ ౧ ౨ ౩ ౪ ౫ ౬ ౭ ౮ ౯'
        tdLog.info("insert Telugu %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(21)
        tdSql.checkData(20, 1, data)

        # Kannada
        data = 'ಂ ಃ ಅ ಆ ಇ ಈ ಉ ಊ ಋ ಌ ಎ ಏ ಐ ಒ ಓ ಔ ಕ ಖ ಗ ಘ ಙ ಚ ಛ ಜ ಝ ಞ ಟ ಠ ಡ ಢ ಣ ತ ಥ ದ ಧ ನ ಪ ಫ ಬ ಭ ಮ ಯ ರ ಱ ಲ ಳ ವ ಶ ಷ ಸ ಹ ಾ ಿ ೀ ು ೂ ೃ ೄ ೆ ೇ ೈ ೊ ೋ ೌ ್ ೕ ೖ ೞ ೠ ೡ ೦ ೧ ೨ ೩ ೪ ೫ ೬ ೭ ೮ ೯'
        tdLog.info("insert Kannada %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(22)
        tdSql.checkData(21, 1, data)

        # Malayalam
        data = 'ം ഃ അ ആ ഇ ഈ ഉ ഊ ഋ ഌ എ ഏ ഐ ഒ ഓ ഔ ക ഖ ഗ ഘ ങ ച ഛ ജ ഝ ഞ ട ഠ ഡ ഢ ണ ത ഥ ദ ധ ന പ ഫ ബ ഭ മ യ ര റ ല ള ഴ വ ശ ഷ സ ഹ ാ ി ീ ു ൂ ൃ െ േ ൈ ൊ ോ ൌ ് ൗ ൠ ൡ ൦ ൧ ൨ ൩ ൪ ൫ ൬ ൭ ൮ ൯'
        tdLog.info("insert Malayalam %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(23)
        tdSql.checkData(22, 1, data)

        # Thai
        data = 'ก ข ฃ ค ฅ ฆ ง จ ฉ ช ซ ฌ ญ ฎ ฏ ฐ ฑ ฒ ณ ด ต ถ ท ธ น บ ป ผ ฝ พ ฟ ภ ม ย ร ฤ ล ฦ ว ศ ษ ส ห ฬ อ ฮ ฯ ะ ั า ำ ิ ี ึ ื ุ ู ฺ ฿ เ แ โ ใ ไ ๅ ๆ ็ ่ ้ ๊ ๋ ์ ํ ๎ ๏ ๐ ๑ ๒ ๓ ๔ ๕ ๖ ๗ ๘ ๙ ๚ ๛'
        tdLog.info("insert Thai %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(24)
        tdSql.checkData(23, 1, data)

        # Thai
        data = 'ก ข ฃ ค ฅ ฆ ง จ ฉ ช ซ ฌ ญ ฎ ฏ ฐ ฑ ฒ ณ ด ต ถ ท ธ น บ ป ผ ฝ พ ฟ ภ ม ย ร ฤ ล ฦ ว ศ ษ ส ห ฬ อ ฮ ฯ ะ ั า ำ ิ ี ึ ื ุ ู ฺ ฿ เ แ โ ใ ไ ๅ ๆ ็ ่ ้ ๊ ๋ ์ ํ ๎ ๏ ๐ ๑ ๒ ๓ ๔ ๕ ๖ ๗ ๘ ๙ ๚ ๛'
        tdLog.info("insert Thai %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(25)
        tdSql.checkData(24, 1, data)

        # Lao
        data = 'ກ ຂ ຄ ງ ຈ ຊ ຍ ດ ຕ ຖ ທ ນ ບ ປ ຜ ຝ ພ ຟ ມ ຢ ຣ ລ ວ ສ ຫ ອ ຮ ຯ ະ ັ າ ຳ ິ ີ ຶ ື ຸ ູ ົ ຼ ຽ ເ ແ ໂ ໃ ໄ ໆ ່ ້ ໊ ໋ ໌ ໍ ໐ ໑ ໒ ໓ ໔ ໕ ໖ ໗ ໘ ໙ ໜ ໝ'
        tdLog.info("insert Lao %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(26)
        tdSql.checkData(25, 1, data)

        # Tibetan
        data = 'ༀ ༁ ༂ ༃ ༄ ༅ ༆ ༇ ༈ ༉ ༊ ་ ༌ ། ༎ ༏ ༐ ༑ ༒ ༓ ༔ ༕ ༖ ༗ ༘ ༙ ༚ ༛ ༜ ༝ ༞ ༟ ༠ ༡ ༢ ༣ ༤ ༥ ༦ ༧ ༨ ༩ ༪ ༫ ༬ ༭ ༮ ༯ ༰ ༱ ༲ ༳ ༴ ༵ ༶ ༷ ༸ ༹ ༺ ༻ ༼ ༽ ༾ ༿ ཀ ཁ ག གྷ ང ཅ ཆ ཇ ཉ ཊ ཋ ཌ ཌྷ ཎ ཏ ཐ ད དྷ ན པ ཕ བ བྷ མ ཙ ཚ ཛ ཛྷ ཝ ཞ ཟ འ ཡ ར ལ ཤ ཥ ས ཧ ཨ ཀྵ ཱ ི ཱི ུ ཱུ ྲྀ ཷ ླྀ ཹ ེ ཻ ོ ཽ ཾ ཿ ྀ ཱྀ ྂ ྃ ྄ ྅ ྆ ྇ ...'
        tdLog.info("insert Tibetan %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(27)
        tdSql.checkData(26, 1, data)

        # Georgian
        data = 'Ⴀ Ⴁ Ⴂ Ⴃ Ⴄ Ⴅ Ⴆ Ⴇ Ⴈ Ⴉ Ⴊ Ⴋ Ⴌ Ⴍ Ⴎ Ⴏ Ⴐ Ⴑ Ⴒ Ⴓ Ⴔ Ⴕ Ⴖ Ⴗ Ⴘ Ⴙ Ⴚ Ⴛ Ⴜ Ⴝ Ⴞ Ⴟ Ⴠ Ⴡ Ⴢ Ⴣ Ⴤ Ⴥ ა ბ გ დ ე ვ ზ თ ი კ ლ მ ნ ო პ ჟ რ ს ტ უ ფ ქ ღ ყ შ ჩ ც ძ წ ჭ ხ ჯ ჰ ჱ ჲ ჳ ჴ ჵ ჶ ჻'
        tdLog.info("insert Georgian %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(28)
        tdSql.checkData(27, 1, data)

        # Hangul Jamo
        data = 'ᄀ ᄁ ᄂ ᄃ ᄄ ᄅ ᄆ ᄇ ᄈ ᄉ ᄊ ᄋ ᄌ ᄍ ᄎ ᄏ ᄐ ᄑ ᄒ ᄓ ᄔ ᄕ ᄖ ᄗ ᄘ ᄙ ᄚ ᄛ ᄜ ᄝ ᄞ ᄟ ᄠ ᄡ ᄢ ᄣ ᄤ ᄥ ᄦ ᄧ ᄨ ᄩ ᄪ ᄫ ᄬ ᄭ ᄮ ᄯ ᄰ ᄱ ᄲ ᄳ ᄴ ᄵ ᄶ ᄷ ᄸ ᄹ ᄺ ᄻ ᄼ ᄽ ᄾ ᄿ ᅀ ᅁ ᅂ ᅃ ᅄ ᅅ ᅆ ᅇ ᅈ ᅉ ᅊ ᅋ ᅌ ᅍ ᅎ ᅏ ᅐ ᅑ ᅒ ᅓ ᅔ ᅕ ᅖ ᅗ ᅘ ᅙ ᅟ ᅠ ᅡ ᅢ ᅣ ᅤ ᅥ ᅦ ᅧ ᅨ ᅩ ᅪ ᅫ ᅬ ᅭ ᅮ ᅯ ᅰ ᅱ ᅲ ᅳ ᅴ ᅵ ᅶ ᅷ ᅸ ᅹ ᅺ ᅻ ᅼ ᅽ ᅾ ᅿ ᆀ ᆁ ᆂ ᆃ ᆄ ...'
        tdLog.info("insert Hangul Jamo %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(29)
        tdSql.checkData(28, 1, data)

        # Latin Extended Additional
        data = 'Ḁ ḁ Ḃ ḃ Ḅ ḅ Ḇ ḇ Ḉ ḉ Ḋ ḋ Ḍ ḍ Ḏ ḏ Ḑ ḑ Ḓ ḓ Ḕ ḕ Ḗ ḗ Ḙ ḙ Ḛ ḛ Ḝ ḝ Ḟ ḟ Ḡ ḡ Ḣ ḣ Ḥ ḥ Ḧ ḧ Ḩ ḩ Ḫ ḫ Ḭ ḭ Ḯ ḯ Ḱ ḱ Ḳ ḳ Ḵ ḵ Ḷ ḷ Ḹ ḹ Ḻ ḻ Ḽ ḽ Ḿ ḿ Ṁ ṁ Ṃ ṃ Ṅ ṅ Ṇ ṇ Ṉ ṉ Ṋ ṋ Ṍ ṍ Ṏ ṏ Ṑ ṑ Ṓ ṓ Ṕ ṕ Ṗ ṗ Ṙ ṙ Ṛ ṛ Ṝ ṝ Ṟ ṟ Ṡ ṡ Ṣ ṣ Ṥ ṥ Ṧ ṧ Ṩ ṩ Ṫ ṫ Ṭ ṭ Ṯ ṯ Ṱ ṱ Ṳ ṳ Ṵ ṵ Ṷ ṷ Ṹ ṹ Ṻ ṻ Ṽ ṽ Ṿ ṿ ...'
        tdLog.info(
            "insert Latin Extended Additional %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(30)
        tdSql.checkData(29, 1, data)

        # Geek Extended
        data = 'ἀ ἁ ἂ ἃ ἄ ἅ ἆ ἇ Ἀ Ἁ Ἂ Ἃ Ἄ Ἅ Ἆ Ἇ ἐ ἑ ἒ ἓ ἔ ἕ Ἐ Ἑ Ἒ Ἓ Ἔ Ἕ ἠ ἡ ἢ ἣ ἤ ἥ ἦ ἧ Ἠ Ἡ Ἢ Ἣ Ἤ Ἥ Ἦ Ἧ ἰ ἱ ἲ ἳ ἴ ἵ ἶ ἷ Ἰ Ἱ Ἲ Ἳ Ἴ Ἵ Ἶ Ἷ ὀ ὁ ὂ ὃ ὄ ὅ Ὀ Ὁ Ὂ Ὃ Ὄ Ὅ ὐ ὑ ὒ ὓ ὔ ὕ ὖ ὗ Ὑ Ὓ Ὕ Ὗ ὠ ὡ ὢ ὣ ὤ ὥ ὦ ὧ Ὠ Ὡ Ὢ Ὣ Ὤ Ὥ Ὦ Ὧ ὰ ά ὲ έ ὴ ή ὶ ί ὸ ό ὺ ύ ὼ ώ ᾀ ᾁ ᾂ ᾃ ᾄ ᾅ ᾆ ᾇ ᾈ ᾉ ᾊ ᾋ ᾌ ᾍ ...'
        tdLog.info(
            "insert Geek Extended %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(31)
        tdSql.checkData(30, 1, data)

        # General Punctuation
        data = '                      ‐ ‑ ‒ – — ― ‖ ‗ ‘ ’ ‚ ‛ “ ” „ ‟ † ‡ • ‣ ․ ‥ … ‧     '
        tdLog.info(
            "insert General Punctuation %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(32)
        tdSql.checkData(31, 1, data)

        # Superscripts and Subscripts
        data = '⁰ ⁴ ⁵ ⁶ ⁷ ⁸ ⁹ ⁺ ⁻ ⁼ ⁽ ⁾ ⁿ ₀ ₁ ₂ ₃ ₄ ₅ ₆ ₇ ₈ ₉ ₊ ₋ ₌ ₍ ₎'
        tdLog.info(
            "insert Superscripts and Subscripts %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(33)
        tdSql.checkData(32, 1, data)

        # Currency Symbols
        data = '₠ ₡ ₢ ₣ ₤ ₥ ₦ ₧ ₨ ₩ ₪ ₫'
        tdLog.info(
            "insert Currency Symbols %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(34)
        tdSql.checkData(33, 1, data)

        # Combining Marks for Symbols
        data = '⃐ ⃑ ⃒ ⃓ ⃔ ⃕ ⃖ ⃗ ⃘ ⃙ ⃚ ⃛ ⃜ ⃝ ⃞ ⃟ ⃠ ⃡'
        tdLog.info(
            "insert Combining Marks for Symbols %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(35)
        tdSql.checkData(34, 1, data)

        # Letterlike Symbols
        data = '℀ ℁ ℂ ℃ ℄ ℅ ℆ ℇ ℈ ℉ ℊ ℋ ℌ ℍ ℎ ℏ ℐ ℑ ℒ ℓ ℔ ℕ № ℗ ℘ ℙ ℚ ℛ ℜ ℝ ℞ ℟ ℠ ℡ ™ ℣ ℤ ℥ Ω ℧ ℨ ℩ K Å ℬ ℭ ℮ ℯ ℰ ℱ Ⅎ ℳ ℴ ℵ ℶ ℷ ℸ'
        tdLog.info(
            "insert Letterlike Symbols %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(36)
        tdSql.checkData(35, 1, data)

        # Number Forms
        data = '⅓ ⅔ ⅕ ⅖ ⅗ ⅘ ⅙ ⅚ ⅛ ⅜ ⅝ ⅞ ⅟ Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ Ⅼ Ⅽ Ⅾ Ⅿ ⅰ ⅱ ⅲ ⅳ ⅴ ⅵ ⅶ ⅷ ⅸ ⅹ ⅺ ⅻ ⅼ ⅽ ⅾ ⅿ ↀ ↁ ↂ'
        tdLog.info(
            "insert Number Forms %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(37)
        tdSql.checkData(36, 1, data)

        # Arrows
        data = '← ↑ → ↓ ↔ ↕ ↖ ↗ ↘ ↙ ↚ ↛ ↜ ↝ ↞ ↟ ↠ ↡ ↢ ↣ ↤ ↥ ↦ ↧ ↨ ↩ ↪ ↫ ↬ ↭ ↮ ↯ ↰ ↱ ↲ ↳ ↴ ↵ ↶ ↷ ↸ ↹ ↺ ↻ ↼ ↽ ↾ ↿ ⇀ ⇁ ⇂ ⇃ ⇄ ⇅ ⇆ ⇇ ⇈ ⇉ ⇊ ⇋ ⇌ ⇍ ⇎ ⇏ ⇐ ⇑ ⇒ ⇓ ⇔ ⇕ ⇖ ⇗ ⇘ ⇙ ⇚ ⇛ ⇜ ⇝ ⇞ ⇟ ⇠ ⇡ ⇢ ⇣ ⇤ ⇥ ⇦ ⇧ ⇨ ⇩ ⇪'
        tdLog.info("insert Arrows %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(38)
        tdSql.checkData(37, 1, data)

        # Mathematical Operators
        data = '∀ ∁ ∂ ∃ ∄ ∅ ∆ ∇ ∈ ∉ ∊ ∋ ∌ ∍ ∎ ∏ ∐ ∑ − ∓ ∔ ∕ ∖ ∗ ∘ ∙ √ ∛ ∜ ∝ ∞ ∟ ∠ ∡ ∢ ∣ ∤ ∥ ∦ ∧ ∨ ∩ ∪ ∫ ∬ ∭ ∮ ∯ ∰ ∱ ∲ ∳ ∴ ∵ ∶ ∷ ∸ ∹ ∺ ∻ ∼ ∽ ∾ ∿ ≀ ≁ ≂ ≃ ≄ ≅ ≆ ≇ ≈ ≉ ≊ ≋ ≌ ≍ ≎ ≏ ≐ ≑ ≒ ≓ ≔ ≕ ≖ ≗ ≘ ≙ ≚ ≛ ≜ ≝ ≞ ≟ ≠ ≡ ≢ ≣ ≤ ≥ ≦ ≧ ≨ ≩ ≪ ≫ ≬ ≭ ≮ ≯ ≰ ≱ ≲ ≳ ≴ ≵ ≶ ≷ ≸ ≹ ≺ ≻ ≼ ≽ ≾ ≿ ...'
        tdLog.info(
            "insert Mathematical Operators %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(39)
        tdSql.checkData(38, 1, data)

        # Miscellaneous Technical
        data = '⌀ ⌂ ⌃ ⌄ ⌅ ⌆ ⌇ ⌈ ⌉ ⌊ ⌋ ⌌ ⌍ ⌎ ⌏ ⌐ ⌑ ⌒ ⌓ ⌔ ⌕ ⌖ ⌗ ⌘ ⌙ ⌚ ⌛ ⌜ ⌝ ⌞ ⌟ ⌠ ⌡ ⌢ ⌣ ⌤ ⌥ ⌦ ⌧ ⌨ 〈 〉 ⌫ ⌬ ⌭ ⌮ ⌯ ⌰ ⌱ ⌲ ⌳ ⌴ ⌵ ⌶ ⌷ ⌸ ⌹ ⌺ ⌻ ⌼ ⌽ ⌾ ⌿ ⍀ ⍁ ⍂ ⍃ ⍄ ⍅ ⍆ ⍇ ⍈ ⍉ ⍊ ⍋ ⍌ ⍍ ⍎ ⍏ ⍐ ⍑ ⍒ ⍓ ⍔ ⍕ ⍖ ⍗ ⍘ ⍙ ⍚ ⍛ ⍜ ⍝ ⍞ ⍟ ⍠ ⍡ ⍢ ⍣ ⍤ ⍥ ⍦ ⍧ ⍨ ⍩ ⍪ ⍫ ⍬ ⍭ ⍮ ⍯ ⍰ ⍱ ⍲ ⍳ ⍴ ⍵ ⍶ ⍷ ⍸ ⍹ ⍺'
        tdLog.info(
            "insert Miscellaneous Technical %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(40)
        tdSql.checkData(39, 1, data)

        # Control Pictures
        data = '␀ ␁ ␂ ␃ ␄ ␅ ␆ ␇ ␈ ␉ ␊ ␋ ␌ ␍ ␎ ␏ ␐ ␑ ␒ ␓ ␔ ␕ ␖ ␗ ␘ ␙ ␚ ␛ ␜ ␝ ␞ ␟ ␠ ␡ ␢ ␣ ␤'
        tdLog.info(
            "insert Control Pictures %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(41)
        tdSql.checkData(40, 1, data)

        # Optical Character Recognition
        data = '⑀ ⑁ ⑂ ⑃ ⑄ ⑅ ⑆ ⑇ ⑈ ⑉ ⑊'
        tdLog.info(
            "insert Optical Character Recognition %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(42)
        tdSql.checkData(41, 1, data)

        # Enclosed Alphanumerics
        data = '① ② ③ ④ ⑤ ⑥ ⑦ ⑧ ⑨ ⑩ ⑪ ⑫ ⑬ ⑭ ⑮ ⑯ ⑰ ⑱ ⑲ ⑳ ⑴ ⑵ ⑶ ⑷ ⑸ ⑹ ⑺ ⑻ ⑼ ⑽ ⑾ ⑿ ⒀ ⒁ ⒂ ⒃ ⒄ ⒅ ⒆ ⒇ ⒈ ⒉ ⒊ ⒋ ⒌ ⒍ ⒎ ⒏ ⒐ ⒑ ⒒ ⒓ ⒔ ⒕ ⒖ ⒗ ⒘ ⒙ ⒚ ⒛ ⒜ ⒝ ⒞ ⒟ ⒠ ⒡ ⒢ ⒣ ⒤ ⒥ ⒦ ⒧ ⒨ ⒩ ⒪ ⒫ ⒬ ⒭ ⒮ ⒯ ⒰ ⒱ ⒲ ⒳ ⒴ ⒵ Ⓐ Ⓑ Ⓒ Ⓓ Ⓔ Ⓕ Ⓖ Ⓗ Ⓘ Ⓙ Ⓚ Ⓛ Ⓜ Ⓝ Ⓞ Ⓟ Ⓠ Ⓡ Ⓢ Ⓣ Ⓤ Ⓥ Ⓦ Ⓧ Ⓨ Ⓩ ⓐ ⓑ ⓒ ⓓ ⓔ ⓕ ⓖ ⓗ ⓘ ⓙ ⓚ ⓛ ⓜ ⓝ ⓞ ⓟ ...'
        tdLog.info(
            "insert Enclosed Alphanumerics %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(43)
        tdSql.checkData(42, 1, data)

        # Box Drawing
        data = '─ ━ │ ┃ ┄ ┅ ┆ ┇ ┈ ┉ ┊ ┋ ┌ ┍ ┎ ┏ ┐ ┑ ┒ ┓ └ ┕ ┖ ┗ ┘ ┙ ┚ ┛ ├ ┝ ┞ ┟ ┠ ┡ ┢ ┣ ┤ ┥ ┦ ┧ ┨ ┩ ┪ ┫ ┬ ┭ ┮ ┯ ┰ ┱ ┲ ┳ ┴ ┵ ┶ ┷ ┸ ┹ ┺ ┻ ┼ ┽ ┾ ┿ ╀ ╁ ╂ ╃ ╄ ╅ ╆ ╇ ╈ ╉ ╊ ╋ ╌ ╍ ╎ ╏ ═ ║ ╒ ╓ ╔ ╕ ╖ ╗ ╘ ╙ ╚ ╛ ╜ ╝ ╞ ╟ ╠ ╡ ╢ ╣ ╤ ╥ ╦ ╧ ╨ ╩ ╪ ╫ ╬ ╭ ╮ ╯ ╰ ╱ ╲ ╳ ╴ ╵ ╶ ╷ ╸ ╹ ╺ ╻ ╼ ╽ ╾ ╿'
        tdLog.info("insert Box Drawing %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(44)
        tdSql.checkData(43, 1, data)

        # Block Elements
        data = '▀ ▁ ▂ ▃ ▄ ▅ ▆ ▇ █ ▉ ▊ ▋ ▌ ▍ ▎ ▏ ▐ ░ ▒ ▓ ▔ ▕'
        tdLog.info(
            "insert Block Elements %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(45)
        tdSql.checkData(44, 1, data)

        # Geometric Shapes
        data = '■ □ ▢ ▣ ▤ ▥ ▦ ▧ ▨ ▩ ▪ ▫ ▬ ▭ ▮ ▯ ▰ ▱ ▲ △ ▴ ▵ ▶ ▷ ▸ ▹ ► ▻ ▼ ▽ ▾ ▿ ◀ ◁ ◂ ◃ ◄ ◅ ◆ ◇ ◈ ◉ ◊ ○ ◌ ◍ ◎ ● ◐ ◑ ◒ ◓ ◔ ◕ ◖ ◗ ◘ ◙ ◚ ◛ ◜ ◝ ◞ ◟ ◠ ◡ ◢ ◣ ◤ ◥ ◦ ◧ ◨ ◩ ◪ ◫ ◬ ◭ ◮ ◯'
        tdLog.info(
            "insert Geometric Shapes %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(46)
        tdSql.checkData(45, 1, data)

        # Miscellaneous Symbols
        data = '☀ ☁ ☂ ☃ ☄ ★ ☆ ☇ ☈ ☉ ☊ ☋ ☌ ☍ ☎ ☏ ☐ ☑ ☒ ☓ ☚ ☛ ☜ ☝ ☞ ☟ ☠ ☡ ☢ ☣ ☤ ☥ ☦ ☧ ☨ ☩ ☪ ☫ ☬ ☭ ☮ ☯ ☰ ☱ ☲ ☳ ☴ ☵ ☶ ☷ ☸ ☹ ☺ ☻ ☼ ☽ ☾ ☿ ♀ ♁ ♂ ♃ ♄ ♅ ♆ ♇ ♈ ♉ ♊ ♋ ♌ ♍ ♎ ♏ ♐ ♑ ♒ ♓ ♔ ♕ ♖ ♗ ♘ ♙ ♚ ♛ ♜ ♝ ♞ ♟ ♠ ♡ ♢ ♣ ♤ ♥ ♦ ♧ ♨ ♩ ♪ ♫ ♬ ♭ ♮ ♯'
        tdLog.info(
            "insert Miscellaneous Symbols %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(47)
        tdSql.checkData(46, 1, data)

        # Dingbats
        data = '✁ ✂ ✃ ✄ ✆ ✇ ✈ ✉ ✌ ✍ ✎ ✏ ✐ ✑ ✒ ✓ ✔ ✕ ✖ ✗ ✘ ✙ ✚ ✛ ✜ ✝ ✞ ✟ ✠ ✡ ✢ ✣ ✤ ✥ ✦ ✧ ✩ ✪ ✫ ✬ ✭ ✮ ✯ ✰ ✱ ✲ ✳ ✴ ✵ ✶ ✷ ✸ ✹ ✺ ✻ ✼ ✽ ✾ ✿ ❀ ❁ ❂ ❃ ❄ ❅ ❆ ❇ ❈ ❉ ❊ ❋ ❍ ❏ ❐ ❑ ❒ ❖ ❘ ❙ ❚ ❛ ❜ ❝ ❞ ❡ ❢ ❣ ❤ ❥ ❦ ❧ ❶ ❷ ❸ ❹ ❺ ❻ ❼ ❽ ❾ ❿ ➀ ➁ ➂ ➃ ➄ ➅ ➆ ➇ ➈ ➉ ➊ ➋ ➌ ➍ ➎ ➏ ➐ ➑ ➒ ➓ ➔ ➘ ➙ ➚ ➛ ➜ ➝ ...'
        tdLog.info("insert Dingbats %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(48)
        tdSql.checkData(47, 1, data)

        # CJK Symbols and Punctuation
        data = '、 。 〃 〄 々 〆 〇 〈 〉 《 》 「 」 『 』 【 】 〒 〓 〔 〕 〖 〗 〘 〙 〚 〛 〜 〝 〞 〟 〠 〡 〢 〣 〤 〥 〦 〧 〨 〩 〪 〫 〬 〭 〮 〯 〰 〱 〲 〳 〴 〵 〶 〷 〿'
        tdLog.info(
            "insert CJK Symbols and Punctuation %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(49)
        tdSql.checkData(48, 1, data)

        # Hiragana
        data = 'ぁ あ ぃ い ぅ う ぇ え ぉ お か が き ぎ く ぐ け げ こ ご さ ざ し じ す ず せ ぜ そ ぞ た だ ち ぢ っ つ づ て で と ど な に ぬ ね の は ば ぱ ひ び ぴ ふ ぶ ぷ へ べ ぺ ほ ぼ ぽ ま み む め も ゃ や ゅ ゆ ょ よ ら り る れ ろ ゎ わ ゐ ゑ を ん ゔ ゙ ゚ ゛ ゜ ゝ ゞ'
        tdLog.info("insert Hiragana %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(50)
        tdSql.checkData(49, 1, data)

        # Katakana
        data = 'ァ ア ィ イ ゥ ウ ェ エ ォ オ カ ガ キ ギ ク グ ケ ゲ コ ゴ サ ザ シ ジ ス ズ セ ゼ ソ ゾ タ ダ チ ヂ ッ ツ ヅ テ デ ト ド ナ ニ ヌ ネ ノ ハ バ パ ヒ ビ ピ フ ブ プ ヘ ベ ペ ホ ボ ポ マ ミ ム メ モ ャ ヤ ュ ユ ョ ヨ ラ リ ル レ ロ ヮ ワ ヰ ヱ ヲ ン ヴ ヵ ヶ ヷ ヸ ヹ ヺ ・ ー ヽ ヾ'
        tdLog.info("insert Katakana %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(51)
        tdSql.checkData(50, 1, data)

        # Bopomofo
        data = 'ㄅ ㄆ ㄇ ㄈ ㄉ ㄊ ㄋ ㄌ ㄍ ㄎ ㄏ ㄐ ㄑ ㄒ ㄓ ㄔ ㄕ ㄖ ㄗ ㄘ ㄙ ㄚ ㄛ ㄜ ㄝ ㄞ ㄟ ㄠ ㄡ ㄢ ㄣ ㄤ ㄥ ㄦ ㄧ ㄨ ㄩ ㄪ ㄫ ㄬ'
        tdLog.info("insert Bopomofo %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(52)
        tdSql.checkData(51, 1, data)

        # Hangul Compatibility Jamo
        data = 'ㄱ ㄲ ㄳ ㄴ ㄵ ㄶ ㄷ ㄸ ㄹ ㄺ ㄻ ㄼ ㄽ ㄾ ㄿ ㅀ ㅁ ㅂ ㅃ ㅄ ㅅ ㅆ ㅇ ㅈ ㅉ ㅊ ㅋ ㅌ ㅍ ㅎ ㅏ ㅐ ㅑ ㅒ ㅓ ㅔ ㅕ ㅖ ㅗ ㅘ ㅙ ㅚ ㅛ ㅜ ㅝ ㅞ ㅟ ㅠ ㅡ ㅢ ㅣ ㅤ ㅥ ㅦ ㅧ ㅨ ㅩ ㅪ ㅫ ㅬ ㅭ ㅮ ㅯ ㅰ ㅱ ㅲ ㅳ ㅴ ㅵ ㅶ ㅷ ㅸ ㅹ ㅺ ㅻ ㅼ ㅽ ㅾ ㅿ ㆀ ㆁ ㆂ ㆃ ㆄ ㆅ ㆆ ㆇ ㆈ ㆉ ㆊ ㆋ ㆌ ㆍ ㆎ'
        tdLog.info(
            "insert Hangul Compatibility Jamo %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(53)
        tdSql.checkData(52, 1, data)

        # Kanbun
        data = '㆐ ㆑ ㆒ ㆓ ㆔ ㆕ ㆖ ㆗ ㆘ ㆙ ㆚ ㆛ ㆜ ㆝ ㆞ ㆟'
        tdLog.info("insert Kanbun %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(54)
        tdSql.checkData(53, 1, data)

        # Enclosed CJK Letters and Months
        data = '㈀ ㈁ ㈂ ㈃ ㈄ ㈅ ㈆ ㈇ ㈈ ㈉ ㈊ ㈋ ㈌ ㈍ ㈎ ㈏ ㈐ ㈑ ㈒ ㈓ ㈔ ㈕ ㈖ ㈗ ㈘ ㈙ ㈚ ㈛ ㈜ ㈠ ㈡ ㈢ ㈣ ㈤ ㈥ ㈦ ㈧ ㈨ ㈩ ㈪ ㈫ ㈬ ㈭ ㈮ ㈯ ㈰ ㈱ ㈲ ㈳ ㈴ ㈵ ㈶ ㈷ ㈸ ㈹ ㈺ ㈻ ㈼ ㈽ ㈾ ㈿ ㉀ ㉁ ㉂ ㉃ ㉠ ㉡ ㉢ ㉣ ㉤ ㉥ ㉦ ㉧ ㉨ ㉩ ㉪ ㉫ ㉬ ㉭ ㉮ ㉯ ㉰ ㉱ ㉲ ㉳ ㉴ ㉵ ㉶ ㉷ ㉸ ㉹ ㉺ ㉻ ㉿ ㊀ ㊁ ㊂ ㊃ ㊄ ㊅ ㊆ ㊇ ㊈ ㊉ ㊊ ㊋ ㊌ ㊍ ㊎ ㊏ ㊐ ㊑ ㊒ ㊓ ㊔ ㊕ ㊖ ㊗ ㊘ ㊙ ㊚ ㊛ ㊜ ㊝ ㊞ ㊟ ㊠ ㊡ ...'
        tdLog.info(
            "insert Enclosed CJK Letters and Months %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(55)
        tdSql.checkData(54, 1, data)

        # CJK Compatibility
        data = '㌀ ㌁ ㌂ ㌃ ㌄ ㌅ ㌆ ㌇ ㌈ ㌉ ㌊ ㌋ ㌌ ㌍ ㌎ ㌏ ㌐ ㌑ ㌒ ㌓ ㌔ ㌕ ㌖ ㌗ ㌘ ㌙ ㌚ ㌛ ㌜ ㌝ ㌞ ㌟ ㌠ ㌡ ㌢ ㌣ ㌤ ㌥ ㌦ ㌧ ㌨ ㌩ ㌪ ㌫ ㌬ ㌭ ㌮ ㌯ ㌰ ㌱ ㌲ ㌳ ㌴ ㌵ ㌶ ㌷ ㌸ ㌹ ㌺ ㌻ ㌼ ㌽ ㌾ ㌿ ㍀ ㍁ ㍂ ㍃ ㍄ ㍅ ㍆ ㍇ ㍈ ㍉ ㍊ ㍋ ㍌ ㍍ ㍎ ㍏ ㍐ ㍑ ㍒ ㍓ ㍔ ㍕ ㍖ ㍗ ㍘ ㍙ ㍚ ㍛ ㍜ ㍝ ㍞ ㍟ ㍠ ㍡ ㍢ ㍣ ㍤ ㍥ ㍦ ㍧ ㍨ ㍩ ㍪ ㍫ ㍬ ㍭ ㍮ ㍯ ㍰ ㍱ ㍲ ㍳ ㍴ ㍵ ㍶ ㍻ ㍼ ㍽ ㍾ ㍿ ㎀ ㎁ ㎂ ㎃ ...'
        tdLog.info(
            "insert CJK Compatibility %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(56)
        tdSql.checkData(55, 1, data)

        # CJK Unified Ideographs
        data = '一 丁 丂 七 丄 丅 丆 万 丈 三 上 下 丌 不 与 丏 丐 丑 丒 专 且 丕 世 丗 丘 丙 业 丛 东 丝 丞 丟 丠 両 丢 丣 两 严 並 丧 丨 丩 个 丫 丬 中 丮 丯 丰 丱 串 丳 临 丵 丶 丷 丸 丹 为 主 丼 丽 举 丿 乀 乁 乂 乃 乄 久 乆 乇 么 义 乊 之 乌 乍 乎 乏 乐 乑 乒 乓 乔 乕 乖 乗 乘 乙 乚 乛 乜 九 乞 也 习 乡 乢 乣 乤 乥 书 乧 乨 乩 乪 乫 乬 乭 乮 乯 买 乱 乲 乳 乴 乵 乶 乷 乸 乹 乺 乻 乼 乽 乾 乿 ...'
        tdLog.info(
            "insert CJK Unified Ideographs %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(57)
        tdSql.checkData(56, 1, data)

        # Hangul Syllables
        data = '一 丁 丂 七 丄 丅 丆 万 丈 三 上 下 丌 不 与 丏 丐 丑 丒 专 且 丕 世 丗 丘 丙 业 丛 东 丝 丞 丟 丠 両 丢 丣 两 严 並 丧 丨 丩 个 丫 丬 中 丮 丯 丰 丱 串 丳 临 丵 丶 丷 丸 丹 为 主 丼 丽 举 丿 乀 乁 乂 乃 乄 久 乆 乇 么 义 乊 之 乌 乍 乎 乏 乐 乑 乒 乓 乔 乕 乖 乗 乘 乙 乚 乛 乜 九 乞 也 习 乡 乢 乣 乤 乥 书 乧 乨 乩 乪 乫 乬 乭 乮 乯 买 乱 乲 乳 乴 乵 乶 乷 乸 乹 乺 乻 乼 乽 乾 乿 ...'
        tdLog.info(
            "insert Hangul Syllables %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(58)
        tdSql.checkData(57, 1, data)

        # Private Use
        data = '                                                                                                                                ...'
        tdLog.info("insert Private Use %d length data: %s" % (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(59)
        tdSql.checkData(58, 1, data)

        # CJK Compatibility Ideographs
        data = '豈 更 車 賈 滑 串 句 龜 龜 契 金 喇 奈 懶 癩 羅 蘿 螺 裸 邏 樂 洛 烙 珞 落 酪 駱 亂 卵 欄 爛 蘭 鸞 嵐 濫 藍 襤 拉 臘 蠟 廊 朗 浪 狼 郎 來 冷 勞 擄 櫓 爐 盧 老 蘆 虜 路 露 魯 鷺 碌 祿 綠 菉 錄 鹿 論 壟 弄 籠 聾 牢 磊 賂 雷 壘 屢 樓 淚 漏 累 縷 陋 勒 肋 凜 凌 稜 綾 菱 陵 讀 拏 樂 諾 丹 寧 怒 率 異 北 磻 便 復 不 泌 數 索 參 塞 省 葉 說 殺 辰 沈 拾 若 掠 略 亮 兩 凉 梁 糧 良 諒 量 勵 ...'
        tdLog.info(
            "insert CJK Compatibility Ideographs %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(60)
        tdSql.checkData(59, 1, data)

        # Alphabetic Presentation Forms
        data = 'ﬀ ﬁ ﬂ ﬃ ﬄ ﬅ ﬆ ﬓ ﬔ ﬕ ﬖ ﬗ ﬞ ײַ ﬠ ﬡ ﬢ ﬣ ﬤ ﬥ ﬦ ﬧ ﬨ ﬩ שׁ שׂ שּׁ שּׂ אַ אָ אּ בּ גּ דּ הּ וּ זּ טּ יּ ךּ כּ לּ מּ נּ סּ ףּ פּ צּ קּ רּ שּ תּ וֹ בֿ כֿ פֿ ﭏ'
        tdLog.info(
            "insert Alphabetic Presentation Forms %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(61)
        tdSql.checkData(60, 1, data)

        # Arabic Presentation Forms-A
        data = 'ﭐ ﭑ ﭒ ﭓ ﭔ ﭕ ﭖ ﭗ ﭘ ﭙ ﭚ ﭛ ﭜ ﭝ ﭞ ﭟ ﭠ ﭡ ﭢ ﭣ ﭤ ﭥ ﭦ ﭧ ﭨ ﭩ ﭪ ﭫ ﭬ ﭭ ﭮ ﭯ ﭰ ﭱ ﭲ ﭳ ﭴ ﭵ ﭶ ﭷ ﭸ ﭹ ﭺ ﭻ ﭼ ﭽ ﭾ ﭿ ﮀ ﮁ ﮂ ﮃ ﮄ ﮅ ﮆ ﮇ ﮈ ﮉ ﮊ ﮋ ﮌ ﮍ ﮎ ﮏ ﮐ ﮑ ﮒ ﮓ ﮔ ﮕ ﮖ ﮗ ﮘ ﮙ ﮚ ﮛ ﮜ ﮝ ﮞ ﮟ ﮠ ﮡ ﮢ ﮣ ﮤ ﮥ ﮦ ﮧ ﮨ ﮩ ﮪ ﮫ ﮬ ﮭ ﮮ ﮯ ﮰ ﮱ ﯓ ﯔ ﯕ ﯖ ﯗ ﯘ ﯙ ﯚ ﯛ ﯜ ﯝ ﯞ ﯟ ﯠ ﯡ ﯢ ﯣ ﯤ ﯥ ﯦ ﯧ ﯨ ﯩ ﯪ ﯫ ﯬ ﯭ ﯮ ﯯ ﯰ ...'
        tdLog.info(
            "insert Arabic Presentation Forms-A %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(62)
        tdSql.checkData(61, 1, data)

        # Combining Half Marks
        data = '︠ ︡ ︢ ︣'
        tdLog.info(
            "insert Combining Half Marks %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(63)
        tdSql.checkData(62, 1, data)

        # CJK Compatibility Forms
        data = '︰ ︱ ︲ ︳ ︴ ︵ ︶ ︷ ︸ ︹ ︺ ︻ ︼ ︽ ︾ ︿ ﹀ ﹁ ﹂ ﹃ ﹄ ﹉ ﹊ ﹋ ﹌ ﹍ ﹎ ﹏'
        tdLog.info(
            "insert CJK Compatibility Forms %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(64)
        tdSql.checkData(63, 1, data)

        # Small Form Variants
        data = '﹐ ﹑ ﹒ ﹔ ﹕ ﹖ ﹗ ﹘ ﹙ ﹚ ﹛ ﹜ ﹝ ﹞ ﹟ ﹠ ﹡ ﹢ ﹣ ﹤ ﹥ ﹦ ﹨ ﹩ ﹪ ﹫'
        tdLog.info(
            "insert Small Form Variants %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(65)
        tdSql.checkData(64, 1, data)

        # Arabic Presentation Forms-B
        data = 'ﹰ ﹱ ﹲ ﹴ ﹶ ﹷ ﹸ ﹹ ﹺ ﹻ ﹼ ﹽ ﹾ ﹿ ﺀ ﺁ ﺂ ﺃ ﺄ ﺅ ﺆ ﺇ ﺈ ﺉ ﺊ ﺋ ﺌ ﺍ ﺎ ﺏ ﺐ ﺑ ﺒ ﺓ ﺔ ﺕ ﺖ ﺗ ﺘ ﺙ ﺚ ﺛ ﺜ ﺝ ﺞ ﺟ ﺠ ﺡ ﺢ ﺣ ﺤ ﺥ ﺦ ﺧ ﺨ ﺩ ﺪ ﺫ ﺬ ﺭ ﺮ ﺯ ﺰ ﺱ ﺲ ﺳ ﺴ ﺵ ﺶ ﺷ ﺸ ﺹ ﺺ ﺻ ﺼ ﺽ ﺾ ﺿ ﻀ ﻁ ﻂ ﻃ ﻄ ﻅ ﻆ ﻇ ﻈ ﻉ ﻊ ﻋ ﻌ ﻍ ﻎ ﻏ ﻐ ﻑ ﻒ ﻓ ﻔ ﻕ ﻖ ﻗ ﻘ ﻙ ﻚ ﻛ ﻜ ﻝ ﻞ ﻟ ﻠ ﻡ ﻢ ﻣ ﻤ ﻥ ﻦ ﻧ ﻨ ﻩ ﻪ ﻫ ﻬ ﻭ ﻮ ﻯ ﻰ ﻱ ...'
        tdLog.info(
            "insert Arabic Presentation Forms-B %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(66)
        tdSql.checkData(65, 1, data)

        # Halfwidth and Fullwidth Forms
        data = '！ ＂ ＃ ＄ ％ ＆ ＇ （ ） ＊ ＋ ， － ． ／ ０ １ ２ ３ ４ ５ ６ ７ ８ ９ ： ； ＜ ＝ ＞ ？ ＠ Ａ Ｂ Ｃ Ｄ Ｅ Ｆ Ｇ Ｈ Ｉ Ｊ Ｋ Ｌ Ｍ Ｎ Ｏ Ｐ Ｑ Ｒ Ｓ Ｔ Ｕ Ｖ Ｗ Ｘ Ｙ Ｚ ［ ＼ ］ ＾ ＿ ｀ ａ ｂ ｃ ｄ ｅ ｆ ｇ ｈ ｉ ｊ ｋ ｌ ｍ ｎ ｏ ｐ ｑ ｒ ｓ ｔ ｕ ｖ ｗ ｘ ｙ ｚ ｛ ｜ ｝ ～ ｡ ｢ ｣ ､ ･ ｦ ｧ ｨ ｩ ｪ ｫ ｬ ｭ ｮ ｯ ｰ ｱ ｲ ｳ ｴ ｵ ｶ ｷ ｸ ｹ ｺ ｻ ｼ ｽ ｾ ｿ ﾀ ﾁ ﾂ ...'
        tdLog.info(
            "insert Halfwidth and Fullwidth Forms %d length data: %s" %
            (len(data), data))
        tdSql.execute("insert into tb values (now, '%s')" % data)
        tdSql.query("select * from tb")
        tdSql.checkRows(67)
        tdSql.checkData(66, 1, data)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
