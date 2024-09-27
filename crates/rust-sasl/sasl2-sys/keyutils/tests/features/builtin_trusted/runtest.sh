#!/bin/bash

. ../../prepare.inc.sh
. ../../toolbox.inc.sh


# ---- do the actual testing ----

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

if [ $have_public_key = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF PUBLIC-KEY SUPPORT"
    exit 0
fi

# Find the various trusted keys
marker "FIND BUILTIN TRUSTED KEYRINGS"
id_key --to=btk %:.builtin_trusted_keys
id_key --to=stk %:.secondary_trusted_keys
id_key --to=blk %:.blacklist

# There should be at least one built-in trusted key for module signing.
list_keyring $btk
expect_keyring_rlist bkeys
if [ `echo $bkeys | wc -w` = 0 ]; then fail; fi

# Check we can't add random keys to those keyrings
marker "TRY ADDING USER KEYS"
create_key --fail user a a $btk
expect_error EACCES
create_key --fail user a a $stk
expect_error EOPNOTSUPP
create_key --fail user a a $blk
if has_kernel_config "CONFIG_SYSTEM_BLACKLIST_AUTH_UPDATE"; then
	expect_error EOPNOTSUPP
else
	expect_error EACCES
fi

# Try adding a key to the keyrings
marker "TRY ADDING ASYMMETRIC KEYS"
x509="
308205a6 3082038e a0030201 02020900 ed049e52 489f38b0 300d0609 2a864886
f70d0101 05050030 50310d30 0b060355 040a0c04 504b4353 3111300f 06035504
030c0843 41206b65 79203131 2c302a06 092a8648 86f70d01 0901161d 736c6172
74696261 72746661 7374406d 61677261 74686561 2e683267 32301e17 0d313530
37333031 30313332 345a170d 31353038 32393130 31333234 5a305c31 0d300b06
0355040a 0c04504b 4353311d 301b0603 5504030c 144d6f64 756c6520 7369676e
696e6720 6b657920 31312c30 2a06092a 864886f7 0d010901 161d736c 61727469
62617274 66617374 406d6167 72617468 65612e68 32673230 82022230 0d06092a
864886f7 0d010101 05000382 020f0030 82020a02 82020100 bf29e7cc a69ff57e
665c10ae 0d84b0a7 3cb71fbb d9f7a40d cdaaafb6 34e44db1 44546020 43ae84fb
d867638a 2aca75a5 9315efc0 9ad8f736 03f13ede 7c3fcab8 90bcb9ca ddcb7e71
f6fae4b5 6073e1c9 6a877857 b75e4ca2 259a17f5 0021d0be d87eb1d4 cfebeb75
d0e9cbce 30eb3d40 a431a761 aaf0443a 5e896fcf 459516b1 86e10c59 9e8026c0
d4e93686 d97ed7a5 315f1a43 93b40219 6482e471 1fda5f90 17c3bf73 cbc20e5c
5f03570f 336df7be c0d241bd a256fff0 2ce4d0d3 f31c847f a0ae38af a028656e
be30f90c e0918cb1 23791733 597cdd42 551b7df6 dc8daf7c ecef601c 63da23ed
f589d945 6e6d4d94 3a60c9d4 67f386aa 41615f49 4606be7d 9525a621 45ab1f77
3b1bf842 174b75db ada0c03d b4d5730d 99f80837 cac7ef59 5f7c10c6 d061663a
a9293377 edefb9a8 80d65a95 c38a38c2 6973d338 75edc65f 4f968b59 5959fdd9
ac4306b7 69c131c6 dc40dd67 be2234c8 8b30bd20 655dc4f3 f0a9975b 69a0e9b0
da73b7cd fcf7c78e 6b80909b 0b246a57 237d3841 c33704c9 a21d0b6f 4ff60a43
eeca00ec 39f1d4d6 a11c6482 4a7230a3 cdbdebe0 ccc46d22 eaa1b8c8 96fff82c
bc38ae82 0dd27672 71762c07 0f0e7866 24fd2f11 c8163d52 ffbb04e6 258dfc7c
885449d0 a39cde9b a231e4a9 c3ae1862 32eebdec 127159ea e61e0f54 e1c66a44
0167a123 8046e709 def7f0b7 f3400247 d879bcb6 c3d4b1cb 02030100 01a37730
75301d06 03551d0e 04160414 cc4e4c77 13b90773 7c54af46 dd576760 374022fa
301f0603 551d2304 18301680 14dee86f 2d22e7b1 213b44c1 43b5671c 73c1b96f
04300c06 03551d13 0101ff04 02300030 0b060355 1d0f0404 03020780 30180603
551d2501 01ff040e 300c060a 2b060104 01920810 0101300d 06092a86 4886f70d
01010505 00038202 0100805f 5fff469e dd0a83d7 1a0a7336 3b34d020 45c616ca
7c3a6cf9 63f03e95 ee9c560c 0bc08812 67e0c927 0aef98e8 695b3f56 67a9197a
e8ab87d0 306f2f82 83f38967 3054abd2 56a53ac8 65896d43 37873f18 13d30bd4
483b560e b542103e 424f7afd e7f34c57 e575581a 0d2b8473 448a2e0b e0018743
a4aadc8d f0867a6d 89bcdb54 c2a6b095 f22d59d2 ef72bb3d ee7958d8 dda59b39
1083b2ed 2e8df5f9 36b3d6a8 ee8bf7e4 baa948e1 d1b66ce3 d4c6fab5 f937ed71
54e0ff59 b381ea18 61bfaf1f 340eeffc ea34baad 6016ecbe d8667bb9 90d4bf49
d976c1b4 4c97f4ab 09266a0a 8969d5e0 4c3d121a 4bf7219e 31833790 ef67f897
81d4c3a0 b17dadcd 07f16920 d43cecd3 49fdc209 3b91c014 500fd6dc 850c6018
98d63da6 568db3fb 16c6aa31 c38ce97b 1432a4a1 704eea79 91cbf89b f22997a9
54601b86 2a5dab5e 1a3d3a74 af46adf5 37a975e8 71d06700 74cf545c 13a1b34d
3652fcb2 9ee0e67a 14fd4724 8eb1fdbe 77875f18 729ed58b f713f343 5df1d621
23a3d16b cb55b741 ec6ac649 0fb831bf 7eb29394 7557410a 25c5488a fa7735b8
50d48fcf b22c54e4 b7834206 1f12726d 77d87ed5 f1b64bbb 71dbf606 35898e0d
7529ca4e bca021c1 a6edf677 18a6910d 6943b215 cd6d6903 eeb18ecf 606459b5
75c3f9ef c4c0d5f2 133b8abc 33a75a3d 933ff833 53e6d572 d3aca771 413e86e2
15aa4cfd d6e37474 0864"
create_key --fail -x asymmetric "" "$x509" $btk
expect_error EACCES
create_key --fail -x asymmetric "" "$x509" $stk
expect_error ENOKEY
create_key --fail -x asymmetric "" "$x509" $blk
if has_kernel_config "CONFIG_SYSTEM_BLACKLIST_AUTH_UPDATE"; then
	expect_error EOPNOTSUPP
else
	expect_error EACCES
fi

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result

