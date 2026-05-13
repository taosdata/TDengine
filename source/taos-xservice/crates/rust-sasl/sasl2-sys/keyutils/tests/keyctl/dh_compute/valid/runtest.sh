#!/bin/bash

. ../../../prepare.inc.sh
. ../../../toolbox.inc.sh


# ---- do the actual testing ----

if [ $have_dh_compute = 0 ]
then
    toolbox_skip_test $TEST "SKIPPING DUE TO LACK OF DIFFIE-HELLMAN"
    exit 0
fi

result=PASS
echo "++++ BEGINNING TEST" >$OUTPUTFILE

# Prime, generator, and key values created with:
#   openssl dhparam 2048 -check -out dh.pem
#   openssl genpkey -paramfile dh.pem -text
prime="00b6894d8ff7af56d446c950c7e91d"
prime+="298a078daea77966f11fc16f229211"
prime+="737f1f39cef3dad7872a538f6c8f9a"
prime+="276a7cf77bb2d63a872a4db9ed12ae"
prime+="0f1c69af9bf2f2e3691d3615a1d7d5"
prime+="77a87d1cb4aba0494bae0c249b0cce"
prime+="ef6b7ab9a7be70b5b45e4cf7cb71ad"
prime+="8feb7a4d6c7ccb96d5298f0feeb478"
prime+="77605e80a0338691e35862f0f4cbb2"
prime+="09e17dd9febcce4c21577006ceb115"
prime+="7b181592d2f784ba44e006c314df53"
prime+="06bdbb17a010b3660d479356d8d52c"
prime+="5af014536c20897e7653218e2c7a65"
prime+="0a73dc27584598de92de5c62706771"
prime+="fa2d67f625441d911ca03f2149b6d4"
prime+="c76b5ecd9896e9d799a3a500ececc5"
prime+="19e31b71154d7b361bd0dd15f7ce8d"
prime+="fc63"

generator="02"

private="40949da2ca2b7c353de38fefb06ddd"
private+="0d67479a6361c89e77b980d2486c4d"
private+="31971eb88f6572069973e3ae5a43ce"
private+="76bccb35ea05ac6538eb0ea6adee49"
private+="37600435dd7940885d2e3f78c72808"
private+="34f878d3d550cc9305330bb8f02085"
private+="ee6c230d42d84eb67a245b92817fd1"
private+="54bcb1394a289d11afb5a1e50e1395"
private+="0908af70756704e9bf03dff0e5d490"
private+="743841c534cb7e2cf4b9f0493a730b"
private+="0d71096a16bdc0e852f175755134b2"
private+="b41112280a88212728afbe16d417f3"
private+="1893cbe442e06d212d8efe227aa003"
private+="9a65ce998107fae278511c6bf4d599"
private+="32534ae9fc39db80635163c0546657"
private+="500866d5461c5fa3540238324a29f3"
private+="16e068f3ba1737d042cb51a8971bc7"
private+="a2"

read -d '' public <<"EOF"
a4cf1f93 95fce03f d02aaece da1f86bd d8d77b69 29039fcc bd138c98 2483bf9c
7e4406c1 4f3cea24 6cafb29e 95095d0c 6768f13b 31babb24 6c590d92 6c343e69
59dbd47f 65982a3b b1baa7a3 05a72054 89b6cd0d 78397962 fc834fc9 3ec0517e
c218396f 9cff860e 29078aee 6b8598b6 79325014 bb84597d f031e149 edbe1c5a
2a55fe4e bbc64a52 6da59e71 1c7ae5e0 954ba23b 9d58c423 17d84841 815708c8
b9059987 48773eac 2244b286 cd118277 48b7ed3a 5af5cc0f 8f254190 5e16f998
a328e894 acc343f4 66a95281 86cea6a3 93eb4fee f83c0e2e f4a00ce6 fcc9ef81
cc4624d5 ba659411 d1ba7b5f 14a3e286 d42e6ac8 afa9f846 41cb7cb5 66965725
EOF

marker "LOAD SOURCE KEYS"
create_key --new=primeid     -x user dh:prime     $prime @s
create_key --new=generatorid -x user dh:generator $generator @s
create_key --new=privateid   -x user dh:private   $private @s

marker "COMPUTE DH PUBLIC KEY"
dh_compute $privateid $primeid $generatorid
expect_multiline payload "$public"

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE


################################################################
# Testing DH compute with KDF according to SP800-56A
#
# test vectors from http://csrc.nist.gov/groups/STM/cavp/documents/keymgmt/KASTestVectorsFFC2014.zip
################################################################

# SHA-256
marker "LOAD SHA-256 SOURCE KEYS"

# XephemCAVS
private="81b2c65f5cbac00b1353ac38bd77a25a"
private+="8650ed485e413eac1d6c4885"

# P
prime="a3cc6223e50c6e3f7bb0581dcb9e9ff0"
prime+="2c580768328a15207b1c32317fb78496"
prime+="815e3cf7f9d09ccb9fa840ff4798511a"
prime+="17b55928721e5dfbccc54147e0f05f85"
prime+="b3ac410b6ae3f59b796f3feac7fc5249"
prime+="217eb2a04588293a5ade227879f46ceb"
prime+="56457b5c431293e5e104d1b964bd2cdf"
prime+="deffa04049a91e67ee8c86e944f04f94"
prime+="4a30e361f8d15d17e5010cabb4ef40c0"
prime+="eba5f4a252d4fd6cf9dae60e86e4b300"
prime+="9b1dfc926670357261587ad05c00a6c6"
prime+="f0106cec8fc59131515084a870594165"
prime+="b49390db2d00e7538f230d532f4a4eca"
prime+="8309d707c0b3835cee04f3ca558a22c6"
prime+="b520fe25de6ffa90efda4927d018594c"
prime+="0c0b77067393b7f1e0fc7cf216aff39f"

# YephemIUT
xa="9a70822d3f06123d0e518ee11651e5f6"
xa+="b119dc3b97d5b1c0a2a6f6de942564ba"
xa+="10061eecdeb7369ca537499e04b036e9"
xa+="7f445a956f6369ae6e63fd27eae3e347"
xa+="855447d3bac1c60c10e7350772c6c0c6"
xa+="fbf9ca3e38f0e8658825d3b20f1f028f"
xa+="35e34d1235103df2339b5b099d3fe3e5"
xa+="346a691642bac5b0bb03cd5d04d75626"
xa+="21493ff1c4273b6a45c5ecb0b5e908a0"
xa+="f9f562282e853efc9a7ea112e9474ff6"
xa+="9418f7c47ae966d4524ca1701b60a4be"
xa+="15c75e27b405806468156e02cbc58ff4"
xa+="663c96ac0c87368135fa9b0bb6337ae2"
xa+="58521d7d60c2a91b4ed772ad65034049"
xa+="97f6799df663a8999cfd747fa067b905"
xa+="8ab33bc14594366f28f5a2d900b6467a"

# Z
read -d '' shared <<"EOF"
0fdbd9a2 ebf50cba 489b4e4d 7cd6924a 42ee6324 a26988b2 22bc38e6 9cc445f1
eb47c1a4 62eca39f 39bcd7b8 19dede51 30bc38da ec99c16f 40a4e5c1 9c97b796
8b41823d a0650e37 13c73e6f 5f2a9dff 2e67dbf5 40ee66f4 e694c28f ba1d604b
71b57b8a eeb67a35 ba425a38 490b6fb9 f713db22 6f893b7a 8962f426 ba3046fb
cff8538c 16f583e8 ae947672 0ba55ff9 75b440d0 c4565cc7 5837d23a fea61a39
e0b7f6c4 e24c2154 7eb19fce f8dbed10 b06a9cce 971c0f0f ba7c1d5c b5035eaa
4fddd3ba fe757339 e3321e3e 4ebfe9e7 9c6c0401 4df63cf9 28d0a2c0 5b2d5521
030c35f1 c84c97fe 64cad509 8012a003 d52d24c4 1a1f9348 b7575251 3facb02f
EOF

# OI
otherinfo="a1b2c3d4e54341565369640d64c1b2"
otherinfo+="3361b261de78688ea865fcff113c84"

# DKM
read -d '' derived <<"EOF"
8284e313 02c8a26b 393ec52d 9f9e0882
EOF

create_key --update=primeid   -x user dh:prime   $prime @s
create_key --new=xaid         -x user dh:xa      $xa @s
create_key --update=privateid -x user dh:private $private @s

marker "COMPUTE DH SHARED SECRET"
dh_compute $privateid $primeid $xaid
expect_multiline payload "$shared"

marker "COMPUTE DERIVED KEY FROM DH SHARED SECRET (SHA-256)"
echo -e -n $otherinfo | dh_compute_kdf_oi -x $privateid $primeid $xaid 16 "sha256"
expect_multiline payload "$derived"

create_key --new=lzid -x user dh:leadingzero "01" @s

read -d '' derived2 <<"EOF"
0066207b cdab1d64 bbf489b3 d6a0dadc
EOF

marker "COMPUTE DERIVED KEY WITH LEADING ZEROS"
echo -e -n $otherinfo | dh_compute_kdf_oi -x $privateid $primeid $lzid 16 "sha256"
expect_multiline payload "$derived2"

# SHA-224
marker "LOAD SHA-224 SOURCE KEYS"

# XephemCAVS
private="861ba259aba6aa577de22f508ecbbc26"
private+="c5acfccb9ea23b434d6d2b79"

# P
prime="a5b1764e13c81699aba38f0dc0d15e15"
prime+="f50fcd5cf7c22372cafc5ed762941bd9"
prime+="e0fb9aabee7466d2c829aab031db7b1b"
prime+="5a64e68ed53bafb283ba0f018beb3edc"
prime+="957fe453be0daab61b3228763e80758c"
prime+="6d8c283cf630edd9d70a8af330dd0af6"
prime+="a8d594c23cdd24c8ad3fcfea41757772"
prime+="ceed921e63862f246e6f49d8747e44ae"
prime+="f01e309b6dcc80d450383bb1f94dd590"
prime+="84f8e96f856ec7c8335edb055f8ec6c4"
prime+="81520b3f28e80b6209b8ae61cc860e24"
prime+="c822b66c4f97804993bcd0a972b35354"
prime+="01330ebe4b2e923f189b633562e468eb"
prime+="99a4bc88ccbff8df0fd5afcfe6ae1918"
prime+="4214ab3fefb7f0668b8b2683bebd5651"
prime+="a4c63843b9b14bc738d520b1b7212c69"

# YephemIUT
xa="17d71af4353c22122aeb2a0619cc2cf7"
xa+="3553f28e9fb191fdb286b115b9fda866"
xa+="2de5173b1aff70488d9bc848e537d7e5"
xa+="021649d37dc78c94369db90c2784c94d"
xa+="970ac9b5e35efd22d418d31b68d9550b"
xa+="aa7716e98ea6783bb3a845059fbaa4a6"
xa+="720a6a23c56ba52b4d9b726e0068e9eb"
xa+="4d175bff4369f3d2a4af66eecd62ef7b"
xa+="23c337d470952b1767c8bf782f0b58b4"
xa+="fc8245f840787170f4b0a51b5eb46075"
xa+="8addc9f44a73a3f607603bd35073d1a6"
xa+="9a203a0494a8c2021ba0da1f0495f560"
xa+="c0ba81794eeeeb825d1bd34316a52ae1"
xa+="c900100c0d6fa02546ed7a9c38a6a343"
xa+="d68659eeb59cf38104a96bb25a6dbbf0"
xa+="cbc0ede73a7bba675181e0cd2e7b9f89"

# Z
read -d '' shared <<"EOF"
057c22b8 c5872fef 08ebe852 fafab4b7 c2c2ffbb 376d71bd a941b16e 32614adf
ebb82aeb d50f29d3 cec63d10 77f50e21 cf381b87 a818c614 52c5cce2 af85f40c
06615b97 fe8c3a80 68990ac5 83957b52 8dd6d52d a3b51e84 aec355fd 4a3fe5ce
faa3b17c 9e71cb4d 28ecab6d 21297280 e52397b7 ccb1b62d 8d5d3ce4 1d26b2a3
bdbf880b b39e8b02 8a745ff2 9f0984da efe97084 5d850884 525403ca d2a52956
f55b9a89 b2d801f1 710333c0 479c5955 b54c8163 83c65ad9 c78b8c67 cc1b211b
208b9fab b9c99a68 18293e6a 8da069e6 75eb4317 668a7d4b 6f235533 f3ff4ed0
4f8ad579 f9ad14e7 f68ae183 41d603d9 d6297123 00716c98 bbbf16eb 2a2cc92f
EOF

# OI
otherinfo="a1b2c3d4e5434156536964aa27e249"
otherinfo+="bf0a1276468d808259f3b8e2687851"

# DKM
read -d '' derived <<"EOF"
88bf39c0 08eec33a dc3b4430 054ba262
EOF

create_key --update=primeid   -x user dh:prime   $prime @s
create_key --update=xaid      -x user dh:xa      $xa @s
create_key --update=privateid -x user dh:private $private @s

marker "COMPUTE DH SHARED SECRET"
dh_compute $privateid $primeid $xaid
expect_multiline payload "$shared"

marker "COMPUTE DERIVED KEY FROM DH SHARED SECRET (SHA-224)"
echo -e -n $otherinfo | dh_compute_kdf_oi -x $privateid $primeid $xaid 16 "sha224"
expect_multiline payload "$derived"

# --- then report the results in the database ---
toolbox_report_result $TEST $result
