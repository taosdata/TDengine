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

create_key --new=primeid	-x user  dh:prime	$prime @s
create_key --new=generatorid	-x user  dh:generator	$generator @s
create_key --new=privateid	-x user  dh:private	$private @s
create_key --new=logonid	-x logon dh:logon	"00" @s

marker "CHECK WRONG KEY TYPE"
dh_compute --fail $privateid $primeid $logonid
expect_error ENOKEY
dh_compute --fail $privateid $primeid @s
expect_error EOPNOTSUPP

marker "CHECK MISSING KEY"
dh_compute --fail $privateid $primeid 0
expect_error ENOKEY
unlink_key --wait $generatorid @s
dh_compute --fail $privateid $primeid $generatorid
expect_error ENOKEY

echo "++++ FINISHED TEST: $result" >>$OUTPUTFILE

# --- then report the results in the database ---
toolbox_report_result $TEST $result
