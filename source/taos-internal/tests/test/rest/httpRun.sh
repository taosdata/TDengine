#!/bin/bash
root=~/dev/taosdata/test/rest
caseDir=$root/casedir
echo "############$caseDir##############"
caseNumber=$1
req=$2
ignoreColTime=$3
scriptDebug=$4
echo "####################################################################"
echo "Begin of handling $caseNumber->$req"

runlogDir="$caseDir/$caseNumber/runlog"
ofclogDir="$caseDir/$caseNumber/ofclog"

cd $root
echo -e "\033[33mhttpie $caseDir/$caseNumber/request/$req.msg\033[0m"
bash $caseDir/$caseNumber/request/$req.msg $root 2>&1| tee $runlogDir/$req.log

cd $runlogDir 
sed '/^Date/d;/^TRANS/d;/h-h id/d;/e-e id/d;/^==/d;/SessionId(F/d' $req.log > $req.log.tmp
sed -re 's/RAND(F:c0)(L:16) =.*/RAND(F:c0)(L:16) =/g
         s/Set-Cookie: meta=.*/Set-Cookie: meta=/g
         s/ConfidentialityKey(F:c0)(L:16) =.*/ConfidentialityKey(F:c0)(L:16) =/g ' $req.log.tmp > $req.log.tmp2
if [ "$ignoreColTime" = 1 ]; then
    perl -pe 's/(\d{10,},(\d{1,}|"serving"),?)?/\2/g' $req.log.tmp2 > $req.log.tmp
else
    mv $req.log.tmp2 $req.log.tmp
fi

cd $ofclogDir 
sed '/^Date/d;/^TRANS/d;/h-h id/d;/e-e id/d;/^==/d;/SessionId(F/d' $req.log  > $req.log.tmp
sed -re 's/RAND(F:c0)(L:16) =.*/RAND(F:c0)(L:16) =/g
         s/Set-Cookie: meta=.*/Set-Cookie: meta=/g
         s/ConfidentialityKey(F:c0)(L:16) =.*/ConfidentialityKey(F:c0)(L:16) =/g ' $req.log.tmp > $req.log.tmp2
if [ "$ignoreColTime" = 1 ]; then
    perl -pe 's/(\d{10,},(\d{1,}|"serving"),?)?/\2/g' $req.log.tmp2 > $req.log.tmp
else
    mv $req.log.tmp2 $req.log.tmp
fi

Sum1=`cat $ofclogDir/$req.log.tmp |tr -d '\n' |md5sum| awk '{print $1}'`
Sum2=`cat $runlogDir/$req.log.tmp |tr -d '\n' |md5sum| awk '{print $1}'`
#Sum1=`md5sum $ofclogDir/$req.log.tmp | awk '{print $1}'`
#Sum2=`md5sum $runlogDir/$req.log.tmp | awk '{print $1}'`
#echo $Sum1
#echo $Sum2

datetime=`date +%Y%m%d%R`
#echo $datetime
echo -e "\n#####################################################################"
if [ "$Sum1" = "$Sum2" ]
then
    echo "case $caseNumber-> $req Http check succ" >> $caseDir/$caseNumber.rpt
    echo -e "\033[33mcase $caseNumber-> $req Http check succ\033[0m"

else
    echo "case $caseNumber-> $req Http check fail" >> $caseDir/$caseNumber.rpt
    echo -e "\033[31mcase $caseNumber-> $req Http check fail\033[0m"
fi
cd $runlogDir
if [ -z $scriptDebug ];then
    rm $req.log.tmp*
fi
cd $ofclogDir
if [ -z $scriptDebug ];then
    rm $req.log.tmp*
fi
cd $root

echo "end of handling $caseNumber->$req"
echo "#####################################################################" 
