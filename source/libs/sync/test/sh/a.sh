#!/bin/bash

if [ $# != 1 ] ; then
	echo "Uasge: $0 log-path"
	echo ""
	exit 1
fi

logpath=$1
echo "logpath: ${logpath}"

echo ""
echo "clean old log ..."
rm -f ${logpath}/log.*

echo ""
echo "generate log.dnode ..."
for dnode in `ls ${logpath} | grep dnode`;do
	echo "generate log.${dnode}"
	cat ${logpath}/${dnode}/log/taosdlog.* | grep SYN > ${logpath}/log.${dnode}
done

echo ""
echo "generate vgId ..."
cat ${logpath}/log.dnode* | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort -T. | uniq | awk -F: '{print $2, $0}' | sort -T. -k1 -n | awk '{print $2}' > ${logpath}/log.vgIds.tmp
echo "all vgIds:" > ${logpath}/log.vgIds
cat ${logpath}/log.dnode* | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort -T. | uniq | awk -F: '{print $2, $0}' | sort -T. -k1 -n | awk '{print $2}' >> ${logpath}/log.vgIds
for dnode in `ls ${logpath} | grep dnode | grep -v log`;do
	echo "" >> ${logpath}/log.vgIds
	echo "" >> ${logpath}/log.vgIds
	echo "${dnode}:" >> ${logpath}/log.vgIds
	cat ${logpath}/${dnode}/log/taosdlog.* | grep SYN | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort -T. | uniq | awk -F: '{print $2, $0}' | sort -T. -k1 -n | awk '{print $2}' >> ${logpath}/log.vgIds
done

echo ""
echo "generate log.dnode.vgId ..."
for logdnode in `ls ${logpath}/log.dnode*`;do
	for vgId in `cat ${logpath}/log.vgIds.tmp`;do
		rowNum=`cat ${logdnode} | grep "${vgId}," | awk 'BEGIN{rowNum=0}{rowNum++}END{print rowNum}'`
		#echo "-----${rowNum}"
		if [ $rowNum -gt 0 ] ; then
			echo "generate ${logdnode}.${vgId}"
			cat ${logdnode} | grep "${vgId}," > ${logdnode}.${vgId}
		fi
	done
done

echo ""
echo "generate log.dnode.main ..."
for file in `ls ${logpath}/log.dnode* | grep -v vgId`;do
	echo "generate ${file}.main"
	cat ${file} | awk '{ if(index($0, "sync open") > 0 || index($0, "sync close") > 0 || index($0, "become leader") > 0) {print $0} }' > ${file}.main
done

echo ""
echo "generate log.leader.term ..."
cat ${logpath}/*.main | grep "become leader" | grep -v "config change" | awk '{print $5,$0}' | awk -F, '{print $4"_"$0}' | sort -T. -k1 > ${logpath}/log.leader.term

echo ""
echo "generate log.index, log.snapshot, log.records, log.actions ..."
for file in `ls ${logpath}/log.dnode*vgId*`;do
	destfile1=${file}.index
	echo "generate ${destfile1}"
	cat ${file} | awk '{ if(index($0, "write index:") > 0 || index($0, "wal truncate, from-index") > 0) {print $0} }' > ${destfile1}

	destfile2=${file}.snapshot
	echo "generate ${destfile2}"
	cat ${file} | awk '{ if(index($0, "snapshot sender") > 0 || index($0, "snapshot receiver") > 0) {print $0} }' | grep -v "save old" | grep -v "create new" | grep -v "udpate replicaIndex" | grep -v "delete old" | grep -v "reset for" > ${destfile2}

	destfile3=${file}.records
	echo "generate ${destfile3}"
	cat ${file} | awk '{ if(index($0, "write index:") > 0 || index($0, "wal truncate, from-index") > 0 || index($0, "snapshot sender") > 0 || index($0, "snapshot receiver") > 0) {print $0} }' | grep -v "save old" | grep -v "create new" | grep -v "udpate replicaIndex" | grep -v "delete old" | grep -v "reset for" > ${destfile3}

	destfile4=${file}.commit
	echo "generate ${destfile4}"
	cat ${file} | awk '{ if(index($0, "commit by") > 0) {print $0} }' > ${destfile4}

	destfile5=${file}.actions
	echo "generate ${destfile5}"
	cat ${file} | awk '{ if(index($0, "commit by") > 0 || index($0, "sync open") > 0 || index($0, "sync close") > 0 || index($0, "become leader") > 0 || index($0, "write index:") > 0 || index($0, "wal truncate, from-index") > 0 || index($0, "snapshot sender") > 0 || index($0, "snapshot receiver") > 0) {print $0} }' | grep -v "save old" | grep -v "create new" | grep -v "udpate replicaIndex" | grep -v "delete old" | grep -v "reset for" > ${destfile5}

done

echo ""
echo "generate log.commit ..."
tmpfile=${logpath}/log.commits.tmp
touch ${tmpfile}
for file in `ls ${logpath}/log.dnode*.vgId*.commit`;do
	line=`cat ${file} | tail -n1`
	echo $line | awk '{print $5, $0}' >> ${tmpfile}
done
cat ${tmpfile} | sort -k1 | awk 'BEGIN{vgid=$1}{if($1==vgid){print $0}else{print ""; print $0; vgid=$1;}}END{}' > ${logpath}/log.commits

exit 0


