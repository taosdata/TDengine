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
cat ${logpath}/log.dnode* | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort | uniq > ${logpath}/log.vgIds.tmp
echo "all vgIds:" > ${logpath}/log.vgIds
cat ${logpath}/log.dnode* | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort | uniq >> ${logpath}/log.vgIds
for dnode in `ls ${logpath} | grep dnode | grep -v log`;do
	echo "" >> ${logpath}/log.vgIds
	echo "" >> ${logpath}/log.vgIds
	echo "${dnode}:" >> ${logpath}/log.vgIds
	cat ${logpath}/${dnode}/log/taosdlog.* | grep SYN | grep "vgId:" | grep -v ERROR | awk '{print $5}' | awk -F, '{print $1}' | sort | uniq >> ${logpath}/log.vgIds 
done

echo ""
echo "generate log.dnode.vgId ..."
for logdnode in `ls ${logpath}/log.dnode*`;do
	for vgId in `cat ${logpath}/log.vgIds.tmp`;do
		rowNum=`cat ${logdnode} | grep "${vgId}" | awk 'BEGIN{rowNum=0}{rowNum++}END{print rowNum}'`
		#echo "-----${rowNum}"
		if [ $rowNum -gt 0 ] ; then
			echo "generate ${logdnode}.${vgId}"
			cat ${logdnode} | grep "${vgId}" > ${logdnode}.${vgId}
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
cat ${logpath}/*.main | grep "become leader" | grep -v "config change" | awk '{print $5,$0}' | awk -F, '{print $4"_"$0}' | sort -k1 > ${logpath}/log.leader.term

echo ""
echo "generate log.index ..."
for file in `ls ${logpath}/log.dnode*vgId*`;do
	destfile=${file}.index
	echo "generate ${destfile}"
	cat ${file} | awk '{ if(index($0, "write index:") > 0 || index($0, "wal truncate, from-index") > 0) {print $0} }' > ${destfile}
done

exit 0
