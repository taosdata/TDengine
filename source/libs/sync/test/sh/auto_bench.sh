#!/bin/bash

if [ $# != 7 ] ; then
	echo "Uasge: $0 instances vgroups replica ctables rows weak drop(yes/no)"
  echo ""
  exit 1
fi

instances=$1
vgroups=$2
replica=$3
ctables=$4
rows=$5
weak=$6
drop=$7


echo "params: instances:${instances}, vgroups:${vgroups}, replica:${replica}, ctables:${ctables}, rows:${rows}, weak:${weak}, drop:${drop}"

dt=`date "+%Y-%m-%d-%H-%M-%S"`
casedir=instances_${instances}_vgroups_${vgroups}_replica_${replica}_ctables_${ctables}_rows_${rows}_weak_${weak}_drop_${drop}_${dt}
mkdir ${casedir}
cp ./insert.tpl.json ${casedir}
cd ${casedir}

for i in `seq 1 ${instances}`;do
	#echo ===$i===
	cfg_file=bench_${i}.json
	cp ./insert.tpl.json ${cfg_file}
	rstfile=result_${i}
	sed -i 's/tpl_drop_tpl/'${drop}'/g' ${cfg_file}
	sed -i 's/tpl_vgroups_tpl/'${vgroups}'/g' ${cfg_file}
	sed -i 's/tpl_replica_tpl/'${replica}'/g' ${cfg_file}
	sed -i 's/tpl_ctables_tpl/'${ctables}'/g' ${cfg_file}
	sed -i 's/tpl_stid_tpl/'${i}'/g' ${cfg_file}
	sed -i 's/tpl_rows_tpl/'${rows}'/g' ${cfg_file}
	sed -i 's/tpl_insert_result_tpl/'${rstfile}'/g' ${cfg_file}
done

for conf_file in `ls ./bench_*.json`;do
	echo "nohup taosBenchmark -f ${conf_file} &"
	nohup taosBenchmark -f ${conf_file} &
done

cd -

exit 0


