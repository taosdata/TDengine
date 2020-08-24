taos1_6="/root/mnt/work/test/td1.6/build/bin/taos"
taosdump1_6="/root/mnt/work/test/td1.6/build/bin/taosdump"
taoscfg1_6="/root/mnt/work/test/td1.6/test/cfg"

taos2_0="/root/mnt/work/test/td2.0/build/bin/taos"
taosdump2_0="/root/mnt/work/test/td2.0/build/bin/taosdump"
taoscfg2_0="/root/mnt/work/test/td2.0/test/cfg"

data_dir="/root/mnt/work/test/td1.6/output"
table_list="/root/mnt/work/test/td1.6/tables"

DBNAME="test"
NTABLES=$(wc -l ${table_list} | awk '{print $1;}')
NTABLES_PER_DUMP=101

mkdir -p ${data_dir}
i=0
round=0
command="${taosdump1_6} -c ${taoscfg1_6} -o ${data_dir} -N 100 -T 20 ${DBNAME}"
while IFS= read -r line
do
  i=$((i+1))

  command="${command} ${line}"

  if [[ "$i" -eq ${NTABLES_PER_DUMP} ]]; then
    round=$((round+1))
    echo "Starting round ${round} dump out..."
    rm -f ${data_dir}/*
    ${command}
    echo "Starting round ${round} dump in..."
    ${taosdump2_0} -c ${taoscfg2_0} -i ${data_dir}

    # Reset variables
    # command="${taosdump1_6} -c ${taoscfg1_6} -o ${data_dir} -N 100 ${DBNAME}"
    command="${taosdump1_6} -c ${taoscfg1_6} -o ${data_dir} -N 100 -T 20 ${DBNAME}"
    i=0
  fi
done < "${table_list}"

if [[ ${i} -ne "0" ]]; then
  round=$((round+1))
  echo "Starting round ${round} dump out..."
  rm -f ${data_dir}/*
  ${command}
  echo "Starting round ${round} dump in..."
  ${taosdump2_0} -c ${taoscfg2_0} -i ${data_dir}
fi
