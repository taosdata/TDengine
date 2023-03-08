case_file="cases_temp_file"

parm_path=$(dirname $0)
parm_path=$(pwd ${parm_path})
echo "execute path:${parm_path}"
cd ${parm_path}
cp cases.task  ${case_file}
sed -i '/^$/d' ${case_file} 
sed -i '$a\%%FINISHED%%' ${case_file} 

utest="unit-test"
tsimtest="script"
systest="system-test"
devtest="develop-test"
doctest="docs-examples-test"
rm -rf win-${utest}.log  win-${tsimtest}.log  win-${systest}.log  win-${devtest}.log win-${doctest}.log 

while read -r line
do
    echo "$line"|grep -q "^#"
    if [ $? -eq 0 ]; then
        continue
    fi
    exec_dir=$(echo "$line"|cut -d ',' -f4)
    case_cmd=$(echo "$line"|cut -d ',' -f5)
    if [[ "${exec_dir}" == "${utest}" ]]; then
        echo ${case_cmd} >> win-${utest}.log
        continue
    fi
    if [[ "${exec_dir}" == "${tsimtest}" ]]; then
        echo ${case_cmd} >> win-${tsimtest}.log
        continue
    fi
    if [[ "${exec_dir}" == "${systest}" ]]; then
        if [[ "${case_cmd}" =~ "pytest.sh" ]]; then
            case_cmd=$(echo "$case_cmd"|cut -d ' ' -f 2-)
            echo ${case_cmd} >> win-${systest}.log
        else
            echo ${case_cmd} >> win-${systest}.log
        fi
        continue
    fi
    if [[ "${exec_dir}" == "${devtest}" ]]; then
        echo ${case_cmd} >> win-${devtest}.log
        continue
    fi
    if [[ "${exec_dir}" == "${doctest}" ]]; then
        echo ${case_cmd} >> win-${doctest}.log
        continue
    fi
done < ${case_file}
mv  win-${utest}.log  ${parm_path}/../${utest}/win-test-file
mv  win-${tsimtest}.log  ${parm_path}/../${tsimtest}/win-test-file
mv  win-${systest}.log  ${parm_path}/../${systest}/win-test-file
mv  win-${devtest}.log  ${parm_path}/../${devtest}/win-test-file


rm -rf ${case_file}

    # while [ 1 ]; do
    #     local line=`flock -x $lock_file -c "head -n1 $task_file;sed -i \"1d\" $task_file"`
    #     if [ "x$line" = "x%%FINISHED%%" ]; then
    #         # echo "$index . $thread_no EXIT"
    #         break
    #     fi
    #     if [ -z "$line" ]; then
    #         continue
    #     fi
    #     echo "$line"|grep -q "^#"
    #     if [ $? -eq 0 ]; then
    #         continue
    #     fi
    #     local case_redo_time=`echo "$line"|cut -d, -f2`
    #     if [ -z "$case_redo_time" ]; then
    #         case_redo_time=${DEFAULT_RETRY_TIME:-2}
    #     fi
    #     local case_build_san=`echo "$line"|cut -d, -f3`
    #     if [ "${case_build_san}" == "y" ]; then
    #         case_build_san="y"
    #     elif [[ "${case_build_san}" == "n" ]] || [[ "${case_build_san}" == "" ]]; then
    #         case_build_san="n"
    #     else
    #         usage
    #         exit 1
    #     fi
    #     local exec_dir=`echo "$line"|cut -d, -f4`
    #     local case_cmd=`echo "$line"|cut -d, -f5`
    #     local case_file=""
    #     echo "$case_cmd"|grep -q "\.sh"
    #     if [ $? -eq 0 ]; then
    #         case_file=`echo "$case_cmd"|grep -o ".*\.sh"|awk '{print $NF}'`
    #     fi
    #     echo "$case_cmd"|grep -q "^python3"
    #     if [ $? -eq 0 ]; then
    #         case_file=`echo "$case_cmd"|grep -o ".*\.py"|awk '{print $NF}'`
    #     fi
    #     echo "$case_cmd"|grep -q "^./pytest.sh"
    #     if [ $? -eq 0 ]; then
    #         case_file=`echo "$case_cmd"|grep -o ".*\.py"|awk '{print $NF}'`
    #     fi
    #     echo "$case_cmd"|grep -q "\.sim"
    #     if [ $? -eq 0 ]; then
    #         case_file=`echo "$case_cmd"|grep -o ".*\.sim"|awk '{print $NF}'`
    #     fi
    #     if [ -z "$case_file" ]; then
    #         case_file=`echo "$case_cmd"|awk '{print $NF}'`
    #     fi
    #     if [ -z "$case_file" ]; then
    #         continue
    #     fi
    #     case_file="$exec_dir/${case_file}.${index}.${thread_no}.${count}"
    #     count=$(( count + 1 ))
    #     local case_path=`dirname "$case_file"`
    #     if [ ! -z "$case_path" ]; then
    #         mkdir -p $log_dir/$case_path
    #     fi
    #     cmd="${runcase_script} ${script} -w ${workdirs[index]} -c \"${case_cmd}\" -t ${thread_no} -d ${exec_dir}  -s ${case_build_san} ${timeout_param}"
    #     # echo "$thread_no $count $cmd"
    #     local ret=0
    #     local redo_count=1
    #     local case_log_file=$log_dir/${case_file}.txt
    #     start_time=`date +%s`
    #     local case_index=`flock -x $lock_file -c "sh -c \"echo \\\$(( \\\$( cat $index_file ) + 1 )) | tee $index_file\""`
    #     case_index=`printf "%5d" $case_index`
    #     local case_info=`echo "$line"|cut -d, -f 3,4,5`
    #     while [ ${redo_count} -lt 6 ]; do
    #         if [ -f $case_log_file ]; then
    #             cp $case_log_file $log_dir/$case_file.${redo_count}.redotxt
    #         fi
    #         echo "${hosts[index]}-${thread_no} order:${count}, redo:${redo_count} task:${line}" >$case_log_file
    #         local current_time=`date "+%Y-%m-%d %H:%M:%S"`
    #         echo -e "$case_index \e[33m START >>>>> \e[0m ${case_info} \e[33m[$current_time]\e[0m"
    #         echo "$current_time" >>$case_log_file
    #         local real_start_time=`date +%s`
    #         # $cmd 2>&1 | tee -a $case_log_file
    #         # ret=${PIPESTATUS[0]}
    #         $cmd >>$case_log_file 2>&1
    #         ret=$?
    #         local real_end_time=`date +%s`
    #         local time_elapsed=$(( real_end_time - real_start_time ))
    #         echo "execute time: ${time_elapsed}s" >>$case_log_file
    #         current_time=`date "+%Y-%m-%d %H:%M:%S"`
    #         echo "${hosts[index]} $current_time exit code:${ret}" >>$case_log_file
    #         if [ $ret -eq 0 ]; then
    #             break
    #         fi
    #         redo=0
    #         grep -q "wait too long for taosd start" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         grep -q "kex_exchange_identification: Connection closed by remote host" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         grep -q "ssh_exchange_identification: Connection closed by remote host" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         grep -q "kex_exchange_identification: read: Connection reset by peer" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         grep -q "Database not ready" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         grep -q "Unable to establish connection" $case_log_file
    #         if [ $? -eq 0 ]; then
    #             redo=1
    #         fi
    #         if [ $redo_count -lt $case_redo_time ]; then
    #             redo=1
    #         fi
    #         if [ $redo -eq 0 ]; then
    #             break
    #         fi
    #         redo_count=$(( redo_count + 1 ))
    #     done