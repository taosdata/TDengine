case_file="cases_temp_file"

parm_path=$(dirname $0)
parm_path=$(pwd ${parm_path})
echo "execute path:${parm_path}"
cd ${parm_path}
cp cases.task  ${case_file}
# comment udf and stream and sma case in windows
sed -i '/udf/d' ${case_file}
sed -i '/Udf/d' ${case_file}
sed -i '/stream/d' ${case_file}
sed -i '/^$/d' ${case_file} 
sed -i '$a\%%FINISHED%%' ${case_file} 
#unsupported case:sma 
sed -i '/tsim\/sync\/vnodesnapshot-rsma-test.sim/d' ${case_file}
sed -i '/tsim\/sma/d' ${case_file}
sed -i '/tsim\/tagindex\/sma_and_tag_index.sim/d' ${case_file}
#unsupported case:to_char
sed -i '/tsim\/query\/sort-pre-cols.sim/d' ${case_file}
sed -i '/2-query\/nestedQueryInterval.py/d' ${case_file}
#unsupported case:stream
sed -i '/database_pre_suf/d' ${case_file}
sed -i '/tsma/d' ${case_file}


utest="unit-test"
tsimtest="script"
systest="system-test"
devtest="develop-test"
doctest="docs-examples-test"
armytest="army"

rm -rf win-${utest}.log  win-${tsimtest}.log  win-${systest}.log  win-${devtest}.log win-${doctest}.log  win-${armytest}.log 

rm -rf ${parm_path}/../${utest}/win-test-file ${parm_path}/../${tsimtest}/win-test-file ${parm_path}/../${systest}/win-test-file  ${parm_path}/../${devtest}/win-test-file ${parm_path}/../${doctest}/win-test-file ${parm_path}/../${armytest}/win-test-file

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
    if [[ "${exec_dir}" == "${armytest}" ]]; then
        if [[ "${case_cmd}" =~ "pytest.sh" ]]; then
            case_cmd=$(echo "$case_cmd"|cut -d ' ' -f 2-)
            echo "${case_cmd}" >> win-${armytest}.log
        else
            echo "${case_cmd}" >> win-${armytest}.log
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
mv  win-${armytest}.log  ${parm_path}/../${armytest}/win-test-file
mv  win-${doctest}.log  ${parm_path}/../${armytest}/win-test-file

rm -rf ${case_file}
