#!/bin/bash
#

set -e
#set -x

################## before run, please modify date for collecting ################################
# set parameters by default value
start_date=2021-12-05
end_date=2021-12-11
#################################################################################################

#while getopts "hs:e:" arg
#do
#  case $arg in
#    s)
#      start_date=$( echo $OPTARG )
#      ;;
#    e)
#      end_date=$(echo $OPTARG)
#      ;;
#    #r)
#    #  Repository=$(echo $OPTARG)
#    #  ;;
#    h)
#      echo "Usage: `basename $0` -s [start date] "
#      echo "                  -e [end date] "
#      #echo "                  -r [repository name] "
#      exit 0
#      ;;
#    ?) #unknow option
#      echo "unknow argument"
#      exit 1
#      ;;
#  esac
#done

script_dir=$(pwd)
echo "script dir: $script_dir"

############# global var #################
filter_usr_name=""


developUsrNameList=('chenhaoran' 'chenghongze' 'dengyihao' 'duankuanjun' 'guanshengliang' 'guoxiangyang' 'jiachenyang' 'jiajingbin' 'jinminglei' 'liaohaojun' \
 'lihui' 'liujicong' 'liuyiqing' 'panwei' 'wangmingming' 'wangxingyuan' 'wenzhou' 'wuchaoping' 'xiaoping' 'xukaili' 'zhaoganlin' 'zhoushenglian' 'wangxiaoyu' 'liminghao')

developGrpFilterUsrNameList=('tomchon|haoranchen' 'Hongze Cheng|hzcheng' 'dengyihao|yihaoDeng' 'alexduan|Alex Duan|AlexDuan|DuanKuanJun' 'Shengliang Guan|slguan' 'happyguoxy' 'jiacy-jcy|jiacy.jcy' 'jiajingbin' 'stephenkgu|Minglei Jin' \
 'Haojun Liao|haojun Liao|hjliao|hjLiao|hjxilinx' 'huili|Hui Li|plum-lihui|lihui' 'Liu Jicong' 'liuyq-617|Yiqing Liu' 'dapan|dapan1121|wpan' 'wangmm0220|WANG MINGMING' \
 'XingY Wang|xywang|XYWang|winshining' 'wenzhouwww' 'cpwu|wu champion|wu-champion' 'Xiao Ping|Ping Xiao' 'Cary Xu|kailixu|Kaili Xu' 'glzhao89|Ganlin Zhao' 'shenglian zhou|shenglian-zhou|Shenglian Zhou|slzhou' \
 'xiao-yu-wang|xiaoyu wang|Xiaoyu Wang' 'minghao li')
 

appUsrNameList=('huolibo' 'huolinhe' 'lixiaolei' 'sangshuduo' 'tanxuefeng' 'wangzhiqiang' 'yangzhiyu' 'zhaoyang' 'dingbo')
appFilterUsrNameList=('huolibo' 'Linhe Huo|Huo Linhe' 'xialei_li|xiaolei li' 'Shuduo Sang' 't_max|huskar_t|T_MAX|tanxuefeng' 'Zhiqiang Wang' \
 'yangzhiyu|yangzy|Zhiyu Yang|zyyang|zyyang-taosdata' 'Yang Zhao|zhaoyanggh' 'Bo Ding')


cloudUsrNameList=('changshuaiqiang' 'lianjingtao' 'wangguan' 'yuanyuan' 'zhangmingshuo')
cloudFilterUsrNameList=('Shuaiqiang|Chang|changshuaiqiang|ShuaiQChang' 'lianjt' 'skye0207|skye|Guan Wang' 'Eavanyuan|yuanyuan' 'mingshuozhang|mszhang')

##########  all branch ################
TDengine_br_1="master"
TDengine_br_2="develop"
TDengine_br_3="2.0"
TDengine_br_4="3.0"

#application group
driverGo_br_1="master"
driverGo_br_2="develop"
grafanaplugin_br_1="master"
Bailongma_br_1="master"
goUtils_br_1="main"
goUtils_br_2="develop"
tdengine_gorm_br_1="master"
alert_br_1="master"
libtaosRs_br_1="main"
taosConnectorPython_br_1="main"
TDengineOperator_br_1="main"
bailongmaRs_br_1="main"
goDemoKafka_br_1="main"
DataX_br_1="master"
taosadapter_br_1="master"
taosadapter_br_2="develop"
kafka_connect_tdengine="master"
taos_tools_br="develop"
taoskeeper_br_1="develop"


#cloud group
TDC_br_1="main"
TDC_br_2="develop"
#########################################

result_file=$script_dir/info_${start_date}_${end_date}.txt

echo "==== input parameters ===="
echo "start_date: $start_date"
echo "end_date: $end_date"
echo "result_file: $result_file"
echo "============================"

rm -f $result_file
#echo "usr_name total_addLines total_rmvLines total_prNum" > $result_file

function gitPullBranchInfo () {
  branch_name=$1

  git checkout $branch_name
  git pull ||:
}

######
# Specify the author and time, and execute the following command under the specified branch
# git log --author="slguan" --since="2021-12-01" --before="2021-12-14" --pretty=tformat: --numstat | awk '{add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }'
#######
function getBranchInfo () {
  output_usr_name=$1
  branch_name=$2
  
  git checkout $branch_name
  #git pull ||:
    
  addLines=$(git log --format='%aN' --since=$start_date --before=$end_date | sort -u | while read name; do echo -en "$name\t"; git log --author="$name" --since=$start_date --before=$end_date --pretty=tformat: --numstat | awk '{add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }' -; done | grep -E "${filter_usr_name}" | awk -F "added lines:" '{print $2}' | awk -F "," '{print $1}' | awk '{s+=$1} END {print s}')
  if [[ -n $addLines ]] && [[ $addLines -ne 0 ]]; then 
    #total_addLines=`expr $total_addLines + $addLines` 
    let total_addLines=total_addLines+addLines
  fi

  rmvLines=$(git log --format='%aN' --since=$start_date --before=$end_date | sort -u | while read name; do echo -en "$name\t"; git log --author="$name" --since=$start_date --before=$end_date --pretty=tformat: --numstat | awk '{add += $1; subs += $2; loc += $1 - $2 } END { printf "added lines: %s, removed lines: %s, total lines: %s\n", add, subs, loc }' -; done | grep -E "${filter_usr_name}" | awk -F "removed lines:" '{print $2}' | awk -F "," '{print $1}' | awk '{s+=$1} END {print s}')
  if [[ -n $rmvLines ]] && [[ $rmvLines -ne 0 ]]; then
    let total_rmvLines=total_rmvLines+rmvLines
  fi
  
  prNum=$(git log --pretty='%aN' --since=$start_date --before=$end_date | sort | uniq -c | sort -k1 -n -r | grep -E "${filter_usr_name}" | awk '{s+=$1} END {print s}')
  #prNum=$(git log --since=$start_date --before=$end_date | grep -A2 "Merge:" | grep -E "${filter_usr_name}" | wc -l)
  if [[ -n $prNum ]] && [[ $prNum -ne 0 ]]; then
    let total_prNum=total_prNum+prNum
  fi 
  
  #echo "$branch_name: $output_usr_name $addLines $rmvLines $prNum"
}

function getAddRmvLinesPrNumForDbEngineGroup () {
  output_usr_name=$1
  
  total_addLines=0
  total_rmvLines=0
  total_prNum=0

  ######## 0. TDinternal repository
  Repository_dir=$script_dir/TDinternal
  cd $Repository_dir
  getBranchInfo $output_usr_name $TDengine_br_1  
  getBranchInfo $output_usr_name $TDengine_br_2
  getBranchInfo $output_usr_name $TDengine_br_3
  getBranchInfo $output_usr_name $TDengine_br_4    
    
  ######## 1. TDengine repository
  Repository_dir=$script_dir/TDinternal/community
  cd $Repository_dir
  getBranchInfo $output_usr_name $TDengine_br_1  
  getBranchInfo $output_usr_name $TDengine_br_2
  getBranchInfo $output_usr_name $TDengine_br_3
  getBranchInfo $output_usr_name $TDengine_br_4
  
  ################ totaol result ###########################
  if [[ $total_addLines -eq 0 ]] && [[ $total_rmvLines -eq 0 ]] && [[ $total_prNum -eq 0 ]]; then
    return
  fi
  
  echo "==== $output_usr_name total_addLines: $total_addLines, total_rmvLines: $total_rmvLines, total_prNum: $total_prNum, file: $result_file"
  echo "$output_usr_name , $total_addLines , $total_rmvLines , $total_prNum" >> $result_file
}

function getAddRmvLinesPrNumForAppGroup () {
  output_usr_name=$1
  
  total_addLines=0
  total_rmvLines=0
  total_prNum=0

  ######## 0. TDinternal repository
  Repository_dir=$script_dir/TDinternal
  cd $Repository_dir 
  getBranchInfo $output_usr_name $TDengine_br_1  
  getBranchInfo $output_usr_name $TDengine_br_2
  getBranchInfo $output_usr_name $TDengine_br_3  
  getBranchInfo $output_usr_name $TDengine_br_4  
    
  ######## 1. TDengine repository
  Repository_dir=$script_dir/TDinternal/community
  cd $Repository_dir 
  getBranchInfo $output_usr_name $TDengine_br_1  
  getBranchInfo $output_usr_name $TDengine_br_2
  getBranchInfo $output_usr_name $TDengine_br_3  
  getBranchInfo $output_usr_name $TDengine_br_4
  
  ######## 2. driver-go repository
  Repository_dir=$script_dir/driver-go
  cd $Repository_dir 
  getBranchInfo $output_usr_name $driverGo_br_1
  getBranchInfo $output_usr_name $driverGo_br_2

  ######## 2. taosadapter repository
  Repository_dir=$script_dir/taosadapter
  cd $Repository_dir 
  getBranchInfo $output_usr_name $taosadapter_br_1
  getBranchInfo $output_usr_name $taosadapter_br_2
    
  ######## 3. grafanaplugin repository
  Repository_dir=$script_dir/grafanaplugin
  cd $Repository_dir 
  getBranchInfo $output_usr_name $grafanaplugin_br_1

  Repository_dir=$script_dir/Bailongma
  cd $Repository_dir 
  getBranchInfo $output_usr_name $Bailongma_br_1
  
  Repository_dir=$script_dir/go-utils
  cd $Repository_dir 
  getBranchInfo $output_usr_name $goUtils_br_1
  getBranchInfo $output_usr_name $goUtils_br_2
  
  Repository_dir=$script_dir/tdengine_gorm 
  cd $Repository_dir 
  getBranchInfo $output_usr_name $tdengine_gorm_br_1
  
  Repository_dir=$script_dir/alert
  cd $Repository_dir 
  getBranchInfo $output_usr_name $alert_br_1
  
  Repository_dir=$script_dir/libtaos-rs
  cd $Repository_dir 
  getBranchInfo $output_usr_name $libtaosRs_br_1
  
  Repository_dir=$script_dir/taos-connector-python 
  cd $Repository_dir 
  getBranchInfo $output_usr_name $taosConnectorPython_br_1
  
  Repository_dir=$script_dir/TDengine-Operator 
  cd $Repository_dir 
  getBranchInfo $output_usr_name $TDengineOperator_br_1
  
  Repository_dir=$script_dir/bailongma-rs
  cd $Repository_dir 
  getBranchInfo $output_usr_name $bailongmaRs_br_1
  
  Repository_dir=$script_dir/go-demo-kafka
  cd $Repository_dir 
  getBranchInfo $output_usr_name $goDemoKafka_br_1
  
  Repository_dir=$script_dir/kafka-connect-tdengine
  cd $Repository_dir 
  getBranchInfo $output_usr_name $kafka_connect_tdengine
  
  Repository_dir=$script_dir/DataX
  cd $Repository_dir 
  getBranchInfo $output_usr_name $DataX_br_1
  
  Repository_dir=$script_dir/taos-tools
  cd $Repository_dir 
  getBranchInfo $output_usr_name $taos_tools_br

  ######## taoskeeper repository
  Repository_dir=$script_dir/taoskeeper
  cd $Repository_dir
  getBranchInfo $output_usr_name $taoskeeper_br_1

  ################ totaol result ###########################
  if [[ $total_addLines -eq 0 ]] && [[ $total_rmvLines -eq 0 ]] && [[ $total_prNum -eq 0 ]]; then
    return
  fi
  
  echo "==== $output_usr_name total_addLines: $total_addLines, total_rmvLines: $total_rmvLines, total_prNum: $total_prNum, file: $result_file"
  echo "$output_usr_name , $total_addLines , $total_rmvLines , $total_prNum" >> $result_file
}

function getAddRmvLinesPrNumForCloudGroup () {
  output_usr_name=$1
  
  total_addLines=0
  total_rmvLines=0
  total_prNum=0
    
  ######## TDC repository
  Repository_dir=$script_dir/TDC
  cd $Repository_dir
  getBranchInfo $output_usr_name $TDC_br_1
  getBranchInfo $output_usr_name $TDC_br_2
  
  ################ totaol result ###########################
  if [[ $total_addLines -eq 0 ]] && [[ $total_rmvLines -eq 0 ]] && [[ $total_prNum -eq 0 ]]; then
    return
  fi
  
  echo "==== $output_usr_name total_addLines: $total_addLines, total_rmvLines: $total_rmvLines, total_prNum: $total_prNum, file: $result_file"
  echo "$output_usr_name , $total_addLines , $total_rmvLines , $total_prNum" >> $result_file
}

############################################# firstly checkout and git pull #######################################
######## 0. TDinternal repository
Repository_dir=$script_dir/TDinternal
cd $Repository_dir   
gitPullBranchInfo $TDengine_br_1
gitPullBranchInfo $TDengine_br_2
gitPullBranchInfo $TDengine_br_3
gitPullBranchInfo $TDengine_br_4
echo  "====pull TDinternal end"
  
######## 1. TDengine repository
Repository_dir=$script_dir/TDinternal/community
cd $Repository_dir   
gitPullBranchInfo $TDengine_br_1
gitPullBranchInfo $TDengine_br_2
gitPullBranchInfo $TDengine_br_3
gitPullBranchInfo $TDengine_br_4
echo  "====pull TDengine end"

######## 2. driver-go repository
Repository_dir=$script_dir/driver-go
cd $Repository_dir 
gitPullBranchInfo $driverGo_br_1
gitPullBranchInfo $driverGo_br_2
echo  "====pull driver-go end"

Repository_dir=$script_dir/taosadapter
cd $Repository_dir 
gitPullBranchInfo $taosadapter_br_1
gitPullBranchInfo $taosadapter_br_2
echo  "====pull taosadapter end"

Repository_dir=$script_dir/Bailongma
cd $Repository_dir 
gitPullBranchInfo $Bailongma_br_1
echo  "====pull Bailongma end"

Repository_dir=$script_dir/go-utils
cd $Repository_dir 
gitPullBranchInfo $goUtils_br_1
gitPullBranchInfo $goUtils_br_2
echo  "====pull go-utils end"

Repository_dir=$script_dir/tdengine_gorm 
cd $Repository_dir 
gitPullBranchInfo $tdengine_gorm_br_1
echo  "====pull tdengine_gorm  end"

Repository_dir=$script_dir/alert
cd $Repository_dir 
gitPullBranchInfo $alert_br_1
echo  "====pull alert end"

Repository_dir=$script_dir/libtaos-rs
cd $Repository_dir 
gitPullBranchInfo $libtaosRs_br_1
echo  "====pull libtaos-rs end"

Repository_dir=$script_dir/taos-connector-python 
cd $Repository_dir 
gitPullBranchInfo $taosConnectorPython_br_1
echo  "====pull taos-connector-python end"

Repository_dir=$script_dir/TDengine-Operator 
cd $Repository_dir 
gitPullBranchInfo $TDengineOperator_br_1
echo  "====pull TDengine-Operator end"

Repository_dir=$script_dir/bailongma-rs
cd $Repository_dir 
gitPullBranchInfo $bailongmaRs_br_1
echo  "====pull bailongma-rs end"

Repository_dir=$script_dir/go-demo-kafka
cd $Repository_dir 
gitPullBranchInfo $goDemoKafka_br_1
echo  "====pull go-demo-kafka end"

Repository_dir=$script_dir/kafka-connect-tdengine
cd $Repository_dir 
gitPullBranchInfo $kafka_connect_tdengine
echo  "====pull kafka-connect-tdengin end"

Repository_dir=$script_dir/DataX
cd $Repository_dir 
gitPullBranchInfo $DataX_br_1
echo  "====pull DataX end"

Repository_dir=$script_dir/taos-tools
cd $Repository_dir 
gitPullBranchInfo $taos_tools_br
echo  "====pull taos-tools end"

######## 3. grafanaplugin repository
Repository_dir=$script_dir/grafanaplugin
cd $Repository_dir   
gitPullBranchInfo $grafanaplugin_br_1
echo  "====pull grafanaplugin end"

######## 4. taoskeeper repository
Repository_dir=$script_dir/taoskeeper
cd $Repository_dir 
gitPullBranchInfo $taoskeeper_br_1
echo  "====pull taoskeeper end"

######## 5. TDC repository
Repository_dir=$script_dir/TDC
cd $Repository_dir 
gitPullBranchInfo $TDC_br_1
gitPullBranchInfo $TDC_br_2
echo  "====pull TDC end"

echo "all repo git pull complete !"


#########################################################################################################################

######################################### get repo info for each member in alphabetical order #################################################

###################################################################################################################################
####============  DbEngine group members ============ ####
#### chenhaoran
taos_name=chenhaoran
filter_usr_name="tomchon|haoranchen"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### chenghongze
taos_name=chenghongze
filter_usr_name="Hongze Cheng|hzcheng"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### dengyihao
taos_name=dengyihao
filter_usr_name="dengyihao|yihaoDeng"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### duankuanjun
taos_name=duankuanjun
filter_usr_name="alexduan|Alex Duan|AlexDuan|DuanKuanJun"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### guanshengliang
taos_name=guanshengliang
filter_usr_name="Shengliang Guan|slguan"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### guoxiangyang
taos_name=guoxiangyang
filter_usr_name="happyguoxy"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### jiachenyang
taos_name=jiachenyang
filter_usr_name="jiacy-jcy|jiacy.jcy"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### jiajingbin
taos_name=jiajingbin
filter_usr_name="jiajingbin"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### jinminglei
taos_name=jinminglei
filter_usr_name="stephenkgu|Minglei Jin"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### liaohaojun
taos_name=liaohaojun
filter_usr_name="Haojun Liao|haojun Liao|hjliao|hjLiao|hjxilinx"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### lichuang
#taos_name=lichuang
#filter_usr_name="Lichuang|lichuang|codedump"
#getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### lihui
taos_name=lihui
filter_usr_name="huili|Hui Li|plum-lihui|lihui"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### liujicong
taos_name=liujicong
filter_usr_name="Liu Jicong"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### liuyiqing
taos_name=liuyiqing
filter_usr_name="liuyq-617|Yiqing Liu"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### panwei
taos_name=panwei
filter_usr_name="dapan|dapan1121|wpan"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### steven
taos_name=steven
filter_usr_name="Steven Li"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### taojianhui
taos_name=JeffTao
filter_usr_name="Jeff Tao|jtao1735"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### wangmingming
taos_name=wangmingming
filter_usr_name="wangmm0220|WANG MINGMING"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### wangxiaoyu
taos_name=wangxiaoyu
filter_usr_name="xiao-yu-wang|xiaoyu wang|Xiaoyu Wang"
getAddRmvLinesPrNumForDbEngineGroup $taos_name 

#### wangxingyuan
taos_name=wangxingyuan
filter_usr_name="XingY Wang|xywang|XYWang|winshining"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### wenzhou
taos_name=wenzhou
filter_usr_name="wenzhouwww"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### wuchaoping
taos_name=wuchaoping
filter_usr_name="cpwu|wu champion|wu-champion"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### xiaoping
taos_name=xiaoping
filter_usr_name="Xiao Ping|Ping Xiao"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### xukaili
taos_name=xukaili
filter_usr_name="Cary Xu|kailixu|Kaili Xu"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### zhaoganlin
taos_name=zhaoganlin
filter_usr_name="glzhao89|Ganlin Zhao"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  

#### zhoushenglian
taos_name=zhoushenglian
filter_usr_name="shenglian zhou|shenglian-zhou|Shenglian Zhou|slzhou"
getAddRmvLinesPrNumForDbEngineGroup $taos_name  


###################################################################################################################################
####============  app group members ============####
#### lihui
#taos_name=lihui
#filter_usr_name="huili|Hui Li|plum-lihui|lihui"
#getAddRmvLinesPrNumForAppGroup $taos_name  

#### dingbo
taos_name=dingbo
filter_usr_name="Bo Ding"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### huolibo
taos_name=huolibo
filter_usr_name="huolibo"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### huolinhe
taos_name=huolinhe
filter_usr_name="Linhe Huo|Huo Linhe"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### lixiaolei
taos_name=lixiaolei
filter_usr_name="xialei_li|xiaolei li"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### sangshuduo
taos_name=sangshuduo
filter_usr_name="Shuduo Sang"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### tanxuefeng
taos_name=tanxuefeng
filter_usr_name="t_max|huskar_t|T_MAX|tanxuefeng"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### wangzhiqiang
taos_name=wangzhiqiang
filter_usr_name="Zhiqiang Wang"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### yangzhiyu
taos_name=yangzhiyu
filter_usr_name="yangzhiyu|yangzy|Zhiyu Yang|zyyang|zyyang-taosdata"
getAddRmvLinesPrNumForAppGroup $taos_name  

#### zhaoyang
taos_name=zhaoyang
filter_usr_name="Yang Zhao|zhaoyanggh"
getAddRmvLinesPrNumForAppGroup $taos_name  

###################################################################################################################################
####============  cloud group members ============####
#### changshuaiqiang
taos_name=changshuaiqiang
filter_usr_name="Shuaiqiang Chang|changshuaiqiang|ShuaiQChang"
getAddRmvLinesPrNumForCloudGroup $taos_name 

#### lianjingtao
taos_name=lianjingtao
filter_usr_name="Jingtao Lian|jtlian|lianjt"
getAddRmvLinesPrNumForCloudGroup $taos_name  

#### wangguan
#taos_name=wangguan
#filter_usr_name="skye0207|skye|Guan Wang"
#getAddRmvLinesPrNumForCloudGroup $taos_name

#### yuanyuan
taos_name=yuanyuan
filter_usr_name="Eavanyuan|yuanyuan"
getAddRmvLinesPrNumForCloudGroup $taos_name  

#### zhangmingshuo
taos_name=zhangmingshuo
filter_usr_name="mingshuozhang|mszhang"
getAddRmvLinesPrNumForCloudGroup $taos_name  

echo "========================================"
echo "======= get github code info end ======="
echo "========================================"

################################################################################################################################################
