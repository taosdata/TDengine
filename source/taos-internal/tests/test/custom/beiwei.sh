#!/bin/sh

ts=1519747200
table_num=40000
row_num=10000

while getopts "t:r:" arg 
do
  case $arg in
    t)
      table_num=$OPTARG
      ;; 
	r)
      row_num=$OPTARG
      ;; 
	?)
      echo "unkonw argument"
      ;;
  esac
done

mmax=50
loop_num=$((table_num/mmax))

row=0
while [ $row -lt $row_num ]; do  
	timestamp=$((ts+10*$row))
	loop=0
	while [ $loop -lt $loop_num ]; do    
		table=0
		post_data="["
		while [ $table -lt $mmax ]; do   
			no=$((loop*mmax+table))
			tablename="card"$no
			value=$((table % 5))
			v1=$((value+10))
			v2=$((value+20))
			v3=$((value+30))
			v4=$((value+40))
			v5=$((value+50))
			v6=$((value+60))
			v7=$((value+70))
			v8=$((value+80))
			v9=$((value+90))
			v10=$((value+100))
			v11=$((value+110))
			v12=$((value+120))
			v13=$((value+130))
			v14=$((value+140))
			v15=$((value+150))
			v16=$((value+160))
			v17=$((value+170))
			v18=$((value+180))
			v19=$((value+190))
			v20=$((value+200))
			
			d0='{"metric": "mt","timestamp": '$timestamp', "value": '$v1',"tags": {"table": "'$tablename'", "column": "v1","t":'$no'}},'
			d1='{"metric": "mt","timestamp": '$timestamp', "value": '$v2',"tags": {"table": "'$tablename'", "column": "v2","t":'$no'}},'
			d2='{"metric": "mt","timestamp": '$timestamp', "value": '$v3',"tags": {"table": "'$tablename'", "column": "v3","t":'$no'}},'
			d3='{"metric": "mt","timestamp": '$timestamp', "value": '$v4',"tags": {"table": "'$tablename'", "column": "v4","t":'$no'}},'
			d4='{"metric": "mt","timestamp": '$timestamp', "value": '$v5',"tags": {"table": "'$tablename'", "column": "v5","t":'$no'}},'
			d5='{"metric": "mt","timestamp": '$timestamp', "value": '$v6',"tags": {"table": "'$tablename'", "column": "v6","t":'$no'}},'
			d6='{"metric": "mt","timestamp": '$timestamp', "value": '$v7',"tags": {"table": "'$tablename'", "column": "v7","t":'$no'}},'
			d7='{"metric": "mt","timestamp": '$timestamp', "value": '$v8',"tags": {"table": "'$tablename'", "column": "v8","t":'$no'}},'
			d8='{"metric": "mt","timestamp": '$timestamp', "value": '$v9',"tags": {"table": "'$tablename'", "column": "v9","t":'$no'}},'
			d9='{"metric": "mt","timestamp": '$timestamp', "value": '$v10',"tags": {"table": "'$tablename'", "column": "v10","t":'$no'}},'
			d10='{"metric": "mt","timestamp": '$timestamp', "value": '$v11',"tags": {"table": "'$tablename'", "column": "v11","t":'$no'}},'
			d11='{"metric": "mt","timestamp": '$timestamp', "value": '$v12',"tags": {"table": "'$tablename'", "column": "v12","t":'$no'}},'
			d12='{"metric": "mt","timestamp": '$timestamp', "value": '$v13',"tags": {"table": "'$tablename'", "column": "v13","t":'$no'}},'
			d13='{"metric": "mt","timestamp": '$timestamp', "value": '$v14',"tags": {"table": "'$tablename'", "column": "v14","t":'$no'}},'
			d14='{"metric": "mt","timestamp": '$timestamp', "value": '$v15',"tags": {"table": "'$tablename'", "column": "v15","t":'$no'}},'
			d15='{"metric": "mt","timestamp": '$timestamp', "value": '$v16',"tags": {"table": "'$tablename'", "column": "v16","t":'$no'}},'
			d16='{"metric": "mt","timestamp": '$timestamp', "value": '$v17',"tags": {"table": "'$tablename'", "column": "v17","t":'$no'}},'
			d17='{"metric": "mt","timestamp": '$timestamp', "value": '$v18',"tags": {"table": "'$tablename'", "column": "v18","t":'$no'}},'
			d18='{"metric": "mt","timestamp": '$timestamp', "value": '$v19',"tags": {"table": "'$tablename'", "column": "v19","t":'$no'}},'
			d19='{"metric": "mt","timestamp": '$timestamp', "value": '$v20',"tags": {"table": "'$tablename'", "column": "v20","t":'$no'}}'
			
			table=$((table + 1))
			
			post_data=$post_data$d0$d1$d2$d3$d4$d5$d6$d7$d8$d9$d10$d11$d12$d13$d14$d15$d16$d17$d18$d19$d20
			if [ $table -lt $mmax ]; then
				post_data=$post_data","
			fi 
		done
		post_data=$post_data"]"
		
		curl_data="
		curl -X POST 'http://ts-m5en905ldn6dua7x0.hitsdb.tsdb.aliyuncs.com:8242/api/put?summary=' -H 'Content-Type: application/json'  -H 'Postman-Token: f0e2d633-84df-4dba-9710-9a72b7be7477' -H 'cache-control: no-cache'   -d '"$post_data"'  "
		
                rm -rf 1.sh
		echo $curl_data > 1.sh
		chmod 777 1.sh
                #$curl_data
                ./1.sh

		loop=$((loop + 1))
	done
	row=$((row + 1))
done
