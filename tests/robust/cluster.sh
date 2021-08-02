#!/bin/bash 
stty erase '^H'
stty erase '^?'

# 运行前需要安装expect; apt install expect
# 运行方式:
# ./cluster.sh -c xxx.cfg
# cfg文件内格式: 每行代表一个节点 第一列为external ip、第二列为密码、第三列为用户名、第四列为hostname、第五列为interal ip
# 注意：列与列直接用空格隔开
# 例子:
# 51.143.97.155 tbase125! root node5 10.2.0.10
# 20.94.253.116 tbase125! root node2 10.2.0.12
# 20.94.250.236 tbase125! root node3 10.2.0.13
# 20.98.72.51 tbase125! root node4 10.2.0.14

menu(){
    echo "=============================="
    echo "-------------Target-----------"
    echo "=============================="
    echo "1 cluster"
    echo "=============================="
    echo "2 dnode"
    echo "=============================="
    echo "3 arbitrator"
    echo "=============================="
    echo "4 alter replica"
    echo "=============================="
    echo "5 exit"
    echo "=============================="
}

cluster_menu(){
    echo "=============================="
    echo "----------Operation-----------"
    echo "=============================="
    echo "1 start cluster"
    echo "=============================="
    echo "2 stop cluster"
    echo "=============================="
    echo "3 exit"
    echo "=============================="
}

dnode_menu(){
    echo "=============================="
    echo "----------Operation-----------"
    echo "=============================="
    echo "1 start dnode"
    echo "=============================="
    echo "2 stop dnode"
    echo "=============================="
    echo "3 add dnode"
    echo "=============================="
    echo "4 drop dnode"
    echo "=============================="
    echo "5 exit"
    echo "=============================="
}

arbitrator_menu(){
    echo "=============================="
    echo "----------Operation-----------"
    echo "=============================="
    echo "1 start arbitrator"
    echo "=============================="
    echo "2 stop arbitrator"
    echo "=============================="
    echo "3 exit"
    echo "=============================="
}

print_cfg() {
    echo "=============================="
    echo "-------Configure file---------"
    echo "=============================="
    echo "Id |   IP address   |  hostname"
    i=1
    while read line || [[ -n ${line} ]]
    do
        arr=($line)
        echo " $i | ${arr[0]} | ${arr[3]}" 
        i=`expr $i + 1`;
    done < $1
    echo "=============================="
}

update(){
    expect -c "
        set timeout -1;
        spawn ssh $3@$1;
        expect {
            *yes/no* { send \"yes\r\"; exp_continue }
            *assword:* { send \"$2\r\" }
        }
        expect {
            *#* { send \"systemctl $4 taosd\r\" }
        }
        expect {
            *#* { send \"exit\r\" }
        }
        expect eof;
    "
    echo -e "\033[32mdnode successfully $4 \033[0m"
}

update_dnode(){
    i=1
    while read line || [[ -n ${line} ]]
    do
        if [[ $1 -eq $i ]]; then
        arr=($line)
        update ${arr[0]} ${arr[1]} ${arr[2]} $2
        break;
        fi
        i=`expr $i + 1`;
    done < $3
}

add_hosts() {
    expect -c "
        set timeout -1;
        spawn ssh $3@$1;
        expect {
            *yes/no* { send \"yes\r\"; exp_continue }
            *assword:* { send \"$2\r\" }
        }
        expect {
            *#* { send \"echo $4 $5 >> /etc/hosts\r\" }
        }
        expect {
            *#* { send \"exit\r\" }
        }
        expect eof;
        "
    echo -e "\033[32mSuccessfully add to /etc/hosts in $1\033[0m"
}

remove_hosts() {
    expect -c "
        set timeout -1;
        spawn ssh $3@$1;
        expect {
            *yes/no* { send \"yes\r\"; exp_continue }
            *assword:* { send \"$2\r\" }
        }
        expect {
            *#* { send \"sed -i '/$4/d\' /etc/hosts\r\" }
        }
        expect {
            *#* { send \"exit\r\" }
        }
        expect eof;
        "
    echo -e "\033[32mSuccessfully remove from /etc/hosts in $1\033[0m"
}

remove_varlibtaos() {
    expect -c "
        set timeout -1;
        spawn ssh $3@$1;
        expect {
            *yes/no* { send \"yes\r\"; exp_continue }
            *assword:* { send \"$2\r\" }
        }
        expect {
            *#* { send \"rm -rf /var/lib/taos/*\r\" }
        }
        expect {
            *#* { send \"exit\r\" }
        }
        expect eof;
        "
    echo -e "\033[32mSuccessfully remove /var/lib/taos/* in $1\033[0m"
}

scp_cfg() {
    expect -c "
        set timeout -1;
        spawn scp /etc/taos/taos.cfg $3@$1:/etc/taos;
        expect {
            *yes/no* { send \"yes\r\"; exp_continue }
            *assword:* { send \"$2\r\" }
        }
        expect eof;
        "
    echo -e "\033[32mSuccessfully scp /etc/taos/taos.cfg to $1\033[0m"
}

manage_dnode(){
    i=1
    while read line || [[ -n ${line} ]]
    do
        if [[ $1 -eq $i ]]; then
        arr=($line)
        scp_cfg ${arr[0]} ${arr[1]} ${arr[2]}
        ip=${arr[0]}
        pd=${arr[1]}
        user=${arr[2]}
        j=1
        while read line2 || [[ -n ${line2} ]]
        do
            arr2=($line2)
            if [[ $1 -ne $j ]]; then
                if [ $3 == "create" ];then
                echo "$3"
                add_hosts $ip $pd $user ${arr2[4]} ${arr2[3]}
                else
                remove_hosts $ip $pd $user ${arr2[4]} ${arr2[3]}
                fi
            fi
            j=`expr $j + 1`;
        done < $2
        remove_varlibtaos $ip $pd $user
        if [ $3 == "create" ];then
            update $ip $pd $user "start"
        else
            update $ip $pd $user "stop"
        fi
        taos -s "$3 dnode \"${arr[3]}:6030\""
        break;
        fi
        i=`expr $i + 1`;
    done < $2
    echo -e "\033[32mSuccessfully $3 dnode id $1\033[0m"
}

update_cluster() {
    while read line || [[ -n ${line} ]]
    do
        arr=($line)
        if [ $1 == "start" ]; then
        scp_cfg ${arr[0]} ${arr[1]} ${arr[2]}
        fi
        update ${arr[0]} ${arr[1]} ${arr[2]} $1
    done < $2
}

while :
do
    clear
    menu
    read -p "select mode: " n
    case $n in
    1)
        clear
        print_cfg $2
        cluster_menu
        read -p "select operation: " c
        case $c in
        1)
            update_cluster "start" $2 
            break
        ;;
        2)
            update_cluster "stop" $2 
            break
        ;;
        3)
            break
        ;;
        esac
    ;;
    2)
        clear
        print_cfg $2
        dnode_menu
        read -p "select operation: " d
        case $d in
        1)
            clear
            print_cfg $2
            read -p "select dnode: " id
            update_dnode $id "start" $2
            break
        ;;
        2)
            clear
            print_cfg $2
            read -p "select dnode: " id
            update_dnode $id "stop" $2
            break
        ;;
        3)
            clear
            print_cfg $2
            read -p "select dnode: " id
            manage_dnode $id $2 "create"
            break
        ;;
        4)
            clear
            print_cfg $2
            read -p "select dnode: " id
            manage_dnode $id $2 "drop"
            break
        ;;
        5)
            break
        ;;
        esac
    ;;
    3)
        clear
        arbitrator_menu
        read -p "select operation: " m
        case $m in
        1)
            nohup /usr/local/taos/bin/tarbitrator >/dev/null 2>&1 &
            echo -e "\033[32mSuccessfully start arbitrator $3 \033[0m"
            break
        ;;
        2)
            var=`ps -ef | grep tarbitrator | awk '{print $2}' | head -n 1`
            kill -9 $var
            echo -e "\033[32mSuccessfully stop arbitrator $3 \033[0m"
            break
        ;;
        3)
            break
        ;;
        esac
    ;;
    4)
        read -p "Enter replica number: " rep
        read -p "Enter database name: " db
        taos -s "alter database $db replica $rep"
        echo -e "\033[32mSuccessfully change $db's replica to $rep \033[0m"
        break
    ;;
    5)
        break
    ;;
    esac
done
