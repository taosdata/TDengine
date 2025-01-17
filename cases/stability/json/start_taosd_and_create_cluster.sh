nohup taosd -c /root/stability_test/cfg3/cfg_1 > /root/stability_test/log/taosd1.log &
sleep 1
nohup taosd -c /root/stability_test/cfg3/cfg_2 > /root/stability_test/log/taosd2.log &
sleep 1
nohup taosd -c /root/stability_test/cfg3/cfg_3 > /root/stability_test/log/taosd3.log &
sleep 1
taos -s "create dnode 'localhost:7030'"
sleep 2
taos -s "create dnode 'localhost:8030'"
sleep 2
taos -s "create mnode on dnode 2;"
sleep 5
taos -s "create mnode on dnode 3;"
sleep 3
taos -s "show dnodes;show mnodes;"
sleep 1
nohup taosBenchmark -f /root/stability_test/code/insertQuery_pk_new.json & 
sleep 30
nohup taosBenchmark -f /root/stability_test/code/insertQuery_new.json & 
sleep 30
taos -s "show dnodes;show mnodes;show queries;show transactions;"
sleep 3
nohup python3 /root/stability_test/for_start_cluster.py >cluster.log&
sleep 3
nohup python3 /root/stability_test/for_changwen.py &
sleep 3
watch "taos -s 'show dnodes;show mnodes;show queries;show transactions;select count(*) from dbnew.stb0;select count(*) from dbnew2.stb0;'"