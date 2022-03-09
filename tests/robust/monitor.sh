while :
do
    dlog=`taos -s "show dnodes"`
    mlog=`taos -s "show mnodes"`
    echo "$dlog" | tee -a dnode.log
    echo "$mlog" | tee -a mnode.log
    sleep 1s
done