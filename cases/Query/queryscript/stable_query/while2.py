import os,time

for i in range(1, 10000):
#while 1:    
    os.system("nohup taostest --use=query_stability_local3_64.yaml --case=Query/queryscript/stable_query/stable_schema_all1.py --keep --disable_collection >while.txt&")
    time.sleep(300)
    os.system("nohup taostest --use=query_stability_local3_64.yaml --case=Query/queryscript/stable_query/stable_schema_all.py --keep --disable_collection >while.txt&")
    time.sleep(300)
