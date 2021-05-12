from datetime import datetime
import time
import os

os.system("awk -v  OFS=','  '{$1=$1;print$0}' ./all_query_res0.txt  > ./new_query_res0.txt")
with open('./new_query_res0.txt','r+') as f0:
    contents = f0.readlines()
    if os.path.exists('./test_query_res0.txt'):
        os.system("rm -rf ./test_query_res0.txt")
    for i in range(len(contents)):
        content = contents[i].rstrip('\n')
        stimestamp = content.split(',')[0]
        timestamp = int(stimestamp)
        d = datetime.fromtimestamp(timestamp/1000)
        str0 = d.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        ts = "'"+str0+"'"
        str1 = "'"+content.split(',')[1]+"'"
        str2 = "'"+content.split(',')[2]+"'"
        content = ts + ","  + str1 + "," + str2 + "," + content.split(',',3)[3] 
        contents[i] = content + "\n"
        with open('./test_query_res0.txt','a') as fi:
            fi.write(contents[i])

os.system("rm -rf ./new_query_res0.txt")




    

        
# timestamp = 1604160000099
# d = datetime.fromtimestamp(timestamp/1000)
# str1 = d.strftime("%Y-%m-%d %H:%M:%S.%f")
# print(str1[:-3])
