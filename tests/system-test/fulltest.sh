python3 test.py -f 0-management/1-stable/create_col_tag.py 
# python3 test.py -f 2-query/9-others/TD-11945_crash.py # this test case must need TD-6140 merge into develop
#python3 test.py -f 2-query/9-others/TD-11389.py # this case will run when this bug fix  TD-11389
python3 test.py -f 4-taosAdapter/taosAdapter_query.py
python3 test.py -f 4-taosAdapter/taosAdapter_insert.py
python3 test.py -f 5-taos-tools/taosdump/basic.py 

#python3 test.py -f 2-query/9-others/TD-11389.py # this case will run when this bug fix  TD-11389

python3 test.py -f 3-connectors/restful/restful_binddbname.py 

python3 test.py -f 5-taos-tools/taosdump/basic.py 

