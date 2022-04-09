taostest --setup=test_217env.yaml

#table_query
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/table_query/table_query_union.py --keep

#stable_query
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_null.py --keep
taostest --use=test_217env.yaml --case=Query/queryscript/stable_query/stable_query_union.py --keep