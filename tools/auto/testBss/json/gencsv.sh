taosBenchmark -f generate.json
taos -s "select * from test100w.d0 >> d0_100w_64.csv"
sed -i "1d" d0_100w_64.csv
