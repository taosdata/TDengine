pkill -9 taosd
rm -rf ./rolling_test/*
python3 run/main.py \
  --from-dir test/3.3.8.0 \
  --to-dir   test/3.3.8.20 \
  --path     ./rolling_test338_cold \
  --fqdn     $(hostname)
