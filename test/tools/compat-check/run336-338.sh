pkill -9 taosd
rm -rf ./rolling_test/*
python3 run/main.py \
  --from-dir test/3.3.6.0 \
  --to-dir   test/3.3.8.0 \
  --path     ./rolling_test336_338 \
  --fqdn     $(hostname)
