pkill -9 taosd
rm -rf ./rolling_test/*
python3 run/main.py \
  --from-dir test/3.3.0.0 \
  --to-dir   test/3.4.0.0 \
  --path     ./rolling_test33_34 \
  --fqdn     $(hostname)
