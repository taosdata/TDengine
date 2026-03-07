cd test/tools/hot_update
python3 run/main.py \
  --from-dir test/3.4.0.0 \
  --to-dir   test/3.4.0.8 \
  --path     ./rolling_test \
  --fqdn     $(hostname)
