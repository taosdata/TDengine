pkill -9 taosd
rm -rf ./rolling_hot3400_3408/*
python3 run/main.py \
  --from-dir test/3.4.0.0 \
  --to-dir   test/3.4.0.8 \
  --path     ./rolling_hot3400_3408 \
  --fqdn     $(hostname) \
  --check-sysinfo \
  --rollupdate
