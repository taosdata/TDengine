pkill -9 taosd
rm -rf ./rolling_cold3380_3408/*
python3 run/main.py \
  --from-dir test/3.3.8.0 \
  --to-dir   test/3.4.0.8 \
  --path     ./rolling_cold3360_3408 \
  --fqdn     $(hostname) \
  --check-sysinfo
  --no-tsma
