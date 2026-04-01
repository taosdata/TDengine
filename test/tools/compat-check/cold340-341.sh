pkill -9 taosd
rm -rf ./rolling_cold3400_3408/*
python3 run/main.py \
  --from-dir test/3.4.0.0 \
  --to-dir   test/3.4.1.0 \
  --path     ./rolling_cold340_341 \
  --fqdn     $(hostname) \
  --check-sysinfo
