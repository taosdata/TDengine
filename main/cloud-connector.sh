# $1 = test | aws | google | azu

tt --group-dir=cloud/connector --use=cloud/cloud_$1.yaml --early_stop --keep

