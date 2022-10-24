# $1 = test | aws | gcp | az

tt --group-dir=cloud/connector --use=cloud/cloud_$1.yaml --early_stop --keep

