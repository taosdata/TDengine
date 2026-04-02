#!/bin/bash

echo "=== Install dependencies ==="
apt update
apt install -y python3-pip jq

echo "=== Clone repositories ==="
cd /root 
git clone -b master https://github.com/taosdata/taos-test-framework
git clone -b master https://github.com/taosdata/TestNG

echo "=== Build taostest ==="
apt install -y python3-pip
pip3 install poetry
cd /root/taos-test-framework
yes | bash reinstall.sh
pip3 install --upgrade numpy pandas

echo "=== Configure passwordless login ==="
[ ! -f "$HOME/.ssh/id_rsa" ] && yes | ssh-keygen -t rsa -b 2048 -N "" -f $HOME/.ssh/id_rsa
[ -f "$HOME/.ssh/id_rsa.pub" ] && \
  ! grep -q -F "$(cat $HOME/.ssh/id_rsa.pub)" "$HOME/.ssh/authorized_keys" && \
  cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"

echo "=== Configure SSH settings ==="
cat <<EOF > /root/.ssh/config
Host *
User xxx
StrictHostKeyChecking no
CheckHostIP no
ConnectTimeout 1
ConnectionAttempts 3
PasswordAuthentication no
ServerAliveInterval 60
UserKnownHostsFile /dev/null
EOF

echo "=== Configure taostest environment ==="
cat > /root/.taostest/.env << 'EOF'
TEST_ROOT=/root/TestNG/
TAOSTEST_COVERAGE_ENABLED=1
EOF

echo "=== SSH restart ==="
sudo service ssh restart

#echo "=== Run test script ==="
# /root/TestNG/scripts/run.sh \
#   -m /root/TestNG/scripts/testng.json \
#   -t /root/TestNG/scripts/testng_cases.txt \
#   -l /root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S") \
#   -d debug -o 12000 -f False -a True

echo "=== Create test cases file ==="
cat > /root/TestNG/scripts/test.txt << 'EOF'
taostest --setup=coverage_test.yaml --case=taosc_insert/bool_check.py --coverage  --keep --disable_collection
taostest --use=coverage_test.yaml --case=taosc_insert/child_tb_check.py  --keep --disable_collection
taostest --use=coverage_test.yaml --case=taosc_insert/alter_insert.py  --keep --disable_collection
taostest --use=coverage_test.yaml --case=taosc_insert/auto_create_table.py  --keep --disable_collection
taostest --use=coverage_test.yaml --case=taosc_insert/batch_insert.py  --keep --disable_collection
EOF

echo "=== Run test script ==="
/root/TestNG/scripts/run.sh \
  -m /root/TestNG/scripts/testng.json \
  -t /root/TestNG/scripts/test.txt \
  -l /root/TestNG/testlog_$(date +"%Y-%m-%d_%H-%M-%S") \
  -d debug -o 1200 -f False -a True

echo "=== Test execution completed ==="