# Cloud taosX History

## 2025-07-25

```bash
git checkout fix/taos-connector-timeout
cargo build --release --features disable-enterprise-only-validation,disable-enterprise-connector-validation
cp ../../target/release/taosx .

docker pull image.cloud.taosdata.com/taosx/serve:1.7.0-8a6d8482-mqtt

tee Dockerfile <<EOF
FROM image.cloud.taosdata.com/taosx/serve:1.7.0-8a6d8482-mqtt
ADD taosx /usr/bin/taosx
EOF

docker build . -t image.cloud.taosdata.com/taosx/serve:1.7.0-a3df1c4c8-mqtt-146f4f4a
docker push docker build . -t image.cloud.taosdata.com/taosx/serve:1.7.0-a3df1c4c8-mqtt-146f4f4a
```

Open [Jenkins Job - pushAndDeployTaosx](http://jenkins.bl.taosdata.com:30080/view/TDC-server/job/pushAndDeployTaosx/build?delay=0sec), and deploy the new version.

Updated regions:

- aliyun

## 2025-06-10

```bash
git checkout fix/ts-6566-dont-del-plz
cargo build --release --features disable-enterprise-only-validation,disable-enterprise-connector-validation
cp ../../target/release/taosx .

docker pull image.cloud.taosdata.com/taosx/serve:1.7.0-8a6d8482-mqtt

tee Dockerfile <<EOF
FROM image.cloud.taosdata.com/taosx/serve:1.7.0-8a6d8482-mqtt
ADD taosx /usr/bin/taosx
EOF

docker build . -t image.cloud.taosdata.com/taosx/serve:1.7.0-8f18b74fc-mqtt-146f4f4a
docker push docker build . -t image.cloud.taosdata.com/taosx/serve:1.7.0-8f18b74fc-mqtt-146f4f4a
```

Open [Jenkins Job - pushAndDeployTaosx](http://jenkins.bl.taosdata.com:30080/view/TDC-server/job/pushAndDeployTaosx/build?delay=0sec), and deploy the new version.

