rm -rf target/package/ >/dev/null 2>&1 || true

mkdir -p target/package/bin
mkdir -p target/package/plugins/influxdb
mkdir -p target/package/plugins/mqtt
mkdir -p target/package/plugins/opc
mkdir -p target/package/etc/systemd

cp target/release/taosx target/release/taosx-agent target/package/bin/
cp plugins/influxdb/target/taosx-influxdb.jar target/package/plugins/influxdb/
cp plugins/mqtt/target/taosx-mqtt target/package/plugins/mqtt/
cp plugins/opc/target/taosx-opc target/package/plugins/opc/
cp target/*.service target/package/etc/systemd/
cp taosx-agent/examples/agent.example.toml target/package/etc/

cp scripts/install.sh target/package/

cd target/package/
version=$(./bin/taosx --version|cut -f 2 -d " ")
tar -cavf taosx-$version.tar.gz bin/ plugins/ etc
