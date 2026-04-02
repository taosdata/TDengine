ext_path=/opt/hivemq-4.8.1/extensions
plugin_path=/root/hivemq-tdengine-extension
lib_path=`find /var/lib/jenkins/workspace/TDinternal/debug/build/lib/ -name libtaos.so.*.*.*.*`
docker kill hivemq4
docker rm hivemq4
docker run -d --net=host -v $plugin_path:$ext_path/hivemq-tdengine-extension -v $lib_path:/usr/lib/libtaos.so  --name hivemq4 hivemq/hivemq4