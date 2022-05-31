ext_path=/opt/hivemq-4.8.1/extensions
plugin_path=/root/hivemq-tdengine-extension
docker kill hivemq4
docker rm hivemq4
docker run -d --net=host -v $plugin_path:$ext_path/hivemq-tdengine-extension -v /var/lib/jenkins/workspace/TDinternal/debug/build/lib/libtaos.so.2.7.0.0:/usr/lib/libtaos.so  --name hivemq4 hivemq/hivemq4