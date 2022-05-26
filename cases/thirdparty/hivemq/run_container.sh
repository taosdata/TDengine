ext_path=/opt/hivemq-4.8.1/extensions
plugin_path=/root/hivemq-tdengine-extension
docker kill hivemq4
docker rm hivemq4
docker run -d -p 8080:8080 -p 1883:1883 -v $plugin_path:$ext_path/hivemq-tdengine-extension --name hivemq4 hivemq/hivemq4