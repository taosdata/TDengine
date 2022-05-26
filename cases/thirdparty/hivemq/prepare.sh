if [ ! `command -v unzip` ]; then
  apt-get install -y unzip
fi

if [ ! `command -v mosquitto_pub`]; then
  apt-get install -y mosquitto-clients
fi

if [ ! `command -v mvn` ]; then
  apt-get install -y maven
fi

apt-get install openjdk-11-jdk -y