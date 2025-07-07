python3 -m venv .test-env
. .test-env/bin/activate
pip3 install paho-mqtt==2.1.0
python3 ./sub.py
