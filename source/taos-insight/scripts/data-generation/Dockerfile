FROM tdengine/tdengine:3.3.6.6

RUN apt update && apt install -y python3 python3-pip
RUN pip3 install taospy

COPY main.py /main.py
COPY metrics.py /metrics.py

CMD ["python3", "/main.py"]
