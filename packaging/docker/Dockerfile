FROM centos:7

WORKDIR /root

COPY tdengine.tar.gz /root/
RUN tar -zxf tdengine.tar.gz
WORKDIR /root/TDengine-server/
RUN sh install.sh


ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
EXPOSE 6020 6030 6031 6032 6033 6034 6035 6036 6037 6038 6039 6040 6041 6042
EXPOSE 6043 6044 6045 6046 6047 6048 6049 6050
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos","/etc/taos/" ]
