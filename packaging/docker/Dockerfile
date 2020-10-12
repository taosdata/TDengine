FROM centos:7

WORKDIR /root

ARG version
RUN echo $version
COPY tdengine.tar.gz /root/
RUN tar -zxf tdengine.tar.gz
WORKDIR /root/TDengine-server-$version/
RUN sh install.sh -e no


ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
EXPOSE 6030 6031 6032 6033 6034 6035 6036 6037 6038 6039 6040 6041 6042
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos","/etc/taos/" ]
