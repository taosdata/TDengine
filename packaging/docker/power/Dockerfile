FROM centos:7

WORKDIR /root

COPY powerdb.tar.gz /root/
RUN tar -zxf powerdb.tar.gz
WORKDIR /root/PowerDB-enterprise-server/
RUN sh install.sh docker


ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
EXPOSE 6020 6030 6031 6032 6033 6034 6035 6036 6037 6038 6039 6040 6041 6042
EXPOSE 6043 6044 6045 6046 6047 6048 6049 6050
CMD ["powerd"]
VOLUME [ "/var/lib/power", "/var/log/power","/etc/power/" ]
