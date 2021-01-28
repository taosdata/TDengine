FROM ubuntu:18.04

WORKDIR /root

ARG pkgFile
ARG dirName
RUN echo ${pkgFile}
RUN echo ${dirName}

COPY ${pkgFile} /root/
RUN tar -zxf ${pkgFile}
WORKDIR /root/${dirName}/
RUN /bin/bash install.sh -e no

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
EXPOSE 6030 6031 6032 6033 6034 6035 6036 6037 6038 6039 6040 6041 6042
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos","/etc/taos/" ]
