FROM ubuntu:18.04

WORKDIR /root

ARG pkgFile
ARG dirName
RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/
RUN tar -zxf ${pkgFile}
WORKDIR /root/${dirName}/
RUN /bin/bash install.sh -e no

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean && apt-get update && apt-get install -y locales tzdata netcat && locale-gen en_US.UTF-8
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

EXPOSE 6030-6049
EXPOSE 6030-6039/udp
COPY ./bin/* /usr/bin/
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile" ]
