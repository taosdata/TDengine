FROM ubuntu:18.04

WORKDIR /root

ARG pkgFile
ARG dirName
ARG cpuType
RUN echo ${pkgFile} && echo ${dirName}

COPY ${pkgFile} /root/
RUN tar -zxf ${pkgFile}
WORKDIR /root/
RUN cd /root/${dirName}/ && /bin/bash install.sh -e no && cd /root
RUN rm /root/${pkgFile}
RUN rm -rf /root/${dirName}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get clean && apt-get update && apt-get install -y locales tzdata netcat && locale-gen en_US.UTF-8
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib" \
    LC_CTYPE=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

COPY ./bin/* /usr/bin/

ENV TINI_VERSION v0.19.0
RUN bash -c 'echo -e "Downloading tini-${cpuType} ..."'
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${cpuType} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "/usr/bin/entrypoint.sh"]
CMD ["taosd"]
VOLUME [ "/var/lib/taos", "/var/log/taos", "/corefile" ]
