## Builder image
FROM hzcheng/centos:dev as builder

COPY community /root/community
COPY enterprise /root/enterprise
COPY CMakeLists.txt /root

WORKDIR /root/build

# build enterprise version
RUN cmake .. && cmake --build .
# # build community version
# RUN cmake ../community && cmake --build .

## Target image
FROM centos:8

WORKDIR /root

COPY --from=builder /root/build/build/bin/taosd /usr/bin
COPY --from=builder /root/build/build/bin/taos /usr/bin
COPY --from=builder /root/build/build/lib/libtaos.so.1 /usr/lib/
COPY community/packaging/cfg/taos.cfg /etc/taos/

RUN yum install -y glibc-langpack-en dmidecode

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8

EXPOSE 6030/tcp
EXPOSE 6030/udp
EXPOSE 6031/tcp
EXPOSE 6031/udp
EXPOSE 6032/tcp
EXPOSE 6032/udp
EXPOSE 6033/tcp
EXPOSE 6033/udp
EXPOSE 6034/tcp
EXPOSE 6034/udp
EXPOSE 6035/tcp
EXPOSE 6035/udp
EXPOSE 6036/tcp
EXPOSE 6036/udp
EXPOSE 6037/tcp
EXPOSE 6037/udp
EXPOSE 6038/tcp
EXPOSE 6038/udp
EXPOSE 6039/tcp
EXPOSE 6039/udp
EXPOSE 6040/tcp
EXPOSE 6041/tcp
EXPOSE 6060/tcp

VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos" ]

CMD [ "taosd" ]