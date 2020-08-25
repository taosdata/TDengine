## Builder image
FROM hzcheng/centos:dev as builder

ARG BRANCH=community

COPY community /root/community
COPY enterprise /root/enterprise
COPY CMakeLists.txt /root

WORKDIR /root/build

RUN if [ "${BRANCH}" = "community" ] ; then cmake ../community && cmake --build . ; else cmake .. && cmake --build . ; fi

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

EXPOSE 6030-6041/tcp 6060/tcp 6030-6039/udp

VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos" ]

CMD [ "taosd" ]