## Builder image
FROM hzcheng/centos:dev as builder

COPY community /root/community
COPY enterprise /root/enterprise
COPY CMakeLists.txt /root

WORKDIR /root/build

# build enterprise version
RUN cmake .. && cmake --build .
# # build community version
# RUN cmake .. -DVERSION=lite && cmake --build .

## Target image
FROM centos:7

WORKDIR /root

# COPY --from=builder /root/build/build/lib/libtaos.so /usr/lib/libtaos.so.1
# RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
COPY --from=builder /root/build/build/bin/taosd .
COPY community/packaging/cfg/taos.cfg /etc/taos/

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8

VOLUME [ "/var/lib/taos", "/var/log/taos" ]

CMD [ "/root/taosd" ]