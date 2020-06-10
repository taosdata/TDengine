## Builder image
FROM tdengine/tdengine:dev as builder1
FROM tdengine/bailongma:dev as builder


COPY server.go /root/blm_prometheus/

WORKDIR /root/blm_prometheus

# build enterprise version

RUN go build
# # build community version
# RUN cmake .. -DVERSION=lite && cmake --build .

## Target image


FROM centos:7

WORKDIR /root/

# COPY --from=builder /root/build/build/lib/libtaos.so /usr/lib/libtaos.so.1
# RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
COPY --from=builder1 /usr/include/taos.h /usr/include/
COPY --from=builder1 /usr/lib/libtaos.so.1 /usr/lib/libtaos.so.1
RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so


COPY --from=builder /root/blm_prometheus/blm_prometheus .

#COPY community/packaging/cfg/taos.cfg /etc/taos/

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LANG=en_US.UTF-8  
ENV LANGUAGE=en_US:en  
ENV LC_ALL=en_US.UTF-8
EXPOSE 10203
VOLUME [ "/var/lib/taos", "/var/log/taos","/etc/taos" ]

ENTRYPOINT  [ "/root/blm_prometheus" ]