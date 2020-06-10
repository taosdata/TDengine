## Builder image
#FROM tdengine/tdengine:dev as builder1
FROM golang:latest as builder1



WORKDIR /root

#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so /usr/lib/libtaos.so
#COPY --from=builder1 /usr/include/taos.h /usr/include/
#COPY --from=builder1 /usr/lib/libtaos.so.1 /usr/lib/
COPY blm_telegraf /root
COPY blm_prometheus /root
COPY TDengine-server-1.6.5.9-Linux-x64.tar.gz /root
COPY go.mod /root
COPY go.sum /root
RUN tar -zxf TDengine-server-1.6.5.9-Linux-x64.tar.gz .
WORKDIR /root/TDengine-server
RUN sh install.sh

#RUN mkdir /usr/lib/ld


WORKDIR /root/blm_telegraf
RUN go build

WORKDIR /root/blm_prometheus
RUN go build
#RUN ln -s /usr/lib/libtaos.so.1 /usr/lib/libtaos.so
FROM centos:7


WORKDIR /root

COPY --from=builder /usr/include/taos.h /usr/include/
COPY --from=builder /usr/lib/libtaos.so /usr/lib/libtaos.so
COPY --from=builder /usr/include/taos.h /usr/include/
COPY --from=builder /usr/lib/libtaos.so.1 /usr/lib/
COPY --from=builder /root/blm_telegraf/blm_telegraf /root/
COPY --from=builder /root/blm_prometheus/blm_prometheus /root/



#RUN git config --global http.sslVerify false
#RUN git config --global http.postbuffer 524288000


#RUN go get -v -u -insecure github.com/taosdata/TDengine/src/connector/go/src/taosSql
#RUN go get -v -u -insecure github.com/gogo/protobuf/proto
#RUN go get -v -u -insecure github.com/golang/snappy
#RUN go get -v -u -insecure github.com/prometheus/common/model
#RUN go get -v -u -insecure github.com/prometheus/prometheus/prompb
#RUN go get github.com/taosdata/driver-go/taosSql





