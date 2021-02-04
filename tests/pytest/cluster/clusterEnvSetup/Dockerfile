FROM ubuntu:latest AS builder
 
ARG PACKAGE=TDengine-server-1.6.5.10-Linux-x64.tar.gz
ARG EXTRACTDIR=TDengine-enterprise-server
ARG CONTENT=taos.tar.gz
 
WORKDIR /root
 
COPY ${PACKAGE} .
 
RUN tar -zxf ${PACKAGE}
RUN tar -zxf ${TARBITRATORPKG}
RUN mv ${EXTRACTDIR}/driver ./lib
RUN tar -zxf ${EXTRACTDIR}/${CONTENT}
 
FROM ubuntu:latest
 
WORKDIR /root

RUN apt-get update
RUN apt-get install -y vim tmux net-tools
RUN echo 'alias ll="ls -l --color=auto"' >> /root/.bashrc
RUN echo 'node1 172.27.0.7' >> /etc/hosts
RUN echo 'node2 172.27.0.8' >> /etc/hosts
RUN echo 'node3 172.27.0.9' >> /etc/hosts
RUN echo 'node4 172.27.0.10' >> /etc/hosts
RUN echo 'node5 172.27.0.11' >> /etc/hosts
RUN ulimit -c unlimited
RUN mkdir /coredump
RUN echo 'kernel.core_pattern=/coredump/core_%e_%p' >> /etc/sysctl.conf
RUN sysctl -p
       
COPY --from=builder /root/bin/taosd /usr/bin
COPY --from=builder /root/bin/tarbitrator /usr/bin
COPY --from=builder /root/bin/taos /usr/bin
COPY --from=builder /root/cfg/taos.cfg /etc/taos/
COPY --from=builder /root/lib/libtaos.so.* /usr/lib/libtaos.so.1
 
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8
 
EXPOSE 6030-6042/tcp 6060/tcp 6030-6039/udp
 
# VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos" ]
 
CMD [ "bash" ]
