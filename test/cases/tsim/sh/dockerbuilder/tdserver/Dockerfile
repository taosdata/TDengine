FROM centos:8 AS builder
 
ARG PACKAGE=TDengine-server-3.0.0.0-Linux-x64.tar.gz
ARG EXTRACTDIR=TDengine-server-3.0.0.0
 
WORKDIR /root
 
COPY ${PACKAGE} .
COPY tmux.conf .
COPY addDnodeToCluster.sh .
 
RUN tar -zxf ${PACKAGE}
RUN mv ${EXTRACTDIR}/* ./
 
FROM centos:8
 
WORKDIR /root

RUN yum install -y glibc-langpack-en dmidecode gdb
RUN yum install -y tmux net-tools
RUN yum install -y sysstat
RUN yum install -y vim
RUN echo 'alias ll="ls -l --color=auto"' >> ~/.bashrc
 
COPY --from=builder /root/taosd /usr/bin
COPY --from=builder /root/taos /usr/bin
COPY --from=builder /root/create_table /usr/bin
COPY --from=builder /root/tmux.conf /root/.tmux.conf
#COPY --from=builder /root/addDnodeToCluster.sh /root/addDnodeToCluster.sh

#COPY --from=builder /root/cfg/taos.cfg /etc/taos/
COPY --from=builder /root/lib/* /usr/lib/
 
ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/lib"
ENV LC_CTYPE=en_US.UTF-8
ENV LANG=en_US.UTF-8
 
EXPOSE 6030-6042/tcp 6060/tcp 6030-6039/udp
 
# VOLUME [ "/var/lib/taos", "/var/log/taos", "/etc/taos" ]
 
CMD [ "bash" ]
