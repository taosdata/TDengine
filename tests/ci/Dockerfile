FROM python:3.8
COPY sources.list /etc/apt/
COPY id_ecdsa /root/.ssh/id_ecdsa
RUN apt-key adv --keyserver keyserver.ubuntu.com  --recv-keys 3B4FE6ACC0B21F32  && apt-key adv --keyserver keyserver.ubuntu.com  --recv-keys 871920D1991BC93C
RUN apt-get update 
RUN apt-get install -y locales psmisc sudo tree  libgeos-dev libgflags2.2 libgflags-dev  libgoogle-glog-dev libjansson-dev libsnappy-dev liblzma-dev libz-dev zlib1g pkg-config build-essential valgrind rsync vim  libjemalloc-dev openssh-server screen sshpass net-tools dirmngr gnupg apt-transport-https ca-certificates software-properties-common  r-base iputils-ping clang-tools-16
RUN sed -i 's/# en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen  && locale-gen 
RUN pip3 config set global.index-url http://admin:123456@192.168.0.212:3141/admin/dev/+simple/
RUN pip3 config set global.trusted-host 192.168.0.212
RUN pip3 install taospy==2.7.15 taos-ws-py==0.3.1 pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog
ENV LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8 LC_ALL=en_US.UTF-8
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu focal-cran40/'
RUN apt install -y r-base
ADD go1.17.6.linux-amd64.tar.gz /usr/local/
ADD jdk-8u144-linux-x64.tar.gz /usr/local/
ADD apache-maven-3.8.4-bin.tar.gz /usr/local/
RUN apt-get install wget -y \
  && wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb \
  && dpkg -i packages-microsoft-prod.deb \
  && rm packages-microsoft-prod.deb \
  && apt-get update && apt-get install -y dotnet-sdk-5.0 && apt-get install -y dotnet-sdk-6.0
ADD node-v12.20.0-linux-x64.tar.gz /usr/local/
RUN sh -c "rm -f /etc/localtime;ln -s /usr/share/zoneinfo/Asia/Shanghai /etc/localtime;echo \"Asia/Shanghai\" >/etc/timezone"
COPY .m2 /root/.m2
COPY .nuget /root/.nuget
COPY .dotnet /root/.dotnet
COPY go /root/go
ADD cmake-3.21.5-linux-x86_64.tar.gz /usr/local/
RUN echo " export RUSTUP_DIST_SERVER=\"https://rsproxy.cn\" " >> /root/.bashrc
RUN echo " export RUSTUP_UPDATE_ROOT=\"https://rsproxy.cn/rustup\" " >> /root/.bashrc
RUN curl https://sh.rustup.rs -o /tmp/rustup-init.sh
RUN sh /tmp/rustup-init.sh -y
COPY .cargo/config /root/.cargo/config
ENV PATH /usr/local/go/bin:/usr/local/node-v12.20.0-linux-x64/bin:/usr/local/apache-maven-3.8.4/bin:/usr/local/jdk1.8.0_144/bin:/usr/local/cmake-3.21.5-linux-x86_64/bin:/root/.cargo/bin:$PATH
ENV JAVA_HOME /usr/local/jdk1.8.0_144
RUN go env -w GOPROXY=https://goproxy.cn
RUN echo "StrictHostKeyChecking no" >>/etc/ssh/ssh_config
RUN npm config -g set unsafe-perm
RUN npm config -g set registry https://registry.npm.taobao.org
COPY .npm /root/.npm
RUN R CMD javareconf JAVA_HOME=${JAVA_HOME} JAVA=${JAVA_HOME}/bin/java JAVAC=${JAVA_HOME}/bin/javac JAVAH=${JAVA_HOME}/bin/javah JAR=${JAVA_HOME}/bin/jar
RUN echo "install.packages(\"RJDBC\", repos=\"http://cran.us.r-project.org\")"|R --no-save
COPY .gitconfig /root/.gitconfig
RUN mkdir -p /run/sshd
COPY id_ecdsa.pub /root/.ssh/id_ecdsa.pub
COPY id_ecdsa.pub /root/.ssh/authorized_keys
RUN pip3 uninstall -y taostest
COPY repository/TDinternal /home/TDinternal
COPY setup.sh /home/setup.sh
