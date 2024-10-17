FROM centos:7
# dev base
RUN yum install -y autoconf make automake cmake gcc gcc-g++ openssl-devel epel-release \
  && sed -e 's!^metalink=!#metalink=!g' \
    -e 's!^#baseurl=!baseurl=!g' \
    -e 's!http://download\.fedoraproject\.org/pub/epel!http://mirrors.tuna.tsinghua.edu.cn/epel!g' \
    -e 's!http://download\.example/pub/epel!http://mirrors.tuna.tsinghua.edu.cn/epel!g' \
    -i /etc/yum.repos.d/epel*.repo \
  && yum install -y curl libcurl wget binutils-aarch64-linux-gnu gcc-c++-aarch64-linux-gnu gcc-aarch64-linux-gnu -y \
  && yum clean all

# rustup
RUN curl --proto '=https'  -fsSL "https://sh.rustup.rs" | sh -s -- --default-toolchain none -y
ENV PATH=/root/.cargo/bin:$PATH

