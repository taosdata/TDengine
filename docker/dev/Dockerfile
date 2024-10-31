FROM alpine AS builder
ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN echo ${BUILDPLATFORM}, ${TARGETPLATFORM}
RUN case "${TARGETPLATFORM}" in \
  "linux/amd64") \
    echo MOLD_ARCH=x86_64 > /etc/environment; \
    echo TINI_ARCH=amd64 >> /etc/environment \
    ;; \
  "linux/arm64") \
    echo MOLD_ARCH=aarch64 > /etc/environment; \
    echo TINI_ARCH=arm64 >> /etc/environment \
    ;; \
  *) \
    echo "Unsupported platform: ${TARGETPLATFORM}" \
    exit 1 \
    ;; \
  esac
RUN cat /etc/environment

# based on https://github.com/pypa/manylinux/issues/1306#issuecomment-1374817895
FROM lukewiwa/manylinux2014
COPY --from=builder /etc/environment /etc/
RUN yum makecache fast && yum install -y wget perl-IPC-Cmd libatomic openssl-devel

# Rust
ENV RUSTUP_UPDATE_ROOT="https://rsproxy.cn/rustup"
ENV RUSTUP_DIST_SERVER="https://rsproxy.cn"
ENV RUST_VERSION=1.81.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --verbose --profile minimal --component clippy,rustfmt --default-toolchain $RUST_VERSION
ENV PATH=/root/.cargo/bin:$PATH


#COPY --from=builder /etc/environment /etc/
ENV TINI_VERSION=v0.19.0
RUN . /etc/environment && wget -O /bin/tini https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${TINI_ARCH} && chmod +x /bin/tini

ENV MOLD_VERSION=2.34.1
RUN cat /etc/environment
RUN . /etc/environment && wget https://github.com/rui314/mold/releases/download/v${MOLD_VERSION}/mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz \
  && tar -xvf mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz  \
	&& cp mold-${MOLD_VERSION}-${MOLD_ARCH}-linux/bin/mold /usr/bin/mold \
	&& rm -rf mold-${MOLD_VERSION}-${MOLD_ARCH}-linux mold-${MOLD_VERSION}-${MOLD_ARCH}-linux.tar.gz

# Fix openssl compile error.
COPY ./cpanm /bin/
ENV CPAN_MIRROR=https://mirrors.aliyun.com/CPAN/
ENV PERL_CPANM_OPT="-M ${CPAN_MIRROR}"
RUN cpanm --force --notest List::Util

# Workdir and trust
WORKDIR /app
RUN git config --global --add safe.directory /app

ADD ./cargo.toml /root/.cargo/config.toml

ENTRYPOINT ["/bin/tini", "--"]
