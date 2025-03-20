FROM python:3.10-slim AS builder

ENV VIRTUAL_ENV=/var/lib/taos/taosanode/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false update -y && \
    apt-get install -y --no-install-recommends gcc libc-dev procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    python3.10 -m venv /var/lib/taos/taosanode/venv && \
    pip install --upgrade pip && \
    pip install --ignore-installed blinker && \
    pip install numpy==1.26.4 pandas==1.5.0 scikit-learn outlier_utils statsmodels pyculiarity pmdarima flask matplotlib uwsgi -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install torch --index-url https://download.pytorch.org/whl/cpu && \
    pip install --upgrade keras -i https://pypi.tuna.tsinghua.edu.cn/simple

FROM python:3.10-slim
COPY --from=builder /var/lib/taos/taosanode/venv /var/lib/taos/taosanode/venv
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV VIRTUAL_ENV=/var/lib/taos/taosanode/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
