FROM nvcr.io/nvidia/cuda:12.6.3-cudnn-runtime-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system packages
RUN rm -f /etc/apt/sources.list.d/*nvidia* /etc/apt/sources.list.d/cuda* 2>/dev/null || true \
    && find /etc/apt/sources.list.d -type f -name '*.list' -exec sed -i '/developer\.download\.nvidia\.com/d' {} + 2>/dev/null || true \
    && apt-get update -o Acquire::Retries=5 && apt-get install -y \
    openssh-client \
    tmux \
    git \
    curl \
    wget \
    vim \
    jq \
    python3 \
    python3-pip \
    iproute2 \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN id -u worker-harness >/dev/null 2>&1 || useradd -m worker-harness

# Tailscale
RUN curl -fsSL https://tailscale.com/install.sh | sh

COPY requirements.txt /requirements.txt
RUN pip3 install --break-system-packages -r /requirements.txt

WORKDIR /workspace
# Copy worker daemon
COPY worker_daemon.py /worker_daemon.py

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
