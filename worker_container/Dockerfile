FROM nvcr.io/nvidia/cuda:13.2.1-cudnn-runtime-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive

# Install system packages
RUN apt-get update && apt-get install -y \
    openssh-server \
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

# SSH server setup
RUN ssh-keygen -A \
    && mkdir -p /run/sshd \
    && echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config \
    && echo "AuthorizedKeysFile /ssh/authorized_keys" >> /etc/ssh/sshd_config \
    && echo "PasswordAuthentication no" >> /etc/ssh/sshd_config \
    && echo "PermitRootLogin prohibit-password" >> /etc/ssh/sshd_config

# Harness directory for job scripts and logs
RUN mkdir -p /harness && chmod 1777 /harness

# Tailscale
RUN curl -fsSL https://tailscale.com/install.sh | sh

# Bake orchestrator public key into worker image (overridable via volume mount).
COPY authorized_keys /ssh/authorized_keys
RUN chmod 600 /ssh/authorized_keys

# Copy worker daemon
COPY worker_daemon.py /worker_daemon.py
COPY requirements.txt /requirements.txt
RUN pip3 install --break-system-packages -r /requirements.txt

WORKDIR /workspace

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]