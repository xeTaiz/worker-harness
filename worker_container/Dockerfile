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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# SSH server setup
RUN ssh-keygen -A \
    && mkdir -p /run/sshd \
    && echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config \
    && echo "AuthorizedKeysFile /ssh/authorized_keys" >> /etc/ssh/sshd_config \
    && echo "PasswordAuthentication no" >> /etc/ssh/sshd_config \
    && echo "PermitRootLogin no" >> /etc/ssh/sshd_config

# Copy ZeroTier install script
RUN curl -s https://install.zerotier.com | bash

# Copy SSH public key for authorized access
COPY authorized_keys /ssh/authorized_keys
RUN chmod 600 /ssh/authorized_keys

# Copy worker daemon
COPY worker_daemon.py /worker_daemon.py
COPY requirements.txt /requirements.txt
RUN pip3 install --break-system-packages -r /requirements.txt

# Working directory
WORKDIR /workspace

# Entrypoint handles ZeroTier bootstrap, SSH, and daemon start
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
