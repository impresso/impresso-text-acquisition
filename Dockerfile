# Set base image
FROM daskdev/dask:2024.9.0-py3.11

# Set environment variables for user
ENV GROUP_NAME=DHLAB-unit
ENV GROUP_ID=11703

ARG USER_NAME
ARG USER_ID

# pip falls back to a --user install under ~/.local when it can't write to
# the conda env's site-packages (e.g. root is squashed on the /rcp-scratch
# PVC, so any pip install run against that mount must be non-root and lands
# here). Put its bin dir on PATH so those console scripts are runnable.
ENV PATH="/home/${USER_NAME}/.local/bin:${PATH}"

# Install build tools and libraries
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    pkg-config \
    cmake \
    software-properties-common \
    jq

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y \
    apt-utils \
    git  \
    curl  \
    vim  \
    unzip  \
    wget  \
    tmux  \
    screen  \
    wget \
    sudo \
    openssh-client

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a group and user
RUN groupadd -g $GROUP_ID $GROUP_NAME
RUN useradd -ms /bin/bash -u $USER_ID -g $GROUP_ID $USER_NAME

# Add new user to sudoers
RUN echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# install desired libraries. 
RUN pip install --upgrade pip setuptools
RUN pip install --upgrade pip impresso-essentials

EXPOSE 8080
EXPOSE 8785
EXPOSE 8786
EXPOSE 8787

# Set the working directory
WORKDIR /home/$USER_NAME/impresso-text-acquisition

# Add local impresso_text_acquisition
COPY . .

# Change ownership of the copied files to the new user and group
RUN chown -R ${USER_NAME}:${GROUP_NAME} /home/${USER_NAME}/impresso-text-acquisition

# Install as root so it lands in the conda env's site-packages (writable,
# on PATH) instead of falling back to a per-user install under ~/.local.
RUN pip install .

# Switch to the new user
USER $USER_NAME

# sleep for 3 days (to sensure I have the time to check the run)
CMD ["sleep", "259200"]
