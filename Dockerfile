# Set base image
FROM daskdev/dask:2023.11.0-py3.11

# Set environment variables for user
ENV GROUP_NAME=DHLAB-unit
ENV GROUP_ID=11703

ARG USER_NAME
ARG USER_ID

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
# TODO remove boto once it's removed from all functions.
RUN pip install --upgrade pip setuptools
RUN pip install numpy pillow beautifulsoup4 pandas PyYAML jsonlines pytest
RUN pip install \
    boto3 \
    docopt \
    opencv-python \
    smart-open \
    git-python \
    python-dotenv

EXPOSE 8080
EXPOSE 8785
EXPOSE 8786
EXPOSE 8787

# Set the working directory
WORKDIR /home/$USER_NAME/impresso-text-acquisition

# Add local impresso_pycommons
COPY . .

# Change ownership of the copied files to the new user and group
RUN chown -R ${USER_NAME}:${GROUP_NAME} /home/${USER_NAME}/impresso-text-acquisition

# Switch to the new user
USER $USER_NAME

RUN pip install -e .

# Make sure the script launching the rebuilt is executable
RUN chmod -x /home/${USER_NAME}/impresso-text-acquisition/bash_scripts/start_rebuilt_runai.sh

CMD ["sleep", "infinity"]
