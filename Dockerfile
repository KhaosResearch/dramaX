FROM debian:bullseye

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \ 
    libssl-dev \
    zlib1g-dev \
    libncurses5-dev \
    libncursesw5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libgdbm-dev \
    libdb5.3-dev \
    libbz2-dev \
    libexpat1-dev \
    liblzma-dev \
    tk-dev \
    libffi-dev \
    wget \
    curl

RUN cd /usr/src && \
    wget https://www.python.org/ftp/python/3.10.2/Python-3.10.2.tgz && \
    tar xzf Python-3.10.2.tgz && \
    cd Python-3.10.2 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    update-alternatives --install /usr/bin/python python /usr/local/bin/python3.10 1 && \
    update-alternatives --install /usr/bin/pip pip /usr/local/bin/pip3.10 1


WORKDIR /code

COPY . .

RUN pip install --upgrade build

RUN python3.10 -m build

RUN python3.10 -m pip install --upgrade dist/*.whl

ENTRYPOINT [ "dramax" ]

CMD [ "server" ]
