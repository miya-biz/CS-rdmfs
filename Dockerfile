FROM python:3.8-buster

RUN apt-get update \
    && apt-get install -y fuse3 libfuse3-dev pkg-config git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY . /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
RUN pip3 install /tmp/

RUN cp /tmp/bin/start.sh / && chmod +x /start.sh

CMD /start.sh
