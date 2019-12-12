FROM ubuntu:latest

# JSON RPC port
EXPOSE 10099/tcp
EXPOSE 8086/tcp

# Install libssl
RUN apt update && apt upgrade && \
    apt-get install -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install influxdb
RUN apt update && apt upgrade && \
    apt-get install influxdb && \
    apt-get install influxdb-client

COPY usr/bin /usr/bin/
ENTRYPOINT [ "/usr/bin/morgan-run.sh" ]
CMD [""]
