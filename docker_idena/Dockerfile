FROM debian:buster-slim

ENV IDENA_VERSION 0.31.0

WORKDIR /idena
ADD idena-conf.json /idena

RUN apt-get update && apt-get install -y curl
ADD https://github.com/idena-network/idena-go/releases/download/v${IDENA_VERSION}/idena-node-linux-${IDENA_VERSION} /idena-go
RUN chmod +x /idena-go

RUN adduser -u 6789 --disabled-password --gecos "" idenauser && chown -R idenauser /idena
USER idenauser

HEALTHCHECK --interval=15s --timeout=10s --start-period=30s --retries=3 CMD [ "/usr/bin/curl", "-f", "http://bna_idena:9009" ]

CMD [ "/idena-go", "--config", "/idena/idena-conf.json" ]
