FROM ubuntu:22.04

RUN apt-get update && apt-get install -y wget jq coreutils openjdk-17-jre-headless

ARG kafka_version
ARG scala_version

ENV KAFKA_VERSION=$kafka_version SCALA_VERSION=$scala_version

COPY preloaded/ /tmp/kafka-preloaded/
RUN set -eux; \
    KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"; \
    LOCAL_TGZ="/tmp/kafka-preloaded/${KAFKA_ARCHIVE}"; \
    REMOTE_TGZ="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"; \
    if [ -f "${LOCAL_TGZ}" ]; then \
        echo "Using preloaded Kafka archive: ${LOCAL_TGZ}"; \
        tar xzf "${LOCAL_TGZ}" -C /opt; \
    else \
        echo "Preloaded archive not found, downloading from: ${REMOTE_TGZ}"; \
        wget -q "${REMOTE_TGZ}" -O - | tar xz -C /opt; \
    fi; \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

RUN mkdir -p /kafka/logs /kafka/kraft-combined-logs

ENV KAFKA_HOME /opt/kafka
ENV PATH ${PATH}:${KAFKA_HOME}/bin
ENV CLUSTER_ID=MkU3OEVCNTcwNTJENDM2Qk

COPY start-kafka.sh /usr/bin/start-kafka.sh
COPY create-topics.sh /usr/bin/create-topics.sh
RUN chmod +x /usr/bin/start-kafka.sh /usr/bin/create-topics.sh

ENV KAFKA_ZOOKEEPER_CONNECT zookeeper
COPY server.properties secure.server.properties client-ssl.properties kraft-server.properties kraft-secure-server.properties /opt/kafka/config/

# set up an SSL certificate for secure mode
RUN keytool \
        -keystore server.keystore.jks \
        -alias localhost \
        -validity 1 \
        -genkeypair \
        -keyalg RSA \
        -dname "CN=Unknown, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" \
        -storepass xxxxxx \
        -keypass xxxxxx

RUN keytool -exportcert -alias localhost -keystore /server.keystore.jks -file server-cert.pem -storepass xxxxxx -noprompt
RUN keytool -importcert -file server-cert.pem -alias server-cert -keystore client.truststore.jks -storepass xxxxxx -noprompt

EXPOSE 9092

# Use "exec" form so that it runs as PID 1 (useful for graceful shutdown)
CMD ["/usr/bin/start-kafka.sh"]
