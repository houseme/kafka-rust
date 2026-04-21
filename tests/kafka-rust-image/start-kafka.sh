#!/bin/bash

if [[ -z "$KAFKA_PORT" ]]; then
    export KAFKA_PORT=9092
fi

# Determine major version for KRaft vs ZooKeeper mode
KAFKA_MAJOR="${KAFKA_VERSION%%.*}"

echo "Starting kafka server (version ${KAFKA_VERSION}, major ${KAFKA_MAJOR})"

create-topics.sh &

if [[ "$KAFKA_MAJOR" -ge 4 ]]; then
    # KRaft mode for Kafka 4.x+
    if [[ -n "$KAFKA_CLIENT_SECURE" ]]; then
        config_fname="$KAFKA_HOME/config/kraft-secure-server.properties"
    else
        config_fname="$KAFKA_HOME/config/kraft-server.properties"
    fi

    # Initialize KRaft storage if not already initialized
    if [[ ! -f /kafka/kraft-combined-logs/meta.properties ]]; then
        $KAFKA_HOME/bin/kafka-storage.sh random-uuid > /tmp/cluster-id
        $KAFKA_HOME/bin/kafka-storage.sh format \
            --config "$config_fname" \
            --cluster-id "$(cat /tmp/cluster-id)" \
            --ignore-formatted
    fi

    set -x
    exec $KAFKA_HOME/bin/kafka-server-start.sh "$config_fname"
else
    # ZooKeeper mode for Kafka 3.x
    if [[ -n "$KAFKA_CLIENT_SECURE" ]]; then
        config_fname="$KAFKA_HOME/config/secure.server.properties"
    else
        config_fname="$KAFKA_HOME/config/server.properties"
    fi

    set -x
    exec $KAFKA_HOME/bin/kafka-server-start.sh "$config_fname"
fi
