#!/bin/bash

if [[ -z "$START_TIMEOUT" ]]; then
    START_TIMEOUT=600
fi

if [[ -z "${KAFKA_ADMIN_PORT:-}" ]]; then
    KAFKA_ADMIN_PORT=9092
fi

if [[ -n "${KAFKA_CLIENT_SASL_USERNAME:-}" ]]; then
    SASL_USERNAME="$KAFKA_CLIENT_SASL_USERNAME"
else
    SASL_USERNAME="test"
fi
if [[ -n "${KAFKA_CLIENT_SASL_PASSWORD:-}" ]]; then
    SASL_PASSWORD="$KAFKA_CLIENT_SASL_PASSWORD"
else
    SASL_PASSWORD="test-pass"
fi

build_sasl_client_properties() {
    local mechanism="$1"
    local jaas_module="org.apache.kafka.common.security.scram.ScramLoginModule"
    if [[ "${mechanism^^}" == "PLAIN" ]]; then
        jaas_module="org.apache.kafka.common.security.plain.PlainLoginModule"
    fi
    cat > /tmp/client-sasl.properties <<EOF
security.protocol=SASL_SSL
ssl.truststore.location=/client.truststore.jks
ssl.truststore.password=xxxxxx
ssl.endpoint.identification.algorithm=
sasl.mechanism=${mechanism}
sasl.jaas.config=${jaas_module} required username="${SASL_USERNAME}" password="${SASL_PASSWORD}";
EOF
}

if [[ -n "$KAFKA_CLIENT_SECURE" && -n "${KAFKA_CLIENT_SASL_MECHANISM:-}" ]]; then
    build_sasl_client_properties "$KAFKA_CLIENT_SASL_MECHANISM"
    SECURE_PARAM_NAME=""
    SECURE_PARAM_VALUE=""
    TOPIC_BOOTSTRAP_PORT="$KAFKA_ADMIN_PORT"
elif [[ -n "$KAFKA_CLIENT_SECURE" ]]; then
    SECURE_PARAM_NAME="--command-config"
    SECURE_PARAM_VALUE="$KAFKA_HOME/config/client-ssl.properties"
    TOPIC_BOOTSTRAP_PORT="$KAFKA_PORT"
else
    SECURE_PARAM_NAME=""
    SECURE_PARAM_VALUE=""
    TOPIC_BOOTSTRAP_PORT="$KAFKA_PORT"
fi

if [[ -n "$KAFKA_CLIENT_SECURE" && "${KAFKA_CLIENT_SASL_MECHANISM:-}" == SCRAM-* ]]; then
    echo "Waiting for internal admin listener on ${KAFKA_ADMIN_PORT} to provision SCRAM user"
    start_timeout_exceeded=false
    count=0
    step=10
    while ! JMX_PORT='' "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "localhost:${KAFKA_ADMIN_PORT}" \
        --list >/dev/null 2>&1; do
        echo "waiting for kafka admin listener to be ready"
        sleep $step;
        count=$(expr $count + $step)
        if [ $count -gt $START_TIMEOUT ]; then
            start_timeout_exceeded=true
            break
        fi
    done

    if $start_timeout_exceeded; then
        echo "Not able to reach admin listener to provision SCRAM user (waited for $START_TIMEOUT sec)"
        exit 1
    fi

    JMX_PORT='' "$KAFKA_HOME/bin/kafka-configs.sh" \
        --bootstrap-server "localhost:${KAFKA_ADMIN_PORT}" \
        --alter \
        --add-config "SCRAM-SHA-256=[password=${SASL_PASSWORD}]" \
        --entity-type users \
        --entity-name "${SASL_USERNAME}" || true
    JMX_PORT='' "$KAFKA_HOME/bin/kafka-configs.sh" \
        --bootstrap-server "localhost:${KAFKA_ADMIN_PORT}" \
        --alter \
        --add-config "SCRAM-SHA-512=[password=${SASL_PASSWORD}]" \
        --entity-type users \
        --entity-name "${SASL_USERNAME}" || true
fi

start_timeout_exceeded=false
count=0
step=10
while ! JMX_PORT='' "$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "localhost:${TOPIC_BOOTSTRAP_PORT}" \
    $SECURE_PARAM_NAME $SECURE_PARAM_VALUE \
    --list >/dev/null 2>&1; do
    echo "waiting for kafka to be ready"
    sleep $step;
    count=$(expr $count + $step)
    if [ $count -gt $START_TIMEOUT ]; then
        start_timeout_exceeded=true
        break
    fi
done

if $start_timeout_exceeded; then
    echo "Not able to auto-create topic (waited for $START_TIMEOUT sec)"
    exit 1
fi

if [[ -n $KAFKA_CREATE_TOPICS ]]; then
    IFS=','; for topicToCreate in $KAFKA_CREATE_TOPICS; do
        echo "creating topics: $topicToCreate"
        IFS=':' read -a topicConfig <<< "$topicToCreate"
        if [ ${topicConfig[3]} ]; then
          JMX_PORT='' $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:$TOPIC_BOOTSTRAP_PORT $SECURE_PARAM_NAME $SECURE_PARAM_VALUE --replication-factor ${topicConfig[2]} --partitions ${topicConfig[1]} --topic "${topicConfig[0]}" --config cleanup.policy="${topicConfig[3]}" 
        else
          JMX_PORT='' $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server localhost:$TOPIC_BOOTSTRAP_PORT $SECURE_PARAM_NAME $SECURE_PARAM_VALUE --replication-factor ${topicConfig[2]} --partitions ${topicConfig[1]} --topic "${topicConfig[0]}" 
        fi
    done
fi
