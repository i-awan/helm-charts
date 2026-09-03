#!/usr/bin/env bash
# Simple ACLs aren't a CFK custom resource — they're granted against the
# LIVE cluster via the kafka-acls CLI, same as vanilla Kafka. Run this
# once the cluster is up and reachable, from any pod/host with kafka-acls
# and the right credentials.
#
# Example: allow a producer principal to write `orders`, and a consumer
# group to read it. Adjust principals/topics/groups for your environment.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka.confluent.svc.cluster.local:9092}"
COMMAND_CONFIG="${COMMAND_CONFIG:-/tmp/admin-client.properties}"  # your admin auth config

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
  --add --allow-principal User:orders-producer \
  --operation WRITE --operation DESCRIBE \
  --topic orders

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
  --add --allow-principal User:orders-consumer \
  --operation READ --operation DESCRIBE \
  --topic orders

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config "$COMMAND_CONFIG" \
  --add --allow-principal User:orders-consumer \
  --operation READ \
  --group orders-consumer-group

echo "ACLs applied. List them with:"
echo "  kafka-acls --bootstrap-server $BOOTSTRAP --command-config $COMMAND_CONFIG --list"
