#!/bin/bash
set -e
cd "$(dirname "$0")/.."

echo '$ docker run -p 9100:9100 ray-agent-sre --interval 2'
docker rm -f ray-agent-sre-demo >/dev/null 2>&1 || true
docker run -d --rm -p 9100:9100 --name ray-agent-sre-demo ray-agent-sre:latest --port 9100 --interval 2 >/dev/null

echo "waiting for the exporter to start a local Ray instance..."
sleep 14

echo
echo '$ curl -s localhost:9100/metrics | grep -E "^ray_(cluster|node_up)"'
curl -s localhost:9100/metrics | grep -E "^ray_(cluster|node_up)"

echo
echo "waiting for more refresh cycles..."
sleep 20

echo
echo '$ curl -s localhost:9100/metrics | grep -E "^langgraph_node_(runs|errors|retries)_total\{"'
curl -s localhost:9100/metrics | grep -E "^langgraph_node_(runs|errors|retries)_total\{"

docker stop ray-agent-sre-demo >/dev/null 2>&1
echo
echo "container stopped."
