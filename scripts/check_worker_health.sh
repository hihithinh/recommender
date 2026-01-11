#!/bin/bash
# Health check script to verify Spark worker is actually registered with master
# This checks the WORKER'S perspective, not just network connectivity

WORKER_PORT=$1
MASTER_IP=$2

# Check 1: Worker web UI is responsive
if ! curl -f -s http://localhost:${WORKER_PORT} > /dev/null 2>&1; then
    echo "Worker web UI not responsive"
    exit 1
fi

# Check 2: Get worker ID from its own API
WORKER_ID=$(curl -s http://localhost:${WORKER_PORT}/json/ 2>/dev/null | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$WORKER_ID" ]; then
    echo "Cannot get worker ID"
    exit 1
fi

# Check 3: Verify this worker is registered on master
MASTER_WORKERS=$(curl -s http://${MASTER_IP}:8080/json/ 2>/dev/null | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if echo "$MASTER_WORKERS" | grep -q "$WORKER_ID"; then
    echo "Worker $WORKER_ID is registered with master"
    exit 0
else
    echo "Worker $WORKER_ID NOT registered with master"
    exit 1
fi
