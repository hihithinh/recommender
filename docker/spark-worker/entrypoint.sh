#!/bin/bash
# Auto-detect Tailscale IP for SPARK_LOCAL_IP
# This script automatically finds the machine's Tailscale IP instead of manual configuration

set -e

# Try to detect Tailscale IP automatically
# Method 1: Use tailscale command if available
if command -v tailscale &> /dev/null; then
    DETECTED_IP=$(tailscale ip -4 2>/dev/null | head -1)
    if [ -n "$DETECTED_IP" ]; then
        export SPARK_LOCAL_IP="$DETECTED_IP"
        echo "Auto-detected Tailscale IP: $SPARK_LOCAL_IP"
    fi
fi

# Method 2: Try to detect from network interfaces (Tailscale IPs are in 100.x.x.x range)
if [ -z "$SPARK_LOCAL_IP" ]; then
    DETECTED_IP=$(ip -4 addr show 2>/dev/null | grep -oP '(?<=inet\s)100\.\d+\.\d+\.\d+' | head -1)
    if [ -n "$DETECTED_IP" ]; then
        export SPARK_LOCAL_IP="$DETECTED_IP"
        echo "Auto-detected Tailscale IP from interface: $SPARK_LOCAL_IP"
    fi
fi

# If no Tailscale interface found, worker is on same machine as master
# DON'T set SPARK_LOCAL_IP - let worker use its Docker IP
# This avoids bind errors when trying to bind to an IP that doesn't exist in the container
if [ -z "$SPARK_LOCAL_IP" ]; then
    echo "No Tailscale interface found. Worker on same machine as master."
    echo "Using default Docker IP for local communication."
fi

# Start Spark worker with correct command for apache/spark image
# Note: SPARK_LOCAL_IP is used for advertising to master, but worker binds to 0.0.0.0
exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker ${SPARK_MASTER_URL}
