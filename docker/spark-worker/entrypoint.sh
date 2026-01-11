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

# Method 2: If tailscale command not available, try to detect from network interfaces
# Tailscale IPs are typically in 100.x.x.x range
if [ -z "$SPARK_LOCAL_IP" ]; then
    DETECTED_IP=$(ip -4 addr show | grep -oP '(?<=inet\s)100\.\d+\.\d+\.\d+' | head -1)
    if [ -n "$DETECTED_IP" ]; then
        export SPARK_LOCAL_IP="$DETECTED_IP"
        echo "Auto-detected IP from interface: $SPARK_LOCAL_IP"
    fi
fi

# Method 3: Fallback to resolving hostname to IP
if [ -z "$SPARK_LOCAL_IP" ]; then
    # Get the IP that can reach the master
    DETECTED_IP=$(getent hosts $(hostname) | awk '{ print $1 }' | grep -v '^127\.' | head -1)
    if [ -n "$DETECTED_IP" ]; then
        export SPARK_LOCAL_IP="$DETECTED_IP"
        echo "Auto-detected IP from hostname: $SPARK_LOCAL_IP"
    fi
fi

# Method 4: Last resort - use the IP that can reach master
if [ -z "$SPARK_LOCAL_IP" ] && [ -n "$MASTER_TAILSCALE_IP" ]; then
    # Get the local IP that would be used to reach master
    DETECTED_IP=$(ip route get $MASTER_TAILSCALE_IP 2>/dev/null | grep -oP 'src \K\S+' | head -1)
    if [ -n "$DETECTED_IP" ]; then
        export SPARK_LOCAL_IP="$DETECTED_IP"
        echo "Auto-detected IP from route to master: $SPARK_LOCAL_IP"
    fi
fi

# If still not set, warn but continue
if [ -z "$SPARK_LOCAL_IP" ]; then
    echo "WARNING: Could not auto-detect Tailscale IP. Executors may advertise Docker internal IP."
    echo "Set SPARK_LOCAL_IP environment variable manually if you encounter connection issues."
fi

# Start Spark worker with original entrypoint
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@"
