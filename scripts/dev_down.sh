#!/bin/bash

# StreamIt Development Environment Shutdown Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Stopping StreamIt development environment...${NC}"

# Stop services if PIDs exist
if [ -f ".broker.pid" ]; then
    BROKER_PID=$(cat .broker.pid)
    if kill -0 $BROKER_PID 2>/dev/null; then
        echo -e "${YELLOW}Stopping broker (PID: $BROKER_PID)...${NC}"
        kill $BROKER_PID
        rm .broker.pid
    fi
fi

if [ -f ".controller.pid" ]; then
    CONTROLLER_PID=$(cat .controller.pid)
    if kill -0 $CONTROLLER_PID 2>/dev/null; then
        echo -e "${YELLOW}Stopping controller (PID: $CONTROLLER_PID)...${NC}"
        kill $CONTROLLER_PID
        rm .controller.pid
    fi
fi

if [ -f ".coordinator.pid" ]; then
    COORDINATOR_PID=$(cat .coordinator.pid)
    if kill -0 $COORDINATOR_PID 2>/dev/null; then
        echo -e "${YELLOW}Stopping coordinator (PID: $COORDINATOR_PID)...${NC}"
        kill $COORDINATOR_PID
        rm .coordinator.pid
    fi
fi

# Wait for processes to stop
sleep 2

# Force kill if still running
pkill -f streamit_broker || true
pkill -f streamit_controller || true
pkill -f streamit_coordinator || true

echo -e "${GREEN}All services stopped!${NC}"

