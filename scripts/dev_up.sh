#!/bin/bash

# StreamIt Development Environment Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting StreamIt development environment...${NC}"

# Check if build directory exists
if [ ! -d "build" ]; then
    echo -e "${YELLOW}Build directory not found. Building project...${NC}"
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
    cmake --build build -j
fi

# Create necessary directories
mkdir -p logs/broker1 logs/broker2 logs/broker3
mkdir -p offsets
mkdir -p config

# Copy configuration files
cp deploy/local/broker1.yaml config/
cp deploy/local/controller1.yaml config/
cp deploy/local/coordinator1.yaml config/
cp deploy/local/topics.yaml config/

# Start services in background
echo -e "${GREEN}Starting services...${NC}"

# Start broker
echo -e "${YELLOW}Starting broker...${NC}"
./build/streamit_broker config/broker1.yaml &
BROKER_PID=$!

# Wait a moment for broker to start
sleep 2

# Start controller
echo -e "${YELLOW}Starting controller...${NC}"
./build/streamit_controller config/controller1.yaml &
CONTROLLER_PID=$!

# Wait a moment for controller to start
sleep 2

# Start coordinator
echo -e "${YELLOW}Starting coordinator...${NC}"
./build/streamit_coordinator config/coordinator1.yaml &
COORDINATOR_PID=$!

# Wait for all services to start
sleep 3

echo -e "${GREEN}All services started!${NC}"
echo -e "${GREEN}Broker PID: $BROKER_PID${NC}"
echo -e "${GREEN}Controller PID: $CONTROLLER_PID${NC}"
echo -e "${GREEN}Coordinator PID: $COORDINATOR_PID${NC}"
echo ""
echo -e "${GREEN}Services are running on:${NC}"
echo -e "  Broker: localhost:9092"
echo -e "  Controller: localhost:9093"
echo -e "  Coordinator: localhost:9094"
echo ""
echo -e "${GREEN}To stop all services, run: ./scripts/dev_down.sh${NC}"
echo -e "${GREEN}To test the system, run: ./scripts/test_system.sh${NC}"

# Save PIDs for cleanup
echo $BROKER_PID > .broker.pid
echo $CONTROLLER_PID > .controller.pid
echo $COORDINATOR_PID > .coordinator.pid

