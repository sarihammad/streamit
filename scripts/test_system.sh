#!/bin/bash

# StreamIt System Test Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Testing StreamIt system...${NC}"

# Check if services are running
if ! pgrep -f streamit_broker > /dev/null; then
    echo -e "${RED}Broker is not running. Please start the system first with ./scripts/dev_up.sh${NC}"
    exit 1
fi

if ! pgrep -f streamit_controller > /dev/null; then
    echo -e "${RED}Controller is not running. Please start the system first with ./scripts/dev_up.sh${NC}"
    exit 1
fi

if ! pgrep -f streamit_coordinator > /dev/null; then
    echo -e "${RED}Coordinator is not running. Please start the system first with ./scripts/dev_up.sh${NC}"
    exit 1
fi

echo -e "${GREEN}All services are running. Starting tests...${NC}"

# Test 1: Create a topic
echo -e "${YELLOW}Test 1: Creating topic 'test-topic'...${NC}"
if ./build/streamit_cli admin create-topic --topic test-topic --partitions 3 --replication-factor 1; then
    echo -e "${GREEN}✓ Topic created successfully${NC}"
else
    echo -e "${RED}✗ Failed to create topic${NC}"
    exit 1
fi

# Test 2: Describe the topic
echo -e "${YELLOW}Test 2: Describing topic 'test-topic'...${NC}"
if ./build/streamit_cli admin describe-topic --topic test-topic; then
    echo -e "${GREEN}✓ Topic described successfully${NC}"
else
    echo -e "${RED}✗ Failed to describe topic${NC}"
    exit 1
fi

# Test 3: Produce messages
echo -e "${YELLOW}Test 3: Producing messages...${NC}"
if timeout 10s ./build/streamit_cli produce --topic test-topic --partition 0 --rate 100 --size 100 --duration 5s; then
    echo -e "${GREEN}✓ Messages produced successfully${NC}"
else
    echo -e "${RED}✗ Failed to produce messages${NC}"
    exit 1
fi

# Test 4: Consume messages
echo -e "${YELLOW}Test 4: Consuming messages...${NC}"
if timeout 10s ./build/streamit_cli consume --topic test-topic --group test-group --from 0; then
    echo -e "${GREEN}✓ Messages consumed successfully${NC}"
else
    echo -e "${RED}✗ Failed to consume messages${NC}"
    exit 1
fi

# Test 5: Test consumer group
echo -e "${YELLOW}Test 5: Testing consumer group...${NC}"
if timeout 10s ./build/streamit_cli consume --topic test-topic --group test-group2 --from 0; then
    echo -e "${GREEN}✓ Consumer group test successful${NC}"
else
    echo -e "${RED}✗ Consumer group test failed${NC}"
    exit 1
fi

echo -e "${GREEN}All tests passed! StreamIt system is working correctly.${NC}"

