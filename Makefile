CXX      ?= c++
CXXFLAGS ?= -O2 -g
CXXFLAGS += -std=c++17 -Wall -Wextra -Wpedantic -Isrc -pthread
LDFLAGS  += -pthread

BUILD_DIR := build
COMMON    := src/protocol.cpp src/protocol.h
SERVER    := $(BUILD_DIR)/chat_server
CLIENT    := $(BUILD_DIR)/chat_client

.PHONY: all clean test

all: $(SERVER) $(CLIENT)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SERVER): src/server.cpp $(COMMON) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ src/server.cpp src/protocol.cpp $(LDFLAGS)

$(CLIENT): src/client.cpp $(COMMON) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ src/client.cpp src/protocol.cpp $(LDFLAGS)

test: all
	python3 tools/load_test.py --spawn-server --port 19000

clean:
	rm -rf $(BUILD_DIR)
