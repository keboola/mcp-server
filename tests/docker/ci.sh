#!/usr/bin/env bash
set -Eeuo pipefail

CONTAINER_NAME="keboola-mcp-server-test-docker"
IMAGE_NAME="keboola/mcp-server:ci"

cleanup() {
    docker stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
    docker rm "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

main() {
    # Start container
    echo "Starting container..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        -p "8080:8000" \
        "$IMAGE_NAME" \
        --transport http-compat \
        --api-url "$STORAGE_API_URL" \
        --storage-token "$STORAGE_API_TOKEN" \
        --host "0.0.0.0" \
        --port 8000 >/dev/null
    
    # Give server time to start
    sleep 5
    
    # Wait and test MCP initialize
    echo "Testing MCP initialize..."
    for i in $(seq 1 30); do
        response=$(curl -s -w "\n%{http_code}" -X POST \
           -H "Content-Type: application/json" \
           -H "Accept: application/json, text/event-stream" \
           -d '{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "ci-docker-test", "version": "1.0.0"}}}' \
           "http://localhost:8080/mcp" 2>/dev/null)
        
        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')
        
        if [ "$http_code" = "200" ] && [ -n "$body" ]; then
            echo "✓ MCP server initialized successfully"
            echo "Response body:"
            if command -v jq >/dev/null 2>&1; then
                echo "$body" | grep "^data: " | sed 's/^data: //' | jq '.'
            else
                echo "$body"
            fi
            echo "✓ Test passed"
            exit 0
        fi
        sleep 1
    done
    
    echo "✗ Server failed to respond"
    docker logs "$CONTAINER_NAME" 2>&1 | tail -10
    exit 1
}

main "$@"