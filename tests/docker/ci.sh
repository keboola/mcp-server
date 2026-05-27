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
    : "${STORAGE_API_TOKEN:?STORAGE_API_TOKEN is required}"
    : "${STORAGE_API_URL:?STORAGE_API_URL is required}"

    # Start container. No --workspace-schema: the smoke test exercises get_buckets, which is a
    # Storage-only tool and needs no workspace, so the server boots without one.
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
        response=$(curl -s -w "\n%{http_code}" -D "headers.txt" -X POST \
           -H "Content-Type: application/json" \
           -H "Accept: application/json, text/event-stream" \
           -d '{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "ci-docker-test", "version": "1.0.0"}}}' \
           "http://localhost:8080/mcp" 2>/dev/null) || true

        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')

        if [ "$http_code" = "200" ] && [ -n "$body" ]; then
            echo "✓ MCP server initialized successfully, session-less mode"

            response=$(curl -s -w "\n%{http_code}" -X POST \
               -H "Content-Type: application/json" \
               -H "Accept: application/json, text/event-stream" \
               -d '{"jsonrpc": "2.0", "method": "notifications/initialized"}' \
               "http://localhost:8080/mcp" 2>/dev/null) || true

            http_code=$(echo "$response" | tail -n1)
            body=$(echo "$response" | sed '$d')

            if [ "$http_code" = "202" ]; then
                echo "✓ MCP initialization confirmed"

                response=$(curl -s -w "\n%{http_code}" -X POST \
                   -H "Content-Type: application/json" \
                   -H "Accept: application/json, text/event-stream" \
                   -d '{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "get_buckets", "arguments": {}}}' \
                   "http://localhost:8080/mcp" 2>/dev/null) || true

                http_code=$(echo "$response" | tail -n1)
                body=$(echo "$response" | sed '$d')

                if [ "$http_code" = "200" ] && [ -n "$body" ]; then
                    # A successful tool call returns a JSON-RPC result without an error and
                    # without the MCP tool-level isError flag.
                    status=$(echo "$body" | grep "^data: " | head -1 | cut -c7- \
                        | jq -r 'if .error then "rpc_error" elif .result.isError == true then "tool_error" elif .result then "ok" else "unknown" end' 2>/dev/null || true)

                    if [ "$status" = "ok" ]; then
                        echo "✓ get_buckets tool call succeeded"
                        exit 0
                    else
                        echo "✗ get_buckets did not return a successful result ($status): $body"
                    fi
                fi
            fi
            # If tool call didn't succeed, continue outer loop
        fi
        sleep 1
    done

    echo "✗ Server failed to respond"
    docker logs "$CONTAINER_NAME" 2>&1 | tail -10
    exit 1
}

main "$@"
