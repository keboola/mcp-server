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
    : "${WORKSPACE_SCHEMA:?WORKSPACE_SCHEMA is required}"

    # Start container
    echo "Starting container..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        -p "8080:8000" \
        "$IMAGE_NAME" \
        --transport http-compat \
        --api-url "$STORAGE_API_URL" \
        --storage-token "$STORAGE_API_TOKEN" \
        --workspace-schema "$WORKSPACE_SCHEMA" \
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
                   -d '{"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "get_project_info", "arguments": {}}}' \
                   "http://localhost:8080/mcp" 2>/dev/null) || true

                http_code=$(echo "$response" | tail -n1)
                body=$(echo "$response" | sed '$d')

                if [ "$http_code" = "200" ] && [ -n "$body" ]; then
                    echo "✓ Tool call succeeded"

                    project_id=$(echo "$body" | grep "^data: " | head -1 | cut -c7- \
                        | jq -r '.result.content[0].text | fromjson | .project_id' 2>/dev/null || true)
                    token_project_id=$(echo "$STORAGE_API_TOKEN" | cut -d- -f1)

                    if [ -n "$project_id" ] && [ "$project_id" = "$token_project_id" ]; then
                        echo "✓ Project ID test passed"
                        exit 0
                    else
                        echo "✗ Wrong project ID returned, expecting $token_project_id, got $project_id"
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