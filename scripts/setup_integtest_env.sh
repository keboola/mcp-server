#!/bin/bash
# Setup script for integration test environment variables
# This script helps set up local environment variables for testing

set -e

echo "🔧 Integration Test Environment Setup"
echo "====================================="

# Function to prompt for input with default value
prompt_with_default() {
    local prompt="$1"
    local default="$2"
    local var_name="$3"
    
    if [ -n "$default" ]; then
        read -p "$prompt [$default]: " value
        value="${value:-$default}"
    else
        read -p "$prompt: " value
    fi
    
    echo "export $var_name=\"$value\"" >> .env.integtest
}

# Create or clear the environment file
echo "# Integration Test Environment Variables" > .env.integtest
echo "# Generated on $(date)" >> .env.integtest
echo "" >> .env.integtest

echo "📝 Setting up environment variables for integration tests..."
echo ""

# Environment 1
echo "🌍 Environment 1:"
prompt_with_default "Storage API URL" "https://connection.keboola.com" "INTEGTEST_STORAGE_API_URL_1"
prompt_with_default "Storage API Token" "" "INTEGTEST_STORAGE_TOKEN_1"
prompt_with_default "Workspace Schema" "test_schema_1" "INTEGTEST_WORKSPACE_SCHEMA_1"
echo ""

# Environment 2
echo "🌍 Environment 2:"
prompt_with_default "Storage API URL" "https://connection.keboola.com" "INTEGTEST_STORAGE_API_URL_2"
prompt_with_default "Storage API Token" "" "INTEGTEST_STORAGE_TOKEN_2"
prompt_with_default "Workspace Schema" "test_schema_2" "INTEGTEST_WORKSPACE_SCHEMA_2"
echo ""

# Environment 3
echo "🌍 Environment 3:"
prompt_with_default "Storage API URL" "https://connection.keboola.com" "INTEGTEST_STORAGE_API_URL_3"
prompt_with_default "Storage API Token" "" "INTEGTEST_STORAGE_TOKEN_3"
prompt_with_default "Workspace Schema" "test_schema_3" "INTEGTEST_WORKSPACE_SCHEMA_3"
echo ""

echo "✅ Environment file created: .env.integtest"
echo ""
echo "📋 To use these variables:"
echo "   source .env.integtest"
echo ""
echo "🔍 To validate the configuration:"
echo "   python scripts/validate_integtest_config.py"
echo ""
echo "⚠️  Note: This file contains sensitive tokens. Add .env.integtest to .gitignore"
echo "   and never commit it to version control."

# Check if .gitignore contains the file
if ! grep -q "\.env\.integtest" .gitignore 2>/dev/null; then
    echo ""
    echo "💡 Adding .env.integtest to .gitignore..."
    echo ".env.integtest" >> .gitignore
    echo "✅ Added to .gitignore"
fi 