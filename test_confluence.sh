#!/bin/bash

# Confluence 适配器测试集
# 主要测试 SpaceKey 同步和 PageID 同步功能

set -e

echo "========================================"
echo "Running Confluence Adapter Test Suite"
echo "========================================"
echo ""

echo "1. Testing API v1 SpaceKey sync with special chars (~username)..."
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeySyncV1" -timeout 60s
echo "✓ PASSED"
echo ""

echo "2. Testing API v1 SpaceKey with special character handling..."
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeyWithSpecialChars" -timeout 60s
echo "✓ PASSED"
echo ""

echo "3. Testing API v2 SpaceKey sync (using space ID)..."
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeySyncV2" -timeout 60s
echo "✓ PASSED"
echo ""

echo "4. Testing PageID sync (parent page with child pages)..."
go test ./internal/adapter/... -v -run "TestConfluencePageIDSync" -timeout 60s
echo "✓ PASSED"
echo ""

echo "5. Testing mixed mode (Space + Page sync)..."
go test ./internal/adapter/... -v -run "TestConfluenceMixedSync" -timeout 60s
echo "✓ PASSED"
echo ""

echo "========================================"
echo "All tests passed successfully!"
echo "========================================"
