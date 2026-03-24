# Confluence 适配器测试集
# 主要测试 SpaceKey 同步和 PageID 同步功能

$ErrorActionPreference = "Stop"

function Write-Header {
    param([string]$message)
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host $message -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

function Write-Success {
    param([string]$message)
    Write-Host "✓ $message" -ForegroundColor Green
}

function Write-Test {
    param([string]$number, [string]$description)
    Write-Host "$number. $description..." -ForegroundColor Yellow
}

Write-Header "Running Confluence Adapter Test Suite"

# Test 1
Write-Test "1" "Testing API v1 SpaceKey sync with special chars (~username)"
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeySyncV1" -timeout 60s
if ($LASTEXITCODE -ne 0) { throw "Test 1 failed" }
Write-Success "PASSED"
Write-Host ""

# Test 2
Write-Test "2" "Testing API v1 SpaceKey with special character handling"
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeyWithSpecialChars" -timeout 60s
if ($LASTEXITCODE -ne 0) { throw "Test 2 failed" }
Write-Success "PASSED"
Write-Host ""

# Test 3
Write-Test "3" "Testing API v2 SpaceKey sync (using space ID)"
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeySyncV2" -timeout 60s
if ($LASTEXITCODE -ne 0) { throw "Test 3 failed" }
Write-Success "PASSED"
Write-Host ""

# Test 4
Write-Test "4" "Testing PageID sync (parent page with child pages)"
go test ./internal/adapter/... -v -run "TestConfluencePageIDSync" -timeout 60s
if ($LASTEXITCODE -ne 0) { throw "Test 4 failed" }
Write-Success "PASSED"
Write-Host ""

# Test 5
Write-Test "5" "Testing mixed mode (Space + Page sync)"
go test ./internal/adapter/... -v -run "TestConfluenceMixedSync" -timeout 60s
if ($LASTEXITCODE -ne 0) { throw "Test 5 failed" }
Write-Success "PASSED"
Write-Host ""

Write-Header "All tests passed successfully!"
