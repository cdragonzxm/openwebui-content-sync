# Confluence 适配器测试集

## 概述

本测试集专注于验证 Confluence 适配器的核心功能，特别是：

1. **SpaceKey 同步** - 基于 Space Key 获取页面
2. **PageID 同步** - 基于 Page ID 获取页面及其子页面
3. **API 版本兼容性** - 支持 Confluence API v1 和 v2

## 测试文件

### confluence_space_page_test.go

包含以下测试用例：

#### 1. TestConfluenceSpaceKeySyncV1
- **目的**: 测试 API v1 使用 SpaceKey 同步
- **关键点**: 
  - 验证使用原始 space key (如 `~zhangxiaoming`) 而非数字 ID
  - 确保 `spaceKey` 参数正确传递到 API

#### 2. TestConfluenceSpaceKeyWithSpecialChars
- **目的**: 测试特殊字符 space key 处理
- **关键点**:
  - 验证 `~username` 格式的 space key 能正确处理
  - 确保 URL 编码正确

#### 3. TestConfluenceSpaceKeySyncV2
- **目的**: 测试 API v2 使用 space ID 同步
- **关键点**:
  - 验证 API v2 使用数字 space ID
  - 确认 `/api/v2/spaces/{spaceID}/pages` 端点调用

#### 4. TestConfluencePageIDSync
- **目的**: 测试 Page ID 同步功能
- **关键点**:
  - 验证父页面和子页面的递归获取
  - 测试页面内容获取和解析

#### 5. TestConfluenceMixedSync
- **目的**: 测试混合模式（Space + Page）同步
- **关键点**:
  - 同时配置 Space 和 Page 映射
  - 验证两者的文件都能正确获取

## 运行测试

### 方式一：直接运行所有测试
```bash
cd c:\Users\zhangxiaoming\Desktop\trae_test\openwebui-content-sync
go test ./internal/adapter/... -v -run "TestConfluence" -timeout 60s
```

### 方式二：使用 PowerShell 脚本
```powershell
cd c:\Users\zhangxiaoming\Desktop\trae_test\openwebui-content-sync
powershell -ExecutionPolicy Bypass -File test_confluence.ps1
```

### 方式三：运行单个测试
```bash
go test ./internal/adapter/... -v -run "TestConfluenceSpaceKeySyncV1" -timeout 60s
```

## 关键修复验证

本测试集验证以下关键修复：

### 1. SpaceKey 404 修复
- **问题**: API v1 使用数字 space ID 作为 `spaceKey` 参数导致 404
- **修复**: 使用原始 space key (如 `~zhangxiaoming`)
- **验证**: `TestConfluenceSpaceKeySyncV1` 和 `TestConfluenceSpaceKeyWithSpecialChars`

### 2. API 版本差异处理
- **API v1**: 使用 `spaceKey` 参数，值为原始 key
- **API v2**: 使用 `spaceID` 路径参数，值为数字 ID
- **验证**: `TestConfluenceSpaceKeySyncV1` 和 `TestConfluenceSpaceKeySyncV2`

## 注意事项

1. **测试覆盖率**: 本测试集专注于核心功能，实际使用时应结合其他测试
2. **Mock 服务器**: 测试使用 httptest 模拟 Confluence API，不依赖真实环境
3. **持续集成**: 建议在 CI/CD 流程中运行此测试集，确保代码变更不会破坏核心功能
