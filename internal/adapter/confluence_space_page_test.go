package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openwebui-content-sync/internal/config"
)

// TestConfluenceSpaceKeySyncV1 测试 API v1 使用 spaceKey 同步
func TestConfluenceSpaceKeySyncV1(t *testing.T) {
	// 创建模拟的 Confluence 服务器
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		query := r.URL.RawQuery

		t.Logf("Request: %s %s?%s", r.Method, path, query)

		switch {
		// Space API - 获取 space ID
		case path == "/rest/api/space/~zhangxiaoming":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":   104857614,
				"key":  "~zhangxiaoming",
				"name": "Xiaoming Zhang",
			})

		// Pages API v1 - 使用 spaceKey (不是 space ID)
		case path == "/rest/api/content":
			// 验证使用的是 spaceKey 而不是数字 ID
			spaceKey := r.URL.Query().Get("spaceKey")
			if spaceKey == "~zhangxiaoming" {
				// 正确：使用的是原始 space key
				json.NewEncoder(w).Encode(map[string]interface{}{
					"results": []map[string]interface{}{
						{
							"id":    "123456",
							"title": "Test Page",
							"type":  "page",
							"space": map[string]interface{}{
								"id":  104857614,
								"key": "~zhangxiaoming",
							},
							"version": map[string]interface{}{
								"number": 1,
							},
						},
					},
				})
			} else if spaceKey == "104857614" {
				// 错误：使用了数字 ID 作为 spaceKey
				t.Errorf("API v1 should use spaceKey '~zhangxiaoming', not numeric ID '104857614'")
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"message": "No space exists with key : 104857614",
				})
			}

		// Page content API
		case path == "/rest/api/content/123456":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    "123456",
				"title": "Test Page",
				"body": map[string]interface{}{
					"storage": map[string]interface{}{
						"value": "<p>Test content</p>",
					},
				},
			})

		default:
			t.Logf("Unhandled request: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// 测试 API v1
	cfg := config.ConfluenceConfig{
		BaseURL:    server.URL,
		Username:   "test",
		APIKey:     "test",
		APIVersion: "v1",
		SpaceMappings: []config.SpaceMapping{
			{SpaceKey: "~zhangxiaoming", KnowledgeID: "test-knowledge"},
		},
	}

	adapter, err := NewConfluenceAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	files, err := adapter.FetchFiles(context.Background())
	if err != nil {
		t.Fatalf("FetchFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected files to be fetched, got none")
	}

	// 验证使用了正确的 spaceKey
	t.Logf("Successfully fetched %d files using spaceKey '~zhangxiaoming'", len(files))
}

// TestConfluenceSpaceKeyWithSpecialChars 测试特殊字符 space key (如 ~username)
func TestConfluenceSpaceKeyWithSpecialChars(t *testing.T) {
	var capturedSpaceKey string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		query := r.URL.Query()

		switch {
		// Space API
		case path == "/rest/api/space/~zhangxiaoming":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":   104857614,
				"key":  "~zhangxiaoming",
				"name": "Xiaoming Zhang",
			})

		// Pages API - 捕获使用的 spaceKey
		case path == "/rest/api/content":
			capturedSpaceKey = query.Get("spaceKey")
			t.Logf("Received spaceKey parameter: %s", capturedSpaceKey)

			// 验证 spaceKey 应该是 ~zhangxiaoming 而不是 104857614
			if capturedSpaceKey == "~zhangxiaoming" {
				// 正确：使用的是原始 space key
				json.NewEncoder(w).Encode(map[string]interface{}{
					"results": []map[string]interface{}{
						{
							"id":    "123456",
							"title": "Test Page",
							"type":  "page",
							"space": map[string]interface{}{
								"id":  104857614,
								"key": "~zhangxiaoming",
							},
							"version": map[string]interface{}{
								"number": 1,
							},
						},
					},
				})
			} else if capturedSpaceKey == "104857614" {
				// 错误：使用了数字 ID 作为 spaceKey，应该返回 404
				t.Errorf("API v1 should use spaceKey '~zhangxiaoming', not numeric ID '104857614'")
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"message": "No space exists with key : 104857614",
				})
			}

		// Page content API
		case path == "/rest/api/content/123456":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    "123456",
				"title": "Test Page",
				"body": map[string]interface{}{
					"storage": map[string]interface{}{
						"value": "<p>Test content</p>",
					},
				},
			})

		default:
			t.Logf("Unhandled request: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// 测试 API v1
	cfg := config.ConfluenceConfig{
		BaseURL:    server.URL,
		Username:   "test",
		APIKey:     "test",
		APIVersion: "v1",
		SpaceMappings: []config.SpaceMapping{
			{SpaceKey: "~zhangxiaoming", KnowledgeID: "test-knowledge"},
		},
	}

	adapter, err := NewConfluenceAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	files, err := adapter.FetchFiles(context.Background())
	if err != nil {
		t.Fatalf("FetchFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected files to be fetched, got none")
	}

	// 验证使用了正确的 spaceKey
	if capturedSpaceKey != "~zhangxiaoming" {
		t.Errorf("Expected spaceKey to be '~zhangxiaoming', got '%s'", capturedSpaceKey)
	}
}

// TestConfluenceSpaceKeySyncV2 测试 API v2 使用 space ID 同步
func TestConfluenceSpaceKeySyncV2(t *testing.T) {
	var capturedSpaceID string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		t.Logf("Request: %s %s", r.Method, path)

		switch {
		// Space API v2 - 获取 space ID
		case path == "/api/v2/spaces":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{
						"id":   "104857614",
						"name": "Xiaoming Zhang",
						"key":  "~zhangxiaoming",
					},
				},
			})

		// Pages API v2 - 使用 space ID
		case path == "/api/v2/spaces/104857614/pages":
			capturedSpaceID = "104857614"
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{
						"id":      "123456",
						"title":   "Test Page",
						"status":  "current",
						"spaceId": "104857614",
					},
				},
			})

		// Page content API v2
		case path == "/api/v2/pages/123456":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    "123456",
				"title": "Test Page",
				"body": map[string]interface{}{
					"storage": map[string]interface{}{
						"value": "<p>Test content</p>",
					},
				},
			})

		default:
			t.Logf("Unhandled request: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := config.ConfluenceConfig{
		BaseURL:    server.URL,
		Username:   "test",
		APIKey:     "test",
		APIVersion: "v2",
		SpaceMappings: []config.SpaceMapping{
			{SpaceKey: "~zhangxiaoming", KnowledgeID: "test-knowledge"},
		},
	}

	adapter, err := NewConfluenceAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	files, err := adapter.FetchFiles(context.Background())
	if err != nil {
		t.Fatalf("FetchFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected files to be fetched, got none")
	}

	// 验证 API v2 使用了正确的 space ID
	if capturedSpaceID != "104857614" {
		t.Errorf("Expected spaceID to be '104857614', got '%s'", capturedSpaceID)
	}
}

// TestConfluencePageIDSync 测试 Page ID 同步功能
func TestConfluencePageIDSync(t *testing.T) {
	var requestLog []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		requestLog = append(requestLog, fmt.Sprintf("%s %s", r.Method, path))
		t.Logf("Request: %s %s", r.Method, path)

		switch {
		// Page API v1 - 获取页面
		case path == "/rest/api/content/789012":
			query := r.URL.Query()
			if query.Get("expand") == "body.storage" {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"id":    "789012",
					"title": "Parent Page",
					"body": map[string]interface{}{
						"storage": map[string]interface{}{
							"value": "<p>Parent content</p>",
						},
					},
				})
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"id":    "789012",
					"title": "Parent Page",
					"type":  "page",
					"space": map[string]interface{}{
						"id":  104857614,
						"key": "~zhangxiaoming",
					},
				})
			}

		// Child pages
		case path == "/rest/api/content/789012/child/page":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{
						"id":    "111111",
						"title": "Child Page 1",
						"type":  "page",
						"space": map[string]interface{}{
							"id":  104857614,
							"key": "~zhangxiaoming",
						},
					},
				},
			})

		default:
			t.Logf("Unhandled request: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := config.ConfluenceConfig{
		BaseURL:    server.URL,
		Username:   "test",
		APIKey:     "test",
		APIVersion: "v1",
		ParentPageMappings: []config.ParentPageMapping{
			{ParentPageID: "789012", KnowledgeID: "test-knowledge"},
		},
	}

	adapter, err := NewConfluenceAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	files, err := adapter.FetchFiles(context.Background())
	if err != nil {
		t.Fatalf("FetchFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected files to be fetched, got none")
	}

	// 验证请求顺序
	t.Log("Request log:")
	for _, req := range requestLog {
		t.Logf("  - %s", req)
	}
}

// TestConfluenceMixedSync 测试混合模式（Space + Page）同步
func TestConfluenceMixedSync(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		t.Logf("Request: %s %s", r.Method, path)

		switch {
		// Space API
		case path == "/rest/api/space/SPACE":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":   999999,
				"key":  "SPACE",
				"name": "Test Space",
			})

		// Space pages
		case path == "/rest/api/content":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{
						"id":    "333333",
						"title": "Space Page",
						"type":  "page",
						"space": map[string]interface{}{
							"id":  999999,
							"key": "SPACE",
						},
					},
				},
			})

		// Page content
		case path == "/rest/api/content/333333":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    "333333",
				"title": "Space Page",
				"body": map[string]interface{}{
					"storage": map[string]interface{}{
						"value": "<p>Space page content</p>",
					},
				},
			})

		// Parent page
		case path == "/rest/api/content/444444":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    "444444",
				"title": "Parent Page",
				"type":  "page",
				"space": map[string]interface{}{
					"id":  999999,
					"key": "SPACE",
				},
			})

		// Child pages
		case path == "/rest/api/content/444444/child/page":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []map[string]interface{}{
					{
						"id":    "555555",
						"title": "Child Page",
						"type":  "page",
						"space": map[string]interface{}{
							"id":  999999,
							"key": "SPACE",
						},
					},
				},
			})

		default:
			t.Logf("Unhandled request: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := config.ConfluenceConfig{
		BaseURL:    server.URL,
		Username:   "test",
		APIKey:     "test",
		APIVersion: "v1",
		SpaceMappings: []config.SpaceMapping{
			{SpaceKey: "SPACE", KnowledgeID: "space-knowledge"},
		},
		ParentPageMappings: []config.ParentPageMapping{
			{ParentPageID: "444444", KnowledgeID: "page-knowledge"},
		},
	}

	adapter, err := NewConfluenceAdapter(cfg)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	files, err := adapter.FetchFiles(context.Background())
	if err != nil {
		t.Fatalf("FetchFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected files to be fetched, got none")
	}

	t.Logf("Fetched %d files:", len(files))
	for _, f := range files {
		t.Logf("  - %s (KnowledgeID: %s)", f.Path, f.KnowledgeID)
	}
}
