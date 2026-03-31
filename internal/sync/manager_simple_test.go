package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/openwebui-content-sync/internal/adapter"
	"github.com/openwebui-content-sync/internal/config"
	"github.com/openwebui-content-sync/internal/mocks"
	"github.com/openwebui-content-sync/internal/openwebui"
)

func TestNewManager(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	openwebuiConfig := config.OpenWebUIConfig{
		BaseURL: "http://localhost:8080",
		APIKey:  "test-key",
	}
	storageConfig := config.StorageConfig{
		Path: tempDir,
	}

	manager, err := NewManager(openwebuiConfig, storageConfig)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	if manager == nil {
		t.Fatal("Expected manager to be created")
	}
	if manager.storagePath != tempDir {
		t.Errorf("Expected storage path %s, got %s", tempDir, manager.storagePath)
	}
}

func TestManager_SetKnowledgeID(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	manager := &Manager{
		storagePath: tempDir,
		fileIndex:   make(map[string]*FileMetadata),
	}

	knowledgeID := "test-knowledge-id"
	manager.SetKnowledgeID(knowledgeID)

	if manager.knowledgeID != knowledgeID {
		t.Errorf("Expected knowledge ID %s, got %s", knowledgeID, manager.knowledgeID)
	}
}

func TestManager_syncFile_NewFile(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	mockClient := &mocks.MockOpenWebUIClient{
		UploadFileFunc: func(ctx context.Context, filename string, content []byte) (*openwebui.File, error) {
			return &openwebui.File{
				ID:       "mock-file-id",
				Filename: filename,
			}, nil
		},
	}

	manager := &Manager{
		openwebuiClient: mockClient,
		storagePath:     tempDir,
		fileIndex:       make(map[string]*FileMetadata),
	}

	file := &adapter.File{
		Path:     "new-file.md",
		Content:  []byte("# New File"),
		Hash:     "test-hash",
		Modified: time.Now(),
		Size:     10,
		Source:   "test",
	}

	ctx := context.Background()
	err := manager.syncFile(ctx, file, "test-source")
	if err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	// Check that file was added to index
	fileKey := "new-file.md" // Now using filename as key
	if _, exists := manager.fileIndex[fileKey]; !exists {
		t.Errorf("Expected file to be added to index")
	}

	// Check that file was saved locally
	expectedPath := filepath.Join(tempDir, "files", "test-source", "new-file.md")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file to be saved locally at %s", expectedPath)
	}
}

func TestManager_syncFile_UnchangedFile(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	mockClient := &mocks.MockOpenWebUIClient{}
	manager := &Manager{
		openwebuiClient: mockClient,
		storagePath:     tempDir,
		fileIndex:       make(map[string]*FileMetadata),
	}

	// Add file to index first
	fileKey := "unchanged-file.md" // Now using filename as key
	manager.fileIndex[fileKey] = &FileMetadata{
		Path:     "unchanged-file.md",
		Hash:     "same-hash",
		FileID:   "existing-file-id",
		Source:   "test-source",
		SyncedAt: time.Now(),
		Modified: time.Now(),
	}

	file := &adapter.File{
		Path:     "unchanged-file.md",
		Content:  []byte("# Unchanged File"),
		Hash:     "same-hash", // Same hash as in index
		Modified: time.Now(),
		Size:     17,
		Source:   "test",
	}

	ctx := context.Background()
	err := manager.syncFile(ctx, file, "test-source")
	if err != nil {
		t.Fatalf("Failed to sync file: %v", err)
	}

	// File should not be uploaded again (we can't easily test this without more complex mocking)
	// But we can verify the file index wasn't updated with a new file ID
	if manager.fileIndex[fileKey].FileID != "existing-file-id" {
		t.Errorf("Expected file ID to remain unchanged")
	}
}

func TestManager_saveFileLocally(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	manager := &Manager{
		storagePath: tempDir,
	}

	filePath := filepath.Join(tempDir, "test", "nested", "file.md")
	content := []byte("# Test Content")

	err := manager.saveFileLocally(filePath, content)
	if err != nil {
		t.Fatalf("Failed to save file locally: %v", err)
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("Expected file to exist at %s", filePath)
	}

	// Check content
	readContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(readContent) != string(content) {
		t.Errorf("Expected content %s, got %s", string(content), string(readContent))
	}
}

func TestGetFileHash(t *testing.T) {
	content := []byte("test content")
	// Calculate the actual expected hash
	expectedHash := "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"

	hash := GetFileHash(content)
	if hash != expectedHash {
		t.Errorf("Expected hash %s, got %s", expectedHash, hash)
	}
}

func TestManager_loadFileIndex(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	manager := &Manager{
		storagePath: tempDir,
		fileIndex:   make(map[string]*FileMetadata),
		indexPath:   filepath.Join(tempDir, "file_index.json"),
	}

	// Test loading non-existent index (should not error)
	err := manager.loadFileIndex()
	if err != nil {
		t.Fatalf("Failed to load non-existent index: %v", err)
	}

	// Create a test index file
	testIndex := map[string]*FileMetadata{
		"file.md": { // Now using filename as key
			Path:     "file.md",
			Hash:     "test-hash",
			FileID:   "test-file-id",
			Source:   "test",
			SyncedAt: time.Now(),
			Modified: time.Now(),
		},
	}

	// Save test index
	manager.fileIndex = testIndex
	err = manager.saveFileIndex()
	if err != nil {
		t.Fatalf("Failed to save test index: %v", err)
	}

	// Create new manager and load index
	newManager := &Manager{
		storagePath: tempDir,
		fileIndex:   make(map[string]*FileMetadata),
		indexPath:   filepath.Join(tempDir, "file_index.json"),
	}

	err = newManager.loadFileIndex()
	if err != nil {
		t.Fatalf("Failed to load index: %v", err)
	}

	if len(newManager.fileIndex) != 1 {
		t.Errorf("Expected 1 file in index, got %d", len(newManager.fileIndex))
	}

	fileKey := "file.md" // Now using filename as key
	if _, exists := newManager.fileIndex[fileKey]; !exists {
		t.Errorf("Expected file %s to be in index", fileKey)
	}
}

// TestManager_SyncFiles_MultiPageAddAndDelete
// 验证：同一次同步中多个页面被添加，
// 且之前知识库中存在、但这次已不存在的页面会被标记为孤儿并从知识库删除
func TestManager_SyncFiles_MultiPageAddAndDelete(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	var uploadCount int
	var addToKnowledgeCount int
	var removedFiles []string
	var deletedFiles []string

	mockClient := &mocks.MockOpenWebUIClient{
		UploadFileFunc: func(ctx context.Context, filename string, content []byte) (*openwebui.File, error) {
			uploadCount++
			return &openwebui.File{
				ID:       "uploaded-" + filename,
				Filename: filename,
			}, nil
		},
		ListKnowledgeFunc: func(ctx context.Context) ([]*openwebui.Knowledge, error) {
			// 对本测试不重要，返回空列表即可
			return []*openwebui.Knowledge{}, nil
		},
		AddFileToKnowledgeFunc: func(ctx context.Context, knowledgeID, fileID string) error {
			addToKnowledgeCount++
			return nil
		},
		RemoveFileFromKnowledgeFunc: func(ctx context.Context, knowledgeID, fileID string) error {
			removedFiles = append(removedFiles, fileID)
			return nil
		},
		DeleteFileFunc: func(ctx context.Context, fileID string) error {
			deletedFiles = append(deletedFiles, fileID)
			return nil
		},
	}

	manager := &Manager{
		openwebuiClient: mockClient,
		storagePath:     tempDir,
		indexPath:       filepath.Join(tempDir, "file_index.json"),
		fileIndex:       make(map[string]*FileMetadata),
		knowledgeID:     "kb-multi",
	}

	// 模拟知识库中已有的老页面（这次同步中被删除）
	manager.fileIndex["old-page.md"] = &FileMetadata{
		Path:        "old-page.md",
		Hash:        "old-hash",
		FileID:      "file-old",
		Source:      "openwebui", // 只有 Source=openwebui 的才会在 cleanup 中删除
		KnowledgeID: "kb-multi",
		SyncedAt:    time.Now().Add(-time.Hour),
		Modified:    time.Now().Add(-time.Hour),
	}

	// 适配器返回两个新页面（相当于新增/当前仍存在的页面树中的页面）
	mockAdapter := &mocks.MockAdapter{
		NameFunc: func() string { return "confluence" },
		FetchFilesFunc: func(ctx context.Context) ([]*adapter.File, error) {
			return []*adapter.File{
				{
					Path:        "page-1.md",
					Content:     []byte("# Page 1"),
					Hash:        "hash-1",
					Modified:    time.Now(),
					Size:        int64(len("# Page 1")),
					Source:      "confluence",
					KnowledgeID: "kb-multi",
				},
				{
					Path:        "page-2.md",
					Content:     []byte("# Page 2"),
					Hash:        "hash-2",
					Modified:    time.Now(),
					Size:        int64(len("# Page 2")),
					Source:      "confluence",
					KnowledgeID: "kb-multi",
				},
			}, nil
		},
	}

	ctx := context.Background()
	if err := manager.SyncFiles(ctx, []adapter.Adapter{mockAdapter}); err != nil {
		t.Fatalf("SyncFiles failed: %v", err)
	}

	// 两个新页面应各自上传并加入知识库
	if uploadCount != 2 {
		t.Errorf("Expected 2 uploads, got %d", uploadCount)
	}
	if addToKnowledgeCount != 2 {
		t.Errorf("Expected 2 AddFileToKnowledge calls, got %d", addToKnowledgeCount)
	}

	// 老页面应被识别为孤儿并从知识库移除
	if len(removedFiles) != 1 || removedFiles[0] != "file-old" {
		t.Errorf("Expected old file to be removed from knowledge once, got %v", removedFiles)
	}

	// 索引中不应再包含老页面
	if _, exists := manager.fileIndex["old-page.md"]; exists {
		t.Errorf("Expected old-page.md to be removed from index")
	}
}

// TestManager_SyncFiles_SinglePageContentUpdate
// 验证：同一个页面内容发生变化时，会更新知识库中的文件
func TestManager_SyncFiles_SinglePageContentUpdate(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	var uploadCount int
	var removedFileIDs []string
	var deletedFileIDs []string

	mockClient := &mocks.MockOpenWebUIClient{
		UploadFileFunc: func(ctx context.Context, filename string, content []byte) (*openwebui.File, error) {
			uploadCount++
			return &openwebui.File{
				ID:       "new-file-id",
				Filename: filename,
			}, nil
		},
		AddFileToKnowledgeFunc: func(ctx context.Context, knowledgeID, fileID string) error {
			return nil
		},
		RemoveFileFromKnowledgeFunc: func(ctx context.Context, knowledgeID, fileID string) error {
			removedFileIDs = append(removedFileIDs, fileID)
			return nil
		},
		DeleteFileFunc: func(ctx context.Context, fileID string) error {
			deletedFileIDs = append(deletedFileIDs, fileID)
			return nil
		},
	}

	manager := &Manager{
		openwebuiClient: mockClient,
		storagePath:     tempDir,
		indexPath:       filepath.Join(tempDir, "file_index.json"),
		fileIndex:       make(map[string]*FileMetadata),
		knowledgeID:     "kb-page",
	}

	// 预先模拟旧版本页面已在索引和知识库中
	manager.fileIndex["page.md"] = &FileMetadata{
		Path:        "page.md",
		Hash:        "old-hash",
		FileID:      "old-file-id",
		Source:      "test-source", // 非 openwebui，走 hash 对比逻辑
		KnowledgeID: "kb-page",
		SyncedAt:    time.Now().Add(-time.Hour),
		Modified:    time.Now().Add(-time.Hour),
	}

	// 适配器返回同一路径，但内容和 hash 已变化，相当于“改”
	mockAdapter := &mocks.MockAdapter{
		NameFunc: func() string { return "test-source" },
		FetchFilesFunc: func(ctx context.Context) ([]*adapter.File, error) {
			return []*adapter.File{
				{
					Path:        "page.md",
					Content:     []byte("# New Content"),
					Hash:        "new-hash",
					Modified:    time.Now(),
					Size:        int64(len("# New Content")),
					Source:      "test-source",
					KnowledgeID: "kb-page",
				},
			}, nil
		},
	}

	ctx := context.Background()
	if err := manager.SyncFiles(ctx, []adapter.Adapter{mockAdapter}); err != nil {
		t.Fatalf("SyncFiles failed: %v", err)
	}

	// 内容变化应触发一次新的上传
	if uploadCount != 1 {
		t.Errorf("Expected 1 upload for updated page, got %d", uploadCount)
	}

	// 旧文件应先从知识库移除并删除
	if len(removedFileIDs) != 1 || removedFileIDs[0] != "old-file-id" {
		t.Errorf("Expected old file to be removed from knowledge once, got %v", removedFileIDs)
	}
	if len(deletedFileIDs) != 1 || deletedFileIDs[0] != "old-file-id" {
		t.Errorf("Expected old file to be deleted once, got %v", deletedFileIDs)
	}

	// 索引中的 hash 和 FileID 应更新为新内容
	meta, exists := manager.fileIndex["page.md"]
	if !exists {
		t.Fatalf("Expected page.md to remain in index")
	}
	if meta.Hash != "new-hash" {
		t.Errorf("Expected hash to be updated to 'new-hash', got %s", meta.Hash)
	}
	if meta.FileID != "new-file-id" {
		t.Errorf("Expected FileID to be 'new-file-id', got %s", meta.FileID)
	}
}
