package adapter

import (
	"context"
	"time"
)

// File represents a file from an external source
type File struct {
	Path            string    `json:"path"`
	Content         []byte    `json:"content"`
	Hash            string    `json:"hash"`
	Modified        time.Time `json:"modified"`
	Size            int64     `json:"size"`
	Source          string    `json:"source"`
	KnowledgeID     string    `json:"knowledge_id,omitempty"` // Optional: specific knowledge base ID for this file
	FallbackPath    string    `json:"fallback_path,omitempty"`
	FallbackContent []byte    `json:"fallback_content,omitempty"`
	// Confluence 特定字段
	ConfluenceVersion int    `json:"confluence_version,omitempty"` // Confluence 页面版本号
	PageID            string `json:"page_id,omitempty"`              // Confluence 页面 ID
}

// Adapter defines the interface for data source adapters
type Adapter interface {
	// Name returns the adapter name
	Name() string

	// FetchFiles retrieves files from the data source
	FetchFiles(ctx context.Context) ([]*File, error)

	// GetLastSync returns the last sync timestamp
	GetLastSync() time.Time

	// SetLastSync updates the last sync timestamp
	SetLastSync(t time.Time)
}
