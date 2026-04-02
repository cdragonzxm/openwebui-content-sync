package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	server := NewServer(8080)
	if server == nil {
		t.Fatal("Expected server to be created")
	}
	if server.server == nil {
		t.Fatal("Expected HTTP server to be created")
	}
	if server.server.Addr != "127.0.0.1:8080" {
		t.Errorf("Expected server address '127.0.0.1:8080', got '%s'", server.server.Addr)
	}
}

func TestServer_healthHandler(t *testing.T) {
	server := NewServer(8080)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	var response HealthResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response.Status)
	}
	if response.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", response.Version)
	}
	if response.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}
}

func TestServer_readyHandler(t *testing.T) {
	server := NewServer(8080)

	req := httptest.NewRequest("GET", "/ready", nil)
	w := httptest.NewRecorder()

	server.readyHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	var response HealthResponse
	err := json.NewDecoder(w.Body).Decode(&response)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Status != "ready" {
		t.Errorf("Expected status 'ready', got '%s'", response.Status)
	}
	if response.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", response.Version)
	}
	if response.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}
}

func TestServer_Start(t *testing.T) {
	server := NewServer(0) // Use port 0 for random port

	// Start server in goroutine
	go func() {
		err := server.Start()
		if err != nil && err != http.ErrServerClosed {
			t.Errorf("Server start error: %v", err)
		}
	}()

	// Wait for server to be ready
	var resp *http.Response
	var err error
	for i := 0; i < 50; i++ {
		if server.server.Addr != "127.0.0.1:0" {
			resp, err = http.Get("http://" + server.server.Addr + "/health")
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					resp.Body.Close()
					break
				}
				resp.Body.Close()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Failed to make health request: %v", err)
	}
	if resp == nil {
		t.Fatal("Failed to make health request (no response)")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	resp, err = http.Get("http://" + server.server.Addr + "/ready")
	if err != nil {
		t.Fatalf("Failed to make ready request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	server.Stop(ctx)
}

func TestServer_Stop(t *testing.T) {
	server := NewServer(0)

	// Start server in goroutine
	go func() {
		server.Start()
	}()

	// Give server time to start
	time.Sleep(10 * time.Millisecond)

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("Failed to stop server: %v", err)
	}
}

func TestHealthResponse_JSON(t *testing.T) {
	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		Version:   "1.0.0",
	}

	// Test JSON marshaling
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("Failed to marshal response: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaled HealthResponse
	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if unmarshaled.Status != response.Status {
		t.Errorf("Expected status %s, got %s", response.Status, unmarshaled.Status)
	}
	if unmarshaled.Version != response.Version {
		t.Errorf("Expected version %s, got %s", response.Version, unmarshaled.Version)
	}
	if !unmarshaled.Timestamp.Equal(response.Timestamp) {
		t.Errorf("Expected timestamp %v, got %v", response.Timestamp, unmarshaled.Timestamp)
	}
}

func TestServer_DifferentPorts(t *testing.T) {
	ports := []int{8080, 8081, 9000, 0}

	for _, port := range ports {
		server := NewServer(port)
		if server == nil {
			t.Fatalf("Failed to create server on port %d", port)
		}

		expectedAddr := "127.0.0.1:" + fmt.Sprintf("%d", port)
		if port != 0 && server.server.Addr != expectedAddr {
			t.Errorf("Expected address %s, got %s", expectedAddr, server.server.Addr)
		}
	}
}

func TestServer_ConcurrentRequests(t *testing.T) {
	server := NewServer(0)

	// Start server
	go func() {
		server.Start()
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	// Wait for server to be ready
	for i := 0; i < 50; i++ {
		if server.server.Addr == "127.0.0.1:0" {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		resp, err := http.Get("http://" + server.server.Addr + "/health")
		if err == nil && resp != nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Make concurrent requests
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			defer func() { done <- true }()

			resp, err := http.Get("http://" + server.server.Addr + "/health")
			if err != nil {
				t.Errorf("Failed to make health request: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
			}
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
