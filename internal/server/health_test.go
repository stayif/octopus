package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestUnauthenticatedHealthContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := newHTTPHandler()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", health.Code, http.StatusOK)
	}
	var metadata map[string]string
	if err := json.Unmarshal(health.Body.Bytes(), &metadata); err != nil {
		t.Fatalf("decode GET /healthz body: %v", err)
	}
	if len(metadata) != 1 || metadata["status"] != "ok" {
		t.Fatalf("GET /healthz body = %v, want metadata-only status=ok", metadata)
	}

	root := httptest.NewRecorder()
	handler.ServeHTTP(root, httptest.NewRequest(http.MethodGet, "/", nil))
	if root.Code != http.StatusNotFound {
		t.Fatalf("GET / status = %d, want %d", root.Code, http.StatusNotFound)
	}

	models := httptest.NewRecorder()
	handler.ServeHTTP(models, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if models.Code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/models status = %d, want %d", models.Code, http.StatusUnauthorized)
	}
}
