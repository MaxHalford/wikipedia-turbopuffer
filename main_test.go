package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestWikipediaPipeline(t *testing.T) {
	streamServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(writer, "data: {\"wiki\":\"enwiki\",\"namespace\":0,\"type\":\"edit\",\"title\":\"Test Article\",\"revision\":{\"new\":12}}\n\n")
		writer.(http.Flusher).Flush()
	}))
	defer streamServer.Close()

	pageServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/page/Test%20Article/with_html", request.URL.EscapedPath())
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":42,"key":"Test_Article","title":"Test Article","latest":{"id":12,"timestamp":"2026-11-04T10:00:00Z"},"html":"<h1>Test Article</h1><p>Hello <strong>world</strong>.</p>"}`)
	}))
	defer pageServer.Close()

	writes := make(chan map[string]any, 1)
	turbopufferServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v2/namespaces/wikipedia-test", request.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		select {
		case writes <- body:
		default:
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer turbopufferServer.Close()

	t.Setenv("WIKIMEDIA_STREAM_URL", streamServer.URL)
	t.Setenv("WIKIPEDIA_API_BASE", pageServer.URL)
	t.Setenv("WIKIMEDIA_USER_AGENT", "turbopuffer-test/0.1 (test@example.com)")
	t.Setenv("TURBOPUFFER_API_KEY", "tpuf_test")
	t.Setenv("TURBOPUFFER_BASE_URL", turbopufferServer.URL)
	t.Setenv("TURBOPUFFER_NAMESPACE", "wikipedia-test")

	config, err := os.ReadFile("config/wikipedia.yaml")
	require.NoError(t, err)
	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(string(config)))
	stream, err := builder.Build()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- stream.Run(context.Background())
	}()

	select {
	case body := <-writes:
		rows := body["upsert_rows"].([]any)
		require.Equal(t, map[string]any{
			"id":          "42",
			"revision_id": float64(12),
			"text":        "Test Article Hello world.",
			"title":       "Test Article",
			"updated_at":  "2026-11-04T10:00:00Z",
			"url":         "https://en.wikipedia.org/wiki/Test_Article",
		}, rows[0])
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the Wikipedia pipeline")
	}

	require.NoError(t, stream.StopWithin(5*time.Second))
	require.NoError(t, <-done)
}
