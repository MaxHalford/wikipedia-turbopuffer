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

func TestConfigureWikipediaDumpURL(t *testing.T) {
	t.Setenv("WIKIPEDIA_DUMP_DATE", "20261101")
	t.Setenv("CLOUD_RUN_TASK_INDEX", "7")
	t.Setenv("WIKIPEDIA_DUMP_BASE_URL", "https://dumps.example.test/")
	t.Setenv("WIKIPEDIA_DUMP_URL", "")

	require.NoError(t, configureWikipediaDumpURL())
	require.Equal(t,
		"https://dumps.example.test/20261101/index_name%3Denwiki_content/enwiki_content-20261101-00007.json.bz2",
		os.Getenv("WIKIPEDIA_DUMP_URL"),
	)
}

func TestConfigureWikipediaDumpURLPreservesExplicitURL(t *testing.T) {
	t.Setenv("WIKIPEDIA_DUMP_DATE", "invalid")
	t.Setenv("CLOUD_RUN_TASK_INDEX", "invalid")
	t.Setenv("WIKIPEDIA_DUMP_URL", "https://dumps.example.test/shard.json.bz2")

	require.NoError(t, configureWikipediaDumpURL())
	require.Equal(t, "https://dumps.example.test/shard.json.bz2", os.Getenv("WIKIPEDIA_DUMP_URL"))
}

func TestConfigureWikipediaDumpURLRejectsInvalidJobEnvironment(t *testing.T) {
	t.Setenv("WIKIPEDIA_DUMP_DATE", "2026-11-01")
	t.Setenv("CLOUD_RUN_TASK_INDEX", "zero")
	t.Setenv("WIKIPEDIA_DUMP_URL", "")

	require.EqualError(t, configureWikipediaDumpURL(), "WIKIPEDIA_DUMP_DATE must use YYYYMMDD format")
}

func TestCatchupRows(t *testing.T) {
	response := cirrusResponse{}
	response.Query.Pages = []cirrusPage{
		{
			PageID:    42,
			Namespace: 0,
			Documents: []cirrusDoc{{Source: cirrusSource{
				Namespace: 0,
				PageID:    42,
				PageType:  "primary",
				Text:      "Hello world.",
				Timestamp: "2026-11-04T10:00:00Z",
				Title:     "Test Article/Subpage",
				Version:   12,
			}}},
		},
		{PageID: 43, Namespace: 0, Missing: true},
		{PageID: 44, Namespace: 0, Documents: []cirrusDoc{{Source: cirrusSource{PageID: 44, PageType: "redirect"}}}},
	}

	upserts, deletes, err := catchupRows([]uint64{42, 43, 44}, response)
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{
		"id":          "42",
		"revision_id": uint64(12),
		"text":        "Hello world.",
		"title":       "Test Article/Subpage",
		"updated_at":  "2026-11-04T10:00:00Z",
		"url":         "https://en.wikipedia.org/wiki/Test_Article%2FSubpage",
	}}, upserts)
	require.Equal(t, []string{"43", "44"}, deletes)
}
