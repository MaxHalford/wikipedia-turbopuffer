package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestDownloadWikipediaDumpResumesPartialResponse(t *testing.T) {
	contents := []byte("complete Wikipedia dump contents")
	var downloads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "wikipedia-test/0.1", request.Header.Get("User-Agent"))
		writer.Header().Set("Accept-Ranges", "bytes")
		if request.Method == http.MethodHead {
			writer.Header().Set("Content-Length", strconv.Itoa(len(contents)))
			return
		}
		attempt := downloads.Add(1)
		if attempt == 1 {
			require.Empty(t, request.Header.Get("Range"))
			writer.Header().Set("Content-Length", strconv.Itoa(len(contents)))
			_, _ = writer.Write(contents[:10])
			return
		}
		require.Equal(t, "bytes=10-", request.Header.Get("Range"))
		writer.Header().Set("Content-Length", strconv.Itoa(len(contents)-10))
		writer.Header().Set("Content-Range", fmt.Sprintf("bytes 10-%d/%d", len(contents)-1, len(contents)))
		writer.WriteHeader(http.StatusPartialContent)
		_, _ = writer.Write(contents[10:])
	}))
	defer server.Close()

	destination := filepath.Join(t.TempDir(), "dump.json.bz2")
	require.NoError(t, downloadWikipediaDump(t.Context(), server.Client(), server.URL, destination, "wikipedia-test/0.1"))
	downloaded, err := os.ReadFile(destination)
	require.NoError(t, err)
	require.True(t, bytes.Equal(contents, downloaded))
	require.Equal(t, int32(2), downloads.Load())
}

func TestWikipediaPipeline(t *testing.T) {
	streamServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(writer, "data: {\"wiki\":\"enwiki\",\"meta\":{\"domain\":\"canary\"},\"namespace\":0,\"type\":\"edit\",\"title\":\"Canary\",\"revision\":{\"new\":11}}\n\n")
		_, _ = fmt.Fprint(writer, "data: {\"wiki\":\"enwiki\",\"meta\":{\"domain\":\"en.wikipedia.org\",\"dt\":\"2026-11-04T10:00:01Z\"},\"namespace\":0,\"type\":\"edit\",\"title\":\"Test Article\",\"revision\":{\"new\":12}}\n\n")
		writer.(http.Flusher).Flush()
	}))
	defer streamServer.Close()

	var canaryRequests atomic.Int32
	pageServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() == "/page/Canary/with_html" {
			canaryRequests.Add(1)
		}
		require.Equal(t, "/page/Test%20Article/with_html", request.URL.EscapedPath())
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"id":11250397,"key":"Test_Article","title":"Test Article","latest":{"id":12,"timestamp":"2026-11-04T10:00:00Z"},"html":"<h1>Test Article</h1><p>Hello <strong>world</strong>.</p>"}`)
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
		row := rows[0].(map[string]any)
		ingestedAt, ok := row["ingested_at"].(string)
		require.True(t, ok)
		parsed, err := time.Parse(time.RFC3339Nano, ingestedAt)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), parsed, 5*time.Second)
		delete(row, "ingested_at")
		require.Equal(t, map[string]any{
			"embedding_text":  "Test Article\n\nTest Article Hello world.",
			"id":              "11250397",
			"source_event_at": "2026-11-04T10:00:01Z",
			"revision_id":     float64(12),
			"text":            "Test Article Hello world.",
			"title":           "Test Article",
			"updated_at":      "2026-11-04T10:00:00Z",
			"url":             "https://en.wikipedia.org/wiki/Test_Article",
		}, row)
		require.Equal(t, []any{
			"revision_id",
			"Lt",
			map[string]any{"$ref_new": "revision_id"},
		}, body["upsert_condition"])
		require.Equal(t, "cosine_distance", body["distance_metric"])
		schema := body["schema"].(map[string]any)
		require.Equal(t, map[string]any{
			"type":             "string",
			"full_text_search": true,
			"filterable":       false,
		}, schema["text"])
		require.Equal(t, map[string]any{
			"type":       "string",
			"filterable": false,
			"embed": map[string]any{
				"model": "nvidia/nemotron-3.5-embed-1b",
				"dims":  float64(1024),
				"dtype": "f16",
			},
		}, schema["embedding_text"])
		require.Zero(t, canaryRequests.Load())
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
				Namespace:   0,
				OpeningText: "Lead section.",
				PageID:      42,
				PageType:    "primary",
				Text:        "Hello world.",
				Timestamp:   "2026-11-04T10:00:00Z",
				Title:       "Test Article/Subpage",
				Version:     12,
			}}},
		},
		{
			PageID:    45,
			Namespace: 0,
			Documents: []cirrusDoc{{Source: cirrusSource{
				Namespace: 0,
				PageID:    45,
				PageType:  "primary",
				Text:      "  ",
				Timestamp: "2026-11-04T10:00:00Z",
				Title:     "Empty Article",
				Version:   13,
			}}},
		},
		{PageID: 43, Namespace: 0, Missing: true},
		{PageID: 44, Namespace: 0, Documents: []cirrusDoc{{Source: cirrusSource{PageID: 44, PageType: "redirect"}}}},
	}

	upserts, deletes, err := catchupRows([]uint64{42, 43, 44, 45}, response)
	require.NoError(t, err)
	require.Equal(t, []map[string]any{{
		"embedding_text": "Test Article/Subpage\n\nLead section.",
		"id":             "42",
		"revision_id":    uint64(12),
		"text":           "Hello world.",
		"title":          "Test Article/Subpage",
		"updated_at":     "2026-11-04T10:00:00Z",
		"url":            "https://en.wikipedia.org/wiki/Test_Article%2FSubpage",
	}, {
		"embedding_text": "Empty Article",
		"id":             "45",
		"revision_id":    uint64(13),
		"text":           "Empty Article",
		"title":          "Empty Article",
		"updated_at":     "2026-11-04T10:00:00Z",
		"url":            "https://en.wikipedia.org/wiki/Empty_Article",
	}}, upserts)
	require.Equal(t, []string{"43", "44"}, deletes)
}

func TestSemanticArticleTextTruncatesBody(t *testing.T) {
	body := strings.Repeat("é", 5000)
	require.Equal(t, "Title\n\n"+strings.Repeat("é", 4096), semanticArticleText("Title", "", body))
}

func TestCollectRecentPageIDsUsesClosedWindow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "2026-11-04T10:00:00Z", request.URL.Query().Get("rcstart"))
		require.Equal(t, "2026-11-04T10:05:00Z", request.URL.Query().Get("rcend"))
		require.Equal(t, "newer", request.URL.Query().Get("rcdir"))
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"query":{"recentchanges":[]}}`)
	}))
	defer server.Close()

	config := &catchupConfig{
		apiURL:      server.URL,
		start:       "2026-11-04T10:00:00Z",
		end:         "2026-11-04T10:05:00Z",
		userAgent:   "test/0.1",
		httpClient:  server.Client(),
		requestGate: &requestGate{},
	}
	require.NoError(t, collectRecentPageIDs(t.Context(), config, map[uint64]struct{}{}, true))
}

func TestRetryDelayHonorsRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)

	require.Equal(t, 30*time.Second, retryDelay(0, "30", now))
	require.Equal(t, 45*time.Second, retryDelay(1, now.Add(45*time.Second).Format(http.TimeFormat), now))
	require.Equal(t, 4*time.Second, retryDelay(2, "invalid", now))
}

func TestWikipediaRequestGateRespectsCancellation(t *testing.T) {
	gate := &requestGate{
		interval: time.Second,
		next:     time.Now().Add(time.Hour),
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, gate.Wait(ctx), context.Canceled)
}

func TestWriteCatchupRowsUsesRevisionGuards(t *testing.T) {
	writes := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		decoder := json.NewDecoder(request.Body)
		decoder.UseNumber()
		var body map[string]any
		require.NoError(t, decoder.Decode(&body))
		writes <- body
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &catchupConfig{
		apiKey:      "tpuf_test",
		baseURL:     server.URL,
		namespace:   "wikipedia-test",
		end:         "2026-11-04T10:05:00Z",
		writeClient: server.Client(),
	}
	require.NoError(t, writeCatchupRows(t.Context(), config,
		[]map[string]any{{"id": "42", "revision_id": 12}},
		[]string{"43"},
	))
	require.Equal(t, map[string]any{
		"delete_condition": []any{"updated_at", "Lt", "2026-11-04T10:05:00Z"},
		"deletes":          []any{"43"},
		"upsert_condition": []any{"revision_id", "Lt", map[string]any{"$ref_new": "revision_id"}},
		"upsert_rows": []any{
			map[string]any{"id": "42", "revision_id": json.Number("12")},
		},
	}, <-writes)
}

func TestCatchupWatermarkRoundTrip(t *testing.T) {
	writes := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		if request.URL.Path == "/v2/namespaces/wikipedia-test/query" {
			require.Equal(t, []any{"id", "Eq", catchupWatermarkID}, body["filters"])
			writer.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(writer, `{"rows":[{"id":"__wikipedia_catchup_watermark__","sync_watermark":"2026-11-04T10:05:00Z"}]}`)
			return
		}
		writes <- body
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := &catchupConfig{
		apiKey:      "tpuf_test",
		baseURL:     server.URL,
		namespace:   "wikipedia-test",
		writeClient: server.Client(),
	}
	require.NoError(t, writeCatchupWatermark(t.Context(), config, "2026-11-04T10:05:00Z"))
	write := <-writes
	require.Equal(t,
		[]any{"sync_watermark", "Lt", map[string]any{"$ref_new": "sync_watermark"}},
		write["upsert_condition"],
	)

	watermark, err := readCatchupWatermark(t.Context(), config)
	require.NoError(t, err)
	require.Equal(t, "2026-11-04T10:05:00Z", watermark)
}
