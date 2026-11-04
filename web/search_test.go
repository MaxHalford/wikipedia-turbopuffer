package web

import (
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchClient(t *testing.T) {
	requests := make(chan map[string]any, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v2/namespaces/wikipedia-test/query", request.URL.Path)
		require.Equal(t, "Bearer tpuf_test", request.Header.Get("Authorization"))
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		requests <- body
		writer.Header().Set("Content-Type", "application/json")
		if _, ok := body["queries"]; ok {
			_, _ = writer.Write([]byte(`{"results":[{"rows":[{"id":"42","title":"Pufferfish","url":"https://en.wikipedia.org/wiki/Pufferfish","updated_at":"2026-11-04T10:00:00Z","$dist":1.25,"excerpt":[{"text":"Pufferfish are toxic fish.","fragment_range":[120,140],"match_ranges":[[120,130]]}]}]}]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"rows":[{"id":"42","title":"Pufferfish","url":"https://en.wikipedia.org/wiki/Pufferfish","updated_at":"2026-11-04T10:00:00Z","$dist":1.25,"excerpt":[{"text":"Pufferfish are toxic fish.","fragment_range":[120,140],"match_ranges":[[120,130]]}]}]}`))
	}))
	defer server.Close()

	client, err := NewSearchClient("tpuf_test", "", server.URL, "wikipedia-test")
	require.NoError(t, err)
	results, err := client.Search(t.Context(), "puffer fish", 12)
	require.NoError(t, err)
	require.Equal(t, []Result{{
		ID:          "42",
		Title:       "Pufferfish",
		URL:         "https://en.wikipedia.org/wiki/Pufferfish",
		Excerpt:     "Pufferfish are toxic fish.",
		ExcerptHTML: template.HTML(`<span class="searchmatch">Pufferfish</span> are toxic fish.`),
		UpdatedAt:   "2026-11-04T10:00:00Z",
		Score:       1.25,
	}}, results)

	request := <-requests
	require.Equal(t, float64(12), request["limit"])
	require.Equal(t, []any{"RRF"}, request["rerank_by"])
	queries := request["queries"].([]any)
	require.Len(t, queries, 2)
	vectorQuery := queries[0].(map[string]any)
	require.Equal(t, []any{"embedding_text", "ANN", []any{"Embed", "puffer fish"}}, vectorQuery["rank_by"])
	require.Equal(t, float64(100), vectorQuery["limit"])
	ftsQuery := queries[1].(map[string]any)
	require.Equal(t, float64(100), ftsQuery["limit"])
	require.Contains(t, ftsQuery, "rank_by")
	excerpt := vectorQuery["compute_attributes"].(map[string]any)["excerpt"].([]any)
	require.Equal(t, "utf-8", excerpt[2].(map[string]any)["include_offsets"])
	require.Equal(t, []any{"$fragment", "BM25", "puffer fish"}, excerpt[2].(map[string]any)["rank_fragments_by"])

	results, err = client.SearchTitles(t.Context(), "puffer f", 8)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "Pufferfish", results[0].Title)

	request = <-requests
	require.Equal(t, float64(8), request["limit"])
	require.NotContains(t, request, "compute_attributes")
	require.Equal(t, []any{
		"title",
		"BM25",
		"puffer f",
		map[string]any{"last_as_prefix": true},
	}, request["rank_by"])
}

func TestSearchClientModes(t *testing.T) {
	requests := make(chan map[string]any, 3)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		requests <- body
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"rows":[]}`))
	}))
	defer server.Close()
	client, err := NewSearchClient("tpuf_test", "", server.URL, "wikipedia-test")
	require.NoError(t, err)

	_, err = client.SearchWithMode(t.Context(), "puffer fish", 7, SearchModeKeyword)
	require.NoError(t, err)
	keyword := <-requests
	require.Equal(t, float64(7), keyword["limit"])
	require.NotContains(t, keyword, "queries")
	require.Equal(t, "Sum", keyword["rank_by"].([]any)[0])

	_, err = client.SearchWithMode(t.Context(), "puffer fish", 7, SearchModeSemantic)
	require.NoError(t, err)
	semantic := <-requests
	require.Equal(t, float64(7), semantic["limit"])
	require.NotContains(t, semantic, "queries")
	require.Equal(t, []any{"embedding_text", "ANN", []any{"Embed", "puffer fish"}}, semantic["rank_by"])

	_, err = client.SearchWithMode(t.Context(), "puffer fish", 7, SearchModeHybrid)
	require.NoError(t, err)
	hybrid := <-requests
	require.Equal(t, float64(7), hybrid["limit"])
	require.Len(t, hybrid["queries"], 2)
	require.Equal(t, []any{"RRF"}, hybrid["rerank_by"])

	_, err = client.SearchWithMode(t.Context(), "puffer fish", 7, SearchMode("unknown"))
	require.ErrorContains(t, err, `unsupported search mode "unknown"`)
}

func TestSearchClientEscapesHighlightHTML(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"rows":[{"id":1,"title":"Unsafe","excerpt":[{"text":"<b>fish</b>","fragment_range":[0,11],"match_ranges":[[3,7]]}]}]}`))
	}))
	defer server.Close()
	client, err := NewSearchClient("tpuf_test", "", server.URL, "wikipedia-test")
	require.NoError(t, err)

	results, err := client.Search(t.Context(), "fish", 1)
	require.NoError(t, err)
	require.Equal(t, "<b>fish</b>", results[0].Excerpt)
	require.Equal(t, template.HTML(`&lt;b&gt;<span class="searchmatch">fish</span>&lt;/b&gt;`), results[0].ExcerptHTML)
}

func TestSearchClientReportsAPIErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "not ready", http.StatusConflict)
	}))
	defer server.Close()
	client, err := NewSearchClient("tpuf_test", "", server.URL, "wikipedia-test")
	require.NoError(t, err)

	_, err = client.Search(t.Context(), "puffer", 12)
	require.ErrorContains(t, err, "409 Conflict: not ready")
}

func TestSearchClientStats(t *testing.T) {
	now := time.Date(2026, 11, 4, 16, 30, 0, 0, time.UTC)
	emptyLatency := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer tpuf_test", request.Header.Get("Authorization"))
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/v2/namespaces/wikipedia-test/metadata":
			require.Equal(t, http.MethodGet, request.Method)
			_, _ = writer.Write([]byte(`{"approx_row_count":7257731,"approx_logical_bytes":34051676253,"last_write_at":"2026-11-04T16:29:52Z","index":{"status":"up-to-date"}}`))
		case "/v2/namespaces/wikipedia-test/query":
			require.Equal(t, http.MethodPost, request.Method)
			var body map[string]any
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			if aggregate, ok := body["aggregate_by"]; ok {
				require.Equal(t, map[string]any{"pages": []any{"Count"}}, aggregate)
				require.Equal(t, []any{"updated_at", "Gte", "2026-11-04T15:30:00Z"}, body["filters"])
				_, _ = writer.Write([]byte(`{"aggregations":{"pages":1500}}`))
				return
			}
			require.Equal(t, []any{"ingested_at", "Gte", "2026-11-04T15:30:00Z"}, body["filters"])
			require.Equal(t, float64(10000), body["top_k"])
			if emptyLatency {
				_, _ = writer.Write([]byte(`{"rows":[]}`))
				return
			}
			_, _ = writer.Write([]byte(`{"rows":[
				{"source_event_at":"2026-11-04T16:29:55Z","ingested_at":"2026-11-04T16:29:56Z"},
				{"source_event_at":"2026-11-04T16:29:54Z","ingested_at":"2026-11-04T16:29:56Z"},
				{"source_event_at":"2026-11-04T16:29:53Z","ingested_at":"2026-11-04T16:29:56Z"},
				{"source_event_at":"2026-11-04T16:29:52Z","ingested_at":"2026-11-04T16:29:56Z"}
			]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client, err := NewSearchClient("tpuf_test", "", server.URL, "wikipedia-test")
	require.NoError(t, err)

	stats, err := client.Stats(t.Context(), now)
	require.NoError(t, err)
	require.Equal(t, Stats{
		Documents:           7257731,
		PagesEditedLastHour: 1500,
		IndexStatus:         "up-to-date",
		LastWriteAt:         "2026-11-04T16:29:52Z",
		LiveIngestLagMS:     4000,
		P99IngestLatencyMS:  4000,
		LatencySampleCount:  4,
	}, stats)

	emptyLatency = true
	stats, err = client.Stats(t.Context(), now)
	require.NoError(t, err)
	require.Equal(t, int64(-1), stats.LiveIngestLagMS)
	require.Zero(t, stats.LatencySampleCount)
	require.Zero(t, stats.P99IngestLatencyMS)
}

func TestNewSearchClientValidation(t *testing.T) {
	_, err := NewSearchClient("", "gcp-us-central1", "", "wikipedia-test")
	require.ErrorContains(t, err, "API_KEY")
	_, err = NewSearchClient("tpuf_test", "", "not-a-url", "wikipedia-test")
	require.ErrorContains(t, err, "BASE_URL")
	_, err = NewSearchClient("tpuf_test", "gcp-us-central1", "", "")
	require.ErrorContains(t, err, "NAMESPACE")
}
