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
	require.Contains(t, request, "rank_by")
	require.Contains(t, request, "compute_attributes")
	excerpt := request["compute_attributes"].(map[string]any)["excerpt"].([]any)
	require.Equal(t, "utf-8", excerpt[2].(map[string]any)["include_offsets"])

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
		P99IngestLatencyMS:  4000,
		LatencySampleCount:  4,
	}, stats)
}

func TestNewSearchClientValidation(t *testing.T) {
	_, err := NewSearchClient("", "gcp-us-central1", "", "wikipedia-test")
	require.ErrorContains(t, err, "API_KEY")
	_, err = NewSearchClient("tpuf_test", "", "not-a-url", "wikipedia-test")
	require.ErrorContains(t, err, "BASE_URL")
	_, err = NewSearchClient("tpuf_test", "gcp-us-central1", "", "")
	require.ErrorContains(t, err, "NAMESPACE")
}
