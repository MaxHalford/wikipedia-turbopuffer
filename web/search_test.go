package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
		ExcerptHTML: `<span class="searchmatch">Pufferfish</span> are toxic fish.`,
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
	require.Equal(t, `&lt;b&gt;<span class="searchmatch">fish</span>&lt;/b&gt;`, results[0].ExcerptHTML)
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

func TestNewSearchClientValidation(t *testing.T) {
	_, err := NewSearchClient("", "gcp-us-central1", "", "wikipedia-test")
	require.ErrorContains(t, err, "API_KEY")
	_, err = NewSearchClient("tpuf_test", "", "not-a-url", "wikipedia-test")
	require.ErrorContains(t, err, "BASE_URL")
	_, err = NewSearchClient("tpuf_test", "gcp-us-central1", "", "")
	require.ErrorContains(t, err, "NAMESPACE")
}
