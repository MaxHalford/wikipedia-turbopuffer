package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerSearchAPIAndPage(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"rows":[{"id":"42","title":"Pufferfish","url":"https://en.wikipedia.org/wiki/Pufferfish","$dist":1.25,"excerpt":[{"text":"Pufferfish are fish.","fragment_range":[0,20],"match_ranges":[[0,6]]}]}]}`))
	}))
	defer upstream.Close()
	client, err := NewSearchClient("tpuf_test", "", upstream.URL, "wikipedia-test")
	require.NoError(t, err)
	server, err := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/api/search?q=puffer", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var body SearchResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, []Result{{
		ID:          "42",
		Title:       "Pufferfish",
		URL:         "https://en.wikipedia.org/wiki/Pufferfish",
		Excerpt:     "Pufferfish are fish.",
		ExcerptHTML: template.HTML(`<span class="searchmatch">Puffer</span>fish are fish.`),
		Score:       1.25,
	}}, body.Results)
	require.GreaterOrEqual(t, body.ElapsedMS, int64(0))
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))

	request = httptest.NewRequest(http.MethodGet, "/?q=puffer", nil)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "Pufferfish")
	require.Contains(t, response.Body.String(), "wikipedia on turbopuffer")
	require.Contains(t, response.Body.String(), `<span class="searchmatch">Puffer</span>fish are fish.`)
	require.NotContains(t, response.Body.String(), `&lt;span class=&#34;searchmatch&#34;&gt;`)
	require.Contains(t, response.Body.String(), "max@turbopuffer.com")
	require.NotContains(t, response.Body.String(), "live updates")
	require.Contains(t, response.Body.String(), `id="health-stats"`)
	require.Contains(t, response.Body.String(), `id="health-activity"`)
	require.Contains(t, response.Body.String(), `id="stat-freshness"`)
	require.Contains(t, response.Body.String(), "page changes")
	require.Equal(t, 1, strings.Count(response.Body.String(), "last 1h"))
	require.Contains(t, response.Body.String(), "p99 ingestion")
	require.Contains(t, response.Body.String(), "live ingestion stalled")
	require.Contains(t, response.Body.String(), `input[type="search"]::-webkit-search-cancel-button`)
	require.Contains(t, response.Body.String(), `freshnessHealth.title=message`)
	require.NotContains(t, response.Body.String(), `freshnessDot.title`)
	require.NotContains(t, response.Body.String(), `aria-label="Search"`)
	require.Contains(t, response.Body.String(), "setInterval(refreshStats,10000)")
	require.Contains(t, response.Body.String(), "const searchDebounceMS=150")
	require.Contains(t, response.Body.String(), "},searchDebounceMS)")
	require.Contains(t, response.Body.String(), `id="about-open"`)
	require.Contains(t, response.Body.String(), `id="about-dialog"`)
	require.Contains(t, response.Body.String(), "https://maxhalford.github.io/blog/wikipedia-on-turbopuffer/")
	require.Contains(t, response.Body.String(), "https://github.com/MaxHalford/wikipedia-turbopuffer")
	require.Contains(t, response.Body.String(), "/w/rest.php/v1/search/title?q=solar&amp;limit=5")
	require.Contains(t, response.Body.String(), "/w/rest.php/v1/search/page?q=solar&amp;limit=5")
	require.Contains(t, response.Body.String(), "/api/search?q=solar&amp;mode=hybrid")
	require.Contains(t, response.Body.String(), `<fieldset class="search-modes" aria-label="Search mode">`)
	require.Contains(t, response.Body.String(), `<input type="radio" name="mode" value="hybrid" checked><span>hybrid</span>`)
	require.Contains(t, response.Body.String(), "modes.forEach(mode=>mode.addEventListener('change',search))")
	require.NotContains(t, response.Body.String(), "Powered by turbopuffer")
}

func TestServerStatsAPI(t *testing.T) {
	liveIngestAt := time.Now().UTC()
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(request.URL.Path, "/metadata") {
			_, _ = writer.Write([]byte(`{"approx_row_count":7257731,"last_write_at":"2026-11-04T16:29:52Z","index":{"status":"up-to-date"}}`))
			return
		}
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		if _, ok := body["aggregate_by"]; ok {
			_, _ = writer.Write([]byte(`{"aggregations":{"pages":1500}}`))
			return
		}
		_, _ = fmt.Fprintf(writer, `{"rows":[{"source_event_at":%q,"ingested_at":%q}]}`, liveIngestAt.Add(-2*time.Second).Format(time.RFC3339Nano), liveIngestAt.Format(time.RFC3339Nano))
	}))
	defer upstream.Close()
	client, err := NewSearchClient("tpuf_test", "", upstream.URL, "wikipedia-test")
	require.NoError(t, err)
	server, err := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "public, max-age=10, stale-while-revalidate=20", response.Header().Get("Cache-Control"))
	var stats Stats
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &stats))
	require.Less(t, stats.LiveIngestLagMS, int64(2000))
	stats.LiveIngestLagMS = 0
	require.Equal(t, Stats{
		Documents:           7257731,
		PagesEditedLastHour: 1500,
		IndexStatus:         "up-to-date",
		LastWriteAt:         "2026-11-04T16:29:52Z",
		P99IngestLatencyMS:  2000,
		LatencySampleCount:  1,
	}, stats)
}

func TestServerRejectsInvalidQuery(t *testing.T) {
	client, err := NewSearchClient("tpuf_test", "gcp-us-central1", "", "wikipedia-test")
	require.NoError(t, err)
	server, err := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/api/search?q="+strings.Repeat("x", 301), nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)

	request = httptest.NewRequest(http.MethodGet, "/api/search?q=puffer&mode=fuzzy", nil)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.JSONEq(t, `{"error":"mode must be keyword, semantic, or hybrid"}`, response.Body.String())
}

func TestWikipediaSearchAPI(t *testing.T) {
	queries := make(chan map[string]any, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
		queries <- body
		writer.Header().Set("Content-Type", "application/json")
		if _, ok := body["queries"]; ok {
			_, _ = writer.Write([]byte(`{"results":[{"rows":[{"id":"26903","title":"Solar System","url":"https://en.wikipedia.org/wiki/Solar_System","$dist":2.5,"excerpt":[{"text":"The Solar System is gravitationally bound."}]}]}]}`))
			return
		}
		_, _ = writer.Write([]byte(`{"rows":[{"id":"26903","title":"Solar System","url":"https://en.wikipedia.org/wiki/Solar_System","$dist":2.5,"excerpt":[{"text":"The Solar System is gravitationally bound."}]}]}`))
	}))
	defer upstream.Close()
	client, err := NewSearchClient("tpuf_test", "", upstream.URL, "wikipedia-test")
	require.NoError(t, err)
	server, err := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/w/rest.php/v1/search/title?q=solar&limit=2", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
	require.JSONEq(t, `{"pages":[{"id":26903,"key":"Solar_System","title":"Solar System","excerpt":"Solar System","matched_title":null,"anchor":null,"description":null,"thumbnail":null}]}`, response.Body.String())
	titleQuery := <-queries
	require.Equal(t, float64(2), titleQuery["limit"])
	require.NotContains(t, titleQuery, "compute_attributes")

	request = httptest.NewRequest(http.MethodGet, "/w/rest.php/v1/search/page?q=solar&limit=1", nil)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"pages":[{"id":26903,"key":"Solar_System","title":"Solar System","excerpt":"The Solar System is gravitationally bound.","matched_title":null,"anchor":null,"description":null,"thumbnail":null}]}`, response.Body.String())
	pageQuery := <-queries
	require.Equal(t, float64(1), pageQuery["limit"])
	require.Equal(t, []any{"RRF"}, pageQuery["rerank_by"])
	subqueries := pageQuery["queries"].([]any)
	require.Len(t, subqueries, 2)
	require.Contains(t, subqueries[0].(map[string]any), "compute_attributes")
	require.Contains(t, subqueries[1].(map[string]any), "compute_attributes")
}

func TestWikipediaSearchAPIValidation(t *testing.T) {
	client, err := NewSearchClient("tpuf_test", "gcp-us-central1", "", "wikipedia-test")
	require.NoError(t, err)
	server, err := NewServer(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodGet, "/w/rest.php/v1/search/title?limit=2", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.JSONEq(t, `{
		"error":"parameter-validation-failed",
		"name":"q",
		"value":null,
		"failureCode":"missingparam",
		"failureData":null,
		"errorKey":"missingparam",
		"messageTranslations":{"en":"The \"q\" parameter must be set."},
		"httpCode":400,
		"httpReason":"Bad Request"
	}`, response.Body.String())

	request = httptest.NewRequest(http.MethodGet, "/w/rest.php/v1/search/page?q=solar&limit=101", nil)
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, "limit", body["name"])
	require.Equal(t, "outofrange", body["failureCode"])
}
