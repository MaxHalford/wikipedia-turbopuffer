package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type SearchClient struct {
	apiKey           string
	endpoint         string
	metadataEndpoint string
	httpClient       *http.Client
}

type Stats struct {
	Documents           uint64 `json:"documents"`
	PagesEditedLastHour uint64 `json:"pages_edited_last_hour"`
	IndexStatus         string `json:"index_status"`
	LastWriteAt         string `json:"last_write_at"`
	LiveIngestLagMS     int64  `json:"live_ingest_lag_ms"`
	P95IngestLatencyMS  int64  `json:"p95_ingest_latency_ms"`
	LatencySampleCount  int    `json:"latency_sample_count"`
}

type Result struct {
	ID          any           `json:"id"`
	Title       string        `json:"title"`
	URL         string        `json:"url"`
	Excerpt     string        `json:"excerpt"`
	ExcerptHTML template.HTML `json:"excerpt_html"`
	UpdatedAt   string        `json:"updated_at"`
	Score       float64       `json:"score"`
}

type SearchResponse struct {
	Results   []Result `json:"results"`
	ElapsedMS int64    `json:"elapsed_ms"`
}

type SearchMode string

const (
	SearchModeHybrid            SearchMode = "hybrid"
	SearchModeKeyword           SearchMode = "keyword"
	SearchModeSemantic          SearchMode = "semantic"
	wikipediaCatchupWatermarkID            = "__wikipedia_catchup_watermark__"
)

func NewSearchClient(apiKey, region, baseURL, namespace string) (*SearchClient, error) {
	if apiKey == "" {
		return nil, errors.New("TURBOPUFFER_API_KEY must not be empty")
	}
	if baseURL == "" {
		if region == "" {
			return nil, errors.New("TURBOPUFFER_REGION must not be empty")
		}
		baseURL = "https://" + region + ".turbopuffer.com"
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("TURBOPUFFER_BASE_URL must be an absolute HTTP or HTTPS URL")
	}
	if namespace == "" {
		return nil, errors.New("TURBOPUFFER_NAMESPACE must not be empty")
	}
	namespaceEndpoint := strings.TrimRight(baseURL, "/") + "/v2/namespaces/" + url.PathEscape(namespace)
	return &SearchClient{
		apiKey:           apiKey,
		endpoint:         namespaceEndpoint + "/query",
		metadataEndpoint: namespaceEndpoint + "/metadata",
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (c *SearchClient) Stats(ctx context.Context, now time.Time) (Stats, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.metadataEndpoint, nil)
	if err != nil {
		return Stats{}, fmt.Errorf("create metadata request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	response, err := c.httpClient.Do(request)
	if err != nil {
		return Stats{}, fmt.Errorf("get turbopuffer metadata: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		return Stats{}, fmt.Errorf("turbopuffer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	var metadata struct {
		ApproxRowCount uint64 `json:"approx_row_count"`
		LastWriteAt    string `json:"last_write_at"`
		Index          struct {
			Status string `json:"status"`
		} `json:"index"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&metadata); err != nil {
		return Stats{}, fmt.Errorf("decode turbopuffer metadata: %w", err)
	}

	payload := map[string]any{
		"aggregate_by": map[string]any{"pages": []any{"Count"}},
		"filters": []any{
			"updated_at",
			"Gte",
			now.Add(-time.Hour).UTC().Format(time.RFC3339),
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Stats{}, fmt.Errorf("marshal stats query: %w", err)
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return Stats{}, fmt.Errorf("create stats query: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err = c.httpClient.Do(request)
	if err != nil {
		return Stats{}, fmt.Errorf("query turbopuffer stats: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		return Stats{}, fmt.Errorf("turbopuffer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	var aggregation struct {
		Aggregations struct {
			Pages uint64 `json:"pages"`
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&aggregation); err != nil {
		return Stats{}, fmt.Errorf("decode turbopuffer stats: %w", err)
	}

	payload = map[string]any{
		"rank_by": []any{
			"Dist",
			[]any{"Attribute", "ingested_at"},
			now.UTC().Format(time.RFC3339),
		},
		"top_k":              10_000,
		"filters":            []any{"ingested_at", "Gte", now.Add(-time.Hour).UTC().Format(time.RFC3339)},
		"include_attributes": []string{"source_event_at", "ingested_at"},
	}
	body, err = json.Marshal(payload)
	if err != nil {
		return Stats{}, fmt.Errorf("marshal latency query: %w", err)
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return Stats{}, fmt.Errorf("create latency query: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err = c.httpClient.Do(request)
	if err != nil {
		return Stats{}, fmt.Errorf("query turbopuffer latency: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		return Stats{}, fmt.Errorf("turbopuffer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	var latencyResponse struct {
		Rows []struct {
			SourceEventAt string `json:"source_event_at"`
			IngestedAt    string `json:"ingested_at"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&latencyResponse); err != nil {
		return Stats{}, fmt.Errorf("decode turbopuffer latency: %w", err)
	}
	latencies := make([]int64, 0, len(latencyResponse.Rows))
	var lastLiveIngest time.Time
	for _, row := range latencyResponse.Rows {
		sourceEventAt, sourceErr := time.Parse(time.RFC3339Nano, row.SourceEventAt)
		ingestedAt, ingestErr := time.Parse(time.RFC3339Nano, row.IngestedAt)
		if ingestErr == nil && ingestedAt.After(lastLiveIngest) {
			lastLiveIngest = ingestedAt
		}
		if sourceErr != nil || ingestErr != nil || ingestedAt.Before(sourceEventAt) {
			continue
		}
		latencies = append(latencies, ingestedAt.Sub(sourceEventAt).Milliseconds())
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95Latency := int64(0)
	if len(latencies) > 0 {
		rank := (95*len(latencies) + 99) / 100
		p95Latency = latencies[rank-1]
	}
	liveIngestLag := int64(-1)
	if !lastLiveIngest.IsZero() {
		liveIngestLag = max(now.Sub(lastLiveIngest).Milliseconds(), 0)
	}
	return Stats{
		Documents:           metadata.ApproxRowCount,
		PagesEditedLastHour: aggregation.Aggregations.Pages,
		IndexStatus:         metadata.Index.Status,
		LastWriteAt:         metadata.LastWriteAt,
		LiveIngestLagMS:     liveIngestLag,
		P95IngestLatencyMS:  p95Latency,
		LatencySampleCount:  len(latencies),
	}, nil
}

func (c *SearchClient) Search(ctx context.Context, query string, limit int) ([]Result, error) {
	return c.SearchWithMode(ctx, query, limit, SearchModeHybrid)
}

func (c *SearchClient) SearchWithMode(ctx context.Context, query string, limit int, mode SearchMode) ([]Result, error) {
	includeAttributes := []string{"title", "url", "updated_at"}
	computeAttributes := map[string]any{
		"excerpt": []any{"Highlight", "text", map[string]any{
			"fragment_by":       "sentence",
			"rank_fragments_by": []any{"$fragment", "BM25", query},
			"fragment_limit":    1,
			"include_offsets":   "utf-8",
		}},
	}
	vectorQuery := map[string]any{
		"rank_by":            []any{"embedding_text", "ANN", []any{"Embed", query}},
		"limit":              limit,
		"include_attributes": includeAttributes,
		"compute_attributes": computeAttributes,
	}
	keywordQuery := map[string]any{
		"rank_by": []any{
			"Sum",
			[]any{
				[]any{"Product", 2.0, []any{"title", "BM25", query}},
				[]any{"text", "BM25", query},
			},
		},
		"limit":              limit,
		"include_attributes": includeAttributes,
		"compute_attributes": computeAttributes,
	}

	var payload map[string]any
	switch mode {
	case SearchModeKeyword:
		payload = keywordQuery
	case SearchModeSemantic:
		payload = vectorQuery
	case SearchModeHybrid:
		vectorQuery["limit"] = 100
		keywordQuery["limit"] = 100
		payload = map[string]any{
			"queries":   []any{vectorQuery, keywordQuery},
			"rerank_by": []any{"RRF"},
			"limit":     limit,
		}
	default:
		return nil, fmt.Errorf("unsupported search mode %q", mode)
	}
	return c.query(ctx, payload)
}

func (c *SearchClient) SearchTitles(ctx context.Context, query string, limit int) ([]Result, error) {
	payload := map[string]any{
		"rank_by": []any{"title", "BM25", query, map[string]any{
			"last_as_prefix": true,
		}},
		"limit":              limit,
		"include_attributes": []string{"title", "url", "updated_at"},
	}
	return c.query(ctx, payload)
}

func (c *SearchClient) query(ctx context.Context, payload map[string]any) ([]Result, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal query: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create query: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("query turbopuffer: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
		return nil, fmt.Errorf("turbopuffer returned %s: %s", response.Status, strings.TrimSpace(string(message)))
	}

	type queryRow struct {
		ID        any     `json:"id"`
		Title     string  `json:"title"`
		URL       string  `json:"url"`
		UpdatedAt string  `json:"updated_at"`
		Score     float64 `json:"$dist"`
		Excerpt   []struct {
			Text          string  `json:"text"`
			FragmentRange []int   `json:"fragment_range"`
			MatchRanges   [][]int `json:"match_ranges"`
		} `json:"excerpt"`
	}
	var decoded struct {
		Rows    []queryRow `json:"rows"`
		Results []struct {
			Rows []queryRow `json:"rows"`
		} `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode turbopuffer response: %w", err)
	}
	rows := decoded.Rows
	if len(decoded.Results) > 0 {
		rows = decoded.Results[0].Rows
	}
	results := make([]Result, 0, len(rows))
	for _, row := range rows {
		if fmt.Sprint(row.ID) == wikipediaCatchupWatermarkID {
			continue
		}
		excerpt, excerptHTML := "", template.HTML("")
		if len(row.Excerpt) > 0 {
			excerpt = strings.Join(strings.Fields(row.Excerpt[0].Text), " ")
			excerptHTML = highlightExcerpt(
				row.Excerpt[0].Text,
				row.Excerpt[0].FragmentRange,
				row.Excerpt[0].MatchRanges,
			)
		}
		results = append(results, Result{
			ID:          row.ID,
			Title:       row.Title,
			URL:         row.URL,
			Excerpt:     excerpt,
			ExcerptHTML: excerptHTML,
			UpdatedAt:   row.UpdatedAt,
			Score:       row.Score,
		})
	}
	return results, nil
}

func highlightExcerpt(text string, fragmentRange []int, matchRanges [][]int) template.HTML {
	if len(matchRanges) == 0 {
		return template.HTML(html.EscapeString(strings.Join(strings.Fields(text), " ")))
	}
	base := 0
	if len(fragmentRange) == 2 {
		base = fragmentRange[0]
	}
	var excerpt strings.Builder
	cursor := 0
	for _, match := range matchRanges {
		if len(match) != 2 {
			continue
		}
		start, end := match[0]-base, match[1]-base
		if start < cursor || end < start || end > len(text) {
			continue
		}
		excerpt.WriteString(html.EscapeString(text[cursor:start]))
		excerpt.WriteString(`<span class="searchmatch">`)
		excerpt.WriteString(html.EscapeString(text[start:end]))
		excerpt.WriteString("</span>")
		cursor = end
	}
	excerpt.WriteString(html.EscapeString(text[cursor:]))
	return template.HTML(strings.TrimSpace(excerpt.String()))
}
