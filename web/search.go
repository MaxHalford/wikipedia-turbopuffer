package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SearchClient struct {
	apiKey     string
	endpoint   string
	httpClient *http.Client
}

type Result struct {
	ID          any     `json:"id"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Excerpt     string  `json:"excerpt"`
	ExcerptHTML string  `json:"-"`
	UpdatedAt   string  `json:"updated_at"`
	Score       float64 `json:"score"`
}

type SearchResponse struct {
	Results   []Result `json:"results"`
	ElapsedMS int64    `json:"elapsed_ms"`
}

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
	return &SearchClient{
		apiKey:   apiKey,
		endpoint: strings.TrimRight(baseURL, "/") + "/v2/namespaces/" + url.PathEscape(namespace) + "/query",
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}, nil
}

func (c *SearchClient) Search(ctx context.Context, query string, limit int) ([]Result, error) {
	payload := map[string]any{
		"rank_by": []any{
			"Sum",
			[]any{
				[]any{"Product", 2.0, []any{"title", "BM25", query}},
				[]any{"text", "BM25", query},
			},
		},
		"limit":              limit,
		"include_attributes": []string{"title", "url", "updated_at"},
		"compute_attributes": map[string]any{
			"excerpt": []any{"Highlight", "text", map[string]any{
				"fragment_by":     "sentence",
				"fragment_limit":  1,
				"include_offsets": "utf-8",
			}},
		},
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

	var decoded struct {
		Rows []struct {
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
		} `json:"rows"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode turbopuffer response: %w", err)
	}
	results := make([]Result, 0, len(decoded.Rows))
	for _, row := range decoded.Rows {
		excerpt, excerptHTML := "", ""
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

func highlightExcerpt(text string, fragmentRange []int, matchRanges [][]int) string {
	if len(matchRanges) == 0 {
		return html.EscapeString(strings.Join(strings.Fields(text), " "))
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
	return strings.TrimSpace(excerpt.String())
}
