package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	catchupBatchSize         = 50
	catchupWatermarkID       = "__wikipedia_catchup_watermark__"
	wikipediaRequestInterval = time.Second
)

type requestGate struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func (gate *requestGate) Wait(ctx context.Context) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()

	if delay := time.Until(gate.next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	gate.next = time.Now().Add(gate.interval)
	return nil
}

type catchupConfig struct {
	apiKey      string
	queryAPIKey string
	apiURL      string
	baseURL     string
	namespace   string
	start       string
	end         string
	userAgent   string
	workers     int
	httpClient  *http.Client
	writeClient *http.Client
	requestGate *requestGate
}

type recentChangesResponse struct {
	Continue struct {
		RCContinue string `json:"rccontinue"`
	} `json:"continue"`
	Query struct {
		RecentChanges []recentChange `json:"recentchanges"`
	} `json:"query"`
}

type recentChange struct {
	PageID    uint64 `json:"pageid"`
	LogType   string `json:"logtype"`
	LogParams struct {
		TargetNamespace int `json:"target_ns"`
	} `json:"logparams"`
}

type cirrusResponse struct {
	Query struct {
		Pages []cirrusPage `json:"pages"`
	} `json:"query"`
}

type cirrusPage struct {
	PageID    uint64      `json:"pageid"`
	Namespace int         `json:"ns"`
	Missing   bool        `json:"missing"`
	Documents []cirrusDoc `json:"cirrusdoc"`
}

type cirrusDoc struct {
	Source cirrusSource `json:"source"`
}

type cirrusSource struct {
	Namespace   int    `json:"namespace"`
	OpeningText string `json:"opening_text"`
	PageID      uint64 `json:"page_id"`
	PageType    string `json:"page_type"`
	Text        string `json:"text"`
	Timestamp   string `json:"timestamp"`
	Title       string `json:"title"`
	Version     uint64 `json:"version"`
}

type apiErrorResponse struct {
	Error *struct {
		Code string `json:"code"`
		Info string `json:"info"`
	} `json:"error"`
}

func runWikipediaCatchup(ctx context.Context) error {
	config, err := catchupConfigFromEnv()
	if err != nil {
		return err
	}
	if err := writeCatchupWatermark(ctx, config, config.start); err != nil {
		return fmt.Errorf("initialize catchup watermark: %w", err)
	}
	config.start, err = readCatchupWatermark(ctx, config)
	if err != nil {
		return fmt.Errorf("read catchup watermark: %w", err)
	}
	config.end = time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	start, _ := time.Parse(time.RFC3339, config.start)
	end, _ := time.Parse(time.RFC3339, config.end)
	if !start.Before(end) {
		return nil
	}

	pageIDs := make(map[uint64]struct{})
	if err := collectRecentPageIDs(ctx, config, pageIDs, true); err != nil {
		return fmt.Errorf("collect main namespace changes: %w", err)
	}
	if err := collectRecentPageIDs(ctx, config, pageIDs, false); err != nil {
		return fmt.Errorf("collect moves into main namespace: %w", err)
	}

	ids := make([]uint64, 0, len(pageIDs))
	for id := range pageIDs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	log.Printf("reconciling %d pages changed between %s and %s", len(ids), config.start, config.end)

	if err := reconcilePages(ctx, config, ids); err != nil {
		return err
	}
	if err := writeCatchupWatermark(ctx, config, config.end); err != nil {
		return fmt.Errorf("commit catchup watermark: %w", err)
	}
	return nil
}

func runWikipediaCatchupLoop(ctx context.Context, interval time.Duration) {
	for {
		if err := runWikipediaCatchup(ctx); err != nil {
			log.Printf("Wikipedia reconciliation failed: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func catchupConfigFromEnv() (*catchupConfig, error) {
	config := &catchupConfig{
		apiKey:      os.Getenv("TURBOPUFFER_API_KEY"),
		apiURL:      getenv("WIKIPEDIA_ACTION_API", "https://en.wikipedia.org/w/api.php"),
		baseURL:     os.Getenv("TURBOPUFFER_BASE_URL"),
		namespace:   os.Getenv("TURBOPUFFER_NAMESPACE"),
		start:       os.Getenv("WIKIPEDIA_CATCHUP_START"),
		userAgent:   os.Getenv("WIKIMEDIA_USER_AGENT"),
		workers:     4,
		httpClient:  &http.Client{Timeout: 2 * time.Minute},
		writeClient: &http.Client{Timeout: time.Minute},
		requestGate: &requestGate{interval: wikipediaRequestInterval},
	}
	config.queryAPIKey = getenv("TURBOPUFFER_QUERY_API_KEY", config.apiKey)
	if config.baseURL == "" {
		region := getenv("TURBOPUFFER_REGION", "gcp-us-central1")
		config.baseURL = "https://" + region + ".turbopuffer.com"
	}
	if value := os.Getenv("WIKIPEDIA_CATCHUP_WORKERS"); value != "" {
		workers, err := strconv.Atoi(value)
		if err != nil || workers < 1 || workers > 16 {
			return nil, errors.New("WIKIPEDIA_CATCHUP_WORKERS must be between 1 and 16")
		}
		config.workers = workers
	}
	if config.apiKey == "" || config.namespace == "" || config.start == "" || config.userAgent == "" {
		return nil, errors.New("TURBOPUFFER_API_KEY, TURBOPUFFER_NAMESPACE, WIKIPEDIA_CATCHUP_START, and WIKIMEDIA_USER_AGENT are required")
	}
	if _, err := time.Parse(time.RFC3339, config.start); err != nil {
		return nil, fmt.Errorf("WIKIPEDIA_CATCHUP_START must use RFC3339 format: %w", err)
	}
	return config, nil
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func collectRecentPageIDs(ctx context.Context, config *catchupConfig, pageIDs map[uint64]struct{}, mainNamespace bool) error {
	continuation := ""
	requests := 0
	for {
		parameters := url.Values{
			"action":        {"query"},
			"format":        {"json"},
			"formatversion": {"2"},
			"list":          {"recentchanges"},
			"maxlag":        {"5"},
			"rcdir":         {"newer"},
			"rclimit":       {"max"},
			"rcprop":        {"ids|loginfo"},
			"rcend":         {config.end},
			"rcstart":       {config.start},
		}
		if mainNamespace {
			parameters.Set("rcnamespace", "0")
			parameters.Set("rctype", "edit|new|log")
		} else {
			parameters.Set("rctype", "log")
		}
		if continuation != "" {
			parameters.Set("rccontinue", continuation)
		}

		var response recentChangesResponse
		if err := getWikipediaJSON(ctx, config, parameters, &response); err != nil {
			return err
		}
		for _, change := range response.Query.RecentChanges {
			if change.PageID == 0 {
				continue
			}
			if mainNamespace || (change.LogType == "move" && change.LogParams.TargetNamespace == 0) {
				pageIDs[change.PageID] = struct{}{}
			}
		}
		requests++
		if requests%100 == 0 {
			log.Printf("listed %d recent-change pages", len(pageIDs))
		}
		continuation = response.Continue.RCContinue
		if continuation == "" {
			return nil
		}
	}
}

func reconcilePages(ctx context.Context, config *catchupConfig, ids []uint64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan []uint64)
	var processed atomic.Int64
	var firstError error
	var errorOnce sync.Once
	var workers sync.WaitGroup

	for range config.workers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for batch := range jobs {
				if err := reconcileBatch(ctx, config, batch); err != nil {
					errorOnce.Do(func() {
						firstError = err
						cancel()
					})
					return
				}
				count := processed.Add(int64(len(batch)))
				if count%5000 < catchupBatchSize {
					log.Printf("reconciled %d of %d pages", count, len(ids))
				}
			}
		}()
	}

sendBatches:
	for start := 0; start < len(ids); start += catchupBatchSize {
		end := min(start+catchupBatchSize, len(ids))
		select {
		case jobs <- ids[start:end]:
		case <-ctx.Done():
			break sendBatches
		}
	}
	close(jobs)
	workers.Wait()
	if firstError != nil {
		return firstError
	}
	log.Printf("reconciled all %d pages", len(ids))
	return nil
}

func reconcileBatch(ctx context.Context, config *catchupConfig, ids []uint64) error {
	values := make([]string, len(ids))
	for index, id := range ids {
		values[index] = strconv.FormatUint(id, 10)
	}
	parameters := url.Values{
		"action":        {"query"},
		"cdincludes":    {"page_id|namespace|page_type|title|opening_text|text|version|timestamp"},
		"format":        {"json"},
		"formatversion": {"2"},
		"maxlag":        {"5"},
		"pageids":       {strings.Join(values, "|")},
		"prop":          {"cirrusdoc"},
	}
	var response cirrusResponse
	if err := getWikipediaJSON(ctx, config, parameters, &response); err != nil {
		return err
	}
	upserts, deletes, err := catchupRows(ids, response)
	if err != nil {
		return err
	}
	return writeCatchupRows(ctx, config, upserts, deletes)
}

func catchupRows(requested []uint64, response cirrusResponse) ([]map[string]any, []string, error) {
	found := make(map[uint64]struct{}, len(response.Query.Pages))
	upserts := make([]map[string]any, 0, len(response.Query.Pages))
	deletes := make([]string, 0)
	for _, page := range response.Query.Pages {
		found[page.PageID] = struct{}{}
		if page.Missing || page.Namespace != 0 || len(page.Documents) == 0 {
			deletes = append(deletes, strconv.FormatUint(page.PageID, 10))
			continue
		}
		source := page.Documents[0].Source
		if source.Namespace != 0 || source.PageType != "primary" {
			deletes = append(deletes, strconv.FormatUint(page.PageID, 10))
			continue
		}
		text := strings.TrimSpace(source.Text)
		if text == "" {
			text = source.Title
		}
		upserts = append(upserts, map[string]any{
			"embedding_text": semanticArticleText(source.Title, source.OpeningText, source.Text),
			"id":             strconv.FormatUint(source.PageID, 10),
			"revision_id":    source.Version,
			"text":           text,
			"title":          source.Title,
			"updated_at":     source.Timestamp,
			"url":            "https://en.wikipedia.org/wiki/" + url.PathEscape(strings.ReplaceAll(source.Title, " ", "_")),
		})
	}
	for _, id := range requested {
		if _, ok := found[id]; !ok {
			return nil, nil, fmt.Errorf("Wikipedia omitted requested page ID %d", id)
		}
	}
	return upserts, deletes, nil
}

func semanticArticleText(title, openingText, text string) string {
	semanticText := strings.TrimSpace(openingText)
	if semanticText == "" {
		runes := []rune(strings.TrimSpace(text))
		if len(runes) > 4096 {
			runes = runes[:4096]
		}
		semanticText = string(runes)
	}
	if semanticText == "" {
		return title
	}
	return title + "\n\n" + semanticText
}

func getWikipediaJSON(ctx context.Context, config *catchupConfig, parameters url.Values, destination any) error {
	if err := config.requestGate.Wait(ctx); err != nil {
		return err
	}
	endpoint := config.apiURL + "?" + parameters.Encode()
	return retryJSON(ctx, config.httpClient, http.MethodGet, endpoint, config.userAgent, "", nil, destination)
}

func writeCatchupRows(ctx context.Context, config *catchupConfig, upserts []map[string]any, deletes []string) error {
	payload := struct {
		Upserts         []map[string]any `json:"upsert_rows,omitempty"`
		Deletes         []string         `json:"deletes,omitempty"`
		UpsertCondition any              `json:"upsert_condition,omitempty"`
		DeleteCondition any              `json:"delete_condition,omitempty"`
	}{Upserts: upserts, Deletes: deletes}
	if len(upserts) > 0 {
		payload.UpsertCondition = []any{"revision_id", "Lt", map[string]any{"$ref_new": "revision_id"}}
	}
	if len(deletes) > 0 {
		payload.DeleteCondition = []any{"updated_at", "Lt", config.end}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal catchup write: %w", err)
	}
	endpoint := strings.TrimRight(config.baseURL, "/") + "/v2/namespaces/" + url.PathEscape(config.namespace)
	return retryJSON(ctx, config.writeClient, http.MethodPost, endpoint, "", config.apiKey, body, nil)
}

func readCatchupWatermark(ctx context.Context, config *catchupConfig) (string, error) {
	payload := map[string]any{
		"consistency":        map[string]any{"level": "strong"},
		"filters":            []any{"id", "Eq", catchupWatermarkID},
		"include_attributes": []string{"sync_watermark"},
		"limit":              1,
		"rank_by":            []any{"id", "asc"},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	var response struct {
		Rows []struct {
			Watermark string `json:"sync_watermark"`
		} `json:"rows"`
	}
	endpoint := strings.TrimRight(config.baseURL, "/") + "/v2/namespaces/" + url.PathEscape(config.namespace) + "/query"
	if err := retryJSON(ctx, config.writeClient, http.MethodPost, endpoint, "", config.queryAPIKey, body, &response); err != nil {
		return "", err
	}
	if len(response.Rows) != 1 || response.Rows[0].Watermark == "" {
		return "", errors.New("catchup watermark is missing")
	}
	if _, err := time.Parse(time.RFC3339, response.Rows[0].Watermark); err != nil {
		return "", fmt.Errorf("invalid catchup watermark: %w", err)
	}
	return response.Rows[0].Watermark, nil
}

func writeCatchupWatermark(ctx context.Context, config *catchupConfig, watermark string) error {
	payload := map[string]any{
		"schema": map[string]any{
			"sync_watermark": map[string]any{"type": "datetime"},
		},
		"upsert_condition": []any{"sync_watermark", "Lt", map[string]any{"$ref_new": "sync_watermark"}},
		"upsert_rows": []any{
			map[string]any{
				"embedding_text": "Internal synchronization watermark",
				"id":             catchupWatermarkID,
				"sync_watermark": watermark,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(config.baseURL, "/") + "/v2/namespaces/" + url.PathEscape(config.namespace)
	return retryJSON(ctx, config.writeClient, http.MethodPost, endpoint, "", config.apiKey, body, nil)
}

func retryJSON(ctx context.Context, client *http.Client, method, endpoint, userAgent, apiKey string, body []byte, destination any) error {
	var lastError error
	for attempt := range 6 {
		retryAfter := ""
		request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if err != nil {
			return err
		}
		if userAgent != "" {
			request.Header.Set("User-Agent", userAgent)
		}
		if apiKey != "" {
			request.Header.Set("Authorization", "Bearer "+apiKey)
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(request)
		if err == nil {
			retryAfter = response.Header.Get("Retry-After")
			responseBody, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil {
				err = readErr
			} else if response.StatusCode >= 200 && response.StatusCode < 300 {
				var apiError apiErrorResponse
				if len(responseBody) > 0 && json.Unmarshal(responseBody, &apiError) == nil && apiError.Error != nil {
					err = fmt.Errorf("API returned %s: %s", apiError.Error.Code, apiError.Error.Info)
				} else if destination == nil || len(responseBody) == 0 {
					return nil
				} else if err := json.Unmarshal(responseBody, destination); err != nil {
					return fmt.Errorf("decode API response: %w", err)
				} else {
					return nil
				}
			} else {
				err = fmt.Errorf("API returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
			}
		}
		lastError = err
		if attempt < 5 {
			delay := retryDelay(attempt, retryAfter, time.Now())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return lastError
}

func retryDelay(attempt int, retryAfter string, now time.Time) time.Duration {
	delay := time.Duration(1<<attempt) * time.Second
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return max(delay, time.Duration(seconds)*time.Second)
	}
	if timestamp, err := http.ParseTime(retryAfter); err == nil {
		return max(delay, timestamp.Sub(now))
	}
	return delay
}
