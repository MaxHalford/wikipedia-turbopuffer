package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const wikipediaHydratorBatchSize = 50

type wikipediaEvent struct {
	Title string `json:"title"`
	Meta  struct {
		Timestamp string `json:"dt"`
	} `json:"meta"`
	Revision struct {
		New uint64 `json:"new"`
	} `json:"revision"`
}

type wikipediaHydrator struct {
	apiURL    string
	userAgent string
	client    *http.Client
}

type hydratedEvent struct {
	event   wikipediaEvent
	message *service.Message
}

func init() {
	service.MustRegisterBatchProcessor(
		"wikipedia_hydrate",
		service.NewConfigSpec().
			Stable().
			Categories("Integration").
			Summary("Hydrates batches of Wikipedia change events with CirrusSearch documents.").
			Fields(
				service.NewStringField("api_url").
					Default("https://en.wikipedia.org/w/api.php"),
				service.NewStringField("user_agent"),
			),
		func(config *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
			apiURL, err := config.FieldString("api_url")
			if err != nil {
				return nil, err
			}
			userAgent, err := config.FieldString("user_agent")
			if err != nil {
				return nil, err
			}
			if userAgent == "" {
				return nil, fmt.Errorf("user_agent must not be empty")
			}
			return &wikipediaHydrator{
				apiURL:    apiURL,
				userAgent: userAgent,
				client:    &http.Client{Timeout: 2 * time.Minute},
			}, nil
		},
	)
}

func (hydrator *wikipediaHydrator) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	events := make(map[string]hydratedEvent, len(batch))
	for _, message := range batch {
		contents, err := message.AsBytes()
		if err != nil {
			return nil, err
		}
		var event wikipediaEvent
		if err := json.Unmarshal(contents, &event); err != nil {
			return nil, fmt.Errorf("decode Wikipedia event: %w", err)
		}
		if event.Title == "" || event.Revision.New == 0 || event.Meta.Timestamp == "" {
			continue
		}
		key := wikipediaTitleKey(event.Title)
		previous, exists := events[key]
		if !exists || event.Revision.New > previous.event.Revision.New {
			events[key] = hydratedEvent{event: event, message: message}
		}
	}
	if len(events) == 0 {
		return nil, nil
	}

	titles := make([]string, 0, len(events))
	for _, event := range events {
		titles = append(titles, event.event.Title)
	}
	sort.Strings(titles)

	result := make(service.MessageBatch, 0, len(titles))
	for start := 0; start < len(titles); start += wikipediaHydratorBatchSize {
		end := min(start+wikipediaHydratorBatchSize, len(titles))
		rows, err := hydrator.fetch(ctx, titles[start:end])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			event, exists := events[wikipediaTitleKey(row["title"].(string))]
			if !exists {
				continue
			}
			row["source_event_at"] = event.event.Meta.Timestamp
			row["ingested_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			message := event.message.Copy()
			message.SetStructured(row)
			result = append(result, message)
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return []service.MessageBatch{result}, nil
}

func (hydrator *wikipediaHydrator) fetch(ctx context.Context, titles []string) ([]map[string]any, error) {
	parameters := url.Values{
		"action":        {"query"},
		"cdincludes":    {"page_id|namespace|page_type|title|opening_text|text|version|timestamp"},
		"format":        {"json"},
		"formatversion": {"2"},
		"maxlag":        {"5"},
		"prop":          {"cirrusdoc"},
		"titles":        {strings.Join(titles, "|")},
	}
	var response cirrusResponse
	endpoint := hydrator.apiURL + "?" + parameters.Encode()
	if err := retryJSON(ctx, hydrator.client, http.MethodGet, endpoint, hydrator.userAgent, "", nil, &response); err != nil {
		return nil, fmt.Errorf("hydrate Wikipedia pages: %w", err)
	}
	rows := make([]map[string]any, 0, len(response.Query.Pages))
	for _, page := range response.Query.Pages {
		if page.Missing || page.Namespace != 0 || len(page.Documents) == 0 {
			continue
		}
		if row, ok := wikipediaArticleRow(page.Documents[0].Source); ok {
			rows = append(rows, row)
		}
	}
	return rows, nil
}

func wikipediaTitleKey(title string) string {
	return strings.ToLower(strings.ReplaceAll(title, "_", " "))
}

func (hydrator *wikipediaHydrator) Close(context.Context) error {
	hydrator.client.CloseIdleConnections()
	return nil
}
