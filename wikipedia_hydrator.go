package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const wikipediaHydratorBatchSize = 50

type wikipediaEvent struct {
	PageID uint64 `json:"page_id"`
	Title  string `json:"title"`
	Meta   struct {
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
	events := make(map[uint64]hydratedEvent, len(batch))
	for _, message := range batch {
		contents, err := message.AsBytes()
		if err != nil {
			return nil, err
		}
		var event wikipediaEvent
		if err := json.Unmarshal(contents, &event); err != nil {
			return nil, fmt.Errorf("decode Wikipedia event: %w", err)
		}
		if event.PageID == 0 || event.Revision.New == 0 || event.Meta.Timestamp == "" {
			continue
		}
		previous, exists := events[event.PageID]
		if !exists || event.Revision.New > previous.event.Revision.New {
			events[event.PageID] = hydratedEvent{event: event, message: message}
		}
	}
	if len(events) == 0 {
		return nil, nil
	}

	ids := make([]uint64, 0, len(events))
	for id := range events {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	result := make(service.MessageBatch, 0, len(ids))
	for start := 0; start < len(ids); start += wikipediaHydratorBatchSize {
		end := min(start+wikipediaHydratorBatchSize, len(ids))
		rows, err := hydrator.fetch(ctx, ids[start:end])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			id, err := strconv.ParseUint(row["id"].(string), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("decode hydrated page ID: %w", err)
			}
			event := events[id]
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

func (hydrator *wikipediaHydrator) fetch(ctx context.Context, ids []uint64) ([]map[string]any, error) {
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
	endpoint := hydrator.apiURL + "?" + parameters.Encode()
	if err := retryJSON(ctx, hydrator.client, http.MethodGet, endpoint, hydrator.userAgent, "", nil, &response); err != nil {
		return nil, fmt.Errorf("hydrate Wikipedia pages: %w", err)
	}
	rows, _, err := catchupRows(ids, response)
	return rows, err
}

func (hydrator *wikipediaHydrator) Close(context.Context) error {
	hydrator.client.CloseIdleConnections()
	return nil
}
