package output

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldAPIKey              = "api_key"
	fieldBaseURL             = "base_url"
	fieldBatching            = "batching"
	fieldDisableBackpressure = "disable_backpressure"
	fieldMaxInFlight         = "max_in_flight"
	fieldNamespace           = "namespace"
	fieldNewerThan           = "newer_than"
	fieldRegion              = "region"
	fieldSchema              = "schema"
	fieldTimeout             = "timeout"
)

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func init() {
	service.MustRegisterBatchOutput("turbopuffer", turbopufferSpec(), newTurbopufferOutput)
}

func turbopufferSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Writes batches of JSON documents to turbopuffer.").
		Description("Each message must be a JSON object with a valid turbopuffer document ID in its id field. If a batch contains the same ID more than once, the last document wins.").
		Fields(
			service.NewStringField(fieldAPIKey).
				Description("A turbopuffer API key.").
				Secret(),
			service.NewStringField(fieldRegion).
				Description("The turbopuffer region used to construct the API URL.").
				Default("gcp-us-central1"),
			service.NewStringField(fieldBaseURL).
				Description("An optional turbopuffer API base URL. This overrides region.").
				Default(""),
			service.NewStringField(fieldNamespace).
				Description("The turbopuffer namespace to write."),
			service.NewAnyMapField(fieldSchema).
				Description("An optional turbopuffer attribute schema sent with every write.").
				Default(map[string]any{}),
			service.NewBoolField(fieldDisableBackpressure).
				Description("Bypass indexing-backlog write protection for controlled bulk loads.").
				Default(false),
			service.NewStringField(fieldNewerThan).
				Description("Only replace an existing document when this attribute is smaller in the stored document.").
				Default(""),
			service.NewDurationField(fieldTimeout).
				Description("The timeout for each turbopuffer write request.").
				Default("60s"),
			service.NewIntField(fieldMaxInFlight).
				Description("The maximum number of concurrent turbopuffer requests.").
				Default(4),
			service.NewBatchPolicyField(fieldBatching),
		)
}

func newTurbopufferOutput(conf *service.ParsedConfig, _ *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	var policy service.BatchPolicy
	var err error
	if policy, err = conf.FieldBatchPolicy(fieldBatching); err != nil {
		return nil, policy, 0, err
	}

	apiKey, err := conf.FieldString(fieldAPIKey)
	if err != nil {
		return nil, policy, 0, err
	}
	if apiKey == "" {
		return nil, policy, 0, errors.New("api_key must not be empty")
	}

	region, err := conf.FieldString(fieldRegion)
	if err != nil {
		return nil, policy, 0, err
	}
	baseURL, err := conf.FieldString(fieldBaseURL)
	if err != nil {
		return nil, policy, 0, err
	}
	if baseURL == "" {
		if region == "" {
			return nil, policy, 0, errors.New("region must not be empty")
		}
		baseURL = "https://" + region + ".turbopuffer.com"
	}
	baseURL, err = validateBaseURL(baseURL)
	if err != nil {
		return nil, policy, 0, err
	}

	namespace, err := conf.FieldString(fieldNamespace)
	if err != nil {
		return nil, policy, 0, err
	}
	if !namespacePattern.MatchString(namespace) {
		return nil, policy, 0, fmt.Errorf("namespace must match %s", namespacePattern)
	}

	schemaValue, err := conf.FieldAny(fieldSchema)
	if err != nil {
		return nil, policy, 0, err
	}
	schema, ok := schemaValue.(map[string]any)
	if !ok {
		return nil, policy, 0, fmt.Errorf("schema must be an object, got %T", schemaValue)
	}
	disableBackpressure, err := conf.FieldBool(fieldDisableBackpressure)
	if err != nil {
		return nil, policy, 0, err
	}
	newerThan, err := conf.FieldString(fieldNewerThan)
	if err != nil {
		return nil, policy, 0, err
	}
	if disableBackpressure && newerThan != "" {
		return nil, policy, 0, errors.New("newer_than cannot be combined with disable_backpressure")
	}

	timeout, err := conf.FieldDuration(fieldTimeout)
	if err != nil {
		return nil, policy, 0, err
	}
	if timeout <= 0 {
		return nil, policy, 0, errors.New("timeout must be positive")
	}
	maxInFlight, err := conf.FieldInt(fieldMaxInFlight)
	if err != nil {
		return nil, policy, 0, err
	}
	if maxInFlight <= 0 {
		return nil, policy, 0, errors.New("max_in_flight must be positive")
	}

	return &turbopufferOutput{
		apiKey:              apiKey,
		disableBackpressure: disableBackpressure,
		endpoint:            baseURL + "/v2/namespaces/" + url.PathEscape(namespace),
		newerThan:           newerThan,
		schema:              schema,
		httpClient:          &http.Client{Timeout: timeout},
	}, policy, maxInFlight, nil
}

func validateBaseURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid base_url: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", errors.New("base_url must be an absolute HTTP or HTTPS URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("base_url must not contain a query or fragment")
	}
	return strings.TrimRight(raw, "/"), nil
}

type turbopufferOutput struct {
	apiKey              string
	disableBackpressure bool
	endpoint            string
	newerThan           string
	schema              map[string]any
	httpClient          *http.Client
}

func (t *turbopufferOutput) Connect(context.Context) error {
	return nil
}

func (t *turbopufferOutput) WriteBatch(ctx context.Context, messages service.MessageBatch) error {
	rows, err := coalesceRows(messages)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}

	payload := struct {
		UpsertRows          []map[string]any `json:"upsert_rows"`
		UpsertCondition     any              `json:"upsert_condition,omitempty"`
		Schema              map[string]any   `json:"schema,omitempty"`
		DisableBackpressure bool             `json:"disable_backpressure,omitempty"`
	}{
		UpsertRows:          rows,
		Schema:              t.schema,
		DisableBackpressure: t.disableBackpressure,
	}
	if t.newerThan != "" {
		payload.UpsertCondition = []any{
			t.newerThan,
			"Lt",
			map[string]any{"$ref_new": t.newerThan},
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal turbopuffer request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create turbopuffer request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := t.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("write to turbopuffer: %w", err)
	}
	defer response.Body.Close()

	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if readErr != nil {
		return fmt.Errorf("read turbopuffer response: %w", readErr)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("turbopuffer returned %s: %s", response.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func (t *turbopufferOutput) Close(context.Context) error {
	t.httpClient.CloseIdleConnections()
	return nil
}

func coalesceRows(messages service.MessageBatch) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(messages))
	positions := make(map[string]int, len(messages))
	for index, message := range messages {
		value, err := message.AsStructured()
		if err != nil {
			return nil, fmt.Errorf("message %d is not valid JSON: %w", index, err)
		}
		row, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("message %d must be a JSON object, got %T", index, value)
		}
		id, exists := row["id"]
		if !exists {
			return nil, fmt.Errorf("message %d has no id field", index)
		}
		key, err := documentIDKey(id)
		if err != nil {
			return nil, fmt.Errorf("message %d has an invalid id: %w", index, err)
		}
		if position, duplicate := positions[key]; duplicate {
			rows[position] = row
			continue
		}
		positions[key] = len(rows)
		rows = append(rows, row)
	}
	return rows, nil
}

func documentIDKey(value any) (string, error) {
	switch id := value.(type) {
	case string:
		if len(id) == 0 || len(id) > 64 {
			return "", errors.New("string IDs must contain 1 to 64 bytes")
		}
		return "string:" + id, nil
	case json.Number:
		parsed, err := strconv.ParseUint(id.String(), 10, 64)
		if err != nil {
			return "", errors.New("numeric IDs must be unsigned 64-bit integers")
		}
		return "number:" + strconv.FormatUint(parsed, 10), nil
	case int:
		if id < 0 {
			return "", errors.New("numeric IDs must be unsigned 64-bit integers")
		}
		return "number:" + strconv.FormatUint(uint64(id), 10), nil
	case int64:
		if id < 0 {
			return "", errors.New("numeric IDs must be unsigned 64-bit integers")
		}
		return "number:" + strconv.FormatUint(uint64(id), 10), nil
	case uint64:
		return "number:" + strconv.FormatUint(id, 10), nil
	case float64:
		if id < 0 || id >= math.Exp2(64) || math.Trunc(id) != id {
			return "", errors.New("numeric IDs must be unsigned 64-bit integers")
		}
		return "number:" + strconv.FormatFloat(id, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("expected a string or unsigned integer, got %T", value)
	}
}
