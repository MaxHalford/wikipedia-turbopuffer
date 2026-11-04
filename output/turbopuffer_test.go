package output

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestTurbopufferOutputWritesBatch(t *testing.T) {
	type capturedRequest struct {
		path          string
		authorization string
		contentType   string
		body          map[string]any
	}
	captured := make(chan capturedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		decoder := json.NewDecoder(request.Body)
		decoder.UseNumber()
		var body map[string]any
		if err := decoder.Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		captured <- capturedRequest{
			path:          request.URL.Path,
			authorization: request.Header.Get("Authorization"),
			contentType:   request.Header.Get("Content-Type"),
			body:          body,
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	output := &turbopufferOutput{
		apiKey:              "tpuf_test",
		disableBackpressure: true,
		endpoint:            server.URL + "/v2/namespaces/wikipedia-en",
		schema:              map[string]any{"text": map[string]any{"type": "string", "full_text_search": true}},
		httpClient:          server.Client(),
	}
	messages := service.MessageBatch{
		service.NewMessage([]byte(`{"id":"42","title":"Old"}`)),
		service.NewMessage([]byte(`{"id":"7","title":"Other"}`)),
		service.NewMessage([]byte(`{"id":"42","title":"New"}`)),
	}

	require.NoError(t, output.WriteBatch(t.Context(), messages))
	request := <-captured
	require.Equal(t, "/v2/namespaces/wikipedia-en", request.path)
	require.Equal(t, "Bearer tpuf_test", request.authorization)
	require.Equal(t, "application/json", request.contentType)
	require.Equal(t, map[string]any{
		"disable_backpressure": true,
		"schema": map[string]any{
			"text": map[string]any{"type": "string", "full_text_search": true},
		},
		"upsert_rows": []any{
			map[string]any{"id": "42", "title": "New"},
			map[string]any{"id": "7", "title": "Other"},
		},
	}, request.body)
}

func TestTurbopufferOutputReturnsAPIErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "slow down", http.StatusTooManyRequests)
	}))
	defer server.Close()

	output := &turbopufferOutput{
		apiKey:     "tpuf_test",
		endpoint:   server.URL,
		httpClient: server.Client(),
	}
	err := output.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"id":"42"}`)),
	})
	require.ErrorContains(t, err, "429 Too Many Requests: slow down")
}

func TestCoalesceRowsRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{name: "array", message: `[]`, want: "must be a JSON object"},
		{name: "missing ID", message: `{}`, want: "has no id field"},
		{name: "empty ID", message: `{"id":""}`, want: "string IDs must contain 1 to 64 bytes"},
		{name: "negative ID", message: `{"id":-1}`, want: "numeric IDs must be unsigned 64-bit integers"},
		{name: "fractional ID", message: `{"id":1.5}`, want: "numeric IDs must be unsigned 64-bit integers"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := coalesceRows(service.MessageBatch{service.NewMessage([]byte(test.message))})
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestTurbopufferOutputConfiguration(t *testing.T) {
	parsed, err := turbopufferSpec().ParseYAML(`
api_key: tpuf_test
base_url: http://localhost:9999/
namespace: wikipedia-en
newer_than: revision_id
schema:
  text:
    type: string
batching:
  count: 50
`, nil)
	require.NoError(t, err)

	batchOutput, policy, maxInFlight, err := newTurbopufferOutput(parsed, nil)
	require.NoError(t, err)
	require.Equal(t, 50, policy.Count)
	require.Equal(t, 4, maxInFlight)
	output := batchOutput.(*turbopufferOutput)
	require.Equal(t, "http://localhost:9999/v2/namespaces/wikipedia-en", output.endpoint)
	require.Equal(t, map[string]any{"text": map[string]any{"type": "string"}}, output.schema)
	require.False(t, output.disableBackpressure)
	require.Equal(t, "revision_id", output.newerThan)
}

func TestTurbopufferOutputRejectsNewerThanWithDisabledBackpressure(t *testing.T) {
	parsed, err := turbopufferSpec().ParseYAML(`
api_key: tpuf_test
namespace: wikipedia-en
disable_backpressure: true
newer_than: revision_id
batching: {}
`, nil)
	require.NoError(t, err)

	_, _, _, err = newTurbopufferOutput(parsed, nil)
	require.ErrorContains(t, err, "newer_than cannot be combined with disable_backpressure")
}

func TestTurbopufferOutputRejectsInvalidConfiguration(t *testing.T) {
	parsed, err := turbopufferSpec().ParseYAML(`
api_key: tpuf_test
namespace: bad/name
batching: {}
`, nil)
	require.NoError(t, err)

	_, _, _, err = newTurbopufferOutput(parsed, nil)
	require.ErrorContains(t, err, "namespace must match")
}

func TestTurbopufferOutputConnectAndClose(t *testing.T) {
	output := &turbopufferOutput{httpClient: http.DefaultClient}
	require.NoError(t, output.Connect(context.Background()))
	require.NoError(t, output.Close(context.Background()))
}
