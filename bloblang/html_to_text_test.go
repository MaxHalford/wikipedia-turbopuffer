package bloblang

import (
	"testing"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/stretchr/testify/require"
)

func TestHTMLToText(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.html.html_to_text()`)
	require.NoError(t, err)

	result, err := mapping.Query(map[string]any{
		"html": `<h1>A &amp; B</h1><p>Hello <strong>world</strong>.</p><script>ignore()</script><p>Next<br>line</p>`,
	})
	require.NoError(t, err)
	require.Equal(t, "A & B Hello world. Next line", result)
}
