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

func TestHTMLLeadToText(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.html.html_lead_to_text()`)
	require.NoError(t, err)

	result, err := mapping.Query(map[string]any{
		"html": `<html><body>
			<section data-mw-section-id="0">
				<div role="note">For other uses, see somewhere else.</div>
				<table><tr><td><p>Infobox text.</p></td></tr></table>
				<p>The <strong>lead</strong> paragraph.<sup class="reference">[1]</sup></p>
				<p>Another sentence.</p>
			</section>
			<section data-mw-section-id="1"><p>Article body.</p></section>
		</body></html>`,
	})
	require.NoError(t, err)
	require.Equal(t, "The lead paragraph. Another sentence.", result)
}

func TestHTMLLeadToTextWithoutLeadParagraph(t *testing.T) {
	mapping, err := bloblang.Parse(`root = this.html.html_lead_to_text()`)
	require.NoError(t, err)

	result, err := mapping.Query(map[string]any{
		"html": `<section data-mw-section-id="0"><div>Hatnote only.</div></section>`,
	})
	require.NoError(t, err)
	require.Equal(t, "", result)
}
