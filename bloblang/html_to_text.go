package bloblang

import (
	"io"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"golang.org/x/net/html"
)

var blockElements = map[string]struct{}{
	"address": {}, "article": {}, "aside": {}, "blockquote": {}, "br": {},
	"dd": {}, "div": {}, "dl": {}, "dt": {}, "figcaption": {}, "figure": {},
	"footer": {}, "h1": {}, "h2": {}, "h3": {}, "h4": {}, "h5": {}, "h6": {},
	"header": {}, "hr": {}, "li": {}, "main": {}, "nav": {}, "ol": {}, "p": {},
	"pre": {}, "section": {}, "table": {}, "td": {}, "th": {}, "tr": {}, "ul": {},
}

var hiddenElements = map[string]struct{}{
	"noscript": {}, "script": {}, "style": {}, "template": {},
}

func init() {
	spec := bloblang.NewPluginSpec().
		Category("String Manipulation").
		Description("Extracts normalized visible text from an HTML string.")
	bloblang.MustRegisterMethodV2("html_to_text", spec, func(*bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(value string) (any, error) {
			return htmlToText(value), nil
		}), nil
	})
}

func htmlToText(value string) string {
	tokenizer := html.NewTokenizer(strings.NewReader(value))
	var text strings.Builder
	hiddenDepth := 0

	for {
		switch tokenizer.Next() {
		case html.ErrorToken:
			if tokenizer.Err() == io.EOF {
				return strings.Join(strings.Fields(text.String()), " ")
			}
			return strings.Join(strings.Fields(text.String()), " ")
		case html.StartTagToken:
			name, _ := tokenizer.TagName()
			element := string(name)
			if _, hidden := hiddenElements[element]; hidden {
				hiddenDepth++
			}
			if hiddenDepth == 0 {
				if _, block := blockElements[element]; block {
					text.WriteByte(' ')
				}
			}
		case html.EndTagToken:
			name, _ := tokenizer.TagName()
			element := string(name)
			if _, hidden := hiddenElements[element]; hidden {
				if hiddenDepth > 0 {
					hiddenDepth--
				}
				continue
			}
			if hiddenDepth == 0 {
				if _, block := blockElements[element]; block {
					text.WriteByte(' ')
				}
			}
		case html.TextToken:
			if hiddenDepth == 0 {
				text.Write(tokenizer.Text())
			}
		}
	}
}
