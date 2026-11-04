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

	leadSpec := bloblang.NewPluginSpec().
		Category("String Manipulation").
		Description("Extracts lead paragraphs from Wikipedia Parsoid HTML.")
	bloblang.MustRegisterMethodV2("html_lead_to_text", leadSpec, func(*bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(value string) (any, error) {
			return htmlLeadToText(value), nil
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

func htmlLeadToText(value string) string {
	document, err := html.Parse(strings.NewReader(value))
	if err != nil {
		return ""
	}
	lead := findLeadSection(document)
	if lead == nil {
		return ""
	}

	paragraphs := make([]string, 0, 4)
	collectLeadParagraphs(lead, false, &paragraphs)
	return strings.Join(paragraphs, " ")
}

func findLeadSection(node *html.Node) *html.Node {
	if node.Type == html.ElementNode && node.Data == "section" && attribute(node, "data-mw-section-id") == "0" {
		return node
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if lead := findLeadSection(child); lead != nil {
			return lead
		}
	}
	return nil
}

func collectLeadParagraphs(node *html.Node, excluded bool, paragraphs *[]string) {
	if node.Type == html.ElementNode {
		switch node.Data {
		case "aside", "figure", "nav", "table":
			excluded = true
		}
		if !excluded && node.Data == "p" {
			var text strings.Builder
			collectVisibleText(node, &text)
			paragraph := strings.Join(strings.Fields(text.String()), " ")
			if paragraph != "" {
				*paragraphs = append(*paragraphs, paragraph)
			}
			return
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectLeadParagraphs(child, excluded, paragraphs)
	}
}

func collectVisibleText(node *html.Node, text *strings.Builder) {
	block := false
	if node.Type == html.ElementNode {
		if _, hidden := hiddenElements[node.Data]; hidden {
			return
		}
		switch node.Data {
		case "aside", "figure", "nav", "sup", "table":
			return
		}
		_, block = blockElements[node.Data]
		if block {
			text.WriteByte(' ')
		}
	}
	if node.Type == html.TextNode {
		text.WriteString(node.Data)
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collectVisibleText(child, text)
	}
	if block {
		text.WriteByte(' ')
	}
}

func attribute(node *html.Node, name string) string {
	for _, attr := range node.Attr {
		if attr.Key == name {
			return attr.Val
		}
	}
	return ""
}
