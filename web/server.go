package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var assets embed.FS

type Server struct {
	client      *SearchClient
	logger      *slog.Logger
	page        *template.Template
	searchSlots chan struct{}
	statsMu     sync.Mutex
	stats       Stats
	statsUntil  time.Time
}

type pageData struct {
	Query    string
	Response SearchResponse
	Error    string
}

func NewServer(client *SearchClient, logger *slog.Logger) (*Server, error) {
	page, err := template.ParseFS(assets, "index.html")
	if err != nil {
		return nil, err
	}
	return &Server{
		client:      client,
		logger:      logger,
		page:        page,
		searchSlots: make(chan struct{}, 32),
	}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handlePage)
	mux.HandleFunc("GET /api/search", s.handleSearch)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /w/rest.php/v1/search/title", s.handleWikipediaTitleSearch)
	mux.HandleFunc("GET /w/rest.php/v1/search/page", s.handleWikipediaPageSearch)
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = writer.Write([]byte("ok\n"))
	})
	return securityHeaders(mux)
}

func (s *Server) handleStats(writer http.ResponseWriter, request *http.Request) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	now := time.Now()
	if !now.Before(s.statsUntil) {
		stats, err := s.client.Stats(request.Context(), now)
		if err != nil {
			s.logger.Error("load health stats", "error", err)
			writeJSON(writer, http.StatusBadGateway, map[string]string{"error": "stats are temporarily unavailable"})
			return
		}
		s.stats = stats
		s.statsUntil = now.Add(10 * time.Second)
	}
	writer.Header().Set("Cache-Control", "public, max-age=10, stale-while-revalidate=20")
	writeJSON(writer, http.StatusOK, s.stats)
}

type wikipediaSearchResponse struct {
	Pages []wikipediaPage `json:"pages"`
}

type wikipediaPage struct {
	ID           any    `json:"id"`
	Key          string `json:"key"`
	Title        string `json:"title"`
	Excerpt      string `json:"excerpt"`
	MatchedTitle any    `json:"matched_title"`
	Anchor       any    `json:"anchor"`
	Description  any    `json:"description"`
	Thumbnail    any    `json:"thumbnail"`
}

func (s *Server) handleWikipediaTitleSearch(writer http.ResponseWriter, request *http.Request) {
	s.handleWikipediaSearch(writer, request, true)
}

func (s *Server) handleWikipediaPageSearch(writer http.ResponseWriter, request *http.Request) {
	s.handleWikipediaSearch(writer, request, false)
}

func (s *Server) handleWikipediaSearch(writer http.ResponseWriter, request *http.Request, titlesOnly bool) {
	writer.Header().Set("Access-Control-Allow-Origin", "*")
	query, ok := validQuery(request.URL.Query().Get("q"))
	if !ok || query == "" {
		writeWikipediaMissingQuery(writer)
		return
	}
	limit, ok := wikipediaLimit(writer, request.URL.Query().Get("limit"))
	if !ok {
		return
	}
	select {
	case s.searchSlots <- struct{}{}:
		defer func() { <-s.searchSlots }()
	default:
		writeWikipediaSearchError(writer)
		return
	}

	var (
		results []Result
		err     error
	)
	if titlesOnly {
		results, err = s.client.SearchTitles(request.Context(), query, limit)
	} else {
		results, err = s.client.Search(request.Context(), query, limit)
	}
	if err != nil {
		s.logger.Error("wikipedia-compatible search failed", "error", err)
		writeWikipediaSearchError(writer)
		return
	}

	pages := make([]wikipediaPage, 0, len(results))
	for _, result := range results {
		excerpt := string(result.ExcerptHTML)
		if titlesOnly {
			excerpt = template.HTMLEscapeString(result.Title)
		}
		pages = append(pages, wikipediaPage{
			ID:      wikipediaID(result.ID),
			Key:     wikipediaKey(result),
			Title:   result.Title,
			Excerpt: excerpt,
		})
	}
	writer.Header().Set("Cache-Control", "public, max-age=30, stale-while-revalidate=60")
	writeJSON(writer, http.StatusOK, wikipediaSearchResponse{Pages: pages})
}

func wikipediaID(id any) any {
	value, ok := id.(string)
	if !ok {
		return id
	}
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return id
	}
	return number
}

func wikipediaKey(result Result) string {
	parsed, err := url.Parse(result.URL)
	if err == nil {
		path := strings.TrimRight(parsed.Path, "/")
		if index := strings.LastIndexByte(path, '/'); index >= 0 && index+1 < len(path) {
			if key, decodeErr := url.PathUnescape(path[index+1:]); decodeErr == nil {
				return key
			}
		}
	}
	return strings.ReplaceAll(result.Title, " ", "_")
}

func wikipediaLimit(writer http.ResponseWriter, raw string) (int, bool) {
	if raw == "" {
		return 50, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]any{
			"error":               "parameter-validation-failed",
			"name":                "limit",
			"value":               raw,
			"failureCode":         "badinteger",
			"failureData":         nil,
			"errorKey":            "badinteger",
			"messageTranslations": map[string]string{"en": fmt.Sprintf("Invalid value %q for integer parameter \"limit\".", raw)},
			"httpCode":            http.StatusBadRequest,
			"httpReason":          "Bad Request",
		})
		return 0, false
	}
	if limit < 1 || limit > 100 {
		bounds := map[string]int{"min": 1, "curmax": 100, "max": 100, "highmax": 100}
		writeJSON(writer, http.StatusBadRequest, map[string]any{
			"error":               "parameter-validation-failed",
			"name":                "limit",
			"value":               raw,
			"failureCode":         "outofrange",
			"failureData":         bounds,
			"min":                 1,
			"curmax":              100,
			"max":                 100,
			"highmax":             100,
			"errorKey":            "outofrange",
			"messageTranslations": map[string]string{"en": fmt.Sprintf("The value %q for parameter \"limit\" must be between 1 and 100.", raw)},
			"httpCode":            http.StatusBadRequest,
			"httpReason":          "Bad Request",
		})
		return 0, false
	}
	return limit, true
}

func writeWikipediaMissingQuery(writer http.ResponseWriter) {
	writeJSON(writer, http.StatusBadRequest, map[string]any{
		"error":               "parameter-validation-failed",
		"name":                "q",
		"value":               nil,
		"failureCode":         "missingparam",
		"failureData":         nil,
		"errorKey":            "missingparam",
		"messageTranslations": map[string]string{"en": "The \"q\" parameter must be set."},
		"httpCode":            http.StatusBadRequest,
		"httpReason":          "Bad Request",
	})
}

func writeWikipediaSearchError(writer http.ResponseWriter) {
	writeJSON(writer, http.StatusInternalServerError, map[string]any{
		"error":      "search-error",
		"httpCode":   http.StatusInternalServerError,
		"httpReason": "Internal Server Error",
	})
}

func (s *Server) handlePage(writer http.ResponseWriter, request *http.Request) {
	query, ok := validQuery(request.URL.Query().Get("q"))
	data := pageData{Query: query}
	if !ok {
		data.Error = "Searches must contain at most 300 characters."
	} else if query != "" {
		started := time.Now()
		results, err := s.client.Search(request.Context(), query, 12)
		if err != nil {
			s.logger.Error("search failed", "error", err)
			data.Error = "Search is temporarily unavailable."
		} else {
			data.Response = SearchResponse{Results: results, ElapsedMS: time.Since(started).Milliseconds()}
		}
	}
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.Header().Set("Cache-Control", "public, max-age=60")
	if err := s.page.Execute(writer, data); err != nil {
		s.logger.Error("render page", "error", err)
	}
}

func (s *Server) handleSearch(writer http.ResponseWriter, request *http.Request) {
	query, ok := validQuery(request.URL.Query().Get("q"))
	if !ok || query == "" {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "q must contain 1 to 300 characters"})
		return
	}
	select {
	case s.searchSlots <- struct{}{}:
		defer func() { <-s.searchSlots }()
	default:
		writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "search is busy"})
		return
	}

	started := time.Now()
	results, err := s.client.Search(request.Context(), query, 12)
	if err != nil {
		s.logger.Error("search failed", "error", err)
		writeJSON(writer, http.StatusBadGateway, map[string]string{"error": "search is temporarily unavailable"})
		return
	}
	writer.Header().Set("Cache-Control", "public, max-age=30, stale-while-revalidate=60")
	writeJSON(writer, http.StatusOK, SearchResponse{
		Results:   results,
		ElapsedMS: time.Since(started).Milliseconds(),
	})
}

func validQuery(raw string) (string, bool) {
	query := strings.TrimSpace(raw)
	return query, len([]rune(query)) <= 300
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'none'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		next.ServeHTTP(writer, request)
	})
}
