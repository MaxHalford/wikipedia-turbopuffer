package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/MaxHalford/wikipedia-turbopuffer/bloblang"
	_ "github.com/MaxHalford/wikipedia-turbopuffer/output"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
)

func main() {
	ctx := context.Background()
	if len(os.Args) > 1 && os.Args[1] == "wikipedia-catchup" {
		if err := runWikipediaCatchup(ctx); err != nil {
			log.Fatal(err)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "wikipedia-backfill" {
		if err := prepareWikipediaDump(ctx); err != nil {
			log.Fatal(err)
		}
		os.Args = []string{os.Args[0], "-c", "/backfill.yaml"}
	}
	if err := configureWikipediaDumpURL(); err != nil {
		log.Fatal(err)
	}
	if value := os.Getenv("WIKIPEDIA_CATCHUP_INTERVAL"); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < time.Minute {
			log.Fatal("WIKIPEDIA_CATCHUP_INTERVAL must be a duration of at least one minute")
		}
		go runWikipediaCatchupLoop(ctx, interval)
	}
	service.RunCLI(ctx)
}

func prepareWikipediaDump(ctx context.Context) error {
	if file := os.Getenv("WIKIPEDIA_DUMP_FILE"); file != "" {
		return nil
	}
	if err := configureWikipediaDumpURL(); err != nil {
		return err
	}
	sourceURL := os.Getenv("WIKIPEDIA_DUMP_URL")
	if sourceURL == "" {
		return fmt.Errorf("WIKIPEDIA_DUMP_URL must not be empty")
	}
	destination := "/tmp/wikipedia-dump.json.bz2"
	if err := downloadWikipediaDump(ctx, http.DefaultClient, sourceURL, destination, os.Getenv("WIKIMEDIA_USER_AGENT")); err != nil {
		return err
	}
	return os.Setenv("WIKIPEDIA_DUMP_FILE", destination)
}

func downloadWikipediaDump(ctx context.Context, client *http.Client, sourceURL, destination, userAgent string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, sourceURL, nil)
	if err != nil {
		return fmt.Errorf("create Wikipedia dump HEAD request: %w", err)
	}
	request.Header.Set("User-Agent", userAgent)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("inspect Wikipedia dump: %w", err)
	}
	response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("inspect Wikipedia dump: unexpected status %s", response.Status)
	}
	expectedSize := response.ContentLength
	if expectedSize <= 0 {
		return fmt.Errorf("inspect Wikipedia dump: missing content length")
	}

	var lastErr error
	for attempt := 0; attempt < 8; attempt++ {
		currentSize := int64(0)
		if info, statErr := os.Stat(destination); statErr == nil {
			currentSize = info.Size()
		} else if !os.IsNotExist(statErr) {
			return fmt.Errorf("inspect partial Wikipedia dump: %w", statErr)
		}
		if currentSize == expectedSize {
			return nil
		}
		if currentSize > expectedSize {
			if err := os.Truncate(destination, 0); err != nil {
				return fmt.Errorf("reset oversized Wikipedia dump: %w", err)
			}
			currentSize = 0
		}

		request, err = http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
		if err != nil {
			return fmt.Errorf("create Wikipedia dump request: %w", err)
		}
		request.Header.Set("User-Agent", userAgent)
		if currentSize > 0 {
			request.Header.Set("Range", fmt.Sprintf("bytes=%d-", currentSize))
		}
		response, err = client.Do(request)
		if err != nil {
			lastErr = fmt.Errorf("download Wikipedia dump: %w", err)
			continue
		}
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
			response.Body.Close()
			lastErr = fmt.Errorf("download Wikipedia dump: unexpected status %s", response.Status)
			continue
		}

		flags := os.O_CREATE | os.O_WRONLY
		if response.StatusCode == http.StatusPartialContent && currentSize > 0 {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
			currentSize = 0
		}
		file, openErr := os.OpenFile(destination, flags, 0o600)
		if openErr != nil {
			response.Body.Close()
			return fmt.Errorf("open Wikipedia dump: %w", openErr)
		}
		_, copyErr := io.Copy(file, response.Body)
		closeErr := file.Close()
		response.Body.Close()
		if copyErr != nil {
			lastErr = fmt.Errorf("download Wikipedia dump: %w", copyErr)
			continue
		}
		if closeErr != nil {
			return fmt.Errorf("close Wikipedia dump: %w", closeErr)
		}
		info, statErr := os.Stat(destination)
		if statErr != nil {
			return fmt.Errorf("inspect downloaded Wikipedia dump: %w", statErr)
		}
		if info.Size() == expectedSize {
			return nil
		}
		lastErr = fmt.Errorf("download Wikipedia dump: got %d of %d bytes", info.Size(), expectedSize)
	}
	return lastErr
}

func configureWikipediaDumpURL() error {
	if os.Getenv("WIKIPEDIA_DUMP_URL") != "" {
		return nil
	}

	dumpDate := os.Getenv("WIKIPEDIA_DUMP_DATE")
	taskIndex := os.Getenv("CLOUD_RUN_TASK_INDEX")
	if dumpDate == "" && taskIndex == "" {
		return nil
	}
	if len(dumpDate) != 8 {
		return fmt.Errorf("WIKIPEDIA_DUMP_DATE must use YYYYMMDD format")
	}
	if _, err := strconv.ParseUint(dumpDate, 10, 32); err != nil {
		return fmt.Errorf("WIKIPEDIA_DUMP_DATE must use YYYYMMDD format: %w", err)
	}

	index, err := strconv.ParseUint(taskIndex, 10, 16)
	if err != nil {
		return fmt.Errorf("CLOUD_RUN_TASK_INDEX must be a non-negative integer: %w", err)
	}

	baseURL := os.Getenv("WIKIPEDIA_DUMP_BASE_URL")
	if baseURL == "" {
		baseURL = "https://dumps.wikimedia.org/other/cirrus_search_index"
	}
	dumpURL := fmt.Sprintf(
		"%s/%s/index_name%%3Denwiki_content/enwiki_content-%s-%05d.json.bz2",
		strings.TrimRight(baseURL, "/"), dumpDate, dumpDate, index,
	)
	return os.Setenv("WIKIPEDIA_DUMP_URL", dumpURL)
}
