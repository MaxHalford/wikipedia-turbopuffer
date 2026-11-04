package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/MaxHalford/wikipedia-turbopuffer/bloblang"
	_ "github.com/MaxHalford/wikipedia-turbopuffer/output"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "wikipedia-catchup" {
		if err := runWikipediaCatchup(context.Background()); err != nil {
			log.Fatal(err)
		}
		return
	}
	if err := configureWikipediaDumpURL(); err != nil {
		log.Fatal(err)
	}
	service.RunCLI(context.Background())
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
