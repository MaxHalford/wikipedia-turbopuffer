package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/MaxHalford/wikipedia-turbopuffer/bloblang"
	_ "github.com/MaxHalford/wikipedia-turbopuffer/output"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
)

func main() {
	service.RunCLI(context.Background())
}
