# Wikipedia search with turbopuffer

A small live English Wikipedia search stack built with
[Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) and
[turbopuffer](https://turbopuffer.com). It consumes Wikimedia recent-change
events, writes searchable article text to turbopuffer, and serves a minimal Go
search site with no external browser assets.

Try the deployed site at
[wikipedia-web-cfscc6ofha-uc.a.run.app](https://wikipedia-web-cfscc6ofha-uc.a.run.app).

The web service also implements Wikipedia-shaped title and page search routes:

```text
GET /w/rest.php/v1/search/title?q=solar&limit=5
GET /w/rest.php/v1/search/page?q=solar&limit=5
```

## Run locally

You need Go 1.27.3, one read-only turbopuffer key, one write-only key, and a
descriptive Wikimedia user agent with a monitored contact address.

Create an ignored `.env` file:

```dotenv
TURBOPUFFER_READ_API_KEY=tpuf_...
TURBOPUFFER_WRITE_API_KEY=tpuf_...
TURBOPUFFER_REGION=gcp-us-central1
TURBOPUFFER_NAMESPACE=wikipedia-en-live
WIKIMEDIA_USER_AGENT=wikipedia-turbopuffer/0.1 (mailto:you@example.com)
```

Start the live ingester:

```bash
set -a
source .env
set +a
TURBOPUFFER_API_KEY="$TURBOPUFFER_WRITE_API_KEY" \
  go run . -c config/wikipedia.yaml
```

Start the web service in another terminal:

```bash
set -a
source .env
set +a
TURBOPUFFER_API_KEY="$TURBOPUFFER_READ_API_KEY" go run ./cmd/wikipedia-web
```

Open <http://localhost:8080>. A successful ingester starts an HTTP listener and
logs that its Wikimedia input and turbopuffer output are active.

## Backfill Wikipedia

The live stream only contains changes received after the ingester starts.
Bootstrap the namespace with every shard from a weekly English Wikipedia
CirrusSearch content dump:

```bash
export WIKIPEDIA_DUMP_URL='https://dumps.wikimedia.org/other/cirrus_search_index/DATE/index_name%3Denwiki_content/SHARD.json.bz2'
TURBOPUFFER_API_KEY="$TURBOPUFFER_WRITE_API_KEY" \
  go run . -c config/wikipedia-backfill.yaml
```

Run one process per shard. The pipeline streams and decompresses each file, so
it does not load a complete dump into memory. The output bypasses turbopuffer's
indexing-backlog guard during this controlled bulk load.

After the dump completes, reconcile every page that changed after the snapshot:

```bash
TURBOPUFFER_API_KEY="$TURBOPUFFER_WRITE_API_KEY" \
  WIKIPEDIA_CATCHUP_START=2026-11-01T00:00:00Z \
  go run . wikipedia-catchup
```

The reconciliation reads MediaWiki recent changes, fetches the current Cirrus
documents in batches, and repairs edited, moved, and deleted pages.

## Deploy to Cloud Run

The repository includes two container images and Terraform for the live demo.
The Terraform defaults target `data-integration-473613`; override the project,
region, image, namespace, and secret variables for another environment.

Build both images with an immutable tag:

```bash
gcloud builds submit . \
  --project=YOUR_PROJECT \
  --config=cloudbuild.yaml \
  --substitutions=_TAG="$(git rev-parse HEAD)"
```

Create the query and write secrets before applying Terraform. Configure the GCS
backend explicitly so state is not stored in the repository:

```bash
terraform -chdir=terraform init \
  -backend-config='bucket=YOUR_TERRAFORM_STATE_BUCKET' \
  -backend-config='prefix=wikipedia'

terraform -chdir=terraform apply \
  -var='project_id=YOUR_PROJECT' \
  -var='web_image=REGISTRY/wikipedia-web:COMMIT' \
  -var='ingester_image=REGISTRY/wikipedia-ingester:COMMIT' \
  -var='wikimedia_user_agent=wikipedia-turbopuffer/0.1 (mailto:you@example.com)'
```

Terraform prints the public Cloud Run URL and both job names. Run a one-shard
canary before you start the complete dump:

```bash
gcloud run jobs execute wikipedia-backfill --region=YOUR_REGION --tasks=1 --wait
gcloud run jobs execute wikipedia-backfill --region=YOUR_REGION --async
```

After the dump completes, point the live ingester at the staging namespace and
run `wikipedia-catchup`. Point the web service at that namespace after the
reconciliation completes.

## Development

Run the Go tests and validate both Redpanda Connect pipelines:

```bash
go test -timeout 45s ./...
TURBOPUFFER_API_KEY=tpuf_test \
  WIKIMEDIA_USER_AGENT='test/0.1 (mailto:test@example.com)' \
  go run . lint config/wikipedia.yaml
TURBOPUFFER_API_KEY=tpuf_test \
  WIKIMEDIA_USER_AGENT='test/0.1 (mailto:test@example.com)' \
  WIKIPEDIA_DUMP_URL=https://example.com/dump.json.bz2 \
  go run . lint config/wikipedia-backfill.yaml
```

The end-to-end test runs the event stream, page fetch, HTML conversion, and
turbopuffer write against local HTTP servers.

## Limits

- Wikimedia's direct event stream has no durable replay cursor. An outage can
  lose events unless Kafka or Redpanda stores the stream first.
- The live pipeline handles article creates and edits. The reconciliation job
  repairs deletes and moves during a dump cutover.
- The live pipeline does not apply deletes and moves immediately after cutover.
- The API returns `null` descriptions and thumbnails because the index does not
  store them.
- A useful initial search index requires the separate multi-shard backfill.

## License

Apache-2.0.
