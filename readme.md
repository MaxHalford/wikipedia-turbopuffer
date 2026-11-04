# Wikipedia search with turbopuffer

A small live English Wikipedia search stack built with
[Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) and
[turbopuffer](https://turbopuffer.com). It consumes Wikimedia recent-change
events, writes searchable article text to turbopuffer, and serves a minimal Go
search site with no external browser assets. Page search combines BM25 full-text
search with native `nvidia/nemotron-3.5-embed-1b` embeddings and fuses both
rankings with reciprocal rank fusion.

Try the deployed site at
[wikipedia-web-cfscc6ofha-uc.a.run.app](https://wikipedia-web-cfscc6ofha-uc.a.run.app).

The web service also implements Wikipedia-shaped title and page search routes:

```text
GET /w/rest.php/v1/search/title?q=solar&limit=5
GET /w/rest.php/v1/search/page?q=solar&limit=5
```

## Run locally

You need Go 1.27.3, one read-only turbopuffer key, one ingestion key with query
and write access, and a descriptive Wikimedia user agent with a monitored
contact address.

Create an ignored `.env` file:

```dotenv
TURBOPUFFER_API_READ_KEY=tpuf_...
TURBOPUFFER_API_WRITE_KEY=tpuf_...
TURBOPUFFER_REGION=gcp-us-central1
TURBOPUFFER_NAMESPACE=wikipedia-en-live
WIKIMEDIA_USER_AGENT=wikipedia-turbopuffer/0.1 (mailto:you@example.com)
WIKIPEDIA_CATCHUP_START=2026-11-01T00:00:00Z
WIKIPEDIA_CATCHUP_INTERVAL=5m
```

Start the live ingester:

```bash
set -a
source .env
set +a
TURBOPUFFER_API_KEY="$TURBOPUFFER_API_WRITE_KEY" \
  go run . -c config/wikipedia.yaml
```

Start the web service in another terminal:

```bash
set -a
source .env
set +a
TURBOPUFFER_API_KEY="$TURBOPUFFER_API_READ_KEY" go run ./cmd/wikipedia-web
```

Open <http://localhost:8080>. A successful ingester starts an HTTP listener and
logs that its Wikimedia input and turbopuffer output are active.

## Backfill Wikipedia

The live stream only contains changes received after the ingester starts.
Bootstrap the namespace with every shard from a weekly English Wikipedia
CirrusSearch content dump:

```bash
export WIKIPEDIA_DUMP_URL='https://dumps.wikimedia.org/other/cirrus_search_index/DATE/index_name%3Denwiki_content/SHARD.json.bz2'
TURBOPUFFER_API_KEY="$TURBOPUFFER_API_WRITE_KEY" \
  go run . wikipedia-backfill
```

Run one process per shard. The downloader resumes interrupted HTTP transfers,
then the pipeline decompresses each local shard as a stream. The output bypasses
turbopuffer's indexing-backlog guard during this controlled bulk load. Native
embeddings use the article title and lead section, while BM25 indexes the full
article body. Adding an embedding schema to an existing namespace does not embed
rows already stored there, so enable embeddings in a new namespace and backfill
it before cutover.

After the dump completes, reconcile every page that changed after the snapshot:

```bash
TURBOPUFFER_API_KEY="$TURBOPUFFER_API_WRITE_KEY" \
  WIKIPEDIA_CATCHUP_START=2026-11-01T00:00:00Z \
  go run . wikipedia-catchup
```

The reconciliation reads MediaWiki recent changes, fetches the current Cirrus
documents in batches, and repairs edited, moved, and deleted pages. The live
service runs the same reconciliation every five minutes. It stores the end of
each completed window in turbopuffer, so a restart resumes from the last
committed watermark and a failed run repeats its window.

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

Create the website query secret and the ingester query/write secret before
applying Terraform. Configure the GCS backend explicitly so state is not stored
in the repository:

```bash
terraform -chdir=terraform init \
  -backend-config='bucket=YOUR_TERRAFORM_STATE_BUCKET' \
  -backend-config='prefix=wikipedia'

terraform -chdir=terraform apply \
  -var='project_id=YOUR_PROJECT' \
  -var='web_image=REGISTRY/wikipedia-web:COMMIT' \
  -var='ingester_image=REGISTRY/wikipedia-ingester:COMMIT' \
  -var='job_image=REGISTRY/wikipedia-ingester:COMMIT' \
  -var='job_write_api_key_secret=YOUR_BACKFILL_WRITE_SECRET' \
  -var='turbopuffer_backfill_namespace=YOUR_HYBRID_NAMESPACE' \
  -var='wikimedia_user_agent=wikipedia-turbopuffer/0.1 (mailto:you@example.com)'
```

The separate job image and write secret let a new namespace backfill in a new
turbopuffer account while the live service continues to use its current image,
secret, and namespace.

Terraform prints the public Cloud Run URL and both job names. Run a one-shard
canary before you start the complete dump:

```bash
gcloud run jobs execute wikipedia-backfill --region=YOUR_REGION --tasks=1 --wait
gcloud run jobs execute wikipedia-backfill --region=YOUR_REGION --async
```

After the dump completes, set `turbopuffer_ingester_namespace` to the staging
namespace. Run `wikipedia-catchup`, then set `turbopuffer_namespace` to the
staging namespace for the public cutover.

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
  WIKIPEDIA_DUMP_FILE=/tmp/dump.json.bz2 \
  go run . lint config/wikipedia-backfill.yaml
```

The end-to-end test runs the event stream, page fetch, HTML conversion, and
turbopuffer write against local HTTP servers.

## Limits

- The index follows article revisions, moves, and deletes. It does not expand a
  template edit into updates for every article that transcludes that template.
- The API returns `null` descriptions and thumbnails because the index does not
  store them.
- A useful initial search index requires the separate multi-shard backfill.

## License

Apache-2.0.
