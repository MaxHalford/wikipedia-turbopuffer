FROM golang:1.27.3 AS build

ENV CGO_ENABLED=0
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -tags timetzdata -ldflags="-w -s" -o /redpanda-connect-turbopuffer .

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /redpanda-connect-turbopuffer /redpanda-connect-turbopuffer
COPY config/wikipedia.yaml /connect.yaml
COPY config/wikipedia-backfill.yaml /backfill.yaml

EXPOSE 4195
ENTRYPOINT ["/redpanda-connect-turbopuffer"]
CMD ["-c", "/connect.yaml"]
