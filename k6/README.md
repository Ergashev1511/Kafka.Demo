# Transfer API stress test

This test targets 20,000 Kafka events per second for 20 seconds by default. Since
every successful HTTP request publishes three events, k6 starts 6,667 requests
per second. Every request uses unique transfer data and waits for Kafka acknowledgements.

## Run

Start Kafka and the API first:

```bash
dotnet run --project Kafka.Demo/Kafka.Demo.csproj --launch-profile http
```

Run a small smoke test before the full test:

```bash
k6 run -e EVENTS_PER_SECOND=30 -e PRE_ALLOCATED_VUS=10 -e MAX_VUS=50 k6/transfers.js
```

Run the default stress test (target: 20,000 Kafka events per second):

```bash
k6 run k6/transfers.js
```

Override settings when needed:

```bash
k6 run \
  -e BASE_URL=http://localhost:5122 \
  -e EVENTS_PER_SECOND=20000 \
  -e PRE_ALLOCATED_VUS=12000 \
  -e MAX_VUS=30000 \
  -e DURATION=20s \
  -e GRACEFUL_STOP=30s \
  -e REQUEST_TIMEOUT=30s \
  k6/transfers.js
```

The run is considered unsuccessful when more than 1% of requests fail, more than
1% of responses have an invalid body, p95 exceeds 2 seconds, or p99 exceeds 5
seconds. Check `dropped_iterations`: it must be zero, otherwise the load-generator
could not start requests at the requested rate. The real Kafka publish rate is
`kafka_events_published` per second and counts only successful responses.
