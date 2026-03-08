## Launch

First build the image and start all of the containers:

```bash
docker compose up --build -d
```

Once the containers are running,

```bash
docker compose run sql-client
```

will drop you into the Flink SQL Client, where you can interact with Flink SQL.

## Shutdown

When you're done, this will shutdown all of the containers and delete the volume that was created for checkpointing:

```bash
docker compose down -v
```



Phase 0  : Environment Setup
Phase 1  : SQL Fundamentals
Phase 2  : Batch Processing
Phase 3  : Streaming Fundamentals
Phase 4  : Kafka Integration
Phase 5  : Event Time & Watermarks
Phase 6  : Windowing
Phase 7  : Joins
Phase 8  : Advanced SQL Patterns
Phase 9  : Upsert & Changelog Streams
Phase 10 : Performance & Debugging
Phase 11 : Batch + Stream Unification
Phase 12 : End-to-End Production Project