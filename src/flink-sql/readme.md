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
Phase 1  : Flink SQL Fundamentals
Phase 2  : Batch Processing in Flink SQL
Phase 3  : Streaming Fundamentals
Phase 4  : Kafka Integration
Phase 5  : Event Time & Watermarks
Phase 6  : Windowing
Phase 7  : Joins
Phase 8  : Advanced Streaming SQL
Phase 9  : Changelog & Upsert Tables
Phase 10 : State & Performance
Phase 11 : Data Lake & Batch + Stream Unification
Phase 12 : End-to-End Production Projects