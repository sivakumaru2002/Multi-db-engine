# Data Migration Worker Engine

Data Migration Worker Engine is a Go-based background worker that consumes migration jobs from RabbitMQ and writes incoming records to MongoDB, PostgreSQL, MySQL, or Microsoft SQL Server.

It is designed for queue-driven ingestion where a producer publishes a JSON job and the worker fan-outs processing across a small worker pool.

## What It Does

- Consumes jobs from the `db_jobs` RabbitMQ queue
- Processes messages concurrently with a fixed worker pool
- Inserts batches into MongoDB with `InsertMany`
- Inserts records into SQL databases as JSON documents stored in a single `data` column
- Acknowledges successful messages and requeues failed writes

## Supported Targets

The worker accepts the following `db_type` values:

- `mongo`
- `postgres`
- `mysql`
- `mssql`

## Architecture

```mermaid
flowchart LR
    A[Producer] --> B[RabbitMQ queue: db_jobs]
    B --> C[Go worker engine]
    C --> D[MongoDB]
    C --> E[PostgreSQL]
    C --> F[MySQL]
    C --> G[SQL Server]
```

## Project Structure

```text
.
|-- Dockerfile
|-- go.mod
|-- go.sum
|-- main.go
|-- README.md
```

## How It Works

1. The application connects to RabbitMQ.
2. It declares and consumes the durable queue named `db_jobs`.
3. Messages are buffered into an internal job channel.
4. Ten workers process jobs in parallel.
5. Each message body is unmarshaled into a payload structure.
6. The worker routes the payload to MongoDB or a supported SQL backend.
7. On success, the message is acknowledged.
8. On failure, the message is negatively acknowledged and requeued.

## Message Schema

Each RabbitMQ message must contain JSON shaped like this:

```json
{
  "db_type": "mongo",
  "conn_str": "mongodb://localhost:27017",
  "database": "migration_db",
  "collection": "customers",
  "table": "customers",
  "data": [
    {
      "name": "Alice",
      "email": "alice@example.com"
    }
  ]
}
```

### Payload Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `db_type` | string | Yes | Target database type: `mongo`, `postgres`, `mysql`, or `mssql` |
| `conn_str` | string | Yes | Target database connection string |
| `database` | string | Mongo: Yes | Database name for MongoDB |
| `collection` | string | Mongo: Yes | Collection name for MongoDB |
| `table` | string | SQL: Yes | Target table name for SQL databases |
| `data` | array | Yes | Batch of documents/rows to insert |

Notes:

- For MongoDB, `database` and `collection` are used.
- For SQL databases, `table` is used.
- The current implementation expects `data` to be a non-empty array.

## SQL Storage Model

For SQL targets, the worker creates the table if it does not already exist and stores each row as JSON in a single `data` column.

Current table patterns:

- PostgreSQL: `id SERIAL PRIMARY KEY, data JSON`
- MySQL: `id INT AUTO_INCREMENT PRIMARY KEY, data JSON`
- SQL Server: `id INT IDENTITY(1,1) PRIMARY KEY, data NVARCHAR(MAX)`

This makes the worker flexible for schemaless migration, but it also means:

- SQL rows are not expanded into typed columns
- Querying migrated data may require JSON functions or application-side parsing
- Table names are interpolated directly into SQL statements and should therefore be controlled carefully by the producer

## Prerequisites

- Go 1.24+
- A reachable RabbitMQ broker
- A reachable target database for each published job
- Docker, if you want to run the worker in a container

## Run Locally

### 1. Install dependencies

```powershell
go mod download
```

### 2. Start the worker

```powershell
go run .
```

### 3. Build a binary

```powershell
go build -o worker-engine.exe .
```

## Run With Docker

### Build the image

```powershell
docker build -t data-migration-worker-engine .
```

### Run the container

```powershell
docker run --rm data-migration-worker-engine
```

## Configuration

The current implementation uses compile-time constants in [main.go](d:/Intrapernueurship/GO%20language/datamigration-MongoDB-workerengine/main.go):

- RabbitMQ queue name: `db_jobs`
- Worker count: `10`
- Internal job buffer size: `100`
- RabbitMQ connection string: hardcoded in source

For production use, these should be moved to environment variables or a configuration file.

## Example Producer Payloads

### MongoDB

```json
{
  "db_type": "mongo",
  "conn_str": "mongodb://localhost:27017",
  "database": "sales",
  "collection": "orders",
  "data": [
    {
      "order_id": 1001,
      "customer": "Contoso",
      "amount": 149.95
    }
  ]
}
```

### PostgreSQL

```json
{
  "db_type": "postgres",
  "conn_str": "postgres://postgres:postgres@localhost:5432/migration?sslmode=disable",
  "table": "orders",
  "data": [
    {
      "order_id": 1001,
      "customer": "Contoso",
      "amount": 149.95
    }
  ]
}
```

### MySQL

```json
{
  "db_type": "mysql",
  "conn_str": "root:root@tcp(localhost:3306)/migration",
  "table": "orders",
  "data": [
    {
      "order_id": 1001,
      "customer": "Contoso",
      "amount": 149.95
    }
  ]
}
```

### SQL Server

```json
{
  "db_type": "mssql",
  "conn_str": "sqlserver://sa:Your_password123@localhost:1433?database=migration",
  "table": "orders",
  "data": [
    {
      "order_id": 1001,
      "customer": "Contoso",
      "amount": 149.95
    }
  ]
}
```

## Operational Behavior

- Invalid JSON messages are rejected without requeue
- Unsupported `db_type` values are rejected without requeue
- Database write failures are requeued
- MongoDB inserts are executed as batch inserts
- SQL inserts are executed one record at a time inside the processing loop

## Current Limitations

- RabbitMQ configuration is hardcoded in source code
- There is no graceful shutdown signal handling
- There is no retry limit or dead-letter routing strategy in the worker itself
- SQL table names are dynamically interpolated into queries
- SQL writes are not wrapped in a transaction
- The Dockerfile exposes port `8080`, but this worker does not serve HTTP traffic

## Recommended Next Improvements

1. Move RabbitMQ and worker settings to environment variables.
2. Remove hardcoded credentials from source code.
3. Add structured logging and metrics.
4. Add dead-letter queue support.
5. Sanitize or validate SQL table names.
6. Add integration tests with RabbitMQ and sample databases.
7. Handle OS signals for graceful shutdown.

## Dependencies

Core dependencies declared in `go.mod`:

- `github.com/rabbitmq/amqp091-go`
- `go.mongodb.org/mongo-driver`
- `github.com/lib/pq`
- `github.com/go-sql-driver/mysql`
- `github.com/denisenkom/go-mssqldb`

## License

No license file is currently present in this repository. Add one if the project is intended for distribution or external collaboration.# Multi-db-engine
