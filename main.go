package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Payload struct {
	DBType     string        `json:"db_type"`  // "mongo", "postgres", "mysql", "mssql"
	ConnStr    string        `json:"conn_str"` // connection string
	Database   string        `json:"database"`
	Collection string        `json:"collection"` // for mongo
	Table      string        `json:"table"`      // for SQL
	Data       []interface{} `json:"data"`
}

const (
	queueName     = "db_jobs"
	rabbitConnStr = "amqps://uvjlemur:FlM46WDk3s7rq7HpWUeuBIbmLMjf4OeR@fly.rmq.cloudamqp.com/uvjlemur" // change if needed
	workerCount   = 10
	jobBufferSize = 100
)

// Insert into MongoDB
func insertMongo(p Payload) error {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(p.ConnStr))
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("MongoDB disconnect error: %v", err)
		}
	}()

	if len(p.Data) == 0 {
		return fmt.Errorf("no data to insert")
	}

	coll := client.Database(p.Database).Collection(p.Collection)
	_, err = coll.InsertMany(ctx, p.Data)
	return err
}

// Insert into SQL DBs (Postgres, MySQL, MSSQL)
func insertSQL(p Payload) error {
	db, err := sql.Open(p.DBType, p.ConnStr)
	if err != nil {
		return err
	}
	defer db.Close()

	// create table if not exists (all columns as TEXT for simplicity)
	createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id SERIAL PRIMARY KEY, data JSON);`, p.Table)
	if p.DBType == "mysql" {
		createStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT AUTO_INCREMENT PRIMARY KEY, data JSON);`, p.Table)
	} else if p.DBType == "mssql" {
		createStmt = fmt.Sprintf(`IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='%s' AND xtype='U') CREATE TABLE %s (id INT IDENTITY(1,1) PRIMARY KEY, data NVARCHAR(MAX));`, p.Table, p.Table)
	}
	if _, err := db.Exec(createStmt); err != nil {
		return err
	}

	// insert each row as JSON
	for _, d := range p.Data {
		jsonData, _ := json.Marshal(d)
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (data) VALUES ($1);`, p.Table), string(jsonData))
		if err != nil && p.DBType == "mysql" {
			_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (data) VALUES (?);`, p.Table), string(jsonData))
		} else if err != nil && p.DBType == "mssql" {
			_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (data) VALUES (@p1);`, p.Table), string(jsonData))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func worker(ctx context.Context, id int, jobs <-chan amqp091.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-jobs:
			if !ok {
				return
			}
			var payload Payload
			if err := json.Unmarshal(msg.Body, &payload); err != nil {
				log.Printf("[Worker %d] JSON error: %v", id, err)
				_ = msg.Nack(false, false)
				continue
			}
			log.Printf("[Worker %d] Received: %s", id, payload.DBType)

			var err error
			switch payload.DBType {
			case "mongo":
				err = insertMongo(payload)
			case "postgres", "mysql", "mssql":
				err = insertSQL(payload)
			default:
				log.Printf("[Worker %d] Unsupported DBType: %s", id, payload.DBType)
				_ = msg.Nack(false, false)
				continue
			}

			if err != nil {
				log.Printf("[Worker %d] Insert error: %v", id, err)
				_ = msg.Nack(false, true) // requeue
			} else {
				_ = msg.Ack(false)
				log.Printf("[Worker %d] Message processed successfully (DBType=%s)", id, payload.DBType)
			}
		}
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to RabbitMQ
	conn, err := amqp091.Dial(rabbitConnStr)
	if err != nil {
		log.Fatal("RabbitMQ connect error:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Channel error:", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatal("Queue declare error:", err)
	}

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal("Consume error:", err)
	}

	jobs := make(chan amqp091.Delivery, jobBufferSize)
	var wg sync.WaitGroup

	// start workers
	wg.Add(workerCount)
	for i := 1; i <= workerCount; i++ {
		go func(workerID int) {
			worker(ctx, workerID, jobs, &wg)
		}(i)
	}

	log.Println("Worker pool started. Waiting for RabbitMQ messages...")

	go func() {
		for msg := range msgs {
			select {
			case jobs <- msg:
			case <-ctx.Done():
				return
			}
		}
		close(jobs)
	}()

	// Wait for all workers to finish
	wg.Wait()
	log.Println("Shutdown complete.")
}
