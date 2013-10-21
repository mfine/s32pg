package main

import (
	"database/sql"
	"encoding/xml"
	"flag"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kr/s3"
	"github.com/lib/pq"
)

var (
	c       = make(chan func())
	db      = dbOpen(mustGetenv("DATABASE_URL"))
	wg      sync.WaitGroup
	bucket  = flag.String("bucket", "", "AWS S3 Bucket")
	prefix  = flag.String("prefix", "", "AWS S3 Prefix")
	workers = flag.Int("workers", 5, "Number of Workers")
	keys    = s3.Keys{AccessKey: mustGetenv("AWS_ACCESS_KEY_ID"), SecretKey: mustGetenv("AWS_SECRET_ACCESS_KEY")}
)

type listBucketResult struct {
	IsTruncated bool
	Objects     []listObject `xml:"Contents"`
}

type listObject struct {
	Key          string
	LastModified string
	Size         string
	ETag         string
	StorageClass string
}

func (o *listObject) lastModified() time.Time {
	t, err := time.Parse("2006-01-02T15:04:05.000Z", o.LastModified)
	if err != nil {
		log.Fatal(err)
	}

	return t
}

func (o *listObject) upsert() {
	log.Printf("fn=upsert key=%v last_modified=%v size=%v etag=%v storage=%v", o.Key, o.LastModified, o.Size, o.ETag, o.StorageClass)

	rows, err := db.Query("SELECT id FROM objects WHERE key=$1", o.Key)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	id := ""
	if rows.Next() {
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
	}

	if id != "" {
		if _, err := db.Exec("UPDATE objects SET last_modified=$2, size=$3, etag=$4 WHERE id=$1", id, o.lastModified(), o.Size, o.ETag); err != nil {
			log.Fatal(err)
		}
	} else {
		if _, err := db.Exec("INSERT INTO objects (key, last_modified, size, etag) VALUES ($1, $2, $3, $4)", o.Key, o.lastModified(), o.Size, o.ETag); err != nil {
			log.Fatal(err)
		}
	}
}

func dbOpen(url string) (db *sql.DB) {
	name, err := pq.ParseURL(url)
	if err != nil {
		log.Fatal(err)
	}

	db, err = sql.Open("postgres", name+" sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	return
}

func mustGetenv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("%v not set", key)
	}

	return value
}

func listBucket(i string) *listBucketResult {
	url := "https://" + *bucket + ".s3.amazonaws.com/?prefix=" + *prefix + "&marker=" + i + "&max-keys=1000"

	log.Printf("fn=list url=%q", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	s3.Sign(req, keys)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	result := &listBucketResult{}
	if err = xml.NewDecoder(resp.Body).Decode(result); err != nil {
		log.Fatal(err)
	}

	return result
}

func list(i string) {
	result := listBucket(i)
	for _, o := range result.Objects {
		c <- func(p listObject) func() { return func() { p.upsert() } }(o)
	}

	if result.IsTruncated {
		c <- func() { list(result.Objects[len(result.Objects)-1].Key) }
	}
}

func work() {
	defer wg.Done()

	for f := range c {
		f()
	}
}

func main() {
	log.SetFlags(log.Lshortfile)
	log.SetPrefix("app=s32pg ")

	flag.Parse()

	if *bucket == "" {
		log.Fatalf("bucket is not set")
	}

	wg.Add(*workers)

	for i := 0; i < *workers; i++ {
		go work()
	}

	c <- func() { list("") }

	wg.Wait()
}
