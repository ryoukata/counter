package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joeshaw/envdecode"
	"github.com/nsqio/go-nsq"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var fatalErr error

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
	fatalErr = e
}

const updateDuration = 1 * time.Second

// connect and close settings for MongoDB.
var db *mgo.Session

func main() {
	defer func() {
		if fatalErr != nil {
			os.Exit(1)
		}
	}()

	log.Println("Connect to DataBase...")
	var err error
	var mongoEnv struct {
		MongoHost   string `env:"MONGO_HOST,required"`
		MongoPort   string `env:"MONGO_PORT,required"`
		MongoDB     string `env:"MONGO_DB,required"`
		MongoUser   string `env:"MONGO_USER,required"`
		MongoPass   string `env:"MONGO_PASS,required"`
		MongoSource string `env:"MONGO_SOURCE,required"`
	}
	if err := envdecode.Decode(&mongoEnv); err != nil {
		fatal(err)
	}
	mongoInfo := &mgo.DialInfo{
		Addrs:    []string{mongoEnv.MongoHost + ":" + mongoEnv.MongoPort},
		Timeout:  20 * time.Second,
		Database: mongoEnv.MongoDB,
		Username: mongoEnv.MongoUser,
		Password: mongoEnv.MongoPass,
		Source:   mongoEnv.MongoSource,
	}
	db, err = mgo.DialWithInfo(mongoInfo)
	if err != nil {
		fatal(err)
		return
	}
	defer func() {
		log.Println("Close Connection for DataBase...")
		db.Close()
	}()
	pollData := db.DB("ballots").C("polls")

	var countsLock sync.Mutex
	var counts map[string]int
	log.Println("Connect to NSQ...")
	// Set up Object to observe NSQ votes Topic.
	var nsqEnv struct {
		NsqHost  string `env:"NSQ_HOST,required"`
		NsqPort  string `env:"NSQ_PORT,required"`
		NsqTopic string `env:"NSQ_TOPIC,required"`
	}
	if err := envdecode.Decode(&nsqEnv); err != nil {
		fatal(err)
	}
	q, err := nsq.NewConsumer(nsqEnv.NsqTopic, "counter", nsq.NewConfig())
	if err != nil {
		fatal(err)
		return
	}

	// Waiting NSQ message regulary.
	q.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		countsLock.Lock()
		defer countsLock.Unlock()
		if counts == nil {
			counts = make(map[string]int)
		}
		vote := string(m.Body)
		counts[vote]++
		return nil
	}))

	// TODO：ホストをコンテナ名に修正すること
	if err := q.ConnectToNSQLookupd(nsqEnv.NsqHost + ":" + nsqEnv.NsqPort); err != nil {
		fatal(err)
		return
	}

	log.Println("Waiting Vote on NSQ...")
	var updater *time.Timer
	// Save counts on MongoDB regulary.
	updater = time.AfterFunc(updateDuration, func() {
		countsLock.Lock()
		defer countsLock.Unlock()
		if len(counts) == 0 {
			log.Println("Not exists New Votes. Skip updating DataBase.")
		} else {
			log.Println("Update DataBase...")
			log.Println(counts)
			ok := true
			for option, count := range counts {
				sel := bson.M{"options": bson.M{"$in": []string{option}}}
				up := bson.M{"$inc": bson.M{"results." + option: count}}
				if _, err := pollData.UpdateAll(sel, up); err != nil {
					log.Println("Failed to Update: ", err)
					ok = false
					continue
				}
				counts[option] = 0
			}
			if ok {
				log.Println("Complete updating DataBase.")
				// Reset Votes counts
				counts = nil
			}
		}
		updater.Reset(updateDuration)
	})

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGHUP)
	for {
		select {
		case <-termChan:
			updater.Stop()
			q.Stop()
		case <-q.StopChan:
			// Completed.
			return
		}
	}

}
