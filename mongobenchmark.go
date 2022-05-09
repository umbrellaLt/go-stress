package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type StarFleet struct {
	Team   string `bson:"team"`
	Person string `bson:"person"`
}

func main() {
	file, err := os.Create("shibuya.log")
	if err != nil {
		log.Fatal(err)
	}

	mw := io.MultiWriter(os.Stdout, file)
	start := time.Now()

	//Lägg in fmt mw till alla timers i coden nedan sen somna om
	fmt.Fprintln(mw, start)

	client, _ := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	collection := client.Database("shibuya_lab").Collection("stress")

	// Drop collection to start from scratch
	collection.Drop(context.Background())

	// Insert documents using 8 goroutines
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go insertMany(&wg)
	}

	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("Insert documents took %s", elapsed)
	fmt.Println()

	// Now, find documents
	start = time.Now()

	batch_size := int32(1000)
	cursor, err := collection.Find(context.Background(), bson.M{"person": "Robert Paulsson"}, &options.FindOptions{BatchSize: &batch_size})
	if err != nil {
		log.Fatal(err)
	}

	var totalChars = 0
	defer cursor.Close(context.Background())
	for cursor.Next(context.Background()) {
		var document bson.M
		if err = cursor.Decode(&document); err != nil {
			log.Fatal(err)
		}

		var team = document["team"].(string)
		var person = document["person"].(string)

		totalChars += len(team) + len(person)
	}

	fmt.Fprintf(mw, "Total chars %d", totalChars)

	elapsed = time.Since(start)
	fmt.Fprintf(mw, "Find documents took %s", elapsed)
}

func insertMany(wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))

	defer func() {
		if err = client.Disconnect(context.Background()); err != nil {
			panic(err)
		}
	}()

	collection := client.Database("shibuya_lab").Collection("stress")

	sec := StarFleet{
		Team:   "DevOps-sec",
		Person: "Rasmus, Robert Rinde",
	}
	dev := StarFleet{
		Team:   "DevOps-dev",
		Person: "Robert Paulsson",
	}
	devops := StarFleet{
		Team:   "DevOps-Engineer",
		Person: "Sai Krishna Ghanta",
	}
	devops2 := StarFleet{
		Team:   "DevOps-Engineer",
		Person: "Robert Paulsson",
	}

	org := []interface{}{sec}

	for j := 0; j < 33; j++ {
		org = append(org, dev, devops, devops2)
	}

	for i := 0; i < 6250; i++ {
		cloned_org := make([]interface{}, len(org))
		copy(cloned_org, org)

		_, err := collection.InsertMany(context.Background(), cloned_org)

		if err != nil {
			log.Fatal(err)
		}

	}

}
