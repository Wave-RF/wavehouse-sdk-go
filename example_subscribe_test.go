package wavehouse_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

func ExampleClient_Subscribe() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	// The ctx bounds the whole subscription: here, until Ctrl-C.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

	sub, err := wh.Subscribe(ctx, "clicks", &wavehouse.SubscribeOptions{
		Since: time.Now().Add(-time.Hour), // replay the last hour first
	})
	// An unknown table is no error: the subscription stays live and empty, so
	// check the table first with TableSchema or a Limit(1) query.
	if err != nil {
		log.Fatal(err)
	}

	for sub.Next() {
		ev := sub.Event()
		fmt.Println(ev.ReceivedAt, ev.Row())
	}
	// Err is nil when the loop ended because ctx was cancelled.
	err = sub.Err()
	_ = sub.Close()
	stop()
	if err != nil {
		log.Fatal(err)
	}
}

// Events is the channel form of Next, for a loop that also waits on other sources.
func ExampleSubscription_Events() {
	wh, err := wavehouse.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	sub, err := wh.Subscribe(ctx, "clicks", nil)
	if err != nil {
		stop()
		log.Fatal(err)
	}
	defer func() { _ = sub.Close(); stop() }()

	tick := time.NewTicker(time.Minute)
	defer func() { tick.Stop() }()
	for events := sub.Events(); ; {
		select {
		case ev, ok := <-events:
			if !ok {
				// Closed: the ctx was cancelled, Close was called, or the stream ended for good.
				if err := sub.Err(); err != nil {
					log.Print(err)
				}
				return
			}
			fmt.Println(ev.Row())
		case <-tick.C:
			fmt.Println("still listening")
		}
	}
}

func ExampleEvent_Decode() {
	wh, err := wavehouse.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	sub, err := wh.Subscribe(context.Background(), "clicks", nil)
	if err != nil {
		log.Fatal(err)
	}

	type click struct {
		Page   string  `json:"page"`
		Button string  `json:"button"`
		Score  float64 `json:"score"`
		UserID uint64  `json:"user_id"` // exact, even above 2^53
	}
	for sub.Next() {
		var c click
		if err := sub.Event().Decode(&c); err != nil {
			log.Print(err)
			continue
		}
		fmt.Println(c.Page, c.Button, c.Score, c.UserID)
	}
	if err := sub.Err(); err != nil {
		log.Fatal(err)
	}
}

// A long-lived consumer, such as a dashboard backend, reports the connection
// state and checkpoints the last event, to resume from after a restart.
func ExampleSubscribeOptions() {
	wh, err := wavehouse.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	checkpoint := loadCheckpoint() // "" on the first run

	sub, err := wh.Subscribe(context.Background(), "clicks", &wavehouse.SubscribeOptions{
		ResumeID: checkpoint,
		// Called on the subscription's goroutine: hand the state off, don't block.
		OnState: func(s wavehouse.State) { log.Println("stream", s) },
	})
	if err != nil {
		log.Fatal(err)
	}

	for sub.Next() {
		ev := sub.Event()
		fmt.Println(ev.Row())
		saveCheckpoint(ev.ID)
	}
	if err := sub.Err(); err != nil {
		log.Fatal(err)
	}
}

func loadCheckpoint() string { return "" }

func saveCheckpoint(string) {}
