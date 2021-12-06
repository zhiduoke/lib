package liteconsul

import (
	"log"
	"testing"
	"time"
)

func TestKV(t *testing.T) {
	client := NewClient("http://localhost:8500", "")
	lastIndex := uint64(0)
	ents, meta, err := client.KVList("infra/apigateway/data/", &QueryOptions{
		LastIndex: lastIndex,
		WaitTime:  time.Second * 10,
	})
	if meta != nil {
		t.Log("meta", meta)
		lastIndex = meta.LastIndex
	}
	if err != nil {
		if e, ok := err.(*Error); ok {
			t.Fatalf("%#+v", e)
		}
		t.Fatal(err)
	}
	t.Log("ents", ents)
}

func TestWatchKey(t *testing.T) {
	client := NewClient("http://localhost:8500", "")
	lastNotified := uint64(0)
	for {
		_, meta, err := client.KVGet("infra/apigateway/notify", &QueryOptions{
			LastIndex: lastNotified,
			WaitTime:  60 * time.Second,
		})
		if err != nil && !IsNotFound(err) {
			log.Printf("pull notify: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if meta.LastIndex == lastNotified {
			continue
		}
		log.Printf("notify")
		lastNotified = meta.LastIndex
	}
}
