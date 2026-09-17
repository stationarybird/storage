package main

import "os"

type config struct {
	ListenAddress string
	Capacity      int
}

func loadConfig() config {
	address := os.Getenv("CACHE_LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}

	return config{
		ListenAddress: address,
		// You will make this capacity meaningful when implementing LRU eviction.
		Capacity: 1_000,
	}
}
