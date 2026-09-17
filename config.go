package main

import "os"

type config struct {
	ListenAddress string
	Capacity      int
	WALPath       string
}

func loadConfig() config {
	address := os.Getenv("CACHE_LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}

	walPath := os.Getenv("CACHE_WAL_PATH")
	if walPath == "" {
		walPath = "cache.wal"
	}
	return config{
		ListenAddress: address,
		Capacity:      1_000,
		WALPath:       walPath,
	}
}
