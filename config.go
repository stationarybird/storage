package main

import (
	"distributedcache/internal/replication"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type config struct {
	ListenAddress string
	Capacity      int
	WALPath       string
	NodeID        string
	Peers         map[string]string
	Replication   replication.Options
}

func loadConfig() (config, error) {
	address := os.Getenv("CACHE_LISTEN_ADDR")
	if address == "" {
		address = ":8080"
	}

	walPath := os.Getenv("CACHE_WAL_PATH")
	if walPath == "" {
		walPath = "replicated.wal"
	}
	id := os.Getenv("CACHE_NODE_ID")
	if id == "" {
		id = "node-a"
	}
	peers := map[string]string{id: "http://localhost:8080"}
	if raw := os.Getenv("CACHE_PEERS"); raw != "" {
		peers = nil
		if err := json.Unmarshal([]byte(raw), &peers); err != nil {
			return config{}, fmt.Errorf("CACHE_PEERS: %w", err)
		}
	} else if address != ":8080" {
		return config{}, fmt.Errorf("set CACHE_PEERS when using a custom listen address")
	}
	options := replication.Defaults(len(peers))
	for _, setting := range []struct {
		name   string
		target *int
	}{{"CACHE_REPLICAS", &options.N}, {"CACHE_READ_QUORUM", &options.R}, {"CACHE_WRITE_QUORUM", &options.W}} {
		if raw := os.Getenv(setting.name); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil {
				return config{}, fmt.Errorf("%s must be an integer", setting.name)
			}
			*setting.target = n
		}
	}
	if os.Getenv("CACHE_READ_QUORUM") == "" {
		options.R = options.N/2 + 1
	}
	if os.Getenv("CACHE_WRITE_QUORUM") == "" {
		options.W = options.N/2 + 1
	}
	if options.N < 1 || options.N > len(peers) || options.R < 1 || options.R > options.N || options.W < 1 || options.W > options.N {
		return config{}, fmt.Errorf("require 1 <= R,W <= N <= membership")
	}
	return config{
		ListenAddress: address,
		Capacity:      1_000,
		WALPath:       walPath,
		NodeID:        id,
		Peers:         peers,
		Replication:   options,
	}, nil
}
