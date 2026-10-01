package main

import "testing"

func TestConfigMembership(t *testing.T) {
	t.Setenv("CACHE_NODE_ID", "a")
	t.Setenv("CACHE_LISTEN_ADDR", ":8080")
	t.Setenv("CACHE_PEERS", `{"b":"http://localhost:8081"}`)
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Peers["a"]; ok {
		t.Fatal("injected local node into explicit membership")
	}
	t.Setenv("CACHE_PEERS", "invalid")
	if _, err := loadConfig(); err == nil {
		t.Fatal("accepted malformed membership")
	}
}

func TestQuorumConfiguration(t *testing.T) {
	t.Setenv("CACHE_NODE_ID", "a")
	t.Setenv("CACHE_LISTEN_ADDR", ":8080")
	t.Setenv("CACHE_PEERS", `{"a":"http://localhost:8080","b":"http://localhost:8081","c":"http://localhost:8082"}`)
	t.Setenv("CACHE_REPLICAS", "")
	t.Setenv("CACHE_READ_QUORUM", "")
	t.Setenv("CACHE_WRITE_QUORUM", "")
	c, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.Replication.N != 3 || c.Replication.R != 2 || c.Replication.W != 2 {
		t.Fatal("incorrect defaults")
	}
	t.Setenv("CACHE_REPLICAS", "1")
	c, err = loadConfig()
	if err != nil || c.Replication.R != 1 || c.Replication.W != 1 {
		t.Fatal("defaults do not follow N")
	}
	t.Setenv("CACHE_WRITE_QUORUM", "2")
	if _, err := loadConfig(); err == nil {
		t.Fatal("accepted W>N")
	}
}
