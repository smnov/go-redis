package server

import (
	"bytes"
	"context"
	"log/slog"
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

const (
	nullBulkString = "$-1\r\n"
)

type Server interface {
	Start() error
	Ping(context.Context, Client) (bool, error)
	Set(context.Context, []string, Client) error
	Get(context.Context, string, Client)
	Info(context.Context, []string, Client)
	Role() string
}

type Replica struct {
	MasterHost string
	MasterPort string
}

type Config struct {
	port    int
	logger  *slog.Logger
	kv      storage.KeyValue
	replica Replica
}

// Client that wil connect to a server
type Client struct {
	conn net.Conn
	data []byte
	d    bytes.Buffer
}

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
		data: make([]byte, 1024),
	}
}

func NewConfig(port int, logger *slog.Logger, kv storage.KeyValue, replica Replica) *Config {
	return &Config{
		port:    port,
		logger:  logger,
		kv:      kv,
		replica: replica,
	}
}
