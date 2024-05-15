package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/service"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

func main() {
	var port int
	var replica server.Replica

	for i, arg := range os.Args {
		if arg == "--replicaof" {
			replica.MasterHost = os.Args[i+1]
			replica.MasterPort = os.Args[i+2]
		}
	}

	flag.IntVar(&port, "port", 6379, "set port of the server")
	flag.String("replicaof", "replicaof", "replicaof")
	flag.Parse()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	kv := storage.NewKeyValue()
	service := service.NewServerService(port, *logger, kv, replica)
	err := service.Start()
	if err != nil {
		panic(err)
	}
}
