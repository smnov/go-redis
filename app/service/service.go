package service

import (
	"log/slog"

	"github.com/codecrafters-io/redis-starter-go/app/server"
	"github.com/codecrafters-io/redis-starter-go/app/storage"
)

type ServerService struct {
	sv server.Server
}

func NewServerService(port int, logger slog.Logger, kv storage.KeyValue, replica server.Replica) *ServerService {
	var s server.Server
	cfg := server.NewConfig(port, &logger, kv, replica)
	if replica.MasterHost != "" && replica.MasterPort != "" {
		s = *server.NewSlaveServer(cfg)
	} else {
		s = *server.NewMasterServer(cfg)
	}
	return &ServerService{
		sv: s,
	}
}

func (svc *ServerService) Start() error {
	err := svc.sv.Start()
	if err != nil {
		return err
	}
	return nil
}
