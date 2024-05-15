package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	storage "github.com/codecrafters-io/redis-starter-go/app/storage"
)

func generateMasterID() string {
	masterID := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	return masterID
}

type MasterServer struct {
	Port             int
	MasterReplid     string
	MasterReplOffset string
	Replicas         map[*Client]bool
	Logger           *slog.Logger
	KeyValue         storage.KeyValue
}

func NewMasterServer(cfg *Config) *MasterServer {
	masterID := generateMasterID()
	offset := "0"
	return &MasterServer{
		Port:             cfg.port,
		MasterReplid:     masterID,
		MasterReplOffset: offset,
		Replicas:         make(map[*Client]bool),
		Logger:           cfg.logger,
		KeyValue:         cfg.kv,
	}
}

func (ms MasterServer) Start() error {
	host := "0.0.0.0:"
	address := host + fmt.Sprintf("%d", ms.Port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	ms.Logger.Info("Server started successfully", "port", ms.Port)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			ms.Logger.Error("error during handle connection", "error", err.Error())
		}

		client := NewClient(conn)
		go ms.handleConnection(*client)
	}
}

func (ms MasterServer) sendTaskToReplica(task []string, conn net.Conn) error {
	arr := resp.CreateArray(task)
	bytes, err := json.Marshal(arr)
	if err != nil {
		return err
	}
	_, err = conn.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (ms MasterServer) Ping(ctx context.Context, cl Client) (bool, error) {
	_, err := cl.conn.Write([]byte(PongCommand))
	if err != nil {
		ms.Logger.Error(err.Error())
		return false, err
	}
	return true, nil
}

func (ms MasterServer) responseLoop(ctx context.Context, conn net.Conn, response, expectedResponse []byte) error {
	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout")
		default:
			_, err := conn.Read(response)
			if err != nil {
				return err
			}
			if bytes.Equal(response, expectedResponse) {
				return nil
			}
		}
	}
}

func (ms MasterServer) Set(ctx context.Context, input []string, cl Client) error {
	withTimeout, _ := context.WithTimeout(ctx, time.Second*6)
	if len(ms.Replicas) > 0 {
		for k, v := range ms.Replicas {
			if v {
				err := ms.sendTaskToReplica(input, k.conn)
				if err != nil {
					return err
				}
				expectedResponse := []byte(StatusOK)
				response := make([]byte, len(expectedResponse))
				err = ms.responseLoop(withTimeout, cl.conn, response, expectedResponse)
				if err != nil {
					return err
				}
				return nil
			}
		}
	} else {
		key := input[1]
		value := input[2]
		args := make(map[string]string)
		if len(input) > 3 {
			if input[3] == "px" { // px allows setting a key with expiry
				args["px"] = input[4]
			}
		}
		ms.KeyValue.SetVariable(key, value, args)

	}
	_, err := cl.conn.Write([]byte(StatusOK))
	if err != nil {
		return err
	}
	return nil
}

func (ms MasterServer) Get(ctx context.Context, key string, cl Client) {
	var arr []string
	v, err := ms.KeyValue.GetVariable(key)
	if err != nil {
		ms.Logger.Error("error while getting value", "error", err.Error())
		cl.conn.Write([]byte(nullBulkString))
		return
	}
	arr = append(arr, v)
	ecnodedV := resp.CreateBulkStringFromArray(arr)
	_, err = cl.conn.Write([]byte(ecnodedV))
	if err != nil {
		ms.Logger.Error("error while writing response", "error", err.Error())
		return
	}
}

func (ms MasterServer) Info(ctx context.Context, args []string, cl Client) {
	cmd := args[0]
	switch cmd {
	case "replication":
		{
			role := "role:" + ms.Role()
			id := "master_replid:" + ms.MasterReplid
			offset := "master_repl_offset:" + ms.MasterReplOffset
			b := resp.CreateBulkString(role + "\r\n" + id + "\r\n" + offset + "\r\n")
			_, err := cl.conn.Write([]byte(b))
			if err != nil {
				ms.Logger.Error("error while handling info command", "error", err)
			}
		}
	}
}

func (ms MasterServer) Echo(ctx context.Context, input []string, cl Client) error {
	resString := resp.CreateBulkStringFromArray(input)
	b := []byte(resString)
	_, err := cl.conn.Write(b)
	if err != nil {
		return err
	}
	return nil
}

func (ms MasterServer) HandleReplconfCommand(ctx context.Context, args []string, cl Client) {
	ms.WriteResponse(cl, "+OK\r\n")
}

func (ms MasterServer) HandlePsyncCommand(ctx context.Context, args []string, cl Client) {
	response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", ms.MasterReplid)
	err := ms.WriteResponse(cl, response)
	if err != nil {
		ms.Logger.Error("error handling psync command", "error", err)
	}
	err = ms.SendRDBFile(cl)
	if err != nil {
		ms.Logger.Error("error sending rdb file", "error", err)
		ms.Logger.Info("closing connection...")
		cl.conn.Close()
		return
	}
	ms.Replicas[&cl] = true
}

func (ms MasterServer) SendRDBFile(cl Client) error {
	emptyRDBFile, err := rdb.DecodeRDBFile()
	if err != nil {
		return err
	}
	response := "$" + strconv.Itoa(len(emptyRDBFile)) + "\r\n" + emptyRDBFile
	err = ms.WriteResponse(cl, response)
	if err != nil {
		return err
	}
	return nil
}

func (ms MasterServer) Role() string {
	return "master"
}

func (ms MasterServer) WriteResponse(cl Client, data string) error {
	_, err := cl.conn.Write([]byte(data))
	if err != nil {
		return err
	}
	return nil
}

func (ms MasterServer) handleConnection(cl Client) {
	defer cl.conn.Close()
	ms.Logger.Info("New connection accepted", "address", cl.conn.RemoteAddr())
	for {
		n, err := cl.conn.Read(cl.data)
		if err != nil {
			if err == io.EOF {
				ms.Logger.Error("connection closed by client")
				break
			}
			ms.Logger.Error("error reading from connection: %v", err)
			return
		}
		receivedData := string(cl.data[:n])
		fmt.Printf("receivedData: %v\n", receivedData)
		if len(receivedData) > 2 {
			parsedString, err := resp.ParseRequestString(receivedData)
			fmt.Printf("parsedString: %v\n", parsedString)
			if err != nil {
				ms.Logger.Error(err.Error())
			}
			command := strings.ToLower(parsedString[0])
			ctx := context.Background()
			fmt.Printf("command: %v\n", command)
			switch command {
			// Common commands
			case "set":
				ms.Set(ctx, parsedString, cl)
			case "get":
				ms.Get(ctx, parsedString[1], cl)
			case "echo":
				ms.Echo(ctx, parsedString[1:], cl)
			case "info":
				ms.Info(ctx, parsedString[1:], cl)
			// Master server commands
			case "replconf":
				ms.HandleReplconfCommand(ctx, parsedString[1:], cl)
			case "psync":
				ms.HandlePsyncCommand(ctx, parsedString[1:], cl)
			default:
				ms.Ping(ctx, cl)
			}
		}
	}
}
